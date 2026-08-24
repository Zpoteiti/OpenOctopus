from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.devices.registry import DeviceOutcomeUnknownError
from openctopus_server.devices.transfer import TransferIntegrityError
from openctopus_server.directory_contract import (
    DirectoryContentEntry,
    DirectoryManifest,
    DirectoryManifestEntry,
    directory_content_sha256,
)

_WARNING_PRIORITY = (
    "transfer_ack_failed",
    "source_delete_failed",
    "source_changed_after_copy",
    "source_cleanup_incomplete",
)


class DirectoryOperationLease(Protocol):
    async def aclose(self) -> None: ...


@dataclass(frozen=True, slots=True)
class DirectoryChildResult:
    relative_path: str
    verified_size: int
    verified_sha256: str
    destination_fingerprint: str

    def __post_init__(self) -> None:
        if self.verified_size < 0:
            raise ValueError("directory child size must be non-negative")
        if len(self.verified_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.verified_sha256
        ):
            raise ValueError("directory child digest is invalid")
        if (
            not 1 <= len(self.destination_fingerprint) <= 512
            or not self.destination_fingerprint.isascii()
            or any(
                not 0x21 <= ord(character) <= 0x7E
                for character in self.destination_fingerprint
            )
        ):
            raise ValueError("directory destination fingerprint is invalid")


@dataclass(frozen=True, slots=True)
class DirectoryTransferResult:
    kind: Literal["directory"]
    files_transferred: int
    bytes_transferred: int
    sha256: str
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not 1 <= self.files_transferred <= 10_000:
            raise ValueError("directory transfer count is invalid")
        if self.bytes_transferred < 0:
            raise ValueError("directory transfer byte count is invalid")
        if len(self.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.sha256
        ):
            raise ValueError("directory transfer digest is invalid")
        unknown = set(self.warnings).difference(_WARNING_PRIORITY)
        if unknown:
            raise ValueError("directory transfer warning is invalid")
        normalized = tuple(
            warning for warning in _WARNING_PRIORITY if warning in self.warnings
        )
        object.__setattr__(self, "warnings", normalized)


class DirectoryTransferBackend(Protocol):
    async def preflight(self, manifest: DirectoryManifest) -> None: ...

    async def prepare_destination(self, mark_issued: Callable[[], None]) -> None: ...

    async def copy_child(
        self,
        entry: DirectoryManifestEntry,
        slot_id: UUID,
        mark_issued: Callable[[], None],
    ) -> DirectoryChildResult: ...

    async def finalize_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> None: ...

    async def cleanup_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> bool: ...

    async def cleanup_source(self, manifest: DirectoryManifest) -> tuple[str, ...]: ...

    async def release(self) -> None: ...


class DirectoryTransferCoordinator:
    """Sequence one bounded manifest without owning transport or storage logic."""

    async def run(
        self,
        *,
        manifest: DirectoryManifest,
        mode: Literal["copy", "move"],
        backend: DirectoryTransferBackend,
        operation_lease: DirectoryOperationLease,
        on_issued: Callable[[], None] | None = None,
    ) -> DirectoryTransferResult:
        issued = False

        def mark_issued() -> None:
            nonlocal issued
            if issued:
                return
            issued = True
            if on_issued is not None:
                on_issued()

        committed: list[DirectoryChildResult] = []
        try:
            await backend.preflight(manifest)
            try:
                await backend.prepare_destination(mark_issued)
                for entry in manifest.entries:
                    child = await backend.copy_child(entry, new_uuid7(), mark_issued)
                    _validate_child(entry, child)
                    committed.append(child)
                await backend.finalize_destination(tuple(committed))
            except BaseException:
                if issued and not await _cleanup_destination(
                    backend,
                    tuple(committed),
                ):
                    raise DeviceOutcomeUnknownError(
                        "Directory transfer outcome is unknown; check destination before retrying"
                    ) from None
                raise

            warnings: tuple[str, ...] = ()
            if mode == "move":
                warnings = await _finish_source_cleanup(backend, manifest)
            content = tuple(
                DirectoryContentEntry(
                    relative_path=child.relative_path,
                    size=child.verified_size,
                    sha256=child.verified_sha256,
                )
                for child in committed
            )
            return DirectoryTransferResult(
                kind="directory",
                files_transferred=len(committed),
                bytes_transferred=sum(child.verified_size for child in committed),
                sha256=directory_content_sha256(content),
                warnings=warnings,
            )
        finally:
            await _release_resources(backend, operation_lease)


def _validate_child(
    expected: DirectoryManifestEntry,
    actual: DirectoryChildResult,
) -> None:
    if (
        actual.relative_path != expected.relative_path
        or actual.verified_size != expected.size
    ):
        raise TransferIntegrityError(
            "directory child result mismatched its manifest entry"
        )


async def _cleanup_destination(
    backend: DirectoryTransferBackend,
    committed: tuple[DirectoryChildResult, ...],
) -> bool:
    cleanup = asyncio.create_task(backend.cleanup_destination(committed))
    try:
        return await await_future_cancellation_safe(cleanup)
    except Exception:
        return False


async def _finish_source_cleanup(
    backend: DirectoryTransferBackend,
    manifest: DirectoryManifest,
) -> tuple[str, ...]:
    cleanup = asyncio.create_task(backend.cleanup_source(manifest))
    while True:
        try:
            return await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            if cleanup.done():
                try:
                    return cleanup.result()
                except BaseException:
                    return ("source_cleanup_incomplete",)
            continue
        except Exception:
            return ("source_cleanup_incomplete",)


async def _release_resources(
    backend: DirectoryTransferBackend,
    operation_lease: DirectoryOperationLease,
) -> None:
    release_error: BaseException | None = None
    release_task = asyncio.create_task(backend.release())
    try:
        await await_future_cancellation_safe(release_task)
    except BaseException as exc:
        release_error = exc
    lease_task = asyncio.create_task(operation_lease.aclose())
    await await_future_cancellation_safe(lease_task)
    if release_error is not None:
        raise release_error
