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
    warnings: tuple[str, ...] = ()

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
        object.__setattr__(self, "warnings", _normalize_warnings(self.warnings))


class DirectoryChildCommittedAfterCancellation(asyncio.CancelledError):
    """A child publish completed before caller cancellation took effect."""

    def __init__(self, result: DirectoryChildResult) -> None:
        super().__init__()
        self.result = result


class DirectoryDestinationFinalizedAfterCancellation(asyncio.CancelledError):
    """The destination was fully verified before cancellation took effect."""


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
        object.__setattr__(self, "warnings", _normalize_warnings(self.warnings))


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
        destination_finalized_after_cancellation = False
        result: DirectoryTransferResult | None = None
        failure: BaseException | None = None
        try:
            await backend.preflight(manifest)
            try:
                await backend.prepare_destination(mark_issued)
                for entry in manifest.entries:
                    try:
                        child = await backend.copy_child(entry, new_uuid7(), mark_issued)
                    except DirectoryChildCommittedAfterCancellation as exc:
                        _validate_child(entry, exc.result)
                        committed.append(exc.result)
                        raise asyncio.CancelledError from None
                    _validate_child(entry, child)
                    committed.append(child)
                try:
                    await backend.finalize_destination(tuple(committed))
                except DirectoryDestinationFinalizedAfterCancellation:
                    destination_finalized_after_cancellation = True
            except BaseException:
                if issued and not await _cleanup_destination(
                    backend,
                    tuple(committed),
                ):
                    raise DeviceOutcomeUnknownError(
                        "Directory transfer outcome is unknown; check destination before retrying"
                    ) from None
                raise

            warnings = tuple(
                warning for child in committed for warning in child.warnings
            )
            if mode == "move":
                if destination_finalized_after_cancellation:
                    warnings += ("source_cleanup_incomplete",)
                else:
                    warnings += await _finish_source_cleanup(backend, manifest)
            content = tuple(
                DirectoryContentEntry(
                    relative_path=child.relative_path,
                    size=child.verified_size,
                    sha256=child.verified_sha256,
                )
                for child in committed
            )
            result = DirectoryTransferResult(
                kind="directory",
                files_transferred=len(committed),
                bytes_transferred=sum(child.verified_size for child in committed),
                sha256=directory_content_sha256(content),
                warnings=warnings,
            )
        except BaseException as exc:
            failure = exc

        release_error, release_cancelled = await _release_resources(
            backend,
            operation_lease,
        )
        if result is not None:
            if release_error is not None:
                result = DirectoryTransferResult(
                    kind=result.kind,
                    files_transferred=result.files_transferred,
                    bytes_transferred=result.bytes_transferred,
                    sha256=result.sha256,
                    warnings=result.warnings + ("transfer_ack_failed",),
                )
            return result
        if failure is not None:
            raise failure
        if release_cancelled:
            raise asyncio.CancelledError
        if release_error is not None:
            raise release_error
        raise AssertionError("directory transfer completed without an outcome")


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
) -> tuple[BaseException | None, bool]:
    release_task = asyncio.create_task(backend.release())
    release_error, release_cancelled = await _settle_resource(release_task)
    lease_task = asyncio.create_task(operation_lease.aclose())
    lease_error, lease_cancelled = await _settle_resource(lease_task)
    return release_error or lease_error, release_cancelled or lease_cancelled


async def _settle_resource(
    task: asyncio.Task[None],
) -> tuple[BaseException | None, bool]:
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            break
    try:
        task.result()
    except BaseException as exc:
        return exc, cancelled
    return None, cancelled


def _normalize_warnings(warnings: tuple[str, ...]) -> tuple[str, ...]:
    if set(warnings).difference(_WARNING_PRIORITY):
        raise ValueError("directory transfer warning is invalid")
    normalized = tuple(warning for warning in _WARNING_PRIORITY if warning in warnings)
    if len(normalized) > 8:
        raise ValueError("too many directory transfer warnings")
    return normalized
