from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, cast
from uuid import UUID

from pydantic import ValidationError

from openctopus_server.devices.registry import (
    DeviceBusyError,
    DeviceOutcomeUnknownError,
    DeviceRouteSnapshot,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import TransferError, TransferIntegrityError
from openctopus_server.devices.workspace import (
    INTERNAL_WORKSPACE_ACTION,
    DestinationDirectoryJobStatus,
    DestinationDirectoryStartResult,
    DirectoryCommandResult,
    DirectoryDeviceResult,
    DirectorySourceProbe,
    LocalDirectoryJobStatus,
    SourceDirectoryJobStatus,
    SourceDirectoryStartResult,
    build_directory_action,
    parse_directory_result,
)
from openctopus_server.directory_contract import (
    MAX_DIRECTORY_PAGE_BYTES,
    DirectoryManifest,
    DirectoryManifestDirectory,
    DirectoryManifestDirectoryItem,
    DirectoryManifestEntry,
    DirectoryManifestFileItem,
    DirectoryManifestPage,
    canonical_json_bytes,
    create_directory_manifest,
)
from openctopus_server.errors.codes import ErrorCode

_CONTROL_RESULT_BYTES = 64 * 1024
_PAGE_RESULT_BYTES = MAX_DIRECTORY_PAGE_BYTES + 4 * 1024
_SOURCE_TERMINAL_STATES = frozenset({"succeeded", "failed", "outcome_unknown"})
_DESTINATION_TERMINAL_STATES = frozenset({"failed", "outcome_unknown"})
_LOCAL_TERMINAL_STATES = frozenset({"succeeded", "failed", "outcome_unknown"})
_COMMAND_STATES: dict[str, str] = {
    "transfer_source_probe_start": "running",
    "transfer_source_probe_hold": "held",
    "transfer_source_probe_cancel": "accepted",
    "transfer_source_probe_release": "released",
    "transfer_directory_authorize_source_child": "accepted",
    "transfer_source_cleanup": "accepted",
    "transfer_directory_preflight": "running",
    "transfer_directory_prepare": "accepted",
    "transfer_directory_authorize_child": "accepted",
    "transfer_directory_finish": "accepted",
    "transfer_directory_cancel": "accepted",
    "transfer_directory_release": "released",
    "transfer_local_directory_start": "running",
    "transfer_local_directory_cancel": "accepted",
    "transfer_local_directory_release": "released",
}

type _SourceState = str
type _DestinationState = str
type _LocalState = str
type _Sleep = Callable[[float], Awaitable[None]]


class _DirectoryRegistry(Protocol):
    async def dispatch_tool_on_snapshot(
        self,
        *,
        route: DeviceRouteSnapshot,
        user_id: UUID,
        expected_device_name: str,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        on_issued: Callable[[], None] | None = None,
    ) -> Any: ...


@dataclass(slots=True)
class _OneShot:
    parameters_sha256: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    issued: bool = False
    result: DirectoryDeviceResult | None = None
    failure: Exception | None = None


@dataclass(slots=True)
class _Release:
    parameters_sha256: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    result: DirectoryCommandResult | None = None


class DeviceDirectoryJobController:
    """Control one generation-frozen Client directory operation.

    The controller owns Server-side command issuance history.  One-shot
    commands are never replayed after their send callback fires; ambiguous
    results are reconciled only through bounded status/cancel/release calls.
    """

    def __init__(
        self,
        *,
        registry: _DirectoryRegistry,
        route: DeviceRouteSnapshot,
        user_id: UUID,
        directory_operation_id: UUID,
        idle_timeout_seconds: float,
        poll_interval_seconds: float = 1.0,
        call_timeout_seconds: float = 30.0,
        monotonic: Callable[[], float] | None = None,
        sleep: _Sleep | None = None,
    ) -> None:
        if idle_timeout_seconds <= 0:
            raise ValueError("directory idle timeout must be positive")
        if poll_interval_seconds <= 0:
            raise ValueError("directory poll interval must be positive")
        if call_timeout_seconds <= 0:
            raise ValueError("directory call timeout must be positive")
        if directory_operation_id.version != 7:
            raise ValueError("directory operation ID must be UUID v7")
        self._registry = registry
        self.route = route
        self.user_id = user_id
        self.directory_operation_id = directory_operation_id
        self._idle_timeout = idle_timeout_seconds
        self._poll_interval = poll_interval_seconds
        self._call_timeout = call_timeout_seconds
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._one_shots: dict[str, _OneShot] = {}
        self._releases: dict[str, _Release] = {}
        self._poll_locks = {
            "source": asyncio.Lock(),
            "destination": asyncio.Lock(),
        }
        self._wait_locks = {
            "source": asyncio.Lock(),
            "destination": asyncio.Lock(),
            "local": asyncio.Lock(),
        }
        self._last_progress = {"source": -1, "destination": -1, "local": -1}
        self._source_path: str | None = None
        self._source_request_digest: str | None = None
        self._source_digest: str | None = None
        self._destination_path: str | None = None
        self._destination_request_digest: str | None = None
        self._destination_digest: str | None = None
        self._source_mutation_issued = False
        self._destination_mutation_issued = False

    @property
    def source_expected_digest(self) -> str | None:
        return self._source_digest

    @property
    def destination_expected_digest(self) -> str | None:
        return self._destination_digest

    async def start_source_probe(self, path: str) -> SourceDirectoryStartResult:
        request_digest = _request_digest({"role": "source", "path": path, "version": 1})
        if self._source_path is None:
            self._source_path = path
            self._source_request_digest = request_digest
            self._source_digest = request_digest
        elif self._source_path != path or self._source_request_digest != request_digest:
            raise TransferIntegrityError("source probe parameters changed")

        async def issue(mark_issued: Callable[[], None]) -> DirectoryDeviceResult:
            return await self._dispatch(
                "transfer_source_probe_start",
                {"path": path},
                on_issued=mark_issued,
            )

        async def reconcile(original: BaseException) -> DirectoryDeviceResult:
            return await self._reconcile_one_status(
                lambda deadline: self.get_source_status(_deadline=deadline),
                original=original,
                mutation_issued=False,
            )

        result = await self._one_shot(
            "source:start",
            {"path": path},
            issue=issue,
            reconcile=reconcile,
        )
        if not isinstance(result, (DirectoryCommandResult, SourceDirectoryJobStatus)):
            raise TransferIntegrityError("source start returned the wrong result type")
        self._accept_source_result(result)
        return result

    async def get_source_status(
        self,
        *,
        outer_progress_seq: int | None = None,
        _deadline: float | None = None,
    ) -> SourceDirectoryJobStatus:
        digest = self._require_source_digest()
        payload: dict[str, object] = {"expected_digest": digest}
        if outer_progress_seq is not None:
            payload["outer_progress_seq"] = outer_progress_seq
        async with self._poll_locks["source"]:
            result = await self._dispatch(
                "transfer_source_probe_status", payload, deadline=_deadline
            )
        if not isinstance(result, SourceDirectoryJobStatus):
            raise TransferIntegrityError("source status returned the wrong result type")
        self._accept_source_status(result)
        self._observe_progress("source", result.progress_seq)
        return result

    async def get_source_page(self, offset: int) -> DirectoryManifestPage:
        digest = self._require_source_digest()

        async def read(deadline: float) -> DirectoryManifestPage:
            result = await self._dispatch(
                "transfer_source_probe_page",
                {"expected_digest": digest, "offset": offset},
                deadline=deadline,
            )
            if not isinstance(result, DirectoryManifestPage):
                raise TransferIntegrityError("source page returned the wrong result type")
            return result

        return cast(DirectoryManifestPage, await self._retry_read(read))

    async def retrieve_source_manifest(
        self,
        probe: DirectorySourceProbe,
    ) -> DirectoryManifest:
        if probe.manifest_sha256 != self._require_source_digest():
            raise TransferIntegrityError("source probe digest changed")
        directories: list[DirectoryManifestDirectory] = []
        entries: list[DirectoryManifestEntry] = []
        previous_path: bytes | None = None
        offset = 0
        pages = 0
        while True:
            page = await self.get_source_page(offset)
            if page.offset != offset:
                raise TransferIntegrityError("source page offset is not contiguous")
            if page.next_offset is not None and (
                not page.items
                or page.next_offset != page.offset + len(page.items)
                or page.next_offset <= page.offset
            ):
                raise TransferIntegrityError("source page did not advance contiguously")
            pages += 1
            for item in page.items:
                encoded_path = item.relative_path.encode("utf-8")
                if previous_path is not None and encoded_path <= previous_path:
                    raise TransferIntegrityError("source pages are not globally ordered")
                previous_path = encoded_path
                if isinstance(item, DirectoryManifestDirectoryItem):
                    directories.append(
                        DirectoryManifestDirectory(
                            relative_path=item.relative_path,
                            identity=item.identity,
                        )
                    )
                elif isinstance(item, DirectoryManifestFileItem):
                    entries.append(
                        DirectoryManifestEntry(
                            relative_path=item.relative_path,
                            size=item.size,
                            fingerprint=item.fingerprint,
                        )
                    )
                else:
                    raise TransferIntegrityError("source page item has an unknown type")
            if page.next_offset is None:
                break
            offset = page.next_offset
        try:
            manifest = create_directory_manifest(
                root_identity=probe.root_identity,
                directories=tuple(directories),
                entries=tuple(entries),
            )
        except (ValidationError, ValueError) as exc:
            raise TransferIntegrityError("source manifest could not be reconstructed") from exc
        if (
            pages != probe.page_count
            or manifest.scanned_entries != probe.scanned_entries
            or len(manifest.entries) != probe.file_count
            or manifest.total_bytes != probe.total_bytes
            or manifest.manifest_sha256 != probe.manifest_sha256
        ):
            raise TransferIntegrityError("source manifest summary mismatched its pages")
        return manifest

    async def hold_source_probe(
        self,
    ) -> DirectoryCommandResult | SourceDirectoryJobStatus:
        return cast(
            DirectoryCommandResult | SourceDirectoryJobStatus,
            await self._source_command(
                key="source:hold",
                operation="transfer_source_probe_hold",
                payload={},
                ambiguous_status_is_success=True,
                evidence_states=frozenset(
                    {"held", "source_cleanup", "succeeded", "failed", "outcome_unknown"}
                ),
            ),
        )

    async def cancel_source_probe(
        self,
        *,
        _deadline: float | None = None,
    ) -> DirectoryCommandResult | SourceDirectoryJobStatus:
        return cast(
            DirectoryCommandResult | SourceDirectoryJobStatus,
            await self._source_command(
                key="source:cancel",
                operation="transfer_source_probe_cancel",
                payload={},
                ambiguous_status_is_success=True,
                deadline=_deadline,
            ),
        )

    async def release_source_probe(self) -> DirectoryCommandResult:
        return await self._release(
            key="source:release",
            operation="transfer_source_probe_release",
            expected_digest=self._require_source_digest(),
            mutation_issued=self._source_mutation_issued,
        )

    async def authorize_source_child(
        self,
        transfer_uuid: UUID,
        relative_path: str,
        fingerprint: str,
    ) -> DirectoryCommandResult:
        result = await self._source_command(
            key=f"source:authorize:{transfer_uuid}",
            operation="transfer_directory_authorize_source_child",
            payload={
                "transfer_uuid": str(transfer_uuid),
                "relative_path": relative_path,
                "fingerprint": fingerprint,
            },
            ambiguous_status_is_success=False,
        )
        if not isinstance(result, DirectoryCommandResult):
            raise DeviceOutcomeUnknownError("Source child authorization was not acknowledged")
        return result

    async def start_source_cleanup(
        self,
    ) -> DirectoryCommandResult | SourceDirectoryJobStatus:
        def issued() -> None:
            self._source_mutation_issued = True

        return cast(
            DirectoryCommandResult | SourceDirectoryJobStatus,
            await self._source_command(
                key="source:cleanup",
                operation="transfer_source_cleanup",
                payload={},
                ambiguous_status_is_success=True,
                evidence_states=frozenset(
                    {"source_cleanup", "succeeded", "failed", "outcome_unknown"}
                ),
                on_issued=issued,
            ),
        )

    async def start_destination_preflight(
        self,
        dst_path: str,
        manifest: DirectoryManifest,
    ) -> DestinationDirectoryStartResult:
        request_digest = _request_digest(
            {
                "role": "destination",
                "dst_path": dst_path,
                "manifest": manifest,
                "version": 1,
            }
        )
        if self._destination_path is None:
            self._destination_path = dst_path
            self._destination_request_digest = request_digest
            self._destination_digest = request_digest
        elif (
            self._destination_path != dst_path or self._destination_request_digest != request_digest
        ):
            raise TransferIntegrityError("destination preflight parameters changed")

        async def issue(mark_issued: Callable[[], None]) -> DirectoryDeviceResult:
            return await self._dispatch(
                "transfer_directory_preflight",
                {"dst_path": dst_path, "manifest": manifest},
                on_issued=mark_issued,
            )

        async def reconcile(original: BaseException) -> DirectoryDeviceResult:
            return await self._reconcile_one_status(
                lambda deadline: self.get_destination_status(_deadline=deadline),
                original=original,
                mutation_issued=False,
            )

        result = await self._one_shot(
            "destination:preflight",
            {"dst_path": dst_path, "manifest": manifest},
            issue=issue,
            reconcile=reconcile,
        )
        if not isinstance(
            result,
            (DirectoryCommandResult, DestinationDirectoryJobStatus, LocalDirectoryJobStatus),
        ):
            raise TransferIntegrityError("destination preflight returned the wrong result type")
        self._accept_destination_result(result)
        return result

    async def get_destination_status(
        self,
        *,
        outer_progress_seq: int | None = None,
        _deadline: float | None = None,
    ) -> DestinationDirectoryJobStatus:
        digest = self._require_destination_digest()
        payload: dict[str, object] = {"expected_digest": digest}
        if outer_progress_seq is not None:
            payload["outer_progress_seq"] = outer_progress_seq
        async with self._poll_locks["destination"]:
            result = await self._dispatch(
                "transfer_directory_status", payload, deadline=_deadline
            )
        if not isinstance(result, DestinationDirectoryJobStatus):
            raise TransferIntegrityError("destination status returned the wrong result type")
        self._accept_destination_status(result)
        self._observe_progress("destination", result.progress_seq)
        return result

    async def prepare_destination(
        self,
        *,
        on_issued: Callable[[], None] | None = None,
    ) -> DirectoryCommandResult | DestinationDirectoryJobStatus:
        def issued() -> None:
            self._destination_mutation_issued = True
            if on_issued is not None:
                on_issued()

        return cast(
            DirectoryCommandResult | DestinationDirectoryJobStatus,
            await self._destination_command(
                key="destination:prepare",
                operation="transfer_directory_prepare",
                payload={},
                ambiguous_status_is_success=True,
                evidence_states=frozenset(
                    {
                        "preparing",
                        "reserved",
                        "copying",
                        "finalizing",
                        "finalized_held",
                        "cleaning",
                        "failed",
                        "outcome_unknown",
                    }
                ),
                on_issued=issued,
            ),
        )

    async def authorize_destination_child(
        self,
        transfer_uuid: UUID,
        relative_path: str,
    ) -> DirectoryCommandResult:
        result = await self._destination_command(
            key=f"destination:authorize:{transfer_uuid}",
            operation="transfer_directory_authorize_child",
            payload={
                "transfer_uuid": str(transfer_uuid),
                "relative_path": relative_path,
            },
            ambiguous_status_is_success=False,
        )
        if not isinstance(result, DirectoryCommandResult):
            raise DeviceOutcomeUnknownError("Destination child authorization was not acknowledged")
        return result

    async def finish_destination(
        self,
    ) -> DirectoryCommandResult | DestinationDirectoryJobStatus:
        return cast(
            DirectoryCommandResult | DestinationDirectoryJobStatus,
            await self._destination_command(
                key="destination:finish",
                operation="transfer_directory_finish",
                payload={},
                ambiguous_status_is_success=True,
                evidence_states=frozenset(
                    {
                        "finalizing",
                        "finalized_held",
                        "cleaning",
                        "failed",
                        "outcome_unknown",
                    }
                ),
                not_applied_mutation_issued=True,
            ),
        )

    async def cancel_destination(
        self,
        *,
        _deadline: float | None = None,
    ) -> DirectoryCommandResult | DestinationDirectoryJobStatus:
        return cast(
            DirectoryCommandResult | DestinationDirectoryJobStatus,
            await self._destination_command(
                key="destination:cancel",
                operation="transfer_directory_cancel",
                payload={},
                ambiguous_status_is_success=True,
                deadline=_deadline,
            ),
        )

    async def release_destination(self) -> DirectoryCommandResult:
        return await self._release(
            key="destination:release",
            operation="transfer_directory_release",
            expected_digest=self._require_destination_digest(),
            mutation_issued=self._destination_mutation_issued,
        )

    async def start_local_directory(
        self,
        *,
        source_path: str,
        dst_path: str,
        mode: Literal["copy", "move"],
        manifest_sha256: str,
        on_issued: Callable[[], None] | None = None,
    ) -> DirectoryCommandResult | LocalDirectoryJobStatus:
        def issued() -> None:
            self._destination_mutation_issued = True
            if on_issued is not None:
                on_issued()

        async def issue(mark_issued: Callable[[], None]) -> DirectoryDeviceResult:
            return await self._dispatch(
                "transfer_local_directory_start",
                {
                    "expected_digest": self._require_destination_digest(),
                    "source_path": source_path,
                    "dst_path": dst_path,
                    "mode": mode,
                    "manifest_sha256": manifest_sha256,
                },
                on_issued=_chain_callbacks(mark_issued, issued),
            )

        async def reconcile(original: BaseException) -> DirectoryDeviceResult:
            status = await self._reconcile_one_status(
                lambda deadline: self.get_local_status(_deadline=deadline),
                original=original,
                mutation_issued=True,
            )
            if not isinstance(status, LocalDirectoryJobStatus):
                raise TransferIntegrityError("local reconciliation returned the wrong type")
            if status.state == "ready_not_started":
                _raise_reconciliation_failure(original, mutation_issued=False)
            return status

        result = await self._one_shot(
            "local:start",
            {
                "source_path": source_path,
                "dst_path": dst_path,
                "mode": mode,
                "manifest_sha256": manifest_sha256,
            },
            issue=issue,
            reconcile=reconcile,
        )
        if not isinstance(result, (DirectoryCommandResult, LocalDirectoryJobStatus)):
            raise TransferIntegrityError("local start returned the wrong result type")
        self._accept_local_result(result)
        return result

    async def get_local_status(
        self,
        *,
        _deadline: float | None = None,
    ) -> LocalDirectoryJobStatus:
        async with self._poll_locks["destination"]:
            result = await self._dispatch(
                "transfer_local_directory_status",
                {"expected_digest": self._require_destination_digest()},
                deadline=_deadline,
            )
        if not isinstance(result, LocalDirectoryJobStatus):
            raise TransferIntegrityError("local status returned the wrong result type")
        self._accept_local_status(result)
        self._observe_progress("local", result.progress_seq)
        return result

    async def cancel_local_directory(
        self,
        *,
        _deadline: float | None = None,
    ) -> DirectoryCommandResult | LocalDirectoryJobStatus:
        return cast(
            DirectoryCommandResult | LocalDirectoryJobStatus,
            await self._local_command(
                key="local:cancel",
                operation="transfer_local_directory_cancel",
                deadline=_deadline,
            ),
        )

    async def release_local_directory(self) -> DirectoryCommandResult:
        return await self._release(
            key="local:release",
            operation="transfer_local_directory_release",
            expected_digest=self._require_destination_digest(),
            mutation_issued=self._destination_mutation_issued,
        )

    async def wait_source_until(
        self,
        states: frozenset[_SourceState],
        *,
        outer_progress_seq: int | None = None,
    ) -> SourceDirectoryJobStatus:
        async with self._wait_locks["source"]:
            return cast(
                SourceDirectoryJobStatus,
                await self._wait_until(
                    role="source",
                    states=states,
                    status=lambda deadline: self.get_source_status(
                        outer_progress_seq=outer_progress_seq,
                        _deadline=deadline,
                    ),
                    cancel=lambda deadline: self.cancel_source_probe(_deadline=deadline),
                    terminal_states=_SOURCE_TERMINAL_STATES,
                    mutation_issued=lambda: self._source_mutation_issued,
                ),
            )

    async def wait_destination_until(
        self,
        states: frozenset[_DestinationState],
        *,
        outer_progress_seq: int | None = None,
    ) -> DestinationDirectoryJobStatus:
        async with self._wait_locks["destination"]:
            return cast(
                DestinationDirectoryJobStatus,
                await self._wait_until(
                    role="destination",
                    states=states,
                    status=lambda deadline: self.get_destination_status(
                        outer_progress_seq=outer_progress_seq,
                        _deadline=deadline,
                    ),
                    cancel=lambda deadline: self.cancel_destination(_deadline=deadline),
                    terminal_states=_DESTINATION_TERMINAL_STATES,
                    mutation_issued=lambda: self._destination_mutation_issued,
                ),
            )

    async def wait_local_until(
        self,
        states: frozenset[_LocalState],
    ) -> LocalDirectoryJobStatus:
        async with self._wait_locks["local"]:
            return cast(
                LocalDirectoryJobStatus,
                await self._wait_until(
                    role="local",
                    states=states,
                    status=lambda deadline: self.get_local_status(_deadline=deadline),
                    cancel=lambda deadline: self.cancel_local_directory(_deadline=deadline),
                    terminal_states=_LOCAL_TERMINAL_STATES,
                    mutation_issued=lambda: self._destination_mutation_issued,
                ),
            )

    async def _source_command(
        self,
        *,
        key: str,
        operation: str,
        payload: dict[str, object],
        ambiguous_status_is_success: bool,
        evidence_states: frozenset[str] | None = None,
        not_applied_mutation_issued: bool = False,
        on_issued: Callable[[], None] | None = None,
        deadline: float | None = None,
    ) -> DirectoryDeviceResult:
        body = {"expected_digest": self._require_source_digest(), **payload}

        async def issue(mark_issued: Callable[[], None]) -> DirectoryDeviceResult:
            return await self._dispatch(
                operation,
                body,
                on_issued=_chain_callbacks(mark_issued, on_issued),
                deadline=deadline,
            )

        async def reconcile(original: BaseException) -> DirectoryDeviceResult:
            status = await self._reconcile_one_status(
                lambda reconcile_deadline: self.get_source_status(
                    _deadline=reconcile_deadline
                ),
                original=original,
                mutation_issued=self._source_mutation_issued,
                deadline=deadline,
            )
            if not isinstance(status, SourceDirectoryJobStatus):
                raise TransferIntegrityError("source reconciliation returned the wrong type")
            if evidence_states is not None and status.state not in evidence_states:
                _raise_reconciliation_failure(
                    original,
                    mutation_issued=not_applied_mutation_issued,
                )
            if not ambiguous_status_is_success:
                raise DeviceOutcomeUnknownError(
                    f"{operation} was issued but not acknowledged"
                ) from original
            return status

        result = await self._one_shot(
            key,
            body,
            issue=issue,
            reconcile=reconcile,
        )
        if not isinstance(result, (DirectoryCommandResult, SourceDirectoryJobStatus)):
            raise TransferIntegrityError("source command returned the wrong result type")
        self._accept_source_result(result)
        return result

    async def _destination_command(
        self,
        *,
        key: str,
        operation: str,
        payload: dict[str, object],
        ambiguous_status_is_success: bool,
        evidence_states: frozenset[str] | None = None,
        not_applied_mutation_issued: bool = False,
        on_issued: Callable[[], None] | None = None,
        deadline: float | None = None,
    ) -> DirectoryDeviceResult:
        body = {"expected_digest": self._require_destination_digest(), **payload}

        async def issue(mark_issued: Callable[[], None]) -> DirectoryDeviceResult:
            return await self._dispatch(
                operation,
                body,
                on_issued=_chain_callbacks(mark_issued, on_issued),
                deadline=deadline,
            )

        async def reconcile(original: BaseException) -> DirectoryDeviceResult:
            status = await self._reconcile_one_status(
                lambda reconcile_deadline: self.get_destination_status(
                    _deadline=reconcile_deadline
                ),
                original=original,
                mutation_issued=self._destination_mutation_issued,
                deadline=deadline,
            )
            if not isinstance(status, DestinationDirectoryJobStatus):
                raise TransferIntegrityError("destination reconciliation returned the wrong type")
            if evidence_states is not None and status.state not in evidence_states:
                _raise_reconciliation_failure(
                    original,
                    mutation_issued=not_applied_mutation_issued,
                )
            if not ambiguous_status_is_success:
                raise DeviceOutcomeUnknownError(
                    f"{operation} was issued but not acknowledged"
                ) from original
            return status

        result = await self._one_shot(
            key,
            body,
            issue=issue,
            reconcile=reconcile,
        )
        if not isinstance(result, (DirectoryCommandResult, DestinationDirectoryJobStatus)):
            raise TransferIntegrityError("destination command returned the wrong result type")
        self._accept_destination_result(result)
        return result

    async def _local_command(
        self,
        *,
        key: str,
        operation: str,
        deadline: float | None = None,
    ) -> DirectoryDeviceResult:
        body: dict[str, object] = {"expected_digest": self._require_destination_digest()}

        async def issue(mark_issued: Callable[[], None]) -> DirectoryDeviceResult:
            return await self._dispatch(
                operation,
                body,
                on_issued=mark_issued,
                deadline=deadline,
            )

        async def reconcile(original: BaseException) -> DirectoryDeviceResult:
            return await self._reconcile_one_status(
                lambda reconcile_deadline: self.get_local_status(
                    _deadline=reconcile_deadline
                ),
                original=original,
                mutation_issued=self._destination_mutation_issued,
                deadline=deadline,
            )

        result = await self._one_shot(
            key,
            body,
            issue=issue,
            reconcile=reconcile,
        )
        if not isinstance(result, (DirectoryCommandResult, LocalDirectoryJobStatus)):
            raise TransferIntegrityError("local command returned the wrong result type")
        self._accept_local_result(result)
        return result

    async def _one_shot(
        self,
        key: str,
        parameters: object,
        *,
        issue: Callable[[Callable[[], None]], Awaitable[DirectoryDeviceResult]],
        reconcile: Callable[[BaseException], Awaitable[DirectoryDeviceResult]],
    ) -> DirectoryDeviceResult:
        parameters_sha256 = _request_digest(parameters)
        record = self._one_shots.get(key)
        if record is None:
            record = _OneShot(parameters_sha256)
            self._one_shots[key] = record
        elif record.parameters_sha256 != parameters_sha256:
            raise TransferIntegrityError("one-shot directory command parameters changed")
        async with record.lock:
            if record.result is not None:
                return record.result
            if record.failure is not None:
                raise record.failure
            if record.issued:
                original = DeviceOutcomeUnknownError(
                    "Directory command was issued without a known result"
                )
                try:
                    record.result = await reconcile(original)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    record.failure = exc
                    raise
                return record.result

            def mark_issued() -> None:
                record.issued = True

            try:
                result = await issue(mark_issued)
            except asyncio.CancelledError:
                raise
            except DeviceOutcomeUnknownError as exc:
                if not record.issued:
                    raise DeviceUnavailableError(
                        "Directory command failed before transport issue"
                    ) from exc
                try:
                    record.result = await reconcile(exc)
                except asyncio.CancelledError:
                    raise
                except Exception as reconcile_error:
                    record.failure = reconcile_error
                    raise
                return record.result
            except Exception as exc:
                if record.issued:
                    record.failure = exc
                raise
            record.result = result
            return result

    async def _release(
        self,
        *,
        key: str,
        operation: str,
        expected_digest: str,
        mutation_issued: bool,
    ) -> DirectoryCommandResult:
        parameters: dict[str, object] = {"expected_digest": expected_digest}
        parameters_sha256 = _request_digest(parameters)
        record = self._releases.get(key)
        if record is None:
            record = _Release(parameters_sha256)
            self._releases[key] = record
        elif record.parameters_sha256 != parameters_sha256:
            raise TransferIntegrityError("directory release parameters changed")
        async with record.lock:
            if record.result is not None:
                return record.result
            deadline = self._monotonic() + self._idle_timeout
            while True:
                if self._monotonic() >= deadline:
                    if mutation_issued:
                        raise DeviceOutcomeUnknownError(
                            "Directory release outcome is unknown"
                        )
                    raise TimeoutError("Directory release timed out")
                try:
                    result = await self._dispatch(
                        operation,
                        parameters,
                        deadline=deadline,
                    )
                except (DeviceBusyError, DeviceOutcomeUnknownError, TimeoutError) as exc:
                    if self._monotonic() >= deadline:
                        if mutation_issued:
                            raise DeviceOutcomeUnknownError(
                                "Directory release outcome is unknown"
                            ) from exc
                        raise TimeoutError("Directory release timed out") from exc
                    await self._poll_sleep(deadline)
                    continue
                if not isinstance(result, DirectoryCommandResult) or result.state != "released":
                    raise TransferIntegrityError("directory release was not acknowledged")
                if result.expected_digest != expected_digest:
                    raise TransferIntegrityError("directory release digest mismatched")
                record.result = result
                return result

    async def _dispatch(
        self,
        operation: str,
        payload: dict[str, object],
        *,
        on_issued: Callable[[], None] | None = None,
        deadline: float | None = None,
    ) -> DirectoryDeviceResult:
        try:
            action = build_directory_action(
                operation,
                directory_operation_id=str(self.directory_operation_id),
                **payload,
            )
        except (ValidationError, ValueError) as exc:
            raise TransferIntegrityError("invalid private directory action") from exc
        raw = await self._registry.dispatch_tool_on_snapshot(
            route=self.route,
            user_id=self.user_id,
            expected_device_name=self.route.device_name,
            name=INTERNAL_WORKSPACE_ACTION,
            args=action,
            max_result_bytes=(
                _PAGE_RESULT_BYTES
                if operation == "transfer_source_probe_page"
                else _CONTROL_RESULT_BYTES
            ),
            timeout_seconds=self._call_timeout_before(deadline),
            on_issued=on_issued,
        )
        if bool(getattr(raw, "is_error", False)):
            _raise_device_error(getattr(raw, "code", None))
        content = getattr(raw, "content", None)
        if not isinstance(content, (str, bytes)):
            raise TransferIntegrityError("private directory result is not JSON text")
        try:
            result = parse_directory_result(operation, content)
        except (ValidationError, ValueError) as exc:
            raise TransferIntegrityError("private directory result is malformed") from exc
        expected_command_state = _COMMAND_STATES.get(operation)
        if (
            expected_command_state is not None
            and isinstance(result, DirectoryCommandResult)
            and result.state != expected_command_state
        ):
            raise TransferIntegrityError(
                "private directory command returned an invalid acknowledgement"
            )
        return result

    async def _reconcile_one_status(
        self,
        status: Callable[[float], Awaitable[DirectoryDeviceResult]],
        *,
        original: BaseException,
        mutation_issued: bool,
        deadline: float | None = None,
    ) -> DirectoryDeviceResult:
        if deadline is None:
            deadline = self._monotonic() + self._idle_timeout
        while True:
            if self._monotonic() >= deadline:
                _raise_reconciliation_failure(
                    original,
                    mutation_issued=mutation_issued,
                )
            try:
                return await status(deadline)
            except TransferError as exc:
                if exc.code == ErrorCode.WORKSPACE_NOT_FOUND.value:
                    _raise_reconciliation_failure(
                        original,
                        mutation_issued=mutation_issued,
                    )
                raise
            except DeviceUnavailableError as exc:
                _raise_reconciliation_failure(
                    original,
                    mutation_issued=mutation_issued,
                    route_loss=exc,
                )
            except DeviceOutcomeUnknownError:
                if self._monotonic() >= deadline:
                    _raise_reconciliation_failure(
                        original,
                        mutation_issued=mutation_issued,
                    )
                await self._poll_sleep(deadline)
            except DeviceBusyError:
                if self._monotonic() >= deadline:
                    _raise_reconciliation_failure(
                        original,
                        mutation_issued=mutation_issued,
                    )
                await self._poll_sleep(deadline)
            except TimeoutError:
                _raise_reconciliation_failure(
                    original,
                    mutation_issued=mutation_issued,
                )

    async def _retry_read(self, read: Callable[[float], Awaitable[Any]]) -> Any:
        deadline = self._monotonic() + self._idle_timeout
        while True:
            if self._monotonic() >= deadline:
                raise TimeoutError("Directory read-only control call timed out")
            try:
                return await read(deadline)
            except (DeviceBusyError, DeviceOutcomeUnknownError, TimeoutError) as exc:
                if self._monotonic() >= deadline:
                    raise TimeoutError("Directory read-only control call timed out") from exc
                await self._poll_sleep(deadline)

    async def _wait_until(
        self,
        *,
        role: str,
        states: frozenset[str],
        status: Callable[[float], Awaitable[Any]],
        cancel: Callable[[float], Awaitable[Any]],
        terminal_states: frozenset[str],
        mutation_issued: Callable[[], bool],
    ) -> Any:
        if not states:
            raise ValueError("directory wait states must not be empty")
        last_progress = self._last_progress[role]
        deadline = self._monotonic() + self._idle_timeout
        while True:
            if self._monotonic() >= deadline:
                break
            try:
                snapshot = await status(deadline)
            except DeviceUnavailableError as exc:
                _raise_reconciliation_failure(
                    exc,
                    mutation_issued=mutation_issued(),
                    route_loss=exc,
                )
            except (DeviceBusyError, DeviceOutcomeUnknownError, TimeoutError):
                if self._monotonic() >= deadline:
                    break
                await self._poll_sleep(deadline)
                continue
            progress_seq = cast(int, snapshot.progress_seq)
            if progress_seq > last_progress:
                last_progress = progress_seq
                deadline = self._monotonic() + self._idle_timeout
            if snapshot.state in states:
                return snapshot
            if self._monotonic() >= deadline:
                break
            await self._poll_sleep(deadline)

        original = TimeoutError(f"Directory {role} job made no progress")
        reconcile_deadline = self._monotonic() + self._idle_timeout
        reconciled_states = terminal_states | (states & {"finalized_held"})
        try:
            cancelled = await cancel(reconcile_deadline)
            if getattr(cancelled, "state", None) in reconciled_states:
                return cancelled
        except (
            DeviceBusyError,
            DeviceOutcomeUnknownError,
            DeviceUnavailableError,
            TransferError,
            TimeoutError,
        ):
            pass
        while True:
            if self._monotonic() >= reconcile_deadline:
                _raise_reconciliation_failure(
                    original,
                    mutation_issued=mutation_issued(),
                )
            try:
                snapshot = await status(reconcile_deadline)
            except DeviceUnavailableError as exc:
                _raise_reconciliation_failure(
                    original,
                    mutation_issued=mutation_issued(),
                    route_loss=exc,
                )
            except (
                DeviceBusyError,
                DeviceOutcomeUnknownError,
                TransferError,
                TimeoutError,
            ):
                snapshot = None
            if snapshot is not None and snapshot.state in reconciled_states:
                return snapshot
            if self._monotonic() >= reconcile_deadline:
                _raise_reconciliation_failure(
                    original,
                    mutation_issued=mutation_issued(),
                )
            await self._poll_sleep(reconcile_deadline)

    async def _poll_sleep(self, deadline: float) -> None:
        remaining = deadline - self._monotonic()
        if remaining > 0:
            await self._sleep(min(self._poll_interval, remaining))

    def _call_timeout_before(self, deadline: float | None) -> float:
        if deadline is None:
            return self._call_timeout
        remaining = deadline - self._monotonic()
        if remaining <= 0:
            raise TimeoutError("Directory control-call deadline expired")
        return min(self._call_timeout, remaining)

    def _accept_source_result(
        self,
        result: DirectoryCommandResult | SourceDirectoryJobStatus,
    ) -> None:
        if isinstance(result, SourceDirectoryJobStatus):
            self._accept_source_status(result)
            self._observe_progress("source", result.progress_seq)
            return
        if result.expected_digest not in {
            self._source_request_digest,
            self._source_digest,
        }:
            raise TransferIntegrityError("source command digest mismatched")

    def _accept_source_status(self, status: SourceDirectoryJobStatus) -> None:
        if status.expected_digest not in {
            self._source_request_digest,
            self._source_digest,
        }:
            probe = status.probe
            if not (
                isinstance(probe, DirectorySourceProbe)
                and probe.manifest_sha256 == status.expected_digest
            ):
                raise TransferIntegrityError("source status digest mismatched")
        if isinstance(status.probe, DirectorySourceProbe):
            if status.probe.manifest_sha256 != status.expected_digest:
                raise TransferIntegrityError("source probe digest mismatched its status")
            self._source_digest = status.expected_digest

    def _accept_destination_result(
        self,
        result: DirectoryCommandResult | DestinationDirectoryJobStatus | LocalDirectoryJobStatus,
    ) -> None:
        if isinstance(result, DestinationDirectoryJobStatus):
            self._accept_destination_status(result)
            self._observe_progress("destination", result.progress_seq)
        elif isinstance(result, LocalDirectoryJobStatus):
            self._accept_local_status(result)
            self._observe_progress("local", result.progress_seq)
        elif result.expected_digest != self._require_destination_digest():
            raise TransferIntegrityError("destination command digest mismatched")

    def _accept_destination_status(self, status: DestinationDirectoryJobStatus) -> None:
        if status.expected_digest != self._require_destination_digest():
            raise TransferIntegrityError("destination status digest mismatched")

    def _accept_local_result(
        self,
        result: DirectoryCommandResult | LocalDirectoryJobStatus,
    ) -> None:
        if isinstance(result, LocalDirectoryJobStatus):
            self._accept_local_status(result)
            self._observe_progress("local", result.progress_seq)
        elif result.expected_digest != self._require_destination_digest():
            raise TransferIntegrityError("local command digest mismatched")

    def _accept_local_status(self, status: LocalDirectoryJobStatus) -> None:
        if status.expected_digest != self._require_destination_digest():
            raise TransferIntegrityError("local status digest mismatched")

    def _observe_progress(self, role: str, value: int) -> None:
        previous = self._last_progress[role]
        if value < previous:
            raise TransferIntegrityError("directory progress sequence regressed")
        if value > previous:
            self._last_progress[role] = value

    def _require_source_digest(self) -> str:
        if self._source_digest is None:
            raise RuntimeError("source probe has not been started")
        return self._source_digest

    def _require_destination_digest(self) -> str:
        if self._destination_digest is None:
            raise RuntimeError("destination preflight has not been started")
        return self._destination_digest


def _request_digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _chain_callbacks(
    first: Callable[[], None],
    second: Callable[[], None] | None,
) -> Callable[[], None]:
    def chained() -> None:
        first()
        if second is not None:
            second()

    return chained


def _raise_device_error(code: str | None) -> None:
    if code in {
        ErrorCode.TOOL_DEVICE_BUSY.value,
        ErrorCode.WORKSPACE_TRANSFER_BUSY.value,
    }:
        raise DeviceBusyError("Device directory control capacity is exhausted")
    if code == ErrorCode.TOOL_DEVICE_UNREACHABLE.value:
        raise DeviceUnavailableError("Device directory job is unavailable")
    if code == ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value:
        raise DeviceOutcomeUnknownError("Device directory job outcome is unknown")
    try:
        error_code = ErrorCode(code) if code is not None else None
    except ValueError as exc:
        raise TransferIntegrityError("Device returned an unknown directory error") from exc
    if error_code is None:
        raise TransferIntegrityError("Device omitted its directory error code")
    raise TransferError(error_code.value)


def _raise_reconciliation_failure(
    original: BaseException,
    *,
    mutation_issued: bool,
    route_loss: BaseException | None = None,
) -> None:
    if mutation_issued:
        raise DeviceOutcomeUnknownError("Issued directory mutation could not be reconciled") from (
            route_loss or original
        )
    cause = original.__cause__
    if isinstance(original, TimeoutError) or isinstance(cause, TimeoutError):
        raise TimeoutError("Read-only directory job timed out") from original
    raise DeviceUnavailableError("Read-only directory job became unavailable") from (
        route_loss or original
    )
