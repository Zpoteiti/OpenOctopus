from __future__ import annotations

import asyncio
import contextlib
import ctypes
import errno
import hashlib
import os
import stat
import sys
import tempfile
import threading
import time
from collections import deque
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ValidationError

from openoctopus_client.tools.common import ToolFailure
from openoctopus_client.tools.directory_contract import (
    MAX_DIRECTORY_ENTRIES,
    DirectoryContentEntry,
    DirectoryContractError,
    DirectoryManifest,
    DirectoryManifestDirectory,
    DirectoryManifestEntry,
    DirectoryManifestPage,
    canonical_json_bytes,
    create_directory_manifest,
    destination_collision_keys,
    directory_content_sha256,
    split_manifest_pages,
)
from openoctopus_client.tools.fingerprints import opaque_stat_fingerprint
from openoctopus_client.tools.locks import PathLockBusyError, PathLocks
from openoctopus_client.tools.paths import WorkspacePaths
from openoctopus_client.tools.workspace_rest import (
    DestinationDirectoryJobStatus,
    DirectoryCleanupResult,
    DirectoryCommandResult,
    DirectorySourceProbe,
    DirectoryStableError,
    DirectoryWorkspaceAction,
    FileSourceProbe,
    LocalDirectoryJobStatus,
    SourceDirectoryJobStatus,
    TransferDirectoryAuthorizeChildAction,
    TransferDirectoryAuthorizeSourceChildAction,
    TransferDirectoryCancelAction,
    TransferDirectoryFinishAction,
    TransferDirectoryPreflightAction,
    TransferDirectoryPrepareAction,
    TransferDirectoryReleaseAction,
    TransferDirectoryStatusAction,
    TransferLocalDirectoryCancelAction,
    TransferLocalDirectoryReleaseAction,
    TransferLocalDirectoryStartAction,
    TransferLocalDirectoryStatusAction,
    TransferSourceCleanupAction,
    TransferSourceProbeCancelAction,
    TransferSourceProbeHoldAction,
    TransferSourceProbePageAction,
    TransferSourceProbeReleaseAction,
    TransferSourceProbeStartAction,
    TransferSourceProbeStatusAction,
    WorkspaceTransferDirectoryResult,
    parse_directory_action,
)
from openoctopus_client.transfer import TOMBSTONE_MAX_ENTRIES, TOMBSTONE_TTL_SECONDS
from openoctopus_client.transfer_admission import LocalTransferAdmission

MAX_ACTIVE_DIRECTORY_JOBS = 2
DIRECTORY_LIFECYCLE_MAX_ENTRIES = TOMBSTONE_MAX_ENTRIES
DIRECTORY_TERMINAL_TTL_SECONDS = TOMBSTONE_TTL_SECONDS
DIRECTORY_IO_CHUNK_BYTES = 64 * 1024
_UNSUPPORTED_DIRECTORY_FSYNC_ERRNOS = frozenset(
    getattr(errno, name)
    for name in ("EINVAL", "ENOSYS", "ENOTSUP", "EOPNOTSUPP")
    if hasattr(errno, name)
)

type JobRole = Literal["source", "destination"]
type JobKey = tuple[UUID, JobRole]
type DestinationPlatform = Literal["linux", "macos", "windows"]


class _ForwardWorkCancelledError(Exception):
    pass


class _CleanupCancelledError(Exception):
    pass


class DirectoryLifecycleCredits:
    """Runtime-wide credits held from job start through its final tombstone."""

    def __init__(self, *, capacity: int = DIRECTORY_LIFECYCLE_MAX_ENTRIES) -> None:
        if capacity < 1:
            raise ValueError("directory lifecycle capacity is invalid")
        self._capacity = capacity
        self._held: set[tuple[object, JobKey]] = set()

    @property
    def active_count(self) -> int:
        return len(self._held)

    def try_acquire(self, owner: object, key: JobKey) -> bool:
        token = (owner, key)
        if token in self._held:
            return True
        if len(self._held) >= self._capacity:
            return False
        self._held.add(token)
        return True

    def release(self, owner: object, key: JobKey) -> None:
        self._held.discard((owner, key))


@dataclass(frozen=True, slots=True)
class SourceChildAuthorization:
    directory_operation_id: UUID
    transfer_uuid: UUID
    source_path: Path
    relative_path: str
    fingerprint: str


@dataclass(frozen=True, slots=True)
class DestinationChildAuthorization:
    directory_operation_id: UUID
    transfer_uuid: UUID
    destination_path: Path
    relative_path: str
    expected_size: int


@dataclass(frozen=True, slots=True)
class _Authorization:
    transfer_uuid: UUID
    relative_path: str
    fingerprint: str | None = None
    expected_size: int | None = None
    expires_at: float = 0.0


@dataclass(frozen=True, slots=True)
class _CommittedDestination:
    relative_path: str
    destination_fingerprint: str
    verified_size: int
    verified_sha256: str


@dataclass(slots=True)
class _DirectoryScan:
    manifest: DirectoryManifest
    content_entries: tuple[DirectoryContentEntry, ...] = ()


@dataclass(slots=True)
class _ScannedSource:
    probe: FileSourceProbe | DirectorySourceProbe
    manifest: DirectoryManifest | None = None
    pages: tuple[DirectoryManifestPage, ...] = ()


@dataclass(slots=True)
class _JobRecord:
    operation_id: UUID
    role: JobRole
    request_digest: str
    expected_digest: str
    state: str
    source_value: str | None = None
    destination_value: str | None = None
    source_path: Path | None = None
    destination_path: Path | None = None
    manifest: DirectoryManifest | None = None
    entries_by_path: dict[str, DirectoryManifestEntry] = field(default_factory=dict)
    pages: tuple[DirectoryManifestPage, ...] = ()
    probe: FileSourceProbe | DirectorySourceProbe | None = None
    progress_seq: int = 0
    entries_processed: int = 0
    files_processed: int = 0
    bytes_processed: int = 0
    phase: str = "waiting"
    terminal_result: WorkspaceTransferDirectoryResult | DirectoryCleanupResult | None = None
    terminal_error: DirectoryStableError | None = None
    deferred_terminal_error: DirectoryStableError | None = None
    cleanup_terminal_error: DirectoryStableError | None = None
    stop_forward_work: threading.Event = field(default_factory=threading.Event)
    stop_cleanup: threading.Event = field(default_factory=threading.Event)
    sync_lock: threading.Lock = field(default_factory=threading.Lock)
    task: asyncio.Task[None] | None = None
    next_page_offset: int = 0
    all_pages_retrieved: bool = False
    last_outer_progress_seq: int = 0
    last_progress_at: float = field(default_factory=time.monotonic)
    source_authorization: _Authorization | None = None
    destination_authorization: _Authorization | None = None
    active_destination_bytes: int = 0
    local_mode: Literal["copy", "move"] | None = None
    reservation: AbstractAsyncContextManager[None] | None = None
    reservation_held: bool = False
    root_claimed: bool = False
    created_directories: dict[Path, str] = field(default_factory=dict)
    committed: dict[str, _CommittedDestination] = field(default_factory=dict)
    completed_source_paths: set[str] = field(default_factory=set)

    def bump(
        self,
        *,
        entries: int = 0,
        files: int = 0,
        byte_count: int = 0,
        phase: str | None = None,
    ) -> None:
        with self.sync_lock:
            self.entries_processed += entries
            file_limit = (
                len(self.entries_by_path) if self.manifest is not None else MAX_DIRECTORY_ENTRIES
            )
            self.files_processed = min(file_limit, self.files_processed + files)
            self.bytes_processed += byte_count
            if phase is not None:
                self.phase = phase
            self.progress_seq += 1
            self.last_progress_at = time.monotonic()

    def set_state(self, state: str, *, phase: str | None = None) -> None:
        with self.sync_lock:
            self.state = state
            if phase is not None:
                self.phase = phase
            self.progress_seq += 1
            self.last_progress_at = time.monotonic()


@dataclass(frozen=True, slots=True)
class _TerminalRecord:
    request_digest: str
    expected_digest: str
    snapshot: SourceDirectoryJobStatus | DestinationDirectoryJobStatus | LocalDirectoryJobStatus
    expires_at: float
    local_snapshot: LocalDirectoryJobStatus | None = None


@dataclass(frozen=True, slots=True)
class _Tombstone:
    request_digest: str
    expected_digest: str
    snapshot: (
        SourceDirectoryJobStatus | DestinationDirectoryJobStatus | LocalDirectoryJobStatus | None
    )
    released: bool
    expires_at: float
    local_snapshot: LocalDirectoryJobStatus | None = None


class DirectoryJobManager:
    """Generation-owned, bounded recursive filesystem job state machine."""

    def __init__(
        self,
        workspace: Path,
        *,
        restrict_to_workspace: bool,
        path_locks: PathLocks,
        admission: LocalTransferAdmission,
        idle_timeout_seconds: float,
        queue_timeout_seconds: float,
        generation: int = 0,
        terminal_ttl_seconds: float = DIRECTORY_TERMINAL_TTL_SECONDS,
        lifecycle_capacity: int = DIRECTORY_LIFECYCLE_MAX_ENTRIES,
        lifecycle_credits: DirectoryLifecycleCredits | None = None,
    ) -> None:
        if idle_timeout_seconds <= 0 or queue_timeout_seconds < 0:
            raise ValueError("directory timeout values are invalid")
        if terminal_ttl_seconds <= 0 or lifecycle_capacity < 1:
            raise ValueError("directory lifecycle bounds are invalid")
        self._paths = WorkspacePaths(
            workspace,
            restrict_to_workspace=restrict_to_workspace,
        )
        self._locks = path_locks
        self._admission = admission
        self._idle_timeout = idle_timeout_seconds
        self._queue_timeout = queue_timeout_seconds
        self._generation = generation
        self._terminal_ttl = terminal_ttl_seconds
        self._lifecycle_capacity = lifecycle_capacity
        self._lifecycle_credits = lifecycle_credits or DirectoryLifecycleCredits(
            capacity=lifecycle_capacity
        )
        self._credit_owner = object()
        self._credited_keys: set[JobKey] = set()
        self._lifecycle_changed = asyncio.Event()
        self._active: dict[JobKey, _JobRecord] = {}
        self._retained: dict[JobKey, _TerminalRecord] = {}
        self._tombstones: dict[JobKey, _Tombstone] = {}
        self._source_authorizations: dict[UUID, tuple[_JobRecord, _Authorization]] = {}
        self._consumed_source: dict[UUID, tuple[_JobRecord, _Authorization]] = {}
        self._destination_authorizations: dict[UUID, tuple[_JobRecord, _Authorization]] = {}
        self._consumed_destination: dict[UUID, tuple[_JobRecord, _Authorization]] = {}
        self._expired_source: dict[UUID, float] = {}
        self._expired_destination: dict[UUID, float] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._cleanup_task: asyncio.Task[None] | None = None
        self._cleanup_stop = asyncio.Event()
        self._closed = False

    @property
    def active_count(self) -> int:
        return len(self._active)

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def retained_count(self) -> int:
        self._purge_expired()
        return len(self._retained)

    @property
    def lifecycle_count(self) -> int:
        self._purge_expired()
        return len(self._active) + len(self._retained) + len(self._tombstones)

    @property
    def drain_tasks(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(task for task in self._tasks if not task.done())

    def start_background_cleanup(self) -> None:
        if self._cleanup_stop.is_set() or (
            self._cleanup_task is not None and not self._cleanup_task.done()
        ):
            return
        self._cleanup_task = asyncio.create_task(self._background_cleanup())

    async def handle(self, raw_action: object) -> BaseModel:
        action = parse_directory_action(raw_action)
        self.start_background_cleanup()
        self._purge_expired()
        return await self._dispatch(action)

    def owns_operation(self, operation_id: UUID, role: JobRole) -> bool:
        self._purge_expired()
        key = (operation_id, role)
        return key in self._active or key in self._retained or key in self._tombstones

    def claims_source_transfer(self, transfer_uuid: UUID) -> bool:
        self._purge_expired()
        return (
            transfer_uuid in self._source_authorizations
            or transfer_uuid in self._consumed_source
            or transfer_uuid in self._expired_source
        )

    def claims_destination_transfer(self, transfer_uuid: UUID) -> bool:
        self._purge_expired()
        return (
            transfer_uuid in self._destination_authorizations
            or transfer_uuid in self._consumed_destination
            or transfer_uuid in self._expired_destination
        )

    async def _dispatch(self, action: DirectoryWorkspaceAction) -> BaseModel:
        if isinstance(action, TransferSourceProbeStartAction):
            return await self._source_start(action)
        if isinstance(action, TransferSourceProbeStatusAction):
            return self._source_status(action)
        if isinstance(action, TransferSourceProbePageAction):
            return self._source_page(action)
        if isinstance(action, TransferSourceProbeHoldAction):
            return self._source_hold(action)
        if isinstance(action, TransferSourceProbeCancelAction):
            return await self._source_cancel(action)
        if isinstance(action, TransferSourceProbeReleaseAction):
            return await self._release(
                action.directory_operation_id, "source", action.expected_digest
            )
        if isinstance(action, TransferDirectoryAuthorizeSourceChildAction):
            return self._authorize_source_child(action)
        if isinstance(action, TransferSourceCleanupAction):
            return await self._source_cleanup(action)
        if isinstance(action, TransferDirectoryPreflightAction):
            return await self._destination_preflight(action)
        if isinstance(action, TransferDirectoryStatusAction):
            return self._destination_status(action, local=False)
        if isinstance(action, TransferDirectoryPrepareAction):
            return await self._destination_prepare(action)
        if isinstance(action, TransferDirectoryAuthorizeChildAction):
            return self._authorize_destination_child(action)
        if isinstance(action, TransferDirectoryFinishAction):
            return await self._destination_finish(action)
        if isinstance(action, TransferDirectoryCancelAction):
            return await self._destination_cancel(
                action.directory_operation_id, action.expected_digest, local=False
            )
        if isinstance(action, TransferDirectoryReleaseAction):
            return await self._release(
                action.directory_operation_id, "destination", action.expected_digest
            )
        if isinstance(action, TransferLocalDirectoryStartAction):
            return await self._local_start(action)
        if isinstance(action, TransferLocalDirectoryStatusAction):
            return self._destination_status(action, local=True)
        if isinstance(action, TransferLocalDirectoryCancelAction):
            return await self._destination_cancel(
                action.directory_operation_id, action.expected_digest, local=True
            )
        if isinstance(action, TransferLocalDirectoryReleaseAction):
            return await self._release(
                action.directory_operation_id, "destination", action.expected_digest
            )
        raise AssertionError("unhandled directory action")

    async def _source_start(self, action: TransferSourceProbeStartAction) -> BaseModel:
        operation_id = UUID(action.directory_operation_id)
        request_digest = _request_digest({"role": "source", "path": action.path, "version": 1})
        existing = self._existing_start((operation_id, "source"), request_digest)
        if existing is not None:
            return existing
        self._ensure_forward_work_available()
        key: JobKey = (operation_id, "source")
        self._ensure_start_capacity(key)
        record = _JobRecord(
            operation_id=operation_id,
            role="source",
            request_digest=request_digest,
            expected_digest=request_digest,
            state="scanning",
            source_value=action.path,
        )
        self._active[key] = record
        self._spawn(record, self._run_source_scan(record))
        return DirectoryCommandResult(state="running", expected_digest=request_digest)

    async def _run_source_scan(self, record: _JobRecord) -> None:
        lease = None
        try:
            lease = await self._admission.acquire(timeout_seconds=self._queue_timeout)
            assert record.source_value is not None
            source = await asyncio.to_thread(
                self._paths.resolve, record.source_value, directory=None
            )
            record.source_path = source
            async with self._locks.hold(str(source), owner=record.operation_id):
                scanned = await asyncio.to_thread(
                    _scan_source_path,
                    source,
                    record.stop_forward_work,
                    record.bump,
                )
            record.probe = scanned.probe
            if scanned.manifest is None:
                await self._terminalize(record, state="succeeded")
                return
            record.manifest = scanned.manifest
            record.entries_by_path = {
                entry.relative_path: entry for entry in scanned.manifest.entries
            }
            record.pages = scanned.pages
            record.expected_digest = scanned.manifest.manifest_sha256
            record.set_state("ready_retrieval")
        except TimeoutError:
            await self._terminalize(
                record,
                state="failed",
                error=_stable_error("workspace_transfer_busy", "Transfer capacity is busy"),
            )
        except _ForwardWorkCancelledError:
            await self._terminalize(
                record,
                state="failed",
                error=_stable_error("tool_execution_cancelled", "Directory probe was cancelled"),
            )
        except ToolFailure as exc:
            await self._terminalize(record, state="failed", error=_tool_error(exc))
        except (OSError, ValidationError, DirectoryContractError):
            await self._terminalize(
                record,
                state="failed",
                error=_stable_error(
                    "workspace_storage_unavailable", "Directory probe could not be completed"
                ),
            )
        finally:
            if lease is not None:
                lease.release()

    def _source_status(self, action: TransferSourceProbeStatusAction) -> SourceDirectoryJobStatus:
        key: JobKey = (UUID(action.directory_operation_id), "source")
        record = self._active.get(key)
        if record is not None:
            self._validate_status_digest(record, action.expected_digest)
            self._observe_outer_progress(record, action.outer_progress_seq)
            return self._source_snapshot(record)
        terminal = self._terminal_snapshot(key, action.expected_digest)
        if isinstance(terminal, SourceDirectoryJobStatus):
            return terminal
        raise ToolFailure("workspace_not_found", "Directory source job was not found")

    def _source_page(self, action: TransferSourceProbePageAction) -> DirectoryManifestPage:
        record = self._require_active(
            UUID(action.directory_operation_id), "source", action.expected_digest
        )
        if record.state not in {"ready_retrieval", "held"} or record.manifest is None:
            raise ToolFailure("workspace_invalid_request", "Directory manifest is not ready")
        page_by_offset = {page.offset: page for page in record.pages}
        page = page_by_offset.get(action.offset)
        if page is None:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Directory page offset is invalid"
            )
        if action.offset > record.next_page_offset:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Directory pages must be read contiguously"
            )
        if action.offset == record.next_page_offset:
            record.next_page_offset = (
                len(record.manifest.directories) + len(record.manifest.entries)
                if page.next_offset is None
                else page.next_offset
            )
            record.all_pages_retrieved = page.next_offset is None
            record.bump()
        return page

    def _source_hold(self, action: TransferSourceProbeHoldAction) -> DirectoryCommandResult:
        record = self._require_active(
            UUID(action.directory_operation_id), "source", action.expected_digest
        )
        if record.state == "held":
            return DirectoryCommandResult(state="held", expected_digest=record.expected_digest)
        if record.state != "ready_retrieval" or not record.all_pages_retrieved:
            raise ToolFailure(
                "workspace_invalid_request", "Complete manifest retrieval is required before hold"
            )
        record.set_state("held")
        return DirectoryCommandResult(state="held", expected_digest=record.expected_digest)

    async def _source_cancel(
        self, action: TransferSourceProbeCancelAction
    ) -> DirectoryCommandResult:
        operation_id = UUID(action.directory_operation_id)
        key: JobKey = (operation_id, "source")
        record = self._active.get(key)
        if record is None:
            terminal = self._terminal_snapshot(key, action.expected_digest)
            if terminal is None:
                raise ToolFailure("workspace_not_found", "Directory source job was not found")
            return DirectoryCommandResult(
                state="accepted", expected_digest=terminal.expected_digest
            )
        self._validate_exact_digest(record, action.expected_digest)
        record.stop_forward_work.set()
        cancelled_error = _stable_error("tool_execution_cancelled", "Directory probe was cancelled")
        if self._source_child_consumed(record):
            self._remember_deferred_error(record, cancelled_error)
            return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)
        if record.task is None or record.task.done():
            await self._terminalize(
                record,
                state="failed",
                error=cancelled_error,
            )
        return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)

    def _authorize_source_child(
        self, action: TransferDirectoryAuthorizeSourceChildAction
    ) -> DirectoryCommandResult:
        record = self._require_active(
            UUID(action.directory_operation_id), "source", action.expected_digest
        )
        if record.state != "held" or record.manifest is None:
            raise ToolFailure("workspace_invalid_request", "Source manifest is not held")
        expected = record.entries_by_path.get(action.relative_path)
        if expected is None or expected.fingerprint != action.fingerprint:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Source child authorization is invalid"
            )
        authorization = _Authorization(
            transfer_uuid=UUID(action.transfer_uuid),
            relative_path=action.relative_path,
            fingerprint=action.fingerprint,
            expires_at=time.monotonic() + self._idle_timeout,
        )
        if record.source_authorization is not None:
            if _same_authorization(record.source_authorization, authorization):
                return DirectoryCommandResult(
                    state="accepted", expected_digest=record.expected_digest
                )
            raise ToolFailure("workspace_transfer_busy", "Another source child is authorized")
        if (
            authorization.transfer_uuid in self._source_authorizations
            or authorization.transfer_uuid in self._consumed_source
            or authorization.transfer_uuid in self._expired_source
        ):
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Transfer UUID is already authorized"
            )
        self._ensure_forward_work_available()
        record.source_authorization = authorization
        self._source_authorizations[authorization.transfer_uuid] = (record, authorization)
        record.last_progress_at = time.monotonic()
        return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)

    async def consume_source_authorization(
        self, transfer_uuid: UUID, source_path: Path
    ) -> SourceChildAuthorization:
        if transfer_uuid in self._expired_source:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Source child authorization expired"
            )
        found = self._source_authorizations.pop(transfer_uuid, None)
        if found is None:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Source child is not authorized"
            )
        record, authorization = found
        if authorization.expires_at <= time.monotonic():
            record.source_authorization = None
            self._remember_expired_authorization(transfer_uuid, source=True)
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Source child authorization expired"
            )
        assert record.source_path is not None
        expected_path = record.source_path.joinpath(*authorization.relative_path.split("/"))
        if source_path != expected_path:
            record.source_authorization = None
            self._remember_expired_authorization(transfer_uuid, source=True)
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Authorized source path does not match"
            )
        self._consumed_source[transfer_uuid] = (record, authorization)
        assert authorization.fingerprint is not None
        return SourceChildAuthorization(
            directory_operation_id=record.operation_id,
            transfer_uuid=transfer_uuid,
            source_path=source_path,
            relative_path=authorization.relative_path,
            fingerprint=authorization.fingerprint,
        )

    async def report_source_child_progress(
        self, transfer_uuid: UUID, *, byte_count: int = 0
    ) -> None:
        found = self._consumed_source.get(transfer_uuid)
        if found is None or byte_count < 0:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Source child is not in flight"
            )
        record, _ = found
        record.bump(byte_count=byte_count)

    async def complete_source_authorization(self, transfer_uuid: UUID, *, success: bool) -> None:
        found = self._consumed_source.pop(transfer_uuid, None)
        if found is None:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Source child is not in flight"
            )
        record, authorization = found
        if record.source_authorization == authorization:
            record.source_authorization = None
        self._remember_expired_authorization(transfer_uuid, source=True)
        if success:
            if authorization.relative_path not in record.completed_source_paths:
                record.completed_source_paths.add(authorization.relative_path)
                record.bump(files=1)
            else:
                record.bump()
        else:
            record.bump()
        deferred_error = record.deferred_terminal_error
        if (deferred_error is not None or self._closed) and self._active.get(
            (record.operation_id, "source")
        ) is record:
            await self._terminalize(
                record,
                state="failed",
                error=deferred_error
                or _stable_error(
                    "tool_execution_cancelled", "Directory source manager was retired"
                ),
            )

    async def _source_cleanup(self, action: TransferSourceCleanupAction) -> DirectoryCommandResult:
        record = self._require_active(
            UUID(action.directory_operation_id), "source", action.expected_digest
        )
        if record.state == "source_cleanup":
            return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)
        if record.state != "held" or record.manifest is None or record.source_path is None:
            raise ToolFailure("workspace_invalid_request", "Held source manifest is required")
        if record.source_authorization is not None:
            raise ToolFailure("workspace_transfer_busy", "A source child is still authorized")
        record.set_state("source_cleanup", phase="cleanup")
        self._spawn(record, self._run_source_cleanup(record))
        return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)

    async def _run_source_cleanup(self, record: _JobRecord) -> None:
        lease = None
        try:
            lease = await self._admission.acquire(timeout_seconds=self._queue_timeout)
            assert record.source_path is not None and record.manifest is not None
            async with self._locks.reserve_subtree(record.operation_id, str(record.source_path)):
                result = await asyncio.to_thread(
                    _conditional_source_cleanup,
                    record.source_path,
                    record.manifest,
                    record.stop_cleanup,
                    record.bump,
                )
            record.terminal_result = result
            await self._terminalize(record, state="succeeded")
        except TimeoutError:
            await self._terminalize(
                record,
                state="outcome_unknown",
                error=_stable_error(
                    "tool_execution_outcome_unknown", "Source cleanup could not acquire capacity"
                ),
            )
        except (ToolFailure, OSError, _CleanupCancelledError) as exc:
            error = (
                _tool_error(exc)
                if isinstance(exc, ToolFailure)
                else _stable_error(
                    "tool_execution_outcome_unknown", "Source cleanup outcome is unknown"
                )
            )
            await self._terminalize(record, state="outcome_unknown", error=error)
        finally:
            if lease is not None:
                lease.release()

    async def _destination_preflight(self, action: TransferDirectoryPreflightAction) -> BaseModel:
        operation_id = UUID(action.directory_operation_id)
        request_digest = _request_digest(
            {
                "role": "destination",
                "dst_path": action.dst_path,
                "manifest": action.manifest,
                "version": 1,
            }
        )
        existing = self._existing_start((operation_id, "destination"), request_digest)
        if existing is not None:
            return existing
        self._ensure_forward_work_available()
        key: JobKey = (operation_id, "destination")
        self._ensure_start_capacity(key)
        record = _JobRecord(
            operation_id=operation_id,
            role="destination",
            request_digest=request_digest,
            expected_digest=request_digest,
            state="preflighting",
            destination_value=action.dst_path,
            manifest=action.manifest,
            entries_by_path={entry.relative_path: entry for entry in action.manifest.entries},
        )
        self._active[key] = record
        self._spawn(record, self._run_destination_preflight(record))
        return DirectoryCommandResult(state="running", expected_digest=request_digest)

    async def _run_destination_preflight(self, record: _JobRecord) -> None:
        lease = None
        try:
            lease = await self._admission.acquire(timeout_seconds=self._queue_timeout)
            _check_forward(record.stop_forward_work)
            assert record.destination_value is not None and record.manifest is not None
            destination = await asyncio.to_thread(
                self._paths.resolve, record.destination_value, directory=None
            )
            await asyncio.to_thread(
                _preflight_destination,
                destination,
                record.manifest,
                _current_destination_platform(),
                record.stop_forward_work,
                record.bump,
            )
            record.destination_path = destination
            record.set_state("ready")
        except TimeoutError:
            await self._terminalize(
                record,
                state="failed",
                error=_stable_error("workspace_transfer_busy", "Transfer capacity is busy"),
            )
        except _ForwardWorkCancelledError:
            await self._terminalize(
                record,
                state="failed",
                error=_stable_error("tool_execution_cancelled", "Destination preflight cancelled"),
            )
        except (ToolFailure, PathLockBusyError) as exc:
            error = (
                _tool_error(exc)
                if isinstance(exc, ToolFailure)
                else _stable_error("workspace_transfer_busy", "Destination subtree is busy")
            )
            await self._terminalize(record, state="failed", error=error)
        except (OSError, DirectoryContractError):
            await self._terminalize(
                record,
                state="failed",
                error=_stable_error(
                    "workspace_storage_unavailable", "Destination preflight failed"
                ),
            )
        finally:
            if lease is not None:
                lease.release()

    def _destination_status(
        self,
        action: TransferDirectoryStatusAction | TransferLocalDirectoryStatusAction,
        *,
        local: bool,
    ) -> DestinationDirectoryJobStatus | LocalDirectoryJobStatus:
        key: JobKey = (UUID(action.directory_operation_id), "destination")
        record = self._active.get(key)
        if record is not None:
            self._validate_status_digest(record, action.expected_digest)
            if isinstance(action, TransferDirectoryStatusAction):
                self._observe_outer_progress(record, action.outer_progress_seq)
            return self._local_snapshot(record) if local else self._destination_snapshot(record)
        terminal = self._terminal_snapshot(key, action.expected_digest, local=local)
        if local and isinstance(terminal, LocalDirectoryJobStatus):
            return terminal
        if not local and isinstance(terminal, DestinationDirectoryJobStatus):
            return terminal
        raise ToolFailure("workspace_not_found", "Directory destination job was not found")

    async def _destination_prepare(
        self, action: TransferDirectoryPrepareAction
    ) -> DirectoryCommandResult:
        record = self._require_active(
            UUID(action.directory_operation_id), "destination", action.expected_digest
        )
        if record.state in {"preparing", "reserved", "copying"}:
            return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)
        if record.state != "ready" or record.destination_path is None:
            raise ToolFailure("workspace_invalid_request", "Destination is not ready")
        self._ensure_forward_work_available()
        record.set_state("preparing", phase="preparing")
        self._spawn(record, self._run_destination_prepare(record))
        return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)

    async def _run_destination_prepare(self, record: _JobRecord) -> None:
        lease = None
        try:
            lease = await self._admission.acquire(timeout_seconds=self._queue_timeout)
            _check_forward(record.stop_forward_work)
            assert record.destination_path is not None and record.manifest is not None
            await self._enter_reservation(record, record.destination_path)
            await asyncio.to_thread(
                _claim_destination_tree,
                record,
                record.destination_path,
                record.manifest,
            )
            if record.stop_forward_work.is_set():
                self._remember_cleanup_error(
                    record,
                    _stable_error(
                        "tool_execution_cancelled", "Directory destination job was cancelled"
                    ),
                )
                await self._run_destination_cleanup(record, lease_already_held=True)
                return
            record.set_state("reserved")
        except TimeoutError:
            await self._terminalize(
                record,
                state="failed",
                error=_stable_error("workspace_transfer_busy", "Transfer capacity is busy"),
            )
        except (_ForwardWorkCancelledError, ToolFailure, PathLockBusyError, OSError) as exc:
            error = (
                _tool_error(exc)
                if isinstance(exc, ToolFailure)
                else _stable_error("workspace_transfer_busy", "Destination could not be reserved")
                if isinstance(exc, PathLockBusyError)
                else _stable_error(
                    "tool_execution_cancelled", "Destination preparation was cancelled"
                )
                if isinstance(exc, _ForwardWorkCancelledError)
                else _stable_error(
                    "workspace_storage_unavailable", "Destination could not be reserved"
                )
            )
            if isinstance(exc, _ForwardWorkCancelledError) and record.cleanup_terminal_error:
                error = record.cleanup_terminal_error
            if record.root_claimed or record.created_directories:
                self._remember_cleanup_error(record, error)
                await self._run_destination_cleanup(record, lease_already_held=True)
                return
            await self._exit_reservation(record)
            await self._terminalize(record, state="failed", error=error)
        finally:
            if lease is not None:
                lease.release()

    def _authorize_destination_child(
        self, action: TransferDirectoryAuthorizeChildAction
    ) -> DirectoryCommandResult:
        record = self._require_active(
            UUID(action.directory_operation_id), "destination", action.expected_digest
        )
        if record.state not in {"reserved", "copying"} or record.manifest is None:
            raise ToolFailure("workspace_invalid_request", "Destination is not reserved")
        expected = record.entries_by_path.get(action.relative_path)
        if expected is None or action.relative_path in record.committed:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination child is not expected"
            )
        authorization = _Authorization(
            transfer_uuid=UUID(action.transfer_uuid),
            relative_path=action.relative_path,
            expected_size=expected.size,
            expires_at=time.monotonic() + self._idle_timeout,
        )
        if record.destination_authorization is not None:
            if _same_authorization(record.destination_authorization, authorization):
                return DirectoryCommandResult(
                    state="accepted", expected_digest=record.expected_digest
                )
            raise ToolFailure("workspace_transfer_busy", "Another destination child is authorized")
        if (
            authorization.transfer_uuid in self._destination_authorizations
            or authorization.transfer_uuid in self._consumed_destination
            or authorization.transfer_uuid in self._expired_destination
        ):
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Transfer UUID is already authorized"
            )
        self._ensure_forward_work_available()
        record.destination_authorization = authorization
        self._destination_authorizations[authorization.transfer_uuid] = (record, authorization)
        record.set_state("copying")
        return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)

    async def consume_destination_authorization(
        self, transfer_uuid: UUID
    ) -> DestinationChildAuthorization:
        if transfer_uuid in self._expired_destination:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination child authorization expired"
            )
        found = self._destination_authorizations.pop(transfer_uuid, None)
        if found is None:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination child is not authorized"
            )
        record, authorization = found
        if authorization.expires_at <= time.monotonic():
            record.destination_authorization = None
            self._remember_expired_authorization(transfer_uuid, source=False)
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination child authorization expired"
            )
        assert record.destination_path is not None
        assert authorization.expected_size is not None
        destination_path = record.destination_path.joinpath(*authorization.relative_path.split("/"))
        self._consumed_destination[transfer_uuid] = (record, authorization)
        return DestinationChildAuthorization(
            directory_operation_id=record.operation_id,
            transfer_uuid=transfer_uuid,
            destination_path=destination_path,
            relative_path=authorization.relative_path,
            expected_size=authorization.expected_size,
        )

    def validate_destination_child_parent(self, transfer_uuid: UUID) -> None:
        found = self._consumed_destination.get(transfer_uuid)
        if found is None:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination child is not in flight"
            )
        record, authorization = found
        if record.destination_path is None:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination child path is unavailable"
            )
        destination = record.destination_path.joinpath(*authorization.relative_path.split("/"))
        parents = (
            record.destination_path,
            *_derived_parents(record.destination_path, destination),
        )
        for parent in parents:
            expected_identity = record.created_directories.get(parent)
            if not expected_identity:
                raise ToolFailure("workspace_file_changed", "Destination parent changed")
            try:
                current_identity = _directory_identity(parent)
            except (OSError, ToolFailure) as exc:
                raise ToolFailure("workspace_file_changed", "Destination parent changed") from exc
            if current_identity != expected_identity:
                raise ToolFailure("workspace_file_changed", "Destination parent changed")

    async def report_destination_child_progress(
        self, transfer_uuid: UUID, *, byte_count: int = 0
    ) -> None:
        found = self._consumed_destination.get(transfer_uuid)
        if found is None or byte_count < 0:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination child is not in flight"
            )
        record, _ = found
        record.active_destination_bytes += byte_count
        record.bump(byte_count=byte_count)

    async def complete_destination_authorization(
        self, transfer_uuid: UUID, *, success: bool
    ) -> None:
        if success:
            raise ValueError("Successful destination children require commit metadata")
        found = self._consumed_destination.pop(transfer_uuid, None)
        if found is None:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination child is not in flight"
            )
        record, authorization = found
        if record.destination_authorization == authorization:
            record.destination_authorization = None
        self._remember_expired_authorization(transfer_uuid, source=False)
        record.active_destination_bytes = 0
        record.bump()
        await self._resume_destination_after_child(record)

    async def record_destination_commit(
        self,
        directory_operation_id: UUID,
        transfer_uuid: UUID,
        *,
        relative_path: str,
        destination_fingerprint: str,
        verified_size: int,
        verified_sha256: str,
    ) -> None:
        found = self._consumed_destination.pop(transfer_uuid, None)
        if found is None:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination commit is not authorized"
            )
        record, authorization = found
        try:
            if (
                record.operation_id != directory_operation_id
                or authorization.relative_path != relative_path
            ):
                raise ToolFailure(
                    "workspace_transfer_integrity_failed",
                    "Destination commit does not match authorization",
                )
            assert record.manifest is not None and record.destination_path is not None
            expected = record.entries_by_path.get(relative_path)
            if expected is None:
                raise ToolFailure(
                    "workspace_transfer_integrity_failed",
                    "Destination commit does not match manifest",
                )
            if verified_size != expected.size or not _is_sha256(verified_sha256):
                raise ToolFailure(
                    "workspace_transfer_integrity_failed", "Destination commit metadata is invalid"
                )
            if record.active_destination_bytes > verified_size:
                raise ToolFailure(
                    "workspace_transfer_integrity_failed",
                    "Destination progress exceeds commit size",
                )
        except BaseException:
            if record.destination_authorization == authorization:
                record.destination_authorization = None
            record.active_destination_bytes = 0
            self._remember_expired_authorization(transfer_uuid, source=False)
            raise
        record.destination_authorization = None
        record.committed[relative_path] = _CommittedDestination(
            relative_path=relative_path,
            destination_fingerprint=destination_fingerprint,
            verified_size=verified_size,
            verified_sha256=verified_sha256,
        )
        remaining_bytes = verified_size - record.active_destination_bytes
        record.active_destination_bytes = 0
        self._remember_expired_authorization(transfer_uuid, source=False)
        record.bump(files=1, byte_count=remaining_bytes, phase="copying")
        await self._resume_destination_after_child(record)

    async def _resume_destination_after_child(self, record: _JobRecord) -> None:
        if self._active.get((record.operation_id, "destination")) is not record:
            return
        error = record.cleanup_terminal_error
        if error is None and not self._closed:
            return
        if record.root_claimed or record.created_directories or record.committed:
            if record.task is None or record.task.done():
                self._spawn(record, self._run_destination_cleanup(record))
            return
        await self._terminalize(
            record,
            state="failed",
            error=error or _stable_error("tool_execution_cancelled", "Directory manager closed"),
        )

    async def _destination_finish(
        self, action: TransferDirectoryFinishAction
    ) -> DirectoryCommandResult:
        record = self._require_active(
            UUID(action.directory_operation_id), "destination", action.expected_digest
        )
        if record.state in {"finalizing", "finalized_held"}:
            return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)
        self._ensure_forward_work_available()
        if record.state not in {"reserved", "copying"} or record.manifest is None:
            raise ToolFailure("workspace_invalid_request", "Destination is not ready to finalize")
        if record.destination_authorization is not None:
            raise ToolFailure("workspace_transfer_busy", "A destination child is still active")
        if len(record.committed) != len(record.manifest.entries):
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Destination commits are incomplete"
            )
        record.set_state("finalizing", phase="revalidating")
        self._spawn(record, self._run_destination_finish(record))
        return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)

    async def _run_destination_finish(self, record: _JobRecord) -> None:
        lease = None
        try:
            lease = await self._admission.acquire(timeout_seconds=self._queue_timeout)
            _check_forward(record.stop_forward_work)
            assert record.destination_path is not None and record.manifest is not None
            await asyncio.to_thread(
                _verify_destination_tree,
                record.destination_path,
                record.manifest,
                record.committed,
                record.stop_forward_work,
                record.bump,
            )
            record.terminal_result = _aggregate_result(record.committed.values())
            record.set_state("finalized_held")
        except TimeoutError:
            self._remember_cleanup_error(
                record,
                _stable_error(
                    "workspace_transfer_timeout", "Destination finalize capacity timed out"
                ),
            )
            await self._run_destination_cleanup(record)
        except (_ForwardWorkCancelledError, ToolFailure, OSError) as exc:
            if isinstance(exc, ToolFailure):
                self._remember_cleanup_error(record, _tool_error(exc))
            elif isinstance(exc, OSError):
                self._remember_cleanup_error(
                    record,
                    _stable_error(
                        "workspace_storage_unavailable", "Destination verification failed"
                    ),
                )
            else:
                self._remember_cleanup_error(
                    record,
                    _stable_error(
                        "tool_execution_cancelled", "Directory destination job was cancelled"
                    ),
                )
            record.stop_forward_work.set()
            await self._run_destination_cleanup(record, lease_already_held=True)
        finally:
            if lease is not None:
                lease.release()

    async def _destination_cancel(
        self, operation_id_value: str, expected_digest: str, *, local: bool
    ) -> DirectoryCommandResult:
        operation_id = UUID(operation_id_value)
        key: JobKey = (operation_id, "destination")
        record = self._active.get(key)
        if record is None:
            terminal = self._terminal_snapshot(key, expected_digest)
            if terminal is None:
                raise ToolFailure("workspace_not_found", "Directory destination job was not found")
            return DirectoryCommandResult(
                state="accepted", expected_digest=terminal.expected_digest
            )
        self._validate_exact_digest(record, expected_digest)
        if record.state == "finalized_held":
            return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)
        record.stop_forward_work.set()
        cancelled_error = _stable_error(
            "tool_execution_cancelled", "Directory destination job was cancelled"
        )
        self._remember_cleanup_error(record, cancelled_error)
        if self._destination_child_consumed(record):
            return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)
        if local and record.local_mode is None and record.state == "ready":
            await self._terminalize(
                record,
                state="failed",
                error=cancelled_error,
                local=True,
            )
            return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)
        if local:
            record.set_state("cancelling", phase="cleanup")
        elif record.state not in {"preflighting", "preparing", "finalizing", "cleaning"}:
            if record.root_claimed or record.committed:
                record.set_state("cleaning", phase="cleanup")
            else:
                await self._terminalize(
                    record,
                    state="failed",
                    error=cancelled_error,
                )
                return DirectoryCommandResult(
                    state="accepted", expected_digest=record.expected_digest
                )
        if (record.task is None or record.task.done()) and record.root_claimed:
            self._spawn(record, self._run_destination_cleanup(record))
        return DirectoryCommandResult(state="accepted", expected_digest=record.expected_digest)

    async def _run_destination_cleanup(
        self, record: _JobRecord, *, lease_already_held: bool = False
    ) -> None:
        record.set_state("cleaning", phase="cleanup")
        lease = None
        try:
            if not lease_already_held:
                lease = await self._admission.acquire(timeout_seconds=self._queue_timeout)
            complete = await asyncio.to_thread(
                _cleanup_destination,
                record,
                record.stop_cleanup,
            )
            await self._exit_reservation(record)
            error = record.cleanup_terminal_error or _stable_error(
                "tool_execution_cancelled", "Directory destination job was cancelled"
            )
            await self._terminalize(
                record,
                state="failed" if complete else "outcome_unknown",
                error=error
                if complete
                else _stable_error(
                    "tool_execution_outcome_unknown", "Destination cleanup is incomplete"
                ),
                cleanup_complete=complete,
            )
        except (TimeoutError, OSError, _CleanupCancelledError):
            await self._exit_reservation(record)
            await self._terminalize(
                record,
                state="outcome_unknown",
                error=_stable_error(
                    "tool_execution_outcome_unknown", "Destination cleanup outcome is unknown"
                ),
                cleanup_complete=False,
            )
        finally:
            if lease is not None:
                lease.release()

    async def _local_start(
        self, action: TransferLocalDirectoryStartAction
    ) -> DirectoryCommandResult:
        record = self._require_active(
            UUID(action.directory_operation_id), "destination", action.expected_digest
        )
        if record.local_mode is not None:
            if (
                record.local_mode == action.mode
                and record.source_value == action.source_path
                and record.destination_value == action.dst_path
                and record.manifest is not None
                and record.manifest.manifest_sha256 == action.manifest_sha256
            ):
                return DirectoryCommandResult(
                    state="running", expected_digest=record.expected_digest
                )
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Local directory start parameters changed"
            )
        self._ensure_forward_work_available()
        if (
            record.state != "ready"
            or record.manifest is None
            or record.manifest.manifest_sha256 != action.manifest_sha256
            or record.destination_value != action.dst_path
        ):
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Local destination preflight does not match"
            )
        record.local_mode = action.mode
        record.source_value = action.source_path
        record.set_state("running", phase="waiting")
        self._spawn(record, self._run_local_job(record))
        return DirectoryCommandResult(state="running", expected_digest=record.expected_digest)

    async def _run_local_job(self, record: _JobRecord) -> None:
        lease = None
        try:
            lease = await self._admission.acquire(timeout_seconds=self._queue_timeout)
            _check_forward(record.stop_forward_work)
            assert record.source_value is not None
            source = await asyncio.to_thread(
                self._paths.resolve, record.source_value, directory=True
            )
            assert record.destination_path is not None and record.manifest is not None
            _reject_overlap(source, record.destination_path, _current_destination_platform())
            record.source_path = source
            reservation = self._locks.reserve_subtree(
                record.operation_id, str(source), str(record.destination_path)
            )
            record.reservation = reservation
            await reservation.__aenter__()
            record.reservation_held = True
            result = await asyncio.to_thread(
                _execute_local_job,
                record,
                source,
                record.destination_path,
                record.manifest,
            )
            record.terminal_result = result
            await self._exit_reservation(record)
            await self._terminalize(record, state="succeeded", local=True)
        except TimeoutError:
            await self._terminalize(
                record,
                state="failed",
                error=_stable_error("workspace_transfer_busy", "Transfer capacity is busy"),
                local=True,
            )
        except _ForwardWorkCancelledError:
            complete = await self._cleanup_local_after_error(record)
            await self._terminalize(
                record,
                state="failed" if complete else "outcome_unknown",
                error=_stable_error(
                    "tool_execution_cancelled", "Local directory transfer was cancelled"
                )
                if complete
                else _stable_error(
                    "tool_execution_outcome_unknown", "Local cleanup outcome is unknown"
                ),
                local=True,
            )
        except (ToolFailure, PathLockBusyError, OSError, DirectoryContractError) as exc:
            complete = await self._cleanup_local_after_error(record)
            error = (
                _tool_error(exc)
                if isinstance(exc, ToolFailure) and complete
                else _stable_error("workspace_transfer_busy", "Workspace subtree is busy")
                if isinstance(exc, PathLockBusyError) and complete
                else _stable_error(
                    "tool_execution_outcome_unknown", "Local transfer outcome is unknown"
                )
                if not complete
                else _stable_error(
                    "workspace_storage_unavailable", "Local directory transfer failed"
                )
            )
            await self._terminalize(
                record,
                state="failed" if complete else "outcome_unknown",
                error=error,
                local=True,
            )
        finally:
            if lease is not None:
                lease.release()

    async def _cleanup_local_after_error(self, record: _JobRecord) -> bool:
        complete = True
        if record.root_claimed or record.created_directories or record.committed:
            record.set_state("cancelling", phase="cleanup")
            try:
                complete = await asyncio.to_thread(
                    _cleanup_destination, record, record.stop_cleanup
                )
            except (OSError, _CleanupCancelledError):
                complete = False
        await self._exit_reservation(record)
        return complete

    async def _enter_reservation(self, record: _JobRecord, *paths: Path) -> None:
        if record.reservation_held:
            return
        reservation = self._locks.reserve_subtree(
            record.operation_id, *(str(path) for path in paths)
        )
        record.reservation = reservation
        await reservation.__aenter__()
        record.reservation_held = True

    async def _exit_reservation(self, record: _JobRecord) -> None:
        if not record.reservation_held or record.reservation is None:
            return
        record.reservation_held = False
        reservation = record.reservation
        record.reservation = None
        await reservation.__aexit__(None, None, None)

    async def _release(
        self, operation_id_value: str, role: JobRole, expected_digest: str
    ) -> DirectoryCommandResult:
        operation_id = UUID(operation_id_value)
        key = (operation_id, role)
        record = self._active.get(key)
        if record is not None:
            self._validate_exact_digest(record, expected_digest)
            if (
                record.source_authorization is not None
                or record.destination_authorization is not None
            ):
                raise ToolFailure("workspace_transfer_busy", "A directory child is still active")
            allowed = (
                {"ready_retrieval", "held"} if role == "source" else {"ready", "finalized_held"}
            )
            if record.state not in allowed:
                raise ToolFailure("workspace_transfer_busy", "Directory job is still active")
            if (
                role == "source"
                and record.state == "ready_retrieval"
                and not record.all_pages_retrieved
            ):
                raise ToolFailure(
                    "workspace_invalid_request", "Complete manifest retrieval is required"
                )
            await self._exit_reservation(record)
            self._remove_active_record(record)
            self._tombstones[key] = _Tombstone(
                request_digest=record.request_digest,
                expected_digest=record.expected_digest,
                snapshot=None,
                released=True,
                expires_at=time.monotonic() + TOMBSTONE_TTL_SECONDS,
            )
            return DirectoryCommandResult(state="released", expected_digest=record.expected_digest)
        terminal = self._retained.pop(key, None)
        if terminal is not None:
            self._validate_terminal_digest(terminal, expected_digest)
            self._tombstones[key] = _Tombstone(
                request_digest=terminal.request_digest,
                expected_digest=terminal.expected_digest,
                snapshot=None,
                released=True,
                expires_at=time.monotonic() + TOMBSTONE_TTL_SECONDS,
            )
            return DirectoryCommandResult(
                state="released", expected_digest=terminal.expected_digest
            )
        tombstone = self._tombstones.get(key)
        if tombstone is None:
            raise ToolFailure("workspace_not_found", "Directory job was not found")
        self._validate_tombstone_digest(tombstone, expected_digest)
        if not tombstone.released:
            self._tombstones[key] = _Tombstone(
                request_digest=tombstone.request_digest,
                expected_digest=tombstone.expected_digest,
                snapshot=None,
                released=True,
                expires_at=time.monotonic() + TOMBSTONE_TTL_SECONDS,
            )
        return DirectoryCommandResult(state="released", expected_digest=tombstone.expected_digest)

    def _existing_start(self, key: JobKey, request_digest: str) -> BaseModel | None:
        record = self._active.get(key)
        if record is not None:
            if record.request_digest != request_digest:
                self._integrity_error()
            return (
                self._source_snapshot(record)
                if record.role == "source"
                else self._local_snapshot(record)
                if record.local_mode is not None
                else self._destination_snapshot(record)
            )
        terminal = self._retained.get(key)
        if terminal is not None:
            if terminal.request_digest != request_digest:
                self._integrity_error()
            return terminal.snapshot
        tombstone = self._tombstones.get(key)
        if tombstone is not None:
            if tombstone.request_digest != request_digest:
                self._integrity_error()
            if tombstone.snapshot is not None:
                return tombstone.snapshot
            raise ToolFailure("workspace_invalid_request", "Directory job was already released")
        return None

    def _ensure_start_capacity(self, key: JobKey) -> None:
        if len(self._active) >= MAX_ACTIVE_DIRECTORY_JOBS:
            raise ToolFailure("workspace_transfer_busy", "Directory job capacity is busy")
        self._purge_expired()
        if not self._lifecycle_credits.try_acquire(self._credit_owner, key):
            raise ToolFailure("workspace_transfer_busy", "Directory lifecycle capacity is full")
        self._credited_keys.add(key)
        self._lifecycle_changed.set()

    def _ensure_forward_work_available(self) -> None:
        if self._closed:
            raise ToolFailure("tool_device_unreachable", "Directory manager is retired")

    def _release_lifecycle_credit(self, key: JobKey) -> None:
        if key not in self._credited_keys:
            return
        self._credited_keys.remove(key)
        self._lifecycle_credits.release(self._credit_owner, key)
        self._lifecycle_changed.set()

    def _require_active(
        self, operation_id: UUID, role: JobRole, expected_digest: str
    ) -> _JobRecord:
        record = self._active.get((operation_id, role))
        if record is None:
            if self._closed:
                key = (operation_id, role)
                lifecycle = self._retained.get(key) or self._tombstones.get(key)
                if lifecycle is not None:
                    if expected_digest != lifecycle.expected_digest:
                        self._integrity_error()
                    raise ToolFailure("tool_device_unreachable", "Directory manager is retired")
            raise ToolFailure("workspace_not_found", "Directory job was not found")
        self._validate_exact_digest(record, expected_digest)
        return record

    def _validate_status_digest(self, record: _JobRecord, expected_digest: str) -> None:
        if expected_digest not in {record.request_digest, record.expected_digest}:
            self._integrity_error()

    def _validate_exact_digest(self, record: _JobRecord, expected_digest: str) -> None:
        if expected_digest != record.expected_digest:
            self._integrity_error()

    def _validate_terminal_digest(self, terminal: _TerminalRecord, expected_digest: str) -> None:
        if expected_digest not in {terminal.request_digest, terminal.expected_digest}:
            self._integrity_error()

    def _validate_tombstone_digest(self, tombstone: _Tombstone, expected_digest: str) -> None:
        if expected_digest not in {tombstone.request_digest, tombstone.expected_digest}:
            self._integrity_error()

    @staticmethod
    def _integrity_error() -> None:
        raise ToolFailure(
            "workspace_transfer_integrity_failed", "Directory job digest does not match"
        )

    @staticmethod
    def _remember_cleanup_error(
        record: _JobRecord,
        error: DirectoryStableError,
    ) -> None:
        if record.cleanup_terminal_error is None:
            record.cleanup_terminal_error = error

    @staticmethod
    def _remember_deferred_error(
        record: _JobRecord,
        error: DirectoryStableError,
    ) -> None:
        if record.deferred_terminal_error is None:
            record.deferred_terminal_error = error

    def _source_child_consumed(self, record: _JobRecord) -> bool:
        authorization = record.source_authorization
        return authorization is not None and authorization.transfer_uuid in self._consumed_source

    def _destination_child_consumed(self, record: _JobRecord) -> bool:
        authorization = record.destination_authorization
        return (
            authorization is not None and authorization.transfer_uuid in self._consumed_destination
        )

    def _observe_outer_progress(self, record: _JobRecord, value: int | None) -> None:
        if value is not None and value > record.last_outer_progress_seq:
            record.last_outer_progress_seq = value
            record.last_progress_at = time.monotonic()

    def _source_snapshot(self, record: _JobRecord) -> SourceDirectoryJobStatus:
        with record.sync_lock:
            return SourceDirectoryJobStatus(
                state=cast(Any, record.state),
                expected_digest=record.expected_digest,
                progress_seq=record.progress_seq,
                entries_processed=record.entries_processed,
                files_processed=record.files_processed,
                bytes_processed=record.bytes_processed,
                probe=record.probe,
                terminal_result=record.terminal_result
                if isinstance(record.terminal_result, DirectoryCleanupResult)
                else None,
                terminal_error=record.terminal_error,
            )

    def _destination_snapshot(self, record: _JobRecord) -> DestinationDirectoryJobStatus:
        with record.sync_lock:
            return DestinationDirectoryJobStatus(
                state=cast(Any, record.state),
                expected_digest=record.expected_digest,
                progress_seq=record.progress_seq,
                files_processed=record.files_processed,
                bytes_processed=record.bytes_processed,
                cleanup_complete=None,
                terminal_result=record.terminal_result
                if isinstance(record.terminal_result, WorkspaceTransferDirectoryResult)
                else None,
                terminal_error=record.terminal_error,
            )

    def _local_snapshot(self, record: _JobRecord) -> LocalDirectoryJobStatus:
        with record.sync_lock:
            state = (
                "ready_not_started"
                if record.local_mode is None and record.state == "ready"
                else record.state
            )
            return LocalDirectoryJobStatus(
                state=cast(Any, state),
                phase=cast(Any, record.phase),
                expected_digest=record.expected_digest,
                progress_seq=record.progress_seq,
                files_processed=record.files_processed,
                bytes_processed=record.bytes_processed,
                terminal_result=record.terminal_result
                if isinstance(record.terminal_result, WorkspaceTransferDirectoryResult)
                else None,
                terminal_error=record.terminal_error,
            )

    async def _terminalize(
        self,
        record: _JobRecord,
        *,
        state: Literal["succeeded", "failed", "outcome_unknown"],
        error: DirectoryStableError | None = None,
        local: bool = False,
        cleanup_complete: bool | None = None,
    ) -> None:
        key = (record.operation_id, record.role)
        if self._active.get(key) is not record:
            return
        record.state = state
        record.terminal_error = error
        if local:
            snapshot: (
                SourceDirectoryJobStatus | DestinationDirectoryJobStatus | LocalDirectoryJobStatus
            ) = self._local_snapshot(record)
        elif record.role == "source":
            snapshot = self._source_snapshot(record)
        else:
            destination = self._destination_snapshot(record)
            if cleanup_complete is not None:
                destination = destination.model_copy(update={"cleanup_complete": cleanup_complete})
            snapshot = destination
        self._remove_active_record(record)
        self._retained[key] = _TerminalRecord(
            request_digest=record.request_digest,
            expected_digest=record.expected_digest,
            snapshot=snapshot,
            expires_at=time.monotonic() + self._terminal_ttl,
        )

    def _retain_ready_destination(self, record: _JobRecord) -> None:
        key: JobKey = (record.operation_id, "destination")
        destination_snapshot = self._destination_snapshot(record)
        local_snapshot = self._local_snapshot(record)
        self._remove_active_record(record)
        self._retained[key] = _TerminalRecord(
            request_digest=record.request_digest,
            expected_digest=record.expected_digest,
            snapshot=destination_snapshot,
            expires_at=time.monotonic() + self._terminal_ttl,
            local_snapshot=local_snapshot,
        )

    def _remove_active_record(self, record: _JobRecord) -> None:
        self._active.pop((record.operation_id, record.role), None)
        if record.source_authorization is not None:
            self._source_authorizations.pop(record.source_authorization.transfer_uuid, None)
            self._consumed_source.pop(record.source_authorization.transfer_uuid, None)
            self._remember_expired_authorization(
                record.source_authorization.transfer_uuid, source=True
            )
        if record.destination_authorization is not None:
            self._destination_authorizations.pop(
                record.destination_authorization.transfer_uuid, None
            )
            self._consumed_destination.pop(record.destination_authorization.transfer_uuid, None)
            self._remember_expired_authorization(
                record.destination_authorization.transfer_uuid, source=False
            )
        for authorization_map, source in (
            (self._source_authorizations, True),
            (self._consumed_source, True),
            (self._destination_authorizations, False),
            (self._consumed_destination, False),
        ):
            for transfer_uuid, (owner, _) in tuple(authorization_map.items()):
                if owner is record:
                    authorization_map.pop(transfer_uuid, None)
                    self._remember_expired_authorization(transfer_uuid, source=source)
        record.manifest = None
        record.entries_by_path.clear()
        record.pages = ()
        record.committed.clear()
        record.created_directories.clear()

    def _terminal_snapshot(
        self, key: JobKey, expected_digest: str, *, local: bool = False
    ) -> SourceDirectoryJobStatus | DestinationDirectoryJobStatus | LocalDirectoryJobStatus | None:
        terminal = self._retained.get(key)
        if terminal is not None:
            self._validate_terminal_digest(terminal, expected_digest)
            return (
                terminal.local_snapshot if local and terminal.local_snapshot else terminal.snapshot
            )
        tombstone = self._tombstones.get(key)
        if tombstone is not None:
            self._validate_tombstone_digest(tombstone, expected_digest)
            return (
                tombstone.local_snapshot
                if local and tombstone.local_snapshot
                else tombstone.snapshot
            )
        return None

    def _spawn(self, record: _JobRecord, coroutine: Any) -> None:
        task = asyncio.create_task(cast(Any, coroutine))
        record.task = task
        self._tasks.add(task)

        def done(completed: asyncio.Task[None]) -> None:
            self._tasks.discard(completed)
            if record.task is completed:
                record.task = None
            if not completed.cancelled():
                with contextlib.suppress(BaseException):
                    completed.exception()

        task.add_done_callback(done)

    async def _background_cleanup(self) -> None:
        interval = max(0.01, min(1.0, self._idle_timeout / 4, self._terminal_ttl / 4))
        while not self._cleanup_stop.is_set():
            try:
                await asyncio.wait_for(self._cleanup_stop.wait(), timeout=interval)
            except TimeoutError:
                pass
            if self._cleanup_stop.is_set():
                break
            self._purge_expired()
            self._expire_unconsumed_authorizations()
            await self._expire_idle_jobs()

    def _expire_unconsumed_authorizations(self) -> None:
        now = time.monotonic()
        for authorizations, attribute in (
            (self._source_authorizations, "source_authorization"),
            (self._destination_authorizations, "destination_authorization"),
        ):
            for transfer_uuid, (record, authorization) in tuple(authorizations.items()):
                if authorization.expires_at > now:
                    continue
                authorizations.pop(transfer_uuid, None)
                if getattr(record, attribute) == authorization:
                    setattr(record, attribute, None)
                self._remember_expired_authorization(
                    transfer_uuid, source=attribute == "source_authorization"
                )

    async def _expire_idle_jobs(self) -> None:
        now = time.monotonic()
        for record in tuple(self._active.values()):
            if now - record.last_progress_at < self._idle_timeout:
                continue
            if record.role == "source":
                if record.state in {"ready_retrieval", "held"}:
                    error = _stable_error(
                        "workspace_transfer_timeout", "Directory source job expired"
                    )
                    record.stop_forward_work.set()
                    if self._source_child_consumed(record):
                        self._remember_deferred_error(record, error)
                    else:
                        await self._terminalize(record, state="failed", error=error)
                elif record.state == "scanning":
                    record.stop_forward_work.set()
                elif record.state == "source_cleanup":
                    record.stop_cleanup.set()
                continue
            if record.state == "ready":
                await self._terminalize(
                    record,
                    state="failed",
                    error=_stable_error(
                        "workspace_transfer_timeout", "Directory destination job expired"
                    ),
                )
            elif record.state == "finalized_held":
                snapshot = self._destination_snapshot(record)
                await self._exit_reservation(record)
                key = (record.operation_id, record.role)
                self._remove_active_record(record)
                self._tombstones[key] = _Tombstone(
                    request_digest=record.request_digest,
                    expected_digest=record.expected_digest,
                    snapshot=snapshot,
                    released=False,
                    expires_at=now + TOMBSTONE_TTL_SECONDS,
                )
            elif record.state in {"reserved", "copying"}:
                self._remember_cleanup_error(
                    record,
                    _stable_error(
                        "workspace_transfer_timeout", "Directory destination job expired"
                    ),
                )
                record.stop_forward_work.set()
                if not self._destination_child_consumed(record) and (
                    record.task is None or record.task.done()
                ):
                    self._spawn(record, self._run_destination_cleanup(record))
            elif record.state in {"preflighting", "preparing", "finalizing", "running"}:
                if record.state in {"preparing", "finalizing"}:
                    self._remember_cleanup_error(
                        record,
                        _stable_error(
                            "workspace_transfer_timeout", "Directory destination job expired"
                        ),
                    )
                record.stop_forward_work.set()
            elif record.state in {"cleaning", "cancelling"}:
                record.stop_cleanup.set()

    def _purge_expired(self) -> None:
        now = time.monotonic()
        for key, terminal in tuple(self._retained.items()):
            if terminal.expires_at > now:
                continue
            self._retained.pop(key, None)
            self._tombstones[key] = _Tombstone(
                request_digest=terminal.request_digest,
                expected_digest=terminal.expected_digest,
                snapshot=terminal.snapshot,
                released=False,
                expires_at=now + TOMBSTONE_TTL_SECONDS,
                local_snapshot=terminal.local_snapshot,
            )
        for key, tombstone in tuple(self._tombstones.items()):
            if tombstone.expires_at <= now:
                self._tombstones.pop(key, None)
                self._release_lifecycle_credit(key)

        for authorizations in (self._expired_source, self._expired_destination):
            for transfer_uuid, expires_at in tuple(authorizations.items()):
                if expires_at <= now:
                    authorizations.pop(transfer_uuid, None)

    def _remember_expired_authorization(self, transfer_uuid: UUID, *, source: bool) -> None:
        authorizations = self._expired_source if source else self._expired_destination
        if len(authorizations) >= self._lifecycle_capacity:
            authorizations.pop(next(iter(authorizations)))
        authorizations[transfer_uuid] = time.monotonic() + TOMBSTONE_TTL_SECONDS

    async def request_close(
        self,
        *,
        preserve_finalized: bool = False,
        final: bool = True,
    ) -> None:
        self._closed = True
        if final:
            self._cleanup_stop.set()
        for record in tuple(self._active.values()):
            record.stop_forward_work.set()
            self._expire_record_unconsumed_authorizations(record)
            if self._record_has_consumed_child(record):
                continue
            if (
                not final
                and record.role == "destination"
                and record.state == "ready"
                and record.local_mode is None
            ):
                await self._exit_reservation(record)
                self._retain_ready_destination(record)
                continue
            if record.state == "finalized_held":
                await self._exit_reservation(record)
                continue
            if record.role == "destination":
                self._remember_cleanup_error(
                    record,
                    _stable_error(
                        "tool_execution_cancelled", "Directory destination job was cancelled"
                    ),
                )
            if record.root_claimed:
                record.stop_cleanup.clear()
                if record.task is None or record.task.done():
                    self._spawn(record, self._run_destination_cleanup(record))
            elif record.task is None or record.task.done():
                await self._exit_reservation(record)
                await self._terminalize(
                    record,
                    state="failed",
                    error=_stable_error("tool_execution_cancelled", "Directory manager closed"),
                    local=record.local_mode is not None,
                )

    def _expire_record_unconsumed_authorizations(self, record: _JobRecord) -> None:
        for authorizations, attribute, source in (
            (self._source_authorizations, "source_authorization", True),
            (self._destination_authorizations, "destination_authorization", False),
        ):
            authorization = getattr(record, attribute)
            if authorization is None or authorization.transfer_uuid not in authorizations:
                continue
            authorizations.pop(authorization.transfer_uuid, None)
            setattr(record, attribute, None)
            self._remember_expired_authorization(authorization.transfer_uuid, source=source)

    def _record_has_consumed_child(self, record: _JobRecord) -> bool:
        return any(
            owner is record
            for authorizations in (self._consumed_source, self._consumed_destination)
            for owner, _ in authorizations.values()
        )

    async def wait_for_drain(self, *, timeout_seconds: float | None) -> bool:
        if timeout_seconds is not None and timeout_seconds < 0:
            raise ValueError("drain timeout must be non-negative")
        loop = asyncio.get_running_loop()
        deadline = None if timeout_seconds is None else loop.time() + timeout_seconds
        while True:
            pending = {task for task in self._tasks if not task.done()}
            if not pending and not self._consumed_source and not self._consumed_destination:
                return True
            remaining = None if deadline is None else max(0.0, deadline - loop.time())
            if remaining == 0:
                return False
            wait_slice = 0.01 if remaining is None else min(0.01, remaining)
            if pending:
                await asyncio.wait(pending, timeout=wait_slice)
            else:
                await asyncio.sleep(wait_slice)

    async def wait_for_lifecycle_empty(self, *, timeout_seconds: float | None) -> bool:
        if timeout_seconds is not None and timeout_seconds < 0:
            raise ValueError("lifecycle timeout must be non-negative")
        loop = asyncio.get_running_loop()
        deadline = None if timeout_seconds is None else loop.time() + timeout_seconds
        while True:
            self._purge_expired()
            if self.lifecycle_count == 0:
                return True
            self._lifecycle_changed.clear()
            self._purge_expired()
            if self.lifecycle_count == 0:
                return True
            remaining = None if deadline is None else max(0.0, deadline - loop.time())
            if remaining == 0:
                return False
            try:
                await asyncio.wait_for(self._lifecycle_changed.wait(), timeout=remaining)
            except TimeoutError:
                return False

    def discard_lifecycle(self) -> None:
        if (
            any(not task.done() for task in self._tasks)
            or self._consumed_source
            or self._consumed_destination
        ):
            raise RuntimeError("directory manager still has in-flight work")
        for key in tuple(self._credited_keys):
            self._release_lifecycle_credit(key)
        self._active.clear()
        self._retained.clear()
        self._tombstones.clear()
        self._source_authorizations.clear()
        self._destination_authorizations.clear()
        self._expired_source.clear()
        self._expired_destination.clear()

    async def aclose(
        self,
        *,
        grace_seconds: float = 0.1,
        preserve_finalized: bool = False,
        final: bool = True,
    ) -> bool:
        await self.request_close(preserve_finalized=preserve_finalized, final=final)
        quiescent = await self.wait_for_drain(timeout_seconds=grace_seconds)
        if quiescent:
            for record in tuple(self._active.values()):
                if record.state == "finalized_held" and preserve_finalized:
                    continue
                await self._exit_reservation(record)
        if final and self._cleanup_task is not None:
            await self._cleanup_task
        return quiescent


def _request_digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _same_authorization(first: _Authorization, second: _Authorization) -> bool:
    return (
        first.transfer_uuid == second.transfer_uuid
        and first.relative_path == second.relative_path
        and first.fingerprint == second.fingerprint
        and first.expected_size == second.expected_size
    )


def _stable_error(code: str, message: str) -> DirectoryStableError:
    return DirectoryStableError(code=code, message=message)


def _tool_error(error: ToolFailure) -> DirectoryStableError:
    return _stable_error(error.code, error.message[:512])


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _current_destination_platform() -> DestinationPlatform:
    if os.name == "nt":
        return "windows"
    if sys.platform == "darwin":
        return "macos"
    return "linux"


def _check_forward(event: threading.Event) -> None:
    if event.is_set():
        raise _ForwardWorkCancelledError


def _check_cleanup(event: threading.Event) -> None:
    if event.is_set():
        raise _CleanupCancelledError


def _is_link_or_reparse(info: os.stat_result) -> bool:
    if stat.S_ISLNK(info.st_mode):
        return True
    attributes = getattr(info, "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _directory_identity(path: Path) -> str:
    info = path.stat(follow_symlinks=False)
    if _is_link_or_reparse(info) or not stat.S_ISDIR(info.st_mode):
        raise ToolFailure("workspace_file_changed", "Directory identity changed")
    raw = f"{info.st_dev}:{info.st_ino}".encode("ascii")
    return hashlib.sha256(raw).hexdigest()


def _file_fingerprint(info: os.stat_result) -> str:
    return opaque_stat_fingerprint((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns))


def _scan_source_path(
    source: Path,
    stop: threading.Event,
    progress: Callable[..., None],
) -> _ScannedSource:
    _check_forward(stop)
    try:
        root_info = source.stat(follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ToolFailure("workspace_not_found", "Source path was not found") from exc
    except OSError as exc:
        raise ToolFailure("workspace_permission_denied", "Source path is unavailable") from exc
    if _is_link_or_reparse(root_info):
        raise ToolFailure("workspace_symlink_escape", "Source path is a link")
    if stat.S_ISREG(root_info.st_mode):
        progress(entries=1, files=1, byte_count=root_info.st_size)
        return _ScannedSource(
            probe=FileSourceProbe(
                size=root_info.st_size,
                fingerprint=_file_fingerprint(root_info),
            )
        )
    if not stat.S_ISDIR(root_info.st_mode):
        raise ToolFailure(
            "workspace_blocked_path", "Source path is not a regular file or directory"
        )
    scanned = _scan_directory(source, stop, progress, hash_contents=False)
    pages = split_manifest_pages(scanned.manifest)
    return _ScannedSource(
        probe=DirectorySourceProbe(
            root_identity=cast(str, scanned.manifest.root_identity),
            scanned_entries=scanned.manifest.scanned_entries,
            file_count=len(scanned.manifest.entries),
            total_bytes=scanned.manifest.total_bytes,
            manifest_sha256=scanned.manifest.manifest_sha256,
            page_count=len(pages),
        ),
        manifest=scanned.manifest,
        pages=pages,
    )


def _scan_directory(
    root: Path,
    stop: threading.Event,
    progress: Callable[..., None],
    *,
    hash_contents: bool,
) -> _DirectoryScan:
    try:
        root_info = root.stat(follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ToolFailure("workspace_not_found", "Source directory was not found") from exc
    except OSError as exc:
        raise ToolFailure("workspace_permission_denied", "Source directory is unavailable") from exc
    if _is_link_or_reparse(root_info):
        raise ToolFailure("workspace_symlink_escape", "Source directory is a link")
    if not stat.S_ISDIR(root_info.st_mode):
        raise ToolFailure("tool_not_a_directory", "Source path is not a directory")

    directories: list[DirectoryManifestDirectory] = []
    entries: list[DirectoryManifestEntry] = []
    content: list[DirectoryContentEntry] = []
    queue: deque[tuple[Path, str]] = deque([(root, "")])
    while queue:
        _check_forward(stop)
        current, prefix = queue.popleft()
        try:
            with os.scandir(current) as iterator:
                children = list(iterator)
        except PermissionError as exc:
            raise ToolFailure("workspace_permission_denied", "Directory cannot be scanned") from exc
        except OSError as exc:
            raise ToolFailure("workspace_storage_unavailable", "Directory scan failed") from exc
        try:
            children.sort(key=lambda item: item.name.encode("utf-8"))
        except UnicodeError as exc:
            raise ToolFailure("workspace_invalid_request", "Filename is not valid UTF-8") from exc
        for child in children:
            _check_forward(stop)
            relative_path = child.name if not prefix else f"{prefix}/{child.name}"
            try:
                info = child.stat(follow_symlinks=False)
            except FileNotFoundError as exc:
                raise ToolFailure("workspace_file_changed", "Source changed during scan") from exc
            except OSError as exc:
                raise ToolFailure(
                    "workspace_permission_denied", "Source entry is unavailable"
                ) from exc
            if _is_link_or_reparse(info):
                raise ToolFailure("workspace_symlink_escape", "Source tree contains a link")
            if stat.S_ISDIR(info.st_mode):
                directories.append(
                    DirectoryManifestDirectory(
                        relative_path=relative_path,
                        identity=_directory_identity(Path(child.path)),
                    )
                )
                queue.append((Path(child.path), relative_path))
                progress(entries=1)
            elif stat.S_ISREG(info.st_mode):
                fingerprint = _file_fingerprint(info)
                entries.append(
                    DirectoryManifestEntry(
                        relative_path=relative_path,
                        size=info.st_size,
                        fingerprint=fingerprint,
                    )
                )
                if hash_contents:
                    size, digest = _hash_regular_file(Path(child.path), info, stop, progress)
                    content.append(
                        DirectoryContentEntry(
                            relative_path=relative_path,
                            size=size,
                            sha256=digest,
                        )
                    )
                    progress(entries=1, files=1)
                else:
                    progress(entries=1, files=1, byte_count=info.st_size)
            else:
                raise ToolFailure(
                    "workspace_blocked_path", "Source tree contains a special filesystem entry"
                )
            if len(directories) + len(entries) > MAX_DIRECTORY_ENTRIES:
                raise ToolFailure(
                    "workspace_directory_too_large", "Directory contains too many entries"
                )
    if not entries:
        raise ToolFailure("workspace_invalid_request", "Directory contains no regular files")
    directories.sort(key=lambda item: item.relative_path.encode("utf-8"))
    entries.sort(key=lambda item: item.relative_path.encode("utf-8"))
    content.sort(key=lambda item: item.relative_path.encode("utf-8"))
    try:
        manifest = create_directory_manifest(
            root_identity=_directory_identity(root),
            directories=directories,
            entries=entries,
        )
    except ValidationError as exc:
        if "5 MiB" in str(exc) or len(directories) + len(entries) >= MAX_DIRECTORY_ENTRIES:
            raise ToolFailure(
                "workspace_directory_too_large", "Directory manifest is too large"
            ) from exc
        raise ToolFailure(
            "workspace_transfer_integrity_failed", "Directory manifest is invalid"
        ) from exc
    return _DirectoryScan(manifest=manifest, content_entries=tuple(content))


def _open_regular_read(path: Path) -> tuple[int, os.stat_result]:
    flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0)) | int(getattr(os, "O_NOFOLLOW", 0))
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ToolFailure("workspace_blocked_path", "Source is not a regular file")
        return descriptor, info
    except BaseException:
        os.close(descriptor)
        raise


def _hash_regular_file(
    path: Path,
    initial: os.stat_result,
    stop: threading.Event,
    progress: Callable[..., None],
) -> tuple[int, str]:
    descriptor, opened = _open_regular_read(path)
    try:
        if _file_fingerprint(opened) != _file_fingerprint(initial):
            raise ToolFailure("workspace_file_changed", "Source changed before reading")
        digest = hashlib.sha256()
        size = 0
        while True:
            _check_forward(stop)
            chunk = os.read(descriptor, DIRECTORY_IO_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            progress(byte_count=len(chunk), phase="hashing")
        current = os.stat(path, follow_symlinks=False)
        if _file_fingerprint(os.fstat(descriptor)) != _file_fingerprint(
            initial
        ) or _file_fingerprint(current) != _file_fingerprint(initial):
            raise ToolFailure("workspace_file_changed", "Source changed while reading")
        return size, digest.hexdigest()
    finally:
        os.close(descriptor)


def _preflight_destination(
    destination: Path,
    manifest: DirectoryManifest,
    platform: DestinationPlatform,
    stop: threading.Event,
    progress: Callable[..., None],
) -> None:
    _check_forward(stop)
    destination_collision_keys(manifest, platform=platform)
    if _lstat_optional(destination) is not None:
        raise ToolFailure("workspace_file_changed", "Destination root already exists")
    current = destination.parent
    while not current.exists():
        _check_forward(stop)
        current = current.parent
    info = current.stat(follow_symlinks=False)
    if _is_link_or_reparse(info) or not stat.S_ISDIR(info.st_mode):
        raise ToolFailure("workspace_symlink_escape", "Destination ancestor is not a directory")
    for entry in manifest.entries:
        _check_forward(stop)
        mapped = destination.joinpath(*entry.relative_path.split("/"))
        if len(str(mapped)) > 4096:
            raise ToolFailure("workspace_invalid_request", "Destination path is too long")
        if platform == "windows":
            _validate_windows_components(entry.relative_path)
        progress(entries=1)


def _validate_windows_components(relative_path: str) -> None:
    reserved = {
        "con",
        "prn",
        "aux",
        "nul",
        *(f"com{index}" for index in range(1, 10)),
        *(f"lpt{index}" for index in range(1, 10)),
    }
    for component in relative_path.split("/"):
        stem = component.split(".", 1)[0].casefold()
        if component.endswith((".", " ")) or stem in reserved:
            raise ToolFailure("workspace_invalid_request", "Path is not representable on Windows")


def _reject_overlap(source: Path, destination: Path, platform: DestinationPlatform) -> None:
    def key(path: Path) -> tuple[str, ...]:
        parts = path.parts
        if platform in {"macos", "windows"}:
            import unicodedata

            return tuple(unicodedata.normalize("NFC", part).casefold() for part in parts)
        return parts

    source_key = key(source)
    destination_key = key(destination)
    if (
        source_key == destination_key
        or destination_key[: len(source_key)] == source_key
        or source_key[: len(destination_key)] == destination_key
    ):
        raise ToolFailure("workspace_invalid_request", "Directory source and destination overlap")


def _claim_destination_root(record: _JobRecord, destination: Path) -> None:
    _check_forward(record.stop_forward_work)
    if _lstat_optional(destination) is not None:
        raise ToolFailure("workspace_file_changed", "Destination root already exists")
    missing: list[Path] = []
    current = destination.parent
    while _lstat_optional(current) is None:
        missing.append(current)
        current = current.parent
    current_info = current.stat(follow_symlinks=False)
    if _is_link_or_reparse(current_info) or not stat.S_ISDIR(current_info.st_mode):
        raise ToolFailure("workspace_symlink_escape", "Destination ancestor is not a directory")
    for parent in reversed(missing):
        _check_forward(record.stop_forward_work)
        try:
            parent.mkdir()
        except FileExistsError as exc:
            raise ToolFailure("workspace_file_changed", "Destination ancestor changed") from exc
        record.created_directories[parent] = ""
        record.created_directories[parent] = _directory_identity(parent)
        record.bump(phase="preparing")
    _check_forward(record.stop_forward_work)
    try:
        destination.mkdir()
    except FileExistsError as exc:
        raise ToolFailure("workspace_file_changed", "Destination root already exists") from exc
    record.root_claimed = True
    record.created_directories[destination] = ""
    record.created_directories[destination] = _directory_identity(destination)
    record.bump(phase="preparing")


def _claim_destination_tree(
    record: _JobRecord,
    destination: Path,
    manifest: DirectoryManifest,
) -> None:
    _claim_destination_root(record, destination)
    relative_directories = {
        "/".join(parts[:end])
        for entry in manifest.entries
        for parts in (entry.relative_path.split("/"),)
        for end in range(1, len(parts))
    }
    for relative_path in sorted(
        relative_directories,
        key=lambda value: (len(value.split("/")), value.encode("utf-8")),
    ):
        _check_forward(record.stop_forward_work)
        path = destination.joinpath(*relative_path.split("/"))
        try:
            path.mkdir()
        except FileExistsError as exc:
            raise ToolFailure("workspace_file_changed", "Destination parent changed") from exc
        record.created_directories[path] = ""
        record.created_directories[path] = _directory_identity(path)
        record.bump(phase="preparing")


def _derived_parents(root: Path, child: Path) -> tuple[Path, ...]:
    parents: list[Path] = []
    current = child.parent
    while current != root and root in current.parents:
        parents.append(current)
        current = current.parent
    return tuple(reversed(parents))


def _verify_destination_tree(
    root: Path,
    manifest: DirectoryManifest,
    committed: dict[str, _CommittedDestination],
    stop: threading.Event,
    progress: Callable[..., None],
) -> None:
    expected_files = {entry.relative_path for entry in manifest.entries}
    expected_directories = {
        "/".join(entry.relative_path.split("/")[:end])
        for entry in manifest.entries
        for end in range(1, len(entry.relative_path.split("/")))
    }
    actual_files: set[str] = set()
    actual_directories: set[str] = set()
    queue: deque[tuple[Path, str]] = deque([(root, "")])
    while queue:
        _check_forward(stop)
        current, prefix = queue.popleft()
        try:
            with os.scandir(current) as iterator:
                children = list(iterator)
        except OSError as exc:
            raise ToolFailure(
                "workspace_storage_unavailable", "Destination cannot be scanned"
            ) from exc
        for child in children:
            _check_forward(stop)
            relative = child.name if not prefix else f"{prefix}/{child.name}"
            info = child.stat(follow_symlinks=False)
            if _is_link_or_reparse(info):
                raise ToolFailure(
                    "workspace_transfer_integrity_failed", "Destination contains a link"
                )
            if stat.S_ISDIR(info.st_mode):
                actual_directories.add(relative)
                queue.append((Path(child.path), relative))
            elif stat.S_ISREG(info.st_mode):
                actual_files.add(relative)
                expected = committed.get(relative)
                if (
                    expected is None
                    or expected.verified_size != info.st_size
                    or expected.destination_fingerprint != _file_fingerprint(info)
                ):
                    raise ToolFailure(
                        "workspace_transfer_integrity_failed", "Destination file changed"
                    )
            else:
                raise ToolFailure(
                    "workspace_transfer_integrity_failed", "Destination contains a special entry"
                )
            progress(entries=1, phase="revalidating")
    if actual_files != expected_files or actual_directories != expected_directories:
        raise ToolFailure(
            "workspace_transfer_integrity_failed", "Destination tree does not match manifest"
        )


def _aggregate_result(
    committed: Any,
    *,
    warnings: list[str] | None = None,
) -> WorkspaceTransferDirectoryResult:
    items = sorted(committed, key=lambda item: item.relative_path.encode("utf-8"))
    content_entries = tuple(
        DirectoryContentEntry(
            relative_path=item.relative_path,
            size=item.verified_size,
            sha256=item.verified_sha256,
        )
        for item in items
    )
    return WorkspaceTransferDirectoryResult(
        files_transferred=len(items),
        bytes_transferred=sum(item.verified_size for item in items),
        sha256=directory_content_sha256(content_entries),
        warnings=warnings or [],
    )


def _execute_local_job(
    record: _JobRecord,
    source: Path,
    destination: Path,
    expected_manifest: DirectoryManifest,
) -> WorkspaceTransferDirectoryResult:
    _check_forward(record.stop_forward_work)
    record.bump(phase="revalidating")
    if record.local_mode == "move":
        return _execute_local_move(record, source, destination, expected_manifest)
    return _execute_local_copy(record, source, destination, expected_manifest)


def _require_matching_manifest(actual: DirectoryManifest, expected: DirectoryManifest) -> None:
    if actual.model_dump() != expected.model_dump():
        raise ToolFailure("workspace_file_changed", "Source directory changed after manifest")


def _execute_local_copy(
    record: _JobRecord,
    source: Path,
    destination: Path,
    expected_manifest: DirectoryManifest,
) -> WorkspaceTransferDirectoryResult:
    scanned = _scan_directory(source, record.stop_forward_work, record.bump, hash_contents=False)
    _require_matching_manifest(scanned.manifest, expected_manifest)
    _claim_destination_root(record, destination)
    for entry in expected_manifest.entries:
        _check_forward(record.stop_forward_work)
        source_file = source.joinpath(*entry.relative_path.split("/"))
        destination_file = destination.joinpath(*entry.relative_path.split("/"))
        for parent in _derived_parents(destination, destination_file):
            _create_owned_directory(record, parent)
        committed = _copy_regular_file(
            source_file,
            destination_file,
            entry,
            record.stop_forward_work,
            record.bump,
        )
        record.committed[entry.relative_path] = committed
        record.bump(files=1, phase="copying")
    _verify_destination_tree(
        destination,
        expected_manifest,
        record.committed,
        record.stop_forward_work,
        record.bump,
    )
    return _aggregate_result(record.committed.values())


def _create_owned_directory(record: _JobRecord, path: Path) -> None:
    _check_forward(record.stop_forward_work)
    info = _lstat_optional(path)
    if info is not None:
        if _is_link_or_reparse(info) or not stat.S_ISDIR(info.st_mode):
            raise ToolFailure("workspace_file_changed", "Destination parent changed")
        return
    try:
        path.mkdir()
    except FileExistsError as exc:
        raise ToolFailure("workspace_file_changed", "Destination parent changed") from exc
    record.created_directories[path] = ""
    record.created_directories[path] = _directory_identity(path)
    record.bump(phase="copying")


def _copy_regular_file(
    source: Path,
    destination: Path,
    expected: DirectoryManifestEntry,
    stop: threading.Event,
    progress: Callable[..., None],
) -> _CommittedDestination:
    try:
        initial = source.stat(follow_symlinks=False)
    except OSError as exc:
        raise ToolFailure("workspace_file_changed", "Source file is unavailable") from exc
    if (
        _is_link_or_reparse(initial)
        or not stat.S_ISREG(initial.st_mode)
        or initial.st_size != expected.size
        or _file_fingerprint(initial) != expected.fingerprint
    ):
        raise ToolFailure("workspace_file_changed", "Source file changed after manifest")
    source_fd, opened = _open_regular_read(source)
    temporary_fd, temporary_value = tempfile.mkstemp(
        prefix=f".{destination.name}.openoctopus-", dir=destination.parent
    )
    temporary = Path(temporary_value)
    try:
        if _file_fingerprint(opened) != expected.fingerprint:
            raise ToolFailure("workspace_file_changed", "Source file changed before copy")
        digest = hashlib.sha256()
        size = 0
        while True:
            _check_forward(stop)
            chunk = os.read(source_fd, DIRECTORY_IO_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            view = memoryview(chunk)
            while view:
                _check_forward(stop)
                written = os.write(temporary_fd, view)
                if written <= 0:
                    raise ToolFailure("workspace_storage_unavailable", "Destination write failed")
                view = view[written:]
            progress(byte_count=len(chunk), phase="copying")
        os.fsync(temporary_fd)
        current = source.stat(follow_symlinks=False)
        if (
            size != expected.size
            or _file_fingerprint(os.fstat(source_fd)) != expected.fingerprint
            or _file_fingerprint(current) != expected.fingerprint
        ):
            raise ToolFailure("workspace_file_changed", "Source file changed during copy")
        os.close(temporary_fd)
        temporary_fd = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as exc:
            raise ToolFailure("workspace_file_changed", "Destination file already exists") from exc
        except OSError as exc:
            raise ToolFailure(
                "workspace_storage_unavailable", "Atomic destination publish failed"
            ) from exc
        try:
            _fsync_directory(destination.parent)
            destination_info = destination.stat(follow_symlinks=False)
        except BaseException:
            _unlink_if_same_file(temporary, destination)
            raise
        return _CommittedDestination(
            relative_path=expected.relative_path,
            destination_fingerprint=_file_fingerprint(destination_info),
            verified_size=size,
            verified_sha256=digest.hexdigest(),
        )
    finally:
        with contextlib.suppress(OSError):
            os.close(source_fd)
        if temporary_fd >= 0:
            with contextlib.suppress(OSError):
                os.close(temporary_fd)
        with contextlib.suppress(OSError):
            temporary.unlink()


def _unlink_if_same_file(source: Path, destination: Path) -> bool:
    """Remove only the destination hard link published from this exact temp."""

    source_info = _lstat_optional(source)
    destination_info = _lstat_optional(destination)
    if (
        source_info is None
        or destination_info is None
        or not stat.S_ISREG(source_info.st_mode)
        or not stat.S_ISREG(destination_info.st_mode)
        or (source_info.st_dev, source_info.st_ino)
        != (destination_info.st_dev, destination_info.st_ino)
    ):
        return False
    try:
        destination.unlink()
    except OSError:
        return False
    return True


def _execute_local_move(
    record: _JobRecord,
    source: Path,
    destination: Path,
    expected_manifest: DirectoryManifest,
) -> WorkspaceTransferDirectoryResult:
    first = _scan_directory(source, record.stop_forward_work, record.bump, hash_contents=True)
    _require_matching_manifest(first.manifest, expected_manifest)
    record.bump(phase="revalidating")
    second = _scan_directory(source, record.stop_forward_work, record.bump, hash_contents=False)
    _require_matching_manifest(second.manifest, expected_manifest)
    _prepare_destination_parents(record, destination)
    _check_forward(record.stop_forward_work)
    record.bump(phase="renaming")
    _rename_directory_no_replace(source, destination)
    record.root_claimed = True
    record.created_directories[destination] = ""
    record.created_directories[destination] = _directory_identity(destination)
    entries = tuple(
        _CommittedDestination(
            relative_path=item.relative_path,
            destination_fingerprint="moved",
            verified_size=item.size,
            verified_sha256=item.sha256,
        )
        for item in first.content_entries
    )
    return _aggregate_result(entries)


def _prepare_destination_parents(record: _JobRecord, destination: Path) -> None:
    _check_forward(record.stop_forward_work)
    if _lstat_optional(destination) is not None:
        raise ToolFailure("workspace_file_changed", "Destination root already exists")
    missing: list[Path] = []
    current = destination.parent
    while _lstat_optional(current) is None:
        missing.append(current)
        current = current.parent
    info = current.stat(follow_symlinks=False)
    if _is_link_or_reparse(info) or not stat.S_ISDIR(info.st_mode):
        raise ToolFailure("workspace_symlink_escape", "Destination ancestor is invalid")
    for parent in reversed(missing):
        _check_forward(record.stop_forward_work)
        parent.mkdir()
        record.created_directories[parent] = ""
        record.created_directories[parent] = _directory_identity(parent)
        record.bump(phase="preparing")


def _rename_directory_no_replace(source: Path, destination: Path) -> None:
    if sys.platform.startswith("linux"):
        try:
            rename = ctypes.CDLL(None, use_errno=True).renameat2
        except AttributeError as exc:
            raise ToolFailure(
                "workspace_storage_unavailable", "Exclusive directory move is unavailable"
            ) from exc
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        ctypes.set_errno(0)
        if rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1) != 0:
            _raise_rename_error(ctypes.get_errno())
    elif sys.platform == "darwin":
        try:
            rename = ctypes.CDLL(None, use_errno=True).renameatx_np
        except AttributeError as exc:
            raise ToolFailure(
                "workspace_storage_unavailable", "Exclusive directory move is unavailable"
            ) from exc
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        ctypes.set_errno(0)
        if rename(-2, os.fsencode(source), -2, os.fsencode(destination), 0x4) != 0:
            _raise_rename_error(ctypes.get_errno())
    elif os.name == "nt":
        from ctypes import wintypes

        kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
        move = kernel32.MoveFileExW
        move.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        move.restype = wintypes.BOOL
        if not move(str(source), str(destination), 0):
            error = ctypes.get_last_error()
            if error in {80, 183}:
                raise ToolFailure("workspace_file_changed", "Destination root already exists")
            if error in {17, 1, 50, 87}:
                raise ToolFailure(
                    "workspace_storage_unavailable", "Exclusive directory move is unavailable"
                )
            raise ToolFailure(
                "workspace_storage_unavailable", "Directory move could not be completed"
            )
    else:
        raise ToolFailure(
            "workspace_storage_unavailable", "Exclusive directory move is unavailable"
        )
    with contextlib.suppress(OSError):
        _fsync_directory(destination.parent)
    if source.parent != destination.parent:
        with contextlib.suppress(OSError):
            _fsync_directory(source.parent)


def _raise_rename_error(error: int) -> None:
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise ToolFailure("workspace_file_changed", "Destination root already exists")
    if error == errno.EXDEV:
        raise ToolFailure("workspace_storage_unavailable", "Same-volume exclusive move is required")
    if error in {
        errno.EINVAL,
        getattr(errno, "ENOSYS", -1),
        getattr(errno, "ENOTSUP", -1),
        getattr(errno, "EOPNOTSUPP", -1),
    }:
        raise ToolFailure(
            "workspace_storage_unavailable", "Exclusive directory move is unavailable"
        )
    raise ToolFailure("workspace_storage_unavailable", "Directory move could not be completed")


def _cleanup_destination(record: _JobRecord, stop: threading.Event) -> bool:
    complete = True
    assert record.destination_path is not None
    for relative_path in sorted(
        record.committed, key=lambda value: value.encode("utf-8"), reverse=True
    ):
        _check_cleanup(stop)
        committed = record.committed[relative_path]
        path = record.destination_path.joinpath(*relative_path.split("/"))
        info = _lstat_optional(path)
        if info is None:
            continue
        if (
            _is_link_or_reparse(info)
            or not stat.S_ISREG(info.st_mode)
            or _file_fingerprint(info) != committed.destination_fingerprint
        ):
            complete = False
            continue
        try:
            path.unlink()
            record.bump(files=1, phase="cleanup")
        except OSError:
            complete = False
    for path, identity in sorted(
        record.created_directories.items(),
        key=lambda item: len(item[0].parts),
        reverse=True,
    ):
        _check_cleanup(stop)
        info = _lstat_optional(path)
        if info is None:
            continue
        try:
            if _directory_identity(path) != identity:
                complete = False
                continue
            path.rmdir()
            record.bump(phase="cleanup")
        except (OSError, ToolFailure):
            complete = False
    if record.root_claimed and _lstat_optional(record.destination_path) is not None:
        complete = False
    return complete


def _conditional_source_cleanup(
    source: Path,
    manifest: DirectoryManifest,
    stop: threading.Event,
    progress: Callable[..., None],
) -> DirectoryCleanupResult:
    changed = False
    incomplete = False
    for entry in manifest.entries:
        _check_cleanup(stop)
        path = source.joinpath(*entry.relative_path.split("/"))
        info = _lstat_optional(path)
        if info is None:
            continue
        if (
            _is_link_or_reparse(info)
            or not stat.S_ISREG(info.st_mode)
            or info.st_size != entry.size
            or _file_fingerprint(info) != entry.fingerprint
        ):
            changed = True
            incomplete = True
            continue
        try:
            path.unlink()
            progress(files=1, phase="cleanup")
        except OSError:
            incomplete = True
    for directory in sorted(
        manifest.directories,
        key=lambda item: len(item.relative_path.split("/")),
        reverse=True,
    ):
        _check_cleanup(stop)
        path = source.joinpath(*directory.relative_path.split("/"))
        if _lstat_optional(path) is None:
            continue
        try:
            if _directory_identity(path) != directory.identity:
                changed = True
                incomplete = True
                continue
            path.rmdir()
            progress(phase="cleanup")
        except OSError:
            incomplete = True
    if _lstat_optional(source) is not None:
        try:
            if _directory_identity(source) != manifest.root_identity:
                changed = True
                incomplete = True
            else:
                source.rmdir()
                progress(phase="cleanup")
        except OSError:
            incomplete = True
    warnings: list[str] = []
    if changed:
        warnings.append("source_changed_after_copy")
    if incomplete:
        warnings.append("source_cleanup_incomplete")
    return DirectoryCleanupResult(cleanup_complete=not incomplete, warnings=warnings)


def _lstat_optional(path: Path) -> os.stat_result | None:
    try:
        return path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return None


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    flags = os.O_RDONLY | int(getattr(os, "O_DIRECTORY", 0))
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno in _UNSUPPORTED_DIRECTORY_FSYNC_ERRNOS:
            return
        raise
    try:
        try:
            os.fsync(descriptor)
        except OSError as exc:
            if exc.errno not in _UNSUPPORTED_DIRECTORY_FSYNC_ERRNOS:
                raise
    finally:
        os.close(descriptor)
