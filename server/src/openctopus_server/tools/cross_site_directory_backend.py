from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.protocol import TransferBeginFrame, new_uuid7
from openctopus_server.devices.registry import DeviceOutcomeUnknownError
from openctopus_server.devices.transfer import (
    TransferCommitResult,
    TransferCommittedAfterCancellation,
    TransferError,
    TransferIntegrityError,
    TransferLease,
    TransferManager,
    TransferResult,
)
from openctopus_server.directory_contract import (
    DirectoryContentEntry,
    DirectoryManifest,
    DirectoryManifestEntry,
    directory_content_sha256,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.tools.device_directory_jobs import DeviceDirectoryJobController
from openctopus_server.tools.directory_transfer import (
    DirectoryChildCommittedAfterCancellation,
    DirectoryChildResult,
    DirectoryDestinationFinalizedAfterCancellation,
)
from openctopus_server.workspace.fs import (
    DirectoryCommittedFile,
    DirectoryDestinationPlan,
    DirectoryQuotaReservation,
    FileMetadata,
    UploadCommittedAfterCancellation,
    WorkspaceFS,
)
from openctopus_server.workspace.locks import SubtreeLease
from openctopus_server.workspace.service import TransferPathTicket, WorkspaceService
from openctopus_server.workspace.storage import ObjectStream, ObjectUpload


class _CrossSiteBackend:
    def __init__(self, manifest: DirectoryManifest) -> None:
        self._manifest = manifest
        self._next_child = 0
        self._committed: list[DirectoryChildResult] = []
        self._release_task: asyncio.Task[None] | None = None

    def _validate_manifest(self, manifest: DirectoryManifest) -> None:
        if manifest != self._manifest:
            raise TransferIntegrityError("directory manifest changed before preflight")

    def _validate_child_order(self, entry: DirectoryManifestEntry) -> None:
        if self._next_child >= len(self._manifest.entries):
            raise TransferIntegrityError("directory coordinator produced an extra child")
        if entry != self._manifest.entries[self._next_child]:
            raise TransferIntegrityError("directory children are not in manifest order")

    def _record_result(
        self,
        entry: DirectoryManifestEntry,
        result: TransferResult,
    ) -> DirectoryChildResult:
        if (
            result.bytes_transferred != entry.size
            or result.etag is None
            or result.created is not True
        ):
            raise TransferIntegrityError("directory child commit metadata is invalid")
        child = DirectoryChildResult(
            relative_path=entry.relative_path,
            verified_size=result.bytes_transferred,
            verified_sha256=result.sha256,
            destination_fingerprint=result.etag,
            warnings=result.warnings,
        )
        self._committed.append(child)
        self._next_child += 1
        return child

    def _validate_committed(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> None:
        if committed != tuple(self._committed) or len(committed) != len(self._manifest.entries):
            raise TransferIntegrityError("directory committed records are incomplete")


class _RealProgressRelay:
    """Forward only observed work progress without manufacturing timer heartbeats."""

    def __init__(self, send: Callable[[int], Awaitable[None]]) -> None:
        self._send = send
        self._progress_seq = 0
        self._sent_progress_seq = 0
        self._task: asyncio.Task[None] | None = None
        self._failure: BaseException | None = None

    def advance(self) -> None:
        self._progress_seq += 1
        if self._failure is None and (self._task is None or self._task.done()):
            self._task = asyncio.create_task(self._run())

    async def flush(self) -> None:
        task = self._task
        if task is not None:
            await await_future_cancellation_safe(task)
        if self._failure is not None:
            raise self._failure

    async def _run(self) -> None:
        try:
            while self._sent_progress_seq < self._progress_seq:
                current = self._progress_seq
                await self._send(current)
                self._sent_progress_seq = current
        except BaseException as exc:
            self._failure = exc
            raise


class _ClientDestinationBackend(_CrossSiteBackend):
    def __init__(
        self,
        manifest: DirectoryManifest,
        *,
        destination: DeviceDirectoryJobController,
        destination_root: str,
    ) -> None:
        _CrossSiteBackend.__init__(self, manifest)
        self._destination = destination
        self._destination_root = destination_root
        self._destination_started = False
        self._destination_finalized = False
        self._destination_terminal = False

    async def _preflight_client_destination(self) -> None:
        self._destination_started = True
        await self._destination.start_destination_preflight(
            self._destination_root,
            self._manifest,
        )
        status = await self._destination.wait_destination_until(
            frozenset({"ready", "failed", "outcome_unknown"}),
            progress_callback=self._destination_progress_callback(),
        )
        _require_device_state(status, "ready")

    async def _prepare_client_destination(
        self,
        mark_issued: Callable[[], None],
    ) -> None:
        await self._destination.prepare_destination(on_issued=mark_issued)
        status = await self._destination.wait_destination_until(
            frozenset({"reserved", "failed", "outcome_unknown"}),
            progress_callback=self._destination_progress_callback(),
        )
        _require_device_state(status, "reserved")

    async def _finalize_client_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> None:
        self._validate_committed(committed)
        finalization = asyncio.create_task(self._finish_and_validate_client_destination(committed))
        cancelled = False
        while not finalization.done():
            try:
                await asyncio.shield(finalization)
            except asyncio.CancelledError:
                cancelled = True
            except BaseException:
                break
        finalization.result()
        if cancelled:
            raise DirectoryDestinationFinalizedAfterCancellation

    async def _finish_and_validate_client_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> None:
        await self._destination.finish_destination()
        status = await self._destination.wait_destination_until(
            frozenset({"finalized_held", "failed", "outcome_unknown"}),
            progress_callback=self._destination_progress_callback(),
        )
        _require_device_state(status, "finalized_held")
        terminal_result = getattr(status, "terminal_result", None)
        if terminal_result is None:
            raise TransferIntegrityError("Client destination omitted its final result")
        expected_digest = directory_content_sha256(
            tuple(
                DirectoryContentEntry(
                    relative_path=child.relative_path,
                    size=child.verified_size,
                    sha256=child.verified_sha256,
                )
                for child in committed
            )
        )
        if (
            terminal_result.files_transferred != len(committed)
            or terminal_result.bytes_transferred != sum(child.verified_size for child in committed)
            or terminal_result.sha256 != expected_digest
        ):
            raise TransferIntegrityError("Client destination final result mismatched children")
        self._destination_finalized = True

    def _destination_progress_callback(
        self,
    ) -> Callable[[int], Awaitable[None]] | None:
        return None

    async def _cleanup_client_destination(self) -> bool:
        if not self._destination_started:
            return True
        try:
            await self._destination.cancel_destination()
            status = await self._destination.wait_destination_until(
                frozenset({"failed", "outcome_unknown"})
            )
        except BaseException:
            return False
        self._destination_terminal = True
        return status.state == "failed" and status.cleanup_complete is True

    async def _release_client_destination(self) -> None:
        if not self._destination_started:
            return
        if not self._destination_finalized and not self._destination_terminal:
            await self._cleanup_client_destination()
        await self._destination.release_destination()


class _ClientSourceBackend(_CrossSiteBackend):
    def __init__(
        self,
        manifest: DirectoryManifest,
        *,
        source: DeviceDirectoryJobController,
        source_root: str,
    ) -> None:
        _CrossSiteBackend.__init__(self, manifest)
        self._source = source
        self._source_root = source_root
        self._source_child_pending = False

    async def _authorize_source_child(
        self,
        entry: DirectoryManifestEntry,
        slot_id: UUID,
    ) -> None:
        self._source_child_pending = True
        await self._source.authorize_source_child(slot_id, entry.relative_path, entry.fingerprint)

    def _source_child_settled(self) -> None:
        self._source_child_pending = False

    def _source_progress_callback(self) -> Callable[[int], Awaitable[None]] | None:
        return None

    async def _cleanup_client_source(self) -> tuple[str, ...]:
        try:
            await self._source.start_source_cleanup()
            status = await self._source.wait_source_until(
                frozenset({"succeeded", "failed", "outcome_unknown"}),
                progress_callback=self._source_progress_callback(),
            )
        except BaseException:
            return ("source_cleanup_incomplete",)
        if status.state != "succeeded" or status.terminal_result is None:
            return ("source_cleanup_incomplete",)
        warnings = tuple(status.terminal_result.warnings)
        if not status.terminal_result.cleanup_complete:
            warnings += ("source_cleanup_incomplete",)
        return warnings

    async def _release_client_source(self) -> None:
        failure: BaseException | None = None
        if self._source_child_pending:
            try:
                await self._source.cancel_source_probe()
                await self._source.wait_source_until(
                    frozenset({"succeeded", "failed", "outcome_unknown"})
                )
                self._source_child_pending = False
            except BaseException as exc:
                failure = exc
        try:
            await self._source.release_source_probe()
        except BaseException as exc:
            if failure is None:
                failure = exc
        if failure is not None:
            raise failure


class ServerToClientDirectoryBackend(_ClientDestinationBackend):
    """Stream a Server workspace directory into one captured Client generation."""

    def __init__(
        self,
        *,
        transfer_manager: TransferManager,
        operation_lease: TransferLease,
        workspace_fs: WorkspaceFS,
        source: TransferPathTicket,
        destination: DeviceDirectoryJobController,
        destination_root: str,
        manifest: DirectoryManifest,
    ) -> None:
        super().__init__(
            manifest,
            destination=destination,
            destination_root=destination_root,
        )
        _validate_operation(destination.directory_operation_id)
        if source.user_id != destination.user_id:
            raise ValueError("directory endpoints must belong to one authorized user")
        self._transfers = transfer_manager
        self._operation_lease = operation_lease
        self._fs = workspace_fs
        self._source_ticket = source
        self._source_lease: SubtreeLease | None = None
        self._source_cleanup_progress_seq = 0
        self._source_cleanup_progress_failure: BaseException | None = None

    async def preflight(self, manifest: DirectoryManifest) -> None:
        self._validate_manifest(manifest)
        _require_server_manifest(manifest)
        await self._preflight_client_destination()

    async def prepare_destination(self, mark_issued: Callable[[], None]) -> None:
        self._source_lease = await self._fs.acquire_subtree_lease(
            self._source_ticket.target,
            self._source_ticket.relative_path,
            owner=self._destination.directory_operation_id,
        )
        await self._prepare_client_destination(mark_issued)

    async def copy_child(
        self,
        entry: DirectoryManifestEntry,
        slot_id: UUID,
        mark_issued: Callable[[], None],
    ) -> DirectoryChildResult:
        self._validate_child_order(entry)
        await self._destination.authorize_destination_child(
            slot_id,
            entry.relative_path,
        )

        async def source_factory() -> ObjectStream:
            source = await self._fs.open_stream(
                self._source_ticket.target,
                _join_path(self._source_ticket.relative_path, entry.relative_path),
            )
            if source.size != entry.size or source.etag != entry.fingerprint:
                close = asyncio.create_task(source.aclose())
                await await_future_cancellation_safe(close)
                raise _source_changed()
            return source

        try:
            result = await self._transfers.start_server_to_client_admitted(
                handle=self._destination.route.handle,
                route=self._destination.route,
                operation_lease=self._operation_lease,
                slot_id=slot_id,
                user_id=self._destination.user_id,
                src_path=_join_path(self._source_ticket.display_path, entry.relative_path),
                dst_path=_join_path(self._destination_root, entry.relative_path),
                source_factory=source_factory,
                total_bytes=entry.size,
                src_device="server",
                dst_device=self._destination.route.device_name,
                on_issued=mark_issued,
            )
        except TransferCommittedAfterCancellation as exc:
            child = self._record_result(entry, exc.result)
            raise DirectoryChildCommittedAfterCancellation(child) from None
        return self._record_result(entry, result)

    async def finalize_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> None:
        await self._finalize_client_destination(committed)

    async def cleanup_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> bool:
        del committed
        return await self._cleanup_client_destination()

    async def cleanup_source(self, manifest: DirectoryManifest) -> tuple[str, ...]:
        self._validate_manifest(manifest)
        return await _cleanup_server_source(
            self._fs,
            self._source_ticket,
            manifest,
            owner=self._destination.directory_operation_id,
            on_progress=self._forward_server_source_progress,
        )

    async def release(self) -> None:
        task = self._release_task
        if task is None:
            task = asyncio.create_task(self._release_once())
            self._release_task = task
        await await_future_cancellation_safe(task)

    async def _release_once(self) -> None:
        failure = self._source_cleanup_progress_failure
        failure = await _release_call(self._release_client_destination, failure)
        if self._source_lease is not None:
            failure = await _release_call(self._source_lease.release, failure)
        if failure is not None:
            raise failure

    async def _forward_server_source_progress(self) -> None:
        self._source_cleanup_progress_seq += 1
        try:
            await self._destination.get_destination_status(
                outer_progress_seq=self._source_cleanup_progress_seq
            )
        except BaseException as exc:
            if self._source_cleanup_progress_failure is None:
                self._source_cleanup_progress_failure = exc


class ClientToServerDirectoryBackend(_ClientSourceBackend):
    """Stream one held Client manifest into an authorized Server workspace."""

    def __init__(
        self,
        *,
        transfer_manager: TransferManager,
        operation_lease: TransferLease,
        workspace_fs: WorkspaceFS,
        workspace_service: WorkspaceService,
        source: DeviceDirectoryJobController,
        source_root: str,
        destination: TransferPathTicket,
        manifest: DirectoryManifest,
    ) -> None:
        super().__init__(manifest, source=source, source_root=source_root)
        _validate_operation(source.directory_operation_id)
        if source.user_id != destination.user_id:
            raise ValueError("directory endpoints must belong to one authorized user")
        self._transfers = transfer_manager
        self._operation_lease = operation_lease
        self._fs = workspace_fs
        self._workspace = workspace_service
        self._destination_ticket = destination
        self._plan: DirectoryDestinationPlan | None = None
        self._reservation: DirectoryQuotaReservation | None = None
        self._destination_lease: SubtreeLease | None = None
        self._destination_progress = _RealProgressRelay(
            self._forward_server_destination_progress
        )

    async def preflight(self, manifest: DirectoryManifest) -> None:
        self._validate_manifest(manifest)
        _require_client_manifest(manifest)
        plan = await self._fs.preflight_directory_destination(
            self._destination_ticket.target,
            self._destination_ticket.relative_path,
            manifest,
            on_progress=self._destination_progress.advance,
        )
        await self._destination_progress.flush()
        self._plan = plan
        self._reservation = await self._fs.reserve_directory_quota(
            self._destination_ticket.target,
            owner=self._source.directory_operation_id,
            total_bytes=manifest.total_bytes,
            quota_bytes=self._destination_ticket.quota_bytes,
            on_progress=self._destination_progress.advance,
        )
        await self._destination_progress.flush()
        await self._workspace.validate_client_directory_skill_manifests(
            self._destination_ticket,
            manifest,
            plan,
            validate_source=self._validate_staged_skill,
        )

    async def prepare_destination(self, mark_issued: Callable[[], None]) -> None:
        del mark_issued
        plan = self._require_plan()
        self._require_reservation()
        self._destination_lease = await self._fs.acquire_subtree_lease(
            self._destination_ticket.target,
            self._destination_ticket.relative_path,
            owner=self._source.directory_operation_id,
        )
        current = await self._fs.preflight_directory_destination(
            self._destination_ticket.target,
            self._destination_ticket.relative_path,
            self._manifest,
            on_progress=self._destination_progress.advance,
        )
        await self._destination_progress.flush()
        if current != plan:
            raise TransferIntegrityError("directory destination plan changed before copy")

    async def copy_child(
        self,
        entry: DirectoryManifestEntry,
        slot_id: UUID,
        mark_issued: Callable[[], None],
    ) -> DirectoryChildResult:
        self._validate_child_order(entry)
        plan = self._require_plan()
        reservation = self._require_reservation()
        if self._destination_lease is None:
            raise TransferIntegrityError("directory destination was not prepared")
        await self._authorize_source_child(entry, slot_id)
        destination_path = plan.mapped_paths[self._next_child]
        committed: DirectoryChildResult | None = None
        temporary_object: str | None = None

        async def sink_factory(begin: TransferBeginFrame) -> ObjectUpload:
            nonlocal temporary_object
            _validate_client_begin(
                begin,
                slot_id=slot_id,
                src_path=_join_path(self._source_root, entry.relative_path),
                dst_path=destination_path,
                entry=entry,
            )
            sink, temporary_object = await self._fs.begin_directory_child_upload(
                reservation,
                destination_path,
                size=entry.size,
            )
            return sink

        async def commit_sink(
            _sink: object,
            _begin: TransferBeginFrame,
            size: int,
            sha256: str,
        ) -> TransferCommitResult:
            nonlocal committed
            if temporary_object is None or size != entry.size:
                raise TransferIntegrityError("directory child upload metadata is invalid")
            try:
                metadata = await self._fs.commit_directory_child_upload(
                    reservation,
                    destination_path,
                    temporary_object,
                    size=size,
                    on_issued=mark_issued,
                )
            except UploadCommittedAfterCancellation as exc:
                if exc.metadata is None:
                    raise
                committed = _child_from_metadata(entry, sha256, exc.metadata)
                return TransferCommitResult(
                    etag=committed.destination_fingerprint,
                    cancel_after_commit=True,
                )
            committed = _child_from_metadata(entry, sha256, metadata)
            return TransferCommitResult(etag=committed.destination_fingerprint)

        try:
            result = await self._transfers.start_client_to_server_admitted(
                handle=self._source.route.handle,
                route=self._source.route,
                operation_lease=self._operation_lease,
                slot_id=slot_id,
                user_id=self._source.user_id,
                src_path=_join_path(self._source_root, entry.relative_path),
                dst_path=destination_path,
                sink_factory=sink_factory,
                commit_sink=commit_sink,
                on_issued=None,
            )
        except TransferCommittedAfterCancellation as exc:
            if committed is None:
                raise TransferIntegrityError(
                    "directory child cancellation omitted its local commit"
                ) from None
            _validate_transfer_result_against_child(exc.result, committed)
            self._source_child_settled()
            child = self._record_result(entry, exc.result)
            raise DirectoryChildCommittedAfterCancellation(child) from None
        except asyncio.CancelledError:
            if committed is not None:
                self._source_child_settled()
                self._committed.append(committed)
                self._next_child += 1
                raise DirectoryChildCommittedAfterCancellation(committed) from None
            raise
        self._source_child_settled()
        if committed is None:
            raise TransferIntegrityError("directory child result mismatched its commit")
        _validate_transfer_result_against_child(result, committed)
        return self._record_result(entry, result)

    async def finalize_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> None:
        self._validate_committed(committed)
        plan = self._require_plan()
        await self._fs.verify_directory_destination(
            plan,
            tuple(
                DirectoryCommittedFile(
                    plan.mapped_paths[index],
                    child.verified_size,
                    child.destination_fingerprint,
                )
                for index, child in enumerate(committed)
            ),
            owner=self._source.directory_operation_id,
            on_progress=self._destination_progress.advance,
        )
        await self._destination_progress.flush()
        self._workspace.directory_transfer_committed(self._destination_ticket)

    async def cleanup_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> bool:
        del committed
        plan = self._plan
        if plan is None:
            return True
        complete = True
        for index in range(len(self._committed) - 1, -1, -1):
            child = self._committed[index]
            try:
                status = await self._fs.conditional_delete_file(
                    self._destination_ticket.target,
                    plan.mapped_paths[index],
                    expected_etag=child.destination_fingerprint,
                    subtree_owner=self._source.directory_operation_id,
                )
            except Exception:
                complete = False
            else:
                complete = complete and status in {"deleted", "missing"}
        try:
            absent = await self._fs.directory_root_is_absent(
                self._destination_ticket.target,
                self._destination_ticket.relative_path,
                owner=self._source.directory_operation_id,
            )
        except Exception:
            absent = False
        return complete and absent

    async def cleanup_source(self, manifest: DirectoryManifest) -> tuple[str, ...]:
        self._validate_manifest(manifest)
        return await self._cleanup_client_source()

    async def release(self) -> None:
        task = self._release_task
        if task is None:
            task = asyncio.create_task(self._release_once())
            self._release_task = task
        await await_future_cancellation_safe(task)

    async def _release_once(self) -> None:
        failure = await _release_call(self._destination_progress.flush)
        failure = await _release_call(self._release_client_source, failure)
        if self._reservation is not None:
            failure = await _release_call(self._reservation.release, failure)
        if self._destination_lease is not None:
            failure = await _release_call(self._destination_lease.release, failure)
        if failure is not None:
            raise failure

    async def _forward_server_destination_progress(self, progress_seq: int) -> None:
        await self._source.get_source_status(outer_progress_seq=progress_seq)

    async def _validate_staged_skill(
        self,
        entry: DirectoryManifestEntry,
        mapped_path: str,
    ) -> None:
        slot_id = new_uuid7()
        await self._authorize_source_child(entry, slot_id)
        temporary_object: str | None = None

        async def sink_factory(begin: TransferBeginFrame) -> ObjectUpload:
            nonlocal temporary_object
            _validate_client_begin(
                begin,
                slot_id=slot_id,
                src_path=_join_path(self._source_root, entry.relative_path),
                dst_path=mapped_path,
                entry=entry,
            )
            sink, temporary_object = self._fs.begin_directory_validation_staging(size=entry.size)
            return sink

        async def validate_sink(
            _sink: object,
            _begin: TransferBeginFrame,
            size: int,
            sha256: str,
        ) -> TransferCommitResult:
            if temporary_object is None or size != entry.size or len(sha256) != 64:
                raise TransferIntegrityError("staged Skill metadata is invalid")
            await self._workspace.validate_staged_directory_skill_manifest(
                mapped_path,
                temporary_object,
                expected_size=entry.size,
            )
            return TransferCommitResult(etag=sha256)

        failure: BaseException | None = None
        try:
            await self._transfers.start_client_to_server_admitted(
                handle=self._source.route.handle,
                route=self._source.route,
                operation_lease=self._operation_lease,
                slot_id=slot_id,
                user_id=self._source.user_id,
                src_path=_join_path(self._source_root, entry.relative_path),
                dst_path=mapped_path,
                sink_factory=sink_factory,
                commit_sink=validate_sink,
                on_issued=None,
            )
            self._source_child_settled()
        except BaseException as exc:
            failure = exc
        if temporary_object is not None:
            cleanup = asyncio.create_task(
                self._fs.delete_directory_validation_staging(temporary_object)
            )
            try:
                await await_future_cancellation_safe(cleanup)
            except BaseException as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure

    def _require_plan(self) -> DirectoryDestinationPlan:
        if self._plan is None:
            raise TransferIntegrityError("directory destination was not preflighted")
        return self._plan

    def _require_reservation(self) -> DirectoryQuotaReservation:
        if self._reservation is None:
            raise TransferIntegrityError("directory quota was not reserved")
        return self._reservation


class ClientToClientDirectoryBackend(_ClientSourceBackend, _ClientDestinationBackend):
    """Relay a held Client manifest into a distinct captured Client generation."""

    def __init__(
        self,
        *,
        transfer_manager: TransferManager,
        operation_lease: TransferLease,
        source: DeviceDirectoryJobController,
        source_root: str,
        destination: DeviceDirectoryJobController,
        destination_root: str,
        manifest: DirectoryManifest,
    ) -> None:
        _ClientSourceBackend.__init__(
            self,
            manifest,
            source=source,
            source_root=source_root,
        )
        self._destination = destination
        self._destination_root = destination_root
        self._destination_started = False
        self._destination_finalized = False
        self._destination_terminal = False
        _validate_operation(source.directory_operation_id)
        if (
            source.directory_operation_id != destination.directory_operation_id
            or source.user_id != destination.user_id
        ):
            raise ValueError("directory controllers must share one operation and user")
        if source.route.handle.device_id == destination.route.handle.device_id:
            raise ValueError("cross-site Client directory endpoints must be distinct")
        self._transfers = transfer_manager
        self._operation_lease = operation_lease

    async def preflight(self, manifest: DirectoryManifest) -> None:
        self._validate_manifest(manifest)
        _require_client_manifest(manifest)
        await self._preflight_client_destination()

    async def prepare_destination(self, mark_issued: Callable[[], None]) -> None:
        await self._prepare_client_destination(mark_issued)

    async def copy_child(
        self,
        entry: DirectoryManifestEntry,
        slot_id: UUID,
        mark_issued: Callable[[], None],
    ) -> DirectoryChildResult:
        self._validate_child_order(entry)
        await self._destination.authorize_destination_child(
            slot_id,
            entry.relative_path,
        )
        await self._authorize_source_child(entry, slot_id)
        try:
            result = await self._transfers.start_client_to_client_admitted(
                source_route=self._source.route,
                destination_route=self._destination.route,
                operation_lease=self._operation_lease,
                slot_id=slot_id,
                user_id=self._source.user_id,
                src_path=_join_path(self._source_root, entry.relative_path),
                dst_path=_join_path(self._destination_root, entry.relative_path),
                expected_source_size=entry.size,
                expected_source_fingerprint=entry.fingerprint,
                on_issued=mark_issued,
            )
        except TransferCommittedAfterCancellation as exc:
            self._source_child_settled()
            child = self._record_result(entry, exc.result)
            raise DirectoryChildCommittedAfterCancellation(child) from None
        self._source_child_settled()
        return self._record_result(entry, result)

    async def _forward_destination_progress(self, progress_seq: int) -> None:
        await self._source.get_source_status(outer_progress_seq=progress_seq)

    async def _forward_source_progress(self, progress_seq: int) -> None:
        await self._destination.get_destination_status(outer_progress_seq=progress_seq)

    def _destination_progress_callback(
        self,
    ) -> Callable[[int], Awaitable[None]] | None:
        return self._forward_destination_progress

    def _source_progress_callback(self) -> Callable[[int], Awaitable[None]] | None:
        return self._forward_source_progress

    async def finalize_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> None:
        await self._finalize_client_destination(committed)

    async def cleanup_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> bool:
        del committed
        return await self._cleanup_client_destination()

    async def cleanup_source(self, manifest: DirectoryManifest) -> tuple[str, ...]:
        self._validate_manifest(manifest)
        return await self._cleanup_client_source()

    async def release(self) -> None:
        task = self._release_task
        if task is None:
            task = asyncio.create_task(self._release_once())
            self._release_task = task
        await await_future_cancellation_safe(task)

    async def _release_once(self) -> None:
        failure = await _release_call(self._release_client_source)
        failure = await _release_call(self._release_client_destination, failure)
        if failure is not None:
            raise failure


async def _cleanup_server_source(
    workspace_fs: WorkspaceFS,
    source: TransferPathTicket,
    manifest: DirectoryManifest,
    *,
    owner: UUID,
    on_progress: Callable[[], Awaitable[None]] | None = None,
) -> tuple[str, ...]:
    changed = False
    incomplete = False
    for entry in manifest.entries:
        try:
            status = await workspace_fs.conditional_delete_file(
                source.target,
                _join_path(source.relative_path, entry.relative_path),
                expected_etag=entry.fingerprint,
                subtree_owner=owner,
            )
        except Exception:
            incomplete = True
        else:
            if status == "mismatch":
                changed = True
                incomplete = True
        if on_progress is not None:
            await on_progress()
    try:
        absent = await workspace_fs.directory_root_is_absent(
            source.target,
            source.relative_path,
            owner=owner,
        )
    except Exception:
        incomplete = True
    else:
        if not absent:
            incomplete = True
    if on_progress is not None:
        await on_progress()
    warnings: list[str] = []
    if changed:
        warnings.append("source_changed_after_copy")
    if incomplete:
        warnings.append("source_cleanup_incomplete")
    return tuple(warnings)


def _validate_client_begin(
    begin: TransferBeginFrame,
    *,
    slot_id: UUID,
    src_path: str,
    dst_path: str,
    entry: DirectoryManifestEntry,
) -> None:
    if (
        begin.id != slot_id
        or begin.src_path != src_path
        or begin.dst_path != dst_path
        or begin.total_bytes != entry.size
        or begin.etag != entry.fingerprint
    ):
        raise _source_changed()


def _child_from_metadata(
    entry: DirectoryManifestEntry,
    sha256: str,
    metadata: FileMetadata,
) -> DirectoryChildResult:
    if not metadata.created or metadata.size != entry.size:
        raise TransferIntegrityError("directory destination commit metadata is invalid")
    return DirectoryChildResult(
        relative_path=entry.relative_path,
        verified_size=entry.size,
        verified_sha256=sha256,
        destination_fingerprint=metadata.etag,
    )


def _validate_transfer_result_against_child(
    result: TransferResult,
    child: DirectoryChildResult,
) -> None:
    if (
        result.bytes_transferred != child.verified_size
        or result.sha256 != child.verified_sha256
        or result.etag != child.destination_fingerprint
        or result.created is not True
    ):
        raise TransferIntegrityError("directory child result mismatched its commit")


def _require_device_state(status: object, expected: str) -> None:
    state = getattr(status, "state", None)
    if state == expected:
        return
    if state == "outcome_unknown":
        raise DeviceOutcomeUnknownError("Client directory outcome is unknown")
    if state == "failed":
        error = getattr(status, "terminal_error", None)
        code = getattr(error, "code", None)
        if not isinstance(code, str):
            raise TransferIntegrityError("Client directory failure omitted its error code")
        if code == ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value:
            raise DeviceOutcomeUnknownError("Client directory outcome is unknown")
        raise TransferError(code)
    raise TransferIntegrityError("Client directory status is invalid")


async def _release_call(
    release: Callable[[], Awaitable[None]],
    prior: BaseException | None = None,
) -> BaseException | None:
    try:
        await release()
    except BaseException as exc:
        return prior or exc
    return prior


def _require_server_manifest(manifest: DirectoryManifest) -> None:
    if manifest.root_identity is not None or any(
        directory.identity is not None for directory in manifest.directories
    ):
        raise TransferIntegrityError("Server directory manifest contains filesystem identities")


def _require_client_manifest(manifest: DirectoryManifest) -> None:
    if manifest.root_identity is None or any(
        directory.identity is None for directory in manifest.directories
    ):
        raise TransferIntegrityError("Client directory manifest is missing filesystem identities")


def _validate_operation(operation_id: UUID) -> None:
    if operation_id.version != 7:
        raise ValueError("directory operation ID must be UUIDv7")


def _join_path(root: str, relative_path: str) -> str:
    return f"{root.rstrip('/')}/{relative_path}" if root else relative_path


def _source_changed() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_FILE_CHANGED,
        "Workspace source changed after its directory manifest was created",
    )
