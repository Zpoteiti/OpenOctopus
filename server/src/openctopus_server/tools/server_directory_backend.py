from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from uuid import UUID

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.devices.transfer import TransferIntegrityError
from openctopus_server.directory_contract import DirectoryManifest, DirectoryManifestEntry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.tools.directory_transfer import (
    DirectoryChildCommittedAfterCancellation,
    DirectoryChildResult,
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


class ServerDirectoryTransferBackend:
    """Pure Server workspace adapter for the directory transfer coordinator."""

    def __init__(
        self,
        *,
        workspace_fs: WorkspaceFS,
        workspace_service: WorkspaceService,
        source: TransferPathTicket,
        destination: TransferPathTicket,
        manifest: DirectoryManifest,
        operation_id: UUID,
    ) -> None:
        if operation_id.version != 7:
            raise ValueError("directory operation ID must be UUIDv7")
        if source.user_id != destination.user_id:
            raise ValueError("directory endpoints must belong to one authorized user")
        self._fs = workspace_fs
        self._workspace = workspace_service
        self._source = source
        self._destination = destination
        self._manifest = manifest
        self._operation_id = operation_id
        self._plan: DirectoryDestinationPlan | None = None
        self._reservation: DirectoryQuotaReservation | None = None
        self._subtree_leases: list[SubtreeLease] = []
        self._committed: list[DirectoryChildResult] = []
        self._next_child = 0
        self._release_task: asyncio.Task[None] | None = None

    async def preflight(self, manifest: DirectoryManifest) -> None:
        if manifest != self._manifest:
            raise TransferIntegrityError("directory manifest changed before preflight")
        if manifest.root_identity is not None or any(
            directory.identity is not None for directory in manifest.directories
        ):
            raise TransferIntegrityError("Server directory manifest contains filesystem identities")
        _validate_server_overlap(self._source, self._destination)
        for entry in manifest.entries:
            if len(_join_path(self._source.relative_path, entry.relative_path)) > 4096:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_INVALID_REQUEST,
                    "Directory source path is too long",
                )

        plan = await self._fs.preflight_directory_destination(
            self._destination.target,
            self._destination.relative_path,
            manifest,
        )
        self._plan = plan
        self._reservation = await self._fs.reserve_directory_quota(
            self._destination.target,
            owner=self._operation_id,
            total_bytes=manifest.total_bytes,
            quota_bytes=self._destination.quota_bytes,
        )
        await self._workspace.validate_directory_skill_manifests(
            self._destination,
            manifest,
            plan,
            open_source=self._open_source,
        )

    async def prepare_destination(self, mark_issued: Callable[[], None]) -> None:
        del mark_issued
        plan = self._require_plan()
        self._require_reservation()
        scopes = (
            (self._source.target, self._source.relative_path),
            (self._destination.target, self._destination.relative_path),
        )
        ordered = sorted(
            scopes,
            key=lambda scope: (
                scope[0].kind,
                scope[0].id.int,
                scope[1].encode("utf-8"),
            ),
        )
        for target, path in ordered:
            lease = await self._fs.acquire_subtree_lease(
                target,
                path,
                owner=self._operation_id,
            )
            self._subtree_leases.append(lease)

        current = await self._fs.preflight_directory_destination(
            self._destination.target,
            self._destination.relative_path,
            self._manifest,
        )
        if current != plan:
            raise TransferIntegrityError("directory destination plan changed before copy")

    async def copy_child(
        self,
        entry: DirectoryManifestEntry,
        slot_id: UUID,
        mark_issued: Callable[[], None],
    ) -> DirectoryChildResult:
        if slot_id.version != 7:
            raise TransferIntegrityError("directory child ID is not UUIDv7")
        if self._next_child >= len(self._manifest.entries):
            raise TransferIntegrityError("directory coordinator produced an extra child")
        if entry != self._manifest.entries[self._next_child]:
            raise TransferIntegrityError("directory children are not in manifest order")
        plan = self._require_plan()
        reservation = self._require_reservation()
        if len(self._subtree_leases) != 2:
            raise TransferIntegrityError("directory destination was not prepared")

        source = await self._open_source(entry)
        sink: ObjectUpload | None = None
        published = False
        try:
            if source.size != entry.size or source.etag != entry.fingerprint:
                raise _source_changed()
            destination_path = plan.mapped_paths[self._next_child]
            sink, temporary_object = await self._fs.begin_directory_child_upload(
                reservation,
                destination_path,
                size=entry.size,
            )
            digest = hashlib.sha256()
            transferred = 0
            while chunk := await source.read():
                transferred += len(chunk)
                if transferred > entry.size:
                    raise _source_changed()
                digest.update(chunk)
                await sink.write(chunk)
            if transferred != entry.size:
                raise _source_changed()
            await sink.finish()
            try:
                metadata = await self._fs.commit_directory_child_upload(
                    reservation,
                    destination_path,
                    temporary_object,
                    size=transferred,
                    on_issued=mark_issued,
                )
            except UploadCommittedAfterCancellation as exc:
                if exc.metadata is None:
                    raise
                result = self._record_commit(
                    entry,
                    destination_path,
                    digest.hexdigest(),
                    exc.metadata,
                )
                published = True
                raise DirectoryChildCommittedAfterCancellation(result) from None
            result = self._record_commit(
                entry,
                destination_path,
                digest.hexdigest(),
                metadata,
            )
            published = True
            return result
        except BaseException:
            if sink is not None and not published:
                abort = asyncio.create_task(sink.abort())
                await await_future_cancellation_safe(abort)
            raise
        finally:
            close = asyncio.create_task(source.aclose())
            await await_future_cancellation_safe(close)

    async def finalize_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> None:
        if committed != tuple(self._committed) or len(committed) != len(
            self._manifest.entries
        ):
            raise TransferIntegrityError("directory committed records are incomplete")
        await self._fs.verify_directory_destination(
            self._require_plan(),
            tuple(
                DirectoryCommittedFile(
                    self._require_plan().mapped_paths[index],
                    item.verified_size,
                    item.destination_fingerprint,
                )
                for index, item in enumerate(committed)
            ),
            owner=self._operation_id,
        )
        self._workspace.directory_transfer_committed(self._destination)

    async def cleanup_destination(
        self,
        committed: tuple[DirectoryChildResult, ...],
    ) -> bool:
        del committed
        plan = self._plan
        complete = True
        if plan is None:
            return True
        for index in range(len(self._committed) - 1, -1, -1):
            child = self._committed[index]
            try:
                status = await self._fs.conditional_delete_file(
                    self._destination.target,
                    plan.mapped_paths[index],
                    expected_etag=child.destination_fingerprint,
                    subtree_owner=self._operation_id,
                )
            except Exception:
                complete = False
            else:
                complete = complete and status in {"deleted", "missing"}
        try:
            root_absent = await self._fs.directory_root_is_absent(
                self._destination.target,
                self._destination.relative_path,
                owner=self._operation_id,
            )
        except Exception:
            root_absent = False
        return complete and root_absent

    async def cleanup_source(self, manifest: DirectoryManifest) -> tuple[str, ...]:
        if manifest != self._manifest:
            return ("source_cleanup_incomplete",)
        changed = False
        incomplete = False
        for entry in manifest.entries:
            try:
                status = await self._fs.conditional_delete_file(
                    self._source.target,
                    _join_path(self._source.relative_path, entry.relative_path),
                    expected_etag=entry.fingerprint,
                    subtree_owner=self._operation_id,
                )
            except Exception:
                incomplete = True
                continue
            if status == "mismatch":
                changed = True
                incomplete = True
        try:
            if not await self._fs.directory_root_is_absent(
                self._source.target,
                self._source.relative_path,
                owner=self._operation_id,
            ):
                incomplete = True
        except Exception:
            incomplete = True
        warnings: list[str] = []
        if changed:
            warnings.append("source_changed_after_copy")
        if incomplete:
            warnings.append("source_cleanup_incomplete")
        return tuple(warnings)

    async def release(self) -> None:
        task = self._release_task
        if task is None:
            task = asyncio.create_task(self._release_resources())
            self._release_task = task
        await await_future_cancellation_safe(task)

    async def _open_source(self, entry: DirectoryManifestEntry) -> ObjectStream:
        return await self._fs.open_stream(
            self._source.target,
            _join_path(self._source.relative_path, entry.relative_path),
        )

    def _record_commit(
        self,
        entry: DirectoryManifestEntry,
        destination_path: str,
        sha256: str,
        metadata: FileMetadata,
    ) -> DirectoryChildResult:
        plan = self._require_plan()
        if (
            not metadata.created
            or metadata.size != entry.size
            or destination_path != plan.mapped_paths[self._next_child]
        ):
            raise TransferIntegrityError("directory destination commit metadata is invalid")
        result = DirectoryChildResult(
            relative_path=entry.relative_path,
            verified_size=entry.size,
            verified_sha256=sha256,
            destination_fingerprint=metadata.etag,
        )
        self._committed.append(result)
        self._next_child += 1
        return result

    def _require_plan(self) -> DirectoryDestinationPlan:
        if self._plan is None:
            raise TransferIntegrityError("directory destination was not preflighted")
        return self._plan

    def _require_reservation(self) -> DirectoryQuotaReservation:
        if self._reservation is None:
            raise TransferIntegrityError("directory quota was not reserved")
        return self._reservation

    async def _release_resources(self) -> None:
        failure: BaseException | None = None
        if self._reservation is not None:
            try:
                await self._reservation.release()
            except BaseException as exc:
                failure = exc
        for lease in reversed(self._subtree_leases):
            try:
                await lease.release()
            except BaseException as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure


def _join_path(root: str, relative_path: str) -> str:
    return f"{root}/{relative_path}" if root else relative_path


def _validate_server_overlap(
    source: TransferPathTicket,
    destination: TransferPathTicket,
) -> None:
    if source.target != destination.target:
        return
    source_parts = _path_parts(source.relative_path)
    destination_parts = _path_parts(destination.relative_path)
    if (
        source_parts[: len(destination_parts)] == destination_parts
        or destination_parts[: len(source_parts)] == source_parts
    ):
        raise WorkspaceError(
            ErrorCode.WORKSPACE_INVALID_REQUEST,
            "Directory source and destination overlap",
        )


def _path_parts(path: str) -> tuple[str, ...]:
    return tuple(path.split("/")) if path else ()


def _source_changed() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_FILE_CHANGED,
        "Workspace source changed after its directory manifest was created",
    )
