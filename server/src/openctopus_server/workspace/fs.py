from __future__ import annotations

import asyncio
import hashlib
import secrets
from collections.abc import AsyncIterator, Callable, Hashable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import PurePosixPath
from typing import Annotated, Literal
from uuid import UUID

from fastapi import Depends

from openctopus_server.admission import AdmissionTimeoutError, KeyedAdmission
from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.config import get_settings
from openctopus_server.directory_contract import (
    MAX_DIRECTORY_ENTRIES as MAX_TRANSFER_DIRECTORY_ENTRIES,
)
from openctopus_server.directory_contract import (
    MAX_DIRECTORY_INTEGER,
    MAX_DIRECTORY_MANIFEST_BYTES,
    DirectoryContractError,
    DirectoryManifest,
    DirectoryManifestDirectory,
    DirectoryManifestEntry,
    canonical_json_bytes,
    create_directory_manifest,
    destination_collision_keys,
    directory_manifest_sha256,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.locks import (
    KeyedLockManager,
    SubtreeLease,
    SubtreeLeaseManager,
)
from openctopus_server.workspace.search import MAX_SCAN_OBJECTS, NOISE_DIRECTORIES, SearchObject
from openctopus_server.workspace.storage import (
    DirectoryObject,
    ObjectMetadata,
    ObjectStorage,
    ObjectStream,
    ObjectUpload,
    StoredObject,
    get_object_storage,
)

MAX_EDIT_BYTES = 8 * 1024 * 1024
MAX_READ_BYTES = 8 * 1024 * 1024
MAX_DIRECTORY_ENTRIES = 1000


@dataclass(frozen=True)
class FileMetadata:
    size: int
    etag: str
    created: bool = False


class UploadCommittedAfterCancellation(asyncio.CancelledError):
    """The destination was published before caller cancellation took effect."""


@dataclass(frozen=True)
class DirectoryEntry:
    path: str
    is_directory: bool
    size: int | None


@dataclass(frozen=True)
class DirectoryPage:
    items: tuple[DirectoryEntry, ...]
    next_offset: int | None
    truncated: bool


@dataclass(frozen=True)
class FileTransform:
    target: WorkspaceTarget
    relative_path: str
    quota_bytes: int
    transform: Callable[[bytes | None], bytes]


@dataclass(frozen=True)
class WorkspaceTarget:
    kind: Literal["personal", "shared"]
    id: UUID

    @classmethod
    def personal(cls, user_id: UUID) -> WorkspaceTarget:
        return cls(kind="personal", id=user_id)

    @classmethod
    def shared(cls, workspace_id: UUID) -> WorkspaceTarget:
        return cls(kind="shared", id=workspace_id)


@dataclass(frozen=True, slots=True)
class ServerFileSourceProbe:
    size: int
    fingerprint: str
    kind: Literal["file"] = "file"


@dataclass(frozen=True, slots=True)
class ServerDirectorySourceProbe:
    manifest: DirectoryManifest
    kind: Literal["directory"] = "directory"


type ServerSourceProbe = ServerFileSourceProbe | ServerDirectorySourceProbe


@dataclass(frozen=True, slots=True)
class DirectoryDestinationPlan:
    target: WorkspaceTarget
    destination_root: str
    manifest_sha256: str
    mapped_paths: tuple[str, ...]


@dataclass(eq=False, slots=True)
class _DirectoryQuotaRecord:
    target: WorkspaceTarget
    owner: Hashable
    quota_bytes: int
    remaining_bytes: int


class DirectoryQuotaReservation:
    """One idempotently released aggregate directory quota reservation."""

    def __init__(self, workspace_fs: WorkspaceFS, record: _DirectoryQuotaRecord) -> None:
        self._workspace_fs = workspace_fs
        self._record = record
        self._release_task: asyncio.Task[None] | None = None

    @property
    def target(self) -> WorkspaceTarget:
        return self._record.target

    @property
    def owner(self) -> Hashable:
        return self._record.owner

    @property
    def quota_bytes(self) -> int:
        return self._record.quota_bytes

    @property
    def remaining_bytes(self) -> int:
        return self._record.remaining_bytes

    async def release(self) -> None:
        task = self._release_task
        if task is None:
            task = asyncio.create_task(
                self._workspace_fs._release_directory_quota_reservation(self._record)
            )
            self._release_task = task
        await await_future_cancellation_safe(task)


class WorkspaceFS:
    """Quota-aware coordination for already-authorized workspace identities."""

    def __init__(
        self,
        storage: ObjectStorage,
        *,
        materialization_concurrency: int = 4,
        heavy_operation_concurrency: int = 4,
        file_operation_concurrency: int = 4,
        server_transfer_max_concurrency_per_user: int = 2,
        server_transfer_queue_timeout_seconds: float = 5.0,
    ) -> None:
        self._storage = storage
        self._materializations = asyncio.Semaphore(materialization_concurrency)
        self._heavy_operations = asyncio.Semaphore(heavy_operation_concurrency)
        self._file_operations = asyncio.Semaphore(file_operation_concurrency)
        connection_limit = getattr(storage, "max_connections", None)
        if not isinstance(connection_limit, int) or connection_limit < 2:
            connection_limit = 8
        server_transfer_capacity = max(1, connection_limit // 2)
        self._server_transfers = KeyedAdmission(
            global_limit=server_transfer_capacity,
            per_key_limit=min(
                server_transfer_max_concurrency_per_user,
                server_transfer_capacity,
            ),
            timeout_seconds=server_transfer_queue_timeout_seconds,
        )
        self._transfer_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._mutation_locks = KeyedLockManager()
        self._subtree_leases = SubtreeLeaseManager()
        self._directory_quota_reservations: dict[
            tuple[WorkspaceTarget, Hashable], _DirectoryQuotaRecord
        ] = {}
        self._retired_targets: set[WorkspaceTarget] = set()

    @property
    def mutation_lock_count(self) -> int:
        return self._mutation_locks.entry_count

    @property
    def directory_quota_reservation_count(self) -> int:
        return len(self._directory_quota_reservations)

    async def acquire_subtree_lease(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        owner: Hashable,
        wait: bool = True,
    ) -> SubtreeLease:
        """Reserve one canonical workspace subtree for a long-lived operation."""
        _, prefix = _subtree_scope(target, relative_path)
        lease = await self._subtree_leases.acquire(
            target=target,
            prefix=prefix,
            owner=owner,
            wait=wait,
        )
        try:
            self._ensure_active(target)
        except BaseException:
            await lease.release()
            raise
        return lease

    @asynccontextmanager
    async def _hold_mutations(
        self,
        scopes: tuple[tuple[WorkspaceTarget, tuple[str, ...]], ...],
        *,
        owner: Hashable | None = None,
    ) -> AsyncIterator[None]:
        lease_owner = object() if owner is None else owner
        ordered = sorted(
            set(scopes),
            key=lambda scope: (
                scope[0].kind,
                scope[0].id.int,
                len(scope[1]),
                scope[1],
            ),
        )
        leases: list[SubtreeLease] = []
        try:
            for target, prefix in ordered:
                leases.append(
                    await self._subtree_leases.acquire(
                        target=target,
                        prefix=prefix,
                        owner=lease_owner,
                    )
                )
            async with self._mutation_locks.hold_many(
                tuple(target for target, _ in ordered)
            ):
                yield
        finally:
            cleanup = asyncio.create_task(_release_subtree_leases(tuple(reversed(leases))))
            await await_future_cancellation_safe(cleanup)

    async def probe_directory_source(
        self,
        target: WorkspaceTarget,
        relative_path: str,
    ) -> ServerSourceProbe:
        """Probe exact object and prefix independently before selecting file or directory."""
        self._ensure_active(target)
        object_name = _object_key(target, relative_path)
        exact = await _stat_optional(self._storage, object_name)
        prefix = f"{object_name}/"
        prefix_page = await self._storage.list_page(prefix, limit=1)
        has_prefix = bool(prefix_page.items)
        if exact is not None and has_prefix:
            raise _workspace_storage_shape_error()
        if exact is not None:
            return ServerFileSourceProbe(size=exact.size, fingerprint=exact.etag)
        if not has_prefix:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_NOT_FOUND,
                "Workspace source was not found",
            )
        return ServerDirectorySourceProbe(
            manifest=await self._scan_server_directory_manifest(prefix)
        )

    async def _scan_server_directory_manifest(self, prefix: str) -> DirectoryManifest:
        entries: list[DirectoryManifestEntry] = []
        directory_paths: set[str] = set()
        entry_paths: set[str] = set()
        total_bytes = 0
        try:
            async for objects in _metadata_pages(self._storage, prefix):
                for item in objects:
                    if not item.object_name.startswith(prefix):
                        raise _workspace_storage_shape_error()
                    relative_path = item.object_name.removeprefix(prefix)
                    entry = DirectoryManifestEntry(
                        relative_path=relative_path,
                        size=item.size,
                        fingerprint=item.etag,
                    )
                    if entry.relative_path in entry_paths:
                        raise _workspace_storage_shape_error()
                    entry_paths.add(entry.relative_path)
                    entries.append(entry)
                    components = entry.relative_path.split("/")
                    directory_paths.update(
                        "/".join(components[:end]) for end in range(1, len(components))
                    )
                    total_bytes += entry.size
                    if (
                        len(entries) + len(directory_paths) > MAX_TRANSFER_DIRECTORY_ENTRIES
                        or total_bytes > MAX_DIRECTORY_INTEGER
                    ):
                        raise _workspace_directory_too_large()
        except WorkspaceError:
            raise
        except (DirectoryContractError, ValueError) as exc:
            raise _workspace_storage_shape_error() from exc

        if not entries:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace source directory changed while it was scanned",
            )
        try:
            directories = tuple(
                DirectoryManifestDirectory(relative_path=path, identity=None)
                for path in sorted(directory_paths, key=lambda value: value.encode("utf-8"))
            )
            sorted_entries = tuple(
                sorted(entries, key=lambda item: item.relative_path.encode("utf-8"))
            )
            digest = directory_manifest_sha256(None, directories, sorted_entries)
            encoded = canonical_json_bytes(
                {
                    "version": 1,
                    "root_identity": None,
                    "scanned_entries": len(directories) + len(sorted_entries),
                    "total_bytes": total_bytes,
                    "directories": directories,
                    "entries": sorted_entries,
                    "manifest_sha256": digest,
                }
            )
            if len(encoded) > MAX_DIRECTORY_MANIFEST_BYTES:
                raise _workspace_directory_too_large()
            return create_directory_manifest(
                root_identity=None,
                directories=directories,
                entries=sorted_entries,
            )
        except WorkspaceError:
            raise
        except (DirectoryContractError, ValueError) as exc:
            raise _workspace_storage_shape_error() from exc

    async def preflight_directory_destination(
        self,
        target: WorkspaceTarget,
        destination_root: str,
        manifest: DirectoryManifest,
    ) -> DirectoryDestinationPlan:
        self._ensure_active(target)
        root_object_name = _object_key(target, destination_root)
        try:
            destination_collision_keys(manifest, platform="linux")
        except DirectoryContractError as exc:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Directory destination paths conflict",
            ) from exc
        mapped_paths = tuple(
            _join_relative_path(destination_root, entry.relative_path)
            for entry in manifest.entries
        )
        if any(len(path) > 4096 for path in mapped_paths):
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Directory destination path is too long",
            )
        for path in mapped_paths:
            _object_key(target, path)

        for parent in _parent_object_names(_workspace_prefix(target), root_object_name):
            if await _stat_optional(self._storage, parent) is not None:
                raise WorkspaceError(
                    ErrorCode.TOOL_NOT_A_DIRECTORY,
                    "A workspace path parent is a file",
                )
        exact = await _stat_optional(self._storage, root_object_name)
        prefix_page = await self._storage.list_page(f"{root_object_name}/", limit=1)
        has_prefix = bool(prefix_page.items)
        if exact is not None and has_prefix:
            raise _workspace_storage_shape_error()
        if exact is not None or has_prefix:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace destination root already exists",
            )
        return DirectoryDestinationPlan(
            target=target,
            destination_root=destination_root,
            manifest_sha256=manifest.manifest_sha256,
            mapped_paths=mapped_paths,
        )

    async def reserve_directory_quota(
        self,
        target: WorkspaceTarget,
        *,
        owner: Hashable,
        total_bytes: int,
        quota_bytes: int,
    ) -> DirectoryQuotaReservation:
        if total_bytes < 0:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Directory size must not be negative",
            )
        record: _DirectoryQuotaRecord | None = None
        try:
            async with self._mutation_locks.hold(target):
                self._ensure_active(target)
                key = (target, owner)
                if key in self._directory_quota_reservations:
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_INVALID_REQUEST,
                        "Directory quota reservation already exists",
                    )
                usage = await self._authoritative_usage(target)
                reserved = self._reserved_directory_bytes(target)
                _validate_projected_quota(
                    usage=usage,
                    reserved=reserved,
                    operation_bytes=total_bytes,
                    quota_bytes=quota_bytes,
                )
                record = _DirectoryQuotaRecord(
                    target=target,
                    owner=owner,
                    quota_bytes=quota_bytes,
                    remaining_bytes=total_bytes,
                )
                self._directory_quota_reservations[key] = record
            return DirectoryQuotaReservation(self, record)
        except BaseException:
            if record is not None:
                cleanup = asyncio.create_task(self._release_directory_quota_reservation(record))
                await await_future_cancellation_safe(cleanup)
            raise

    async def begin_directory_child_upload(
        self,
        reservation: DirectoryQuotaReservation,
        relative_path: str,
        *,
        size: int,
    ) -> tuple[ObjectUpload, str]:
        if size < 0:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Transfer size must not be negative",
            )
        async with self._hold_mutations(
            (_subtree_scope(reservation.target, relative_path),),
            owner=reservation.owner,
        ):
            record = self._active_directory_quota_record(reservation)
            if size > record.remaining_bytes:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED,
                    "Directory child exceeds its remaining quota reservation",
                )
            self._ensure_active(record.target)
            await self._ensure_destination_absent(record.target, relative_path)
        temporary_object = f"_openoctopus-transfers/{secrets.token_hex(16)}"
        return self._storage.begin_upload(temporary_object, length=size), temporary_object

    async def commit_directory_child_upload(
        self,
        reservation: DirectoryQuotaReservation,
        relative_path: str,
        temporary_object: str,
        *,
        size: int,
        on_issued: Callable[[], None] | None = None,
    ) -> FileMetadata:
        return await self.commit_uploaded_object(
            reservation.target,
            relative_path,
            temporary_object,
            size=size,
            quota_bytes=reservation.quota_bytes,
            on_issued=on_issued,
            subtree_owner=reservation.owner,
            quota_reservation=reservation,
        )

    async def conditional_delete_file(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        expected_etag: str,
        subtree_owner: Hashable | None = None,
    ) -> Literal["deleted", "missing", "mismatch"]:
        object_name = _object_key(target, relative_path)
        async with self._hold_mutations(
            (_subtree_scope(target, relative_path),),
            owner=subtree_owner,
        ):
            self._ensure_active(target)
            existing = await _stat_optional(self._storage, object_name)
            if existing is None:
                prefix_page = await self._storage.list_page(f"{object_name}/", limit=1)
                return "mismatch" if prefix_page.items else "missing"
            if existing.etag != expected_etag:
                return "mismatch"
            await self._storage.delete(object_name)
            return "deleted"

    async def _authoritative_usage(self, target: WorkspaceTarget) -> int:
        usage = 0
        async for objects in _metadata_pages(self._storage, _workspace_prefix(target)):
            usage += sum(item.size for item in objects)
        return usage

    def _reserved_directory_bytes(self, target: WorkspaceTarget) -> int:
        return sum(
            record.remaining_bytes
            for record in self._directory_quota_reservations.values()
            if record.target == target
        )

    def _active_directory_quota_record(
        self,
        reservation: DirectoryQuotaReservation,
    ) -> _DirectoryQuotaRecord:
        record = reservation._record
        if self._directory_quota_reservations.get((record.target, record.owner)) is not record:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Directory quota reservation is no longer active",
            )
        return record

    async def _release_directory_quota_reservation(
        self,
        record: _DirectoryQuotaRecord,
    ) -> None:
        async with self._mutation_locks.hold(record.target):
            key = (record.target, record.owner)
            if self._directory_quota_reservations.get(key) is record:
                self._directory_quota_reservations.pop(key)

    async def stat(self, target: WorkspaceTarget, relative_path: str) -> FileMetadata:
        metadata = await self._storage.stat(_object_key(target, relative_path))
        return FileMetadata(size=metadata.size, etag=metadata.etag)

    @asynccontextmanager
    async def materialization_slot(self) -> AsyncIterator[None]:
        async with self._materializations:
            yield

    @asynccontextmanager
    async def file_operation_slot(self) -> AsyncIterator[None]:
        async with self._file_operations:
            yield

    @asynccontextmanager
    async def heavy_operation_slot(self) -> AsyncIterator[None]:
        async with self._heavy_operations:
            yield

    async def scan_objects(
        self,
        target: WorkspaceTarget,
        relative_path: str = "",
        *,
        scan_limit: int = MAX_SCAN_OBJECTS,
    ) -> tuple[tuple[SearchObject, ...], bool]:
        self._ensure_active(target)
        workspace_prefix = _workspace_prefix(target)
        normalized = relative_path.strip("/")
        prefix = f"{workspace_prefix}{normalized}/" if normalized else workspace_prefix
        items: list[SearchObject] = []
        start_after: str | None = None
        while len(items) <= scan_limit:
            page = await self._storage.list_page(prefix, start_after=start_after)
            for item in page.items:
                if len(items) > scan_limit:
                    break
                items.append(
                    SearchObject(
                        path=item.object_name.removeprefix(workspace_prefix),
                        size=item.size,
                        modified=item.modified,
                    )
                )
            if len(items) > scan_limit or page.next_start_after is None:
                break
            start_after = page.next_start_after
        if not items and normalized:
            object_name = f"{workspace_prefix}{normalized}"
            try:
                item = await self._storage.stat(object_name)
            except WorkspaceError as exc:
                if exc.code is ErrorCode.WORKSPACE_NOT_FOUND:
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_NOT_FOUND,
                        "Workspace path was not found",
                    ) from exc
                raise
            items.append(
                SearchObject(
                    path=normalized,
                    size=item.size,
                    modified=item.modified,
                )
            )
        return tuple(items[:scan_limit]), len(items) > scan_limit

    async def read_search_object(
        self,
        target: WorkspaceTarget,
        item: SearchObject,
        *,
        max_bytes: int,
    ) -> SearchObject:
        stored = await self._storage.read(
            _object_key(target, item.path),
            max_bytes=max_bytes,
        )
        if stored.truncated:
            return item
        return SearchObject(
            path=item.path,
            size=item.size,
            modified=item.modified,
            content=stored.data,
        )

    async def open_stream(
        self,
        target: WorkspaceTarget,
        relative_path: str,
    ) -> ObjectStream:
        self._ensure_active(target)
        await self._ensure_regular_file(target, relative_path)
        return await self._storage.open_stream(_object_key(target, relative_path))

    async def begin_transfer_upload(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        size: int,
        quota_bytes: int,
    ) -> tuple[ObjectUpload, str]:
        """Reserve a bounded temporary object for a no-overwrite transfer."""
        if size < 0:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Transfer size must not be negative",
            )
        async with self._mutation_locks.hold(target):
            self._ensure_active(target)
            await self._ensure_destination_absent(target, relative_path)
            await self._ensure_transfer_quota(target, size=size, quota_bytes=quota_bytes)
        temporary_object = f"_openoctopus-transfers/{secrets.token_hex(16)}"
        return self._storage.begin_upload(temporary_object, length=size), temporary_object

    async def transfer_server_to_server(
        self,
        source_target: WorkspaceTarget,
        source_path: str,
        destination_target: WorkspaceTarget,
        destination_path: str,
        *,
        user_id: UUID,
        quota_bytes: int,
        mode: str,
        on_issued: Callable[[], None] | None = None,
    ) -> tuple[int, str, tuple[str, ...]]:
        try:
            async with self._server_transfers.slot(user_id):
                return await self._transfer_server_to_server(
                    source_target,
                    source_path,
                    destination_target,
                    destination_path,
                    quota_bytes=quota_bytes,
                    mode=mode,
                    on_issued=on_issued,
                )
        except AdmissionTimeoutError as exc:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_TRANSFER_BUSY,
                "Workspace transfer capacity is busy; retry later",
            ) from exc

    async def _transfer_server_to_server(
        self,
        source_target: WorkspaceTarget,
        source_path: str,
        destination_target: WorkspaceTarget,
        destination_path: str,
        *,
        quota_bytes: int,
        mode: str,
        on_issued: Callable[[], None] | None,
    ) -> tuple[int, str, tuple[str, ...]]:
        """Stream one server workspace object through a temporary RustFS object."""
        if mode not in {"copy", "move"}:
            raise WorkspaceError(ErrorCode.WORKSPACE_INVALID_REQUEST, "Transfer mode is invalid")
        source = await self.open_stream(source_target, source_path)
        try:
            sink, temporary_object = await self.begin_transfer_upload(
                destination_target,
                destination_path,
                size=source.size,
                quota_bytes=quota_bytes,
            )
        except BaseException:
            await source.aclose()
            raise
        digest = hashlib.sha256()
        transferred = 0
        try:
            while chunk := await source.read():
                digest.update(chunk)
                transferred += len(chunk)
                await sink.write(chunk)
            await sink.finish()
            try:
                await self.commit_uploaded_object(
                    destination_target,
                    destination_path,
                    temporary_object,
                    size=transferred,
                    quota_bytes=quota_bytes,
                    on_issued=on_issued,
                )
            except UploadCommittedAfterCancellation:
                # Publication is the irreversible success point. Continue to
                # report its true result and, for a move, conditionally remove
                # the source instead of entering the pre-commit abort path.
                pass
        except BaseException:
            await sink.abort()
            raise
        finally:
            await source.aclose()

        warnings: list[str] = []
        if mode == "move":
            try:
                await self.delete_file(source_target, source_path, if_match=source.etag)
            except Exception:
                warnings.append("source_delete_failed")
        return transferred, digest.hexdigest(), tuple(warnings)

    async def _ensure_regular_file(
        self,
        target: WorkspaceTarget,
        relative_path: str,
    ) -> FileMetadata:
        object_name = _object_key(target, relative_path)
        try:
            metadata = await self._storage.stat(object_name)
        except WorkspaceError as exc:
            if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                raise
            if (await self._storage.list_page(f"{object_name}/", limit=1)).items:
                raise WorkspaceError(
                    ErrorCode.TOOL_IS_DIRECTORY,
                    "Workspace path is a directory",
                ) from exc
            raise
        return FileMetadata(size=metadata.size, etag=metadata.etag)

    async def _ensure_destination_absent(
        self,
        target: WorkspaceTarget,
        relative_path: str,
    ) -> None:
        object_name = _object_key(target, relative_path)
        try:
            await self._storage.stat(object_name)
        except WorkspaceError as exc:
            if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                raise
        else:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace file already exists",
            )
        if (await self._storage.list_page(f"{object_name}/", limit=1)).items:
            raise WorkspaceError(
                ErrorCode.TOOL_IS_DIRECTORY,
                "Workspace path is a directory",
            )

    async def _ensure_transfer_quota(
        self,
        target: WorkspaceTarget,
        *,
        size: int,
        quota_bytes: int,
    ) -> None:
        usage = await self._authoritative_usage(target)
        _validate_projected_quota(
            usage=usage,
            reserved=self._reserved_directory_bytes(target),
            operation_bytes=size,
            quota_bytes=quota_bytes,
        )

    @asynccontextmanager
    async def collect_upload(
        self,
        chunks: AsyncIterator[bytes],
        *,
        max_bytes: int,
    ) -> AsyncIterator[bytes]:
        collected = bytearray()
        async for chunk in chunks:
            remaining = max_bytes + 1 - len(collected)
            collected.extend(chunk[:remaining])
            if len(collected) > max_bytes:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
                    "Workspace upload exceeds the REST upload limit",
                )
        data = bytes(collected)
        del collected
        yield data

    async def read(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        offset: int = 0,
        length: int = 0,
        max_bytes: int = MAX_READ_BYTES,
    ) -> bytes:
        if offset < 0 or length < 0 or not 1 <= max_bytes <= MAX_READ_BYTES:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_BLOCKED_PATH,
                "Workspace byte range or limit is invalid",
            )
        read_limit = min(length, max_bytes) if length else max_bytes
        stored = await self._storage.read(
            _object_key(target, relative_path),
            max_bytes=read_limit,
            offset=offset,
            length=length,
        )
        return stored.data

    async def read_with_metadata(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        max_bytes: int = MAX_READ_BYTES,
    ) -> StoredObject:
        if not 1 <= max_bytes <= MAX_READ_BYTES:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_BLOCKED_PATH,
                "Workspace byte limit is invalid",
            )
        return await self._storage.read(
            _object_key(target, relative_path),
            max_bytes=max_bytes,
        )

    async def list_dir(
        self,
        target: WorkspaceTarget,
        relative_path: str = "",
    ) -> list[DirectoryEntry]:
        page = await self.list_dir_page(
            target,
            relative_path,
            limit=MAX_DIRECTORY_ENTRIES,
            offset=0,
        )
        if page.next_offset is not None or page.truncated:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE,
                "Workspace directory has too many entries to list",
            )
        return sorted(page.items, key=lambda entry: entry.path)

    async def list_dir_page(
        self,
        target: WorkspaceTarget,
        relative_path: str = "",
        *,
        limit: int,
        offset: int,
        scan_limit: int = 10_000,
        include_noise_directories: bool = False,
    ) -> DirectoryPage:
        workspace_prefix = _workspace_prefix(target)
        if relative_path:
            object_name = _object_key(target, relative_path)
            directory_prefix = f"{object_name}/"
        else:
            directory_prefix = workspace_prefix

        seen: set[str] = set()
        items: list[DirectoryEntry] = []
        matched = 0
        scanned = 0
        start_after: str | None = None
        truncated = False
        has_raw_child = False
        while True:
            page = await self._storage.list_directory_page(
                directory_prefix,
                start_after=start_after,
            )
            for item in page.items:
                if scanned == scan_limit:
                    truncated = True
                    break
                scanned += 1
                if not item.object_name.startswith(directory_prefix):
                    continue
                remainder = item.object_name.removeprefix(directory_prefix)
                if not remainder:
                    continue
                has_raw_child = True
                name, separator, _ = remainder.rstrip("/").partition("/")
                is_directory = item.is_directory or bool(separator)
                if is_directory and name in NOISE_DIRECTORIES and not include_noise_directories:
                    continue
                public_path = f"{relative_path.rstrip('/')}/{name}" if relative_path else name
                if public_path in seen:
                    continue
                seen.add(public_path)
                if is_directory:
                    entry = DirectoryEntry(
                        path=public_path,
                        is_directory=True,
                        size=None,
                    )
                else:
                    entry = DirectoryEntry(
                        path=public_path,
                        is_directory=False,
                        size=item.size,
                    )
                if matched >= offset:
                    items.append(entry)
                    if len(items) > limit:
                        return DirectoryPage(
                            items=tuple(items[:limit]),
                            next_offset=offset + limit,
                            truncated=False,
                        )
                matched += 1
            if truncated or page.next_start_after is None:
                break
            if scanned == scan_limit:
                truncated = True
                break
            start_after = page.next_start_after

        if relative_path and not has_raw_child:
            try:
                await self._storage.stat(object_name)
            except WorkspaceError as exc:
                if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                    raise
            else:
                raise WorkspaceError(
                    ErrorCode.TOOL_NOT_A_DIRECTORY,
                    "Workspace path is not a directory",
                )
            raise WorkspaceError(
                ErrorCode.WORKSPACE_NOT_FOUND,
                "Workspace folder was not found",
            )
        return DirectoryPage(
            items=tuple(items),
            next_offset=None,
            truncated=truncated,
        )

    async def usage(self, target: WorkspaceTarget) -> int:
        async with self._heavy_operations:
            usage = 0
            async for objects in _metadata_pages(self._storage, _workspace_prefix(target)):
                usage += sum(item.size for item in objects)
            return usage

    async def write(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        data: bytes,
        *,
        quota_bytes: int,
        if_match: str | None = None,
        if_none_match: bool = False,
        subtree_owner: Hashable | None = None,
    ) -> FileMetadata:
        async with self.materialization_slot():
            return await self._write(
                target,
                relative_path,
                data,
                quota_bytes=quota_bytes,
                if_match=if_match,
                if_none_match=if_none_match,
                subtree_owner=subtree_owner,
            )

    async def write_collected_upload(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        data: bytes,
        *,
        quota_bytes: int,
        if_match: str | None = None,
        if_none_match: bool = False,
        subtree_owner: Hashable | None = None,
    ) -> FileMetadata:
        return await self._write(
            target,
            relative_path,
            data,
            quota_bytes=quota_bytes,
            if_match=if_match,
            if_none_match=if_none_match,
            subtree_owner=subtree_owner,
        )

    async def commit_uploaded_object(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        temporary_object: str,
        *,
        size: int,
        quota_bytes: int,
        on_issued: Callable[[], None] | None = None,
        subtree_owner: Hashable | None = None,
        quota_reservation: DirectoryQuotaReservation | None = None,
    ) -> FileMetadata:
        """Atomically publish a completed RustFS upload under a mutation lock."""
        if size < 0:
            raise ValueError("uploaded object size must be non-negative")
        object_name = _object_key(target, relative_path)
        async with self._hold_mutations(
            (_subtree_scope(target, relative_path),),
            owner=subtree_owner,
        ):
            self._ensure_active(target)
            reservation_record = None
            if quota_reservation is not None:
                reservation_record = self._active_directory_quota_record(quota_reservation)
                if (
                    reservation_record.target != target
                    or reservation_record.owner != subtree_owner
                    or reservation_record.quota_bytes != quota_bytes
                    or size > reservation_record.remaining_bytes
                ):
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED,
                        "Directory child does not match its quota reservation",
                    )
            usage = 0
            existing = None
            parent_names = set(_parent_object_names(_workspace_prefix(target), object_name))
            parent_is_file = False
            target_is_directory = False
            folder_prefix = f"{object_name}/"
            async for objects in _metadata_pages(self._storage, _workspace_prefix(target)):
                for item in objects:
                    usage += item.size
                    if item.object_name == object_name:
                        existing = item
                    elif item.object_name in parent_names:
                        parent_is_file = True
                    elif item.object_name.startswith(folder_prefix):
                        target_is_directory = True
            if parent_is_file:
                raise WorkspaceError(
                    ErrorCode.TOOL_NOT_A_DIRECTORY,
                    "A workspace path parent is a file",
                )
            if target_is_directory:
                raise WorkspaceError(
                    ErrorCode.TOOL_IS_DIRECTORY,
                    "Workspace path is a directory",
                )
            if existing is not None:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_FILE_CHANGED,
                    "Workspace file already exists",
                )
            if usage > quota_bytes:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_SOFT_LOCKED,
                    "Workspace is over quota; delete files before writing",
                )
            reserved = self._reserved_directory_bytes(target)
            if reservation_record is None and size * 5 > quota_bytes * 4:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
                    "Workspace operation exceeds the single-operation size limit",
                )
            projected_usage = usage + reserved
            if reservation_record is None:
                projected_usage += size
            if projected_usage > quota_bytes:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_QUOTA_EXCEEDED,
                    "Workspace quota would be exceeded",
                )
            if on_issued is not None:
                on_issued()
            publish = asyncio.create_task(
                self._storage.promote_if_absent(
                    temporary_object,
                    object_name,
                    size=size,
                )
            )
            uploaded, cancelled = await _await_irreversible_result(publish)
            if reservation_record is not None:
                reservation_record.remaining_bytes -= size
            cleanup = asyncio.create_task(self._delete_transfer_temporary(temporary_object))
            self._transfer_cleanup_tasks.add(cleanup)
            cleanup.add_done_callback(self._transfer_cleanup_tasks.discard)
            metadata = FileMetadata(size=size, etag=uploaded.etag, created=True)
            if cancelled:
                raise UploadCommittedAfterCancellation
            return metadata

    async def _write(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        data: bytes,
        *,
        quota_bytes: int,
        if_match: str | None,
        if_none_match: bool,
        subtree_owner: Hashable | None,
    ) -> FileMetadata:
        object_name = _object_key(target, relative_path)
        async with self._hold_mutations(
            (_subtree_scope(target, relative_path),),
            owner=subtree_owner,
        ):
            self._ensure_active(target)
            return await self._write_locked(
                target,
                object_name,
                data,
                quota_bytes=quota_bytes,
                if_match=if_match,
                if_none_match=if_none_match,
                single_operation_bytes=len(data),
            )

    async def edit(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        transform: Callable[[bytes], bytes],
        *,
        quota_bytes: int,
        if_match: str | None = None,
        subtree_owner: Hashable | None = None,
    ) -> FileMetadata:
        async with self.materialization_slot():
            return await self.edit_materialized(
                target,
                relative_path,
                transform,
                quota_bytes=quota_bytes,
                if_match=if_match,
                subtree_owner=subtree_owner,
            )

    async def edit_materialized(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        transform: Callable[[bytes], bytes],
        *,
        quota_bytes: int,
        if_match: str | None = None,
        subtree_owner: Hashable | None = None,
    ) -> FileMetadata:
        object_name = _object_key(target, relative_path)
        async with self._hold_mutations(
            (_subtree_scope(target, relative_path),),
            owner=subtree_owner,
        ):
            self._ensure_active(target)
            metadata = await self._storage.stat(object_name)
            if if_match is not None and metadata.etag != if_match:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_FILE_CHANGED,
                    "Workspace file changed after it was read",
                )
            if metadata.size > MAX_EDIT_BYTES:
                raise _too_large_to_edit()
            current = await self._storage.read(
                object_name,
                max_bytes=MAX_EDIT_BYTES,
            )
            if current.truncated:
                raise _too_large_to_edit()
            updated = await _run_transform(transform, current.data)
            if len(updated) > MAX_EDIT_BYTES:
                raise _too_large_to_edit()
            return await self._write_locked(
                target,
                object_name,
                updated,
                quota_bytes=quota_bytes,
                if_match=if_match,
                if_none_match=False,
                single_operation_bytes=max(0, len(updated) - metadata.size),
            )

    async def edit_optional_materialized(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        transform: Callable[[bytes | None], bytes],
        *,
        quota_bytes: int,
        if_match: str | None = None,
        subtree_owner: Hashable | None = None,
    ) -> FileMetadata:
        object_name = _object_key(target, relative_path)
        async with self._hold_mutations(
            (_subtree_scope(target, relative_path),),
            owner=subtree_owner,
        ):
            self._ensure_active(target)
            metadata = None
            current_data = None
            try:
                metadata = await self._storage.stat(object_name)
            except WorkspaceError as exc:
                if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                    raise
            if if_match is not None and (metadata is None or metadata.etag != if_match):
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_FILE_CHANGED,
                    "Workspace file changed after it was read",
                )
            if metadata is not None:
                if metadata.size > MAX_EDIT_BYTES:
                    raise _too_large_to_edit()
                current = await self._storage.read(object_name, max_bytes=MAX_EDIT_BYTES)
                if current.truncated:
                    raise _too_large_to_edit()
                current_data = current.data
            updated = await _run_optional_transform(transform, current_data)
            if len(updated) > MAX_EDIT_BYTES:
                raise _too_large_to_edit()
            existing_size = 0 if metadata is None else metadata.size
            return await self._write_locked(
                target,
                object_name,
                updated,
                quota_bytes=quota_bytes,
                if_match=if_match,
                if_none_match=False,
                single_operation_bytes=max(0, len(updated) - existing_size),
            )

    async def _write_locked(
        self,
        target: WorkspaceTarget,
        object_name: str,
        data: bytes,
        *,
        quota_bytes: int,
        if_match: str | None,
        if_none_match: bool,
        single_operation_bytes: int,
    ) -> FileMetadata:
        prefix = _workspace_prefix(target)
        usage = 0
        existing = None
        parent_names = set(_parent_object_names(prefix, object_name))
        parent_is_file = False
        folder_prefix = f"{object_name}/"
        target_is_directory = False
        async for objects in _metadata_pages(self._storage, prefix):
            for item in objects:
                usage += item.size
                if item.object_name == object_name:
                    existing = item
                elif item.object_name in parent_names:
                    parent_is_file = True
                elif item.object_name.startswith(folder_prefix):
                    target_is_directory = True

        if parent_is_file:
            raise WorkspaceError(
                ErrorCode.TOOL_NOT_A_DIRECTORY,
                "A workspace path parent is a file",
            )
        if target_is_directory:
            raise WorkspaceError(
                ErrorCode.TOOL_IS_DIRECTORY,
                "Workspace path is a directory",
            )

        if usage > quota_bytes:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_SOFT_LOCKED,
                "Workspace is over quota; delete files before writing",
            )
        if single_operation_bytes * 5 > quota_bytes * 4:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
                "Workspace operation exceeds the single-operation size limit",
            )
        if if_match is not None and (existing is None or existing.etag != if_match):
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace file changed after it was read",
            )
        if if_none_match and existing is not None:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace file already exists",
            )

        replaced_size = existing.size if existing is not None else 0
        if (
            usage
            + self._reserved_directory_bytes(target)
            - replaced_size
            + len(data)
            > quota_bytes
        ):
            raise WorkspaceError(
                ErrorCode.WORKSPACE_QUOTA_EXCEEDED,
                "Workspace quota would be exceeded",
            )

        uploaded = await self._storage.write(object_name, data)
        return FileMetadata(
            size=len(data),
            etag=uploaded.etag,
            created=existing is None,
        )

    async def delete_file(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        if_match: str | None = None,
        subtree_owner: Hashable | None = None,
    ) -> None:
        object_name = _object_key(target, relative_path)
        async with self._hold_mutations(
            (_subtree_scope(target, relative_path),),
            owner=subtree_owner,
        ):
            self._ensure_active(target)
            try:
                existing = await self._storage.stat(object_name)
            except WorkspaceError as exc:
                if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                    raise
                folder = await self._storage.list_page(f"{object_name}/", limit=1)
                if folder.items:
                    raise WorkspaceError(
                        ErrorCode.TOOL_IS_DIRECTORY,
                        "Workspace path is a directory",
                    )
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_NOT_FOUND,
                    "Workspace file was not found",
                ) from exc
            if if_match is not None and existing.etag != if_match:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_FILE_CHANGED,
                    "Workspace file changed after it was read",
                )
            await self._storage.delete(object_name)

    async def delete_folder(
        self,
        target: WorkspaceTarget,
        relative_path: str,
        *,
        subtree_owner: Hashable | None = None,
    ) -> None:
        object_name = _object_key(target, relative_path)
        folder_prefix = f"{object_name}/"
        async with self._hold_mutations(
            (_subtree_scope(target, relative_path),),
            owner=subtree_owner,
        ):
            self._ensure_active(target)
            try:
                await self._storage.stat(object_name)
            except WorkspaceError as exc:
                if exc.code is not ErrorCode.WORKSPACE_NOT_FOUND:
                    raise
            else:
                raise WorkspaceError(
                    ErrorCode.TOOL_IS_FILE,
                    "Workspace path is a file",
                )
            if not await _delete_prefix(self._storage, folder_prefix):
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_NOT_FOUND,
                    "Workspace folder was not found",
                )

    async def apply_transforms(
        self,
        edits: tuple[FileTransform, ...],
        *,
        dry_run: bool,
        subtree_owner: Hashable | None = None,
    ) -> tuple[FileMetadata, ...]:
        if not 1 <= len(edits) <= 20:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Workspace patch must contain between 1 and 20 edits",
            )
        async with self.materialization_slot():
            return await self.apply_transforms_admitted(
                edits,
                dry_run=dry_run,
                subtree_owner=subtree_owner,
            )

    async def apply_transforms_admitted(
        self,
        edits: tuple[FileTransform, ...],
        *,
        dry_run: bool,
        subtree_owner: Hashable | None = None,
    ) -> tuple[FileMetadata, ...]:
        if not 1 <= len(edits) <= 20:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Workspace patch must contain between 1 and 20 edits",
            )
        scopes = tuple(_subtree_scope(edit.target, edit.relative_path) for edit in edits)
        async with self._hold_mutations(scopes, owner=subtree_owner):
            return await self._apply_transforms_locked(edits, dry_run=dry_run)

    async def _apply_transforms_locked(
        self,
        edits: tuple[FileTransform, ...],
        *,
        dry_run: bool,
    ) -> tuple[FileMetadata, ...]:
        object_names = tuple(_object_key(edit.target, edit.relative_path) for edit in edits)
        edit_keys = tuple(
            (edit.target, object_name)
            for edit, object_name in zip(edits, object_names, strict=True)
        )
        if len(set(edit_keys)) != len(edit_keys):
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Workspace patch cannot edit the same path more than once",
            )
        relevant_names_by_target: dict[WorkspaceTarget, tuple[str, ...]] = {}
        for edit, object_name in zip(edits, object_names, strict=True):
            relevant_names_by_target.setdefault(edit.target, ())
            relevant_names_by_target[edit.target] += (object_name,)

        metadata_by_target: dict[WorkspaceTarget, dict[str, ObjectMetadata]] = {}
        directory_keys: set[tuple[WorkspaceTarget, str]] = set()
        usage_by_target: dict[WorkspaceTarget, int] = {}
        quota_by_target: dict[WorkspaceTarget, int] = {}
        for edit in edits:
            self._ensure_active(edit.target)
            previous_quota = quota_by_target.setdefault(edit.target, edit.quota_bytes)
            if previous_quota != edit.quota_bytes:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_INVALID_REQUEST,
                    "Workspace quota changed while preparing patch",
                )
        for target, quota_bytes in quota_by_target.items():
            metadata: dict[str, ObjectMetadata] = {}
            relevant_names = relevant_names_by_target[target]
            relevant_parents = {
                parent
                for object_name in relevant_names
                for parent in _parent_object_names(_workspace_prefix(target), object_name)
            }
            usage = 0
            async for page in _metadata_pages(self._storage, _workspace_prefix(target)):
                for item in page:
                    usage += item.size
                    if item.object_name in relevant_names or item.object_name in relevant_parents:
                        metadata[item.object_name] = item
                    for object_name in relevant_names:
                        if item.object_name.startswith(f"{object_name}/"):
                            directory_keys.add((target, object_name))
            if usage > quota_bytes:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_SOFT_LOCKED,
                    "Workspace is over quota; delete files before writing",
                )
            metadata_by_target[target] = metadata
            usage_by_target[target] = usage + self._reserved_directory_bytes(target)

        staged: dict[tuple[WorkspaceTarget, str], bytes | None] = {}
        prepared: list[tuple[FileTransform, str, bytes, FileMetadata]] = []
        prepared_bytes = 0
        for edit, object_name in zip(edits, object_names, strict=True):
            key = (edit.target, object_name)
            target_metadata = metadata_by_target[edit.target]
            if key not in staged:
                current_metadata = target_metadata.get(object_name)
                if current_metadata is None:
                    current_data = None
                else:
                    if current_metadata.size > MAX_EDIT_BYTES:
                        raise _too_large_to_edit()
                    current = await self._storage.read(object_name, max_bytes=MAX_EDIT_BYTES)
                    if current.truncated:
                        raise _too_large_to_edit()
                    current_data = current.data
                staged[key] = current_data
            current_data = staged[key]
            _validate_patch_object_shape(
                edit.target,
                object_name,
                metadata_by_target[edit.target],
                staged,
                directory_keys,
            )
            updated = await _run_optional_transform(edit.transform, current_data)
            if len(updated) > MAX_EDIT_BYTES:
                raise _too_large_to_edit()
            prepared_bytes += len(updated)
            if prepared_bytes > MAX_EDIT_BYTES:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
                    "Workspace patch exceeds the 8 MiB aggregate materialization limit",
                )
            previous_size = 0 if current_data is None else len(current_data)
            growth = max(0, len(updated) - previous_size)
            if growth * 5 > edit.quota_bytes * 4:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
                    "Workspace operation exceeds the single-operation size limit",
                )
            usage = usage_by_target[edit.target] - previous_size + len(updated)
            if usage > edit.quota_bytes:
                raise WorkspaceError(
                    ErrorCode.WORKSPACE_QUOTA_EXCEEDED,
                    "Workspace quota would be exceeded",
                )
            usage_by_target[edit.target] = usage
            preview = FileMetadata(
                size=len(updated),
                etag="dry-run",
                created=current_data is None,
            )
            prepared.append((edit, object_name, updated, preview))
            staged[key] = updated

        if dry_run:
            return tuple(item[3] for item in prepared)
        committed = 0
        results: list[FileMetadata] = []
        try:
            for _, object_name, data, preview in prepared:
                uploaded = await self._storage.write(object_name, data)
                committed += 1
                results.append(
                    FileMetadata(
                        size=len(data),
                        etag=uploaded.etag,
                        created=preview.created,
                    )
                )
        except WorkspaceError as exc:
            raise WorkspaceError(
                exc.code,
                f"Workspace patch storage failure after {committed} edits committed",
            ) from exc
        return tuple(results)

    async def purge_workspace(self, target: WorkspaceTarget) -> None:
        """Idempotently remove every object for an already-authorized lifecycle event."""
        async with self._hold_mutations(((target, ()),)):
            self._retired_targets.add(target)
            await _delete_prefix(self._storage, _workspace_prefix(target))

    async def retire_workspace(self, target: WorkspaceTarget) -> None:
        """Fence mutations before committing durable lifecycle deletion state."""
        async with self._hold_mutations(((target, ()),)):
            self._retired_targets.add(target)

    async def reactivate_workspace(self, target: WorkspaceTarget) -> None:
        """Undo retirement when the corresponding database deletion rolls back."""
        async with self._hold_mutations(((target, ()),)):
            self._retired_targets.discard(target)

    async def forget_workspace(self, target: WorkspaceTarget) -> None:
        """Release a completed deletion tombstone after its durable job commits."""
        async with self._hold_mutations(((target, ()),)):
            self._retired_targets.discard(target)

    def _ensure_active(self, target: WorkspaceTarget) -> None:
        if target in self._retired_targets:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_NOT_FOUND,
                "Workspace was not found",
            )

    async def _delete_transfer_temporary(self, object_name: str) -> None:
        try:
            await self._storage.delete(object_name)
        except Exception:
            # Startup recovery removes leftovers. The published destination is
            # already verified and must remain successful after cleanup errors.
            pass


async def _release_subtree_leases(leases: tuple[SubtreeLease, ...]) -> None:
    for lease in leases:
        await lease.release()


def _subtree_scope(
    target: WorkspaceTarget,
    relative_path: str,
) -> tuple[WorkspaceTarget, tuple[str, ...]]:
    if not relative_path:
        return target, ()
    object_name = _object_key(target, relative_path)
    relative_object_name = object_name.removeprefix(_workspace_prefix(target))
    return target, tuple(relative_object_name.split("/"))


async def _stat_optional(
    storage: ObjectStorage,
    object_name: str,
) -> ObjectMetadata | None:
    try:
        return await storage.stat(object_name)
    except WorkspaceError as exc:
        if exc.code is ErrorCode.WORKSPACE_NOT_FOUND:
            return None
        raise


def _join_relative_path(root: str, relative_path: str) -> str:
    return f"{root}/{relative_path}"


def _workspace_storage_shape_error() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_STORAGE_ERROR,
        "Workspace object storage contains an invalid file/prefix shape",
    )


def _workspace_directory_too_large() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE,
        "Workspace directory exceeds the recursive transfer limit",
    )


def _validate_projected_quota(
    *,
    usage: int,
    reserved: int,
    operation_bytes: int,
    quota_bytes: int,
) -> None:
    if usage > quota_bytes:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_SOFT_LOCKED,
            "Workspace is over quota; delete files before writing",
        )
    if operation_bytes * 5 > quota_bytes * 4:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
            "Workspace operation exceeds the single-operation size limit",
        )
    if usage + reserved + operation_bytes > quota_bytes:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_QUOTA_EXCEEDED,
            "Workspace quota would be exceeded",
        )


async def _run_transform(transform: Callable[[bytes], bytes], current: bytes) -> bytes:
    worker = asyncio.create_task(asyncio.to_thread(transform, current))
    return await await_future_cancellation_safe(worker)


async def _await_irreversible_result[T](future: asyncio.Future[T]) -> tuple[T, bool]:
    """Resolve a started publish and report cancellation after a successful result."""

    cancelled = False
    while True:
        try:
            result = await asyncio.shield(future)
        except asyncio.CancelledError:
            cancelled = True
            continue
        except BaseException:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
        return result, cancelled


async def _run_optional_transform(
    transform: Callable[[bytes | None], bytes],
    current: bytes | None,
) -> bytes:
    worker = asyncio.create_task(asyncio.to_thread(transform, current))
    return await await_future_cancellation_safe(worker)


async def _metadata_pages(
    storage: ObjectStorage,
    prefix: str,
) -> AsyncIterator[tuple[ObjectMetadata, ...]]:
    start_after: str | None = None
    while True:
        page = await storage.list_page(prefix, start_after=start_after)
        yield page.items
        if page.next_start_after is None:
            return
        start_after = page.next_start_after


async def _directory_pages(
    storage: ObjectStorage,
    prefix: str,
) -> AsyncIterator[tuple[DirectoryObject, ...]]:
    start_after: str | None = None
    while True:
        page = await storage.list_directory_page(prefix, start_after=start_after)
        yield page.items
        if page.next_start_after is None:
            return
        start_after = page.next_start_after


async def _delete_prefix(storage: ObjectStorage, prefix: str) -> bool:
    deleted = False
    start_after: str | None = None
    while True:
        page = await storage.list_page(prefix, start_after=start_after)
        for item in page.items:
            await storage.delete(item.object_name)
            deleted = True
        if page.next_start_after is None:
            return deleted
        start_after = page.next_start_after


def _workspace_prefix(target: WorkspaceTarget) -> str:
    collection = "users" if target.kind == "personal" else "workspaces"
    return f"{collection}/{target.id}/"


def _object_key(target: WorkspaceTarget, relative_path: str) -> str:
    path = PurePosixPath(relative_path)
    raw_parts = relative_path.split("/")
    if (
        not relative_path
        or not path.parts
        or relative_path.startswith("/")
        or "\x00" in relative_path
        or any(part in {"", ".", ".."} for part in raw_parts)
    ):
        raise WorkspaceError(
            ErrorCode.WORKSPACE_BLOCKED_PATH,
            "Workspace path is invalid",
        )
    return f"{_workspace_prefix(target)}{path.as_posix()}"


def _parent_object_names(prefix: str, object_name: str) -> list[str]:
    relative_parts = object_name.removeprefix(prefix).split("/")
    return [
        f"{prefix}{'/'.join(relative_parts[:index])}" for index in range(1, len(relative_parts))
    ]


def _validate_patch_object_shape(
    target: WorkspaceTarget,
    object_name: str,
    metadata: dict[str, ObjectMetadata],
    staged: dict[tuple[WorkspaceTarget, str], bytes | None],
    directory_keys: set[tuple[WorkspaceTarget, str]],
) -> None:
    prefix = _workspace_prefix(target)
    parents = set(_parent_object_names(prefix, object_name))
    staged_names = {
        name
        for (staged_target, name), data in staged.items()
        if staged_target == target and data is not None
    }
    if parents.intersection(metadata) or parents.intersection(staged_names):
        raise WorkspaceError(ErrorCode.TOOL_NOT_A_DIRECTORY, "A workspace path parent is a file")
    folder_prefix = f"{object_name}/"
    if (target, object_name) in directory_keys or any(
        name.startswith(folder_prefix) for name in staged_names
    ):
        raise WorkspaceError(ErrorCode.TOOL_IS_DIRECTORY, "Workspace path is a directory")


def _too_large_to_edit() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
        "Workspace file exceeds the 8 MiB server edit limit",
    )


@lru_cache
def _workspace_fs_for_storage(storage: ObjectStorage) -> WorkspaceFS:
    settings = get_settings()
    return WorkspaceFS(
        storage,
        server_transfer_max_concurrency_per_user=(
            settings.rest_transfer_max_concurrency_per_user
        ),
        server_transfer_queue_timeout_seconds=(
            settings.rest_transfer_queue_timeout_seconds
        ),
    )


def get_workspace_fs(
    storage: Annotated[ObjectStorage, Depends(get_object_storage)],
) -> WorkspaceFS:
    return _workspace_fs_for_storage(storage)
