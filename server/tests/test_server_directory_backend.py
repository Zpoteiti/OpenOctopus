from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from unittest.mock import ANY, AsyncMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.directory_contract import (
    DirectoryManifest,
    DirectoryManifestDirectory,
    DirectoryManifestEntry,
    create_directory_manifest,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.tools.directory_transfer import DirectoryTransferCoordinator
from openctopus_server.tools.server_directory_backend import ServerDirectoryTransferBackend
from openctopus_server.workspace.fs import (
    DirectoryCommittedFile,
    DirectoryDestinationPlan,
    FileMetadata,
    UploadCommittedAfterCancellation,
    WorkspaceFS,
    WorkspaceTarget,
)
from openctopus_server.workspace.service import TransferPathTicket, WorkspaceService


@pytest_asyncio.fixture(autouse=True)
async def _no_database_cleanup():
    yield


def _manifest() -> DirectoryManifest:
    return create_directory_manifest(
        root_identity=None,
        directories=(DirectoryManifestDirectory(relative_path="nested", identity=None),),
        entries=(
            DirectoryManifestEntry(
                relative_path="a.txt",
                size=1,
                fingerprint="source-a",
            ),
            DirectoryManifestEntry(
                relative_path="nested/b.txt",
                size=2,
                fingerprint="source-b",
            ),
        ),
    )


def _tickets() -> tuple[TransferPathTicket, TransferPathTicket]:
    user_id = uuid4()
    return (
        TransferPathTicket(
            user_id=user_id,
            display_path="source",
            target=WorkspaceTarget.personal(user_id),
            relative_path="source",
            quota_bytes=100,
        ),
        TransferPathTicket(
            user_id=user_id,
            display_path="/shared@destination/backup",
            target=WorkspaceTarget.shared(uuid4()),
            relative_path="backup",
            quota_bytes=100,
        ),
    )


@dataclass
class _OperationLease:
    close_count: int = 0

    async def aclose(self) -> None:
        self.close_count += 1


@dataclass
class _SubtreeLease:
    release_count: int = 0

    async def release(self) -> None:
        self.release_count += 1


@dataclass
class _Reservation:
    release_count: int = 0

    async def release(self) -> None:
        self.release_count += 1


@dataclass
class _Stream:
    data: bytes
    etag: str
    closed: bool = False
    _chunks: list[bytes] = field(init=False)

    def __post_init__(self) -> None:
        self.size = len(self.data)
        self._chunks = [self.data, b""]

    async def read(self) -> bytes:
        return self._chunks.pop(0)

    async def aclose(self) -> None:
        self.closed = True


@dataclass
class _Sink:
    chunks: list[bytes] = field(default_factory=list)
    finished: bool = False
    aborted: bool = False

    async def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)

    async def finish(self) -> None:
        self.finished = True

    async def abort(self) -> None:
        self.aborted = True


async def test_server_backend_streams_serial_children_and_finalizes_exact_tree() -> None:
    manifest = _manifest()
    source, destination = _tickets()
    plan = DirectoryDestinationPlan(
        target=destination.target,
        destination_root=destination.relative_path,
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("backup/a.txt", "backup/nested/b.txt"),
    )
    fs = AsyncMock(spec=WorkspaceFS)
    workspace = AsyncMock(spec=WorkspaceService)
    reservation = _Reservation()
    leases = (_SubtreeLease(), _SubtreeLease())
    fs.preflight_directory_destination.return_value = plan
    fs.reserve_directory_quota.return_value = reservation
    fs.acquire_subtree_lease.side_effect = leases
    streams = (_Stream(b"a", "source-a"), _Stream(b"bb", "source-b"))
    fs.open_stream.side_effect = streams
    sinks = (_Sink(), _Sink())
    fs.begin_directory_child_upload.side_effect = (
        (sinks[0], "temporary-a"),
        (sinks[1], "temporary-b"),
    )

    async def commit(
        _reservation: object,
        relative_path: str,
        _temporary: str,
        *,
        size: int,
        on_issued,
    ) -> FileMetadata:
        on_issued()
        return FileMetadata(size=size, etag=f"etag-{relative_path}", created=True)

    fs.commit_directory_child_upload.side_effect = commit
    operation_lease = _OperationLease()
    issued = 0

    def mark_issued() -> None:
        nonlocal issued
        issued += 1

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="copy",
        backend=ServerDirectoryTransferBackend(
            workspace_fs=fs,
            workspace_service=workspace,
            source=source,
            destination=destination,
            manifest=manifest,
            operation_id=new_uuid7(),
        ),
        operation_lease=operation_lease,
        on_issued=mark_issued,
    )

    assert result.files_transferred == 2
    assert result.bytes_transferred == 3
    assert issued == 1
    assert [sink.chunks for sink in sinks] == [[b"a"], [b"bb"]]
    assert all(sink.finished and not sink.aborted for sink in sinks)
    assert all(stream.closed for stream in streams)
    fs.verify_directory_destination.assert_awaited_once_with(
        plan,
        (
            DirectoryCommittedFile("backup/a.txt", 1, "etag-backup/a.txt"),
            DirectoryCommittedFile(
                "backup/nested/b.txt",
                2,
                "etag-backup/nested/b.txt",
            ),
        ),
        owner=ANY,
    )
    workspace.validate_directory_skill_manifests.assert_awaited_once()
    assert reservation.release_count == 1
    assert [lease.release_count for lease in leases] == [1, 1]
    assert operation_lease.close_count == 1


async def test_publish_cancellation_keeps_committed_etag_for_cleanup() -> None:
    manifest = create_directory_manifest(
        root_identity=None,
        directories=(),
        entries=(
            DirectoryManifestEntry(
                relative_path="a.txt",
                size=1,
                fingerprint="source-a",
            ),
        ),
    )
    source, destination = _tickets()
    plan = DirectoryDestinationPlan(
        target=destination.target,
        destination_root="backup",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("backup/a.txt",),
    )
    fs = AsyncMock(spec=WorkspaceFS)
    workspace = AsyncMock(spec=WorkspaceService)
    fs.preflight_directory_destination.return_value = plan
    fs.reserve_directory_quota.return_value = _Reservation()
    fs.acquire_subtree_lease.side_effect = (_SubtreeLease(), _SubtreeLease())
    fs.open_stream.return_value = _Stream(b"a", "source-a")
    fs.begin_directory_child_upload.return_value = (_Sink(), "temporary")

    async def commit(*_args, on_issued, **_kwargs):
        on_issued()
        raise UploadCommittedAfterCancellation(
            FileMetadata(size=1, etag="destination-etag", created=True)
        )

    fs.commit_directory_child_upload.side_effect = commit
    fs.conditional_delete_file.return_value = "deleted"
    fs.directory_root_is_absent.return_value = True

    with pytest.raises(asyncio.CancelledError):
        await DirectoryTransferCoordinator().run(
            manifest=manifest,
            mode="copy",
            backend=ServerDirectoryTransferBackend(
                workspace_fs=fs,
                workspace_service=workspace,
                source=source,
                destination=destination,
                manifest=manifest,
                operation_id=new_uuid7(),
            ),
            operation_lease=_OperationLease(),
        )

    fs.conditional_delete_file.assert_awaited_once_with(
        destination.target,
        "backup/a.txt",
        expected_etag="destination-etag",
        subtree_owner=ANY,
    )


async def test_move_source_cleanup_aggregates_changed_and_incomplete_warnings() -> None:
    manifest = create_directory_manifest(
        root_identity=None,
        directories=(),
        entries=(
            DirectoryManifestEntry(
                relative_path="a.txt",
                size=1,
                fingerprint="source-a",
            ),
        ),
    )
    source, destination = _tickets()
    plan = DirectoryDestinationPlan(
        target=destination.target,
        destination_root="backup",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("backup/a.txt",),
    )
    fs = AsyncMock(spec=WorkspaceFS)
    workspace = AsyncMock(spec=WorkspaceService)
    fs.preflight_directory_destination.return_value = plan
    fs.reserve_directory_quota.return_value = _Reservation()
    fs.acquire_subtree_lease.side_effect = (_SubtreeLease(), _SubtreeLease())
    fs.open_stream.return_value = _Stream(b"a", "source-a")
    fs.begin_directory_child_upload.return_value = (_Sink(), "temporary")

    async def commit(*_args, on_issued, **_kwargs):
        on_issued()
        return FileMetadata(size=1, etag="destination-etag", created=True)

    fs.commit_directory_child_upload.side_effect = commit
    fs.conditional_delete_file.return_value = "mismatch"
    fs.directory_root_is_absent.return_value = False

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="move",
        backend=ServerDirectoryTransferBackend(
            workspace_fs=fs,
            workspace_service=workspace,
            source=source,
            destination=destination,
            manifest=manifest,
            operation_id=new_uuid7(),
        ),
        operation_lease=_OperationLease(),
    )

    assert result.warnings == (
        "source_changed_after_copy",
        "source_cleanup_incomplete",
    )


async def test_same_server_namespace_rejects_destination_inside_source() -> None:
    manifest = _manifest()
    user_id = uuid4()
    target = WorkspaceTarget.personal(user_id)
    source = TransferPathTicket(user_id, "tree", target, "tree", 100)
    destination = TransferPathTicket(user_id, "tree/child", target, "tree/child", 100)
    fs = AsyncMock(spec=WorkspaceFS)
    workspace = AsyncMock(spec=WorkspaceService)
    backend = ServerDirectoryTransferBackend(
        workspace_fs=fs,
        workspace_service=workspace,
        source=source,
        destination=destination,
        manifest=manifest,
        operation_id=new_uuid7(),
    )

    with pytest.raises(WorkspaceError) as caught:
        await backend.preflight(manifest)

    assert caught.value.code is ErrorCode.WORKSPACE_INVALID_REQUEST
    fs.preflight_directory_destination.assert_not_awaited()


async def test_workspace_root_source_maps_child_without_leading_slash() -> None:
    manifest = create_directory_manifest(
        root_identity=None,
        directories=(),
        entries=(
            DirectoryManifestEntry(
                relative_path="root.txt",
                size=1,
                fingerprint="source-root",
            ),
        ),
    )
    source, destination = _tickets()
    source = TransferPathTicket(
        source.user_id,
        f"/{source.user_id}",
        source.target,
        "",
        source.quota_bytes,
    )
    plan = DirectoryDestinationPlan(
        target=destination.target,
        destination_root="backup",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("backup/root.txt",),
    )
    fs = AsyncMock(spec=WorkspaceFS)
    workspace = AsyncMock(spec=WorkspaceService)
    fs.preflight_directory_destination.return_value = plan
    fs.reserve_directory_quota.return_value = _Reservation()
    fs.acquire_subtree_lease.side_effect = (_SubtreeLease(), _SubtreeLease())
    fs.open_stream.return_value = _Stream(b"r", "source-root")
    fs.begin_directory_child_upload.return_value = (_Sink(), "temporary")

    async def commit(*_args, on_issued, **_kwargs):
        on_issued()
        return FileMetadata(size=1, etag="destination-etag", created=True)

    fs.commit_directory_child_upload.side_effect = commit

    await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="copy",
        backend=ServerDirectoryTransferBackend(
            workspace_fs=fs,
            workspace_service=workspace,
            source=source,
            destination=destination,
            manifest=manifest,
            operation_id=new_uuid7(),
        ),
        operation_lease=_OperationLease(),
    )

    fs.open_stream.assert_awaited_once_with(source.target, "root.txt")


def test_server_backend_rejects_non_uuid7_operation_id() -> None:
    manifest = _manifest()
    source, destination = _tickets()

    with pytest.raises(ValueError, match="UUIDv7"):
        ServerDirectoryTransferBackend(
            workspace_fs=AsyncMock(spec=WorkspaceFS),
            workspace_service=AsyncMock(spec=WorkspaceService),
            source=source,
            destination=destination,
            manifest=manifest,
            operation_id=UUID(int=0),
        )
