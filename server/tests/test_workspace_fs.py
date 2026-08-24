import asyncio
import io
import threading
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from storage_http import object_storage_for_fake

from openctopus_server.directory_contract import (
    DirectoryManifestDirectory,
    DirectoryManifestEntry,
    create_directory_manifest,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import (
    MAX_EDIT_BYTES,
    FileTransform,
    ServerDirectorySourceProbe,
    ServerFileSourceProbe,
    UploadCommittedAfterCancellation,
    WorkspaceFS,
    WorkspaceTarget,
)
from openctopus_server.workspace.locks import KeyedLockManager
from openctopus_server.workspace.storage import (
    ObjectMetadata,
    ObjectStorage,
    normalize_storage_error,
)


async def test_download_waiting_for_storage_capacity_does_not_block_workspace_retirement() -> None:
    target = WorkspaceTarget.personal(uuid4())
    entered = asyncio.Event()
    release = asyncio.Event()
    stream = object()
    storage = AsyncMock()

    async def blocked_open(object_name: str) -> object:
        assert object_name.endswith("/report.bin")
        entered.set()
        await release.wait()
        return stream

    storage.open_stream.side_effect = blocked_open
    workspace_fs = WorkspaceFS(storage)
    opening = asyncio.create_task(workspace_fs.open_stream(target, "report.bin"))
    await entered.wait()

    retiring = asyncio.create_task(workspace_fs.retire_workspace(target))
    await asyncio.wait_for(asyncio.shield(retiring), timeout=0.2)
    release.set()

    assert await opening is stream


async def test_upload_collection_does_not_take_an_agent_materialization_slot() -> None:
    storage = AsyncMock()
    workspace_fs = WorkspaceFS(storage, materialization_concurrency=1)
    first_chunk_read = asyncio.Event()
    release_upload = asyncio.Event()

    async def chunks():
        yield b"first"
        first_chunk_read.set()
        await release_upload.wait()
        yield b"second"

    async def collect() -> None:
        async with workspace_fs.collect_upload(chunks(), max_bytes=20) as data:
            assert data == b"firstsecond"

    upload = asyncio.create_task(collect())
    await first_chunk_read.wait()
    async with asyncio.timeout(0.1), workspace_fs.materialization_slot():
        pass
    release_upload.set()
    await upload


@pytest_asyncio.fixture(autouse=True)
async def _truncate_tables() -> None:
    """These unit tests do not need the suite's PostgreSQL cleanup fixture."""
    yield


@dataclass(frozen=True)
class _Object:
    object_name: str
    size: int
    etag: str


class _Response(io.BytesIO):
    def __init__(self, content: bytes, etag: str) -> None:
        super().__init__(content)
        self.headers = {"ETag": f'"{etag}"'}

    def release_conn(self) -> None:
        pass


class _S3Error(Exception):
    def __init__(self, code: str, message: str = "internal/key/secret.txt") -> None:
        self.code = code
        super().__init__(message)


class _MemoryMinio:
    """Small synchronous fake matching the minio-py calls used by WorkspaceFS."""

    def __init__(self) -> None:
        self._objects: dict[str, tuple[bytes, str]] = {}
        self._guard = threading.Lock()
        self._revision = 0
        self.put_calls = 0
        self.get_calls = 0
        self.stat_calls = 0
        self.list_calls = 0
        self.list_prefixes: list[str] = []
        self.get_ranges: list[tuple[int, int]] = []
        self.removed_names: list[str] = []

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: str,
        recursive: bool,
        start_after: str | None = None,
    ) -> Iterator[_Object]:
        del bucket, recursive
        with self._guard:
            self.list_calls += 1
            self.list_prefixes.append(prefix)
            objects = sorted(self._objects.items())
        return iter(
            _Object(key, len(content), etag)
            for key, (content, etag) in objects
            if key.startswith(prefix) and (start_after is None or key > start_after)
        )

    def stat_object(self, bucket: str, object_name: str) -> _Object:
        del bucket
        with self._guard:
            stored = self._objects.get(object_name)
            self.stat_calls += 1
        if stored is None:
            raise _S3Error("NoSuchKey")
        content, etag = stored
        return _Object(object_name, len(content), etag)

    def get_object(
        self,
        bucket: str,
        object_name: str,
        offset: int = 0,
        length: int = 0,
    ) -> _Response:
        del bucket
        with self._guard:
            stored = self._objects.get(object_name)
            self.get_calls += 1
            self.get_ranges.append((offset, length))
        if stored is None:
            raise _S3Error("NoSuchKey")
        content, etag = stored
        end = offset + length if length else None
        return _Response(content[offset:end], etag)

    def put_object(
        self,
        bucket: str,
        object_name: str,
        data: io.BytesIO,
        length: int,
        *,
        num_parallel_uploads: int,
        part_size: int | None = None,
    ) -> SimpleNamespace:
        del bucket
        assert num_parallel_uploads == 1
        assert part_size is None or part_size == 5 * 1024 * 1024
        content = data.read(length)
        with self._guard:
            self.put_calls += 1
            self._revision += 1
            etag = f"revision-{self._revision}"
            self._objects[object_name] = (content, etag)
        return SimpleNamespace(etag=etag)

    def remove_object(self, bucket: str, object_name: str) -> None:
        del bucket
        with self._guard:
            if object_name not in self._objects:
                raise _S3Error("NoSuchKey")
            del self._objects[object_name]
            self.removed_names.append(object_name)

    def copy_object(
        self,
        bucket: str,
        object_name: str,
        source: object,
    ) -> SimpleNamespace:
        del bucket
        source_name = getattr(source, "object_name", None)
        if not isinstance(source_name, str):
            raise AssertionError("fake copy source is missing object_name")
        with self._guard:
            stored = self._objects.get(source_name)
            if stored is None:
                raise _S3Error("NoSuchKey")
            self._revision += 1
            etag = f"revision-{self._revision}"
            self._objects[object_name] = (stored[0], etag)
        return SimpleNamespace(etag=etag)


class _FailingSecondPutMinio(_MemoryMinio):
    def put_object(
        self,
        bucket: str,
        object_name: str,
        data: io.BytesIO,
        length: int,
        *,
        num_parallel_uploads: int,
        part_size: int | None = None,
    ) -> SimpleNamespace:
        if self.put_calls == 1:
            raise _S3Error("InternalError")
        return super().put_object(
            bucket,
            object_name,
            data,
            length,
            num_parallel_uploads=num_parallel_uploads,
            part_size=part_size,
        )


class _BlockingStatMinio(_MemoryMinio):
    def __init__(self, target: int) -> None:
        super().__init__()
        self.target = target
        self.active = 0
        self.max_active = 0
        self.started = threading.Event()
        self.release = threading.Event()

    def stat_object(self, bucket: str, object_name: str) -> _Object:
        with self._guard:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active == self.target:
                self.started.set()
        self.release.wait(timeout=2)
        try:
            return super().stat_object(bucket, object_name)
        finally:
            with self._guard:
                self.active -= 1


class _BlockingPutMinio(_MemoryMinio):
    def __init__(self) -> None:
        super().__init__()
        self.active_puts = 0
        self.max_active_puts = 0
        self.entered = threading.Event()
        self.release = threading.Event()

    def put_object(
        self,
        bucket: str,
        object_name: str,
        data: io.BytesIO,
        length: int,
        *,
        num_parallel_uploads: int,
        part_size: int | None = None,
    ) -> SimpleNamespace:
        with self._guard:
            self.active_puts += 1
            self.max_active_puts = max(self.max_active_puts, self.active_puts)
            self.entered.set()
        self.release.wait(timeout=2)
        try:
            return super().put_object(
                bucket,
                object_name,
                data,
                length,
                num_parallel_uploads=num_parallel_uploads,
                part_size=part_size,
            )
        finally:
            with self._guard:
                self.active_puts -= 1


class _BlockingGetMinio(_MemoryMinio):
    def __init__(self, target: int) -> None:
        super().__init__()
        self.target = target
        self.active_gets = 0
        self.max_active_gets = 0
        self.started = threading.Event()
        self.release = threading.Event()

    def get_object(
        self,
        bucket: str,
        object_name: str,
        offset: int = 0,
        length: int = 0,
    ) -> _Response:
        with self._guard:
            self.active_gets += 1
            self.max_active_gets = max(self.max_active_gets, self.active_gets)
            if self.active_gets == self.target:
                self.started.set()
        self.release.wait(timeout=2)
        try:
            return super().get_object(
                bucket,
                object_name,
                offset=offset,
                length=length,
            )
        finally:
            with self._guard:
                self.active_gets -= 1


class _BlockingRemoveMinio(_MemoryMinio):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def remove_object(self, bucket: str, object_name: str) -> None:
        self.entered.set()
        self.release.wait(timeout=2)
        super().remove_object(bucket, object_name)


class _UnderreportedStatMinio(_MemoryMinio):
    def stat_object(self, bucket: str, object_name: str) -> _Object:
        metadata = super().stat_object(bucket, object_name)
        return _Object(metadata.object_name, 1, metadata.etag)


class _CapacityMinio(_MemoryMinio):
    def __init__(self, target: int) -> None:
        super().__init__()
        self.target = target
        self.active_operations = 0
        self.max_active_operations = 0
        self.started = threading.Event()
        self.release = threading.Event()

    def _enter_operation(self) -> None:
        with self._guard:
            self.active_operations += 1
            self.max_active_operations = max(
                self.max_active_operations,
                self.active_operations,
            )
            if self.active_operations == self.target:
                self.started.set()
        self.release.wait(timeout=2)

    def _leave_operation(self) -> None:
        with self._guard:
            self.active_operations -= 1

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: str,
        recursive: bool,
        start_after: str | None = None,
    ) -> Iterator[_Object]:
        self._enter_operation()
        try:
            return super().list_objects(
                bucket,
                prefix=prefix,
                recursive=recursive,
                start_after=start_after,
            )
        finally:
            self._leave_operation()

    def put_object(
        self,
        bucket: str,
        object_name: str,
        data: io.BytesIO,
        length: int,
        *,
        num_parallel_uploads: int,
        part_size: int | None = None,
    ) -> SimpleNamespace:
        self._enter_operation()
        try:
            return super().put_object(
                bucket,
                object_name,
                data,
                length,
                num_parallel_uploads=num_parallel_uploads,
                part_size=part_size,
            )
        finally:
            self._leave_operation()


async def _wait_for(predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(1):
        while not predicate():
            await asyncio.sleep(0)


def _fs(
    client: _MemoryMinio,
    *,
    max_connections: int = 8,
    materialization_concurrency: int = 4,
    heavy_operation_concurrency: int = 4,
) -> WorkspaceFS:
    storage = object_storage_for_fake(
        client,
        "openoctopus",
        max_connections=max_connections,
    )
    return WorkspaceFS(
        storage,
        materialization_concurrency=materialization_concurrency,
        heavy_operation_concurrency=heavy_operation_concurrency,
    )


async def test_object_calls_respect_configured_connection_limit() -> None:
    configured_limit = 2
    client = _BlockingStatMinio(configured_limit)
    target = WorkspaceTarget.personal(uuid4())
    fs = _fs(client, max_connections=configured_limit)

    client.release.set()
    await fs.write(target, "file.txt", b"data", quota_bytes=100)
    client.release.clear()
    client.started.clear()
    client.max_active = 0
    tasks = [asyncio.create_task(fs.stat(target, "file.txt")) for _ in range(6)]
    await _wait_for(client.started.is_set)

    assert client.max_active == configured_limit
    client.release.set()
    await asyncio.gather(*tasks)


async def test_same_workspace_mutations_serialize_and_idle_lock_is_evicted() -> None:
    client = _BlockingPutMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())

    first = asyncio.create_task(fs.write(target, "one.txt", b"1", quota_bytes=100))
    await _wait_for(client.entered.is_set)
    second = asyncio.create_task(fs.write(target, "two.txt", b"2", quota_bytes=100))
    await asyncio.sleep(0.05)

    assert client.active_puts == 1
    client.release.set()
    await asyncio.gather(first, second)

    assert client.max_active_puts == 1
    assert fs.mutation_lock_count == 0


async def test_subtree_lease_blocks_overlap_but_not_sibling_or_other_target() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    other_target = WorkspaceTarget.personal(uuid4())
    lease = await fs.acquire_subtree_lease(
        target,
        "reserved",
        owner="directory-operation",
    )

    overlapping = asyncio.create_task(
        fs.write(target, "reserved/file.txt", b"blocked", quota_bytes=100)
    )
    await _wait_for(lambda: fs._subtree_leases.pending_count == 1)

    sibling = asyncio.create_task(
        fs.write(target, "sibling/file.txt", b"sibling", quota_bytes=100)
    )
    other = asyncio.create_task(
        fs.write(other_target, "reserved/file.txt", b"other", quota_bytes=100)
    )
    await asyncio.wait_for(asyncio.gather(sibling, other), timeout=1)

    assert not overlapping.done()
    await lease.release()
    await asyncio.wait_for(overlapping, timeout=1)

    assert await fs.read(target, "sibling/file.txt") == b"sibling"
    assert await fs.read(other_target, "reserved/file.txt") == b"other"
    assert await fs.read(target, "reserved/file.txt") == b"blocked"


async def test_subtree_owner_mutation_bypasses_its_queued_non_owner() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    owner = "directory-operation"
    lease = await fs.acquire_subtree_lease(target, "reserved", owner=owner)

    ordinary = asyncio.create_task(
        fs.write(target, "reserved/ordinary.txt", b"ordinary", quota_bytes=100)
    )
    await _wait_for(lambda: fs._subtree_leases.pending_count == 1)

    await asyncio.wait_for(
        fs.write(
            target,
            "reserved/owned.txt",
            b"owned",
            quota_bytes=100,
            subtree_owner=owner,
        ),
        timeout=1,
    )

    assert not ordinary.done()
    await lease.release()
    await asyncio.wait_for(ordinary, timeout=1)


async def test_cancelled_multi_target_mutation_releases_partially_acquired_subtrees() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    first_target = WorkspaceTarget.personal(UUID(int=1))
    second_target = WorkspaceTarget.personal(UUID(int=2))
    blocker = await fs.acquire_subtree_lease(
        second_target,
        "reserved",
        owner="directory-operation",
    )
    operation = asyncio.create_task(
        fs.apply_transforms_admitted(
            (
                FileTransform(first_target, "free/file.txt", 100, lambda _: b"first"),
                FileTransform(second_target, "reserved/file.txt", 100, lambda _: b"second"),
            ),
            dry_run=False,
        )
    )
    await _wait_for(
        lambda: fs._subtree_leases.active_count == 2
        and fs._subtree_leases.pending_count == 1
    )

    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation

    assert fs._subtree_leases.active_count == 1
    assert fs._subtree_leases.pending_count == 0
    await asyncio.wait_for(
        fs.write(first_target, "free/file.txt", b"available", quota_bytes=100),
        timeout=1,
    )
    await blocker.release()
    assert fs._subtree_leases.active_count == 0


async def test_inactive_target_rejects_subtree_lease_without_ghost() -> None:
    fs = _fs(_MemoryMinio())
    target = WorkspaceTarget.personal(uuid4())
    await fs.retire_workspace(target)

    with pytest.raises(WorkspaceError) as caught:
        await fs.acquire_subtree_lease(target, "reserved", owner="directory-operation")

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    assert fs._subtree_leases.active_count == 0
    assert fs._subtree_leases.pending_count == 0


async def test_server_source_probe_distinguishes_file_directory_missing_and_invalid_shape() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    prefix = f"users/{target.id}/"
    client._objects[f"{prefix}file.txt"] = (b"file", "file-etag")

    file_probe = await fs.probe_directory_source(target, "file.txt")

    assert file_probe == ServerFileSourceProbe(size=4, fingerprint="file-etag")

    with pytest.raises(WorkspaceError) as caught:
        await fs.probe_directory_source(target, "missing")
    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND

    client._objects[f"{prefix}tree"] = (b"exact", "exact-etag")
    client._objects[f"{prefix}tree/child.txt"] = (b"child", "child-etag")
    with pytest.raises(WorkspaceError) as caught:
        await fs.probe_directory_source(target, "tree")
    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_ERROR


async def test_server_directory_manifest_pages_exact_prefix_without_noise_filter() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    object_prefix = f"users/{target.id}/tree/"
    seeded = {
        ".git/config": b"git",
        "node_modules/pkg/index.js": b"js",
        "plain.txt": b"plain",
    }
    seeded.update({f"bulk/{index:04d}.txt": b"x" for index in range(1001)})
    for index, (relative_path, data) in enumerate(seeded.items()):
        client._objects[f"{object_prefix}{relative_path}"] = (data, f"etag-{index}")

    probe = await fs.probe_directory_source(target, "tree")

    assert isinstance(probe, ServerDirectorySourceProbe)
    manifest = probe.manifest
    assert manifest.root_identity is None
    assert {item.relative_path for item in manifest.directories} >= {
        ".git",
        "node_modules",
        "node_modules/pkg",
        "bulk",
    }
    assert {item.relative_path for item in manifest.entries} >= {
        ".git/config",
        "node_modules/pkg/index.js",
        "plain.txt",
    }
    assert all(item.identity is None for item in manifest.directories)
    assert client.list_calls >= 3
    assert all(prefix == object_prefix for prefix in client.list_prefixes)


async def test_server_destination_plan_maps_files_only_and_checks_root_shape() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    manifest = create_directory_manifest(
        root_identity=None,
        directories=(
            DirectoryManifestDirectory(relative_path="empty", identity=None),
            DirectoryManifestDirectory(relative_path="lib", identity=None),
        ),
        entries=(
            DirectoryManifestEntry(relative_path="lib/a.py", size=1, fingerprint="a"),
            DirectoryManifestEntry(relative_path="README.md", size=2, fingerprint="b"),
        ),
    )

    plan = await fs.preflight_directory_destination(target, "backup", manifest)

    assert plan.destination_root == "backup"
    assert plan.mapped_paths == ("backup/README.md", "backup/lib/a.py")
    object_prefix = f"users/{target.id}/"

    client._objects[f"{object_prefix}backup"] = (b"file", "root-etag")
    with pytest.raises(WorkspaceError) as caught:
        await fs.preflight_directory_destination(target, "backup", manifest)
    assert caught.value.code is ErrorCode.WORKSPACE_FILE_CHANGED

    client._objects.pop(f"{object_prefix}backup")
    client._objects[f"{object_prefix}backup/existing.txt"] = (b"existing", "child-etag")
    with pytest.raises(WorkspaceError) as caught:
        await fs.preflight_directory_destination(target, "backup", manifest)
    assert caught.value.code is ErrorCode.WORKSPACE_FILE_CHANGED

    client._objects[f"{object_prefix}backup"] = (b"file", "root-etag")
    with pytest.raises(WorkspaceError) as caught:
        await fs.preflight_directory_destination(target, "backup", manifest)
    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_ERROR


async def test_server_destination_plan_rejects_file_parent_and_oversized_mapping() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    manifest = create_directory_manifest(
        root_identity=None,
        directories=(),
        entries=(DirectoryManifestEntry(relative_path="file.txt", size=1, fingerprint="a"),),
    )
    client._objects[f"users/{target.id}/parent"] = (b"file", "parent-etag")

    with pytest.raises(WorkspaceError) as caught:
        await fs.preflight_directory_destination(target, "parent/backup", manifest)
    assert caught.value.code is ErrorCode.TOOL_NOT_A_DIRECTORY

    with pytest.raises(WorkspaceError) as caught:
        await fs.preflight_directory_destination(target, "x" * 4090, manifest)
    assert caught.value.code is ErrorCode.WORKSPACE_INVALID_REQUEST


async def test_directory_quota_reservation_fences_writes_and_child_commit_consumes_bytes() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    owner = "directory-operation"
    subtree = await fs.acquire_subtree_lease(target, "reserved", owner=owner)
    reservation = await fs.reserve_directory_quota(
        target,
        owner=owner,
        total_bytes=6,
        quota_bytes=10,
    )

    with pytest.raises(WorkspaceError) as caught:
        await fs.write(target, "ordinary.txt", b"12345", quota_bytes=10)
    assert caught.value.code is ErrorCode.WORKSPACE_QUOTA_EXCEEDED

    with pytest.raises(WorkspaceError) as caught:
        await fs.reserve_directory_quota(
            target,
            owner="second-operation",
            total_bytes=5,
            quota_bytes=10,
        )
    assert caught.value.code is ErrorCode.WORKSPACE_QUOTA_EXCEEDED

    sink, temporary_object = await fs.begin_directory_child_upload(
        reservation,
        "reserved/a.bin",
        size=4,
    )
    await sink.write(b"data")
    await sink.finish()
    committed = await fs.commit_directory_child_upload(
        reservation,
        "reserved/a.bin",
        temporary_object,
        size=4,
    )

    assert committed.created is True
    assert committed.etag
    assert reservation.remaining_bytes == 2
    await fs.write(target, "ordinary.txt", b"1234", quota_bytes=10)

    await reservation.release()
    await reservation.release()
    await subtree.release()
    assert fs.directory_quota_reservation_count == 0


async def test_active_directory_reservation_fences_upload_patch_and_single_file_transfer() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    source_target = WorkspaceTarget.personal(uuid4())
    destination_target = WorkspaceTarget.personal(uuid4())
    client._objects[f"users/{source_target.id}/source.bin"] = (b"12345", "source-etag")
    reservation = await fs.reserve_directory_quota(
        destination_target,
        owner="directory-operation",
        total_bytes=6,
        quota_bytes=10,
    )

    with pytest.raises(WorkspaceError) as caught:
        await fs.begin_transfer_upload(
            destination_target,
            "upload.bin",
            size=5,
            quota_bytes=10,
        )
    assert caught.value.code is ErrorCode.WORKSPACE_QUOTA_EXCEEDED

    temporary = "_openoctopus-transfers/ordinary"
    sink = fs._storage.begin_upload(temporary, length=5)
    await sink.write(b"12345")
    await sink.finish()
    with pytest.raises(WorkspaceError) as caught:
        await fs.commit_uploaded_object(
            destination_target,
            "commit.bin",
            temporary,
            size=5,
            quota_bytes=10,
        )
    assert caught.value.code is ErrorCode.WORKSPACE_QUOTA_EXCEEDED

    with pytest.raises(WorkspaceError) as caught:
        await fs.apply_transforms_admitted(
            (
                FileTransform(
                    destination_target,
                    "patch.bin",
                    10,
                    lambda _: b"12345",
                ),
            ),
            dry_run=False,
        )
    assert caught.value.code is ErrorCode.WORKSPACE_QUOTA_EXCEEDED

    with pytest.raises(WorkspaceError) as caught:
        await fs.transfer_server_to_server(
            source_target,
            "source.bin",
            destination_target,
            "copied.bin",
            user_id=uuid4(),
            quota_bytes=10,
            mode="copy",
        )
    assert caught.value.code is ErrorCode.WORKSPACE_QUOTA_EXCEEDED

    await reservation.release()


async def test_directory_child_temp_uses_existing_restart_recovery_prefix() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    reservation = await fs.reserve_directory_quota(
        target,
        owner="directory-operation",
        total_bytes=2,
        quota_bytes=10,
    )
    sink, temporary_object = await fs.begin_directory_child_upload(
        reservation,
        "reserved/file.bin",
        size=2,
    )
    await sink.write(b"xx")
    await sink.finish()

    removed = await fs._storage.recover_transfer_uploads()

    assert removed == 1
    assert temporary_object not in client._objects
    await reservation.release()


async def test_conditional_cleanup_deletes_only_matching_etag() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    written = await fs.write(target, "tree/file.txt", b"data", quota_bytes=100)

    assert (
        await fs.conditional_delete_file(
            target,
            "tree/file.txt",
            expected_etag="different",
        )
        == "mismatch"
    )
    assert await fs.read(target, "tree/file.txt") == b"data"
    assert (
        await fs.conditional_delete_file(
            target,
            "tree/file.txt",
            expected_etag=written.etag,
        )
        == "deleted"
    )
    assert (
        await fs.conditional_delete_file(
            target,
            "tree/file.txt",
            expected_etag=written.etag,
        )
        == "missing"
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "write",
        "write_collected_upload",
        "commit_uploaded_object",
        "edit_materialized",
        "edit_optional_materialized",
        "delete_file",
        "delete_folder",
        "apply_transforms_admitted",
        "purge_workspace",
        "retire_workspace",
        "reactivate_workspace",
        "forget_workspace",
    ],
)
async def test_every_workspace_mutation_participates_in_subtree_leases(
    mutation: str,
) -> None:
    fs = WorkspaceFS(AsyncMock())
    target = WorkspaceTarget.personal(uuid4())
    lease = await fs.acquire_subtree_lease(target, "reserved", owner="directory-operation")

    operation: Awaitable[object]
    if mutation == "write":
        operation = fs.write(target, "reserved/file.txt", b"data", quota_bytes=100)
    elif mutation == "write_collected_upload":
        operation = fs.write_collected_upload(
            target,
            "reserved/file.txt",
            b"data",
            quota_bytes=100,
        )
    elif mutation == "commit_uploaded_object":
        operation = fs.commit_uploaded_object(
            target,
            "reserved/file.txt",
            "temporary-object",
            size=4,
            quota_bytes=100,
        )
    elif mutation == "edit_materialized":
        operation = fs.edit_materialized(
            target,
            "reserved/file.txt",
            lambda data: data,
            quota_bytes=100,
        )
    elif mutation == "edit_optional_materialized":
        operation = fs.edit_optional_materialized(
            target,
            "reserved/file.txt",
            lambda data: data or b"data",
            quota_bytes=100,
        )
    elif mutation == "delete_file":
        operation = fs.delete_file(target, "reserved/file.txt")
    elif mutation == "delete_folder":
        operation = fs.delete_folder(target, "reserved/folder")
    elif mutation == "apply_transforms_admitted":
        operation = fs.apply_transforms_admitted(
            (FileTransform(target, "reserved/file.txt", 100, lambda _: b"data"),),
            dry_run=False,
        )
    elif mutation == "purge_workspace":
        operation = fs.purge_workspace(target)
    elif mutation == "retire_workspace":
        operation = fs.retire_workspace(target)
    elif mutation == "reactivate_workspace":
        operation = fs.reactivate_workspace(target)
    elif mutation == "forget_workspace":
        operation = fs.forget_workspace(target)
    else:
        raise AssertionError(f"Unhandled mutation: {mutation}")

    task = asyncio.ensure_future(operation)
    await _wait_for(lambda: fs._subtree_leases.pending_count == 1)
    assert not task.done()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert fs._subtree_leases.pending_count == 0
    await lease.release()


async def test_multi_file_transform_validates_every_edit_before_first_write() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "a.txt", b"old-a", quota_bytes=100)
    await fs.write(target, "b.txt", b"old-b", quota_bytes=100)
    writes_before = client.put_calls

    def reject(_: bytes | None) -> bytes:
        raise WorkspaceError(ErrorCode.TOOL_NO_MATCH, "missing")

    with pytest.raises(WorkspaceError) as caught:
        await fs.apply_transforms(
            (
                FileTransform(target, "a.txt", 100, lambda _: b"new-a"),
                FileTransform(target, "b.txt", 100, reject),
            ),
            dry_run=False,
        )

    assert caught.value.code is ErrorCode.TOOL_NO_MATCH
    assert client.put_calls == writes_before
    assert client._objects[f"users/{target.id}/a.txt"][0] == b"old-a"


async def test_multi_file_transform_dry_run_returns_sizes_without_writing() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "a.txt", b"old", quota_bytes=100)
    writes_before = client.put_calls

    results = await fs.apply_transforms(
        (FileTransform(target, "a.txt", 100, lambda _: b"updated"),),
        dry_run=True,
    )

    assert results[0].size == 7
    assert client.put_calls == writes_before


async def test_multi_file_transform_reports_partial_storage_commit_count() -> None:
    client = _FailingSecondPutMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())

    with pytest.raises(WorkspaceError) as caught:
        await fs.apply_transforms(
            (
                FileTransform(target, "a.txt", 100, lambda _: b"first"),
                FileTransform(target, "b.txt", 100, lambda _: b"second"),
            ),
            dry_run=False,
        )

    assert "after 1 edits committed" in caught.value.message
    assert client._objects[f"users/{target.id}/a.txt"][0] == b"first"
    assert f"users/{target.id}/b.txt" not in client._objects


async def test_multi_file_transform_has_an_aggregate_materialization_limit() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    writes_before = client.put_calls

    with pytest.raises(WorkspaceError) as caught:
        await fs.apply_transforms(
            (
                FileTransform(target, "a.txt", 100 * 1024 * 1024, lambda _: b"a" * (5 << 20)),
                FileTransform(target, "b.txt", 100 * 1024 * 1024, lambda _: b"b" * (5 << 20)),
            ),
            dry_run=False,
        )

    assert caught.value.code is ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT
    assert client.put_calls == writes_before


async def test_waiter_lease_prevents_lock_eviction_during_handoff() -> None:
    locks = KeyedLockManager()
    key = "workspace"
    first_acquired = asyncio.Event()
    second_acquired = asyncio.Event()
    third_acquired = asyncio.Event()
    release_first = asyncio.Event()
    release_second = asyncio.Event()
    release_third = asyncio.Event()
    active = 0
    max_active = 0

    async def participant(acquired: asyncio.Event, release: asyncio.Event) -> None:
        nonlocal active, max_active
        async with locks.hold(key):
            active += 1
            max_active = max(max_active, active)
            acquired.set()
            await release.wait()
            active -= 1

    first = asyncio.create_task(participant(first_acquired, release_first))
    await first_acquired.wait()
    second = asyncio.create_task(participant(second_acquired, release_second))
    await _wait_for(lambda: locks._entries[key].leases == 2)
    leased_entry = locks._entries[key]

    release_first.set()
    await second_acquired.wait()
    await first

    assert locks._entries[key] is leased_entry
    third = asyncio.create_task(participant(third_acquired, release_third))
    await _wait_for(lambda: locks._entries[key].leases == 2)
    await asyncio.sleep(0.05)
    assert not third_acquired.is_set()

    release_second.set()
    await third_acquired.wait()
    release_third.set()
    await asyncio.gather(second, third)

    assert max_active == 1
    assert locks.entry_count == 0


async def test_cancelled_waiter_does_not_evict_held_lock() -> None:
    locks = KeyedLockManager()
    key = "workspace"
    holder_acquired = asyncio.Event()
    release_holder = asyncio.Event()

    async def holder() -> None:
        async with locks.hold(key):
            holder_acquired.set()
            await release_holder.wait()

    async def wait_for_lock() -> None:
        async with locks.hold(key):
            pass

    held = asyncio.create_task(holder())
    await holder_acquired.wait()
    waiter = asyncio.create_task(wait_for_lock())
    await _wait_for(lambda: locks._entries[key].leases == 2)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert locks.entry_count == 1
    assert locks._entries[key].leases == 1
    release_holder.set()
    await held
    assert locks.entry_count == 0


async def test_cancellation_keeps_object_slot_until_worker_exits() -> None:
    storage = ObjectStorage(SimpleNamespace(), "openoctopus", max_connections=1)
    first_started = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()

    def blocking_operation() -> str:
        first_started.set()
        release_first.wait(timeout=2)
        return "first"

    def next_operation() -> str:
        second_started.set()
        return "second"

    first = asyncio.create_task(storage.execute(blocking_operation))
    await _wait_for(first_started.is_set)
    first.cancel()
    try:
        await asyncio.sleep(0.05)
        assert not first.done()
        second = asyncio.create_task(storage.execute(next_operation))
        await asyncio.sleep(0.05)
        assert not second_started.is_set()
    finally:
        release_first.set()

    with pytest.raises(asyncio.CancelledError):
        await first
    assert await second == "second"


async def test_detached_cancellation_returns_but_keeps_object_slot() -> None:
    storage = ObjectStorage(SimpleNamespace(), "openoctopus", max_connections=1)
    first_started = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()

    def blocking_operation() -> str:
        first_started.set()
        release_first.wait(timeout=2)
        return "first"

    def next_operation() -> str:
        second_started.set()
        return "second"

    first = asyncio.create_task(storage.execute_detached_on_cancel(blocking_operation))
    await _wait_for(first_started.is_set)
    first.cancel()
    await asyncio.sleep(0.05)
    second = asyncio.create_task(storage.execute(next_operation))
    try:
        assert first.done()
        await asyncio.sleep(0.05)
        assert not second_started.is_set()
    finally:
        release_first.set()

    with pytest.raises(asyncio.CancelledError):
        await first
    assert await second == "second"


async def test_cancelled_write_holds_workspace_lock_until_put_finishes() -> None:
    client = _BlockingPutMinio()
    fs = _fs(client, max_connections=2)
    target = WorkspaceTarget.personal(uuid4())

    first = asyncio.create_task(fs.write(target, "first.txt", b"first", quota_bytes=100))
    await _wait_for(client.entered.is_set)
    first.cancel()
    second = asyncio.create_task(fs.write(target, "second.txt", b"second", quota_bytes=100))
    try:
        await asyncio.sleep(0.05)
        assert not first.done()
        assert not second.done()
        assert client.active_puts == 1
    finally:
        client.release.set()

    with pytest.raises(asyncio.CancelledError):
        await first
    await second
    assert client.max_active_puts == 1
    assert fs.mutation_lock_count == 0


async def test_materialization_concurrency_is_bounded() -> None:
    limit = 2
    client = _BlockingGetMinio(limit)
    fs = _fs(client, materialization_concurrency=limit)
    targets = [WorkspaceTarget.personal(uuid4()) for _ in range(5)]
    for target in targets:
        await fs.write(target, "file.txt", b"before", quota_bytes=100)

    tasks = [
        asyncio.create_task(
            fs.edit(
                target,
                "file.txt",
                lambda current: current.replace(b"before", b"after"),
                quota_bytes=100,
            )
        )
        for target in targets
    ]
    await _wait_for(client.started.is_set)

    assert client.max_active_gets == limit
    client.release.set()
    await asyncio.gather(*tasks)


async def test_usage_scan_concurrency_is_bounded_process_wide() -> None:
    limit = 2
    client = _CapacityMinio(limit)
    fs = _fs(client, max_connections=8, heavy_operation_concurrency=limit)
    targets = [WorkspaceTarget.personal(uuid4()) for _ in range(5)]
    tasks = [asyncio.create_task(fs.usage(target)) for target in targets]
    await _wait_for(client.started.is_set)

    assert client.max_active_operations == limit
    client.release.set()
    assert await asyncio.gather(*tasks) == [0] * len(targets)


async def test_write_materialization_concurrency_is_bounded() -> None:
    limit = 2
    client = _BlockingPutMinio()
    fs = _fs(client, max_connections=8, materialization_concurrency=limit)
    targets = [WorkspaceTarget.personal(uuid4()) for _ in range(5)]

    tasks = [
        asyncio.create_task(fs.write(target, "file.txt", b"data", quota_bytes=100))
        for target in targets
    ]
    await _wait_for(lambda: client.max_active_puts == limit)

    assert client.max_active_puts == limit
    client.release.set()
    await asyncio.gather(*tasks)


async def test_edit_rejects_source_larger_than_eight_mib_before_download() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(
        target,
        "large.txt",
        b"x" * (MAX_EDIT_BYTES + 1),
        quota_bytes=64 * 1024 * 1024,
    )

    with pytest.raises(WorkspaceError) as caught:
        await fs.edit(
            target,
            "large.txt",
            lambda current: current,
            quota_bytes=64 * 1024 * 1024,
        )

    assert MAX_EDIT_BYTES == 8 * 1024 * 1024
    assert caught.value.code == ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT
    assert client.get_calls == 0


async def test_edit_rejects_result_larger_than_eight_mib_before_upload() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "file.txt", b"small", quota_bytes=64 * 1024 * 1024)
    puts_before_edit = client.put_calls

    with pytest.raises(WorkspaceError) as caught:
        await fs.edit(
            target,
            "file.txt",
            lambda current: b"x" * (MAX_EDIT_BYTES + 1),
            quota_bytes=64 * 1024 * 1024,
        )

    assert caught.value.code == ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT
    assert client.put_calls == puts_before_edit


async def test_edit_rejects_oversized_received_body_before_transform() -> None:
    client = _UnderreportedStatMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(
        target,
        "large.txt",
        b"x" * (MAX_EDIT_BYTES + 1),
        quota_bytes=64 * 1024 * 1024,
    )
    transformed = False

    def transform(current: bytes) -> bytes:
        nonlocal transformed
        transformed = True
        return current

    with pytest.raises(WorkspaceError) as caught:
        await fs.edit(
            target,
            "large.txt",
            transform,
            quota_bytes=64 * 1024 * 1024,
        )

    assert caught.value.code == ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT
    assert not transformed


async def test_concurrent_quota_checks_use_the_latest_committed_usage() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    start = asyncio.Event()

    async def write(path: str) -> object:
        await start.wait()
        return await fs.write(target, path, b"123456", quota_bytes=10)

    tasks = [asyncio.create_task(write(path)) for path in ("one.txt", "two.txt")]
    start.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)

    assert sum(not isinstance(result, BaseException) for result in results) == 1
    assert sum(isinstance(result, WorkspaceError) for result in results) == 1
    assert (
        sum(item.size for item in client.list_objects("openoctopus", prefix="", recursive=True))
        <= 10
    )


async def test_stale_if_match_is_rejected_without_overwriting() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    initial = await fs.write(target, "file.txt", b"first", quota_bytes=100)
    puts_before_conflict = client.put_calls

    with pytest.raises(WorkspaceError) as caught:
        await fs.write(
            target,
            "file.txt",
            b"stale replacement",
            quota_bytes=100,
            if_match=f"not-{initial.etag}",
        )

    assert caught.value.code == ErrorCode.WORKSPACE_FILE_CHANGED
    assert client.put_calls == puts_before_conflict
    assert (await fs.read(target, "file.txt")) == b"first"


async def test_edit_rejects_stale_if_match_before_transform() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    written = await fs.write(target, "file.txt", b"first", quota_bytes=100)
    transformed = False

    def transform(current: bytes) -> bytes:
        nonlocal transformed
        transformed = True
        return current + b" edited"

    with pytest.raises(WorkspaceError) as caught:
        await fs.edit(
            target,
            "file.txt",
            transform,
            quota_bytes=100,
            if_match=f"stale-{written.etag}",
        )

    assert caught.value.code is ErrorCode.WORKSPACE_FILE_CHANGED
    assert not transformed


async def test_personal_and_shared_targets_use_distinct_internal_prefixes() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    identity = uuid4()
    personal = WorkspaceTarget.personal(identity)
    shared = WorkspaceTarget.shared(identity)

    personal_result = await fs.write(personal, "note.txt", b"personal", quota_bytes=100)
    shared_result = await fs.write(shared, "note.txt", b"shared", quota_bytes=100)

    assert set(client._objects) == {
        f"users/{identity}/note.txt",
        f"workspaces/{identity}/note.txt",
    }
    assert not hasattr(personal_result, "object_name")
    assert not hasattr(shared_result, "object_name")


async def test_read_with_metadata_uses_etag_from_the_get_response() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    written = await fs.write(target, "file.txt", b"contents", quota_bytes=100)
    stat_calls_before_read = client.stat_calls

    stored = await fs.read_with_metadata(target, "file.txt")

    assert stored.data == b"contents"
    assert stored.etag == written.etag
    assert client.stat_calls == stat_calls_before_read


async def test_server_move_uses_open_source_etag_for_conditional_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_etag = "source-v1"
    delete_calls: list[str | None] = []

    class Source:
        size = 7
        etag = "source-v1"

        async def read(self) -> bytes:
            nonlocal current_etag
            if current_etag == "source-v1":
                current_etag = "source-v2"
                return b"payload"
            return b""

        async def aclose(self) -> None:
            pass

    class Sink:
        async def write(self, chunk: bytes) -> None:
            assert chunk == b"payload"

        async def finish(self) -> None:
            pass

        async def abort(self) -> None:
            pass

    fs = WorkspaceFS(AsyncMock())
    source = Source()
    sink = Sink()
    monkeypatch.setattr(fs, "open_stream", AsyncMock(return_value=source))
    monkeypatch.setattr(fs, "begin_transfer_upload", AsyncMock(return_value=(sink, "temp")))
    commit = AsyncMock()
    monkeypatch.setattr(fs, "commit_uploaded_object", commit)

    async def conditional_delete(
        _target: WorkspaceTarget,
        _path: str,
        *,
        if_match: str | None = None,
    ) -> None:
        delete_calls.append(if_match)
        if if_match != current_etag:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_CHANGED,
                "Workspace file changed after it was read",
            )

    monkeypatch.setattr(fs, "delete_file", conditional_delete)
    target = WorkspaceTarget.personal(uuid4())

    transferred, digest, warnings = await fs.transfer_server_to_server(
        target,
        "source.txt",
        target,
        "destination.txt",
        user_id=uuid4(),
        quota_bytes=100,
        mode="move",
    )

    assert transferred == 7
    assert digest
    assert warnings == ("source_delete_failed",)
    assert delete_calls == ["source-v1"]
    commit.assert_awaited_once()


async def test_server_move_finishes_after_publish_observes_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Source:
        size = 7
        etag = "source-v1"

        def __init__(self) -> None:
            self._read = False

        async def read(self) -> bytes:
            if self._read:
                return b""
            self._read = True
            return b"payload"

        async def aclose(self) -> None:
            pass

    class Sink:
        def __init__(self) -> None:
            self.aborted = False

        async def write(self, chunk: bytes) -> None:
            assert chunk == b"payload"

        async def finish(self) -> None:
            pass

        async def abort(self) -> None:
            self.aborted = True

    fs = WorkspaceFS(AsyncMock())
    source = Source()
    sink = Sink()
    delete = AsyncMock()
    monkeypatch.setattr(fs, "open_stream", AsyncMock(return_value=source))
    monkeypatch.setattr(fs, "begin_transfer_upload", AsyncMock(return_value=(sink, "temp")))
    monkeypatch.setattr(
        fs,
        "commit_uploaded_object",
        AsyncMock(side_effect=UploadCommittedAfterCancellation),
    )
    monkeypatch.setattr(fs, "delete_file", delete)
    target = WorkspaceTarget.personal(uuid4())

    transferred, digest, warnings = await fs.transfer_server_to_server(
        target,
        "source.txt",
        target,
        "destination.txt",
        user_id=uuid4(),
        quota_bytes=100,
        mode="move",
    )

    assert transferred == 7
    assert digest
    assert warnings == ()
    assert sink.aborted is False
    delete.assert_awaited_once_with(target, "source.txt", if_match="source-v1")


async def test_server_to_server_admission_reserves_two_storage_connections_per_transfer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = AsyncMock()
    storage.max_connections = 4
    fs = WorkspaceFS(
        storage,
        server_transfer_max_concurrency_per_user=2,
        server_transfer_queue_timeout_seconds=0.2,
    )
    opened = 0
    two_open = asyncio.Event()
    release = asyncio.Event()

    class Source:
        size = 0
        etag = "source-v1"

        async def read(self) -> bytes:
            await release.wait()
            return b""

        async def aclose(self) -> None:
            pass

    class Sink:
        object_name = "temporary"

        async def write(self, _chunk: bytes) -> None:
            pass

        async def finish(self) -> None:
            pass

        async def abort(self) -> None:
            pass

    async def open_stream(_target: WorkspaceTarget, _path: str) -> Source:
        nonlocal opened
        opened += 1
        if opened == 2:
            two_open.set()
        return Source()

    monkeypatch.setattr(fs, "open_stream", open_stream)
    monkeypatch.setattr(
        fs,
        "begin_transfer_upload",
        AsyncMock(return_value=(Sink(), "temporary")),
    )
    monkeypatch.setattr(fs, "commit_uploaded_object", AsyncMock())
    target = WorkspaceTarget.personal(uuid4())
    tasks = [
        asyncio.create_task(
            fs.transfer_server_to_server(
                target,
                f"source-{index}",
                target,
                f"destination-{index}",
                user_id=uuid4(),
                quota_bytes=100,
                mode="copy",
            )
        )
        for index in range(3)
    ]

    await two_open.wait()
    await asyncio.sleep(0)
    assert opened == 2
    release.set()
    await asyncio.gather(*tasks)
    assert opened == 3


async def test_uploaded_object_publish_propagates_cancellation_after_true_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = AsyncMock()
    storage.max_connections = 4
    published = asyncio.Event()
    release_publish = asyncio.Event()
    metadata = ObjectMetadata("destination", 1, "destination-etag")

    async def metadata_pages(*_args: object, **_kwargs: object):
        yield ()

    async def promote(*_args: object, **_kwargs: object) -> ObjectMetadata:
        published.set()
        await release_publish.wait()
        return metadata

    storage.promote_if_absent.side_effect = promote
    monkeypatch.setattr("openctopus_server.workspace.fs._metadata_pages", metadata_pages)
    fs = WorkspaceFS(storage)
    target = WorkspaceTarget.personal(uuid4())
    task = asyncio.create_task(
        fs.commit_uploaded_object(
            target,
            "destination",
            "temporary",
            size=1,
            quota_bytes=100,
        )
    )
    await published.wait()
    task.cancel()
    await asyncio.sleep(0)

    assert task.done() is False
    release_publish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    storage.promote_if_absent.assert_awaited_once()


async def test_uploaded_object_marks_issued_only_at_irreversible_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = AsyncMock()
    storage.max_connections = 4
    preflight_started = asyncio.Event()
    release_preflight = asyncio.Event()
    issued: list[None] = []

    async def metadata_pages(*_args: object, **_kwargs: object):
        preflight_started.set()
        await release_preflight.wait()
        yield ()

    async def promote(*_args: object, **_kwargs: object) -> ObjectMetadata:
        assert issued == [None]
        return ObjectMetadata("destination", 1, "destination-etag")

    storage.promote_if_absent.side_effect = promote
    monkeypatch.setattr("openctopus_server.workspace.fs._metadata_pages", metadata_pages)
    fs = WorkspaceFS(storage)
    task = asyncio.create_task(
        fs.commit_uploaded_object(
            WorkspaceTarget.personal(uuid4()),
            "destination",
            "temporary",
            size=1,
            quota_bytes=100,
            on_issued=lambda: issued.append(None),
        )
    )

    await preflight_started.wait()
    assert issued == []
    release_preflight.set()
    await task

    assert issued == [None]


async def test_uploaded_object_cleanup_does_not_hold_committed_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = AsyncMock()
    storage.max_connections = 4
    storage.promote_if_absent.return_value = ObjectMetadata(
        "destination",
        1,
        "destination-etag",
    )
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def metadata_pages(*_args: object, **_kwargs: object):
        yield ()

    async def delete(_object_name: str) -> None:
        cleanup_started.set()
        await release_cleanup.wait()

    storage.delete.side_effect = delete
    monkeypatch.setattr("openctopus_server.workspace.fs._metadata_pages", metadata_pages)
    fs = WorkspaceFS(storage)

    result = await asyncio.wait_for(
        fs.commit_uploaded_object(
            WorkspaceTarget.personal(uuid4()),
            "destination",
            "temporary",
            size=1,
            quota_bytes=100,
        ),
        timeout=0.1,
    )

    assert result.etag == "destination-etag"
    await cleanup_started.wait()
    release_cleanup.set()


async def test_list_dir_returns_public_metadata_without_reading_file_contents() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "docs/readme.md", b"readme", quota_bytes=100)
    await fs.write(target, "docs/nested/child.txt", b"child", quota_bytes=100)
    await fs.write(target, "outside.txt", b"outside", quota_bytes=100)
    list_calls_before = client.list_calls
    get_calls_before = client.get_calls

    assert hasattr(fs, "list_dir"), "WorkspaceFS.list_dir is required"
    entries = await fs.list_dir(target, "docs")

    assert [(entry.path, entry.is_directory, entry.size) for entry in entries] == [
        ("docs/nested", True, None),
        ("docs/readme.md", False, 6),
    ]
    assert all(not hasattr(entry, "object_name") for entry in entries)
    assert client.list_calls == list_calls_before + 1
    assert client.list_prefixes[-1].endswith("/docs/")
    assert client.get_calls == get_calls_before


async def test_non_recursive_list_skips_noise_directories() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, ".git/config", b"hidden", quota_bytes=100)
    await fs.write(target, "node_modules/pkg/index.js", b"hidden", quota_bytes=100)
    await fs.write(target, "src/app.py", b"visible", quota_bytes=100)

    page = await fs.list_dir_page(target, "", limit=20, offset=0)

    assert [(entry.path, entry.is_directory) for entry in page.items] == [("src", True)]


async def test_folder_with_only_noise_children_lists_as_empty() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "project/node_modules/pkg/index.js", b"hidden", quota_bytes=100)

    page = await fs.list_dir_page(target, "project", limit=20, offset=0)

    assert page.items == ()


async def test_non_recursive_list_keeps_files_named_like_noise_directories() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "build", b"file", quota_bytes=100)

    page = await fs.list_dir_page(target, "", limit=20, offset=0)

    assert [(entry.path, entry.is_directory) for entry in page.items] == [("build", False)]


async def test_skill_discovery_can_include_noise_named_directories() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "skills/build/SKILL.md", b"manifest", quota_bytes=100)

    page = await fs.list_dir_page(
        target,
        "skills",
        limit=20,
        offset=0,
        include_noise_directories=True,
    )

    assert [(entry.path, entry.is_directory) for entry in page.items] == [("skills/build", True)]


async def test_usage_sums_workspace_metadata_without_downloading_objects() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    other_target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "one.bin", b"123", quota_bytes=100)
    await fs.write(target, "nested/two.bin", b"45678", quota_bytes=100)
    await fs.write(other_target, "excluded.bin", b"not-counted", quota_bytes=100)
    list_calls_before = client.list_calls
    get_calls_before = client.get_calls

    assert hasattr(fs, "usage"), "WorkspaceFS.usage is required"
    usage = await fs.usage(target)

    assert usage == 8
    assert client.list_calls == list_calls_before + 1
    assert client.get_calls == get_calls_before


async def test_bounded_read_requests_only_the_selected_byte_range() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "data.bin", b"0123456789", quota_bytes=100)

    selected = await fs.read(target, "data.bin", offset=3, length=4)

    assert selected == b"3456"
    assert client.get_ranges[-1] == (3, 4)


async def test_delete_folder_matches_a_slash_delimited_prefix_only() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    for path in ("foo/a.txt", "foo/nested/b.txt", "foobar/keep.txt"):
        await fs.write(target, path, b"data", quota_bytes=100)

    await fs.delete_folder(target, "foo")

    for deleted_path in ("foo/a.txt", "foo/nested/b.txt"):
        with pytest.raises(WorkspaceError) as caught:
            await fs.read(target, deleted_path)
        assert caught.value.code == ErrorCode.WORKSPACE_NOT_FOUND
    assert await fs.read(target, "foobar/keep.txt") == b"data"
    assert all("/foo/" in name for name in client.removed_names)


async def test_folder_delete_serializes_with_write_below_the_prefix() -> None:
    client = _BlockingRemoveMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "folder/old.txt", b"old", quota_bytes=100)
    puts_before_race = client.put_calls

    deleting = asyncio.create_task(fs.delete_folder(target, "folder"))
    await _wait_for(client.entered.is_set)
    writing = asyncio.create_task(fs.write(target, "folder/new.txt", b"new", quota_bytes=100))
    try:
        await asyncio.sleep(0.05)
        assert client.put_calls == puts_before_race
        assert not writing.done()
    finally:
        client.release.set()

    await asyncio.gather(deleting, writing)

    assert await fs.read(target, "folder/new.txt") == b"new"
    with pytest.raises(WorkspaceError) as caught:
        await fs.read(target, "folder/old.txt")
    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND


async def test_workspace_purge_rejects_an_already_authorized_waiting_write() -> None:
    client = _BlockingRemoveMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "old.txt", b"old", quota_bytes=100)

    purging = asyncio.create_task(fs.purge_workspace(target))
    await _wait_for(client.entered.is_set)
    stale_write = asyncio.create_task(fs.write(target, "orphan.txt", b"orphan", quota_bytes=100))
    try:
        await asyncio.sleep(0.05)
        assert not stale_write.done()
    finally:
        client.release.set()

    await purging
    with pytest.raises(WorkspaceError) as caught:
        await stale_write

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    assert client._objects == {}


async def test_edit_serializes_with_unconditional_write() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "file.txt", b"before", quota_bytes=100)
    transform_entered = threading.Event()
    release_transform = threading.Event()

    def transform(current: bytes) -> bytes:
        assert current == b"before"
        transform_entered.set()
        release_transform.wait(timeout=2)
        return b"edited"

    editing = asyncio.create_task(fs.edit(target, "file.txt", transform, quota_bytes=100))
    await _wait_for(transform_entered.is_set)
    writing = asyncio.create_task(fs.write(target, "file.txt", b"writer", quota_bytes=100))
    try:
        await asyncio.sleep(0.05)
        assert not writing.done()
    finally:
        release_transform.set()

    await asyncio.gather(editing, writing)

    assert await fs.read(target, "file.txt") == b"writer"


async def test_repeated_edit_cancellation_waits_for_transform_thread() -> None:
    client = _MemoryMinio()
    fs = _fs(client, materialization_concurrency=1)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "file.txt", b"before", quota_bytes=100)
    transform_entered = threading.Event()
    release_transform = threading.Event()

    def transform(current: bytes) -> bytes:
        assert current == b"before"
        transform_entered.set()
        assert release_transform.wait(timeout=2)
        return b"edited"

    editing = asyncio.create_task(fs.edit(target, "file.txt", transform, quota_bytes=100))
    await _wait_for(transform_entered.is_set)
    editing.cancel()
    await asyncio.sleep(0)
    editing.cancel()
    await asyncio.sleep(0)
    editing.cancel()
    await asyncio.sleep(0)

    try:
        assert not editing.done()
        assert fs.mutation_lock_count == 1
    finally:
        release_transform.set()

    with pytest.raises(asyncio.CancelledError):
        await editing
    assert fs.mutation_lock_count == 0


async def test_500_personal_workspaces_stay_isolated_with_bounded_object_io() -> None:
    configured_limit = 8
    materialization_limit = 4
    client = _CapacityMinio(materialization_limit)
    fs = _fs(
        client,
        max_connections=configured_limit,
        materialization_concurrency=materialization_limit,
    )
    targets = [WorkspaceTarget.personal(uuid4()) for _ in range(500)]
    tasks = [
        asyncio.create_task(
            fs.write(
                target,
                "capacity.txt",
                f"workspace-{index}".encode(),
                quota_bytes=100,
            )
        )
        for index, target in enumerate(targets)
    ]

    try:
        await _wait_for(client.started.is_set)

        async def event_loop_probe() -> int:
            ticks = 0
            for _ in range(25):
                await asyncio.sleep(0)
                ticks += 1
            return ticks

        assert await asyncio.wait_for(event_loop_probe(), timeout=0.25) == 25
        assert client.max_active_operations == materialization_limit
    finally:
        client.release.set()

    await asyncio.gather(*tasks)

    assert client.max_active_operations <= configured_limit
    assert len(client._objects) == 500
    assert {key: content for key, (content, _etag) in client._objects.items()} == {
        f"users/{target.id}/capacity.txt": f"workspace-{index}".encode()
        for index, target in enumerate(targets)
    }
    assert fs.mutation_lock_count == 0


async def test_delete_folder_rejects_workspace_root() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "keep.txt", b"data", quota_bytes=100)

    with pytest.raises(WorkspaceError) as caught:
        await fs.delete_folder(target, "")

    assert caught.value.code == ErrorCode.WORKSPACE_BLOCKED_PATH
    assert await fs.read(target, "keep.txt") == b"data"
    assert client.removed_names == []


@pytest.mark.parametrize("path", [".", "a/./b", "a//b", "a/../b"])
async def test_file_operations_reject_noncanonical_paths(path: str) -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())

    with pytest.raises(WorkspaceError) as caught:
        await fs.write(target, path, b"data", quota_bytes=100)

    assert caught.value.code is ErrorCode.WORKSPACE_BLOCKED_PATH


async def test_file_and_folder_names_cannot_overlap() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "file", b"data", quota_bytes=100)
    await fs.write(target, "folder/child", b"data", quota_bytes=100)

    with pytest.raises(WorkspaceError) as file_parent:
        await fs.write(target, "file/child", b"data", quota_bytes=100)
    with pytest.raises(WorkspaceError) as folder_target:
        await fs.write(target, "folder", b"data", quota_bytes=100)

    assert file_parent.value.code == ErrorCode.TOOL_NOT_A_DIRECTORY
    assert folder_target.value.code == ErrorCode.TOOL_IS_DIRECTORY


async def test_delete_rejects_file_folder_kind_mismatches() -> None:
    client = _MemoryMinio()
    fs = _fs(client)
    target = WorkspaceTarget.personal(uuid4())
    await fs.write(target, "file.txt", b"data", quota_bytes=100)
    await fs.write(target, "folder/child.txt", b"data", quota_bytes=100)

    with pytest.raises(WorkspaceError) as file_as_folder:
        await fs.delete_folder(target, "file.txt")
    with pytest.raises(WorkspaceError) as folder_as_file:
        await fs.delete_file(target, "folder")

    assert file_as_folder.value.code == ErrorCode.TOOL_IS_FILE
    assert folder_as_file.value.code == ErrorCode.TOOL_IS_DIRECTORY


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (_S3Error("NoSuchKey"), ErrorCode.WORKSPACE_NOT_FOUND),
        (_S3Error("NoSuchObject"), ErrorCode.WORKSPACE_NOT_FOUND),
        (_S3Error("NoSuchBucket"), ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE),
        (_S3Error("AccessDenied"), ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE),
        (_S3Error("InvalidAccessKeyId"), ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE),
        (_S3Error("InternalError"), ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE),
        (TimeoutError("internal/key/secret.txt"), ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE),
        (ConnectionError("internal/key/secret.txt"), ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE),
        (ValueError("internal/key/secret.txt"), ErrorCode.WORKSPACE_STORAGE_ERROR),
    ],
)
def test_storage_errors_are_normalized_without_internal_details(
    failure: Exception, expected: ErrorCode
) -> None:
    normalized = normalize_storage_error(failure)

    assert normalized.code == expected
    assert "internal/key/secret.txt" not in str(normalized)
