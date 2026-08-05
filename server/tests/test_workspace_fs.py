import asyncio
import io
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import MAX_EDIT_BYTES, WorkspaceFS, WorkspaceTarget
from openctopus_server.workspace.locks import KeyedLockManager
from openctopus_server.workspace.storage import ObjectStorage, normalize_storage_error


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
            objects = list(self._objects.items())
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
    ) -> SimpleNamespace:
        del bucket
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
    ) -> SimpleNamespace:
        with self._guard:
            self.active_puts += 1
            self.max_active_puts = max(self.max_active_puts, self.active_puts)
            self.entered.set()
        self.release.wait(timeout=2)
        try:
            return super().put_object(bucket, object_name, data, length)
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
    ) -> SimpleNamespace:
        self._enter_operation()
        try:
            return super().put_object(bucket, object_name, data, length)
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
) -> WorkspaceFS:
    storage = ObjectStorage(client, "openoctopus", max_connections=max_connections)
    return WorkspaceFS(
        storage,
        materialization_concurrency=materialization_concurrency,
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
