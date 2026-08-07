import io
from collections.abc import Iterator
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from storage_http import object_storage_for_fake

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import WorkspaceFS, WorkspaceTarget
from openctopus_server.workspace.storage import ObjectStorage

_PAGE_SIZE = 1000


@pytest_asyncio.fixture(autouse=True)
async def _truncate_tables() -> None:
    """These unit tests do not need the suite's PostgreSQL cleanup fixture."""
    yield


class _ReadResponse:
    def __init__(self, content: bytes) -> None:
        self._content = content
        self.headers = {"ETag": '"revision-1"'}
        self.read_sizes: list[int | None] = []
        self.closed = False
        self.released = False

    def read(self, size: int | None = None) -> bytes:
        self.read_sizes.append(size)
        if size is None:
            raise AssertionError("response.read() must always have a byte bound")
        return self._content[:size]

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class _ReadClient:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.requests: list[tuple[int, int]] = []
        self.response: _ReadResponse | None = None

    def get_object(
        self,
        bucket: str,
        object_name: str,
        *,
        offset: int,
        length: int,
    ) -> _ReadResponse:
        del bucket, object_name
        self.requests.append((offset, length))
        self.response = _ReadResponse(self.content[offset : offset + length])
        return self.response


class _NoSuchKeyError(Exception):
    code = "NoSuchKey"


class _PagedClient:
    """Rejects any SDK listing that consumes more than one bounded page."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[int, str]] = {}
        self.list_calls = 0
        self.max_items_consumed_per_call = 0
        self.put_calls = 0

    def seed(self, prefix: str, count: int) -> None:
        for index in range(count):
            self.objects[f"{prefix}{index:04d}.txt"] = (1, f"etag-{index}")

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: str,
        recursive: bool,
        start_after: str | None = None,
    ) -> Iterator[SimpleNamespace]:
        del bucket, recursive
        self.list_calls += 1
        consumed = 0
        for name in sorted(self.objects):
            if not name.startswith(prefix) or (start_after is not None and name <= start_after):
                continue
            consumed += 1
            self.max_items_consumed_per_call = max(
                self.max_items_consumed_per_call,
                consumed,
            )
            if consumed > _PAGE_SIZE + 1:
                raise AssertionError("one object listing attempted to materialize the full prefix")
            size, etag = self.objects[name]
            yield SimpleNamespace(object_name=name, size=size, etag=etag)

    def stat_object(self, bucket: str, object_name: str) -> SimpleNamespace:
        del bucket
        stored = self.objects.get(object_name)
        if stored is None:
            raise _NoSuchKeyError()
        size, etag = stored
        return SimpleNamespace(object_name=object_name, size=size, etag=etag)

    def put_object(
        self,
        bucket: str,
        object_name: str,
        data: io.BytesIO,
        length: int,
        *,
        num_parallel_uploads: int,
    ) -> SimpleNamespace:
        del bucket, data
        assert num_parallel_uploads == 1
        self.put_calls += 1
        self.objects[object_name] = (length, "written-etag")
        return SimpleNamespace(etag="written-etag")

    def remove_object(self, bucket: str, object_name: str) -> None:
        del bucket
        del self.objects[object_name]


class _DelimiterClient:
    def __init__(self) -> None:
        self.recursive_values: list[bool] = []

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: str,
        recursive: bool,
        start_after: str | None = None,
    ) -> Iterator[SimpleNamespace]:
        del bucket, start_after
        self.recursive_values.append(recursive)
        if recursive:
            raise AssertionError("non-recursive list must use object-store delimiter mode")
        return iter(
            (
                SimpleNamespace(
                    object_name=f"{prefix}huge/",
                    size=0,
                    etag=None,
                    is_dir=True,
                ),
                SimpleNamespace(
                    object_name=f"{prefix}sibling.txt",
                    size=1,
                    etag="sibling-etag",
                    is_dir=False,
                ),
            )
        )


async def test_read_uses_limit_plus_one_and_never_unbounded_response_read() -> None:
    client = _ReadClient(b"0123456789")
    storage = object_storage_for_fake(client, "openoctopus", max_connections=1)
    try:
        stored = await storage.read("file.txt", offset=2, max_bytes=4)
    finally:
        await storage.close()

    assert stored.data == b"2345"
    assert stored.truncated is True
    assert client.requests == [(2, 5)]
    assert client.response is not None
    assert client.response.read_sizes == [5]
    assert client.response.closed is True
    assert client.response.released is True


async def test_list_page_consumes_only_limit_plus_one_items() -> None:
    client = _PagedClient()
    client.seed("users/example/", 5)
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    try:
        page = await storage.list_page("users/example/", limit=2)
    finally:
        await storage.close()

    assert [item.object_name for item in page.items] == [
        "users/example/0000.txt",
        "users/example/0001.txt",
    ]
    assert page.next_start_after == "users/example/0001.txt"
    assert client.max_items_consumed_per_call == 3


async def test_quota_scan_includes_objects_after_the_first_bounded_page() -> None:
    client = _PagedClient()
    target = WorkspaceTarget.personal(uuid4())
    prefix = f"users/{target.id}/"
    client.seed(prefix, _PAGE_SIZE + 2)
    storage = ObjectStorage(client, "openoctopus", max_connections=2)
    fs = WorkspaceFS(storage)
    try:
        assert await fs.usage(target) == _PAGE_SIZE + 2
        with pytest.raises(WorkspaceError) as caught:
            await fs.write(
                target,
                "new.txt",
                b"x",
                quota_bytes=_PAGE_SIZE + 2,
            )
    finally:
        await storage.close()

    assert caught.value.code == ErrorCode.WORKSPACE_QUOTA_EXCEEDED
    assert client.put_calls == 0
    assert client.list_calls >= 4
    assert client.max_items_consumed_per_call <= _PAGE_SIZE + 1


async def test_folder_delete_and_purge_process_bounded_pages() -> None:
    client = _PagedClient()
    folder_target = WorkspaceTarget.personal(uuid4())
    purge_target = WorkspaceTarget.shared(uuid4())
    folder_prefix = f"users/{folder_target.id}/folder/"
    purge_prefix = f"workspaces/{purge_target.id}/"
    client.seed(folder_prefix, _PAGE_SIZE + 2)
    client.seed(purge_prefix, _PAGE_SIZE + 2)
    storage = ObjectStorage(client, "openoctopus", max_connections=2)
    fs = WorkspaceFS(storage)
    try:
        await fs.delete_folder(folder_target, "folder")
        await fs.purge_workspace(purge_target)
    finally:
        await storage.close()

    assert not any(name.startswith(folder_prefix) for name in client.objects)
    assert not any(name.startswith(purge_prefix) for name in client.objects)
    assert client.max_items_consumed_per_call <= _PAGE_SIZE + 1


async def test_directory_listing_refuses_an_unbounded_result() -> None:
    client = _PagedClient()
    target = WorkspaceTarget.personal(uuid4())
    prefix = f"users/{target.id}/"
    client.seed(prefix, _PAGE_SIZE + 1)
    storage = ObjectStorage(client, "openoctopus", max_connections=2)
    fs = WorkspaceFS(storage)
    try:
        with pytest.raises(WorkspaceError) as caught:
            await fs.list_dir(target)
    finally:
        await storage.close()

    assert caught.value.code == ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE
    assert client.max_items_consumed_per_call <= _PAGE_SIZE + 1


async def test_non_recursive_directory_listing_uses_object_store_delimiter() -> None:
    client = _DelimiterClient()
    target = WorkspaceTarget.personal(uuid4())
    storage = ObjectStorage(client, "openoctopus", max_connections=1)
    fs = WorkspaceFS(storage)
    try:
        entries = await fs.list_dir(target, "docs")
    finally:
        await storage.close()

    assert [(entry.path, entry.is_directory, entry.size) for entry in entries] == [
        ("docs/huge", True, None),
        ("docs/sibling.txt", False, 1),
    ]
    assert client.recursive_values == [False]
