import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio

from openctopus_server.config import get_settings
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import FileTransform, WorkspaceFS, WorkspaceTarget
from openctopus_server.workspace.storage import (
    STARTUP_PROBE_KEY,
    ObjectStorage,
    build_object_storage,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_RUSTFS_INTEGRATION") != "1",
    reason="set RUN_RUSTFS_INTEGRATION=1 to run against configured RustFS",
)


@pytest_asyncio.fixture(autouse=True)
async def _truncate_tables() -> None:
    """RustFS integration does not need the suite's PostgreSQL cleanup fixture."""
    yield


@pytest_asyncio.fixture
async def rustfs_storage() -> AsyncIterator[ObjectStorage]:
    storage = build_object_storage(get_settings())
    try:
        yield storage
    finally:
        try:
            await storage.delete(STARTUP_PROBE_KEY)
        except WorkspaceError:
            pass
        await storage.close()


async def _delete_prefix(storage: ObjectStorage, prefix: str) -> None:
    start_after: str | None = None
    while True:
        page = await storage.list_page(prefix, start_after=start_after)
        for item in page.items:
            await storage.delete(item.object_name)
        if page.next_start_after is None:
            return
        start_after = page.next_start_after


async def test_real_rustfs_startup_probe_removes_probe_object(
    rustfs_storage: ObjectStorage,
) -> None:
    await rustfs_storage.probe_startup()

    with pytest.raises(WorkspaceError) as caught:
        await rustfs_storage.stat(STARTUP_PROBE_KEY)

    assert caught.value.code == ErrorCode.WORKSPACE_NOT_FOUND


async def test_real_rustfs_personal_and_shared_file_lifecycle(
    rustfs_storage: ObjectStorage,
) -> None:
    fs = WorkspaceFS(rustfs_storage)
    identity = uuid4()
    personal = WorkspaceTarget.personal(identity)
    shared = WorkspaceTarget.shared(identity)
    path = "integration/file.txt"
    cases = (
        (personal, b"personal rustfs bytes"),
        (shared, b"shared rustfs bytes"),
    )

    try:
        for target, content in cases:
            written = await fs.write(target, path, content, quota_bytes=1024 * 1024)
            stored = await fs.read_with_metadata(target, path)
            metadata = await fs.stat(target, path)
            selected = await fs.read(target, path, offset=2, length=5)
            entries = await fs.list_dir(target, "integration")

            assert written.etag
            assert written.created is True
            assert stored.data == content
            assert stored.etag == written.etag
            assert metadata.size == len(content)
            assert metadata.etag == written.etag
            assert selected == content[2:7]
            assert [(entry.path, entry.size) for entry in entries] == [(path, len(content))]
            assert await fs.usage(target) == len(content)

            replacement = await fs.write(
                target,
                path,
                content + b" replaced",
                quota_bytes=1024 * 1024,
                if_match=written.etag,
            )
            assert replacement.etag != written.etag
            assert replacement.created is False
            assert await fs.read(target, path) == content + b" replaced"

            with pytest.raises(WorkspaceError) as caught:
                await fs.write(
                    target,
                    path,
                    b"must not replace",
                    quota_bytes=1024 * 1024,
                    if_none_match=True,
                )
            assert caught.value.code == ErrorCode.WORKSPACE_FILE_CHANGED

        for target, _ in cases:
            await fs.delete_file(target, path)
            with pytest.raises(WorkspaceError) as caught:
                await fs.stat(target, path)
            assert caught.value.code == ErrorCode.WORKSPACE_NOT_FOUND
    finally:
        for prefix in (f"users/{identity}/", f"workspaces/{identity}/"):
            try:
                await _delete_prefix(rustfs_storage, prefix)
            except WorkspaceError:
                pass


async def test_real_rustfs_streams_bounded_chunks_and_releases_response(
    rustfs_storage: ObjectStorage,
) -> None:
    fs = WorkspaceFS(rustfs_storage)
    target = WorkspaceTarget.personal(uuid4())
    content = b"x" * (64 * 1024 + 17)
    path = "integration/stream.bin"
    try:
        written = await fs.write(target, path, content, quota_bytes=1024 * 1024)
        stream = await fs.open_stream(target, path)
        try:
            chunks = []
            while chunk := await stream.read():
                chunks.append(chunk)
        finally:
            await stream.aclose()

        assert stream.size == len(content)
        assert stream.etag == written.etag
        assert [len(chunk) for chunk in chunks] == [64 * 1024, 17]
        assert b"".join(chunks) == content
    finally:
        await _delete_prefix(rustfs_storage, f"users/{target.id}/")


async def test_real_rustfs_recursive_scan_and_validated_patch(
    rustfs_storage: ObjectStorage,
) -> None:
    fs = WorkspaceFS(rustfs_storage)
    target = WorkspaceTarget.personal(uuid4())
    try:
        await fs.write(target, "docs/a.txt", b"old", quota_bytes=1024 * 1024)
        await fs.write(target, "docs/sub/b.txt", b"second", quota_bytes=1024 * 1024)

        objects, truncated = await fs.scan_objects(target, "docs")
        assert [(item.path, item.size) for item in objects] == [
            ("docs/a.txt", 3),
            ("docs/sub/b.txt", 6),
        ]
        assert truncated is False

        results = await fs.apply_transforms(
            (
                FileTransform(
                    target,
                    "docs/a.txt",
                    1024 * 1024,
                    lambda current: (current or b"") + b" updated",
                ),
                FileTransform(
                    target,
                    "docs/new.txt",
                    1024 * 1024,
                    lambda current: b"created" if current is None else current,
                ),
            ),
            dry_run=False,
        )

        assert [result.created for result in results] == [False, True]
        assert await fs.read(target, "docs/a.txt") == b"old updated"
        assert await fs.read(target, "docs/new.txt") == b"created"
    finally:
        await _delete_prefix(rustfs_storage, f"users/{target.id}/")
