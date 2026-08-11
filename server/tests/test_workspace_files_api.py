"""HTTP contract tests for the Py4 server-workspace file API."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Iterator
from io import BytesIO
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from storage_http import object_storage_for_fake

from openctopus_server.admission import KeyedDirectionalAdmission
from openctopus_server.api.workspace_files import get_rest_transfer_admission
from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import Session, User, Workspace, WorkspaceMember
from openctopus_server.tools.base import MessageDeliveryEffect, ToolContext
from openctopus_server.tools.message import MessageTool
from openctopus_server.tools.registry import ToolRegistry
from openctopus_server.workspace.fs import WorkspaceFS, _workspace_fs_for_storage
from openctopus_server.workspace.service import WorkspaceService
from openctopus_server.workspace.storage import ObjectStorage, get_object_storage

_MIB = 1024 * 1024
_REST_UPLOAD_LIMIT = 64 * _MIB


class _NoSuchKeyError(Exception):
    code = "NoSuchKey"


class _ObjectBody(BytesIO):
    def __init__(self, data: bytes, etag: str) -> None:
        super().__init__(data)
        self.headers = {
            "Content-Length": str(len(data)),
            "ETag": f'"{etag}"',
        }

    def release_conn(self) -> None:
        return None


class _MemoryMinio:
    """Small synchronous MinIO stand-in behind the real ObjectStorage adapter."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self._revision = 0

    def seed(self, user_id: UUID, path: str, data: bytes) -> str:
        return self._store(f"users/{user_id}/{path}", data)

    def data_for(self, user_id: UUID, path: str) -> bytes | None:
        stored = self.objects.get(f"users/{user_id}/{path}")
        return None if stored is None else stored[0]

    def stat_object(self, bucket: str, object_name: str) -> SimpleNamespace:
        del bucket
        try:
            data, etag = self.objects[object_name]
        except KeyError as exc:
            raise _NoSuchKeyError from exc
        return SimpleNamespace(object_name=object_name, size=len(data), etag=etag)

    def get_object(
        self,
        bucket: str,
        object_name: str,
        *,
        offset: int = 0,
        length: int = 0,
    ) -> _ObjectBody:
        del bucket
        try:
            data, etag = self.objects[object_name]
        except KeyError as exc:
            raise _NoSuchKeyError from exc
        end = None if length == 0 else offset + length
        return _ObjectBody(data[offset:end], etag)

    def put_object(
        self,
        bucket: str,
        object_name: str,
        stream: Any,
        length: int,
        **kwargs: Any,
    ) -> SimpleNamespace:
        del bucket, kwargs
        data = stream.read(length)
        assert len(data) == length
        return SimpleNamespace(etag=self._store(object_name, data))

    def remove_object(self, bucket: str, object_name: str) -> None:
        del bucket
        if object_name not in self.objects:
            raise _NoSuchKeyError
        del self.objects[object_name]

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: str = "",
        start_after: str | None = None,
        **kwargs: Any,
    ) -> Iterator[SimpleNamespace]:
        del bucket, kwargs
        for object_name in sorted(self.objects):
            if object_name.startswith(prefix) and (
                start_after is None or object_name > start_after
            ):
                data, etag = self.objects[object_name]
                yield SimpleNamespace(
                    object_name=object_name,
                    size=len(data),
                    etag=etag,
                )

    def _store(self, object_name: str, data: bytes) -> str:
        self._revision += 1
        etag = f"revision-{self._revision}"
        self.objects[object_name] = (data, etag)
        return etag


@pytest_asyncio.fixture
async def workspace_storage(test_app) -> AsyncIterator[_MemoryMinio]:
    client = _MemoryMinio()
    storage = object_storage_for_fake(client, "test", max_connections=1)
    test_app.dependency_overrides[get_object_storage] = lambda: storage
    _workspace_fs_for_storage.cache_clear()
    try:
        yield client
    finally:
        _workspace_fs_for_storage.cache_clear()
        await storage.close()


@pytest_asyncio.fixture
async def workspace_api(
    user_client,
    workspace_storage: _MemoryMinio,
    pg_engine,
) -> tuple[Any, _MemoryMinio, UUID]:
    async with AsyncSession(pg_engine) as db:
        user_id = await db.scalar(select(User.id).where(User.email == "user@test.com"))
    assert user_id is not None
    return user_client, workspace_storage, user_id


def _assert_standard_error(response, status: int, code: str | None = None) -> None:
    assert response.status_code == status
    body = response.json()
    assert set(body) == {"code", "message"}
    assert isinstance(body["message"], str) and body["message"]
    if code is not None:
        assert body["code"] == code


async def test_runtime_openapi_describes_raw_download_and_upload(async_client) -> None:
    schema = (await async_client.get("/openapi.json")).json()
    operations = schema["paths"]["/api/workspace/files/{path}"]

    device = next(
        parameter
        for parameter in operations["get"]["parameters"]
        if parameter["name"] == "openoctopus_device"
    )
    assert device["required"] is True
    assert device["schema"]["type"] == "string"

    download = operations["get"]["responses"]["200"]
    assert set(download["headers"]) == {
        "ETag",
        "Content-Length",
        "Content-Disposition",
        "X-Content-Type-Options",
    }
    assert download["content"]["application/octet-stream"]["schema"] == {
        "type": "string",
        "format": "binary",
    }
    upload = operations["put"]["requestBody"]
    assert upload["required"] is True
    assert upload["content"]["application/octet-stream"]["schema"] == {
        "type": "string",
        "format": "binary",
    }


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("GET", "/api/workspace/files/a.txt", {}),
        ("PUT", "/api/workspace/files/a.txt", {"content": b"a"}),
        (
            "PATCH",
            "/api/workspace/files/a.txt",
            {"json": {"old_text": "a", "new_text": "b"}},
        ),
        ("DELETE", "/api/workspace/files/a.txt", {}),
        ("DELETE", "/api/workspace/folders/docs", {}),
        ("GET", "/api/workspace/list/docs", {}),
        ("GET", "/api/workspace/find-files", {}),
        ("GET", "/api/workspace/grep", {"params": {"pattern": "x"}}),
        (
            "POST",
            "/api/workspace/patch",
            {"json": {"edits": [{"path": "a.txt", "action": "add", "new_text": "x"}]}},
        ),
    ],
)
async def test_every_file_route_requires_explicit_server_device(
    user_client,
    workspace_storage,
    method: str,
    path: str,
    kwargs: dict[str, Any],
) -> None:
    response = await user_client.request(method, path, **kwargs)
    _assert_standard_error(response, 400)


async def test_file_routes_report_an_unavailable_paired_device(user_client, workspace_storage) -> None:
    response = await user_client.get(
        "/api/workspace/files/a.txt",
        params={"openoctopus_device": "laptop"},
    )
    _assert_standard_error(response, 409, "tool_device_unreachable")


async def test_file_routes_require_authentication(async_client, workspace_storage) -> None:
    response = await async_client.get(
        "/api/workspace/files/a.txt",
        params={"openoctopus_device": "server"},
    )
    _assert_standard_error(response, 401, "auth_unauthorized")


async def test_get_file_streams_raw_bytes_and_download_headers(workspace_api) -> None:
    client, storage, user_id = workspace_api
    etag = storage.seed(user_id, "reports/report.bin", b"\x00report\xff")

    response = await client.get(
        "/api/workspace/files/reports/report.bin",
        params={"openoctopus_device": "server"},
    )

    assert response.status_code == 200
    assert response.content == b"\x00report\xff"
    assert response.headers["content-type"] == "application/octet-stream"
    assert response.headers["content-length"] == "8"
    assert response.headers["etag"] == f'"{etag}"'
    assert response.headers["content-disposition"].startswith("attachment;")
    assert 'filename="report.bin"' in response.headers["content-disposition"]
    assert response.headers["x-content-type-options"] == "nosniff"


async def test_get_file_uses_ascii_safe_and_utf8_download_filenames(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "reports/\u62a5\u544a.txt", b"report")

    response = await client.get(
        f"/api/workspace/files/{quote('reports/\u62a5\u544a.txt')}",
        params={"openoctopus_device": "server"},
    )

    assert response.status_code == 200
    disposition = response.headers["content-disposition"]
    assert 'filename="__.txt"' in disposition
    assert "filename*=UTF-8''%E6%8A%A5%E5%91%8A.txt" in disposition


async def test_invalid_download_etag_closes_stream_before_returning_error(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.objects[f"users/{user_id}/bad.bin"] = (b"bad", "invalid-\u2603")
    storage.seed(user_id, "good.bin", b"good")

    invalid = await client.get(
        "/api/workspace/files/bad.bin",
        params={"openoctopus_device": "server"},
    )
    assert invalid.status_code == 503
    assert invalid.json()["code"] == "workspace_storage_error"

    unblocked = await client.get(
        "/api/workspace/files/good.bin",
        params={"openoctopus_device": "server"},
    )
    assert unblocked.status_code == 200
    assert unblocked.content == b"good"


async def test_download_waiting_for_object_storage_does_not_hold_database_connection(
    workspace_api,
) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "slow.bin", b"slow")
    entered = threading.Event()
    release = threading.Event()
    original_get = storage.get_object

    def blocking_get(*args: Any, **kwargs: Any) -> _ObjectBody:
        entered.set()
        release.wait(timeout=2)
        return original_get(*args, **kwargs)

    storage.get_object = blocking_get  # type: ignore[method-assign]
    downloading = asyncio.create_task(
        client.get(
            "/api/workspace/files/slow.bin",
            params={"openoctopus_device": "server"},
        )
    )
    assert await asyncio.to_thread(entered.wait, 1)
    try:
        assert get_engine().pool.checkedout() == 0
    finally:
        release.set()
    response = await downloading
    assert response.status_code == 200


async def test_slow_upload_body_does_not_hold_database_connection(workspace_api) -> None:
    client, storage, user_id = workspace_api
    body_requested = asyncio.Event()
    release = asyncio.Event()

    async def slow_body() -> AsyncIterator[bytes]:
        yield b"first"
        body_requested.set()
        await release.wait()
        yield b"second"

    uploading = asyncio.create_task(
        client.put(
            "/api/workspace/files/slow-upload.bin",
            params={"openoctopus_device": "server"},
            headers={"Content-Type": "application/octet-stream"},
            content=slow_body(),
        )
    )
    await asyncio.wait_for(body_requested.wait(), timeout=1)
    try:
        assert get_engine().pool.checkedout() == 0
    finally:
        release.set()
    response = await uploading
    assert response.status_code == 200
    assert storage.data_for(user_id, "slow-upload.bin") == b"firstsecond"


@pytest.mark.parametrize(
    ("race", "status", "code"),
    [
        ("revoke", 404, "workspace_not_found"),
        ("quota", 409, "workspace_upload_too_large"),
    ],
)
async def test_upload_reauthorizes_after_collecting_body(
    workspace_api,
    pg_engine,
    race: str,
    status: int,
    code: str,
) -> None:
    client, storage, user_id = workspace_api
    workspace_id = uuid4()
    suffix = workspace_id.hex[:8]
    async with AsyncSession(pg_engine) as db:
        db.add(
            Workspace(
                id=workspace_id,
                name="UploadRace",
                suffix=suffix,
                quota_bytes=100,
                created_by=user_id,
            )
        )
        db.add(WorkspaceMember(workspace_id=workspace_id, user_id=user_id))
        await db.commit()

    body_requested = asyncio.Event()
    release = asyncio.Event()

    async def slow_body() -> AsyncIterator[bytes]:
        yield b"payload"
        body_requested.set()
        await release.wait()

    uploading = asyncio.create_task(
        client.put(
            f"/api/workspace/files//UploadRace@{suffix}/race.bin",
            params={"openoctopus_device": "server"},
            headers={"Content-Type": "application/octet-stream"},
            content=slow_body(),
        )
    )
    await asyncio.wait_for(body_requested.wait(), timeout=1)
    async with AsyncSession(pg_engine) as db:
        if race == "revoke":
            await db.execute(
                delete(WorkspaceMember).where(
                    WorkspaceMember.workspace_id == workspace_id,
                    WorkspaceMember.user_id == user_id,
                )
            )
        else:
            workspace = await db.get(Workspace, workspace_id)
            assert workspace is not None
            workspace.quota_bytes = 1
        await db.commit()
    release.set()

    response = await uploading
    _assert_standard_error(response, status, code)
    assert f"workspaces/{workspace_id}/race.bin" not in storage.objects


async def test_upload_waiting_for_object_storage_does_not_hold_database_connection(
    workspace_api,
) -> None:
    client, storage, user_id = workspace_api
    entered = threading.Event()
    release = threading.Event()
    original_put = storage.put_object

    def blocking_put(*args: Any, **kwargs: Any) -> SimpleNamespace:
        entered.set()
        release.wait(timeout=2)
        return original_put(*args, **kwargs)

    storage.put_object = blocking_put  # type: ignore[method-assign]
    uploading = asyncio.create_task(
        client.put(
            "/api/workspace/files/slow-storage.bin",
            params={"openoctopus_device": "server"},
            headers={"Content-Type": "application/octet-stream"},
            content=b"data",
        )
    )
    assert await asyncio.to_thread(entered.wait, 1)
    try:
        assert get_engine().pool.checkedout() == 0
    finally:
        release.set()

    response = await uploading
    assert response.status_code == 200
    assert storage.data_for(user_id, "slow-storage.bin") == b"data"


async def test_transfer_queue_timeout_is_retryable_and_releases_waiter(
    workspace_api,
    test_app,
) -> None:
    client, storage, user_id = workspace_api
    admission = KeyedDirectionalAdmission(
        direction_limits={"upload": 1, "download": 1},
        per_key_limit=1,
        timeout_seconds=0.01,
    )
    test_app.dependency_overrides[get_rest_transfer_admission] = lambda: admission
    held = await admission.acquire(user_id, "upload")
    try:
        response = await client.put(
            "/api/workspace/files/queued.bin",
            params={"openoctopus_device": "server"},
            headers={"Content-Type": "application/octet-stream"},
            content=b"queued",
        )
    finally:
        await held.aclose()

    _assert_standard_error(response, 429, "workspace_transfer_busy")
    assert response.headers["retry-after"] == "5"
    assert storage.data_for(user_id, "queued.bin") is None
    assert admission.entry_count == 0


async def test_download_queue_uses_shared_user_limit_and_retry_header(
    workspace_api,
    test_app,
) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "queued-download.bin", b"data")
    admission = KeyedDirectionalAdmission(
        direction_limits={"upload": 1, "download": 1},
        per_key_limit=1,
        timeout_seconds=0.01,
    )
    test_app.dependency_overrides[get_rest_transfer_admission] = lambda: admission
    held = await admission.acquire(user_id, "upload")
    try:
        response = await client.get(
            "/api/workspace/files/queued-download.bin",
            params={"openoctopus_device": "server"},
        )
    finally:
        await held.aclose()

    _assert_standard_error(response, 429, "workspace_transfer_busy")
    assert response.headers["retry-after"] == "5"
    assert admission.entry_count == 0


async def test_completed_download_releases_transfer_admission(
    workspace_api,
    test_app,
) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "complete-download.bin", b"data")
    admission = KeyedDirectionalAdmission(
        direction_limits={"upload": 1, "download": 1},
        per_key_limit=1,
        timeout_seconds=0.1,
    )
    test_app.dependency_overrides[get_rest_transfer_admission] = lambda: admission

    response = await client.get(
        "/api/workspace/files/complete-download.bin",
        params={"openoctopus_device": "server"},
    )

    assert response.status_code == 200
    assert response.content == b"data"
    assert admission.entry_count == 0


async def test_upload_idle_timeout_returns_408_without_writing(
    workspace_api,
    test_app,
) -> None:
    client, storage, user_id = workspace_api
    admission = KeyedDirectionalAdmission(
        direction_limits={"upload": 1, "download": 1},
        per_key_limit=1,
        timeout_seconds=0.1,
    )
    settings = get_settings().model_copy(update={"rest_transfer_idle_timeout_seconds": 0.01})
    test_app.dependency_overrides[get_rest_transfer_admission] = lambda: admission
    test_app.dependency_overrides[get_settings] = lambda: settings

    async def stalled_body() -> AsyncIterator[bytes]:
        yield b"partial"
        await asyncio.Event().wait()

    response = await client.put(
        "/api/workspace/files/stalled.bin",
        params={"openoctopus_device": "server"},
        headers={"Content-Type": "application/octet-stream"},
        content=stalled_body(),
    )

    _assert_standard_error(response, 408, "workspace_transfer_timeout")
    assert storage.data_for(user_id, "stalled.bin") is None
    assert admission.entry_count == 0


async def test_put_file_returns_uniform_json_mutation_shape(workspace_api) -> None:
    client, storage, user_id = workspace_api
    url = "/api/workspace/files/notes/today.md"
    params = {"openoctopus_device": "server"}

    created = await client.put(
        url,
        params=params,
        headers={"Content-Type": "application/octet-stream"},
        content=b"first",
    )
    assert created.status_code == 200
    assert created.json() == {
        "path": "notes/today.md",
        "size": 5,
        "etag": created.json()["etag"],
        "created": True,
    }
    assert created.headers["etag"] == f'"{created.json()["etag"]}"'

    replaced = await client.put(
        url,
        params=params,
        headers={"Content-Type": "application/octet-stream"},
        content=b"replacement",
    )
    assert replaced.status_code == 200
    assert replaced.json() == {
        "path": "notes/today.md",
        "size": 11,
        "etag": replaced.json()["etag"],
        "created": False,
    }
    assert storage.data_for(user_id, "notes/today.md") == b"replacement"


async def test_put_rejects_oversized_content_length_before_reading_body(
    workspace_api,
) -> None:
    client, storage, user_id = workspace_api
    response = await client.put(
        "/api/workspace/files/large.bin",
        params={"openoctopus_device": "server"},
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Length": str(_REST_UPLOAD_LIMIT + 1),
        },
        content=b"small",
    )

    _assert_standard_error(response, 409, "workspace_upload_too_large")
    assert storage.data_for(user_id, "large.bin") is None


async def test_put_counts_streamed_chunks_when_content_length_is_absent(
    workspace_api,
) -> None:
    client, storage, user_id = workspace_api
    chunk = b"x" * _MIB

    async def oversized_body() -> AsyncIterator[bytes]:
        for _ in range(65):
            yield chunk

    response = await client.put(
        "/api/workspace/files/chunked.bin",
        params={"openoctopus_device": "server"},
        headers={"Content-Type": "application/octet-stream"},
        content=oversized_body(),
    )

    _assert_standard_error(response, 409, "workspace_upload_too_large")
    assert storage.data_for(user_id, "chunked.bin") is None


async def test_put_supports_optional_if_match(workspace_api) -> None:
    client, storage, user_id = workspace_api
    etag = storage.seed(user_id, "draft.txt", b"version one")
    url = "/api/workspace/files/draft.txt"
    params = {"openoctopus_device": "server"}

    matched = await client.put(
        url,
        params=params,
        headers={"Content-Type": "application/octet-stream", "If-Match": f'"{etag}"'},
        content=b"version two",
    )
    assert matched.status_code == 200

    stale = await client.put(
        url,
        params=params,
        headers={"Content-Type": "application/octet-stream", "If-Match": f'"{etag}"'},
        content=b"stale overwrite",
    )
    _assert_standard_error(stale, 409, "workspace_file_changed")
    assert storage.data_for(user_id, "draft.txt") == b"version two"


async def test_put_supports_if_none_match_star_for_create_only(workspace_api) -> None:
    client, storage, user_id = workspace_api
    url = "/api/workspace/files/new.txt"
    params = {"openoctopus_device": "server"}
    headers = {"Content-Type": "application/octet-stream", "If-None-Match": "*"}

    created = await client.put(url, params=params, headers=headers, content=b"new")
    assert created.status_code == 200
    assert created.json()["created"] is True

    conflict = await client.put(url, params=params, headers=headers, content=b"replace")
    _assert_standard_error(conflict, 409, "workspace_file_changed")
    assert storage.data_for(user_id, "new.txt") == b"new"


@pytest.mark.parametrize(
    "headers",
    [
        {"If-Match": 'W/"revision-1"'},
        {"If-Match": '"revision-1", "revision-2"'},
        {"If-None-Match": '"revision-1"'},
        {"If-Match": '"revision-1"', "If-None-Match": "*"},
    ],
)
async def test_put_rejects_malformed_or_conflicting_etag_conditions(
    workspace_api,
    headers: dict[str, str],
) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "guarded.txt", b"original")
    response = await client.put(
        "/api/workspace/files/guarded.txt",
        params={"openoctopus_device": "server"},
        headers={"Content-Type": "application/octet-stream", **headers},
        content=b"changed",
    )

    _assert_standard_error(response, 400)
    assert storage.data_for(user_id, "guarded.txt") == b"original"


async def test_patch_file_returns_edit_mutation_result(workspace_api) -> None:
    client, storage, user_id = workspace_api
    etag = storage.seed(user_id, "notes/edit.txt", b"hello world world")

    response = await client.patch(
        "/api/workspace/files/notes/edit.txt",
        params={"openoctopus_device": "server"},
        headers={"If-Match": f'"{etag}"'},
        json={
            "old_text": "world",
            "new_text": "octopus",
            "occurrence": 2,
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "path": "notes/edit.txt",
        "size": 19,
        "etag": response.json()["etag"],
        "created": False,
        "replacements": 1,
    }
    assert response.headers["etag"] == f'"{response.json()["etag"]}"'
    assert storage.data_for(user_id, "notes/edit.txt") == b"hello world octopus"


async def test_delete_file_honors_if_match_and_returns_no_content(workspace_api) -> None:
    client, storage, user_id = workspace_api
    etag = storage.seed(user_id, "delete-me.txt", b"content")
    url = "/api/workspace/files/delete-me.txt"
    params = {"openoctopus_device": "server"}

    stale = await client.delete(
        url,
        params=params,
        headers={"If-Match": '"stale"'},
    )
    _assert_standard_error(stale, 409, "workspace_file_changed")

    deleted = await client.delete(
        url,
        params=params,
        headers={"If-Match": f'"{etag}"'},
    )
    assert deleted.status_code == 204
    assert deleted.content == b""
    assert storage.data_for(user_id, "delete-me.txt") is None


async def test_delete_folder_is_recursive_and_does_not_delete_siblings(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "docs/a.txt", b"a")
    storage.seed(user_id, "docs/nested/b.txt", b"b")
    storage.seed(user_id, "docs-sibling.txt", b"keep")

    response = await client.delete(
        "/api/workspace/folders/docs",
        params={"openoctopus_device": "server"},
    )

    assert response.status_code == 204
    assert response.content == b""
    assert storage.data_for(user_id, "docs/a.txt") is None
    assert storage.data_for(user_id, "docs/nested/b.txt") is None
    assert storage.data_for(user_id, "docs-sibling.txt") == b"keep"


async def test_list_directory_uses_standard_page_envelope(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "docs/a.txt", b"a")
    storage.seed(user_id, "docs/sub/b.txt", b"bb")

    first = await client.get(
        "/api/workspace/list/docs",
        params={"openoctopus_device": "server", "limit": 1, "offset": 0},
    )
    assert first.status_code == 200
    assert first.json() == {
        "items": [{"name": "a.txt", "path": "docs/a.txt", "kind": "file", "size": 1}],
        "limit": 1,
        "offset": 0,
        "next_offset": 1,
        "truncated": False,
    }

    second = await client.get(
        "/api/workspace/list/docs",
        params={"openoctopus_device": "server", "limit": 1, "offset": 1},
    )
    assert second.status_code == 200
    assert second.json() == {
        "items": [{"name": "sub", "path": "docs/sub", "kind": "directory", "size": 0}],
        "limit": 1,
        "offset": 1,
        "next_offset": None,
        "truncated": False,
    }


async def test_list_directory_paginates_more_than_one_thousand_entries(workspace_api) -> None:
    client, storage, user_id = workspace_api
    for index in range(1_001):
        storage.seed(user_id, f"large/{index:04d}.txt", b"x")

    response = await client.get(
        "/api/workspace/list/large",
        params={"openoctopus_device": "server", "limit": 1, "offset": 1_000},
    )

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "name": "1000.txt",
                "path": "large/1000.txt",
                "kind": "file",
                "size": 1,
            }
        ],
        "limit": 1,
        "offset": 1_000,
        "next_offset": None,
        "truncated": False,
    }


async def test_shared_directory_entries_preserve_reusable_virtual_paths(
    workspace_api,
    pg_engine,
) -> None:
    client, storage, user_id = workspace_api
    workspace_id = UUID("a4f7e2d1-0000-4000-8000-000000000001")
    async with AsyncSession(pg_engine) as db:
        db.add(
            Workspace(
                id=workspace_id,
                name="Team",
                suffix="a4f7e2d1",
                quota_bytes=1_000_000,
                created_by=user_id,
            )
        )
        db.add(WorkspaceMember(workspace_id=workspace_id, user_id=user_id))
        await db.commit()
    storage._store(f"workspaces/{workspace_id}/docs/a.txt", b"a")

    response = await client.get(
        "/api/workspace/list//Team@a4f7e2d1/docs",
        params={"openoctopus_device": "server"},
    )

    assert response.status_code == 200
    assert response.json()["items"] == [
        {
            "name": "a.txt",
            "path": "/Team@a4f7e2d1/docs/a.txt",
            "kind": "file",
            "size": 1,
        }
    ]


async def test_message_ref_recovers_shared_download_after_workspace_rename(
    workspace_api,
    test_app,
    pg_engine,
) -> None:
    client, storage_client, user_id = workspace_api
    created_response = await client.post(
        "/api/workspaces",
        json={"name": "Before", "quota_bytes": 1_000_000},
    )
    assert created_response.status_code == 201
    created = created_response.json()
    workspace_id = UUID(created["id"])
    relative_path = "reports/report.pdf"
    storage_client._store(
        f"workspaces/{workspace_id}/{relative_path}",
        b"shared report",
    )
    session_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(
            Session(
                id=session_id,
                user_id=user_id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="Shared delivery",
            )
        )
        await db.commit()

    object_storage: ObjectStorage = test_app.dependency_overrides[get_object_storage]()
    service = WorkspaceService(WorkspaceFS(object_storage))
    registry = ToolRegistry((MessageTool(pg_engine, service),))
    original_path = f"/{created['ref']}/{relative_path}"
    result = await registry.execute(
        name="message",
        args={"content": "Shared report", "media": [original_path]},
        ctx=ToolContext(user_id=user_id, session_id=session_id),
    )

    assert isinstance(result.side_effect, MessageDeliveryEffect)
    ref = result.side_effect.delivery_refs[0]
    assert ref.workspace_id == workspace_id
    assert ref.workspace_relative_path == relative_path

    renamed_response = await client.patch(
        f"/api/workspaces/{quote(created['ref'], safe='@')}",
        json={"name": "After Rename"},
    )
    assert renamed_response.status_code == 200
    workspace_page = (await client.get("/api/workspaces")).json()
    renamed = next(item for item in workspace_page["items"] if item["id"] == str(ref.workspace_id))
    recovered_path = f"/{renamed['ref']}/{ref.workspace_relative_path}"

    stale = await client.get(
        f"/api/workspace/files/{quote(ref.path, safe='/@')}",
        params={"openoctopus_device": "server"},
    )
    recovered = await client.get(
        f"/api/workspace/files/{quote(recovered_path, safe='/@')}",
        params={"openoctopus_device": "server"},
    )

    assert stale.status_code == 404
    assert recovered.status_code == 200
    assert recovered.content == b"shared report"


async def test_recursive_list_synthesizes_directories_and_skips_noise(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "docs/a.txt", b"a")
    storage.seed(user_id, "docs/sub/b.txt", b"b")
    storage.seed(user_id, "docs/node_modules/ignored.js", b"ignored")

    response = await client.get(
        "/api/workspace/list/docs",
        params={"openoctopus_device": "server", "recursive": True},
    )

    assert response.status_code == 200
    assert [(item["path"], item["kind"]) for item in response.json()["items"]] == [
        ("docs/a.txt", "file"),
        ("docs/sub", "directory"),
        ("docs/sub/b.txt", "file"),
    ]


async def test_find_files_uses_the_standard_page_envelope(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "src/api.py", b"python")
    storage.seed(user_id, "src/api.ts", b"typescript")
    storage.seed(user_id, "src/worker.py", b"python")

    response = await client.get(
        "/api/workspace/find-files",
        params={
            "openoctopus_device": "server",
            "path": "src",
            "query": "api",
            "type": "py",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "items": [{"name": "api.py", "path": "src/api.py", "kind": "file", "size": 6}],
        "limit": 200,
        "offset": 0,
        "next_offset": None,
        "truncated": False,
    }


async def test_grep_returns_structured_files_counts_and_content(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, ".gitignore", b"ignored.txt\n")
    storage.seed(user_id, "a.txt", b"before\nneedle\nafter\nneedle\n")
    storage.seed(user_id, "ignored.txt", b"needle\n")

    files = await client.get(
        "/api/workspace/grep",
        params={"openoctopus_device": "server", "pattern": "needle"},
    )
    counts = await client.get(
        "/api/workspace/grep",
        params={
            "openoctopus_device": "server",
            "pattern": "needle",
            "output_mode": "count",
        },
    )
    content = await client.get(
        "/api/workspace/grep",
        params={
            "openoctopus_device": "server",
            "pattern": "needle",
            "output_mode": "content",
            "context_before": 1,
            "context_after": 1,
        },
    )

    assert files.status_code == counts.status_code == content.status_code == 200
    assert files.json()["items"] == [{"path": "a.txt"}]
    assert counts.json()["items"] == [{"path": "a.txt", "count": 2}]
    assert content.json()["items"][0] == {
        "path": "a.txt",
        "line_number": 2,
        "line": "needle",
        "before": [{"line_number": 1, "line": "before"}],
        "after": [{"line_number": 3, "line": "after"}],
    }


async def test_structured_patch_dry_run_and_commit_share_one_shape(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "a.txt", b"old")
    body = {
        "edits": [
            {"path": "a.txt", "action": "replace", "old_text": "old", "new_text": "new"},
            {"path": "b.txt", "action": "add", "new_text": "created"},
        ]
    }

    dry_run = await client.post(
        "/api/workspace/patch",
        params={"openoctopus_device": "server"},
        json={**body, "dry_run": True},
    )
    committed = await client.post(
        "/api/workspace/patch",
        params={"openoctopus_device": "server"},
        json=body,
    )

    assert dry_run.status_code == committed.status_code == 200
    assert dry_run.json()["dry_run"] is True
    assert dry_run.json()["committed"] == 0
    assert committed.json()["dry_run"] is False
    assert committed.json()["committed"] == 2
    assert storage.data_for(user_id, "a.txt") == b"new"
    assert storage.data_for(user_id, "b.txt") == b"created"


async def test_structured_patch_validation_is_atomic_before_storage_write(workspace_api) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "a.txt", b"old-a")
    storage.seed(user_id, "b.txt", b"old-b")

    response = await client.post(
        "/api/workspace/patch",
        params={"openoctopus_device": "server"},
        json={
            "edits": [
                {
                    "path": "a.txt",
                    "action": "replace",
                    "old_text": "old-a",
                    "new_text": "new-a",
                },
                {
                    "path": "b.txt",
                    "action": "replace",
                    "old_text": "missing",
                    "new_text": "new-b",
                },
            ]
        },
    )

    _assert_standard_error(response, 409, "tool_no_match")
    assert storage.data_for(user_id, "a.txt") == b"old-a"
    assert storage.data_for(user_id, "b.txt") == b"old-b"


async def test_file_routes_return_standard_domain_and_validation_errors(workspace_api) -> None:
    client, storage, user_id = workspace_api
    missing = await client.get(
        "/api/workspace/files/missing.txt",
        params={"openoctopus_device": "server"},
    )
    _assert_standard_error(missing, 404, "workspace_not_found")

    storage.seed(user_id, "file.txt", b"content")
    file_as_folder = await client.delete(
        "/api/workspace/folders/file.txt",
        params={"openoctopus_device": "server"},
    )
    _assert_standard_error(file_as_folder, 409, "tool_is_file")

    invalid_json = await client.patch(
        "/api/workspace/files/file.txt",
        params={"openoctopus_device": "server"},
        json={"old_text": "content", "new_text": "new", "unexpected": True},
    )
    _assert_standard_error(invalid_json, 400)

    wrong_media_type = await client.put(
        "/api/workspace/files/file.txt",
        params={"openoctopus_device": "server"},
        headers={"Content-Type": "text/plain"},
        content=b"new",
    )
    _assert_standard_error(wrong_media_type, 400)
