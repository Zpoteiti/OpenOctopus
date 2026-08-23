"""Opt-in real TCP transfer and device Workspace Files E2E.

The test deliberately starts Uvicorn on a loopback TCP socket and launches the
source client as a subprocess.  It is skipped in the normal suite because it
needs the real PostgreSQL fixture and is slower than the protocol tests.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
from collections.abc import AsyncIterator, Iterator
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine
from storage_http import object_storage_for_fake
from test_device_client_e2e import (
    _CLIENT_CWD,
    _client_environment,
    _start_client,
    _start_server,
    _stop_client,
    _stop_server,
    _wait_online,
)

from openctopus_server.api.router import router as api_router
from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.errors.http import register_error_handler
from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.file_transfer import FileTransferTool
from openctopus_server.workspace.fs import WorkspaceFS
from openctopus_server.workspace.service import WorkspaceService, get_workspace_service

pytestmark = pytest.mark.skipif(
    os.environ.get("PY5_REAL_E2E") != "1",
    reason="set PY5_REAL_E2E=1 to run the real TCP device transfer E2E",
)


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


class _TrackingMemoryMinio:
    """Small RustFS-shaped store with observable write operations.

    The HTTP relay path must not touch this store.  FileTransferTool uses it
    only for the server endpoint, while the device route uses the live client
    socket directly.
    """

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self._revision = 0
        self.put_calls = 0
        self.copy_calls = 0
        self.remove_calls = 0

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
        self.put_calls += 1
        chunks: list[bytes] = []
        remaining = length if length >= 0 else None
        while remaining is None or remaining > 0:
            chunk = stream.read(-1 if remaining is None else remaining)
            if not chunk:
                break
            chunks.append(chunk)
            if remaining is not None:
                remaining -= len(chunk)
        data = b"".join(chunks)
        if length >= 0 and len(data) != length:
            raise AssertionError("fake object upload read an unexpected length")
        return SimpleNamespace(etag=self._store(object_name, data))

    def copy_object(self, bucket: str, object_name: str, source: Any) -> SimpleNamespace:
        del bucket
        self.copy_calls += 1
        source_name = getattr(source, "object_name", None)
        if not isinstance(source_name, str):
            raise AssertionError("fake object copy source is missing object_name")
        try:
            data, _ = self.objects[source_name]
        except KeyError as exc:
            raise _NoSuchKeyError from exc
        return SimpleNamespace(etag=self._store(object_name, data))

    def remove_object(self, bucket: str, object_name: str) -> None:
        del bucket
        self.remove_calls += 1
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


async def _wait_for_transfer_cleanup(registry: Any) -> None:
    for _ in range(200):
        if registry.transfers.active_slots == 0:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("device transfer slot did not clean up")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def _start_real_client(server_url: str, token: str) -> asyncio.subprocess.Process:
    """Run a frozen client when supplied, otherwise use source mode."""

    executable = os.environ.get("OO_CLIENT_BIN")
    if executable is None:
        return await _start_client(server_url, token)
    return await asyncio.create_subprocess_exec(
        executable,
        "run",
        cwd=_CLIENT_CWD,
        env=_client_environment(server_url, token),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def test_real_file_transfer_and_device_workspace_relay(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Exercise both transfer directions, HTTP relay, and cancellation cleanup."""

    monkeypatch.setenv(
        "OPENOCTOPUS_DATABASE_URL", pg_engine.url.render_as_string(hide_password=False)
    )
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_device_registry.cache_clear()
    registry = get_device_registry()

    fake_rustfs = _TrackingMemoryMinio()
    object_storage = object_storage_for_fake(fake_rustfs, "test", max_connections=2)
    workspace_service = WorkspaceService(WorkspaceFS(object_storage))
    app = FastAPI()
    app.include_router(api_router)
    app.dependency_overrides[get_workspace_service] = lambda: workspace_service
    register_error_handler(app)
    server, server_task, server_url, listener = await _start_server(app)
    client_processes: list[asyncio.subprocess.Process] = []

    device_workspace = tmp_path / "device-workspace"
    device_workspace.mkdir()
    token: str | None = None
    try:
        async with httpx.AsyncClient(
            base_url=server_url,
            timeout=10,
            trust_env=False,
        ) as http_client:
            auth_response = await http_client.post(
                "/api/auth/register",
                json={
                    "email": f"transfer-e2e-{uuid4().hex}@example.com",
                    "password": "testpassword",
                    "name": "Transfer E2E",
                },
            )
            assert auth_response.status_code == 201, auth_response.text
            auth = auth_response.json()
            jwt = auth["jwt"]
            user_id = UUID(auth["user"]["id"])
            headers = {"Authorization": f"Bearer {jwt}"}

            create_response = await http_client.post(
                "/api/devices",
                headers=headers,
                json={
                    "name": "transfer-device",
                    "workspace_path": str(device_workspace),
                    "restrict_to_workspace": True,
                    "ssrf_denylist": [],
                },
            )
            assert create_response.status_code == 201, create_response.text
            created = create_response.json()
            token = created["token"]
            device_name = created["device"]["name"]
            process = await _start_real_client(server_url, token)
            client_processes.append(process)
            await _wait_online(
                http_client,
                jwt,
                device_name,
                online=True,
                process=process,
            )

            rest_bytes = (b"rest-to-device\x00" * 1024) + b"end"
            fake_rustfs.seed(user_id, "rest-source.bin", rest_bytes)
            rest_transfer = await http_client.post(
                "/api/workspace/transfer",
                headers=headers,
                json={
                    "openoctopus_src_device": "server",
                    "src_path": "rest-source.bin",
                    "openoctopus_dst_device": device_name,
                    "dst_path": "from-rest.bin",
                    "mode": "copy",
                },
            )
            assert rest_transfer.status_code == 200, rest_transfer.text
            assert rest_transfer.json() == {
                "bytes_transferred": len(rest_bytes),
                "sha256": _sha256(rest_bytes),
                "warnings": [],
            }
            assert (device_workspace / "from-rest.bin").read_bytes() == rest_bytes

            server_bytes = (b"server-to-device\x00" * 8192) + b"end"
            fake_rustfs.seed(user_id, "server-source.bin", server_bytes)
            transfer_tool = FileTransferTool(pg_engine, workspace_service, registry)
            context = ToolContext(user_id=user_id, session_id=uuid4())

            server_to_client = await transfer_tool.execute(
                {
                    "openoctopus_src_device": "server",
                    "src_path": "server-source.bin",
                    "openoctopus_dst_device": device_name,
                    "dst_path": "from-server.bin",
                    "mode": "copy",
                },
                context,
            )
            assert server_to_client.is_error is False
            assert _sha256(server_bytes) in str(server_to_client.content)
            assert (device_workspace / "from-server.bin").read_bytes() == server_bytes
            assert fake_rustfs.data_for(user_id, "server-source.bin") == server_bytes
            await _wait_for_transfer_cleanup(registry)

            client_bytes = (b"client-to-server\xff" * 8192) + b"end"
            (device_workspace / "client-source.bin").write_bytes(client_bytes)
            client_to_server = await transfer_tool.execute(
                {
                    "openoctopus_src_device": device_name,
                    "src_path": "client-source.bin",
                    "openoctopus_dst_device": "server",
                    "dst_path": "to-server.bin",
                    "mode": "copy",
                },
                context,
            )
            assert client_to_server.is_error is False
            assert _sha256(client_bytes) in str(client_to_server.content)
            assert fake_rustfs.data_for(user_id, "to-server.bin") == client_bytes
            assert (device_workspace / "client-source.bin").read_bytes() == client_bytes
            await _wait_for_transfer_cleanup(registry)

            upload_bytes = b"browser upload to device\x00" * 1024
            before_upload_writes = (
                fake_rustfs.put_calls,
                fake_rustfs.copy_calls,
                fake_rustfs.remove_calls,
            )
            upload = await http_client.put(
                "/api/workspace/files/browser.bin",
                params={"openoctopus_device": device_name},
                headers={"Content-Type": "application/octet-stream"},
                content=upload_bytes,
            )
            assert upload.status_code == 200, upload.text
            upload_body = upload.json()
            assert upload_body["created"] is True
            assert upload_body["size"] == len(upload_bytes)
            assert upload_body["etag"]
            assert upload.headers["etag"] == f'"{upload_body["etag"]}"'
            assert (device_workspace / "browser.bin").read_bytes() == upload_bytes
            assert (
                fake_rustfs.put_calls,
                fake_rustfs.copy_calls,
                fake_rustfs.remove_calls,
            ) == before_upload_writes

            replacement_bytes = b"browser replacement\xff"
            replacement = await http_client.put(
                "/api/workspace/files/browser.bin",
                params={"openoctopus_device": device_name},
                headers={
                    "Content-Type": "application/octet-stream",
                    "If-Match": f'"{upload_body["etag"]}"',
                },
                content=replacement_bytes,
            )
            assert replacement.status_code == 200, replacement.text
            replacement_body = replacement.json()
            assert replacement_body["created"] is False
            assert replacement_body["etag"] != upload_body["etag"]
            assert replacement.headers["etag"] == f'"{replacement_body["etag"]}"'
            assert (device_workspace / "browser.bin").read_bytes() == replacement_bytes

            writes_before_relay = (
                fake_rustfs.put_calls,
                fake_rustfs.copy_calls,
                fake_rustfs.remove_calls,
            )
            download = await http_client.get(
                "/api/workspace/files/browser.bin",
                params={"openoctopus_device": device_name},
            )
            assert download.status_code == 200, download.text
            assert download.content == replacement_bytes
            assert download.headers["content-length"] == str(len(replacement_bytes))
            assert download.headers["etag"] == f'"{replacement_body["etag"]}"'
            assert download.headers["x-content-type-options"] == "nosniff"
            assert (
                fake_rustfs.put_calls,
                fake_rustfs.copy_calls,
                fake_rustfs.remove_calls,
            ) == writes_before_relay
            await _wait_for_transfer_cleanup(registry)

            device_id = UUID(created["device"]["id"])
            route_before_disconnect = await registry.get_route_snapshot(
                device_id,
                user_id=user_id,
                expected_device_name=device_name,
            )
            assert route_before_disconnect is not None
            success_ack_blocked = asyncio.Event()
            success_ack_cancelled = asyncio.Event()
            success_ack_delivered = asyncio.Event()
            release_success_ack = asyncio.Event()
            raw_http_closed = asyncio.Event()
            committed_abort_entered = asyncio.Event()
            success_ack_results: list[bool] = []
            block_next_success_ack = True
            original_send_text = registry.send_text
            original_abort = registry.transfers._abort

            async def hold_success_ack(
                handle: Any,
                payload: str,
                **kwargs: Any,
            ) -> bool:
                nonlocal block_next_success_ack
                frame = json.loads(payload)
                is_transfer_end = isinstance(frame, dict) and frame.get("type") == "transfer_end"
                if (
                    block_next_success_ack
                    and is_transfer_end
                    and frame.get("ack") is True
                    and frame.get("ok") is True
                ):
                    block_next_success_ack = False
                    success_ack_blocked.set()
                    try:
                        await release_success_ack.wait()
                    except asyncio.CancelledError:
                        success_ack_cancelled.set()
                        raise
                result = bool(await original_send_text(handle, payload, **kwargs))
                if is_transfer_end and frame.get("ack") is True and frame.get("ok") is True:
                    success_ack_results.append(result)
                    success_ack_delivered.set()
                return result

            async def observe_committed_abort(
                slot: Any,
                code: str,
                *,
                send_frame: bool,
                error: BaseException | None = None,
            ) -> None:
                if code == "cancelled" and slot.committed_result is not None:
                    assert raw_http_closed.is_set()
                    committed_abort_entered.set()
                await original_abort(
                    slot,
                    code,
                    send_frame=send_frame,
                    error=error,
                )

            with (
                monkeypatch.context() as race_patch,
                contextlib.ExitStack() as race_cleanup,
            ):
                race_cleanup.callback(release_success_ack.set)
                race_patch.setattr(registry, "send_text", hold_success_ack)
                race_patch.setattr(registry.transfers, "_abort", observe_committed_abort)

                host, port_text = server_url.removeprefix("http://").rsplit(":", 1)
                reader, writer = await asyncio.open_connection(host, int(port_text))
                try:
                    target = (
                        "/api/workspace/files/browser.bin"
                        f"?openoctopus_device={device_name}"
                    )
                    request = (
                        f"GET {target} HTTP/1.1\r\n"
                        f"Host: {host}:{port_text}\r\n"
                        f"Authorization: Bearer {jwt}\r\n"
                        "Connection: close\r\n\r\n"
                    )
                    writer.write(request.encode("ascii"))
                    await writer.drain()
                    response_head = await asyncio.wait_for(
                        reader.readuntil(b"\r\n\r\n"),
                        timeout=5,
                    )
                    header_lines = response_head.decode("latin-1").split("\r\n")
                    assert header_lines[0].startswith("HTTP/1.1 200 ")
                    response_headers = {
                        name.strip().lower(): value.strip()
                        for line in header_lines[1:]
                        if ":" in line
                        for name, value in (line.split(":", 1),)
                    }
                    content_length = int(response_headers["content-length"])
                    assert content_length == len(replacement_bytes)
                    assert await asyncio.wait_for(
                        reader.readexactly(content_length),
                        timeout=5,
                    ) == replacement_bytes
                    await asyncio.wait_for(success_ack_blocked.wait(), timeout=5)
                finally:
                    writer.close()
                    await writer.wait_closed()
                    raw_http_closed.set()

                await asyncio.wait_for(committed_abort_entered.wait(), timeout=5)
                await asyncio.sleep(0)
                assert success_ack_cancelled.is_set() is False
                release_success_ack.set()
                await asyncio.wait_for(success_ack_delivered.wait(), timeout=5)
                assert success_ack_results == [True]
                assert process.returncode is None
                route_after_disconnect = await registry.get_route_snapshot(
                    device_id,
                    user_id=user_id,
                    expected_device_name=device_name,
                )
                assert route_after_disconnect is not None
                assert route_after_disconnect.handle == route_before_disconnect.handle

            follow_up = await http_client.get(
                "/api/workspace/files/browser.bin",
                params={"openoctopus_device": device_name},
            )
            assert follow_up.status_code == 200, follow_up.text
            assert follow_up.content == replacement_bytes
            await _wait_for_transfer_cleanup(registry)

            partial_started = asyncio.Event()
            release_partial_body = asyncio.Event()
            partial_bytes = b"partial upload that must never become visible"

            async def partial_body() -> AsyncIterator[bytes]:
                yield partial_bytes
                partial_started.set()
                await release_partial_body.wait()
                yield b"late bytes"

            partial_upload = asyncio.create_task(
                http_client.put(
                    "/api/workspace/files/cancelled.bin",
                    params={"openoctopus_device": device_name},
                    headers={"Content-Type": "application/octet-stream"},
                    content=partial_body(),
                )
            )
            await asyncio.wait_for(partial_started.wait(), timeout=5)
            partial_upload.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await partial_upload
            release_partial_body.set()
            await _wait_for_transfer_cleanup(registry)
            assert not (device_workspace / "cancelled.bin").exists()
            assert not list(device_workspace.glob(".*.openoctopus-*.tmp"))
    finally:
        for process in client_processes:
            if process.returncode is None:
                with contextlib.suppress(Exception):
                    await _stop_client(process, expected_returncode=0, secret=token)
        try:
            await _stop_server(
                server,
                server_task,
                listener,
                registry,
                get_engine(),
            )
        finally:
            await object_storage.close()
            get_settings.cache_clear()
            get_engine.cache_clear()
            get_device_registry.cache_clear()
