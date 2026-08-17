"""Opt-in, real TCP server-to-source-client device smoke test.

This test is intentionally skipped in the normal suite.  Set
``PY5_REAL_E2E=1`` to run it against the real PostgreSQL fixture and a
loopback Uvicorn listener.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import socket
import sys
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from openctopus_server.api.router import router as api_router
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import Device, SystemConfig
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.protocol import ToolResultFrame
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.http import register_error_handler
from openctopus_server.provider.anthropic import (
    DeltaCallback,
    ProviderResult,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.tools.base import Tool, ToolContext, ToolResult
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME
from openctopus_server.tools.registry import ToolRegistry
from openctopus_server.tools.result import UNTRUSTED_TOOL_RESULT_WARNING

pytestmark = pytest.mark.skipif(
    os.environ.get("PY5_REAL_E2E") != "1",
    reason="set PY5_REAL_E2E=1 to run the real TCP device E2E",
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CLIENT_SOURCE = _REPO_ROOT / "client" / "src"
_CLIENT_CWD = _REPO_ROOT / "client"


class _RemoteReadTool(Tool):
    """A minimal routing-only tool used to verify offline normalization."""

    def name(self) -> str:
        return "read_file"

    def schema(self) -> dict[str, Any]:
        return {
            "name": "read_file",
            "description": "Read a file on a paired device.",
            "input_schema": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        }

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        del args, ctx
        raise AssertionError("routing-only tool must not execute locally")


class _RemoteWriteTool(Tool):
    """A second routing-only tool used by the real ChatRuntime loop."""

    def name(self) -> str:
        return "write_file"

    def schema(self) -> dict[str, Any]:
        return {
            "name": "write_file",
            "description": "Write a file on a paired device.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["path", "content"],
            },
        }

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        del args, ctx
        raise AssertionError("routing-only tool must not execute locally")


@dataclass(slots=True)
class _ProviderStep:
    content: list[dict[str, Any]]


class _ScriptedProvider:
    """Provider stub that still runs through ChatRuntime's full tool loop."""

    def __init__(self, steps: list[_ProviderStep]) -> None:
        self._steps = deque(steps)
        self.calls: list[dict[str, Any]] = []

    async def stream_turn(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
        on_delta: DeltaCallback,
        tools: list[dict[str, Any]] | None = None,
    ) -> ProviderResult:
        del limiter, on_delta
        if not self._steps:
            raise AssertionError("provider stub exhausted")
        self.calls.append(
            {
                "system": system,
                "messages": deepcopy(messages),
                "effort": effort,
                "tools": deepcopy(tools),
            }
        )
        return ProviderResult(
            content=deepcopy(self._steps.popleft().content),
            fingerprint=provider_fingerprint(config),
        )

    async def estimate_tokens(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> int:
        del system, messages, tools
        return 1

    async def close(self) -> None:
        return None


async def _start_server(
    app: FastAPI | None = None,
) -> tuple[uvicorn.Server, asyncio.Task[None], str, socket.socket]:
    if app is None:
        app = FastAPI()
        app.include_router(api_router)
        register_error_handler(app)

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.setblocking(False)
    port = int(listener.getsockname()[1])
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        ws="websockets-sansio",
        ws_max_size=12 * 1024 * 1024,
        ws_max_queue=1,
        ws_ping_interval=None,
        ws_per_message_deflate=False,
        lifespan="off",
        log_config=None,
        access_log=False,
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[listener]))
    for _ in range(200):
        if server.started:
            return server, task, f"http://127.0.0.1:{port}", listener
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    server.should_exit = True
    try:
        await asyncio.wait_for(task, timeout=5)
    finally:
        listener.close()
    raise AssertionError("Uvicorn did not start")


async def _stop_server(
    server: uvicorn.Server,
    task: asyncio.Task[None],
    listener: socket.socket,
    registry: DeviceRegistry,
    engine: AsyncEngine,
    runtime: ChatRuntime | None = None,
) -> None:
    if runtime is not None:
        await runtime.close()
    await registry.close()
    server.should_exit = True
    if not task.done():
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(task, timeout=5)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    listener.close()
    await engine.dispose()


def _client_environment(server_url: str, token: str) -> dict[str, str]:
    environment = os.environ.copy()
    for key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        environment.pop(key, None)
    environment["OPENOCTOPUS_SERVER_URL"] = server_url
    environment["OPENOCTOPUS_DEVICE_TOKEN"] = token
    current_pythonpath = environment.get("PYTHONPATH")
    paths = [str(_CLIENT_SOURCE)]
    if current_pythonpath:
        paths.append(current_pythonpath)
    environment["PYTHONPATH"] = os.pathsep.join(paths)
    return environment


async def _start_client(server_url: str, token: str) -> asyncio.subprocess.Process:
    executable = os.environ.get("OO_CLIENT_BIN")
    if executable:
        return await asyncio.create_subprocess_exec(
            executable,
            "run",
            cwd=_CLIENT_CWD,
            env=_client_environment(server_url, token),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "openoctopus_client",
        "run",
        cwd=_CLIENT_CWD,
        env=_client_environment(server_url, token),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def _stop_client(
    process: asyncio.subprocess.Process,
    *,
    expected_returncode: int,
    secret: str | None = None,
) -> None:
    # A non-zero expected code must come from the client itself.  Sending
    # SIGTERM here would race the permanent-error exit that this helper is
    # supposed to observe.
    if process.returncode is None and expected_returncode == 0:
        process.send_signal(signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), timeout=8)
    except TimeoutError:
        process.kill()
        await asyncio.wait_for(process.wait(), timeout=3)
    stdout, stderr = await process.communicate()
    if secret is not None:
        secret_bytes = secret.encode("utf-8")
        assert secret_bytes not in (stdout or b"")
        assert secret_bytes not in (stderr or b"")
    assert process.returncode == expected_returncode


async def _wait_online(
    client: httpx.AsyncClient,
    jwt: str,
    name: str,
    *,
    online: bool,
    process: asyncio.subprocess.Process | None = None,
) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {jwt}"}
    for _ in range(240):
        if process is not None and process.returncode is not None:
            raise AssertionError("source client exited before the expected online state")
        response = await client.get("/api/devices", headers=headers)
        if response.status_code == 200:
            rows = response.json()
            for value in rows:
                if isinstance(value, dict):
                    row = cast(dict[str, Any], value)
                    if row.get("name") == name and row.get("online") is online:
                        return row
        await asyncio.sleep(0.05)
    raise AssertionError(f"device {name!r} did not reach online={online}")


async def _assert_old_token_is_unauthorized(server_url: str, token: str) -> None:
    websocket_url = server_url.replace("http://", "ws://", 1) + "/ws/device"
    try:
        async with connect(
            websocket_url,
            additional_headers={"Authorization": f"Bearer {token}"},
            compression=None,
            ping_interval=None,
            proxy=None,
        ) as websocket:
            await websocket.recv()
    except ConnectionClosed as exc:
        assert exc.rcvd is not None
        assert exc.rcvd.code == 4401
    else:
        raise AssertionError("rotated device token unexpectedly opened a socket")


async def test_stop_client_observes_an_expected_natural_failure() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(0.2); raise SystemExit(1)",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await _stop_client(process, expected_returncode=1)


async def test_real_postgres_source_client_device_lifecycle(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        "OPENOCTOPUS_DATABASE_URL", pg_engine.url.render_as_string(hide_password=False)
    )
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_device_registry.cache_clear()
    registry = get_device_registry()
    server, server_task, server_url, listener = await _start_server()
    client_processes: list[asyncio.subprocess.Process] = []

    workspace = tmp_path / "workspace"
    reconfigured_workspace = tmp_path / "reconfigured-workspace"
    workspace.mkdir()
    (workspace / "seed.txt").write_text("seed line\n", encoding="utf-8")

    try:
        async with httpx.AsyncClient(
            base_url=server_url,
            timeout=5,
            trust_env=False,
        ) as http_client:
            auth_response = await http_client.post(
                "/api/auth/register",
                json={
                    "email": f"device-e2e-{uuid4().hex}@example.com",
                    "password": "testpassword",
                    "name": "Device E2E",
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
                    "name": "E2E Laptop",
                    "workspace_path": str(workspace),
                    "sandbox_mode": True,
                    "ssrf_denylist": [],
                },
            )
            assert create_response.status_code == 201
            created = create_response.json()
            token = created["token"]
            device = created["device"]
            device_id = UUID(device["id"])
            async with AsyncSession(pg_engine, expire_on_commit=False) as db:
                row = await db.scalar(select(Device).where(Device.id == device_id))
            assert row is not None
            assert row.token_hash is not None
            assert token not in repr(row)

            name = device["name"]
            first_process = await _start_client(server_url, token)
            client_processes.append(first_process)
            await _wait_online(http_client, jwt, name, online=True, process=first_process)

            list_result = await registry.dispatch_tool(
                device_id=device_id,
                user_id=user_id,
                name="list_dir",
                args={"path": "."},
                max_result_bytes=16_000,
                timeout_seconds=5,
                expected_device_name=name,
            )
            assert list_result.is_error is False
            assert isinstance(list_result.content, str)
            assert "seed.txt" in list_result.content

            read_result = await registry.dispatch_tool(
                device_id=device_id,
                user_id=user_id,
                name="read_file",
                args={"path": "seed.txt"},
                max_result_bytes=16_000,
                timeout_seconds=5,
                expected_device_name=name,
            )
            assert read_result.is_error is False
            assert read_result.content == "1|seed line"

            write_result = await registry.dispatch_tool(
                device_id=device_id,
                user_id=user_id,
                name="write_file",
                args={"path": "written.txt", "content": "written by e2e\n"},
                max_result_bytes=16_000,
                timeout_seconds=5,
                expected_device_name=name,
            )
            assert write_result.is_error is False
            assert (workspace / "written.txt").read_text(encoding="utf-8") == "written by e2e\n"

            new_name = "e2e-renamed"
            patch_response = await http_client.patch(
                f"/api/devices/{name}/config",
                headers=headers,
                json={"name": new_name, "workspace_path": str(reconfigured_workspace)},
            )
            assert patch_response.status_code == 200
            assert patch_response.json()["name"] == new_name
            await _wait_online(
                http_client,
                jwt,
                new_name,
                online=True,
                process=first_process,
            )
            for _ in range(100):
                if reconfigured_workspace.is_dir():
                    break
                await asyncio.sleep(0.05)
            assert reconfigured_workspace.is_dir()

            await _stop_client(first_process, expected_returncode=0)
            await _wait_online(http_client, jwt, new_name, online=False)

            second_process = await _start_client(server_url, token)
            client_processes.append(second_process)
            await _wait_online(http_client, jwt, new_name, online=True, process=second_process)

            rotation_response = await http_client.post(
                f"/api/devices/{new_name}/regenerate-token",
                headers=headers,
            )
            assert rotation_response.status_code == 200
            rotated_token = rotation_response.json()["token"]
            assert rotated_token != token
            await _stop_client(second_process, expected_returncode=1)
            await _wait_online(http_client, jwt, new_name, online=False)
            await _assert_old_token_is_unauthorized(server_url, token)

            offline_registry = ToolRegistry((_RemoteReadTool(),))
            offline_result = await offline_registry.execute(
                name="read_file",
                args={"path": "seed.txt", DEVICE_FIELD_NAME: new_name},
                ctx=ToolContext(user_id=user_id, session_id=uuid4()),
                device_targets={new_name: device_id},
                device_registry=registry,
            )
            assert offline_result.is_error is True
            assert offline_result.code == ErrorCode.TOOL_DEVICE_UNREACHABLE
    finally:
        for process in client_processes:
            if process.returncode is None:
                with contextlib.suppress(Exception):
                    await _stop_client(process, expected_returncode=0)
        try:
            await _stop_server(server, server_task, listener, registry, get_engine())
        finally:
            get_settings.cache_clear()
            get_engine.cache_clear()
            get_device_registry.cache_clear()


async def test_real_chat_runtime_source_client_read_write_and_offline(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Run paired-device tools through a real HTTP/Uvicorn ChatRuntime turn."""

    monkeypatch.setenv(
        "OPENOCTOPUS_DATABASE_URL", pg_engine.url.render_as_string(hide_password=False)
    )
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_device_registry.cache_clear()
    registry = get_device_registry()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "read-1",
                        "name": "read_file",
                        "input": {
                            "path": "seed.txt",
                            DEVICE_FIELD_NAME: "owner-laptop",
                        },
                    }
                ]
            ),
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "write-1",
                        "name": "write_file",
                        "input": {
                            "path": "written.txt",
                            "content": "written by agent\n",
                            DEVICE_FIELD_NAME: "owner-laptop",
                        },
                    }
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Read and wrote the file."}]),
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "cross-1",
                        "name": "write_file",
                        "input": {
                            "path": "forbidden.txt",
                            "content": "must not be written",
                            DEVICE_FIELD_NAME: "other-laptop",
                        },
                    }
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Cross-user target rejected."}]),
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "offline-1",
                        "name": "read_file",
                        "input": {
                            "path": "seed.txt",
                            DEVICE_FIELD_NAME: "owner-laptop",
                        },
                    }
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Offline target rejected."}]),
        ]
    )
    app = FastAPI()
    app.include_router(api_router)
    register_error_handler(app)
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((_RemoteReadTool(), _RemoteWriteTool())),
        device_registry=registry,
    )
    app.state.chat_runtime = runtime
    server, server_task, server_url, listener = await _start_server(app)
    client_processes: list[asyncio.subprocess.Process] = []
    client_secrets: dict[int, str] = {}
    owner_process: asyncio.subprocess.Process | None = None

    owner_workspace = tmp_path / "owner-workspace"
    other_workspace = tmp_path / "other-workspace"
    owner_workspace.mkdir()
    other_workspace.mkdir()
    (owner_workspace / "seed.txt").write_text("seed line\n", encoding="utf-8")
    (other_workspace / "seed.txt").write_text("other line\n", encoding="utf-8")

    dispatch_calls: list[dict[str, Any]] = []
    original_dispatch = registry.dispatch_tool

    async def record_dispatch(**kwargs: Any) -> ToolResultFrame:
        dispatch_calls.append(deepcopy(kwargs))
        return await original_dispatch(**kwargs)

    monkeypatch.setattr(registry, "dispatch_tool", record_dispatch)

    try:
        async with (
            httpx.AsyncClient(base_url=server_url, timeout=5, trust_env=False) as owner_client,
            httpx.AsyncClient(base_url=server_url, timeout=5, trust_env=False) as other_client,
        ):
            async with AsyncSession(pg_engine, expire_on_commit=False) as db:
                db.add_all(
                    [
                        SystemConfig(key="llm_endpoint", value="http://fake.test"),
                        SystemConfig(key="llm_api_key", value="fake-key"),
                        SystemConfig(key="llm_model", value="fake-model"),
                    ]
                )
                await db.commit()

            owner_auth = await owner_client.post(
                "/api/auth/register",
                json={
                    "email": f"runtime-owner-{uuid4().hex}@example.com",
                    "password": "testpassword",
                    "name": "Runtime Owner",
                },
            )
            assert owner_auth.status_code == 201, owner_auth.text
            owner_auth_body = owner_auth.json()
            owner_jwt = owner_auth_body["jwt"]
            owner_headers = {"Authorization": f"Bearer {owner_jwt}"}
            owner_device_response = await owner_client.post(
                "/api/devices",
                headers=owner_headers,
                json={
                    "name": "owner-laptop",
                    "workspace_path": str(owner_workspace),
                    "sandbox_mode": True,
                    "ssrf_denylist": [],
                },
            )
            assert owner_device_response.status_code == 201, owner_device_response.text
            owner_device_body = owner_device_response.json()
            owner_token = owner_device_body["token"]

            other_auth = await other_client.post(
                "/api/auth/register",
                json={
                    "email": f"runtime-other-{uuid4().hex}@example.com",
                    "password": "testpassword",
                    "name": "Runtime Other",
                },
            )
            assert other_auth.status_code == 201, other_auth.text
            other_jwt = other_auth.json()["jwt"]
            other_headers = {"Authorization": f"Bearer {other_jwt}"}
            other_device_response = await other_client.post(
                "/api/devices",
                headers=other_headers,
                json={
                    "name": "other-laptop",
                    "workspace_path": str(other_workspace),
                    "sandbox_mode": True,
                    "ssrf_denylist": [],
                },
            )
            assert other_device_response.status_code == 201, other_device_response.text
            other_token = other_device_response.json()["token"]

            owner_process = await _start_client(server_url, owner_token)
            other_process = await _start_client(server_url, other_token)
            client_processes.extend((owner_process, other_process))
            client_secrets[id(owner_process)] = owner_token
            client_secrets[id(other_process)] = other_token
            await _wait_online(
                owner_client,
                owner_jwt,
                "owner-laptop",
                online=True,
                process=owner_process,
            )
            await _wait_online(
                other_client,
                other_jwt,
                "other-laptop",
                online=True,
                process=other_process,
            )

            session_id = uuid4()

            async def post_turn(text: str) -> httpx.Response:
                return await owner_client.post(
                    f"/api/sessions/{session_id}/messages",
                    headers=owner_headers,
                    json={
                        "content": [{"type": "text", "text": text}],
                        "attachments": [],
                    },
                )

            first_response = await post_turn("Read seed.txt, then write the result.")
            assert first_response.status_code == 200, first_response.text
            first_events = [json.loads(line) for line in first_response.text.splitlines()]
            assert [
                event["status"]
                for event in first_events
                if event["type"] == "turn_finished"
            ] == ["completed", "completed", "completed"]
            assert (owner_workspace / "written.txt").read_text(encoding="utf-8") == (
                "written by agent\n"
            )
            assert [
                (call["name"], call["args"])
                for call in dispatch_calls
            ] == [
                ("read_file", {"path": "seed.txt"}),
                ("write_file", {"path": "written.txt", "content": "written by agent\n"}),
            ]
            assert all(DEVICE_FIELD_NAME not in call["args"] for call in dispatch_calls)

            first_read_result = provider.calls[1]["messages"][-1]["content"]
            assert first_read_result == [
                {
                    "type": "tool_result",
                    "tool_use_id": "read-1",
                    "content": [
                        {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
                        {"type": "text", "text": "1|seed line"},
                    ],
                    "is_error": False,
                }
            ]
            first_write_result = provider.calls[2]["messages"][-1]["content"]
            assert first_write_result == [
                {
                    "type": "tool_result",
                    "tool_use_id": "write-1",
                    "content": [
                        {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
                        {"type": "text", "text": "Wrote written.txt (17 bytes)."},
                    ],
                    "is_error": False,
                }
            ]
            first_history = await owner_client.get(
                f"/api/sessions/{session_id}/messages",
                headers=owner_headers,
            )
            assert first_history.status_code == 200, first_history.text
            first_history_body = first_history.json()
            assert [
                message["message_kind"] for message in first_history_body["messages"]
            ] == [
                "human",
                "assistant",
                "tool_result",
                "assistant",
                "tool_result",
                "assistant",
            ]
            assert first_history_body["messages"][2]["content"][0]["tool_use_id"] == "read-1"
            assert first_history_body["messages"][4]["content"][0]["tool_use_id"] == "write-1"
            assert first_history_body["messages"][-1]["content"] == [
                {"type": "text", "text": "Read and wrote the file."}
            ]

            def assert_owned_device_enum(call: dict[str, Any]) -> None:
                assert call["tools"] is not None
                for schema in call["tools"]:
                    enum = schema["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"]
                    assert enum == ["server", "owner-laptop"]
                    assert "other-laptop" not in enum

            for call in provider.calls:
                assert_owned_device_enum(call)

            cross_response = await post_turn("Try the other user's device.")
            assert cross_response.status_code == 200, cross_response.text
            assert len(dispatch_calls) == 2
            cross_result = provider.calls[4]["messages"][-1]["content"]
            assert cross_result == [
                {
                    "type": "tool_result",
                    "tool_use_id": "cross-1",
                    "content": [
                        {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
                        {
                            "type": "text",
                            "text": (
                                "[tool_device_unreachable] "
                                "Tool install site is unavailable: other-laptop"
                            ),
                        },
                    ],
                    "is_error": True,
                }
            ]
            assert not (other_workspace / "forbidden.txt").exists()

            assert owner_process is not None
            await _stop_client(owner_process, expected_returncode=0, secret=owner_token)
            await _wait_online(owner_client, owner_jwt, "owner-laptop", online=False)

            offline_response = await post_turn("Read the owner device while it is offline.")
            assert offline_response.status_code == 200, offline_response.text
            offline_events = [json.loads(line) for line in offline_response.text.splitlines()]
            assert [
                event["status"]
                for event in offline_events
                if event["type"] == "turn_finished"
            ] == ["completed", "completed"]
            assert len(provider.calls) == 7
            assert len(dispatch_calls) == 3
            offline_result = provider.calls[6]["messages"][-1]["content"]
            assert offline_result == [
                {
                    "type": "tool_result",
                    "tool_use_id": "offline-1",
                    "content": [
                        {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
                        {
                            "type": "text",
                            "text": "[tool_device_unreachable] Tool install site became unavailable",
                        },
                    ],
                    "is_error": True,
                }
            ]
            assert sum(
                block.get("tool_use_id") == "offline-1"
                for block in offline_result
            ) == 1
            assert dispatch_calls[-1]["args"] == {"path": "seed.txt"}
            offline_history = await owner_client.get(
                f"/api/sessions/{session_id}/messages",
                headers=owner_headers,
            )
            assert offline_history.status_code == 200, offline_history.text
            offline_tool_result = next(
                message
                for message in offline_history.json()["messages"]
                if message["message_kind"] == "tool_result"
                and message["content"][0]["tool_use_id"] == "offline-1"
            )
            assert offline_tool_result["content"][0]["code"] == ErrorCode.TOOL_DEVICE_UNREACHABLE.value
    finally:
        for process in client_processes:
            if process.returncode is None:
                with contextlib.suppress(Exception):
                    await _stop_client(
                        process,
                        expected_returncode=0,
                        secret=client_secrets.get(id(process)),
                    )
        try:
            await _stop_server(
                server,
                server_task,
                listener,
                registry,
                get_engine(),
                runtime=runtime,
            )
        finally:
            get_settings.cache_clear()
            get_engine.cache_clear()
            get_device_registry.cache_clear()
