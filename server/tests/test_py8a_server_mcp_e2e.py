"""Opt-in real TCP and stdio acceptance for private conversation Server MCP.

Set ``PY8A_REAL_E2E=1`` to run with the local PostgreSQL test fixture.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import sys
from collections import deque
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from test_device_client_e2e import _start_server, _stop_server

from openctopus_server.api.router import router as api_router
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import SystemConfig
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.errors.http import register_error_handler
from openctopus_server.mcp.authority import ServerMcpAuthorityFence
from openctopus_server.mcp.catalog import discover_server_catalog
from openctopus_server.mcp.models import empty_server_mcp_envelope, parse_server_mcp_configs
from openctopus_server.mcp.supervisor import ServerMcpSupervisor
from openctopus_server.services import server_mcp
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME
from openctopus_server.tools.registry import ToolRegistry, _owned_mcp_route_resolver
from openctopus_server.workspace.service import get_workspace_service
from openctopus_server.workspace.storage import get_object_storage

pytestmark = pytest.mark.skipif(
    os.environ.get("PY8A_REAL_E2E") != "1",
    reason="set PY8A_REAL_E2E=1 to run real private Server MCP acceptance",
)

_STDIO_FIXTURE = Path(__file__).parent / "fixtures" / "py8a_server_mcp_stdio.py"
_REMOTE_FIXTURE = Path(__file__).parent / "fixtures" / "py8a_server_mcp_remote.py"


class _HealthyStorage:
    async def check_health(self) -> None:
        return None


class _RegistrationWorkspace:
    async def write(self, *args: Any, **kwargs: Any) -> None:
        return None


def _tool(name: str, **arguments: object) -> list[dict[str, Any]]:
    return [{
        "type": "tool_use",
        "id": f"tool_{uuid4().hex}",
        "name": name,
        "input": {**arguments, DEVICE_FIELD_NAME: "server"},
    }]


def _sse(payload: dict[str, Any]) -> str:
    return f"event: {payload['type']}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"


def _anthropic_sse(content: list[dict[str, Any]]) -> str:
    events = [_sse({
        "type": "message_start",
        "message": {
            "id": f"msg_{uuid4().hex}", "type": "message", "role": "assistant",
            "content": [], "model": "fake-model", "stop_reason": None,
            "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 0},
        },
    })]
    for index, block in enumerate(content):
        if block["type"] == "tool_use":
            start = {"type": "tool_use", "id": block["id"], "name": block["name"], "input": {}}
            delta = {"type": "input_json_delta", "partial_json": json.dumps(block["input"])}
        else:
            start = {"type": "text", "text": ""}
            delta = {"type": "text_delta", "text": block["text"]}
        events.extend((
            _sse({"type": "content_block_start", "index": index, "content_block": start}),
            _sse({"type": "content_block_delta", "index": index, "delta": delta}),
            _sse({"type": "content_block_stop", "index": index}),
        ))
    events.extend((
        _sse({
            "type": "message_delta",
            "delta": {
                "stop_reason": "tool_use" if any(b["type"] == "tool_use" for b in content) else "end_turn",
                "stop_sequence": None,
            },
            "usage": {"output_tokens": 1},
        }),
        _sse({"type": "message_stop"}),
    ))
    return "".join(events)


class _Provider:
    def __init__(self) -> None:
        self.steps = deque([
            _tool("mcp_http_echo", text="first"),
            _tool("mcp_http_counter"),
            _tool("mcp_local_counter"),
            [{"type": "text", "text": "first done"}],
            _tool("mcp_http_counter"),
            _tool("mcp_local_counter"),
            [{"type": "text", "text": "second done"}],
            _tool("mcp_http_counter"),
            _tool("mcp_local_counter"),
            [{"type": "text", "text": "other owner done"}],
        ])
        self.calls: list[dict[str, Any]] = []

    async def respond(self, request: Request) -> StreamingResponse:
        assert self.steps, "fake provider script exhausted"
        self.calls.append(cast(dict[str, Any], await request.json()))
        payload = _anthropic_sse(self.steps.popleft())

        async def stream() -> AsyncIterator[str]:
            yield payload

        return StreamingResponse(stream(), media_type="text/event-stream")


def _port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return cast(int, listener.getsockname()[1])


async def _start_remote(schema_file: Path) -> tuple[asyncio.subprocess.Process, str]:
    port = _port()
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(_REMOTE_FIXTURE), "--transport", "streamable_http",
        "--port", str(port), "--marker", "http", "--schema-file", str(schema_file),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    )
    for _ in range(300):
        if process.returncode is not None:
            raise AssertionError(f"MCP fixture exited during startup: {process.returncode}")
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
        except OSError:
            await asyncio.sleep(0.01)
            continue
        del reader
        writer.close()
        await writer.wait_closed()
        return process, f"http://127.0.0.1:{port}/mcp"
    process.kill()
    await process.wait()
    raise AssertionError("MCP fixture did not start")


async def _register(client: httpx.AsyncClient, label: str, *, admin: bool = False) -> dict[str, Any]:
    payload: dict[str, object] = {
        "email": f"py8a-{label}-{uuid4().hex}@example.com",
        "password": "testpassword",
        "name": label,
    }
    if admin:
        payload["admin_token"] = "dev-admin-token"
    response = await client.post("/api/auth/register", json=payload)
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


def _auth(identity: dict[str, Any]) -> dict[str, str]:
    return {"Authorization": f"Bearer {identity['jwt']}"}


def _schemas(call: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {schema["name"]: schema for schema in call["tools"]}


async def _chat(client: httpx.AsyncClient, owner: dict[str, Any], session_id: str) -> None:
    client.cookies.clear()
    response = await client.post(
        f"/api/sessions/{session_id}/messages",
        headers=_auth(owner),
        json={"content": [{"type": "text", "text": "Use the requested MCP tools."}], "attachments": []},
    )
    assert response.status_code == 200, response.text


async def test_private_conversation_mcp_state_and_fresh_discovery(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_URL", pg_engine.url.render_as_string(hide_password=False))
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_device_registry.cache_clear()
    provider = _Provider()
    supervisor = ServerMcpSupervisor.create_default()
    authority = ServerMcpAuthorityFence(empty_server_mcp_envelope())
    registry = get_device_registry()
    tool_registry = ToolRegistry(
        (),
        mcp_route_resolver=_owned_mcp_route_resolver(pg_engine),
        server_mcp_dispatcher=supervisor,
        server_mcp_authority=authority,
    )
    runtime = ChatRuntime(
        pg_engine, tool_registry=tool_registry, device_registry=registry,
        server_mcp_sessions=supervisor,
    )
    remote: asyncio.subprocess.Process | None = None
    server = None
    server_task = None
    listener = None
    try:
        schema_file = tmp_path / "remote-schema"
        schema_file.write_text("echo", encoding="utf-8")
        remote, remote_url = await _start_remote(schema_file)
        await supervisor.start(empty_server_mcp_envelope())
        app = FastAPI()

        @app.post("/v1/messages")
        async def fake_anthropic(request: Request) -> StreamingResponse:
            return await provider.respond(request)

        app.include_router(api_router)
        register_error_handler(app)
        app.state.chat_runtime = runtime
        app.state.server_mcp_supervisor = supervisor
        app.state.server_mcp_authority = authority
        app.dependency_overrides[get_object_storage] = lambda: _HealthyStorage()
        app.dependency_overrides[get_workspace_service] = lambda: _RegistrationWorkspace()
        server, server_task, server_url, listener = await _start_server(app)
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            db.add_all([
                SystemConfig(key="llm_endpoint", value=server_url),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
            ])
            await db.commit()
        async with httpx.AsyncClient(base_url=server_url, timeout=20, trust_env=False) as client:
            admin = await _register(client, "admin", admin=True)
            first = await _register(client, "first")
            second = await _register(client, "second")
            client.cookies.clear()
            configured = await client.put(
                "/api/admin/server-mcp",
                headers=_auth(admin),
                json={
                    "base_config_revision": 1,
                    "mcp_servers": [
                        {
                            "name": "http", "transport": "streamable_http", "url": remote_url,
                            "headers": {}, "enabled_capabilities": [], "max_concurrent_calls": 8,
                        },
                        {
                            "name": "local", "transport": "stdio", "command": sys.executable,
                            "args": [str(_STDIO_FIXTURE)], "cwd": str(_STDIO_FIXTURE.parent),
                            "env": {}, "enabled_capabilities": [], "max_concurrent_calls": 1,
                        },
                    ],
                },
            )
            assert configured.status_code == 200, configured.text
            assert configured.json()["config_revision"] == 2
            assert configured.json()["runtimes"]["http"]["active_sessions"] == 0
            assert configured.json()["runtimes"]["local"]["active_sessions"] == 0

            conversation = str(uuid4())
            await _chat(client, first, conversation)
            assert len(provider.calls) == 4
            assert _schemas(provider.calls[0])["mcp_http_echo"]["description"] != (
                _schemas(provider.calls[1])["mcp_http_echo"]["description"]
            )
            wire = json.dumps(provider.calls, ensure_ascii=False)
            assert "http-counter:1" in wire
            assert "stdio-counter:1" in wire

            await _chat(client, first, conversation)
            assert len(provider.calls) == 7
            wire = json.dumps(provider.calls[4:7], ensure_ascii=False)
            assert "http-counter:2" in wire
            assert "stdio-counter:2" in wire

            client.cookies.clear()
            foreign = await client.post(
                f"/api/sessions/{conversation}/messages",
                headers=_auth(second),
                json={
                    "content": [{"type": "text", "text": "Try another owner's conversation."}],
                    "attachments": [],
                },
            )
            assert foreign.status_code == 404, foreign.text
            assert len(provider.calls) == 7

            await _chat(client, second, str(uuid4()))
            assert len(provider.calls) == 10
            wire = json.dumps(provider.calls[7:10], ensure_ascii=False)
            assert "http-counter:1" in wire
            assert "stdio-counter:1" in wire
            client.cookies.clear()
            states = await client.get("/api/admin/server-mcp", headers=_auth(admin))
            assert states.status_code == 200, states.text
            for name in ("http", "local"):
                slot = states.json()["runtimes"][name]
                assert slot["active_sessions"] == 0
                assert slot["idle_sessions"] == 2
                assert slot["active_calls"] == 0
            deleted = await client.delete(
                f"/api/sessions/{conversation}", headers=_auth(first)
            )
            assert deleted.status_code == 204, deleted.text
            after_delete = await client.get("/api/admin/server-mcp", headers=_auth(admin))
            assert after_delete.status_code == 200, after_delete.text
            for name in ("http", "local"):
                assert after_delete.json()["runtimes"][name]["idle_sessions"] == 1
            deleted_user = await client.delete(
                f"/api/admin/users/{second['user']['id']}", headers=_auth(admin)
            )
            assert deleted_user.status_code == 204, deleted_user.text
            after_user_delete = await client.get(
                "/api/admin/server-mcp", headers=_auth(admin)
            )
            assert after_user_delete.status_code == 200, after_user_delete.text
            for name in ("http", "local"):
                assert after_user_delete.json()["runtimes"][name]["idle_sessions"] == 0
            assert (await client.get("/health")).status_code == 200
    finally:
        with contextlib.suppress(BaseException):
            await supervisor.begin_shutdown()
        with contextlib.suppress(BaseException):
            await runtime.close()
        with contextlib.suppress(BaseException):
            await supervisor.shutdown()
        if server is not None and server_task is not None and listener is not None:
            with contextlib.suppress(BaseException):
                await _stop_server(server, server_task, listener, registry, get_engine())
        else:
            with contextlib.suppress(BaseException):
                await registry.close()
            with contextlib.suppress(BaseException):
                await get_engine().dispose()
        if remote is not None and remote.returncode is None:
            remote.terminate()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(remote.wait(), timeout=5)
            if remote.returncode is None:
                remote.kill()
                await remote.wait()
        get_settings.cache_clear()
        get_engine.cache_clear()
        get_device_registry.cache_clear()
    assert supervisor.runtime_snapshot(None) == {}
    assert remote is not None and remote.returncode is not None


async def test_private_http_client_expires_after_idle_timeout(tmp_path: Path) -> None:
    schema_file = tmp_path / "remote-schema"
    schema_file.write_text("echo", encoding="utf-8")
    remote, url = await _start_remote(schema_file)
    supervisor = ServerMcpSupervisor(
        discoverer=discover_server_catalog, idle_seconds=0.05
    )
    try:
        configs = parse_server_mcp_configs([{
            "name": "http", "transport": "streamable_http", "url": url,
            "headers": {}, "enabled_capabilities": [], "max_concurrent_calls": 8,
        }])
        candidate = await supervisor.validate(
            configs=configs, changed_names=("http",), validate_servers=("http",)
        )
        envelope = server_mcp.build_candidate_envelope(
            empty_server_mcp_envelope(), configs,
            validate_servers=("http",), source_catalog=candidate.source_catalog,
        )
        await supervisor.publish(candidate, envelope)
        user_id, session_id = uuid4(), uuid4()
        async with supervisor.run(user_id=user_id, session_id=session_id):
            private, generations = await supervisor.prepare(
                user_id=user_id, session_id=session_id, envelope=envelope
            )
            assert private.mcp_catalog.servers
            assert isinstance(generations["http"], UUID)
            assert supervisor.runtime_snapshot(envelope)["http"].active_sessions == 1
        assert supervisor.runtime_snapshot(envelope)["http"].idle_sessions == 1
        deadline = asyncio.get_running_loop().time() + 3
        while supervisor.runtime_snapshot(envelope)["http"].idle_sessions:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        assert supervisor.runtime_snapshot(envelope)["http"].active_sessions == 0
    finally:
        await supervisor.begin_shutdown()
        await supervisor.shutdown()
        if remote.returncode is None:
            remote.terminate()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(remote.wait(), timeout=5)
            if remote.returncode is None:
                remote.kill()
                await remote.wait()
    assert supervisor.runtime_snapshot(None) == {}
