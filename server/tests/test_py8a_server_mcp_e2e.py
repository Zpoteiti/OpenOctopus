"""Opt-in real TCP Py8a shared Server MCP and Agent E2E.

Set ``PY8A_REAL_E2E=1`` to run. Every service and credential is a local test
fixture; the scenario never requires public network access.
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from test_device_client_e2e import (
    _start_client,
    _start_server,
    _stop_client,
    _stop_server,
    _wait_online,
)
from test_py7_client_mcp_e2e import _wait_mcp_ready

from openctopus_server.api.router import router as api_router
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import SystemConfig
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.http import register_error_handler
from openctopus_server.mcp import scheduler as mcp_scheduler
from openctopus_server.mcp import supervisor as mcp_supervisor
from openctopus_server.mcp.authority import ServerMcpAuthorityFence
from openctopus_server.mcp.models import empty_server_mcp_envelope
from openctopus_server.mcp.supervisor import ServerMcpSupervisor
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME
from openctopus_server.tools.registry import ToolRegistry, _owned_mcp_route_resolver
from openctopus_server.workspace.service import get_workspace_service
from openctopus_server.workspace.storage import get_object_storage

pytestmark = pytest.mark.skipif(
    os.environ.get("PY8A_REAL_E2E") != "1",
    reason="set PY8A_REAL_E2E=1 to run the real TCP Py8a Server MCP E2E",
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SERVER_STDIO_FIXTURE = Path(__file__).parent / "fixtures" / "py8a_server_mcp_stdio.py"
_REMOTE_MCP_FIXTURE = Path(__file__).parent / "fixtures" / "py8a_server_mcp_remote.py"
_DEVICE_STDIO_FIXTURE = (
    _REPO_ROOT / "client" / "tests" / "fixtures" / "fake_mcp_surfaces_stdio.py"
)
_SECRET_SENTINEL = "py8a-e2e-secret-sentinel"


@dataclass(slots=True)
class _RemoteMcpProcess:
    process: asyncio.subprocess.Process
    port: int
    url: str


class _HealthyObjectStorage:
    async def check_health(self) -> None:
        return None


class _RegistrationWorkspace:
    async def write(self, *args: Any, **kwargs: Any) -> None:
        return None


def _tool_step(tool_id: str, name: str, **arguments: object) -> list[dict[str, Any]]:
    return [
        {
            "type": "tool_use",
            "id": tool_id,
            "name": name,
            "input": {**arguments, DEVICE_FIELD_NAME: "server"},
        }
    ]


def _sse_event(payload: dict[str, Any]) -> str:
    return f"event: {payload['type']}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"


def _anthropic_sse(content: list[dict[str, Any]]) -> str:
    events = [
        _sse_event(
            {
                "type": "message_start",
                "message": {
                    "id": f"msg_{uuid4().hex}",
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": "fake-model",
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 1, "output_tokens": 0},
                },
            }
        )
    ]
    for index, block in enumerate(content):
        if block["type"] == "tool_use":
            start = {
                "type": "tool_use",
                "id": block["id"],
                "name": block["name"],
                "input": {},
            }
            delta = {
                "type": "input_json_delta",
                "partial_json": json.dumps(block["input"], separators=(",", ":")),
            }
        else:
            start = {"type": "text", "text": ""}
            delta = {"type": "text_delta", "text": block["text"]}
        events.extend(
            [
                _sse_event(
                    {
                        "type": "content_block_start",
                        "index": index,
                        "content_block": start,
                    }
                ),
                _sse_event(
                    {
                        "type": "content_block_delta",
                        "index": index,
                        "delta": delta,
                    }
                ),
                _sse_event({"type": "content_block_stop", "index": index}),
            ]
        )
    events.extend(
        [
            _sse_event(
                {
                    "type": "message_delta",
                    "delta": {
                        "stop_reason": (
                            "tool_use"
                            if any(block["type"] == "tool_use" for block in content)
                            else "end_turn"
                        ),
                        "stop_sequence": None,
                    },
                    "usage": {"output_tokens": 1},
                }
            ),
            _sse_event({"type": "message_stop"}),
        ]
    )
    return "".join(events)


class _AnthropicHttpScript:
    def __init__(self, steps: list[list[dict[str, Any]]]) -> None:
        self._steps = deque(steps)
        self.calls: list[dict[str, Any]] = []
        self.api_keys: list[str | None] = []

    async def respond(self, request: Request) -> StreamingResponse:
        if not self._steps:
            raise AssertionError("Anthropic HTTP script exhausted")
        self.calls.append(cast(dict[str, Any], await request.json()))
        self.api_keys.append(request.headers.get("x-api-key"))
        payload = _anthropic_sse(self._steps.popleft())

        async def stream() -> AsyncIterator[str]:
            yield payload

        return StreamingResponse(stream(), media_type="text/event-stream")


def _available_port() -> int:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", 0))
        return cast(int, listener.getsockname()[1])
    finally:
        listener.close()


async def _start_remote_mcp(
    *,
    transport: str,
    marker: str,
    schema_file: Path,
    port: int | None = None,
) -> _RemoteMcpProcess:
    selected_port = _available_port() if port is None else port
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(_REMOTE_MCP_FIXTURE),
        "--transport",
        transport,
        "--port",
        str(selected_port),
        "--marker",
        marker,
        "--schema-file",
        str(schema_file),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    )
    for _ in range(300):
        if process.returncode is not None:
            raise AssertionError(f"remote MCP exited during startup: {process.returncode}")
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", selected_port)
        except OSError:
            await asyncio.sleep(0.01)
            continue
        del reader
        writer.close()
        await writer.wait_closed()
        return _RemoteMcpProcess(
            process=process,
            port=selected_port,
            url=f"http://127.0.0.1:{selected_port}",
        )
    process.kill()
    await process.wait()
    raise AssertionError("remote MCP did not start")


async def _stop_remote_mcp(value: _RemoteMcpProcess, *, force: bool = False) -> None:
    if value.process.returncode is not None:
        return
    if force:
        value.process.kill()
    else:
        value.process.terminate()
    try:
        await asyncio.wait_for(value.process.wait(), timeout=5)
    except TimeoutError:
        value.process.kill()
        await value.process.wait()


async def _register(
    client: httpx.AsyncClient,
    *,
    label: str,
    admin: bool = False,
) -> dict[str, Any]:
    payload: dict[str, object] = {
        "email": f"py8a-{label}-{uuid4().hex}@example.com",
        "password": "testpassword",
        "name": f"Py8a {label}",
    }
    if admin:
        payload["admin_token"] = "dev-admin-token"
    response = await client.post("/api/auth/register", json=payload)
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


def _auth(identity: dict[str, Any]) -> dict[str, str]:
    return {"Authorization": f"Bearer {identity['jwt']}"}


def _device_mcp_config() -> list[dict[str, object]]:
    return [
        {
            "name": "local",
            "transport": "stdio",
            "command": sys.executable,
            "args": [str(_DEVICE_STDIO_FIXTURE)],
            "cwd": str(_DEVICE_STDIO_FIXTURE.parent),
            "env": {},
            "enabled_capabilities": [],
        }
    ]


def _server_mcp_configs(http_url: str, sse_url: str) -> list[dict[str, object]]:
    return [
        {
            "name": "local",
            "transport": "stdio",
            "command": sys.executable,
            "args": [str(_SERVER_STDIO_FIXTURE)],
            "cwd": str(_SERVER_STDIO_FIXTURE.parent),
            "env": {"PY8A_E2E_SECRET": _SECRET_SENTINEL},
            "enabled_capabilities": [],
            "max_concurrent_calls": 8,
        },
        {
            "name": "http",
            "transport": "streamable_http",
            "url": f"{http_url}/mcp",
            "headers": {},
            "enabled_capabilities": [],
            "max_concurrent_calls": 8,
        },
        {
            "name": "legacy",
            "transport": "sse",
            "url": f"{sse_url}/sse",
            "headers": {},
            "enabled_capabilities": [],
            "max_concurrent_calls": 8,
        },
    ]


async def _wait_runtime_state(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
    name: str,
    state: str,
) -> dict[str, Any]:
    last: dict[str, Any] = {}
    for _ in range(600):
        response = await client.get("/api/admin/server-mcp", headers=headers)
        assert response.status_code == 200, response.text
        last = cast(dict[str, Any], response.json())
        active = cast(dict[str, Any], last["runtimes"])[name]["active"]
        if active is not None and active["state"] == state:
            return last
        await asyncio.sleep(0.05)
    raise AssertionError(f"Server MCP runtime did not reach {state}: {last.get('runtimes')}")


async def _wait_runtime_absent(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
    names: set[str],
) -> None:
    last: dict[str, Any] = {}
    for _ in range(300):
        response = await client.get("/api/admin/server-mcp", headers=headers)
        assert response.status_code == 200, response.text
        last = cast(dict[str, Any], response.json())
        if names.isdisjoint(cast(dict[str, Any], last["runtimes"])):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Server MCP runtimes did not clean up: {last.get('runtimes')}")


def _tool_names(call: dict[str, Any]) -> set[str]:
    return {cast(str, schema["name"]) for schema in call.get("tools", [])}


async def test_real_server_mcp_admin_agent_shadow_drift_and_cleanup(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        "OPENOCTOPUS_DATABASE_URL",
        pg_engine.url.render_as_string(hide_password=False),
    )
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_device_registry.cache_clear()
    # Exact 60-second policy boundaries are covered by scheduler/supervisor tests.
    # Accelerate them here while retaining real process and TCP failure boundaries.
    monkeypatch.setattr(mcp_scheduler, "PUBLIC_DEADLINE_SECONDS", 3.0)
    monkeypatch.setattr(mcp_supervisor, "REMOTE_RESULT_DRAIN_SECONDS", 3.0)

    provider = _AnthropicHttpScript(
        [
            _tool_step("server-tool", "mcp_local_echo", text="agent"),
            _tool_step("server-resource", "mcp_local_manual"),
            _tool_step("server-template", "mcp_local_issue", issue_id="42"),
            _tool_step("server-prompt", "mcp_local_explain", topic="Py8a"),
            _tool_step("server-http", "mcp_http_echo", text="remote"),
            _tool_step("server-sse", "mcp_legacy_echo", text="legacy"),
            [{"type": "text", "text": "All surfaces completed."}],
            _tool_step("server-http-down", "mcp_http_echo", text="down"),
            [{"type": "text", "text": "Failure boundary handled."}],
            _tool_step("server-http-unavailable", "mcp_http_echo", text="unavailable"),
            [{"type": "text", "text": "Unavailable handled."}],
            _tool_step("server-http-v2", "mcp_http_lookup", text="changed"),
            [{"type": "text", "text": "Changed schema accepted."}],
        ]
    )
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
        pg_engine,
        tool_registry=tool_registry,
        device_registry=registry,
        server_mcp_generation_resolver=supervisor.ready_generations,
    )

    http_mcp: _RemoteMcpProcess | None = None
    sse_mcp: _RemoteMcpProcess | None = None
    remote_processes: list[_RemoteMcpProcess] = []
    oo_server: uvicorn.Server | None = None
    oo_task: asyncio.Task[None] | None = None
    oo_listener: socket.socket | None = None
    client_processes: list[asyncio.subprocess.Process] = []
    client_tokens: dict[int, str] = {}

    try:
        http_schema = tmp_path / "http-schema"
        sse_schema = tmp_path / "sse-schema"
        http_schema.write_text("echo", encoding="utf-8")
        sse_schema.write_text("echo", encoding="utf-8")
        http_mcp = await _start_remote_mcp(
            transport="streamable_http",
            marker="http",
            schema_file=http_schema,
        )
        sse_mcp = await _start_remote_mcp(
            transport="sse",
            marker="sse",
            schema_file=sse_schema,
        )
        remote_processes.extend((http_mcp, sse_mcp))
        configs = _server_mcp_configs(http_mcp.url, sse_mcp.url)
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
        app.dependency_overrides[get_object_storage] = lambda: _HealthyObjectStorage()
        app.dependency_overrides[get_workspace_service] = lambda: _RegistrationWorkspace()
        oo_server, oo_task, oo_url, oo_listener = await _start_server(app)

        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            db.add_all(
                [
                    SystemConfig(key="llm_endpoint", value=oo_url),
                    SystemConfig(key="llm_api_key", value="fake-key"),
                    SystemConfig(key="llm_model", value="fake-model"),
                ]
            )
            await db.commit()

        async with httpx.AsyncClient(
            base_url=oo_url,
            timeout=15,
            trust_env=False,
        ) as client:
            admin = await _register(client, label="admin", admin=True)
            owners = [
                await _register(client, label="owner-one"),
                await _register(client, label="owner-two"),
            ]
            client.cookies.clear()
            admin_headers = _auth(admin)
            initial = await client.get("/api/admin/server-mcp", headers=admin_headers)
            assert initial.status_code == 200, initial.text
            assert initial.json()["config_revision"] == 1
            assert initial.json()["mcp_servers"] == []

            devices: list[tuple[dict[str, Any], str, UUID, asyncio.subprocess.Process]] = []
            for index, owner in enumerate(owners, start=1):
                name = f"owner-{index}-laptop"
                headers = _auth(owner)
                workspace = tmp_path / name
                workspace.mkdir()
                created = await client.post(
                    "/api/devices",
                    headers=headers,
                    json={"name": name, "workspace_path": str(workspace)},
                )
                assert created.status_code == 201, created.text
                token = cast(str, created.json()["token"])
                device_id = UUID(created.json()["device"]["id"])
                process = await _start_client(oo_url, token)
                client_processes.append(process)
                client_tokens[id(process)] = token
                await _wait_online(
                    client,
                    cast(str, owner["jwt"]),
                    name,
                    online=True,
                    process=process,
                )
                patched = await client.patch(
                    f"/api/devices/{name}/config",
                    headers=headers,
                    json={"base_config_revision": 1, "mcp_servers": _device_mcp_config()},
                )
                assert patched.status_code == 200, patched.text
                assert patched.json()["device"]["config_revision"] == 2
                await _wait_mcp_ready(
                    engine=pg_engine,
                    registry=registry,
                    device_id=device_id,
                    user_id=UUID(owner["user"]["id"]),
                )
                devices.append((owner, name, device_id, process))

            handles = {
                device_id: await registry.get_handle(
                    device_id,
                    user_id=UUID(owner["user"]["id"]),
                    expected_device_name=name,
                )
                for owner, name, device_id, _process in devices
            }
            assert all(handle is not None for handle in handles.values())

            added = await client.put(
                "/api/admin/server-mcp",
                headers=admin_headers,
                json={"base_config_revision": 1, "mcp_servers": configs},
            )
            assert added.status_code == 200, added.text
            added_body = added.json()
            assert added_body["config_revision"] == 2
            added_configs = {
                config["name"]: config for config in added_body["mcp_servers"]
            }
            assert added_configs["local"]["env"] == {
                "PY8A_E2E_SECRET": "<redacted>"
            }
            assert _SECRET_SENTINEL not in added.text
            stale = await client.put(
                "/api/admin/server-mcp",
                headers=admin_headers,
                json={"base_config_revision": 1, "mcp_servers": added_body["mcp_servers"]},
            )
            assert stale.status_code == 409, stale.text
            assert stale.json()["code"] == ErrorCode.SERVER_MCP_CONFIG_CONFLICT
            await _wait_runtime_state(
                client,
                headers=admin_headers,
                name="local",
                state="ready",
            )
            await _wait_runtime_state(
                client,
                headers=admin_headers,
                name="http",
                state="ready",
            )
            ready = await _wait_runtime_state(
                client,
                headers=admin_headers,
                name="legacy",
                state="ready",
            )
            discovered_names = {
                capability["final_name"]
                for surfaces in ready["mcp_discovered"].values()
                for capabilities in surfaces.values()
                for capability in capabilities
            }
            assert discovered_names == {
                "mcp_local_echo",
                "mcp_local_manual",
                "mcp_local_issue",
                "mcp_local_explain",
                "mcp_http_echo",
                "mcp_legacy_echo",
            }

            for owner, name, device_id, process in devices:
                shadowed = await client.get(
                    f"/api/devices/{name}/config",
                    headers=_auth(owner),
                )
                assert shadowed.status_code == 200, shadowed.text
                assert shadowed.json()["device"]["config_revision"] == 2
                assert shadowed.json()["mcp_servers"][0]["effective_status"] == (
                    "shadowed_by_server"
                )
                assert all(
                    capability["suppression_reason"] == "server_namespace_reserved"
                    and capability["provider_visible"] is False
                    for capabilities in shadowed.json()["mcp_discovered"]["local"].values()
                    for capability in capabilities
                )
                assert process.returncode is None
                assert (
                    await registry.get_handle(
                        device_id,
                        user_id=UUID(owner["user"]["id"]),
                        expected_device_name=name,
                    )
                    == handles[device_id]
                )

            owner = owners[0]
            owner_headers = _auth(owner)
            first_chat = await client.post(
                f"/api/sessions/{uuid4()}/messages",
                headers=owner_headers,
                json={
                    "content": [{"type": "text", "text": "Run every shared MCP surface."}],
                    "attachments": [],
                },
            )
            assert first_chat.status_code == 200, first_chat.text
            assert len(provider.calls) == 7
            provider_wire = json.dumps(provider.calls, ensure_ascii=False)
            for marker in (
                "stdio:agent",
                "resource:py8a-manual",
                "resource:py8a-issue-42",
                "prompt:Py8a",
                "http:remote",
                "sse:legacy",
            ):
                assert marker in provider_wire
            assert all(key == "fake-key" for key in provider.api_keys)
            local_schema = next(
                schema
                for schema in provider.calls[0]["tools"]
                if schema["name"] == "mcp_local_echo"
            )
            assert local_schema["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"] == [
                "server"
            ]

            await _stop_remote_mcp(http_mcp, force=True)
            health = await client.get("/health")
            assert health.status_code == 200, health.text
            assert health.json()["status"] == "ok"

            down_chat = await client.post(
                f"/api/sessions/{uuid4()}/messages",
                headers=owner_headers,
                json={
                    "content": [{"type": "text", "text": "Try the unavailable HTTP MCP."}],
                    "attachments": [],
                },
            )
            assert down_chat.status_code == 200, down_chat.text
            assert len(provider.calls) == 9
            assert "mcp_http_echo" in _tool_names(provider.calls[7])
            assert any(
                code in json.dumps(provider.calls[8], ensure_ascii=False)
                for code in ("tool_execution_outcome_unknown", "tool_mcp_unavailable")
            )
            down = await _wait_runtime_state(
                client,
                headers=admin_headers,
                name="http",
                state="backoff",
            )
            assert "mcp_http_echo" in {
                capability["final_name"]
                for capability in down["mcp_discovered"]["http"]["tools"]
            }
            unavailable_chat = await client.post(
                f"/api/sessions/{uuid4()}/messages",
                headers=owner_headers,
                json={
                    "content": [{"type": "text", "text": "Retry the unavailable HTTP MCP."}],
                    "attachments": [],
                },
            )
            assert unavailable_chat.status_code == 200, unavailable_chat.text
            assert len(provider.calls) == 11
            assert "mcp_http_echo" in _tool_names(provider.calls[9])
            assert "tool_mcp_unavailable" in json.dumps(provider.calls[10], ensure_ascii=False)

            http_schema.write_text("lookup", encoding="utf-8")
            http_mcp = await _start_remote_mcp(
                transport="streamable_http",
                marker="http-v2",
                schema_file=http_schema,
                port=http_mcp.port,
            )
            remote_processes.append(http_mcp)
            drifted = await _wait_runtime_state(
                client,
                headers=admin_headers,
                name="http",
                state="drifted",
            )
            assert {
                capability["final_name"]
                for capability in drifted["mcp_discovered"]["http"]["tools"]
            } == {"mcp_http_echo"}

            accepted = await client.put(
                "/api/admin/server-mcp",
                headers=admin_headers,
                json={"base_config_revision": 2, "mcp_servers": configs},
            )
            assert accepted.status_code == 200, accepted.text
            assert accepted.json()["config_revision"] == 3
            assert {
                capability["final_name"]
                for capability in accepted.json()["mcp_discovered"]["http"]["tools"]
            } == {"mcp_http_lookup"}
            await _wait_runtime_state(
                client,
                headers=admin_headers,
                name="http",
                state="ready",
            )
            changed_chat = await client.post(
                f"/api/sessions/{uuid4()}/messages",
                headers=owner_headers,
                json={
                    "content": [{"type": "text", "text": "Use the accepted HTTP schema."}],
                    "attachments": [],
                },
            )
            assert changed_chat.status_code == 200, changed_chat.text
            assert len(provider.calls) == 13
            assert "mcp_http_lookup" in _tool_names(provider.calls[11])
            assert "mcp_http_echo" not in _tool_names(provider.calls[11])
            assert "http-v2:changed" in json.dumps(provider.calls[12], ensure_ascii=False)

            removed_local = await client.put(
                "/api/admin/server-mcp",
                headers=admin_headers,
                json={"base_config_revision": 3, "mcp_servers": configs[1:]},
            )
            assert removed_local.status_code == 200, removed_local.text
            assert removed_local.json()["config_revision"] == 4
            for owner, name, device_id, process in devices:
                restored = await client.get(
                    f"/api/devices/{name}/config",
                    headers=_auth(owner),
                )
                assert restored.status_code == 200, restored.text
                assert restored.json()["device"]["config_revision"] == 2
                assert restored.json()["mcp_servers"][0]["effective_status"] == "active"
                assert all(
                    capability["provider_visible"] is True
                    and capability["suppression_reason"] is None
                    for capabilities in restored.json()["mcp_discovered"]["local"].values()
                    for capability in capabilities
                )
                assert process.returncode is None
                assert (
                    await registry.get_handle(
                        device_id,
                        user_id=UUID(owner["user"]["id"]),
                        expected_device_name=name,
                    )
                    == handles[device_id]
                )

            removed_all = await client.put(
                "/api/admin/server-mcp",
                headers=admin_headers,
                json={"base_config_revision": 4, "mcp_servers": []},
            )
            assert removed_all.status_code == 200, removed_all.text
            assert removed_all.json()["config_revision"] == 5
            await _wait_runtime_absent(
                client,
                headers=admin_headers,
                names={"local", "http", "legacy"},
            )
    finally:
        for process in client_processes:
            if process.returncode is None:
                with contextlib.suppress(BaseException):
                    await _stop_client(
                        process,
                        expected_returncode=0,
                        secret=client_tokens.get(id(process)),
                    )
        with contextlib.suppress(BaseException):
            await supervisor.begin_shutdown()
        with contextlib.suppress(BaseException):
            await runtime.close()
        with contextlib.suppress(BaseException):
            await supervisor.shutdown()
        if (
            oo_server is not None
            and oo_task is not None
            and oo_listener is not None
        ):
            with contextlib.suppress(BaseException):
                await _stop_server(
                    oo_server,
                    oo_task,
                    oo_listener,
                    registry,
                    get_engine(),
                )
        else:
            with contextlib.suppress(BaseException):
                await registry.close()
            with contextlib.suppress(BaseException):
                await get_engine().dispose()
        for remote_process in reversed(remote_processes):
            with contextlib.suppress(BaseException):
                await _stop_remote_mcp(remote_process)
        get_settings.cache_clear()
        get_engine.cache_clear()
        get_device_registry.cache_clear()

    assert supervisor.snapshot() == {}
    assert all(process.returncode == 0 for process in client_processes)
    assert all(process.process.returncode is not None for process in remote_processes)
