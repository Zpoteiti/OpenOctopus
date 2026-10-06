"""Opt-in real TCP Py7 MCP E2E for source and frozen clients.

Set ``PY7_REAL_E2E=1`` to run this test. ``OO_CLIENT_BIN`` selects a frozen
client; otherwise the current source client is launched as a subprocess.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from test_device_client_e2e import (
    _ProviderStep,
    _ScriptedProvider,
    _start_client,
    _start_server,
    _stop_client,
    _stop_server,
    _wait_online,
)

from openctopus_server.api.router import router as api_router
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import Device, SystemConfig
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.mcp_routes import (
    OwnerMcpDevice,
    build_owner_mcp_snapshot,
)
from openctopus_server.devices.registry import DeviceMcpUnavailableError
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.http import register_error_handler
from openctopus_server.services.devices import parse_stored_mcp_catalog
from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME
from openctopus_server.tools.registry import ToolRegistry, _owned_mcp_route_resolver

pytestmark = pytest.mark.skipif(
    os.environ.get("PY7_REAL_E2E") != "1",
    reason="set PY7_REAL_E2E=1 to run the real TCP Py7 MCP E2E",
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MCP_FIXTURE = _REPO_ROOT / "client" / "tests" / "fixtures" / "fake_mcp_surfaces_stdio.py"


def _remote_mcp_result(request: dict[str, Any], *, marker: str) -> dict[str, Any]:
    request_id = request["id"]
    method = request["method"]
    params = request.get("params", {})
    if method == "initialize":
        result: dict[str, Any] = {
            "protocolVersion": params["protocolVersion"],
            "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
            "serverInfo": {"name": marker, "version": "1"},
        }
    elif method == "tools/list":
        result = {
            "tools": [
                {
                    "name": "echo",
                    "description": f"Echo through {marker}.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                        "additionalProperties": False,
                    },
                }
            ]
        }
    elif method == "resources/list":
        result = {"resources": []}
    elif method == "resources/templates/list":
        result = {"resourceTemplates": []}
    elif method == "prompts/list":
        result = {"prompts": []}
    elif method == "tools/call":
        result = {
            "content": [
                {
                    "type": "text",
                    "text": f"{marker}:{params['arguments']['text']}",
                }
            ]
        }
    else:
        raise AssertionError(f"unexpected remote MCP request: {method}")
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _install_remote_mcp_fixtures(app: FastAPI) -> None:
    sse_sessions: dict[str, asyncio.Queue[str | None]] = {}

    @app.post("/_test/mcp/streamable")
    async def streamable_mcp(request: Request) -> Response:
        payload = cast(dict[str, Any], await request.json())
        if "id" not in payload:
            return Response(status_code=202)
        return JSONResponse(_remote_mcp_result(payload, marker="streamable"))

    @app.get("/_test/mcp/sse")
    async def legacy_sse(request: Request) -> StreamingResponse:
        session_id = uuid4().hex
        queue: asyncio.Queue[str | None] = asyncio.Queue()
        sse_sessions[session_id] = queue
        endpoint = str(request.base_url).rstrip("/") + (
            f"/_test/mcp/sse-messages?session_id={session_id}"
        )

        async def events() -> AsyncIterator[str]:
            try:
                yield f"event: endpoint\ndata: {endpoint}\n\n"
                while (event := await queue.get()) is not None:
                    yield f"event: message\ndata: {event}\n\n"
            finally:
                sse_sessions.pop(session_id, None)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    @app.post("/_test/mcp/sse-messages")
    async def legacy_sse_messages(request: Request) -> Response:
        session_id = request.query_params.get("session_id")
        queue = sse_sessions.get(session_id or "")
        if queue is None:
            return Response(status_code=404)
        payload = cast(dict[str, Any], await request.json())
        if "id" in payload:
            queue.put_nowait(
                json.dumps(
                    _remote_mcp_result(payload, marker="sse"),
                    separators=(",", ":"),
                )
            )
        return Response(status_code=202)


async def _load_owner_snapshot(
    *,
    engine: AsyncEngine,
    user_id: UUID,
) -> Any:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        rows = list(
            (
                await db.execute(
                    select(
                        Device.id,
                        Device.name,
                        Device.config_revision,
                        Device.mcp_catalog,
                    )
                    .where(Device.user_id == user_id)
                    .order_by(Device.created_at, Device.id)
                )
            )
            .tuples()
            .all()
        )
    return build_owner_mcp_snapshot(
        [
            OwnerMcpDevice(
                device_id=device_id,
                name=name,
                config_revision=config_revision,
                catalog=parse_stored_mcp_catalog(catalog),
            )
            for device_id, name, config_revision, catalog in rows
        ]
    )


async def _wait_mcp_ready(
    *,
    engine: AsyncEngine,
    registry: Any,
    device_id: UUID,
    user_id: UUID,
) -> Any:
    for _ in range(240):
        async with AsyncSession(engine, expire_on_commit=False) as db:
            row = (
                await db.execute(
                    select(
                        Device.name,
                        Device.config_revision,
                        Device.mcp_catalog,
                    ).where(Device.id == device_id)
                )
            ).one()
        catalog = parse_stored_mcp_catalog(row.mcp_catalog)
        snapshot = build_owner_mcp_snapshot(
            [
                OwnerMcpDevice(
                    device_id=device_id,
                    name=row.name,
                    config_revision=row.config_revision,
                    catalog=catalog,
                )
            ]
        )
        route = next(route for route in snapshot.routes if route.final_name == "mcp_local_echo")
        try:
            result = await registry.dispatch_mcp_tool(
                route=route,
                user_id=user_id,
                name=route.final_name,
                args={"text": "ready"},
                max_result_bytes=64 * 1024,
                timeout_seconds=5,
            )
        except DeviceMcpUnavailableError:
            await asyncio.sleep(0.05)
            continue
        assert result.is_error is False
        assert "echo:ready" in str(result.content)
        return snapshot
    raise AssertionError("MCP runtime did not publish a ready registration")


async def test_real_client_validates_registers_and_runs_all_mcp_surfaces(
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
    registry = get_device_registry()
    device_name = "py7-mcp-client"
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "mcp-tool-1",
                        "name": "mcp_local_echo",
                        "input": {"text": "agent", DEVICE_FIELD_NAME: device_name},
                    }
                ]
            ),
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "mcp-resource-1",
                        "name": "mcp_local_manual",
                        "input": {DEVICE_FIELD_NAME: device_name},
                    }
                ]
            ),
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "mcp-template-1",
                        "name": "mcp_local_issue",
                        "input": {"id": "42", DEVICE_FIELD_NAME: device_name},
                    }
                ]
            ),
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "mcp-prompt-1",
                        "name": "mcp_local_explain",
                        "input": {"topic": "Py7", DEVICE_FIELD_NAME: device_name},
                    }
                ]
            ),
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "mcp-streamable-1",
                        "name": "mcp_remote_echo",
                        "input": {"text": "http", DEVICE_FIELD_NAME: device_name},
                    }
                ]
            ),
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "mcp-sse-1",
                        "name": "mcp_legacy_echo",
                        "input": {"text": "legacy", DEVICE_FIELD_NAME: device_name},
                    }
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "MCP surfaces completed."}]),
        ]
    )
    tool_registry = ToolRegistry(
        (),
        mcp_route_resolver=_owned_mcp_route_resolver(pg_engine),
    )
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=tool_registry,
        device_registry=registry,
    )
    app = FastAPI()
    app.include_router(api_router)
    _install_remote_mcp_fixtures(app)
    register_error_handler(app)
    app.state.chat_runtime = runtime
    server, server_task, server_url, listener = await _start_server(app)
    client_process: asyncio.subprocess.Process | None = None
    token: str | None = None
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    try:
        async with httpx.AsyncClient(base_url=server_url, timeout=10, trust_env=False) as client:
            async with AsyncSession(pg_engine, expire_on_commit=False) as db:
                db.add_all(
                    [
                        SystemConfig(key="llm_endpoint", value="http://fake.test"),
                        SystemConfig(key="llm_api_key", value="fake-key"),
                        SystemConfig(key="llm_model", value="fake-model"),
                    ]
                )
                await db.commit()

            auth_response = await client.post(
                "/api/auth/register",
                json={
                    "email": f"py7-mcp-{uuid4().hex}@example.com",
                    "password": "testpassword",
                    "name": "Py7 MCP E2E",
                },
            )
            assert auth_response.status_code == 201, auth_response.text
            auth = auth_response.json()
            jwt = cast(str, auth["jwt"])
            user_id = UUID(auth["user"]["id"])
            headers = {"Authorization": f"Bearer {jwt}"}

            create_response = await client.post(
                "/api/devices",
                headers=headers,
                json={
                    "name": device_name,
                    "workspace_path": str(workspace),
                    "restrict_to_workspace": True,
                    "ssrf_denylist": [],
                },
            )
            assert create_response.status_code == 201, create_response.text
            created = create_response.json()
            token = cast(str, created["token"])
            device_id = UUID(created["device"]["id"])

            client_process = await _start_client(server_url, token)
            await _wait_online(client, jwt, device_name, online=True, process=client_process)

            patch_response = await client.patch(
                f"/api/devices/{device_name}/config",
                headers=headers,
                json={
                    "base_config_revision": 1,
                    "mcp_servers": [
                        {
                            "name": "local",
                            "transport": "stdio",
                            "command": sys.executable,
                            "args": [str(_MCP_FIXTURE)],
                            "cwd": str(_MCP_FIXTURE.parent),
                            "env": {},
                            "enabled_capabilities": [],
                        },
                        {
                            "name": "remote",
                            "transport": "streamable_http",
                            "url": f"{server_url}/_test/mcp/streamable",
                            "headers": {},
                            "enabled_capabilities": [],
                        },
                        {
                            "name": "legacy",
                            "transport": "sse",
                            "url": f"{server_url}/_test/mcp/sse",
                            "headers": {},
                            "enabled_capabilities": [],
                        }
                    ],
                },
            )
            assert patch_response.status_code == 200, patch_response.text
            patched = patch_response.json()
            assert patched["device"]["config_revision"] == 2
            assert {
                entry["final_name"]
                for surfaces in patched["mcp_discovered"].values()
                for entries in surfaces.values()
                for entry in entries
            } == {
                "mcp_local_echo",
                "mcp_local_manual",
                "mcp_local_issue",
                "mcp_local_explain",
                "mcp_remote_echo",
                "mcp_legacy_echo",
            }

            snapshot = await _wait_mcp_ready(
                engine=pg_engine,
                registry=registry,
                device_id=device_id,
                user_id=user_id,
            )
            assert {schema.name for schema in snapshot.schemas} == {
                "mcp_local_echo",
                "mcp_local_manual",
                "mcp_local_issue",
                "mcp_local_explain",
                "mcp_remote_echo",
                "mcp_legacy_echo",
            }

            session_id = uuid4()
            chat_response = await client.post(
                f"/api/sessions/{session_id}/messages",
                headers=headers,
                json={
                    "content": [{"type": "text", "text": "Run every MCP surface."}],
                    "attachments": [],
                },
            )
            assert chat_response.status_code == 200, chat_response.text
            assert len(provider.calls) == 7
            provider_wire = json.dumps(provider.calls, ensure_ascii=False)
            assert "echo:agent" in provider_wire
            assert "resource:file:///openoctopus-manual.txt" in provider_wire
            assert "resource:https://example.test/issues/42" in provider_wire
            assert "prompt:Py7" in provider_wire
            assert "streamable:http" in provider_wire
            assert "sse:legacy" in provider_wire

            await _stop_client(client_process, expected_returncode=0, secret=token)
            await _wait_online(client, jwt, device_name, online=False)
            client_process = None

            offline_snapshot = await _load_owner_snapshot(
                engine=pg_engine,
                user_id=user_id,
            )
            assert {schema.name for schema in offline_snapshot.schemas} == {
                "mcp_local_echo",
                "mcp_local_manual",
                "mcp_local_issue",
                "mcp_local_explain",
                "mcp_remote_echo",
                "mcp_legacy_echo",
            }
            offline = await tool_registry.execute(
                name="mcp_local_manual",
                args={DEVICE_FIELD_NAME: device_name},
                ctx=ToolContext(user_id=user_id, session_id=uuid4()),
                mcp_snapshot=offline_snapshot,
                device_registry=registry,
            )
            assert offline.is_error is True
            assert offline.code == ErrorCode.TOOL_DEVICE_UNREACHABLE

            removed = await client.patch(
                f"/api/devices/{device_name}/config",
                headers=headers,
                json={"base_config_revision": 2, "mcp_servers": []},
            )
            assert removed.status_code == 200, removed.text
            assert removed.json()["mcp_servers"] == []

            offline_add = await client.patch(
                f"/api/devices/{device_name}/config",
                headers=headers,
                json={
                    "base_config_revision": 3,
                    "mcp_servers": [
                        {
                            "name": "local",
                            "transport": "stdio",
                            "command": sys.executable,
                            "args": [str(_MCP_FIXTURE)],
                            "cwd": str(_MCP_FIXTURE.parent),
                            "env": {},
                            "enabled_capabilities": [],
                        }
                    ],
                },
            )
            assert offline_add.status_code == 409
            assert offline_add.json()["code"] == "device_offline"
    finally:
        if client_process is not None and client_process.returncode is None:
            with contextlib.suppress(Exception):
                await _stop_client(client_process, expected_returncode=0, secret=token)
        try:
            await _stop_server(
                server,
                server_task,
                listener,
                registry,
                get_engine(),
                runtime,
            )
        finally:
            get_settings.cache_clear()
            get_engine.cache_clear()
            get_device_registry.cache_clear()
