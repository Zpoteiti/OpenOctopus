"""Opt-in real TCP Py6 exec E2E for source and frozen clients.

Set ``PY6_REAL_E2E=1`` to run this test.  ``OO_CLIENT_BIN`` selects a frozen
client; without it the current source client is launched as a subprocess.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
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
from openctopus_server.db.models import SystemConfig
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.protocol import ToolResultFrame
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.http import register_error_handler
from openctopus_server.tools.registry import (
    _EXEC_SCHEMA,
    ToolRegistry,
    _ClientOnlyTool,
    _owned_device_resolver,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("PY6_REAL_E2E") != "1",
    reason="set PY6_REAL_E2E=1 to run the real TCP Py6 exec E2E",
)

_SESSION_ID = re.compile(r"^session_id=([0-9a-f-]{36})$", re.MULTILINE)


def _content(result: ToolResultFrame) -> str:
    assert isinstance(result.content, str)
    return result.content


def _session_id(result: ToolResultFrame) -> UUID:
    matched = _SESSION_ID.search(_content(result))
    assert matched is not None
    return UUID(matched.group(1))


async def _dispatch(
    registry: DeviceRegistry,
    *,
    device_id: UUID,
    user_id: UUID,
    device_name: str,
    chat_id: UUID,
    name: str,
    args: dict[str, object],
    timeout: float = 10,
) -> ToolResultFrame:
    return await registry.dispatch_tool(
        device_id=device_id,
        user_id=user_id,
        name=name,
        args=args,
        max_result_bytes=128_000,
        timeout_seconds=timeout,
        expected_device_name=device_name,
        chat_session_id=chat_id,
    )


async def _drop_retryable_connection(
    registry: DeviceRegistry,
    *,
    device_id: UUID,
    user_id: UUID,
    device_name: str,
) -> int:
    """Close the live test transport like a transient server/network restart."""

    handle = await registry.get_handle(
        device_id,
        user_id=user_id,
        expected_device_name=device_name,
    )
    assert handle is not None
    connections = cast(dict[UUID, Any], registry._connections)
    connection = connections[device_id]
    await connection.transport.close(1001, "test_restart")
    return handle.generation


async def _wait_for_new_generation(
    registry: DeviceRegistry,
    *,
    device_id: UUID,
    user_id: UUID,
    device_name: str,
    previous_generation: int,
    process: asyncio.subprocess.Process,
) -> None:
    for _ in range(300):
        if process.returncode is not None:
            raise AssertionError("client exited instead of reconnecting")
        handle = await registry.get_handle(
            device_id,
            user_id=user_id,
            expected_device_name=device_name,
        )
        if handle is not None and handle.generation > previous_generation:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("client did not reconnect with a new generation")


async def test_real_client_exec_pipe_tty_reconnect_and_chat_isolation(
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
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[
                    {
                        "type": "tool_use",
                        "id": "agent-exec-1",
                        "name": "exec",
                        "input": {
                            "command": (
                                "printf 'py6-agent-sentinel\\n' > agent-sentinel.txt; "
                                "printf 'py6-agent-output\\n'"
                            ),
                            "yield_time_ms": 3000,
                            "openoctopus_device": "py6-linux",
                        },
                    }
                ]
            ),
            _ProviderStep(content=[{"type": "text", "text": "Agent exec completed."}]),
        ]
    )
    app = FastAPI()
    app.include_router(api_router)
    register_error_handler(app)
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry(
            (_ClientOnlyTool("exec", _EXEC_SCHEMA),),
            trusted_device_resolver=_owned_device_resolver(pg_engine, trusted_only=True),
        ),
        device_registry=registry,
    )
    app.state.chat_runtime = runtime
    server, server_task, server_url, listener = await _start_server(app)
    client_process: asyncio.subprocess.Process | None = None
    token: str | None = None

    workspace = tmp_path / "trusted-workspace"
    workspace.mkdir()
    device_name = "py6-linux"
    try:
        async with httpx.AsyncClient(
            base_url=server_url,
            timeout=10,
            trust_env=False,
        ) as http_client:
            async with AsyncSession(pg_engine, expire_on_commit=False) as db:
                db.add_all(
                    [
                        SystemConfig(key="llm_endpoint", value="http://fake.test"),
                        SystemConfig(key="llm_api_key", value="fake-key"),
                        SystemConfig(key="llm_model", value="fake-model"),
                    ]
                )
                await db.commit()

            auth_response = await http_client.post(
                "/api/auth/register",
                json={
                    "email": f"py6-exec-{uuid4().hex}@example.com",
                    "password": "testpassword",
                    "name": "Py6 Exec E2E",
                },
            )
            assert auth_response.status_code == 201, auth_response.text
            auth = auth_response.json()
            jwt = cast(str, auth["jwt"])
            user_id = UUID(auth["user"]["id"])
            headers = {"Authorization": f"Bearer {jwt}"}

            create_response = await http_client.post(
                "/api/devices",
                headers=headers,
                json={
                    "name": device_name,
                    "workspace_path": str(workspace),
                    "sandbox_mode": False,
                    "ssrf_denylist": [],
                    "shell_timeout_max": 120,
                    "env_allowlist": ["PATH", "HOME", "LANG", "TERM"],
                },
            )
            assert create_response.status_code == 201, create_response.text
            created = create_response.json()
            token = cast(str, created["token"])
            device_id = UUID(created["device"]["id"])

            client_process = await _start_client(server_url, token)
            await _wait_online(
                http_client,
                jwt,
                device_name,
                online=True,
                process=client_process,
            )

            dispatch_calls: list[dict[str, Any]] = []
            original_dispatch = registry.dispatch_tool

            async def record_dispatch(**kwargs: Any) -> ToolResultFrame:
                dispatch_calls.append(dict(kwargs))
                return await original_dispatch(**kwargs)

            monkeypatch.setattr(registry, "dispatch_tool", record_dispatch)

            owner_chat = uuid4()
            foreign_chat = uuid4()
            pipe = await _dispatch(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                chat_id=owner_chat,
                name="exec",
                args={
                    "command": "printf 'pipe-e2e\\n'",
                    "timeout": 10,
                    "yield_time_ms": 3000,
                },
            )
            assert pipe.is_error is False, _content(pipe)
            assert "status=exited" in _content(pipe)
            assert "stdout=pipe-e2e\n" in _content(pipe)

            background = await _dispatch(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                chat_id=owner_chat,
                name="exec",
                args={
                    "command": "printf 'before-reconnect\\n'; sleep 30",
                    "timeout": 60,
                    "yield_time_ms": 100,
                },
            )
            assert background.is_error is False, _content(background)
            assert "status=running" in _content(background)
            assert "stdout=before-reconnect\n" in _content(background)
            background_id = _session_id(background)

            foreign = await _dispatch(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                chat_id=foreign_chat,
                name="write_stdin",
                args={"session_id": str(background_id), "yield_time_ms": 0},
            )
            assert foreign.is_error is True
            assert foreign.code == "tool_exec_session_not_found"

            previous_generation = await _drop_retryable_connection(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
            )
            await _wait_for_new_generation(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                previous_generation=previous_generation,
                process=client_process,
            )

            resumed = await _dispatch(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                chat_id=owner_chat,
                name="write_stdin",
                args={"session_id": str(background_id), "yield_time_ms": 0},
            )
            assert resumed.is_error is False, _content(resumed)
            assert "status=running" in _content(resumed)

            terminated = await _dispatch(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                chat_id=owner_chat,
                name="write_stdin",
                args={"session_id": str(background_id), "terminate": True},
            )
            assert terminated.is_error is False, _content(terminated)
            assert "status=terminated" in _content(terminated)
            assert "reason=terminated" in _content(terminated)

            tty = await _dispatch(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                chat_id=owner_chat,
                name="exec",
                args={
                    "command": (
                        "printf 'READY> '; read -r value; printf 'tty-echo:%s\\n' \"$value\""
                    ),
                    "timeout": 10,
                    "tty": True,
                    "yield_time_ms": 100,
                },
                timeout=15,
            )
            assert tty.is_error is False, _content(tty)
            assert "status=running" in _content(tty)
            assert "tty=true" in _content(tty)
            tty_id = _session_id(tty)

            tty_result = await _dispatch(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                chat_id=owner_chat,
                name="write_stdin",
                args={
                    "session_id": str(tty_id),
                    "chars": "hello-e2e\n",
                    "wait_for": "tty-echo:hello-e2e",
                    "wait_timeout_ms": 5000,
                },
                timeout=10,
            )
            assert tty_result.is_error is False, _content(tty_result)
            assert "tty-echo:hello-e2e" in _content(tty_result)
            tty_final = await _dispatch(
                registry,
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                chat_id=owner_chat,
                name="write_stdin",
                args={"session_id": str(tty_id), "yield_time_ms": 3000},
            )
            assert tty_final.is_error is False, _content(tty_final)
            assert "status=exited" in _content(tty_final)

            agent_chat = uuid4()
            agent_response = await http_client.post(
                f"/api/sessions/{agent_chat}/messages",
                headers=headers,
                json={
                    "content": [{"type": "text", "text": "Run the agent exec sentinel."}],
                    "attachments": [],
                },
            )
            assert agent_response.status_code == 200, agent_response.text
            agent_events = [json.loads(line) for line in agent_response.text.splitlines()]
            assert [
                event["status"] for event in agent_events if event["type"] == "turn_finished"
            ] == ["completed", "completed"]
            assert (workspace / "agent-sentinel.txt").read_text(encoding="utf-8") == (
                "py6-agent-sentinel\n"
            )
            exec_schema = next(
                schema for schema in provider.calls[0]["tools"] if schema["name"] == "exec"
            )
            assert exec_schema["input_schema"]["properties"]["openoctopus_device"]["enum"] == [
                device_name
            ]
            assert (
                "py6-agent-output"
                in provider.calls[1]["messages"][-1]["content"][0]["content"][-1]["text"]
            )
            agent_dispatch = next(
                call for call in dispatch_calls if call["chat_session_id"] == agent_chat
            )
            assert agent_dispatch["name"] == "exec"
            tool_result_payload = json.dumps(provider.calls[1]["messages"][-1], sort_keys=True)
            assert str(agent_chat) not in tool_result_payload
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
                runtime=runtime,
            )
        finally:
            get_settings.cache_clear()
            get_engine.cache_clear()
            get_device_registry.cache_clear()
