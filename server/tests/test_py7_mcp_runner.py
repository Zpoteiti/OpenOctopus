from __future__ import annotations

from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import Device, SystemConfig, User
from openctopus_server.devices.mcp_catalog import with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
)
from openctopus_server.devices.protocol import ToolResultFrame, new_uuid7
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.provider.anthropic import (
    DeltaCallback,
    ProviderResult,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.tools.registry import ToolRegistry, _owned_mcp_route_resolver

_ENTRY_ID = UUID("01890f7c-bb80-7000-8000-000000000003")
_MCP_SERVERS = [
    {
        "name": "demo",
        "transport": "stdio",
        "command": "demo-mcp",
        "args": [],
        "cwd": None,
        "env": {},
        "enabled_capabilities": None,
    }
]


def _catalog() -> PersistedMcpCatalog:
    return with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[
                PersistedMcpServerCatalog(
                    name="demo",
                    entries=[
                        PersistedMcpCatalogEntry(
                            entry_id=_ENTRY_ID,
                            server="demo",
                            surface="tool",
                            raw_name="search",
                            invocation_identity="search",
                            final_name="mcp_demo_search",
                            provider_description="Search with demo.",
                            input_schema={
                                "type": "object",
                                "properties": {"query": {"type": "string"}},
                                "required": ["query"],
                                "additionalProperties": False,
                            },
                            enabled=True,
                        )
                    ],
                )
            ],
        )
    )


class _McpProvider:
    def __init__(
        self,
        *,
        before_first_result: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.calls: list[dict[str, Any]] = []
        self.before_first_result = before_first_result

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
        del system, effort, limiter, on_delta
        self.calls.append(
            {
                "messages": deepcopy(messages),
                "tools": deepcopy(tools),
            }
        )
        if len(self.calls) == 1:
            if self.before_first_result is not None:
                await self.before_first_result()
            content = [
                {
                    "type": "tool_use",
                    "id": "mcp-call-1",
                    "name": "mcp_demo_search",
                    "input": {
                        "query": "octopus",
                        "openoctopus_device": "laptop",
                    },
                }
            ]
        else:
            content = [{"type": "text", "text": "done"}]
        return ProviderResult(
            content=content,
            fingerprint=provider_fingerprint(config),
        )

    async def close(self) -> None:
        return None


async def test_runner_freezes_durable_mcp_schema_and_route_for_dispatch(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
) -> None:
    catalog = _catalog()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user_id = (
            await db.execute(select(User.id).where(User.email == "user@test.com"))
        ).scalar_one()
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
                Device(
                    id=uuid4(),
                    user_id=user_id,
                    name="laptop",
                    token_hash=b"m" * 32,
                    token_hint="mcp-test",
                    mcp_servers=deepcopy(_MCP_SERVERS),
                    mcp_catalog=catalog.model_dump(mode="json"),
                    config_revision=7,
                ),
            ]
        )
        await db.commit()

    dispatched: list[dict[str, object]] = []
    device_registry = DeviceRegistry()

    async def dispatch_mcp_tool(**kwargs: object) -> ToolResultFrame:
        callback = kwargs["on_issued"]
        assert callable(callback)
        callback()
        dispatched.append(kwargs)
        return ToolResultFrame(id=new_uuid7(), content="found", is_error=False)

    monkeypatch.setattr(
        device_registry,
        "dispatch_mcp_tool",
        dispatch_mcp_tool,
        raising=False,
    )
    provider = _McpProvider()
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry(
            (),
            mcp_route_resolver=_owned_mcp_route_resolver(pg_engine),
        ),
        device_registry=device_registry,
        request_token_estimator=lambda **kwargs: 1,
    )
    test_app.state.chat_runtime = runtime
    session_id = uuid4()
    try:
        response = await user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "search"}], "attachments": []},
        )
    finally:
        await runtime.close()

    assert response.status_code == 200
    assert len(provider.calls) == 2
    assert [tool["name"] for tool in provider.calls[0]["tools"]] == ["mcp_demo_search"]
    assert provider.calls[0]["tools"][0]["input_schema"]["properties"][
        "openoctopus_device"
    ]["enum"] == ["laptop"]
    assert len(dispatched) == 1
    assert dispatched[0]["name"] == "mcp_demo_search"
    assert dispatched[0]["args"] == {"query": "octopus"}
    route = dispatched[0]["route"]
    assert route.entry_id == _ENTRY_ID
    assert route.config_revision == 7
    assert route.catalog_digest == catalog.digest


async def test_runner_rejects_a_frozen_mcp_route_that_changed_before_send(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
) -> None:
    catalog = _catalog()
    device_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user_id = (
            await db.execute(select(User.id).where(User.email == "user@test.com"))
        ).scalar_one()
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
                Device(
                    id=device_id,
                    user_id=user_id,
                    name="laptop",
                    token_hash=b"r" * 32,
                    token_hint="mcp-race",
                    mcp_servers=deepcopy(_MCP_SERVERS),
                    mcp_catalog=catalog.model_dump(mode="json"),
                    config_revision=7,
                ),
            ]
        )
        await db.commit()

    async def advance_revision() -> None:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            device = await db.get(Device, device_id)
            assert device is not None
            device.config_revision = 8
            await db.commit()

    dispatched = False
    device_registry = DeviceRegistry()

    async def dispatch_mcp_tool(**kwargs: object) -> ToolResultFrame:
        del kwargs
        nonlocal dispatched
        dispatched = True
        return ToolResultFrame(id=new_uuid7(), content="unexpected", is_error=False)

    monkeypatch.setattr(
        device_registry,
        "dispatch_mcp_tool",
        dispatch_mcp_tool,
        raising=False,
    )
    provider = _McpProvider(before_first_result=advance_revision)
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry(
            (),
            mcp_route_resolver=_owned_mcp_route_resolver(pg_engine),
        ),
        device_registry=device_registry,
        request_token_estimator=lambda **kwargs: 1,
    )
    test_app.state.chat_runtime = runtime
    session_id = uuid4()
    try:
        response = await user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "search"}], "attachments": []},
        )
    finally:
        await runtime.close()

    assert response.status_code == 200
    assert len(provider.calls) == 2
    assert dispatched is False
    tool_result = provider.calls[1]["messages"][-1]["content"][0]
    assert tool_result["is_error"] is True
    assert "[tool_mcp_unavailable]" in tool_result["content"][1]["text"]
