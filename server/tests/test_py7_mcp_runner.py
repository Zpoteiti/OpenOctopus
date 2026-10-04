from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from native_provider_fixture import NativeProviderFixture
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import Device, Message, Session, SystemConfig, User
from openctopus_server.devices.mcp_catalog import with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
)
from openctopus_server.devices.protocol import ToolResultFrame, new_uuid7
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.runtime import (
    DeltaCallback,
    ProviderResult,
    provider_fingerprint,
)
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
        "enabled_capabilities": [],
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


class _McpProvider(NativeProviderFixture):
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
        del effort, limiter, on_delta
        self.calls.append(
            {
                "system": system,
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


class _CompactionMcpProvider(_McpProvider):
    def __init__(self):
        super().__init__()
        self.summary_started = asyncio.Event()
        self.release_summary = asyncio.Event()

    def native_model(self, config):
        from pydantic_ai.messages import ModelResponse, TextPart
        from pydantic_ai.models.function import FunctionModel
        base = super().native_model(config)
        async def summarize(messages, info):
            self.summary_started.set()
            await self.release_summary.wait()
            return ModelResponse([TextPart('compacted')])
        return FunctionModel(function=summarize, stream_function=base.stream_function)


def _message(
    session_id: UUID,
    *,
    kind: str,
    text: str,
    created_at: datetime,
) -> Message:
    authority = (
        {
            "sender_id": str(session_id),
            "sender_classification": "owner",
            "ingress_tool_profile": "owner_full",
        }
        if kind == "human"
        else {}
    )
    return Message(
        id=uuid4(),
        session_id=session_id,
        message_kind=kind,
        content=[{"type": "text", "text": text}],
        delivery_refs=[],
        llm_fingerprint=None,

        created_at=created_at,
        **authority,
    )


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
    assert [tool["name"] for tool in provider.calls[0]["tools"] if tool["name"].startswith("mcp_")] == ["mcp_demo_search"]
    assert next(t for t in provider.calls[0]["tools"] if t["name"] == "mcp_demo_search")["input_schema"]["properties"][
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
    assert "[tool_mcp_unavailable]" in tool_result["content"]


async def test_runner_uses_one_owner_device_snapshot_for_prompt_schema_and_route(
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
                    token_hash=b"s" * 32,
                    token_hint="snapshot-race",
                    mcp_servers=deepcopy(_MCP_SERVERS),
                    mcp_catalog=catalog.model_dump(mode="json"),
                    config_revision=7,
                ),
            ]
        )
        await db.commit()

    context_built = asyncio.Event()
    release_context = asyncio.Event()
    from openctopus_server.chat import prompt as prompt_module
    original_build_context = prompt_module.build_system_prompt
    context_calls = 0

    async def paused_build_context(*args: Any, **kwargs: Any) -> tuple[str, list[dict[str, Any]]]:
        nonlocal context_calls
        result = await original_build_context(*args, **kwargs)
        context_calls += 1
        if context_calls == 1:
            context_built.set()
            await release_context.wait()
        return result

    monkeypatch.setattr(prompt_module, "build_system_prompt", paused_build_context)
    dispatched: list[dict[str, object]] = []
    device_registry = DeviceRegistry()

    async def dispatch_mcp_tool(**kwargs: object) -> ToolResultFrame:
        callback = kwargs["on_issued"]
        assert callable(callback)
        callback()
        dispatched.append(kwargs)
        return ToolResultFrame(id=new_uuid7(), content="found", is_error=False)

    async def accept_snapshot_route(_user_id: UUID, _route: object) -> bool:
        return True

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
        tool_registry=ToolRegistry((), mcp_route_resolver=accept_snapshot_route),
        device_registry=device_registry,
        request_token_estimator=lambda **kwargs: 1,
    )
    execution_states: list[dict[str, Any]] = []
    original_execute = runtime.tool_registry.execute

    async def capture_execute(**kwargs: Any) -> Any:
        execution_states.append(kwargs)
        return await original_execute(**kwargs)

    monkeypatch.setattr(runtime.tool_registry, "execute", capture_execute)
    test_app.state.chat_runtime = runtime
    session_id = uuid4()
    request = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "search"}], "attachments": []},
        )
    )
    try:
        await asyncio.wait_for(context_built.wait(), timeout=2)
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            device = await db.get(Device, device_id)
            assert device is not None
            device.name = "desktop"
            device.config_revision = 8
            await db.commit()
        release_context.set()
        response = await request
    finally:
        release_context.set()
        if not request.done():
            request.cancel()
        await runtime.close()

    assert response.status_code == 200
    first_call = provider.calls[0]
    assert "- laptop —" in first_call["system"]
    assert "- desktop —" not in first_call["system"]
    assert next(t for t in first_call["tools"] if t["name"] == "mcp_demo_search")["input_schema"]["properties"][
        "openoctopus_device"
    ]["enum"] == ["laptop"]
    assert len(dispatched) == 1
    assert len(execution_states) == 1
    assert execution_states[0]["device_targets"] == {"laptop": device_id}
    frozen_snapshot = execution_states[0]["mcp_snapshot"]
    assert frozen_snapshot.routes[0].device_name == "laptop"
    assert frozen_snapshot.routes[0].config_revision == 7
    route = dispatched[0]["route"]
    assert route.device_name == "laptop"
    assert route.config_revision == 7


async def test_native_compaction_preserves_captured_owner_snapshot(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
) -> None:
    catalog = _catalog()
    device_id = uuid4()
    session_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user_id = (
            await db.execute(select(User.id).where(User.email == "user@test.com"))
        ).scalar_one()
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
                SystemConfig(key="llm_max_output_tokens", value=1000),
                SystemConfig(key="llm_max_context_tokens", value=10_000),
                SystemConfig(key="llm_compaction_threshold_tokens", value=5000),
                Session(
                    id=session_id,
                    user_id=user_id,
                    session_key=f"web:{session_id}",
                    channel="web",
                    chat_id=str(session_id),
                    title="New chat",
                    created_at=now,
                ),
                Device(
                    id=device_id,
                    user_id=user_id,
                    name="laptop",
                    token_hash=b"c" * 32,
                    token_hint="compaction-race",
                    mcp_servers=deepcopy(_MCP_SERVERS),
                    mcp_catalog=catalog.model_dump(mode="json"),
                    config_revision=7,
                ),
            ]
        )
        await db.flush()
        db.add_all([
            _message(session_id, kind="human" if i % 2 == 0 else "assistant",
                     text="history " * 3000, created_at=now + timedelta(microseconds=i))
            for i in range(12)
        ])
        await db.commit()

    dispatched: list[dict[str, object]] = []
    device_registry = DeviceRegistry()

    async def dispatch_mcp_tool(**kwargs: object) -> ToolResultFrame:
        callback = kwargs["on_issued"]
        assert callable(callback)
        callback()
        dispatched.append(kwargs)
        return ToolResultFrame(id=new_uuid7(), content="found", is_error=False)

    async def accept_snapshot_route(_user_id: UUID, _route: object) -> bool:
        return True

    monkeypatch.setattr(
        device_registry,
        "dispatch_mcp_tool",
        dispatch_mcp_tool,
        raising=False,
    )
    provider = _CompactionMcpProvider()
    estimates = deque((6000, 1000, 1000))
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((), mcp_route_resolver=accept_snapshot_route),
        device_registry=device_registry,
        request_token_estimator=lambda **kwargs: estimates.popleft(),
    )
    execution_states: list[dict[str, Any]] = []
    original_execute = runtime.tool_registry.execute

    async def capture_execute(**kwargs: Any) -> Any:
        execution_states.append(kwargs)
        return await original_execute(**kwargs)

    monkeypatch.setattr(runtime.tool_registry, "execute", capture_execute)
    test_app.state.chat_runtime = runtime
    request = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "U2"}], "attachments": []},
        )
    )
    try:
        await asyncio.wait_for(provider.summary_started.wait(), timeout=2)
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            device = await db.get(Device, device_id)
            assert device is not None
            device.name = "desktop"
            device.config_revision = 8
            await db.commit()
        provider.release_summary.set()
        response = await request
    finally:
        provider.release_summary.set()
        if not request.done():
            request.cancel()
        await runtime.close()

    assert response.status_code == 200
    first_call = provider.calls[0]
    assert "- laptop —" in first_call["system"]
    assert "- desktop —" not in first_call["system"]
    assert next(t for t in first_call["tools"] if t["name"] == "mcp_demo_search")["input_schema"]["properties"][
        "openoctopus_device"
    ]["enum"] == ["laptop"]
    assert len(dispatched) == 1
    assert len(execution_states) == 1
    assert execution_states[0]["device_targets"] == {"laptop": device_id}
    frozen_snapshot = execution_states[0]["mcp_snapshot"]
    assert frozen_snapshot.routes[0].device_name == "laptop"
    assert frozen_snapshot.routes[0].config_revision == 7
    route = dispatched[0]["route"]
    assert route.device_name == "laptop"
    assert route.config_revision == 7
