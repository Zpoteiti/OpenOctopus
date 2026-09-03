from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

import openctopus_server.tools.registry as registry_module
from openctopus_server.channels.types import ChannelName, ExternalChannel, ToolProfile
from openctopus_server.devices.mcp_models import ProviderMcpTool
from openctopus_server.devices.mcp_routes import OwnerMcpSnapshot
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import Tool, ToolContext, ToolResult
from openctopus_server.tools.message import (
    MessageTool,
    ResolvedMessageTarget,
)
from openctopus_server.tools.registry import ToolRegistry
from openctopus_server.workspace.service import WorkspaceService


class _ExecSpy(Tool):
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, Any], ToolContext]] = []

    def name(self) -> str:
        return "exec"

    def schema(self) -> dict[str, Any]:
        return {
            "name": "exec",
            "description": "Execute a command.",
            "input_schema": {
                "type": "object",
                "properties": {"cmd": {"type": "string"}},
                "required": ["cmd"],
                "additionalProperties": False,
            },
        }

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        self.calls.append((args, ctx))
        return ToolResult(content="executed")


class _TargetResolver:
    def __init__(self, result: ResolvedMessageTarget | ToolResult) -> None:
        self.result = result
        self.calls: list[dict[str, object]] = []

    async def resolve_message_target(
        self,
        *,
        user_id: UUID,
        session_id: UUID,
        tool_profile: ToolProfile,
        current_channel: ChannelName | None,
        current_chat_id: str | None,
        current_binding_generation: UUID | None,
        requested_channel: ExternalChannel | None,
        requested_chat_id: str | None,
        has_media: bool,
    ) -> ResolvedMessageTarget | ToolResult:
        self.calls.append(
            {
                "user_id": user_id,
                "session_id": session_id,
                "tool_profile": tool_profile,
                "current_channel": current_channel,
                "current_chat_id": current_chat_id,
                "current_binding_generation": current_binding_generation,
                "requested_channel": requested_channel,
                "requested_chat_id": requested_chat_id,
                "has_media": has_media,
            }
        )
        return self.result


class _DeliveryRouter:
    def __init__(self, result: ToolResult | None = None) -> None:
        self.result = result or ToolResult(content="Message delivered.")
        self.calls: list[dict[str, object]] = []

    async def deliver_message(
        self,
        *,
        target: ResolvedMessageTarget,
        content: str,
        delivery_refs: tuple[object, ...],
        ctx: ToolContext,
    ) -> ToolResult:
        self.calls.append(
            {
                "target": target,
                "content": content,
                "delivery_refs": delivery_refs,
                "ctx": ctx,
            }
        )
        if ctx.on_issued is not None:
            ctx.on_issued()
        return self.result


def _ctx(
    *,
    tool_profile: ToolProfile = "owner_full",
    current_channel: ChannelName | None = None,
    current_chat_id: str | None = None,
    current_binding_generation: UUID | None = None,
) -> ToolContext:
    return ToolContext(
        user_id=uuid4(),
        session_id=uuid4(),
        tool_profile=tool_profile,
        current_channel=current_channel,
        current_chat_id=current_chat_id,
        current_binding_generation=current_binding_generation,
    )


def _mcp_snapshot() -> OwnerMcpSnapshot:
    return OwnerMcpSnapshot(
        schemas=(
            ProviderMcpTool(
                name="mcp_private_read",
                description="Read private data.",
                input_schema={
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            ),
        ),
        routes=(),
        shape_key="private-shape",
    )


def test_registry_projects_fixed_schemas_per_tool_profile(
    pg_engine: AsyncEngine,
) -> None:
    registry = ToolRegistry(
        (
            MessageTool(pg_engine, AsyncMock(spec=WorkspaceService)),
            _ExecSpy(),
        )
    )

    owner = registry.get_tool_schemas(
        tool_profile="owner_full",
        device_names=("laptop",),
        mcp_snapshot=_mcp_snapshot(),
    )
    restricted = registry.get_tool_schemas(
        tool_profile="message_only",
        device_names=("laptop",),
        mcp_snapshot=_mcp_snapshot(),
    )
    owner_again = registry.get_tool_schemas(
        tool_profile="owner_full",
        device_names=("laptop",),
        mcp_snapshot=_mcp_snapshot(),
    )

    assert [schema["name"] for schema in owner] == [
        "message",
        "exec",
        "mcp_private_read",
    ]
    assert [schema["name"] for schema in restricted] == ["message"]
    assert restricted[0]["input_schema"] == {
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "Message text to deliver.",
                "minLength": 1,
                "maxLength": 16_000,
            },
            "channel": {
                "type": "string",
                "enum": ["discord", "dingtalk"],
            },
            "chat_id": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512,
            },
        },
        "required": ["content"],
        "additionalProperties": False,
    }
    assert [schema["name"] for schema in owner_again] == [
        "message",
        "exec",
        "mcp_private_read",
    ]


async def test_message_only_rejects_builtin_before_resolution_or_issue() -> None:
    tool = _ExecSpy()
    device_resolver = AsyncMock()
    on_issued = Mock()
    registry = ToolRegistry((tool,), device_resolver=device_resolver)

    result = await registry.execute(
        name="exec",
        args={"cmd": "cat private.txt", "openoctopus_device": "laptop"},
        ctx=_ctx(tool_profile="message_only"),
        device_targets={"laptop": uuid4()},
        device_registry=Mock(),
        on_issued=on_issued,
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_NOT_ALLOWED
    assert tool.calls == []
    device_resolver.assert_not_awaited()
    on_issued.assert_not_called()


async def test_message_only_rejects_mcp_before_snapshot_resolution_or_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    select_mcp_call = Mock(side_effect=AssertionError("must not resolve MCP"))
    monkeypatch.setattr(registry_module, "select_mcp_call", select_mcp_call)
    on_issued = Mock()

    result = await ToolRegistry(()).execute(
        name="mcp_private_read",
        args={},
        ctx=_ctx(tool_profile="message_only"),
        mcp_snapshot=_mcp_snapshot(),
        on_issued=on_issued,
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_NOT_ALLOWED
    select_mcp_call.assert_not_called()
    on_issued.assert_not_called()


async def test_message_only_rejects_hidden_media_before_target_or_workspace_access(
    pg_engine: AsyncEngine,
) -> None:
    service = AsyncMock(spec=WorkspaceService)
    resolver = _TargetResolver(
        ResolvedMessageTarget(
            channel="discord",
            chat_id="owner-dm",
            binding_generation=uuid4(),
        )
    )
    on_issued = Mock()
    registry = ToolRegistry((MessageTool(pg_engine, service, target_resolver=resolver),))

    result = await registry.execute(
        name="message",
        args={"content": "send this", "media": ["private.txt"]},
        ctx=_ctx(
            tool_profile="message_only",
            current_channel="discord",
            current_chat_id="group-1",
        ),
        on_issued=on_issued,
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_NOT_ALLOWED
    assert resolver.calls == []
    service.resolve_delivery_file.assert_not_awaited()
    on_issued.assert_not_called()


async def test_message_only_uses_target_resolver_without_platform_io(
    pg_engine: AsyncEngine,
) -> None:
    generation = uuid4()
    target = ResolvedMessageTarget(
        channel="discord",
        chat_id="owner-dm",
        binding_generation=generation,
    )
    resolver = _TargetResolver(target)
    router = _DeliveryRouter()
    on_issued = Mock()
    ctx = _ctx(
        tool_profile="message_only",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=generation,
    )
    registry = ToolRegistry(
        (
            MessageTool(
                pg_engine,
                AsyncMock(spec=WorkspaceService),
                target_resolver=resolver,
                delivery_router=router,
            ),
        )
    )

    result = await registry.execute(
        name="message",
        args={
            "content": "Please return to the source group.",
            "channel": "discord",
            "chat_id": "owner-dm",
        },
        ctx=ctx,
        on_issued=on_issued,
    )

    assert result.is_error is False
    assert result.side_effect is None
    assert resolver.calls == [
        {
            "user_id": ctx.user_id,
            "session_id": ctx.session_id,
            "tool_profile": "message_only",
            "current_channel": "discord",
            "current_chat_id": "group-1",
            "current_binding_generation": generation,
            "requested_channel": "discord",
            "requested_chat_id": "owner-dm",
            "has_media": False,
        }
    ]
    assert router.calls == [
        {
            "target": target,
            "content": "Please return to the source group.",
            "delivery_refs": (),
            "ctx": replace(ctx, on_issued=on_issued),
        }
    ]
    on_issued.assert_called_once_with()


async def test_owner_message_uses_same_target_resolver_and_router(
    pg_engine: AsyncEngine,
) -> None:
    generation = uuid4()
    target = ResolvedMessageTarget(
        channel="dingtalk",
        chat_id="group-2",
        binding_generation=generation,
    )
    resolver = _TargetResolver(target)
    router = _DeliveryRouter()
    ctx = _ctx(tool_profile="owner_full", current_channel="web")
    registry = ToolRegistry(
        (
            MessageTool(
                pg_engine,
                AsyncMock(spec=WorkspaceService),
                target_resolver=resolver,
                delivery_router=router,
            ),
        )
    )

    result = await registry.execute(
        name="message",
        args={
            "content": "Owner delivery",
            "channel": "dingtalk",
            "chat_id": "group-2",
        },
        ctx=ctx,
    )

    assert result.is_error is False
    assert resolver.calls[0]["tool_profile"] == "owner_full"
    assert resolver.calls[0]["requested_channel"] == "dingtalk"
    assert resolver.calls[0]["requested_chat_id"] == "group-2"
    assert router.calls[0]["target"] == target
    assert router.calls[0]["content"] == "Owner delivery"


async def test_message_target_pair_is_validated_before_resolver_or_issue(
    pg_engine: AsyncEngine,
) -> None:
    resolver = _TargetResolver(
        ResolvedMessageTarget(
            channel="discord",
            chat_id="owner-dm",
            binding_generation=uuid4(),
        )
    )
    on_issued = Mock()
    registry = ToolRegistry(
        (
            MessageTool(
                pg_engine,
                AsyncMock(spec=WorkspaceService),
                target_resolver=resolver,
            ),
        )
    )

    result = await registry.execute(
        name="message",
        args={"content": "invalid target", "channel": "discord"},
        ctx=_ctx(
            tool_profile="message_only",
            current_channel="discord",
            current_chat_id="group-1",
        ),
        on_issued=on_issued,
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_INVALID_ARGS
    assert resolver.calls == []
    on_issued.assert_not_called()


async def test_target_resolver_error_stops_before_router_or_issue(
    pg_engine: AsyncEngine,
) -> None:
    resolver = _TargetResolver(
        ToolResult(
            content="target denied",
            is_error=True,
            code=ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED,
        )
    )
    router = _DeliveryRouter()
    on_issued = Mock()
    registry = ToolRegistry(
        (
            MessageTool(
                pg_engine,
                AsyncMock(spec=WorkspaceService),
                target_resolver=resolver,
                delivery_router=router,
            ),
        )
    )

    result = await registry.execute(
        name="message",
        args={"content": "do not send"},
        ctx=_ctx(
            tool_profile="message_only",
            current_channel="discord",
            current_chat_id="group-1",
        ),
        on_issued=on_issued,
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED
    assert router.calls == []
    on_issued.assert_not_called()


async def test_external_message_without_delivery_router_does_not_claim_success(
    pg_engine: AsyncEngine,
) -> None:
    resolver = _TargetResolver(
        ResolvedMessageTarget(
            channel="discord",
            chat_id="owner-dm",
            binding_generation=uuid4(),
        )
    )
    on_issued = Mock()
    registry = ToolRegistry(
        (
            MessageTool(
                pg_engine,
                AsyncMock(spec=WorkspaceService),
                target_resolver=resolver,
            ),
        )
    )

    result = await registry.execute(
        name="message",
        args={"content": "do not drop this"},
        ctx=_ctx(
            tool_profile="message_only",
            current_channel="discord",
            current_chat_id="group-1",
        ),
        on_issued=on_issued,
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED
    on_issued.assert_not_called()
