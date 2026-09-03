from __future__ import annotations

from typing import Protocol, cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import DingTalkConfig, DiscordConfig
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import DeliveryRef, ToolContext, ToolResult
from openctopus_server.tools.message import ResolvedMessageTarget

from .manager import ChannelRuntimeSnapshot
from .router import ChannelDeliveryRouter, TargetFenceFailure
from .types import ExternalChannel, OutboundMessage, ResolvedDeliveryFile, ToolProfile


class ChannelRuntimeStatusLookup(Protocol):
    def __call__(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> ChannelRuntimeSnapshot | None: ...


class ChannelMessageTargetResolver:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def resolve_message_target(
        self,
        *,
        user_id: UUID,
        session_id: UUID,
        tool_profile: ToolProfile,
        current_channel: str | None,
        current_chat_id: str | None,
        current_binding_generation: UUID | None,
        requested_channel: ExternalChannel | None,
        requested_chat_id: str | None,
        has_media: bool,
    ) -> ResolvedMessageTarget | ToolResult:
        del session_id
        if tool_profile == "message_only" and has_media:
            return _error(
                ErrorCode.TOOL_NOT_ALLOWED,
                "message_only does not permit media delivery",
            )
        if requested_channel is None:
            if current_channel not in {"discord", "dingtalk"} or current_chat_id is None:
                return _error(
                    ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED,
                    "The current conversation is not an external channel target",
                )
            channel = cast(ExternalChannel, current_channel)
            chat_id = current_chat_id
        else:
            if requested_chat_id is None:
                return _error(ErrorCode.TOOL_INVALID_ARGS, "channel and chat_id are required")
            channel = requested_channel
            chat_id = requested_chat_id

        config = await _load_config(self._engine, user_id=user_id, channel=channel)
        if config is None or not _is_paired(config):
            return _error(
                ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED,
                "The selected channel is not configured and paired",
            )

        if current_channel in {"discord", "dingtalk"}:
            current_external_channel = cast(ExternalChannel, current_channel)
            current_config = (
                config
                if current_external_channel == channel
                else await _load_config(
                    self._engine,
                    user_id=user_id,
                    channel=current_external_channel,
                )
            )
            if (
                current_config is None
                or current_binding_generation != current_config.binding_generation
            ):
                return _error(
                    ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED,
                    "The current channel binding is no longer available",
                )
        if requested_channel is not None and tool_profile == "message_only" and (
            channel != requested_channel or chat_id != config.owner_dm_chat_id
        ):
            return _error(
                ErrorCode.TOOL_NOT_ALLOWED,
                "message_only may explicitly target only the paired owner direct message",
            )

        return ResolvedMessageTarget(
            channel=channel,
            chat_id=chat_id,
            binding_generation=config.binding_generation,
        )


class ChannelMessageDeliveryBridge:
    def __init__(self, router: ChannelDeliveryRouter) -> None:
        self._router = router

    async def deliver_message(
        self,
        *,
        target: ResolvedMessageTarget,
        content: str,
        delivery_refs: tuple[DeliveryRef, ...],
        ctx: ToolContext,
    ) -> ToolResult:
        if ctx.turn_id is None or ctx.tool_use_id is None:
            return _error(
                ErrorCode.TOOL_DB_ERROR,
                "External message delivery is missing its persisted tool identity",
            )
        message = OutboundMessage(
            delivery_key=f"message_tool:{ctx.turn_id}:{ctx.tool_use_id}",
            user_id=ctx.user_id,
            turn_id=ctx.turn_id,
            origin="message_tool",
            channel=target.channel,
            chat_id=target.chat_id,
            binding_generation=target.binding_generation,
            content=content,
            media=cast(tuple[ResolvedDeliveryFile, ...], delivery_refs),
            source_channel=(
                ctx.current_channel
                if ctx.current_channel in {"discord", "dingtalk"}
                else None
            ),
            source_binding_generation=(
                ctx.current_binding_generation
                if ctx.current_channel in {"discord", "dingtalk"}
                else None
            ),
        )
        try:
            result = await self._router.deliver(
                message,
                session_id=ctx.session_id,
                assistant_message_id=ctx.assistant_message_id,
                tool_use_id=ctx.tool_use_id,
                on_issued=ctx.on_issued,
            )
        except LookupError:
            return _error(
                ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED,
                "The selected channel runtime is unavailable",
            )
        except ValueError as exc:
            return _error(ErrorCode.TOOL_DELIVERY_FAILED, str(exc))

        summary = (
            f"Channel delivery {result.delivery_id} finished with status={result.status}; "
            f"visible_sent={result.visible_sent_actions}/{result.visible_total_actions}."
        )
        if result.status == "sent":
            return ToolResult(content=summary)
        if result.last_error_code == ErrorCode.TOOL_CHANNEL_RETRY_REQUIRES_NEW_TURN.value:
            code = ErrorCode.TOOL_CHANNEL_RETRY_REQUIRES_NEW_TURN
        elif result.status == "unknown":
            code = ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN
        else:
            code = ErrorCode.TOOL_DELIVERY_FAILED
        return ToolResult(
            content=f"[{code.value}] {summary}",
            is_error=True,
            code=code,
        )


class ChannelTargetIssueFence:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        runtime_status: ChannelRuntimeStatusLookup,
    ) -> None:
        self._engine = engine
        self._runtime_status = runtime_status

    async def __call__(self, message: OutboundMessage) -> TargetFenceFailure | None:
        config = await _load_config(
            self._engine,
            user_id=message.user_id,
            channel=message.channel,
        )
        runtime = self._runtime_status(message.user_id, message.channel)
        source_config = None
        if message.source_channel is not None:
            source_config = (
                config
                if message.source_channel == message.channel
                else await _load_config(
                    self._engine,
                    user_id=message.user_id,
                    channel=message.source_channel,
                )
            )
        source_is_current = (
            message.source_channel is None
            and message.source_binding_generation is None
        ) or (
            source_config is not None
            and message.source_binding_generation is not None
            and source_config.binding_generation
            == message.source_binding_generation
            and _is_paired(source_config)
        )
        if (
            not source_is_current
            or config is None
            or runtime is None
            or runtime.config_revision != config.revision
            or runtime.binding_generation != config.binding_generation
            or config.binding_generation != message.binding_generation
            or (
                message.origin == "pairing_confirmation"
                and config.owner_platform_user_id is not None
            )
            or (
                message.origin != "pairing_confirmation"
                and not _is_paired(config)
            )
        ):
            return TargetFenceFailure(
                error_code="channel_target_stale",
                error_message="The channel target changed before platform issue.",
            )
        return None


async def _load_config(
    engine: AsyncEngine,
    *,
    user_id: UUID,
    channel: ExternalChannel,
) -> DiscordConfig | DingTalkConfig | None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        if channel == "discord":
            return await db.get(DiscordConfig, user_id)
        return await db.get(DingTalkConfig, user_id)


def _is_paired(config: DiscordConfig | DingTalkConfig) -> bool:
    return (
        config.owner_platform_user_id is not None
        and config.owner_dm_chat_id is not None
        and config.paired_at is not None
    )


def _error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=f"[{code.value}] {message}",
        is_error=True,
        code=code,
    )
