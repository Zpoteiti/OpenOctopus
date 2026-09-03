from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.channels.delivery import ActionResult
from openctopus_server.channels.manager import ChannelRuntimeSnapshot
from openctopus_server.channels.router import ChannelDeliveryResult, ChannelDeliveryRouter
from openctopus_server.channels.tool_delivery import (
    ChannelMessageDeliveryBridge,
    ChannelMessageTargetResolver,
    ChannelTargetIssueFence,
)
from openctopus_server.channels.types import DeliveryAction, DeliveryPlan, OutboundMessage
from openctopus_server.db.models import (
    DingTalkConfig,
    DiscordConfig,
    Session,
    TurnRun,
    User,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.message import ResolvedMessageTarget


async def _configured(pg_engine):
    user_id = uuid4()
    session_id = uuid4()
    turn_id = uuid4()
    generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(
            User(
                id=user_id,
                email=f"{user_id}@test.com",
                password_hash="hash",
                name="Owner",
            )
        )
        await db.flush()
        db.add(
            Session(
                id=session_id,
                user_id=user_id,
                session_key="discord:application:group-1",
                channel="discord",
                chat_id="group-1",
                title="Group",
            )
        )
        db.add(
            DiscordConfig(
                user_id=user_id,
                bot_token="secret",
                application_id=str(uuid4()),
                bot_user_id="bot-1",
                bot_display_name="Bot",
                binding_generation=generation,
                owner_platform_user_id="owner-1",
                owner_dm_chat_id="owner-dm",
                paired_at=datetime.now(UTC),
                allow_list=["allowed-1"],
            )
        )
        await db.flush()
        db.add(
            TurnRun(
                id=turn_id,
                session_id=session_id,
                runner_instance_id=uuid4(),
                status="running",
                tool_profile="message_only",
                input_message_ids=[],
                failed_delivery_targets=[],
            )
        )
        await db.commit()
    return user_id, session_id, turn_id, generation


def _runtime_snapshot(
    user_id,
    generation,
    *,
    revision: int = 1,
) -> ChannelRuntimeSnapshot:
    return ChannelRuntimeSnapshot(
        user_id=user_id,
        channel="discord",
        binding_generation=generation,
        config_revision=revision,
        runtime_generation=uuid4(),
        state="ready",
        last_error=None,
    )


class _IssueProbeAdapter:
    def __init__(self, platform="discord") -> None:
        self.platform = platform
        self.issue_calls: list[DeliveryAction] = []

    def plan_delivery(self, _message: OutboundMessage) -> DeliveryPlan:
        return DeliveryPlan(
            actions=(
                DeliveryAction(kind="text_message", visible=True, content="answer"),
            )
        )

    async def execute_action(
        self,
        action: DeliveryAction,
        *,
        on_issued: Callable[[], Awaitable[None]],
    ) -> ActionResult:
        await on_issued()
        self.issue_calls.append(action)
        return ActionResult(status="sent")


async def test_restricted_current_target_uses_frozen_binding(pg_engine) -> None:
    user_id, session_id, _, generation = await _configured(pg_engine)
    resolver = ChannelMessageTargetResolver(pg_engine)

    result = await resolver.resolve_message_target(
        user_id=user_id,
        session_id=session_id,
        tool_profile="message_only",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=generation,
        requested_channel=None,
        requested_chat_id=None,
        has_media=False,
    )

    assert result == ResolvedMessageTarget(
        channel="discord",
        chat_id="group-1",
        binding_generation=generation,
    )


async def test_restricted_explicit_target_must_equal_owner_dm(pg_engine) -> None:
    user_id, session_id, _, generation = await _configured(pg_engine)
    resolver = ChannelMessageTargetResolver(pg_engine)

    owner = await resolver.resolve_message_target(
        user_id=user_id,
        session_id=session_id,
        tool_profile="message_only",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=generation,
        requested_channel="discord",
        requested_chat_id="owner-dm",
        has_media=False,
    )
    other = await resolver.resolve_message_target(
        user_id=user_id,
        session_id=session_id,
        tool_profile="message_only",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=generation,
        requested_channel="discord",
        requested_chat_id="other-dm",
        has_media=False,
    )

    assert owner == ResolvedMessageTarget(
        channel="discord",
        chat_id="owner-dm",
        binding_generation=generation,
    )
    assert getattr(other, "code", None) is ErrorCode.TOOL_NOT_ALLOWED


async def test_current_target_fails_closed_after_binding_replacement(pg_engine) -> None:
    user_id, session_id, _, old_generation = await _configured(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.binding_generation = uuid4()
        await db.commit()

    result = await ChannelMessageTargetResolver(pg_engine).resolve_message_target(
        user_id=user_id,
        session_id=session_id,
        tool_profile="owner_full",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=old_generation,
        requested_channel=None,
        requested_chat_id=None,
        has_media=False,
    )

    assert getattr(result, "code", None) is ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED


async def test_owner_explicit_target_fails_closed_after_binding_replacement(
    pg_engine,
) -> None:
    user_id, session_id, _, old_generation = await _configured(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.binding_generation = uuid4()
        config.owner_dm_chat_id = "new-owner-dm"
        await db.commit()

    result = await ChannelMessageTargetResolver(pg_engine).resolve_message_target(
        user_id=user_id,
        session_id=session_id,
        tool_profile="owner_full",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=old_generation,
        requested_channel="discord",
        requested_chat_id="new-owner-dm",
        has_media=False,
    )

    assert getattr(result, "code", None) is ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED


async def test_restricted_explicit_owner_target_fails_closed_after_binding_replacement(
    pg_engine,
) -> None:
    user_id, session_id, _, old_generation = await _configured(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.binding_generation = uuid4()
        config.owner_dm_chat_id = "new-owner-dm"
        await db.commit()

    result = await ChannelMessageTargetResolver(pg_engine).resolve_message_target(
        user_id=user_id,
        session_id=session_id,
        tool_profile="message_only",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=old_generation,
        requested_channel="discord",
        requested_chat_id="new-owner-dm",
        has_media=False,
    )

    assert getattr(result, "code", None) is ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED


@pytest.mark.parametrize("source_change", ["delete", "replace"])
async def test_cross_channel_issue_fence_rechecks_source_binding_after_resolution(
    pg_engine,
    source_change: str,
) -> None:
    user_id, session_id, turn_id, source_generation = await _configured(pg_engine)
    target_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(
            DingTalkConfig(
                user_id=user_id,
                client_id="dingtalk-client",
                client_secret="dingtalk-secret",
                bot_user_id="dingtalk-bot",
                bot_display_name="DingTalk Bot",
                binding_generation=target_generation,
                revision=1,
                owner_platform_user_id="dingtalk-owner",
                owner_dm_chat_id="dingtalk-owner-dm",
                paired_at=datetime.now(UTC),
                allow_list=[],
            )
        )
        await db.commit()

    target = await ChannelMessageTargetResolver(pg_engine).resolve_message_target(
        user_id=user_id,
        session_id=session_id,
        tool_profile="message_only",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=source_generation,
        requested_channel="dingtalk",
        requested_chat_id="dingtalk-owner-dm",
        has_media=False,
    )
    assert target == ResolvedMessageTarget(
        channel="dingtalk",
        chat_id="dingtalk-owner-dm",
        binding_generation=target_generation,
    )

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        source = await db.get(DiscordConfig, user_id)
        assert source is not None
        if source_change == "delete":
            await db.delete(source)
        else:
            source.binding_generation = uuid4()
        await db.commit()

    target_adapter = _IssueProbeAdapter("dingtalk")
    target_snapshot = ChannelRuntimeSnapshot(
        user_id=user_id,
        channel="dingtalk",
        binding_generation=target_generation,
        config_revision=1,
        runtime_generation=uuid4(),
        state="ready",
        last_error=None,
    )
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: target_adapter,
        issue_fence=ChannelTargetIssueFence(
            pg_engine,
            runtime_status=lambda _user_id, channel: (
                target_snapshot if channel == "dingtalk" else None
            ),
        ),
    )
    result = await ChannelMessageDeliveryBridge(router).deliver_message(
        target=target,
        content="answer",
        delivery_refs=(),
        ctx=ToolContext(
            user_id=user_id,
            session_id=session_id,
            turn_id=turn_id,
            tool_use_id=f"cross-channel-{source_change}",
            tool_profile="message_only",
            current_channel="discord",
            current_chat_id="group-1",
            current_binding_generation=source_generation,
        ),
    )

    assert result.is_error is True
    assert target_adapter.issue_calls == []


async def test_message_bridge_uses_turn_and_tool_identity(pg_engine) -> None:
    user_id, session_id, turn_id, generation = await _configured(pg_engine)
    delivery_id = uuid4()
    router = AsyncMock()
    router.deliver.return_value = ChannelDeliveryResult(
        delivery_id=delivery_id,
        status="sent",
        visible_sent_actions=2,
        visible_total_actions=2,
        last_error_code=None,
        last_error_message=None,
    )
    assistant_id = uuid4()
    issued = AsyncMock()
    ctx = ToolContext(
        user_id=user_id,
        session_id=session_id,
        turn_id=turn_id,
        tool_use_id="tool-1",
        assistant_message_id=assistant_id,
        tool_profile="message_only",
        current_channel="discord",
        current_chat_id="group-1",
        current_binding_generation=generation,
        on_issued=issued,
    )

    result = await ChannelMessageDeliveryBridge(router).deliver_message(
        target=ResolvedMessageTarget(
            channel="discord",
            chat_id="group-1",
            binding_generation=generation,
        ),
        content="hello",
        delivery_refs=(),
        ctx=ctx,
    )

    assert result.is_error is False
    outbound = router.deliver.await_args.args[0]
    assert outbound.delivery_key == f"message_tool:{turn_id}:tool-1"
    assert outbound.source_channel == "discord"
    assert outbound.source_binding_generation == generation
    assert router.deliver.await_args.kwargs == {
        "session_id": session_id,
        "assistant_message_id": assistant_id,
        "tool_use_id": "tool-1",
        "on_issued": issued,
    }


async def test_issue_fence_rejects_secret_rotation_before_manager_apply(
    pg_engine,
) -> None:
    user_id, session_id, turn_id, generation = await _configured(pg_engine)
    manager_snapshot = _runtime_snapshot(user_id, generation)
    old_adapter = _IssueProbeAdapter()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.bot_token = "rotated-secret"
        config.revision += 1
        assert config.binding_generation == generation
        await db.commit()
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: old_adapter,
        issue_fence=ChannelTargetIssueFence(
            pg_engine,
            runtime_status=lambda _user_id, _channel: manager_snapshot,
        ),
    )
    message = OutboundMessage(
        delivery_key="secret-rotation-before-manager-apply",
        user_id=user_id,
        turn_id=turn_id,
        origin="final",
        channel="discord",
        chat_id="owner-dm",
        binding_generation=generation,
        content="answer",
    )

    result = await router.deliver(message, session_id=session_id)

    assert result.status == "failed"
    assert result.last_error_code == "channel_target_stale"
    assert old_adapter.issue_calls == []


async def test_issue_fence_rejects_deleted_binding(pg_engine) -> None:
    user_id, _, turn_id, generation = await _configured(pg_engine)
    manager_snapshot = _runtime_snapshot(user_id, generation)
    message = OutboundMessage(
        delivery_key="final:1",
        user_id=user_id,
        turn_id=turn_id,
        origin="final",
        channel="discord",
        chat_id="group-1",
        binding_generation=generation,
        content="answer",
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        await db.delete(config)
        await db.commit()

    failure = await ChannelTargetIssueFence(
        pg_engine,
        runtime_status=lambda _user_id, _channel: manager_snapshot,
    )(message)

    assert failure is not None
    assert failure.error_code == "channel_target_stale"


async def test_issue_fence_allows_only_pairing_confirmation_before_owner_bind(
    pg_engine,
) -> None:
    user_id, _, turn_id, generation = await _configured(pg_engine)
    manager_snapshot = _runtime_snapshot(user_id, generation)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.owner_platform_user_id = None
        config.owner_dm_chat_id = None
        config.paired_at = None
        await db.commit()
    base = dict(
        delivery_key="delivery",
        user_id=user_id,
        turn_id=turn_id,
        channel="discord",
        chat_id="owner-dm",
        binding_generation=generation,
        content="confirmation",
    )
    fence = ChannelTargetIssueFence(
        pg_engine,
        runtime_status=lambda _user_id, _channel: manager_snapshot,
    )

    pairing = await fence(OutboundMessage(origin="pairing_confirmation", **base))
    normal = await fence(OutboundMessage(origin="final", **base))

    assert pairing is None
    assert normal is not None and normal.error_code == "channel_target_stale"
