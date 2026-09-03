import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import fields
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.channels.adapters.base import ChannelAdapter
from openctopus_server.channels.delivery import ActionResult
from openctopus_server.channels.router import (
    ChannelDeliveryRouter,
    TargetFenceFailure,
)
from openctopus_server.channels.types import (
    DeliveryAction,
    DeliveryPlan,
    OutboundMessage,
)
from openctopus_server.db.models import (
    ChannelDelivery,
    ChannelDeliveryAction,
    Session,
    TurnRun,
    User,
)


class _Adapter:
    platform = "discord"

    def __init__(
        self,
        actions: tuple[DeliveryAction, ...],
        results: list[ActionResult],
        *,
        before_issue: Callable[[DeliveryAction], object] | None = None,
        after_issue: Callable[[DeliveryAction], object] | None = None,
    ) -> None:
        self.actions = actions
        self.results = results
        self.before_issue = before_issue
        self.after_issue = after_issue
        self.plan_calls = 0
        self.execute_calls: list[DeliveryAction] = []
        self.issue_calls: list[DeliveryAction] = []

    def plan_delivery(self, message: OutboundMessage) -> DeliveryPlan:
        self.plan_calls += 1
        return DeliveryPlan(actions=self.actions)

    async def execute_action(
        self,
        action: DeliveryAction,
        *,
        on_issued: Callable[[], Awaitable[None]],
    ) -> ActionResult:
        self.execute_calls.append(action)
        if self.before_issue is not None:
            outcome = self.before_issue(action)
            if hasattr(outcome, "__await__"):
                await outcome
        await on_issued()
        self.issue_calls.append(action)
        if self.after_issue is not None:
            outcome = self.after_issue(action)
            if hasattr(outcome, "__await__"):
                await outcome
        return self.results.pop(0)


async def _allow(_message: OutboundMessage) -> TargetFenceFailure | None:
    return None


async def _user_session_turn(
    engine: AsyncEngine,
    *,
    chat_id: str = "chat-1",
) -> tuple[UUID, UUID, UUID]:
    user_id = uuid4()
    session_id = uuid4()
    turn_id = uuid4()
    async with AsyncSession(engine, expire_on_commit=False) as db:
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
                session_key=f"discord:bot:{chat_id}",
                channel="discord",
                chat_id=chat_id,
                title="Channel",
            )
        )
        await db.flush()
        db.add(
            TurnRun(
                id=turn_id,
                session_id=session_id,
                runner_instance_id=uuid4(),
                status="running",
                tool_profile="owner_full",
                input_message_ids=[],
                failed_delivery_targets=[],
                started_at=datetime.now(UTC),
            )
        )
        await db.commit()
    return user_id, session_id, turn_id


def _message(
    *,
    user_id: UUID,
    turn_id: UUID,
    delivery_key: str,
    chat_id: str = "chat-1",
    binding_generation: UUID | None = None,
) -> OutboundMessage:
    return OutboundMessage(
        delivery_key=delivery_key,
        user_id=user_id,
        turn_id=turn_id,
        origin="message_tool",
        channel="discord",
        chat_id=chat_id,
        binding_generation=binding_generation or uuid4(),
        content="hello",
    )


def _text(content: str) -> DeliveryAction:
    return DeliveryAction(kind="text_message", visible=True, content=content)


async def _stored_delivery(
    engine: AsyncEngine,
    delivery_key: str,
) -> tuple[ChannelDelivery, list[ChannelDeliveryAction]]:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        delivery = await db.scalar(
            select(ChannelDelivery).where(ChannelDelivery.delivery_key == delivery_key)
        )
        assert delivery is not None
        actions = list(
            (
                await db.scalars(
                    select(ChannelDeliveryAction)
                    .where(ChannelDeliveryAction.delivery_id == delivery.id)
                    .order_by(ChannelDeliveryAction.action_index)
                )
            ).all()
        )
        return delivery, actions


async def test_router_keeps_prepared_until_issue_hook_then_commits_attempting(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    preflight_entered = asyncio.Event()
    release_preflight = asyncio.Event()
    platform_issued = asyncio.Event()
    release_platform = asyncio.Event()

    async def wait_in_preflight(_action: DeliveryAction) -> None:
        preflight_entered.set()
        await release_preflight.wait()

    async def wait_after_issue(_action: DeliveryAction) -> None:
        platform_issued.set()
        await release_platform.wait()

    adapter = _Adapter(
        (_text("one"), _text("two")),
        [ActionResult(status="sent"), ActionResult(status="sent")],
        before_issue=wait_in_preflight,
        after_issue=wait_after_issue,
    )
    fence_observations: list[tuple[str, ...]] = []

    async def inspect_fence(_message: OutboundMessage) -> TargetFenceFailure | None:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            rows = list(
                (
                    await db.scalars(
                        select(ChannelDeliveryAction)
                        .order_by(ChannelDeliveryAction.action_index)
                        .with_for_update(nowait=True)
                    )
                ).all()
            )
            fence_observations.append(tuple(row.status for row in rows))
            await db.rollback()
        return None

    issued: list[str] = []
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=inspect_fence,
    )
    message = _message(user_id=user_id, turn_id=turn_id, delivery_key="delivery-1")

    task = asyncio.create_task(
        router.deliver(message, session_id=session_id, on_issued=lambda: issued.append("yes"))
    )
    await preflight_entered.wait()

    delivery, actions = await _stored_delivery(pg_engine, "delivery-1")
    assert delivery.status == "prepared"
    assert [action.status for action in actions] == ["prepared", "prepared"]
    assert fence_observations == []
    assert issued == []

    release_preflight.set()
    await platform_issued.wait()

    delivery, actions = await _stored_delivery(pg_engine, "delivery-1")
    assert delivery.status == "attempting"
    assert [action.status for action in actions] == ["attempting", "prepared"]
    assert fence_observations == [("prepared", "prepared")]
    assert issued == ["yes"]

    release_platform.set()
    result = await task

    assert result.status == "sent"
    assert result.visible_sent_actions == 2
    assert result.visible_total_actions == 2
    assert issued == ["yes"]
    assert fence_observations == [
        ("prepared", "prepared"),
        ("sent", "prepared"),
    ]


async def test_concurrent_and_later_duplicate_delivery_never_reissues(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def block(_action: DeliveryAction) -> None:
        entered.set()
        await release.wait()

    adapter = _Adapter(
        (_text("hello"),),
        [ActionResult(status="sent", platform_message_id="platform-1")],
        after_issue=block,
    )
    lookup_calls = 0

    def lookup(_user_id: UUID, _channel: str) -> ChannelAdapter:
        nonlocal lookup_calls
        lookup_calls += 1
        return adapter

    router = ChannelDeliveryRouter(pg_engine, adapter_lookup=lookup, issue_fence=_allow)
    message = _message(user_id=user_id, turn_id=turn_id, delivery_key="same-key")
    first_issued: list[bool] = []
    second_issued: list[bool] = []

    first = asyncio.create_task(
        router.deliver(
            message,
            session_id=session_id,
            on_issued=lambda: first_issued.append(True),
        )
    )
    await entered.wait()
    second = asyncio.create_task(
        router.deliver(
            message,
            session_id=session_id,
            on_issued=lambda: second_issued.append(True),
        )
    )
    await asyncio.sleep(0)
    release.set()
    first_result, second_result = await asyncio.gather(first, second)

    assert first_result == second_result
    assert len(adapter.execute_calls) == 1
    assert len(adapter.issue_calls) == 1
    assert adapter.plan_calls == 1
    assert lookup_calls == 2
    assert first_issued == [True]
    assert second_issued == []

    def unexpected_lookup(_user_id: UUID, _channel: str) -> ChannelAdapter:
        raise AssertionError("terminal duplicate must not look up or issue an adapter")

    restarted_router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=unexpected_lookup,
        issue_fence=_allow,
    )
    restarted_result = await restarted_router.deliver(message, session_id=session_id)
    assert restarted_result == first_result


async def test_missing_adapter_is_durably_failed_before_planning(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: None,
        issue_fence=_allow,
    )

    result = await router.deliver(
        _message(user_id=user_id, turn_id=turn_id, delivery_key="offline"),
        session_id=session_id,
    )
    delivery, actions = await _stored_delivery(pg_engine, "offline")

    assert result.status == "failed"
    assert result.visible_total_actions == 0
    assert result.last_error_code == "channel_not_ready"
    assert delivery.status == "failed"
    assert delivery.total_actions == 0
    assert actions == []


async def test_plan_failure_is_durably_failed_without_actions(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    adapter = _Adapter((_text("unused"),), [ActionResult(status="sent")])

    def fail_plan(_message: OutboundMessage) -> DeliveryPlan:
        raise RuntimeError("raw adapter planning detail")

    adapter.plan_delivery = fail_plan  # type: ignore[method-assign]
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
    )

    result = await router.deliver(
        _message(user_id=user_id, turn_id=turn_id, delivery_key="bad-plan"),
        session_id=session_id,
    )
    delivery, actions = await _stored_delivery(pg_engine, "bad-plan")

    assert result.status == "failed"
    assert result.last_error_code == "channel_delivery_plan_failed"
    assert result.last_error_message == "The channel delivery plan could not be created."
    assert "raw adapter" not in (delivery.last_error_message or "")
    assert actions == []


async def test_failure_stops_tail_aggregates_and_blocks_same_turn_target(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    generation = uuid4()
    adapter = _Adapter(
        (_text("one"), _text("two"), _text("three")),
        [
            ActionResult(status="sent", platform_message_id="platform-1"),
            ActionResult(
                status="failed",
                error_code="rate_limited",
                error_message="The platform rejected the action.",
            ),
        ],
    )
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
    )
    first = _message(
        user_id=user_id,
        turn_id=turn_id,
        delivery_key="first",
        binding_generation=generation,
    )

    first_result = await router.deliver(first, session_id=session_id)
    first_delivery, first_actions = await _stored_delivery(pg_engine, "first")

    assert first_result.status == "partial"
    assert first_result.last_error_code == "rate_limited"
    assert first_delivery.status == "partial"
    assert [action.status for action in first_actions] == ["sent", "failed", "skipped"]
    assert len(adapter.issue_calls) == 2

    second_issued: list[bool] = []
    second = _message(
        user_id=user_id,
        turn_id=turn_id,
        delivery_key="second",
        binding_generation=generation,
    )
    second_result = await router.deliver(
        second,
        session_id=session_id,
        on_issued=lambda: second_issued.append(True),
    )
    _, second_actions = await _stored_delivery(pg_engine, "second")

    assert second_result.status == "failed"
    assert second_result.last_error_code == "tool_channel_retry_requires_new_turn"
    assert second_actions == []
    assert adapter.plan_calls == 1
    assert len(adapter.issue_calls) == 2
    assert second_issued == []

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        turn = await db.get(TurnRun, turn_id)
    assert turn is not None
    assert turn.failed_delivery_targets == [
        {
            "channel": "discord",
            "chat_id": "chat-1",
            "binding_generation": str(generation),
        }
    ]
    assert await router.has_failed_target(
        turn_id=turn_id,
        channel="discord",
        chat_id="chat-1",
        binding_generation=generation,
    )

    adapter.actions = (_text("allowed"),)
    adapter.results = [ActionResult(status="sent"), ActionResult(status="sent")]
    other_target = await router.deliver(
        _message(
            user_id=user_id,
            turn_id=turn_id,
            delivery_key="other-target",
            chat_id="chat-2",
            binding_generation=generation,
        ),
        session_id=session_id,
    )
    new_turn_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        prior_turn = await db.get(TurnRun, turn_id)
        assert prior_turn is not None
        prior_turn.status = "completed"
        prior_turn.finished_at = datetime.now(UTC)
        await db.flush()
        db.add(
            TurnRun(
                id=new_turn_id,
                session_id=session_id,
                runner_instance_id=uuid4(),
                status="running",
                tool_profile="owner_full",
                input_message_ids=[],
                failed_delivery_targets=[],
                started_at=datetime.now(UTC),
            )
        )
        await db.commit()
    new_turn = await router.deliver(
        _message(
            user_id=user_id,
            turn_id=new_turn_id,
            delivery_key="new-turn",
            binding_generation=generation,
        ),
        session_id=session_id,
    )

    assert other_target.status == "sent"
    assert new_turn.status == "sent"
    assert len(adapter.issue_calls) == 4


async def test_live_fence_failure_after_visible_send_is_partial_without_issue(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    adapter = _Adapter(
        (_text("one"), _text("two"), _text("three")),
        [ActionResult(status="sent")],
    )
    fence_calls = 0

    async def fence(_message: OutboundMessage) -> TargetFenceFailure | None:
        nonlocal fence_calls
        fence_calls += 1
        if fence_calls == 2:
            return TargetFenceFailure(
                error_code="stale_binding",
                error_message="The channel binding changed.",
            )
        return None

    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=fence,
    )

    result = await router.deliver(
        _message(user_id=user_id, turn_id=turn_id, delivery_key="fenced"),
        session_id=session_id,
    )
    _, actions = await _stored_delivery(pg_engine, "fenced")

    assert result.status == "partial"
    assert result.last_error_code == "stale_binding"
    assert [action.status for action in actions] == ["sent", "failed", "skipped"]
    assert len(adapter.execute_calls) == 2
    assert len(adapter.issue_calls) == 1


async def test_each_action_rejects_a_replaced_captured_adapter_before_issue(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    replacement = _Adapter((_text("unused"),), [ActionResult(status="sent")])
    active_adapter: _Adapter

    def replace_adapter(_action: DeliveryAction) -> None:
        nonlocal active_adapter
        active_adapter = replacement

    captured = _Adapter(
        (_text("one"), _text("two")),
        [ActionResult(status="sent"), ActionResult(status="sent")],
        after_issue=replace_adapter,
    )
    active_adapter = captured
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: active_adapter,
        issue_fence=_allow,
    )

    result = await router.deliver(
        _message(user_id=user_id, turn_id=turn_id, delivery_key="adapter-replaced"),
        session_id=session_id,
    )
    _, actions = await _stored_delivery(pg_engine, "adapter-replaced")

    assert result.status == "partial"
    assert result.last_error_code == "channel_target_stale"
    assert [action.status for action in actions] == ["sent", "failed"]
    assert captured.execute_calls == [_text("one"), _text("two")]
    assert captured.issue_calls == [_text("one")]
    assert replacement.issue_calls == []


async def test_on_issued_fires_once_before_an_invisible_upload_action(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    order: list[str] = []

    async def executing(_action: DeliveryAction) -> None:
        order.append("execute")

    adapter = _Adapter(
        (DeliveryAction(kind="file_upload", visible=False, media_index=0),),
        [ActionResult(status="sent", platform_message_id="upload-1")],
        after_issue=executing,
    )
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
    )

    result = await router.deliver(
        _message(user_id=user_id, turn_id=turn_id, delivery_key="upload"),
        session_id=session_id,
        on_issued=lambda: order.append("issued"),
    )

    assert result.status == "sent"
    assert result.visible_sent_actions == 0
    assert result.visible_total_actions == 0
    assert order == ["issued", "execute"]


async def test_upload_artifact_is_injected_only_into_its_dependent_action(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    upload = DeliveryAction(kind="file_upload", visible=False, media_index=0)
    visible = DeliveryAction(
        kind="file_message",
        visible=True,
        media_index=0,
        dependency_action_index=0,
    )
    adapter = _Adapter(
        (upload, visible),
        [
            ActionResult(status="sent", artifact_id="media-id-1"),
            ActionResult(status="sent", platform_message_id="message-id-1"),
        ],
    )
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
    )

    result = await router.deliver(
        _message(user_id=user_id, turn_id=turn_id, delivery_key="artifact"),
        session_id=session_id,
    )
    _, stored_actions = await _stored_delivery(pg_engine, "artifact")

    assert result.status == "sent"
    assert adapter.execute_calls == [
        upload,
        DeliveryAction(
            kind="file_message",
            visible=True,
            media_index=0,
            dependency_action_index=0,
            dependency_artifact_id="media-id-1",
        ),
    ]
    assert stored_actions[0].platform_message_id == "media-id-1"
    assert stored_actions[1].platform_message_id == "message-id-1"


async def test_missing_upload_artifact_fails_dependent_action_before_issue(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    upload = DeliveryAction(kind="file_upload", visible=False, media_index=0)
    visible = DeliveryAction(
        kind="file_message",
        visible=True,
        media_index=0,
        dependency_action_index=0,
    )
    adapter = _Adapter((upload, visible), [ActionResult(status="sent")])
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
    )

    result = await router.deliver(
        _message(user_id=user_id, turn_id=turn_id, delivery_key="missing-artifact"),
        session_id=session_id,
    )
    _, stored_actions = await _stored_delivery(pg_engine, "missing-artifact")

    assert result.status == "failed"
    assert result.last_error_code == "channel_delivery_artifact_missing"
    assert adapter.execute_calls == [upload]
    assert [action.status for action in stored_actions] == ["sent", "failed"]


async def test_cancel_before_platform_issue_is_durably_failed(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    preflight_entered = asyncio.Event()
    never_release = asyncio.Event()

    async def blocking_preflight(_action: DeliveryAction) -> None:
        preflight_entered.set()
        await never_release.wait()

    adapter = _Adapter(
        (_text("one"), _text("two")),
        [ActionResult(status="sent")],
        before_issue=blocking_preflight,
    )
    issued: list[bool] = []
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
    )
    task = asyncio.create_task(
        router.deliver(
            _message(user_id=user_id, turn_id=turn_id, delivery_key="cancel-before"),
            session_id=session_id,
            on_issued=lambda: issued.append(True),
        )
    )
    await preflight_entered.wait()

    delivery, actions = await _stored_delivery(pg_engine, "cancel-before")
    assert delivery.status == "prepared"
    assert [action.status for action in actions] == ["prepared", "prepared"]

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    delivery, actions = await _stored_delivery(pg_engine, "cancel-before")
    assert delivery.status == "failed"
    assert delivery.last_error_code == "user_cancelled"
    assert [action.status for action in actions] == ["failed", "skipped"]
    assert adapter.execute_calls == [_text("one")]
    assert adapter.issue_calls == []
    assert issued == []


async def test_cancel_while_prepare_finishes_is_durably_failed(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    adapter = _Adapter((_text("one"),), [ActionResult(status="sent")])
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
    )
    original_install = router._install_plan
    prepared = asyncio.Event()
    release_prepare = asyncio.Event()

    async def delayed_install(*args, **kwargs) -> None:
        await original_install(*args, **kwargs)
        prepared.set()
        await release_prepare.wait()

    router._install_plan = delayed_install  # type: ignore[method-assign]
    task = asyncio.create_task(
        router.deliver(
            _message(user_id=user_id, turn_id=turn_id, delivery_key="cancel-prepare"),
            session_id=session_id,
        )
    )
    await prepared.wait()

    task.cancel()
    release_prepare.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    delivery, actions = await _stored_delivery(pg_engine, "cancel-prepare")
    assert delivery.status == "failed"
    assert [action.status for action in actions] == ["failed"]
    assert adapter.execute_calls == []


async def test_cancel_after_platform_issue_is_durably_unknown(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    adapter_entered = asyncio.Event()
    never_release = asyncio.Event()

    async def blocking_execute(_action: DeliveryAction) -> None:
        adapter_entered.set()
        await never_release.wait()

    adapter = _Adapter(
        (_text("one"), _text("two")),
        [ActionResult(status="sent")],
        after_issue=blocking_execute,
    )
    issued: list[bool] = []
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
    )
    task = asyncio.create_task(
        router.deliver(
            _message(user_id=user_id, turn_id=turn_id, delivery_key="cancel-after"),
            session_id=session_id,
            on_issued=lambda: issued.append(True),
        )
    )
    await adapter_entered.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    delivery, actions = await _stored_delivery(pg_engine, "cancel-after")
    assert delivery.status == "unknown"
    assert delivery.last_error_code == "user_cancelled"
    assert [action.status for action in actions] == ["unknown", "skipped"]
    assert len(adapter.execute_calls) == 1
    assert issued == [True]


@pytest.mark.parametrize(
    ("after_issue", "expected_status"),
    [(False, "failed"), (True, "unknown")],
)
async def test_action_deadline_uses_real_issue_boundary(
    pg_engine: AsyncEngine,
    after_issue: bool,
    expected_status: str,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    blocked = asyncio.Event()
    never_release = asyncio.Event()

    async def block(_action: DeliveryAction) -> None:
        blocked.set()
        await never_release.wait()

    adapter = _Adapter(
        (_text("one"),),
        [ActionResult(status="sent")],
        before_issue=None if after_issue else block,
        after_issue=block if after_issue else None,
    )
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
        action_timeout_seconds=0.05,
        logical_timeout_seconds=10,
    )

    result = await router.deliver(
        _message(
            user_id=user_id,
            turn_id=turn_id,
            delivery_key=f"action-timeout-{after_issue}",
        ),
        session_id=session_id,
    )
    _, actions = await _stored_delivery(
        pg_engine,
        f"action-timeout-{after_issue}",
    )

    assert blocked.is_set()
    assert result.status == expected_status
    assert result.last_error_code == "channel_action_timeout"
    assert [action.status for action in actions] == [expected_status]
    assert len(adapter.issue_calls) == int(after_issue)


async def test_logical_deadline_before_issue_returns_durable_failed(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    preflight_entered = asyncio.Event()
    never_release = asyncio.Event()

    async def blocking_preflight(_action: DeliveryAction) -> None:
        preflight_entered.set()
        await never_release.wait()

    adapter = _Adapter(
        (_text("one"), _text("two")),
        [ActionResult(status="sent")],
        before_issue=blocking_preflight,
    )
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
        action_timeout_seconds=10,
        logical_timeout_seconds=0.05,
    )

    result = await router.deliver(
        _message(
            user_id=user_id,
            turn_id=turn_id,
            delivery_key="logical-timeout-before",
        ),
        session_id=session_id,
    )
    delivery, actions = await _stored_delivery(pg_engine, "logical-timeout-before")

    assert preflight_entered.is_set()
    assert result.status == "failed"
    assert result.last_error_code == "channel_delivery_timeout"
    assert delivery.status == "failed"
    assert [action.status for action in actions] == ["failed", "skipped"]
    assert adapter.issue_calls == []


async def test_logical_deadline_after_issue_returns_durable_unknown(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    adapter_entered = asyncio.Event()
    never_release = asyncio.Event()

    async def blocking_execute(_action: DeliveryAction) -> None:
        adapter_entered.set()
        await never_release.wait()

    adapter = _Adapter(
        (_text("one"), _text("two")),
        [ActionResult(status="sent")],
        after_issue=blocking_execute,
    )
    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=lambda _user_id, _channel: adapter,
        issue_fence=_allow,
        action_timeout_seconds=10,
        logical_timeout_seconds=0.05,
    )

    result = await router.deliver(
        _message(user_id=user_id, turn_id=turn_id, delivery_key="logical-timeout"),
        session_id=session_id,
    )
    delivery, actions = await _stored_delivery(pg_engine, "logical-timeout")

    assert adapter_entered.is_set()
    assert result.status == "unknown"
    assert result.last_error_code == "channel_delivery_timeout"
    assert delivery.status == "unknown"
    assert [action.status for action in actions] == ["unknown", "skipped"]


async def test_startup_repair_never_calls_adapter_and_repairs_ordered_tail(
    pg_engine: AsyncEngine,
) -> None:
    user_id, session_id, turn_id = await _user_session_turn(pg_engine)
    now = datetime.now(UTC)
    prepared_id = uuid4()
    attempting_id = uuid4()
    unplanned_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                ChannelDelivery(
                    id=prepared_id,
                    user_id=user_id,
                    session_id=session_id,
                    turn_id=turn_id,
                    delivery_key="repair-prepared",
                    origin="message_tool",
                    channel="discord",
                    chat_id="chat-prepared",
                    binding_generation=uuid4(),
                    status="prepared",
                    total_actions=2,
                    visible_sent_actions=0,
                    created_at=now,
                ),
                ChannelDelivery(
                    id=attempting_id,
                    user_id=user_id,
                    session_id=session_id,
                    turn_id=turn_id,
                    delivery_key="repair-attempting",
                    origin="message_tool",
                    channel="discord",
                    chat_id="chat-attempting",
                    binding_generation=uuid4(),
                    status="attempting",
                    total_actions=3,
                    visible_sent_actions=0,
                    created_at=now,
                    started_at=now,
                ),
                ChannelDelivery(
                    id=unplanned_id,
                    user_id=user_id,
                    session_id=session_id,
                    turn_id=turn_id,
                    delivery_key="repair-unplanned",
                    origin="final",
                    channel="discord",
                    chat_id="chat-unplanned",
                    binding_generation=uuid4(),
                    status="prepared",
                    total_actions=0,
                    visible_sent_actions=0,
                    created_at=now,
                ),
            ]
        )
        db.add_all(
            [
                ChannelDeliveryAction(
                    delivery_id=prepared_id,
                    action_index=0,
                    action_kind="text_message",
                    visible=True,
                    status="prepared",
                ),
                ChannelDeliveryAction(
                    delivery_id=prepared_id,
                    action_index=1,
                    action_kind="text_message",
                    visible=True,
                    status="prepared",
                ),
                ChannelDeliveryAction(
                    delivery_id=attempting_id,
                    action_index=0,
                    action_kind="text_message",
                    visible=True,
                    status="sent",
                    started_at=now,
                    finished_at=now,
                ),
                ChannelDeliveryAction(
                    delivery_id=attempting_id,
                    action_index=1,
                    action_kind="file_upload",
                    visible=False,
                    status="attempting",
                    started_at=now,
                ),
                ChannelDeliveryAction(
                    delivery_id=attempting_id,
                    action_index=2,
                    action_kind="file_message",
                    visible=True,
                    status="prepared",
                ),
            ]
        )
        await db.commit()

    lookup_calls = 0

    def no_adapter_lookup(_user_id: UUID, _channel: str) -> ChannelAdapter:
        nonlocal lookup_calls
        lookup_calls += 1
        raise AssertionError("startup repair must not call an adapter")

    router = ChannelDeliveryRouter(
        pg_engine,
        adapter_lookup=no_adapter_lookup,
        issue_fence=_allow,
    )

    repaired = await router.repair_incomplete_deliveries()

    prepared, prepared_actions = await _stored_delivery(pg_engine, "repair-prepared")
    attempting, attempting_actions = await _stored_delivery(
        pg_engine, "repair-attempting"
    )
    unplanned, unplanned_actions = await _stored_delivery(
        pg_engine, "repair-unplanned"
    )
    assert repaired == 3
    assert lookup_calls == 0
    assert prepared.status == "failed"
    assert [action.status for action in prepared_actions] == ["failed", "skipped"]
    assert attempting.status == "partial"
    assert attempting.visible_sent_actions == 1
    assert [action.status for action in attempting_actions] == [
        "sent",
        "unknown",
        "skipped",
    ]
    assert unplanned.status == "failed"
    assert unplanned.last_error_code == "server_restart"
    assert unplanned_actions == []


def test_adapter_result_exposes_only_sanitized_fields() -> None:
    assert tuple(field.name for field in fields(ActionResult)) == (
        "status",
        "platform_message_id",
        "artifact_id",
        "error_code",
        "error_message",
    )
