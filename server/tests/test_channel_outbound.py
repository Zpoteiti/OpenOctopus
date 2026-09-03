from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.channels.ingress import PolicyNotice
from openctopus_server.channels.outbound import ChannelOutbound
from openctopus_server.channels.router import ChannelDeliveryResult
from openctopus_server.chat.runner import (
    ChatRuntime,
    _CompletedProviderTurn,
    _SessionState,
)
from openctopus_server.chat.types import TurnStart
from openctopus_server.db.models import Message, Session, TurnRun, User


class _Router:
    def __init__(self, *, failed_target: bool = False) -> None:
        self.failed_target = failed_target
        self.failed_checks: list[dict[str, object]] = []
        self.deliveries: list[tuple[object, dict[str, object]]] = []

    async def has_failed_target(self, **kwargs: object) -> bool:
        self.failed_checks.append(kwargs)
        return self.failed_target

    async def deliver(self, message: object, **kwargs: object) -> ChannelDeliveryResult:
        self.deliveries.append((message, kwargs))
        return ChannelDeliveryResult(
            delivery_id=uuid4(),
            status="sent",
            visible_sent_actions=1,
            visible_total_actions=1,
            last_error_code=None,
            last_error_message=None,
        )


def _turn() -> TurnStart:
    return TurnStart(
        session_id=uuid4(),
        turn_id=uuid4(),
        message_ids=(uuid4(),),
        effort=None,
        tool_profile="owner_full",
    )


def _assistant(turn: TurnStart) -> Message:
    return Message(
        id=uuid4(),
        session_id=turn.session_id,
        message_kind="assistant",
        content=[
            {"type": "thinking", "thinking": "hidden"},
            {"type": "text", "text": "first"},
            {"type": "text", "text": "second"},
        ],
        attachment_refs=[],
        created_at=datetime.now(UTC),
    )


async def test_final_delivery_uses_full_persisted_text_and_stable_identity() -> None:
    router = _Router()
    outbound = ChannelOutbound(router)  # type: ignore[arg-type]
    turn = _turn()
    assistant = _assistant(turn)
    user_id = uuid4()
    generation = uuid4()

    await outbound.deliver_final(
        turn=turn,
        assistant=assistant,
        user_id=user_id,
        channel="discord",
        chat_id="chat-1",
        binding_generation=generation,
    )

    assert len(router.deliveries) == 1
    message, kwargs = router.deliveries[0]
    assert message.delivery_key == f"final:{assistant.id}"  # type: ignore[attr-defined]
    assert message.content == "first\n\nsecond"  # type: ignore[attr-defined]
    assert message.turn_id == turn.turn_id  # type: ignore[attr-defined]
    assert kwargs == {
        "session_id": turn.session_id,
        "assistant_message_id": assistant.id,
    }


async def test_final_delivery_skips_internal_but_audits_failed_same_turn_target() -> None:
    router = _Router(failed_target=True)
    outbound = ChannelOutbound(router)  # type: ignore[arg-type]
    turn = _turn()
    assistant = _assistant(turn)
    user_id = uuid4()

    await outbound.deliver_final(
        turn=turn,
        assistant=assistant,
        user_id=user_id,
        channel="web",
        chat_id="web-chat",
        binding_generation=None,
    )
    await outbound.deliver_final(
        turn=turn,
        assistant=assistant,
        user_id=user_id,
        channel="dingtalk",
        chat_id="chat-2",
        binding_generation=uuid4(),
    )

    assert router.failed_checks == []
    assert len(router.deliveries) == 1
    message, kwargs = router.deliveries[0]
    assert message.origin == "final"  # type: ignore[attr-defined]
    assert kwargs == {
        "session_id": turn.session_id,
        "assistant_message_id": assistant.id,
    }


async def test_policy_delivery_uses_router_without_turn_or_session() -> None:
    router = _Router()
    outbound = ChannelOutbound(router)  # type: ignore[arg-type]
    notice = PolicyNotice(
        delivery_key="policy:key",
        user_id=uuid4(),
        channel="dingtalk",
        chat_id="chat-3",
        binding_generation=uuid4(),
        source_message_id="source-1",
        content="Attachments are not accepted.",
    )

    await outbound.deliver_policy(notice)

    message, kwargs = router.deliveries[0]
    assert message.delivery_key == "policy:key"  # type: ignore[attr-defined]
    assert message.origin == "policy_notice"  # type: ignore[attr-defined]
    assert kwargs == {}


async def test_runtime_delivers_persisted_external_final_before_finishing_turn(
    pg_engine,
    user_client,
) -> None:
    del user_client
    turn = _turn()
    assistant = _assistant(turn)
    generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.scalars(select(User).where(User.email == "user@test.com"))).one()
        session = Session(
            id=turn.session_id,
            user_id=user.id,
            session_key=f"discord:bot:{turn.session_id}",
            channel="discord",
            chat_id="external-chat",
            title="Discord",
        )
        db.add(session)
        await db.flush()
        run = TurnRun(
            id=turn.turn_id,
            session_id=turn.session_id,
            runner_instance_id=uuid4(),
            status="running",
            tool_profile="owner_full",
            input_message_ids=[],
            failed_delivery_targets=[],
            started_at=datetime.now(UTC),
        )
        db.add(run)
        await db.flush()
        db.add(assistant)
        await db.commit()

    observed: list[tuple[str, str]] = []

    class _FinalDelivery:
        async def deliver_final(self, **kwargs: object) -> None:
            async with AsyncSession(pg_engine, expire_on_commit=False) as db:
                persisted = await db.get(Message, assistant.id)
                run = await db.get(TurnRun, turn.turn_id)
            assert persisted is not None
            assert run is not None
            observed.append((run.status, str(kwargs["channel"])))

    runtime = ChatRuntime(pg_engine, channel_final_delivery=_FinalDelivery())
    completed = _CompletedProviderTurn(
        turn=turn,
        assistant=assistant,
        user_id=user.id,
        device_targets={},
        mcp_snapshot=None,  # type: ignore[arg-type]
        current_channel="discord",
        current_chat_id="external-chat",
        current_binding_generation=generation,
    )

    async def completed_iteration(
        _state: _SessionState,
        _turn: TurnStart,
    ) -> _CompletedProviderTurn:
        return completed

    runtime._invoke_provider_iteration = completed_iteration  # type: ignore[method-assign]
    try:
        await runtime._execute_chain(_SessionState(turn.session_id), turn)
    finally:
        await runtime.close()

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        run = await db.get(TurnRun, turn.turn_id)
    assert observed == [("running", "discord")]
    assert run is not None and run.status == "completed"
