import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.channels.types import (
    DeliveryOrigin,
    ExternalChannel,
    InboundMessage,
    ToolProfile,
)
from openctopus_server.chat.public_projection import message_response, pending_response
from openctopus_server.chat.repair import synthetic_tool_result
from openctopus_server.chat.runtime_context import build_runtime_block
from openctopus_server.chat.types import AcceptedMessage, TurnStart
from openctopus_server.db.advisory import lock_uuid_identity
from openctopus_server.db.models import (
    ChannelDelivery,
    CronJob,
    DingTalkConfig,
    DiscordConfig,
    Message,
    PendingMessage,
    Session,
    TurnRun,
    User,
)
from openctopus_server.dto.message import ChannelDeliveryResponse, MessagesResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.config import load_provider_config
from openctopus_server.provider.wire_types import Effort
from openctopus_server.services.inbound import (
    lock_inbound_identity,
    serialize_channel_context,
    web_inbound,
)

_CANCEL_TEXT = "[user cancelled: tool was not executed because the user pressed stop]"
_OUTCOME_UNKNOWN_TEXT = (
    "[user cancelled: tool execution outcome is unknown because the server stopped "
    "waiting before recording its result]"
)
_CHANNEL_AUTHORITY_REVOKED_TEXT = (
    "[channel_authority_revoked] This channel request was closed because its "
    "sender authority or Bot binding is no longer current."
)
_cancel_waiters: dict[UUID, set[asyncio.Future[None]]] = {}


async def accept_message(
    db: AsyncSession,
    *,
    user: User,
    session_id: UUID,
    content: list[dict[str, Any]],
    attachment_refs: list[dict[str, Any]] | None = None,
    effort: Effort | None,
    runner_instance_id: UUID,
) -> AcceptedMessage:
    inbound = web_inbound(
        owner_user_id=user.id,
        session_id=session_id,
        content=content,
        attachment_refs=attachment_refs,
        effort=effort,
    )
    try:
        if await lock_inbound_identity(db, inbound) is None:
            raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
        await _require_writable_web_target(
            db,
            user_id=user.id,
            session_id=session_id,
        )
        await load_provider_config(db)
        accepted = await publish_inbound_locked(
            db,
            inbound=inbound,
            title="New chat",
            runner_instance_id=runner_instance_id,
            queue_if_busy=True,
        )
        assert accepted is not None
        await db.commit()
        return accepted
    except Exception:
        await db.rollback()
        raise


async def publish_inbound_locked(
    db: AsyncSession,
    *,
    inbound: InboundMessage,
    title: str,
    runner_instance_id: UUID,
    queue_if_busy: bool,
) -> AcceptedMessage | None:
    """Publish after the caller acquired identity and owner locks; never commit."""
    now = datetime.now(UTC)
    session = await db.scalar(
        select(Session).where(Session.id == inbound.session_id).with_for_update()
    )
    created_session = False
    if session is None:
        if inbound.channel == "web" and await _identity_is_reserved(
            db, inbound.session_id
        ):
            raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
        session = Session(
            id=inbound.session_id,
            user_id=inbound.owner_user_id,
            session_key=inbound.session_key,
            channel=inbound.channel,
            chat_id=inbound.chat_id,
            title=title,
            last_inbound_at=now,
            created_at=now,
        )
        db.add(session)
        await db.flush()
        created_session = True
    elif not _session_matches_inbound(session, inbound=inbound):
        raise ChatError(ErrorCode.NOT_FOUND, "Session not found")

    running = await db.scalar(
        select(TurnRun)
        .where(
            TurnRun.session_id == inbound.session_id,
            TurnRun.status == "running",
        )
        .with_for_update()
    )
    pending_rows = await _pending_rows(
        db,
        session_id=inbound.session_id,
        for_update=True,
    )
    if not queue_if_busy and (running is not None or pending_rows):
        return None

    session.last_inbound_at = now
    db.add(
        PendingMessage(
            id=inbound.message_id,
            session_id=inbound.session_id,
            user_id=inbound.owner_user_id,
            session_key=inbound.session_key,
            content=[
                build_runtime_block(
                    timestamp=now.isoformat(),
                    session=session,
                    sender_id=inbound.sender.id,
                    trust=inbound.sender.classification,
                ),
                *(dict(block) for block in inbound.content),
            ],
            attachment_refs=[dict(ref) for ref in inbound.attachment_refs],
            effort=inbound.effort.value if inbound.effort is not None else None,
            sender_id=inbound.sender.id,
            sender_display_name=inbound.sender.display_name,
            sender_classification=inbound.sender.classification,
            ingress_tool_profile=inbound.ingress_tool_profile,
            source_message_id=inbound.source_message_id,
            channel_binding_generation=inbound.channel_binding_generation,
            channel_context=serialize_channel_context(inbound),
            received_at=now,
        )
    )
    await db.flush()

    if running is not None:
        return AcceptedMessage(
            session_id=inbound.session_id,
            message_id=inbound.message_id,
            accepted_at=now,
            disposition="queued",
            created_session=created_session,
            turn=None,
        )

    pending_rows = await _pending_rows(
        db,
        session_id=inbound.session_id,
        for_update=True,
    )
    turn = await _reserve_fresh_pending_locked(
        db,
        session_id=inbound.session_id,
        runner_instance_id=runner_instance_id,
        pending_rows=pending_rows,
        started_at=now,
    )
    assert turn is not None
    return AcceptedMessage(
        session_id=inbound.session_id,
        message_id=inbound.message_id,
        accepted_at=now,
        disposition="started",
        created_session=created_session,
        turn=turn,
    )


async def preflight_message_target(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
) -> None:
    """Reject unusable message targets before resolving browser attachments."""
    await _require_writable_web_target(
        db,
        user_id=user_id,
        session_id=session_id,
    )
    await load_provider_config(db)


async def _require_writable_web_target(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
) -> None:
    session = await db.scalar(select(Session).where(Session.id == session_id))
    if session is None:
        if await _identity_is_reserved(db, session_id):
            raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
    elif not _is_writable_web_session(session, user_id=user_id):
        raise ChatError(ErrorCode.NOT_FOUND, "Session not found")


async def reserve_pending_turn(
    db: AsyncSession,
    *,
    session_id: UUID,
    runner_instance_id: UUID,
) -> TurnStart | None:
    try:
        await lock_uuid_identity(db, session_id)
        running = (
            await db.execute(
                select(TurnRun)
                .where(TurnRun.session_id == session_id, TurnRun.status == "running")
                .with_for_update()
            )
        ).scalar_one_or_none()
        if running is not None:
            await db.commit()
            return None
        pending_rows = await _pending_rows(db, session_id=session_id, for_update=True)
        if not pending_rows:
            await db.commit()
            return None
        turn = await _reserve_fresh_pending_locked(
            db,
            session_id=session_id,
            runner_instance_id=runner_instance_id,
            pending_rows=pending_rows,
        )
        await db.commit()
        return turn
    except Exception:
        await db.rollback()
        raise


async def close_revoked_pending_prefix(
    db: AsyncSession,
    *,
    session_id: UUID,
    runner_instance_id: UUID,
) -> int:
    """Close obsolete external Pending rows without starting a current Turn."""
    try:
        await lock_uuid_identity(db, session_id)
        running = (
            await db.execute(
                select(TurnRun)
                .where(TurnRun.session_id == session_id, TurnRun.status == "running")
                .with_for_update()
            )
        ).scalar_one_or_none()
        if running is not None:
            await db.commit()
            return 0
        remaining = await _pending_rows(db, session_id=session_id, for_update=True)
        closed = 0
        while remaining:
            if await _valid_contiguous_profile_prefix(db, remaining):
                break
            await _close_revoked_pending(
                db,
                row=remaining[0],
                runner_instance_id=runner_instance_id,
            )
            remaining = remaining[1:]
            closed += 1
        await db.commit()
        return closed
    except Exception:
        await db.rollback()
        raise


async def drain_pending_and_create_turn(
    db: AsyncSession,
    *,
    session_id: UUID,
    runner_instance_id: UUID,
) -> TurnStart | None:
    """Reserve and promote a recovered pending batch outside the live runner."""
    turn = await reserve_pending_turn(
        db,
        session_id=session_id,
        runner_instance_id=runner_instance_id,
    )
    if turn is None:
        return None
    return await promote_pending_for_turn(db, turn=turn)


async def capture_pending_for_turn(
    db: AsyncSession,
    *,
    turn: TurnStart,
) -> TurnStart:
    """Capture one pending boundary for an otherwise empty continuation turn."""
    try:
        await lock_uuid_identity(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        _require_turn_profile(turn, run)
        if turn.message_ids:
            await db.commit()
            return turn
        if run.input_message_ids:
            captured = TurnStart(
                session_id=turn.session_id,
                turn_id=turn.turn_id,
                message_ids=tuple(UUID(message_id) for message_id in run.input_message_ids),
                effort=turn.effort,
                tool_profile=_stored_tool_profile(run.tool_profile),
            )
            await db.commit()
            return captured
        pending_rows = await _pending_rows(db, session_id=turn.session_id, for_update=True)
        captured_rows = await _valid_contiguous_profile_prefix(
            db,
            pending_rows,
            expected_profile=run.tool_profile,
        )
        if not captured_rows:
            await db.commit()
            return turn
        captured = TurnStart(
            session_id=turn.session_id,
            turn_id=turn.turn_id,
            message_ids=tuple(row.id for row in captured_rows),
            effort=_effort_from_pending(captured_rows[-1]),
            tool_profile=_stored_tool_profile(run.tool_profile),
        )
        run.input_message_ids = [str(message_id) for message_id in captured.message_ids]
        await db.commit()
        return captured
    except Exception:
        await db.rollback()
        raise


async def promote_pending_for_turn(
    db: AsyncSession,
    *,
    turn: TurnStart,
) -> TurnStart:
    try:
        await lock_uuid_identity(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        _require_turn_profile(turn, run)
        captured_ids = turn.message_ids
        if not captured_ids:
            await db.commit()
            return turn
        pending_rows = await _pending_rows(db, session_id=turn.session_id, for_update=True)
        captured_rows = pending_rows[: len(captured_ids)]
        if tuple(row.id for row in captured_rows) == captured_ids:
            await _promote_pending_rows(db, captured_rows)
            await db.commit()
            return turn

        canonical_ids = set(
            (
                await db.execute(
                    select(Message.id).where(
                        Message.session_id == turn.session_id,
                        Message.id.in_(captured_ids),
                    )
                )
            )
            .scalars()
            .all()
        )
        if len(canonical_ids) == len(captured_ids):
            await db.commit()
            return turn
        raise RuntimeError("Captured pending prefix changed before promotion")
    except Exception:
        await db.rollback()
        raise


async def persist_assistant(
    db: AsyncSession,
    *,
    turn: TurnStart,
    content: list[dict[str, Any]],
    fingerprint: str | None,
    failed: bool = False,
) -> Message:
    try:
        await lock_uuid_identity(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        _require_turn_profile(turn, run)
        now = datetime.now(UTC)
        message = Message(
            id=uuid.uuid4(),
            session_id=turn.session_id,
            message_kind=("synthetic_assistant_error" if failed else "assistant"),
            content=content,
            delivery_refs=[],
            llm_fingerprint=fingerprint,
            is_compacted=False,
            created_at=now,
        )
        db.add(message)
        if failed:
            run.status = "failed"
            run.finished_at = now
            session = await db.get(Session, turn.session_id)
            if session is not None:
                session.cancel_requested = False
        await db.commit()
        return message
    except Exception:
        await db.rollback()
        raise


async def persist_tool_result(
    db: AsyncSession,
    *,
    turn: TurnStart,
    block: dict[str, Any],
    synthetic: bool = False,
    assistant_message_id: UUID | None = None,
    delivery_refs: list[dict[str, Any]] | None = None,
) -> tuple[Message | None, Message]:
    if (assistant_message_id is None) != (delivery_refs is None):
        raise ValueError("assistant_message_id and delivery_refs must be provided together")
    if synthetic and assistant_message_id is not None:
        raise ValueError("synthetic tool results cannot attach delivery refs")

    try:
        await lock_uuid_identity(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        _require_turn_profile(turn, run)
        updated_assistant: Message | None = None
        if assistant_message_id is not None:
            updated_assistant = (
                await db.execute(
                    select(Message)
                    .where(
                        Message.id == assistant_message_id,
                        Message.session_id == turn.session_id,
                        Message.message_kind == "assistant",
                        Message.is_compacted.is_(False),
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if updated_assistant is None:
                raise RuntimeError("Message delivery assistant message is unavailable")

            tool_use_id = block.get("tool_use_id")
            matching_uses = [
                item
                for item in updated_assistant.content
                if item.get("type") == "tool_use" and item.get("id") == tool_use_id
            ]
            if len(matching_uses) != 1 or matching_uses[0].get("name") != "message":
                raise RuntimeError("Message delivery tool use is unavailable")
            assert delivery_refs is not None
            if any(ref.get("tool_use_id") != tool_use_id for ref in delivery_refs):
                raise RuntimeError("Message delivery ref does not match its tool use")
            if any(
                isinstance(ref, dict) and ref.get("tool_use_id") == tool_use_id
                for ref in updated_assistant.delivery_refs
            ):
                raise RuntimeError("Message delivery refs are already attached")
            updated_assistant.delivery_refs = [
                *updated_assistant.delivery_refs,
                *[dict(ref) for ref in delivery_refs],
            ]

        message = Message(
            id=uuid.uuid4(),
            session_id=turn.session_id,
            message_kind="synthetic_tool_result" if synthetic else "tool_result",
            content=[block],
            delivery_refs=[],
            is_compacted=False,
            created_at=datetime.now(UTC),
        )
        db.add(message)
        await db.commit()
        return updated_assistant, message
    except Exception:
        await db.rollback()
        raise


async def persist_human_marker(
    db: AsyncSession,
    *,
    turn: TurnStart,
    text_content: str,
) -> Message:
    try:
        await lock_uuid_identity(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        _require_turn_profile(turn, run)
        message = Message(
            id=uuid.uuid4(),
            session_id=turn.session_id,
            message_kind="human",
            content=[{"type": "text", "text": text_content}],
            delivery_refs=[],
            sender_id="openoctopus:server",
            sender_display_name="OpenOctopus",
            sender_classification="internal",
            ingress_tool_profile=turn.tool_profile,
            is_compacted=False,
            created_at=datetime.now(UTC),
        )
        db.add(message)
        await db.commit()
        return message
    except Exception:
        await db.rollback()
        raise


async def finish_final_turn(db: AsyncSession, *, turn: TurnStart) -> None:
    try:
        await lock_uuid_identity(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        _require_turn_profile(turn, run)
        run.status = "completed"
        run.finished_at = datetime.now(UTC)
        session = await db.get(Session, turn.session_id)
        if session is not None:
            session.cancel_requested = False
        await db.commit()
    except Exception:
        await db.rollback()
        raise


async def finish_tool_batch_and_continue(
    db: AsyncSession,
    *,
    turn: TurnStart,
    runner_instance_id: UUID,
) -> TurnStart:
    try:
        await lock_uuid_identity(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        _require_turn_profile(turn, run)
        now = datetime.now(UTC)
        run.status = "completed"
        run.finished_at = now
        await db.flush()
        pending_rows = await _pending_rows(db, session_id=turn.session_id, for_update=True)
        captured_rows = await _valid_contiguous_profile_prefix(
            db,
            pending_rows,
            expected_profile=run.tool_profile,
        )
        next_turn = _create_turn(
            db,
            session_id=turn.session_id,
            runner_instance_id=runner_instance_id,
            message_ids=tuple(row.id for row in captured_rows),
            effort=(
                _effort_from_pending(captured_rows[-1])
                if captured_rows
                else turn.effort
            ),
            tool_profile=_stored_tool_profile(run.tool_profile),
            failed_delivery_targets=[
                dict(target) for target in run.failed_delivery_targets
            ],
            started_at=now,
        )
        await db.commit()
        return next_turn
    except Exception:
        await db.rollback()
        raise


async def cancel_tool_batch(
    db: AsyncSession,
    *,
    turn: TurnStart,
    outcome_unknown_tool_ids: list[str],
    cancelled_tool_ids: list[str],
) -> tuple[list[Message], Message]:
    try:
        await lock_uuid_identity(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        _require_turn_profile(turn, run)
        now = datetime.now(UTC)
        result_rows: list[Message] = []
        outcomes = [
            (
                tool_id,
                ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value,
                _OUTCOME_UNKNOWN_TEXT,
            )
            for tool_id in outcome_unknown_tool_ids
        ]
        outcomes.extend(
            (tool_id, ErrorCode.USER_CANCELLED.value, _CANCEL_TEXT)
            for tool_id in cancelled_tool_ids
        )
        for index, (tool_id, code, text_content) in enumerate(outcomes):
            row = Message(
                id=uuid.uuid4(),
                session_id=turn.session_id,
                message_kind="synthetic_tool_result",
                content=[
                    synthetic_tool_result(
                        tool_id,
                        code=code,
                        text=text_content,
                    )
                ],
                delivery_refs=[],
                is_compacted=False,
                created_at=now + timedelta(microseconds=index),
            )
            db.add(row)
            result_rows.append(row)
        marker = Message(
            id=uuid.uuid4(),
            session_id=turn.session_id,
            message_kind="human",
            content=[{"type": "text", "text": "[User pressed stop]"}],
            delivery_refs=[],
            sender_id="openoctopus:server",
            sender_display_name="OpenOctopus",
            sender_classification="internal",
            ingress_tool_profile=turn.tool_profile,
            is_compacted=False,
            created_at=now + timedelta(microseconds=len(result_rows)),
        )
        db.add(marker)
        run.status = "cancelled"
        run.finished_at = marker.created_at
        session = await db.get(Session, turn.session_id)
        if session is not None:
            session.cancel_requested = False
        await db.commit()
        return result_rows, marker
    except Exception:
        await db.rollback()
        raise


async def is_cancel_requested(db: AsyncSession, *, session_id: UUID) -> bool:
    value = (
        await db.execute(select(Session.cancel_requested).where(Session.id == session_id))
    ).scalar_one_or_none()
    return bool(value)


async def request_cancel(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
) -> bool:
    transition = asyncio.create_task(
        _request_cancel_transition(
            db,
            user_id=user_id,
            session_id=session_id,
        )
    )
    return await await_future_cancellation_safe(transition)


async def _request_cancel_transition(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
) -> bool:
    try:
        await lock_uuid_identity(db, session_id)
        session = (
            await db.execute(
                select(Session)
                .where(Session.id == session_id, Session.user_id == user_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if session is None:
            raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
        running = (
            await db.execute(
                select(TurnRun.id).where(
                    TurnRun.session_id == session_id,
                    TurnRun.status == "running",
                )
            )
        ).scalar_one_or_none()
        cancel_requested = running is not None
        session.cancel_requested = cancel_requested
        await db.commit()
        if cancel_requested:
            _notify_cancel_waiters(session_id)
        return cancel_requested
    except Exception:
        await db.rollback()
        raise


def register_cancel_waiter(session_id: UUID) -> asyncio.Future[None]:
    waiter = asyncio.get_running_loop().create_future()
    _cancel_waiters.setdefault(session_id, set()).add(waiter)
    return waiter


def discard_cancel_waiter(session_id: UUID, waiter: asyncio.Future[None]) -> None:
    waiters = _cancel_waiters.get(session_id)
    if waiters is not None:
        waiters.discard(waiter)
        if not waiters:
            _cancel_waiters.pop(session_id, None)
    if not waiter.done():
        waiter.cancel()


def _notify_cancel_waiters(session_id: UUID) -> None:
    for waiter in tuple(_cancel_waiters.get(session_id, ())):
        if not waiter.done():
            waiter.set_result(None)


async def get_messages_response(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
    before: UUID | None,
    after: UUID | None,
    limit: int,
) -> MessagesResponse:
    if before is not None and after is not None:
        raise ChatError(ErrorCode.INVALID_CURSOR, "before and after are mutually exclusive")
    await lock_uuid_identity(db, session_id, shared=True)
    session = await _owned_session(db, user_id=user_id, session_id=session_id)
    anchor_id = before if before is not None else after
    anchor: Message | None = None
    if anchor_id is not None:
        anchor = (
            await db.execute(
                select(Message).where(
                    Message.id == anchor_id,
                    Message.session_id == session_id,
                )
            )
        ).scalar_one_or_none()
        if anchor is None:
            raise ChatError(ErrorCode.INVALID_CURSOR, "Message cursor is invalid")

    query = select(Message).where(Message.session_id == session_id)
    if before is not None and anchor is not None:
        query = query.where(_is_before_anchor(anchor)).order_by(
            Message.created_at.desc(), Message.id.desc()
        )
    elif after is not None and anchor is not None:
        query = query.where(_is_after_anchor(anchor)).order_by(Message.created_at, Message.id)
    else:
        query = query.order_by(Message.created_at.desc(), Message.id.desc())

    rows = list((await db.execute(query.limit(limit + 1))).scalars().all())
    if after is None:
        has_more_before = len(rows) > limit
        rows = rows[:limit]
        rows.reverse()
    else:
        rows = rows[:limit]
        has_more_before = await _has_older_rows(
            db,
            session_id=session_id,
            first=rows[0] if rows else None,
        )

    deliveries_by_message: dict[UUID, list[ChannelDeliveryResponse]] = {}
    message_ids = tuple(row.id for row in rows)
    if message_ids:
        delivery_rows = list(
            (
                await db.scalars(
                    select(ChannelDelivery)
                    .where(ChannelDelivery.assistant_message_id.in_(message_ids))
                    .order_by(ChannelDelivery.created_at, ChannelDelivery.id)
                )
            ).all()
        )
        for delivery in delivery_rows:
            if delivery.assistant_message_id is None:
                continue
            deliveries_by_message.setdefault(delivery.assistant_message_id, []).append(
                ChannelDeliveryResponse(
                    channel=cast(ExternalChannel, delivery.channel),
                    chat_id=delivery.chat_id,
                    origin=cast(DeliveryOrigin, delivery.origin),
                    status=cast(
                        Literal[
                            "prepared",
                            "attempting",
                            "sent",
                            "partial",
                            "failed",
                            "unknown",
                        ],
                        delivery.status,
                    ),
                    total_actions=delivery.total_actions,
                    visible_sent_actions=delivery.visible_sent_actions,
                    error_code=delivery.last_error_code,
                    error_message=delivery.last_error_message,
                    created_at=delivery.created_at,
                )
            )

    pending_rows = await _pending_rows(db, session_id=session_id)
    latest_run = (
        await db.execute(
            select(TurnRun)
            .where(TurnRun.session_id == session_id)
            .order_by(TurnRun.started_at.desc(), TurnRun.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    last_message_id = (
        await db.execute(
            select(Message.id)
            .where(Message.session_id == session_id)
            .order_by(Message.created_at.desc(), Message.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()

    status: Literal["idle", "running", "failed", "abandoned"] = "idle"
    active_turn_id: UUID | None = None
    if latest_run is not None and latest_run.status in {"running", "failed", "abandoned"}:
        status = cast(
            Literal["running", "failed", "abandoned"],
            latest_run.status,
        )
        if latest_run.status == "running":
            active_turn_id = latest_run.id

    return MessagesResponse(
        messages=[
            message_response(
                row,
                session=session,
                deliveries=deliveries_by_message.get(row.id, []),
            )
            for row in rows
        ],
        pending_messages=[pending_response(row, session=session) for row in pending_rows],
        status=status,
        active_turn_id=active_turn_id,
        last_message_id=last_message_id,
        pending_count=len(pending_rows),
        has_more_before=has_more_before,
    )


async def get_owned_session(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
) -> Session:
    return await _owned_session(db, user_id=user_id, session_id=session_id)


async def _owned_session(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
) -> Session:
    session = (
        await db.execute(
            select(Session).where(Session.id == session_id, Session.user_id == user_id)
        )
    ).scalar_one_or_none()
    if session is None:
        raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
    return session


def _is_writable_web_session(session: Session, *, user_id: UUID) -> bool:
    return (
        session.user_id == user_id
        and session.channel == "web"
        and session.session_key.startswith("web:")
    )


def _session_matches_inbound(
    session: Session,
    *,
    inbound: InboundMessage,
) -> bool:
    return (
        session.user_id == inbound.owner_user_id
        and session.session_key == inbound.session_key
        and session.channel == inbound.channel
        and session.chat_id == inbound.chat_id
    )


async def _identity_is_reserved(db: AsyncSession, session_id: UUID) -> bool:
    if await db.scalar(select(User.id).where(User.id == session_id)) is not None:
        return True
    return (
        await db.scalar(select(CronJob.id).where(CronJob.id == session_id))
        is not None
    )


async def _pending_rows(
    db: AsyncSession,
    *,
    session_id: UUID,
    for_update: bool = False,
) -> list[PendingMessage]:
    query = (
        select(PendingMessage)
        .where(PendingMessage.session_id == session_id)
        .order_by(PendingMessage.received_at, PendingMessage.id)
    )
    if for_update:
        query = query.with_for_update()
    return list((await db.execute(query)).scalars().all())


async def _promote_pending_rows(
    db: AsyncSession,
    rows: list[PendingMessage],
) -> tuple[UUID, ...]:
    promoted_at = datetime.now(UTC)
    for index, row in enumerate(rows):
        db.add(
            Message(
                id=row.id,
                session_id=row.session_id,
                message_kind="human",
                content=row.content,
                attachment_refs=[dict(ref) for ref in (row.attachment_refs or [])],
                delivery_refs=[],
                sender_id=row.sender_id,
                sender_display_name=row.sender_display_name,
                sender_classification=row.sender_classification,
                ingress_tool_profile=row.ingress_tool_profile,
                source_message_id=row.source_message_id,
                channel_binding_generation=row.channel_binding_generation,
                channel_context=[dict(item) for item in (row.channel_context or [])],
                is_compacted=False,
                created_at=promoted_at + timedelta(microseconds=index),
            )
        )
        await db.delete(row)
    return tuple(row.id for row in rows)


def _create_turn(
    db: AsyncSession,
    *,
    session_id: UUID,
    runner_instance_id: UUID,
    message_ids: tuple[UUID, ...],
    effort: Effort | None,
    tool_profile: ToolProfile,
    failed_delivery_targets: list[dict[str, Any]] | None = None,
    started_at: datetime | None = None,
) -> TurnStart:
    turn_id = uuid.uuid4()
    db.add(
        TurnRun(
            id=turn_id,
            session_id=session_id,
            runner_instance_id=runner_instance_id,
            status="running",
            tool_profile=tool_profile,
            input_message_ids=[str(message_id) for message_id in message_ids],
            failed_delivery_targets=[
                dict(target) for target in (failed_delivery_targets or [])
            ],
            started_at=started_at or datetime.now(UTC),
        )
    )
    return TurnStart(
        session_id=session_id,
        turn_id=turn_id,
        message_ids=message_ids,
        effort=effort,
        tool_profile=tool_profile,
    )


async def _running_turn(db: AsyncSession, turn_id: UUID) -> TurnRun:
    run = (
        await db.execute(select(TurnRun).where(TurnRun.id == turn_id).with_for_update())
    ).scalar_one_or_none()
    if run is None or run.status != "running":
        raise ChatError(ErrorCode.NOT_FOUND, "Active turn not found")
    return run


def _effort_from_pending(row: PendingMessage) -> Effort | None:
    return Effort(row.effort) if row.effort is not None else None


def _stored_tool_profile(value: str) -> ToolProfile:
    if value not in {"owner_full", "message_only"}:
        raise RuntimeError("Stored tool profile is invalid")
    return cast(ToolProfile, value)


async def _reserve_fresh_pending_locked(
    db: AsyncSession,
    *,
    session_id: UUID,
    runner_instance_id: UUID,
    pending_rows: list[PendingMessage],
    started_at: datetime | None = None,
) -> TurnStart | None:
    remaining = pending_rows
    next_started_at = started_at
    while remaining:
        captured_rows = await _valid_contiguous_profile_prefix(db, remaining)
        if captured_rows:
            return _create_turn(
                db,
                session_id=session_id,
                runner_instance_id=runner_instance_id,
                message_ids=tuple(row.id for row in captured_rows),
                effort=_effort_from_pending(captured_rows[-1]),
                tool_profile=_stored_tool_profile(
                    captured_rows[0].ingress_tool_profile
                ),
                started_at=next_started_at,
            )
        await _close_revoked_pending(
            db,
            row=remaining[0],
            runner_instance_id=runner_instance_id,
        )
        remaining = remaining[1:]
        next_started_at = None
    return None


async def _valid_contiguous_profile_prefix(
    db: AsyncSession,
    rows: list[PendingMessage],
    *,
    expected_profile: str | None = None,
) -> list[PendingMessage]:
    if not rows:
        return []
    profile = expected_profile or rows[0].ingress_tool_profile
    if rows[0].ingress_tool_profile != profile:
        return []
    session = await db.get(Session, rows[0].session_id)
    if session is None:
        raise RuntimeError("Pending channel Session disappeared")
    config = await _locked_channel_config(
        db,
        user_id=rows[0].user_id,
        channel=session.channel,
    )
    captured: list[PendingMessage] = []
    for row in rows:
        if row.ingress_tool_profile != profile:
            break
        if not _pending_authority_is_current(
            row,
            channel=session.channel,
            config=config,
        ):
            break
        captured.append(row)
    return captured


async def _locked_channel_config(
    db: AsyncSession,
    *,
    user_id: UUID,
    channel: str,
) -> DiscordConfig | DingTalkConfig | None:
    if channel == "discord":
        return (
            await db.execute(
                select(DiscordConfig)
                .where(DiscordConfig.user_id == user_id)
                .with_for_update(read=True)
            )
        ).scalar_one_or_none()
    if channel == "dingtalk":
        return (
            await db.execute(
                select(DingTalkConfig)
                .where(DingTalkConfig.user_id == user_id)
                .with_for_update(read=True)
            )
        ).scalar_one_or_none()
    return None


def _pending_authority_is_current(
    row: PendingMessage,
    *,
    channel: str,
    config: DiscordConfig | DingTalkConfig | None,
) -> bool:
    if channel not in {"discord", "dingtalk"}:
        return (
            row.channel_binding_generation is None
            and row.ingress_tool_profile == "owner_full"
            and row.sender_classification in {"owner", "internal"}
        )
    if config is None or row.channel_binding_generation != config.binding_generation:
        return False
    if row.ingress_tool_profile == "owner_full":
        return (
            row.sender_classification == "owner"
            and config.owner_platform_user_id is not None
            and row.sender_id == config.owner_platform_user_id
        )
    return (
        row.ingress_tool_profile == "message_only"
        and row.sender_classification == "allowed_non_owner"
        and row.sender_id in config.allow_list
    )


async def _close_revoked_pending(
    db: AsyncSession,
    *,
    row: PendingMessage,
    runner_instance_id: UUID,
) -> None:
    now = datetime.now(UTC)
    await _promote_pending_rows(db, [row])
    terminal_at = datetime.now(UTC) + timedelta(microseconds=1)
    turn_id = uuid.uuid4()
    db.add(
        TurnRun(
            id=turn_id,
            session_id=row.session_id,
            runner_instance_id=runner_instance_id,
            status="failed",
            tool_profile=row.ingress_tool_profile,
            input_message_ids=[str(row.id)],
            failed_delivery_targets=[],
            started_at=now,
            finished_at=terminal_at,
        )
    )
    db.add(
        Message(
            id=uuid.uuid4(),
            session_id=row.session_id,
            message_kind="synthetic_assistant_error",
            content=[{"type": "text", "text": _CHANNEL_AUTHORITY_REVOKED_TEXT}],
            attachment_refs=[],
            delivery_refs=[],
            is_compacted=False,
            created_at=terminal_at,
        )
    )


def _require_turn_profile(turn: TurnStart, run: TurnRun) -> None:
    if turn.tool_profile != run.tool_profile:
        raise RuntimeError("Turn tool profile changed after reservation")


def _is_before_anchor(anchor: Message) -> Any:
    return or_(
        Message.created_at < anchor.created_at,
        and_(Message.created_at == anchor.created_at, Message.id < anchor.id),
    )


def _is_after_anchor(anchor: Message) -> Any:
    return or_(
        Message.created_at > anchor.created_at,
        and_(Message.created_at == anchor.created_at, Message.id > anchor.id),
    )


async def _has_older_rows(
    db: AsyncSession,
    *,
    session_id: UUID,
    first: Message | None,
) -> bool:
    if first is None:
        return False
    result = await db.execute(
        select(Message.id)
        .where(Message.session_id == session_id, _is_before_anchor(first))
        .limit(1)
    )
    return result.scalar_one_or_none() is not None
