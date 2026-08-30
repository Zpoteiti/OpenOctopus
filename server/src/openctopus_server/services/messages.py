import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.chat.public_projection import message_response, pending_response
from openctopus_server.chat.repair import synthetic_tool_result
from openctopus_server.chat.runtime_context import build_runtime_block
from openctopus_server.chat.types import AcceptedMessage, TurnStart
from openctopus_server.db.models import Message, PendingMessage, Session, TurnRun, User
from openctopus_server.dto.message import MessagesResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.config import load_provider_config
from openctopus_server.provider.wire_types import Effort

_CANCEL_TEXT = "[user cancelled: tool was not executed because the user pressed stop]"
_OUTCOME_UNKNOWN_TEXT = (
    "[user cancelled: tool execution outcome is unknown because the server stopped "
    "waiting before recording its result]"
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
    now = datetime.now(UTC)
    message_id = uuid.uuid4()
    created_session = False
    try:
        await _advisory_lock(db, session_id)
        await load_provider_config(db)
        session = (
            await db.execute(select(Session).where(Session.id == session_id).with_for_update())
        ).scalar_one_or_none()
        if session is None:
            session = Session(
                id=session_id,
                user_id=user.id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="New chat",
                last_inbound_at=now,
                created_at=now,
            )
            db.add(session)
            await db.flush()
            created_session = True
        elif not _is_writable_web_session(session, user_id=user.id):
            raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
        else:
            session.last_inbound_at = now

        stored_content = [
            build_runtime_block(timestamp=now.isoformat(), session=session, user_id=user.id),
            *content,
        ]
        db.add(
            PendingMessage(
                id=message_id,
                session_id=session_id,
                user_id=user.id,
                session_key=session.session_key,
                content=stored_content,
                attachment_refs=[dict(ref) for ref in (attachment_refs or [])],
                effort=effort.value if effort is not None else None,
                received_at=now,
            )
        )
        await db.flush()

        running = (
            await db.execute(
                select(TurnRun)
                .where(TurnRun.session_id == session_id, TurnRun.status == "running")
                .with_for_update()
            )
        ).scalar_one_or_none()
        if running is not None:
            await db.commit()
            return AcceptedMessage(
                session_id=session_id,
                message_id=message_id,
                accepted_at=now,
                disposition="queued",
                created_session=created_session,
                turn=None,
            )

        pending_rows = await _pending_rows(db, session_id=session_id, for_update=True)
        turn = _create_turn(
            db,
            session_id=session_id,
            runner_instance_id=runner_instance_id,
            message_ids=tuple(row.id for row in pending_rows),
            effort=_effort_from_pending(pending_rows[-1]),
            started_at=now,
        )
        await db.commit()
        return AcceptedMessage(
            session_id=session_id,
            message_id=message_id,
            accepted_at=now,
            disposition="started",
            created_session=created_session,
            turn=turn,
        )
    except Exception:
        await db.rollback()
        raise


async def preflight_message_target(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
) -> None:
    """Reject unusable message targets before resolving browser attachments."""
    await load_provider_config(db)
    session = await db.scalar(select(Session).where(Session.id == session_id))
    if session is not None and not _is_writable_web_session(session, user_id=user_id):
        raise ChatError(ErrorCode.NOT_FOUND, "Session not found")


async def reserve_pending_turn(
    db: AsyncSession,
    *,
    session_id: UUID,
    runner_instance_id: UUID,
) -> TurnStart | None:
    try:
        await _advisory_lock(db, session_id)
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
        turn = _create_turn(
            db,
            session_id=session_id,
            runner_instance_id=runner_instance_id,
            message_ids=tuple(row.id for row in pending_rows),
            effort=_effort_from_pending(pending_rows[-1]),
        )
        await db.commit()
        return turn
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
        await _advisory_lock(db, turn.session_id)
        await _running_turn(db, turn.turn_id)
        if turn.message_ids:
            await db.commit()
            return turn
        pending_rows = await _pending_rows(db, session_id=turn.session_id, for_update=True)
        if not pending_rows:
            await db.commit()
            return turn
        captured = TurnStart(
            session_id=turn.session_id,
            turn_id=turn.turn_id,
            message_ids=tuple(row.id for row in pending_rows),
            effort=_effort_from_pending(pending_rows[-1]),
        )
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
        await _advisory_lock(db, turn.session_id)
        await _running_turn(db, turn.turn_id)
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
        await _advisory_lock(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
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
        await _advisory_lock(db, turn.session_id)
        await _running_turn(db, turn.turn_id)
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
        await _advisory_lock(db, turn.session_id)
        await _running_turn(db, turn.turn_id)
        message = Message(
            id=uuid.uuid4(),
            session_id=turn.session_id,
            message_kind="human",
            content=[{"type": "text", "text": text_content}],
            delivery_refs=[],
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
        await _advisory_lock(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
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
        await _advisory_lock(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        now = datetime.now(UTC)
        run.status = "completed"
        run.finished_at = now
        await db.flush()
        pending_rows = await _pending_rows(db, session_id=turn.session_id, for_update=True)
        next_turn = _create_turn(
            db,
            session_id=turn.session_id,
            runner_instance_id=runner_instance_id,
            message_ids=tuple(row.id for row in pending_rows),
            effort=(_effort_from_pending(pending_rows[-1]) if pending_rows else turn.effort),
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
        await _advisory_lock(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
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
        await _advisory_lock(db, session_id)
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
    await _advisory_lock(db, session_id, shared=True)
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

    status = "idle"
    active_turn_id: UUID | None = None
    if latest_run is not None and latest_run.status in {"running", "failed", "abandoned"}:
        status = latest_run.status
        if latest_run.status == "running":
            active_turn_id = latest_run.id

    return MessagesResponse(
        messages=[message_response(row, session=session) for row in rows],
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
    started_at: datetime | None = None,
) -> TurnStart:
    turn_id = uuid.uuid4()
    db.add(
        TurnRun(
            id=turn_id,
            session_id=session_id,
            runner_instance_id=runner_instance_id,
            status="running",
            started_at=started_at or datetime.now(UTC),
        )
    )
    return TurnStart(
        session_id=session_id,
        turn_id=turn_id,
        message_ids=message_ids,
        effort=effort,
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


async def _advisory_lock(
    db: AsyncSession,
    session_id: UUID,
    *,
    shared: bool = False,
) -> None:
    key = session_id.int & ((1 << 63) - 1)
    statement = (
        "SELECT pg_advisory_xact_lock_shared(:key)"
        if shared
        else "SELECT pg_advisory_xact_lock(:key)"
    )
    await db.execute(text(statement), {"key": key})


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
