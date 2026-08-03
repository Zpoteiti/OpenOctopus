import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.public_projection import message_response, pending_response
from openctopus_server.chat.repair import synthetic_tool_result
from openctopus_server.chat.runtime_context import build_runtime_block
from openctopus_server.chat.types import AcceptedMessage, TurnStart
from openctopus_server.db.models import Message, PendingMessage, Session, TurnRun, User
from openctopus_server.dto.message import MessagesResponse, PostMessageRequest
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.config import load_provider_config
from openctopus_server.provider.wire_types import Effort

_CANCEL_TEXT = "[user cancelled: tool was not executed because the user pressed stop]"


async def accept_message(
    db: AsyncSession,
    *,
    user: User,
    session_id: UUID,
    body: PostMessageRequest,
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
        elif (
            session.user_id != user.id
            or session.channel != "web"
            or not session.session_key.startswith("web:")
        ):
            raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
        else:
            session.last_inbound_at = now

        stored_content = [
            build_runtime_block(timestamp=now.isoformat(), session=session, user_id=user.id),
            *[block.model_dump(mode="json", exclude_none=True) for block in body.content],
        ]
        db.add(
            PendingMessage(
                id=message_id,
                session_id=session_id,
                user_id=user.id,
                session_key=session.session_key,
                content=stored_content,
                effort=body.effort.value if body.effort is not None else None,
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
) -> Message:
    try:
        await _advisory_lock(db, turn.session_id)
        await _running_turn(db, turn.turn_id)
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
        return message
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
    remaining_tool_ids: list[str],
) -> tuple[list[Message], Message]:
    try:
        await _advisory_lock(db, turn.session_id)
        run = await _running_turn(db, turn.turn_id)
        now = datetime.now(UTC)
        result_rows: list[Message] = []
        for index, tool_id in enumerate(remaining_tool_ids):
            row = Message(
                id=uuid.uuid4(),
                session_id=turn.session_id,
                message_kind="synthetic_tool_result",
                content=[
                    synthetic_tool_result(
                        tool_id,
                        code="user_cancelled",
                        text=_CANCEL_TEXT,
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
        session.cancel_requested = running is not None
        await db.commit()
        return session.cancel_requested
    except Exception:
        await db.rollback()
        raise


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
    session = await _owned_session(db, user_id=user_id, session_id=session_id)
    await _advisory_lock(db, session_id, shared=True)
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
