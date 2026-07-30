import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import and_, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.public_projection import (
    build_runtime_block,
    message_response,
    pending_response,
)
from openctopus_server.chat.types import AcceptedMessage, TurnStart
from openctopus_server.db.models import (
    Message,
    PendingMessage,
    Session,
    TurnRun,
    User,
)
from openctopus_server.dto.message import MessagesResponse, PostMessageRequest
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.config import load_provider_config
from openctopus_server.provider.wire_types import Effort


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
            build_runtime_block(
                timestamp=now.isoformat(),
                session=session,
                user_id=user.id,
            ),
            *[block.model_dump(mode="json", exclude_none=True) for block in body.content],
        ]
        running = (
            await db.execute(
                select(TurnRun)
                .where(
                    TurnRun.session_id == session_id,
                    TurnRun.status == "running",
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if running is not None:
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
            await db.commit()
            return AcceptedMessage(
                session_id=session_id,
                message_id=message_id,
                accepted_at=now,
                disposition="queued",
                created_session=created_session,
                turn=None,
            )

        pending_rows = list(
            (
                await db.execute(
                    select(PendingMessage)
                    .where(PendingMessage.session_id == session_id)
                    .order_by(PendingMessage.received_at, PendingMessage.id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        if pending_rows:
            incoming = PendingMessage(
                id=message_id,
                session_id=session_id,
                user_id=user.id,
                session_key=session.session_key,
                content=stored_content,
                effort=body.effort.value if body.effort is not None else None,
                received_at=now,
            )
            pending_rows.append(incoming)
            pending_rows.sort(key=lambda row: (row.received_at, row.id))
            message_ids = await _promote_pending_rows(db, pending_rows)
            turn_effort = _effort_from_pending(pending_rows[-1])
        else:
            db.add(
                Message(
                    id=message_id,
                    session_id=session_id,
                    role="user",
                    message_kind="human",
                    content=stored_content,
                    delivery_refs=[],
                    created_at=now,
                )
            )
            message_ids = (message_id,)
            turn_effort = body.effort

        turn_id = uuid.uuid4()
        db.add(
            TurnRun(
                id=turn_id,
                session_id=session_id,
                runner_instance_id=runner_instance_id,
                status="running",
                started_at=now,
            )
        )
        await db.commit()
        return AcceptedMessage(
            session_id=session_id,
            message_id=message_id,
            accepted_at=now,
            disposition="started",
            created_session=created_session,
            turn=TurnStart(
                session_id=session_id,
                turn_id=turn_id,
                message_ids=message_ids,
                effort=turn_effort,
            ),
        )
    except Exception:
        await db.rollback()
        raise


async def drain_pending_and_create_turn(
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
                .where(
                    TurnRun.session_id == session_id,
                    TurnRun.status == "running",
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if running is not None:
            await db.commit()
            return None

        pending_rows = list(
            (
                await db.execute(
                    select(PendingMessage)
                    .where(PendingMessage.session_id == session_id)
                    .order_by(PendingMessage.received_at, PendingMessage.id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        if not pending_rows:
            await db.commit()
            return None

        message_ids = await _promote_pending_rows(db, pending_rows)
        turn_id = uuid.uuid4()
        now = datetime.now(UTC)
        db.add(
            TurnRun(
                id=turn_id,
                session_id=session_id,
                runner_instance_id=runner_instance_id,
                status="running",
                started_at=now,
            )
        )
        await db.commit()
        return TurnStart(
            session_id=session_id,
            turn_id=turn_id,
            message_ids=message_ids,
            effort=_effort_from_pending(pending_rows[-1]),
        )
    except Exception:
        await db.rollback()
        raise


async def persist_turn_outcome(
    db: AsyncSession,
    *,
    turn: TurnStart,
    content: list[dict[str, Any]],
    fingerprint: str | None,
    failed: bool,
) -> Message:
    try:
        await _advisory_lock(db, turn.session_id)
        run = (
            await db.execute(select(TurnRun).where(TurnRun.id == turn.turn_id).with_for_update())
        ).scalar_one_or_none()
        if run is None or run.status != "running":
            raise ChatError(ErrorCode.NOT_FOUND, "Active turn not found")
        now = datetime.now(UTC)
        message = Message(
            id=uuid.uuid4(),
            session_id=turn.session_id,
            role="assistant",
            message_kind=("synthetic_assistant_error" if failed else "assistant"),
            content=content,
            delivery_refs=[],
            llm_fingerprint=fingerprint,
            created_at=now,
        )
        db.add(message)
        run.status = "failed" if failed else "completed"
        run.finished_at = now
        await db.commit()
        return message
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
        raise ChatError(
            ErrorCode.INVALID_CURSOR,
            "before and after are mutually exclusive",
        )
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
        query = query.where(_is_before_anchor(anchor))
        query = query.order_by(Message.created_at.desc(), Message.id.desc())
    elif after is not None and anchor is not None:
        query = query.where(_is_after_anchor(anchor))
        query = query.order_by(Message.created_at, Message.id)
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

    pending_rows = (
        (
            await db.execute(
                select(PendingMessage)
                .where(PendingMessage.session_id == session_id)
                .order_by(PendingMessage.received_at, PendingMessage.id)
            )
        )
        .scalars()
        .all()
    )
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
    if latest_run is not None and latest_run.status in {
        "running",
        "failed",
        "abandoned",
    }:
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
            select(Session).where(
                Session.id == session_id,
                Session.user_id == user_id,
            )
        )
    ).scalar_one_or_none()
    if session is None:
        raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
    return session


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
                role="user",
                message_kind="human",
                content=row.content,
                delivery_refs=[],
                created_at=promoted_at + timedelta(microseconds=index),
            )
        )
        if row in db:
            await db.delete(row)
    return tuple(row.id for row in rows)


def _effort_from_pending(row: PendingMessage) -> Effort | None:
    return Effort(row.effort) if row.effort is not None else None


async def _advisory_lock(db: AsyncSession, session_id: UUID) -> None:
    key = session_id.int & ((1 << 63) - 1)
    await db.execute(
        text("SELECT pg_advisory_xact_lock(:key)"),
        {"key": key},
    )


def _is_before_anchor(anchor: Message) -> Any:
    return or_(
        Message.created_at < anchor.created_at,
        and_(
            Message.created_at == anchor.created_at,
            Message.id < anchor.id,
        ),
    )


def _is_after_anchor(anchor: Message) -> Any:
    return or_(
        Message.created_at > anchor.created_at,
        and_(
            Message.created_at == anchor.created_at,
            Message.id > anchor.id,
        ),
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
        .where(
            Message.session_id == session_id,
            _is_before_anchor(first),
        )
        .limit(1)
    )
    return result.scalar_one_or_none() is not None
