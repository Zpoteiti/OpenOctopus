import asyncio
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import exists, or_, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.chat.runner import ChatRuntime, DetachedSession
from openctopus_server.db.advisory import lock_uuid_identity
from openctopus_server.db.models import Message, Session, TurnRun
from openctopus_server.dto.session import SessionPatchRequest, SessionResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError


async def list_owned(
    db: AsyncSession,
    *,
    user_id: UUID,
    limit: int,
    offset: int,
) -> list[SessionResponse]:
    unread = _unread_expression()
    rows = (
        await db.execute(
            select(Session, unread.label("unread"))
            .where(Session.user_id == user_id)
            .order_by(
                Session.last_inbound_at.desc().nulls_last(),
                Session.created_at.desc(),
                Session.id.desc(),
            )
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return [_response(session, unread=bool(is_unread)) for session, is_unread in rows]


async def patch_owned(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
    patch: SessionPatchRequest,
) -> SessionResponse:
    try:
        await lock_uuid_identity(db, session_id)
        session = await _owned_session_for_update(
            db,
            user_id=user_id,
            session_id=session_id,
        )
        if patch.read_through_message_id is not None:
            message_created_at = await db.scalar(
                select(Message.created_at).where(
                    Message.id == patch.read_through_message_id,
                    Message.session_id == session_id,
                )
            )
            if message_created_at is None:
                raise ChatError(
                    ErrorCode.SESSION_INVALID_REQUEST,
                    "Read marker message is invalid",
                )
            if session.last_read_at is None or message_created_at > session.last_read_at:
                session.last_read_at = message_created_at
        if patch.title is not None:
            session.title = patch.title
        await db.flush()
        is_unread = bool(
            await db.scalar(
                select(_unread_expression()).where(Session.id == session_id)
            )
        )
        response = _response(session, unread=is_unread)
        await db.commit()
        return response
    except BaseException:
        await db.rollback()
        raise


async def delete_owned(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
    runtime: ChatRuntime,
) -> None:
    transition = asyncio.create_task(
        _delete_owned_transition(
            db,
            user_id=user_id,
            session_id=session_id,
            runtime=runtime,
        )
    )
    await await_future_cancellation_safe(transition)


async def _delete_owned_transition(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
    runtime: ChatRuntime,
) -> None:
    async with runtime.session_operation(session_id):
        detached: DetachedSession | None = None
        try:
            await lock_uuid_identity(db, session_id)
            session = await _owned_session_for_update(
                db,
                user_id=user_id,
                session_id=session_id,
            )
            detached = await runtime.detach_session(session_id)
            await db.delete(session)
            await db.commit()
            runtime.finalize_detached_session(detached, deleted=True)
        except BaseException:
            try:
                try:
                    await db.rollback()
                finally:
                    if detached is not None:
                        recovery = asyncio.create_task(
                            _abandon_interrupted_turns(
                                runtime.engine,
                                session_id=session_id,
                            )
                        )
                        await await_future_cancellation_safe(recovery)
            finally:
                if detached is not None:
                    runtime.finalize_detached_session(detached, deleted=False)
            raise


async def _abandon_interrupted_turns(
    engine: AsyncEngine,
    *,
    session_id: UUID,
) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        now = datetime.now(UTC)
        await db.execute(
            update(TurnRun)
            .where(
                TurnRun.session_id == session_id,
                TurnRun.status == "running",
            )
            .values(status="abandoned", finished_at=now)
        )
        await db.execute(
            update(Session)
            .where(Session.id == session_id)
            .values(cancel_requested=False)
        )
        await db.commit()


async def _owned_session_for_update(
    db: AsyncSession,
    *,
    user_id: UUID,
    session_id: UUID,
) -> Session:
    session = await db.scalar(
        select(Session)
        .where(Session.id == session_id, Session.user_id == user_id)
        .with_for_update()
    )
    if session is None:
        raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
    return session


def _unread_expression() -> ColumnElement[bool]:
    return exists(
        select(Message.id).where(
            Message.session_id == Session.id,
            or_(
                Session.last_read_at.is_(None),
                Message.created_at > Session.last_read_at,
            ),
        )
    )


def _response(session: Session, *, unread: bool) -> SessionResponse:
    return SessionResponse(
        id=session.id,
        user_id=session.user_id,
        session_key=session.session_key,
        channel=session.channel,
        chat_id=session.chat_id,
        title=session.title,
        last_inbound_at=session.last_inbound_at,
        unread=unread,
        cancel_requested=session.cancel_requested,
        created_at=session.created_at,
    )
