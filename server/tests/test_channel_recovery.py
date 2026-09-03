from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.channels.recovery import (
    close_obsolete_channel_pending,
    recover_channel_pending,
)
from openctopus_server.db.models import (
    DiscordConfig,
    Message,
    PendingMessage,
    Session,
    TurnRun,
    User,
)


class _Runtime:
    def __init__(self) -> None:
        self.runner_instance_id = uuid4()
        self.scheduled = []
        self.operations = []

    @asynccontextmanager
    async def session_operation(self, session_id):
        self.operations.append(session_id)
        yield

    async def schedule(self, accepted):
        self.scheduled.append(accepted)


async def test_ready_recovery_only_reserves_current_binding_pending(
    pg_engine,
    user_client,
) -> None:
    del user_client
    current_generation = uuid4()
    stale_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.scalars(select(User).where(User.email == "user@test.com"))).one()
        db.add(
            DiscordConfig(
                user_id=user.id,
                bot_token="secret",
                application_id="application-1",
                bot_user_id="bot-1",
                bot_display_name="Bot",
                binding_generation=current_generation,
                revision=1,
                owner_platform_user_id="owner-1",
                owner_dm_chat_id="dm-1",
                paired_at=datetime.now(UTC),
                allow_list=[],
            )
        )
        sessions = []
        for suffix in ("current", "stale"):
            session = Session(
                id=uuid4(),
                user_id=user.id,
                session_key=f"discord:application-1:{suffix}",
                channel="discord",
                chat_id=suffix,
                title=suffix,
            )
            db.add(session)
            sessions.append(session)
        await db.flush()
        db.add_all(
            [
                PendingMessage(
                    id=uuid4(),
                    session_id=session.id,
                    user_id=user.id,
                    session_key=session.session_key,
                    content=[{"type": "text", "text": "hello"}],
                    attachment_refs=[],
                    sender_id="owner-1",
                    sender_display_name="Owner",
                    sender_classification="owner",
                    ingress_tool_profile="owner_full",
                    source_message_id=f"source-{index}",
                    channel_binding_generation=generation,
                    channel_context=[],
                    received_at=datetime.now(UTC),
                )
                for index, (session, generation) in enumerate(
                    zip(
                        sessions,
                        (current_generation, stale_generation),
                        strict=True,
                    )
                )
            ]
        )
        await db.commit()

    runtime = _Runtime()
    await recover_channel_pending(
        pg_engine,
        runtime,  # type: ignore[arg-type]
        user.id,
        "discord",
        current_generation,
    )

    assert runtime.operations == [sessions[0].id]
    assert len(runtime.scheduled) == 1
    assert runtime.scheduled[0].session_id == sessions[0].id


async def test_startup_recovery_closes_deleted_binding_pending(
    pg_engine,
    user_client,
) -> None:
    del user_client
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.scalars(select(User).where(User.email == "user@test.com"))).one()
        session = Session(
            id=uuid4(),
            user_id=user.id,
            session_key="discord:deleted-application:dm-1",
            channel="discord",
            chat_id="dm-1",
            title="deleted",
        )
        pending_id = uuid4()
        db.add(session)
        await db.flush()
        db.add(
            PendingMessage(
                id=pending_id,
                session_id=session.id,
                user_id=user.id,
                session_key=session.session_key,
                content=[{"type": "text", "text": "hello"}],
                attachment_refs=[],
                sender_id="owner-1",
                sender_display_name="Owner",
                sender_classification="owner",
                ingress_tool_profile="owner_full",
                source_message_id="source-deleted",
                channel_binding_generation=uuid4(),
                channel_context=[],
                received_at=datetime.now(UTC),
            )
        )
        await db.commit()

    runtime = _Runtime()
    await close_obsolete_channel_pending(pg_engine, runtime)  # type: ignore[arg-type]

    assert runtime.operations == [session.id]
    assert runtime.scheduled == []
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        assert await db.get(PendingMessage, pending_id) is None
        human = await db.get(Message, pending_id)
        assert human is not None
        assert human.message_kind == "human"
        turn = (
            await db.scalars(select(TurnRun).where(TurnRun.session_id == session.id))
        ).one()
        assert turn.status == "failed"
        assert turn.input_message_ids == [str(pending_id)]
        terminal = (
            await db.scalars(
                select(Message).where(
                    Message.session_id == session.id,
                    Message.message_kind == "synthetic_assistant_error",
                )
            )
        ).one()
        assert "channel_authority_revoked" in terminal.content[0]["text"]


async def test_startup_recovery_closes_replaced_binding_and_leaves_current_pending(
    pg_engine,
    user_client,
) -> None:
    del user_client
    stale_generation = uuid4()
    current_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.scalars(select(User).where(User.email == "user@test.com"))).one()
        db.add(
            DiscordConfig(
                user_id=user.id,
                bot_token="new-secret",
                application_id="new-application",
                bot_user_id="new-bot",
                bot_display_name="New Bot",
                binding_generation=current_generation,
                revision=1,
                owner_platform_user_id="new-owner",
                owner_dm_chat_id="new-dm",
                paired_at=datetime.now(UTC),
                allow_list=[],
            )
        )
        stale_session = Session(
            id=uuid4(),
            user_id=user.id,
            session_key="discord:old-application:old-dm",
            channel="discord",
            chat_id="old-dm",
            title="old",
        )
        current_session = Session(
            id=uuid4(),
            user_id=user.id,
            session_key="discord:new-application:new-dm",
            channel="discord",
            chat_id="new-dm",
            title="new",
        )
        db.add_all([stale_session, current_session])
        await db.flush()
        pending_ids = [uuid4(), uuid4()]
        db.add_all(
            [
                PendingMessage(
                    id=pending_id,
                    session_id=session.id,
                    user_id=user.id,
                    session_key=session.session_key,
                    content=[{"type": "text", "text": "hello"}],
                    attachment_refs=[],
                    sender_id=sender_id,
                    sender_display_name="Owner",
                    sender_classification="owner",
                    ingress_tool_profile="owner_full",
                    source_message_id=f"source-{index}",
                    channel_binding_generation=generation,
                    channel_context=[],
                    received_at=datetime.now(UTC),
                )
                for index, (pending_id, session, sender_id, generation) in enumerate(
                    zip(
                        pending_ids,
                        (stale_session, current_session),
                        ("old-owner", "new-owner"),
                        (stale_generation, current_generation),
                        strict=True,
                    )
                )
            ]
        )
        await db.commit()

    runtime = _Runtime()
    await close_obsolete_channel_pending(pg_engine, runtime)  # type: ignore[arg-type]

    assert set(runtime.operations) == {stale_session.id, current_session.id}
    assert runtime.scheduled == []
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        assert await db.get(PendingMessage, pending_ids[0]) is None
        assert await db.get(Message, pending_ids[0]) is not None
        assert await db.get(PendingMessage, pending_ids[1]) is not None
        assert await db.get(Message, pending_ids[1]) is None
        assert (
            await db.scalar(
                select(TurnRun).where(TurnRun.session_id == current_session.id)
            )
            is None
        )
