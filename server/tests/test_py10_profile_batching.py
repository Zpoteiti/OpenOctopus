from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.types import TurnStart
from openctopus_server.db.models import (
    DiscordConfig,
    Message,
    PendingMessage,
    Session,
    TurnRun,
    User,
)
from openctopus_server.services.messages import (
    capture_pending_for_turn,
    finish_final_turn,
    finish_tool_batch_and_continue,
    promote_pending_for_turn,
    reserve_pending_turn,
)


async def _owner(db: AsyncSession) -> User:
    return (await db.scalars(select(User).where(User.email == "user@test.com"))).one()


async def _session(db: AsyncSession, *, owner: User) -> Session:
    application_id = str(uuid4())
    binding_generation = uuid4()
    session = Session(
        id=uuid4(),
        user_id=owner.id,
        session_key=f"discord:{application_id}:{uuid4()}",
        channel="discord",
        chat_id=str(uuid4()),
        title="Discord test",
        created_at=datetime.now(UTC),
    )
    db.add(session)
    await db.flush()
    db.add(
        DiscordConfig(
            user_id=owner.id,
            bot_token="secret",
            application_id=application_id,
            bot_user_id="bot-1",
            bot_display_name="Bot",
            binding_generation=binding_generation,
            owner_platform_user_id="sender-owner_full",
            owner_dm_chat_id="owner-dm",
            paired_at=datetime.now(UTC),
            allow_list=["sender-message_only"],
        )
    )
    await db.flush()
    session._test_binding_generation = binding_generation  # type: ignore[attr-defined]
    return session


def _pending(
    *,
    session: Session,
    owner: User,
    profile: str,
    received_at: datetime,
) -> PendingMessage:
    sender_classification = (
        "owner" if profile == "owner_full" else "allowed_non_owner"
    )
    return PendingMessage(
        id=uuid4(),
        session_id=session.id,
        user_id=owner.id,
        session_key=session.session_key,
        content=[{"type": "text", "text": profile}],
        attachment_refs=[],
        effort=None,
        sender_id=f"sender-{profile}",
        sender_display_name=None,
        sender_classification=sender_classification,
        ingress_tool_profile=profile,
        source_message_id=str(uuid4()),
        channel_binding_generation=session._test_binding_generation,  # type: ignore[attr-defined]
        channel_context=[],
        received_at=received_at,
    )


async def _turn_run(db: AsyncSession, turn_id: UUID) -> TurnRun:
    run = await db.get(TurnRun, turn_id)
    assert run is not None
    return run


async def test_fresh_turns_capture_only_the_maximum_contiguous_profile_prefix(
    user_client,
    pg_engine,
) -> None:
    del user_client
    runner_instance_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner=owner)
        rows = [
            _pending(
                session=session,
                owner=owner,
                profile=profile,
                received_at=now + timedelta(microseconds=index),
            )
            for index, profile in enumerate(
                ("owner_full", "owner_full", "message_only", "message_only", "owner_full")
            )
        ]
        db.add_all(rows)
        await db.commit()

    expected = (("owner_full", rows[:2]), ("message_only", rows[2:4]), ("owner_full", rows[4:]))
    for profile, expected_rows in expected:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            turn = await reserve_pending_turn(
                db,
                session_id=session.id,
                runner_instance_id=runner_instance_id,
            )
            assert turn is not None
            assert turn.tool_profile == profile
            assert turn.message_ids == tuple(row.id for row in expected_rows)
            run = await _turn_run(db, turn.turn_id)
            assert run.tool_profile == profile
            assert run.input_message_ids == [str(row.id) for row in expected_rows]

            await promote_pending_for_turn(db, turn=turn)
            await finish_final_turn(db, turn=turn)

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        assert (
            await reserve_pending_turn(
                db,
                session_id=session.id,
                runner_instance_id=runner_instance_id,
            )
            is None
        )


async def test_tool_continuation_does_not_capture_a_different_profile(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner=owner)
        allowed = _pending(
            session=session,
            owner=owner,
            profile="message_only",
            received_at=now,
        )
        db.add(allowed)
        run = TurnRun(
            id=uuid4(),
            session_id=session.id,
            runner_instance_id=uuid4(),
            status="running",
            tool_profile="owner_full",
            input_message_ids=[],
            started_at=now,
        )
        db.add(run)
        await db.commit()

    turn = TurnStart(
        session_id=session.id,
        turn_id=run.id,
        message_ids=(),
        effort=None,
        tool_profile="owner_full",
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        captured = await capture_pending_for_turn(db, turn=turn)
        assert captured == turn
        persisted = await _turn_run(db, run.id)
        assert persisted.input_message_ids == []
        assert await db.get(PendingMessage, allowed.id) is not None


async def test_tool_continuation_persists_same_profile_capture(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner=owner)
        rows = [
            _pending(
                session=session,
                owner=owner,
                profile="message_only",
                received_at=now + timedelta(microseconds=index),
            )
            for index in range(2)
        ]
        db.add_all(rows)
        run = TurnRun(
            id=uuid4(),
            session_id=session.id,
            runner_instance_id=uuid4(),
            status="running",
            tool_profile="message_only",
            input_message_ids=[],
            started_at=now,
        )
        db.add(run)
        await db.commit()

    turn = TurnStart(
        session_id=session.id,
        turn_id=run.id,
        message_ids=(),
        effort=None,
        tool_profile="message_only",
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        captured = await capture_pending_for_turn(db, turn=turn)
        assert captured.message_ids == tuple(row.id for row in rows)
        assert captured.tool_profile == "message_only"
        persisted = await _turn_run(db, run.id)
        assert persisted.input_message_ids == [str(row.id) for row in rows]


async def test_tool_batch_leaves_different_profile_for_a_fresh_turn(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner=owner)
        allowed = _pending(
            session=session,
            owner=owner,
            profile="message_only",
            received_at=now,
        )
        db.add(allowed)
        current_run = TurnRun(
            id=uuid4(),
            session_id=session.id,
            runner_instance_id=uuid4(),
            status="running",
            tool_profile="owner_full",
            input_message_ids=[],
            started_at=now,
        )
        db.add(current_run)
        await db.commit()

    current = TurnStart(
        session_id=session.id,
        turn_id=current_run.id,
        message_ids=(),
        effort=None,
        tool_profile="owner_full",
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        continuation = await finish_tool_batch_and_continue(
            db,
            turn=current,
            runner_instance_id=uuid4(),
        )
        assert continuation.tool_profile == "owner_full"
        assert continuation.message_ids == ()
        assert await db.get(PendingMessage, allowed.id) is not None

        continuation_run = await _turn_run(db, continuation.turn_id)
        continuation_run.status = "completed"
        continuation_run.finished_at = datetime.now(UTC)
        await db.commit()

        fresh = await reserve_pending_turn(
            db,
            session_id=session.id,
            runner_instance_id=uuid4(),
        )
        assert fresh is not None
        assert fresh.tool_profile == "message_only"
        assert fresh.message_ids == (allowed.id,)


async def test_removed_allow_list_sender_is_closed_without_provider_turn(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner=owner)
        pending = _pending(
            session=session,
            owner=owner,
            profile="message_only",
            received_at=now,
        )
        db.add(pending)
        config = await db.get(DiscordConfig, owner.id)
        assert config is not None
        config.allow_list = []
        await db.commit()

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        turn = await reserve_pending_turn(
            db,
            session_id=session.id,
            runner_instance_id=uuid4(),
        )
        assert turn is None
        assert await db.get(PendingMessage, pending.id) is None
        rows = list(
            (
                await db.scalars(
                    select(Message)
                    .where(Message.session_id == session.id)
                    .order_by(Message.created_at, Message.id)
                )
            ).all()
        )
        runs = list(
            (
                await db.scalars(select(TurnRun).where(TurnRun.session_id == session.id))
            ).all()
        )

    assert [row.message_kind for row in rows] == [
        "human",
        "synthetic_assistant_error",
    ]
    assert "channel_authority_revoked" in rows[-1].content[0]["text"]
    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert runs[0].tool_profile == "message_only"
    assert runs[0].input_message_ids == [str(pending.id)]


async def test_old_allowed_row_stays_message_only_after_sender_becomes_owner(
    user_client,
    pg_engine,
) -> None:
    del user_client
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner=owner)
        pending = _pending(
            session=session,
            owner=owner,
            profile="message_only",
            received_at=datetime.now(UTC),
        )
        db.add(pending)
        config = await db.get(DiscordConfig, owner.id)
        assert config is not None
        config.owner_platform_user_id = pending.sender_id
        await db.commit()

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        turn = await reserve_pending_turn(
            db,
            session_id=session.id,
            runner_instance_id=uuid4(),
        )

    assert turn is not None
    assert turn.tool_profile == "message_only"
    assert turn.message_ids == (pending.id,)


async def test_old_binding_pending_is_closed_instead_of_retargeted(
    user_client,
    pg_engine,
) -> None:
    del user_client
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner=owner)
        pending = _pending(
            session=session,
            owner=owner,
            profile="owner_full",
            received_at=datetime.now(UTC),
        )
        db.add(pending)
        config = await db.get(DiscordConfig, owner.id)
        assert config is not None
        config.binding_generation = uuid4()
        await db.commit()

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        turn = await reserve_pending_turn(
            db,
            session_id=session.id,
            runner_instance_id=uuid4(),
        )
        assert turn is None
        closed = await db.scalar(
            select(TurnRun).where(TurnRun.session_id == session.id)
        )

    assert closed is not None
    assert closed.status == "failed"
    assert closed.input_message_ids == [str(pending.id)]
