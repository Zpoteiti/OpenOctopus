from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import (
    DiscordConfig,
    Message,
    PendingMessage,
    Session,
    TurnRun,
    User,
)
from openctopus_server.services.messages import reserve_pending_turn
from openctopus_server.services.turn_runs import abandon_running_turns


async def _owner(db: AsyncSession) -> User:
    return (await db.scalars(select(User).where(User.email == "user@test.com"))).one()


async def _session(db: AsyncSession, owner: User) -> Session:
    application_id = str(uuid4())
    binding_generation = uuid4()
    row = Session(
        id=uuid4(),
        user_id=owner.id,
        session_key=f"discord:{application_id}:{uuid4()}",
        channel="discord",
        chat_id=str(uuid4()),
        title="Channel",
    )
    db.add(row)
    await db.flush()
    db.add(
        DiscordConfig(
            user_id=owner.id,
            bot_token="secret",
            application_id=application_id,
            bot_user_id="bot-id",
            bot_display_name="Bot",
            binding_generation=binding_generation,
            revision=1,
            owner_platform_user_id="owner-1",
            owner_dm_chat_id="owner-dm",
            paired_at=datetime.now(UTC),
            allow_list=["allowed-1"],
        )
    )
    row._test_binding_generation = binding_generation  # type: ignore[attr-defined]
    return row


def _human(*, message_id: UUID, session_id: UUID, created_at: datetime) -> Message:
    return Message(
        id=message_id,
        session_id=session_id,
        message_kind="human",
        content=[{"type": "text", "text": "allowed request"}],
        attachment_refs=[],
        delivery_refs=[],
        sender_id="allowed-1",
        sender_display_name="Allowed",
        sender_classification="allowed_non_owner",
        ingress_tool_profile="message_only",
        source_message_id="source-1",
        channel_binding_generation=uuid4(),
        channel_context=[],
        created_at=created_at,
    )


def _run(*, session_id: UUID, message_id: UUID, started_at: datetime) -> TurnRun:
    return TurnRun(
        id=uuid4(),
        session_id=session_id,
        runner_instance_id=uuid4(),
        status="running",
        tool_profile="message_only",
        input_message_ids=[str(message_id)],
        failed_delivery_targets=[],
        started_at=started_at,
    )


async def test_promoted_abandoned_turn_gets_one_terminal_boundary(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner)
        message_id = uuid4()
        run = _run(
            session_id=session.id,
            message_id=message_id,
            started_at=now,
        )
        db.add_all(
            [
                _human(
                    message_id=message_id,
                    session_id=session.id,
                    created_at=now + timedelta(microseconds=1),
                ),
                run,
            ]
        )
        await db.commit()

    current_runner = uuid4()
    await abandon_running_turns(pg_engine, runner_instance_id=current_runner)
    await abandon_running_turns(pg_engine, runner_instance_id=current_runner)

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        stored = await db.get(TurnRun, run.id)
        errors = list(
            (
                await db.scalars(
                    select(Message).where(
                        Message.session_id == session.id,
                        Message.message_kind == "synthetic_assistant_error",
                    )
                )
            ).all()
        )

    assert stored is not None
    assert stored.status == "abandoned"
    assert len(errors) == 1
    assert errors[0].content == [
        {
            "type": "text",
            "text": (
                "[turn_abandoned] The previous request was closed because the "
                "OpenOctopus server restarted."
            ),
        }
    ]


async def test_unpromoted_pending_survives_without_abandoned_error(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner)
        pending_id = uuid4()
        db.add(
            PendingMessage(
                id=pending_id,
                session_id=session.id,
                user_id=owner.id,
                session_key=session.session_key,
                content=[{"type": "text", "text": "not promoted"}],
                attachment_refs=[],
                sender_id="allowed-1",
                sender_display_name=None,
                sender_classification="allowed_non_owner",
                ingress_tool_profile="message_only",
                source_message_id="source-1",
                channel_binding_generation=session._test_binding_generation,  # type: ignore[attr-defined]
                channel_context=[],
                received_at=now,
            )
        )
        run = _run(
            session_id=session.id,
            message_id=pending_id,
            started_at=now,
        )
        db.add(run)
        await db.commit()

    current_runner = uuid4()
    await abandon_running_turns(pg_engine, runner_instance_id=current_runner)

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        assert await db.get(PendingMessage, pending_id) is not None
        assert (
            await db.scalar(
                select(Message.id).where(
                    Message.session_id == session.id,
                    Message.message_kind == "synthetic_assistant_error",
                )
            )
            is None
        )
        fresh = await reserve_pending_turn(
            db,
            session_id=session.id,
            runner_instance_id=current_runner,
        )

    assert fresh is not None
    assert fresh.message_ids == (pending_id,)
    assert fresh.tool_profile == "message_only"


async def test_existing_terminal_assistant_is_not_duplicated(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner)
        message_id = uuid4()
        run = _run(
            session_id=session.id,
            message_id=message_id,
            started_at=now,
        )
        db.add_all(
            [
                _human(
                    message_id=message_id,
                    session_id=session.id,
                    created_at=now + timedelta(microseconds=1),
                ),
                Message(
                    id=uuid4(),
                    session_id=session.id,
                    message_kind="assistant",
                    content=[{"type": "text", "text": "already complete"}],
                    attachment_refs=[],
                    delivery_refs=[],
                    is_compacted=False,
                    created_at=now + timedelta(microseconds=2),
                ),
                run,
            ]
        )
        await db.commit()

    await abandon_running_turns(pg_engine, runner_instance_id=uuid4())

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        count = len(
            (
                await db.scalars(
                    select(Message.id).where(
                        Message.session_id == session.id,
                        Message.message_kind == "synthetic_assistant_error",
                    )
                )
            ).all()
        )
    assert count == 0


async def test_dangling_tool_use_is_repaired_before_abandoned_boundary(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        session = await _session(db, owner)
        message_id = uuid4()
        run = _run(
            session_id=session.id,
            message_id=message_id,
            started_at=now,
        )
        db.add_all(
            [
                _human(
                    message_id=message_id,
                    session_id=session.id,
                    created_at=now + timedelta(microseconds=1),
                ),
                Message(
                    id=uuid4(),
                    session_id=session.id,
                    message_kind="assistant",
                    content=[
                        {
                            "type": "tool_use",
                            "id": "tool-1",
                            "name": "message",
                            "input": {"content": "hello"},
                        }
                    ],
                    attachment_refs=[],
                    delivery_refs=[],
                    is_compacted=False,
                    created_at=now + timedelta(microseconds=2),
                ),
                run,
            ]
        )
        await db.commit()

    await abandon_running_turns(pg_engine, runner_instance_id=uuid4())

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        rows = list(
            (
                await db.scalars(
                    select(Message)
                    .where(Message.session_id == session.id)
                    .order_by(Message.created_at, Message.id)
                )
            ).all()
        )

    assert [row.message_kind for row in rows[-2:]] == [
        "synthetic_tool_result",
        "synthetic_assistant_error",
    ]
