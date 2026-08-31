from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.runtime_context import parse_runtime_block
from openctopus_server.db.models import (
    CronJob,
    PendingMessage,
    Session,
    TurnRun,
    User,
)
from openctopus_server.services.inbound import (
    cron_inbound,
    heartbeat_inbound,
    lock_inbound_identity,
    web_inbound,
)
from openctopus_server.services.messages import publish_inbound_locked


async def _user(db: AsyncSession) -> User:
    return (await db.scalars(select(User).where(User.email == "user@test.com"))).one()


def test_inbound_route_constructors_are_fixed() -> None:
    user_id = uuid4()
    session_id = uuid4()

    web = web_inbound(
        owner_user_id=user_id,
        session_id=session_id,
        content=[{"type": "text", "text": "hello"}],
        attachment_refs=None,
        effort=None,
    )
    cron = cron_inbound(
        owner_user_id=user_id,
        job_id=session_id,
        content=[{"type": "text", "text": "run"}],
    )
    heartbeat = heartbeat_inbound(
        owner_user_id=user_id,
        content=[{"type": "text", "text": "check"}],
    )

    assert (web.session_key, web.channel, web.chat_id) == (
        f"web:{session_id}",
        "web",
        str(session_id),
    )
    assert (cron.session_key, cron.channel, cron.chat_id) == (
        f"cron:{session_id}",
        "cron",
        str(session_id),
    )
    assert (heartbeat.session_id, heartbeat.session_key, heartbeat.channel) == (
        user_id,
        f"heartbeat:{user_id}",
        "heartbeat",
    )


async def test_internal_publish_creates_stable_session_pending_and_turn(
    user_client,
    pg_engine,
) -> None:
    del user_client
    runner_instance_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        inbound = heartbeat_inbound(
            owner_user_id=owner.id,
            content=[{"type": "text", "text": "Check active tasks."}],
        )
        assert await lock_inbound_identity(db, inbound) is not None
        accepted = await publish_inbound_locked(
            db,
            inbound=inbound,
            title="Heartbeat",
            runner_instance_id=runner_instance_id,
            queue_if_busy=False,
        )
        await db.commit()

    assert accepted is not None
    assert accepted.disposition == "started"
    assert accepted.created_session is True
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        session = await db.get(Session, owner.id)
        pending = await db.get(PendingMessage, inbound.message_id)
        run = await db.scalar(
            select(TurnRun).where(TurnRun.session_id == owner.id)
        )

    assert session is not None
    assert session.session_key == f"heartbeat:{owner.id}"
    assert pending is not None
    runtime = parse_runtime_block(pending.content[0])
    assert runtime is not None
    assert runtime.channel == "heartbeat"
    assert runtime.chat_id == str(owner.id)
    assert run is not None
    assert run.runner_instance_id == runner_instance_id


async def test_non_queueing_publish_skips_busy_session_without_new_rows(
    user_client,
    pg_engine,
) -> None:
    del user_client
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        job_id = uuid4()
        session = Session(
            id=job_id,
            user_id=owner.id,
            session_key=f"cron:{job_id}",
            channel="cron",
            chat_id=str(job_id),
            title="Cron · test",
            created_at=datetime.now(UTC),
        )
        db.add(session)
        await db.flush()
        db.add(
            TurnRun(
                id=uuid4(),
                session_id=job_id,
                runner_instance_id=uuid4(),
                status="running",
                started_at=datetime.now(UTC),
            )
        )
        await db.commit()

    inbound = cron_inbound(
        owner_user_id=owner.id,
        job_id=job_id,
        content=[{"type": "text", "text": "must not queue"}],
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        assert await lock_inbound_identity(db, inbound) is not None
        accepted = await publish_inbound_locked(
            db,
            inbound=inbound,
            title="Cron · test",
            runner_instance_id=uuid4(),
            queue_if_busy=False,
        )
        await db.commit()
        pending = await db.get(PendingMessage, inbound.message_id)

    assert accepted is None
    assert pending is None


async def test_web_post_cannot_claim_reserved_heartbeat_or_cron_identity(
    user_client,
    pg_engine,
) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        job_id = uuid4()
        db.add(
            CronJob(
                id=job_id,
                user_id=owner.id,
                name="reserved",
                schedule_kind="every",
                schedule_value="60",
                timezone=None,
                message="run",
                next_fire_at=now + timedelta(minutes=1),
                created_at=now,
            )
        )
        await db.commit()

    for reserved_id in (owner.id, job_id):
        response = await user_client.post(
            f"/api/sessions/{reserved_id}/messages",
            json={
                "content": [{"type": "text", "text": "claim"}],
                "attachments": [],
            },
        )
        assert response.status_code == 404
        assert response.json()["code"] == "not_found"


async def test_existing_automation_session_is_read_only_for_human_post(
    user_client,
    pg_engine,
) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        db.add(
            Session(
                id=owner.id,
                user_id=owner.id,
                session_key=f"heartbeat:{owner.id}",
                channel="heartbeat",
                chat_id=str(owner.id),
                title="Heartbeat",
                created_at=datetime.now(UTC),
            )
        )
        await db.commit()

    response = await user_client.post(
        f"/api/sessions/{owner.id}/messages",
        json={
            "content": [{"type": "text", "text": "not allowed"}],
            "attachments": [],
        },
    )

    assert response.status_code == 404
    assert response.json()["code"] == "not_found"
