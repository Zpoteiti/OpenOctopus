import asyncio
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.automations.cron import (
    CronScheduler,
)
from openctopus_server.db.models import (
    CronJob,
    PendingMessage,
    Session,
    TurnRun,
    User,
)
from openctopus_server.dto.cron import CronCreateRequest
from openctopus_server.services import cron as cron_service

NOW = datetime(2026, 9, 1, 12, 5, 30, tzinfo=UTC)


class _Runtime:
    def __init__(self) -> None:
        self.runner_instance_id = uuid.uuid4()
        self.accepted = []
        self._locks: defaultdict[uuid.UUID, asyncio.Lock] = defaultdict(asyncio.Lock)

    @asynccontextmanager
    async def session_operation(self, session_id):
        async with self._locks[session_id]:
            yield

    async def schedule(self, accepted) -> None:
        self.accepted.append(accepted)


class _BlockingRuntime(_Runtime):
    def __init__(self) -> None:
        super().__init__()
        self.handoff_started = asyncio.Event()
        self.release_handoff = asyncio.Event()

    async def schedule(self, accepted) -> None:
        self.handoff_started.set()
        await self.release_handoff.wait()
        self.accepted.append(accepted)


async def _user(db: AsyncSession, *, email: str = "owner@example.com") -> User:
    user = User(
        id=uuid.uuid4(),
        email=email,
        password_hash="hash",
        name="Owner",
        timezone="UTC",
        is_admin=False,
        created_at=NOW - timedelta(days=1),
    )
    db.add(user)
    await db.commit()
    return user


async def _job(
    db: AsyncSession,
    user: User,
    *,
    name: str = "job",
    every_seconds: int | None = 60,
    at: str | None = None,
) -> CronJob:
    created = await cron_service.create_owned(
        db,
        user_id=user.id,
        request=CronCreateRequest(
            name=name,
            message=f"run {name}",
            every_seconds=every_seconds,
            at=at,
        ),
        now=NOW - timedelta(hours=1),
    )
    job = await db.get(CronJob, created.id)
    assert job is not None
    return job


async def test_startup_recovery_advances_recurring_and_drops_missed_once(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db)
        recurring = await _job(db, user, name="recurring")
        once = await _job(
            db,
            user,
            name="once",
            every_seconds=None,
            at=(NOW - timedelta(minutes=10)).isoformat(),
        )
        db.add(
            Session(
                id=once.id,
                user_id=user.id,
                session_key=f"cron:{once.id}",
                channel="cron",
                chat_id=str(once.id),
                title="Cron · once",
                created_at=NOW - timedelta(hours=1),
            )
        )
        await db.commit()

    runtime = _Runtime()
    scheduler = CronScheduler(pg_engine, runtime)
    recovered = sum([await scheduler._recover_job(job.id, snapshot=NOW) for job in (recurring, once)])

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        stored = await db.get(CronJob, recurring.id)
        assert stored is not None
        assert stored.next_fire_at > NOW
        assert stored.last_fired_at is None
        assert await db.get(CronJob, once.id) is None
        assert await db.get(Session, once.id) is not None
        assert await db.scalar(select(func.count()).select_from(PendingMessage)) == 0
    assert recovered == 2
    assert runtime.accepted == []


async def test_runtime_fire_accepts_only_closest_missed_boundary(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db)
        job = await _job(db, user)
        job.next_fire_at = NOW - timedelta(minutes=5, seconds=30)
        await db.commit()

    runtime = _Runtime()
    scheduler = CronScheduler(pg_engine, runtime)
    assert await scheduler._fire_job(job.id, user_id=user.id, now=NOW) == 1

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        stored = await db.get(CronJob, job.id)
        assert stored is not None
        assert stored.last_fired_at == NOW
        assert stored.next_fire_at == NOW + timedelta(seconds=30)
        session = await db.get(Session, job.id)
        assert session is not None
        assert session.session_key == f"cron:{job.id}"
        pending = await db.scalar(
            select(PendingMessage).where(PendingMessage.session_id == job.id)
        )
        assert pending is not None
        text = "\n".join(
            str(block.get("text", "")) for block in pending.content if isinstance(block, dict)
        )
        assert "2026-09-01T12:05:00Z" in text
        assert "run job" in text


async def test_busy_fire_skips_without_creating_chat_rows_or_last_fired(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db)
        job = await _job(db, user)
        job.next_fire_at = NOW - timedelta(minutes=1)
        db.add(
            Session(
                id=job.id,
                user_id=user.id,
                session_key=f"cron:{job.id}",
                channel="cron",
                chat_id=str(job.id),
                title="Cron · job",
                created_at=NOW - timedelta(hours=1),
            )
        )
        db.add(
            TurnRun(
                id=uuid.uuid4(),
                session_id=job.id,
                runner_instance_id=uuid.uuid4(),
                status="running",
                tool_profile="owner_full",
                started_at=NOW - timedelta(minutes=2),
            )
        )
        await db.commit()

    runtime = _Runtime()
    scheduler = CronScheduler(pg_engine, runtime)
    assert await scheduler._fire_job(job.id, user_id=user.id, now=NOW) == 1

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        stored = await db.get(CronJob, job.id)
        assert stored is not None
        assert stored.last_fired_at is None
        assert stored.next_fire_at > NOW
        assert await db.scalar(select(func.count()).select_from(PendingMessage)) == 0
    assert runtime.accepted == []


async def test_one_shot_acceptance_deletes_job_but_keeps_stable_session(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db)
        job = await _job(
            db,
            user,
            every_seconds=None,
            at=NOW.isoformat(),
        )

    runtime = _Runtime()
    scheduler = CronScheduler(pg_engine, runtime)
    assert await scheduler._fire_job(job.id, user_id=user.id, now=NOW) == 1

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        assert await db.get(CronJob, job.id) is None
        assert await db.get(Session, job.id) is not None
        assert await db.scalar(select(func.count()).select_from(PendingMessage)) == 1


async def test_two_schedulers_cannot_accept_the_same_due_job_twice(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db)
        job = await _job(db, user)
        job.next_fire_at = NOW
        await db.commit()

    runtime = _Runtime()
    first = CronScheduler(pg_engine, runtime)
    second = CronScheduler(pg_engine, runtime)
    await asyncio.gather(first._fire_job(job.id, user_id=user.id, now=NOW), second._fire_job(job.id, user_id=user.id, now=NOW))

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        assert (
            await db.scalar(
                select(func.count()).select_from(PendingMessage).where(
                    PendingMessage.session_id == job.id
                )
            )
            == 1
        )
        assert (
            await db.scalar(
                select(func.count()).select_from(TurnRun).where(
                    TurnRun.session_id == job.id,
                    TurnRun.status == "running",
                )
            )
            == 1
        )
