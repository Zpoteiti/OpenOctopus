"""DBOS timers and queues; product services keep task eligibility and publication."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal
from uuid import UUID, uuid5

from dbos import DBOS
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat import durable as durable_host
from openctopus_server.chat.durable import current_runtime
from openctopus_server.db.models import CronJob, User

if TYPE_CHECKING:
    from openctopus_server.automations.dream import DreamService
    from openctopus_server.automations.heartbeat import HeartbeatPulse

_heartbeat: HeartbeatPulse | None = None
_dream: DreamService | None = None


def bind_automations(heartbeat: HeartbeatPulse, dream: DreamService) -> None:
    global _heartbeat, _dream
    _heartbeat, _dream = heartbeat, dream


def cron_workflow_id(job: CronJob) -> str:
    return f"cron:{job.id}:{job.next_fire_at.isoformat()}"


async def enqueue_cron(db: AsyncSession, job: CronJob) -> None:
    await durable_host.enqueue_transaction(db, {
        "workflow_name": "oo.cron", "workflow_id": cron_workflow_id(job), "queue_name": "oo-cron",
        "queue_partition_key": str(job.id), "app_version": "oo-harness-1",
        "delay_seconds": max(0, (job.next_fire_at - datetime.now(UTC)).total_seconds()),
    }, job.id, job.user_id, job.next_fire_at)


async def cancel_cron(db: AsyncSession, job: CronJob) -> None:
    target = cron_workflow_id(job)
    await durable_host.enqueue_transaction(db, {
        "workflow_name": "oo.cancel_timer", "workflow_id": f"cancel:{target}",
        "queue_name": "oo-maintenance", "app_version": "oo-harness-1",
    }, target)


@DBOS.workflow(name="oo.cancel_timer")
async def cancel_timer(workflow_id: str) -> None:
    await DBOS.cancel_workflow_async(workflow_id)


@DBOS.workflow(name="oo.cron")
async def cron(job_id: UUID, owner: UUID, scheduled_at: datetime) -> None:
    await fire_cron(job_id, owner, scheduled_at)


@DBOS.step(name="oo.fire_cron", retries_allowed=True, max_attempts=3, interval_seconds=0.25)
async def fire_cron(job_id: UUID, owner: UUID, scheduled_at: datetime) -> None:
    from openctopus_server.automations.cron import CronScheduler
    runtime = current_runtime()
    service = CronScheduler(runtime.engine, runtime)
    now = datetime.now(UTC)
    if now > scheduled_at + timedelta(seconds=5):
        await service._recover_job(job_id, snapshot=now, expected_fire_at=scheduled_at)
    else:
        await service._fire_job(job_id, user_id=owner, now=now, expected_fire_at=scheduled_at)


@DBOS.step(name="oo.pulse_is_current")
async def pulse_is_current(scheduled_at: datetime) -> bool:
    return datetime.now(UTC) <= scheduled_at + timedelta(seconds=5)


@DBOS.step(name="oo.automation_users")
async def users(after: UUID | None) -> list[UUID]:
    async with AsyncSession(current_runtime().engine) as db:
        query = select(User.id).order_by(User.id).limit(100)
        if after is not None:
            query = query.where(User.id > after)
        return list((await db.scalars(query)).all())


@DBOS.workflow(name="oo.automation_pulse")
async def pulse(scheduled_at: datetime, kind: Literal["heartbeat", "dream"]) -> None:
    if not await pulse_is_current(scheduled_at):
        return
    after = None
    while page := await users(after):
        for owner in page:
            await DBOS.enqueue_workflow_with_options_async({
                "workflow_name": "oo.automation_user", "workflow_id": f"{kind}:{owner}:{scheduled_at.isoformat()}",
                "queue_name": f"oo-{kind}", "deduplication_id": str(owner),
                "duplication_policy": "return-existing", "app_version": "oo-harness-1",
            }, owner, scheduled_at, kind)
        after = page[-1]


@DBOS.workflow(name="oo.automation_user")
async def automation_user(owner: UUID, scheduled_at: datetime, kind: Literal["heartbeat", "dream"]) -> None:
    await process_user(owner, scheduled_at, kind)


@DBOS.step(name="oo.process_automation", retries_allowed=True, max_attempts=3, interval_seconds=0.25)
async def process_user(owner: UUID, scheduled_at: datetime, kind: Literal["heartbeat", "dream"]) -> None:
    if kind == "dream":
        assert _dream is not None
        await _dream.process_user(owner, now=scheduled_at, run_id=uuid5(owner, "dream:" + scheduled_at.isoformat()))
    else:
        from openctopus_server.automations.heartbeat import _HeartbeatUser
        assert _heartbeat is not None
        async with AsyncSession(current_runtime().engine) as db:
            user = await db.get(User, owner)
        if user is not None:
            await _heartbeat._process_user(_HeartbeatUser(user.id, user.created_at, user.timezone), now=scheduled_at)


async def start_schedules() -> None:
    await DBOS.register_queue_async("oo-heartbeat", worker_concurrency=8)
    await DBOS.register_queue_async("oo-dream", worker_concurrency=4)
    await DBOS.apply_schedules_async([
        {"schedule_name": "oo-heartbeat", "workflow_fn": pulse, "schedule": "0 */30 * * * *", "context": "heartbeat"},
        {"schedule_name": "oo-dream", "workflow_fn": pulse, "schedule": "0 * * * * *", "context": "dream"},
    ])
