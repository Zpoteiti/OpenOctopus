from __future__ import annotations

import logging
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.automations.schedule import (
    advance_recurring,
    latest_due_occurrence,
    schedule_from_storage,
)
from openctopus_server.chat.types import AcceptedMessage
from openctopus_server.db.advisory import lock_uuid_identity
from openctopus_server.db.models import CronJob
from openctopus_server.services.inbound import cron_inbound, lock_inbound_identity
from openctopus_server.services.messages import (
    publish_inbound_locked,
)

_LOGGER = logging.getLogger(__name__)


class AutomationRuntime(Protocol):
    runner_instance_id: UUID

    def session_operation(
        self,
        session_id: UUID,
    ) -> AbstractAsyncContextManager[None]: ...

    async def schedule(self, accepted: AcceptedMessage) -> None: ...


class CronScheduler:
    """Cron acceptance rules invoked by durable timer workflows."""

    def __init__(
        self,
        engine: AsyncEngine,
        runtime: AutomationRuntime,
    ) -> None:
        self._engine = engine
        self._runtime = runtime

    async def _recover_job(self, job_id: UUID, *, snapshot: datetime, expected_fire_at: datetime | None = None) -> bool:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            try:
                await lock_uuid_identity(db, job_id)
                job = await db.scalar(
                    select(CronJob).where(CronJob.id == job_id).with_for_update()
                )
                if job is None or job.next_fire_at > snapshot or (expected_fire_at is not None and job.next_fire_at != expected_fire_at):
                    await db.rollback()
                    return False
                if job.schedule_kind == "at":
                    await db.delete(job)
                else:
                    schedule = schedule_from_storage(
                        kind=job.schedule_kind,
                        value=job.schedule_value,
                        timezone=job.timezone,
                        next_fire_at=job.next_fire_at,
                    )
                    job.next_fire_at = advance_recurring(
                        schedule,
                        scheduled_at=job.next_fire_at,
                        now=snapshot,
                    )
                if job.schedule_kind != "at":
                    from openctopus_server.automations.durable import enqueue_cron
                    await enqueue_cron(db, job)
                await db.commit()
                return True
            except BaseException:
                await db.rollback()
                raise

    async def _fire_job(
        self,
        job_id: UUID,
        *,
        user_id: UUID,
        now: datetime,
        expected_fire_at: datetime | None = None,
    ) -> bool:
        accepted: AcceptedMessage | None = None
        async with self._runtime.session_operation(job_id):
            async with AsyncSession(self._engine, expire_on_commit=False) as db:
                try:
                    identity_inbound = cron_inbound(
                        owner_user_id=user_id,
                        job_id=job_id,
                        content=[],
                    )
                    owner = await lock_inbound_identity(db, identity_inbound)
                    if owner is None:
                        await db.rollback()
                        return False
                    job = await db.scalar(
                        select(CronJob)
                        .where(CronJob.id == job_id, CronJob.user_id == user_id)
                        .with_for_update()
                    )
                    if job is None or job.next_fire_at > now or (expected_fire_at is not None and job.next_fire_at != expected_fire_at):
                        await db.rollback()
                        return False

                    schedule = schedule_from_storage(
                        kind=job.schedule_kind,
                        value=job.schedule_value,
                        timezone=job.timezone,
                        next_fire_at=job.next_fire_at,
                    )
                    scheduled_at = latest_due_occurrence(
                        schedule,
                        scheduled_at=job.next_fire_at,
                        now=now,
                    )
                    inbound = cron_inbound(
                        owner_user_id=user_id,
                        job_id=job_id,
                        content=_cron_content(job, scheduled_at=scheduled_at),
                    )
                    accepted = await publish_inbound_locked(
                        db,
                        inbound=inbound,
                        title=f"Cron · {job.name}",
                        runner_instance_id=self._runtime.runner_instance_id,
                        queue_if_busy=False,
                    )

                    if job.schedule_kind == "at":
                        await db.delete(job)
                    else:
                        job.next_fire_at = advance_recurring(
                            schedule,
                            scheduled_at=scheduled_at,
                            now=now,
                        )
                        if accepted is not None:
                            job.last_fired_at = now
                    if job.schedule_kind != "at":
                        from openctopus_server.automations.durable import enqueue_cron
                        await enqueue_cron(db, job)
                    await db.commit()
                except BaseException:
                    await db.rollback()
                    raise

            if accepted is None:
                _LOGGER.info(
                    "Cron fire skipped because its session is busy",
                    extra={"job_id": str(job_id), "user_id": str(user_id)},
                )
        return True







def _cron_content(job: CronJob, *, scheduled_at: datetime) -> list[dict[str, str]]:
    occurrence = scheduled_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return [
        {
            "type": "text",
            "text": (
                "[Server scheduled automation]\n"
                f"Cron job: {job.name}\n"
                f"Job ID: {job.id}\n"
                f"Scheduled occurrence (UTC): {occurrence}"
            ),
        },
        {"type": "text", "text": job.message},
    ]
