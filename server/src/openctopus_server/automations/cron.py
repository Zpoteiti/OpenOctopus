from __future__ import annotations

import asyncio
import logging
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.automations.schedule import (
    advance_recurring,
    latest_due_occurrence,
    schedule_from_storage,
)
from openctopus_server.chat.types import AcceptedMessage
from openctopus_server.db.advisory import lock_uuid_identity
from openctopus_server.db.models import CronJob, PendingMessage, Session
from openctopus_server.services.inbound import cron_inbound, lock_inbound_identity
from openctopus_server.services.messages import (
    publish_inbound_locked,
    reserve_pending_turn,
)

_LOGGER = logging.getLogger(__name__)
_SCAN_BATCH_SIZE = 100
_MAX_SLEEP_SECONDS = 60.0
_MAX_BACKOFF_SECONDS = 60.0


class AutomationRuntime(Protocol):
    runner_instance_id: UUID

    def session_operation(
        self,
        session_id: UUID,
    ) -> AbstractAsyncContextManager[None]: ...

    async def schedule(self, accepted: AcceptedMessage) -> None: ...


class CronScheduler:
    """Single-process database-backed Cron ticker."""

    def __init__(
        self,
        engine: AsyncEngine,
        runtime: AutomationRuntime,
        *,
        wake_event: asyncio.Event | None = None,
    ) -> None:
        self._engine = engine
        self._runtime = runtime
        self._wake = wake_event or asyncio.Event()
        self._stop_requested = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Cron scheduler is already started")
        self._stop_requested.clear()
        await self.recover_startup()
        await recover_automation_pending(self._engine, self._runtime)
        self._task = asyncio.create_task(self._run(), name="cron-scheduler")

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        self._stop_requested.set()
        self._wake.set()
        await await_future_cancellation_safe(task)
        self._task = None

    def wake(self) -> None:
        self._wake.set()

    async def recover_startup(self, *, now: datetime | None = None) -> int:
        snapshot = _now_utc(now)
        recovered = 0
        while True:
            async with AsyncSession(self._engine, expire_on_commit=False) as db:
                job_ids = list(
                    (
                        await db.execute(
                            select(CronJob.id)
                            .where(CronJob.next_fire_at <= snapshot)
                            .order_by(CronJob.next_fire_at, CronJob.id)
                            .limit(_SCAN_BATCH_SIZE)
                        )
                    ).scalars()
                )
            if not job_ids:
                return recovered
            for job_id in job_ids:
                if await self._recover_job(job_id, snapshot=snapshot):
                    recovered += 1
            await asyncio.sleep(0)

    async def scan_due(self, *, now: datetime | None = None) -> int:
        scan_time = _now_utc(now)
        processed = 0
        while True:
            async with AsyncSession(self._engine, expire_on_commit=False) as db:
                candidates = list(
                    (
                        await db.execute(
                            select(CronJob.id, CronJob.user_id)
                            .where(CronJob.next_fire_at <= scan_time)
                            .order_by(CronJob.next_fire_at, CronJob.id)
                            .limit(_SCAN_BATCH_SIZE)
                        )
                    ).all()
                )
            if not candidates:
                return processed
            for job_id, user_id in candidates:
                if await self._fire_job(job_id, user_id=user_id, now=scan_time):
                    processed += 1
            await asyncio.sleep(0)

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop_requested.is_set():
            self._wake.clear()
            try:
                await self.scan_due()
                next_fire = await self._next_fire_at()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception("Cron scheduler scan failed")
                await self._wait_for_stop(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_SECONDS)
                continue

            if self._stop_requested.is_set() or self._wake.is_set():
                continue
            delay = _MAX_SLEEP_SECONDS
            if next_fire is not None:
                delay = min(
                    delay,
                    max(0.0, (next_fire - datetime.now(UTC)).total_seconds()),
                )
            await self._wait_for_wake(delay)

    async def _recover_job(self, job_id: UUID, *, snapshot: datetime) -> bool:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            try:
                await lock_uuid_identity(db, job_id)
                job = await db.scalar(
                    select(CronJob).where(CronJob.id == job_id).with_for_update()
                )
                if job is None or job.next_fire_at > snapshot:
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
                    if job is None or job.next_fire_at > now:
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
                    await db.commit()
                except BaseException:
                    await db.rollback()
                    raise

            if accepted is None:
                _LOGGER.info(
                    "Cron fire skipped because its session is busy",
                    extra={"job_id": str(job_id), "user_id": str(user_id)},
                )
            else:
                handoff = asyncio.create_task(
                    self._runtime.schedule(accepted),
                    name=f"cron-handoff-{job_id}",
                )
                await await_future_cancellation_safe(handoff)
        return True

    async def _next_fire_at(self) -> datetime | None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            return await db.scalar(select(func.min(CronJob.next_fire_at)))

    async def _wait_for_wake(self, delay: float) -> None:
        if delay <= 0:
            return
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=delay)
        except TimeoutError:
            pass

    async def _wait_for_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop_requested.wait(), timeout=delay)
        except TimeoutError:
            pass


async def recover_automation_pending(
    engine: AsyncEngine,
    runtime: AutomationRuntime,
) -> int:
    """Reserve durable automation pending rows left by an earlier process."""
    async with AsyncSession(engine, expire_on_commit=False) as db:
        session_ids = list(
            (
                await db.execute(
                    select(Session.id)
                    .join(PendingMessage, PendingMessage.session_id == Session.id)
                    .where(Session.channel.in_(("cron", "heartbeat")))
                    .distinct()
                    .order_by(Session.id)
                )
            ).scalars()
        )

    recovered = 0
    for session_id in session_ids:
        async with runtime.session_operation(session_id):
            async with AsyncSession(engine, expire_on_commit=False) as db:
                turn = await reserve_pending_turn(
                    db,
                    session_id=session_id,
                    runner_instance_id=runtime.runner_instance_id,
                )
            if turn is None:
                continue
            accepted = AcceptedMessage(
                session_id=session_id,
                message_id=turn.message_ids[-1],
                accepted_at=datetime.now(UTC),
                disposition="started",
                created_session=False,
                turn=turn,
            )
            handoff = asyncio.create_task(
                runtime.schedule(accepted),
                name=f"automation-recovery-handoff-{session_id}",
            )
            await await_future_cancellation_safe(handoff)
            recovered += 1
    return recovered


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


def _now_utc(value: datetime | None) -> datetime:
    current = value or datetime.now(UTC)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return current.astimezone(UTC)
