from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import cast as type_cast
from uuid import UUID

from sqlalchemy import Text, and_, cast, literal, select, union_all
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from openctopus_server.automations.schedule import (
    InvalidScheduleError,
    InvalidTimezoneError,
    ScheduleSpec,
    canonicalize_schedule,
    schedule_from_storage,
)
from openctopus_server.db.advisory import lock_uuid_identity
from openctopus_server.db.models import CronJob, Session, User
from openctopus_server.dto.cron import (
    AtScheduleResponse,
    CronCreateRequest,
    CronJobResponse,
    CronJobsResponse,
    CronJobSummary,
    CronPatchRequest,
    CronScheduleProjection,
    CronScheduleResponse,
    EveryScheduleResponse,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError

MAX_CRON_NAME_CHARS = 120
MAX_CRON_MESSAGE_CHARS = 32_000


async def create_owned(
    db: AsyncSession,
    *,
    user_id: UUID,
    request: CronCreateRequest,
    now: datetime | None = None,
) -> CronJobResponse:
    write_time = _now_utc(now)
    message = _message(request.message)
    name = _name(request.name, default=message[:30])
    try:
        job_id = uuid.uuid4()
        await lock_uuid_identity(db, job_id)
        user = await _user_for_identity_write(db, user_id=user_id)
        collision = await db.scalar(
            union_all(
                select(User.id).where(User.id == job_id),
                select(Session.id).where(Session.id == job_id),
                select(CronJob.id).where(CronJob.id == job_id),
            ).limit(1)
        )
        if collision is not None:
            raise RuntimeError("Generated cron identity is already reserved")
        schedule = _schedule(request, user_timezone=user.timezone, now=write_time)
        job = CronJob(
            id=job_id,
            user_id=user_id,
            name=name,
            schedule_kind=schedule.kind,
            schedule_value=schedule.value,
            timezone=schedule.timezone,
            message=message,
            last_fired_at=None,
            next_fire_at=schedule.next_fire_at,
            created_at=write_time,
        )
        db.add(job)
        await db.commit()
        return _job_response(job, session_id=None)
    except BaseException:
        await db.rollback()
        raise


async def get_owned(
    db: AsyncSession,
    *,
    user_id: UUID,
    job_id: UUID,
) -> CronJobResponse:
    row = (
        await db.execute(_owned_projection_query(user_id=user_id).where(CronJob.id == job_id))
    ).one_or_none()
    if row is None:
        raise _not_found()
    job, session_id = row
    return _job_response(job, session_id=session_id)


async def list_owned(
    db: AsyncSession,
    *,
    user_id: UUID,
    limit: int,
    offset: int,
) -> CronJobsResponse:
    rows = (
        await db.execute(
            _owned_projection_query(user_id=user_id)
            .order_by(CronJob.created_at.desc(), CronJob.id.desc())
            .offset(offset)
            .limit(limit + 1)
        )
    ).all()
    has_more = len(rows) > limit
    page = rows[:limit]
    return CronJobsResponse(
        items=[_job_summary(job, session_id=session_id) for job, session_id in page],
        next_offset=offset + len(page) if has_more else None,
    )


async def patch_owned(
    db: AsyncSession,
    *,
    user_id: UUID,
    job_id: UUID,
    request: CronPatchRequest,
    now: datetime | None = None,
) -> CronJobResponse:
    if not request.model_fields_set or any(
        getattr(request, field) is None for field in request.model_fields_set
    ):
        raise _invalid("Cron update must contain non-null fields")
    schedule_fields = {"every_seconds", "cron_expr", "at", "tz"}
    updates_schedule = bool(request.model_fields_set & schedule_fields)
    write_time = _now_utc(now)
    try:
        await lock_uuid_identity(db, job_id)
        user = await _user_for_identity_write(db, user_id=user_id)
        job = await db.scalar(
            select(CronJob)
            .where(CronJob.id == job_id, CronJob.user_id == user_id)
            .with_for_update()
        )
        if job is None:
            raise _not_found()

        schedule: ScheduleSpec | None = None
        if updates_schedule:
            schedule = _schedule(request, user_timezone=user.timezone, now=write_time)
        new_name = (
            _name(request.name, default=job.name)
            if "name" in request.model_fields_set
            else job.name
        )
        new_message = (
            _message(request.message)
            if "message" in request.model_fields_set
            else job.message
        )

        job.name = new_name
        job.message = new_message
        if schedule is not None:
            job.schedule_kind = schedule.kind
            job.schedule_value = schedule.value
            job.timezone = schedule.timezone
            job.next_fire_at = schedule.next_fire_at
        await db.commit()
        session_id = await _history_session_id(db, job)
        return _job_response(job, session_id=session_id)
    except BaseException:
        await db.rollback()
        raise


async def delete_owned(
    db: AsyncSession,
    *,
    user_id: UUID,
    job_id: UUID,
) -> None:
    try:
        await lock_uuid_identity(db, job_id)
        job = await db.scalar(
            select(CronJob)
            .where(CronJob.id == job_id, CronJob.user_id == user_id)
            .with_for_update()
        )
        if job is None:
            raise _not_found()
        await db.delete(job)
        await db.commit()
    except BaseException:
        await db.rollback()
        raise


def _owned_projection_query(*, user_id: UUID) -> Select[tuple[CronJob, UUID | None]]:
    query = (
        select(CronJob, Session.id)
        .outerjoin(
            Session,
            and_(
                Session.id == CronJob.id,
                Session.user_id == CronJob.user_id,
                Session.session_key == literal("cron:") + cast(CronJob.id, Text),
                Session.channel == "cron",
                Session.chat_id == cast(CronJob.id, Text),
            ),
        )
        .where(CronJob.user_id == user_id)
    )
    return type_cast(Select[tuple[CronJob, UUID | None]], query)


async def _history_session_id(db: AsyncSession, job: CronJob) -> UUID | None:
    value = await db.scalar(
        select(Session.id).where(
            Session.id == job.id,
            Session.user_id == job.user_id,
            Session.channel == "cron",
            Session.session_key == f"cron:{job.id}",
            Session.chat_id == str(job.id),
        )
    )
    return value if isinstance(value, UUID) else None


async def _user_for_identity_write(db: AsyncSession, *, user_id: UUID) -> User:
    user = await db.scalar(
        select(User)
        .where(User.id == user_id)
        .with_for_update(read=True, key_share=True)
    )
    if user is None:
        raise _not_found()
    return user


def _schedule(
    request: CronCreateRequest | CronPatchRequest,
    *,
    user_timezone: str,
    now: datetime,
) -> ScheduleSpec:
    try:
        return canonicalize_schedule(
            every_seconds=request.every_seconds,
            cron_expr=request.cron_expr,
            at=request.at,
            timezone=request.tz,
            user_timezone=user_timezone,
            now=now,
        )
    except InvalidTimezoneError as exc:
        raise OpenOctopusError(ErrorCode.TIMEZONE_INVALID, str(exc)) from exc
    except InvalidScheduleError as exc:
        raise _invalid(str(exc)) from exc


def _job_response(job: CronJob, *, session_id: UUID | None) -> CronJobResponse:
    summary = _job_summary(job, session_id=session_id)
    return CronJobResponse(**summary.model_dump(), message=job.message)


def _job_summary(job: CronJob, *, session_id: UUID | None) -> CronJobSummary:
    schedule = schedule_from_storage(
        kind=job.schedule_kind,
        value=job.schedule_value,
        timezone=job.timezone,
        next_fire_at=job.next_fire_at,
    )
    return CronJobSummary(
        id=job.id,
        name=job.name,
        schedule=_schedule_projection(schedule),
        session_id=session_id,
        last_fired_at=job.last_fired_at,
        next_fire_at=job.next_fire_at,
        created_at=job.created_at,
    )


def _schedule_projection(schedule: ScheduleSpec) -> CronScheduleProjection:
    if schedule.kind == "every":
        return EveryScheduleResponse(every_seconds=int(schedule.value))
    if schedule.kind == "cron":
        assert schedule.timezone is not None
        return CronScheduleResponse(cron_expr=schedule.value, tz=schedule.timezone)
    assert schedule.timezone is not None
    return AtScheduleResponse(
        at=datetime.fromisoformat(schedule.value.replace("Z", "+00:00")),
        tz=schedule.timezone,
    )


def _message(value: str | None) -> str:
    if not isinstance(value, str):
        raise _invalid("Cron message is required")
    trimmed = value.strip()
    if not 1 <= len(trimmed) <= MAX_CRON_MESSAGE_CHARS:
        raise _invalid("Cron message is invalid")
    return trimmed


def _name(value: str | None, *, default: str) -> str:
    trimmed = default if value is None else value.strip()
    if not 1 <= len(trimmed) <= MAX_CRON_NAME_CHARS:
        raise _invalid("Cron name is invalid")
    return trimmed


def _now_utc(value: datetime | None) -> datetime:
    current = value or datetime.now(UTC)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return current.astimezone(UTC)


def _invalid(message: str) -> OpenOctopusError:
    return OpenOctopusError(ErrorCode.CRON_INVALID_SCHEDULE, message)


def _not_found() -> OpenOctopusError:
    return OpenOctopusError(ErrorCode.CRON_JOB_NOT_FOUND, "Cron job not found")
