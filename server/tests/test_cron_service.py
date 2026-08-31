import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import CronJob, Session, User
from openctopus_server.dto.cron import CronCreateRequest, CronPatchRequest
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError
from openctopus_server.services import cron as cron_service

NOW = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)


async def _user(db: AsyncSession, *, email: str, timezone: str = "UTC") -> User:
    user = User(
        id=uuid.uuid4(),
        email=email,
        password_hash="hash",
        name=email,
        timezone=timezone,
        is_admin=False,
        created_at=NOW,
    )
    db.add(user)
    await db.commit()
    return user


async def test_create_uses_shared_schedule_and_derives_default_name(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db, email="owner@example.com", timezone="Asia/Shanghai")
        created = await cron_service.create_owned(
            db,
            user_id=user.id,
            request=CronCreateRequest(
                message="  Prepare the weekday report for the team.  ",
                cron_expr=" 0  9 * * MON-FRI ",
            ),
            now=NOW,
        )

    assert created.name == "Prepare the weekday report for"
    assert created.message == "Prepare the weekday report for the team."
    assert created.schedule.type == "cron"
    assert created.schedule.cron_expr == "0 9 * * MON-FRI"
    assert created.schedule.tz == "Asia/Shanghai"
    assert created.session_id is None
    assert created.next_fire_at == datetime(2026, 9, 1, 1, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (CronCreateRequest(message=" ", every_seconds=60), ErrorCode.CRON_INVALID_SCHEDULE),
        (
            CronCreateRequest(message="ok", every_seconds=60, cron_expr="* * * * *"),
            ErrorCode.CRON_INVALID_SCHEDULE,
        ),
        (
            CronCreateRequest(message="ok", cron_expr="* * * * *", tz="Not/AZone"),
            ErrorCode.TIMEZONE_INVALID,
        ),
    ],
)
async def test_create_maps_validation_errors(pg_engine, payload, code) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db, email=f"{uuid.uuid4()}@example.com")
        with pytest.raises(OpenOctopusError) as raised:
            await cron_service.create_owned(
                db,
                user_id=user.id,
                request=payload,
                now=NOW,
            )
        count = await db.scalar(select(func.count()).select_from(CronJob))

    assert raised.value.code is code
    assert count == 0


async def test_get_and_delete_are_owner_scoped_and_delete_retains_session(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db, email="owner@example.com")
        other = await _user(db, email="other@example.com")
        owner_id = owner.id
        other_id = other.id
        created = await cron_service.create_owned(
            db,
            user_id=owner_id,
            request=CronCreateRequest(name="hourly", message="Do work", every_seconds=3600),
            now=NOW,
        )
        db.add(
            Session(
                id=created.id,
                user_id=owner_id,
                session_key=f"cron:{created.id}",
                channel="cron",
                chat_id=str(created.id),
                title="Cron · hourly",
                last_inbound_at=NOW,
                created_at=NOW,
            )
        )
        await db.commit()

        projected = await cron_service.get_owned(db, user_id=owner.id, job_id=created.id)
        assert projected.session_id == created.id

        for operation in (
            cron_service.get_owned(db, user_id=other_id, job_id=created.id),
            cron_service.delete_owned(db, user_id=other_id, job_id=created.id),
        ):
            with pytest.raises(OpenOctopusError) as raised:
                await operation
            assert raised.value.code is ErrorCode.CRON_JOB_NOT_FOUND

        await cron_service.delete_owned(db, user_id=owner_id, job_id=created.id)
        assert await db.get(CronJob, created.id) is None
        assert await db.get(Session, created.id) is not None


async def test_list_is_stable_paginated_and_summary_omits_message(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db, email="owner@example.com")
        created = []
        for index in range(3):
            created.append(
                await cron_service.create_owned(
                    db,
                    user_id=user.id,
                    request=CronCreateRequest(
                        name=f"job-{index}",
                        message="x" * 32_000,
                        every_seconds=60 + index,
                    ),
                    now=NOW + timedelta(seconds=index),
                )
            )

        first = await cron_service.list_owned(db, user_id=user.id, limit=2, offset=0)
        second = await cron_service.list_owned(
            db, user_id=user.id, limit=2, offset=first.next_offset or 0
        )

    assert [item.name for item in first.items] == ["job-2", "job-1"]
    assert first.next_offset == 2
    assert [item.name for item in second.items] == ["job-0"]
    assert second.next_offset is None
    assert "message" not in first.items[0].model_dump()


async def test_patch_requires_non_null_fields_and_complete_schedule_replacement(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db, email="owner@example.com", timezone="Asia/Shanghai")
        user_id = user.id
        created = await cron_service.create_owned(
            db,
            user_id=user_id,
            request=CronCreateRequest(name="old", message="old message", every_seconds=60),
            now=NOW,
        )

        updated = await cron_service.patch_owned(
            db,
            user_id=user_id,
            job_id=created.id,
            request=CronPatchRequest(name=" new ", message=" new message "),
            now=NOW + timedelta(minutes=1),
        )
        assert updated.name == "new"
        assert updated.message == "new message"
        assert updated.schedule.type == "every"
        assert updated.schedule.every_seconds == 60
        assert updated.next_fire_at == created.next_fire_at

        for patch in (
            CronPatchRequest(),
            CronPatchRequest(name=None),
            CronPatchRequest(tz="UTC"),
            CronPatchRequest(cron_expr="0 9 * * *", every_seconds=60),
        ):
            with pytest.raises(OpenOctopusError) as raised:
                await cron_service.patch_owned(
                    db,
                    user_id=user_id,
                    job_id=created.id,
                    request=patch,
                    now=NOW + timedelta(minutes=2),
                )
            assert raised.value.code is ErrorCode.CRON_INVALID_SCHEDULE

        stored = await db.get(CronJob, created.id)
        assert stored is not None
        assert stored.name == "new"
        assert stored.schedule_kind == "every"


async def test_patch_schedule_recomputes_next_fire_from_write_time(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _user(db, email="owner@example.com")
        created = await cron_service.create_owned(
            db,
            user_id=user.id,
            request=CronCreateRequest(message="old", every_seconds=60),
            now=NOW,
        )
        write_time = NOW + timedelta(hours=2)
        updated = await cron_service.patch_owned(
            db,
            user_id=user.id,
            job_id=created.id,
            request=CronPatchRequest(cron_expr="15 * * * *", tz="UTC"),
            now=write_time,
        )

    assert updated.schedule.type == "cron"
    assert updated.next_fire_at == datetime(2026, 9, 1, 2, 15, tzinfo=UTC)
