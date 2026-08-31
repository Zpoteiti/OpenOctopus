import asyncio
import json
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.runner import ChatRuntime, DetachedSession
from openctopus_server.chat.stream import StreamSubscriber
from openctopus_server.chat.types import TurnStart
from openctopus_server.db.models import (
    CronJob,
    Message,
    PendingMessage,
    Session,
    SystemConfig,
    TurnRun,
    User,
)
from openctopus_server.services import messages, sessions


async def _user(db: AsyncSession, email: str = "user@test.com") -> User:
    return (await db.scalars(select(User).where(User.email == email))).one()


def _session(
    *,
    user_id: UUID,
    session_id: UUID | None = None,
    channel: str = "web",
    title: str = "New chat",
    last_inbound_at: datetime | None = None,
    last_read_at: datetime | None = None,
    created_at: datetime | None = None,
) -> Session:
    resolved_id = session_id or uuid4()
    return Session(
        id=resolved_id,
        user_id=user_id,
        session_key=f"{channel}:{resolved_id}",
        channel=channel,
        chat_id=str(resolved_id),
        title=title,
        last_inbound_at=last_inbound_at,
        last_read_at=last_read_at,
        created_at=created_at or datetime.now(UTC),
    )


def _message(
    session_id: UUID,
    *,
    created_at: datetime,
    kind: str = "assistant",
) -> Message:
    return Message(
        id=uuid4(),
        session_id=session_id,
        message_kind=kind,
        content=[{"type": "text", "text": kind}],
        delivery_refs=[],
        is_compacted=False,
        created_at=created_at,
    )


async def _configure_provider(db: AsyncSession) -> None:
    db.add_all(
        [
            SystemConfig(key="llm_endpoint", value="http://fake.test"),
            SystemConfig(key="llm_api_key", value="fake-key"),
            SystemConfig(key="llm_model", value="fake-model"),
        ]
    )
    await db.commit()


async def test_list_sessions_is_owned_sorted_paginated_and_derives_unread(
    user_client,
    pg_engine,
):
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        outsider = User(
            id=uuid4(),
            email="other@test.com",
            password_hash="unused",
            name="Other",
        )
        db.add(outsider)
        await db.flush()
        newest = _session(
            user_id=owner.id,
            title="newest",
            last_inbound_at=now,
            created_at=now - timedelta(days=3),
        )
        older = _session(
            user_id=owner.id,
            title="older",
            last_inbound_at=now - timedelta(hours=1),
            created_at=now - timedelta(days=2),
        )
        null_newer = _session(
            user_id=owner.id,
            title="null-newer",
            created_at=now - timedelta(days=1),
        )
        null_older = _session(
            user_id=owner.id,
            title="null-older",
            created_at=now - timedelta(days=4),
        )
        foreign = _session(
            user_id=outsider.id,
            title="foreign",
            last_inbound_at=now + timedelta(days=1),
        )
        db.add_all([newest, older, null_newer, null_older, foreign])
        await db.flush()
        db.add(_message(older.id, created_at=now, kind="compaction_summary"))
        await db.commit()

    response = await user_client.get("/api/sessions", params={"limit": 2, "offset": 1})

    assert response.status_code == 200
    body = response.json()
    assert [item["title"] for item in body] == ["older", "null-newer"]
    assert body[0]["unread"] is True
    assert body[1]["unread"] is False
    assert all(item["user_id"] == str(owner.id) for item in body)
    assert body[0]["cancel_requested"] is False


async def test_list_unread_ignores_pending_and_respects_read_timestamp(
    user_client,
    pg_engine,
):
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        read = _session(user_id=owner.id, title="read", last_read_at=now)
        pending_only = _session(user_id=owner.id, title="pending", last_read_at=now)
        db.add_all([read, pending_only])
        await db.flush()
        db.add(_message(read.id, created_at=now))
        db.add(
            PendingMessage(
                id=uuid4(),
                session_id=pending_only.id,
                user_id=owner.id,
                session_key=pending_only.session_key,
                content=[{"type": "text", "text": "pending"}],
                received_at=now + timedelta(seconds=1),
            )
        )
        await db.commit()

    response = await user_client.get("/api/sessions")

    assert response.status_code == 200
    assert {item["title"]: item["unread"] for item in response.json()} == {
        "pending": False,
        "read": False,
    }


async def test_patch_updates_non_web_title_and_monotonic_read_marker(
    user_client,
    pg_engine,
):
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        session = _session(
            user_id=owner.id,
            channel="telegram",
            title="old",
            last_read_at=now,
        )
        db.add(session)
        await db.flush()
        old_message = _message(session.id, created_at=now - timedelta(minutes=1))
        new_message = _message(session.id, created_at=now + timedelta(minutes=1))
        db.add_all([old_message, new_message])
        await db.commit()

    advanced = await user_client.patch(
        f"/api/sessions/{session.id}",
        json={"title": "renamed", "read_through_message_id": str(new_message.id)},
    )
    stale = await user_client.patch(
        f"/api/sessions/{session.id}",
        json={"read_through_message_id": str(old_message.id)},
    )

    assert advanced.status_code == 200
    assert advanced.json()["title"] == "renamed"
    assert advanced.json()["unread"] is False
    assert stale.status_code == 200
    async with AsyncSession(pg_engine) as db:
        stored = await db.get(Session, session.id)
        assert stored is not None
        assert stored.title == "renamed"
        assert stored.last_read_at == new_message.created_at


async def test_patch_invalid_marker_is_atomic_and_pending_does_not_qualify(
    user_client,
    pg_engine,
):
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        session = _session(user_id=owner.id, title="unchanged")
        other = _session(user_id=owner.id, title="other")
        db.add_all([session, other])
        await db.flush()
        other_message = _message(other.id, created_at=now)
        pending = PendingMessage(
            id=uuid4(),
            session_id=session.id,
            user_id=owner.id,
            session_key=session.session_key,
            content=[{"type": "text", "text": "pending"}],
            received_at=now,
        )
        db.add_all([other_message, pending])
        await db.commit()

    for marker in (other_message.id, pending.id):
        response = await user_client.patch(
            f"/api/sessions/{session.id}",
            json={"title": "must-not-commit", "read_through_message_id": str(marker)},
        )
        assert response.status_code == 400
        assert response.json()["code"] == "session_invalid_request"

    async with AsyncSession(pg_engine) as db:
        stored = await db.get(Session, session.id)
        assert stored is not None
        assert stored.title == "unchanged"
        assert stored.last_read_at is None


async def test_patch_validation_uses_session_error_contract(user_client):
    session_id = uuid4()
    invalid_payloads = [
        {},
        {"title": None},
        {"read_through_message_id": None},
        {"title": "x" * 121},
        {"title": "ok", "extra": True},
    ]

    for payload in invalid_payloads:
        response = await user_client.patch(f"/api/sessions/{session_id}", json=payload)
        assert response.status_code == 400
        assert response.json() == {
            "code": "session_invalid_request",
            "message": "Session request is invalid",
        }


async def test_delete_rejects_foreign_but_removes_cron_history_only(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
):
    now = datetime.now(UTC)
    runtime = ChatRuntime(pg_engine)
    test_app.state.chat_runtime = runtime
    stopped: list[UUID] = []

    async def record_stop(session_id: UUID) -> DetachedSession:
        stopped.append(session_id)
        return DetachedSession(session_id=session_id, subscribers=())

    monkeypatch.setattr(runtime, "detach_session", record_stop)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        outsider = User(
            id=uuid4(),
            email="foreign@test.com",
            password_hash="unused",
            name="Foreign",
        )
        db.add(outsider)
        await db.flush()
        foreign = _session(user_id=outsider.id)
        cron = _session(user_id=owner.id, channel="cron")
        db.add_all([foreign, cron])
        await db.flush()
        db.add(
            CronJob(
                id=cron.id,
                user_id=owner.id,
                name="active",
                schedule_kind="every",
                schedule_value="60",
                timezone=None,
                message="run",
                next_fire_at=now + timedelta(minutes=1),
            )
        )
        await db.commit()

    foreign_response = await user_client.delete(f"/api/sessions/{foreign.id}")
    cron_response = await user_client.delete(f"/api/sessions/{cron.id}")

    assert foreign_response.status_code == 404
    assert cron_response.status_code == 204
    assert stopped == [cron.id]
    async with AsyncSession(pg_engine) as db:
        assert await db.get(Session, cron.id) is None
        assert await db.get(CronJob, cron.id) is not None
    await runtime.close()


async def test_delete_cascades_completed_cron_session_history(
    user_client,
    pg_engine,
):
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        session = _session(user_id=owner.id, channel="cron")
        db.add(session)
        await db.flush()
        db.add_all(
            [
                _message(session.id, created_at=now),
                PendingMessage(
                    id=uuid4(),
                    session_id=session.id,
                    user_id=owner.id,
                    session_key=session.session_key,
                    content=[{"type": "text", "text": "pending"}],
                    received_at=now,
                ),
                TurnRun(
                    id=uuid4(),
                    session_id=session.id,
                    runner_instance_id=uuid4(),
                    status="completed",
                    started_at=now,
                    finished_at=now,
                ),
            ]
        )
        await db.commit()

    response = await user_client.delete(f"/api/sessions/{session.id}")

    assert response.status_code == 204
    async with AsyncSession(pg_engine) as db:
        assert await db.get(Session, session.id) is None
        assert await db.scalar(
            select(func.count()).select_from(Message).where(Message.session_id == session.id)
        ) == 0
        assert await db.scalar(
            select(func.count())
            .select_from(PendingMessage)
            .where(PendingMessage.session_id == session.id)
        ) == 0
        assert await db.scalar(
            select(func.count()).select_from(TurnRun).where(TurnRun.session_id == session.id)
        ) == 0


async def test_runtime_terminate_session_closes_all_streams_without_respawn(pg_engine):
    runtime = ChatRuntime(pg_engine)
    session_id = uuid4()
    turn_id = uuid4()
    started_message_id = uuid4()
    queued_message_id = uuid4()
    active = StreamSubscriber(
        message_id=started_message_id,
        accepted_at=datetime.now(UTC),
    )
    queued = StreamSubscriber(
        message_id=queued_message_id,
        accepted_at=datetime.now(UTC),
    )
    blocker = asyncio.Event()
    runner_started = asyncio.Event()

    async def blocked_runner() -> None:
        runner_started.set()
        await blocker.wait()

    runner_task = asyncio.create_task(blocked_runner())
    await runner_started.wait()
    async with runtime._lease_state(session_id) as state:
        assert state is not None
        async with state.lock:
            state.runner_task = runner_task
            state.starts.append(
                TurnStart(
                    session_id=session_id,
                    turn_id=uuid4(),
                    message_ids=(queued_message_id,),
                    effort=None,
                )
            )
            state.turn_subscribers[turn_id] = active
            state.queued_subscribers[queued_message_id] = queued

    await runtime.terminate_session(session_id)

    assert runner_task.cancelled()
    assert session_id not in runtime._states
    for subscriber in (active, queued):
        events = [json.loads(chunk) async for chunk in subscriber.ndjson()]
        assert events == [{"type": "session_deleted", "session_id": str(session_id)}]
    await asyncio.sleep(0)
    assert session_id not in runtime._states
    await runtime.close()


async def test_get_messages_holds_snapshot_lock_before_ownership_read(
    user_client,
    pg_engine,
    monkeypatch,
):
    session_id = uuid4()
    ownership_read = asyncio.Event()
    release_get = asyncio.Event()
    original_owned_session = messages._owned_session
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        db.add(_session(user_id=owner.id, session_id=session_id))
        await db.commit()

    async def owned_session_after_barrier(*args, **kwargs):
        session = await original_owned_session(*args, **kwargs)
        ownership_read.set()
        await release_get.wait()
        return session

    monkeypatch.setattr(messages, "_owned_session", owned_session_after_barrier)
    get_task = asyncio.create_task(
        user_client.get(f"/api/sessions/{session_id}/messages")
    )
    await asyncio.wait_for(ownership_read.wait(), timeout=2)
    delete_task = asyncio.create_task(user_client.delete(f"/api/sessions/{session_id}"))
    await asyncio.sleep(0.05)

    assert delete_task.done() is False
    release_get.set()
    get_response, delete_response = await asyncio.wait_for(
        asyncio.gather(get_task, delete_task),
        timeout=2,
    )
    assert get_response.status_code == 200
    assert delete_response.status_code == 204


async def test_failed_delete_abandons_interrupted_turn_and_allows_next_message(
    user_client,
    pg_engine,
    monkeypatch,
):
    del user_client
    session_id = uuid4()
    old_turn_id = uuid4()
    runtime = ChatRuntime(pg_engine)
    subscriber = StreamSubscriber(
        message_id=uuid4(),
        accepted_at=datetime.now(UTC),
    )
    runner_started = asyncio.Event()

    async def blocked_runner() -> None:
        runner_started.set()
        await asyncio.Event().wait()

    runner_task = asyncio.create_task(blocked_runner())
    await runner_started.wait()
    async with runtime._lease_state(session_id) as state:
        assert state is not None
        async with state.lock:
            state.runner_task = runner_task
            state.active_turn_id = old_turn_id
            state.turn_subscribers[old_turn_id] = subscriber
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _user(db)
        await _configure_provider(db)
        db.add(_session(user_id=owner.id, session_id=session_id))
        await db.flush()
        db.add(
            TurnRun(
                id=old_turn_id,
                session_id=session_id,
                runner_instance_id=runtime.runner_instance_id,
                status="running",
                started_at=datetime.now(UTC),
            )
        )
        await db.commit()

    async with AsyncSession(pg_engine, expire_on_commit=False) as failing_db:
        original_rollback = failing_db.rollback

        async def rollback_then_fail() -> None:
            await original_rollback()
            raise RuntimeError("rollback failed")

        monkeypatch.setattr(
            failing_db,
            "commit",
            AsyncMock(side_effect=RuntimeError("commit failed")),
        )
        monkeypatch.setattr(failing_db, "rollback", rollback_then_fail)
        with pytest.raises(RuntimeError, match="rollback failed"):
            await sessions.delete_owned(
                failing_db,
                user_id=owner.id,
                session_id=session_id,
                runtime=runtime,
            )

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        stored = await db.get(Session, session_id)
        old_turn = await db.get(TurnRun, old_turn_id)
        assert stored is not None
        assert old_turn is not None
        assert old_turn.status == "abandoned"
        accepted = await messages.accept_message(
            db,
            user=owner,
            session_id=session_id,
            content=[{"type": "text", "text": "continue"}],
            effort=None,
            runner_instance_id=runtime.runner_instance_id,
        )

    assert runner_task.cancelled()
    assert [json.loads(chunk) async for chunk in subscriber.ndjson()] == []
    assert accepted.disposition == "started"
    assert accepted.turn is not None
    assert accepted.turn.turn_id != old_turn_id
    await runtime.close()


async def test_post_acceptance_and_delete_are_linearized(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
):
    session_id = uuid4()
    accepted = asyncio.Event()
    release_accept = asyncio.Event()
    runtime = ChatRuntime(pg_engine)
    test_app.state.chat_runtime = runtime
    original_accept = messages.accept_message

    async with AsyncSession(pg_engine) as db:
        await _configure_provider(db)

    async def accept_after_barrier(*args, **kwargs):
        result = await original_accept(*args, **kwargs)
        accepted.set()
        await release_accept.wait()
        return result

    async def skip_activation(_accepted) -> None:
        return None

    monkeypatch.setattr(messages, "accept_message", accept_after_barrier)
    monkeypatch.setattr(runtime, "schedule", skip_activation)

    post_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "hello"}], "attachments": []},
        )
    )
    await asyncio.wait_for(accepted.wait(), timeout=2)
    delete_task = asyncio.create_task(user_client.delete(f"/api/sessions/{session_id}"))
    await asyncio.sleep(0.05)
    assert delete_task.done() is False

    release_accept.set()
    post_response, delete_response = await asyncio.wait_for(
        asyncio.gather(post_task, delete_task),
        timeout=2,
    )

    assert delete_response.status_code == 204
    assert [event["type"] for event in map(json.loads, post_response.text.splitlines())] == [
        "message_accepted",
        "session_deleted",
    ]
    async with AsyncSession(pg_engine) as db:
        assert await db.get(Session, session_id) is None
    await runtime.close()


async def test_cancelled_delete_finishes_and_releases_session_gate(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
):
    runtime = ChatRuntime(pg_engine)
    test_app.state.chat_runtime = runtime
    session_id = uuid4()
    stop_entered = asyncio.Event()
    release_stop = asyncio.Event()
    original_stop = runtime.detach_session

    async with AsyncSession(pg_engine) as db:
        owner = await _user(db)
        db.add(_session(user_id=owner.id, session_id=session_id))
        await db.commit()

    async def stop_after_barrier(candidate: UUID):
        stop_entered.set()
        await release_stop.wait()
        return await original_stop(candidate)

    monkeypatch.setattr(runtime, "detach_session", stop_after_barrier)
    delete_task = asyncio.create_task(user_client.delete(f"/api/sessions/{session_id}"))
    await asyncio.wait_for(stop_entered.wait(), timeout=2)
    delete_task.cancel()
    await asyncio.sleep(0)
    assert delete_task.done() is False
    release_stop.set()
    with suppress(asyncio.CancelledError):
        await asyncio.wait_for(delete_task, timeout=2)

    async with AsyncSession(pg_engine) as db:
        assert await db.get(Session, session_id) is None
    async with runtime.session_operation(session_id):
        pass
    await runtime.close()
