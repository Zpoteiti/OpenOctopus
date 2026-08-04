import asyncio
import json
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.public_projection import build_runtime_block
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.chat.types import AcceptedMessage, TurnStart
from openctopus_server.db.models import (
    Message,
    PendingMessage,
    Session,
    SystemConfig,
    TurnRun,
    User,
)
from openctopus_server.provider.anthropic import (
    DeltaCallback,
    ProviderInvocationError,
    ProviderResult,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.services.messages import (
    capture_pending_for_turn,
    drain_pending_and_create_turn,
    get_messages_response,
    promote_pending_for_turn,
)
from openctopus_server.services.turn_runs import abandon_running_turns


@dataclass(slots=True)
class FakeStep:
    content: list[dict[str, Any]]
    deltas: list[tuple[str, str]] = field(default_factory=list)
    started: asyncio.Event | None = None
    release: asyncio.Event | None = None
    error: ProviderInvocationError | None = None


class FakeProvider:
    def __init__(self, steps: list[FakeStep]) -> None:
        self.steps = deque(steps)
        self.calls: list[dict[str, Any]] = []

    async def stream_turn(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
        on_delta: DeltaCallback,
        tools: list[dict[str, Any]] | None = None,
    ) -> ProviderResult:
        step = self.steps.popleft()
        self.calls.append(
            {
                "config": config,
                "system": system,
                "messages": messages,
                "effort": effort,
                "tools": tools,
            }
        )
        if step.started is not None:
            step.started.set()
        for channel, text in step.deltas:
            await on_delta(channel, text)  # type: ignore[arg-type]
        if step.release is not None:
            await step.release.wait()
        if step.error is not None:
            raise step.error
        return ProviderResult(
            content=step.content,
            fingerprint=provider_fingerprint(config),
        )

    async def count_tokens(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
    ) -> int:
        del config, system, tools, effort, limiter
        return sum(len(json.dumps(message)) for message in messages)

    async def close(self) -> None:
        return None


async def _configure_provider(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
            ]
        )
        await db.commit()


def _install_fake_runtime(test_app, pg_engine, provider: FakeProvider) -> ChatRuntime:
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
    )
    test_app.state.chat_runtime = runtime
    return runtime


def _events(response) -> list[dict[str, Any]]:
    return [json.loads(line) for line in response.text.splitlines()]


async def _wait_for_pending(client, session_id: str, expected: int) -> dict[str, Any]:
    for _ in range(100):
        response = await client.get(f"/api/sessions/{session_id}/messages")
        if response.status_code == 200 and response.json()["pending_count"] == expected:
            return response.json()
        await asyncio.sleep(0.01)
    raise AssertionError(f"pending_count never reached {expected}")


async def test_get_empty_session_keeps_required_nullable_fields(
    user_client,
    pg_engine,
):
    session_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        db.add(
            Session(
                id=session_id,
                user_id=user.id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="New chat",
                created_at=now,
            )
        )
        await db.commit()

    response = await user_client.get(f"/api/sessions/{session_id}/messages")

    assert response.status_code == 200
    assert response.json()["active_turn_id"] is None
    assert response.json()["last_message_id"] is None


async def test_get_rejects_malformed_query_with_documented_error(user_client):
    session_id = uuid4()
    invalid_queries = [
        {"before": "not-a-uuid"},
        {"after": "not-a-uuid"},
        {"limit": "0"},
        {"limit": "201"},
    ]

    for params in invalid_queries:
        response = await user_client.get(
            f"/api/sessions/{session_id}/messages",
            params=params,
        )

        assert response.status_code == 400
        assert response.json() == {
            "code": "invalid_cursor",
            "message": "Message query parameters are invalid",
        }


async def test_post_streams_and_get_recovers_canonical_history(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    provider = FakeProvider(
        [
            FakeStep(
                deltas=[("thinking", "reason"), ("text", "hello")],
                content=[
                    {
                        "type": "thinking",
                        "thinking": "reason",
                        "signature": "secret-signature",
                    },
                    {"type": "text", "text": "hello"},
                ],
            )
        ]
    )
    runtime = _install_fake_runtime(test_app, pg_engine, provider)
    session_id = "1bd4a7e8-4010-4dc3-b2e4-021a8fac60a7"

    response = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={
            "effort": "high",
            "content": [{"type": "text", "text": "Hi"}],
            "attachments": [],
        },
    )

    assert response.status_code == 200
    events = _events(response)
    assert [event["type"] for event in events] == [
        "message_accepted",
        "turn_started",
        "token_delta",
        "token_delta",
        "message_persisted",
        "turn_finished",
    ]
    assert [event["channel"] for event in events if event["type"] == "token_delta"] == [
        "thinking",
        "text",
    ]
    assert events[-1]["status"] == "completed"

    history = await user_client.get(f"/api/sessions/{session_id}/messages")
    assert history.status_code == 200
    body = history.json()
    assert body["status"] == "idle"
    assert body["pending_count"] == 0
    assert [message["message_kind"] for message in body["messages"]] == [
        "human",
        "assistant",
    ]
    assert body["messages"][0]["content"] == [{"type": "text", "text": "Hi"}]
    assert body["messages"][1]["content"][0] == {
        "type": "thinking",
        "thinking": "reason",
    }
    assert "<runtime>" not in json.dumps(body)

    call = provider.calls[0]
    assert call["config"].max_output_tokens == 16384
    assert call["effort"] == Effort.HIGH
    assert call["messages"][0]["content"][0]["text"].startswith("<runtime>\n")
    assert call["messages"][0]["content"][1] == {"type": "text", "text": "Hi"}
    await runtime.close()


async def test_same_session_overlap_is_durable_and_drains(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    started = asyncio.Event()
    release = asyncio.Event()
    provider = FakeProvider(
        [
            FakeStep(
                started=started,
                release=release,
                content=[{"type": "text", "text": "first answer"}],
            ),
            FakeStep(content=[{"type": "text", "text": "second answer"}]),
        ]
    )
    runtime = _install_fake_runtime(test_app, pg_engine, provider)
    session_id = "2bd4a7e8-4010-4dc3-b2e4-021a8fac60a7"

    first_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={
                "content": [{"type": "text", "text": "first"}],
                "attachments": [],
            },
        )
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    second_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={
                "content": [{"type": "text", "text": "second"}],
                "attachments": [],
            },
        )
    )

    pending = await _wait_for_pending(user_client, session_id, 1)
    pending_id = pending["pending_messages"][0]["id"]
    assert pending["pending_messages"][0]["content"] == [{"type": "text", "text": "second"}]
    assert pending["status"] == "running"

    release.set()
    first_response, second_response = await asyncio.gather(
        first_task,
        second_task,
    )
    assert first_response.status_code == 200
    assert second_response.status_code == 200
    second_events = _events(second_response)
    assert second_events[0]["disposition"] == "queued"
    assert second_events[1]["type"] == "turn_started"
    assert second_events[1]["message_ids"] == [pending_id]

    history = (await user_client.get(f"/api/sessions/{session_id}/messages")).json()
    assert [message["content"][-1]["text"] for message in history["messages"]] == [
        "first",
        "first answer",
        "second",
        "second answer",
    ]
    assert history["pending_count"] == 0
    await runtime.close()


async def test_get_snapshot_keeps_message_visible_during_pending_promotion(
    user_client,
    pg_engine,
    monkeypatch,
):
    session_id = uuid4()
    pending_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        session = Session(
            id=session_id,
            user_id=user.id,
            session_key=f"web:{session_id}",
            channel="web",
            chat_id=str(session_id),
            title="New chat",
            created_at=now,
        )
        db.add(session)
        await db.flush()
        db.add(
            Message(
                id=uuid4(),
                session_id=session_id,
                message_kind="human",
                content=[{"type": "text", "text": "first"}],
                delivery_refs=[],
                created_at=now,
            )
        )
        db.add(
            PendingMessage(
                id=pending_id,
                session_id=session_id,
                user_id=user.id,
                session_key=session.session_key,
                content=[{"type": "text", "text": "second"}],
                received_at=now + timedelta(microseconds=1),
            )
        )
        await db.commit()

    canonical_read = asyncio.Event()
    continue_get = asyncio.Event()

    async def promote_pending():
        await canonical_read.wait()
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            return await drain_pending_and_create_turn(
                db,
                session_id=session_id,
                runner_instance_id=uuid4(),
            )

    promotion_task = asyncio.create_task(promote_pending())
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        original_execute = db.execute
        canonical_paused = False

        async def execute_with_barrier(statement, *args, **kwargs):
            nonlocal canonical_paused
            result = await original_execute(statement, *args, **kwargs)
            descriptions = getattr(statement, "column_descriptions", ())
            if not canonical_paused and descriptions and descriptions[0].get("expr") is Message:
                canonical_paused = True
                canonical_read.set()
                await continue_get.wait()
            return result

        monkeypatch.setattr(db, "execute", execute_with_barrier)
        get_task = asyncio.create_task(
            get_messages_response(
                db,
                user_id=user.id,
                session_id=session_id,
                before=None,
                after=None,
                limit=50,
            )
        )
        await canonical_read.wait()
        try:
            await asyncio.wait_for(asyncio.shield(promotion_task), timeout=0.2)
        except TimeoutError:
            pass
        continue_get.set()
        response = await get_task
        await db.rollback()

    turn = await asyncio.wait_for(promotion_task, timeout=2)
    assert turn is not None
    visible_ids = [message.id for message in response.messages]
    visible_ids.extend(message.id for message in response.pending_messages)
    assert visible_ids.count(pending_id) == 1


async def test_pending_promotion_consumes_only_the_turns_captured_prefix(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    session_id = uuid4()
    turn_id = uuid4()
    first_id = uuid4()
    second_id = uuid4()
    late_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        session = Session(
            id=session_id,
            user_id=user.id,
            session_key=f"web:{session_id}",
            channel="web",
            chat_id=str(session_id),
            title="New chat",
            created_at=now,
        )
        db.add(session)
        await db.flush()
        db.add_all(
            [
                TurnRun(
                    id=turn_id,
                    session_id=session_id,
                    runner_instance_id=uuid4(),
                    status="running",
                    started_at=now,
                ),
                PendingMessage(
                    id=first_id,
                    session_id=session_id,
                    user_id=user.id,
                    session_key=session.session_key,
                    content=[{"type": "text", "text": "captured"}],
                    effort="low",
                    received_at=now,
                ),
                PendingMessage(
                    id=second_id,
                    session_id=session_id,
                    user_id=user.id,
                    session_key=session.session_key,
                    content=[{"type": "text", "text": "also captured"}],
                    effort="medium",
                    received_at=now + timedelta(microseconds=1),
                ),
            ]
        )
        await db.commit()
        turn = TurnStart(
            session_id=session_id,
            turn_id=turn_id,
            message_ids=(),
            effort=None,
        )

        captured = await capture_pending_for_turn(db, turn=turn)
        db.add(
            PendingMessage(
                id=late_id,
                session_id=session_id,
                user_id=user.id,
                session_key=session.session_key,
                content=[{"type": "text", "text": "late"}],
                effort="high",
                received_at=now + timedelta(microseconds=2),
            )
        )
        await db.commit()
        captured_again = await capture_pending_for_turn(db, turn=captured)
        promoted = await promote_pending_for_turn(db, turn=captured)
        promoted_again = await promote_pending_for_turn(db, turn=captured)
        canonical_ids = tuple(
            (
                await db.execute(
                    select(Message.id)
                    .where(Message.session_id == session_id)
                    .order_by(Message.created_at, Message.id)
                )
            )
            .scalars()
            .all()
        )
        pending_ids = tuple(
            (
                await db.execute(
                    select(PendingMessage.id)
                    .where(PendingMessage.session_id == session_id)
                    .order_by(PendingMessage.received_at, PendingMessage.id)
                )
            )
            .scalars()
            .all()
        )

    assert captured.message_ids == (first_id, second_id)
    assert captured.effort == Effort.MEDIUM
    assert captured_again == promoted == promoted_again == captured
    assert canonical_ids == (first_id, second_id)
    assert pending_ids == (late_id,)


async def test_pending_promotion_with_an_empty_capture_leaves_the_queue_untouched(
    user_client,
    pg_engine,
) -> None:
    del user_client
    now = datetime.now(UTC)
    session_id = uuid4()
    turn_id = uuid4()
    pending_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        session = Session(
            id=session_id,
            user_id=user.id,
            session_key=f"web:{session_id}",
            channel="web",
            chat_id=str(session_id),
            title="New chat",
            created_at=now,
        )
        db.add(session)
        await db.flush()
        db.add_all(
            [
                TurnRun(
                    id=turn_id,
                    session_id=session_id,
                    runner_instance_id=uuid4(),
                    status="running",
                    started_at=now,
                ),
                PendingMessage(
                    id=pending_id,
                    session_id=session_id,
                    user_id=user.id,
                    session_key=session.session_key,
                    content=[{"type": "text", "text": "late"}],
                    received_at=now,
                ),
            ]
        )
        await db.commit()
        turn = TurnStart(
            session_id=session_id,
            turn_id=turn_id,
            message_ids=(),
            effort=None,
        )

        unchanged = await promote_pending_for_turn(db, turn=turn)
        canonical_count = len(
            (await db.execute(select(Message.id).where(Message.session_id == session_id)))
            .scalars()
            .all()
        )
        pending_ids = tuple(
            (
                await db.execute(
                    select(PendingMessage.id).where(PendingMessage.session_id == session_id)
                )
            )
            .scalars()
            .all()
        )

    assert unchanged == turn
    assert canonical_count == 0
    assert pending_ids == (pending_id,)


async def test_latest_queued_post_replaces_older_stream(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    started = asyncio.Event()
    release = asyncio.Event()
    provider = FakeProvider(
        [
            FakeStep(
                started=started,
                release=release,
                content=[{"type": "text", "text": "one"}],
            ),
            FakeStep(content=[{"type": "text", "text": "batch"}]),
        ]
    )
    runtime = _install_fake_runtime(test_app, pg_engine, provider)
    session_id = "3bd4a7e8-4010-4dc3-b2e4-021a8fac60a7"

    first_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "one"}], "attachments": []},
        )
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    second_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "two"}], "attachments": []},
        )
    )
    await _wait_for_pending(user_client, session_id, 1)
    third_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={
                "content": [{"type": "text", "text": "three"}],
                "attachments": [],
            },
        )
    )
    await _wait_for_pending(user_client, session_id, 2)

    await asyncio.sleep(0)
    assert second_task.done() is False

    release.set()
    first_response, second_response, third_response = await asyncio.gather(
        first_task,
        second_task,
        third_task,
    )
    assert first_response.status_code == 200
    second_events = _events(second_response)
    assert [event["type"] for event in second_events] == [
        "message_accepted",
        "stream_replaced",
    ]
    third_events = _events(third_response)
    assert third_events[0]["disposition"] == "queued"
    assert len(third_events[1]["message_ids"]) == 2
    await runtime.close()


async def test_late_registration_does_not_replace_unrelated_running_preview(
    pg_engine,
    monkeypatch,
):
    runtime = ChatRuntime(pg_engine)
    session_id = uuid4()
    turn_id = uuid4()
    active_message_id = uuid4()
    late_message_id = uuid4()
    accepted_at = datetime.now(UTC)
    location = "pending"

    async def queued_location(
        accepted: AcceptedMessage,
    ) -> tuple[str, Any]:
        return location, turn_id if location == "running" else None

    monkeypatch.setattr(runtime, "_queued_location", queued_location)
    active = await runtime.register(
        AcceptedMessage(
            session_id=session_id,
            message_id=active_message_id,
            accepted_at=accepted_at,
            disposition="queued",
            created_session=False,
            turn=None,
        )
    )
    state = await runtime._state_for(session_id)
    await runtime._assign_queued_subscribers(
        state,
        TurnStart(
            session_id=session_id,
            turn_id=turn_id,
            message_ids=(active_message_id,),
            effort=None,
        ),
    )
    location = "running"
    late = await runtime.register(
        AcceptedMessage(
            session_id=session_id,
            message_id=late_message_id,
            accepted_at=accepted_at + timedelta(microseconds=1),
            disposition="queued",
            created_session=False,
            turn=None,
        )
    )

    assert active.closed is False
    assert late.closed is True
    late_events = [json.loads(chunk) async for chunk in late.ndjson()]
    assert [event["type"] for event in late_events] == ["message_accepted"]
    await runtime.unregister(session_id=session_id, subscriber=active)
    await runtime.close()


async def test_late_registration_joins_its_matching_running_continuation(
    pg_engine,
    monkeypatch,
):
    runtime = ChatRuntime(pg_engine)
    session_id = uuid4()
    initial_turn_id = uuid4()
    continuation_turn_id = uuid4()
    message_id = uuid4()

    async def running_location(
        accepted: AcceptedMessage,
    ) -> tuple[str, Any]:
        return "running", continuation_turn_id

    monkeypatch.setattr(runtime, "_queued_location", running_location)
    state = await runtime._state_for(session_id)
    await runtime._assign_queued_subscribers(
        state,
        TurnStart(
            session_id=session_id,
            turn_id=initial_turn_id,
            message_ids=(message_id,),
            effort=None,
        ),
    )
    await runtime._transfer_turn_subscriber(
        state,
        initial_turn_id,
        TurnStart(
            session_id=session_id,
            turn_id=continuation_turn_id,
            message_ids=(),
            effort=None,
        ),
    )

    subscriber = await runtime.register(
        AcceptedMessage(
            session_id=session_id,
            message_id=message_id,
            accepted_at=datetime.now(UTC),
            disposition="queued",
            created_session=False,
            turn=None,
        )
    )

    assert subscriber.closed is False
    assert state.turn_subscribers[continuation_turn_id] is subscriber
    await runtime.unregister(session_id=session_id, subscriber=subscriber)
    await runtime.close()


async def test_idle_assignment_keeps_post_boundary_subscriber_queued(
    pg_engine,
    monkeypatch,
):
    runtime = ChatRuntime(pg_engine)
    session_id = uuid4()
    turn_id = uuid4()
    captured_id = uuid4()
    later_id = uuid4()
    accepted_at = datetime.now(UTC)

    async def pending_location(accepted: AcceptedMessage) -> tuple[str, Any]:
        return "pending", None

    monkeypatch.setattr(runtime, "_queued_location", pending_location)
    captured = await runtime.register(
        AcceptedMessage(
            session_id=session_id,
            message_id=captured_id,
            accepted_at=accepted_at,
            disposition="queued",
            created_session=False,
            turn=None,
        )
    )
    later = await runtime.register(
        AcceptedMessage(
            session_id=session_id,
            message_id=later_id,
            accepted_at=accepted_at + timedelta(microseconds=1),
            disposition="queued",
            created_session=False,
            turn=None,
        )
    )

    state = await runtime._state_for(session_id)
    await runtime._assign_queued_subscribers(
        state,
        TurnStart(
            session_id=session_id,
            turn_id=turn_id,
            message_ids=(captured_id,),
            effort=None,
        ),
    )

    assert state.turn_subscribers[turn_id] is captured
    assert state.queued_subscribers[later_id] is later
    assert captured.closed is False
    assert later.closed is False
    await runtime.unregister(session_id=session_id, subscriber=captured)
    await runtime.unregister(session_id=session_id, subscriber=later)
    await runtime.close()


async def test_delayed_initial_registration_keeps_post_boundary_subscriber_queued(
    pg_engine,
    monkeypatch,
):
    runtime = ChatRuntime(pg_engine)
    session_id = uuid4()
    turn_id = uuid4()
    older_id = uuid4()
    newer_id = uuid4()
    accepted_at = datetime.now(UTC)

    async def pending_location(accepted: AcceptedMessage) -> tuple[str, Any]:
        return "pending", None

    async def turn_is_running(candidate: Any) -> bool:
        return candidate == turn_id

    monkeypatch.setattr(runtime, "_queued_location", pending_location)
    monkeypatch.setattr(runtime, "_turn_is_running", turn_is_running)
    newer = await runtime.register(
        AcceptedMessage(
            session_id=session_id,
            message_id=newer_id,
            accepted_at=accepted_at + timedelta(microseconds=1),
            disposition="queued",
            created_session=False,
            turn=None,
        )
    )
    older = await runtime.register(
        AcceptedMessage(
            session_id=session_id,
            message_id=older_id,
            accepted_at=accepted_at,
            disposition="started",
            created_session=False,
            turn=TurnStart(
                session_id=session_id,
                turn_id=turn_id,
                message_ids=(older_id,),
                effort=None,
            ),
        )
    )

    state = await runtime._state_for(session_id)
    assert state.turn_subscribers[turn_id] is older
    assert state.queued_subscribers[newer_id] is newer
    assert older.closed is False
    assert newer.closed is False
    await runtime.unregister(session_id=session_id, subscriber=older)
    await runtime.unregister(session_id=session_id, subscriber=newer)
    await runtime.close()


async def test_failure_after_delta_persists_only_synthetic_error(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    provider = FakeProvider(
        [
            FakeStep(
                deltas=[("text", "partial")],
                content=[],
                error=ProviderInvocationError("stream failed"),
            )
        ]
    )
    runtime = _install_fake_runtime(test_app, pg_engine, provider)
    session_id = "4bd4a7e8-4010-4dc3-b2e4-021a8fac60a7"

    response = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [{"type": "text", "text": "hello"}], "attachments": []},
    )
    events = _events(response)
    assert [event["type"] for event in events] == [
        "message_accepted",
        "turn_started",
        "token_delta",
        "message_persisted",
        "turn_finished",
    ]
    assert events[-1]["status"] == "failed"

    history = (await user_client.get(f"/api/sessions/{session_id}/messages")).json()
    assert history["status"] == "failed"
    assert history["messages"][-1]["message_kind"] == "synthetic_assistant_error"
    assert "partial" not in json.dumps(history["messages"][-1])
    await runtime.close()


async def test_invalid_attachment_and_unconfigured_provider_fail_before_persistence(
    user_client,
):
    session_id = "5bd4a7e8-4010-4dc3-b2e4-021a8fac60a7"
    invalid = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={
            "content": [{"type": "text", "text": "hello"}],
            "attachments": [{"path": "file.txt"}],
        },
    )
    assert invalid.status_code == 400
    assert invalid.json()["code"] == "invalid_message_content"

    unconfigured = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [{"type": "text", "text": "hello"}], "attachments": []},
    )
    assert unconfigured.status_code == 503
    assert unconfigured.json()["code"] == "provider_not_configured"
    missing = await user_client.get(f"/api/sessions/{session_id}/messages")
    assert missing.status_code == 404


async def test_direct_inline_image_is_persisted_and_sent_to_provider(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    provider = FakeProvider([FakeStep(content=[{"type": "text", "text": "image received"}])])
    runtime = _install_fake_runtime(test_app, pg_engine, provider)
    session_id = "7bd4a7e8-4010-4dc3-b2e4-021a8fac60a7"
    image = {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": "image/png",
            "data": "aQ==",
        },
    }

    response = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [image], "attachments": []},
    )
    assert response.status_code == 200
    history = (await user_client.get(f"/api/sessions/{session_id}/messages")).json()
    assert history["messages"][0]["content"] == [image]
    assert provider.calls[0]["messages"][0]["content"][1] == image
    await runtime.close()


async def test_disconnected_post_does_not_cancel_runner(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    started = asyncio.Event()
    release = asyncio.Event()
    provider = FakeProvider(
        [
            FakeStep(
                started=started,
                release=release,
                content=[{"type": "text", "text": "finished"}],
            )
        ]
    )
    runtime = _install_fake_runtime(test_app, pg_engine, provider)
    session_id = "6bd4a7e8-4010-4dc3-b2e4-021a8fac60a7"
    post_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "hello"}], "attachments": []},
        )
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    post_task.cancel()
    with suppress(asyncio.CancelledError):
        await post_task
    release.set()

    for _ in range(100):
        history = await user_client.get(f"/api/sessions/{session_id}/messages")
        if history.status_code == 200:
            body = history.json()
            if body["status"] == "idle" and body["messages"][-1]["content"] == [
                {"type": "text", "text": "finished"}
            ]:
                break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("detached runner did not complete")
    assert history.json()["messages"][-1]["content"] == [{"type": "text", "text": "finished"}]
    await runtime.close()


async def test_cancel_before_registration_still_starts_and_drains_runner(
    user_client,
    test_app,
    pg_engine,
    monkeypatch,
):
    await _configure_provider(pg_engine)
    started = asyncio.Event()
    release = asyncio.Event()
    provider = FakeProvider(
        [
            FakeStep(
                started=started,
                release=release,
                content=[{"type": "text", "text": "first answer"}],
            ),
            FakeStep(content=[{"type": "text", "text": "second answer"}]),
        ]
    )
    runtime = _install_fake_runtime(test_app, pg_engine, provider)
    original_register = runtime.register
    register_entered = asyncio.Event()
    block_first_registration = True

    async def register_after_barrier(accepted):
        nonlocal block_first_registration
        if block_first_registration:
            block_first_registration = False
            register_entered.set()
            await asyncio.Event().wait()
        return await original_register(accepted)

    monkeypatch.setattr(runtime, "register", register_after_barrier)
    session_id = "8bd4a7e8-4010-4dc3-b2e4-021a8fac60a7"
    first_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "first"}], "attachments": []},
        )
    )
    await asyncio.wait_for(register_entered.wait(), timeout=2)
    first_task.cancel()
    with suppress(asyncio.CancelledError):
        await first_task

    await asyncio.wait_for(started.wait(), timeout=2)
    second_task = asyncio.create_task(
        user_client.post(
            f"/api/sessions/{session_id}/messages",
            json={"content": [{"type": "text", "text": "second"}], "attachments": []},
        )
    )
    await _wait_for_pending(user_client, session_id, 1)
    release.set()
    second_response = await asyncio.wait_for(second_task, timeout=2)

    assert second_response.status_code == 200
    history = (await user_client.get(f"/api/sessions/{session_id}/messages")).json()
    assert [message["content"][-1]["text"] for message in history["messages"]] == [
        "first",
        "first answer",
        "second",
        "second answer",
    ]
    assert len(provider.calls) == 2
    assert history["status"] == "idle"
    await runtime.close()


async def test_abandoned_run_recovery_drains_old_pending_before_new_input(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    provider = FakeProvider([FakeStep(content=[{"type": "text", "text": "recovered"}])])
    runtime = _install_fake_runtime(test_app, pg_engine, provider)
    session_id = uuid4()
    pending_id = uuid4()
    old_turn_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        session = Session(
            id=session_id,
            user_id=user.id,
            session_key=f"web:{session_id}",
            channel="web",
            chat_id=str(session_id),
            title="New chat",
            last_inbound_at=now - timedelta(seconds=1),
            created_at=now - timedelta(seconds=3),
        )
        db.add(session)
        await db.flush()
        db.add(
            Message(
                id=uuid4(),
                session_id=session_id,
                message_kind="human",
                content=[
                    build_runtime_block(
                        timestamp=(now - timedelta(seconds=2)).isoformat(),
                        session=session,
                        user_id=user.id,
                    ),
                    {"type": "text", "text": "original"},
                ],
                delivery_refs=[],
                created_at=now - timedelta(seconds=2),
            )
        )
        db.add(
            PendingMessage(
                id=pending_id,
                session_id=session_id,
                user_id=user.id,
                session_key=session.session_key,
                content=[
                    build_runtime_block(
                        timestamp=(now - timedelta(seconds=1)).isoformat(),
                        session=session,
                        user_id=user.id,
                    ),
                    {"type": "text", "text": "queued before crash"},
                ],
                received_at=now - timedelta(seconds=1),
            )
        )
        db.add(
            TurnRun(
                id=old_turn_id,
                session_id=session_id,
                runner_instance_id=uuid4(),
                status="running",
                started_at=now - timedelta(seconds=2),
            )
        )
        await db.commit()

    await abandon_running_turns(
        pg_engine,
        runner_instance_id=runtime.runner_instance_id,
    )
    response = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={
            "content": [{"type": "text", "text": "new after restart"}],
            "attachments": [],
        },
    )
    events = _events(response)
    assert events[0]["disposition"] == "started"
    assert events[1]["message_ids"][0] == str(pending_id)
    assert len(events[1]["message_ids"]) == 2

    history = (await user_client.get(f"/api/sessions/{session_id}/messages")).json()
    assert [message["content"][-1]["text"] for message in history["messages"]] == [
        "original",
        "queued before crash",
        "new after restart",
        "recovered",
    ]
    await runtime.close()
