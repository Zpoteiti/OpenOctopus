from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.automations.heartbeat import (
    HEARTBEAT_MAX_BYTES,
    HEARTBEAT_MAX_CODEPOINTS,
    HEARTBEAT_QUEUE_CAPACITY,
    HEARTBEAT_USER_PAGE_SIZE,
    HEARTBEAT_WORKERS,
    HeartbeatEvaluation,
    HeartbeatPhaseTwoRequest,
    HeartbeatPulse,
    build_heartbeat_phase_two_text,
    extract_active_tasks,
    load_heartbeat_document,
    next_heartbeat_boundary,
    parse_heartbeat_tasks,
)
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import (
    Message,
    PendingMessage,
    Session,
    SystemConfig,
    TurnRun,
    User,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.provider.jev import JevService
from openctopus_server.services.heartbeat import publish_heartbeat_phase_two


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        ("", None),
        ("# Heartbeat\n\n## Notes\nhello\n", None),
        ("# Heartbeat\n\n## Active tasks\n- wrong case\n", None),
        (
            "# Heartbeat\n\n<!--\n## Active Tasks\n- hidden\n-->\n",
            None,
        ),
        (
            "# Heartbeat\n\n```markdown\n## Active Tasks\n- hidden\n```\n",
            None,
        ),
        (
            "# Heartbeat\n\n## Active Tasks\n\n<!-- comment only -->\n\n## Notes\nignored\n",
            None,
        ),
        (
            "# Heartbeat\n\n## Active Tasks\n- inspect blockers\n\n"
            "### Detail\nkeep this\n\n## Notes\nignored\n",
            "- inspect blockers\n\n### Detail\nkeep this",
        ),
        (
            "# Heartbeat\n\n~~~text\n## Active Tasks\n- hidden\n~~~\n"
            "## Active Tasks\n- visible\n",
            "- visible",
        ),
    ],
)
def test_extract_active_tasks_is_deterministic(document: str, expected: str | None) -> None:
    assert extract_active_tasks(document) == expected


class _Workspace:
    def __init__(
        self,
        *,
        size: int = 0,
        data: bytes = b"",
        stat_error: Exception | None = None,
        read_error: Exception | None = None,
    ) -> None:
        self.size = size
        self.data = data
        self.stat_error = stat_error
        self.read_error = read_error
        self.stat_calls = 0
        self.read_calls = 0
        self.read_length: int | None = None

    async def stat(self, db: object, *, user_id: object, path: str) -> object:
        del db, user_id
        self.stat_calls += 1
        assert path == "HEARTBEAT.md"
        if self.stat_error is not None:
            raise self.stat_error
        return SimpleNamespace(size=self.size)

    async def read(
        self,
        db: object,
        *,
        user_id: object,
        path: str,
        offset: int,
        length: int,
    ) -> bytes:
        del db, user_id, offset
        self.read_calls += 1
        self.read_length = length
        assert path == "HEARTBEAT.md"
        if self.read_error is not None:
            raise self.read_error
        return self.data[:length]


async def test_heartbeat_read_stats_before_get_and_uses_one_extra_byte() -> None:
    workspace = _Workspace(
        size=HEARTBEAT_MAX_BYTES,
        data=b"# Heartbeat\n\n## Active Tasks\n- inspect\n",
    )

    result = await load_heartbeat_document(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        user_id=uuid4(),
    )

    assert result.content is not None
    assert result.reason == "ready"
    assert workspace.stat_calls == 1
    assert workspace.read_calls == 1
    assert workspace.read_length == HEARTBEAT_MAX_BYTES + 1


async def test_heartbeat_read_skips_oversize_without_get() -> None:
    workspace = _Workspace(size=HEARTBEAT_MAX_BYTES + 1)

    result = await load_heartbeat_document(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        user_id=uuid4(),
    )

    assert result.content is None
    assert result.reason == "too_large"
    assert workspace.read_calls == 0


@pytest.mark.parametrize(
    ("workspace", "reason"),
    [
        (
            _Workspace(
                stat_error=WorkspaceError(
                    ErrorCode.WORKSPACE_NOT_FOUND,
                    "missing",
                )
            ),
            "unavailable",
        ),
        (
            _Workspace(
                size=1,
                read_error=WorkspaceError(
                    ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                    "failed",
                ),
            ),
            "unavailable",
        ),
        (_Workspace(size=1, data=b"\xff"), "invalid_utf8"),
        (
            _Workspace(
                size=HEARTBEAT_MAX_BYTES,
                data=b"x" * (HEARTBEAT_MAX_BYTES + 1),
            ),
            "too_large",
        ),
        (_Workspace(size=1, data=b"xy"), "changed_during_read"),
        (
            _Workspace(
                size=HEARTBEAT_MAX_CODEPOINTS + 1,
                data=b"x" * (HEARTBEAT_MAX_CODEPOINTS + 1),
            ),
            "too_many_codepoints",
        ),
    ],
)
async def test_heartbeat_read_failures_are_closed(
    workspace: _Workspace,
    reason: str,
) -> None:
    result = await load_heartbeat_document(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        user_id=uuid4(),
    )

    assert result.content is None
    assert result.reason == reason


@pytest.mark.parametrize(
    ("section", "expected"),
    [
        ("", ()),
        ("- inspect blockers\n- review jobs", ("inspect blockers", "review jobs")),
        ("1. preserve text!\n   Keep this detail.\n2. second", ("preserve text!\n   Keep this detail.", "second")),
        ("- [x] completed\n- [ ] pending", ("pending",)),
        ("<!-- - hidden -->\n- visible", ("visible",)),
        ("### Checks\n- inspect", ("inspect",)),
    ],
)
def test_parse_heartbeat_tasks_preserves_original_bodies(section: str, expected: tuple[str, ...]) -> None:
    assert parse_heartbeat_tasks("## Active Tasks\n" + section) == expected


@pytest.mark.parametrize("section", [
    "- " + "x" * 501,
    "\n".join("- " + str(index) for index in range(9)),
    "\n".join("- " + str(index) + "x" * 499 for index in range(5)),
])
def test_parse_heartbeat_tasks_rejects_unbounded_task_sets(section: str) -> None:
    with pytest.raises(ValueError):
        parse_heartbeat_tasks("## Active Tasks\n" + section)


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (
            datetime(2026, 9, 1, 10, 12, tzinfo=UTC),
            datetime(2026, 9, 1, 10, 30, tzinfo=UTC),
        ),
        (
            datetime(2026, 9, 1, 10, 30, tzinfo=UTC),
            datetime(2026, 9, 1, 11, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 9, 1, 10, 30, 0, 1, tzinfo=UTC),
            datetime(2026, 9, 1, 11, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 9, 1, 10, 59, 59, tzinfo=UTC),
            datetime(2026, 9, 1, 11, 0, tzinfo=UTC),
        ),
    ],
)
def test_next_heartbeat_boundary_is_strictly_future(
    now: datetime,
    expected: datetime,
) -> None:
    assert next_heartbeat_boundary(now) == expected


def test_heartbeat_scan_bounds_are_fixed() -> None:
    assert HEARTBEAT_USER_PAGE_SIZE == 100
    assert HEARTBEAT_WORKERS == 32
    assert HEARTBEAT_QUEUE_CAPACITY == 64


def test_heartbeat_phase_two_message_contains_only_selected_tasks_and_times() -> None:
    request = HeartbeatPhaseTwoRequest(
        user_id=uuid4(),
        now_utc=datetime(2026, 9, 1, 1, 30, tzinfo=UTC),
        local_time=datetime.fromisoformat("2026-09-01T09:30:00+08:00"),
        timezone="Asia/Shanghai",
        tasks=("inspect blockers", "summarize status"),
    )

    text = build_heartbeat_phase_two_text(request)

    assert "2026-09-01T01:30:00Z" in text
    assert "2026-09-01T09:30:00+08:00" in text
    assert "1. inspect blockers" in text
    assert "2. summarize status" in text
    assert "HEARTBEAT.md" not in text


async def test_heartbeat_pulse_start_waits_for_strictly_future_boundary() -> None:
    now = datetime(2026, 9, 1, 10, 12, tzinfo=UTC)
    waiting = asyncio.Event()
    targets: list[datetime] = []

    async def wait_until(target: datetime, stop: asyncio.Event) -> bool:
        targets.append(target)
        waiting.set()
        await stop.wait()
        return True

    pulse = HeartbeatPulse(
        engine=SimpleNamespace(),  # type: ignore[arg-type]
        runtime=SimpleNamespace(),  # type: ignore[arg-type]
        workspace_service=SimpleNamespace(),  # type: ignore[arg-type]
        publish_phase_two=SimpleNamespace(),  # type: ignore[arg-type]
        now_utc=lambda: now,
        wait_until=wait_until,
    )

    pulse.start()
    await waiting.wait()
    assert targets == [datetime(2026, 9, 1, 10, 30, tzinfo=UTC)]
    await pulse.close()


async def test_heartbeat_pulse_does_not_catch_up_a_missed_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = [datetime(2026, 9, 1, 10, 12, tzinfo=UTC)]
    next_wait = asyncio.Event()
    targets: list[datetime] = []
    scans = 0

    async def wait_until(target: datetime, stop: asyncio.Event) -> bool:
        targets.append(target)
        if len(targets) == 1:
            current[0] = datetime(2026, 9, 1, 10, 31, tzinfo=UTC)
            return False
        next_wait.set()
        await stop.wait()
        return True

    async def scan(self: HeartbeatPulse) -> None:
        nonlocal scans
        del self
        scans += 1

    monkeypatch.setattr(HeartbeatPulse, "run_scan", scan)
    pulse = HeartbeatPulse(
        engine=SimpleNamespace(),  # type: ignore[arg-type]
        runtime=SimpleNamespace(),  # type: ignore[arg-type]
        workspace_service=SimpleNamespace(),  # type: ignore[arg-type]
        publish_phase_two=SimpleNamespace(),  # type: ignore[arg-type]
        now_utc=lambda: current[0],
        wait_until=wait_until,
    )

    pulse.start()
    await next_wait.wait()
    assert targets == [
        datetime(2026, 9, 1, 10, 30, tzinfo=UTC),
        datetime(2026, 9, 1, 11, 0, tzinfo=UTC),
    ]
    assert scans == 0
    await pulse.close()


async def test_heartbeat_pulse_does_not_reenter_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocked_scan(self: HeartbeatPulse) -> None:
        del self
        started.set()
        await release.wait()

    monkeypatch.setattr(HeartbeatPulse, "run_scan", blocked_scan)
    pulse = HeartbeatPulse(
        engine=SimpleNamespace(),  # type: ignore[arg-type]
        runtime=SimpleNamespace(),  # type: ignore[arg-type]
        workspace_service=SimpleNamespace(),  # type: ignore[arg-type]
        publish_phase_two=SimpleNamespace(),  # type: ignore[arg-type]
    )

    assert pulse.trigger_scan(datetime(2026, 9, 1, 10, 30, tzinfo=UTC))
    await started.wait()
    assert not pulse.trigger_scan(datetime(2026, 9, 1, 11, 0, tzinfo=UTC))

    release.set()
    await pulse.wait_for_scan()
    assert pulse.trigger_scan(datetime(2026, 9, 1, 11, 30, tzinfo=UTC))
    release.set()
    await pulse.wait_for_scan()


async def test_heartbeat_pulse_contains_scan_failure_and_allows_next_boundary(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    first_finished = asyncio.Event()
    second_finished = asyncio.Event()
    calls = 0

    async def scan(self: HeartbeatPulse) -> None:
        nonlocal calls
        del self
        calls += 1
        if calls == 1:
            first_finished.set()
            raise RuntimeError("must not reach the event loop")
        second_finished.set()

    monkeypatch.setattr(HeartbeatPulse, "run_scan", scan)
    pulse = HeartbeatPulse(
        engine=SimpleNamespace(),  # type: ignore[arg-type]
        runtime=SimpleNamespace(),  # type: ignore[arg-type]
        workspace_service=SimpleNamespace(),  # type: ignore[arg-type]
        publish_phase_two=SimpleNamespace(),  # type: ignore[arg-type]
    )

    assert pulse.trigger_scan(datetime(2026, 9, 1, 10, 30, tzinfo=UTC))
    await first_finished.wait()
    await pulse.wait_for_scan()
    assert "heartbeat scan failed" in caplog.text
    assert "must not reach the event loop" not in caplog.text

    assert pulse.trigger_scan(datetime(2026, 9, 1, 11, 0, tzinfo=UTC))
    await second_finished.wait()
    await pulse.wait_for_scan()
    assert calls == 2


async def test_heartbeat_scan_pages_users_bounds_workers_and_isolates_failures(
    pg_engine: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    users = [
        User(
            id=uuid4(),
            email=f"heartbeat-{index}@test.example",
            password_hash="hash",
            name=f"User {index}",
            timezone="UTC",
        )
        for index in range(205)
    ]
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(users)
        await db.commit()

    active = 0
    peak = 0
    seen: set[Any] = set()
    failed_id = users[73].id

    async def process_user(self: HeartbeatPulse, user: Any) -> None:
        nonlocal active, peak
        del self
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0)
            seen.add(user.id)
            if user.id == failed_id:
                raise RuntimeError("isolated")
        finally:
            active -= 1

    monkeypatch.setattr(HeartbeatPulse, "_process_user", process_user)
    pulse = HeartbeatPulse(
        engine=pg_engine,
        runtime=SimpleNamespace(),  # type: ignore[arg-type]
        workspace_service=SimpleNamespace(),  # type: ignore[arg-type]
        publish_phase_two=SimpleNamespace(),  # type: ignore[arg-type]
    )

    await pulse.run_scan()

    assert seen == {user.id for user in users}
    assert peak <= HEARTBEAT_WORKERS


async def test_heartbeat_phase_one_logs_bounded_diagnostics(
    pg_engine: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    user = User(
        id=uuid4(),
        email="heartbeat-diagnostics@test.example",
        password_hash="hash",
        name="Diagnostics",
        timezone="UTC",
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(user)
        await db.commit()

    workspace = _Workspace(
        size=42,
        data=b"# Heartbeat\n\n## Active Tasks\n- inspect\n",
    )
    runtime = AsyncMock(
        evaluate_heartbeat_decision=AsyncMock(
            return_value=HeartbeatEvaluation(decision=None, reason="invalid_response")
        )
    )
    caplog.set_level("INFO", logger="openctopus_server.automations.heartbeat")
    pulse = HeartbeatPulse(
        engine=pg_engine,
        runtime=runtime,
        workspace_service=workspace,
        publish_phase_two=AsyncMock(),
    )

    await pulse.run_scan()

    record = next(
        record for record in caplog.records
        if record.getMessage().startswith("heartbeat phase1 completed")
    )
    assert record.reason_code == "invalid_response"
    assert record.outcome == "skip"
    assert isinstance(record.latency_ms, int)
    assert record.latency_ms >= 0
    assert record.user_id == str(user.id)


class _PublishingRuntime:
    def __init__(self) -> None:
        self.runner_instance_id = uuid4()
        self.scheduled: list[Any] = []
        self.operations: list[Any] = []

    @asynccontextmanager
    async def session_operation(self, session_id: Any):
        self.operations.append(session_id)
        yield

    async def schedule(self, accepted: Any) -> None:
        self.scheduled.append(accepted)


async def test_heartbeat_phase_two_publishes_durably_to_stable_session(
    pg_engine: Any,
) -> None:
    user = User(
        id=uuid4(),
        email="heartbeat-owner@test.example",
        password_hash="hash",
        name="Owner",
        timezone="Asia/Shanghai",
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(user)
        await db.commit()
    runtime = _PublishingRuntime()
    request = HeartbeatPhaseTwoRequest(
        user_id=user.id,
        now_utc=datetime(2026, 9, 1, 1, 30, tzinfo=UTC),
        local_time=datetime.fromisoformat("2026-09-01T09:30:00+08:00"),
        timezone="Asia/Shanghai",
        tasks=("inspect blockers",),
    )

    accepted = await publish_heartbeat_phase_two(
        pg_engine,
        runtime,  # type: ignore[arg-type]
        request,
    )

    assert accepted
    assert runtime.operations == [user.id]
    assert len(runtime.scheduled) == 1
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        session = await db.get(Session, user.id)
        assert session is not None
        assert session.session_key == f"heartbeat:{user.id}"
        assert session.channel == "heartbeat"
        assert session.chat_id == str(user.id)
        pending = (await db.execute(select(PendingMessage))).scalar_one()
        assert pending.session_id == user.id
        assert pending.content[-1]["type"] == "text"
        assert "inspect blockers" in pending.content[-1]["text"]
        assert "HEARTBEAT.md" not in pending.content[-1]["text"]
        turn = (await db.execute(select(TurnRun))).scalar_one()
        assert turn.session_id == user.id

        await db.execute(delete(Session).where(Session.id == user.id))
        await db.commit()

    recreated = await publish_heartbeat_phase_two(
        pg_engine,
        runtime,  # type: ignore[arg-type]
        request,
    )
    assert recreated
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        session = await db.get(Session, user.id)
        assert session is not None
        assert session.id == user.id


async def test_heartbeat_phase_two_rechecks_busy_and_missing_owner(pg_engine: Any) -> None:
    user = User(
        id=uuid4(),
        email="heartbeat-busy@test.example",
        password_hash="hash",
        name="Owner",
        timezone="UTC",
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(user)
        await db.commit()
    runtime = _PublishingRuntime()
    request = HeartbeatPhaseTwoRequest(
        user_id=user.id,
        now_utc=datetime(2026, 9, 1, 1, 30, tzinfo=UTC),
        local_time=datetime(2026, 9, 1, 1, 30, tzinfo=UTC),
        timezone="UTC",
        tasks=("inspect",),
    )

    assert await publish_heartbeat_phase_two(
        pg_engine,
        runtime,  # type: ignore[arg-type]
        request,
    )
    assert not await publish_heartbeat_phase_two(
        pg_engine,
        runtime,  # type: ignore[arg-type]
        request,
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        await db.execute(delete(User).where(User.id == user.id))
        await db.commit()
    assert not await publish_heartbeat_phase_two(
        pg_engine,
        runtime,  # type: ignore[arg-type]
        request,
    )


async def _configure_jev(engine: Any) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        db.add_all([
            SystemConfig(key="jev_endpoint", value="http://jev.test"),
            SystemConfig(key="jev_api_key", value="jev-secret"),
            SystemConfig(key="jev_revision", value="first"),
        ])
        await db.commit()


def _jev_response(choices: dict[str, str]) -> dict[str, Any]:
    return {
        "model": "jev-1.13.0",
        "answers": {
            key: {"type": "choice", "choice": choice,
                  "probabilities": {"run": 0.8 if choice == "run" else 0.2,
                                    "skip": 0.2 if choice == "run" else 0.8},
                  "confidence": 0.5}
            for key, choice in choices.items()
        },
        "usage": {"input_tokens": 100, "output_tokens": 20},
    }


async def test_chat_runtime_heartbeat_uses_jev_and_original_task_order(pg_engine: Any) -> None:
    await _configure_jev(pg_engine)
    calls: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        assert request.url.path == "/v1/systemone"
        assert request.headers["authorization"] == "Bearer jev-secret"
        return httpx.Response(200, json=_jev_response({"task_3": "run", "task_2": "skip", "task_1": "run"}))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = JevService(pg_engine, client=client)
        provider_factory = AsyncMock(side_effect=AssertionError("normal provider cannot make Phase 1 decisions"))
        runtime = ChatRuntime(pg_engine, jev_service=service, provider_factory=provider_factory)
        document = "## Active Tasks\n- Original first!\n- second\n- Original third.\n"
        result = await runtime.evaluate_heartbeat_decision(
            document=document,
            now_utc=datetime(2026, 9, 1, 1, 30, tzinfo=UTC),
            timezone="Asia/Shanghai",
        )
        assert result.decision is not None
        assert result.decision.tasks == ("Original first!", "Original third.")
        assert result.reason == "decision_run"
        assert provider_factory.call_count == 0
        assert calls[0]["model"] == "jev-latest"
        assert calls[0]["state"] == {
            "utc_time": "2026-09-01T01:30:00Z", "local_time": "2026-09-01T09:30:00+08:00",
            "timezone": "Asia/Shanghai", "document": document,
            "tasks": {"task_1": "Original first!", "task_2": "second", "task_3": "Original third."},
        }
        assert all(question["type"] == "choice" and set(question["criteria"]) == {"run", "skip"}
                   for question in calls[0]["questions"].values())
        await runtime.close()


@pytest.mark.parametrize("response,reason", [
    (httpx.Response(401, text="jev-secret rejected"), "jev_unauthorized"),
    (httpx.Response(529, text="overloaded"), "jev_unavailable"),
    (httpx.Response(200, json=_jev_response({"wrong_id": "run"})), "jev_invalid_response"),
])
async def test_heartbeat_jev_failure_has_no_chat_side_effects(pg_engine: Any, response: httpx.Response, reason: str) -> None:
    await _configure_jev(pg_engine)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
        runtime = ChatRuntime(pg_engine, jev_service=JevService(pg_engine, client=client))
        result = await runtime.evaluate_heartbeat_decision(
            document="## Active Tasks\n- inspect\n", now_utc=datetime(2026, 9, 1, tzinfo=UTC), timezone="UTC",
        )
        assert result.decision is None
        assert result.reason == reason
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            for model in (Session, Message, PendingMessage, TurnRun):
                assert await db.scalar(select(func.count()).select_from(model)) == 0
        await runtime.close()


async def test_heartbeat_requires_jev_configuration_even_when_llm_is_configured(pg_engine: Any) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(SystemConfig(key=key, value=value) for key, value in {
            "llm_endpoint": "http://provider.test", "llm_api_key": "secret", "llm_model": "model",
        }.items())
        await db.commit()
    runtime = ChatRuntime(pg_engine, provider_factory=AsyncMock(side_effect=AssertionError("no fallback")))
    result = await runtime.evaluate_heartbeat_decision(
        document="## Active Tasks\n- inspect\n", now_utc=datetime(2026, 9, 1, tzinfo=UTC), timezone="UTC",
    )
    assert result.decision is None
    assert result.reason == "jev_not_configured"
    await runtime.close()


@pytest.mark.parametrize("document", ["## Active Tasks\n<!-- empty -->", "## Active Tasks\n- " + "x" * 501])
async def test_heartbeat_empty_or_oversize_tasks_do_not_call_jev(pg_engine: Any, document: str) -> None:
    service = JevService(pg_engine)
    service.evaluate = AsyncMock(side_effect=AssertionError("preflight must stop"))  # type: ignore[method-assign]
    runtime = ChatRuntime(pg_engine, jev_service=service)
    await runtime.evaluate_heartbeat_decision(
        document=document, now_utc=datetime(2026, 9, 1, tzinfo=UTC), timezone="UTC",
    )
    service.evaluate.assert_not_called()
    await runtime.close()
    await service.close()
