from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any, Literal, Protocol
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import PendingMessage, TurnRun, User
from openctopus_server.errors.exceptions import WorkspaceError

HEARTBEAT_PATH = "HEARTBEAT.md"
HEARTBEAT_MAX_BYTES = 128_000
HEARTBEAT_MAX_CODEPOINTS = 32_000
HEARTBEAT_MAX_TASKS = 8
HEARTBEAT_MAX_TASK_CODEPOINTS = 500
HEARTBEAT_MAX_TOTAL_TASK_CODEPOINTS = 2_000
HEARTBEAT_USER_PAGE_SIZE = 100
HEARTBEAT_WORKERS = 32
HEARTBEAT_QUEUE_CAPACITY = 64

HEARTBEAT_DECISION_TOOL: dict[str, Any] = {
    "name": "heartbeat_decision",
    "description": "Decide which active heartbeat tasks, if any, should run now.",
    "input_schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["action", "tasks"],
        "properties": {
            "action": {"type": "string", "enum": ["skip", "run"]},
            "tasks": {
                "type": "array",
                "maxItems": HEARTBEAT_MAX_TASKS,
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": HEARTBEAT_MAX_TASK_CODEPOINTS,
                },
            },
        },
    },
}
HEARTBEAT_TOOL_CHOICE = {"type": "tool", "name": "heartbeat_decision"}
HEARTBEAT_DECISION_SYSTEM = (
    "Review HEARTBEAT.md and select only tasks that should run now. "
    "Use heartbeat_decision exactly once. Return at most 8 tasks in file priority order. "
    "Do not run future conditions early or invent tasks. Exact-time work belongs in Cron."
)

_FENCE_START = re.compile(r"^(`{3,}|~{3,})")
_ATX_LEVEL_ONE_OR_TWO = re.compile(r"^#{1,2}(?:\s+|$)")
_LOGGER = logging.getLogger(__name__)
_PULSE_LATE_GRACE = timedelta(seconds=5)


class HeartbeatWorkspace(Protocol):
    async def stat(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> Any: ...

    async def read(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        offset: int,
        length: int,
    ) -> bytes: ...


class HeartbeatDecisionRuntime(Protocol):
    async def evaluate_heartbeat_decision(
        self,
        *,
        document: str,
        now_utc: datetime,
        timezone: str,
    ) -> HeartbeatEvaluation: ...


@dataclass(frozen=True, slots=True)
class HeartbeatDocument:
    content: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class HeartbeatDecision:
    action: Literal["skip", "run"]
    tasks: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HeartbeatEvaluation:
    decision: HeartbeatDecision | None
    reason: str


@dataclass(frozen=True, slots=True)
class HeartbeatPhaseTwoRequest:
    user_id: UUID
    now_utc: datetime
    local_time: datetime
    timezone: str
    tasks: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _HeartbeatUser:
    id: UUID
    created_at: datetime
    timezone: str


HeartbeatPhaseTwoPublisher = Callable[[HeartbeatPhaseTwoRequest], Awaitable[bool]]


class _DecisionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    action: Literal["skip", "run"]
    tasks: list[str] = Field(max_length=HEARTBEAT_MAX_TASKS)


def extract_active_tasks(document: str) -> str | None:
    """Return the first meaningful Active Tasks section without interpreting Markdown."""
    visible = _remove_html_comments(document)
    lines = visible.splitlines()
    section_start: int | None = None
    fence: tuple[str, int] | None = None

    for index, line in enumerate(lines):
        stripped = line.strip()
        fence = _updated_fence(fence, stripped)
        if fence is not None:
            continue
        if section_start is None:
            if stripped == "## Active Tasks":
                section_start = index + 1
            continue
        if _ATX_LEVEL_ONE_OR_TWO.match(stripped):
            section = "\n".join(lines[section_start:index]).strip()
            return section or None

    if section_start is None:
        return None
    section = "\n".join(lines[section_start:]).strip()
    return section or None


async def load_heartbeat_document(
    db: AsyncSession,
    workspace_service: HeartbeatWorkspace,
    *,
    user_id: UUID,
) -> HeartbeatDocument:
    """Read HEARTBEAT.md without ever materializing more than its accepted bound."""
    try:
        metadata = await workspace_service.stat(
            db,
            user_id=user_id,
            path=HEARTBEAT_PATH,
        )
        size = getattr(metadata, "size", None)
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            return HeartbeatDocument(content=None, reason="unavailable")
        if size > HEARTBEAT_MAX_BYTES:
            return HeartbeatDocument(content=None, reason="too_large")
        if size == 0:
            return HeartbeatDocument(content=None, reason="empty")
        data = await workspace_service.read(
            db,
            user_id=user_id,
            path=HEARTBEAT_PATH,
            offset=0,
            length=HEARTBEAT_MAX_BYTES + 1,
        )
    except WorkspaceError:
        return HeartbeatDocument(content=None, reason="unavailable")

    if len(data) > HEARTBEAT_MAX_BYTES:
        return HeartbeatDocument(content=None, reason="too_large")
    if len(data) > size:
        return HeartbeatDocument(content=None, reason="changed_during_read")
    try:
        content = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return HeartbeatDocument(content=None, reason="invalid_utf8")
    if len(content) > HEARTBEAT_MAX_CODEPOINTS:
        return HeartbeatDocument(content=None, reason="too_many_codepoints")
    if not content:
        return HeartbeatDocument(content=None, reason="empty")
    return HeartbeatDocument(content=content, reason="ready")


def parse_heartbeat_decision(content: list[dict[str, Any]]) -> HeartbeatEvaluation:
    tool_uses = [block for block in content if block.get("type") == "tool_use"]
    if len(tool_uses) != 1 or tool_uses[0].get("name") != "heartbeat_decision":
        return HeartbeatEvaluation(decision=None, reason="invalid_response")
    raw_input = tool_uses[0].get("input")
    try:
        parsed = _DecisionInput.model_validate(raw_input)
    except ValidationError:
        return HeartbeatEvaluation(decision=None, reason="invalid_response")

    tasks: list[str] = []
    for raw_task in parsed.tasks:
        if len(raw_task) > HEARTBEAT_MAX_TASK_CODEPOINTS:
            return HeartbeatEvaluation(decision=None, reason="invalid_response")
        task = raw_task.strip()
        if not task or task in tasks:
            return HeartbeatEvaluation(decision=None, reason="invalid_response")
        tasks.append(task)
    if sum(len(task) for task in tasks) > HEARTBEAT_MAX_TOTAL_TASK_CODEPOINTS:
        return HeartbeatEvaluation(decision=None, reason="invalid_response")
    if parsed.action == "skip" and tasks:
        return HeartbeatEvaluation(decision=None, reason="invalid_response")
    if parsed.action == "run" and not tasks:
        return HeartbeatEvaluation(decision=None, reason="invalid_response")
    decision = HeartbeatDecision(action=parsed.action, tasks=tuple(tasks))
    return HeartbeatEvaluation(
        decision=decision,
        reason="decision_run" if parsed.action == "run" else "decision_skip",
    )


def heartbeat_decision_messages(
    *,
    document: str,
    now_utc: datetime,
    timezone: str,
) -> list[dict[str, Any]]:
    local_time = now_utc.astimezone(ZoneInfo(timezone))
    return [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        f"Current UTC time: {_rfc3339(now_utc)}\n"
                        f"Current local time: {_rfc3339(local_time)}\n"
                        f"IANA timezone: {timezone}\n\n"
                        "HEARTBEAT.md:\n"
                        f"{document}"
                    ),
                }
            ],
        }
    ]


def build_heartbeat_phase_two_text(request: HeartbeatPhaseTwoRequest) -> str:
    numbered = "\n".join(
        f"{index}. {task}" for index, task in enumerate(request.tasks, start=1)
    )
    return (
        "[OpenOctopus Heartbeat]\n"
        f"UTC time: {_rfc3339(request.now_utc)}\n"
        f"Local time: {_rfc3339(request.local_time)}\n"
        f"Timezone: {request.timezone}\n"
        "Tasks selected for this pulse:\n"
        f"{numbered}"
    )


def next_heartbeat_boundary(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Heartbeat clock must be timezone-aware")
    utc_now = now.astimezone(UTC)
    minute = 30 if utc_now.minute < 30 else 60
    base = utc_now.replace(second=0, microsecond=0)
    if minute == 30:
        return base.replace(minute=30)
    return base.replace(minute=0) + timedelta(hours=1)


class HeartbeatPulse:
    def __init__(
        self,
        *,
        engine: AsyncEngine,
        runtime: HeartbeatDecisionRuntime,
        workspace_service: HeartbeatWorkspace,
        publish_phase_two: HeartbeatPhaseTwoPublisher,
        now_utc: Callable[[], datetime] | None = None,
        wait_until: Callable[[datetime, asyncio.Event], Awaitable[bool]] | None = None,
    ) -> None:
        self._engine = engine
        self._runtime = runtime
        self._workspace_service = workspace_service
        self._publish_phase_two = publish_phase_two
        self._now_utc = now_utc or (lambda: datetime.now(UTC))
        self._wait_until = wait_until or self._default_wait_until
        self._stop = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None
        self._scan_task: asyncio.Task[None] | None = None
        self._last_boundary: datetime | None = None

    def start(self) -> None:
        if self._loop_task is not None:
            raise RuntimeError("Heartbeat pulse is already started")
        self._loop_task = asyncio.create_task(self._run(), name="heartbeat-pulse")

    async def close(self) -> None:
        self._stop.set()
        tasks = [task for task in (self._loop_task, self._scan_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._loop_task = None
        self._scan_task = None

    def trigger_scan(self, boundary: datetime) -> bool:
        if self._stop.is_set():
            return False
        scan = self._scan_task
        if scan is not None and not scan.done():
            _LOGGER.info("heartbeat pulse skipped because the previous scan is active")
            return False
        self._last_boundary = boundary
        self._scan_task = asyncio.create_task(
            self._run_scan_guarded(),
            name=f"heartbeat-scan-{boundary.isoformat()}",
        )
        return True

    async def wait_for_scan(self) -> None:
        scan = self._scan_task
        if scan is not None:
            await scan

    async def run_scan(self) -> None:
        upper = await self._user_upper_bound()
        if upper is None:
            return
        queue: asyncio.Queue[_HeartbeatUser | None] = asyncio.Queue(
            maxsize=HEARTBEAT_QUEUE_CAPACITY
        )
        workers = [
            asyncio.create_task(self._worker(queue), name=f"heartbeat-worker-{index}")
            for index in range(HEARTBEAT_WORKERS)
        ]
        try:
            await self._produce_users(queue, upper=upper)
            for _ in workers:
                await queue.put(None)
            failures = sum(await asyncio.gather(*workers))
            if failures:
                _LOGGER.warning(
                    "heartbeat scan completed with user_failures=%d",
                    failures,
                )
        finally:
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def _run_scan_guarded(self) -> None:
        try:
            await self.run_scan()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.error("heartbeat scan failed")

    async def _run(self) -> None:
        boundary = next_heartbeat_boundary(self._now_utc())
        while not self._stop.is_set():
            if await self._wait_until(boundary, self._stop):
                return
            now = self._now_utc().astimezone(UTC)
            if boundary <= now <= boundary + _PULSE_LATE_GRACE:
                self.trigger_scan(boundary)
            if self._last_boundary is not None and boundary <= self._last_boundary:
                boundary = next_heartbeat_boundary(self._last_boundary)
            else:
                boundary = next_heartbeat_boundary(now)

    async def _user_upper_bound(self) -> tuple[datetime, UUID] | None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            row = (
                await db.execute(
                    select(User.created_at, User.id)
                    .order_by(User.created_at.desc(), User.id.desc())
                    .limit(1)
                )
            ).one_or_none()
        return None if row is None else (row.created_at, row.id)

    async def _produce_users(
        self,
        queue: asyncio.Queue[_HeartbeatUser | None],
        *,
        upper: tuple[datetime, UUID],
    ) -> None:
        cursor: tuple[datetime, UUID] | None = None
        while True:
            async with AsyncSession(self._engine, expire_on_commit=False) as db:
                query = select(User.id, User.created_at, User.timezone).where(
                    or_(
                        User.created_at < upper[0],
                        and_(User.created_at == upper[0], User.id <= upper[1]),
                    )
                )
                if cursor is not None:
                    query = query.where(
                        or_(
                            User.created_at > cursor[0],
                            and_(User.created_at == cursor[0], User.id > cursor[1]),
                        )
                    )
                rows = (
                    (
                        await db.execute(
                            query.order_by(User.created_at, User.id).limit(
                                HEARTBEAT_USER_PAGE_SIZE
                            )
                        )
                    )
                    .all()
                )
            if not rows:
                return
            for row in rows:
                await queue.put(
                    _HeartbeatUser(
                        id=row.id,
                        created_at=row.created_at,
                        timezone=row.timezone,
                    )
                )
            last = rows[-1]
            cursor = (last.created_at, last.id)
            if len(rows) < HEARTBEAT_USER_PAGE_SIZE:
                return
            await asyncio.sleep(0)

    async def _worker(self, queue: asyncio.Queue[_HeartbeatUser | None]) -> int:
        failures = 0
        while True:
            user = await queue.get()
            try:
                if user is None:
                    return failures
                try:
                    await self._process_user(user)
                except Exception:
                    failures += 1
            finally:
                queue.task_done()

    async def _process_user(self, user: _HeartbeatUser) -> None:
        if await self._session_is_busy(user.id):
            return
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            loaded = await load_heartbeat_document(
                db,
                self._workspace_service,
                user_id=user.id,
            )
        if loaded.content is None or extract_active_tasks(loaded.content) is None:
            return
        now = self._now_utc().astimezone(UTC)
        phase_one_started = monotonic()
        evaluation = await self._runtime.evaluate_heartbeat_decision(
            document=loaded.content,
            now_utc=now,
            timezone=user.timezone,
        )
        decision = evaluation.decision
        _LOGGER.info(
            "heartbeat phase1 completed",
            extra={
                "user_id": str(user.id),
                "outcome": decision.action if decision is not None else "skip",
                "reason_code": evaluation.reason,
                "latency_ms": max(0, int((monotonic() - phase_one_started) * 1000)),
            },
        )
        if decision is None or decision.action != "run":
            return
        try:
            local_time = now.astimezone(ZoneInfo(user.timezone))
        except (ValueError, ZoneInfoNotFoundError):
            return
        await self._publish_phase_two(
            HeartbeatPhaseTwoRequest(
                user_id=user.id,
                now_utc=now,
                local_time=local_time,
                timezone=user.timezone,
                tasks=decision.tasks,
            )
        )

    async def _session_is_busy(self, session_id: UUID) -> bool:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            pending_id = await db.scalar(
                select(PendingMessage.id)
                .where(PendingMessage.session_id == session_id)
                .limit(1)
            )
            if pending_id is not None:
                return True
            running_id = await db.scalar(
                select(TurnRun.id)
                .where(TurnRun.session_id == session_id, TurnRun.status == "running")
                .limit(1)
            )
            return running_id is not None

    async def _default_wait_until(self, target: datetime, stop: asyncio.Event) -> bool:
        delay = max(0.0, (target - self._now_utc().astimezone(UTC)).total_seconds())
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            return False
        return True


def _remove_html_comments(document: str) -> str:
    result: list[str] = []
    index = 0
    in_comment = False
    while index < len(document):
        if not in_comment and document.startswith("<!--", index):
            result.extend(" " * 4)
            index += 4
            in_comment = True
            continue
        if in_comment and document.startswith("-->", index):
            result.extend(" " * 3)
            index += 3
            in_comment = False
            continue
        char = document[index]
        result.append("\n" if char == "\n" else (" " if in_comment else char))
        index += 1
    return "".join(result)


def _updated_fence(
    fence: tuple[str, int] | None,
    stripped_line: str,
) -> tuple[str, int] | None:
    match = _FENCE_START.match(stripped_line)
    if match is None:
        return fence
    marker = match.group(1)
    if fence is None:
        return marker[0], len(marker)
    if marker[0] == fence[0] and len(marker) >= fence[1]:
        return None
    return fence


def _rfc3339(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
