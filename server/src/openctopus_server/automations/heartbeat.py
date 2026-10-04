from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Literal, Protocol
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import PendingMessage, TurnRun
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.provider.jev import JevChoiceQuestion, JevState

HEARTBEAT_PATH = "HEARTBEAT.md"
HEARTBEAT_MAX_BYTES = 128_000
HEARTBEAT_MAX_CODEPOINTS = 32_000
HEARTBEAT_MAX_TASKS = 8
HEARTBEAT_MAX_TASK_CODEPOINTS = 500
HEARTBEAT_MAX_TOTAL_TASK_CODEPOINTS = 2_000

_FENCE_START = re.compile(r"^(`{3,}|~{3,})")
_ATX_LEVEL_ONE_OR_TWO = re.compile(r"^#{1,2}(?:\s+|$)")
_TASK_MARKER = re.compile(r"^(?:[-*+]|\d+[.)])\s+(.*)$")
_LOGGER = logging.getLogger(__name__)


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


def parse_heartbeat_tasks(document: str) -> tuple[str, ...]:
    """Extract original task bodies; Jev selects IDs rather than rewriting tasks."""
    section = extract_active_tasks(document)
    if section is None:
        return ()
    tasks: list[str] = []
    current: list[str] = []
    completed = False
    fence: tuple[str, int] | None = None

    def finish() -> None:
        task = "\n".join(current).strip()
        if task and not completed:
            tasks.append(task)
        current.clear()

    for line in section.splitlines():
        stripped = line.strip()
        previous_fence = fence
        fence = _updated_fence(fence, stripped)
        if previous_fence is not None or fence is not None:
            if current:
                current.append(line)
            continue
        match = _TASK_MARKER.match(line)
        if match is not None:
            finish()
            body = match.group(1)
            completed = body.startswith(("[x] ", "[X] "))
            if body.startswith(("[ ] ", "[x] ", "[X] ")):
                body = body[4:]
            current.append(body)
        elif stripped.startswith("#"):
            finish()
            completed = False
        elif stripped or current:
            if not current:
                completed = False
            current.append(line)
    finish()
    if (
        len(tasks) > HEARTBEAT_MAX_TASKS
        or any(len(task) > HEARTBEAT_MAX_TASK_CODEPOINTS for task in tasks)
        or sum(len(task) for task in tasks) > HEARTBEAT_MAX_TOTAL_TASK_CODEPOINTS
    ):
        raise ValueError("Heartbeat tasks exceed decision limits")
    return tuple(tasks)


def heartbeat_jev_request(
    *,
    document: str,
    tasks: tuple[str, ...],
    now_utc: datetime,
    timezone: str,
) -> tuple[JevState, dict[str, JevChoiceQuestion]]:
    local_time = now_utc.astimezone(ZoneInfo(timezone))
    task_map = {f"task_{index}": task for index, task in enumerate(tasks, start=1)}
    state = {
        "utc_time": _rfc3339(now_utc),
        "local_time": _rfc3339(local_time),
        "timezone": timezone,
        "document": document,
        "tasks": task_map,
    }
    questions = {
        task_id: JevChoiceQuestion(
            instructions=(
                f"Should the task in state.tasks.{task_id} run at the supplied current time? "
                "Use only the supplied document and time. Live external conditions cannot be checked here; "
                "the Agent performs requested checks after run. An unknown external outcome does not prevent a due check. "
                "Only the supplied scheduling constraints and known prerequisites determine whether to check now. "
                "Do not run future conditions early. "
                "Exact-time work belongs in Cron."
            ),
            criteria={
                "run": "A task or requested check is due now, including when its external outcome is unknown.",
                "skip": "The supplied schedule or an explicitly known prerequisite makes the task inapplicable now.",
            },
        )
        for task_id in task_map
    }
    return state, questions


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




class HeartbeatPulse:
    def __init__(
        self,
        *,
        engine: AsyncEngine,
        runtime: HeartbeatDecisionRuntime,
        workspace_service: HeartbeatWorkspace,
        publish_phase_two: HeartbeatPhaseTwoPublisher,
        now_utc: Callable[[], datetime] | None = None,
    ) -> None:
        self._engine = engine
        self._runtime = runtime
        self._workspace_service = workspace_service
        self._publish_phase_two = publish_phase_two
        self._now_utc = now_utc or (lambda: datetime.now(UTC))

    async def _process_user(self, user: _HeartbeatUser, *, now: datetime | None = None) -> None:
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
        now = (now or self._now_utc()).astimezone(UTC)
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
