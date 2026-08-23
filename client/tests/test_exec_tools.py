from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from openoctopus_client.exec_sessions import ExecPolicy, ExecStart, ExecWrite
from openoctopus_client.tools.common import ToolOutput
from openoctopus_client.tools.exec import ExecToolDispatcher

CHAT_ID = UUID("00000000-0000-4000-8000-000000000003")


class RecordingManager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, UUID, object]] = []

    async def start(self, owner_chat: UUID, request: ExecStart) -> ToolOutput:
        self.calls.append(("start", owner_chat, request))
        return ToolOutput("started")

    async def write(self, owner_chat: UUID, request: ExecWrite) -> ToolOutput:
        self.calls.append(("write", owner_chat, request))
        return ToolOutput("written")

    async def list_sessions(self, owner_chat: UUID) -> ToolOutput:
        self.calls.append(("list", owner_chat, object()))
        return ToolOutput("listed")


def _dispatcher(
    *, timeout_cap: int = 600, restrict_to_workspace: bool = False
) -> tuple[ExecToolDispatcher, RecordingManager]:
    manager = RecordingManager()
    dispatcher = ExecToolDispatcher(
        manager,
        ExecPolicy(
            workspace=Path("/workspace"),
            restrict_to_workspace=restrict_to_workspace,
            shell_timeout_max=timeout_cap,
            env_allowlist=("PATH", "HOME"),
            available_shells=("bash", "sh"),
            default_shell="bash",
            epoch=1,
        ),
    )
    return dispatcher, manager


@pytest.mark.asyncio
async def test_exec_applies_defaults_and_passes_hidden_chat_owner() -> None:
    dispatcher, manager = _dispatcher()

    result = await dispatcher.execute(
        "exec",
        {"command": "printf ok"},
        chat_session_id=CHAT_ID,
    )

    assert result == ToolOutput("started")
    _, owner, raw_request = manager.calls[0]
    assert owner == CHAT_ID
    request = raw_request
    assert isinstance(request, ExecStart)
    assert request.command == "printf ok"
    assert request.shell == "bash"
    assert request.timeout_seconds == 60
    assert request.tty is False
    assert request.max_output_chars == 10_000


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ["cmd", "workdir", "max_output_tokens", "interactive", "mode"])
async def test_exec_rejects_removed_aliases(alias: str) -> None:
    dispatcher, _ = _dispatcher()

    result = await dispatcher.execute(
        "exec",
        {"command": "true", alias: "unexpected"},
        chat_session_id=CHAT_ID,
    )

    assert result.is_error is True
    assert result.code == "tool_invalid_args"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args",
    [
        {"command": "true", "timeout": 601, "yield_time_ms": 1},
        {"command": "true", "timeout": 120},
        {"command": "true", "timeout": 0, "yield_time_ms": 1},
    ],
)
async def test_exec_rejects_invalid_timeout_combinations(args: dict[str, Any]) -> None:
    dispatcher, manager = _dispatcher()

    result = await dispatcher.execute("exec", args, chat_session_id=CHAT_ID)

    assert result.is_error is True
    assert result.code == "tool_invalid_args"
    assert manager.calls == []


@pytest.mark.asyncio
async def test_exec_allows_unlimited_timeout_only_when_policy_and_yield_allow_it() -> None:
    dispatcher, manager = _dispatcher(timeout_cap=0)

    result = await dispatcher.execute(
        "exec",
        {"command": "python", "tty": True, "timeout": 0, "yield_time_ms": 100},
        chat_session_id=CHAT_ID,
    )

    assert result.is_error is False
    request = manager.calls[0][2]
    assert isinstance(request, ExecStart)
    assert request.timeout_seconds == 0


@pytest.mark.asyncio
async def test_write_stdin_enforces_presence_based_deadlines_and_utf8_credit() -> None:
    dispatcher, manager = _dispatcher()
    session_id = "0190d5a7-0000-7000-8000-000000000004"

    invalid = await dispatcher.execute(
        "write_stdin",
        {"session_id": session_id, "wait_timeout_ms": 10},
        chat_session_id=CHAT_ID,
    )
    mutually_exclusive = await dispatcher.execute(
        "write_stdin",
        {"session_id": session_id, "wait_for": "ready", "yield_time_ms": 10},
        chat_session_id=CHAT_ID,
    )
    too_many_bytes = await dispatcher.execute(
        "write_stdin",
        {"session_id": session_id, "chars": "界" * 30_000},
        chat_session_id=CHAT_ID,
    )

    assert invalid.code == "tool_invalid_args"
    assert mutually_exclusive.code == "tool_invalid_args"
    assert too_many_bytes.code == "tool_invalid_args"
    assert manager.calls == []


@pytest.mark.asyncio
async def test_list_requires_empty_args_and_all_exec_tools_require_owner() -> None:
    dispatcher, manager = _dispatcher()

    bad_args = await dispatcher.execute(
        "list_exec_sessions",
        {"unexpected": True},
        chat_session_id=CHAT_ID,
    )
    missing_owner = await dispatcher.execute("list_exec_sessions", {}, chat_session_id=None)
    success = await dispatcher.execute(
        "list_exec_sessions",
        {},
        chat_session_id=CHAT_ID,
    )

    assert bad_args.code == "tool_invalid_args"
    assert missing_owner.code == "tool_invalid_args"
    assert success == ToolOutput("listed")
    assert manager.calls[0][0:2] == ("list", CHAT_ID)


@pytest.mark.asyncio
async def test_workspace_restriction_still_exposes_exec() -> None:
    dispatcher, manager = _dispatcher(restrict_to_workspace=True)

    result = await dispatcher.execute(
        "exec",
        {"command": "true"},
        chat_session_id=CHAT_ID,
    )

    assert result == ToolOutput("started")
    request = manager.calls[0][2]
    assert isinstance(request, ExecStart)
    assert request.policy.restrict_to_workspace is True
