from __future__ import annotations

from typing import Annotated, Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from openoctopus_client.exec_sessions import ExecPolicy, ExecStart, ExecWrite
from openoctopus_client.protocol import EXEC_TOOL_NAMES
from openoctopus_client.tools.common import ToolOutput, fail

_SHELLS = {"bash", "sh", "zsh", "pwsh", "powershell", "powershell_x86", "cmd"}


class ExecManager(Protocol):
    async def start(self, owner_chat: UUID, request: ExecStart) -> ToolOutput: ...

    async def write(self, owner_chat: UUID, request: ExecWrite) -> ToolOutput: ...

    async def list_sessions(self, owner_chat: UUID) -> ToolOutput: ...


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _ExecArgs(_StrictModel):
    command: Annotated[str, Field(min_length=1, max_length=24_000)]
    working_dir: Annotated[str, Field(min_length=1, max_length=4096)] | None = None
    timeout: Annotated[int, Field(ge=0, le=86_400)] | None = None
    shell: str | None = None
    login: bool = False
    tty: bool = False
    yield_time_ms: Annotated[int, Field(ge=0, le=30_000)] | None = None
    max_output_chars: Annotated[int, Field(ge=1000, le=50_000)] = 10_000

    @model_validator(mode="after")
    def _reject_nul_and_unknown_shell(self) -> _ExecArgs:
        if "\x00" in self.command or (self.working_dir is not None and "\x00" in self.working_dir):
            raise ValueError("exec strings must not contain NUL")
        if self.shell is not None and self.shell not in _SHELLS:
            raise ValueError("unsupported shell")
        return self


class _WriteArgs(_StrictModel):
    session_id: Annotated[str, Field(min_length=36, max_length=36)]
    chars: Annotated[str, Field(max_length=65_536)] | None = None
    terminate: bool = False
    yield_time_ms: Annotated[int, Field(ge=0, le=30_000)] | None = None
    wait_for: Annotated[str, Field(min_length=1, max_length=4096)] | None = None
    wait_timeout_ms: Annotated[int, Field(ge=0, le=30_000)] | None = None
    max_output_chars: Annotated[int, Field(ge=1000, le=50_000)] = 10_000

    @model_validator(mode="after")
    def _validate_operation(self) -> _WriteArgs:
        fields = self.model_fields_set
        if self.chars is not None and len(self.chars.encode("utf-8")) > 65_536:
            raise ValueError("chars exceeds UTF-8 byte limit")
        if self.terminate and ((self.chars is not None and self.chars != "") or self.wait_for):
            raise ValueError("terminate cannot be combined with input or wait_for")
        if self.wait_for is not None and "yield_time_ms" in fields:
            raise ValueError("wait_for and yield_time_ms are mutually exclusive")
        if self.wait_for is None and "wait_timeout_ms" in fields:
            raise ValueError("wait_timeout_ms requires wait_for")
        return self


class _ListArgs(_StrictModel):
    pass


class ExecToolDispatcher:
    def __init__(self, manager: ExecManager, policy: ExecPolicy) -> None:
        self._manager = manager
        self._policy = policy

    async def execute(
        self,
        name: str,
        args: dict[str, Any],
        *,
        chat_session_id: UUID | None,
    ) -> ToolOutput:
        if name not in EXEC_TOOL_NAMES:
            return fail("tool_not_available", f"This client does not implement {name}")
        if chat_session_id is None:
            return fail("tool_invalid_args", "Exec tool ownership is invalid")
        try:
            if name == "exec":
                return await self._exec(args, chat_session_id)
            if name == "write_stdin":
                return await self._write(args, chat_session_id)
            _ListArgs.model_validate(args, strict=True)
            return await self._manager.list_sessions(chat_session_id)
        except (ValidationError, ValueError):
            return fail("tool_invalid_args", "Tool arguments are invalid")

    async def _exec(self, args: dict[str, Any], owner: UUID) -> ToolOutput:
        parsed = _ExecArgs.model_validate(args, strict=True)
        fields = parsed.model_fields_set
        timeout = parsed.timeout
        if timeout is None:
            timeout = (
                60
                if self._policy.shell_timeout_max == 0
                else min(60, self._policy.shell_timeout_max)
            )
        elif self._policy.shell_timeout_max > 0 and timeout > self._policy.shell_timeout_max:
            raise ValueError("timeout exceeds device policy")
        if timeout == 0 and (self._policy.shell_timeout_max != 0 or "yield_time_ms" not in fields):
            raise ValueError("unlimited timeout requires explicit yield and policy")
        if timeout > 60 and "yield_time_ms" not in fields:
            raise ValueError("long timeout requires explicit yield")
        shell = parsed.shell or self._policy.default_shell
        if shell not in self._policy.available_shells:
            return fail("tool_shell_unavailable", "Requested shell is unavailable")
        request = ExecStart(
            policy=self._policy,
            command=parsed.command,
            working_dir=parsed.working_dir,
            timeout_seconds=timeout,
            shell=shell,
            login=parsed.login,
            tty=parsed.tty,
            yield_time_ms=parsed.yield_time_ms if parsed.yield_time_ms is not None else 30_000,
            max_output_chars=parsed.max_output_chars,
        )
        return await self._manager.start(owner, request)

    async def _write(self, args: dict[str, Any], owner: UUID) -> ToolOutput:
        parsed = _WriteArgs.model_validate(args, strict=True)
        try:
            session_id = UUID(parsed.session_id)
        except ValueError as exc:
            raise ValueError("invalid session ID") from exc
        if session_id.version != 7:
            raise ValueError("session ID must be UUID v7")
        if parsed.wait_for is not None:
            yield_time = None
            wait_timeout = parsed.wait_timeout_ms if parsed.wait_timeout_ms is not None else 10_000
        else:
            yield_time = parsed.yield_time_ms if parsed.yield_time_ms is not None else 1000
            wait_timeout = None
        request = ExecWrite(
            session_id=session_id,
            chars=parsed.chars or None,
            terminate=parsed.terminate,
            yield_time_ms=yield_time,
            wait_for=parsed.wait_for,
            wait_timeout_ms=wait_timeout,
            max_output_chars=parsed.max_output_chars,
        )
        return await self._manager.write(owner, request)
