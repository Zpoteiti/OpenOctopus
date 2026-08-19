from __future__ import annotations

import asyncio
import codecs
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from openoctopus_client.process import (
    ProcessExit,
    ProcessHandle,
    ProcessSpec,
    build_argv,
    build_child_env,
    resolve_cwd,
    spawn_process,
)
from openoctopus_client.protocol import new_uuid7
from openoctopus_client.tools.common import ToolOutput, fail


@dataclass(frozen=True, slots=True)
class ExecPolicy:
    workspace: Path
    sandbox_mode: bool
    shell_timeout_max: int
    env_allowlist: tuple[str, ...]
    available_shells: tuple[str, ...]
    default_shell: str
    epoch: int


@dataclass(frozen=True, slots=True)
class ExecStart:
    policy: ExecPolicy
    command: str
    working_dir: str | None
    timeout_seconds: int
    shell: str
    login: bool
    tty: bool
    yield_time_ms: int
    max_output_chars: int


@dataclass(frozen=True, slots=True)
class ExecWrite:
    session_id: UUID
    chars: str | None
    terminate: bool
    yield_time_ms: int | None
    wait_for: str | None
    wait_timeout_ms: int | None
    max_output_chars: int


@dataclass(frozen=True, slots=True)
class BufferSnapshot:
    text: str
    dropped_chars: int
    total_dropped_chars: int
    response_truncated_chars: int
    truncated: bool


class HeadTailBuffer:
    """A bounded unread-text buffer that preserves both useful ends."""

    def __init__(self, *, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._head_limit = capacity // 2
        self._tail_limit = capacity - self._head_limit
        self._head = ""
        self._tail = ""
        self._unread_chars = 0
        self._total_dropped_chars = 0

    def append(self, text: str) -> None:
        if not text:
            return
        previous_dropped = max(0, self._unread_chars - self._capacity)
        if self._unread_chars <= self._capacity:
            combined = self._head + text
            if len(combined) <= self._capacity:
                self._head = combined
            else:
                self._head = combined[: self._head_limit]
                self._tail = combined[-self._tail_limit :]
        else:
            self._tail = (self._tail + text)[-self._tail_limit :]
        self._unread_chars += len(text)
        current_dropped = max(0, self._unread_chars - self._capacity)
        self._total_dropped_chars += current_dropped - previous_dropped

    def consume(self, *, max_chars: int) -> BufferSnapshot:
        if max_chars < 1:
            raise ValueError("max_chars must be positive")
        dropped = max(0, self._unread_chars - self._capacity)
        retained = self._head if self._unread_chars <= self._capacity else self._head + self._tail
        response_truncated = max(0, len(retained) - max_chars)
        if response_truncated:
            response_head = max_chars // 2
            response_tail = max_chars - response_head
            retained = retained[:response_head] + retained[-response_tail:]
        snapshot = BufferSnapshot(
            text=retained,
            dropped_chars=dropped,
            total_dropped_chars=self._total_dropped_chars,
            response_truncated_chars=response_truncated,
            truncated=bool(dropped or response_truncated),
        )
        self._head = ""
        self._tail = ""
        self._unread_chars = 0
        return snapshot

    def peek(self) -> str:
        """Return unread text without advancing the delivery cursor."""

        if self._unread_chars <= self._capacity:
            return self._head
        return self._head + self._tail

    def contains(self, expected: str) -> bool:
        """Search retained regions without treating the dropped gap as text."""

        if not expected:
            return True
        if expected in self._head:
            return True
        return self._unread_chars > self._capacity and expected in self._tail


class ProcessLauncher(Protocol):
    async def launch(self, spec: ProcessSpec) -> ProcessHandle: ...


class _DefaultProcessLauncher:
    async def launch(self, spec: ProcessSpec) -> ProcessHandle:
        return await spawn_process(spec)


@dataclass(slots=True)
class _ExecSession:
    session_id: UUID
    owner_chat: UUID
    request: ExecStart
    started_at: float
    last_access: float
    deadline: float | None
    cwd: Path | None = None
    terminal_at: float | None = None
    handle: ProcessHandle | None = None
    state: str = "starting"
    reason: str = ""
    exit: ProcessExit | None = None
    stdout: HeadTailBuffer | None = None
    stderr: HeadTailBuffer | None = None
    output: HeadTailBuffer | None = None
    stdout_decoder: Any = None
    stderr_decoder: Any = None
    output_decoder: Any = None
    stdout_task: asyncio.Task[None] | None = None
    stderr_task: asyncio.Task[None] | None = None
    output_task: asyncio.Task[None] | None = None
    waiter_task: asyncio.Task[None] | None = None
    timeout_task: asyncio.Task[None] | None = None
    spawn_claimed: bool = False
    spawn_done: asyncio.Future[None] | None = None
    terminal: asyncio.Future[None] | None = None
    lock: asyncio.Lock | None = None
    condition: asyncio.Condition | None = None
    terminal_control_truncated: bool = False
    cleanup_incomplete: bool = False


class ExecSessionManager:
    """Own the bounded, chat-scoped lifecycle of local shell processes."""

    MAX_SESSIONS = 8
    BUFFER_CHARS = 50_000
    IDLE_SECONDS = 30 * 60
    TERMINAL_RETAIN_SECONDS = 30 * 60
    TERMINATE_TIMEOUT_SECONDS = 3.0
    REAP_TIMEOUT_SECONDS = 1.0
    READ_CHUNK = 64 * 1024

    def __init__(
        self,
        launcher: ProcessLauncher | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        max_sessions: int = MAX_SESSIONS,
        idle_seconds: float = IDLE_SECONDS,
    ) -> None:
        self._launcher = launcher or _DefaultProcessLauncher()
        self._clock = clock
        self._max_sessions = max_sessions
        self._idle_seconds = idle_seconds
        self._sessions: dict[UUID, _ExecSession] = {}
        self._admission = asyncio.Lock()
        self._policy_epoch: int | None = None
        self._policy_transition = False
        self._shutting_down = False
        self._cleanup_task: asyncio.Task[None] | None = None

    async def start(self, owner_chat: UUID, request: ExecStart) -> ToolOutput:
        now = self._clock()
        session = _ExecSession(
            session_id=new_uuid7(),
            owner_chat=owner_chat,
            request=request,
            started_at=now,
            last_access=now,
            deadline=None,
            stdout=HeadTailBuffer(capacity=self.BUFFER_CHARS),
            stderr=HeadTailBuffer(capacity=self.BUFFER_CHARS),
            output=HeadTailBuffer(capacity=self.BUFFER_CHARS),
            stdout_decoder=codecs.getincrementaldecoder("utf-8")(errors="replace"),
            stderr_decoder=codecs.getincrementaldecoder("utf-8")(errors="replace"),
            output_decoder=codecs.getincrementaldecoder("utf-8")(errors="replace"),
        )
        loop = asyncio.get_running_loop()
        session.spawn_done = loop.create_future()
        session.terminal = loop.create_future()
        session.lock = asyncio.Lock()
        session.condition = asyncio.Condition(session.lock)
        async with self._admission:
            if self._shutting_down:
                return fail("tool_client_shutting_down", "Client is shutting down")
            if self._policy_transition:
                return fail("tool_device_busy", "Exec policy is changing")
            if self._policy_epoch is not None and request.policy.epoch != self._policy_epoch:
                return fail("tool_device_busy", "Exec policy is changing")
            if len(self._sessions) >= self._max_sessions:
                return fail(
                    "tool_device_busy",
                    f"Exec session capacity is full ({self._max_sessions}/{self._max_sessions})",
                )
            self._sessions[session.session_id] = session
        self._ensure_cleanup_task()

        try:
            cwd = await asyncio.to_thread(
                resolve_cwd, request.working_dir, request.policy.workspace
            )
            session.cwd = cwd
            argv = tuple(
                build_argv(
                    request.shell,
                    request.command,
                    login=request.login,
                    tty=request.tty,
                )
            )
            env = build_child_env(os.environ, request.policy.env_allowlist)
            startup_error = await self._claim_spawn(session)
            if startup_error is not None:
                await self._remove_unstarted(session.session_id)
                return startup_error
            launch_task = asyncio.create_task(
                self._launch(ProcessSpec(argv=argv, cwd=cwd, env=env, tty=request.tty))
            )
            try:
                handle = await asyncio.shield(launch_task)
            except asyncio.CancelledError:
                # The launcher may have crossed the OS spawn boundary.  Wait for
                # its answer and reap the handle before propagating cancellation.
                while True:
                    try:
                        handle = await asyncio.shield(launch_task)
                        break
                    except asyncio.CancelledError:
                        continue
                    except BaseException:
                        await self._remove_unstarted(session.session_id)
                        raise
                await self._terminate_cancellation_safe(handle)
                await self._remove_unstarted(session.session_id)
                raise
        except asyncio.CancelledError:
            await self._remove_unstarted(session.session_id)
            raise
        except Exception as exc:
            await self._remove_unstarted(session.session_id)
            code = getattr(exc, "code", "tool_exec_failed")
            message = {
                "tool_invalid_args": "Exec arguments are invalid",
                "tool_shell_unavailable": "Requested shell is unavailable",
                "tool_pty_unavailable": "TTY backend is unavailable",
            }.get(str(code), "Unable to start process")
            return fail(str(code), message)

        activation_task = asyncio.create_task(self._activate_spawned(session, handle))
        activation_cancelled = False
        while True:
            try:
                active = await asyncio.shield(activation_task)
                break
            except asyncio.CancelledError:
                activation_cancelled = True
        if not active:
            _, cancelled = await self._terminate_cancellation_safe(handle)
            await self._remove_unstarted(session.session_id)
            if cancelled or activation_cancelled:
                raise asyncio.CancelledError
            if session.reason == "policy_changed":
                return fail(
                    "tool_exec_failed",
                    "Exec policy changed before startup completed",
                )
            return fail(
                "tool_client_shutting_down", "Exec session was stopped before spawn completed"
            )
        if not request.tty:
            session.stdout_task = asyncio.create_task(
                self._reader(session, handle.stdout, "stdout")
            )
            session.stderr_task = asyncio.create_task(
                self._reader(session, handle.stderr, "stderr")
            )
        else:
            session.output_task = asyncio.create_task(
                self._reader(session, handle.output, "output")
            )
        session.waiter_task = asyncio.create_task(self._wait_for_exit(session, handle))
        if session.deadline is not None:
            session.timeout_task = asyncio.create_task(self._timeout(session, session.deadline))
        if activation_cancelled:
            raise asyncio.CancelledError
        try:
            await asyncio.wait_for(
                asyncio.shield(session.terminal), timeout=max(0, request.yield_time_ms) / 1000
            )
        except TimeoutError:
            if session.state == "running":
                return self._report(session, consume=True)
            if session.state == "terminating":
                try:
                    await asyncio.wait_for(
                        asyncio.shield(session.terminal),
                        timeout=self.TERMINATE_TIMEOUT_SECONDS + self.REAP_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    return self._report(session, consume=True)
        return await self._final_report(
            session,
            remove=session.state in {"exited", "terminated"} and not session.cleanup_incomplete,
        )

    async def write(self, owner_chat: UUID, request: ExecWrite) -> ToolOutput:
        session = await self._get(owner_chat, request.session_id)
        if session is None:
            return fail("tool_exec_session_not_found", "Exec session was not found")
        assert session.lock is not None and session.condition is not None
        should_wait_for = False
        terminated = False
        termination_succeeded = False
        async with session.lock:
            session.last_access = self._clock()
            if request.terminate:
                termination_succeeded = await self._terminate_locked(session, "terminated")
                terminated = True
            elif session.state != "running":
                pass
            elif request.chars:
                if not session.request.tty:
                    if request.chars != "\x03":
                        return fail(
                            "tool_exec_stdin_closed",
                            "pipe stdin is closed; restart with tty=true",
                        )
                    assert session.handle is not None
                    try:
                        interrupted = await asyncio.wait_for(session.handle.interrupt(), 5)
                    except (TimeoutError, OSError):
                        interrupted = False
                    if not interrupted:
                        return fail("tool_exec_interrupt_failed", "Unable to deliver interrupt")
                else:
                    assert session.handle is not None
                    try:
                        await asyncio.wait_for(
                            session.handle.write(request.chars.encode("utf-8")), 5
                        )
                    except Exception:
                        return fail(
                            "tool_execution_outcome_unknown",
                            "Input delivery outcome is unknown",
                        )
            should_wait_for = request.wait_for is not None and session.state == "running"

        if terminated:
            return await self._final_report(
                session,
                remove=termination_succeeded and not session.cleanup_incomplete,
                max_chars=request.max_output_chars,
            )

        if session.state != "running":
            return await self._final_report(
                session,
                remove=session.state in {"exited", "terminated"} and not session.cleanup_incomplete,
                max_chars=request.max_output_chars,
            )

        if should_wait_for:
            timeout_ms = 10_000 if request.wait_timeout_ms is None else request.wait_timeout_ms
            timeout = timeout_ms / 1000
            try:
                await asyncio.wait_for(
                    self._wait_for_text(session, request.wait_for or ""), timeout=timeout
                )
            except TimeoutError:
                pass
        elif request.yield_time_ms:
            try:
                assert session.terminal is not None
                await asyncio.wait_for(
                    asyncio.shield(session.terminal), request.yield_time_ms / 1000
                )
            except TimeoutError:
                pass
        if session.state == "running":
            return self._report(session, consume=True, max_chars=request.max_output_chars)
        return await self._final_report(
            session,
            remove=session.state in {"exited", "terminated"} and not session.cleanup_incomplete,
            max_chars=request.max_output_chars,
        )

    async def list_sessions(self, owner_chat: UUID) -> ToolOutput:
        async with self._admission:
            sessions = [s for s in self._sessions.values() if s.owner_chat == owner_chat]
        lines: list[str] = []
        now = self._clock()
        for session in sessions:
            remaining = (
                "unlimited"
                if session.deadline is None
                else f"{max(0, session.deadline - now):.1f}s"
            )
            preview = " ".join(session.request.command.split())[:200]
            lines.append(
                " ".join(
                    [
                        f"session_id={session.session_id}",
                        f"status={session.state}",
                        f"tty={str(session.request.tty).lower()}",
                        f"shell={session.request.shell}",
                        f"login={str(session.request.login).lower()}",
                        f"cwd={session.cwd or session.request.policy.workspace}",
                        f"elapsed={now - session.started_at:.1f}s",
                        f"idle={now - session.last_access:.1f}s",
                        f"timeout_remaining={remaining}",
                        f"command={preview}",
                    ]
                )
            )
        return ToolOutput(("\n".join(lines) or "(no exec sessions)")[:16_000])

    async def apply_policy(self, policy: ExecPolicy) -> None:
        async with self._admission:
            if not self._policy_transition and self._policy_epoch == policy.epoch:
                return
            self._policy_transition = True
            sessions = [
                s for s in self._sessions.values() if s.request.policy.epoch != policy.epoch
            ]
        converged = await asyncio.gather(*(self._terminate(s, "policy_changed") for s in sessions))
        if not all(converged):
            raise RuntimeError("old exec sessions did not terminate")
        async with self._admission:
            self._policy_epoch = policy.epoch
            self._policy_transition = False

    async def cleanup_idle(self) -> None:
        now = self._clock()
        async with self._admission:
            sessions = list(self._sessions.values())
        for session in sessions:
            if session.state == "running":
                if now - session.last_access < self._idle_seconds:
                    continue
                await self._terminate(session, "idle_timeout")
            elif session.state == "terminating":
                if now - session.last_access < self._idle_seconds:
                    continue
                await self._terminate(session, "idle_timeout")
            elif (
                session.terminal_at is not None
                and now - session.terminal_at >= self.TERMINAL_RETAIN_SECONDS
            ):
                await self._remove(session.session_id)

    async def shutdown(self) -> bool:
        async with self._admission:
            self._shutting_down = True
            sessions = list(self._sessions.values())
        outcomes = await asyncio.gather(
            *(self._terminate(s, "client_shutdown") for s in sessions),
            return_exceptions=True,
        )
        complete = all(outcome is True for outcome in outcomes)
        await asyncio.gather(
            *(self._remove(s.session_id) for s in sessions), return_exceptions=True
        )
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            await asyncio.gather(self._cleanup_task, return_exceptions=True)
            self._cleanup_task = None
        return complete

    async def _launch(self, spec: ProcessSpec) -> ProcessHandle:
        return await self._launcher.launch(spec)

    async def _claim_spawn(self, session: _ExecSession) -> ToolOutput | None:
        async with self._admission:
            if self._shutting_down:
                return fail("tool_client_shutting_down", "Client is shutting down")
            active = (
                not self._policy_transition
                and self._sessions.get(session.session_id) is session
                and session.state == "starting"
                and (
                    self._policy_epoch is None or session.request.policy.epoch == self._policy_epoch
                )
            )
            if not active:
                return fail(
                    "tool_exec_failed",
                    "Exec policy changed before startup completed",
                )
            session.spawn_claimed = True
            return None

    async def _activate_spawned(
        self,
        session: _ExecSession,
        handle: ProcessHandle,
    ) -> bool:
        async with self._admission:
            active = (
                not self._shutting_down
                and session.session_id in self._sessions
                and session.state == "starting"
            )
            if not active:
                return False
            started_at = self._clock()
            session.started_at = started_at
            session.last_access = started_at
            session.deadline = (
                started_at + session.request.timeout_seconds
                if session.request.timeout_seconds
                else None
            )
            session.handle = handle
            session.state = "running"
            assert session.spawn_done is not None
            session.spawn_done.set_result(None)
            return True

    @staticmethod
    async def _terminate_cancellation_safe(
        handle: ProcessHandle,
    ) -> tuple[ProcessExit, bool]:
        task = asyncio.create_task(handle.terminate())
        cancelled = False
        while True:
            try:
                return await asyncio.shield(task), cancelled
            except asyncio.CancelledError:
                if task.done():
                    return task.result(), cancelled
                cancelled = True

    def _ensure_cleanup_task(self) -> None:
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def _cleanup_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(30)
                await self.cleanup_idle()
        except asyncio.CancelledError:
            return

    async def _remove_unstarted(self, session_id: UUID) -> None:
        async with self._admission:
            session = self._sessions.pop(session_id, None)
        if session is not None and session.spawn_done is not None and not session.spawn_done.done():
            session.spawn_done.set_result(None)

    async def _get(self, owner_chat: UUID, session_id: UUID) -> _ExecSession | None:
        async with self._admission:
            session = self._sessions.get(session_id)
            if session is None or session.owner_chat != owner_chat:
                return None
            return session

    async def _reader(self, session: _ExecSession, reader: Any, stream: str) -> None:
        decoder = getattr(session, f"{stream}_decoder")
        buffer = getattr(session, stream)
        assert isinstance(buffer, HeadTailBuffer)
        try:
            while True:
                chunk = await reader.read(self.READ_CHUNK)
                if not chunk:
                    break
                buffer.append(decoder.decode(chunk, final=False))
                assert session.condition is not None
                async with session.condition:
                    session.condition.notify_all()
            tail = decoder.decode(b"", final=True)
            if tail:
                buffer.append(tail)
                assert session.condition is not None
                async with session.condition:
                    session.condition.notify_all()
        except asyncio.CancelledError:
            return
        except Exception:
            await self._terminate(session, "reader_failed")

    async def _wait_for_exit(self, session: _ExecSession, handle: ProcessHandle) -> None:
        try:
            session.exit = await handle.wait()
            if session.state == "running":
                session.state = "exited"
                session.reason = "exit"
                session.terminal_at = self._clock()
            if session.terminal is not None and not session.terminal.done():
                session.terminal.set_result(None)
            if session.condition is not None:
                async with session.condition:
                    session.condition.notify_all()
        except asyncio.CancelledError:
            return
        except Exception:
            session.state = "terminating"
            session.reason = "termination_failed"
            session.cleanup_incomplete = True
            session.terminal_at = None
            await self._terminate(session, "wait_failed")

    async def _timeout(self, session: _ExecSession, deadline: float) -> None:
        delay = max(0, deadline - self._clock())
        await asyncio.sleep(delay)
        if session.state == "running":
            await self._terminate(session, "timeout")

    async def _wait_for_text(self, session: _ExecSession, expected: str) -> None:
        assert session.condition is not None
        while session.state == "running":
            if self._contains(session, expected):
                return
            async with session.condition:
                if self._contains(session, expected):
                    return
                await session.condition.wait()
        if self._contains(session, expected):
            return

    @staticmethod
    def _contains(session: _ExecSession, expected: str) -> bool:
        if session.request.tty:
            return session.output.contains(expected) if session.output is not None else False
        return (session.stdout.contains(expected) if session.stdout is not None else False) or (
            session.stderr.contains(expected) if session.stderr is not None else False
        )

    async def _terminate(self, session: _ExecSession, reason: str) -> bool:
        assert session.lock is not None
        async with session.lock:
            return await self._terminate_locked(session, reason)

    @staticmethod
    async def _bounded_process_call(
        operation: Callable[[], Awaitable[ProcessExit]], timeout: float
    ) -> tuple[ProcessExit | None, bool, bool]:
        task: asyncio.Future[ProcessExit] = asyncio.ensure_future(operation())
        deadline = asyncio.get_running_loop().time() + timeout
        cancelled = False
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                return None, False, cancelled
            try:
                result = await asyncio.wait_for(asyncio.shield(task), remaining)
                return result, True, cancelled
            except asyncio.CancelledError:
                cancelled = True
                if task.done():
                    try:
                        return task.result(), True, cancelled
                    except BaseException:
                        return None, False, cancelled
            except Exception:
                return None, False, cancelled

    async def _terminate_locked(self, session: _ExecSession, reason: str) -> bool:
        if session.state in {"exited", "terminated"} and not session.cleanup_incomplete:
            if session.reason == "":
                session.reason = reason
            return True
        defer_fence = (
            session.spawn_claimed
            and session.spawn_done is not None
            and not session.spawn_done.done()
        )
        if not defer_fence:
            session.state = "terminating"
        session.reason = reason
        # Cleanup remains unproven while terminate/reap is in flight.  A
        # concurrent yield must never report a terminating session as clean.
        session.cleanup_incomplete = True
        cancelled = False
        if session.spawn_done is not None and not session.spawn_done.done():
            while True:
                try:
                    await asyncio.shield(session.spawn_done)
                    break
                except asyncio.CancelledError:
                    cancelled = True
        session.state = "terminating"
        converged = session.handle is None
        if session.handle is not None:
            terminated_exit, terminated_ok, terminate_cancelled = await self._bounded_process_call(
                session.handle.terminate, self.TERMINATE_TIMEOUT_SECONDS
            )
            cancelled = cancelled or terminate_cancelled
            reaped_exit, reaped_ok, reap_cancelled = await self._bounded_process_call(
                session.handle.wait, self.REAP_TIMEOUT_SECONDS
            )
            cancelled = cancelled or reap_cancelled
            session.exit = reaped_exit or terminated_exit
            converged = (
                terminated_ok
                and reaped_ok
                and not bool(getattr(session.handle, "cleanup_incomplete", False))
            )
        session.cleanup_incomplete = not converged
        if converged:
            session.state = "terminated"
            session.reason = reason
            session.terminal_at = self._clock()
        else:
            session.state = "terminating"
            session.reason = "termination_failed"
            session.cleanup_incomplete = True
            session.terminal_at = None
            session.last_access = self._clock()
        if session.terminal is not None and not session.terminal.done():
            session.terminal.set_result(None)
        if session.condition is not None:
            session.condition.notify_all()
        if cancelled:
            raise asyncio.CancelledError
        return converged

    def _report(
        self, session: _ExecSession, *, consume: bool, max_chars: int | None = None
    ) -> ToolOutput:
        limit = max_chars or session.request.max_output_chars
        out_snap: BufferSnapshot | None = None
        err_snap: BufferSnapshot | None = None
        stdout_response = 0
        stderr_response = 0
        if session.request.tty:
            snap = session.output.consume(max_chars=limit) if consume and session.output else None
            stdout = stderr = ""
            output = snap.text if snap else (session.output.peek() if session.output else "")
            trunc = snap
            output_dropped = snap.dropped_chars if snap else 0
            output_total_dropped = snap.total_dropped_chars if snap else 0
            output_response_truncated = snap.response_truncated_chars if snap else 0
        else:
            out_snap = (
                session.stdout.consume(max_chars=self.BUFFER_CHARS)
                if consume and session.stdout
                else None
            )
            err_snap = (
                session.stderr.consume(max_chars=self.BUFFER_CHARS)
                if consume and session.stderr
                else None
            )
            raw_stdout = (
                out_snap.text if out_snap else (session.stdout.peek() if session.stdout else "")
            )
            raw_stderr = (
                err_snap.text if err_snap else (session.stderr.peek() if session.stderr else "")
            )
            stdout, stderr, stdout_response, stderr_response = self._aggregate_output(
                raw_stdout, raw_stderr, limit
            )
            output = ""
            trunc = None
            output_dropped = 0
            output_total_dropped = 0
            output_response_truncated = stdout_response + stderr_response
        exit_code = session.exit.returncode if session.exit else "null"
        signal = session.exit.signal if session.exit else "null"
        reason = session.reason or ("running" if session.state == "running" else "")
        if session.handle is not None:
            session.terminal_control_truncated = bool(
                getattr(session.handle, "terminal_control_truncated", False)
            )
            session.cleanup_incomplete = session.cleanup_incomplete or bool(
                getattr(session.handle, "cleanup_incomplete", False)
            )
        content = "\n".join(
            [
                f"session_id={session.session_id}",
                f"status={session.state}",
                f"tty={str(session.request.tty).lower()}",
                f"shell={session.request.shell}",
                f"login={str(session.request.login).lower()}",
                f"exit_code={exit_code}",
                f"signal={signal}",
                f"reason={reason}",
                f"elapsed_ms={int((self._clock() - session.started_at) * 1000)}",
                f"cwd={session.cwd or session.request.policy.workspace}",
                f"stdout={stdout}",
                f"stderr={stderr}",
                f"output={output}",
            ]
        )
        if trunc is not None:
            content += "\n".join(
                [
                    "",
                    f"output_truncated={str(trunc.truncated).lower()}",
                    f"output_dropped_chars={output_dropped}",
                    f"output_total_dropped_chars={output_total_dropped}",
                    f"response_truncated_chars={output_response_truncated}",
                    f"terminal_control_truncated={str(session.terminal_control_truncated).lower()}",
                    f"cleanup_incomplete={str(session.cleanup_incomplete).lower()}",
                ]
            )
        elif out_snap is not None and err_snap is not None:
            content += (
                "\nstdout_truncated="
                f"{str(bool(out_snap.dropped_chars or stdout_response)).lower()}\n"
                f"stderr_truncated={str(bool(err_snap.dropped_chars or stderr_response)).lower()}\n"
                f"stdout_dropped_chars={out_snap.dropped_chars}\n"
                f"stderr_dropped_chars={err_snap.dropped_chars}\n"
                f"stdout_total_dropped_chars={out_snap.total_dropped_chars}\n"
                f"stderr_total_dropped_chars={err_snap.total_dropped_chars}\n"
                "response_truncated_chars="
                f"{output_response_truncated}\n"
                f"terminal_control_truncated={str(session.terminal_control_truncated).lower()}\n"
                f"cleanup_incomplete={str(session.cleanup_incomplete).lower()}"
            )
        failed_exit = session.exit is not None and (
            session.exit.returncode not in (None, 0) or session.exit.signal is not None
        )
        error_code = None
        successful_termination = session.reason in {
            "terminated",
            "client_shutdown",
            "idle_timeout",
        }
        if session.cleanup_incomplete or session.state == "terminating":
            error_code = "tool_exec_failed"
        elif (failed_exit and not successful_termination) or (
            session.state == "terminated" and not successful_termination
        ):
            error_code = "tool_exec_timeout" if session.reason == "timeout" else "tool_exec_failed"
        return ToolOutput(
            content,
            is_error=error_code is not None,
            code=error_code,
        )

    @staticmethod
    def _fit_text(text: str, limit: int) -> str:
        if limit <= 0:
            return ""
        if len(text) <= limit:
            return text
        head = limit // 2
        return text[:head] + text[-(limit - head) :]

    @classmethod
    def _aggregate_output(cls, stdout: str, stderr: str, limit: int) -> tuple[str, str, int, int]:
        if len(stdout) + len(stderr) <= limit:
            return stdout, stderr, 0, 0
        stdout_budget = min(len(stdout), (limit + 1) // 2)
        stderr_budget = min(len(stderr), limit - stdout_budget)
        remaining = limit - stdout_budget - stderr_budget
        if remaining:
            stdout_budget = min(len(stdout), stdout_budget + remaining)
        fitted_stdout = cls._fit_text(stdout, stdout_budget)
        fitted_stderr = cls._fit_text(stderr, stderr_budget)
        return (
            fitted_stdout,
            fitted_stderr,
            len(stdout) - len(fitted_stdout),
            len(stderr) - len(fitted_stderr),
        )

    async def _final_report(
        self, session: _ExecSession, *, remove: bool, max_chars: int | None = None
    ) -> ToolOutput:
        readers = [
            task
            for task in (session.stdout_task, session.stderr_task, session.output_task)
            if task is not None and task is not asyncio.current_task()
        ]
        if readers:
            try:
                await asyncio.wait_for(asyncio.gather(*readers, return_exceptions=True), 1)
            except TimeoutError:
                for task in readers:
                    task.cancel()
        result = self._report(session, consume=True, max_chars=max_chars)
        if remove:
            await self._remove(session.session_id)
        return result

    async def _remove(self, session_id: UUID) -> None:
        async with self._admission:
            session = self._sessions.pop(session_id, None)
        if session is None:
            return
        tasks = [
            session.stdout_task,
            session.stderr_task,
            session.output_task,
            session.waiter_task,
            session.timeout_task,
        ]
        owned = [task for task in tasks if task is not None and task is not asyncio.current_task()]
        for task in owned:
            task.cancel()
        if owned:
            await asyncio.gather(*owned, return_exceptions=True)
