from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest

from openoctopus_client.exec_sessions import (
    ExecPolicy,
    ExecSessionManager,
    ExecStart,
    ExecWrite,
    HeadTailBuffer,
)
from openoctopus_client.process import ProcessExit, ProcessHandle, ProcessSpec
from openoctopus_client.tools.common import ToolOutput

CHAT_ID = UUID("0190d5a7-0000-7000-8000-000000000003")
OTHER_CHAT_ID = UUID("0190d5a7-0000-7000-8000-000000000005")
TEST_SHELL = "cmd" if os.name == "nt" else "sh"
TEST_COMMAND = "echo test"


class _Reader:
    def __init__(self) -> None:
        self.chunks: asyncio.Queue[bytes | None] = asyncio.Queue()

    async def read(self, n: int = -1) -> bytes:
        del n
        chunk = await self.chunks.get()
        return b"" if chunk is None else chunk


class _Handle(ProcessHandle):
    def __init__(self, *, tty: bool = False) -> None:
        self.pid = 100
        self.tty = tty
        self.stdout = _Reader()
        self.stderr = _Reader()
        self.output = self.stdout
        self.exit: asyncio.Future[ProcessExit] = asyncio.get_running_loop().create_future()
        self.writes: list[bytes] = []
        self.interrupts = 0
        self.terminated = False
        self.terminal_control_truncated = False
        self.cleanup_incomplete = False

    async def wait(self) -> ProcessExit:
        return await self.exit

    async def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def interrupt(self) -> bool:
        self.interrupts += 1
        return True

    async def terminate(self) -> ProcessExit:
        self.terminated = True
        if not self.exit.done():
            self.exit.set_result(ProcessExit(-15, 15))
        return await self.exit


class _PartialWriteHandle(_Handle):
    async def write(self, data: bytes) -> None:
        self.writes.append(data[:1])
        raise OSError("write failed after a partial write")


class _CleanupIncompleteOnWaitHandle(_Handle):
    async def wait(self) -> ProcessExit:
        result = await super().wait()
        self.cleanup_incomplete = True
        return result


class _CleanupIncompleteSpawnHandle(_Handle):
    def __init__(self) -> None:
        super().__init__()
        self.mark_cleanup_incomplete = True

    async def terminate(self) -> ProcessExit:
        result = await super().terminate()
        self.cleanup_incomplete = self.mark_cleanup_incomplete
        return result


class _BlockingWriteHandle(_Handle):
    def __init__(self) -> None:
        super().__init__(tty=True)
        self.write_started = asyncio.Event()
        self.release_write = asyncio.Event()

    async def write(self, data: bytes) -> None:
        self.write_started.set()
        await self.release_write.wait()
        await super().write(data)


class _TerminateFailureHandle(_Handle):
    def __init__(self) -> None:
        super().__init__()
        self.fail_terminate = True
        self.fail_wait_after_terminate = True
        self.terminate_calls = 0

    async def wait(self) -> ProcessExit:
        if self.fail_wait_after_terminate and self.terminate_calls:
            raise RuntimeError("wait backend failed")
        return await super().wait()

    async def terminate(self) -> ProcessExit:
        self.terminate_calls += 1
        if self.fail_terminate:
            raise RuntimeError("terminate backend failed")
        return await super().terminate()


class _WaitFailureHandle(_Handle):
    async def wait(self) -> ProcessExit:
        raise RuntimeError("wait backend failed")


class _TransientWaitFailureHandle(_Handle):
    def __init__(self) -> None:
        super().__init__()
        self.wait_calls = 0

    async def wait(self) -> ProcessExit:
        self.wait_calls += 1
        if self.wait_calls == 1:
            raise RuntimeError("wait backend failed once")
        return await super().wait()


class _Launcher:
    def __init__(self) -> None:
        self.handles: list[_Handle] = []
        self.specs: list[ProcessSpec] = []

    async def launch(self, spec: ProcessSpec) -> ProcessHandle:
        handle = _Handle(tty=spec.tty)
        self.handles.append(handle)
        self.specs.append(spec)
        return handle


class _BlockingTerminateHandle(_Handle):
    def __init__(self, gate: asyncio.Event) -> None:
        super().__init__()
        self.gate = gate

    async def terminate(self) -> ProcessExit:
        await self.gate.wait()
        return await super().terminate()


class _DelayedLauncher:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.handle: _Handle | None = None

    async def launch(self, spec: ProcessSpec) -> ProcessHandle:
        del spec
        self.started.set()
        await self.release.wait()
        self.handle = _Handle()
        return self.handle


class _DelayedLauncherWithBlockingTerminate:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release_spawn = asyncio.Event()
        self.terminate_started = asyncio.Event()
        self.release_terminate = asyncio.Event()
        self.handle: _Handle | None = None

    async def launch(self, spec: ProcessSpec) -> ProcessHandle:
        del spec
        self.started.set()
        await self.release_spawn.wait()

        owner = self

        class Handle(_Handle):
            async def terminate(self) -> ProcessExit:
                owner.terminate_started.set()
                await owner.release_terminate.wait()
                return await super().terminate()

        self.handle = Handle()
        return self.handle


class _DelayedLauncherWithHandle:
    def __init__(self, handle: _Handle) -> None:
        self.started = asyncio.Event()
        self.release_spawn = asyncio.Event()
        self.handle = handle

    async def launch(self, spec: ProcessSpec) -> ProcessHandle:
        del spec
        self.started.set()
        await self.release_spawn.wait()
        return self.handle


class _FailingLauncher:
    async def launch(self, spec: ProcessSpec) -> ProcessHandle:
        del spec
        raise OSError("/secret/private/workspace")


def _session_id(result: ToolOutput) -> UUID:
    content = cast(str, result.content)
    return UUID(content.split("session_id=", 1)[1].split("\n", 1)[0])


def _request(*, tty: bool = False, timeout: int = 60, yield_ms: int = 1) -> ExecStart:
    policy = ExecPolicy(
        workspace=Path("/workspace"),
        restrict_to_workspace=False,
        shell_timeout_max=600,
        env_allowlist=("PATH",),
        available_shells=(TEST_SHELL,),
        default_shell=TEST_SHELL,
        epoch=1,
    )
    return ExecStart(
        policy=policy,
        command=TEST_COMMAND,
        working_dir=None,
        timeout_seconds=timeout,
        shell=TEST_SHELL,
        login=False,
        tty=tty,
        yield_time_ms=yield_ms,
        max_output_chars=10_000,
    )


def test_head_tail_buffer_bounds_output_and_reports_dropped_characters() -> None:
    buffer = HeadTailBuffer(capacity=10)

    buffer.append("0123456789abcdef")
    snapshot = buffer.consume(max_chars=10)

    assert snapshot.text == "01234bcdef"
    assert snapshot.dropped_chars == 6
    assert snapshot.total_dropped_chars == 6
    assert snapshot.truncated is True
    assert snapshot.response_truncated_chars == 0


def test_head_tail_buffer_consumption_is_at_most_once() -> None:
    buffer = HeadTailBuffer(capacity=10)
    buffer.append("first")

    assert buffer.consume(max_chars=10).text == "first"
    assert buffer.consume(max_chars=10).text == ""

    buffer.append("second")
    assert buffer.consume(max_chars=10).text == "second"


def test_head_tail_buffer_does_not_match_across_dropped_gap() -> None:
    buffer = HeadTailBuffer(capacity=10)
    buffer.append("0123456789abcdef")

    assert buffer.contains("012") is True
    assert buffer.contains("cde") is True
    assert buffer.contains("4b") is False


def test_head_tail_buffer_applies_response_limit_without_retaining_hidden_text() -> None:
    buffer = HeadTailBuffer(capacity=10)
    buffer.append("0123456789")

    snapshot = buffer.consume(max_chars=6)

    assert snapshot.text == "012789"
    assert snapshot.response_truncated_chars == 4
    assert snapshot.truncated is True
    assert buffer.consume(max_chars=10).text == ""


def test_head_tail_buffer_tracks_lifetime_drops_across_polls() -> None:
    buffer = HeadTailBuffer(capacity=4)
    buffer.append("abcdef")
    first = buffer.consume(max_chars=4)
    buffer.append("ghijkl")
    second = buffer.consume(max_chars=4)

    assert first.dropped_chars == 2
    assert first.total_dropped_chars == 2
    assert second.dropped_chars == 2
    assert second.total_dropped_chars == 4


def test_manager_reserves_eight_slots_atomically_and_rejects_ninth() -> None:
    async def run() -> None:
        launcher = _Launcher()
        manager = ExecSessionManager(launcher)
        results = await asyncio.gather(*(manager.start(CHAT_ID, _request()) for _ in range(8)))
        assert all(not result.is_error for result in results)
        ninth = await manager.start(CHAT_ID, _request())
        assert ninth.code == "tool_device_busy"
        assert len(launcher.handles) == 8
        await manager.shutdown()

    asyncio.run(run())


def test_hard_timeout_starts_only_after_process_spawn_succeeds() -> None:
    async def run() -> None:
        now = [0.0]

        class SlowLauncher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                now[0] = 100.0
                return await super().launch(spec)

        launcher = SlowLauncher()
        manager = ExecSessionManager(launcher, clock=lambda: now[0])

        started = await manager.start(CHAT_ID, _request(timeout=60))

        assert started.is_error is False
        assert "status=running" in cast(str, started.content)
        listed = await manager.list_sessions(CHAT_ID)
        assert "timeout_remaining=60.0s" in cast(str, listed.content)
        await manager.shutdown()

    asyncio.run(run())


def test_manager_hides_foreign_sessions_and_consumes_final_output_once() -> None:
    async def run() -> None:
        launcher = _Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request())
        session_id = _session_id(started)
        handle = launcher.handles[0]
        await handle.stdout.chunks.put(b"hello\n")
        await handle.stderr.chunks.put(None)
        await handle.stdout.chunks.put(None)
        handle.exit.set_result(ProcessExit(0))

        foreign = await manager.write(
            OTHER_CHAT_ID,
            ExecWrite(session_id, None, False, 1_000, None, None, 10_000),
        )
        assert foreign.code == "tool_exec_session_not_found"
        final = await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, False, 1_000, None, None, 10_000),
        )
        assert "stdout=hello\n" in cast(str, final.content)
        missing = await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, False, 1_000, None, None, 10_000),
        )
        assert missing.code == "tool_exec_session_not_found"
        await manager.shutdown()

    asyncio.run(run())


def test_manager_pipe_input_is_closed_but_tty_input_and_interrupt_work() -> None:
    async def run() -> None:
        launcher = _Launcher()
        manager = ExecSessionManager(launcher)
        pipe = await manager.start(CHAT_ID, _request())
        pipe_id = _session_id(pipe)
        rejected = await manager.write(
            CHAT_ID,
            ExecWrite(pipe_id, "text", False, 1, None, None, 10_000),
        )
        assert rejected.code == "tool_exec_stdin_closed"
        interrupted = await manager.write(
            CHAT_ID,
            ExecWrite(pipe_id, "\x03", False, 1, None, None, 10_000),
        )
        assert interrupted.is_error is False
        assert launcher.handles[0].interrupts == 1

        tty = await manager.start(CHAT_ID, _request(tty=True))
        tty_id = _session_id(tty)
        await manager.write(
            CHAT_ID,
            ExecWrite(tty_id, "python\n", False, 1, None, None, 10_000),
        )
        assert launcher.handles[1].writes == [b"python\n"]
        await manager.shutdown()

    asyncio.run(run())


def test_tty_write_failure_after_partial_delivery_has_unknown_outcome() -> None:
    async def run() -> None:
        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                handle = _PartialWriteHandle(tty=spec.tty)
                self.handles.append(handle)
                self.specs.append(spec)
                return handle

        launcher = Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request(tty=True))
        result = await manager.write(
            CHAT_ID,
            ExecWrite(_session_id(started), "python\n", False, 1, None, None, 10_000),
        )

        assert result.code == "tool_execution_outcome_unknown"
        assert launcher.handles[0].writes == [b"p"]
        await manager.shutdown()

    asyncio.run(run())


def test_wait_for_zero_polls_without_waiting_for_the_default_timeout() -> None:
    async def run() -> None:
        manager = ExecSessionManager(_Launcher())
        started = await manager.start(CHAT_ID, _request())
        task = asyncio.create_task(
            manager.write(
                CHAT_ID,
                ExecWrite(
                    _session_id(started),
                    None,
                    False,
                    1,
                    "never-produced",
                    0,
                    10_000,
                ),
            )
        )
        try:
            result = await asyncio.wait_for(task, 0.2)
        except TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            pytest.fail("wait_timeout_ms=0 used the default timeout")

        assert result.code is None
        await manager.shutdown()

    asyncio.run(run())


def test_failed_termination_reports_cleanup_incomplete_and_retains_session() -> None:
    async def run() -> None:
        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                handle = _TerminateFailureHandle()
                self.handles.append(handle)
                self.specs.append(spec)
                return handle

        launcher = Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request())
        session_id = _session_id(started)
        failed = await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, True, None, None, None, 10_000),
        )

        assert failed.code == "tool_exec_failed"
        assert "cleanup_incomplete=true" in cast(str, failed.content)
        listed = await manager.list_sessions(CHAT_ID)
        assert str(session_id) in cast(str, listed.content)

        handle = cast(_TerminateFailureHandle, launcher.handles[0])
        handle.fail_terminate = False
        handle.fail_wait_after_terminate = False
        succeeded = await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, True, None, None, None, 10_000),
        )
        assert succeeded.is_error is False
        missing = await manager.list_sessions(CHAT_ID)
        assert str(session_id) not in cast(str, missing.content)
        await manager.shutdown()

    asyncio.run(run())


def test_natural_wait_cleanup_incomplete_is_propagated_before_final_removal() -> None:
    async def run() -> None:
        handle = _CleanupIncompleteOnWaitHandle()

        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                del spec
                self.handles.append(handle)
                return handle

        manager = ExecSessionManager(Launcher())
        started = await manager.start(CHAT_ID, _request())
        session_id = _session_id(started)
        handle.exit.set_result(ProcessExit(0))

        final = await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, False, 1_000, None, None, 10_000),
        )

        assert final.code == "tool_exec_failed"
        assert "cleanup_incomplete=true" in cast(str, final.content)
        listed = await manager.list_sessions(CHAT_ID)
        assert str(session_id) in cast(str, listed.content)
        await manager.shutdown()

    asyncio.run(run())


def test_cancelled_spawn_with_terminate_failure_retains_cleanup_session() -> None:
    async def run() -> None:
        handle = _TerminateFailureHandle()
        launcher = _DelayedLauncherWithHandle(handle)
        manager = ExecSessionManager(launcher)
        task = asyncio.create_task(manager.start(CHAT_ID, _request()))
        await launcher.started.wait()
        task.cancel()
        launcher.release_spawn.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        session_id = next(iter(manager._sessions))
        listed = await manager.list_sessions(CHAT_ID)
        content = cast(str, listed.content)
        assert str(session_id) in content
        assert "status=terminating" in content
        assert len(manager._sessions) == 1

        handle.fail_terminate = False
        handle.fail_wait_after_terminate = False
        await manager.cleanup_idle()
        assert len(manager._sessions) == 1
        await manager.shutdown()

    asyncio.run(run())


def test_cancelled_spawn_with_cleanup_incomplete_handle_is_retryable() -> None:
    async def run() -> None:
        handle = _CleanupIncompleteSpawnHandle()
        launcher = _DelayedLauncherWithHandle(handle)
        manager = ExecSessionManager(launcher, idle_seconds=0)
        task = asyncio.create_task(manager.start(CHAT_ID, _request()))
        await launcher.started.wait()
        task.cancel()
        launcher.release_spawn.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        session_id = next(iter(manager._sessions))
        assert manager._sessions[session_id].cleanup_incomplete is True
        handle.mark_cleanup_incomplete = False
        await manager.cleanup_idle()
        assert manager._sessions[session_id].cleanup_incomplete is False
        await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, False, None, None, None, 10_000),
        )
        assert session_id not in manager._sessions
        await manager.shutdown()

    asyncio.run(run())


def test_shutdown_can_finish_while_tty_write_is_in_flight() -> None:
    async def run() -> None:
        handle = _BlockingWriteHandle()

        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                del spec
                self.handles.append(handle)
                return handle

        manager = ExecSessionManager(Launcher())
        started = await manager.start(CHAT_ID, _request(tty=True))
        write_task = asyncio.create_task(
            manager.write(
                CHAT_ID,
                ExecWrite(_session_id(started), "input\n", False, None, None, None, 10_000),
            )
        )
        await handle.write_started.wait()

        shutdown = asyncio.create_task(manager.shutdown())
        assert await asyncio.wait_for(asyncio.shield(shutdown), 0.2) is True
        assert handle.terminated is True

        handle.release_write.set()
        await write_task

    asyncio.run(run())


def test_wait_backend_failure_reports_failure_and_retains_session() -> None:
    async def run() -> None:
        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                handle = _WaitFailureHandle()
                self.handles.append(handle)
                self.specs.append(spec)
                return handle

        launcher = Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request())

        assert started.code == "tool_exec_failed"
        assert "cleanup_incomplete=true" in cast(str, started.content)
        listed = await manager.list_sessions(CHAT_ID)
        assert "status=terminating" in cast(str, listed.content)
        await manager.shutdown()

    asyncio.run(run())


def test_wait_backend_failure_auto_reaps_and_preserves_failure_reason() -> None:
    async def run() -> None:
        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                handle = _TransientWaitFailureHandle()
                self.handles.append(handle)
                self.specs.append(spec)
                return handle

        launcher = Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request())

        content = cast(str, started.content)
        assert started.code == "tool_exec_failed"
        assert "status=terminated" in content
        assert "reason=wait_failed" in content
        assert "cleanup_incomplete=false" in content
        assert await manager.list_sessions(CHAT_ID) == ToolOutput("(no exec sessions)")
        await manager.shutdown()

    asyncio.run(run())


def test_failed_termination_is_retried_after_idle_without_a_busy_loop() -> None:
    async def run() -> None:
        now = [0.0]

        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                handle = _TerminateFailureHandle()
                self.handles.append(handle)
                self.specs.append(spec)
                return handle

        launcher = Launcher()
        manager = ExecSessionManager(launcher, clock=lambda: now[0])
        started = await manager.start(CHAT_ID, _request())
        session_id = _session_id(started)
        failed = await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, True, None, None, None, 10_000),
        )
        assert failed.code == "tool_exec_failed"
        handle = cast(_TerminateFailureHandle, launcher.handles[0])
        assert handle.terminate_calls == 1

        now[0] = manager.IDLE_SECONDS
        await manager.cleanup_idle()
        assert handle.terminate_calls == 2
        await manager.cleanup_idle()
        assert handle.terminate_calls == 2

        handle.fail_terminate = False
        handle.fail_wait_after_terminate = False
        now[0] += manager.IDLE_SECONDS
        await manager.cleanup_idle()
        assert handle.terminate_calls == 3
        listed = await manager.list_sessions(CHAT_ID)
        assert "status=terminated" in cast(str, listed.content)
        await manager.shutdown()

    asyncio.run(run())


def test_shutdown_reports_incomplete_when_process_cleanup_cannot_converge() -> None:
    async def run() -> None:
        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                handle = _TerminateFailureHandle()
                self.handles.append(handle)
                self.specs.append(spec)
                return handle

        launcher = Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request())
        session_id = _session_id(started)

        assert await manager.shutdown() is False
        listed = await manager.list_sessions(CHAT_ID)
        assert str(session_id) in cast(str, listed.content)
        assert "status=terminating" in cast(str, listed.content)

    asyncio.run(run())


def test_policy_transition_blocks_new_start_until_old_sessions_are_terminated() -> None:
    async def run() -> None:
        gate = asyncio.Event()

        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                handle = _BlockingTerminateHandle(gate)
                self.handles.append(handle)
                self.specs.append(spec)
                return handle

        launcher = Launcher()
        manager = ExecSessionManager(launcher)
        old = await manager.start(CHAT_ID, _request())
        new_policy = _request().policy.__class__(
            workspace=Path("/workspace"),
            restrict_to_workspace=False,
            shell_timeout_max=600,
            env_allowlist=("PATH",),
            available_shells=(TEST_SHELL,),
            default_shell=TEST_SHELL,
            epoch=2,
        )
        transition = asyncio.create_task(manager.apply_policy(new_policy))
        await asyncio.sleep(0)
        busy = await manager.start(
            CHAT_ID,
            _request().__class__(
                policy=new_policy,
                command=TEST_COMMAND,
                working_dir=None,
                timeout_seconds=60,
                shell=TEST_SHELL,
                login=False,
                tty=False,
                yield_time_ms=1,
                max_output_chars=10_000,
            ),
        )
        assert busy.code == "tool_device_busy"
        gate.set()
        await transition
        assert old.is_error is False
        await manager.apply_policy(new_policy)
        await manager.shutdown()

    asyncio.run(run())


def test_policy_transition_stays_fenced_until_old_session_cleanup_converges() -> None:
    async def run() -> None:
        class Launcher(_Launcher):
            async def launch(self, spec: ProcessSpec) -> ProcessHandle:
                handle = _TerminateFailureHandle()
                self.handles.append(handle)
                self.specs.append(spec)
                return handle

        launcher = Launcher()
        manager = ExecSessionManager(launcher)
        await manager.start(CHAT_ID, _request())
        old_handle = cast(_TerminateFailureHandle, launcher.handles[0])
        new_policy = ExecPolicy(
            workspace=Path("/workspace"),
            restrict_to_workspace=False,
            shell_timeout_max=600,
            env_allowlist=("PATH",),
            available_shells=(TEST_SHELL,),
            default_shell=TEST_SHELL,
            epoch=2,
        )
        new_request = ExecStart(
            policy=new_policy,
            command=TEST_COMMAND,
            working_dir=None,
            timeout_seconds=60,
            shell=TEST_SHELL,
            login=False,
            tty=False,
            yield_time_ms=1,
            max_output_chars=10_000,
        )

        with pytest.raises(RuntimeError, match="old exec sessions"):
            await manager.apply_policy(new_policy)
        busy = await manager.start(CHAT_ID, new_request)
        assert busy.code == "tool_device_busy"

        old_handle.fail_terminate = False
        old_handle.fail_wait_after_terminate = False
        await manager.apply_policy(new_policy)
        started = await manager.start(CHAT_ID, new_request)
        assert started.is_error is False
        await manager.shutdown()

    asyncio.run(run())


def test_policy_transition_waits_for_an_inflight_spawn_to_be_reaped() -> None:
    async def run() -> None:
        launcher = _DelayedLauncher()
        manager = ExecSessionManager(launcher)
        old_start = asyncio.create_task(manager.start(CHAT_ID, _request()))
        await launcher.started.wait()
        new_policy = _request().policy.__class__(
            workspace=Path("/workspace"),
            restrict_to_workspace=False,
            shell_timeout_max=600,
            env_allowlist=("PATH",),
            available_shells=(TEST_SHELL,),
            default_shell=TEST_SHELL,
            epoch=2,
        )

        transition = asyncio.create_task(manager.apply_policy(new_policy))
        try:
            await asyncio.wait_for(asyncio.shield(transition), timeout=0.05)
            finished_before_spawn = True
        except TimeoutError:
            finished_before_spawn = False
        launcher.release.set()
        await transition
        result = await old_start

        assert finished_before_spawn is False
        assert result.code == "tool_exec_failed"
        assert launcher.handle is not None and launcher.handle.terminated is True
        await manager.shutdown()

    asyncio.run(run())


def test_policy_transition_fences_an_old_start_before_process_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        resolving = threading.Event()
        release_resolve = threading.Event()

        def blocked_resolve(
            working_dir: str | None,
            workspace: Path,
            *,
            restrict_to_workspace: bool,
        ) -> Path:
            del working_dir, restrict_to_workspace
            resolving.set()
            assert release_resolve.wait(timeout=5)
            return workspace

        monkeypatch.setattr(
            "openoctopus_client.exec_sessions.resolve_cwd",
            blocked_resolve,
        )
        launcher = _Launcher()
        manager = ExecSessionManager(launcher)
        old_start = asyncio.create_task(manager.start(CHAT_ID, _request()))
        assert await asyncio.to_thread(resolving.wait, 5)
        new_policy = _request().policy.__class__(
            workspace=Path("/workspace"),
            restrict_to_workspace=False,
            shell_timeout_max=600,
            env_allowlist=("PATH",),
            available_shells=(TEST_SHELL,),
            default_shell=TEST_SHELL,
            epoch=2,
        )

        transition = asyncio.create_task(manager.apply_policy(new_policy))

        async def wait_until_fenced() -> None:
            while True:
                sessions = tuple(manager._sessions.values())
                if sessions and sessions[0].state == "terminating":
                    return
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_until_fenced(), timeout=5)
        release_resolve.set()
        result = await old_start
        await transition

        assert result.code == "tool_exec_failed"
        assert launcher.specs == []
        await manager.shutdown()

    asyncio.run(run())


def test_report_uses_resolved_cwd_and_aggregate_output_limit(tmp_path: Path) -> None:
    async def run() -> None:
        launcher = _Launcher()
        manager = ExecSessionManager(launcher)
        request = _request()
        request = ExecStart(
            policy=ExecPolicy(
                workspace=tmp_path,
                restrict_to_workspace=False,
                shell_timeout_max=request.policy.shell_timeout_max,
                env_allowlist=request.policy.env_allowlist,
                available_shells=request.policy.available_shells,
                default_shell=request.policy.default_shell,
                epoch=request.policy.epoch,
            ),
            command=TEST_COMMAND,
            working_dir=str(tmp_path),
            timeout_seconds=60,
            shell=TEST_SHELL,
            login=False,
            tty=False,
            yield_time_ms=1,
            max_output_chars=10,
        )
        started = await manager.start(CHAT_ID, request)
        session_id = _session_id(started)
        handle = launcher.handles[0]
        await handle.stdout.chunks.put(b"123456")
        await handle.stderr.chunks.put(b"abcdef")
        result = await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, False, 1, None, None, 10),
        )
        content = cast(str, result.content)
        stdout = content.split("stdout=", 1)[1].split("\nstderr=", 1)[0]
        stderr = content.split("stderr=", 1)[1].split("\noutput=", 1)[0]
        assert f"cwd={tmp_path}" in content
        assert len(stdout) + len(stderr) <= 10
        await manager.shutdown()

    asyncio.run(run())


def test_terminal_retention_starts_at_terminal_time_and_errors_are_sanitized() -> None:
    async def run() -> None:
        now = [0.0]
        launcher = _Launcher()
        manager = ExecSessionManager(launcher, clock=lambda: now[0], idle_seconds=10)
        started = await manager.start(CHAT_ID, _request())
        session_id = _session_id(started)
        handle = launcher.handles[0]
        handle.exit.set_result(ProcessExit(-1, 1))
        await asyncio.sleep(0)
        now[0] = 11
        await manager.cleanup_idle()
        listed = await manager.list_sessions(CHAT_ID)
        assert str(session_id) in cast(str, listed.content)
        now[0] = 1812
        await manager.cleanup_idle()
        missing = await manager.list_sessions(CHAT_ID)
        assert str(session_id) not in cast(str, missing.content)
        await manager.shutdown()

    asyncio.run(run())


def test_tty_report_exposes_backend_flags_and_nonzero_has_stable_error_code() -> None:
    async def run() -> None:
        launcher = _Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request(tty=True))
        handle = launcher.handles[0]
        handle.terminal_control_truncated = True
        handle.cleanup_incomplete = True
        handle.exit.set_result(ProcessExit(1))
        result = await manager.write(
            CHAT_ID,
            ExecWrite(_session_id(started), None, False, 1, None, None, 10_000),
        )
        content = cast(str, result.content)
        assert result.code == "tool_exec_failed"
        assert "terminal_control_truncated=true" in content
        assert "cleanup_incomplete=true" in content
        await manager.shutdown()

    asyncio.run(run())


def test_spawn_failure_does_not_leak_local_exception_details() -> None:
    async def run() -> None:
        manager = ExecSessionManager(_FailingLauncher())
        result = await manager.start(CHAT_ID, _request())
        assert result.code == "tool_exec_failed"
        assert "/secret/private/workspace" not in cast(str, result.content)
        await manager.shutdown()

    asyncio.run(run())


def test_hard_timeout_has_stable_error_code_and_retains_pollable_session() -> None:
    async def run() -> None:
        launcher = _Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request(timeout=1, yield_ms=1))
        session_id = _session_id(started)
        await asyncio.sleep(1.05)
        result = await manager.write(
            CHAT_ID,
            ExecWrite(session_id, None, False, 1, None, None, 10_000),
        )
        assert result.code == "tool_exec_timeout"
        await manager.shutdown()

    asyncio.run(run())


def test_user_terminate_is_a_successful_final_report() -> None:
    async def run() -> None:
        launcher = _Launcher()
        manager = ExecSessionManager(launcher)
        started = await manager.start(CHAT_ID, _request())
        result = await manager.write(
            CHAT_ID,
            ExecWrite(_session_id(started), None, True, None, None, None, 10_000),
        )
        assert result.is_error is False
        assert result.code is None
        await manager.shutdown()

    asyncio.run(run())


def test_cancelled_spawn_is_reaped_even_when_cancelled_again() -> None:
    async def run() -> None:
        launcher = _DelayedLauncher()
        manager = ExecSessionManager(launcher)
        task = asyncio.create_task(manager.start(CHAT_ID, _request()))
        await launcher.started.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        launcher.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert launcher.handle is not None
        assert launcher.handle.terminated is True
        assert await manager.list_sessions(CHAT_ID) == ToolOutput("(no exec sessions)")
        await manager.shutdown()

    asyncio.run(run())


def test_cancelled_spawn_reap_survives_another_cancel_during_terminate() -> None:
    async def run() -> None:
        launcher = _DelayedLauncherWithBlockingTerminate()
        manager = ExecSessionManager(launcher)
        task = asyncio.create_task(manager.start(CHAT_ID, _request()))
        await launcher.started.wait()
        task.cancel()
        launcher.release_spawn.set()
        await launcher.terminate_started.wait()

        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        launcher.release_terminate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert launcher.handle is not None
        assert launcher.handle.terminated is True
        assert await manager.list_sessions(CHAT_ID) == ToolOutput("(no exec sessions)")
        await manager.shutdown()

    asyncio.run(run())


def test_cancelled_spawn_second_cancel_during_handle_bind_still_reaps() -> None:
    async def run() -> None:
        handle = _Handle()
        launcher = _DelayedLauncherWithHandle(handle)
        bind_started = asyncio.Event()

        class Manager(ExecSessionManager):
            async def _bind_spawned_handle(
                self,
                session: object,
                spawned_handle: ProcessHandle,
            ) -> None:
                bind_started.set()
                await super()._bind_spawned_handle(cast(Any, session), spawned_handle)

        manager = Manager(launcher)
        task = asyncio.create_task(manager.start(CHAT_ID, _request()))
        await launcher.started.wait()
        task.cancel()
        await asyncio.sleep(0)
        await manager._admission.acquire()
        try:
            launcher.release_spawn.set()
            await bind_started.wait()
            task.cancel()
        finally:
            manager._admission.release()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert handle.terminated is True
        assert await manager.list_sessions(CHAT_ID) == ToolOutput("(no exec sessions)")
        await manager.shutdown()

    asyncio.run(run())


def test_cancelled_spawn_repeated_cancel_during_failed_cleanup_still_marks_session() -> None:
    async def run() -> None:
        handle = _TerminateFailureHandle()
        launcher = _DelayedLauncherWithHandle(handle)
        bind_finished = asyncio.Event()
        mark_started = asyncio.Event()
        mark_finished = asyncio.Event()

        class Manager(ExecSessionManager):
            async def _bind_spawned_handle(
                self,
                session: object,
                spawned_handle: ProcessHandle,
            ) -> None:
                await super()._bind_spawned_handle(cast(Any, session), spawned_handle)
                bind_finished.set()

            async def _mark_spawn_cleanup_incomplete(
                self,
                session: object,
                spawned_handle: ProcessHandle,
                reason: str,
            ) -> None:
                mark_started.set()
                await super()._mark_spawn_cleanup_incomplete(
                    cast(Any, session), spawned_handle, reason
                )
                mark_finished.set()

        manager = Manager(launcher)
        task = asyncio.create_task(manager.start(CHAT_ID, _request()))
        await launcher.started.wait()
        task.cancel()
        await asyncio.sleep(0)
        launcher.release_spawn.set()
        await bind_finished.wait()
        await manager._admission.acquire()
        try:
            await mark_started.wait()
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
        finally:
            manager._admission.release()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert mark_finished.is_set()
        session_id = next(iter(manager._sessions))
        assert manager._sessions[session_id].cleanup_incomplete is True
        assert manager._sessions[session_id].state == "terminating"
        await manager.shutdown()


def test_cancel_after_spawn_preserves_a_fully_activated_session() -> None:
    async def run() -> None:
        launcher = _DelayedLauncher()
        activation_started = asyncio.Event()

        class Manager(ExecSessionManager):
            async def _activate_spawned(
                self,
                session: object,
                handle: ProcessHandle,
            ) -> bool:
                activation_started.set()
                return await super()._activate_spawned(cast(Any, session), handle)

        manager = Manager(launcher)
        start = asyncio.create_task(manager.start(CHAT_ID, _request(yield_ms=30_000)))
        await launcher.started.wait()
        await manager._admission.acquire()
        launcher.release.set()
        await activation_started.wait()
        start.cancel()
        manager._admission.release()

        with pytest.raises(asyncio.CancelledError):
            await start

        sessions = await manager.list_sessions(CHAT_ID)
        assert "status=running" in cast(str, sessions.content)
        assert launcher.handle is not None and launcher.handle.terminated is False
        await manager.shutdown()
        assert launcher.handle.terminated is True

    asyncio.run(run())
