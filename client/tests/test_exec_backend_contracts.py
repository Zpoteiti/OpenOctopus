from __future__ import annotations

import asyncio
import errno
import json
import os
import signal
import socket
import struct
import sys
import threading
import time
import types
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, cast

import pytest

import openoctopus_client.process as process_module
import openoctopus_client.pty_worker as pty_worker_module
from openoctopus_client.process import (
    InvalidProcessArgumentsError,
    ProcessBackendError,
    PtyProcessHandle,
    ShellLoginUnsupportedError,
    _AsyncOutputBuffer,
    _QueueReader,
    _recv_exact,
    _terminate_windows,
    _ThreadOutputBuffer,
    _ThreadQueueReader,
    _validate_windows_command_line,
    build_argv,
    build_child_env,
    spawn_pty,
)
from openoctopus_client.pty_worker import _write_all
from openoctopus_client.terminal import TerminalNormalizer


def test_queue_reader_returns_one_available_chunk_without_waiting_for_more() -> None:
    async def run() -> bytes:
        queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        reader = _QueueReader(queue)
        await queue.put(b"prompt> ")
        return await asyncio.wait_for(reader.read(65_536), 0.1)

    assert asyncio.run(run()) == b"prompt> "


def test_async_pty_output_bridge_applies_byte_backpressure() -> None:
    async def run() -> None:
        bridge = _AsyncOutputBuffer(8)
        assert await bridge.put(b"12345678")
        pending = asyncio.create_task(bridge.put(b"9"))
        await asyncio.sleep(0)
        assert not pending.done()
        assert await bridge.get() == b"12345678"
        assert await asyncio.wait_for(pending, 0.1)
        await bridge.close()
        assert await bridge.get() == b"9"
        assert await bridge.get() is None

    asyncio.run(run())


def test_conpty_output_bridge_applies_byte_backpressure_without_event_loop_blocking() -> None:
    bridge = _ThreadOutputBuffer(8)
    stop = threading.Event()
    assert bridge.put(b"12345678", stop)
    started = threading.Event()
    finished = threading.Event()

    def put_extra() -> None:
        started.set()
        bridge.put(b"9", stop)
        finished.set()

    thread = threading.Thread(target=put_extra)
    thread.start()
    assert started.wait(0.1)
    time.sleep(0.02)
    assert not finished.is_set()
    assert bridge.get() == b"12345678"
    assert finished.wait(0.1)
    bridge.close()
    assert bridge.get() == b"9"
    assert bridge.get() is None
    thread.join(timeout=0.1)


def test_idle_conpty_readers_do_not_consume_the_default_executor() -> None:
    async def run() -> None:
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        release_default_worker = threading.Event()
        occupied = loop.run_in_executor(None, release_default_worker.wait)
        bridges: list[_ThreadOutputBuffer] = []
        readers: list[_ThreadQueueReader] = []

        def wake_for(ready: asyncio.Event) -> Callable[[], None]:
            def wake() -> None:
                loop.call_soon_threadsafe(ready.set)

            return wake

        try:
            for _ in range(3):
                ready = asyncio.Event()
                bridge = _ThreadOutputBuffer(
                    8,
                    wake=wake_for(ready),
                )
                bridges.append(bridge)
                readers.append(_ThreadQueueReader(bridge, ready))
            pending = [asyncio.create_task(reader.read(1)) for reader in readers]
            await asyncio.sleep(0)
            for index, bridge in enumerate(bridges):
                assert bridge.put(bytes((ord("a") + index,)), threading.Event())
            assert await asyncio.wait_for(asyncio.gather(*pending), 0.2) == [
                b"a",
                b"b",
                b"c",
            ]

            eof = [asyncio.create_task(reader.read(1)) for reader in readers]
            for bridge in bridges:
                bridge.close()
            assert await asyncio.wait_for(asyncio.gather(*eof), 0.2) == [b"", b"", b""]
        finally:
            release_default_worker.set()
            await occupied

    asyncio.run(run())


def test_terminal_prompt_is_visible_without_newline_and_state_is_bounded() -> None:
    normalizer = TerminalNormalizer(escape_limit=32)

    assert normalizer.feed(b"prompt> ") == "prompt> "
    assert normalizer.feed(b"\x1b]" + b"x" * 1000) == ""
    assert normalizer.control_truncated is True
    assert normalizer.pending_control_bytes <= 32


def test_terminal_dsr_responses_are_drained_instead_of_accumulating() -> None:
    normalizer = TerminalNormalizer()

    for _ in range(100):
        normalizer.feed(b"\x1b[5n")
        assert normalizer.take_responses() == (b"\x1b[0n",)
        assert normalizer.pending_response_count == 0


def test_terminal_unknown_query_is_ignored() -> None:
    normalizer = TerminalNormalizer()

    assert normalizer.feed(b"before\x1b[99nafter") == "beforeafter"
    assert normalizer.take_responses() == ()


def test_terminal_utf8_split_and_invalid_prefix_are_bounded() -> None:
    normalizer = TerminalNormalizer()

    assert normalizer.feed("中".encode()[:2]) == ""
    assert normalizer.feed("中".encode()[2:]) == "中"
    assert normalizer.feed(b"\xf0\n") == "�\n"


def test_terminal_invalid_utf8_prefix_does_not_drop_following_text() -> None:
    normalizer = TerminalNormalizer()

    assert normalizer.feed(b"\xe2A") == "�A"
    assert normalizer.feed(b"\xe2\x82B") == "�B"


def test_write_all_retries_partial_writes_and_eagain() -> None:
    calls: list[bytes] = []
    waits: list[int] = []
    outcomes: list[int | OSError] = [BlockingIOError(errno.EAGAIN, "busy"), 2, 3]

    def write(fd: int, data: bytes | memoryview) -> int:
        del fd
        calls.append(bytes(data))
        outcome = outcomes.pop(0)
        if isinstance(outcome, OSError):
            raise outcome
        return outcome

    def wait_writable(fd: int) -> None:
        waits.append(fd)

    _write_all(9, b"abcde", write=write, wait_writable=wait_writable)

    assert calls == [b"abcde", b"abcde", b"cde"]
    assert waits == [9]


def test_posix_pty_worker_refuses_windows_before_opening_channels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_opened(*args: object, **kwargs: object) -> socket.socket:
        del args, kwargs
        raise AssertionError("Windows must not open POSIX PTY worker channels")

    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setattr(socket, "socket", fail_if_opened)

    assert pty_worker_module.run(10, 11) == 2


def test_login_unsupported_has_stable_error_code() -> None:
    shell = "cmd" if os.name == "nt" else "sh"
    with pytest.raises(ShellLoginUnsupportedError) as raised:
        build_argv(shell, "echo ok", login=True, tty=False)

    assert raised.value.code == "tool_shell_login_unsupported"


def test_child_environment_removes_case_variants_of_client_secrets() -> None:
    result = build_child_env(
        {
            "PATH": "/bin",
            "openoctopus_device_token": "secret",
            "OpenOctopus_Another": "secret-too",
        },
        ["PATH", "openoctopus_device_token", "OpenOctopus_Another"],
    )

    assert "openoctopus_device_token" not in result
    assert "OpenOctopus_Another" not in result


def test_windows_child_environment_matching_and_overrides_are_case_insensitive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as context:
        context.setattr(os, "name", "nt")
        result = build_child_env(
            {
                "Path": "C:/Windows",
                "term": "secret-terminal-value",
                "oPeNoCtOpUs_DeViCe_ToKeN": "secret-token",
            },
            ["PATH", "TERM", "OPENOCTOPUS_DEVICE_TOKEN"],
        )

    assert result["Path"] == "C:/Windows"
    assert result["TERM"] == "dumb"
    assert "term" not in result
    assert not any(key.casefold().startswith("openoctopus_") for key in result)


def test_backspace_does_not_remove_a_completed_line_in_same_chunk() -> None:
    normalizer = TerminalNormalizer()

    assert normalizer.feed(b"done\n\bnext") == "done\nnext"


def test_windows_command_line_limit_includes_quoting_expansion() -> None:
    command = '"' * 20_000
    argv = [r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe", "-Command", command]

    with pytest.raises(InvalidProcessArgumentsError, match="Windows command line"):
        _validate_windows_command_line("powershell", command, argv)


def test_posix_pty_has_fixed_dimensions_and_does_not_leak_bootstrap_pythonpath() -> None:
    if os.name == "nt":
        pytest.skip("POSIX PTY contract")

    async def run() -> tuple[bytes, int | None]:
        command = (
            "stty size; "
            f"{sys.executable} -c \"import os; print(os.getenv('PYTHONPATH', '<none>'))\""
        )
        handle = await spawn_pty(
            build_argv("sh", command, login=False, tty=True),
            cwd=None,
            env=build_child_env(os.environ, ["PATH"]),
        )
        output = await asyncio.wait_for(handle.output.read(), 3)
        result = await asyncio.wait_for(handle.wait(), 3)
        return output, result.returncode

    output, returncode = asyncio.run(run())
    assert output.splitlines() == [b"24 80", b"<none>"]
    assert returncode == 0


def test_posix_pty_accepts_immediate_input_after_ready_ack() -> None:
    if os.name == "nt":
        pytest.skip("POSIX PTY contract")

    async def run() -> tuple[bytes, int | None]:
        handle = await spawn_pty(
            build_argv("sh", "read value; printf 'got:%s\\n' \"$value\"", login=False, tty=True),
            cwd=None,
            env=build_child_env(os.environ, ["PATH"]),
        )
        await asyncio.wait_for(handle.write(b"hello\n"), 2)
        output = await asyncio.wait_for(handle.output.read(), 3)
        result = await asyncio.wait_for(handle.wait(), 3)
        return output, result.returncode

    output, returncode = asyncio.run(run())
    assert b"got:hello\n" in output
    assert returncode == 0


def test_posix_prompt_chunk_is_returned_before_process_exit() -> None:
    if os.name == "nt":
        pytest.skip("POSIX PTY contract")

    async def run() -> tuple[bytes, int | None]:
        handle = await spawn_pty(
            build_argv("sh", "printf 'prompt> '; sleep 1", login=False, tty=True),
            cwd=None,
            env=build_child_env(os.environ, ["PATH"]),
        )
        first = await asyncio.wait_for(handle.output.read(65_536), 0.5)
        result = await asyncio.wait_for(handle.wait(), 3)
        return first, result.returncode

    first, returncode = asyncio.run(run())
    assert first == b"prompt> "
    assert returncode == 0


def test_recv_exact_fails_on_partial_channel_eof() -> None:
    async def run() -> None:
        reader, writer = socket.socketpair()
        reader.setblocking(False)
        try:
            writer.sendall(b"ab")
            writer.close()
            with pytest.raises(ProcessBackendError, match="channel closed"):
                await asyncio.wait_for(_recv_exact(reader, 5), 0.2)
        finally:
            reader.close()

    asyncio.run(run())


class _FakeHelper:
    def __init__(self) -> None:
        self.pid = 123_456_789
        self.returncode: int | None = None
        self.killed = False

    async def wait(self) -> int:
        self.returncode = -9 if self.killed else 0
        return self.returncode

    def kill(self) -> None:
        self.killed = True


def test_posix_dsr_flood_does_not_block_ack_demultiplexing() -> None:
    async def run() -> tuple[int | None, bool]:
        control_parent, control_peer = socket.socketpair()
        events_parent, events_peer = socket.socketpair()
        for sock in (control_parent, control_peer, events_parent, events_peer):
            sock.setblocking(False)
        helper = _FakeHelper()
        handle = PtyProcessHandle(helper, control_parent, events_parent)  # type: ignore[arg-type]
        loop = asyncio.get_running_loop()

        def frame(kind: bytes, payload: bytes) -> bytes:
            return kind + struct.pack(">I", len(payload)) + payload

        try:
            flood = b"\x1b[5n" * 100
            await loop.sock_sendall(events_peer, frame(b"O", flood))
            request_bytes = await asyncio.wait_for(loop.sock_recv(control_peer, 4096), 0.5)
            request = json.loads(request_bytes.splitlines()[0])
            ack = json.dumps({"id": request["id"], "ok": True}).encode("utf-8")
            exit_event = json.dumps({"returncode": 0, "cleanup_incomplete": False}).encode("utf-8")
            await loop.sock_sendall(events_peer, frame(b"A", ack) + frame(b"E", exit_event))
            result = await asyncio.wait_for(handle.wait(), 0.5)
            return result.returncode, handle.terminal_control_truncated
        finally:
            control_peer.close()
            events_peer.close()

    assert asyncio.run(run()) == (0, True)


def test_cancelled_pty_channel_reader_makes_wait_bounded() -> None:
    async def run() -> None:
        control_parent, control_peer = socket.socketpair()
        events_parent, events_peer = socket.socketpair()
        control_parent.setblocking(False)
        events_parent.setblocking(False)
        helper = _FakeHelper()
        handle = PtyProcessHandle(helper, control_parent, events_parent)  # type: ignore[arg-type]
        handle._reader_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await handle._reader_task
        result = await asyncio.wait_for(handle.wait(), 0.2)
        assert result.returncode is None
        control_peer.close()
        events_peer.close()

    asyncio.run(run())


def test_frozen_helper_command_reenters_client_executable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "executable", "/tmp/openoctopus-client")

    assert process_module._pty_worker_command(10, 11, frozen=True) == [
        "/tmp/openoctopus-client",
        "_pty-worker",
        "10",
        "11",
    ]
    assert process_module._pty_worker_command(10, 11, frozen=False) == [
        "/tmp/openoctopus-client",
        "-m",
        "openoctopus_client.pty_worker",
        "10",
        "11",
    ]


def test_conpty_receives_argv_dimensions_and_eof_is_normal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeProcess:
        pid = 42
        exitstatus = 0

        def read(self) -> str:
            raise EOFError

        def close(self, force: bool = False) -> None:
            del force

        def write(self, value: str) -> int:
            return len(value)

    class FakePtyProcess:
        @classmethod
        def spawn(cls, argv: list[str], **kwargs: Any) -> FakeProcess:
            captured["argv"] = argv
            captured.update(kwargs)
            return FakeProcess()

    fake_winpty = types.ModuleType("winpty")
    fake_winpty.PtyProcess = FakePtyProcess  # type: ignore[attr-defined]
    fake_winpty.Backend = types.SimpleNamespace(ConPTY=0)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "winpty", fake_winpty)
    monkeypatch.setattr(process_module, "_create_windows_job", lambda pid: None)

    async def run() -> int | None:
        handle = await process_module._spawn_conpty(
            [r"C:\Program Files\PowerShell\7\pwsh.exe", "-Command", "echo hello world"],
            cwd=Path("C:/workspace"),
            env={"PATH": "C:/Windows"},
        )
        return (await asyncio.wait_for(handle.wait(), 1)).returncode

    assert asyncio.run(run()) == 0
    assert captured["argv"] == [
        r"C:\Program Files\PowerShell\7\pwsh.exe",
        "-Command",
        "echo hello world",
    ]
    assert captured["dimensions"] == (24, 80)
    assert captured["backend"] == "0"


def test_conpty_marks_cleanup_incomplete_when_job_close_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeJob:
        def terminate(self) -> bool:
            return True

        def close(self) -> bool:
            return False

    class EofProcess:
        pid = 42
        exitstatus = 0

        def read(self) -> str:
            raise EOFError

        def write(self, value: str) -> int:
            return len(value)

        def close(self, force: bool = False) -> None:
            del force

    monkeypatch.setattr(process_module, "_create_windows_job", lambda pid: FakeJob())

    async def run() -> bool:
        handle = process_module.ConPtyProcessHandle(EofProcess())
        await asyncio.wait_for(handle.wait(), 1)
        return handle.cleanup_incomplete

    assert asyncio.run(run()) is True


@pytest.mark.parametrize("job", [None, "failing"])
def test_windows_tree_termination_falls_back_to_taskkill(
    monkeypatch: pytest.MonkeyPatch, job: str | None
) -> None:
    calls: list[tuple[object, ...]] = []

    class FakeProcess:
        pid = 42

    class FailingJob:
        def terminate(self) -> bool:
            calls.append(("job",))
            return False

        def close(self) -> bool:
            raise AssertionError("failed jobs must use taskkill fallback")

    class FakeKiller:
        async def wait(self) -> int:
            return 0

    async def create_process(*args: object, **kwargs: object) -> FakeKiller:
        del kwargs
        calls.append(args)
        return FakeKiller()

    monkeypatch.setenv("SystemRoot", r"C:\Windows")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)

    actual_job = FailingJob() if job else None
    assert asyncio.run(_terminate_windows(FakeProcess(), actual_job)) is True  # type: ignore[arg-type]
    assert (
        calls
        == [
            ("job",),
            (
                r"C:\Windows/System32/taskkill.exe",
                "/PID",
                "42",
                "/T",
                "/F",
            ),
        ]
        if job
        else [
            (
                r"C:\Windows/System32/taskkill.exe",
                "/PID",
                "42",
                "/T",
                "/F",
            )
        ]
    )


def test_conpty_dsr_write_failure_is_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeJob:
        terminated = False
        closed = False

        def terminate(self) -> bool:
            self.terminated = True
            return True

        def close(self) -> bool:
            self.closed = True
            return True

    class FailingProcess:
        pid = 42
        exitstatus = None

        def __init__(self) -> None:
            self.reads = 0
            self.closed = False

        def read(self) -> str:
            self.reads += 1
            if self.reads == 1:
                return "\x1b[5n"
            raise EOFError

        def write(self, value: str) -> int:
            del value
            raise OSError("write failed")

        def close(self, force: bool = False) -> None:
            del force
            self.closed = True

    job = FakeJob()
    child = FailingProcess()
    monkeypatch.setattr(process_module, "_create_windows_job", lambda pid: job)

    async def run() -> tuple[int | None, bool]:
        handle = process_module.ConPtyProcessHandle(child)
        result = await asyncio.wait_for(handle.wait(), 1)
        return result.returncode, handle.cleanup_incomplete

    assert asyncio.run(run()) == (1, False)
    assert child.closed is True
    assert job.terminated is True
    assert job.closed is True


def test_terminate_posix_cleans_group_even_after_leader_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[int] = []
    existence = [True, True, False]

    class ExitedProcess:
        returncode = 0

    monkeypatch.setattr(
        process_module,
        "_process_group_exists",
        lambda pid: existence.pop(0) if existence else False,
    )
    monkeypatch.setattr(
        os,
        "killpg",
        lambda pid, sig: sent.append(sig),
    )

    async def no_sleep(delay: float) -> None:
        del delay

    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    complete = asyncio.run(process_module._terminate_posix(ExitedProcess(), 123))  # type: ignore[arg-type]

    assert complete is True
    assert sent == [signal.SIGTERM]


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    status = Path(f"/proc/{pid}/stat")
    if status.exists():
        fields = status.read_text(encoding="utf-8").split()
        return len(fields) < 3 or fields[2] != "Z"
    return True


def _assert_process_stopped(pid: int) -> None:
    deadline = time.monotonic() + 2
    while _pid_is_running(pid) and time.monotonic() < deadline:
        time.sleep(0.02)
    if _pid_is_running(pid):
        os.kill(pid, signal.SIGKILL)
        pytest.fail("same-group descendant survived natural leader exit")


def test_pipe_natural_leader_exit_cleans_same_group_descendant() -> None:
    if os.name == "nt":
        pytest.skip("POSIX process-group contract")

    async def run() -> tuple[int, bool]:
        handle = await process_module.spawn_pipe(
            build_argv(
                "sh",
                "sleep 60 </dev/null >/dev/null 2>&1 & printf '%s\\n' \"$!\"; exit 0",
                login=False,
                tty=False,
            ),
            cwd=None,
            env=build_child_env(os.environ, ["PATH"]),
        )
        child_pid = int((await asyncio.wait_for(handle.stdout.read(), 1)).strip())
        result = await asyncio.wait_for(handle.wait(), 4)
        assert result.returncode == 0
        return child_pid, handle.cleanup_incomplete

    child_pid, cleanup_incomplete = asyncio.run(run())
    assert cleanup_incomplete is False
    _assert_process_stopped(child_pid)


def test_pty_natural_leader_exit_cleans_same_group_descendant() -> None:
    if os.name == "nt":
        pytest.skip("POSIX process-group contract")

    async def run() -> tuple[int, bool]:
        handle = await spawn_pty(
            build_argv(
                "sh",
                "sleep 60 </dev/null >/dev/null 2>&1 & printf '%s\\n' \"$!\"; exit 0",
                login=False,
                tty=True,
            ),
            cwd=None,
            env=build_child_env(os.environ, ["PATH"]),
        )
        output = await asyncio.wait_for(handle.output.read(), 4)
        result = await asyncio.wait_for(handle.wait(), 4)
        assert result.returncode == 0
        return int(output.strip()), handle.cleanup_incomplete

    child_pid, cleanup_incomplete = asyncio.run(run())
    assert cleanup_incomplete is False
    _assert_process_stopped(child_pid)


def test_dsr_write_failure_terminates_pty_session() -> None:
    if os.name == "nt":
        pytest.skip("POSIX PTY contract")

    async def run() -> tuple[int | None, bool]:
        handle = cast(
            PtyProcessHandle,
            await spawn_pty(
                build_argv(
                    "sh",
                    "printf '\\033[5n'; sleep 30",
                    login=False,
                    tty=True,
                ),
                cwd=None,
                env=build_child_env(os.environ, ["PATH"]),
            ),
        )

        async def fail_response(payload: dict[str, Any], *, timeout: float = 5) -> dict[str, Any]:
            del payload, timeout
            raise ProcessBackendError("response write failed")

        handle._command = fail_response  # type: ignore[method-assign]
        result = await asyncio.wait_for(handle.wait(), 3)
        return result.returncode, handle.cleanup_incomplete

    returncode, cleanup_incomplete = asyncio.run(run())
    assert returncode == 1
    assert cleanup_incomplete is False


def test_cli_checks_configuration_before_pty_backend(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import openoctopus_client.cli as cli

    monkeypatch.delenv("OPENOCTOPUS_SERVER_URL", raising=False)
    monkeypatch.setenv("OPENOCTOPUS_DEVICE_TOKEN", "openoctopus_dev_secret-sentinel")
    monkeypatch.setattr(sys, "argv", ["openoctopus-client", "run"])
    monkeypatch.setattr(
        cli,
        "validate_pty_backend",
        lambda: (_ for _ in ()).throw(AssertionError("preflight ran too early")),
    )

    assert cli.main() == 78
    captured = capsys.readouterr()
    assert "OPENOCTOPUS_SERVER_URL is required" in captured.err
    assert "secret-sentinel" not in captured.err
    assert "Traceback" not in captured.err


def test_cli_reports_sanitized_pty_preflight_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import openoctopus_client.cli as cli

    monkeypatch.setenv("OPENOCTOPUS_SERVER_URL", "https://openoctopus.example")
    monkeypatch.setenv("OPENOCTOPUS_DEVICE_TOKEN", "openoctopus_dev_secret-sentinel")
    monkeypatch.setattr(sys, "argv", ["openoctopus-client", "run"])
    monkeypatch.setattr(
        cli,
        "validate_pty_backend",
        lambda: (_ for _ in ()).throw(
            process_module.PtyUnavailableError("backend-internal-secret")
        ),
    )

    assert cli.main() == 78
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "backend error: PTY backend is unavailable\n"
    assert "secret-sentinel" not in captured.err
    assert "backend-internal-secret" not in captured.err
    assert "Traceback" not in captured.err


def test_source_backend_smoke_exercises_pipe_and_pty() -> None:
    if os.name == "nt":
        pytest.skip("native Windows runs this through the frozen smoke harness")

    payload = asyncio.run(process_module.frozen_backend_smoke())

    assert payload == {
        "ok": True,
        "pipe": True,
        "tty": True,
        "shell": process_module.discover_shells().default,
    }
