"""Frozen-safe POSIX PTY helper with a small acknowledged control protocol."""

from __future__ import annotations

import base64
import errno
import json
import os
import select
import selectors
import signal
import socket
import struct
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, cast

# Keep the write helper importable on Windows without presenting this POSIX
# worker as a runnable backend there.
if sys.platform == "win32":
    _fcntl = cast(Any, None)
    _termios = cast(Any, None)
    _posix_os = cast(Any, os)
    _posix_signal = cast(Any, signal)
else:
    import fcntl as _fcntl
    import termios as _termios

    _posix_os = os
    _posix_signal = signal

ROWS = 24
COLS = 80
WRITE_TIMEOUT_SECONDS = 5.0
TERMINATE_GRACE_SECONDS = 2.0


def _frame(kind: bytes, payload: bytes) -> bytes:
    return kind + len(payload).to_bytes(4, "big") + payload


def _recv_lines(sock: socket.socket) -> Iterator[dict[str, object]]:
    buffer = bytearray()
    while True:
        data = sock.recv(65536)
        if not data:
            return
        buffer.extend(data)
        while b"\n" in buffer:
            line, _, remainder = buffer.partition(b"\n")
            buffer = bytearray(remainder)
            if line:
                value = json.loads(line.decode("utf-8"))
                if isinstance(value, dict):
                    yield value


def _send_json(sock: socket.socket, kind: bytes, value: Mapping[str, object]) -> None:
    payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
    sock.sendall(_frame(kind, payload))


def _send_exit(sock: socket.socket, code: int | None, cleanup_incomplete: bool) -> None:
    _send_json(
        sock,
        b"E",
        {"returncode": code, "cleanup_incomplete": cleanup_incomplete},
    )


def _send_ack(
    sock: socket.socket,
    request_id: int,
    *,
    ok: bool,
    pid: int | None = None,
) -> None:
    value: dict[str, object] = {"id": request_id, "ok": ok}
    if pid is not None:
        value["pid"] = pid
    _send_json(sock, b"A", value)


def _set_cooked(fd: int) -> None:
    attrs = _termios.tcgetattr(fd)
    attrs[3] |= _termios.ICANON | _termios.ECHO | _termios.ISIG
    attrs[6][_termios.VINTR] = 3
    _termios.tcsetattr(fd, _termios.TCSANOW, attrs)


def _set_window_size(fd: int) -> None:
    size = struct.pack("HHHH", ROWS, COLS, 0, 0)
    _fcntl.ioctl(fd, _termios.TIOCSWINSZ, size)


def _default_wait_writable(fd: int) -> None:
    _, writable, _ = select.select([], [fd], [], WRITE_TIMEOUT_SECONDS)
    if not writable:
        raise TimeoutError("PTY input did not become writable")


def _write_all(
    fd: int,
    data: bytes,
    *,
    write: Callable[[int, bytes | memoryview], int] = os.write,
    wait_writable: Callable[[int], None] = _default_wait_writable,
) -> None:
    remaining = memoryview(data)
    while remaining:
        try:
            written = write(fd, remaining)
        except OSError as exc:
            if exc.errno == errno.EINTR:
                continue
            if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                wait_writable(fd)
                continue
            raise
        if written <= 0:
            raise OSError(errno.EIO, "PTY input write made no progress")
        remaining = remaining[written:]


def _group_exists(pid: int) -> bool:
    try:
        _posix_os.killpg(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _cleanup_group(pid: int) -> bool:
    if not _group_exists(pid):
        return True
    try:
        _posix_os.killpg(pid, _posix_signal.SIGTERM)
    except ProcessLookupError:
        return True
    except PermissionError:
        pass
    deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
    while _group_exists(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    if _group_exists(pid):
        try:
            _posix_os.killpg(pid, _posix_signal.SIGKILL)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        time.sleep(0.01)
    return not _group_exists(pid)


def _validate_spawn(
    request: Mapping[str, object],
) -> tuple[int, list[str], str | None, dict[str, str]] | None:
    request_id = request.get("id")
    argv = request.get("argv")
    cwd = request.get("cwd")
    environment = request.get("env")
    if not isinstance(request_id, int):
        return None
    if not isinstance(argv, list) or not argv or not all(isinstance(item, str) for item in argv):
        return None
    if cwd is not None and not isinstance(cwd, str):
        return None
    if not isinstance(environment, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in environment.items()
    ):
        return None
    return request_id, argv, cwd, environment


def _fork_child(
    argv: Sequence[str],
    cwd: str | None,
    environment: Mapping[str, str],
    *,
    control_fd: int | None = None,
    events_fd: int | None = None,
) -> None:
    for fd in (control_fd, events_fd):
        if fd is not None and fd > 2:
            try:
                _posix_os.close(fd)
            except OSError:
                pass
    if cwd is not None:
        _posix_os.chdir(cwd)
    _set_window_size(0)
    _set_cooked(0)
    _posix_os.execvpe(argv[0], list(argv), dict(environment))
    raise AssertionError("execvpe returned")


def run(control_fd: int, events_fd: int) -> int:
    if os.name == "nt":
        return 2
    control = socket.socket(fileno=control_fd)
    events = socket.socket(fileno=events_fd)
    first = next(_recv_lines(control), None)
    if not first or first.get("type") != "spawn":
        return 2
    spawn = _validate_spawn(first)
    if spawn is None:
        return 2
    request_id, argv, cwd, environment = spawn
    pid, master = _posix_os.forkpty()
    if pid == 0:
        _fork_child(
            argv,
            cwd,
            environment,
            control_fd=control_fd,
            events_fd=events_fd,
        )
    _set_window_size(master)
    _posix_os.set_blocking(master, False)
    control.setblocking(False)
    selector = selectors.DefaultSelector()
    selector.register(master, selectors.EVENT_READ, "master")
    selector.register(control, selectors.EVENT_READ, "control")
    _send_ack(events, request_id, ok=True, pid=pid)

    command_buffer = bytearray()
    exit_code: int | None = None
    terminate_deadline: float | None = None
    master_open = True
    while True:
        try:
            child_pid, status = _posix_os.waitpid(pid, _posix_os.WNOHANG)
            if child_pid == pid:
                exit_code = _posix_os.waitstatus_to_exitcode(status)
        except ChildProcessError:
            pass
        if terminate_deadline is not None and time.monotonic() >= terminate_deadline:
            try:
                _posix_os.killpg(pid, _posix_signal.SIGKILL)
            except ProcessLookupError:
                pass
            terminate_deadline = None
        if exit_code is not None:
            cleanup_incomplete = not _cleanup_group(pid)
            if master_open:
                try:
                    while True:
                        output = _posix_os.read(master, 65536)
                        if not output:
                            break
                        events.sendall(_frame(b"O", output))
                except OSError:
                    pass
            _send_exit(events, exit_code, cleanup_incomplete)
            return 0

        for key, _ in selector.select(0.1):
            if key.data == "master":
                try:
                    output = _posix_os.read(master, 65536)
                except OSError as exc:
                    if exc.errno not in {errno.EAGAIN, errno.EWOULDBLOCK, errno.EIO}:
                        raise
                    output = b""
                    if exc.errno == errno.EIO:
                        selector.unregister(master)
                        master_open = False
                if output:
                    events.sendall(_frame(b"O", output))
                continue

            try:
                data = control.recv(65536)
            except BlockingIOError:
                continue
            if not data:
                if terminate_deadline is None:
                    try:
                        _posix_os.killpg(pid, _posix_signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    terminate_deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
                selector.unregister(control)
                continue
            command_buffer.extend(data)
            while b"\n" in command_buffer:
                line, _, rest = command_buffer.partition(b"\n")
                command_buffer = bytearray(rest)
                if not line:
                    continue
                request = json.loads(line.decode("utf-8"))
                request_id_value = request.get("id")
                if not isinstance(request_id_value, int):
                    continue
                try:
                    kind = request.get("type")
                    if kind in {"write", "write_raw"}:
                        payload = base64.b64decode(str(request.get("data", "")), validate=True)
                        _write_all(master, payload)
                    elif kind == "interrupt":
                        _posix_os.killpg(pid, _posix_signal.SIGINT)
                    elif kind == "terminate":
                        _posix_os.killpg(pid, _posix_signal.SIGTERM)
                        terminate_deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
                    else:
                        raise ValueError("unknown PTY control request")
                except (OSError, ValueError):
                    _send_ack(events, request_id_value, ok=False)
                else:
                    _send_ack(events, request_id_value, ok=True)
    return 0


def main() -> int:
    if os.name == "nt" or len(sys.argv) != 3:
        return 2
    return run(int(sys.argv[1]), int(sys.argv[2]))


if __name__ == "__main__":
    raise SystemExit(main())
