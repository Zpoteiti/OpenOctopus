"""Cross-platform shell discovery and process backends for Py6 exec.

The module intentionally owns only low-level process concerns.  Session
ownership, buffers, deadlines and tool result formatting belong to the caller.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import ctypes
import importlib
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from shutil import which
from typing import Any, Protocol, cast

from openoctopus_client.terminal import TerminalNormalizer

MAX_COMMAND_CHARS = 24_000
MAX_CMD_COMMAND_UNITS = 8_000
MAX_WINDOWS_COMMAND_UNITS = 32_767
MAX_PTY_EVENT_BYTES = 1024 * 1024
PTY_OUTPUT_QUEUE_BYTES = 256 * 1024
PTY_RESPONSE_QUEUE_MAX = 32


class ProcessBackendError(RuntimeError):
    code = "tool_exec_failed"


class ShellUnavailableError(ProcessBackendError):
    code = "tool_shell_unavailable"


class ShellLoginUnsupportedError(ShellUnavailableError):
    code = "tool_shell_login_unsupported"


class PtyUnavailableError(ProcessBackendError):
    code = "tool_pty_unavailable"


class InvalidProcessArgumentsError(ProcessBackendError):
    code = "tool_invalid_args"


@dataclass(frozen=True)
class ShellInventory:
    default: str
    available: tuple[str, ...]


@dataclass(frozen=True)
class ShellCommand:
    name: str
    executable: str
    argv: tuple[str, ...]


def _windows_executable(name: str, environment: Mapping[str, str] | None = None) -> str | None:
    source = os.environ if environment is None else environment
    if name == "cmd":
        root = source.get("SystemRoot", r"C:\\Windows")
        return str(Path(root) / "System32" / "cmd.exe")
    if name == "powershell_x86":
        root = source.get("SystemRoot", r"C:\\Windows")
        return str(Path(root) / "SysWOW64" / "WindowsPowerShell" / "v1.0" / "powershell.exe")
    if name == "powershell":
        root = source.get("SystemRoot", r"C:\\Windows")
        return str(Path(root) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe")
    return which(name, path=source.get("PATH"))


def discover_shells(environment: Mapping[str, str] | None = None) -> ShellInventory:
    """Discover canonical shells without accepting arbitrary executable paths."""

    env = os.environ if environment is None else environment
    if os.name == "nt":
        names: tuple[str, ...] = ("pwsh", "powershell", "powershell_x86", "cmd")
    elif sys.platform == "darwin":
        names = ("zsh", "bash", "sh")
    else:
        names = ("bash", "sh", "zsh")
    available = tuple(name for name in names if _resolve_executable(name, env) is not None)
    if not available:
        raise ShellUnavailableError("no supported shell was found")
    return ShellInventory(default=available[0], available=available)


def validate_pty_backend() -> None:
    """Fail before connecting when the platform's required PTY backend is absent."""

    try:
        if os.name == "nt":
            winpty = importlib.import_module("winpty")
            backend_type = getattr(winpty, "Backend")
            pty_process_type = getattr(winpty, "PtyProcess")

            if not callable(getattr(pty_process_type, "spawn", None)) or not isinstance(
                getattr(backend_type, "ConPTY", None),
                int,
            ):
                raise ImportError
            taskkill = Path(
                os.environ.get("SystemRoot", r"C:\\Windows"),
                "System32",
                "taskkill.exe",
            )
            if not taskkill.is_file():
                raise ImportError
        else:
            from openoctopus_client import pty_worker

            if not callable(getattr(os, "forkpty", None)) or not callable(pty_worker.run):
                raise ImportError
    except (AttributeError, ImportError, OSError) as exc:
        raise PtyUnavailableError("PTY backend is unavailable") from exc


async def frozen_backend_smoke() -> dict[str, object]:
    """Exercise both frozen backends using only fixed, non-secret sentinels."""

    validate_pty_backend()
    inventory = discover_shells()
    shell = inventory.default
    marker = "openoctopus-exec-smoke"
    if shell in {"pwsh", "powershell", "powershell_x86"}:
        pipe_command = f"Write-Output '{marker}'"
        tty_command = "$value = Read-Host; Write-Output ('got:' + $value)"
    elif shell == "cmd":
        pipe_command = f"echo {marker}"
        tty_command = 'set /P "value=" & call echo got:%%value%%'
    else:
        pipe_command = f"printf '{marker}\\n'"
        tty_command = "read value; printf 'got:%s\\n' \"$value\""
    environment = build_child_env(
        os.environ,
        ("PATH", "SystemRoot", "WINDIR", "COMSPEC"),
    )
    pipe = await spawn_pipe(
        build_argv(shell, pipe_command, login=False, tty=False),
        cwd=None,
        env=environment,
    )
    stdout, stderr = await asyncio.gather(pipe.stdout.read(), pipe.stderr.read())
    pipe_exit = await pipe.wait()
    if (
        pipe_exit.returncode != 0
        or marker.encode() not in stdout
        or stderr
        or pipe.cleanup_incomplete
    ):
        raise ProcessBackendError("frozen pipe smoke failed")

    tty = await spawn_pty(
        build_argv(shell, tty_command, login=False, tty=True),
        cwd=None,
        env=environment,
    )
    await tty.write((marker + "\r\n").encode())
    output = await tty.output.read()
    tty_exit = await tty.wait()
    if tty_exit.returncode != 0 or f"got:{marker}".encode() not in output or tty.cleanup_incomplete:
        raise ProcessBackendError("frozen PTY smoke failed")
    return {
        "ok": True,
        "pipe": True,
        "tty": True,
        "shell": shell,
    }


def _resolve_executable(name: str, environment: Mapping[str, str] | None = None) -> str | None:
    if name not in {"bash", "sh", "zsh", "pwsh", "powershell", "powershell_x86", "cmd"}:
        return None
    if os.name == "nt":
        path = _windows_executable(name, environment)
        if path and Path(path).exists():
            return path
        return which(name, path=(environment or os.environ).get("PATH"))
    return which(name, path=(environment or os.environ).get("PATH"))


def build_argv(shell: str, command: str, *, login: bool, tty: bool) -> list[str]:
    """Build an explicit argv; never route command through ``shell=True``."""

    if not command or "\x00" in command:
        raise InvalidProcessArgumentsError("command must be non-empty and contain no NUL")
    if len(command) > MAX_COMMAND_CHARS:
        raise InvalidProcessArgumentsError("command is too long")
    executable = _resolve_executable(shell)
    if executable is None:
        raise ShellUnavailableError(f"shell unavailable: {shell}")
    if os.name != "nt":
        if shell == "bash":
            flags = ["-l"] if login else ["--noprofile", "--norc"]
            return [executable, *flags, "-c", command]
        if shell == "zsh":
            flags = ["-l"] if login else ["-f"]
            return [executable, *flags, "-c", command]
        if shell == "sh":
            if login:
                raise ShellLoginUnsupportedError("sh does not support login=true")
            return [executable, "-c", command]
        raise ShellUnavailableError(f"shell unavailable: {shell}")

    if shell in {"pwsh", "powershell", "powershell_x86"}:
        args = [executable, "-NoLogo"]
        if not login:
            args.append("-NoProfile")
        if not tty:
            args.append("-NonInteractive")
        args.extend(["-Command", command])
    elif shell == "cmd":
        if login:
            raise ShellLoginUnsupportedError("cmd does not support login=true")
        args = [executable, "/D", "/S", "/C", command]
    else:
        raise ShellUnavailableError(f"shell unavailable: {shell}")
    _validate_windows_command_line(shell, command, args)
    return args


def _validate_windows_command_line(shell: str, command: str, argv: Sequence[str]) -> None:
    command_units = len(command.encode("utf-16-le")) // 2
    if shell == "cmd" and command_units > MAX_CMD_COMMAND_UNITS:
        raise InvalidProcessArgumentsError("cmd command is too long")
    command_line = subprocess.list2cmdline(list(argv))
    wrapper_units = len(command_line.encode("utf-16-le")) // 2
    if wrapper_units >= MAX_WINDOWS_COMMAND_UNITS:
        raise InvalidProcessArgumentsError("Windows command line is too long")


def build_child_env(parent: Mapping[str, str], allowlist: Sequence[str]) -> dict[str, str]:
    """Create the sole environment snapshot passed to a child/helper."""

    result: dict[str, str] = {}
    insensitive = os.name == "nt"
    requested = {item.casefold() if insensitive else item for item in allowlist}
    for key, value in parent.items():
        normalized = key.casefold() if insensitive else key
        if normalized in requested and not key.casefold().startswith("openoctopus_"):
            result[key] = value
    fixed = {
        "TERM": "dumb",
        "NO_COLOR": "1",
        "PAGER": "cat",
        "GIT_PAGER": "cat",
        "GH_PAGER": "cat",
    }
    if insensitive:
        fixed_names = {key.casefold() for key in fixed}
        result = {key: value for key, value in result.items() if key.casefold() not in fixed_names}
    result.update(fixed)
    return {
        key: value for key, value in result.items() if not key.casefold().startswith("openoctopus_")
    }


def resolve_cwd(value: str | None, workspace: Path) -> Path:
    if value is None:
        return workspace
    if "\x00" in value or not value.strip():
        raise InvalidProcessArgumentsError("working_dir is invalid")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = workspace / path
    if not path.is_dir():
        raise InvalidProcessArgumentsError("working_dir must be an existing directory")
    return path


class OutputReader(Protocol):
    async def read(self, n: int = -1) -> bytes: ...


class ProcessHandle(Protocol):
    pid: int
    tty: bool
    stdout: OutputReader
    stderr: OutputReader
    output: OutputReader

    async def wait(self) -> ProcessExit: ...

    async def write(self, data: bytes) -> None: ...

    async def interrupt(self) -> bool: ...

    async def terminate(self) -> ProcessExit: ...


@dataclass(frozen=True)
class ProcessExit:
    returncode: int | None
    signal: int | None = None


class _JobHandle(Protocol):
    def terminate(self) -> bool: ...

    def close(self) -> bool: ...


class _WindowsJob:
    """Small dependency-free KILL_ON_JOB_CLOSE wrapper."""

    def __init__(self, handle: int, kernel32: Any) -> None:
        self._handle = handle
        self._kernel32 = kernel32

    def terminate(self) -> bool:
        return bool(self._kernel32.TerminateJobObject(self._handle, 1))

    def close(self) -> bool:
        if not self._handle:
            return True
        handle, self._handle = self._handle, 0
        return bool(self._kernel32.CloseHandle(handle))


class _JobBasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", ctypes.c_uint32),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", ctypes.c_uint32),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", ctypes.c_uint32),
        ("SchedulingClass", ctypes.c_uint32),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_ulonglong),
        ("WriteOperationCount", ctypes.c_ulonglong),
        ("OtherOperationCount", ctypes.c_ulonglong),
        ("ReadTransferCount", ctypes.c_ulonglong),
        ("WriteTransferCount", ctypes.c_ulonglong),
        ("OtherTransferCount", ctypes.c_ulonglong),
    ]


class _JobExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _JobBasicLimitInformation),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


def _create_windows_job(pid: int) -> _JobHandle | None:
    if os.name != "nt":
        return None
    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
    kernel32.CreateJobObjectW.restype = ctypes.c_void_p
    kernel32.SetInformationJobObject.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
    ]
    kernel32.SetInformationJobObject.restype = ctypes.c_int
    kernel32.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    kernel32.OpenProcess.restype = ctypes.c_void_p
    kernel32.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    kernel32.AssignProcessToJobObject.restype = ctypes.c_int
    kernel32.TerminateJobObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel32.TerminateJobObject.restype = ctypes.c_int
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_int
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        return None
    information = _JobExtendedLimitInformation()
    information.BasicLimitInformation.LimitFlags = 0x00002000
    configured = kernel32.SetInformationJobObject(
        job,
        9,
        ctypes.byref(information),
        ctypes.sizeof(information),
    )
    process_handle = 0
    assigned = False
    try:
        if not configured:
            return None
        process_handle = kernel32.OpenProcess(0x0001 | 0x0100, False, pid)
        if not process_handle:
            return None
        assigned = bool(kernel32.AssignProcessToJobObject(job, process_handle))
        if not assigned:
            return None
        return _WindowsJob(int(job), kernel32)
    finally:
        if process_handle:
            kernel32.CloseHandle(process_handle)
        if not assigned:
            kernel32.CloseHandle(job)


def _new_session_kwargs() -> dict[str, Any]:
    if os.name == "nt":
        creationflags = getattr(asyncio.subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        if not creationflags:
            creationflags = 0x00000200  # CREATE_NEW_PROCESS_GROUP
        return {"creationflags": creationflags}
    return {"start_new_session": True}


async def spawn_pipe(
    argv: Sequence[str], *, cwd: Path | None, env: Mapping[str, str]
) -> PipeProcessHandle:
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(cwd) if cwd is not None else None,
        env=dict(env),
        **_new_session_kwargs(),
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        raise ProcessBackendError("pipe streams were not created")
    # Windows' NUL character device reports isatty() as true.  Closing the
    # parent side of a real anonymous pipe gives the child immediate EOF while
    # preserving the non-TTY pipe contract.
    process.stdin.close()
    return PipeProcessHandle(process)


class PipeProcessHandle:
    def __init__(self, process: asyncio.subprocess.Process) -> None:
        self.process = process
        self.pid = process.pid
        self.tty = False
        assert process.stdout is not None and process.stderr is not None
        self.stdout = process.stdout
        self.stderr = process.stderr
        self.output = self.stdout
        self.terminal_control_truncated = False
        self.cleanup_incomplete = False
        self._job = _create_windows_job(self.pid) if os.name == "nt" else None
        self._job_assignment_failed = os.name == "nt" and self._job is None

    async def write(self, data: bytes) -> None:
        del data
        raise ProcessBackendError("pipe stdin is closed")

    async def wait(self) -> ProcessExit:
        if os.name == "nt":
            # Proactor process.wait() is not resolved until inherited stdout
            # and stderr handles reach EOF.  A surviving descendant can hold
            # those handles forever, so observe the root exit first and close
            # its kill-on-close Job before awaiting transport convergence.
            while self.process.returncode is None:
                await asyncio.sleep(0.01)
            code = self.process.returncode
            if self._job is None:
                self.cleanup_incomplete = self._job_assignment_failed
            elif not self._job.close():
                self.cleanup_incomplete = True
            try:
                await asyncio.wait_for(self.process.wait(), 2)
            except TimeoutError:
                self.cleanup_incomplete = True
        else:
            code = await self.process.wait()
            complete = await _terminate_posix(self.process, self.pid)
            self.cleanup_incomplete = self.cleanup_incomplete or not complete
        return ProcessExit(code, -code if code < 0 else None)

    async def interrupt(self) -> bool:
        if self.process.returncode is not None:
            return True
        try:
            if os.name == "nt":
                self.process.send_signal(getattr(signal, "CTRL_BREAK_EVENT", signal.SIGINT))
            else:
                _send_process_group_signal(self.pid, signal.SIGINT)
            return True
        except (OSError, ValueError):
            return False

    async def terminate(self) -> ProcessExit:
        if self.process.returncode is None:
            if os.name == "nt":
                complete = await _terminate_windows(self.process, self._job)
                self.cleanup_incomplete = self.cleanup_incomplete or not complete
                if complete:
                    self._job_assignment_failed = False
            else:
                complete = await _terminate_posix(self.process, self.pid)
                self.cleanup_incomplete = self.cleanup_incomplete or not complete
        elif os.name != "nt":
            complete = await _terminate_posix(self.process, self.pid)
            self.cleanup_incomplete = self.cleanup_incomplete or not complete
        return await self.wait()


def _send_process_group_signal(pid: int, sig: int) -> None:
    killpg = cast(Callable[[int, int], None], getattr(os, "killpg"))
    killpg(pid, sig)


def _process_group_exists(pid: int) -> bool:
    try:
        _send_process_group_signal(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _windows_process_snapshot() -> dict[int, int] | None:
    if os.name != "nt":
        return None
    from ctypes import wintypes

    class ProcessEntry32W(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_size_t),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", wintypes.WCHAR * 260),
        ]

    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    create_snapshot = kernel32.CreateToolhelp32Snapshot
    create_snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    create_snapshot.restype = wintypes.HANDLE
    first = kernel32.Process32FirstW
    first.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W)]
    first.restype = wintypes.BOOL
    next_entry = kernel32.Process32NextW
    next_entry.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W)]
    next_entry.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL
    set_last_error = cast(Callable[[int], None], getattr(ctypes, "set_last_error"))
    get_last_error = cast(Callable[[], int], getattr(ctypes, "get_last_error"))

    snapshot = create_snapshot(0x00000002, 0)  # TH32CS_SNAPPROCESS
    if not snapshot or int(snapshot) == ctypes.c_void_p(-1).value:
        return None
    processes: dict[int, int] = {}
    complete = True
    try:
        entry = ProcessEntry32W()
        entry.dwSize = ctypes.sizeof(entry)
        set_last_error(0)
        available = bool(first(snapshot, ctypes.byref(entry)))
        if not available and get_last_error() != 18:  # ERROR_NO_MORE_FILES
            complete = False
        while available:
            processes[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            set_last_error(0)
            available = bool(next_entry(snapshot, ctypes.byref(entry)))
        if get_last_error() not in {0, 18}:
            complete = False
    finally:
        if not close_handle(snapshot):
            complete = False
    return processes if complete else None


def _windows_process_tree(pid: int) -> set[int] | None:
    snapshot = _windows_process_snapshot()
    if snapshot is None:
        return None
    tree = {pid}
    while True:
        children = {
            child_pid
            for child_pid, parent_pid in snapshot.items()
            if parent_pid in tree and child_pid not in tree
        }
        if not children:
            return tree
        tree.update(children)


async def _wait_windows_processes_gone(pids: set[int]) -> bool:
    tracked = set(pids)
    deadline = asyncio.get_running_loop().time() + 2
    while True:
        snapshot = _windows_process_snapshot()
        if snapshot is None:
            return False
        while True:
            children = {
                child_pid
                for child_pid, parent_pid in snapshot.items()
                if parent_pid in tracked and child_pid not in tracked
            }
            if not children:
                break
            tracked.update(children)
        if not tracked.intersection(snapshot):
            return True
        if asyncio.get_running_loop().time() >= deadline:
            return False
        await asyncio.sleep(0.05)


async def _terminate_posix(process: asyncio.subprocess.Process, pid: int) -> bool:
    del process
    if not _process_group_exists(pid):
        return True
    try:
        _send_process_group_signal(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except PermissionError:
        pass
    deadline = asyncio.get_running_loop().time() + 2
    while _process_group_exists(pid) and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.05)
    if _process_group_exists(pid):
        try:
            sigkill = cast(int, getattr(signal, "SIGKILL"))
            _send_process_group_signal(pid, sigkill)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        await asyncio.sleep(0)
    return not _process_group_exists(pid)


async def _terminate_windows(
    process: asyncio.subprocess.Process, job: _JobHandle | None = None
) -> bool:
    if job is not None and job.terminate():
        return job.close()
    tree = _windows_process_tree(process.pid)
    taskkill = os.path.join(
        os.environ.get("SystemRoot", r"C:\\Windows"), "System32", "taskkill.exe"
    )
    killer: asyncio.subprocess.Process | None = None
    taskkill_complete = False
    try:
        killer = await asyncio.create_subprocess_exec(
            taskkill,
            "/PID",
            str(process.pid),
            "/T",
            "/F",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        taskkill_complete = await asyncio.wait_for(killer.wait(), 2) == 0
    except (OSError, TimeoutError):
        if killer is not None and getattr(killer, "returncode", None) is None:
            with contextlib.suppress(OSError, ProcessLookupError):
                killer.kill()
            with contextlib.suppress(OSError, TimeoutError):
                await asyncio.wait_for(killer.wait(), 1)
    tree_gone = bool(
        taskkill_complete
        and tree is not None
        and await _wait_windows_processes_gone(tree)
    )
    job_closed = job is None or job.close()
    complete = taskkill_complete and tree_gone and job_closed
    if not complete and getattr(process, "returncode", None) is None:
        kill = getattr(process, "kill", None)
        if callable(kill):
            with contextlib.suppress(OSError, ProcessLookupError):
                kill()
    return complete


class _NullReader:
    async def read(self, n: int = -1) -> bytes:
        del n
        return b""


class _AsyncOutputBuffer:
    """Byte-bounded async bridge for output read from the POSIX helper."""

    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._chunks: deque[bytes] = deque()
        self._bytes = 0
        self._closed = False
        self._condition = asyncio.Condition()

    async def put(self, payload: bytes) -> bool:
        """Apply backpressure until the bounded bridge accepts ``payload``."""

        if not payload:
            return True
        offset = 0
        while offset < len(payload):
            async with self._condition:
                while not self._closed and self._bytes >= self._max_bytes:
                    await self._condition.wait()
                if self._closed:
                    return False
                size = min(len(payload) - offset, self._max_bytes - self._bytes)
                self._chunks.append(payload[offset : offset + size])
                self._bytes += size
                offset += size
                self._condition.notify_all()
        return True

    async def put_if_fits(self, payload: bytes) -> bool:
        """Queue a small final fragment without blocking the event loop."""

        if not payload:
            return True
        async with self._condition:
            if self._closed or self._bytes + len(payload) > self._max_bytes:
                return False
            self._chunks.append(payload)
            self._bytes += len(payload)
            self._condition.notify_all()
            return True

    async def get(self) -> bytes | None:
        async with self._condition:
            while not self._chunks and not self._closed:
                await self._condition.wait()
            if not self._chunks:
                return None
            payload = self._chunks.popleft()
            self._bytes -= len(payload)
            self._condition.notify_all()
            return payload

    async def close(self) -> None:
        async with self._condition:
            self._closed = True
            self._condition.notify_all()


class _ThreadOutputBuffer:
    """Byte-bounded bridge used by the synchronous ConPTY reader thread."""

    def __init__(self, max_bytes: int, *, wake: Callable[[], None] | None = None) -> None:
        self._max_bytes = max_bytes
        self._chunks: deque[bytes] = deque()
        self._bytes = 0
        self._closed = False
        self._condition = threading.Condition()
        self._wake = wake

    def put(self, payload: bytes, stop: threading.Event) -> bool:
        if not payload:
            return True
        offset = 0
        while offset < len(payload):
            wake = False
            with self._condition:
                while not self._closed and not stop.is_set() and self._bytes >= self._max_bytes:
                    self._condition.wait()
                if self._closed or stop.is_set():
                    return False
                size = min(len(payload) - offset, self._max_bytes - self._bytes)
                wake = not self._chunks
                self._chunks.append(payload[offset : offset + size])
                self._bytes += size
                offset += size
                self._condition.notify_all()
            if wake and self._wake is not None:
                self._wake()
        return True

    def put_if_fits(self, payload: bytes) -> bool:
        if not payload:
            return True
        wake = False
        with self._condition:
            if self._closed or self._bytes + len(payload) > self._max_bytes:
                return False
            wake = not self._chunks
            self._chunks.append(payload)
            self._bytes += len(payload)
            self._condition.notify_all()
        if wake and self._wake is not None:
            self._wake()
        return True

    def get(self) -> bytes | None:
        with self._condition:
            while not self._chunks and not self._closed:
                self._condition.wait()
            if not self._chunks:
                return None
            payload = self._chunks.popleft()
            self._bytes -= len(payload)
            self._condition.notify_all()
            return payload

    def get_nowait(self) -> tuple[bool, bytes | None]:
        with self._condition:
            if self._chunks:
                payload = self._chunks.popleft()
                self._bytes -= len(payload)
                self._condition.notify_all()
                return True, payload
            if self._closed:
                return True, None
            return False, None

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        if self._wake is not None:
            self._wake()


class _AsyncChunkSource(Protocol):
    async def get(self) -> bytes | None: ...


class _QueueReader:
    """Stream-like reader that never waits for a second chunk after data arrives."""

    def __init__(self, queue: _AsyncChunkSource) -> None:
        self._queue = queue
        self._remainder = b""

    async def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n < 0:
            chunks = [self._remainder] if self._remainder else []
            self._remainder = b""
            while True:
                chunk = await self._queue.get()
                if chunk is None:
                    return b"".join(chunks)
                chunks.append(chunk)
        if self._remainder:
            chunk, self._remainder = self._remainder[:n], self._remainder[n:]
            return chunk
        chunk = await self._queue.get()
        if chunk is None:
            return b""
        result, self._remainder = chunk[:n], chunk[n:]
        return result


class _ThreadQueueReader:
    """Async reader for a thread-safe bridge without using the default executor."""

    def __init__(self, queue: _ThreadOutputBuffer, ready: asyncio.Event) -> None:
        self._queue = queue
        self._ready = ready
        self._remainder = b""

    async def _get(self) -> bytes | None:
        while True:
            available, payload = self._queue.get_nowait()
            if available:
                return payload
            self._ready.clear()
            available, payload = self._queue.get_nowait()
            if available:
                return payload
            await self._ready.wait()

    async def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n < 0:
            chunks = [self._remainder] if self._remainder else []
            self._remainder = b""
            while True:
                chunk = await self._get()
                if chunk is None:
                    return b"".join(chunks)
                chunks.append(chunk)
        if self._remainder:
            chunk, self._remainder = self._remainder[:n], self._remainder[n:]
            return chunk
        chunk = await self._get()
        if chunk is None:
            return b""
        result, self._remainder = chunk[:n], chunk[n:]
        return result


async def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    loop = asyncio.get_running_loop()
    while len(data) < size:
        chunk = await loop.sock_recv(sock, size - len(data))
        if not chunk:
            raise ProcessBackendError("pty helper channel closed")
        data.extend(chunk)
    return bytes(data)


class PtyProcessHandle:
    """A PTY process controlled through the isolated POSIX helper."""

    def __init__(
        self,
        helper: asyncio.subprocess.Process,
        control: socket.socket,
        events: socket.socket,
    ) -> None:
        self.helper = helper
        self.control = control
        self.events = events
        self.pid = 0
        self.tty = True
        self._queue = _AsyncOutputBuffer(PTY_OUTPUT_QUEUE_BYTES)
        self.stdout = _QueueReader(self._queue)
        self.stderr = _NullReader()
        self.output = self.stdout
        self.normalizer = TerminalNormalizer()
        self.cleanup_incomplete = False
        self._exit: asyncio.Future[ProcessExit] = asyncio.get_running_loop().create_future()
        self._send_lock = asyncio.Lock()
        self._next_request_id = 1
        self._acks: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._response_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=PTY_RESPONSE_QUEUE_MAX)
        self._response_pump_task = asyncio.create_task(self._response_pump())
        self._queue_closed = False
        self._cancel_cleanup_task: asyncio.Task[bool] | None = None
        self._reader_task = asyncio.create_task(self._read_events())
        self._reader_task.add_done_callback(self._reader_done)

    @property
    def terminal_control_truncated(self) -> bool:
        return self.normalizer.control_truncated

    def _reader_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            return
        if not self._exit.done():
            self._exit.set_result(ProcessExit(None))
        if not self._queue_closed:
            self._queue_closed = True
            asyncio.create_task(self._queue.close())
        if self._cancel_cleanup_task is None:
            self._cancel_cleanup_task = asyncio.create_task(self._force_stop())

    async def _command(self, payload: dict[str, Any], *, timeout: float = 5) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        async with self._send_lock:
            request_id = self._next_request_id
            self._next_request_id += 1
            message = dict(payload, id=request_id)
            future: asyncio.Future[dict[str, Any]] = loop.create_future()
            self._acks[request_id] = future
            data = (json.dumps(message, separators=(",", ":")) + "\n").encode()
            try:
                await loop.sock_sendall(self.control, data)
            except BaseException:
                self._acks.pop(request_id, None)
                raise
        try:
            result = await asyncio.wait_for(asyncio.shield(future), timeout)
        except BaseException:
            self._acks.pop(request_id, None)
            raise
        if not result.get("ok"):
            raise ProcessBackendError("pty helper rejected a control request")
        return result

    async def _read_events(self) -> None:
        cancelled = False
        try:
            while True:
                header = await _recv_exact(self.events, 5)
                kind, size = header[:1], struct.unpack(">I", header[1:])[0]
                if size > MAX_PTY_EVENT_BYTES:
                    raise ProcessBackendError("pty helper event is too large")
                payload = await _recv_exact(self.events, size)
                if kind == b"O":
                    text = self.normalizer.feed(payload)
                    if text:
                        await self._queue.put(text.encode("utf-8"))
                    for response in self.normalizer.take_responses():
                        try:
                            self._response_queue.put_nowait(response)
                        except asyncio.QueueFull:
                            self.normalizer.control_truncated = True
                elif kind == b"A":
                    event = json.loads(payload.decode("utf-8"))
                    request_id = event.get("id")
                    if isinstance(request_id, int):
                        future = self._acks.pop(request_id, None)
                        if future is not None and not future.done():
                            future.set_result(event)
                elif kind == b"E":
                    event = json.loads(payload.decode("utf-8"))
                    self.cleanup_incomplete = bool(event.get("cleanup_incomplete", False))
                    tail = self.normalizer.flush()
                    if tail:
                        await self._queue.put_if_fits(tail.encode("utf-8"))
                    if not self._exit.done():
                        code = event.get("returncode")
                        self._exit.set_result(
                            ProcessExit(
                                code,
                                -code if isinstance(code, int) and code < 0 else None,
                            )
                        )
                    break
                else:
                    raise ProcessBackendError("pty helper sent an unknown event")
        except asyncio.CancelledError:
            cancelled = True
            if not self._exit.done():
                self._exit.set_result(ProcessExit(None))
            raise
        except (OSError, ValueError, ProcessBackendError):
            self.cleanup_incomplete = not await self._force_stop()
            if not self._exit.done():
                self._exit.set_result(ProcessExit(1))
        finally:
            self._response_pump_task.cancel()
            await asyncio.gather(self._response_pump_task, return_exceptions=True)
            error = ProcessBackendError("pty helper channel closed")
            for future in self._acks.values():
                if not future.done():
                    future.set_exception(error)
            self._acks.clear()
            await self._close_queue()
            if cancelled:
                self._cancel_cleanup_task = asyncio.create_task(self._force_stop())
                complete = await asyncio.shield(self._cancel_cleanup_task)
                self.cleanup_incomplete = not complete
            else:
                await self._reap_helper()

    async def _close_queue(self) -> None:
        if not self._queue_closed:
            self._queue_closed = True
            await self._queue.close()

    async def _reap_helper(self) -> None:
        wait_task = asyncio.create_task(self.helper.wait())
        try:
            await asyncio.wait_for(asyncio.shield(wait_task), 1)
        except TimeoutError:
            try:
                self.helper.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(asyncio.shield(wait_task), 1)
            except TimeoutError:
                self.cleanup_incomplete = True
        finally:
            self.control.close()
            self.events.close()

    async def _force_stop(self) -> bool:
        complete = True
        if os.name != "nt" and self.pid > 0:
            complete = await _terminate_posix(self.helper, self.pid)
        if getattr(self.helper, "returncode", None) is None:
            try:
                self.helper.kill()
            except ProcessLookupError:
                pass
        wait_task = asyncio.create_task(self.helper.wait())
        try:
            await asyncio.wait_for(asyncio.shield(wait_task), 1)
        except TimeoutError:
            complete = False
        self.control.close()
        self.events.close()
        return complete

    async def _write_terminal_response(self, response: bytes) -> None:
        try:
            await self._command(
                {"type": "write_raw", "data": base64.b64encode(response).decode()},
                timeout=1,
            )
        except (OSError, TimeoutError, ProcessBackendError):
            if not self._exit.done():
                self._exit.set_result(ProcessExit(1))
            complete = await self._force_stop()
            self.cleanup_incomplete = self.cleanup_incomplete or not complete

    async def _response_pump(self) -> None:
        while True:
            response = await self._response_queue.get()
            try:
                await self._write_terminal_response(response)
            finally:
                self._response_queue.task_done()

    async def write(self, data: bytes) -> None:
        await self._command({"type": "write", "data": base64.b64encode(data).decode()})

    async def wait(self) -> ProcessExit:
        result = await asyncio.shield(self._exit)
        if asyncio.current_task() is not self._reader_task:
            try:
                await asyncio.shield(self._reader_task)
            except asyncio.CancelledError:
                if not self._reader_task.cancelled():
                    raise
            if self._cancel_cleanup_task is not None:
                complete = await asyncio.shield(self._cancel_cleanup_task)
                self.cleanup_incomplete = self.cleanup_incomplete or not complete
        return result

    async def interrupt(self) -> bool:
        if self._exit.done():
            return True
        try:
            await self._command({"type": "interrupt"})
            return True
        except (OSError, TimeoutError, ProcessBackendError):
            return False

    async def terminate(self) -> ProcessExit:
        if not self._exit.done():
            try:
                await self._command({"type": "terminate"})
            except (OSError, TimeoutError, ProcessBackendError):
                pass
        try:
            return await asyncio.wait_for(self.wait(), 3)
        except TimeoutError:
            complete = await self._force_stop()
            self.cleanup_incomplete = self.cleanup_incomplete or not complete
            if not self._exit.done():
                self._exit.set_result(ProcessExit(None))
            return await self.wait()


async def spawn_pty(
    argv: Sequence[str], *, cwd: Path | None, env: Mapping[str, str]
) -> PtyProcessHandle | ConPtyProcessHandle:
    if os.name == "nt":
        return await _spawn_conpty(argv, cwd=cwd, env=env)
    if not argv:
        raise InvalidProcessArgumentsError("argv is empty")
    control_parent, control_child = socket.socketpair()
    events_parent, events_child = socket.socketpair()
    for sock in (control_parent, events_parent):
        sock.setblocking(False)
    helper_env = dict(env)
    # Source installs need an import path for ``-m``; frozen builds bundle the
    # module and use their bootloader path instead.  This is a bootstrap path,
    # not a copy of the parent environment and contains no credentials.
    if not getattr(sys, "frozen", False):
        helper_env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    helper_command = _pty_worker_command(
        control_child.fileno(),
        events_child.fileno(),
        frozen=bool(getattr(sys, "frozen", False)),
    )
    try:
        helper = await asyncio.create_subprocess_exec(
            *helper_command,
            pass_fds=(control_child.fileno(), events_child.fileno()),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            cwd=str(cwd) if cwd is not None else None,
            env=helper_env,
            **_new_session_kwargs(),
        )
    except asyncio.CancelledError:
        for sock in (control_parent, control_child, events_parent, events_child):
            sock.close()
        raise
    except Exception as exc:
        for sock in (control_parent, control_child, events_parent, events_child):
            sock.close()
        raise PtyUnavailableError("PTY helper could not start") from exc
    finally:
        control_child.close()
        events_child.close()
    handle = PtyProcessHandle(helper, control_parent, events_parent)
    request = {
        "type": "spawn",
        "argv": list(argv),
        "cwd": str(cwd) if cwd else None,
        "env": dict(env),
    }
    try:
        ready = await handle._command(request)
        pid = ready.get("pid")
        if not isinstance(pid, int) or pid < 1:
            raise ProcessBackendError("pty helper returned an invalid child pid")
        handle.pid = pid
        return handle
    except asyncio.CancelledError:
        await handle._force_stop()
        raise
    except Exception as exc:
        await handle._force_stop()
        raise PtyUnavailableError("PTY allocation failed") from exc


def _pty_worker_command(control_fd: int, events_fd: int, *, frozen: bool) -> list[str]:
    if frozen:
        return [sys.executable, "_pty-worker", str(control_fd), str(events_fd)]
    return [
        sys.executable,
        "-m",
        "openoctopus_client.pty_worker",
        str(control_fd),
        str(events_fd),
    ]


class ConPtyProcessHandle:
    """pywinpty adapter; synchronous ConPTY reads stay on one reader thread."""

    def __init__(self, process: Any) -> None:
        self.process = process
        self.pid = int(getattr(process, "pid", 0))
        self.tty = True
        self._loop = asyncio.get_running_loop()
        self._output_ready = asyncio.Event()
        self._output_buffer = _ThreadOutputBuffer(
            PTY_OUTPUT_QUEUE_BYTES,
            wake=self._wake_output_reader,
        )
        self.stdout = _ThreadQueueReader(self._output_buffer, self._output_ready)
        self.output = self.stdout
        self.stderr = _NullReader()
        self._normalizer = TerminalNormalizer()
        self._stop = threading.Event()
        self._io_lock = threading.Lock()
        self._writer_lock = asyncio.Lock()
        self._writer = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="openoctopus-conpty-writer",
        )
        self._exit: asyncio.Future[ProcessExit] = self._loop.create_future()
        self._finished = False
        self.cleanup_incomplete = False
        self._job = _create_windows_job(self.pid)
        self._job_assignment_failed = self._job is None
        self._reader = threading.Thread(
            target=self._read_loop, name="openoctopus-conpty", daemon=True
        )
        self._exit_watcher = threading.Thread(
            target=self._watch_exit,
            name="openoctopus-conpty-exit",
            daemon=True,
        )
        self._reader.start()
        self._exit_watcher.start()

    @property
    def terminal_control_truncated(self) -> bool:
        return self._normalizer.control_truncated

    def _wake_output_reader(self) -> None:
        self._loop.call_soon_threadsafe(self._output_ready.set)

    def _read_loop(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    raw = self.process.read()
                except EOFError:
                    break
                if not raw:
                    break
                payload = raw.encode() if isinstance(raw, str) else bytes(raw)
                text = self._normalizer.feed(payload)
                for response in self._normalizer.take_responses():
                    try:
                        self._write_sync(response.decode("ascii"))
                    except (EOFError, OSError, RuntimeError):
                        self._force_close_sync()
                        self._loop.call_soon_threadsafe(self._fail_terminal_response)
                        return
                if text:
                    if not self._output_buffer.put(text.encode("utf-8"), self._stop):
                        return
            self._loop.call_soon_threadsafe(self._finish)
        except BaseException as exc:
            self._force_close_sync()
            self._loop.call_soon_threadsafe(self._fail, exc)

    def _watch_exit(self) -> None:
        try:
            self.process.wait()
            if self._reader.is_alive():
                self.process.pty.cancel_io()
        except Exception as exc:
            self._force_close_sync()
            self._loop.call_soon_threadsafe(self._fail, exc)

    def _write_sync(self, value: str) -> None:
        if not value:
            return
        with self._io_lock:
            # pywinpty 3.0.5 submits ConPTY input with overlapped writes.  Its
            # count can describe an earlier completed write, so zero is valid
            # even after the complete current string was accepted.  Retrying a
            # suffix from that count can duplicate multibyte input.
            written = self.process.write(value)
            if not isinstance(written, int) or written < 0:
                raise OSError("ConPTY input write returned an invalid result")

    def _force_close_sync(self) -> None:
        self._stop.set()
        self._output_buffer.close()
        try:
            self.process.close(True)
        except (EOFError, OSError, RuntimeError, TypeError):
            self.cleanup_incomplete = True

    def _finish(self) -> None:
        if self._finished:
            return
        self._finished = True
        self._stop.set()
        tail = self._normalizer.flush()
        if tail:
            self._output_buffer.put_if_fits(tail.encode("utf-8"))
        self._output_buffer.close()
        if self._job is None:
            self.cleanup_incomplete = self._job_assignment_failed
        elif not self._job.close():
            self.cleanup_incomplete = True
        if not self._exit.done():
            try:
                code = getattr(self.process, "exitstatus", None)
            except (EOFError, OSError, RuntimeError):
                code = None
            if not isinstance(code, int):
                code = 1
            self._exit.set_result(ProcessExit(code, -code if code < 0 else None))
        self._writer.shutdown(wait=False, cancel_futures=True)

    def _fail_terminal_response(self) -> None:
        if self._finished:
            return
        self._finished = True
        if self._job is None:
            self.cleanup_incomplete = True
        else:
            terminated = self._job.terminate()
            closed = self._job.close()
            self.cleanup_incomplete = not (terminated and closed)
        if not self._exit.done():
            self._exit.set_result(ProcessExit(1))
        self._output_buffer.close()
        self._writer.shutdown(wait=False, cancel_futures=True)

    def _fail(self, exc: BaseException) -> None:
        del exc
        if self._finished:
            return
        self._finished = True
        if self._job is None:
            self.cleanup_incomplete = True
        else:
            terminated = self._job.terminate()
            closed = self._job.close()
            self.cleanup_incomplete = not (terminated and closed)
        if not self._exit.done():
            self._exit.set_result(ProcessExit(1))
        self._output_buffer.close()
        self._writer.shutdown(wait=False, cancel_futures=True)

    async def write(self, data: bytes) -> None:
        value = data.decode("utf-8", errors="replace")
        async with self._writer_lock:
            await self._loop.run_in_executor(self._writer, self._write_sync, value)

    async def wait(self) -> ProcessExit:
        return await asyncio.shield(self._exit)

    async def interrupt(self) -> bool:
        try:
            await self.write(b"\x03")
            return True
        except (OSError, RuntimeError):
            return False

    async def terminate(self) -> ProcessExit:
        self._stop.set()
        self._output_buffer.close()
        if os.name == "nt":
            complete = await _terminate_windows(cast(Any, self.process), self._job)
        else:
            complete = self._job.terminate() if self._job is not None else False
        if not complete:
            async with self._writer_lock:
                try:
                    await self._loop.run_in_executor(self._writer, self._force_close_sync)
                except RuntimeError:
                    self._force_close_sync()
            complete = not self.cleanup_incomplete
        self.cleanup_incomplete = self.cleanup_incomplete or not complete
        deadline = self._loop.time() + 2
        while self._reader.is_alive() and self._loop.time() < deadline:
            await asyncio.sleep(0.05)
        if self._reader.is_alive():
            self._force_close_sync()
            deadline = self._loop.time() + 1
            while self._reader.is_alive() and self._loop.time() < deadline:
                await asyncio.sleep(0.05)
            if self._reader.is_alive():
                self.cleanup_incomplete = True
        if not self._exit.done():
            self._finish()
        return await self.wait()


async def _spawn_conpty(
    argv: Sequence[str], *, cwd: Path | None, env: Mapping[str, str]
) -> ConPtyProcessHandle:
    try:
        winpty = importlib.import_module("winpty")
        backend_type = getattr(winpty, "Backend")
        pty_process_type = getattr(winpty, "PtyProcess")
    except (AttributeError, ImportError) as exc:
        raise PtyUnavailableError("pywinpty/ConPTY is not installed") from exc
    if not argv:
        raise InvalidProcessArgumentsError("argv is empty")
    spawn_call = partial(
        pty_process_type.spawn,
        list(argv),
        cwd=str(cwd) if cwd is not None else None,
        env=dict(env),
        dimensions=(24, 80),
        # ``Backend.ConPTY`` is integer zero.  pywinpty 3.0.5 treats a numeric
        # zero as absent and consults PYWINPTY_BACKEND, while the equivalent
        # truthy string is parsed back to the required explicit backend.
        backend=str(backend_type.ConPTY),
    )
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="openoctopus-conpty-spawn")
    try:
        process = await asyncio.get_running_loop().run_in_executor(executor, spawn_call)
    except (OSError, RuntimeError, TypeError) as exc:
        raise PtyUnavailableError("ConPTY allocation failed") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=False)
    return ConPtyProcessHandle(process)


@dataclass(frozen=True)
class ProcessSpec:
    """Stable adapter input for ExecSessionManager."""

    argv: tuple[str, ...]
    cwd: Path | None
    env: Mapping[str, str]
    tty: bool = False


async def spawn_process(spec: ProcessSpec) -> ProcessHandle:
    """Spawn either backend without exposing platform details to the manager."""

    if spec.tty:
        return cast(ProcessHandle, await spawn_pty(spec.argv, cwd=spec.cwd, env=spec.env))
    return cast(ProcessHandle, await spawn_pipe(spec.argv, cwd=spec.cwd, env=spec.env))
