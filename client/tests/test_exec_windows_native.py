from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import psutil  # type: ignore[import-untyped]
import pytest

import openoctopus_client.process as process_module
from openoctopus_client.exec_sessions import ExecPolicy, ExecSessionManager, ExecStart, ExecWrite
from openoctopus_client.process import (
    ConPtyProcessHandle,
    PipeProcessHandle,
    build_argv,
    build_child_env,
    discover_shells,
    spawn_pipe,
)

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="native Windows contract")


def _child_environment(*extra: str) -> dict[str, str]:
    return build_child_env(
        os.environ,
        (
            "PATH",
            "SystemRoot",
            "WINDIR",
            "COMSPEC",
            "PATHEXT",
            "TEMP",
            "TMP",
            "USERPROFILE",
            *extra,
        ),
    )


def _shell_command(shell: str, argv: tuple[str, ...]) -> str:
    if shell in {"pwsh", "powershell", "powershell_x86"}:
        quoted = " ".join("'" + value.replace("'", "''") + "'" for value in argv)
        return f"& {quoted}"
    if shell == "cmd":
        return subprocess.list2cmdline(list(argv))
    raise AssertionError(f"unexpected native Windows shell: {shell}")


def _exec_request(
    workspace: Path,
    command: str,
    *,
    timeout: int = 30,
    yield_ms: int = 0,
    tty: bool = False,
) -> ExecStart:
    shell = discover_shells().default
    return ExecStart(
        policy=ExecPolicy(
            workspace=workspace,
            sandbox_mode=False,
            shell_timeout_max=60,
            env_allowlist=(
                "PATH",
                "SystemRoot",
                "WINDIR",
                "COMSPEC",
                "PATHEXT",
                "TEMP",
                "TMP",
                "USERPROFILE",
            ),
            available_shells=discover_shells().available,
            default_shell=shell,
            epoch=1,
        ),
        command=command,
        working_dir=None,
        timeout_seconds=timeout,
        shell=shell,
        login=False,
        tty=tty,
        yield_time_ms=yield_ms,
        max_output_chars=100_000,
    )


def _session_id(output: object) -> UUID:
    content = cast(Any, output).content
    match = re.search(r"^session_id=([^\r\n]+)", cast(str, content), re.MULTILINE)
    assert match is not None
    return UUID(match.group(1))


def _process_alive(process: psutil.Process) -> bool:
    try:
        return bool(process.is_running() and process.status() != psutil.STATUS_ZOMBIE)
    except psutil.NoSuchProcess:
        return False


def _write_tree_script(path: Path, release: Path) -> None:
    path.write_text(
        "import json, os, subprocess, sys, time\n"
        "from pathlib import Path\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        "print(json.dumps({'root': os.getpid(), 'child': child.pid}), flush=True)\n"
        f"release = Path({str(release)!r})\n"
        "while not release.exists():\n"
        "    time.sleep(0.02)\n",
        encoding="utf-8",
    )


async def _tree_processes(handle: PipeProcessHandle) -> tuple[psutil.Process, psutil.Process]:
    payload = json.loads(await asyncio.wait_for(handle.stdout.readline(), timeout=5))
    assert payload["root"] == handle.pid
    return psutil.Process(payload["root"]), psutil.Process(payload["child"])


async def _wait_processes_gone(
    processes: tuple[psutil.Process, ...], *, timeout: float = 8
) -> None:
    async with asyncio.timeout(timeout):
        while any(_process_alive(process) for process in processes):
            await asyncio.sleep(0.02)


async def _force_stop_processes(processes: tuple[psutil.Process, ...]) -> None:
    for process in reversed(processes):
        if _process_alive(process):
            with contextlib.suppress(psutil.Error):
                process.kill()
    with contextlib.suppress(TimeoutError):
        await _wait_processes_gone(processes, timeout=5)


async def _read_until_bytes(
    reader: Any,
    marker: bytes,
    *,
    timeout: float = 10,
) -> bytes:
    retained = bytearray()
    async with asyncio.timeout(timeout):
        while marker not in retained:
            chunk = await reader.read(64 * 1024)
            if not chunk:
                raise AssertionError("process reached EOF before the readiness marker")
            retained.extend(chunk)
    return bytes(retained)


async def _drain_stream(reader: Any, marker: bytes, ready: asyncio.Event) -> bytes:
    retained = bytearray()
    while True:
        chunk = await reader.read(64 * 1024)
        if not chunk:
            return bytes(retained)
        retained.extend(chunk)
        if marker in retained:
            ready.set()


async def _read_all(reader: Any) -> bytes:
    retained = bytearray()
    while True:
        chunk = await reader.read(64 * 1024)
        if not chunk:
            return bytes(retained)
        retained.extend(chunk)


async def _close_pipe(handle: PipeProcessHandle) -> None:
    if handle.process.returncode is None:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(handle.terminate(), timeout=8)
    elif handle._job is not None:  # noqa: SLF001 - native handle-close contract
        with contextlib.suppress(Exception):
            await asyncio.wait_for(handle.wait(), timeout=3)


async def _read_until(
    handle: ConPtyProcessHandle,
    expected: str,
    *,
    timeout: float = 10,
) -> None:
    marker = expected.encode("utf-8")
    retained = bytearray()
    async with asyncio.timeout(timeout):
        while marker not in retained:
            chunk = await handle.output.read(64 * 1024)
            if not chunk:
                raise AssertionError("ConPTY reached EOF before the readiness marker")
            retained.extend(chunk)
            if len(retained) > 256 * 1024:
                del retained[: len(retained) - 256 * 1024]


async def _close_conpty(handle: ConPtyProcessHandle) -> None:
    if not handle._exit.done():
        with contextlib.suppress(Exception):
            await asyncio.wait_for(handle.terminate(), timeout=8)
    with contextlib.suppress(Exception):
        await asyncio.to_thread(handle.process.close, True)
    await asyncio.to_thread(handle._reader.join, 2)
    await asyncio.to_thread(handle._exit_watcher.join, 2)


async def _wait_for_backend_process(
    parent: psutil.Process,
    before: set[int],
) -> set[str]:
    expected = {"openconsole.exe", "winpty-agent.exe"}
    async with asyncio.timeout(5):
        while True:
            names = {
                child.name().casefold()
                for child in parent.children(recursive=True)
                if child.pid not in before
            }
            if names & expected:
                return names
            await asyncio.sleep(0.01)


def test_conpty_backend_ignores_parent_winpty_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        monkeypatch.setenv("PYWINPTY_BACKEND", "1")
        parent = psutil.Process()
        before = {child.pid for child in parent.children(recursive=True)}
        handle = await process_module._spawn_conpty(
            (sys.executable, "-u", "-i"),
            cwd=Path.cwd(),
            env=_child_environment(),
        )
        try:
            spawned_names = await _wait_for_backend_process(parent, before)
            assert "openconsole.exe" in spawned_names
            assert "winpty-agent.exe" not in spawned_names
        finally:
            await _close_conpty(handle)

    asyncio.run(asyncio.wait_for(run(), timeout=20))
