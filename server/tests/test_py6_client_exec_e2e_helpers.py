from __future__ import annotations

import asyncio
import os
import shutil
import signal
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
from test_device_client_e2e import (
    _client_creationflags,
    _client_environment,
    _start_client,
    _stop_client,
)
from test_py6_client_exec_e2e import _platform_commands, _resolve_windows_shell


@pytest.fixture(autouse=True)
def _truncate_tables() -> Iterator[None]:
    """Override the integration-suite fixture for these database-free unit tests."""

    yield


def test_frozen_client_environment_drops_parent_pythonpath(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OO_CLIENT_BIN", r"C:\bundle\openoctopus-client.exe")
    monkeypatch.setenv("PYTHONPATH", r"C:\editable-source")

    environment = _client_environment("http://127.0.0.1:1234", "test-token")

    assert "PYTHONPATH" not in environment


def test_source_client_environment_keeps_bootstrap_pythonpath(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OO_CLIENT_BIN", raising=False)
    monkeypatch.setenv("PYTHONPATH", "existing-source")

    environment = _client_environment("http://127.0.0.1:1234", "test-token")

    assert environment["PYTHONPATH"].endswith(os.pathsep + "existing-source")


def test_client_environment_drops_websocket_proxy_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proxy_names = ("WS_PROXY", "WSS_PROXY", "ws_proxy", "wss_proxy")
    for name in proxy_names:
        monkeypatch.setenv(name, "http://127.0.0.1:7897")

    environment = _client_environment("http://127.0.0.1:1234", "test-token")

    assert not proxy_names & environment.keys()


def test_client_creationflags_match_platform() -> None:
    expected = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0

    assert _client_creationflags() == expected


async def test_frozen_client_start_uses_platform_group_and_clean_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    sentinel = cast(asyncio.subprocess.Process, object())

    async def fake_create_subprocess_exec(
        *args: object, **kwargs: object
    ) -> asyncio.subprocess.Process:
        calls.append((args, kwargs))
        return sentinel

    monkeypatch.setenv("OO_CLIENT_BIN", r"C:\bundle\openoctopus-client.exe")
    monkeypatch.setenv("PYTHONPATH", r"C:\editable-source")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    process = await _start_client("http://127.0.0.1:1234", "test-token")

    assert process is sentinel
    assert calls[0][1]["creationflags"] == _client_creationflags()
    environment = cast(dict[str, str], calls[0][1]["env"])
    assert "PYTHONPATH" not in environment


class _FakeClientProcess:
    def __init__(self) -> None:
        self.returncode: int | None = None
        self.signal_sent: int | None = None

    def send_signal(self, value: int) -> None:
        self.signal_sent = value

    async def wait(self) -> int:
        self.returncode = 0
        return 0

    def kill(self) -> None:
        self.returncode = 1

    async def communicate(self) -> tuple[bytes, bytes]:
        return b"", b""


async def test_client_stop_uses_platform_graceful_signal() -> None:
    fake = _FakeClientProcess()

    await _stop_client(cast(Any, fake), expected_returncode=0)

    expected = getattr(signal, "CTRL_BREAK_EVENT") if os.name == "nt" else signal.SIGTERM
    assert fake.signal_sent == expected


async def test_client_stop_gracefully_stops_real_process_group() -> None:
    script = (
        "import signal, time; "
        "stop = lambda *_: (_ for _ in ()).throw(SystemExit(0)); "
        "signal.signal(getattr(signal, 'SIGBREAK', signal.SIGTERM), stop); "
        "print('READY', flush=True); "
        "exec('while True:\\n pass')"
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-u",
        "-c",
        script,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        creationflags=_client_creationflags(),
    )
    assert process.stdout is not None
    try:
        ready = await asyncio.wait_for(process.stdout.readline(), timeout=5)
        assert ready.rstrip(b"\r\n") == b"READY"
        await _stop_client(process, expected_returncode=0)
    finally:
        if process.returncode is None:
            process.kill()
            await asyncio.wait_for(process.wait(), timeout=3)


def test_linux_commands_remain_byte_for_byte_equivalent() -> None:
    def fail_resolver(name: str) -> str | None:
        raise AssertionError(f"unexpected Windows resolver call: {name}")

    commands = _platform_commands(
        platform_name="posix",
        python_executable="ignored",
        shell_resolver=fail_resolver,
    )

    assert commands.shell is None
    assert commands.agent == (
        "printf 'py6-agent-sentinel\\n' > agent-sentinel.txt; "
        "printf 'py6-agent-output\\n'"
    )
    assert commands.pipe == "printf 'pipe-e2e\\n'"
    assert commands.background == "printf 'before-reconnect\\n'; sleep 30"
    assert commands.tty == (
        "printf 'READY> '; read -r value; printf 'tty-echo:%s\\n' \"$value\""
    )
    assert commands.tty_input == "hello-e2e\n"


def test_windows_commands_select_shell_in_required_order() -> None:
    calls: list[str] = []

    def resolver(name: str) -> str | None:
        calls.append(name)
        return r"C:\Windows\System32\cmd.exe" if name == "cmd" else None

    commands = _platform_commands(
        platform_name="nt",
        python_executable=r"C:\Python 3.12\python.exe",
        shell_resolver=resolver,
    )

    assert calls == ["pwsh", "powershell", "powershell_x86", "cmd"]
    assert commands.shell == "cmd"
    assert commands.pipe == subprocess.list2cmdline(
        [
            r"C:\Python 3.12\python.exe",
            "-u",
            "-c",
            (
                "import sys; sys.stdout.buffer.write(b'pipe-e2e\\n'); "
                "sys.stdout.buffer.flush()"
            ),
        ]
    )
    assert commands.tty_input == "hello-e2e\r\n"


def test_windows_powershell_command_quotes_python_path_and_code() -> None:
    def resolver(name: str) -> str | None:
        return r"C:\Program Files\PowerShell\7\pwsh.exe" if name == "pwsh" else None

    commands = _platform_commands(
        platform_name="nt",
        python_executable=r"C:\Python's Tools\python.exe",
        shell_resolver=resolver,
    )

    assert commands.shell == "pwsh"
    assert commands.pipe == (
        "& 'C:\\Python''s Tools\\python.exe' -u -c "
        "'import sys; sys.stdout.buffer.write(b''pipe-e2e\\n''); "
        "sys.stdout.buffer.flush()'"
    )


def _command_argv(shell: str | None, command: str) -> list[str]:
    if shell is None:
        bash = shutil.which("bash")
        assert bash is not None
        return [bash, "--noprofile", "--norc", "-c", command]
    executable = _resolve_windows_shell(shell)
    assert executable is not None
    if shell in {"pwsh", "powershell", "powershell_x86"}:
        return [
            executable,
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            command,
        ]
    return [executable, "/D", "/S", "/C", command]


def test_platform_pipe_agent_and_line_input_commands_execute(tmp_path: Path) -> None:
    commands = _platform_commands()

    pipe = subprocess.run(
        _command_argv(commands.shell, commands.pipe),
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert pipe.returncode == 0
    assert pipe.stdout == "pipe-e2e\n"
    assert pipe.stderr == ""

    agent = subprocess.run(
        _command_argv(commands.shell, commands.agent),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert agent.returncode == 0
    assert agent.stdout == "py6-agent-output\n"
    assert agent.stderr == ""
    assert (tmp_path / "agent-sentinel.txt").read_text(encoding="utf-8") == (
        "py6-agent-sentinel\n"
    )

    tty = subprocess.run(
        _command_argv(commands.shell, commands.tty),
        input=commands.tty_input,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert tty.returncode == 0
    assert "READY> tty-echo:hello-e2e" in tty.stdout
    assert tty.stderr == ""
