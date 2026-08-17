from __future__ import annotations

import asyncio
import os

import pytest

from openoctopus_client.process import (
    ShellUnavailableError,
    build_argv,
    build_child_env,
    discover_shells,
    spawn_pipe,
    spawn_pty,
)
from openoctopus_client.terminal import TerminalNormalizer


def test_child_environment_is_allowlisted_and_removes_client_secrets() -> None:
    environment = {
        "PATH": "/bin",
        "HOME": "/home/test",
        "OPENOCTOPUS_DEVICE_TOKEN": "secret",
        "NOT_ALLOWED": "no",
    }
    result = build_child_env(environment, ["PATH", "HOME", "OPENOCTOPUS_DEVICE_TOKEN"])
    assert result == {
        "PATH": "/bin",
        "HOME": "/home/test",
        "TERM": "dumb",
        "NO_COLOR": "1",
        "PAGER": "cat",
        "GIT_PAGER": "cat",
        "GH_PAGER": "cat",
    }


@pytest.mark.parametrize(
    ("shell", "login", "expected"),
    [
        ("bash", False, ["bash", "--noprofile", "--norc", "-c", "echo ok"]),
        ("bash", True, ["bash", "-l", "-c", "echo ok"]),
        ("zsh", False, ["zsh", "-f", "-c", "echo ok"]),
        ("sh", False, ["sh", "-c", "echo ok"]),
    ],
)
def test_posix_shell_argv(shell: str, login: bool, expected: list[str]) -> None:
    if shell == "zsh" and not os.path.exists("/bin/zsh"):
        pytest.skip("zsh is not installed on this test host")
    actual = build_argv(shell, "echo ok", login=login, tty=False)
    assert actual[1:] == expected[1:]
    assert actual[0].endswith(expected[0])


def test_login_is_rejected_for_sh() -> None:
    with pytest.raises(ShellUnavailableError, match="login"):
        build_argv("sh", "echo ok", login=True, tty=False)


def test_discovery_has_a_valid_default() -> None:
    shells = discover_shells()
    assert shells.default in shells.available


def test_pipe_runs_with_closed_stdin_and_drains_both_streams() -> None:
    if os.name == "nt":
        pytest.skip("POSIX command used for this source test")

    async def run() -> tuple[bytes, bytes, int | None]:
        handle = await spawn_pipe(
            build_argv("sh", "printf out; printf err >&2", login=False, tty=False),
            cwd=None,
            env=build_child_env(os.environ, ["PATH"]),
        )
        stdout, stderr = await asyncio.gather(handle.stdout.read(), handle.stderr.read())
        result = await handle.wait()
        return stdout, stderr, result.returncode

    stdout, stderr, returncode = asyncio.run(run())
    assert stdout == b"out"
    assert stderr == b"err"
    assert returncode is not None


def test_terminal_normalizer_handles_chunks_and_line_controls() -> None:
    normalizer = TerminalNormalizer()
    assert normalizer.feed(b"50%\r") == "50%\n"
    assert normalizer.feed(b"100%\n") == "100%\n"
    assert normalizer.feed(b"ab\b\n") == "a\n"


def test_terminal_normalizer_strips_ansi_and_answers_dsr() -> None:
    normalizer = TerminalNormalizer()
    assert normalizer.feed(b"hello\x1b[31m world\x1b[0m\x1b[5n\x1b[6n\x1b[?6n\n") == "hello world\n"
    assert normalizer.responses == [b"\x1b[0n", b"\x1b[1;1R", b"\x1b[?1;1R"]


def test_terminal_normalizer_handles_escape_split_across_chunks() -> None:
    normalizer = TerminalNormalizer()
    assert normalizer.feed(b"x\x1b[") == "x"
    assert normalizer.feed(b"31my\n") == "y\n"
    assert normalizer.control_truncated is False


def test_posix_pty_helper_runs_a_command() -> None:
    if os.name == "nt":
        pytest.skip("POSIX PTY helper")

    async def run() -> tuple[bytes, int | None]:
        handle = await spawn_pty(
            build_argv("sh", "printf 'pty-ok\\n'", login=False, tty=True),
            cwd=None,
            env=build_child_env(os.environ, ["PATH"]),
        )
        output = await handle.stdout.read()
        result = await handle.wait()
        return output, result.returncode

    output, returncode = asyncio.run(run())
    assert output == b"pty-ok\n"
    assert returncode == 0


def test_pipe_interrupts_process_group() -> None:
    if os.name == "nt":
        pytest.skip("POSIX process group test")

    async def run() -> tuple[bool, int | None]:
        handle = await spawn_pipe(
            build_argv(
                "sh", "trap 'exit 0' INT; while :; do sleep 1; done", login=False, tty=False
            ),
            cwd=None,
            env=build_child_env(os.environ, ["PATH"]),
        )
        interrupted = await handle.interrupt()
        result = await asyncio.wait_for(handle.wait(), 3)
        return interrupted, result.returncode

    interrupted, returncode = asyncio.run(run())
    assert interrupted
    assert returncode is not None
