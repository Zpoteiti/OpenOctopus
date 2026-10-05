"""Subprocess tests of the private core pipe entry (``_core-run``)."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

CLIENT_ROOT = Path(__file__).parents[1]
_TIMEOUT_SECONDS = 30


class CoreHarness:
    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        self.process = process

    def send(self, payload: dict[str, Any]) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
        self.process.stdin.flush()

    def send_raw(self, line: bytes) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(line)
        self.process.stdin.flush()

    def close_stdin(self) -> None:
        if self.process.stdin is not None:
            self.process.stdin.close()

    def read_event(self) -> dict[str, Any]:
        assert self.process.stdout is not None
        deadline = time.monotonic() + _TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            line = self.process.stdout.readline()
            if not line:
                raise AssertionError("core closed stdout without an event")
            text = line.decode("utf-8").strip()
            if not text:
                continue
            payload = json.loads(text)
            assert isinstance(payload, dict)
            return payload
        raise AssertionError("timed out waiting for a core event")

    def wait(self) -> tuple[int, str, str]:
        stdout_lines: list[str] = []
        assert self.process.stdout is not None and self.process.stderr is not None
        return_code = self.process.wait(timeout=_TIMEOUT_SECONDS)
        stdout = self.process.stdout.read().decode("utf-8", errors="replace")
        stderr = self.process.stderr.read().decode("utf-8", errors="replace")
        return return_code, stdout + "\n".join(stdout_lines), stderr


def _start_core() -> CoreHarness:
    environment = {
        **os.environ,
        "PYTHONPATH": str(CLIENT_ROOT / "src"),
        "OPENOCTOPUS_DEVICE_TOKEN": "openoctopus_dev_should-not-be-read",
    }
    process = subprocess.Popen(
        [sys.executable, "-m", "openoctopus_client", "_core-run"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
    )
    return CoreHarness(process)


def _terminate(harness: CoreHarness) -> None:
    if harness.process.poll() is None:
        harness.process.send_signal(signal.SIGTERM)
        try:
            harness.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            harness.process.kill()
            harness.process.wait(timeout=5)


def test_core_never_reads_the_device_token_from_environment() -> None:
    harness = _start_core()
    try:
        harness.close_stdin()
        event = harness.read_event()
        return_code, _stdout, stderr = harness.wait()
    finally:
        _terminate(harness)
    # The inherited environment carries a token but no startup config arrives:
    # the core must reject startup instead of using the environment value.
    assert event == {
        "type": "exit",
        "generation": 0,
        "return_code": 78,
        "cleanup_complete": True,
        "reason": "startup_config_invalid",
    }
    assert return_code == 78
    assert "should-not-be-read" not in stderr


@pytest.mark.parametrize(
    "line",
    [
        b"not json at all\n",
        b'{"type":"stop"}\n',
        b'{"type":"startup-config","generation":1,"server_url":"http://host/api",'
        b'"token":"openoctopus_dev_secret-echo-check"}\n',
        b'{"type":"startup-config","generation":1,"server_url":"http://127.0.0.1:1",'
        b'"token":"wrong-prefix"}\n',
    ],
)
def test_invalid_startup_config_exits_78_without_echoing_the_token(line: bytes) -> None:
    harness = _start_core()
    try:
        harness.send_raw(line)
        event = harness.read_event()
        return_code, _stdout, stderr = harness.wait()
    finally:
        _terminate(harness)
    assert event["type"] == "exit"
    assert event["reason"] == "startup_config_invalid"
    assert event["return_code"] == 78
    assert return_code == 78
    assert "secret-echo-check" not in stderr
    assert "secret-echo-check" not in json.dumps(event)


def test_core_retries_unreachable_server_then_stops_cleanly() -> None:
    harness = _start_core()
    observed: list[dict[str, Any]] = []
    try:
        harness.send(
            {
                "type": "startup-config",
                "generation": 3,
                "server_url": "http://127.0.0.1:1",
                "token": "openoctopus_dev_stop-flow-secret",
            }
        )
        observed.append(harness.read_event())
        observed.append(harness.read_event())
        assert observed[0] == {
            "type": "status",
            "generation": 3,
            "state": "connecting",
            "attempt": 1,
        }
        assert observed[1]["type"] == "status"
        assert observed[1]["state"] == "reconnecting"
        assert observed[1]["error_code"] == "server_unreachable"
        harness.send({"type": "stop"})
        while True:
            event = harness.read_event()
            observed.append(event)
            if event["type"] == "exit":
                break
        return_code, _stdout, stderr = harness.wait()
    finally:
        _terminate(harness)
    exit_event = observed[-1]
    assert exit_event["type"] == "exit"
    assert exit_event["generation"] == 3
    assert exit_event["reason"] == "stopped"
    assert exit_event["cleanup_complete"] is True
    assert return_code == 0
    for event in observed:
        assert "stop-flow-secret" not in json.dumps(event)


def test_stdin_close_ends_ownership_and_stops_the_core() -> None:
    harness = _start_core()
    try:
        harness.send(
            {
                "type": "startup-config",
                "generation": 5,
                "server_url": "http://127.0.0.1:1",
                "token": "openoctopus_dev_owner-gone-secret",
            }
        )
        harness.read_event()
        harness.close_stdin()
        while True:
            event = harness.read_event()
            if event["type"] == "exit":
                break
        return_code, _stdout, stderr = harness.wait()
    finally:
        _terminate(harness)
    assert return_code == 0
    assert event["reason"] == "owner_gone"


def test_unknown_user_command_is_rejected_and_gui_is_the_default() -> None:
    environment = {**os.environ, "PYTHONPATH": str(CLIENT_ROOT / "src")}
    result = subprocess.run(
        [sys.executable, "-m", "openoctopus_client", "run"],
        capture_output=True,
        text=True,
        env=environment,
        timeout=30,
    )
    assert result.returncode == 2
    assert "internal modes" in result.stderr
    # The removed user CLI must no longer accept ``run``; only the tray GUI
    # (no arguments) and underscore internal modes exist.
    assert "OPENOCTOPUS_SERVER_URL" not in result.stderr


def test_version_internal_mode_is_stable() -> None:
    environment = {**os.environ, "PYTHONPATH": str(CLIENT_ROOT / "src")}
    result = subprocess.run(
        [sys.executable, "-m", "openoctopus_client", "_version"],
        capture_output=True,
        text=True,
        env=environment,
        timeout=30,
    )
    assert result.returncode == 0
    assert result.stdout == "0.0.1\n"
