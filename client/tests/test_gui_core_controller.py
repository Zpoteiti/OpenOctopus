from __future__ import annotations

import sys
import textwrap
from collections.abc import Callable
from pathlib import Path

import pytest
from PySide6.QtCore import QEventLoop, QProcess, QTimer
from PySide6.QtWidgets import QApplication

from openoctopus_client.core_channel import ExitResultMessage, StartupConfigMessage
from openoctopus_client.gui import core_process
from openoctopus_client.gui.core_process import CoreController

_FAKE_CORE = textwrap.dedent(
    """
    import json, sys
    generation = 0
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        message = json.loads(raw)
        if message["type"] == "startup-config":
            generation = message["generation"]
            print("diagnostic line for stderr", file=sys.stderr, flush=True)
            print(json.dumps({"type": "status", "generation": generation,
                              "state": "connecting", "attempt": 1}), flush=True)
            print(json.dumps({"type": "status", "generation": generation,
                              "state": "online", "device_name": "Fake Device"}), flush=True)
        elif message["type"] == "stop":
            print(json.dumps({"type": "exit", "generation": generation, "return_code": 0,
                              "cleanup_complete": True, "reason": "stopped"}), flush=True)
            break
    else:
        print(json.dumps({"type": "exit", "generation": generation, "return_code": 0,
                          "cleanup_complete": True, "reason": "owner_gone"}), flush=True)
    """
)


def _drain(milliseconds: int) -> None:
    loop = QEventLoop()
    QTimer.singleShot(milliseconds, loop.quit)
    loop.exec()


def _drain_until(predicate: Callable[[], bool], timeout_ms: int = 5000) -> bool:
    waited = 0
    while waited < timeout_ms:
        if predicate():
            return True
        _drain(50)
        waited += 50
    return bool(predicate())


@pytest.fixture
def fake_core(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    script = tmp_path / "fake_core.py"
    script.write_text(_FAKE_CORE, encoding="utf-8")
    monkeypatch.setattr(
        core_process, "core_command", lambda: [sys.executable, str(script)]
    )
    return str(script)


def _startup() -> StartupConfigMessage:
    return StartupConfigMessage(
        type="startup-config",
        generation=0,
        server_url="http://127.0.0.1:1",
        token="openoctopus_dev_controller-secret",
    )


def test_controller_streams_events_and_stop_completes(
    qapp: QApplication, fake_core: str
) -> None:
    controller = CoreController()
    events: list[object] = []
    outcomes: list[object] = []
    controller.status_received.connect(events.append)
    controller.core_finished.connect(outcomes.append)

    controller.start(_startup())
    assert _drain_until(lambda: len(events) >= 2)
    assert controller.running is True
    assert controller.generation == 1
    assert "controller-secret" not in repr(events)

    controller.stop()
    assert _drain_until(lambda: bool(outcomes))
    outcome = outcomes[0]
    assert outcome.reason == "stopped"  # type: ignore[attr-defined]
    assert outcome.cleanup_complete is True  # type: ignore[attr-defined]
    assert outcome.crashed is False  # type: ignore[attr-defined]
    assert controller.running is False


def test_closing_stdin_reports_ownership_end(qapp: QApplication, fake_core: str) -> None:
    controller = CoreController()
    outcomes: list[object] = []
    controller.core_finished.connect(outcomes.append)
    controller.start(_startup())
    assert _drain_until(lambda: controller.running)
    controller.release_ownership()
    assert _drain_until(lambda: bool(outcomes))
    assert outcomes[0].reason == "owner_gone"  # type: ignore[attr-defined]


def test_stderr_is_kept_for_the_settings_window(
    qapp: QApplication, fake_core: str
) -> None:
    controller = CoreController()
    outcomes: list[object] = []
    controller.core_finished.connect(outcomes.append)
    controller.start(_startup())
    assert _drain_until(lambda: "diagnostic line" in controller.stderr_diagnostics)
    assert "controller-secret" not in controller.stderr_diagnostics
    controller.stop()
    assert _drain_until(lambda: bool(outcomes))


def test_second_start_is_rejected_while_running(qapp: QApplication, fake_core: str) -> None:
    controller = CoreController()
    controller.start(_startup())
    assert _drain_until(lambda: controller.running)
    with pytest.raises(RuntimeError):
        controller.start(_startup())
    controller.stop()
    assert _drain_until(lambda: not controller.running and not controller.stopping)


def test_old_exit_generation_cannot_confirm_new_core_cleanup(
    qapp: QApplication, fake_core: str,
) -> None:
    controller = CoreController()
    controller.start(_startup())
    controller._accept_event(ExitResultMessage(
        type="exit", generation=0, return_code=0, reason="stopped", cleanup_complete=True,
    ))
    assert controller._exit_result is None
    controller.stop()
    assert _drain_until(lambda: not controller.running)


def test_repeated_runs_release_process_objects(qapp: QApplication, fake_core: str) -> None:
    controller = CoreController()
    for _ in range(3):
        controller.start(_startup())
        controller.stop()
        assert _drain_until(lambda: not controller.running)
        _drain(10)
    assert controller.findChildren(QProcess) == []
