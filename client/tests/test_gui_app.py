from __future__ import annotations

from pathlib import Path

import pytest
from PySide6.QtWidgets import QApplication
from test_gui_credentials import RecordingBackend

from openoctopus_client.core_channel import CoreState, StartupConfigMessage, StatusEventMessage
from openoctopus_client.gui.app import TrayController, TrayState
from openoctopus_client.gui.autostart import AutostartController
from openoctopus_client.gui.background import DirectRunner
from openoctopus_client.gui.core_process import CoreController, CoreOutcome
from openoctopus_client.gui.credentials import CredentialStore
from openoctopus_client.gui.i18n import _EN, Translator
from openoctopus_client.gui.settings_store import ClientSettings, SettingsStore
from openoctopus_client.gui.single_instance import SingleInstance


class StubCore(CoreController):
    def __init__(self) -> None:
        super().__init__()
        self.started: list[StartupConfigMessage] = []
        self.stop_calls = 0
        self._is_running = False

    @property
    def running(self) -> bool:
        return self._is_running

    @property
    def stopping(self) -> bool:
        return False

    @property
    def stderr_diagnostics(self) -> str:
        return ""

    def start(self, startup: StartupConfigMessage) -> None:
        self.started.append(startup)
        self._is_running = True

    def stop(self) -> None:
        if not self._is_running:
            return
        self.stop_calls += 1
        self._is_running = False
        self.core_finished.emit(
            CoreOutcome(
                return_code=0,
                reason="stopped",
                cleanup_complete=True,
                crashed=False,
                exit_reported=True,
            )
        )

    def release_ownership(self) -> None:
        self.stop()


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.store = SettingsStore(tmp_path / "config-dir")
        self.backend = RecordingBackend()
        self.core = StubCore()
        self.controller = TrayController(
            QApplication.instance(),  # type: ignore[arg-type]
            settings_store=self.store,
            credentials=CredentialStore(backend=self.backend),
            runner=DirectRunner(),
            autostart=AutostartController(
                launch_command=["/bin/false"],
                autostart_directory=tmp_path / "autostart",
            ),
            single_instance=SingleInstance(tmp_path / "instance"),
            translator=Translator(dict(_EN)),
            core=self.core,
        )


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


def _save(controller: TrayController, address: str, token: str) -> None:
    controller._window.save_requested.emit(address, token)


def test_first_run_shows_settings_and_saves_configuration(
    qapp: QApplication, harness: Harness
) -> None:
    controller = harness.controller
    controller._load_configuration()
    assert controller.current_state() == TrayState.NOT_CONFIGURED
    assert not harness.store.path.exists()

    _save(controller, "https://openoctopus.example", "openoctopus_dev_first-run")

    saved = harness.store.load()
    assert saved is not None
    assert saved.server_url == "https://openoctopus.example"
    assert saved.token_account is not None
    assert saved.token_account.startswith("device-token-")
    assert len(harness.core.started) == 1
    assert harness.core.started[0].server_url == "https://openoctopus.example"
    assert harness.core.started[0].token == "openoctopus_dev_first-run"
    assert controller.current_state() == TrayState.CONNECTING
    assert controller._window.isHidden()


def test_changing_address_requires_a_new_token(
    qapp: QApplication, harness: Harness
) -> None:
    controller = harness.controller
    _save(controller, "https://a.example", "openoctopus_dev_alpha")
    assert controller.current_state() == TrayState.CONNECTING

    _save(controller, "https://b.example", "")

    assert controller.current_state() == TrayState.CONNECTING
    assert len(harness.core.started) == 1
    saved = harness.store.load()
    assert saved is not None
    assert saved.server_url == "https://a.example"


def test_blank_token_keeps_the_saved_credential(
    qapp: QApplication, harness: Harness
) -> None:
    controller = harness.controller
    _save(controller, "https://a.example", "openoctopus_dev_alpha")
    saved = harness.store.load()
    assert saved is not None
    account = saved.token_account

    controller._core.stop()  # user stops first
    assert controller.current_state() == TrayState.STOPPED
    _save(controller, "https://a.example", "")

    assert len(harness.core.started) == 2
    assert harness.core.started[1].token == "openoctopus_dev_alpha"
    committed = harness.store.load()
    assert committed is not None
    assert committed.token_account == account


def test_credential_store_failure_leaves_configuration_unchanged(
    qapp: QApplication, harness: Harness
) -> None:
    controller = harness.controller
    _save(controller, "https://a.example", "openoctopus_dev_alpha")
    saved = harness.store.load()
    assert saved is not None
    account = saved.token_account

    harness.backend.fail_set = True
    _save(controller, "https://b.example", "openoctopus_dev_beta")

    unchanged = harness.store.load()
    assert unchanged is not None
    assert unchanged.server_url == "https://a.example"
    assert unchanged.token_account == account
    # The failed save must not leave a committed entry behind.
    assert list(harness.backend.entries) == [("OpenOctopus Client", account)]


def test_invalid_address_is_rejected_without_touching_state(
    qapp: QApplication, harness: Harness
) -> None:
    controller = harness.controller
    _save(controller, "http://host/api", "openoctopus_dev_nope")
    assert not harness.store.path.exists()
    assert harness.core.started == []


def test_permanent_auth_failure_shows_attention(
    qapp: QApplication, harness: Harness
) -> None:
    controller = harness.controller
    _save(controller, "https://a.example", "openoctopus_dev_alpha")
    controller._core.core_finished.emit(
        CoreOutcome(
            return_code=1,
            reason="auth_rejected",
            cleanup_complete=True,
            crashed=False,
            exit_reported=True,
        )
    )
    assert controller.current_state() == TrayState.ATTENTION


def test_online_event_shows_the_server_device_name(
    qapp: QApplication, harness: Harness
) -> None:
    controller = harness.controller
    controller._core.status_received.emit(
        StatusEventMessage(
            type="status",
            generation=0,
            state=CoreState.ONLINE,
            device_name="Studio PC",
        )
    )
    assert controller.current_state() == TrayState.ONLINE
    assert "Studio PC" in controller._status_text()


def test_missing_saved_credential_needs_attention(
    qapp: QApplication, harness: Harness
) -> None:
    controller = harness.controller
    harness.store.commit(
        ClientSettings(server_url="https://a.example", token_account="acc-404")
    )
    controller._load_configuration()
    assert controller.current_state() == TrayState.ATTENTION
    assert not controller._core.running
