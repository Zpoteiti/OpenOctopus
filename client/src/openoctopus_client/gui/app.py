"""The resident tray application: menu, states, settings flow, and lifecycle.

This module wires the tray menu, the settings window, the credential store,
the single-instance channel, autostart, and the private core process.  It
never imports MCP, document conversion, or exec machinery directly; all core
behaviour arrives over the private pipe.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, replace
from enum import Enum
from typing import cast

from PySide6.QtCore import QObject, QRect, Qt, QTimer, QUrl
from PySide6.QtGui import QColor, QDesktopServices, QIcon, QPainter, QPixmap
from PySide6.QtWidgets import QApplication, QMenu, QSystemTrayIcon

from openoctopus_client.config import (
    ConfigurationError,
    configuration_from_startup,
    validate_server_url,
)
from openoctopus_client.core_channel import (
    CoreState,
    StartupConfigMessage,
    StatusEventMessage,
)
from openoctopus_client.gui.autostart import AutostartController, AutostartError
from openoctopus_client.gui.background import Runner
from openoctopus_client.gui.core_process import CoreController, CoreOutcome
from openoctopus_client.gui.credentials import (
    CredentialStore,
    install_platform_backend,
    new_token_account,
)
from openoctopus_client.gui.i18n import Translator, detect_catalog
from openoctopus_client.gui.settings_store import (
    ClientSettings,
    SettingsError,
    SettingsStore,
    default_settings_directory,
)
from openoctopus_client.gui.settings_window import SettingsWindow
from openoctopus_client.gui.single_instance import SingleInstance


class TrayState(Enum):
    NOT_CONFIGURED = "none"
    CONNECTING = "connecting"
    ONLINE = "online"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"
    ATTENTION = "attention"


@dataclass(frozen=True)
class _PendingCommit:
    server_url: str
    token: str
    account: str
    old_account: str | None
    staged_new_account: bool


_STATE_ICON_COLORS = {
    TrayState.NOT_CONFIGURED: QColor(128, 128, 128),
    TrayState.CONNECTING: QColor(235, 179, 8),
    TrayState.ONLINE: QColor(22, 163, 74),
    TrayState.RECONNECTING: QColor(235, 179, 8),
    TrayState.STOPPED: QColor(128, 128, 128),
    TrayState.ATTENTION: QColor(220, 38, 38),
}

_EXIT_REASON_MESSAGES = {
    "auth_rejected": "error.auth_rejected",
    "connection_replaced": "error.connection_replaced",
    "config_rejected": "error.config_rejected",
    "startup_config_invalid": "error.startup_config_invalid",
    "runtime_failed": "error.core_crash",
}


def state_icon(state: TrayState) -> QIcon:
    pixmap = QPixmap(32, 32)
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    painter.setBrush(_STATE_ICON_COLORS[state])
    painter.setPen(Qt.PenStyle.NoPen)
    painter.drawEllipse(QRect(4, 4, 24, 24))
    painter.setPen(QColor(255, 255, 255))
    painter.drawText(QRect(4, 4, 24, 24), Qt.AlignmentFlag.AlignCenter, "O")
    painter.end()
    return QIcon(pixmap)


class TrayController(QObject):
    def __init__(
        self,
        application: QApplication,
        *,
        settings_store: SettingsStore,
        credentials: CredentialStore,
        runner: Runner,
        autostart: AutostartController,
        single_instance: SingleInstance,
        translator: Translator | None = None,
        core: CoreController | None = None,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._application = application
        self._store = settings_store
        self._credentials = credentials
        self._runner = runner
        self._autostart = autostart
        self._single = single_instance
        self._single.activate_requested.connect(self._on_activate_requested)
        self._translator = translator if translator is not None else Translator(detect_catalog())
        self._state = TrayState.NOT_CONFIGURED
        self._settings: ClientSettings | None = None
        self._has_saved_token = False
        self._device_name: str | None = None
        self._attempt: int | None = None
        self._last_error_key: str | None = None
        self._pending: _PendingCommit | None = None
        self._quitting = False
        self._busy = False
        self._operation_cancelled = False
        self._cleanup_failed = False
        self._tray: QSystemTrayIcon | None = None
        self._tray_watch_timer: QTimer | None = None

        self._core = core if core is not None else CoreController(self)
        self._core.status_received.connect(self._on_core_status)
        self._core.core_finished.connect(self._on_core_finished)

        self._window = SettingsWindow(self._translator)
        self._window.save_requested.connect(self._on_save_requested)
        self._window.retry_requested.connect(self._retry_tray)
        self._window.close_accepted.connect(self._quit)

        self._menu = QMenu()
        self._status_action = self._menu.addAction("")
        self._status_action.setEnabled(False)
        self._menu.addSeparator()
        self._open_web_action = self._menu.addAction(
            self._translator.tr("menu.open_web"), lambda: self._open_web()
        )
        self._settings_action = self._menu.addAction(
            self._translator.tr("menu.settings"), lambda: self._show_settings()
        )
        self._stop_action = self._menu.addAction(
            self._translator.tr("menu.stop"), lambda: self._stop_core()
        )
        self._start_action = self._menu.addAction(
            self._translator.tr("menu.start"), lambda: self._start_requested()
        )
        self._autostart_action = self._menu.addAction(
            self._translator.tr("menu.autostart"), lambda: None
        )
        self._autostart_action.setCheckable(True)
        self._autostart_action.toggled.connect(self._on_autostart_toggled)
        self._menu.addSeparator()
        self._quit_action = self._menu.addAction(
            self._translator.tr("menu.quit"), lambda: self._quit()
        )
        self._sync_autostart_checked()

    # -- startup ----------------------------------------------------------------

    def startup(self) -> None:
        if not QSystemTrayIcon.isSystemTrayAvailable():
            self._window.set_close_means_quit(True)
            self._window.show_message(self._translator.tr("settings.no_tray"))
            self._window.show_retry_entry()
            self._window.show()
            self._window.raise_()
            self._refresh_chrome()
            return
        self._window.set_close_means_quit(False)
        self._install_tray()
        self._load_configuration()
        self._start_tray_support_watch()

    def reveal_settings_for_missing_tray(self) -> None:
        """Tray support vanished at runtime: stop connecting and show the window."""

        if QSystemTrayIcon.isSystemTrayAvailable():
            return
        self._uninstall_tray()
        self._window.set_close_means_quit(True)
        self._stop_core()
        self._window.show_message(self._translator.tr("settings.no_tray"))
        self._window.show_retry_entry()
        self._show_settings()

    def _start_tray_support_watch(self) -> None:
        if self._tray_watch_timer is not None:
            return

        def tick() -> None:
            if self._quitting:
                return
            self.reveal_settings_for_missing_tray()
            if self._tray_watch_timer is not None:
                self._tray_watch_timer.start()

        self._tray_watch_timer = QTimer(self)
        self._tray_watch_timer.setSingleShot(True)
        self._tray_watch_timer.timeout.connect(tick)
        self._tray_watch_timer.start(30_000)

    def _install_tray(self) -> None:
        if self._tray is not None:
            return
        self._tray = QSystemTrayIcon(state_icon(self._state), self._application)
        self._tray.setContextMenu(self._menu)
        self._tray.show()
        self._refresh_chrome()

    def _uninstall_tray(self) -> None:
        if self._tray is not None:
            self._tray.hide()
            self._tray = None

    def _retry_tray(self) -> None:
        if self._busy or self._core.running or self._cleanup_failed or self._quitting:
            return
        if QSystemTrayIcon.isSystemTrayAvailable():
            self._window.show_retry_entry(False)
            self._window.clear_message()
            self._window.set_close_means_quit(False)
            self._install_tray()
            self._window.hide()
            self._start_tray_support_watch()
            self._load_configuration()
        else:
            self._window.show_message(self._translator.tr("settings.no_tray"))

    def _load_configuration(self) -> None:
        try:
            self._settings = self._store.load()
        except SettingsError:
            self._set_state(TrayState.ATTENTION)
            self._window.show_message(
                self._translator.tr("error.save_failed", detail="settings file")
            )
            self._show_settings()
            return
        if self._settings is None:
            self._has_saved_token = False
            self._window.set_has_saved_token(False)
            self._set_state(TrayState.NOT_CONFIGURED)
            self._show_settings()
            return
        self._window.set_server_url(self._settings.server_url)
        self._refresh_chrome()
        account = self._settings.token_account
        if account is None:
            self._attention_missing_credentials()
            return
        self._start_requested()

    def _attention_missing_credentials(self) -> None:
        self._has_saved_token = False
        self._last_error_key = "error.credential_store_unavailable"
        self._set_state(TrayState.ATTENTION)
        self._window.show_message(self._translator.tr("error.credential_store_unavailable"))
        self._show_settings()

    # -- menu/window helpers ------------------------------------------------------

    def _show_settings(self) -> None:
        self._window.show()
        self._window.raise_()
        self._window.activateWindow()

    def _on_activate_requested(self) -> None:
        """The second launch woke us: surface the settings window."""

        self._window.show()
        self._window.raise_()
        self._window.activateWindow()

    def _open_web(self) -> None:
        if self._settings is None or not self._settings.server_url:
            self._show_settings()
            return
        QDesktopServices.openUrl(QUrl(self._settings.server_url))

    def _set_state(self, state: TrayState) -> None:
        self._state = state
        self._refresh_chrome()

    def current_state(self) -> TrayState:
        """Public accessor for the tray lifecycle state."""

        return self._state

    def _status_text(self) -> str:
        key = f"menu.status.{self._state.value}"
        if self._state is TrayState.ONLINE and self._device_name:
            return self._translator.tr(key, device=self._device_name)
        if self._state is TrayState.RECONNECTING:
            return self._translator.tr(key, attempt=self._attempt if self._attempt else "?")
        return self._translator.tr(key)

    def _refresh_chrome(self) -> None:
        self._status_action.setText(self._status_text())
        if self._tray is not None:
            self._tray.setIcon(state_icon(self._state))
            self._tray.setToolTip(self._status_text())
        available = QSystemTrayIcon.isSystemTrayAvailable()
        self._window.set_busy(
            self._busy or self._cleanup_failed or self._core.stopping
            or self._quitting or not available
        )
        self._stop_action.setEnabled(
            (self._core.running or self._busy) and not self._core.stopping
            and not self._quitting
        )
        self._start_action.setEnabled(
            available and not self._busy and not self._cleanup_failed and not self._quitting
            and not self._core.running
            and not self._core.stopping
            and self._settings is not None
            and self._settings.token_account is not None
        )
        if self._core.stopping:
            self._stop_action.setText(self._translator.tr("menu.stopping"))
        else:
            self._stop_action.setText(self._translator.tr("menu.stop"))

    def _sync_autostart_checked(self) -> None:
        try:
            enabled = self._autostart.is_enabled()
        except Exception:  # noqa: BLE001
            enabled = False
        self._autostart_action.blockSignals(True)
        self._autostart_action.setChecked(enabled)
        self._autostart_action.blockSignals(False)

    def _on_autostart_toggled(self, checked: bool) -> None:
        try:
            if checked:
                self._autostart.enable()
            else:
                self._autostart.disable()
        except AutostartError as exc:
            self._window.show_message(str(exc))
        finally:
            self._sync_autostart_checked()

    # -- core control --------------------------------------------------------------

    def _can_connect(self) -> bool:
        return (
            not self._quitting and not self._cleanup_failed
            and QSystemTrayIcon.isSystemTrayAvailable()
        )

    def _begin_operation(self) -> None:
        self._busy = True
        self._operation_cancelled = False
        self._refresh_chrome()

    def _finish_operation(self) -> None:
        self._busy = False
        self._refresh_chrome()
        self._maybe_quit()

    def _start_requested(self) -> None:
        if self._busy or self._core.running or self._core.stopping or not self._can_connect():
            return
        settings = self._settings
        if settings is None or settings.token_account is None:
            self._show_settings()
            return
        account = settings.token_account
        self._begin_operation()
        self._runner.submit(
            lambda: self._credentials.load(account),
            lambda result, error: self._on_start_token(settings, result, error),
        )

    def _on_start_token(
        self, settings: ClientSettings, result: object, error: BaseException | None,
    ) -> None:
        if self._operation_cancelled or not self._can_connect():
            self._finish_operation()
            return
        if error is not None or not isinstance(result, str) or not result:
            self._finish_operation()
            self._attention_missing_credentials()
            return
        self._has_saved_token = True
        self._window.set_has_saved_token(True)
        self._start_core(settings.server_url, result)
        self._finish_operation()

    def _start_core(self, server_url: str, token: str) -> None:
        if not self._can_connect() or self._core.running or self._core.stopping:
            return
        self._window.clear_message()
        self._core.start(
            StartupConfigMessage(
                type="startup-config", generation=0, server_url=server_url, token=token,
            )
        )
        self._set_state(TrayState.CONNECTING)

    def _stop_core(self) -> None:
        self._operation_cancelled = True
        self._core.stop()
        self._refresh_chrome()

    def _on_core_status(self, event: StatusEventMessage) -> None:
        if event.generation != self._core.generation:
            return
        if event.state is CoreState.CONNECTING:
            self._set_state(TrayState.CONNECTING)
        elif event.state is CoreState.ONLINE:
            self._device_name = event.device_name
            self._last_error_key = None
            self._set_state(TrayState.ONLINE)
        elif event.state is CoreState.RECONNECTING:
            self._attempt = event.attempt
            self._set_state(TrayState.RECONNECTING)
        # Only the process exit and its cleanup result can confirm STOPPED.
        if event.state is CoreState.RECONNECTING and event.error_code is not None:
            # First-run and ordinary unreachable failures stay visible in the
            # settings window without stealing the tray state.
            self._window.show_message(
                self._translator.tr("details.server_unreachable"),
                details=self._core.stderr_diagnostics,
            )

    def _on_core_finished(self, outcome: CoreOutcome) -> None:
        pending = self._pending
        self._pending = None
        clean = outcome.exit_reported and outcome.cleanup_complete is True and not outcome.crashed
        # FailedToStart never created a process or any owned work.
        self._cleanup_failed = not clean and outcome.reason != "failed_to_start"
        if self._cleanup_failed:
            self._operation_cancelled = True
            self._quitting = False
            self._window.prepare_for_quit(False)
            self._last_error_key = "error.stop_failed"
        elif outcome.reason in _EXIT_REASON_MESSAGES:
            self._last_error_key = _EXIT_REASON_MESSAGES[outcome.reason]
        elif outcome.reason == "failed_to_start":
            self._last_error_key = "error.core_crash"
        else:
            self._last_error_key = None
        if self._last_error_key:
            self._set_state(TrayState.ATTENTION)
            self._window.show_message(
                self._translator.tr(self._last_error_key), details=self._core.stderr_diagnostics,
            )
        else:
            self._set_state(TrayState.STOPPED)
        if pending is not None:
            if clean and not self._operation_cancelled and self._can_connect():
                self._finish_commit(pending)
            else:
                self._rollback_pending(pending)
        if self._cleanup_failed:
            self._show_settings()
        self._maybe_quit()

    def _rollback_pending(self, pending: _PendingCommit) -> None:
        if pending.staged_new_account:
            self._runner.submit(
                lambda: self._credentials.delete(pending.account),
                lambda _result, _error: self._finish_operation(),
            )
        else:
            self._finish_operation()

    # -- settings save flow ----------------------------------------------------------

    def _on_save_requested(self, server_url_text: str, token_text: str) -> None:
        if self._busy or self._core.stopping or not self._can_connect():
            return
        try:
            canonical = validate_server_url(server_url_text)
            if token_text:
                configuration_from_startup(canonical, token_text)
        except ConfigurationError:
            self._window.show_message(self._translator.tr("error.startup_config_invalid"))
            return
        previous = self._settings
        if not token_text:
            if previous is None or previous.token_account is None:
                self._window.show_message(self._translator.tr("error.token_required"))
                return
            if previous.server_url != canonical:
                self._window.show_message(
                    self._translator.tr("error.token_required_for_new_server")
                )
                return
            account = previous.token_account
        else:
            account = new_token_account()
        pending = _PendingCommit(
            server_url=canonical, token=token_text, account=account,
            old_account=previous.token_account if previous is not None and token_text else None,
            staged_new_account=bool(token_text),
        )
        self._begin_operation()

        def credential_operation() -> str | None:
            if token_text:
                self._credentials.save(account, token_text)
                return token_text
            return self._credentials.load(account)

        self._runner.submit(
            credential_operation,
            lambda result, error: self._on_save_credential(pending, result, error),
        )

    def _on_save_credential(
        self, pending: _PendingCommit, result: object, error: BaseException | None,
    ) -> None:
        if self._operation_cancelled or not self._can_connect():
            self._rollback_pending(pending)
            return
        if error is not None or not isinstance(result, str) or not result:
            self._window.show_message(self._translator.tr("error.credential_store_unavailable"))
            self._rollback_pending(pending)
            return
        pending = replace(pending, token=result)
        if self._core.running:
            self._pending = pending
            self._core.stop()
            self._refresh_chrome()
        else:
            self._finish_commit(pending)

    def _finish_commit(self, pending: _PendingCommit) -> None:
        try:
            self._store.commit(ClientSettings(pending.server_url, pending.account))
        except SettingsError:
            self._window.show_message(
                self._translator.tr("error.save_failed", detail="settings file")
            )
            self._rollback_pending(pending)
            return
        self._settings = ClientSettings(pending.server_url, pending.account)
        self._has_saved_token = True
        if pending.old_account is not None:
            old_account = pending.old_account
            self._runner.submit(
                lambda: self._credentials.delete(old_account), lambda _result, _error: None,
            )
        self._window.set_has_saved_token(True)
        self._window.clear_message()
        self._window.hide()
        self._start_core(pending.server_url, pending.token)
        self._finish_operation()

    # -- quit -----------------------------------------------------------------------

    def quit_for_logout(self) -> None:
        if not self._quitting:
            self._quit()

    def _quit(self) -> None:
        if self._quitting:
            return
        self._quitting = True
        self._operation_cancelled = True
        self._window.prepare_for_quit()
        if self._core.running:
            self._core.release_ownership()
        self._maybe_quit()

    def _maybe_quit(self) -> None:
        if self._quitting and not self._core.running and not self._busy:
            self._application.quit()


def gui_main() -> int:
    existing = QApplication.instance()
    application = (
        cast(QApplication, existing) if existing is not None else QApplication(sys.argv)
    )
    application.setApplicationName("openoctopus-client")
    application.setApplicationDisplayName("OpenOctopus")
    application.setQuitOnLastWindowClosed(False)
    directory = default_settings_directory()
    single = SingleInstance(directory)
    try:
        is_primary = single.try_become_primary()
    except RuntimeError:
        return 3
    if not is_primary:
        # A second launch only wakes the running program: no credentials are
        # read, no Server connection is attempted, no device is replaced.
        return 0
    translator = Translator(detect_catalog())
    store = SettingsStore(directory)
    # Pin the platform credential backend before any credential access.
    install_platform_backend()
    controller = TrayController(
        application,
        settings_store=store,
        credentials=CredentialStore(),
        runner=_production_runner(),
        autostart=AutostartController(),
        single_instance=single,
        translator=translator,
    )
    application.aboutToQuit.connect(single.release)
    application.commitDataRequest.connect(lambda _event: controller.quit_for_logout())
    controller.startup()
    return application.exec()


def _production_runner() -> Runner:
    from openoctopus_client.gui.background import ThreadPoolRunner

    return ThreadPoolRunner()
