"""The small connection-settings window shared by all states."""

from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from openoctopus_client.gui.i18n import Translator

MAX_INPUT_LENGTH = 4096


class SettingsWindow(QWidget):
    """Server address / Device token form; closing only hides the window."""

    save_requested = Signal(str, str)  # (server_url, token_text; "" keeps saved)
    retry_requested = Signal()
    close_accepted = Signal()  # emitted only when closing really means quitting

    def __init__(self, translator: Translator, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._translator = translator
        self._quitting = False
        self._close_means_quit = False
        self.setWindowTitle(translator.tr("settings.title"))
        self._address = QLineEdit(self)
        self._address.setMaxLength(MAX_INPUT_LENGTH)
        self._address.setClearButtonEnabled(True)
        self._token = QLineEdit(self)
        self._token.setMaxLength(MAX_INPUT_LENGTH)
        self._token.setEchoMode(QLineEdit.EchoMode.Password)
        self._token.setClearButtonEnabled(True)
        self._status = QLabel(self)
        self._status.setWordWrap(True)
        self._status.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._status.setVisible(False)
        self._details = QPlainTextEdit(self)
        self._details.setReadOnly(True)
        self._details.setVisible(False)
        self._details.setMaximumHeight(140)
        self._save = QPushButton(translator.tr("settings.save"), self)
        self._save.setDefault(True)
        self._retry = QPushButton(translator.tr("settings.retry_tray"), self)
        self._retry.setVisible(False)
        self._hint = QLabel(translator.tr("settings.close_hint"), self)
        self._hint.setWordWrap(True)
        self._hint.setVisible(False)

        form = QFormLayout()
        form.addRow(translator.tr("settings.server"), self._address)
        form.addRow(translator.tr("settings.token"), self._token)
        buttons = QHBoxLayout()
        buttons.addWidget(self._save)
        buttons.addWidget(self._retry)
        buttons.addStretch(1)
        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(self._status)
        layout.addWidget(QLabel(translator.tr("details.label")))
        layout.addWidget(self._details)
        layout.addLayout(buttons)
        layout.addWidget(self._hint)

        self._save.clicked.connect(self._emit_save)
        self._retry.clicked.connect(self.retry_requested)

    def set_server_url(self, server_url: str | None) -> None:
        if server_url:
            self._address.setText(server_url)

    def set_has_saved_token(self, has_saved_token: bool) -> None:
        if has_saved_token:
            self._token.setPlaceholderText(self._translator.tr("settings.token_saved"))
            # A saved token is never refilled into the input box.
            self._token.setText("")
        else:
            self._token.setPlaceholderText(self._translator.tr("settings.token_placeholder"))

    def set_busy(self, busy: bool) -> None:
        self._save.setEnabled(not busy)
        self._address.setEnabled(not busy)
        self._token.setEnabled(not busy)
        if busy:
            self._save.setText(self._translator.tr("settings.saving"))
        else:
            self._save.setText(self._translator.tr("settings.save"))

    def show_message(self, text: str, *, details: str | None = None) -> None:
        self._status.setText(text)
        self._status.setVisible(True)
        if details:
            self._details.setPlainText(details)
            self._details.setVisible(True)

    def clear_message(self) -> None:
        self._status.clear()
        self._status.setVisible(False)
        self._details.clear()
        self._details.setVisible(False)

    def show_retry_entry(self) -> None:
        self._retry.setVisible(True)

    def show_close_hint(self) -> None:
        self._hint.setVisible(True)

    def prepare_for_quit(self) -> None:
        self._quitting = True

    def set_close_means_quit(self, close_means_quit: bool) -> None:
        # Without a tray, hiding the only window would leave an unoperable
        # process; closing it must quit instead.
        self._close_means_quit = close_means_quit

    def _emit_save(self) -> None:
        self.clear_message()
        self.save_requested.emit(self._address.text().strip(), self._token.text())

    def closeEvent(self, event: QCloseEvent) -> None:  # noqa: N802 - Qt naming
        if not self._quitting and not self._close_means_quit:
            event.ignore()
            self.hide()
            return
        if self._close_means_quit and not self._quitting:
            self.prepare_for_quit()
            self.close_accepted.emit()
