"""Single-instance arbitration: QLockFile plus a local activate socket.

A second launch of the same OS user's client wakes the already-running tray
and exits without reading credentials, contacting the Server, or taking over
the device connection.  The activation channel only accepts the fixed
``activate`` request; it carries no credentials and no execution requests,
and lives inside a user-private directory (POSIX socket permission bits are
not enough on their own).
"""

from __future__ import annotations

import os
from pathlib import Path

from PySide6.QtCore import QLockFile, QObject, Signal
from PySide6.QtNetwork import QLocalServer, QLocalSocket

_LOCK_NAME = "tray.lock"
_SOCKET_NAME = "tray-instance.sock"
_ACTIVATE_MESSAGE = b"activate\n"
_ACTIVATE_READ_MAX = 64


class SingleInstance(QObject):
    activate_requested = Signal()

    def __init__(self, directory: Path, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._directory = directory
        self._lock: QLockFile | None = None
        self._server: QLocalServer | None = None

    @property
    def is_primary(self) -> bool:
        return self._lock is not None and self._lock.isLocked()

    def try_become_primary(self) -> bool:
        """Return ``True`` when this process owns the instance.

        When another instance owns it, send it one activation request and
        return ``False``.  Stale locks and sockets are reclaimed via the
        stale-lock timeout and ``removeServer``.
        """

        if self._lock is not None and self._lock.isLocked():
            send_activation(self._directory)
            return False
        self._directory.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            os.chmod(self._directory, 0o700)
        lock = QLockFile(str(self._directory / _LOCK_NAME))
        # Long-lived ownership: Qt checks the recorded PID; elapsed time alone
        # must not make a running application's lock stale.
        lock.setStaleLockTime(0)
        if not lock.tryLock(200):
            send_activation(self._directory)
            return False
        self._lock = lock
        QLocalServer.removeServer(self._socket_path())
        server = QLocalServer(self)
        server.setSocketOptions(QLocalServer.SocketOption.UserAccessOption)
        server.newConnection.connect(self._on_connection)
        if not server.listen(self._socket_path()):
            lock.unlock()
            self._lock = None
            raise RuntimeError("could not open the single-instance channel")
        self._server = server
        return True

    def _socket_path(self) -> str:
        return str(self._directory / _SOCKET_NAME)

    def release(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        if self._lock is not None:
            self._lock.unlock()
            self._lock = None

    def _on_connection(self) -> None:
        while self._server is not None and self._server.hasPendingConnections():
            socket = self._server.nextPendingConnection()
            socket.readyRead.connect(lambda s=socket: self._read_activation(s))
            socket.disconnected.connect(socket.deleteLater)

    def _read_activation(self, socket: QLocalSocket) -> None:
        data = bytes(socket.read(_ACTIVATE_READ_MAX).data())
        if data == _ACTIVATE_MESSAGE:
            self.activate_requested.emit()
        socket.disconnectFromServer()




def send_activation(directory: Path) -> bool:
    """Wake the running instance; never raises, result is advisory."""

    socket = QLocalSocket()
    socket.connectToServer(str(directory / _SOCKET_NAME))
    if not socket.waitForConnected(300):
        socket.abort()
        return False
    socket.write(_ACTIVATE_MESSAGE)
    socket.waitForBytesWritten(200)
    socket.disconnectFromServer()
    return True
