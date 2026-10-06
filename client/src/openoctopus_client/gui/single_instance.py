"""Single-instance arbitration: QLockFile plus a local activate socket.

A second launch of the same OS user's client wakes the already-running tray
and exits without reading credentials, contacting the Server, or taking over
the device connection.  The activation channel only accepts the fixed
``activate`` request; it carries no credentials and no execution requests,
and lives inside a user-private directory (POSIX socket permission bits are
not enough on their own).
"""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path

from PySide6.QtCore import QLockFile, QObject, Signal
from PySide6.QtNetwork import QLocalServer, QLocalSocket

_LOCK_NAME = "tray.lock"
_ACTIVATE_MESSAGE = b"activate\n"
_ACTIVATE_READ_MAX = 64


def _socket_name(directory: Path) -> str:
    identity = hashlib.sha256(os.fsencode(directory.resolve())).hexdigest()[:24]
    if os.name == "nt":
        return f"openoctopus-client-{identity}"
    # macOS limits Unix socket addresses to 104 bytes; configuration and
    # temporary directories can already exceed that before adding a filename.
    return f"/tmp/openoctopus-{getattr(os, 'getuid')()}-{identity}/tray.sock"


def _prepare_socket_directory(name: str) -> None:
    if os.name == "nt":
        return
    directory = Path(name).parent
    directory.mkdir(mode=0o700, exist_ok=True)
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != getattr(os, "getuid")():
        raise RuntimeError("single-instance directory is not owned by this user")
    directory.chmod(0o700)


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
        return ``False``. Qt reclaims locks owned by dead processes; the new
        owner removes any stale socket before listening.
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
        name = _socket_name(self._directory)
        try:
            _prepare_socket_directory(name)
        except (OSError, RuntimeError):
            self.release()
            raise
        QLocalServer.removeServer(name)
        server = QLocalServer(self)
        server.setSocketOptions(QLocalServer.SocketOption.UserAccessOption)
        server.newConnection.connect(self._on_connection)
        if not server.listen(name):
            lock.unlock()
            self._lock = None
            server.deleteLater()
            raise RuntimeError("could not open the single-instance channel")
        self._server = server
        return True

    def release(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server.deleteLater()
            self._server = None
            if os.name != "nt":
                Path(_socket_name(self._directory)).parent.rmdir()
        if self._lock is not None:
            self._lock.unlock()
            self._lock = None

    def _on_connection(self) -> None:
        while self._server is not None and self._server.hasPendingConnections():
            socket = self._server.nextPendingConnection()
            socket.readyRead.connect(lambda s=socket: self._read_activation(s))
            socket.disconnected.connect(socket.deleteLater)
            # Windows can buffer the request before newConnection is emitted.
            self._read_activation(socket)

    def _read_activation(self, socket: QLocalSocket) -> None:
        if not socket.canReadLine():
            if socket.bytesAvailable() >= _ACTIVATE_READ_MAX:
                socket.disconnectFromServer()
            return
        data = bytes(socket.readLine(_ACTIVATE_READ_MAX).data())
        if data == _ACTIVATE_MESSAGE:
            self.activate_requested.emit()
        socket.disconnectFromServer()

def send_activation(directory: Path) -> bool:
    """Wake the running instance; never raises, result is advisory."""

    socket = QLocalSocket()
    socket.connectToServer(_socket_name(directory))
    if not socket.waitForConnected(300):
        socket.abort()
        return False
    socket.write(_ACTIVATE_MESSAGE)
    socket.waitForBytesWritten(200)
    socket.disconnectFromServer()
    return True
