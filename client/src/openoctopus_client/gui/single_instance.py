"""Single-instance arbitration: QLockFile plus a local activate socket.

A second launch of the same OS user's client wakes the already-running tray
and exits without reading credentials, contacting the Server, or taking over
the device connection.  The activation channel only accepts the fixed
``activate`` request; it carries no credentials and no execution requests,
and lives inside a user-private directory (POSIX socket permission bits are
not enough on their own).
"""

from __future__ import annotations

import json
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
        lock.setStaleLockTime(15_000)
        if not lock.tryLock(200):
            # Either a live owner (activate it) or a lock file left behind by
            # a process that died inside the stale window.  QLockFile refuses
            # the latter until the window passes, which would make a
            # supervised relaunch look like "another instance is running";
            # reclaim an orphaned lock file, then retry once.
            if not self.clear_orphaned_lock():
                send_activation(self._directory)
                return False
            if not lock.tryLock(5_000):
                send_activation(self._directory)
                return False
        self._lock = lock
        QLocalServer.removeServer(self._socket_path())
        server = QLocalServer(self)
        server.newConnection.connect(self._on_connection)
        if not server.listen(self._socket_path()):
            lock.unlock()
            self._lock = None
            raise RuntimeError("could not open the single-instance channel")
        self._server = server
        return True

    def _socket_path(self) -> str:
        return str(self._directory / _SOCKET_NAME)

    def clear_orphaned_lock(self) -> bool:
        """Reclaim a lock file whose recorded owning process is gone.

        ``QLockFile`` only classifies a lock as stale after the stale
        timeout, which is too slow for a supervisor that deliberately
        relaunches its own process tree (the smoke harness restarts the tray
        while the stale window is still open).  The owning pid is read from
        the lock payload; a live owner is never touched, and a dead owner's
        orphaned lock file is reclaimed through ``QLockFile`` itself so a
        concurrent relaunch can never both believe they own the instance.
        """

        lock_path = self._directory / _LOCK_NAME
        try:
            raw = json.loads(lock_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return False
        pid = raw.get("pid") if isinstance(raw, dict) else None
        if not isinstance(pid, int) or pid <= 0 or pid == os.getpid():
            return False
        if _process_is_alive(pid):
            # A live owner keeps its lock; the caller activates it instead.
            return False
        probe = QLockFile(str(lock_path))
        if probe.tryLock(2_000):
            probe.unlock()
            return True
        return False

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
        if _ACTIVATE_MESSAGE in data or data == b"activate":
            self.activate_requested.emit()
        socket.disconnectFromServer()




def _process_is_alive(pid: int) -> bool:
    if os.name == "nt":
        return _windows_process_is_alive(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _windows_process_is_alive(pid: int) -> bool:
    # ``os.kill(pid, 0)`` would terminate the process on Windows, so query a
    # limited-information handle instead; a closed handle means the pid free.
    import ctypes  # noqa: PLC0415 - platform-local probe

    kernel32 = ctypes.WinDLL("kernel32")  # type: ignore[attr-defined]
    handle = kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        return False
    try:
        code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return False
        return code.value != 259  # 259 == STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


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
