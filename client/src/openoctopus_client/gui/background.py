"""Background execution for blocking GUI-adjacent work (credential store).

The Qt main thread only handles Qt events; credential-store calls run on the
global thread pool and deliver their result back through a queued signal.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Signal

OnDone = Callable[[object, BaseException | None], None]


class Runner(Protocol):
    def submit(self, action: Callable[[], object], on_done: OnDone) -> None: ...


class _TaskSignals(QObject):
    finished = Signal(object, object)  # result, error


class _Task(QRunnable):
    def __init__(self, signals: _TaskSignals, action: Callable[[], object]) -> None:
        super().__init__()
        self._signals = signals
        self._action = action
        self.setAutoDelete(True)

    def run(self) -> None:  # pragma: no cover - executed on the pool thread
        try:
            result = self._action()
        except BaseException as exc:  # noqa: BLE001 - delivered to the GUI thread
            self._signals.finished.emit(None, exc)
        else:
            self._signals.finished.emit(result, None)


class ThreadPoolRunner(QObject):
    """Production runner: pool thread in, queued callback on the GUI thread."""

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._pool = QThreadPool.globalInstance()
        self._in_flight: list[_TaskSignals] = []

    def submit(self, action: Callable[[], object], on_done: OnDone) -> None:
        signals = _TaskSignals()
        self._in_flight.append(signals)

        def deliver(result: object, error: BaseException | None) -> None:
            if signals in self._in_flight:
                self._in_flight.remove(signals)
            on_done(result, error)

        signals.finished.connect(deliver)
        self._pool.start(_Task(signals, action))


class DirectRunner(QObject):
    """Test runner that executes inline while keeping the same contract."""

    def submit(self, action: Callable[[], object], on_done: OnDone) -> None:
        try:
            result = action()
        except BaseException as exc:  # noqa: BLE001
            on_done(None, exc)
        else:
            on_done(result, None)
