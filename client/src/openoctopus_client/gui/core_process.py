"""QProcess control of the private core process.

The GUI never imports the MCP, conversion, or exec machinery.  It launches
the core mode of the same installation, writes the single startup
configuration to the process' private stdin pipe, consumes the structured
stdout events line by line, keeps a bounded ring buffer of stderr for the
settings window, and reports terminal outcomes.  Each start gets a new
generation and its own QProcess; late output from a replaced process is
dropped with the process itself.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from PySide6.QtCore import QObject, QProcess, QTimer, Signal

from openoctopus_client.core_channel import (
    ChannelError,
    CoreEvent,
    ExitResultMessage,
    StartupConfigMessage,
    StatusEventMessage,
    StopCommandMessage,
    encode_message,
    parse_core_event,
)

_STDOUT_BUFFER_MAX = 1024 * 1024
_STDERR_RING_MAX = 64 * 1024
_STOP_GRACE_SECONDS = 20_000
_TERMINATE_GRACE_SECONDS = 5_000


@dataclass(frozen=True)
class CoreOutcome:
    """Terminal result for one core process run."""

    return_code: int | None
    reason: str
    cleanup_complete: bool | None
    crashed: bool
    exit_reported: bool


def core_command() -> list[str]:
    """Locate the core run mode of this installation.

    Frozen builds launch the sibling core mode of the same bundle; source
    launches use the current interpreter without touching the GUI mode.
    """

    if getattr(sys, "frozen", False):
        return [sys.executable, "_core-run"]
    return [sys.executable, "-m", "openoctopus_client", "_core-run"]


class CoreController(QObject):
    """Owns at most one core process and reports its structured events."""

    status_received = Signal(object)  # StatusEventMessage
    core_finished = Signal(object)  # CoreOutcome

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._process: QProcess | None = None
        self._generation = 0
        self._stdout_buffer = bytearray()
        self._stderr_ring = bytearray()
        self._exit_result: ExitResultMessage | None = None
        self._stop_requested = False
        self._escalation: QTimer | None = None

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def running(self) -> bool:
        return self._process is not None and (
            self._process.state() != QProcess.ProcessState.NotRunning
        )

    @property
    def stopping(self) -> bool:
        return self._stop_requested and self.running

    @property
    def stderr_diagnostics(self) -> str:
        return self._stderr_ring.decode("utf-8", errors="replace")

    def start(self, startup: StartupConfigMessage) -> None:
        """Launch the core and hand it the startup configuration once."""

        if self.running or self._process is not None:
            raise RuntimeError("a core process is already running or finishing")
        self._generation += 1
        startup = startup.model_copy(update={"generation": self._generation})
        self._stdout_buffer = bytearray()
        self._stderr_ring = bytearray()
        self._exit_result = None
        self._stop_requested = False
        process = QProcess(self)
        self._process = process
        process.setProcessChannelMode(QProcess.ProcessChannelMode.SeparateChannels)
        program, *arguments = core_command()
        # Defense in depth: credentials must never reach the core environment.
        environment = [
            entry
            for entry in QProcess.systemEnvironment()
            if not entry.startswith("OPENOCTOPUS_")
        ]
        process.setEnvironment(environment)
        process.readyReadStandardOutput.connect(self._on_stdout)
        process.readyReadStandardError.connect(self._on_stderr)
        process.finished.connect(self._on_finished)
        process.errorOccurred.connect(self._on_error)
        process.setProgram(program)
        process.setArguments(arguments)
        process.start(QProcess.OpenModeFlag.ReadWrite)
        payload = (encode_message(startup) + "\n").encode("utf-8")
        # The channel is written once, before ``started``; QProcess buffers
        # the bytes until the pipe exists.  The token never appears again.
        process.write(payload)

    def stop(self) -> None:
        """Ask the core for its existing full stop flow, with escalation."""

        process = self._process
        if process is None or not self.running or self._stop_requested:
            return
        self._stop_requested = True
        process.write((encode_message(StopCommandMessage(type="stop")) + "\n").encode("utf-8"))
        self._arm_escalation(process)

    def release_ownership(self) -> None:
        """Close stdin (ownership ends) and let the core stop itself."""

        process = self._process
        if process is None:
            return
        if self.running:
            process.closeWriteChannel()
            self._arm_escalation(process)

    def _arm_escalation(self, process: QProcess) -> None:
        timer = QTimer(self)
        timer.setSingleShot(True)

        def escalate() -> None:
            if process.state() != QProcess.ProcessState.NotRunning:
                timer2 = QTimer(self)
                timer2.setSingleShot(True)
                timer2.timeout.connect(lambda: self._force_kill(process))
                process.terminate()
                timer2.start(_TERMINATE_GRACE_SECONDS)

        timer.timeout.connect(escalate)
        timer.start(_STOP_GRACE_SECONDS)
        self._escalation = timer

    def _force_kill(self, process: QProcess) -> None:
        if process.state() != QProcess.ProcessState.NotRunning:
            process.kill()

    def _on_stdout(self) -> None:
        process = self.sender()
        if not isinstance(process, QProcess) or process is not self._process:
            return
        data = process.readAllStandardOutput().data()
        if len(self._stdout_buffer) + len(data) > _STDOUT_BUFFER_MAX:
            self._stdout_buffer.clear()
        self._stdout_buffer.extend(data)
        while True:
            newline = self._stdout_buffer.find(b"\n")
            if newline < 0:
                break
            line = bytes(self._stdout_buffer[:newline])
            del self._stdout_buffer[: newline + 1]
            if not line.strip():
                continue
            try:
                event = parse_core_event(line)
            except ChannelError:
                continue
            self._accept_event(event)

    def _accept_event(self, event: CoreEvent) -> None:
        if isinstance(event, StatusEventMessage):
            self.status_received.emit(event)
            return
        if isinstance(event, ExitResultMessage) and self._exit_result is None:
            self._exit_result = event

    def _on_stderr(self) -> None:
        process = self.sender()
        if not isinstance(process, QProcess) or process is not self._process:
            return
        data = process.readAllStandardError().data()
        self._stderr_ring.extend(data)
        if len(self._stderr_ring) > _STDERR_RING_MAX:
            del self._stderr_ring[: len(self._stderr_ring) - _STDERR_RING_MAX]

    def _on_finished(self, exit_code: int, exit_status: QProcess.ExitStatus) -> None:
        if self._escalation is not None:
            self._escalation.stop()
            self._escalation = None
        # Flush any tail events that arrived with the last chunk.
        self._on_stdout()
        exit_result = self._exit_result
        outcome = CoreOutcome(
            return_code=exit_code,
            reason=exit_result.reason if exit_result is not None else "process_crashed",
            cleanup_complete=exit_result.cleanup_complete if exit_result is not None else None,
            crashed=exit_status == QProcess.ExitStatus.CrashExit
            or (exit_result is None and exit_code != 0)
            or (
                exit_result is not None and exit_result.return_code != exit_code
            ),
            exit_reported=exit_result is not None,
        )
        self._process = None
        self._exit_result = None
        self._stop_requested = False
        self.core_finished.emit(outcome)

    def _on_error(self, error: QProcess.ProcessError) -> None:
        if error == QProcess.ProcessError.FailedToStart:
            process = self._process
            if process is not None and process.state() == QProcess.ProcessState.NotRunning:
                # ``finished`` is not emitted for FailedToStart on all platforms.
                self._process = None
                self.core_finished.emit(
                    CoreOutcome(
                        return_code=None,
                        reason="failed_to_start",
                        cleanup_complete=None,
                        crashed=True,
                        exit_reported=False,
                    )
                )
