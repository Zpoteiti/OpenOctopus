"""Single launch entry point of the OpenOctopus Client program.

Running the program without arguments starts the tray GUI; that is the only
user-facing way to start the Client.  Every underscore-prefixed command is an
internal mode of the core and its helpers.  The old ``run``/``version`` CLI
surface and its environment-variable configuration were removed: the core is
started by the tray over the private pipe, and tests launch the same
``_core-run`` mode directly.
"""

from __future__ import annotations

import sys
from typing import NoReturn

_INTERNAL_USAGE = """usage: openoctopus-client            # start the tray client
internal modes (used by the tray and frozen smokes only):
  _core-run                 run the device core fed by the private pipe
  _conversion-worker        run one isolated document conversion request
  _pty-worker CONTROL_FD EVENTS_FD
  _exec-backend-smoke       smoke-test the frozen pipe/PTY backends
  _mcp-stdio-smoke EXECUTABLE FIXTURE
  _spike-convert PATH [--pages RANGE]
  _version                  print the bundled client version
"""


def main() -> int:
    argv = sys.argv[1:]
    command = argv[0] if argv else "gui"
    if command == "gui":
        # Keep GUI stdio defaults untouched; the GUI never parses its own
        # console output and may run without a console on Windows.
        from openoctopus_client.gui.app import gui_main

        return gui_main()
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="strict")
    if command == "_core-run":
        from openoctopus_client.core import core_main

        return core_main()
    if command == "_conversion-worker":
        from openoctopus_client.document_convert import conversion_worker_main

        return conversion_worker_main()
    if command == "_pty-worker":
        from openoctopus_client.pty_worker import run as pty_worker_run

        if len(argv) != 3:
            _reject_internal_usage()
        try:
            control_fd = int(argv[1])
            events_fd = int(argv[2])
        except ValueError:
            _reject_internal_usage()
        return pty_worker_run(control_fd, events_fd)
    if command == "_exec-backend-smoke":
        from openoctopus_client.internal_modes import exec_backend_smoke

        return exec_backend_smoke()
    if command == "_mcp-stdio-smoke":
        from openoctopus_client.internal_modes import mcp_stdio_smoke

        if len(argv) != 3:
            _reject_internal_usage()
        from pathlib import Path

        return mcp_stdio_smoke(argv[1], Path(argv[2]))
    if command == "_spike-convert":
        from pathlib import Path

        from openoctopus_client.internal_modes import spike_convert

        if len(argv) == 2:
            return spike_convert(Path(argv[1]), None)
        if len(argv) == 4 and argv[2] == "--pages":
            return spike_convert(Path(argv[1]), argv[3])
        _reject_internal_usage()
    if command == "_version":
        from openoctopus_client import __version__

        print(__version__)
        return 0
    _reject_internal_usage()


def _reject_internal_usage() -> NoReturn:
    print(_INTERNAL_USAGE, file=sys.stderr)
    raise SystemExit(2)
