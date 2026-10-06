"""Internal core entry: runs the device runtime fed by the tray's pipe.

The core accepts exactly one startup configuration on stdin, containing the
Server address and the in-memory device token.  It never reads credentials
from the environment or the command line.  Structured status events go to
stdout as UTF-8 JSON lines; stderr carries bounded, sanitized diagnostics
only.  Closing stdin ends GUI ownership and triggers the existing full stop
flow.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import threading
from collections.abc import Callable
from typing import cast

from openoctopus_client.config import ConfigurationError, configuration_from_startup
from openoctopus_client.connection import ClientRuntime
from openoctopus_client.core_channel import (
    MAX_MESSAGE_BYTES,
    ChannelError,
    ExitReason,
    ExitResultMessage,
    StartupConfigMessage,
    StatusEventMessage,
    StopCommandMessage,
    encode_message,
    parse_gui_message,
)

_STARTUP_TIMEOUT_SECONDS = 60.0

LOGGER = logging.getLogger("openoctopus_client.core")


def _write_event(event: StatusEventMessage | ExitResultMessage) -> None:
    stdout = getattr(sys.stdout, "buffer", None)
    if stdout is None:
        return
    with contextlib.suppress(OSError, ValueError):
        stdout.write((encode_message(event) + "\n").encode("utf-8"))
        stdout.flush()


def _emit_stderr_diagnostic(message: str) -> None:
    stderr = getattr(sys.stderr, "buffer", None)
    if stderr is None:
        return
    with contextlib.suppress(OSError, ValueError):
        stderr.write((message[:4096] + "\n").encode("utf-8", errors="replace"))
        stderr.flush()


def _start_stdin_reader(lines: asyncio.Queue[bytes | None]) -> None:
    loop = asyncio.get_running_loop()

    def read_lines() -> None:
        stream = sys.stdin.buffer if getattr(sys.stdin, "buffer", None) is not None else None
        if stream is None:
            asyncio.run_coroutine_threadsafe(lines.put(None), loop).result()
            return
        while True:
            try:
                raw: bytes | None = stream.readline(MAX_MESSAGE_BYTES + 1)
            except OSError:
                raw = None
            if raw is None or raw == b"":
                asyncio.run_coroutine_threadsafe(lines.put(None), loop).result()
                return
            # Backpressure also bounds scheduled callbacks; EOF must never be dropped.
            asyncio.run_coroutine_threadsafe(lines.put(raw), loop).result()

    threading.Thread(target=read_lines, name="oo-core-stdin", daemon=True).start()


async def _read_startup(lines: asyncio.Queue[bytes | None]) -> StartupConfigMessage:
    raw = await asyncio.wait_for(lines.get(), timeout=_STARTUP_TIMEOUT_SECONDS)
    if raw is None:
        raise ChannelError("stdin closed before a startup configuration arrived")
    message = parse_gui_message(raw)
    if not isinstance(message, StartupConfigMessage):
        raise ChannelError("the first stdin message must be the startup configuration")
    return message


async def _follow_up_commands(
    lines: asyncio.Queue[bytes | None],
    runtime: ClientRuntime,
    owner_gone: dict[str, bool],
) -> None:
    while True:
        raw = await lines.get()
        if raw is None:
            # stdin closed: the GUI no longer owns this core.
            owner_gone["value"] = True
            runtime.request_shutdown()
            return
        try:
            message = parse_gui_message(raw)
        except ChannelError:
            _emit_stderr_diagnostic("ignored an invalid pipe command")
            continue
        if isinstance(message, StopCommandMessage):
            runtime.request_shutdown()


async def run_core(
    runtime_factory: Callable[[Callable[[StatusEventMessage], None]], ClientRuntime] | None = None,
) -> int:
    generation = 0

    def emit_status(event: StatusEventMessage) -> None:
        _write_event(event.model_copy(update={"generation": generation}))

    lines: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=32)
    _start_stdin_reader(lines)
    try:
        startup = await _read_startup(lines)
    except ChannelError as exc:
        _emit_stderr_diagnostic(f"startup configuration rejected: {exc}")
        _write_event(
            ExitResultMessage(
                type="exit",
                generation=0,
                return_code=78,
                cleanup_complete=True,
                reason="startup_config_invalid",
            )
        )
        return 78
    except TimeoutError:
        _emit_stderr_diagnostic("startup configuration timed out")
        _write_event(
            ExitResultMessage(
                type="exit",
                generation=0,
                return_code=78,
                cleanup_complete=True,
                reason="startup_config_invalid",
            )
        )
        return 78
    generation = startup.generation
    try:
        configuration = configuration_from_startup(
            startup.server_url,
            startup.token,
        )
    except ConfigurationError as exc:
        _emit_stderr_diagnostic(f"startup configuration rejected: {exc}")
        _write_event(
            ExitResultMessage(
                type="exit",
                generation=generation,
                return_code=78,
                cleanup_complete=True,
                reason="startup_config_invalid",
            )
        )
        return 78

    if runtime_factory is None:
        runtime = ClientRuntime(
            configuration,
            status_sink=emit_status,
        )
    else:
        runtime = runtime_factory(emit_status)
    owner_gone = {"value": False}
    follower = asyncio.create_task(_follow_up_commands(lines, runtime, owner_gone))
    installed = await runtime.install_signal_handlers()
    runtime_failed = False
    try:
        return_code = await runtime.run()
    except Exception:
        LOGGER.exception("the core runtime failed unexpectedly")
        return_code = 1
        runtime_failed = True
    finally:
        runtime.restore_signal_handlers(installed)
        follower.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await follower
    reason: str = "owner_gone" if owner_gone["value"] else runtime.terminal_reason
    if runtime_failed:
        reason = "runtime_failed"
    _write_event(
        ExitResultMessage(
            type="exit",
            generation=generation,
            return_code=min(max(return_code, 0), 255),
            cleanup_complete=runtime.cleanup_complete,
            reason=cast(ExitReason, reason),
        )
    )
    return return_code


def core_main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        code = asyncio.run(run_core())
    except KeyboardInterrupt:
        code = 0
    sys.stdout.flush()
    sys.stderr.flush()
    # The stdin reader is a daemon thread blocked in readline(); a normal
    # interpreter shutdown can abort while it touches the closing stdio
    # objects.  Every externally visible result (the exit event line) has
    # already been flushed, so terminate the process directly.
    os._exit(code)
