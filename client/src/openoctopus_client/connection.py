from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import platform
import random
import signal
import stat
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from uuid import UUID

from websockets.asyncio.client import connect

from openoctopus_client import __version__
from openoctopus_client.config import ClientConfiguration
from openoctopus_client.exec_sessions import ExecPolicy, ExecSessionManager
from openoctopus_client.mcp.supervisor import McpInvocationLease, McpSupervisor
from openoctopus_client.process import ShellInventory, discover_shells
from openoctopus_client.protocol import (
    EXEC_TOOL_NAMES,
    ConfigApplied,
    ConfigAppliedAck,
    ConfigUpdate,
    ConfigValidate,
    ConfigValidateCancel,
    ConfigValidateResult,
    DeviceConfig,
    ErrorFrame,
    Hello,
    HelloAck,
    PersistedMcpCatalog,
    Ping,
    Pong,
    ProtocolError,
    RegisterMcpAck,
    ShellMetadata,
    ToolCall,
    ToolResult,
    TransferBegin,
    TransferEnd,
    TransferProgress,
    TransferReady,
    TransferRequest,
    decode_server_frame,
    encode_frame,
)
from openoctopus_client.tools import ClientToolDispatcher, ToolOutput
from openoctopus_client.tools.exec import ExecManager, ExecToolDispatcher
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.transfer import (
    MAX_ACTIVE_TRANSFER_SLOTS,
    TransferConfigSnapshot,
    TransferManager,
)
from openoctopus_client.writer import SerializedWriter, TextWebSocket, WriterOverflowError

LOGGER = logging.getLogger(__name__)
_TOOL_QUEUE_MAX = 64
_TOOL_QUEUE_BYTES_MAX = 32 * 1024 * 1024
_SHUTDOWN_GRACE_SECONDS = 2.0
_SHUTDOWN_WATCHDOG_SECONDS = 15.0
_HELLO_ACK_TIMEOUT_SECONDS = 10.0
_RETRYABLE_CLOSE_CODES = frozenset({1000, 1001, 1006, 1013, 4408})


class ClosableWebSocket(TextWebSocket, Protocol):
    async def recv(self) -> str | bytes | None: ...

    async def close(self, code: int, reason: str) -> None: ...


class LocalToolDispatcher(Protocol):
    async def execute(self, name: str, args: dict[str, Any]) -> ToolOutput: ...


class ToolDispatcher(Protocol):
    async def execute_call(self, call: ToolCall) -> ToolOutput: ...


class _RuntimeExecManager(ExecManager, Protocol):
    async def apply_policy(self, policy: ExecPolicy) -> None: ...

    async def shutdown(self) -> bool: ...


_QueuedToolDispatcher = ToolDispatcher | LocalToolDispatcher


class _DeviceToolDispatcher:
    """Combine existing local tools with the policy-bound exec tools."""

    def __init__(
        self,
        local: LocalToolDispatcher,
        exec_tools: ExecToolDispatcher,
    ) -> None:
        self._local = local
        self._exec_tools = exec_tools

    async def execute_call(self, call: ToolCall) -> ToolOutput:
        if call.name in EXEC_TOOL_NAMES:
            return await self._exec_tools.execute(
                call.name,
                call.args,
                chat_session_id=call.chat_session_id,
            )
        return await self._local.execute(call.name, call.args)

    def has_pending_blocking(self) -> bool:
        return _dispatcher_has_pending_blocking(self._local)

    async def wait_for_pending_blocking(self) -> None:
        await _wait_for_dispatcher_blocking(self._local)


class _McpToolDispatcher:
    def __init__(self, supervisor: McpSupervisor) -> None:
        self._supervisor = supervisor

    async def execute_call(self, call: ToolCall) -> ToolOutput:
        return await self._supervisor.invoke(call)

    def reserve_invocation(self, call: ToolCall) -> McpInvocationLease:
        return self._supervisor.reserve_invocation(call)


class _RetryableConfigError(RuntimeError):
    """A local device configuration could not be prepared safely."""


class _ConfigBoundDispatcher:
    """Bind a tool call to the configuration generation visible on receipt."""

    def __init__(self, prepared: asyncio.Task[_PreparedConfig]) -> None:
        self._prepared = prepared
        self._resolved: ToolDispatcher | None = None

    async def execute_call(self, call: ToolCall) -> ToolOutput:
        dispatcher = (await asyncio.shield(self._prepared)).dispatcher
        self._resolved = dispatcher
        return await dispatcher.execute_call(call)

    def has_pending_blocking(self) -> bool:
        return self._resolved is not None and _dispatcher_has_pending_blocking(self._resolved)

    async def wait_for_pending_blocking(self) -> None:
        if self._resolved is not None:
            await _wait_for_dispatcher_blocking(self._resolved)


@dataclass(frozen=True)
class _PreparedConfig:
    workspace: Path
    dispatcher: ToolDispatcher
    config: DeviceConfig
    device_name: str


@dataclass(frozen=True)
class _PreparedConfigCandidate:
    workspace: Path
    dispatcher: LocalToolDispatcher
    config: DeviceConfig
    device_name: str


@dataclass(frozen=True)
class _QueuedToolCall:
    call: ToolCall
    dispatcher: _QueuedToolDispatcher
    retained_bytes: int
    mcp_invocation: McpInvocationLease | None = None


class _ToolWorker:
    def __init__(self, runtime: ClientRuntime, writer: SerializedWriter) -> None:
        self._runtime = runtime
        self._writer = writer
        self._queue: asyncio.Queue[_QueuedToolCall] = asyncio.Queue(maxsize=_TOOL_QUEUE_MAX)
        self._retained_bytes = 0
        self._active = False
        self._failed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._dispatchers: list[_QueuedToolDispatcher] = []
        self._task = asyncio.create_task(self._run())

    @property
    def failed(self) -> asyncio.Future[None]:
        return self._failed

    def enqueue(self, call: ToolCall, dispatcher: _QueuedToolDispatcher) -> bool:
        retained_bytes = len(call.model_dump_json().encode("utf-8")) + call.max_result_bytes
        if (
            (call.name in EXEC_TOOL_NAMES and (self._active or not self._queue.empty()))
            or self._queue.full()
            or (self._retained_bytes + retained_bytes > _TOOL_QUEUE_BYTES_MAX)
        ):
            return False
        if not any(item is dispatcher for item in self._dispatchers):
            self._dispatchers.append(dispatcher)
        mcp_invocation = (
            dispatcher.reserve_invocation(call)
            if isinstance(dispatcher, _McpToolDispatcher)
            else None
        )
        self._queue.put_nowait(
            _QueuedToolCall(
                call=call,
                dispatcher=dispatcher,
                retained_bytes=retained_bytes,
                mcp_invocation=mcp_invocation,
            )
        )
        self._retained_bytes += retained_bytes
        return True

    async def stop(self, *, timeout: float | None = None) -> bool:
        self._task.cancel()
        stopped = False
        if timeout is None:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except BaseException:
                # The worker's failure is reported through ``self.failed``;
                # stopping an already-failed worker must still release the
                # connection resources that own it.
                pass
            stopped = True
        else:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout)
                stopped = True
            except asyncio.CancelledError:
                # Cancellation is the normal successful terminal state after
                # ``self._task.cancel()``.
                stopped = True
            except TimeoutError:
                # A filesystem syscall running in a worker thread cannot be
                # cancelled.  The process watchdog bounds the remaining
                # shutdown instead of waiting for that thread indefinitely.
                pass
            except BaseException:
                # A failed worker is already stopped.  Its exception is
                # surfaced by ``worker_failure_task`` and must not abort the
                # cleanup path a second time.
                stopped = True
        if stopped:
            if timeout is None:
                await asyncio.gather(
                    *(
                        _wait_for_dispatcher_blocking(dispatcher)
                        for dispatcher in self._dispatchers
                    ),
                    return_exceptions=True,
                )
            stopped = not any(
                _dispatcher_has_pending_blocking(dispatcher) for dispatcher in self._dispatchers
            )
        while not self._queue.empty():
            request = self._queue.get_nowait()
            if request.mcp_invocation is not None:
                request.mcp_invocation.release()
            self._retained_bytes -= request.retained_bytes
            self._queue.task_done()
        return stopped

    async def _run(self) -> None:
        try:
            while True:
                request = await self._queue.get()
                self._active = True
                try:
                    if request.mcp_invocation is not None:
                        invocation = self._runtime._start_mcp_tool(
                            request.call,
                            request.mcp_invocation,
                        )
                        result = await asyncio.shield(invocation)
                    else:
                        result = await self._runtime._run_tool(
                            request.call, request.dispatcher
                        )
                    self._writer.enqueue_normal(encode_frame(result))
                    await _wait_for_dispatcher_blocking(request.dispatcher)
                finally:
                    self._active = False
                    self._retained_bytes -= request.retained_bytes
                    self._queue.task_done()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if not self._failed.done():
                self._failed.set_exception(exc)
            raise


class ReconnectDisposition(Enum):
    RETRY = "retry"
    PERMANENT_AUTH = "permanent_auth"
    PERMANENT_CONFIG = "permanent_config"
    PERMANENT_REPLACED = "permanent_replaced"


class CloseDisposition(Enum):
    RETRY = "retry"
    SHUTDOWN = "shutdown"


class ClientRuntime:
    def __init__(
        self,
        config: ClientConfiguration,
        *,
        hello_factory: Callable[[], Hello] | None = None,
        random_value: Callable[[], float] = random.random,
        tool_dispatcher_factory: Callable[[Path, bool, list[str]], LocalToolDispatcher]
        | None = None,
        shell_inventory: ShellInventory | None = None,
        exec_session_manager: _RuntimeExecManager | None = None,
        mcp_supervisor: McpSupervisor | None = None,
        hard_exit: Callable[[int], object] = os._exit,
    ) -> None:
        self._config = config
        self._hello_factory = hello_factory or self._new_hello
        self._random_value = random_value
        self._path_locks = PathLocks()
        self._shell_inventory = shell_inventory or discover_shells()
        self._exec_sessions: _RuntimeExecManager = exec_session_manager or ExecSessionManager()
        self._exec_policy_lock = asyncio.Lock()
        self._exec_policy_key: tuple[object, ...] | None = None
        self._exec_policy: ExecPolicy | None = None
        self._exec_policy_epoch = 0
        self._exec_shutdown = False
        self._mcp_supervisor = mcp_supervisor or McpSupervisor(
            secret_transport_safe=config.server_url.startswith("https://")
        )
        self._mcp_dispatcher = _McpToolDispatcher(self._mcp_supervisor)
        self._mcp_shutdown = False
        self._mcp_invocation_tasks: set[asyncio.Task[ToolResult]] = set()
        self._tool_dispatcher_factory = tool_dispatcher_factory or self._default_dispatcher
        self._stopping = asyncio.Event()
        self._active_config: DeviceConfig | None = None
        self._config_revision: int | None = None
        self._mcp_catalog: PersistedMcpCatalog | None = None
        self._ever_ready = False
        self._device_name: str | None = None
        self._tools: ToolDispatcher | None = None
        self._transfer_manager: TransferManager | None = None
        self._config_tasks: set[asyncio.Task[Any]] = set()
        self._config_generation = 0
        self._hard_exit = hard_exit
        self._shutdown_watchdog_lock = threading.Lock()
        self._shutdown_watchdog: threading.Timer | None = None
        self._shutdown_cleanup_incomplete = False

    def request_shutdown(self) -> None:
        self._stopping.set()
        self._arm_shutdown_watchdog()

    def _arm_shutdown_watchdog(self) -> None:
        with self._shutdown_watchdog_lock:
            if self._shutdown_watchdog is not None:
                return
            watchdog = threading.Timer(_SHUTDOWN_WATCHDOG_SECONDS, self._force_exit)
            watchdog.daemon = True
            self._shutdown_watchdog = watchdog
            watchdog.start()

    def _force_exit(self) -> None:
        with self._shutdown_watchdog_lock:
            self._shutdown_watchdog = None
        self._hard_exit(1)

    def _cancel_shutdown_watchdog(self) -> None:
        with self._shutdown_watchdog_lock:
            watchdog = self._shutdown_watchdog
            self._shutdown_watchdog = None
        if watchdog is not None:
            watchdog.cancel()

    async def run(self) -> int:
        attempt = 0
        loop = asyncio.get_running_loop()
        installed_signals: list[signal.Signals] = []
        windows_break_signal = getattr(signal, "SIGBREAK", None)
        previous_windows_break_handler: Any = None
        windows_break_installed = False
        if os.name == "nt" and windows_break_signal is not None:
            try:
                previous_windows_break_handler = signal.getsignal(windows_break_signal)
                signal.signal(
                    windows_break_signal,
                    lambda _signum, _frame: self.request_shutdown(),
                )
                windows_break_installed = True
            except (OSError, ValueError):
                pass
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, self.request_shutdown)
            except (NotImplementedError, OSError):
                # Native Windows can use the console's KeyboardInterrupt; the
                # Unix source build gets an async wake-up for both signals.
                continue
            installed_signals.append(signum)
        try:
            while not self._stopping.is_set():
                retry_after: float | None = None
                attempt_task = asyncio.create_task(self._run_connection_attempt())
                stop_task = asyncio.create_task(self._stopping.wait())
                done, _ = await asyncio.wait(
                    {attempt_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_task in done:
                    attempt_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await attempt_task
                    return 0
                stop_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stop_task
                failed_attempt = False
                try:
                    disposition = attempt_task.result()
                except Exception as exc:
                    disposition = None
                    failed_attempt = True
                    disposition_from_error = reconnect_disposition_from_exception(exc)
                    retry_after = retry_after_from_exception(exc)
                    if disposition_from_error == ReconnectDisposition.PERMANENT_AUTH:
                        LOGGER.error("Device authentication was rejected")
                        return 1
                    if disposition_from_error == ReconnectDisposition.PERMANENT_REPLACED:
                        LOGGER.error("This client was replaced by a newer device connection")
                        return 1
                    if disposition_from_error == ReconnectDisposition.PERMANENT_CONFIG:
                        detail = _sanitized_close_detail(exc)
                        suffix = f": {detail}" if detail else ""
                        LOGGER.error(
                            "Device server URL or protocol configuration was rejected%s",
                            suffix,
                        )
                        return 78
                    if not self._ever_ready:
                        LOGGER.error("The initial device connection is unreachable")
                        return 1
                if disposition == CloseDisposition.SHUTDOWN or self._stopping.is_set():
                    return 0
                if disposition == CloseDisposition.RETRY and not self._ever_ready:
                    LOGGER.error("The initial device connection is unreachable")
                    return 1
                if not failed_attempt:
                    attempt = 0
                    retry_after = None if disposition is not None else retry_after
                delay = reconnect_delay(
                    attempt,
                    retry_after=retry_after,
                    random_value=self._random_value(),
                )
                attempt += 1
                try:
                    await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                except TimeoutError:
                    pass
            return 0
        finally:
            self._arm_shutdown_watchdog()
            await asyncio.gather(
                self._shutdown_exec_sessions(),
                self._shutdown_mcp(),
            )
            if not self._shutdown_cleanup_incomplete:
                self._cancel_shutdown_watchdog()
            for signum in installed_signals:
                with contextlib.suppress(NotImplementedError, OSError):
                    loop.remove_signal_handler(signum)
            if windows_break_installed:
                assert windows_break_signal is not None
                with contextlib.suppress(OSError, ValueError):
                    signal.signal(windows_break_signal, previous_windows_break_handler)

    async def _run_connection_attempt(self) -> CloseDisposition:
        async with connect(
            self._config.websocket_url,
            additional_headers={"Authorization": f"Bearer {self._config.token.reveal()}"},
            compression=None,
            max_queue=1,
            max_size=12 * 1024 * 1024,
            ping_interval=None,
        ) as websocket:
            return await self.run_connection(websocket)

    async def run_connection(self, websocket: ClosableWebSocket) -> CloseDisposition:
        self._config_generation += 1
        config_generation = self._config_generation
        writer = SerializedWriter()
        writer_task = asyncio.create_task(writer.run(websocket))
        worker = _ToolWorker(self, writer)
        worker_failure_task = asyncio.create_task(_wait_for_worker_failure(worker.failed))
        transfer_manager: TransferManager | None = None
        transfer_failure_task: asyncio.Task[str | bytes | None] | None = None
        config_update_tasks: list[asyncio.Task[_PreparedConfig]] = []
        config_update_frames: dict[asyncio.Task[_PreparedConfig], ConfigUpdate] = {}
        pending_config_ack: ConfigUpdate | None = None
        config_ack_deadline: float | None = None
        config_bound_transfer_tasks: list[asyncio.Task[None]] = []
        validation_tasks: dict[asyncio.Task[ConfigValidateResult | None], UUID] = {}
        suppressed_validation_tasks: set[
            asyncio.Task[ConfigValidateResult | None]
        ] = set()
        mcp_control_tasks: dict[asyncio.Task[None], UUID] = {}
        registration_signal_task: asyncio.Task[bool] | None = None
        registration_deadline: float | None = None
        mcp_attached = False
        receive_task: asyncio.Task[str | bytes | None] | None = None
        stopping_task = asyncio.create_task(self._stopping.wait())
        hello = self._hello_factory()
        acknowledged = False
        pending_hello_ack: HelloAck | None = None
        shutdown_requested = False
        hello_deadline = asyncio.get_running_loop().time() + _HELLO_ACK_TIMEOUT_SECONDS
        try:
            writer.enqueue_normal(encode_frame(hello))
            # The protocol requires hello to be the first outbound frame.
            hello_drain = asyncio.create_task(writer.drain())
            hello_wait: set[asyncio.Task[Any]] = {
                hello_drain,
                writer_task,
                stopping_task,
            }
            done, _ = await asyncio.wait(
                hello_wait,
                timeout=_HELLO_ACK_TIMEOUT_SECONDS,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stopping_task in done:
                shutdown_requested = True
                hello_drain.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await hello_drain
                return CloseDisposition.SHUTDOWN
            if writer_task in done:
                writer_task.result()
            if hello_drain not in done:
                hello_drain.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await hello_drain
                raise ProtocolError("Timed out sending hello")
            await hello_drain
            while True:
                if receive_task is None:
                    receive_task = asyncio.create_task(websocket.recv())
                wait_set: set[asyncio.Task[object]] = {
                    receive_task,
                    worker_failure_task,
                    writer_task,
                    stopping_task,
                }
                if transfer_failure_task is not None:
                    wait_set.add(transfer_failure_task)
                wait_set.update(config_update_tasks)
                wait_set.update(config_bound_transfer_tasks)
                wait_set.update(validation_tasks)
                wait_set.update(mcp_control_tasks)
                if (
                    acknowledged
                    and not self._mcp_supervisor.has_pending_registration
                    and registration_signal_task is None
                ):
                    registration_signal_task = asyncio.create_task(
                        self._mcp_supervisor.registration_changed.wait()
                    )
                if registration_signal_task is not None:
                    wait_set.add(registration_signal_task)
                timeout = None
                if not acknowledged:
                    timeout = max(0.0, hello_deadline - asyncio.get_running_loop().time())
                elif pending_config_ack is not None:
                    assert config_ack_deadline is not None
                    timeout = max(
                        0.0,
                        config_ack_deadline - asyncio.get_running_loop().time(),
                    )
                if registration_deadline is not None:
                    registration_timeout = max(
                        0.0,
                        registration_deadline - asyncio.get_running_loop().time(),
                    )
                    timeout = (
                        registration_timeout
                        if timeout is None
                        else min(timeout, registration_timeout)
                    )
                done, _ = await asyncio.wait(
                    wait_set,
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    receive_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await receive_task
                    if pending_config_ack is not None:
                        raise ProtocolError("Timed out waiting for config applied acknowledgement")
                    if registration_deadline is not None:
                        raise ProtocolError(
                            "Timed out waiting for MCP registration acknowledgement"
                        )
                    raise ProtocolError("Timed out waiting for hello acknowledgement")
                if stopping_task in done:
                    shutdown_requested = True
                    receive_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await receive_task
                    return CloseDisposition.SHUTDOWN
                if writer_task in done:
                    writer_task.result()
                if worker_failure_task in done:
                    receive_task.cancel()
                    try:
                        await receive_task
                    except asyncio.CancelledError:
                        pass
                    worker_failure_task.result()
                if transfer_failure_task is not None and transfer_failure_task in done:
                    receive_task.cancel()
                    try:
                        await receive_task
                    except asyncio.CancelledError:
                        pass
                    transfer_failure_task.result()
                completed_controls = [task for task in mcp_control_tasks if task in done]
                for control_task in completed_controls:
                    control_task.result()
                    mcp_control_tasks.pop(control_task)
                completed_validations = [task for task in validation_tasks if task in done]
                for validation_task in completed_validations:
                    result = validation_task.result()
                    validation_tasks.pop(validation_task)
                    if (
                        result is not None
                        and validation_task not in suppressed_validation_tasks
                    ):
                        writer.enqueue_normal(encode_frame(result))
                    suppressed_validation_tasks.discard(validation_task)
                if registration_signal_task is not None and registration_signal_task in done:
                    registration_signal_task.result()
                    registration_signal_task = None
                    self._mcp_supervisor.consume_registration_signal()
                    registration = self._mcp_supervisor.next_registration()
                    if registration is not None:
                        writer.enqueue_normal(encode_frame(registration))
                        registration_deadline = (
                            asyncio.get_running_loop().time() + _HELLO_ACK_TIMEOUT_SECONDS
                        )
                completed_updates = [task for task in config_update_tasks if task in done]
                completed_transfers = [task for task in config_bound_transfer_tasks if task in done]
                if completed_updates or completed_transfers:
                    for update_task in completed_updates:
                        update_task.result()
                        config_update_tasks.remove(update_task)
                        update = config_update_frames.pop(update_task)
                        if pending_config_ack is not None:
                            raise ProtocolError("Concurrent configuration updates are not allowed")
                        writer.enqueue_normal(
                            encode_frame(
                                ConfigApplied(
                                    id=update.id,
                                    config_revision=update.config_revision,
                                )
                            )
                        )
                        pending_config_ack = update
                        config_ack_deadline = (
                            asyncio.get_running_loop().time() + _HELLO_ACK_TIMEOUT_SECONDS
                        )
                    for transfer_task in completed_transfers:
                        transfer_task.result()
                        config_bound_transfer_tasks.remove(transfer_task)
                if receive_task not in done:
                    continue
                payload = receive_task.result()
                receive_task = None
                if payload is None:
                    return CloseDisposition.RETRY
                if isinstance(payload, bytes):
                    if not acknowledged or transfer_manager is None:
                        raise ProtocolError("Unexpected binary frame")
                    await transfer_manager.handle_binary(payload)
                    continue
                frame = decode_server_frame(payload)
                if not acknowledged:
                    if pending_hello_ack is None:
                        if not isinstance(frame, HelloAck) or frame.id != hello.id:
                            raise ProtocolError("Expected matching hello acknowledgement")
                        await self._install_config(
                            frame.device_name,
                            frame.config,
                            generation=config_generation,
                        )
                        writer.enqueue_normal(
                            encode_frame(
                                ConfigApplied(
                                    id=frame.id,
                                    config_revision=frame.config_revision,
                                )
                            )
                        )
                        pending_hello_ack = frame
                        hello_deadline = (
                            asyncio.get_running_loop().time() + _HELLO_ACK_TIMEOUT_SECONDS
                        )
                        continue
                    if (
                        not isinstance(frame, ConfigAppliedAck)
                        or frame.id != pending_hello_ack.id
                        or frame.config_revision != pending_hello_ack.config_revision
                    ):
                        raise ProtocolError("Expected matching config applied acknowledgement")
                    assert self._active_config is not None
                    transfer_manager = TransferManager(
                        TransferConfigSnapshot.from_values(
                            Path(self._active_config.workspace_path),
                            restrict_to_workspace=self._active_config.restrict_to_workspace,
                            ssrf_denylist=self._active_config.ssrf_denylist,
                            device_name=pending_hello_ack.device_name,
                        ),
                        writer,
                        path_locks=self._path_locks,
                    )
                    self._transfer_manager = transfer_manager
                    transfer_failure_task = asyncio.create_task(
                        _wait_for_transfer_failure(transfer_manager.failed)
                    )
                    self._config_revision = pending_hello_ack.config_revision
                    self._mcp_catalog = pending_hello_ack.mcp_catalog
                    await self._mcp_supervisor.activate_authoritative(
                        revision=pending_hello_ack.config_revision,
                        config=pending_hello_ack.config,
                        catalog=pending_hello_ack.mcp_catalog,
                    )
                    self._mcp_supervisor.attach_connection()
                    mcp_attached = True
                    acknowledged = True
                    self._ever_ready = True
                    continue
                if pending_config_ack is not None:
                    if isinstance(frame, RegisterMcpAck):
                        self._mcp_supervisor.accept_registration(frame)
                        registration_deadline = None
                        continue
                    if (
                        isinstance(frame, ConfigAppliedAck)
                        and frame.id == pending_config_ack.id
                        and frame.config_revision == pending_config_ack.config_revision
                    ):
                        self._config_revision = pending_config_ack.config_revision
                        self._mcp_catalog = pending_config_ack.mcp_catalog
                        await self._mcp_supervisor.activate_authoritative(
                            revision=pending_config_ack.config_revision,
                            config=pending_config_ack.config,
                            catalog=pending_config_ack.mcp_catalog,
                            validation_id=pending_config_ack.id,
                        )
                        pending_config_ack = None
                        config_ack_deadline = None
                        continue
                    if isinstance(frame, Ping):
                        writer.enqueue_critical(encode_frame(Pong(id=frame.id)))
                        continue
                    if isinstance(frame, (TransferReady, TransferProgress, TransferEnd)):
                        if transfer_manager is None:
                            raise ProtocolError("Transfer arrived before device configuration")
                        await transfer_manager.handle_control(frame)
                        continue
                    if isinstance(frame, ErrorFrame):
                        continue
                    raise ProtocolError("Expected matching config applied acknowledgement")
                if isinstance(frame, Ping):
                    writer.enqueue_critical(encode_frame(Pong(id=frame.id)))
                elif isinstance(frame, ConfigValidate):
                    validation_task = self._mcp_supervisor.begin_validation(frame)
                    validation_tasks[validation_task] = frame.id
                elif isinstance(frame, ConfigValidateCancel):
                    suppressed_validation_tasks.update(
                        task
                        for task, validation_id in validation_tasks.items()
                        if validation_id == frame.id
                    )
                    control_task = asyncio.create_task(
                        self._mcp_supervisor.cancel_validation(frame.id)
                    )
                    mcp_control_tasks[control_task] = frame.id
                elif isinstance(frame, RegisterMcpAck):
                    self._mcp_supervisor.accept_registration(frame)
                    registration_deadline = None
                elif isinstance(frame, ConfigUpdate):
                    if config_update_tasks or pending_config_ack is not None:
                        raise ProtocolError("Concurrent configuration updates are not allowed")
                    if (
                        self._config_revision is None
                        or frame.config_revision <= self._config_revision
                    ):
                        raise ProtocolError("Configuration revision is not newer")
                    update_task = self._schedule_config_update(
                        frame.device_name,
                        frame.config,
                        generation=config_generation,
                        previous=None,
                        transfer_manager=transfer_manager,
                    )
                    config_update_tasks.append(update_task)
                    config_update_frames[update_task] = frame
                elif isinstance(frame, ToolCall):
                    if self._tools is None:
                        raise ProtocolError("Tool call arrived before device configuration")
                    if config_update_tasks and frame.name in EXEC_TOOL_NAMES:
                        writer.enqueue_normal(encode_frame(self._busy_tool_result(frame)))
                        continue
                    dispatcher: ToolDispatcher = (
                        self._mcp_dispatcher if frame.mcp_route is not None else self._tools
                    )
                    if config_update_tasks and frame.mcp_route is None:
                        dispatcher = _ConfigBoundDispatcher(config_update_tasks[-1])
                    if not worker.enqueue(frame, dispatcher):
                        writer.enqueue_normal(encode_frame(self._busy_tool_result(frame)))
                elif isinstance(frame, ErrorFrame):
                    # Server errors are already correlated and safe to ignore here;
                    # untrusted details never become client logs.
                    continue
                elif isinstance(frame, (TransferRequest, TransferBegin)) and config_update_tasks:
                    if transfer_manager is None:
                        raise ProtocolError("Transfer arrived before device configuration")
                    if len(config_bound_transfer_tasks) >= MAX_ACTIVE_TRANSFER_SLOTS:
                        transfer_manager.reject_busy_start(frame)
                    else:
                        config_bound_transfer_tasks.append(
                            self._schedule_config_bound_transfer(
                                transfer_manager,
                                frame,
                                prepared=config_update_tasks[-1],
                                previous=config_bound_transfer_tasks[-1]
                                if config_bound_transfer_tasks
                                else None,
                            )
                        )
                elif isinstance(
                    frame,
                    (
                        TransferRequest,
                        TransferBegin,
                        TransferReady,
                        TransferProgress,
                        TransferEnd,
                    ),
                ):
                    if transfer_manager is None:
                        raise ProtocolError("Transfer arrived before device configuration")
                    await transfer_manager.handle_control(frame)
                else:
                    raise ProtocolError("Unexpected server frame")
            return CloseDisposition.SHUTDOWN
        except (ProtocolError, WriterOverflowError):
            if not self._stopping.is_set():
                try:
                    error = ErrorFrame(
                        code="protocol_invalid_frame", message="Invalid protocol frame"
                    )
                    writer.enqueue_critical(encode_frame(error))
                    await asyncio.wait_for(writer.drain(), _SHUTDOWN_GRACE_SECONDS)
                except (WriterOverflowError, TimeoutError):
                    pass
                await websocket.close(1002, "protocol_error")
                if not acknowledged:
                    # A peer that cannot complete the strict v3 handshake will
                    # not become compatible by reconnecting with the same
                    # binary/configuration.  Let run() classify this as a
                    # permanent protocol error (exit 78).
                    raise
                return CloseDisposition.RETRY
            shutdown_requested = True
            return CloseDisposition.SHUTDOWN
        finally:
            if mcp_attached:
                self._mcp_supervisor.detach_connection()
            if registration_signal_task is not None:
                registration_signal_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await registration_signal_task
            for validation_task in validation_tasks:
                if not validation_task.done():
                    validation_task.cancel()
            for control_task in mcp_control_tasks:
                if not control_task.done():
                    control_task.cancel()
            if validation_tasks or mcp_control_tasks:
                await asyncio.gather(
                    *validation_tasks,
                    *mcp_control_tasks,
                    return_exceptions=True,
                )
            for config_update_task in config_update_tasks:
                if not config_update_task.done():
                    config_update_task.cancel()
            for transfer_task in config_bound_transfer_tasks:
                if not transfer_task.done():
                    transfer_task.cancel()
            if config_bound_transfer_tasks:
                await asyncio.gather(*config_bound_transfer_tasks, return_exceptions=True)
            if self._config_generation == config_generation:
                self._config_generation += 1
            shutdown_requested = shutdown_requested or self._stopping.is_set()
            stopping_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stopping_task
            if receive_task is not None:
                receive_task.cancel()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass
            worker_failure_task.cancel()
            with contextlib.suppress(BaseException):
                await worker_failure_task
            if transfer_failure_task is not None:
                transfer_failure_task.cancel()
                with contextlib.suppress(BaseException):
                    await transfer_failure_task
            transfer_shutdown_task: asyncio.Task[None] | None = None
            if transfer_manager is not None:
                transfer_shutdown_task = asyncio.create_task(
                    transfer_manager.shutdown(
                        code="tool_device_unreachable"
                        if shutdown_requested
                        else "peer_disconnected"
                    )
                )
            self._transfer_manager = None
            if transfer_shutdown_task is not None:
                # Let shutdown enqueue terminal transfer frames before the
                # writer begins its own stop sequence.
                await asyncio.sleep(0)
            await _stop_writer(writer, writer_task)
            transfer_stopped = True
            if transfer_shutdown_task is not None:
                transfer_stopped = await _wait_task_bounded(
                    transfer_shutdown_task, _SHUTDOWN_GRACE_SECONDS
                )
            worker_stopped = await worker.stop(
                timeout=_SHUTDOWN_GRACE_SECONDS if shutdown_requested else None
            )
            config_stopped = await self._wait_for_config_tasks(
                timeout=_SHUTDOWN_GRACE_SECONDS if shutdown_requested else None
            )
            if shutdown_requested and (
                not transfer_stopped or not worker_stopped or not config_stopped
            ):
                self._shutdown_cleanup_incomplete = True
            if shutdown_requested:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(writer.drain(), _SHUTDOWN_GRACE_SECONDS)
            if shutdown_requested:
                with contextlib.suppress(Exception):
                    await websocket.close(1001, "shutdown")
            if not self._shutdown_cleanup_incomplete:
                self._cancel_shutdown_watchdog()

    def _default_dispatcher(
        self, workspace: Path, restrict_to_workspace: bool, denylist: list[str]
    ) -> LocalToolDispatcher:
        return ClientToolDispatcher(
            workspace,
            restrict_to_workspace=restrict_to_workspace,
            ssrf_denylist=denylist,
            path_locks=self._path_locks,
        )

    def _new_hello(self) -> Hello:
        system = platform.system().lower()
        if system == "darwin":
            operating_system = "darwin"
        elif system == "windows":
            operating_system = "windows"
        else:
            operating_system = "linux"
        return Hello.new(
            client_version=__version__,
            operating_system=cast(Literal["linux", "darwin", "windows"], operating_system),
            shells=ShellMetadata(
                default=self._shell_inventory.default,
                available=list(self._shell_inventory.available),
            ),
        )

    async def _install_config(
        self,
        device_name: str,
        config: DeviceConfig,
        *,
        generation: int | None = None,
    ) -> ToolDispatcher:
        expected_generation = generation
        task = asyncio.create_task(
            asyncio.to_thread(
                _prepare_config_candidate,
                self._tool_dispatcher_factory,
                device_name,
                config,
            )
        )
        self._track_config_task(task)
        try:
            candidate = await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise _RetryableConfigError(
                "Device workspace configuration could not be prepared"
            ) from None
        if expected_generation is not None and expected_generation != self._config_generation:
            raise asyncio.CancelledError
        prepared = await self._bind_exec_policy(
            candidate,
            expected_generation=expected_generation,
        )
        return self._activate_prepared_config(prepared).dispatcher

    def _schedule_config_update(
        self,
        device_name: str,
        config: DeviceConfig,
        *,
        generation: int,
        previous: asyncio.Task[_PreparedConfig] | None,
        transfer_manager: TransferManager | None,
    ) -> asyncio.Task[_PreparedConfig]:
        async def install() -> _PreparedConfig:
            if previous is not None:
                await asyncio.shield(previous)
            prepare_task = asyncio.create_task(
                asyncio.to_thread(
                    _prepare_config_candidate,
                    self._tool_dispatcher_factory,
                    device_name,
                    config,
                )
            )
            self._track_config_task(prepare_task)
            try:
                candidate = await asyncio.shield(prepare_task)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise _RetryableConfigError(
                    "Device workspace configuration could not be prepared"
                ) from None
            if generation != self._config_generation:
                raise asyncio.CancelledError
            prepared = await self._bind_exec_policy(
                candidate,
                expected_generation=generation,
            )
            self._activate_prepared_config(prepared)
            if transfer_manager is not None:
                transfer_manager.update_config(_transfer_snapshot(prepared))
            return prepared

        task = asyncio.create_task(install())
        self._track_config_task(task)
        return task

    @staticmethod
    def _schedule_config_bound_transfer(
        transfer_manager: TransferManager,
        frame: TransferRequest | TransferBegin,
        *,
        prepared: asyncio.Task[_PreparedConfig],
        previous: asyncio.Task[None] | None,
    ) -> asyncio.Task[None]:
        async def dispatch() -> None:
            if previous is not None:
                await asyncio.shield(previous)
            installed = await asyncio.shield(prepared)
            await transfer_manager.handle_control(
                frame,
                start_snapshot=_transfer_snapshot(installed),
            )

        return asyncio.create_task(dispatch())

    def _activate_prepared_config(self, prepared: _PreparedConfig) -> _PreparedConfig:
        self._active_config = prepared.config.model_copy(
            update={"workspace_path": str(prepared.workspace)}
        )
        self._device_name = prepared.device_name
        self._tools = prepared.dispatcher
        return prepared

    async def _bind_exec_policy(
        self,
        candidate: _PreparedConfigCandidate,
        *,
        expected_generation: int | None,
    ) -> _PreparedConfig:
        key = (
            candidate.workspace,
            candidate.config.restrict_to_workspace,
            candidate.config.shell_timeout_max,
            tuple(candidate.config.env_allowlist),
            self._shell_inventory.available,
            self._shell_inventory.default,
        )
        async with self._exec_policy_lock:
            if expected_generation is not None and expected_generation != self._config_generation:
                raise asyncio.CancelledError
            if key != self._exec_policy_key:
                self._exec_policy_epoch += 1
                policy = ExecPolicy(
                    workspace=candidate.workspace,
                    restrict_to_workspace=candidate.config.restrict_to_workspace,
                    shell_timeout_max=candidate.config.shell_timeout_max,
                    env_allowlist=tuple(candidate.config.env_allowlist),
                    available_shells=self._shell_inventory.available,
                    default_shell=self._shell_inventory.default,
                    epoch=self._exec_policy_epoch,
                )
                apply_task = asyncio.create_task(self._exec_sessions.apply_policy(policy))
                cancelled = False
                try:
                    while True:
                        try:
                            await asyncio.shield(apply_task)
                            break
                        except asyncio.CancelledError:
                            cancelled = True
                            if apply_task.done():
                                apply_task.result()
                except RuntimeError:
                    if cancelled:
                        raise asyncio.CancelledError
                    raise _RetryableConfigError(
                        "Exec policy cleanup could not be completed"
                    ) from None
                self._exec_policy_key = key
                self._exec_policy = policy
                if cancelled:
                    raise asyncio.CancelledError
            else:
                assert self._exec_policy is not None
                policy = self._exec_policy
        return _PreparedConfig(
            workspace=candidate.workspace,
            dispatcher=_DeviceToolDispatcher(
                candidate.dispatcher,
                ExecToolDispatcher(self._exec_sessions, policy),
            ),
            config=candidate.config,
            device_name=candidate.device_name,
        )

    async def _shutdown_exec_sessions(self) -> None:
        if self._exec_shutdown:
            return
        self._exec_shutdown = True
        self._arm_shutdown_watchdog()
        task = asyncio.create_task(self._exec_sessions.shutdown())
        try:
            complete = await asyncio.wait_for(asyncio.shield(task), _SHUTDOWN_WATCHDOG_SECONDS)
            if not complete:
                self._shutdown_cleanup_incomplete = True
        except TimeoutError:
            self._shutdown_cleanup_incomplete = True
            task.cancel()
            task.add_done_callback(_consume_task_result)
        except Exception:
            self._shutdown_cleanup_incomplete = True

    async def _shutdown_mcp(self) -> None:
        if self._mcp_shutdown:
            return
        self._mcp_shutdown = True
        pending = [task for task in self._mcp_invocation_tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        try:
            await self._mcp_supervisor.shutdown()
        except Exception:
            self._shutdown_cleanup_incomplete = True

    def _track_config_task(self, task: asyncio.Task[Any]) -> None:
        self._config_tasks.add(task)
        task.add_done_callback(self._config_task_done)

    def _config_task_done(self, task: asyncio.Task[Any]) -> None:
        self._config_tasks.discard(task)
        if not task.cancelled():
            with contextlib.suppress(BaseException):
                task.exception()

    async def _wait_for_config_tasks(self, *, timeout: float | None) -> bool:
        pending = {task for task in self._config_tasks if not task.done()}
        if not pending:
            return True
        if timeout is None:
            await asyncio.wait(pending)
            return True
        _, remaining = await asyncio.wait(pending, timeout=timeout)
        return not remaining

    async def _run_tool(
        self, call: ToolCall, dispatcher: _QueuedToolDispatcher | None = None
    ) -> ToolResult:
        active_dispatcher = dispatcher or self._tools
        if active_dispatcher is None:
            raise ProtocolError("Tool call arrived before device configuration")
        execute_call = getattr(active_dispatcher, "execute_call", None)
        if callable(execute_call):
            output = await execute_call(call)
        else:
            execute = getattr(active_dispatcher, "execute", None)
            if not callable(execute):
                raise ProtocolError("Tool dispatcher is unavailable")
            output = await execute(call.name, call.args)
        return self._result_with_credit(call, output)

    def _start_mcp_tool(
        self,
        call: ToolCall,
        invocation: McpInvocationLease,
    ) -> asyncio.Task[ToolResult]:
        task = asyncio.create_task(
            self._run_reserved_mcp_tool(call, invocation),
            name=f"mcp-tool-{call.id}",
        )
        self._mcp_invocation_tasks.add(task)
        task.add_done_callback(lambda completed: self._mcp_tool_done(completed, invocation))
        return task

    def _mcp_tool_done(
        self,
        task: asyncio.Task[ToolResult],
        invocation: McpInvocationLease,
    ) -> None:
        invocation.release()
        self._mcp_invocation_tasks.discard(task)
        if not task.cancelled():
            with contextlib.suppress(BaseException):
                task.exception()

    async def _run_reserved_mcp_tool(
        self,
        call: ToolCall,
        invocation: McpInvocationLease,
    ) -> ToolResult:
        return self._result_with_credit(call, await invocation.invoke())

    @staticmethod
    def _result_with_credit(call: ToolCall, output: ToolOutput) -> ToolResult:
        result = ToolResult(
            id=call.id,
            content=cast(Any, output.content),
            is_error=output.is_error,
            code=output.code,
        )
        if len(encode_frame(result).encode("utf-8")) <= call.max_result_bytes:
            return result
        reduced = ToolResult(
            id=call.id,
            content="[tool_result_too_large] Tool result exceeded its response credit",
            is_error=True,
            code="tool_result_too_large",
        )
        if len(encode_frame(reduced).encode("utf-8")) > call.max_result_bytes:
            raise ProtocolError("Tool result credit cannot carry an error result")
        return reduced

    @staticmethod
    def _busy_tool_result(call: ToolCall) -> ToolResult:
        return ClientRuntime._result_with_credit(
            call,
            ToolOutput(
                content="[tool_device_busy] Device tool queue is full; try again",
                is_error=True,
                code="tool_device_busy",
            ),
        )


def _prepare_workspace(value: str) -> Path:
    if "\x00" in value:
        raise ProtocolError("Workspace path is invalid")
    if value == "~":
        workspace = Path.home()
    elif value.startswith("~/") or value.startswith("~\\"):
        workspace = Path.home() / value[2:]
    else:
        workspace = Path(value)
    try:
        if workspace.exists() or workspace.is_symlink():
            mode = os.lstat(workspace).st_mode
            attributes = getattr(os.lstat(workspace), "st_file_attributes", 0)
            reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
            if stat.S_ISLNK(mode) or attributes & reparse_flag:
                raise ProtocolError("Workspace path is unavailable")
        workspace.mkdir(parents=True, exist_ok=True)
        if not workspace.is_dir():
            raise ProtocolError("Workspace path is unavailable")
    except OSError as exc:
        raise ProtocolError("Workspace path is unavailable") from exc
    return workspace


def _prepare_config_candidate(
    factory: Callable[[Path, bool, list[str]], LocalToolDispatcher],
    device_name: str,
    config: DeviceConfig,
) -> _PreparedConfigCandidate:
    workspace = _prepare_workspace(config.workspace_path)
    dispatcher = factory(
        workspace,
        config.restrict_to_workspace,
        config.ssrf_denylist,
    )
    return _PreparedConfigCandidate(
        workspace=workspace,
        dispatcher=dispatcher,
        config=config,
        device_name=device_name,
    )


def _transfer_snapshot(prepared: _PreparedConfig) -> TransferConfigSnapshot:
    return TransferConfigSnapshot.from_values(
        prepared.workspace,
        restrict_to_workspace=prepared.config.restrict_to_workspace,
        ssrf_denylist=prepared.config.ssrf_denylist,
        device_name=prepared.device_name,
    )


async def _wait_for_worker_failure(failure: asyncio.Future[None]) -> str | bytes | None:
    await failure
    raise AssertionError("Tool worker finished without a failure")


async def _wait_for_transfer_failure(failure: asyncio.Future[None]) -> str | bytes | None:
    await failure
    raise AssertionError("Transfer manager finished without a failure")


def _dispatcher_has_pending_blocking(dispatcher: object) -> bool:
    checker = getattr(dispatcher, "has_pending_blocking", None)
    return bool(checker()) if callable(checker) else False


async def _wait_for_dispatcher_blocking(dispatcher: object) -> None:
    waiter = getattr(dispatcher, "wait_for_pending_blocking", None)
    if callable(waiter):
        await waiter()


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return
    with contextlib.suppress(BaseException):
        task.exception()


async def _wait_task_bounded(task: asyncio.Task[Any], timeout: float) -> bool:
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if task in done:
        _consume_task_result(task)
        return True
    task.add_done_callback(_consume_task_result)
    task.cancel()
    return False


async def _stop_writer(writer: SerializedWriter, writer_task: asyncio.Task[None]) -> None:
    """Bound shutdown even when the socket's send call never returns."""

    stop_task = asyncio.create_task(writer.stop())
    try:
        await asyncio.wait_for(asyncio.shield(stop_task), _SHUTDOWN_GRACE_SECONDS)
    except TimeoutError:
        stop_task.cancel()
    writer_task.cancel()
    pending = {task for task in (stop_task, writer_task) if not task.done()}
    if pending:
        done, pending = await asyncio.wait(pending, timeout=_SHUTDOWN_GRACE_SECONDS)
        for task in done:
            _consume_task_result(task)
        for task in pending:
            task.add_done_callback(_consume_task_result)
            task.cancel()


def reconnect_disposition(
    *, http_status: int | None = None, close_code: int | None = None
) -> ReconnectDisposition:
    if http_status is not None:
        if http_status in {401, 403}:
            return ReconnectDisposition.PERMANENT_AUTH
        if http_status == 429 or http_status >= 500:
            return ReconnectDisposition.RETRY
        return ReconnectDisposition.PERMANENT_CONFIG
    if close_code is not None:
        if close_code == 4401:
            return ReconnectDisposition.PERMANENT_AUTH
        if close_code == 4000:
            return ReconnectDisposition.PERMANENT_REPLACED
        if close_code == 4409 or close_code not in _RETRYABLE_CLOSE_CODES:
            return ReconnectDisposition.PERMANENT_CONFIG
        return ReconnectDisposition.RETRY
    return ReconnectDisposition.RETRY


def reconnect_disposition_from_exception(exc: BaseException) -> ReconnectDisposition:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if not isinstance(status, int):
        status = getattr(exc, "status_code", None)
    close_code = getattr(exc, "code", None)
    if not isinstance(close_code, int):
        for attribute in ("rcvd", "sent"):
            close_code = getattr(getattr(exc, attribute, None), "code", None)
            if isinstance(close_code, int):
                break
    if isinstance(status, int) or isinstance(close_code, int):
        return reconnect_disposition(
            http_status=status if isinstance(status, int) else None,
            close_code=close_code if isinstance(close_code, int) else None,
        )
    if isinstance(exc, (OSError, _RetryableConfigError)):
        return ReconnectDisposition.RETRY
    return ReconnectDisposition.PERMANENT_CONFIG


def retry_after_from_exception(exc: BaseException, *, now: datetime | None = None) -> float | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if not isinstance(value, str):
        return None
    try:
        return min(30.0, max(0.0, float(value)))
    except ValueError:
        try:
            deadline = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        current = now or datetime.now(UTC)
        return min(30.0, max(0.0, (deadline - current).total_seconds()))


def _sanitized_close_detail(exc: BaseException) -> str | None:
    """Return a bounded, control-free 4409 detail without echoing raw bodies."""

    reason: object | None = getattr(exc, "reason", None)
    if reason is None:
        for attribute in ("rcvd", "sent"):
            close = getattr(exc, attribute, None)
            reason = getattr(close, "reason", None)
            if reason is not None:
                break
    if not isinstance(reason, str):
        return None
    cleaned = "".join(" " if ord(char) < 32 or ord(char) == 127 else char for char in reason)
    cleaned = " ".join(cleaned.split())
    return cleaned[:256] or None


def reconnect_delay(
    attempt: int, *, retry_after: float | None = None, random_value: float
) -> float:
    if retry_after is not None:
        return min(30.0, max(0.0, retry_after))
    base = min(30.0, float(2 ** min(max(attempt, 0), 5)))
    jitter = 0.8 + (0.4 * min(1.0, max(0.0, random_value)))
    return base * jitter
