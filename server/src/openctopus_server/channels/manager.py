from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.db.models import DingTalkConfig, DiscordConfig

from .adapters.base import ChannelAdapter, ChannelEventSink
from .types import ChannelEvent, ExternalChannel

CONFIG_PAGE_SIZE = 100
STARTUP_CONCURRENCY = 32
STOP_TIMEOUT_SECONDS = 10.0

type ChannelState = Literal[
    "connecting",
    "awaiting_pairing",
    "ready",
    "degraded",
    "stopped",
]
type PersistedChannelConfig = DiscordConfig | DingTalkConfig
type RuntimeKey = tuple[UUID, ExternalChannel]


@dataclass(frozen=True, slots=True)
class SanitizedChannelError:
    code: str
    message: str
    at: datetime


@dataclass(frozen=True, slots=True)
class ChannelRuntimeSnapshot:
    user_id: UUID
    channel: ExternalChannel
    binding_generation: UUID
    config_revision: int
    runtime_generation: UUID
    state: ChannelState
    last_error: SanitizedChannelError | None


class ChannelAdapterFactory(Protocol):
    def __call__(
        self,
        config: PersistedChannelConfig,
        runtime_generation: UUID,
    ) -> ChannelAdapter: ...


class RoutedChannelEventSink(Protocol):
    async def __call__(
        self,
        user_id: UUID,
        event: ChannelEvent,
    ) -> object | None: ...


class ChannelManagerClock(Protocol):
    def now(self) -> datetime: ...

    async def sleep(self, delay: float) -> None: ...


class ReadyRecovery(Protocol):
    async def __call__(
        self,
        user_id: UUID,
        channel: ExternalChannel,
        binding_generation: UUID,
    ) -> None: ...


class _EventLoopClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, delay: float) -> None:
        await asyncio.sleep(delay)


@dataclass(frozen=True, slots=True)
class _LoadedConfig:
    model: PersistedChannelConfig
    user_id: UUID
    channel: ExternalChannel
    binding_generation: UUID
    revision: int
    paired: bool
    connection_fingerprint: tuple[str, ...]


@dataclass(slots=True)
class ChannelRuntimeEntry:
    config: _LoadedConfig
    runtime_generation: UUID
    state: ChannelState
    adapter: ChannelAdapter | None
    reconnect_attempt: int
    reconnect_enabled: bool = True
    online: bool = False
    last_error: SanitizedChannelError | None = None
    initialized: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    stop_task: asyncio.Task[None] | None = None

    @property
    def key(self) -> RuntimeKey:
        return self.config.user_id, self.config.channel


@dataclass(slots=True)
class _MutationLock:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0


class ChannelManager:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        adapter_factory: ChannelAdapterFactory,
        event_sink: RoutedChannelEventSink,
        clock: ChannelManagerClock | None = None,
        random_source: Callable[[], float] | None = None,
        startup_concurrency: int = STARTUP_CONCURRENCY,
        stop_timeout_seconds: float = STOP_TIMEOUT_SECONDS,
        ready_recovery: ReadyRecovery | None = None,
    ) -> None:
        if not 1 <= startup_concurrency <= STARTUP_CONCURRENCY:
            raise ValueError("Channel startup concurrency must be between 1 and 32")
        if stop_timeout_seconds <= 0:
            raise ValueError("Channel stop timeout must be positive")
        self._engine = engine
        self._adapter_factory = adapter_factory
        self._event_sink = event_sink
        self._clock = clock or _EventLoopClock()
        self._random_source = random_source or random.random
        self._startup_admission = asyncio.Semaphore(startup_concurrency)
        self._stop_timeout_seconds = stop_timeout_seconds
        self._ready_recovery = ready_recovery
        self._entries: dict[RuntimeKey, ChannelRuntimeEntry] = {}
        self._mutation_locks: dict[RuntimeKey, _MutationLock] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._restored_bindings: set[tuple[UUID, ExternalChannel, UUID]] = set()
        self._restoring_bindings: set[tuple[UUID, ExternalChannel, UUID]] = set()
        self._accept_inbound = True
        self._allow_reconnect = True
        self._closing = False
        self._startup_task: asyncio.Task[None] | None = None
        self._begin_shutdown_task: asyncio.Task[None] | None = None
        self._shutdown_task: asyncio.Task[None] | None = None

    @property
    def background_task_count(self) -> int:
        return sum(not task.done() for task in self._tasks)

    def status(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> ChannelRuntimeSnapshot | None:
        entry = self._entries.get((user_id, channel))
        if entry is None:
            return None
        return ChannelRuntimeSnapshot(
            user_id=user_id,
            channel=channel,
            binding_generation=entry.config.binding_generation,
            config_revision=entry.config.revision,
            runtime_generation=entry.runtime_generation,
            state=entry.state,
            last_error=entry.last_error,
        )

    def adapter_lookup(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> ChannelAdapter | None:
        entry = self._entries.get((user_id, channel))
        if entry is None or not entry.online:
            return None
        return entry.adapter

    async def is_current_runtime(
        self,
        *,
        user_id: UUID,
        platform: ExternalChannel,
        binding_generation: UUID,
        runtime_generation: UUID,
    ) -> bool:
        entry = self._entries.get((user_id, platform))
        return bool(
            self._accept_inbound
            and entry is not None
            and entry.online
            and entry.config.binding_generation == binding_generation
            and entry.runtime_generation == runtime_generation
        )

    async def startup(self) -> None:
        if self._startup_task is None:
            self._startup_task = asyncio.create_task(
                self._startup(), name="channel-manager-startup"
            )
        await await_future_cancellation_safe(self._startup_task)

    async def apply(self, user_id: UUID, channel: ExternalChannel) -> None:
        key = (user_id, channel)
        async with self._mutate(key):
            config = await self._load_config(user_id, channel)
            if config is None:
                removed = self._remove_current_locked(key)
                activation = None
            else:
                removed = None
                activation = self._apply_loaded_locked(config)
        if removed is not None:
            await self._cleanup_removed(*removed)
        if activation is not None:
            await self._finish_activation(*activation)

    async def remove(self, user_id: UUID, channel: ExternalChannel) -> None:
        key = (user_id, channel)
        async with self._mutate(key):
            removed = self._remove_current_locked(key)
        if removed is None:
            return
        await self._cleanup_removed(*removed)

    async def mark_degraded(
        self,
        user_id: UUID,
        channel: ExternalChannel,
        *,
        binding_generation: UUID,
        code: str,
        message: str,
    ) -> None:
        key = (user_id, channel)
        async with self._mutate(key):
            entry = self._entries.get(key)
            if (
                entry is None
                or entry.config.binding_generation != binding_generation
                or not entry.online
            ):
                return
            entry.state = "degraded"
            entry.last_error = self._error(code, message)

    async def _cleanup_removed(
        self,
        entry: ChannelRuntimeEntry,
        stop_task: asyncio.Task[None] | None,
    ) -> None:
        cleanup_task = self._spawn(
            self._finish_stop(entry, stop_task),
            name=f"channel-cleanup-{entry.config.channel}",
        )
        await await_future_cancellation_safe(cleanup_task)

    async def begin_shutdown(self) -> None:
        if self._begin_shutdown_task is None:
            self._begin_shutdown_task = asyncio.create_task(
                self._begin_shutdown(), name="channel-manager-begin-shutdown"
            )
        await await_future_cancellation_safe(self._begin_shutdown_task)

    async def shutdown(self) -> None:
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(
                self._shutdown(), name="channel-manager-shutdown"
            )
        await await_future_cancellation_safe(self._shutdown_task)

    async def _startup(self) -> None:
        await self._startup_channel("discord")
        await self._startup_channel("dingtalk")

    async def _startup_channel(self, channel: ExternalChannel) -> None:
        after: UUID | None = None
        while not self._closing:
            page = await self._load_config_page(channel, after=after)
            if not page:
                return

            async def start_one(config: _LoadedConfig) -> None:
                async with self._startup_admission:
                    await self._apply_loaded(config)

            await asyncio.gather(
                *(start_one(config) for config in page),
                return_exceptions=True,
            )
            after = page[-1].user_id
            if len(page) < CONFIG_PAGE_SIZE:
                return

    async def _apply_loaded(self, config: _LoadedConfig) -> None:
        key = (config.user_id, config.channel)
        async with self._mutate(key):
            initialized, recovery_entry = self._apply_loaded_locked(config)
        await self._finish_activation(initialized, recovery_entry)

    def _apply_loaded_locked(
        self,
        config: _LoadedConfig,
    ) -> tuple[asyncio.Event, ChannelRuntimeEntry | None]:
        if self._closing:
            raise RuntimeError("Channel manager is shutting down")
        current = self._entries.get((config.user_id, config.channel))
        recovery_entry: ChannelRuntimeEntry | None = None
        if current is not None and self._same_transport(current.config, config):
            pairing_changed = current.config.paired != config.paired
            current.config = config
            if current.online and (current.state != "degraded" or pairing_changed):
                current.state = "ready" if config.paired else "awaiting_pairing"
                current.last_error = None
                recovery_entry = current if config.paired else None
            initialized = current.initialized
        else:
            if current is not None:
                current.reconnect_enabled = False
            entry = self._make_entry(config, reconnect_attempt=0)
            self._entries[entry.key] = entry
            self._activate(entry)
            if current is not None:
                stop_task = self._request_stop(current)
                self._spawn(
                    self._finish_stop(current, stop_task),
                    name=f"channel-replacement-cleanup-{current.config.channel}",
                )
            initialized = entry.initialized
        return initialized, recovery_entry

    async def _finish_activation(
        self,
        initialized: asyncio.Event,
        recovery_entry: ChannelRuntimeEntry | None,
    ) -> None:
        await initialized.wait()
        if recovery_entry is not None:
            await self._recover_ready(recovery_entry)

    def _remove_current_locked(
        self,
        key: RuntimeKey,
    ) -> tuple[ChannelRuntimeEntry, asyncio.Task[None] | None] | None:
        entry = self._entries.pop(key, None)
        if entry is None:
            return None
        entry.reconnect_enabled = False
        entry.online = False
        entry.state = "stopped"
        stop_task = self._request_stop(entry)
        self._forget_recovery(key)
        return entry, stop_task

    def _make_entry(
        self,
        config: _LoadedConfig,
        *,
        reconnect_attempt: int,
    ) -> ChannelRuntimeEntry:
        runtime_generation = uuid4()
        try:
            adapter = self._adapter_factory(config.model, runtime_generation)
        except Exception:
            return ChannelRuntimeEntry(
                config=config,
                runtime_generation=runtime_generation,
                state="degraded",
                adapter=None,
                reconnect_attempt=reconnect_attempt,
                last_error=self._error(
                    "channel_runtime_factory_failed",
                    "Channel runtime could not be created.",
                ),
            )
        if adapter.platform != config.channel:
            return ChannelRuntimeEntry(
                config=config,
                runtime_generation=runtime_generation,
                state="degraded",
                adapter=None,
                reconnect_attempt=reconnect_attempt,
                last_error=self._error(
                    "channel_runtime_platform_mismatch",
                    "Channel runtime does not match its configured platform.",
                ),
            )
        return ChannelRuntimeEntry(
            config=config,
            runtime_generation=runtime_generation,
            state="connecting",
            adapter=adapter,
            reconnect_attempt=reconnect_attempt,
        )

    def _activate(self, entry: ChannelRuntimeEntry) -> None:
        if entry.adapter is None:
            entry.initialized.set()
            entry.task = self._spawn(
                self._reconnect_after(entry),
                name="channel-runtime-factory-reconnect",
            )
        else:
            entry.task = self._spawn(
                self._run_entry(entry),
                name=f"channel-runtime-{entry.config.channel}",
            )

    async def _run_entry(self, entry: ChannelRuntimeEntry) -> None:
        adapter = entry.adapter
        assert adapter is not None
        try:
            await adapter.start(self._fenced_sink(entry))
        except asyncio.CancelledError:
            entry.initialized.set()
            raise
        except Exception:
            await self._mark_start_failed(entry)
            entry.initialized.set()
            await self._reconnect_after(entry)
            return

        async with self._mutate(entry.key):
            current = self._entries.get(entry.key)
            if current is entry and not self._closing:
                entry.online = True
                entry.reconnect_attempt = 0
                entry.state = "ready" if entry.config.paired else "awaiting_pairing"
                entry.last_error = None
                became_current = True
            else:
                became_current = False
        if not became_current:
            entry.initialized.set()
            self._request_stop(entry)
            return

        recovery_succeeded = True
        try:
            if entry.config.paired:
                recovery_succeeded = await self._recover_ready(entry)
        finally:
            entry.initialized.set()
        if not recovery_succeeded:
            await adapter.wait_closed()
            return
        async with self._mutate(entry.key):
            if self._entries.get(entry.key) is not entry or not entry.online:
                return
        try:
            await adapter.wait_closed()
        except asyncio.CancelledError:
            raise
        except Exception:
            error = self._error(
                "channel_runtime_exited",
                "Channel runtime exited unexpectedly.",
            )
        else:
            error = self._error(
                "channel_runtime_closed",
                "Channel runtime closed unexpectedly.",
            )
        await self._mark_closed(entry, error)
        await self._reconnect_after(entry)

    async def _mark_start_failed(self, entry: ChannelRuntimeEntry) -> None:
        async with self._mutate(entry.key):
            if self._entries.get(entry.key) is entry:
                entry.online = False
                entry.state = "degraded"
                entry.last_error = self._error(
                    "channel_runtime_start_failed",
                    "Channel runtime could not connect.",
                )

    async def _mark_closed(
        self,
        entry: ChannelRuntimeEntry,
        error: SanitizedChannelError,
    ) -> None:
        async with self._mutate(entry.key):
            if self._entries.get(entry.key) is entry:
                entry.online = False
                entry.state = "degraded"
                entry.last_error = error

    async def _reconnect_after(self, entry: ChannelRuntimeEntry) -> None:
        async with self._mutate(entry.key):
            if not self._can_reconnect(entry):
                return
        delay = _full_jitter_delay(entry.reconnect_attempt, self._random_source())
        try:
            await self._clock.sleep(delay)
        except asyncio.CancelledError:
            raise
        async with self._mutate(entry.key):
            if not self._can_reconnect(entry):
                return
            replacement = self._make_entry(
                entry.config,
                reconnect_attempt=entry.reconnect_attempt + 1,
            )
            entry.reconnect_enabled = False
            self._entries[entry.key] = replacement
            self._activate(replacement)
            self._request_stop(entry)

    def _fenced_sink(self, entry: ChannelRuntimeEntry) -> ChannelEventSink:
        async def accept(event: ChannelEvent) -> object | None:
            if not await self.is_current_runtime(
                user_id=entry.config.user_id,
                platform=entry.config.channel,
                binding_generation=event.binding_generation,
                runtime_generation=event.runtime_generation,
            ):
                return None
            if event.platform != entry.config.channel:
                return None
            return await self._event_sink(entry.config.user_id, event)

        return accept

    async def _recover_ready(self, entry: ChannelRuntimeEntry) -> bool:
        if self._ready_recovery is None or not entry.config.paired:
            return True
        token = (*entry.key, entry.config.binding_generation)
        async with self._mutate(entry.key):
            current = self._entries.get(entry.key) is entry and entry.online
            if not current:
                return False
            if token in self._restored_bindings or token in self._restoring_bindings:
                return True
            self._restoring_bindings.add(token)
        try:
            await self._ready_recovery(*token)
        except asyncio.CancelledError:
            async with self._mutate(entry.key):
                self._restoring_bindings.discard(token)
            raise
        except Exception:
            should_stop = False
            async with self._mutate(entry.key):
                self._restoring_bindings.discard(token)
                if self._entries.get(entry.key) is entry:
                    entry.online = False
                    entry.state = "degraded"
                    entry.last_error = self._error(
                        "channel_pending_recovery_failed",
                        "Channel pending recovery could not complete.",
                    )
                    should_stop = True
            if should_stop:
                self._spawn(
                    self._stop_then_reconnect(entry),
                    name=f"channel-recovery-cleanup-{entry.config.channel}",
                )
            return False
        else:
            async with self._mutate(entry.key):
                self._restoring_bindings.discard(token)
                current = self._entries.get(entry.key) is entry and entry.online
                if current:
                    self._restored_bindings.add(token)
                return current

    async def _stop_then_reconnect(self, entry: ChannelRuntimeEntry) -> None:
        stop_task = self._request_stop(entry)
        await self._finish_stop(entry, stop_task)
        current = asyncio.current_task()
        assert current is not None
        entry.task = current
        await self._reconnect_after(entry)

    def _request_stop(self, entry: ChannelRuntimeEntry) -> asyncio.Task[None] | None:
        if entry.adapter is None:
            return None
        if entry.stop_task is None:
            entry.stop_task = self._spawn(
                entry.adapter.stop(),
                name=f"channel-stop-{entry.config.channel}",
            )
        return entry.stop_task

    async def _finish_stop(
        self,
        entry: ChannelRuntimeEntry,
        stop_task: asyncio.Task[None] | None,
    ) -> None:
        if stop_task is not None and not stop_task.cancelled():
            try:
                async with asyncio.timeout(self._stop_timeout_seconds):
                    await asyncio.shield(stop_task)
            except TimeoutError:
                entry.last_error = self._error(
                    "channel_runtime_stop_timeout",
                    "Channel runtime did not stop before the cleanup deadline.",
                )
            except Exception:
                entry.last_error = self._error(
                    "channel_runtime_stop_failed",
                    "Channel runtime cleanup failed.",
                )
        pending: list[asyncio.Task[None]] = []
        if stop_task is not None and not stop_task.done():
            stop_task.cancel()
            pending.append(stop_task)
        task = entry.task
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
            if task is not stop_task:
                pending.append(task)
        if pending:
            await asyncio.wait(
                pending,
                timeout=self._stop_timeout_seconds,
            )

    async def _begin_shutdown(self) -> None:
        self._accept_inbound = False
        self._allow_reconnect = False
        self._closing = True
        for key in tuple(self._entries):
            async with self._mutate(key):
                entry = self._entries.get(key)
                if entry is not None:
                    entry.reconnect_enabled = False

    async def _shutdown(self) -> None:
        await self.begin_shutdown()
        if self._startup_task is not None and not self._startup_task.done():
            self._startup_task.cancel()
            await asyncio.gather(self._startup_task, return_exceptions=True)

        entries: list[ChannelRuntimeEntry] = []
        for key in tuple(self._entries):
            async with self._mutate(key):
                entry = self._entries.pop(key, None)
                if entry is not None:
                    entry.reconnect_enabled = False
                    entry.online = False
                    entry.state = "stopped"
                    self._request_stop(entry)
                    entries.append(entry)
                    self._forget_recovery(key)
        await asyncio.gather(
            *(self._finish_stop(entry, entry.stop_task) for entry in entries),
            return_exceptions=True,
        )

        current = asyncio.current_task()
        remaining = tuple(task for task in self._tasks if task is not current and not task.done())
        for task in remaining:
            task.cancel()
        if remaining:
            await asyncio.wait(
                remaining,
                timeout=self._stop_timeout_seconds,
            )

    async def _load_config(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> _LoadedConfig | None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            model: PersistedChannelConfig | None
            if channel == "discord":
                model = await db.get(DiscordConfig, user_id)
            else:
                model = await db.get(DingTalkConfig, user_id)
        return _loaded_config(model, channel) if model is not None else None

    async def _load_config_page(
        self,
        channel: ExternalChannel,
        *,
        after: UUID | None,
    ) -> list[_LoadedConfig]:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            if channel == "discord":
                statement = (
                    select(DiscordConfig).order_by(DiscordConfig.user_id).limit(CONFIG_PAGE_SIZE)
                )
                if after is not None:
                    statement = statement.where(DiscordConfig.user_id > after)
                models: list[PersistedChannelConfig] = list((await db.scalars(statement)).all())
            else:
                ding_statement = (
                    select(DingTalkConfig).order_by(DingTalkConfig.user_id).limit(CONFIG_PAGE_SIZE)
                )
                if after is not None:
                    ding_statement = ding_statement.where(DingTalkConfig.user_id > after)
                models = list((await db.scalars(ding_statement)).all())
        return [_loaded_config(model, channel) for model in models]

    @asynccontextmanager
    async def _mutate(self, key: RuntimeKey) -> AsyncIterator[None]:
        lock = self._mutation_locks.get(key)
        if lock is None:
            lock = _MutationLock()
            self._mutation_locks[key] = lock
        lock.users += 1
        try:
            async with lock.lock:
                yield
        finally:
            lock.users -= 1
            if lock.users == 0 and key not in self._entries:
                self._mutation_locks.pop(key, None)

    def _spawn(
        self,
        coroutine: Coroutine[Any, Any, None],
        *,
        name: str,
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.add(task)

        def consume(completed: asyncio.Task[None]) -> None:
            self._tasks.discard(completed)
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(consume)
        return task

    def _error(self, code: str, message: str) -> SanitizedChannelError:
        return SanitizedChannelError(code=code, message=message[:512], at=self._clock.now())

    def _forget_recovery(self, key: RuntimeKey) -> None:
        self._restored_bindings = {token for token in self._restored_bindings if token[:2] != key}
        self._restoring_bindings = {token for token in self._restoring_bindings if token[:2] != key}

    @staticmethod
    def _same_transport(first: _LoadedConfig, second: _LoadedConfig) -> bool:
        return (
            first.binding_generation == second.binding_generation
            and first.connection_fingerprint == second.connection_fingerprint
        )

    def _can_reconnect(self, entry: ChannelRuntimeEntry) -> bool:
        return (
            self._entries.get(entry.key) is entry
            and entry.reconnect_enabled
            and self._allow_reconnect
            and not self._closing
        )


def _loaded_config(
    model: PersistedChannelConfig,
    channel: ExternalChannel,
) -> _LoadedConfig:
    if isinstance(model, DiscordConfig):
        fingerprint = (model.bot_token, model.application_id, model.bot_user_id)
    else:
        fingerprint = (model.client_id, model.client_secret, model.bot_user_id)
    return _LoadedConfig(
        model=model,
        user_id=model.user_id,
        channel=channel,
        binding_generation=model.binding_generation,
        revision=model.revision,
        paired=model.owner_platform_user_id is not None,
        connection_fingerprint=fingerprint,
    )


def _full_jitter_delay(attempt: int, random_value: float) -> float:
    if attempt < 0 or not 0 <= random_value <= 1:
        raise ValueError("Reconnect jitter inputs are outside their valid ranges")
    cap = min(60.0, float(2 ** min(attempt, 6)))
    return cap * random_value
