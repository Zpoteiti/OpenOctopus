import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.channels.adapters.base import ChannelEventSink
from openctopus_server.channels.delivery import ActionResult
from openctopus_server.channels.manager import ChannelManager
from openctopus_server.channels.types import (
    ChannelEvent,
    DeliveryAction,
    DeliveryPlan,
    ExternalChannel,
    OutboundMessage,
)
from openctopus_server.db.models import DingTalkConfig, DiscordConfig, User


class _Clock:
    def __init__(self) -> None:
        self.delays: list[float] = []
        self._entered = [asyncio.Event() for _ in range(20)]
        self._release = [asyncio.Event() for _ in range(20)]

    def now(self) -> datetime:
        return datetime(2026, 9, 2, tzinfo=UTC)

    async def sleep(self, delay: float) -> None:
        index = len(self.delays)
        self.delays.append(delay)
        self._entered[index].set()
        await self._release[index].wait()

    async def wait_for_sleep(self, count: int) -> None:
        await self._entered[count - 1].wait()

    def release(self, index: int) -> None:
        self._release[index].set()


class _StartProbe:
    def __init__(self, target: int) -> None:
        self.target = target
        self.current = 0
        self.maximum = 0
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def hold(self) -> None:
        self.current += 1
        self.maximum = max(self.maximum, self.current)
        if self.current == self.target:
            self.reached.set()
        try:
            await self.release.wait()
        finally:
            self.current -= 1


class _Adapter:
    platform = "discord"

    def __init__(
        self,
        *,
        runtime_generation: UUID,
        binding_generation: UUID,
        start_error: Exception | None = None,
        stop_error: Exception | None = None,
        block_start: bool = False,
        block_stop: bool = False,
        resist_stop_cancellation: bool = False,
        close_stop_on_wait_cancel: bool = False,
        start_probe: _StartProbe | None = None,
    ) -> None:
        self.runtime_generation = runtime_generation
        self.binding_generation = binding_generation
        self.start_error = start_error
        self.stop_error = stop_error
        self.resist_stop_cancellation = resist_stop_cancellation
        self.close_stop_on_wait_cancel = close_stop_on_wait_cancel
        self.start_gate = asyncio.Event()
        self.stop_gate = asyncio.Event()
        if not block_start:
            self.start_gate.set()
        if not block_stop:
            self.stop_gate.set()
        self.start_probe = start_probe
        self.start_entered = asyncio.Event()
        self.start_returned = asyncio.Event()
        self.stop_entered = asyncio.Event()
        self.stop_cancelled = asyncio.Event()
        self.wait_finished = asyncio.Event()
        self.closed = asyncio.Event()
        self.stop_calls = 0
        self.sink: ChannelEventSink | None = None

    async def start(self, sink: ChannelEventSink) -> None:
        self.sink = sink
        self.start_entered.set()
        if self.start_probe is not None:
            await self.start_probe.hold()
        await self.start_gate.wait()
        if self.start_error is not None:
            raise self.start_error
        self.start_returned.set()

    async def wait_closed(self) -> None:
        try:
            await self.closed.wait()
        except asyncio.CancelledError:
            if self.close_stop_on_wait_cancel:
                self.stop_gate.set()
            raise
        else:
            self.wait_finished.set()

    async def stop(self) -> None:
        self.stop_calls += 1
        self.stop_entered.set()
        while not self.stop_gate.is_set():
            try:
                await self.stop_gate.wait()
            except asyncio.CancelledError:
                self.stop_cancelled.set()
                if not self.resist_stop_cancellation:
                    raise
        if self.stop_error is not None:
            raise self.stop_error
        self.closed.set()

    def plan_delivery(self, message: OutboundMessage) -> DeliveryPlan:
        return DeliveryPlan(
            actions=(DeliveryAction(kind="text_message", visible=True, content=message.content),)
        )

    async def execute_action(
        self,
        action: DeliveryAction,
        *,
        on_issued: Callable[[], Awaitable[None]],
    ) -> ActionResult:
        await on_issued()
        return ActionResult(status="sent")

    async def emit(self, event: ChannelEvent) -> object | None:
        assert self.sink is not None
        return await self.sink(event)


class _Factory:
    def __init__(
        self,
        builder: Callable[[DiscordConfig | DingTalkConfig, UUID], _Adapter] | None = None,
    ) -> None:
        self.builder = builder
        self.adapters: list[_Adapter] = []
        self._created = [asyncio.Event() for _ in range(256)]

    def __call__(
        self,
        config: DiscordConfig | DingTalkConfig,
        runtime_generation: UUID,
    ) -> _Adapter:
        if self.builder is None:
            adapter = _Adapter(
                runtime_generation=runtime_generation,
                binding_generation=config.binding_generation,
            )
        else:
            adapter = self.builder(config, runtime_generation)
        self.adapters.append(adapter)
        self._created[len(self.adapters) - 1].set()
        return adapter

    async def wait_created(self, count: int) -> _Adapter:
        await self._created[count - 1].wait()
        return self.adapters[count - 1]


async def _discord(
    engine: AsyncEngine,
    *,
    user_id: UUID | None = None,
    token: str = "token-1",
    revision: int = 1,
    binding_generation: UUID | None = None,
    paired: bool = False,
) -> tuple[UUID, UUID]:
    user_id = user_id or uuid4()
    binding_generation = binding_generation or uuid4()
    async with AsyncSession(engine, expire_on_commit=False) as db:
        db.add(
            User(
                id=user_id,
                email=f"{user_id}@test.com",
                password_hash="hash",
                name="Owner",
            )
        )
        await db.flush()
        db.add(
            DiscordConfig(
                user_id=user_id,
                bot_token=token,
                application_id=f"application-{user_id}",
                bot_user_id=f"bot-{user_id}",
                bot_display_name="Bot",
                binding_generation=binding_generation,
                revision=revision,
                owner_platform_user_id="owner" if paired else None,
                owner_dm_chat_id="dm" if paired else None,
                paired_at=datetime.now(UTC) if paired else None,
                allow_list=[],
            )
        )
        await db.commit()
    return user_id, binding_generation


def _event(adapter: _Adapter, *, source_id: str = "message-1") -> ChannelEvent:
    return ChannelEvent(
        platform="discord",
        binding_generation=adapter.binding_generation,
        runtime_generation=adapter.runtime_generation,
        source_message_id=source_id,
        chat_id="chat-1",
        conversation_kind="dm",
        sender_id="owner",
        sender_display_name="Owner",
        sender_kind="human",
        explicitly_mentions_bot=False,
        text="hello",
        attachments=(),
    )


async def _eventually(predicate: Callable[[], bool]) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


async def test_startup_pages_all_configs_limits_parallelism_and_isolates_failure(
    pg_engine: AsyncEngine,
) -> None:
    bad_user = uuid4()
    users = [bad_user, *(uuid4() for _ in range(100))]
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                User(
                    id=user_id,
                    email=f"{user_id}@test.com",
                    password_hash="hash",
                    name="Owner",
                )
                for user_id in users
            ]
        )
        await db.flush()
        db.add_all(
            [
                DiscordConfig(
                    user_id=user_id,
                    bot_token="bad" if user_id == bad_user else f"token-{user_id}",
                    application_id=f"application-{user_id}",
                    bot_user_id=f"bot-{user_id}",
                    bot_display_name="Bot",
                    binding_generation=uuid4(),
                    revision=1,
                    allow_list=[],
                )
                for user_id in users
            ]
        )
        await db.commit()

    probe = _StartProbe(32)

    def build(config: DiscordConfig | DingTalkConfig, generation: UUID) -> _Adapter:
        if isinstance(config, DiscordConfig) and config.bot_token == "bad":
            raise RuntimeError("raw bad credential")
        return _Adapter(
            runtime_generation=generation,
            binding_generation=config.binding_generation,
            start_probe=probe,
        )

    factory = _Factory(build)
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=_Clock(),
        random_source=lambda: 1.0,
    )

    startup = asyncio.create_task(manager.startup())
    await probe.reached.wait()
    assert probe.maximum == 32
    assert len(factory.adapters) == 32
    probe.release.set()
    await startup

    assert len(factory.adapters) == 100
    assert manager.status(bad_user, "discord").state == "degraded"  # type: ignore[union-attr]
    assert sum(manager.status(user_id, "discord") is not None for user_id in users) == 101

    await manager.shutdown()
    assert manager.background_task_count == 0


async def test_apply_refreshes_pairing_without_restart_and_restarts_credentials(
    pg_engine: AsyncEngine,
) -> None:
    user_id, first_binding = await _discord(pg_engine)
    factory = _Factory()
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=_Clock(),
    )

    await manager.apply(user_id, "discord")
    first = factory.adapters[0]
    assert manager.status(user_id, "discord").state == "awaiting_pairing"  # type: ignore[union-attr]

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.revision = 2
        config.allow_list = ["42"]
        config.owner_platform_user_id = "owner"
        config.owner_dm_chat_id = "dm"
        config.paired_at = datetime.now(UTC)
        await db.commit()
    await manager.apply(user_id, "discord")

    assert len(factory.adapters) == 1
    assert manager.adapter_lookup(user_id, "discord") is first
    assert manager.status(user_id, "discord").state == "ready"  # type: ignore[union-attr]

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.revision = 3
        config.bot_token = "token-rotated"
        await db.commit()
    await manager.apply(user_id, "discord")
    second = factory.adapters[1]
    await first.stop_entered.wait()

    assert manager.adapter_lookup(user_id, "discord") is second
    assert first.stop_calls == 1
    assert manager.status(user_id, "discord").binding_generation == first_binding  # type: ignore[union-attr]

    replacement_binding = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.revision = 4
        config.binding_generation = replacement_binding
        config.bot_token = "replacement-token"
        config.bot_user_id = "replacement-bot"
        config.owner_platform_user_id = None
        config.owner_dm_chat_id = None
        config.paired_at = None
        await db.commit()
    await manager.apply(user_id, "discord")
    await second.stop_entered.wait()

    snapshot = manager.status(user_id, "discord")
    assert snapshot is not None
    assert snapshot.binding_generation == replacement_binding
    assert snapshot.state == "awaiting_pairing"
    assert second.stop_calls == 1
    await manager.shutdown()


@pytest.mark.parametrize("stop_mode", ["block", "fail", "resist"])
async def test_hot_replacement_bounds_old_stop_without_runtime_task_leak(
    pg_engine: AsyncEngine,
    stop_mode: str,
) -> None:
    user_id, _ = await _discord(pg_engine, token="token-a")
    created = 0

    def build(config: DiscordConfig | DingTalkConfig, generation: UUID) -> _Adapter:
        nonlocal created
        created += 1
        return _Adapter(
            runtime_generation=generation,
            binding_generation=config.binding_generation,
            block_stop=created == 1 and stop_mode in {"block", "resist"},
            resist_stop_cancellation=created == 1 and stop_mode == "resist",
            close_stop_on_wait_cancel=created == 1 and stop_mode == "resist",
            stop_error=(
                RuntimeError("stop failed")
                if created == 1 and stop_mode == "fail"
                else None
            ),
        )

    factory = _Factory(build)
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=_Clock(),
        stop_timeout_seconds=1e-6,
    )
    await manager.apply(user_id, "discord")
    first = factory.adapters[0]

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.revision = 2
        config.bot_token = "token-b"
        await db.commit()

    try:
        await manager.apply(user_id, "discord")
        second = factory.adapters[1]
        await first.stop_entered.wait()
        await _eventually(lambda: manager.background_task_count == 1)

        assert manager.adapter_lookup(user_id, "discord") is second
        assert first.stop_calls == 1
        if stop_mode in {"block", "resist"}:
            assert first.stop_cancelled.is_set()
    finally:
        first.stop_gate.set()
        await manager.shutdown()
    assert manager.background_task_count == 0


async def test_allow_list_refresh_does_not_clear_runtime_degraded_state(
    pg_engine: AsyncEngine,
) -> None:
    user_id, binding = await _discord(pg_engine, paired=True)
    factory = _Factory()
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=_Clock(),
    )
    await manager.apply(user_id, "discord")
    await manager.mark_degraded(
        user_id,
        "discord",
        binding_generation=binding,
        code="pairing_confirmation_failed",
        message="The pairing confirmation could not be delivered.",
    )

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.revision += 1
        config.allow_list = ["42"]
        await db.commit()
    await manager.apply(user_id, "discord")

    snapshot = manager.status(user_id, "discord")
    assert snapshot is not None
    assert snapshot.state == "degraded"
    assert snapshot.last_error is not None
    assert snapshot.last_error.code == "pairing_confirmation_failed"
    assert manager.adapter_lookup(user_id, "discord") is factory.adapters[0]
    await manager.shutdown()


async def test_late_generation_cannot_publish_or_overwrite_new_entry(
    pg_engine: AsyncEngine,
) -> None:
    user_id, binding = await _discord(pg_engine, token="token-a")

    def build(config: DiscordConfig | DingTalkConfig, generation: UUID) -> _Adapter:
        return _Adapter(
            runtime_generation=generation,
            binding_generation=config.binding_generation,
            block_start=isinstance(config, DiscordConfig) and config.bot_token == "token-a",
        )

    factory = _Factory(build)
    accepted: list[ChannelEvent] = []
    accepted_result = object()

    async def sink(_user_id: UUID, event: ChannelEvent) -> object:
        accepted.append(event)
        return accepted_result

    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=sink,
        clock=_Clock(),
    )
    apply_a = asyncio.create_task(manager.apply(user_id, "discord"))
    first = await factory.wait_created(1)
    await first.start_entered.wait()
    assert manager.adapter_lookup(user_id, "discord") is None

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.revision = 2
        config.bot_token = "token-b"
        await db.commit()
    await manager.apply(user_id, "discord")
    second = factory.adapters[1]
    await first.stop_entered.wait()

    assert await first.emit(_event(first, source_id="stale-before-ready")) is None
    first.start_gate.set()
    await apply_a
    assert await first.emit(_event(first, source_id="stale-after-ready")) is None
    assert await second.emit(_event(second, source_id="current")) is accepted_result

    snapshot = manager.status(user_id, "discord")
    assert snapshot is not None
    assert snapshot.runtime_generation == second.runtime_generation
    assert snapshot.binding_generation == binding
    assert snapshot.state == "awaiting_pairing"
    assert [event.source_message_id for event in accepted] == ["current"]
    assert first.stop_calls == 1
    await manager.shutdown()


async def test_apply_load_is_serialized_with_committed_remove(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id, _ = await _discord(pg_engine)
    factory = _Factory()
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=_Clock(),
    )
    await manager.apply(user_id, "discord")

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        config.revision = 2
        config.bot_token = "loaded-before-delete"
        await db.commit()

    original_load = manager._load_config
    loaded = asyncio.Event()
    release = asyncio.Event()

    async def gated_load(
        target_user_id: UUID,
        target_channel: ExternalChannel,
    ) -> object:
        config = await original_load(target_user_id, target_channel)
        loaded.set()
        await release.wait()
        return config

    monkeypatch.setattr(manager, "_load_config", gated_load)
    applying = asyncio.create_task(manager.apply(user_id, "discord"))
    await loaded.wait()

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        config = await db.get(DiscordConfig, user_id)
        assert config is not None
        await db.delete(config)
        await db.commit()

    try:
        removing = asyncio.create_task(manager.remove(user_id, "discord"))
        await asyncio.sleep(0)
        assert not removing.done()
        release.set()
        await applying
        await removing
        assert manager.status(user_id, "discord") is None
    finally:
        release.set()
        await asyncio.gather(applying, return_exceptions=True)
        await manager.shutdown()


async def test_reconnect_uses_full_jitter_backoff_and_online_resets_attempt(
    pg_engine: AsyncEngine,
) -> None:
    user_id, _ = await _discord(pg_engine, token="raw-secret-token")
    starts = 0

    def build(config: DiscordConfig | DingTalkConfig, generation: UUID) -> _Adapter:
        nonlocal starts
        starts += 1
        return _Adapter(
            runtime_generation=generation,
            binding_generation=config.binding_generation,
            start_error=(
                RuntimeError("raw-secret-token must never reach status") if starts <= 3 else None
            ),
        )

    clock = _Clock()
    factory = _Factory(build)
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=clock,
        random_source=lambda: 1.0,
    )

    await manager.apply(user_id, "discord")
    snapshot = manager.status(user_id, "discord")
    assert snapshot is not None and snapshot.state == "degraded"
    assert snapshot.last_error is not None
    assert "raw-secret-token" not in snapshot.last_error.message

    for index, expected in enumerate((1.0, 2.0, 4.0)):
        await clock.wait_for_sleep(index + 1)
        assert clock.delays[index] == expected
        clock.release(index)
    online = await factory.wait_created(4)
    await online.start_returned.wait()
    await _eventually(
        lambda: manager.status(user_id, "discord").state == "awaiting_pairing"  # type: ignore[union-attr]
    )

    online.closed.set()
    await clock.wait_for_sleep(4)
    assert clock.delays[3] == 1.0
    clock.release(3)
    reconnected = await factory.wait_created(5)
    await reconnected.start_returned.wait()
    await _eventually(lambda: manager.adapter_lookup(user_id, "discord") is reconnected)
    await manager.shutdown()


async def test_ready_recovery_runs_once_per_binding_across_runtime_reconnect(
    pg_engine: AsyncEngine,
) -> None:
    user_id, binding = await _discord(pg_engine, paired=True)
    recovered: list[tuple[UUID, ExternalChannel, UUID]] = []

    async def recover_ready(
        target_user_id: UUID,
        channel: ExternalChannel,
        binding_generation: UUID,
    ) -> None:
        recovered.append((target_user_id, channel, binding_generation))

    clock = _Clock()
    factory = _Factory()
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=clock,
        random_source=lambda: 1.0,
        ready_recovery=recover_ready,
    )

    await manager.apply(user_id, "discord")
    await manager.apply(user_id, "discord")
    assert recovered == [(user_id, "discord", binding)]

    factory.adapters[0].closed.set()
    await clock.wait_for_sleep(1)
    clock.release(0)
    reconnected = await factory.wait_created(2)
    await _eventually(lambda: manager.adapter_lookup(user_id, "discord") is reconnected)
    assert recovered == [(user_id, "discord", binding)]
    await manager.shutdown()


async def test_ready_recovery_failure_reconnects_and_retries(
    pg_engine: AsyncEngine,
) -> None:
    user_id, binding = await _discord(pg_engine, paired=True)
    recoveries: list[tuple[UUID, ExternalChannel, UUID]] = []

    async def recover_ready(
        target_user_id: UUID,
        channel: ExternalChannel,
        binding_generation: UUID,
    ) -> None:
        recoveries.append((target_user_id, channel, binding_generation))
        if len(recoveries) == 1:
            raise RuntimeError("transient recovery failure")

    clock = _Clock()
    factory = _Factory()
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=clock,
        random_source=lambda: 1.0,
        ready_recovery=recover_ready,
    )

    await manager.apply(user_id, "discord")
    first = factory.adapters[0]
    await first.stop_entered.wait()
    await clock.wait_for_sleep(1)
    snapshot = manager.status(user_id, "discord")
    assert snapshot is not None and snapshot.state == "degraded"
    assert manager.adapter_lookup(user_id, "discord") is None

    clock.release(0)
    second = await factory.wait_created(2)
    await second.start_returned.wait()
    await _eventually(
        lambda: manager.status(user_id, "discord").state == "ready"  # type: ignore[union-attr]
    )

    assert first.stop_calls == 1
    assert manager.adapter_lookup(user_id, "discord") is second
    assert recoveries == [
        (user_id, "discord", binding),
        (user_id, "discord", binding),
    ]
    await manager.shutdown()


@pytest.mark.parametrize("stop_mode", ["block", "fail", "resist"])
async def test_recovery_failure_bounds_stop_then_reconnects_without_task_leak(
    pg_engine: AsyncEngine,
    stop_mode: str,
) -> None:
    user_id, binding = await _discord(pg_engine, paired=True)
    recovery_attempts = 0
    created = 0

    async def recover_ready(
        _target_user_id: UUID,
        _channel: ExternalChannel,
        _binding_generation: UUID,
    ) -> None:
        nonlocal recovery_attempts
        recovery_attempts += 1
        if recovery_attempts == 1:
            raise RuntimeError("transient recovery failure")

    def build(config: DiscordConfig | DingTalkConfig, generation: UUID) -> _Adapter:
        nonlocal created
        created += 1
        return _Adapter(
            runtime_generation=generation,
            binding_generation=config.binding_generation,
            block_stop=created == 1 and stop_mode in {"block", "resist"},
            resist_stop_cancellation=created == 1 and stop_mode == "resist",
            close_stop_on_wait_cancel=created == 1 and stop_mode == "resist",
            stop_error=(
                RuntimeError("stop failed")
                if created == 1 and stop_mode == "fail"
                else None
            ),
        )

    clock = _Clock()
    factory = _Factory(build)
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=clock,
        random_source=lambda: 1.0,
        stop_timeout_seconds=1e-6,
        ready_recovery=recover_ready,
    )

    applying = asyncio.create_task(manager.apply(user_id, "discord"))
    first = await factory.wait_created(1)
    try:
        await asyncio.wait_for(clock.wait_for_sleep(1), timeout=0.5)
        await applying
        clock.release(0)
        second = await factory.wait_created(2)
        await second.start_returned.wait()
        await _eventually(lambda: manager.adapter_lookup(user_id, "discord") is second)
        await _eventually(lambda: manager.background_task_count == 1)

        assert manager.status(user_id, "discord").binding_generation == binding  # type: ignore[union-attr]
        assert recovery_attempts == 2
        assert first.stop_calls == 1
        if stop_mode in {"block", "resist"}:
            assert first.stop_cancelled.is_set()
    finally:
        first.stop_gate.set()
        clock.release(0)
        await asyncio.gather(applying, return_exceptions=True)
        await manager.shutdown()
    assert manager.background_task_count == 0


async def test_remove_drops_current_before_bounded_idempotent_stop(
    pg_engine: AsyncEngine,
) -> None:
    user_id, _ = await _discord(pg_engine)

    def build(config: DiscordConfig | DingTalkConfig, generation: UUID) -> _Adapter:
        return _Adapter(
            runtime_generation=generation,
            binding_generation=config.binding_generation,
            block_stop=True,
        )

    factory = _Factory(build)
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=_Clock(),
    )
    await manager.apply(user_id, "discord")
    adapter = factory.adapters[0]

    removing = asyncio.create_task(manager.remove(user_id, "discord"))
    await adapter.stop_entered.wait()
    assert manager.status(user_id, "discord") is None
    assert manager.adapter_lookup(user_id, "discord") is None

    adapter.stop_gate.set()
    await removing
    await manager.remove(user_id, "discord")
    assert adapter.stop_calls == 1
    assert len(factory.adapters) == 1
    await manager.shutdown()
    assert manager.background_task_count == 0


async def test_cancelled_remove_finishes_cleanup_before_propagating_cancellation(
    pg_engine: AsyncEngine,
) -> None:
    user_id, _ = await _discord(pg_engine)

    def build(config: DiscordConfig | DingTalkConfig, generation: UUID) -> _Adapter:
        return _Adapter(
            runtime_generation=generation,
            binding_generation=config.binding_generation,
            block_stop=True,
        )

    factory = _Factory(build)
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=_Clock(),
    )
    await manager.apply(user_id, "discord")
    adapter = factory.adapters[0]

    removing = asyncio.create_task(manager.remove(user_id, "discord"))
    await adapter.stop_entered.wait()
    removing.cancel()
    await asyncio.sleep(0)
    assert not removing.done()

    adapter.stop_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await removing
    assert adapter.stop_calls == 1
    assert manager.status(user_id, "discord") is None
    await manager.shutdown()
    assert manager.background_task_count == 0


async def test_begin_shutdown_drops_inbound_but_keeps_outbound_until_shutdown(
    pg_engine: AsyncEngine,
) -> None:
    user_id, _ = await _discord(pg_engine, paired=True)
    accepted: list[ChannelEvent] = []

    accepted_result = object()

    async def sink(_user_id: UUID, event: ChannelEvent) -> object:
        accepted.append(event)
        return accepted_result

    factory = _Factory()
    clock = _Clock()
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=sink,
        clock=clock,
    )
    await manager.apply(user_id, "discord")
    adapter = factory.adapters[0]
    assert await adapter.emit(_event(adapter, source_id="before")) is accepted_result

    await manager.begin_shutdown()
    assert manager.adapter_lookup(user_id, "discord") is adapter
    assert await adapter.emit(_event(adapter, source_id="after")) is None
    adapter.closed.set()
    await adapter.wait_finished.wait()
    await _eventually(lambda: manager.adapter_lookup(user_id, "discord") is None)

    assert [event.source_message_id for event in accepted] == ["before"]
    assert clock.delays == []
    await manager.shutdown()
    await manager.shutdown()
    assert adapter.stop_calls == 1
    assert manager.background_task_count == 0


async def test_shutdown_does_not_unboundedly_join_cancellation_resistant_stop(
    pg_engine: AsyncEngine,
) -> None:
    user_id, _ = await _discord(pg_engine)

    def build(config: DiscordConfig | DingTalkConfig, generation: UUID) -> _Adapter:
        return _Adapter(
            runtime_generation=generation,
            binding_generation=config.binding_generation,
            block_stop=True,
            resist_stop_cancellation=True,
        )

    factory = _Factory(build)
    manager = ChannelManager(
        pg_engine,
        adapter_factory=factory,
        event_sink=lambda _user_id, _event: asyncio.sleep(0),
        clock=_Clock(),
        stop_timeout_seconds=1e-6,
    )
    await manager.apply(user_id, "discord")
    adapter = factory.adapters[0]

    shutting_down = asyncio.create_task(manager.shutdown())
    try:
        await adapter.stop_entered.wait()
        await _eventually(shutting_down.done)
        assert adapter.stop_cancelled.is_set()
        assert manager.background_task_count == 1
    finally:
        adapter.stop_gate.set()
        await shutting_down
    await _eventually(lambda: manager.background_task_count == 0)
