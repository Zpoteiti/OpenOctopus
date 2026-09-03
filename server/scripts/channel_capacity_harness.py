#!/usr/bin/env python3
"""Run the Py10 ChannelManager source-mode capacity harness.

The harness uses the production ``ChannelManager`` with an in-memory metadata
configuration source and lightweight metadata adapters.  It never opens a
database or a Discord/DingTalk connection.  The default run is the 500-adapter,
ten-minute merge-gate profile; 1000 adapters is a separately recorded run.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import resource
import statistics
import sys
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine

from openctopus_server.channels.adapters.base import (
    ActionIssueHook,
    ChannelEventSink,
    ContextFetchResult,
)
from openctopus_server.channels.attachments import AuthenticatedAttachmentStream
from openctopus_server.channels.delivery import ActionResult
from openctopus_server.channels.manager import (
    STARTUP_CONCURRENCY,
    ChannelManager,
    _loaded_config,
    _LoadedConfig,
)
from openctopus_server.channels.types import (
    ChannelCapabilities,
    ChannelEvent,
    DeliveryAction,
    DeliveryPlan,
    ExternalAttachmentDescriptor,
    ExternalChannel,
    OutboundMessage,
)
from openctopus_server.db.models import DingTalkConfig, DiscordConfig

_NAMESPACE = UUID("59b0c2b8-5605-4d81-97e4-8050f01e664d")
_DEFAULT_ADAPTERS = 500
_DEFAULT_DURATION_SECONDS = 600.0
_DEFAULT_SAMPLE_INTERVAL_SECONDS = 1.0
_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30.0
_DEFAULT_START_DELAY_SECONDS = 0.002
_RECONNECT_TIMEOUT_SECONDS = 10.0
_TIGHT_HERD_WINDOW_SECONDS = 0.05


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    adapters: int = _DEFAULT_ADAPTERS
    duration_seconds: float = _DEFAULT_DURATION_SECONDS
    sample_interval_seconds: float = _DEFAULT_SAMPLE_INTERVAL_SECONDS
    heartbeat_interval_seconds: float = _DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    start_delay_seconds: float = _DEFAULT_START_DELAY_SECONDS
    reconnect_timeout_seconds: float = _RECONNECT_TIMEOUT_SECONDS

    def normalized(self) -> HarnessConfig:
        if self.adapters < 1:
            raise ValueError("adapters must be positive")
        if self.duration_seconds <= 0:
            raise ValueError("duration must be positive")
        if self.sample_interval_seconds <= 0:
            raise ValueError("sample interval must be positive")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat interval must be positive")
        if self.start_delay_seconds < 0:
            raise ValueError("start delay must not be negative")
        if self.reconnect_timeout_seconds <= 1:
            raise ValueError("reconnect timeout must exceed the one-second jitter cap")
        return self

    @property
    def profile(self) -> str:
        if self.adapters == 500:
            return "merge_gate_500"
        if self.adapters == 1000:
            return "recorded_run_1000"
        return "ci_smoke"


@dataclass(frozen=True, slots=True)
class _ProcessSample:
    rss_bytes: int | None
    fd_count: int | None
    task_count: int


def _process_sample() -> _ProcessSample:
    rss_bytes: int | None = None
    fd_count: int | None = None
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            resident_pages = int(statm.read().split()[1])
        rss_bytes = resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, OSError, ValueError):
        try:
            peak_rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            rss_bytes = peak_rss if sys.platform == "darwin" else peak_rss * 1024
        except (OSError, ValueError):
            pass
    try:
        fd_count = len(os.listdir("/proc/self/fd"))
    except (FileNotFoundError, OSError):
        pass
    return _ProcessSample(
        rss_bytes=rss_bytes,
        fd_count=fd_count,
        task_count=len(asyncio.all_tasks()),
    )


def _sample_dict(sample: _ProcessSample) -> dict[str, int | None]:
    return {
        "rss_bytes": sample.rss_bytes,
        "fd_count": sample.fd_count,
        "task_count": sample.task_count,
    }


class _MetricsSampler:
    def __init__(self, interval_seconds: float) -> None:
        self._interval_seconds = interval_seconds
        self.samples: list[_ProcessSample] = []
        self.event_loop_lags: list[float] = []
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._record()
        self._task = asyncio.create_task(
            self._run(), name="channel-capacity-metrics-sampler"
        )

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._task = None
        self._record()

    def _record(self) -> None:
        self.samples.append(_process_sample())

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._interval_seconds
        while True:
            await asyncio.sleep(max(0.0, deadline - loop.time()))
            observed = loop.time()
            self.event_loop_lags.append(max(0.0, observed - deadline))
            self._record()
            deadline = observed + self._interval_seconds

    @property
    def peak(self) -> _ProcessSample:
        rss_values = [sample.rss_bytes for sample in self.samples if sample.rss_bytes is not None]
        fd_values = [sample.fd_count for sample in self.samples if sample.fd_count is not None]
        return _ProcessSample(
            rss_bytes=max(rss_values) if rss_values else None,
            fd_count=max(fd_values) if fd_values else None,
            task_count=max((sample.task_count for sample in self.samples), default=0),
        )


class _MetadataConfigSource:
    """Detached ORM metadata consumed through ChannelManager's loader seam."""

    def __init__(self, adapters: int) -> None:
        loaded: list[_LoadedConfig] = []
        for index in range(adapters):
            user_id = uuid5(_NAMESPACE, f"user-{index:06d}")
            binding_generation = uuid5(_NAMESPACE, f"binding-{index:06d}")
            if index % 2 == 0:
                model = DiscordConfig(
                    user_id=user_id,
                    bot_token=f"capacity-token-{index}",
                    application_id=f"capacity-application-{index}",
                    bot_user_id=f"capacity-discord-bot-{index}",
                    bot_display_name="Capacity Bot",
                    binding_generation=binding_generation,
                    revision=1,
                    owner_platform_user_id=f"owner-{index}",
                    owner_dm_chat_id=f"dm-{index}",
                    paired_at=datetime.now(UTC),
                    allow_list=[],
                )
                loaded.append(_loaded_config(model, "discord"))
            else:
                ding_model = DingTalkConfig(
                    user_id=user_id,
                    client_id=f"capacity-client-{index}",
                    client_secret=f"capacity-secret-{index}",
                    bot_user_id=f"capacity-dingtalk-bot-{index}",
                    bot_display_name="Capacity Bot",
                    binding_generation=binding_generation,
                    revision=1,
                    owner_platform_user_id=f"owner-{index}",
                    owner_dm_chat_id=f"dm-{index}",
                    paired_at=datetime.now(UTC),
                    allow_list=[],
                )
                loaded.append(_loaded_config(ding_model, "dingtalk"))
        self.configs = tuple(loaded)
        self._by_key = {(item.user_id, item.channel): item for item in loaded}
        self._user_by_binding = {
            item.binding_generation: item.user_id for item in loaded
        }

    def page(
        self,
        channel: ExternalChannel,
        *,
        after: UUID | None,
        limit: int,
    ) -> list[_LoadedConfig]:
        candidates = sorted(
            (
                item
                for item in self.configs
                if item.channel == channel and (after is None or item.user_id > after)
            ),
            key=lambda item: item.user_id,
        )
        return candidates[:limit]

    def get(self, user_id: UUID, channel: ExternalChannel) -> _LoadedConfig | None:
        return self._by_key.get((user_id, channel))

    def user_for_binding(self, binding_generation: UUID) -> UUID:
        return self._user_by_binding[binding_generation]


class _ReconnectClock:
    def __init__(self) -> None:
        self.delays: list[float] = []

    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, delay: float) -> None:
        self.delays.append(delay)
        await asyncio.sleep(delay)


@dataclass(slots=True)
class _StartProbe:
    active_initial: int = 0
    maximum_initial: int = 0

    async def hold(self, delay: float, *, initial: bool) -> None:
        if not initial:
            await asyncio.sleep(delay)
            return
        self.active_initial += 1
        self.maximum_initial = max(self.maximum_initial, self.active_initial)
        try:
            await asyncio.sleep(delay)
        finally:
            self.active_initial -= 1


class _MetadataAdapter:
    capabilities = ChannelCapabilities(history_backfill=False, file_delivery=False)

    def __init__(
        self,
        *,
        platform: ExternalChannel,
        user_id: UUID,
        binding_generation: UUID,
        runtime_generation: UUID,
        initial: bool,
        start_probe: _StartProbe,
        start_delay_seconds: float,
        heartbeat_interval_seconds: float,
    ) -> None:
        self.platform = platform
        self.user_id = user_id
        self.binding_generation = binding_generation
        self.runtime_generation = runtime_generation
        self.initial = initial
        self._start_probe = start_probe
        self._start_delay_seconds = start_delay_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._closed = asyncio.Event()
        self._sink: ChannelEventSink | None = None
        self.started_at: float | None = None
        self.stop_calls = 0
        self.heartbeat_intervals: list[float] = []

    @property
    def stopped(self) -> bool:
        return self.stop_calls > 0

    async def start(self, sink: ChannelEventSink) -> None:
        self._sink = sink
        await self._start_probe.hold(self._start_delay_seconds, initial=self.initial)
        self.started_at = time.perf_counter()

    async def wait_closed(self) -> None:
        previous = time.perf_counter()
        while not self._closed.is_set():
            try:
                await asyncio.wait_for(
                    self._closed.wait(), timeout=self._heartbeat_interval_seconds
                )
            except TimeoutError:
                observed = time.perf_counter()
                self.heartbeat_intervals.append(observed - previous)
                previous = observed

    async def stop(self) -> None:
        self.stop_calls += 1
        self._closed.set()

    def force_disconnect(self) -> None:
        self._closed.set()

    async def emit_probe(self, source_message_id: str) -> object | None:
        sink = self._sink
        if sink is None:
            raise RuntimeError("metadata adapter has not started")
        return await sink(
            ChannelEvent(
                platform=self.platform,
                binding_generation=self.binding_generation,
                runtime_generation=self.runtime_generation,
                source_message_id=source_message_id,
                chat_id=f"capacity-{self.user_id}",
                conversation_kind="dm",
                sender_id=f"owner-{self.user_id}",
                sender_display_name="Capacity Owner",
                sender_kind="human",
                explicitly_mentions_bot=False,
                text="capacity probe",
                attachments=(),
            )
        )

    async def open_authenticated_attachment(
        self,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream:
        raise RuntimeError("capacity metadata adapters do not expose attachments")

    async def fetch_recent_context(
        self,
        *,
        chat_id: str,
        before_message_id: str,
        limit: int,
    ) -> ContextFetchResult:
        return ContextFetchResult(status="unsupported")

    def plan_delivery(self, message: OutboundMessage) -> DeliveryPlan:
        return DeliveryPlan(
            actions=(DeliveryAction(kind="text_message", visible=True, content=message.content),)
        )

    async def execute_action(
        self,
        action: DeliveryAction,
        *,
        on_issued: ActionIssueHook,
    ) -> ActionResult:
        await on_issued()
        return ActionResult(status="sent")


class _MetadataAdapterFactory:
    def __init__(self, config: HarnessConfig) -> None:
        self._config = config
        self.start_probe = _StartProbe()
        self.adapters: list[_MetadataAdapter] = []
        self._created_by_key: Counter[tuple[UUID, ExternalChannel]] = Counter()

    def __call__(
        self,
        model: DiscordConfig | DingTalkConfig,
        runtime_generation: UUID,
    ) -> _MetadataAdapter:
        platform: ExternalChannel = "discord" if isinstance(model, DiscordConfig) else "dingtalk"
        key = (model.user_id, platform)
        initial = self._created_by_key[key] == 0
        self._created_by_key[key] += 1
        adapter = _MetadataAdapter(
            platform=platform,
            user_id=model.user_id,
            binding_generation=model.binding_generation,
            runtime_generation=runtime_generation,
            initial=initial,
            start_probe=self.start_probe,
            start_delay_seconds=self._config.start_delay_seconds,
            heartbeat_interval_seconds=self._config.heartbeat_interval_seconds,
        )
        self.adapters.append(adapter)
        return adapter

    def for_key(self, key: tuple[UUID, ExternalChannel]) -> list[_MetadataAdapter]:
        return [
            adapter
            for adapter in self.adapters
            if (adapter.user_id, adapter.platform) == key
        ]


class _SourceChannelManager(ChannelManager):
    """Production manager with only its persistence seam replaced."""

    def __init__(self, source: _MetadataConfigSource, **kwargs: Any) -> None:
        self._capacity_source = source
        super().__init__(cast(AsyncEngine, object()), **kwargs)

    async def _load_config(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> _LoadedConfig | None:
        return self._capacity_source.get(user_id, channel)

    async def _load_config_page(
        self,
        channel: ExternalChannel,
        *,
        after: UUID | None,
    ) -> list[_LoadedConfig]:
        return self._capacity_source.page(channel, after=after, limit=100)


class _JitterSequence:
    def __init__(self, count: int) -> None:
        self._count = count
        self._index = 0

    def __call__(self) -> float:
        value = (self._index + 0.5) / self._count
        self._index += 1
        return value


async def _eventually(
    predicate: Callable[[], bool],
    *,
    timeout_seconds: float,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("channel capacity condition did not become true")
        await asyncio.sleep(0.005)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _spread(values: list[float]) -> float:
    return max(values) - min(values) if len(values) > 1 else 0.0


def _maximum_in_window(values: list[float], window_seconds: float) -> int:
    ordered = sorted(values)
    maximum = 0
    left = 0
    for right, value in enumerate(ordered):
        while value - ordered[left] > window_seconds:
            left += 1
        maximum = max(maximum, right - left + 1)
    return maximum


def _delta(current: int | None, baseline: int | None) -> int | None:
    if current is None or baseline is None:
        return None
    return current - baseline


async def run_harness(config: HarnessConfig = HarnessConfig()) -> dict[str, object]:
    config = config.normalized()
    current_task = asyncio.current_task()
    baseline_tasks = {
        id(task)
        for task in asyncio.all_tasks()
        if task is not current_task and not task.done()
    }
    baseline = _process_sample()
    sampler = _MetricsSampler(config.sample_interval_seconds)
    source = _MetadataConfigSource(config.adapters)
    factory = _MetadataAdapterFactory(config)
    reconnect_clock = _ReconnectClock()
    callback_counts: Counter[str] = Counter()
    callback_user_mismatches = 0

    async def accept(user_id: UUID, event: ChannelEvent) -> object:
        nonlocal callback_user_mismatches
        expected_user = source.user_for_binding(event.binding_generation)
        if user_id != expected_user:
            callback_user_mismatches += 1
        callback_counts[event.source_message_id] += 1
        return "accepted"

    manager = _SourceChannelManager(
        source,
        adapter_factory=factory,
        event_sink=accept,
        clock=reconnect_clock,
        random_source=_JitterSequence(config.adapters),
    )
    startup_seconds = 0.0
    reconnect_seconds = 0.0
    old_adapters: list[_MetadataAdapter] = []
    current_adapters: list[_MetadataAdapter] = []
    stale_callbacks_rejected = 0
    generation_current = 0
    live_per_config_maximum = 0
    failure: str | None = None
    started = time.perf_counter()

    await sampler.start()
    try:
        startup_started = time.perf_counter()
        await manager.startup()
        startup_seconds = time.perf_counter() - startup_started
        old_adapters = [
            cast(_MetadataAdapter, manager.adapter_lookup(item.user_id, item.channel))
            for item in source.configs
        ]
        if any(adapter is None for adapter in old_adapters):
            raise RuntimeError("startup left a configured adapter offline")

        await asyncio.gather(
            *(
                adapter.emit_probe(f"initial:{adapter.user_id}:{adapter.platform}")
                for adapter in old_adapters
            )
        )

        reconnect_started = time.perf_counter()
        for adapter in old_adapters:
            adapter.force_disconnect()

        def reconnected() -> bool:
            if len(factory.adapters) != config.adapters * 2:
                return False
            for item, old in zip(source.configs, old_adapters, strict=True):
                current = manager.adapter_lookup(item.user_id, item.channel)
                current_metadata = cast(_MetadataAdapter | None, current)
                status = manager.status(item.user_id, item.channel)
                if (
                    current_metadata is None
                    or current_metadata is old
                    or status is None
                    or status.state != "ready"
                    or status.runtime_generation != current_metadata.runtime_generation
                    or not old.stopped
                ):
                    return False
            return True

        await _eventually(reconnected, timeout_seconds=config.reconnect_timeout_seconds)
        reconnect_seconds = time.perf_counter() - reconnect_started
        current_adapters = [
            cast(_MetadataAdapter, manager.adapter_lookup(item.user_id, item.channel))
            for item in source.configs
        ]

        for old, current in zip(old_adapters, current_adapters, strict=True):
            source_id = f"handoff:{current.user_id}:{current.platform}"
            if await old.emit_probe(source_id) is None:
                stale_callbacks_rejected += 1
            await current.emit_probe(source_id)

        await asyncio.sleep(config.duration_seconds)

        for item in source.configs:
            status = manager.status(item.user_id, item.channel)
            lookup_adapter = cast(
                _MetadataAdapter | None,
                manager.adapter_lookup(item.user_id, item.channel),
            )
            if (
                status is not None
                and lookup_adapter is not None
                and status.runtime_generation == lookup_adapter.runtime_generation
            ):
                generation_current += 1
            live_count = sum(
                not candidate.stopped
                for candidate in factory.for_key((item.user_id, item.channel))
            )
            live_per_config_maximum = max(live_per_config_maximum, live_count)
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            await manager.shutdown()
        except Exception as exc:
            if failure is None:
                failure = f"{type(exc).__name__}: {exc}"
        await sampler.stop()

    await asyncio.sleep(0)
    await asyncio.sleep(0)
    after_shutdown = _process_sample()
    leaked_tasks = sorted(
        task.get_name()
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and not task.done()
        and id(task) not in baseline_tasks
    )
    elapsed = time.perf_counter() - started
    peak = sampler.peak

    planned_delays = reconnect_clock.delays
    reconnect_start_times = [
        adapter.started_at
        for adapter in factory.adapters
        if not adapter.initial and adapter.started_at is not None
    ]
    observed_reconnect_times = reconnect_start_times
    maximum_tight_starts = _maximum_in_window(
        observed_reconnect_times, _TIGHT_HERD_WINDOW_SECONDS
    )
    tight_herd_limit = max(
        4,
        math.ceil(config.adapters * _TIGHT_HERD_WINDOW_SECONDS * 2.5),
    )
    planned_spread = _spread(planned_delays)
    observed_spread = _spread(observed_reconnect_times)
    heartbeat_intervals = [
        interval
        for adapter in current_adapters
        for interval in adapter.heartbeat_intervals
    ]
    heartbeat_adapters_observed = sum(
        bool(adapter.heartbeat_intervals) for adapter in current_adapters
    )
    duplicate_source_ids = sum(count > 1 for count in callback_counts.values())
    expected_callback_count = config.adapters * 2
    expected_initial_parallelism = min(
        STARTUP_CONCURRENCY,
        max(
            sum(item.channel == "discord" for item in source.configs),
            sum(item.channel == "dingtalk" for item in source.configs),
        ),
    )
    heartbeat_p95 = _percentile(heartbeat_intervals, 0.95)
    heartbeat_reasonable_limit = max(
        config.heartbeat_interval_seconds * 3,
        config.heartbeat_interval_seconds + 0.05,
    )
    checks = {
        "harness_completed": failure is None,
        "all_configs_started": len(old_adapters) == config.adapters,
        "startup_concurrency_bounded": factory.start_probe.maximum_initial
        == expected_initial_parallelism
        and factory.start_probe.maximum_initial <= STARTUP_CONCURRENCY,
        "all_configs_reconnected": len(current_adapters) == config.adapters
        and len(observed_reconnect_times) == config.adapters,
        "reconnect_jitter_is_spread": config.adapters == 1
        or (
            len(planned_delays) == config.adapters
            and planned_spread >= 0.5
            and observed_spread >= planned_spread * 0.7
        ),
        "reconnect_has_no_tight_herd": maximum_tight_starts <= tight_herd_limit,
        "each_config_has_one_current_generation": generation_current == config.adapters
        and live_per_config_maximum == 1,
        "stale_generation_callbacks_are_fenced": stale_callbacks_rejected
        == config.adapters,
        "callbacks_are_exactly_once": sum(callback_counts.values())
        == expected_callback_count
        and duplicate_source_ids == 0
        and callback_user_mismatches == 0,
        "heartbeat_observed_for_every_current_adapter": heartbeat_adapters_observed
        == config.adapters,
        "heartbeat_interval_is_reasonable": heartbeat_p95 is not None
        and heartbeat_p95 <= heartbeat_reasonable_limit,
        "event_loop_lag_was_sampled": bool(sampler.event_loop_lags),
        "manager_tasks_are_empty_after_shutdown": manager.background_task_count == 0,
        "harness_tasks_do_not_leak": not leaked_tasks,
    }

    return {
        "ok": all(checks.values()),
        "failure": failure,
        "profile": config.profile,
        "mode": "source",
        "transport": "in_memory_metadata_adapter",
        "database_exercised": False,
        "platform_network_exercised": False,
        "adapters": config.adapters,
        "duration_seconds": config.duration_seconds,
        "checks": checks,
        "metrics": {
            "wall_time_seconds": round(elapsed, 6),
            "startup": {
                "seconds": round(startup_seconds, 6),
                "maximum_concurrent_starts": factory.start_probe.maximum_initial,
                "concurrency_limit": STARTUP_CONCURRENCY,
            },
            "reconnect": {
                "seconds": round(reconnect_seconds, 6),
                "created_replacements": len(observed_reconnect_times),
                "planned_delay_min_seconds": min(planned_delays, default=None),
                "planned_delay_max_seconds": max(planned_delays, default=None),
                "planned_delay_spread_seconds": round(planned_spread, 6),
                "observed_start_spread_seconds": round(observed_spread, 6),
                "tight_herd_window_seconds": _TIGHT_HERD_WINDOW_SECONDS,
                "maximum_starts_in_tight_window": maximum_tight_starts,
                "tight_herd_limit": tight_herd_limit,
                "stale_callbacks_rejected": stale_callbacks_rejected,
            },
            "callbacks": {
                "accepted": sum(callback_counts.values()),
                "unique_source_ids": len(callback_counts),
                "duplicate_source_ids": duplicate_source_ids,
                "user_mismatches": callback_user_mismatches,
            },
            "generations": {
                "current": generation_current,
                "live_per_config_maximum": live_per_config_maximum,
            },
            "heartbeat": {
                "configured_interval_seconds": config.heartbeat_interval_seconds,
                "adapters_observed": heartbeat_adapters_observed,
                "samples": len(heartbeat_intervals),
                "mean_interval_seconds": (
                    round(statistics.fmean(heartbeat_intervals), 6)
                    if heartbeat_intervals
                    else None
                ),
                "p95_interval_seconds": (
                    round(heartbeat_p95, 6) if heartbeat_p95 is not None else None
                ),
                "maximum_interval_seconds": (
                    round(max(heartbeat_intervals), 6) if heartbeat_intervals else None
                ),
            },
            "event_loop_lag": {
                "sample_interval_seconds": config.sample_interval_seconds,
                "samples": len(sampler.event_loop_lags),
                "mean_seconds": (
                    round(statistics.fmean(sampler.event_loop_lags), 6)
                    if sampler.event_loop_lags
                    else None
                ),
                "p95_seconds": (
                    round(_percentile(sampler.event_loop_lags, 0.95) or 0.0, 6)
                    if sampler.event_loop_lags
                    else None
                ),
                "maximum_seconds": (
                    round(max(sampler.event_loop_lags), 6)
                    if sampler.event_loop_lags
                    else None
                ),
            },
            "process": {
                "baseline": _sample_dict(baseline),
                "peak": _sample_dict(peak),
                "after_shutdown": _sample_dict(after_shutdown),
                "rss_peak_delta_bytes": _delta(peak.rss_bytes, baseline.rss_bytes),
                "rss_after_delta_bytes": _delta(
                    after_shutdown.rss_bytes, baseline.rss_bytes
                ),
                "fd_peak_delta": _delta(peak.fd_count, baseline.fd_count),
                "fd_after_delta": _delta(after_shutdown.fd_count, baseline.fd_count),
                "task_peak_delta": peak.task_count - baseline.task_count,
                "task_after_delta": after_shutdown.task_count - baseline.task_count,
                "rss_and_fd_values_are_observations_not_portable_limits": True,
            },
            "after_shutdown": {
                "manager_background_tasks": manager.background_task_count,
                "leaked_harness_tasks": leaked_tasks,
            },
        },
        "limitations": [
            "Metadata adapters do not connect to Discord or DingTalk.",
            "The metadata config source is in memory and does not exercise PostgreSQL.",
            "RSS and file-descriptor values are recorded evidence, not machine-portable gates.",
        ],
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapters", type=int, default=_DEFAULT_ADAPTERS)
    parser.add_argument(
        "--duration-seconds",
        "--duration",
        dest="duration_seconds",
        type=float,
        default=_DEFAULT_DURATION_SECONDS,
    )
    parser.add_argument(
        "--sample-interval-seconds",
        "--sample-interval",
        dest="sample_interval_seconds",
        type=float,
        default=_DEFAULT_SAMPLE_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--heartbeat-interval-seconds",
        type=float,
        default=_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    )
    parser.add_argument("--indent", type=int, default=2)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        result = asyncio.run(
            run_harness(
                HarnessConfig(
                    adapters=args.adapters,
                    duration_seconds=args.duration_seconds,
                    sample_interval_seconds=args.sample_interval_seconds,
                    heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                )
            )
        )
    except (OSError, ValueError) as exc:
        result = {
            "ok": False,
            "failure": f"{type(exc).__name__}: {exc}",
            "checks": {"configuration_valid": False},
        }
    print(json.dumps(result, indent=args.indent, sort_keys=True))
    return 0 if result.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
