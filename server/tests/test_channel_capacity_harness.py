"""Ordinary-CI smoke for the Py10 ChannelManager capacity harness."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from channel_capacity_harness import HarnessConfig, run_harness  # noqa: E402


def test_channel_capacity_config_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="adapters must be positive"):
        HarnessConfig(adapters=0).normalized()
    with pytest.raises(ValueError, match="duration must be positive"):
        HarnessConfig(duration_seconds=0).normalized()
    with pytest.raises(ValueError, match="sample interval must be positive"):
        HarnessConfig(sample_interval_seconds=0).normalized()
    with pytest.raises(ValueError, match="heartbeat interval must be positive"):
        HarnessConfig(heartbeat_interval_seconds=0).normalized()


@pytest.mark.asyncio
async def test_channel_capacity_harness_small_source_smoke() -> None:
    result = await run_harness(
        HarnessConfig(
            adapters=12,
            duration_seconds=0.08,
            sample_interval_seconds=0.002,
            heartbeat_interval_seconds=0.01,
            start_delay_seconds=0.002,
        )
    )

    assert result["ok"] is True, json.dumps(result, indent=2, sort_keys=True)
    assert result["mode"] == "source"
    assert result["transport"] == "in_memory_metadata_adapter"
    assert result["database_exercised"] is False
    assert result["platform_network_exercised"] is False
    assert result["adapters"] == 12
    assert result["profile"] == "ci_smoke"

    checks = result["checks"]
    metrics = result["metrics"]
    assert isinstance(checks, dict)
    assert isinstance(metrics, dict)
    assert all(checks.values())
    # ChannelManager pages the two platforms independently; this smoke has six
    # configs on each platform, while the 500/1000 profiles both reach 32.
    assert metrics["startup"]["maximum_concurrent_starts"] == 6
    assert metrics["startup"]["concurrency_limit"] == 32
    assert metrics["reconnect"]["created_replacements"] == 12
    assert metrics["reconnect"]["stale_callbacks_rejected"] == 12
    assert metrics["callbacks"]["accepted"] == 24
    assert metrics["callbacks"]["duplicate_source_ids"] == 0
    assert metrics["generations"]["current"] == 12
    assert metrics["generations"]["live_per_config_maximum"] == 1
    assert metrics["heartbeat"]["adapters_observed"] == 12
    assert metrics["heartbeat"]["configured_interval_seconds"] == 0.01
    assert metrics["event_loop_lag"]["samples"] > 0
    assert metrics["process"]["peak"]["task_count"] >= metrics["process"]["baseline"][
        "task_count"
    ]
    assert metrics["after_shutdown"]["manager_background_tasks"] == 0
    assert metrics["after_shutdown"]["leaked_harness_tasks"] == []


def test_channel_capacity_cli_profiles_are_named() -> None:
    assert HarnessConfig(adapters=500).normalized().profile == "merge_gate_500"
    assert HarnessConfig(adapters=1000).normalized().profile == "recorded_run_1000"
    assert HarnessConfig(adapters=8).normalized().profile == "ci_smoke"
