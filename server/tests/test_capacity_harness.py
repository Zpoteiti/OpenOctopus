"""Opt-in source-mode capacity harness checks.

These tests are skipped by ordinary pytest runs because they intentionally
create many asyncio tasks.  Run the smoke test with
``OO_RUN_CAPACITY_HARNESS=1``; use the script directly for the 500
connection evidence run.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from device_capacity_harness import HarnessConfig, run_harness  # noqa: E402

pytestmark = pytest.mark.skipif(
    os.environ.get("OO_RUN_CAPACITY_HARNESS") != "1",
    reason="capacity harness is opt-in; run with OO_RUN_CAPACITY_HARNESS=1",
)


@pytest.mark.asyncio
async def test_source_capacity_harness_smoke() -> None:
    result = await run_harness(
        HarnessConfig(
            connections=8,
            users=4,
            sessions=8,
            dispatch_concurrency=4,
            queue_capacity=4,
            call_delay_seconds=0.001,
            slow_delay_seconds=0.03,
            ping_interval_seconds=0.01,
            liveness_timeout_seconds=0.05,
            sample_interval_seconds=0.005,
        )
    )

    assert result["ok"] is True
    assert result["network_exercised"] is False
    assert result["authenticated_connections"] == 8
    assert result["independent_sessions"] == 8
    assert result["cross_user_result_errors"] == 0
    assert result["cross_user_slot_errors"] == 0
    assert result["slot_spoof_rejected"] is True
    assert result["slot_spoof_future_survived"] is True
    metrics = result["metrics"]
    assert isinstance(metrics, dict)
    assert metrics["device_queue_high_water"] <= metrics["queue_capacity"]
    assert metrics["registry_pending_high_water"] > 0
    assert result["checks"] == {
        "authenticated_connections": True,
        "minimum_users": True,
        "no_cross_user_result_or_slot_delivery": True,
        "cross_user_and_stale_generation_slots_rejected": True,
        "same_device_fifo": True,
        "cross_device_concurrency": True,
        "slow_user_does_not_block_other_user": True,
        "ping_pong_under_load": True,
        "busy_result_is_documented": True,
        "unreachable_result_is_documented": True,
        "bulk_calls_complete_or_documented": True,
        "disconnect_cleans_pending_and_limiter": True,
        "task_count_returns_to_baseline": True,
        "queue_high_water_is_bounded": True,
        "rss_plateau": True,
    }
