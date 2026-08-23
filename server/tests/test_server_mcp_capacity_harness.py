"""Ordinary-CI smoke for the Py8a 500-user capacity harness."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from server_mcp_capacity_harness import HarnessConfig, run_harness  # noqa: E402


@pytest.mark.asyncio
async def test_server_mcp_capacity_harness_small_real_http_smoke() -> None:
    result = await run_harness(
        HarnessConfig(
            users=20,
            runtime_concurrency=2,
            sample_interval_seconds=0.001,
        )
    )

    assert result["ok"] is True, json.dumps(result, indent=2, sort_keys=True)
    assert result["http_transport_exercised"] is True
    assert result["transport"] == "real_loopback_streamable_http"
    assert result["fastmcp_clients"] == 1
    assert result["fastmcp_sessions"] == 1
    assert result["users"] == 20
    assert result["outcomes"] == {
        "accepted": 10,
        "issued": 2,
        "queued": 8,
        "busy": 10,
        "expired": 8,
        "completed": 2,
    }

    limits = result["limits"]
    metrics = result["metrics"]
    assert isinstance(limits, dict)
    assert isinstance(metrics, dict)
    assert metrics["runtime_reserved_high_water"] == 2
    assert metrics["global_reserved_high_water"] == limits["global_reserved"] == 32
    assert metrics["per_user_reserved_high_water"] == limits["per_user_reserved"] == 4
    assert metrics["queue_high_water"] == limits["runtime_waiting"] == 8
    assert metrics["pending_future_high_water"] == limits["runtime_waiting"]
    assert 1 <= metrics["http_connection_high_water"] <= 2
    assert metrics["http_active_request_high_water"] == 2
    assert metrics["http_search_requests"] == 2
    assert metrics["after_cleanup"]["http_connections"] == 0
    assert metrics["after_cleanup"]["scheduler_reserved"] == 0
    assert metrics["after_cleanup"]["scheduler_waiting"] == 0
    checks = result["checks"]
    assert isinstance(checks, dict)
    assert all(checks.values())
