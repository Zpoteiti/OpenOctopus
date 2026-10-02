"""Ordinary-CI smoke for the opt-in 500-user private Server MCP harness."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from server_mcp_capacity_harness import HarnessConfig, run_harness  # noqa: E402


@pytest.mark.asyncio
async def test_server_mcp_capacity_harness_small_real_http_smoke() -> None:
    result = await run_harness(HarnessConfig(
        users=20, max_clients=2, sample_interval_seconds=0.001,
    ))

    assert result["ok"] is True, json.dumps(result, indent=2, sort_keys=True)
    assert result["transport"] == "real_loopback_streamable_http"
    assert result["users"] == 20
    assert result["outcomes"] == {
        "issued": 2, "busy": 18, "completed": 2, "reused": 1, "evicted": 1,
    }
    metrics = result["metrics"]
    assert metrics["initial_private_http_sessions_used"] == 2
    assert metrics["private_http_sessions_used"] == 3
    assert metrics["private_http_sessions_initialized"] == 3
    assert metrics["active_session_high_water"] == 2
    assert metrics["idle_sessions_after_calls"] == 2
    assert metrics["http_active_request_high_water"] == 2
    assert metrics["after_cleanup"]["private_sessions"] == 0
    assert metrics["after_cleanup"]["http_connections"] == 0
    assert all(result["checks"].values())
