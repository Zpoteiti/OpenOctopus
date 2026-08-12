"""Opt-in real PostgreSQL/Uvicorn source-protocol capacity smoke."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import device_network_capacity_harness as capacity_harness  # noqa: E402
from device_network_capacity_harness import (  # noqa: E402
    NetworkHarnessConfig,
    run_network_harness,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("OO_RUN_NETWORK_CAPACITY_HARNESS") != "1",
    reason="real network capacity harness is opt-in",
)


@pytest.mark.asyncio
async def test_real_network_capacity_smoke(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "OPENOCTOPUS_DATABASE_URL", pg_engine.url.render_as_string(hide_password=False)
    )
    get_settings.cache_clear()
    get_engine.cache_clear()
    # Eight transfers exceed the configured admission's two active plus two
    # waiting slots unless the harness bounds launches at the active limit.
    monkeypatch.setattr(capacity_harness, "_TRANSFER_MAX_CONCURRENCY", 2)
    result = await run_network_harness(
        NetworkHarnessConfig(
            connections=8,
            users=4,
            sessions=8,
            dispatch_concurrency=4,
        )
    )

    assert result["ok"] is True
    assert result["network_exercised"] is True
    assert result["transfers_exercised"] is True
    assert result["transport"] == "real_fastapi_uvicorn_websocket"
    assert result["client_kind"] == "lightweight source-protocol peers; not PyInstaller bundles"
    assert result["provider_turns_exercised"] is False
    assert result["authenticated_connections"] == 8
    assert result["live_peer_readers_before_shutdown"] == 8
    assert result["users"] == 4
    assert result["independent_sessions"] == 8
    assert result["cross_user_result_errors"] == 0
    assert result["cross_user_dispatch_rejected"] is True
    assert result["successful_transfers"] == 8
    assert result["transfer_bytes_received"] > 0
    assert result["heartbeat_peers"] == result["authenticated_connections"]
    assert 0 <= result["in_flight_pings_at_shutdown"] <= result["authenticated_connections"]
    checks = result["checks"]
    assert isinstance(checks, dict)
    assert all(checks.values())
    metrics = result["metrics"]
    assert isinstance(metrics, dict)
    assert metrics["source_peer_queue_high_water"] <= metrics["source_peer_queue_capacity"]
    assert metrics["transfer_active_high_water"] > 0
    assert metrics["transfer_bytes_received"] == result["transfer_bytes_received"]
    assert metrics["online_connections_before_shutdown"] == 8
    assert metrics["online_connections_after_cleanup"] == 0
    assert metrics["after_cleanup"]["pending_calls"] == 0
    assert metrics["after_cleanup"]["transfer_slots"] == 0
    assert metrics["after_cleanup"]["transfer_waiters"] == 0
