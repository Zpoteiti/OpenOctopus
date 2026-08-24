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
    # Eight ordinary transfers exceed the configured active limit, while the
    # bridge burst additionally fills its bounded wait queue and reports busy.
    monkeypatch.setattr(capacity_harness, "_TRANSFER_MAX_CONCURRENCY", 4)
    sampler_started = False
    catalog_samples = 0
    original_sampler_start = capacity_harness._MetricsSampler.start
    original_offline_catalog_metrics = capacity_harness._offline_catalog_metrics

    async def observed_sampler_start(sampler: capacity_harness._MetricsSampler) -> None:
        nonlocal sampler_started
        sampler_started = True
        await original_sampler_start(sampler)

    async def observed_offline_catalog_metrics(*args: object, **kwargs: object) -> dict[str, int]:
        nonlocal catalog_samples
        assert sampler_started, "capacity sampler must cover offline catalog projection"
        sample = kwargs.get("sample")
        assert callable(sample), "catalog projection must expose explicit peak samples"

        def observed_sample() -> None:
            nonlocal catalog_samples
            catalog_samples += 1
            sample()

        kwargs["sample"] = observed_sample
        return await original_offline_catalog_metrics(*args, **kwargs)

    monkeypatch.setattr(capacity_harness._MetricsSampler, "start", observed_sampler_start)
    monkeypatch.setattr(
        capacity_harness,
        "_offline_catalog_metrics",
        observed_offline_catalog_metrics,
    )
    result = await run_network_harness(
        NetworkHarnessConfig(
            connections=8,
            users=4,
            sessions=8,
            dispatch_concurrency=4,
        )
    )

    assert result["ok"] is True, result
    assert catalog_samples >= 3
    assert result["network_exercised"] is True
    assert result["transfers_exercised"] is True
    assert result["client_bridges_exercised"] is True
    assert result["offline_catalogs_exercised"] is True
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
    assert result["successful_client_bridges"] > 0
    assert result["busy_client_bridges"] > 0
    assert result["bridge_errors"] == {"TransferBusyError": result["busy_client_bridges"]}
    assert result["bridge_warnings"] == {"none": result["successful_client_bridges"]}
    assert result["offline_catalog_devices"] == 8
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
    assert metrics["bridge_active_high_water"] == 4
    assert metrics["bridge_admission_active_high_water"] == 4
    assert metrics["bridge_per_user_active_high_water"] == 2
    assert metrics["bridge_endpoint_high_water"] == 8
    assert 0 < metrics["bridge_queue_high_water"] <= metrics["bridge_queue_capacity"] == 4
    assert metrics["client_local_slot_high_water"] == 2
    assert metrics["client_local_slot_capacity"] == 2
    assert metrics["bridge_admission_waiting_high_water"] > 0
    assert metrics["bridge_task_high_water"] > 0
    assert metrics["transfer_bytes_received"] == result["transfer_bytes_received"]
    assert metrics["offline_catalog_routes_high_water"] == 2
    assert metrics["offline_catalog_schemas_high_water"] == 1
    assert metrics["offline_catalog_schema_bytes_high_water"] > 0
    assert metrics["mcp_runtime_high_water"] == 0
    assert metrics["online_connections_before_shutdown"] == 8
    assert metrics["online_connections_after_cleanup"] == 0
    assert metrics["after_cleanup"]["pending_calls"] == 0
    assert metrics["after_cleanup"]["transfer_slots"] == 0
    assert metrics["after_cleanup"]["transfer_waiters"] == 0
    assert metrics["after_cleanup"]["bridge_slots"] == 0
    assert metrics["after_cleanup"]["bridge_endpoints"] == 0
    assert metrics["after_cleanup"]["bridge_pinned_tombstones"] == 0
    assert metrics["after_cleanup"]["bridge_reserved_tombstones"] == 0
    assert metrics["after_cleanup"]["bridge_tasks"] == 0
    assert metrics["after_cleanup"]["client_local_slots"] == 0
