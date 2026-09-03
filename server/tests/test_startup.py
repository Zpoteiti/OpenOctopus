import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI

from openctopus_server.channels.adapters.base import ContextFetchResult
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.main import _fetch_recent_channel_context, _lifespan
from openctopus_server.mcp.models import empty_server_mcp_envelope


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        object_storage_endpoint="http://127.0.0.1:9000",
        object_storage_bucket="test",
        object_storage_region="us-east-1",
        object_storage_access_key="test-access",
        object_storage_secret_key="test-secret",
        workspace_deletion_purge_timeout_seconds=300,
        workspace_deletion_shutdown_grace_seconds=5,
        device_transfer_idle_timeout_seconds=30,
    )


@pytest.fixture(autouse=True)
def _startup_dependencies(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    converter = SimpleNamespace(probe=AsyncMock())
    supervisor = SimpleNamespace(
        start=AsyncMock(),
        begin_shutdown=AsyncMock(),
        shutdown=AsyncMock(),
        ready_generations=Mock(return_value={}),
        dispatch_server_mcp=AsyncMock(),
    )
    scheduler = SimpleNamespace(
        start=AsyncMock(),
        stop=AsyncMock(),
        wake=Mock(),
    )
    pulse = SimpleNamespace(start=Mock(), close=AsyncMock())
    pending_recovery = AsyncMock()
    monkeypatch.setattr("openctopus_server.main.initialize_token_estimator", Mock())
    monkeypatch.setattr("openctopus_server.main.get_content_converter", lambda: converter)
    monkeypatch.setattr(
        "openctopus_server.main.ServerMcpSupervisor.create_default",
        Mock(return_value=supervisor),
    )
    monkeypatch.setattr(
        "openctopus_server.main._load_server_mcp_authority",
        AsyncMock(return_value=empty_server_mcp_envelope()),
    )
    monkeypatch.setattr(
        "openctopus_server.main.CronScheduler",
        Mock(return_value=scheduler),
    )
    monkeypatch.setattr(
        "openctopus_server.main.HeartbeatPulse",
        Mock(return_value=pulse),
    )
    monkeypatch.setattr(
        "openctopus_server.main.close_obsolete_channel_pending",
        pending_recovery,
    )
    return SimpleNamespace(
        probe=converter.probe,
        supervisor=supervisor,
        scheduler=scheduler,
        pulse=pulse,
        pending_recovery=pending_recovery,
    )


def _app_with_runtime() -> tuple[FastAPI, SimpleNamespace]:
    app = FastAPI()
    device_registry = SimpleNamespace(close=AsyncMock())
    runtime = SimpleNamespace(runner_instance_id=uuid4(), close=AsyncMock())
    runtime.device_registry = device_registry
    app.state.chat_runtime = runtime
    return app, runtime


def _engine() -> tuple[Mock, AsyncMock]:
    engine = Mock()
    connection = AsyncMock()
    transaction = AsyncMock()
    transaction.__aenter__.return_value = connection
    engine.begin.return_value = transaction
    engine.dispose = AsyncMock()
    return engine, connection


def _storage() -> Mock:
    return Mock(
        probe_startup=AsyncMock(),
        recover_transfer_uploads=AsyncMock(),
        close=AsyncMock(),
    )


def _deletion_worker() -> Mock:
    return Mock(start=Mock(), close=AsyncMock())


async def test_production_context_fetch_logs_only_bounded_failure_facts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    user_id = uuid4()
    event = SimpleNamespace(
        platform="discord",
        chat_id="secret-chat-id",
        source_message_id="secret-source-id",
    )
    adapter = SimpleNamespace(
        fetch_recent_context=AsyncMock(
            side_effect=[
                ContextFetchResult(
                    status="failed",
                    error_code="discord_history_unavailable",
                    error_message="raw secret platform response",
                ),
                RuntimeError("raw secret exception"),
            ]
        )
    )
    manager = SimpleNamespace(adapter_lookup=Mock(return_value=adapter))
    caplog.set_level("WARNING", logger="openctopus_server.main")

    failed = await _fetch_recent_channel_context(
        manager, user_id, event, limit=100  # type: ignore[arg-type]
    )
    raised = await _fetch_recent_channel_context(
        manager, user_id, event, limit=100  # type: ignore[arg-type]
    )

    assert failed == raised == ()
    records = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "channel_context_fetch_failed"
    ]
    assert [record.error_code for record in records] == [
        "discord_history_unavailable",
        "channel_history_fetch_exception",
    ]
    assert all(record.platform == "discord" for record in records)
    assert all(record.user_id == str(user_id) for record in records)
    assert all(record.context_count == 0 for record in records)
    assert "secret-chat-id" not in caplog.text
    assert "secret-source-id" not in caplog.text
    assert "raw secret" not in caplog.text


async def test_lifespan_runs_storage_probe_and_closes_storage(
    _startup_dependencies: SimpleNamespace,
) -> None:
    app, runtime = _app_with_runtime()
    events: list[str] = []

    async def begin_shutdown() -> None:
        events.append("mcp-begin")

    async def stop_scheduler() -> None:
        events.append("cron-stop")

    async def close_pulse() -> None:
        events.append("heartbeat-close")

    async def close_runtime() -> None:
        events.append("chat-close")

    async def shutdown() -> None:
        events.append("mcp-close")

    _startup_dependencies.supervisor.begin_shutdown.side_effect = begin_shutdown
    _startup_dependencies.scheduler.stop.side_effect = stop_scheduler
    _startup_dependencies.pulse.close.side_effect = close_pulse
    runtime.close.side_effect = close_runtime
    _startup_dependencies.supervisor.shutdown.side_effect = shutdown
    engine, connection = _engine()
    storage = Mock(
        probe_startup=AsyncMock(),
        recover_transfer_uploads=AsyncMock(),
        close=AsyncMock(),
    )
    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ) as recover_deletions,
        patch("openctopus_server.main.abandon_running_turns", new_callable=AsyncMock),
    ):
        async with _lifespan(app):
            storage.probe_startup.assert_awaited_once()
            storage.recover_transfer_uploads.assert_awaited_once()
            _startup_dependencies.probe.assert_awaited_once()
            recover_deletions.assert_awaited_once()
            _startup_dependencies.supervisor.start.assert_awaited_once()
            assert app.state.server_mcp_supervisor is (
                _startup_dependencies.supervisor
            )
            assert app.state.cron_scheduler is _startup_dependencies.scheduler
            assert app.state.heartbeat_pulse is _startup_dependencies.pulse

    _startup_dependencies.scheduler.start.assert_awaited_once()
    _startup_dependencies.pulse.start.assert_called_once_with()

    assert connection.execute.await_count == 2
    connection.run_sync.assert_awaited_once()
    runtime.close.assert_awaited_once()
    assert events == [
        "heartbeat-close",
        "cron-stop",
        "mcp-begin",
        "chat-close",
        "mcp-close",
    ]
    _startup_dependencies.supervisor.shutdown.assert_awaited_once()
    runtime.device_registry.close.assert_awaited_once()
    storage.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_channel_recovery_startup_and_two_phase_shutdown_order(
    _startup_dependencies: SimpleNamespace,
) -> None:
    app, runtime = _app_with_runtime()
    events: list[str] = []
    channel_manager = SimpleNamespace(
        startup=AsyncMock(side_effect=lambda: events.append("channel-start")),
        begin_shutdown=AsyncMock(
            side_effect=lambda: events.append("channel-begin")
        ),
        shutdown=AsyncMock(side_effect=lambda: events.append("channel-stop")),
    )
    delivery_router = SimpleNamespace(
        repair_incomplete_deliveries=AsyncMock(
            side_effect=lambda: events.append("delivery-repair")
        )
    )
    ingress = SimpleNamespace(
        close_gate=Mock(side_effect=lambda: events.append("ingress-close")),
        drain=AsyncMock(side_effect=lambda: events.append("ingress-drain")),
    )
    app.state.channel_runtime = channel_manager
    app.state.channel_delivery_router = delivery_router
    app.state.channel_ingress = ingress
    _startup_dependencies.scheduler.start.side_effect = lambda: events.append(
        "cron-start"
    )
    _startup_dependencies.scheduler.stop.side_effect = lambda: events.append(
        "cron-stop"
    )
    _startup_dependencies.pulse.start.side_effect = lambda: events.append(
        "heartbeat-start"
    )
    _startup_dependencies.pulse.close.side_effect = lambda: events.append(
        "heartbeat-close"
    )
    _startup_dependencies.supervisor.begin_shutdown.side_effect = lambda: events.append(
        "mcp-begin"
    )
    _startup_dependencies.supervisor.shutdown.side_effect = lambda: events.append(
        "mcp-close"
    )
    runtime.close.side_effect = lambda: events.append("chat-close")
    engine, _ = _engine()
    storage = _storage()

    async def abandon(*_args: object, **_kwargs: object) -> None:
        events.append("turn-repair")

    _startup_dependencies.pending_recovery.side_effect = lambda *_args: events.append(
        "pending-repair"
    )

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ),
        patch("openctopus_server.main.abandon_running_turns", side_effect=abandon),
    ):
        async with _lifespan(app):
            assert events[:6] == [
                "turn-repair",
                "pending-repair",
                "delivery-repair",
                "channel-start",
                "cron-start",
                "heartbeat-start",
            ]

    assert events[6:] == [
        "ingress-close",
        "ingress-drain",
        "channel-begin",
        "heartbeat-close",
        "cron-stop",
        "mcp-begin",
        "chat-close",
        "channel-stop",
        "mcp-close",
    ]


async def test_lifespan_builds_and_publishes_the_default_channel_stack(
    _startup_dependencies: SimpleNamespace,
) -> None:
    app = FastAPI()
    engine, _ = _engine()
    storage = _storage()
    worker = _deletion_worker()
    device_registry = SimpleNamespace(close=AsyncMock())
    runtime = SimpleNamespace(
        runner_instance_id=uuid4(),
        device_registry=device_registry,
        close=AsyncMock(),
    )
    manager = SimpleNamespace(
        adapter_lookup=Mock(return_value=None),
        is_current_runtime=AsyncMock(return_value=True),
        status=Mock(return_value=None),
        apply=AsyncMock(),
        remove=AsyncMock(),
        startup=AsyncMock(),
        begin_shutdown=AsyncMock(),
        shutdown=AsyncMock(),
    )
    delivery_router = SimpleNamespace(
        repair_incomplete_deliveries=AsyncMock(),
    )
    outbound = Mock()
    ingress = SimpleNamespace(
        close_gate=Mock(),
        drain=AsyncMock(),
        accept_external=AsyncMock(),
    )
    validator = Mock()
    registry = Mock()

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch("openctopus_server.main.WorkspaceDeletionWorker", return_value=worker),
        patch("openctopus_server.main.get_device_registry", return_value=device_registry),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ),
        patch("openctopus_server.main.build_py4_registry", return_value=registry) as build_registry,
        patch("openctopus_server.main.ChatRuntime", return_value=runtime) as build_runtime,
        patch("openctopus_server.main.ChannelManager", return_value=manager),
        patch("openctopus_server.main.ChannelDeliveryRouter", return_value=delivery_router),
        patch("openctopus_server.main.ChannelOutbound", return_value=outbound),
        patch("openctopus_server.main.ChannelIngress", return_value=ingress),
        patch("openctopus_server.main._ChannelCredentialValidator", return_value=validator),
        patch("openctopus_server.main.abandon_running_turns", new_callable=AsyncMock),
    ):
        async with _lifespan(app):
            assert app.state.chat_runtime is runtime
            assert app.state.channel_runtime is manager
            assert app.state.channel_delivery_router is delivery_router
            assert app.state.channel_ingress is ingress
            assert app.state.channel_credential_validator is validator

    build_registry.assert_called_once()
    assert build_registry.call_args.kwargs["message_target_resolver"] is not None
    assert build_registry.call_args.kwargs["message_delivery_router"] is not None
    build_runtime.assert_called_once()
    assert build_runtime.call_args.kwargs["channel_final_delivery"] is outbound
    delivery_router.repair_incomplete_deliveries.assert_awaited_once()
    manager.startup.assert_awaited_once()
    ingress.close_gate.assert_called_once_with()
    ingress.drain.assert_awaited_once_with()
    manager.begin_shutdown.assert_awaited_once()
    manager.shutdown.assert_awaited_once()


async def test_lifespan_fails_startup_when_storage_probe_fails() -> None:
    app, _ = _app_with_runtime()
    engine, _ = _engine()
    storage = SimpleNamespace(
        probe_startup=AsyncMock(
            side_effect=WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                "Object storage is unavailable",
            )
        ),
        close=AsyncMock(),
    )

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ),
        pytest.raises(SystemExit),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    storage.probe_startup.assert_awaited_once()
    storage.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_lifespan_disposes_database_when_bootstrap_fails() -> None:
    app, _ = _app_with_runtime()
    engine, _ = _engine()
    engine.begin.side_effect = RuntimeError("database unavailable")

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        pytest.raises(SystemExit),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    engine.dispose.assert_awaited_once()


async def test_lifespan_disposes_database_when_storage_construction_fails() -> None:
    app, _ = _app_with_runtime()
    engine, _ = _engine()

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch(
            "openctopus_server.main.get_object_storage",
            side_effect=ValueError("invalid endpoint"),
        ),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ),
        pytest.raises(SystemExit),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    engine.dispose.assert_awaited_once()


async def test_lifespan_cancels_a_probe_after_the_outer_timeout() -> None:
    app, _ = _app_with_runtime()
    engine, _ = _engine()
    storage = SimpleNamespace(
        probe_startup=AsyncMock(side_effect=lambda: None),
        close=AsyncMock(),
    )

    async def never_finishes() -> None:
        await asyncio.Event().wait()

    storage.probe_startup.side_effect = never_finishes
    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch("openctopus_server.main.STARTUP_PROBE_TIMEOUT_SECONDS", 0.01),
        pytest.raises(SystemExit),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    storage.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_lifespan_cleans_resources_when_turn_recovery_fails() -> None:
    app, runtime = _app_with_runtime()
    engine, _ = _engine()
    storage = Mock(
        probe_startup=AsyncMock(),
        recover_transfer_uploads=AsyncMock(),
        close=AsyncMock(),
    )
    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ),
        patch(
            "openctopus_server.main.abandon_running_turns",
            new_callable=AsyncMock,
            side_effect=RuntimeError("recovery failed"),
        ),
        pytest.raises(RuntimeError, match="recovery failed"),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    runtime.close.assert_awaited_once()
    runtime.device_registry.close.assert_awaited_once()
    storage.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_lifespan_cleans_resources_when_workspace_service_fails() -> None:
    app = FastAPI()
    engine, _ = _engine()
    storage = _storage()
    worker = _deletion_worker()

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch("openctopus_server.main.WorkspaceDeletionWorker", return_value=worker),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ),
        patch(
            "openctopus_server.main.WorkspaceService",
            side_effect=RuntimeError("workspace service failed"),
        ),
        pytest.raises(RuntimeError, match="workspace service failed"),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    worker.start.assert_called_once()
    worker.close.assert_awaited_once()
    storage.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_lifespan_cleans_resources_when_registry_build_fails() -> None:
    app = FastAPI()
    engine, _ = _engine()
    storage = _storage()
    worker = _deletion_worker()
    registry = SimpleNamespace(close=AsyncMock())

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch("openctopus_server.main.WorkspaceDeletionWorker", return_value=worker),
        patch("openctopus_server.main.get_device_registry", return_value=registry),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ),
        patch(
            "openctopus_server.main.build_py4_registry",
            side_effect=RuntimeError("tool registry failed"),
        ),
        pytest.raises(RuntimeError, match="tool registry failed"),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    worker.close.assert_awaited_once()
    registry.close.assert_awaited_once()
    storage.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_lifespan_cleans_resources_when_runtime_construction_fails() -> None:
    app = FastAPI()
    engine, _ = _engine()
    storage = _storage()
    worker = _deletion_worker()
    registry = SimpleNamespace(close=AsyncMock())

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine", return_value=engine),
        patch("openctopus_server.main.get_object_storage", return_value=storage),
        patch("openctopus_server.main.WorkspaceDeletionWorker", return_value=worker),
        patch("openctopus_server.main.get_device_registry", return_value=registry),
        patch(
            "openctopus_server.main.recover_workspace_deletions",
            new_callable=AsyncMock,
        ),
        patch("openctopus_server.main.build_py4_registry", return_value=Mock()),
        patch(
            "openctopus_server.main.ChatRuntime",
            side_effect=RuntimeError("runtime failed"),
        ),
        pytest.raises(RuntimeError, match="runtime failed"),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    worker.close.assert_awaited_once()
    registry.close.assert_awaited_once()
    storage.close.assert_awaited_once()
    engine.dispose.assert_awaited_once()


async def test_lifespan_fails_before_database_when_content_probe_fails(
    _startup_dependencies: SimpleNamespace,
) -> None:
    app, _ = _app_with_runtime()
    _startup_dependencies.probe.side_effect = RuntimeError("missing converter")

    with (
        patch("openctopus_server.main.get_settings", return_value=_settings()),
        patch("openctopus_server.main.get_engine") as get_engine,
        pytest.raises(SystemExit),
    ):
        async with _lifespan(app):
            pytest.fail("lifespan should not start")

    _startup_dependencies.probe.assert_awaited_once()
    get_engine.assert_not_called()
