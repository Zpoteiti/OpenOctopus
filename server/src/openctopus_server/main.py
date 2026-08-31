import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.api.router import router as api_router
from openctopus_server.automations.cron import CronScheduler
from openctopus_server.automations.heartbeat import HeartbeatPulse
from openctopus_server.chat.runner import ChatRuntime, get_context_admission
from openctopus_server.chat.token_estimator import initialize_token_estimator
from openctopus_server.config import get_settings
from openctopus_server.db.base import Base
from openctopus_server.db.engine import get_engine
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.protocol import MAX_TEXT_FRAME_BYTES
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.http import register_error_handler
from openctopus_server.frontend import FRONTEND_BUILD_DIR, install_frontend
from openctopus_server.mcp.authority import ServerMcpAuthorityFence
from openctopus_server.mcp.models import ServerMcpEnvelope, empty_server_mcp_envelope
from openctopus_server.mcp.supervisor import ServerMcpSupervisor
from openctopus_server.services.heartbeat import publish_heartbeat_phase_two
from openctopus_server.services.server_mcp import load_envelope as load_server_mcp_envelope
from openctopus_server.services.turn_runs import abandon_running_turns
from openctopus_server.services.workspace_deletions import (
    WorkspaceDeletionWorker,
    recover_workspace_deletions,
)
from openctopus_server.services.workspace_purge import WorkspacePurgeStorageConfig
from openctopus_server.tools.registry import (
    build_py4_registry,
    get_content_converter,
    get_web_fetch_admission,
)
from openctopus_server.workspace.fs import _workspace_fs_for_storage
from openctopus_server.workspace.service import WorkspaceService
from openctopus_server.workspace.storage import ObjectStorage, get_object_storage

STARTUP_PROBE_TIMEOUT_SECONDS = 60.0
DEVICE_WS_MAX_SIZE = MAX_TEXT_FRAME_BYTES
DEVICE_WS_MAX_QUEUE = 1
DEVICE_WS_PER_MESSAGE_DEFLATE = False
DEVICE_WS_PROTOCOL_PING_INTERVAL = None
_DOMAIN_422_OPERATIONS = {
    ("/api/admin/server-mcp", "put"),
    ("/api/devices/{name}/config", "patch"),
    ("/api/workspace/files/{path}", "put"),
    ("/api/workspace/files/{path}", "patch"),
    ("/api/workspace/patch", "post"),
    ("/api/workspace/transfer", "post"),
}
_VALIDATION_422_OPERATIONS = {
    ("/api/admin/server-mcp", "put"),
}


class OpenOctopusAPI(FastAPI):
    def openapi(self) -> dict[str, Any]:
        schema = super().openapi()
        _normalize_validation_responses(schema)
        return schema


def _normalize_validation_responses(schema: dict[str, Any]) -> None:
    for path, path_item in schema.get("paths", {}).items():
        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue
            responses = operation.get("responses")
            if not isinstance(responses, dict):
                continue
            validation = responses.get("422")
            if not _is_generated_validation_response(validation):
                continue
            key = (path, method)
            if key not in _VALIDATION_422_OPERATIONS:
                responses.setdefault("400", _validation_error_response())
            if key in _DOMAIN_422_OPERATIONS:
                responses["422"] = _validation_error_response()
            else:
                responses.pop("422")

    components = schema.get("components", {}).get("schemas", {})
    components.pop("HTTPValidationError", None)
    components.pop("ValidationError", None)


def _validation_error_response() -> dict[str, Any]:
    return {
        "description": "Invalid request.",
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/ErrorResponse"},
            }
        },
    }


def _is_generated_validation_response(response: object) -> bool:
    if not isinstance(response, dict):
        return False
    try:
        ref = response["content"]["application/json"]["schema"]["$ref"]
    except (KeyError, TypeError):
        return False
    return isinstance(ref, str) and ref == "#/components/schemas/HTTPValidationError"


async def _close_lifespan_resources(
    *,
    heartbeat_pulse: HeartbeatPulse | None,
    cron_scheduler: CronScheduler | None,
    server_mcp_supervisor: ServerMcpSupervisor | None,
    runtime: ChatRuntime | None,
    device_registry: DeviceRegistry | None,
    deletion_worker: WorkspaceDeletionWorker | None,
    object_storage: ObjectStorage | None,
    engine: AsyncEngine | None,
) -> None:
    try:
        if heartbeat_pulse is not None:
            await heartbeat_pulse.close()
    finally:
        try:
            if cron_scheduler is not None:
                await cron_scheduler.stop()
        finally:
            try:
                if server_mcp_supervisor is not None:
                    await server_mcp_supervisor.begin_shutdown()
            finally:
                try:
                    if runtime is not None:
                        await runtime.close()
                finally:
                    try:
                        if server_mcp_supervisor is not None:
                            await server_mcp_supervisor.shutdown()
                    finally:
                        try:
                            if device_registry is not None:
                                await device_registry.close()
                        finally:
                            try:
                                if deletion_worker is not None:
                                    await deletion_worker.close()
                            finally:
                                try:
                                    if object_storage is not None:
                                        await object_storage.close()
                                finally:
                                    if engine is not None:
                                        await engine.dispose()


async def _load_server_mcp_authority(engine: AsyncEngine) -> ServerMcpEnvelope:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        envelope = await load_server_mcp_envelope(db)
        await db.rollback()
        return envelope


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    try:
        settings = get_settings()
        initialize_token_estimator()
    except Exception as exc:
        print(f"Config validation failed: {exc}", file=sys.stderr)
        sys.exit(1)

    content_converter = get_content_converter()
    try:
        await content_converter.probe()
    except Exception as exc:
        print(f"Content conversion probe failed: {exc}", file=sys.stderr)
        sys.exit(1)

    engine = get_engine()
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:
        print(f"Database bootstrap failed: {exc}", file=sys.stderr)
        await _close_lifespan_resources(
            heartbeat_pulse=None,
            cron_scheduler=None,
            server_mcp_supervisor=None,
            runtime=None,
            device_registry=None,
            deletion_worker=None,
            object_storage=None,
            engine=engine,
        )
        sys.exit(1)

    object_storage = None
    try:
        object_storage = get_object_storage()
        await asyncio.wait_for(
            object_storage.probe_startup(),
            timeout=STARTUP_PROBE_TIMEOUT_SECONDS,
        )
        await asyncio.wait_for(
            object_storage.recover_transfer_uploads(),
            timeout=STARTUP_PROBE_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        print(f"Object storage probe failed: {exc}", file=sys.stderr)
        if object_storage is not None:
            try:
                await object_storage.close()
            except Exception:
                pass
        await engine.dispose()
        sys.exit(1)

    workspace_fs = _workspace_fs_for_storage(object_storage)
    try:
        await recover_workspace_deletions(
            engine,
            workspace_fs,
        )
    except Exception as exc:
        print(f"Workspace deletion recovery failed: {exc}", file=sys.stderr)
        try:
            await object_storage.close()
        except Exception:
            pass
        await engine.dispose()
        sys.exit(1)

    deletion_worker: WorkspaceDeletionWorker | None = None
    server_mcp_supervisor: ServerMcpSupervisor | None = None
    cron_scheduler: CronScheduler | None = None
    heartbeat_pulse: HeartbeatPulse | None = None
    server_mcp_authority = getattr(app.state, "server_mcp_authority", None)
    if server_mcp_authority is None:
        server_mcp_authority = ServerMcpAuthorityFence(empty_server_mcp_envelope())
        app.state.server_mcp_authority = server_mcp_authority
    runtime = getattr(app.state, "chat_runtime", None)
    device_registry = getattr(runtime, "device_registry", None)
    cron_wake_event = asyncio.Event()
    try:
        deletion_worker = WorkspaceDeletionWorker(
            engine,
            workspace_fs,
            purge_storage=WorkspacePurgeStorageConfig.from_settings(settings),
            purge_timeout_seconds=settings.workspace_deletion_purge_timeout_seconds,
            shutdown_grace_seconds=settings.workspace_deletion_shutdown_grace_seconds,
        )
        deletion_worker.start()

        server_mcp_supervisor = getattr(
            app.state,
            "server_mcp_supervisor",
            None,
        )
        if server_mcp_supervisor is None:
            server_mcp_supervisor = ServerMcpSupervisor.create_default()
            envelope = await _load_server_mcp_authority(engine)
            server_mcp_authority.publish(envelope)
            await server_mcp_supervisor.start(envelope)
            app.state.server_mcp_supervisor = server_mcp_supervisor

        workspace_service = WorkspaceService(workspace_fs)
        if runtime is None:
            device_registry = get_device_registry()
            runtime = ChatRuntime(
                engine,
                workspace_service=workspace_service,
                tool_registry=build_py4_registry(
                    engine,
                    workspace_service,
                    workspace_fs,
                    web_admission=get_web_fetch_admission(),
                    content_converter=content_converter,
                    device_registry=device_registry,
                    server_mcp_dispatcher=server_mcp_supervisor,
                    server_mcp_authority=server_mcp_authority,
                    cron_wake=cron_wake_event.set,
                ),
                context_admission=get_context_admission(),
                device_registry=device_registry,
                server_mcp_generation_resolver=(
                    server_mcp_supervisor.ready_generations
                ),
            )
            app.state.chat_runtime = runtime
        await abandon_running_turns(
            engine,
            runner_instance_id=runtime.runner_instance_id,
        )
        cron_scheduler = CronScheduler(
            engine,
            runtime,
            wake_event=cron_wake_event,
        )
        await cron_scheduler.start()
        app.state.cron_scheduler = cron_scheduler
        heartbeat_pulse = HeartbeatPulse(
            engine=engine,
            runtime=runtime,
            workspace_service=workspace_service,
            publish_phase_two=lambda request: publish_heartbeat_phase_two(
                engine,
                runtime,
                request,
            ),
        )
        heartbeat_pulse.start()
        app.state.heartbeat_pulse = heartbeat_pulse
    except BaseException:
        await _close_lifespan_resources(
            heartbeat_pulse=heartbeat_pulse,
            cron_scheduler=cron_scheduler,
            server_mcp_supervisor=server_mcp_supervisor,
            runtime=runtime,
            device_registry=device_registry,
            deletion_worker=deletion_worker,
            object_storage=object_storage,
            engine=engine,
        )
        raise

    try:
        yield
    finally:
        await _close_lifespan_resources(
            heartbeat_pulse=heartbeat_pulse,
            cron_scheduler=cron_scheduler,
            server_mcp_supervisor=server_mcp_supervisor,
            runtime=runtime,
            device_registry=device_registry,
            deletion_worker=deletion_worker,
            object_storage=object_storage,
            engine=engine,
        )


def create_app(*, frontend_dir: Path | None = FRONTEND_BUILD_DIR) -> FastAPI:
    app = OpenOctopusAPI(title="OpenOctopus", lifespan=_lifespan)
    app.state.server_mcp_authority = ServerMcpAuthorityFence(
        empty_server_mcp_envelope()
    )
    app.include_router(api_router)
    register_error_handler(app)
    install_frontend(app, frontend_dir)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        ws_max_size=DEVICE_WS_MAX_SIZE,
        ws_max_queue=DEVICE_WS_MAX_QUEUE,
        ws_per_message_deflate=DEVICE_WS_PER_MESSAGE_DEFLATE,
        ws_ping_interval=DEVICE_WS_PROTOCOL_PING_INTERVAL,
    )
