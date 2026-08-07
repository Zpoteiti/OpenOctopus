import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from openctopus_server.api.router import router as api_router
from openctopus_server.chat.runner import ChatRuntime, get_context_admission
from openctopus_server.chat.token_estimator import initialize_token_estimator
from openctopus_server.config import get_settings
from openctopus_server.db.base import Base
from openctopus_server.db.engine import get_engine
from openctopus_server.errors.http import register_error_handler
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
from openctopus_server.workspace.storage import get_object_storage

STARTUP_PROBE_TIMEOUT_SECONDS = 60.0


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
        sys.exit(1)

    object_storage = None
    try:
        object_storage = get_object_storage()
        await asyncio.wait_for(
            object_storage.probe_startup(),
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

    deletion_worker = WorkspaceDeletionWorker(
        engine,
        workspace_fs,
        purge_storage=WorkspacePurgeStorageConfig.from_settings(settings),
        purge_timeout_seconds=settings.workspace_deletion_purge_timeout_seconds,
        shutdown_grace_seconds=settings.workspace_deletion_shutdown_grace_seconds,
    )
    deletion_worker.start()

    runtime = getattr(app.state, "chat_runtime", None)
    if runtime is None:
        workspace_service = WorkspaceService(workspace_fs)
        runtime = ChatRuntime(
            engine,
            workspace_service=workspace_service,
            tool_registry=build_py4_registry(
                engine,
                workspace_service,
                web_admission=get_web_fetch_admission(),
                content_converter=content_converter,
            ),
            context_admission=get_context_admission(),
        )
        app.state.chat_runtime = runtime
    try:
        await abandon_running_turns(
            engine,
            runner_instance_id=runtime.runner_instance_id,
        )
    except BaseException:
        try:
            await runtime.close()
        finally:
            try:
                await deletion_worker.close()
            finally:
                try:
                    await object_storage.close()
                finally:
                    await engine.dispose()
        raise

    try:
        yield
    finally:
        try:
            await runtime.close()
        finally:
            try:
                await deletion_worker.close()
            finally:
                try:
                    await object_storage.close()
                finally:
                    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="OpenOctopus", lifespan=_lifespan)
    app.include_router(api_router)
    register_error_handler(app)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(app, host=settings.host, port=settings.port)
