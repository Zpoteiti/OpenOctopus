"""DBOS execution ownership and transactionally accepted conversation work."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import asdict, replace
from typing import TYPE_CHECKING, Any
from uuid import UUID

from dbos import DBOS, DBOSClient, EnqueueOptions
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.types import TurnStart
from openctopus_server.db.models import AgentTask, ModelConfiguration, TurnRun, WorkflowCancellation
from openctopus_server.mcp.routes import CompositeMcpSnapshot
from openctopus_server.provider.config import ProviderConfig, load_provider_config
from openctopus_server.services.messages import capture_pending_for_turn, reserve_pending_turn

if TYPE_CHECKING:
    from openctopus_server.chat.runner import ChatRuntime, _PreparedTurn

_runtime: ChatRuntime | None = None
_clients: dict[str, DBOSClient] = {}


def current_runtime() -> ChatRuntime:
    assert _runtime is not None
    return _runtime


def _client(db: AsyncSession) -> DBOSClient:
    url = db.get_bind().engine.url.set(drivername="postgresql+psycopg").render_as_string(hide_password=False)
    if url not in _clients:
        _clients[url] = DBOSClient(system_database_url=url, application_name="openoctopus", lazy=True)
    return _clients[url]


async def enqueue_transaction(db: AsyncSession, options: EnqueueOptions, *args: Any) -> None:
    client = _client(db)
    connection = await db.connection()
    await connection.run_sync(lambda sync: client.enqueue_in_transaction(sync, options, *args))


@DBOS.workflow(name="oo.cleanup_memory")
async def cleanup_memory(user_id: UUID) -> None:
    await purge_memory(user_id)


@DBOS.step(name="oo.purge_memory")
async def purge_memory(user_id: UUID) -> None:
    await current_runtime().memory.purge(user_id)


async def enqueue_turn(db: AsyncSession, turn: TurnStart) -> None:
    """The caller's input and workflow become visible in the same commit."""
    run = await db.get(TurnRun, turn.turn_id)
    assert run is not None
    run.workflow_id = str(turn.turn_id)
    client = _client(db)
    connection = await db.connection()
    await connection.run_sync(lambda sync: client.enqueue_in_transaction(
        sync, {
            "workflow_name": "oo.conversation", "workflow_id": str(turn.turn_id),
            "queue_name": "oo-conversations", "queue_partition_key": str(turn.session_id),
            "app_version": "oo-harness-1",
        }, turn,
    ))


@DBOS.workflow(name="oo.conversation")
async def conversation(turn: TurnStart) -> None:
    assert _runtime is not None
    async with _runtime._lease_state(turn.session_id) as state:
        assert state is not None
        await _runtime._run_session(state, initial_turn=turn)


@DBOS.step(name="oo.prepare_turn")
async def prepare_turn(runtime_id: UUID, turn: TurnStart, model_revision: str) -> _PreparedTurn:
    # The runtime id routes dependency-injected tests; production recovery always
    # binds the sole active runtime on startup, independent of the old process id.
    from openctopus_server.chat.runner import runtime_for
    runtime = _runtime or runtime_for(runtime_id)
    async with AsyncSession(runtime.engine, expire_on_commit=False) as db:
        turn = await capture_pending_for_turn(db, turn=turn)
    prepared = await runtime._prepare_turn(turn, config=runtime._model_configurations[model_revision])
    mcp = prepared.mcp_snapshot
    if isinstance(mcp, CompositeMcpSnapshot):
        mcp = replace(mcp, suppression_by_entry=dict(mcp.suppression_by_entry))
    return replace(prepared, config=replace(prepared.config, api_key=""), model_id=model_revision, mcp_snapshot=mcp)


async def snapshot_model(runtime: ChatRuntime, config: ProviderConfig) -> str:
    value = asdict(config)
    revision = hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
    async with AsyncSession(runtime.engine, expire_on_commit=False) as db:
        await db.execute(insert(ModelConfiguration).values(id=revision, value=value).on_conflict_do_nothing())
        await db.commit()
    runtime._model_configurations[revision] = config
    return revision


@DBOS.step(name="oo.initial_model")
async def initial_model(runtime_id: UUID) -> str:
    from openctopus_server.chat.runner import runtime_for
    runtime = _runtime or runtime_for(runtime_id)
    async with AsyncSession(runtime.engine) as db:
        config = await load_provider_config(db)
    return await snapshot_model(runtime, config)


@DBOS.step(name="oo.reserve_pending", retries_allowed=True, max_attempts=3, interval_seconds=0.25)
async def reserve_pending(runtime_id: UUID, session_id: UUID, reservation_id: UUID, workflow_id: str) -> TurnStart | None:
    from openctopus_server.chat.runner import runtime_for
    runtime = _runtime or runtime_for(runtime_id)
    async with AsyncSession(runtime.engine, expire_on_commit=False) as db:
        turn = await reserve_pending_turn(
            db, session_id=session_id, runner_instance_id=runtime.runner_instance_id,
            reservation_turn_id=reservation_id,
        )
        if turn is not None:
            run = await db.get(TurnRun, turn.turn_id)
            assert run is not None
            run.workflow_id = workflow_id
            await db.commit()
        return turn


class DurableHost:
    def __init__(self, runtime: ChatRuntime) -> None:
        self.runtime = runtime
        self.started = False
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        async with self.lock:
            await self._start()

    async def _start(self) -> None:
        global _runtime
        if self.started:
            return
        _runtime = self.runtime
        async with AsyncSession(self.runtime.engine) as db:
            for row in (await db.scalars(select(ModelConfiguration))).all():
                self.runtime._model_configurations[row.id] = ProviderConfig(**row.value)
        DBOS(config={
            "name": "openoctopus",
            "system_database_url": self.runtime.engine.url.set(drivername="postgresql+psycopg").render_as_string(hide_password=False),
            "application_version": "oo-harness-1",
            "executor_id": "openoctopus-server",
            "enable_otlp": False,
        })
        await self.apply_cancellations()
        DBOS.launch()
        await DBOS.register_queue_async("oo-conversations", partition_concurrency=1)
        await DBOS.register_queue_async("oo-maintenance", worker_concurrency=4)
        await DBOS.register_queue_async("oo-cron", partition_concurrency=1)
        self.started = True

    async def apply_cancellations(self) -> None:
        from openctopus_server.chat.cancellation import reconcile_cancellation
        async with AsyncSession(self.runtime.engine, expire_on_commit=False) as db:
            cancellations = list((await db.scalars(select(WorkflowCancellation))).all())
            for cancellation in cancellations:
                await _client(db).cancel_workflow_async(cancellation.workflow_id, cancel_children=True)
                result = await reconcile_cancellation(db, cancellation.session_id, cancellation.workflow_id)
                task = await db.get(AgentTask, cancellation.session_id)
                if task is not None and task.status == "running":
                    task.status = "cancelled"
                    await db.commit()
                if result is not None:
                    turn, rows, marker = result
                    async with self.runtime._lease_state(turn.session_id, create=False) as state:
                        if state is not None:
                            for row in [*rows, marker]:
                                await self.runtime._publish_message(state, turn, row)
                            await self.runtime._publish_turn_finished(state, turn, status="cancelled", final_message_id=marker.id)
                            await self.runtime._close_turn_subscriber(state, turn.turn_id)

    async def submit(self, turn: TurnStart) -> None:
        # Activation may race a late browser subscription. Stable workflow IDs
        # make this notification safe after the transactional ingress enqueue.
        await self.start()
        await DBOS.enqueue_workflow_with_options_async({
            "workflow_name": "oo.conversation", "workflow_id": str(turn.turn_id),
            "queue_name": "oo-conversations", "queue_partition_key": str(turn.session_id),
            "app_version": "oo-harness-1",
        }, turn)

    async def close(self) -> None:
        global _runtime
        if self.started:
            await asyncio.to_thread(DBOS.destroy)
            self.started = False
            _runtime = None
        for client in _clients.values():
            client.destroy()
        _clients.clear()
