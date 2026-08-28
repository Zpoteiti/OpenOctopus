import asyncio
import inspect
import json
import uuid
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from functools import lru_cache
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.admission import AdmissionTimeoutError, KeyedAdmission
from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.chat.attachments import (
    build_device_attachment_targets,
    fence_owner_device_targets,
)
from openctopus_server.chat.compaction import (
    StaleCompactionSelectionError,
    commit_stage_one,
    commit_stage_two,
    compaction_max_output_tokens,
    compaction_required,
    stage_one_source_ids,
    stage_two_source_ids,
)
from openctopus_server.chat.context import (
    build_provider_context,
    project_message_rows,
    project_provider_messages,
)
from openctopus_server.chat.device_snapshot import (
    OwnerDeviceSnapshot,
    load_owner_device_snapshot,
)
from openctopus_server.chat.public_projection import message_response
from openctopus_server.chat.repair import repair_unpaired_tool_uses
from openctopus_server.chat.stream import StreamSubscriber
from openctopus_server.chat.token_estimator import estimate_request_tokens
from openctopus_server.chat.types import AcceptedMessage, TurnStart
from openctopus_server.config import get_settings
from openctopus_server.db.models import Message, PendingMessage, Session, TurnRun
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.mcp_routes import (
    OwnerMcpDevice,
    OwnerMcpSnapshot,
    build_owner_mcp_snapshot,
)
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.mcp.models import ServerMcpEnvelope
from openctopus_server.mcp.routes import (
    CompositeMcpSnapshot,
    build_composite_mcp_snapshot,
)
from openctopus_server.provider.anthropic import (
    AnthropicProvider,
    Provider,
    ProviderInvocationError,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig, load_provider_config
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.services.messages import (
    cancel_tool_batch,
    capture_pending_for_turn,
    discard_cancel_waiter,
    finish_final_turn,
    finish_tool_batch_and_continue,
    is_cancel_requested,
    persist_assistant,
    persist_human_marker,
    persist_tool_result,
    promote_pending_for_turn,
    register_cancel_waiter,
    reserve_pending_turn,
)
from openctopus_server.services.server_mcp import load_envelope as load_server_mcp_envelope
from openctopus_server.tools.base import (
    MessageDeliveryEffect,
    ToolContext,
    WorkspaceFileDeliveryRef,
)
from openctopus_server.tools.registry import ToolRegistry, build_py3_registry
from openctopus_server.workspace.service import WorkspaceService
from openctopus_server.workspace.skills import get_skills_cache

ProviderFactory = Callable[[ProviderConfig], Provider]
RequestTokenEstimator = Callable[..., int | Awaitable[int]]
ServerMcpGenerationResolver = Callable[
    [ServerMcpEnvelope], Mapping[str, UUID | None]
]

_MAX_ITERATIONS = 200
_MCP_AUTHORITY_SNAPSHOT_ATTEMPTS = 3
_COMPACTION_SYSTEM = (
    "Summarize the conversation state for another assistant that will continue it. "
    "Preserve user intent, constraints, decisions, completed work, tool findings, "
    "open questions, and errors. Do not add new instructions or commentary."
)
_COMPACTION_REQUEST = "Write the compacted summary now."


@lru_cache
def get_context_admission() -> KeyedAdmission:
    settings = get_settings()
    return KeyedAdmission(
        global_limit=settings.chat_context_max_concurrency,
        per_key_limit=settings.chat_context_max_concurrency_per_user,
        timeout_seconds=settings.chat_context_queue_timeout_seconds,
    )


@dataclass(slots=True)
class _SessionState:
    session_id: UUID
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    leases: int = 0
    starts: deque[TurnStart] = field(default_factory=deque)
    runner_task: asyncio.Task[None] | None = None
    turn_subscribers: dict[UUID, StreamSubscriber] = field(default_factory=dict)
    queued_subscribers: dict[UUID, StreamSubscriber] = field(default_factory=dict)
    active_turn_id: UUID | None = None
    active_preview_message_ids: frozenset[UUID] = field(default_factory=frozenset)


@dataclass(slots=True)
class _SessionOperation:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    leases: int = 0


@dataclass(frozen=True, slots=True)
class DetachedSession:
    session_id: UUID
    subscribers: tuple[StreamSubscriber, ...]


@dataclass(frozen=True, slots=True)
class _PreparedTurn:
    turn: TurnStart
    config: ProviderConfig
    system: str
    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]]
    user_id: UUID
    device_targets: dict[str, UUID]
    mcp_snapshot: OwnerMcpSnapshot | CompositeMcpSnapshot


@dataclass(frozen=True, slots=True)
class _CompletedProviderTurn:
    turn: TurnStart
    assistant: Message
    user_id: UUID
    device_targets: dict[str, UUID]
    mcp_snapshot: OwnerMcpSnapshot | CompositeMcpSnapshot


@dataclass(frozen=True, slots=True)
class _UnhandledProviderFailure:
    pass


async def _load_mcp_authority_snapshot(
    db: AsyncSession,
    *,
    user_id: UUID,
) -> tuple[ServerMcpEnvelope, tuple[OwnerDeviceSnapshot, ...]]:
    """Capture Server and owner Device MCP authority from one stable interval."""

    for _attempt in range(_MCP_AUTHORITY_SNAPSHOT_ATTEMPTS):
        before = await load_server_mcp_envelope(db)
        devices = await load_owner_device_snapshot(db, user_id=user_id)
        after = await load_server_mcp_envelope(db)
        if (
            before.config_revision == after.config_revision
            and before.mcp_catalog.digest == after.mcp_catalog.digest
        ):
            return after, devices
    raise RuntimeError("Server MCP authority changed repeatedly while preparing a turn")


def _build_owner_tool_state(
    devices: Sequence[OwnerDeviceSnapshot],
    *,
    tool_registry: ToolRegistry,
    attachment_targets: Mapping[str, UUID | None] | None = None,
    server_envelope: ServerMcpEnvelope | None = None,
    runtime_generations: Mapping[str, UUID | None] | None = None,
) -> tuple[
    dict[str, UUID],
    OwnerMcpSnapshot | CompositeMcpSnapshot,
    list[dict[str, Any]],
]:
    attachment_targets = attachment_targets or {}
    device_targets, device_sites = fence_owner_device_targets(
        {device.name: device.id for device in devices},
        attachment_targets,
    )
    owner_authority = [
        OwnerMcpDevice(
            device_id=device.id,
            name=device.name,
            config_revision=device.config_revision,
            catalog=device.mcp_catalog,
        )
        for device in _attachment_fenced_devices(devices, attachment_targets)
    ]
    mcp_snapshot: OwnerMcpSnapshot | CompositeMcpSnapshot
    if server_envelope is None:
        mcp_snapshot = build_owner_mcp_snapshot(
            owner_authority,
            built_in_names=tool_registry.tool_names,
        )
    else:
        mcp_snapshot = build_composite_mcp_snapshot(
            server_envelope,
            owner_authority,
            built_in_names=tool_registry.tool_names,
            runtime_generations=runtime_generations,
        )
    registry_schemas = tool_registry.get_tool_schemas(
        device_names=device_sites,
        mcp_snapshot=mcp_snapshot,
    )
    return device_targets, mcp_snapshot, registry_schemas


def _attachment_fenced_devices(
    devices: Sequence[OwnerDeviceSnapshot],
    attachment_targets: Mapping[str, UUID | None],
) -> tuple[OwnerDeviceSnapshot, ...]:
    return tuple(
        device
        for device in devices
        if device.name not in attachment_targets
        or attachment_targets[device.name] == device.id
    )


_UNHANDLED_PROVIDER_FAILURE = _UnhandledProviderFailure()


class ChatRuntime:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        provider_factory: ProviderFactory | None = None,
        tool_registry: ToolRegistry | None = None,
        workspace_service: WorkspaceService | None = None,
        device_registry: DeviceRegistry | None = None,
        context_admission: KeyedAdmission | None = None,
        request_token_estimator: RequestTokenEstimator = estimate_request_tokens,
        server_mcp_generation_resolver: ServerMcpGenerationResolver | None = None,
    ) -> None:
        self.engine = engine
        self.runner_instance_id = uuid.uuid4()
        self.limiter = ProviderLimiter()
        self.tool_registry = tool_registry or build_py3_registry(engine=engine)
        self.workspace_service = workspace_service
        self.device_registry = device_registry or get_device_registry()
        self.context_admission = context_admission or get_context_admission()
        self._estimate_request_tokens = request_token_estimator
        self._server_mcp_generation_resolver = server_mcp_generation_resolver
        self.skills_cache = get_skills_cache()
        self._provider_factory = provider_factory or AnthropicProvider
        self._providers: dict[tuple[str, str], Provider] = {}
        self._provider_lock = asyncio.Lock()
        self._activation_tasks: set[asyncio.Task[None]] = set()
        self._states: dict[UUID, _SessionState] = {}
        self._states_lock = asyncio.Lock()
        self._session_operations: dict[UUID, _SessionOperation] = {}

    def set_provider_factory(self, factory: ProviderFactory) -> None:
        if self._providers:
            raise RuntimeError("Cannot replace provider factory after provider use")
        self._provider_factory = factory

    async def schedule(self, accepted: AcceptedMessage) -> None:
        if accepted.turn is None:
            return
        task = asyncio.create_task(
            self._schedule_turn(accepted.turn),
            name=f"chat-activate-{accepted.turn.turn_id}",
        )
        self._activation_tasks.add(task)
        task.add_done_callback(self._activation_tasks.discard)
        await await_future_cancellation_safe(task)

    @asynccontextmanager
    async def session_operation(self, session_id: UUID) -> AsyncIterator[None]:
        operation = self._session_operations.get(session_id)
        if operation is None:
            operation = _SessionOperation()
            self._session_operations[session_id] = operation
        operation.leases += 1
        try:
            async with operation.lock:
                yield
        finally:
            operation.leases -= 1
            if (
                operation.leases == 0
                and self._session_operations.get(session_id) is operation
            ):
                self._session_operations.pop(session_id)

    async def terminate_session(self, session_id: UUID) -> None:
        detached = await self.detach_session(session_id)
        self.finalize_detached_session(detached, deleted=True)

    async def detach_session(self, session_id: UUID) -> DetachedSession:
        async with self._states_lock:
            state = self._states.pop(session_id, None)
        if state is None:
            return DetachedSession(session_id=session_id, subscribers=())

        async with state.lock:
            task = state.runner_task
            state.runner_task = None
            state.starts.clear()
            subscribers = [
                *state.turn_subscribers.values(),
                *state.queued_subscribers.values(),
            ]
            state.turn_subscribers.clear()
            state.queued_subscribers.clear()
            state.active_turn_id = None
            state.active_preview_message_ids = frozenset()

        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return DetachedSession(
            session_id=session_id,
            subscribers=tuple(subscribers),
        )

    def finalize_detached_session(
        self,
        detached: DetachedSession,
        *,
        deleted: bool,
    ) -> None:
        event = {
            "type": "session_deleted",
            "session_id": str(detached.session_id),
        }
        for subscriber in detached.subscribers:
            if deleted:
                subscriber.send(event)
            subscriber.close()

    async def register(self, accepted: AcceptedMessage) -> StreamSubscriber:
        subscriber = StreamSubscriber(
            message_id=accepted.message_id,
            accepted_at=accepted.accepted_at,
        )
        subscriber.send(
            {
                "type": "message_accepted",
                "message_id": str(accepted.message_id),
                "disposition": accepted.disposition,
                "created_session": accepted.created_session,
            }
        )
        async with self._lease_state(accepted.session_id) as state:
            assert state is not None
            async with state.lock:
                if accepted.turn is None:
                    location, running_turn_id = await self._queued_location(accepted)
                    if location == "running" and running_turn_id is not None:
                        if (
                            state.active_turn_id != running_turn_id
                            or accepted.message_id not in state.active_preview_message_ids
                        ):
                            subscriber.close()
                            return subscriber
                        self._install_newest_subscriber(
                            state,
                            turn_id=running_turn_id,
                            subscriber=subscriber,
                        )
                        return subscriber
                    if location == "done":
                        subscriber.close()
                        return subscriber
                    self._queue_subscriber(state, subscriber)
                    return subscriber

                if not await self._turn_is_running(accepted.turn.turn_id):
                    subscriber.close()
                    return subscriber
                self._set_active_turn(state, accepted.turn, inherit_preview=False)
                candidates = [
                    subscriber,
                    *self._take_queued_subscribers(state, accepted.turn.message_ids),
                ]
                for candidate in candidates:
                    self._install_newest_subscriber(
                        state,
                        turn_id=accepted.turn.turn_id,
                        subscriber=candidate,
                    )
        return subscriber

    async def unregister(
        self,
        *,
        session_id: UUID,
        subscriber: StreamSubscriber,
    ) -> None:
        async with self._lease_state(session_id, create=False) as state:
            if state is not None:
                async with state.lock:
                    if state.queued_subscribers.get(subscriber.message_id) is subscriber:
                        state.queued_subscribers.pop(subscriber.message_id, None)
                    for turn_id, candidate in tuple(state.turn_subscribers.items()):
                        if candidate is subscriber:
                            state.turn_subscribers.pop(turn_id, None)
            subscriber.close()

    async def close(self) -> None:
        activation_tasks = list(self._activation_tasks)
        for task in activation_tasks:
            task.cancel()
        if activation_tasks:
            await asyncio.gather(*activation_tasks, return_exceptions=True)
        async with self._states_lock:
            tasks = [
                state.runner_task
                for state in self._states.values()
                if state.runner_task is not None
            ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        async with self._provider_lock:
            providers = list(self._providers.values())
            self._providers.clear()
        await asyncio.gather(*(provider.close() for provider in providers), return_exceptions=True)

    async def _schedule_turn(self, turn: TurnStart) -> None:
        async with self._lease_state(turn.session_id) as state:
            assert state is not None
            async with state.lock:
                self._set_active_turn(state, turn, inherit_preview=False)
                state.starts.append(turn)
                self._ensure_runner_locked(state)

    @asynccontextmanager
    async def _lease_state(
        self,
        session_id: UUID,
        *,
        create: bool = True,
    ) -> AsyncIterator[_SessionState | None]:
        async with self._states_lock:
            state = self._states.get(session_id)
            if state is None and create:
                state = _SessionState(session_id=session_id)
                self._states[session_id] = state
            if state is not None:
                state.leases += 1
        try:
            yield state
        finally:
            if state is not None:
                async with self._states_lock:
                    state.leases -= 1
                    self._evict_state_locked(state)

    async def _evict_state_if_idle(self, state: _SessionState) -> None:
        async with self._states_lock:
            self._evict_state_locked(state)

    def _evict_state_locked(self, state: _SessionState) -> None:
        if self._states.get(state.session_id) is not state:
            return
        if (
            state.leases == 0
            and state.runner_task is None
            and not state.starts
            and not state.turn_subscribers
            and not state.queued_subscribers
        ):
            self._states.pop(state.session_id)

    async def _queued_location(
        self,
        accepted: AcceptedMessage,
    ) -> tuple[str, UUID | None]:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            pending_exists = (
                await db.execute(
                    select(PendingMessage.id).where(PendingMessage.id == accepted.message_id)
                )
            ).scalar_one_or_none()
            if pending_exists is not None:
                return "pending", None
            canonical_exists = (
                await db.execute(select(Message.id).where(Message.id == accepted.message_id))
            ).scalar_one_or_none()
            if canonical_exists is None:
                return "done", None
            running_turn_id = (
                await db.execute(
                    select(TurnRun.id).where(
                        TurnRun.session_id == accepted.session_id,
                        TurnRun.status == "running",
                    )
                )
            ).scalar_one_or_none()
            if running_turn_id is None:
                return "done", None
            return "running", running_turn_id

    async def _turn_is_running(self, turn_id: UUID) -> bool:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            status = (
                await db.execute(select(TurnRun.status).where(TurnRun.id == turn_id))
            ).scalar_one_or_none()
            return status == "running"

    def _ensure_runner_locked(self, state: _SessionState) -> None:
        if state.runner_task is None or state.runner_task.done():
            state.runner_task = asyncio.create_task(
                self._run_session(state),
                name=f"chat-runner-{state.session_id}",
            )

    async def _run_session(self, state: _SessionState) -> None:
        current: TurnStart | None = None
        try:
            while True:
                if current is None:
                    current = await self._take_start_or_stop(state)
                    if current is None:
                        return
                try:
                    await self._execute_chain(state, current)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await self._fail_unexpected_chain(state)
                async with AsyncSession(self.engine, expire_on_commit=False) as db:
                    current = await reserve_pending_turn(
                        db,
                        session_id=state.session_id,
                        runner_instance_id=self.runner_instance_id,
                    )
                if current is not None:
                    await self._assign_queued_subscribers(state, current)
        finally:
            async with state.lock:
                current_task = asyncio.current_task()
                if state.runner_task is current_task:
                    if state.starts:
                        state.runner_task = asyncio.create_task(
                            self._run_session(state),
                            name=f"chat-runner-{state.session_id}",
                        )
                    else:
                        state.runner_task = None
            await self._evict_state_if_idle(state)

    async def _take_start_or_stop(self, state: _SessionState) -> TurnStart | None:
        async with state.lock:
            if state.starts:
                return state.starts.popleft()
            return None

    async def _assign_queued_subscribers(
        self,
        state: _SessionState,
        turn: TurnStart,
    ) -> None:
        async with state.lock:
            self._set_active_turn(state, turn, inherit_preview=False)
            for subscriber in self._take_queued_subscribers(state, turn.message_ids):
                self._install_newest_subscriber(
                    state,
                    turn_id=turn.turn_id,
                    subscriber=subscriber,
                )

    async def _execute_chain(self, state: _SessionState, initial_turn: TurnStart) -> None:
        turn = initial_turn
        repeated_call: tuple[str, str] | None = None
        repeated_count = 0

        for iteration in range(_MAX_ITERATIONS):
            completed = await self._invoke_provider_iteration(state, turn)
            if isinstance(completed, _UnhandledProviderFailure):
                raise RuntimeError("Provider failure recovery failed")
            if completed is None:
                return
            turn = completed.turn
            assistant = completed.assistant

            tool_uses = [block for block in assistant.content if block.get("type") == "tool_use"]
            if not tool_uses:
                async with AsyncSession(self.engine, expire_on_commit=False) as db:
                    await finish_final_turn(db, turn=turn)
                await self._publish_turn_finished(
                    state,
                    turn,
                    status="completed",
                    final_message_id=assistant.id,
                )
                await self._close_turn_subscriber(state, turn.turn_id)
                return

            if await self._cancel_requested(turn.session_id):
                await self._cancel_turn(
                    state,
                    turn,
                    outcome_unknown_tool_ids=[],
                    cancelled_tool_ids=[str(block["id"]) for block in tool_uses],
                )
                return

            last_result_id: UUID | None = None
            for index, tool_use in enumerate(tool_uses):
                tool_id = str(tool_use["id"])
                tool_name = str(tool_use["name"])
                tool_input = tool_use.get("input")
                if not isinstance(tool_input, dict):
                    tool_input = {}
                cancel_waiter = register_cancel_waiter(turn.session_id)
                try:
                    if await self._cancel_requested(turn.session_id) or cancel_waiter.done():
                        await self._cancel_turn(
                            state,
                            turn,
                            outcome_unknown_tool_ids=[],
                            cancelled_tool_ids=[str(block["id"]) for block in tool_uses[index:]],
                        )
                        return

                    await self._publish_tool_progress(
                        state,
                        turn,
                        kind="tool_started",
                        tool_call_id=tool_id,
                        tool_name=tool_name,
                    )
                    if cancel_waiter.done():
                        await self._cancel_turn(
                            state,
                            turn,
                            outcome_unknown_tool_ids=[],
                            cancelled_tool_ids=[str(block["id"]) for block in tool_uses[index:]],
                        )
                        return

                    issued = asyncio.Event()
                    tool_task = asyncio.create_task(
                        self.tool_registry.execute(
                            name=tool_name,
                            args=tool_input,
                            ctx=ToolContext(
                                user_id=completed.user_id,
                                session_id=turn.session_id,
                            ),
                            device_targets=completed.device_targets,
                            mcp_snapshot=completed.mcp_snapshot,
                            device_registry=self.device_registry,
                            on_issued=issued.set,
                        )
                    )
                    try:
                        await asyncio.wait(
                            (tool_task, cancel_waiter),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except asyncio.CancelledError:
                        tool_task.cancel()
                        await asyncio.gather(tool_task, return_exceptions=True)
                        raise
                    if cancel_waiter.done():
                        tool_task.cancel()
                        await asyncio.gather(tool_task, return_exceptions=True)
                        tool_completed = not tool_task.cancelled() and tool_task.exception() is None
                        if not tool_completed:
                            was_issued = issued.is_set()
                            outcome_unknown_tool_ids = [tool_id] if was_issued else []
                            cancelled_tool_ids = [
                                str(block["id"]) for block in tool_uses[index + 1 :]
                            ]
                            if not was_issued:
                                cancelled_tool_ids.insert(0, tool_id)
                            await self._cancel_turn(
                                state,
                                turn,
                                outcome_unknown_tool_ids=outcome_unknown_tool_ids,
                                cancelled_tool_ids=cancelled_tool_ids,
                            )
                            return
                    tool_result = tool_task.result()
                finally:
                    discard_cancel_waiter(turn.session_id, cancel_waiter)
                result_block: dict[str, Any] = {
                    "type": "tool_result",
                    "tool_use_id": tool_id,
                    "content": tool_result.content,
                    "is_error": tool_result.is_error,
                }
                if tool_result.code is not None:
                    result_block["code"] = tool_result.code.value
                delivery_effect = tool_result.side_effect
                delivery_refs: list[dict[str, Any]] | None = None
                assistant_message_id: UUID | None = None
                if isinstance(delivery_effect, MessageDeliveryEffect):
                    assistant_message_id = assistant.id
                    delivery_refs = []
                    for ref in delivery_effect.delivery_refs:
                        rendered_ref: dict[str, Any] = {
                            "tool_use_id": tool_id,
                            "type": ref.type,
                            "openoctopus_device": ref.openoctopus_device,
                            "path": ref.path,
                            "filename": ref.filename,
                            "mime": ref.mime,
                            "online_only": ref.online_only,
                        }
                        if ref.size is not None:
                            rendered_ref["size"] = ref.size
                        if isinstance(ref, WorkspaceFileDeliveryRef):
                            rendered_ref["workspace_id"] = str(ref.workspace_id)
                            rendered_ref["workspace_relative_path"] = ref.workspace_relative_path
                        else:
                            rendered_ref["device_id"] = str(ref.device_id)
                        delivery_refs.append(rendered_ref)
                async with AsyncSession(self.engine, expire_on_commit=False) as db:
                    updated_assistant, result_message = await persist_tool_result(
                        db,
                        turn=turn,
                        block=result_block,
                        assistant_message_id=assistant_message_id,
                        delivery_refs=delivery_refs,
                    )
                last_result_id = result_message.id
                if updated_assistant is not None:
                    await self._publish_message(state, turn, updated_assistant)
                await self._publish_message(state, turn, result_message)
                await self._publish_tool_progress(
                    state,
                    turn,
                    kind="tool_finished",
                    tool_call_id=tool_id,
                    tool_name=tool_name,
                )

                call_key = (
                    tool_name,
                    json.dumps(tool_input, sort_keys=True, separators=(",", ":")),
                )
                if call_key == repeated_call:
                    repeated_count += 1
                else:
                    repeated_call = call_key
                    repeated_count = 1

                if await self._cancel_requested(turn.session_id):
                    await self._cancel_turn(
                        state,
                        turn,
                        outcome_unknown_tool_ids=[],
                        cancelled_tool_ids=[str(block["id"]) for block in tool_uses[index + 1 :]],
                    )
                    return

            if iteration + 1 == _MAX_ITERATIONS:
                await self._fail_iteration_limit(state, turn)
                return

            if repeated_count >= 3:
                assert repeated_call is not None
                warning = (
                    f"You've called `{repeated_call[0]}` with the same args 3 times. "
                    "Reconsider or ask the user for clarification."
                )
                async with AsyncSession(self.engine, expire_on_commit=False) as db:
                    marker = await persist_human_marker(
                        db,
                        turn=turn,
                        text_content=warning,
                    )
                await self._publish_message(state, turn, marker)
                repeated_call = None
                repeated_count = 0

            async with AsyncSession(self.engine, expire_on_commit=False) as db:
                next_turn = await finish_tool_batch_and_continue(
                    db,
                    turn=turn,
                    runner_instance_id=self.runner_instance_id,
                )
            await self._publish_turn_finished(
                state,
                turn,
                status="completed",
                final_message_id=last_result_id,
            )
            await self._transfer_turn_subscriber(state, turn.turn_id, next_turn)
            turn = next_turn

    async def _invoke_provider_iteration(
        self,
        state: _SessionState,
        turn: TurnStart,
    ) -> _CompletedProviderTurn | _UnhandledProviderFailure | None:
        started = False
        try:
            user_id = await self._session_owner_id(turn.session_id)
        except Exception as exc:
            await self._fail_preflight(state, turn, exc)
            return None

        admitted = False
        try:
            async with self._context_slot(user_id):
                admitted = True
                try:
                    prepared: _PreparedTurn | None = None
                    for attempt in range(2):
                        async with AsyncSession(self.engine, expire_on_commit=False) as db:
                            turn = await capture_pending_for_turn(db, turn=turn)
                        async with state.lock:
                            self._set_active_turn(state, turn, inherit_preview=True)
                        try:
                            prepared = await self._prepare_turn(turn)
                            turn = prepared.turn
                            break
                        except StaleCompactionSelectionError:
                            if attempt == 1:
                                raise
                    if prepared is None:
                        raise RuntimeError("Turn preflight did not produce provider context")

                    await self._claim_promoted_subscriber(state, turn)
                    await self._publish_turn_started(state, turn)
                    started = True
                    if await self._cancel_requested(turn.session_id):
                        await self._cancel_turn(
                            state,
                            turn,
                            outcome_unknown_tool_ids=[],
                            cancelled_tool_ids=[],
                        )
                        return None

                    provider = await self._provider_for(prepared.config)

                    async def on_delta(channel: str, text: str) -> None:
                        await self._publish(
                            state,
                            turn.turn_id,
                            {
                                "type": "token_delta",
                                "turn_id": str(turn.turn_id),
                                "channel": channel,
                                "text": text,
                            },
                        )

                    result = await provider.stream_turn(
                        config=prepared.config,
                        system=prepared.system,
                        messages=prepared.messages,
                        effort=turn.effort,
                        limiter=self.limiter,
                        on_delta=on_delta,
                        tools=prepared.tools,
                    )
                    content = result.content
                    fingerprint = result.fingerprint
                    prepared_user_id = prepared.user_id
                    prepared_device_targets = prepared.device_targets
                    prepared_mcp_snapshot = prepared.mcp_snapshot
                    del prepared, provider, result
                except Exception as exc:
                    try:
                        if started:
                            error = exc if isinstance(exc, ProviderInvocationError) else None
                            await self._fail_provider(state, turn, error=error)
                        else:
                            await self._fail_preflight(state, turn, exc)
                    except Exception:
                        try:
                            await self._fail_unexpected_chain(state)
                        except Exception:
                            return _UNHANDLED_PROVIDER_FAILURE
                    return None
        except Exception as exc:
            if admitted:
                raise
            await self._fail_preflight(state, turn, exc)
            return None

        assistant = await self._persist_assistant_message(
            state,
            turn,
            content=content,
            fingerprint=fingerprint,
        )
        return _CompletedProviderTurn(
            turn=turn,
            assistant=assistant,
            user_id=prepared_user_id,
            device_targets=prepared_device_targets,
            mcp_snapshot=prepared_mcp_snapshot,
        )

    async def _session_owner_id(self, session_id: UUID) -> UUID:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            user_id = await db.scalar(select(Session.user_id).where(Session.id == session_id))
        if user_id is None:
            raise RuntimeError("Session disappeared while waiting for context admission")
        return user_id

    @asynccontextmanager
    async def _context_slot(self, user_id: UUID) -> AsyncIterator[None]:
        admitted = False
        try:
            async with self.context_admission.slot(user_id):
                admitted = True
                yield
        except AdmissionTimeoutError as exc:
            if admitted:
                raise
            raise ProviderInvocationError(
                "Context admission timed out",
                safe_message="The server is busy preparing other conversations. Please retry.",
            ) from exc

    async def _prepare_turn(self, turn: TurnStart) -> _PreparedTurn:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            await repair_unpaired_tool_uses(db, session_id=turn.session_id)

        compacted = False
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            config = await load_provider_config(db)
            session = await db.get(Session, turn.session_id)
            if session is None:
                raise RuntimeError("Session disappeared while preparing a turn")
            user_id = session.user_id
            server_envelope, owner_devices = await _load_mcp_authority_snapshot(
                db,
                user_id=user_id,
            )
            active_rows = list(
                (
                    await db.execute(
                        select(Message)
                        .where(
                            Message.session_id == turn.session_id,
                            Message.is_compacted.is_(False),
                        )
                        .order_by(Message.created_at, Message.id)
                    )
                )
                .scalars()
                .all()
            )
            current_pending_rows = list(
                (
                    await db.execute(
                        select(PendingMessage)
                        .where(PendingMessage.session_id == turn.session_id)
                        .order_by(PendingMessage.received_at, PendingMessage.id)
                    )
                )
                .scalars()
                .all()
            )
            captured_ids = set(turn.message_ids)
            pending_rows = [row for row in current_pending_rows if row.id in captured_ids]
            if tuple(row.id for row in pending_rows) != turn.message_ids:
                raise StaleCompactionSelectionError(
                    "Captured pending rows changed before preflight"
                )
            attachment_targets = build_device_attachment_targets(
                [*active_rows, *pending_rows]
            )
            system, prospective_messages = await build_provider_context(
                db,
                session_id=turn.session_id,
                config=config,
                add_compaction_continuation=not pending_rows,
                workspace_service=self.workspace_service,
                skills_cache=self.skills_cache,
                device_registry=self.device_registry,
                device_snapshot=_attachment_fenced_devices(
                    owner_devices,
                    attachment_targets,
                ),
            )
            prospective_messages.extend(
                {
                    "role": "user",
                    "content": [dict(block) for block in row.content],
                }
                for row in pending_rows
            )
            active_messages = project_provider_messages(
                active_rows,
                current_fingerprint=provider_fingerprint(config),
                add_compaction_continuation=False,
            )
        device_targets, mcp_snapshot, registry_schemas = _build_owner_tool_state(
            owner_devices,
            tool_registry=self.tool_registry,
            attachment_targets=attachment_targets,
            server_envelope=server_envelope,
            runtime_generations=(
                self._server_mcp_generation_resolver(server_envelope)
                if self._server_mcp_generation_resolver is not None
                else {}
            ),
        )

        should_compact = False
        if config.max_context_tokens is not None and config.compaction_threshold_tokens is not None:
            input_tokens = await self._estimate_tokens(
                system=system,
                messages=prospective_messages,
                tools=registry_schemas,
            )
            should_compact = compaction_required(
                input_tokens=input_tokens,
                max_context_tokens=config.max_context_tokens,
                threshold_tokens=config.compaction_threshold_tokens,
            )

        if should_compact and pending_rows and active_rows:
            provider = await self._provider_for(config)
            summary_content = await self._generate_summary(
                provider=provider,
                config=config,
                messages=active_messages,
            )
            async with AsyncSession(self.engine, expire_on_commit=False) as db:
                _, promoted_ids, latest_effort = await commit_stage_one(
                    db,
                    session_id=turn.session_id,
                    source_ids=stage_one_source_ids(active_rows),
                    pending_ids=tuple(row.id for row in pending_rows),
                    summary_content=summary_content,
                )
            turn = replace(
                turn,
                message_ids=promoted_ids,
                effort=Effort(latest_effort) if latest_effort is not None else None,
            )
            compacted = True
        elif should_compact and not pending_rows:
            source_ids = stage_two_source_ids(active_rows)
            if source_ids:
                provider = await self._provider_for(config)
                source_set = set(source_ids)
                tail_messages = project_message_rows(
                    [row for row in active_rows if row.id in source_set],
                    current_fingerprint=provider_fingerprint(config),
                )
                summary_content = await self._generate_summary(
                    provider=provider,
                    config=config,
                    messages=_stage_two_summary_messages(tail_messages),
                )
                async with AsyncSession(self.engine, expire_on_commit=False) as db:
                    await commit_stage_two(
                        db,
                        session_id=turn.session_id,
                        source_ids=source_ids,
                        summary_content=summary_content,
                    )
                compacted = True
        elif pending_rows:
            async with AsyncSession(self.engine, expire_on_commit=False) as db:
                turn = await promote_pending_for_turn(db, turn=turn)

        provider_messages = prospective_messages
        if compacted:
            async with AsyncSession(self.engine, expire_on_commit=False) as db:
                config = await load_provider_config(db)
                server_envelope, owner_devices = await _load_mcp_authority_snapshot(
                    db,
                    user_id=user_id,
                )
                provider_visible_rows = list(
                    (
                        await db.execute(
                            select(Message)
                            .where(
                                Message.session_id == turn.session_id,
                                Message.is_compacted.is_(False),
                            )
                            .order_by(Message.created_at, Message.id)
                        )
                    )
                    .scalars()
                    .all()
                )
                attachment_targets = build_device_attachment_targets(
                    provider_visible_rows
                )
                system, provider_messages = await build_provider_context(
                    db,
                    session_id=turn.session_id,
                    config=config,
                    workspace_service=self.workspace_service,
                    skills_cache=self.skills_cache,
                    device_registry=self.device_registry,
                    device_snapshot=_attachment_fenced_devices(
                        owner_devices,
                        attachment_targets,
                    ),
                )
            device_targets, mcp_snapshot, registry_schemas = _build_owner_tool_state(
                owner_devices,
                tool_registry=self.tool_registry,
                attachment_targets=attachment_targets,
                server_envelope=server_envelope,
                runtime_generations=(
                    self._server_mcp_generation_resolver(server_envelope)
                    if self._server_mcp_generation_resolver is not None
                    else {}
                ),
            )
            await self._estimate_tokens(
                system=system,
                messages=provider_messages,
                tools=registry_schemas,
            )
        return _PreparedTurn(
            turn=turn,
            config=config,
            system=system,
            messages=provider_messages,
            tools=registry_schemas,
            user_id=user_id,
            device_targets=device_targets,
            mcp_snapshot=mcp_snapshot,
        )

    async def _estimate_tokens(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> int:
        estimator = self._estimate_request_tokens
        if inspect.iscoroutinefunction(estimator) or inspect.iscoroutinefunction(
            getattr(estimator, "__call__", None)
        ):
            result = estimator(system=system, messages=messages, tools=tools)
        else:
            worker = asyncio.create_task(
                asyncio.to_thread(
                    estimator,
                    system=system,
                    messages=messages,
                    tools=tools,
                )
            )
            result = await await_future_cancellation_safe(worker)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _generate_summary(
        self,
        *,
        provider: Provider,
        config: ProviderConfig,
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        threshold = config.compaction_threshold_tokens
        if threshold is None:
            raise RuntimeError("Compaction threshold disappeared")
        summary_config = replace(
            config,
            max_output_tokens=compaction_max_output_tokens(threshold),
        )

        async def ignore_delta(channel: str, text: str) -> None:
            del channel, text

        result = await provider.stream_turn(
            config=summary_config,
            system=_COMPACTION_SYSTEM,
            messages=[
                *messages,
                {
                    "role": "user",
                    "content": [{"type": "text", "text": _COMPACTION_REQUEST}],
                },
            ],
            effort=Effort.OFF,
            limiter=self.limiter,
            on_delta=ignore_delta,
            tools=[],
        )
        text_blocks = [
            {"type": "text", "text": str(block["text"])}
            for block in result.content
            if block.get("type") == "text" and str(block.get("text", "")).strip()
        ]
        if not text_blocks:
            raise ProviderInvocationError(
                "Compaction provider returned no summary text",
                protocol=True,
            )
        return text_blocks

    async def _persist_assistant_message(
        self,
        state: _SessionState,
        turn: TurnStart,
        *,
        content: list[dict[str, Any]],
        fingerprint: str | None,
    ) -> Message:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            message = await persist_assistant(
                db,
                turn=turn,
                content=content,
                fingerprint=fingerprint,
            )
        await self._publish_message(state, turn, message)
        return message

    async def _fail_preflight(
        self,
        state: _SessionState,
        turn: TurnStart,
        exc: Exception,
    ) -> None:
        promoted = False
        try:
            async with AsyncSession(self.engine, expire_on_commit=False) as db:
                turn = await promote_pending_for_turn(db, turn=turn)
            promoted = True
        except Exception:
            pass
        if promoted:
            await self._claim_promoted_subscriber(state, turn)
        await self._publish_turn_started(state, turn)
        await self._fail_provider(
            state,
            turn,
            error=exc if isinstance(exc, ProviderInvocationError) else None,
        )

    async def _fail_provider(
        self,
        state: _SessionState,
        turn: TurnStart,
        *,
        error: ProviderInvocationError | None = None,
    ) -> None:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            message = await persist_assistant(
                db,
                turn=turn,
                content=[_synthetic_error_content(error=error)],
                fingerprint=None,
                failed=True,
            )
        await self._publish_message(state, turn, message)
        await self._publish_turn_finished(
            state,
            turn,
            status="failed",
            final_message_id=message.id,
        )
        await self._close_turn_subscriber(state, turn.turn_id)

    async def _fail_unexpected_chain(self, state: _SessionState) -> None:
        turn = await self._running_turn_for_session(state.session_id)
        if turn is None:
            await self._close_chain_subscribers(state)
            return

        await self._adopt_running_turn_subscriber(state, turn.turn_id)
        # The tool may have run even if its result failed to persist. Without a
        # dispatch journal, the existing repair marker is necessarily ambiguous.
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            repaired = await repair_unpaired_tool_uses(db, session_id=turn.session_id)
        for row in repaired:
            await self._publish_message(state, turn, row)
        try:
            await self._fail_provider(state, turn)
        finally:
            await self._close_turn_subscriber(state, turn.turn_id)

    async def _running_turn_for_session(self, session_id: UUID) -> TurnStart | None:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            turn_id = (
                await db.execute(
                    select(TurnRun.id).where(
                        TurnRun.session_id == session_id,
                        TurnRun.status == "running",
                    )
                )
            ).scalar_one_or_none()
        if turn_id is None:
            return None
        return TurnStart(
            session_id=session_id,
            turn_id=turn_id,
            message_ids=(),
            effort=None,
        )

    async def _fail_iteration_limit(
        self,
        state: _SessionState,
        turn: TurnStart,
    ) -> None:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            message = await persist_assistant(
                db,
                turn=turn,
                content=[
                    {
                        "type": "text",
                        "text": "The agent stopped after reaching the 200-iteration safety limit.",
                    }
                ],
                fingerprint=None,
                failed=True,
            )
        await self._publish_message(state, turn, message)
        await self._publish_turn_finished(
            state,
            turn,
            status="failed",
            final_message_id=message.id,
        )
        await self._close_turn_subscriber(state, turn.turn_id)

    async def _cancel_turn(
        self,
        state: _SessionState,
        turn: TurnStart,
        *,
        outcome_unknown_tool_ids: list[str],
        cancelled_tool_ids: list[str],
    ) -> None:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            result_rows, marker = await cancel_tool_batch(
                db,
                turn=turn,
                outcome_unknown_tool_ids=outcome_unknown_tool_ids,
                cancelled_tool_ids=cancelled_tool_ids,
            )
        for row in result_rows:
            await self._publish_message(state, turn, row)
        await self._publish_message(state, turn, marker)
        await self._publish_turn_finished(
            state,
            turn,
            status="cancelled",
            final_message_id=marker.id,
        )
        await self._close_turn_subscriber(state, turn.turn_id)

    async def _cancel_requested(self, session_id: UUID) -> bool:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            return await is_cancel_requested(db, session_id=session_id)

    async def _provider_for(self, config: ProviderConfig) -> Provider:
        key = (config.endpoint, config.api_key)
        async with self._provider_lock:
            provider = self._providers.get(key)
            if provider is None:
                provider = self._provider_factory(config)
                self._providers[key] = provider
            return provider

    async def _publish_turn_started(self, state: _SessionState, turn: TurnStart) -> None:
        await self._publish(
            state,
            turn.turn_id,
            {
                "type": "turn_started",
                "turn_id": str(turn.turn_id),
                "message_ids": [str(message_id) for message_id in turn.message_ids],
            },
        )

    async def _publish_message(
        self,
        state: _SessionState,
        turn: TurnStart,
        message: Message,
    ) -> None:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            session = await db.get(Session, turn.session_id)
            if session is None:
                return
            public_message = message_response(message, session=session)
        await self._publish(
            state,
            turn.turn_id,
            {
                "type": "message_persisted",
                "turn_id": str(turn.turn_id),
                "message": public_message.model_dump(mode="json", exclude_none=True),
            },
        )

    async def _publish_tool_progress(
        self,
        state: _SessionState,
        turn: TurnStart,
        *,
        kind: str,
        tool_call_id: str,
        tool_name: str,
    ) -> None:
        await self._publish(
            state,
            turn.turn_id,
            {
                "type": "tool_progress",
                "turn_id": str(turn.turn_id),
                "kind": kind,
                "tool_call_id": tool_call_id,
                "tool_name": tool_name,
            },
        )

    async def _publish_turn_finished(
        self,
        state: _SessionState,
        turn: TurnStart,
        *,
        status: str,
        final_message_id: UUID | None,
    ) -> None:
        event: dict[str, Any] = {
            "type": "turn_finished",
            "turn_id": str(turn.turn_id),
            "status": status,
        }
        if final_message_id is not None:
            event["final_message_id"] = str(final_message_id)
        await self._publish(state, turn.turn_id, event)

    async def _publish(
        self,
        state: _SessionState,
        turn_id: UUID,
        event: dict[str, Any],
    ) -> None:
        async with state.lock:
            subscriber = state.turn_subscribers.get(turn_id)
            if subscriber is not None:
                subscriber.send(event)

    async def _transfer_turn_subscriber(
        self,
        state: _SessionState,
        old_turn_id: UUID,
        new_turn: TurnStart,
    ) -> None:
        async with state.lock:
            self._set_active_turn(state, new_turn, inherit_preview=True)
            candidates = [
                candidate
                for candidate in [
                    state.turn_subscribers.pop(old_turn_id, None),
                    state.turn_subscribers.pop(new_turn.turn_id, None),
                    *self._take_queued_subscribers(state, new_turn.message_ids),
                ]
                if candidate is not None and not candidate.closed
            ]
            if not candidates:
                return
            winner = max(candidates, key=lambda candidate: candidate.accepted_at)
            for candidate in candidates:
                if candidate is winner:
                    continue
                candidate.send(
                    {
                        "type": "stream_replaced",
                        "message_id": str(candidate.message_id),
                        "by_message_id": str(winner.message_id),
                    }
                )
                candidate.close()
            state.turn_subscribers[new_turn.turn_id] = winner

    async def _close_turn_subscriber(
        self,
        state: _SessionState,
        turn_id: UUID,
    ) -> None:
        async with state.lock:
            subscriber = state.turn_subscribers.pop(turn_id, None)
            if subscriber is not None:
                subscriber.close()
            self._clear_active_turn(state, turn_id)

    async def _close_chain_subscribers(self, state: _SessionState) -> None:
        async with state.lock:
            subscribers = list(state.turn_subscribers.values())
            state.turn_subscribers.clear()
            state.active_turn_id = None
            state.active_preview_message_ids = frozenset()
            for subscriber in subscribers:
                subscriber.close()

    async def _adopt_running_turn_subscriber(
        self,
        state: _SessionState,
        turn_id: UUID,
    ) -> None:
        async with state.lock:
            candidates = [
                candidate for candidate in state.turn_subscribers.values() if not candidate.closed
            ]
            state.turn_subscribers.clear()
            state.active_turn_id = turn_id
            if not candidates:
                return
            winner = candidates[0]
            for candidate in candidates[1:]:
                winner = self._replace_older(winner, candidate)
            state.turn_subscribers[turn_id] = winner

    async def _claim_promoted_subscriber(
        self,
        state: _SessionState,
        turn: TurnStart,
    ) -> None:
        async with state.lock:
            for subscriber in self._take_queued_subscribers(state, turn.message_ids):
                self._install_newest_subscriber(
                    state,
                    turn_id=turn.turn_id,
                    subscriber=subscriber,
                )

    def _queue_subscriber(
        self,
        state: _SessionState,
        subscriber: StreamSubscriber,
    ) -> None:
        current = state.queued_subscribers.get(subscriber.message_id)
        if current is None:
            state.queued_subscribers[subscriber.message_id] = subscriber
            return
        state.queued_subscribers[subscriber.message_id] = self._replace_older(
            current,
            subscriber,
        )

    @staticmethod
    def _set_active_turn(
        state: _SessionState,
        turn: TurnStart,
        *,
        inherit_preview: bool,
    ) -> None:
        state.active_turn_id = turn.turn_id
        if turn.message_ids:
            state.active_preview_message_ids = frozenset(turn.message_ids)
        elif not inherit_preview:
            state.active_preview_message_ids = frozenset()

    @staticmethod
    def _clear_active_turn(state: _SessionState, turn_id: UUID) -> None:
        if state.active_turn_id != turn_id:
            return
        state.active_turn_id = None
        state.active_preview_message_ids = frozenset()

    @staticmethod
    def _take_queued_subscribers(
        state: _SessionState,
        message_ids: tuple[UUID, ...],
    ) -> list[StreamSubscriber]:
        return [
            subscriber
            for message_id in message_ids
            if (subscriber := state.queued_subscribers.pop(message_id, None)) is not None
            and not subscriber.closed
        ]

    def _install_newest_subscriber(
        self,
        state: _SessionState,
        *,
        turn_id: UUID,
        subscriber: StreamSubscriber,
    ) -> None:
        current = state.turn_subscribers.get(turn_id)
        if current is None:
            state.turn_subscribers[turn_id] = subscriber
            return
        winner = self._replace_older(current, subscriber)
        state.turn_subscribers[turn_id] = winner

    @staticmethod
    def _replace_older(
        left: StreamSubscriber,
        right: StreamSubscriber,
    ) -> StreamSubscriber:
        winner, loser = (right, left) if left.accepted_at <= right.accepted_at else (left, right)
        loser.send(
            {
                "type": "stream_replaced",
                "message_id": str(loser.message_id),
                "by_message_id": str(winner.message_id),
            }
        )
        loser.close()
        return winner


def _synthetic_error_content(*, error: ProviderInvocationError | None) -> dict[str, str]:
    if error is not None and error.protocol:
        code = ErrorCode.PROVIDER_PROTOCOL_ERROR
        message = "The model provider returned an unsupported response."
    else:
        code = ErrorCode.PROVIDER_UNAVAILABLE
        message = (
            error.safe_message
            if error is not None and error.safe_message is not None
            else "The model provider could not complete this response. Please try again."
        )
    return {"type": "text", "text": f"[{code.value}] {message}"}


def _stage_two_summary_messages(
    tail_messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "Summarize only the following agent activity and internal markers. "
                        "The latest user request is preserved separately."
                    ),
                }
            ],
        },
        *tail_messages,
    ]
