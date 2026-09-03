from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from threading import Lock
from typing import Protocol, cast
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import (
    ChannelDelivery,
    ChannelDeliveryAction,
    TurnRun,
)

from .adapters.base import ActionIssueHook, ChannelAdapter
from .delivery import (
    MAX_DELIVERY_ACTIONS,
    ActionResult,
    DeliveryAggregateStatus,
    DeliveryPlanTooLargeError,
)
from .types import DeliveryAction, ExternalChannel, OutboundMessage

_TERMINAL_STATUSES = frozenset({"sent", "partial", "failed", "unknown"})
_RETRY_FENCE_CODE = "tool_channel_retry_requires_new_turn"
_CANCELLED_CODE = "user_cancelled"
_DELIVERY_TIMEOUT_CODE = "channel_delivery_timeout"
_SERVER_RESTART_CODE = "server_restart"
_NOT_READY_CODE = "channel_not_ready"
_PLAN_FAILED_CODE = "channel_delivery_plan_failed"
_INVALID_PLAN_CODE = "channel_delivery_plan_invalid"
_PLAN_TOO_LARGE_CODE = "channel_delivery_plan_too_large"

@dataclass(frozen=True, slots=True)
class TargetFenceFailure:
    error_code: str
    error_message: str


class _IssueFenceRejectedError(Exception):
    def __init__(self, failure: TargetFenceFailure) -> None:
        self.failure = failure


@dataclass(frozen=True, slots=True)
class ChannelDeliveryResult:
    delivery_id: UUID
    status: DeliveryAggregateStatus
    visible_sent_actions: int
    visible_total_actions: int
    last_error_code: str | None
    last_error_message: str | None


class AdapterLookup(Protocol):
    def __call__(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> ChannelAdapter | None: ...


class TargetIssueFence(Protocol):
    async def __call__(
        self,
        message: OutboundMessage,
    ) -> TargetFenceFailure | None: ...


@dataclass(slots=True)
class _LockEntry:
    lock: asyncio.Lock
    users: int = 0


@dataclass(slots=True)
class _ExecutionState:
    current_index: int = 0
    current_issued: bool = False


_lock_entries: dict[tuple[UUID, str], _LockEntry] = {}
_lock_entries_guard = Lock()


@asynccontextmanager
async def _delivery_lock(key: tuple[UUID, str]) -> AsyncIterator[None]:
    with _lock_entries_guard:
        entry = _lock_entries.get(key)
        if entry is None:
            entry = _LockEntry(lock=asyncio.Lock())
            _lock_entries[key] = entry
        entry.users += 1

    acquired = False
    try:
        await entry.lock.acquire()
        acquired = True
        yield
    finally:
        if acquired:
            entry.lock.release()
        with _lock_entries_guard:
            entry.users -= 1
            if entry.users == 0:
                _lock_entries.pop(key, None)


class ChannelDeliveryRouter:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        adapter_lookup: AdapterLookup,
        issue_fence: TargetIssueFence,
        action_timeout_seconds: float = 30.0,
        logical_timeout_seconds: float = 120.0,
    ) -> None:
        if action_timeout_seconds <= 0 or logical_timeout_seconds <= 0:
            raise ValueError("Channel delivery timeouts must be positive")
        self._engine = engine
        self._adapter_lookup = adapter_lookup
        self._issue_fence = issue_fence
        self._action_timeout_seconds = action_timeout_seconds
        self._logical_timeout_seconds = logical_timeout_seconds

    async def deliver(
        self,
        message: OutboundMessage,
        *,
        session_id: UUID | None = None,
        assistant_message_id: UUID | None = None,
        tool_use_id: str | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ChannelDeliveryResult:
        key = (message.user_id, message.delivery_key)
        async with _delivery_lock(key):
            existing = await self._existing_result(message.user_id, message.delivery_key)
            if existing is not None:
                return existing

            prepare_task = asyncio.create_task(
                self._prepare(
                    message,
                    session_id=session_id,
                    assistant_message_id=assistant_message_id,
                    tool_use_id=tool_use_id,
                )
            )
            try:
                delivery_id = await asyncio.shield(prepare_task)
            except asyncio.CancelledError:
                delivery_id = await _wait_through_cancellation(prepare_task)
                prepare_cleanup = asyncio.create_task(
                    self._fail_without_actions(
                        delivery_id,
                        error_code=_CANCELLED_CODE,
                        error_message="Delivery cancelled before planning.",
                    )
                )
                await _wait_through_cancellation(prepare_cleanup)
                raise
            except IntegrityError:
                existing = await self._existing_result(
                    message.user_id,
                    message.delivery_key,
                )
                if existing is None:
                    raise
                return existing

            try:
                if await self.has_failed_target(
                    turn_id=message.turn_id,
                    channel=message.channel,
                    chat_id=message.chat_id,
                    binding_generation=message.binding_generation,
                ):
                    return await self._fail_without_actions(
                        delivery_id,
                        error_code=_RETRY_FENCE_CODE,
                        error_message=(
                            "A new user turn is required before retrying this target."
                        ),
                    )

                adapter = self._adapter_lookup(message.user_id, message.channel)
                if adapter is None:
                    return await self._fail_without_actions(
                        delivery_id,
                        error_code=_NOT_READY_CODE,
                        error_message=(
                            "No connected channel adapter is available for the target."
                        ),
                    )
                if adapter.platform != message.channel:
                    return await self._fail_without_actions(
                        delivery_id,
                        error_code=_INVALID_PLAN_CODE,
                        error_message="The channel adapter does not match the target.",
                    )
                try:
                    plan = adapter.plan_delivery(message)
                    self._validate_actions(plan.actions)
                except DeliveryPlanTooLargeError:
                    return await self._fail_without_actions(
                        delivery_id,
                        error_code=_PLAN_TOO_LARGE_CODE,
                        error_message="The channel delivery plan has too many actions.",
                    )
                except ValueError:
                    return await self._fail_without_actions(
                        delivery_id,
                        error_code=_INVALID_PLAN_CODE,
                        error_message="The channel adapter returned an invalid delivery plan.",
                    )
                except Exception:
                    return await self._fail_without_actions(
                        delivery_id,
                        error_code=_PLAN_FAILED_CODE,
                        error_message="The channel delivery plan could not be created.",
                    )
            except asyncio.CancelledError:
                planning_cleanup = asyncio.create_task(
                    self._fail_without_actions(
                        delivery_id,
                        error_code=_CANCELLED_CODE,
                        error_message="Delivery cancelled before planning completed.",
                    )
                )
                await _wait_through_cancellation(planning_cleanup)
                raise

            install_task = asyncio.create_task(
                self._install_plan(delivery_id, plan.actions)
            )
            try:
                await asyncio.shield(install_task)
            except asyncio.CancelledError:
                await _wait_through_cancellation(install_task)
                install_cleanup = asyncio.create_task(
                    self._cancel_delivery(
                        delivery_id,
                        current_index=0,
                        current_issued=False,
                    )
                )
                await _wait_through_cancellation(install_cleanup)
                raise
            except Exception:
                return await self._fail_without_actions(
                    delivery_id,
                    error_code=_PLAN_FAILED_CODE,
                    error_message="The channel delivery plan could not be persisted.",
                )

            execution_state = _ExecutionState()
            execute_task = asyncio.create_task(
                self._execute(
                    delivery_id,
                    message,
                    plan.actions,
                    adapter,
                    execution_state=execution_state,
                    on_issued=on_issued,
                )
            )
            try:
                done, _pending = await asyncio.wait(
                    (execute_task,),
                    timeout=self._logical_timeout_seconds,
                )
            except asyncio.CancelledError:
                execute_task.cancel()
                await _drain_cancelled_task(execute_task)
                execution_cleanup = asyncio.create_task(
                    self._cancel_delivery(
                        delivery_id,
                        current_index=execution_state.current_index,
                        current_issued=execution_state.current_issued,
                    )
                )
                await _wait_through_cancellation(execution_cleanup)
                raise
            if done:
                return execute_task.result()

            execute_task.cancel()
            await _drain_cancelled_task(execute_task)
            timeout_cleanup = asyncio.create_task(
                self._cancel_delivery(
                    delivery_id,
                    current_index=execution_state.current_index,
                    current_issued=execution_state.current_issued,
                    error_code=_DELIVERY_TIMEOUT_CODE,
                    before_issue_message=(
                        "Logical delivery timed out before platform issue."
                    ),
                    after_issue_message=(
                        "Logical delivery timed out after platform issue; "
                        "outcome is unknown."
                    ),
                )
            )
            await _wait_through_cancellation(timeout_cleanup)
            return await self._result(delivery_id)

    async def has_failed_target(
        self,
        *,
        turn_id: UUID | None,
        channel: ExternalChannel,
        chat_id: str,
        binding_generation: UUID,
    ) -> bool:
        if turn_id is None:
            return False
        target = _target_record(channel, chat_id, binding_generation)
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            failed_targets = await db.scalar(
                select(TurnRun.failed_delivery_targets).where(TurnRun.id == turn_id)
            )
        return target in (failed_targets or [])

    async def record_failed_target(
        self,
        *,
        turn_id: UUID | None,
        channel: ExternalChannel,
        chat_id: str,
        binding_generation: UUID,
    ) -> None:
        if turn_id is None:
            return
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                turn = await db.scalar(
                    select(TurnRun).where(TurnRun.id == turn_id).with_for_update()
                )
                if turn is not None:
                    _append_failed_target(turn, channel, chat_id, binding_generation)

    async def repair_incomplete_deliveries(self) -> int:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                deliveries = list(
                    (
                        await db.scalars(
                            select(ChannelDelivery)
                            .where(ChannelDelivery.status.in_(("prepared", "attempting")))
                            .order_by(ChannelDelivery.created_at, ChannelDelivery.id)
                            .with_for_update()
                        )
                    ).all()
                )
                for delivery in deliveries:
                    actions = await self._locked_actions(db, delivery.id)
                    if not actions:
                        await self._fail_without_actions_locked(
                            db,
                            delivery,
                            error_code=_SERVER_RESTART_CODE,
                            error_message="Server restarted before delivery planning completed.",
                        )
                        continue
                    self._repair_actions(delivery.status, actions)
                    await self._finish_locked(db, delivery, actions)
        return len(deliveries)

    async def _existing_result(
        self,
        user_id: UUID,
        delivery_key: str,
    ) -> ChannelDeliveryResult | None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            row = await db.execute(
                select(ChannelDelivery.id, ChannelDelivery.status).where(
                    ChannelDelivery.user_id == user_id,
                    ChannelDelivery.delivery_key == delivery_key,
                )
            )
            existing = row.one_or_none()
        if existing is None:
            return None
        delivery_id, status = existing
        if status not in _TERMINAL_STATUSES:
            await self._repair_delivery(delivery_id)
        return await self._result(delivery_id)

    async def _prepare(
        self,
        message: OutboundMessage,
        *,
        session_id: UUID | None,
        assistant_message_id: UUID | None,
        tool_use_id: str | None,
    ) -> UUID:
        delivery_id = uuid4()
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            db.add(
                ChannelDelivery(
                    id=delivery_id,
                    user_id=message.user_id,
                    session_id=session_id,
                    turn_id=message.turn_id,
                    assistant_message_id=assistant_message_id,
                    tool_use_id=tool_use_id,
                    delivery_key=message.delivery_key,
                    origin=message.origin,
                    channel=message.channel,
                    chat_id=message.chat_id,
                    binding_generation=message.binding_generation,
                    status="prepared",
                    total_actions=0,
                    visible_sent_actions=0,
                )
            )
            await db.commit()
        return delivery_id

    async def _install_plan(
        self,
        delivery_id: UUID,
        actions: tuple[DeliveryAction, ...],
    ) -> None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                delivery = await db.scalar(
                    select(ChannelDelivery)
                    .where(ChannelDelivery.id == delivery_id)
                    .with_for_update()
                )
                if (
                    delivery is None
                    or delivery.status != "prepared"
                    or delivery.total_actions != 0
                ):
                    raise RuntimeError("Delivery is not awaiting its plan")
                existing_actions = await self._locked_actions(db, delivery_id)
                if existing_actions:
                    raise RuntimeError("Delivery plan has already been installed")
                delivery.total_actions = len(actions)
                db.add_all(
                    [
                        ChannelDeliveryAction(
                            id=uuid4(),
                            delivery_id=delivery_id,
                            action_index=index,
                            action_kind=action.kind,
                            visible=action.visible,
                            status="prepared",
                        )
                        for index, action in enumerate(actions)
                    ]
                )

    async def _fail_without_actions(
        self,
        delivery_id: UUID,
        *,
        error_code: str,
        error_message: str,
    ) -> ChannelDeliveryResult:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                delivery = await db.scalar(
                    select(ChannelDelivery)
                    .where(ChannelDelivery.id == delivery_id)
                    .with_for_update()
                )
                if delivery is None:
                    raise RuntimeError("Delivery does not exist")
                if delivery.status not in _TERMINAL_STATUSES:
                    actions = await self._locked_actions(db, delivery_id)
                    if actions:
                        raise RuntimeError("Delivery already has a plan")
                    await self._fail_without_actions_locked(
                        db,
                        delivery,
                        error_code=error_code,
                        error_message=error_message,
                    )
        return await self._result(delivery_id)

    async def _fail_without_actions_locked(
        self,
        db: AsyncSession,
        delivery: ChannelDelivery,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        delivery.status = "failed"
        delivery.visible_sent_actions = 0
        delivery.last_error_code = error_code
        delivery.last_error_message = error_message
        delivery.finished_at = datetime.now(UTC)
        if delivery.turn_id is not None:
            turn = await db.scalar(
                select(TurnRun).where(TurnRun.id == delivery.turn_id).with_for_update()
            )
            if turn is not None:
                _append_failed_target(
                    turn,
                    delivery.channel,
                    delivery.chat_id,
                    delivery.binding_generation,
                )

    async def _execute(
        self,
        delivery_id: UUID,
        message: OutboundMessage,
        actions: tuple[DeliveryAction, ...],
        adapter: ChannelAdapter,
        *,
        execution_state: _ExecutionState,
        on_issued: Callable[[], None] | None,
    ) -> ChannelDeliveryResult:
        callback_fired = False
        artifacts: dict[int, str] = {}
        for current_index, action in enumerate(actions):
            execution_state.current_index = current_index
            execution_state.current_issued = False
            if await self.has_failed_target(
                turn_id=message.turn_id,
                channel=message.channel,
                chat_id=message.chat_id,
                binding_generation=message.binding_generation,
            ):
                await self._fail_unissued(
                    delivery_id,
                    current_index,
                    error_code=_RETRY_FENCE_CODE,
                    error_message="A new user turn is required before retrying this target.",
                )
                return await self._result(delivery_id)

            if action.dependency_action_index is not None:
                artifact_id = artifacts.get(action.dependency_action_index)
                if artifact_id is None:
                    await self._fail_unissued(
                        delivery_id,
                        current_index,
                        error_code="channel_delivery_artifact_missing",
                        error_message=(
                            "A required prior upload did not return a media identity."
                        ),
                    )
                    return await self._result(delivery_id)
                action = replace(action, dependency_artifact_id=artifact_id)

            issue_hook_called = False

            async def mark_issued() -> None:
                nonlocal callback_fired, issue_hook_called
                if issue_hook_called:
                    raise RuntimeError("Adapter called the issue hook more than once")
                issue_hook_called = True
                fence_failure = await self._issue_fence(message)
                if fence_failure is not None:
                    raise _IssueFenceRejectedError(fence_failure)
                await self._mark_attempting(delivery_id, current_index)
                if self._adapter_lookup(message.user_id, message.channel) is not adapter:
                    raise _IssueFenceRejectedError(
                        TargetFenceFailure(
                            error_code="channel_target_stale",
                            error_message=(
                                "The channel runtime changed before platform issue."
                            ),
                        )
                    )
                execution_state.current_issued = True
                if (
                    not callback_fired
                    and on_issued is not None
                    and (action.visible or action.kind == "file_upload")
                ):
                    on_issued()
                    callback_fired = True

            result = await self._execute_adapter_action(
                adapter,
                action,
                on_issued=mark_issued,
                execution_state=execution_state,
            )
            if execution_state.current_issued:
                await self._mark_result(delivery_id, current_index, result)
                execution_state.current_issued = False
            else:
                if result.status != "failed":
                    result = ActionResult(
                        status="failed",
                        error_code="channel_adapter_issue_not_reported",
                        error_message=(
                            "The platform adapter did not report its issue boundary."
                        ),
                    )
                await self._fail_unissued(
                    delivery_id,
                    current_index,
                    error_code=result.error_code or "channel_action_failed_before_issue",
                    error_message=(
                        result.error_message
                        or "The platform action failed before platform issue."
                    ),
                )
                return await self._result(delivery_id)
            if result.status == "sent" and result.artifact_id is not None:
                artifacts[current_index] = result.artifact_id
            if result.status in {"failed", "unknown"}:
                return await self._finalize(delivery_id)

        return await self._finalize(delivery_id)

    async def _execute_adapter_action(
        self,
        adapter: ChannelAdapter,
        action: DeliveryAction,
        *,
        on_issued: ActionIssueHook,
        execution_state: _ExecutionState,
    ) -> ActionResult:
        try:
            async with asyncio.timeout(self._action_timeout_seconds):
                result = await adapter.execute_action(action, on_issued=on_issued)
        except _IssueFenceRejectedError as exc:
            return ActionResult(
                status="failed",
                error_code=exc.failure.error_code,
                error_message=exc.failure.error_message,
            )
        except TimeoutError:
            return ActionResult(
                status="unknown" if execution_state.current_issued else "failed",
                error_code="channel_action_timeout",
                error_message=(
                    "The platform action timed out after issue."
                    if execution_state.current_issued
                    else "The platform action timed out before issue."
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return ActionResult(
                status="unknown" if execution_state.current_issued else "failed",
                error_code="channel_adapter_error",
                error_message=(
                    "The platform action ended without a sanitized result after issue."
                    if execution_state.current_issued
                    else "The platform adapter failed before platform issue."
                ),
            )
        if not isinstance(result, ActionResult) or result.status not in {
            "sent",
            "failed",
            "unknown",
        }:
            return ActionResult(
                status="unknown" if execution_state.current_issued else "failed",
                error_code="channel_adapter_invalid_result",
                error_message="The platform adapter returned an invalid result.",
            )
        return result

    async def _mark_attempting(self, delivery_id: UUID, action_index: int) -> None:
        now = datetime.now(UTC)
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                delivery = await db.scalar(
                    select(ChannelDelivery)
                    .where(ChannelDelivery.id == delivery_id)
                    .with_for_update()
                )
                action = await db.scalar(
                    select(ChannelDeliveryAction)
                    .where(
                        ChannelDeliveryAction.delivery_id == delivery_id,
                        ChannelDeliveryAction.action_index == action_index,
                    )
                    .with_for_update()
                )
                if delivery is None or action is None or action.status != "prepared":
                    raise RuntimeError("Delivery action is not prepared")
                action.status = "attempting"
                action.started_at = now
                delivery.status = "attempting"
                if delivery.started_at is None:
                    delivery.started_at = now

    async def _mark_result(
        self,
        delivery_id: UUID,
        action_index: int,
        result: ActionResult,
    ) -> None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                action = await db.scalar(
                    select(ChannelDeliveryAction)
                    .where(
                        ChannelDeliveryAction.delivery_id == delivery_id,
                        ChannelDeliveryAction.action_index == action_index,
                    )
                    .with_for_update()
                )
                if action is None or action.status != "attempting":
                    raise RuntimeError("Delivery action is not attempting")
                action.status = result.status
                action.platform_message_id = (
                    result.platform_message_id or result.artifact_id
                )
                action.last_error_code = result.error_code
                action.last_error_message = result.error_message
                action.finished_at = datetime.now(UTC)

    async def _fail_unissued(
        self,
        delivery_id: UUID,
        action_index: int,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                delivery = await db.scalar(
                    select(ChannelDelivery)
                    .where(ChannelDelivery.id == delivery_id)
                    .with_for_update()
                )
                if delivery is None:
                    raise RuntimeError("Delivery does not exist")
                actions = await self._locked_actions(db, delivery_id)
                action = actions[action_index]
                if action.status not in {"prepared", "attempting"}:
                    raise RuntimeError("Delivery action has already finished")
                action.status = "failed"
                action.last_error_code = error_code
                action.last_error_message = error_message
                action.finished_at = datetime.now(UTC)
                self._skip_after(actions, action_index)
                await self._finish_locked(db, delivery, actions)

    async def _finalize(self, delivery_id: UUID) -> ChannelDeliveryResult:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                delivery = await db.scalar(
                    select(ChannelDelivery)
                    .where(ChannelDelivery.id == delivery_id)
                    .with_for_update()
                )
                if delivery is None:
                    raise RuntimeError("Delivery does not exist")
                actions = await self._locked_actions(db, delivery_id)
                terminal_index = next(
                    (
                        index
                        for index, action in enumerate(actions)
                        if action.status in {"failed", "unknown"}
                    ),
                    None,
                )
                if terminal_index is not None:
                    self._skip_after(actions, terminal_index)
                await self._finish_locked(db, delivery, actions)
        return await self._result(delivery_id)

    async def _cancel_delivery(
        self,
        delivery_id: UUID,
        *,
        current_index: int,
        current_issued: bool,
        error_code: str = _CANCELLED_CODE,
        before_issue_message: str = "Delivery cancelled before platform issue.",
        after_issue_message: str = (
            "Delivery cancelled after platform issue; outcome is unknown."
        ),
    ) -> None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                delivery = await db.scalar(
                    select(ChannelDelivery)
                    .where(ChannelDelivery.id == delivery_id)
                    .with_for_update()
                )
                if delivery is None or delivery.status in _TERMINAL_STATUSES:
                    return
                actions = await self._locked_actions(db, delivery_id)
                if not actions:
                    await self._fail_without_actions_locked(
                        db,
                        delivery,
                        error_code=error_code,
                        error_message=before_issue_message,
                    )
                    return
                terminal_index = next(
                    (
                        index
                        for index, action in enumerate(actions)
                        if action.status in {"failed", "unknown"}
                    ),
                    None,
                )
                if terminal_index is None:
                    candidate_index = current_index
                    if actions[candidate_index].status == "sent":
                        candidate_index = next(
                            (
                                index
                                for index in range(candidate_index + 1, len(actions))
                                if actions[index].status in {"prepared", "attempting"}
                            ),
                            candidate_index,
                        )
                    candidate = actions[candidate_index]
                    if candidate.status in {"prepared", "attempting"}:
                        was_issued = candidate_index == current_index and current_issued
                        candidate.status = "unknown" if was_issued else "failed"
                        candidate.last_error_code = error_code
                        candidate.last_error_message = (
                            after_issue_message
                            if was_issued
                            else before_issue_message
                        )
                        candidate.finished_at = datetime.now(UTC)
                        terminal_index = candidate_index
                if terminal_index is not None:
                    self._skip_after(actions, terminal_index)
                await self._finish_locked(db, delivery, actions)

    async def _repair_delivery(self, delivery_id: UUID) -> None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                delivery = await db.scalar(
                    select(ChannelDelivery)
                    .where(ChannelDelivery.id == delivery_id)
                    .with_for_update()
                )
                if delivery is None or delivery.status in _TERMINAL_STATUSES:
                    return
                actions = await self._locked_actions(db, delivery_id)
                if not actions:
                    await self._fail_without_actions_locked(
                        db,
                        delivery,
                        error_code=_SERVER_RESTART_CODE,
                        error_message="Server restarted before delivery planning completed.",
                    )
                    return
                self._repair_actions(delivery.status, actions)
                await self._finish_locked(db, delivery, actions)

    def _repair_actions(
        self,
        delivery_status: str,
        actions: list[ChannelDeliveryAction],
    ) -> None:
        terminal_index = next(
            (
                index
                for index, action in enumerate(actions)
                if action.status in {"failed", "unknown"}
            ),
            None,
        )
        if terminal_index is None:
            candidate_index = next(
                (
                    index
                    for index, action in enumerate(actions)
                    if action.status in {"prepared", "attempting"}
                ),
                None,
            )
            if candidate_index is not None:
                candidate = actions[candidate_index]
                uncertain = delivery_status == "attempting" and candidate.status == "attempting"
                candidate.status = "unknown" if uncertain else "failed"
                candidate.last_error_code = _SERVER_RESTART_CODE
                candidate.last_error_message = (
                    "Server restarted after platform issue; outcome is unknown."
                    if uncertain
                    else "Server restarted before platform issue."
                )
                candidate.finished_at = datetime.now(UTC)
                terminal_index = candidate_index
        if terminal_index is not None:
            self._skip_after(actions, terminal_index)

    async def _finish_locked(
        self,
        db: AsyncSession,
        delivery: ChannelDelivery,
        actions: list[ChannelDeliveryAction],
    ) -> None:
        if not actions:
            raise RuntimeError("Delivery plan contains no actions")
        terminal = next(
            (action for action in actions if action.status in {"failed", "unknown"}),
            None,
        )
        if terminal is None and any(action.status != "sent" for action in actions):
            raise RuntimeError("Delivery actions are not terminal")

        visible_sent = sum(action.visible and action.status == "sent" for action in actions)
        if terminal is None:
            status: DeliveryAggregateStatus = "sent"
        elif visible_sent:
            status = "partial"
        elif terminal.status == "failed":
            status = "failed"
        else:
            status = "unknown"

        delivery.status = status
        delivery.visible_sent_actions = visible_sent
        delivery.last_error_code = terminal.last_error_code if terminal is not None else None
        delivery.last_error_message = (
            terminal.last_error_message if terminal is not None else None
        )
        delivery.finished_at = datetime.now(UTC)
        if status != "sent" and delivery.turn_id is not None:
            turn = await db.scalar(
                select(TurnRun).where(TurnRun.id == delivery.turn_id).with_for_update()
            )
            if turn is not None:
                _append_failed_target(
                    turn,
                    delivery.channel,
                    delivery.chat_id,
                    delivery.binding_generation,
                )

    async def _locked_actions(
        self,
        db: AsyncSession,
        delivery_id: UUID,
    ) -> list[ChannelDeliveryAction]:
        return list(
            (
                await db.scalars(
                    select(ChannelDeliveryAction)
                    .where(ChannelDeliveryAction.delivery_id == delivery_id)
                    .order_by(ChannelDeliveryAction.action_index)
                    .with_for_update()
                )
            ).all()
        )

    async def _result(self, delivery_id: UUID) -> ChannelDeliveryResult:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            delivery = await db.get(ChannelDelivery, delivery_id)
            if delivery is None or delivery.status not in _TERMINAL_STATUSES:
                raise RuntimeError("Delivery is not terminal")
            visible_total_count = await db.scalar(
                select(func.count(ChannelDeliveryAction.id)).where(
                    ChannelDeliveryAction.delivery_id == delivery_id,
                    ChannelDeliveryAction.visible.is_(True),
                )
            )
        return ChannelDeliveryResult(
            delivery_id=delivery.id,
            status=cast(DeliveryAggregateStatus, delivery.status),
            visible_sent_actions=delivery.visible_sent_actions,
            visible_total_actions=visible_total_count or 0,
            last_error_code=delivery.last_error_code,
            last_error_message=delivery.last_error_message,
        )

    @staticmethod
    def _skip_after(actions: list[ChannelDeliveryAction], terminal_index: int) -> None:
        now = datetime.now(UTC)
        for action in actions[terminal_index + 1 :]:
            if action.status in {"prepared", "attempting"}:
                action.status = "skipped"
                action.finished_at = now

    @staticmethod
    def _validate_actions(actions: tuple[DeliveryAction, ...]) -> None:
        if not actions:
            raise ValueError("Delivery plan must contain at least one action")
        if len(actions) > MAX_DELIVERY_ACTIONS:
            raise DeliveryPlanTooLargeError(
                f"Delivery plan exceeds the {MAX_DELIVERY_ACTIONS}-action limit"
            )
        for index, action in enumerate(actions):
            if action.dependency_artifact_id is not None:
                raise ValueError("Delivery plans cannot contain runtime artifact IDs")
            if action.dependency_action_index is not None and not (
                0 <= action.dependency_action_index < index
            ):
                raise ValueError("Delivery action dependency must reference prior action")


async def _wait_through_cancellation[T](task: asyncio.Task[T]) -> T:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return await task


async def _drain_cancelled_task[T](task: asyncio.Task[T]) -> None:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    try:
        task.result()
    except asyncio.CancelledError:
        pass


def _target_record(
    channel: str,
    chat_id: str,
    binding_generation: UUID,
) -> dict[str, str]:
    return {
        "channel": channel,
        "chat_id": chat_id,
        "binding_generation": str(binding_generation),
    }


def _append_failed_target(
    turn: TurnRun,
    channel: str,
    chat_id: str,
    binding_generation: UUID,
) -> None:
    target = _target_record(channel, chat_id, binding_generation)
    if target not in turn.failed_delivery_targets:
        turn.failed_delivery_targets = [*turn.failed_delivery_targets, target]
