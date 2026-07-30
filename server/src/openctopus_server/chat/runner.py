import asyncio
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.chat.context import build_provider_context
from openctopus_server.chat.public_projection import message_response
from openctopus_server.chat.stream import StreamSubscriber
from openctopus_server.chat.types import AcceptedMessage, TurnStart
from openctopus_server.db.models import Message, PendingMessage, Session, TurnRun
from openctopus_server.provider.anthropic import (
    AnthropicProvider,
    Provider,
    ProviderInvocationError,
)
from openctopus_server.provider.config import ProviderConfig, load_provider_config
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.services.messages import (
    drain_pending_and_create_turn,
    persist_turn_outcome,
)

ProviderFactory = Callable[[ProviderConfig], Provider]


@dataclass(slots=True)
class _SessionState:
    session_id: UUID
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    starts: deque[TurnStart] = field(default_factory=deque)
    runner_task: asyncio.Task[None] | None = None
    turn_subscribers: dict[UUID, StreamSubscriber] = field(default_factory=dict)
    queued_subscriber: StreamSubscriber | None = None


class ChatRuntime:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        provider_factory: ProviderFactory | None = None,
    ) -> None:
        self.engine = engine
        self.runner_instance_id = uuid.uuid4()
        self.limiter = ProviderLimiter()
        self._provider_factory = provider_factory or AnthropicProvider
        self._providers: dict[tuple[str, str], Provider] = {}
        self._provider_lock = asyncio.Lock()
        self._states: dict[UUID, _SessionState] = {}
        self._states_lock = asyncio.Lock()

    def set_provider_factory(self, factory: ProviderFactory) -> None:
        if self._providers:
            raise RuntimeError("Cannot replace provider factory after provider use")
        self._provider_factory = factory

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
        state = await self._state_for(accepted.session_id)
        async with state.lock:
            if accepted.turn is None:
                location, running_turn_id = await self._queued_location(accepted)
                if location == "running" and running_turn_id is not None:
                    current = state.turn_subscribers.get(running_turn_id)
                    if current is None:
                        state.turn_subscribers[running_turn_id] = subscriber
                    elif current.accepted_at <= subscriber.accepted_at:
                        current.send(
                            {
                                "type": "stream_replaced",
                                "message_id": str(current.message_id),
                                "by_message_id": str(subscriber.message_id),
                            }
                        )
                        current.close()
                        state.turn_subscribers[running_turn_id] = subscriber
                    else:
                        subscriber.send(
                            {
                                "type": "stream_replaced",
                                "message_id": str(subscriber.message_id),
                                "by_message_id": str(current.message_id),
                            }
                        )
                        subscriber.close()
                    return subscriber
                if location == "done":
                    subscriber.close()
                    return subscriber
                previous = state.queued_subscriber
                if previous is not None:
                    if previous.accepted_at <= subscriber.accepted_at:
                        previous.send(
                            {
                                "type": "stream_replaced",
                                "message_id": str(previous.message_id),
                                "by_message_id": str(accepted.message_id),
                            }
                        )
                        previous.close()
                    else:
                        subscriber.send(
                            {
                                "type": "stream_replaced",
                                "message_id": str(subscriber.message_id),
                                "by_message_id": str(previous.message_id),
                            }
                        )
                        subscriber.close()
                        return subscriber
                state.queued_subscriber = subscriber
                return subscriber

            if state.queued_subscriber is not None:
                previous = state.queued_subscriber
                previous.send(
                    {
                        "type": "stream_replaced",
                        "message_id": str(previous.message_id),
                        "by_message_id": str(accepted.message_id),
                    }
                )
                previous.close()
                state.queued_subscriber = None
            state.turn_subscribers[accepted.turn.turn_id] = subscriber
            state.starts.append(accepted.turn)
            self._ensure_runner_locked(state)
        return subscriber

    async def unregister(
        self,
        *,
        session_id: UUID,
        subscriber: StreamSubscriber,
    ) -> None:
        state = await self._state_for(session_id)
        async with state.lock:
            if state.queued_subscriber is subscriber:
                state.queued_subscriber = None
            for turn_id, candidate in tuple(state.turn_subscribers.items()):
                if candidate is subscriber:
                    state.turn_subscribers.pop(turn_id, None)
            subscriber.close()

    async def close(self) -> None:
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
        await asyncio.gather(
            *(provider.close() for provider in providers),
            return_exceptions=True,
        )

    async def _state_for(
        self,
        session_id: UUID,
    ) -> _SessionState:
        async with self._states_lock:
            state = self._states.get(session_id)
            if state is None:
                state = _SessionState(session_id=session_id)
                self._states[session_id] = state
            return state

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
                await self._execute_turn(state, current)
                async with AsyncSession(self.engine, expire_on_commit=False) as db:
                    current = await drain_pending_and_create_turn(
                        db,
                        session_id=state.session_id,
                        runner_instance_id=self.runner_instance_id,
                    )
                if current is not None:
                    await self._assign_queued_subscriber(state, current)
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

    async def _take_start_or_stop(
        self,
        state: _SessionState,
    ) -> TurnStart | None:
        async with state.lock:
            if state.starts:
                return state.starts.popleft()
            return None

    async def _assign_queued_subscriber(
        self,
        state: _SessionState,
        turn: TurnStart,
    ) -> None:
        async with state.lock:
            if state.queued_subscriber is not None:
                state.turn_subscribers[turn.turn_id] = state.queued_subscriber
                state.queued_subscriber = None

    async def _execute_turn(
        self,
        state: _SessionState,
        turn: TurnStart,
    ) -> None:
        await self._publish(
            state,
            turn.turn_id,
            {
                "type": "turn_started",
                "turn_id": str(turn.turn_id),
                "message_ids": [str(message_id) for message_id in turn.message_ids],
            },
        )
        failed = False
        fingerprint: str | None = None
        content: list[dict[str, Any]]
        try:
            async with AsyncSession(self.engine, expire_on_commit=False) as db:
                config = await load_provider_config(db)
                system, provider_messages = await build_provider_context(
                    db,
                    session_id=turn.session_id,
                    config=config,
                )
            provider = await self._provider_for(config)

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
                config=config,
                system=system,
                messages=provider_messages,
                effort=turn.effort,
                limiter=self.limiter,
                on_delta=on_delta,
            )
            content = result.content
            fingerprint = result.fingerprint
        except ProviderInvocationError as exc:
            failed = True
            content = [_synthetic_error_content(protocol=exc.protocol)]
        except Exception:
            failed = True
            content = [_synthetic_error_content(protocol=False)]

        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            message = await persist_turn_outcome(
                db,
                turn=turn,
                content=content,
                fingerprint=fingerprint,
                failed=failed,
            )
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
                "message": public_message.model_dump(
                    mode="json",
                    exclude_none=True,
                ),
            },
        )
        await self._publish(
            state,
            turn.turn_id,
            {
                "type": "turn_finished",
                "turn_id": str(turn.turn_id),
                "status": "failed" if failed else "completed",
                "final_message_id": str(message.id),
            },
        )
        await self._close_turn_subscriber(state, turn.turn_id)

    async def _provider_for(self, config: ProviderConfig) -> Provider:
        key = (config.endpoint, config.api_key)
        async with self._provider_lock:
            provider = self._providers.get(key)
            if provider is None:
                provider = self._provider_factory(config)
                self._providers[key] = provider
            return provider

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

    async def _close_turn_subscriber(
        self,
        state: _SessionState,
        turn_id: UUID,
    ) -> None:
        async with state.lock:
            subscriber = state.turn_subscribers.pop(turn_id, None)
            if subscriber is not None:
                subscriber.close()


def _synthetic_error_content(*, protocol: bool) -> dict[str, str]:
    if protocol:
        text = "The model provider returned an unsupported response."
    else:
        text = "The model provider could not complete this response. Please try again."
    return {"type": "text", "text": text}
