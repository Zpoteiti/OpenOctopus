from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.chat.types import AcceptedMessage
from openctopus_server.db.models import PendingMessage, Session
from openctopus_server.services.messages import (
    close_revoked_pending_prefix,
    reserve_pending_turn,
)

from .types import ExternalChannel


class ChannelRecoveryRuntime(Protocol):
    runner_instance_id: UUID

    def session_operation(
        self,
        session_id: UUID,
    ) -> AbstractAsyncContextManager[None]: ...

    async def schedule(self, accepted: AcceptedMessage) -> None: ...


async def close_obsolete_channel_pending(
    engine: AsyncEngine,
    runtime: ChannelRecoveryRuntime,
) -> None:
    """Close stale external Pending prefixes before current Adapters start."""
    async with AsyncSession(engine, expire_on_commit=False) as db:
        session_ids = list(
            (
                await db.scalars(
                    select(Session.id)
                    .join(PendingMessage, PendingMessage.session_id == Session.id)
                    .where(Session.channel.in_(("discord", "dingtalk")))
                    .distinct()
                    .order_by(Session.id)
                )
            ).all()
        )

    for session_id in session_ids:
        async with runtime.session_operation(session_id):
            async with AsyncSession(engine, expire_on_commit=False) as db:
                await close_revoked_pending_prefix(
                    db,
                    session_id=session_id,
                    runner_instance_id=runtime.runner_instance_id,
                )


async def recover_channel_pending(
    engine: AsyncEngine,
    runtime: ChannelRecoveryRuntime,
    user_id: UUID,
    channel: ExternalChannel,
    binding_generation: UUID,
) -> None:
    """Resume durable Pending rows only after their current binding is online."""
    async with AsyncSession(engine, expire_on_commit=False) as db:
        session_ids = list(
            (
                await db.scalars(
                    select(Session.id)
                    .join(PendingMessage, PendingMessage.session_id == Session.id)
                    .where(
                        Session.user_id == user_id,
                        Session.channel == channel,
                        PendingMessage.channel_binding_generation
                        == binding_generation,
                    )
                    .distinct()
                    .order_by(Session.id)
                )
            ).all()
        )

    for session_id in session_ids:
        async with runtime.session_operation(session_id):
            async with AsyncSession(engine, expire_on_commit=False) as db:
                turn = await reserve_pending_turn(
                    db,
                    session_id=session_id,
                    runner_instance_id=runtime.runner_instance_id,
                )
            if turn is None:
                continue
            accepted = AcceptedMessage(
                session_id=session_id,
                message_id=turn.message_ids[-1],
                accepted_at=datetime.now(UTC),
                disposition="started",
                created_session=False,
                turn=turn,
            )
            handoff = asyncio.create_task(runtime.schedule(accepted))
            await await_future_cancellation_safe(handoff)
