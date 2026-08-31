from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.automations.heartbeat import (
    HeartbeatPhaseTwoRequest,
    build_heartbeat_phase_two_text,
)
from openctopus_server.chat.types import AcceptedMessage
from openctopus_server.services.inbound import heartbeat_inbound, lock_inbound_identity
from openctopus_server.services.messages import publish_inbound_locked


class HeartbeatPublishingRuntime(Protocol):
    runner_instance_id: UUID

    def session_operation(self, session_id: UUID) -> AbstractAsyncContextManager[None]: ...

    async def schedule(self, accepted: AcceptedMessage) -> None: ...


async def publish_heartbeat_phase_two(
    engine: AsyncEngine,
    runtime: HeartbeatPublishingRuntime,
    request: HeartbeatPhaseTwoRequest,
) -> bool:
    """Atomically publish one selected pulse and hand its turn to ChatRuntime."""
    task = asyncio.create_task(
        _publish_heartbeat_phase_two(engine, runtime, request),
        name=f"heartbeat-publish-{request.user_id}",
    )
    return await await_future_cancellation_safe(task)


async def _publish_heartbeat_phase_two(
    engine: AsyncEngine,
    runtime: HeartbeatPublishingRuntime,
    request: HeartbeatPhaseTwoRequest,
) -> bool:
    async with runtime.session_operation(request.user_id):
        async with AsyncSession(engine, expire_on_commit=False) as db:
            inbound = heartbeat_inbound(
                owner_user_id=request.user_id,
                content=[
                    {
                        "type": "text",
                        "text": build_heartbeat_phase_two_text(request),
                    }
                ],
            )
            try:
                if await lock_inbound_identity(db, inbound) is None:
                    await db.rollback()
                    return False
                accepted = await publish_inbound_locked(
                    db,
                    inbound=inbound,
                    title="Heartbeat",
                    runner_instance_id=runtime.runner_instance_id,
                    queue_if_busy=False,
                )
                if accepted is None:
                    await db.rollback()
                    return False
                await db.commit()
            except Exception:
                await db.rollback()
                raise
        await runtime.schedule(accepted)
        return True

