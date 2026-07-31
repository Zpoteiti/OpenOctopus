from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import TurnRun


async def abandon_running_turns(
    engine: AsyncEngine,
    *,
    runner_instance_id: UUID,
) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        await db.execute(
            update(TurnRun)
            .where(
                TurnRun.status == "running",
                TurnRun.runner_instance_id != runner_instance_id,
            )
            .values(status="abandoned", finished_at=datetime.now(UTC))
        )
        await db.commit()
