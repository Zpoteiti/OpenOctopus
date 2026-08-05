from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_PERSONAL_QUOTA_LOCK_KEY = "openoctopus:personal_quota"
_SHARED_QUOTA_LOCK_KEY = "openoctopus:shared_workspace_quota"


async def lock_personal_quota_read(db: AsyncSession) -> None:
    await db.execute(
        text(
            "SELECT pg_advisory_xact_lock_shared("
            f"hashtextextended('{_PERSONAL_QUOTA_LOCK_KEY}', 0))"
        )
    )


async def lock_personal_quota_write(db: AsyncSession) -> None:
    await db.execute(
        text(f"SELECT pg_advisory_xact_lock(hashtextextended('{_PERSONAL_QUOTA_LOCK_KEY}', 0))")
    )


async def lock_shared_quota_read(db: AsyncSession) -> None:
    await db.execute(
        text(
            f"SELECT pg_advisory_xact_lock_shared(hashtextextended('{_SHARED_QUOTA_LOCK_KEY}', 0))"
        )
    )


async def lock_shared_quota_write(db: AsyncSession) -> None:
    await db.execute(
        text(f"SELECT pg_advisory_xact_lock(hashtextextended('{_SHARED_QUOTA_LOCK_KEY}', 0))")
    )


async def lock_workspace_refs(db: AsyncSession, user_ids: Iterable[UUID]) -> None:
    statement = text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))")
    for user_id in sorted(set(user_ids), key=str):
        await db.execute(statement, {"key": f"openoctopus:workspace_refs:{user_id}"})
