from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_PERSONAL_QUOTA_LOCK_KEY = "openoctopus:personal_quota"


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
