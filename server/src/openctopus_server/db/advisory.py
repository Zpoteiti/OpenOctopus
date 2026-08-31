from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_PERSONAL_QUOTA_LOCK_KEY = "openoctopus:personal_quota"
_SHARED_QUOTA_LOCK_KEY = "openoctopus:shared_workspace_quota"
_GLOBAL_MCP_CATALOG_LOCK_KEY = "openoctopus:mcp_catalog"
_SERVER_MCP_CONFIG_LOCK_KEY = "openoctopus:server_mcp_config"


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


async def lock_global_mcp_catalog_write(db: AsyncSession) -> None:
    """Fence short Server/Device MCP authority commits across processes."""
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": _GLOBAL_MCP_CATALOG_LOCK_KEY},
    )


async def lock_server_mcp_config_write(db: AsyncSession) -> None:
    """Serialize Server MCP first-writer transactions before the row exists."""
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": _SERVER_MCP_CONFIG_LOCK_KEY},
    )


async def lock_uuid_identity(
    db: AsyncSession,
    value: UUID,
    *,
    shared: bool = False,
) -> None:
    """Serialize all transitions that reserve one stable public UUID."""
    key = value.int & ((1 << 63) - 1)
    statement = (
        "SELECT pg_advisory_xact_lock_shared(:key)"
        if shared
        else "SELECT pg_advisory_xact_lock(:key)"
    )
    await db.execute(
        text(statement),
        {"key": key},
    )
