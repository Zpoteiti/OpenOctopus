from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.engine import get_engine


async def get_db() -> AsyncIterator[AsyncSession]:
    async with AsyncSession(get_engine(), expire_on_commit=False) as session:
        yield session
