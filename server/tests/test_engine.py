import pytest
from sqlalchemy import text

from openctopus_server.db.engine import get_engine


@pytest.mark.asyncio
async def test_engine_can_select_one():
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 1"))
        assert result.scalar_one() == 1
