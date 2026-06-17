import asyncio

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from openctopus_server.db.engine import get_engine

router = APIRouter()


@router.get("/health")
async def health(engine: AsyncEngine = Depends(get_engine)):
    try:
        await asyncio.wait_for(_check_db(engine), timeout=2.0)
    except (asyncio.TimeoutError, DBAPIError):
        return {"status": "error", "db": "disconnected"}, 503
    return {"status": "ok", "db": "connected"}


async def _check_db(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
