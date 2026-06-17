import asyncio

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from openctopus_server.db.engine import get_engine

router = APIRouter()


@router.get("/health")
async def health(engine: AsyncEngine = Depends(get_engine)):
    try:
        await asyncio.wait_for(_check_db(engine), timeout=2.0)
    except (TimeoutError, DBAPIError):
        return JSONResponse(
            content={"status": "error", "db": "disconnected"},
            status_code=503,
        )
    return {"status": "ok", "db": "connected"}


async def _check_db(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
