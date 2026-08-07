import asyncio
from collections.abc import Awaitable

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from openctopus_server.db.engine import get_engine
from openctopus_server.workspace.storage import ObjectStorage, get_object_storage

router = APIRouter()


@router.get("/health", response_model=None)
async def health(
    engine: AsyncEngine = Depends(get_engine),
    object_storage: ObjectStorage = Depends(get_object_storage),
) -> JSONResponse | dict[str, str]:
    db_connected, storage_connected = await asyncio.gather(
        _is_connected(_check_db(engine)),
        _is_connected(_check_object_storage(object_storage)),
    )
    content = {
        "status": "ok" if db_connected and storage_connected else "error",
        "db": "connected" if db_connected else "disconnected",
        "object_storage": "connected" if storage_connected else "disconnected",
    }
    if not db_connected or not storage_connected:
        return JSONResponse(content=content, status_code=503)
    return content


async def _check_db(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def _check_object_storage(object_storage: ObjectStorage) -> None:
    await object_storage.check_health()


async def _is_connected(check: Awaitable[None]) -> bool:
    try:
        await asyncio.wait_for(check, timeout=2.0)
    except Exception:
        return False
    return True
