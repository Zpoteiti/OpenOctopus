import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from openctopus_server.api.router import router as api_router
from openctopus_server.config import get_settings
from openctopus_server.db.base import Base
from openctopus_server.db.engine import get_engine


@asynccontextmanager
async def _lifespan(app: FastAPI):
    try:
        get_settings()
    except Exception as exc:
        print(f"Config validation failed: {exc}", file=sys.stderr)
        sys.exit(1)

    engine = get_engine()
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:
        print(f"Database bootstrap failed: {exc}", file=sys.stderr)
        sys.exit(1)

    yield

    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="OpenOctopus", lifespan=_lifespan)
    app.include_router(api_router)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(app, host=settings.host, port=settings.port)
