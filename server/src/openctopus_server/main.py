import sys

from fastapi import FastAPI
from sqlalchemy import text

from openctopus_server.api.router import router as api_router
from openctopus_server.config import get_settings
from openctopus_server.db.base import Base
from openctopus_server.db.engine import get_engine


def create_app() -> FastAPI:
    app = FastAPI(title="OpenOctopus")
    app.include_router(api_router)

    @app.on_event("startup")
    async def startup() -> None:
        try:
            settings = get_settings()
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

    @app.on_event("shutdown")
    async def shutdown() -> None:
        engine = get_engine()
        await engine.dispose()

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(app, host=settings.host, port=settings.port)
