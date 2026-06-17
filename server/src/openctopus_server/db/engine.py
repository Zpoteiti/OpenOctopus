from sqlalchemy.ext.asyncio import create_async_engine

from openctopus_server.config import get_settings


def get_engine():
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout,
        pool_pre_ping=settings.database_pool_pre_ping,
    )
