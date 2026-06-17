"""Shared pytest fixtures and configuration."""

import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from openctopus_server.config import get_settings
from openctopus_server.db.base import Base
from openctopus_server.db.engine import get_engine
from openctopus_server.main import create_app


@pytest.fixture(scope="session")
def admin_database_url():
    settings = get_settings()
    url = settings.database_url.rsplit("/", 1)[0] + "/postgres"
    return url


@pytest_asyncio.fixture(scope="session")
async def pg_engine(admin_database_url):
    settings = get_settings()
    test_db_name = f"oo_test_{uuid.uuid4().hex[:8]}"

    admin_engine = create_async_engine(
        admin_database_url,
        isolation_level="AUTOCOMMIT",
    )
    async with admin_engine.begin() as conn:
        await conn.execute(text(f'CREATE DATABASE "{test_db_name}"'))

    test_url = settings.database_url.rsplit("/", 1)[0] + f"/{test_db_name}"
    engine = create_async_engine(test_url)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    await engine.dispose()
    async with admin_engine.begin() as conn:
        await conn.execute(text(f'DROP DATABASE "{test_db_name}"'))
    await admin_engine.dispose()


@pytest_asyncio.fixture
async def async_client(pg_engine):
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
