"""Shared pytest fixtures and configuration."""

import uuid
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from openctopus_server.api.workspace_files import get_rest_transfer_admission
from openctopus_server.chat.runner import get_context_admission
from openctopus_server.config import get_settings
from openctopus_server.db.base import Base
from openctopus_server.db.engine import get_engine
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.main import create_app
from openctopus_server.tools.registry import get_content_converter, get_web_fetch_admission
from openctopus_server.workspace.fs import _workspace_fs_for_storage
from openctopus_server.workspace.storage import ObjectStorage, get_object_storage


@pytest.fixture(autouse=True)
def _no_environment_proxies(monkeypatch: pytest.MonkeyPatch):
    """Keep the local-only test suite off ambient proxies.

    The whole suite talks to in-process apps, localhost Uvicorn, and local
    PostgreSQL/RustFS.  A developer machine with a TUN-mode proxy (for
    example ALL_PROXY=socks://127.0.0.1:7897) otherwise breaks httpx client
    construction and localhost routing inside the tests themselves.
    """
    for key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "WS_PROXY",
        "WSS_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "ws_proxy",
        "wss_proxy",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")


@pytest.fixture(autouse=True)
def _clear_settings_and_engine_cache():
    """Ensure singleton caches are cleared around every test."""
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_device_registry.cache_clear()
    get_object_storage.cache_clear()
    get_rest_transfer_admission.cache_clear()
    get_context_admission.cache_clear()
    get_content_converter.cache_clear()
    get_web_fetch_admission.cache_clear()
    _workspace_fs_for_storage.cache_clear()
    yield
    _workspace_fs_for_storage.cache_clear()
    get_object_storage.cache_clear()
    get_rest_transfer_admission.cache_clear()
    get_context_admission.cache_clear()
    get_content_converter.cache_clear()
    get_web_fetch_admission.cache_clear()
    get_engine.cache_clear()
    get_device_registry.cache_clear()
    get_settings.cache_clear()


@pytest_asyncio.fixture(autouse=True)
async def _truncate_tables(pg_engine):
    """Clean tables before each test for isolation (pg_engine is session-scoped)."""
    yield
    table_names = ", ".join(f'"{t.name}"' for t in reversed(Base.metadata.sorted_tables))
    async with pg_engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE {table_names} CASCADE"))


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
    async with admin_engine.connect() as conn:
        await conn.execute(text(f'CREATE DATABASE "{test_db_name}"'))
        await conn.commit()

    test_url = settings.database_url.rsplit("/", 1)[0] + f"/{test_db_name}"
    engine = create_async_engine(test_url)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    await engine.dispose()
    async with admin_engine.connect() as conn:
        # Force-close any lingering client connections before dropping.
        await conn.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname = '{test_db_name}' AND pid <> pg_backend_pid()"
            )
        )
        await conn.execute(text(f'DROP DATABASE "{test_db_name}"'))
        await conn.commit()
    await admin_engine.dispose()


@pytest_asyncio.fixture
async def test_app(pg_engine, monkeypatch):
    # Point the app at the per-session test database.
    # render_as_string preserves the password; str(URL) masks it as '***'.
    monkeypatch.setenv(
        "OPENOCTOPUS_DATABASE_URL", pg_engine.url.render_as_string(hide_password=False)
    )
    get_settings.cache_clear()
    get_engine.cache_clear()

    app = create_app()
    client = Mock()
    client.list_objects.return_value = []
    client.put_object.return_value.etag = "test-etag"
    object_storage = ObjectStorage(client, "test", max_connections=1)
    object_storage.check_health = AsyncMock()  # type: ignore[method-assign]
    app.dependency_overrides[get_object_storage] = lambda: object_storage
    return app


@pytest_asyncio.fixture
async def async_client(test_app):
    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# --- Shared auth test helpers (importable as fixtures) ---


@pytest_asyncio.fixture
async def register_user_fn(async_client):
    async def _register(email="user@test.com", password="testpassword", name="User"):
        response = await async_client.post(
            "/api/auth/register",
            json={"email": email, "password": password, "name": name},
        )
        return response.json()

    return _register


@pytest_asyncio.fixture
async def register_admin_fn(async_client):
    async def _register(email="admin@test.com", password="testpassword", name="Admin"):
        response = await async_client.post(
            "/api/auth/register",
            json={
                "email": email,
                "password": password,
                "name": name,
                "admin_token": "dev-admin-token",
            },
        )
        return response.json()

    return _register


@pytest_asyncio.fixture
async def login_fn(async_client):
    async def _login(email, password="testpassword"):
        response = await async_client.post(
            "/api/auth/login",
            json={"email": email, "password": password},
        )
        return response.json()

    return _login


@pytest_asyncio.fixture
async def admin_client(async_client):
    """Register an admin and login so the client has an admin cookie."""
    await async_client.post(
        "/api/auth/register",
        json={
            "email": "admin@test.com",
            "password": "testpassword",
            "name": "Admin",
            "admin_token": "dev-admin-token",
        },
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "admin@test.com", "password": "testpassword"},
    )
    return async_client


@pytest_asyncio.fixture
async def user_client(async_client):
    """Register a regular user and login so the client has a user cookie."""
    await async_client.post(
        "/api/auth/register",
        json={"email": "user@test.com", "password": "testpassword", "name": "User"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "user@test.com", "password": "testpassword"},
    )
    return async_client


@pytest_asyncio.fixture(autouse=True)
async def _inline_workflow_transport(monkeypatch):
    """Product unit scenarios use an inline queue; subprocess tests exercise DBOS itself."""
    import asyncio

    from openctopus_server.chat import durable
    from openctopus_server.db.models import TurnRun

    locks = {}
    tasks = set()
    submitted = set()

    async def accept(db, turn):
        run = await db.get(TurnRun, turn.turn_id)
        run.workflow_id = str(turn.turn_id)

    async def start(host):
        return None

    async def submit(host, turn):
        key = (host.runtime.runner_instance_id, turn.turn_id)
        if key in submitted:
            return
        submitted.add(key)
        lock = locks.setdefault(turn.session_id, asyncio.Lock())
        async def run():
            async with lock:
                if not await host.runtime._turn_is_running(turn.turn_id):
                    return
                async with host.runtime._lease_state(turn.session_id) as state:
                    await host.runtime._run_session(state, initial_turn=turn)
        task = asyncio.create_task(run())
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    monkeypatch.setattr(durable, "enqueue_transaction", AsyncMock())
    monkeypatch.setattr(durable, "enqueue_turn", accept)
    monkeypatch.setattr(durable.DurableHost, "start", start)
    monkeypatch.setattr(durable.DurableHost, "submit", submit)
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
