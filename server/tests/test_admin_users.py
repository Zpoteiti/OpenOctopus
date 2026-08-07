import asyncio
import uuid
from unittest.mock import AsyncMock

from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import SystemConfig
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.service import WorkspaceService, get_workspace_service


async def test_list_users(admin_client, register_user_fn, login_fn):
    await register_user_fn(email="user1@test.com")
    await register_user_fn(email="user2@test.com")
    await login_fn("admin@test.com")
    response = await admin_client.get("/api/admin/users")
    assert response.status_code == 200
    users = response.json()
    assert len(users) >= 3
    for u in users:
        assert "id" in u
        assert "email" in u
        assert u["quota_bytes"] == 524_288_000
        assert u["bytes_used"] == 0
        assert u["locked"] is False


async def test_list_users_renders_live_usage_and_lock_state(
    admin_client,
    login_fn,
    pg_engine,
    register_user_fn,
    test_app,
):
    await register_user_fn(email="usage@test.com")
    await login_fn("admin@test.com")
    async with AsyncSession(pg_engine) as db:
        db.add(SystemConfig(key="quota_bytes", value=10))
        await db.commit()

    service = AsyncMock(spec=WorkspaceService)

    async def personal_usages(user_ids):
        return [10, 11][: len(user_ids)]

    service.personal_usages.side_effect = personal_usages
    test_app.dependency_overrides[get_workspace_service] = lambda: service

    response = await admin_client.get("/api/admin/users")

    assert response.status_code == 200
    assert [row["bytes_used"] for row in response.json()] == [10, 11]
    assert [row["quota_bytes"] for row in response.json()] == [10, 10]
    assert [row["locked"] for row in response.json()] == [False, True]
    service.personal_usages.assert_awaited_once()


async def test_list_users_releases_database_before_live_usage(
    admin_client,
    pg_engine,
    test_app,
):
    entered = asyncio.Event()
    release = asyncio.Event()
    service = AsyncMock(spec=WorkspaceService)

    async def blocked_usages(user_ids):
        entered.set()
        await release.wait()
        return [0] * len(user_ids)

    service.personal_usages.side_effect = blocked_usages
    test_app.dependency_overrides[get_workspace_service] = lambda: service

    request = asyncio.create_task(admin_client.get("/api/admin/users"))
    await entered.wait()
    try:
        assert pg_engine.pool.checkedout() == 0
    finally:
        release.set()
    response = await request
    assert response.status_code == 200


async def test_list_users_fails_the_whole_page_when_usage_is_unavailable(
    admin_client,
    test_app,
):
    service = AsyncMock(spec=WorkspaceService)
    service.personal_usages.side_effect = WorkspaceError(
        ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
        "Object storage is unavailable",
    )
    test_app.dependency_overrides[get_workspace_service] = lambda: service

    response = await admin_client.get("/api/admin/users")

    assert response.status_code == 503
    assert response.json() == {
        "code": "workspace_storage_unavailable",
        "message": "Object storage is unavailable",
    }


async def test_admin_user_usage_fields_are_required_in_openapi(async_client):
    response = await async_client.get("/openapi.json")
    schema = response.json()["components"]["schemas"]["AdminUserResponse"]

    assert {"quota_bytes", "bytes_used", "locked"}.issubset(schema["required"])


async def test_list_users_pagination(admin_client, register_user_fn, login_fn):
    for i in range(5):
        await register_user_fn(email=f"page{i}@test.com")
    await login_fn("admin@test.com")
    response = await admin_client.get("/api/admin/users?limit=2&offset=0")
    assert response.status_code == 200
    assert len(response.json()) == 2


async def test_delete_user(admin_client, register_user_fn, login_fn):
    await register_user_fn(email="todelete@test.com")
    await login_fn("admin@test.com")
    users = (await admin_client.get("/api/admin/users")).json()
    target = next(u for u in users if u["email"] == "todelete@test.com")
    response = await admin_client.delete(f"/api/admin/users/{target['id']}")
    assert response.status_code == 204


async def test_delete_user_not_found(admin_client):
    response = await admin_client.delete(f"/api/admin/users/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.json()["code"] == "user_not_found"


async def test_delete_last_admin_returns_409(admin_client):
    users = (await admin_client.get("/api/admin/users")).json()
    admin = next(u for u in users if u["is_admin"])
    response = await admin_client.delete(f"/api/admin/users/{admin['id']}")
    assert response.status_code == 409
    assert response.json()["code"] == "auth_last_admin_required"


async def test_non_admin_list_users_returns_403(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "nonadmin@test.com", "password": "testpassword", "name": "Non"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "nonadmin@test.com", "password": "testpassword"},
    )
    response = await async_client.get("/api/admin/users")
    assert response.status_code == 403
    assert response.json()["code"] == "auth_forbidden"
