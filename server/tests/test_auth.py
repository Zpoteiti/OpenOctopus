from unittest.mock import AsyncMock, Mock

from openctopus_server.auth.cookies import COOKIE_NAME
from openctopus_server.workspace.service import get_workspace_service


async def test_register_returns_201_and_sets_cookie(async_client):
    response = await async_client.post(
        "/api/auth/register",
        json={"email": "newuser@test.com", "password": "testpassword", "name": "New User"},
    )
    assert response.status_code == 201
    body = response.json()
    assert "jwt" in body
    assert body["user"]["email"] == "newuser@test.com"
    assert body["user"]["is_admin"] is False
    assert "password_hash" not in body["user"]
    set_cookie = response.headers.get("set-cookie", "")
    assert COOKIE_NAME in set_cookie


async def test_register_creates_personal_soul_and_memory_files(async_client, test_app):
    workspace = Mock()
    workspace.write = AsyncMock()
    test_app.dependency_overrides[get_workspace_service] = lambda: workspace
    admin = await async_client.post(
        "/api/auth/register",
        json={
            "email": "seed-admin@test.com",
            "password": "testpassword",
            "name": "Seed Admin",
            "admin_token": "dev-admin-token",
        },
    )
    assert admin.status_code == 201
    configured = await async_client.patch(
        "/api/admin/config",
        json={"default_soul": "# Company Agent\nBe clear and practical."},
    )
    assert configured.status_code == 200
    workspace.write.reset_mock()

    response = await async_client.post(
        "/api/auth/register",
        json={"email": "seeded@test.com", "password": "testpassword", "name": "Seeded"},
    )
    assert response.status_code == 201

    assert workspace.write.await_count == 2
    soul = workspace.write.await_args_list[0].kwargs
    memory = workspace.write.await_args_list[1].kwargs
    assert soul["path"] == "SOUL.md"
    assert soul["data"] == b"# Company Agent\nBe clear and practical."
    assert soul["if_none_match"] is True
    assert memory["path"] == "MEMORY.md"
    assert b"how they would like to be addressed" in memory["data"]
    assert b"what they would like to call you" in memory["data"]
    assert memory["if_none_match"] is True


async def test_register_duplicate_email_returns_409(async_client):
    payload = {"email": "dup@test.com", "password": "testpassword", "name": "Dup"}
    await async_client.post("/api/auth/register", json=payload)
    response = await async_client.post("/api/auth/register", json=payload)
    assert response.status_code == 409
    assert response.json()["code"] == "auth_email_taken"


async def test_register_with_admin_token_creates_admin(async_client):
    response = await async_client.post(
        "/api/auth/register",
        json={
            "email": "admin@test.com",
            "password": "testpassword",
            "name": "Admin",
            "admin_token": "dev-admin-token",
        },
    )
    assert response.status_code == 201
    assert response.json()["user"]["is_admin"] is True


async def test_login_returns_200_and_sets_cookie(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "login@test.com", "password": "testpassword", "name": "Login"},
    )
    response = await async_client.post(
        "/api/auth/login",
        json={"email": "login@test.com", "password": "testpassword"},
    )
    assert response.status_code == 200
    body = response.json()
    assert "jwt" in body
    assert body["user"]["email"] == "login@test.com"
    set_cookie = response.headers.get("set-cookie", "")
    assert COOKIE_NAME in set_cookie


async def test_login_wrong_password_returns_401(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "wrongpw@test.com", "password": "testpassword", "name": "Wrong"},
    )
    response = await async_client.post(
        "/api/auth/login",
        json={"email": "wrongpw@test.com", "password": "wrongpassword"},
    )
    assert response.status_code == 401
    assert response.json()["code"] == "auth_invalid_credentials"


async def test_logout_returns_204_and_clears_cookie(async_client):
    response = await async_client.post("/api/auth/logout")
    assert response.status_code == 204
    set_cookie = response.headers.get("set-cookie", "")
    assert COOKIE_NAME in set_cookie


async def test_register_wrong_admin_token_creates_regular_user(async_client):
    response = await async_client.post(
        "/api/auth/register",
        json={
            "email": "wrongtoken@test.com",
            "password": "testpassword",
            "name": "Wrong Token",
            "admin_token": "not-the-right-token",
        },
    )
    assert response.status_code == 201
    assert response.json()["user"]["is_admin"] is False


async def test_bearer_token_auth_works(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "bearer@test.com", "password": "testpassword", "name": "Bearer"},
    )
    login_resp = await async_client.post(
        "/api/auth/login",
        json={"email": "bearer@test.com", "password": "testpassword"},
    )
    token = login_resp.json()["jwt"]
    # Use a fresh client without cookies — bearer header only
    response = await async_client.get(
        "/api/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json()["email"] == "bearer@test.com"
