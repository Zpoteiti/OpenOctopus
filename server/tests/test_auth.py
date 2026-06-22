from openctopus_server.auth.cookies import COOKIE_NAME


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
