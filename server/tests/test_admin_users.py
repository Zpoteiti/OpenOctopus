import uuid


async def _register_admin(client):
    await client.post(
        "/api/auth/register",
        json={
            "email": "admin@test.com",
            "password": "testpassword",
            "name": "Admin",
            "admin_token": "dev-admin-token",
        },
    )
    await client.post(
        "/api/auth/login",
        json={"email": "admin@test.com", "password": "testpassword"},
    )


async def _register_user(client, email="regular@test.com"):
    # Use a separate POST that doesn't interfere with the client's cookie jar.
    # Register returns a Set-Cookie that would overwrite the admin's session.
    await client.post(
        "/api/auth/register",
        json={"email": email, "password": "testpassword", "name": "Regular"},
    )


async def _relogin_admin(client):
    await client.post(
        "/api/auth/login",
        json={"email": "admin@test.com", "password": "testpassword"},
    )


async def test_list_users(async_client):
    await _register_admin(async_client)
    await _register_user(async_client, "user1@test.com")
    await _register_user(async_client, "user2@test.com")
    await _relogin_admin(async_client)
    response = await async_client.get("/api/admin/users")
    assert response.status_code == 200
    users = response.json()
    assert len(users) >= 3
    for u in users:
        assert "id" in u
        assert "email" in u
        assert u["quota_bytes"] is None
        assert u["bytes_used"] is None
        assert u["locked"] is None


async def test_list_users_pagination(async_client):
    await _register_admin(async_client)
    for i in range(5):
        await _register_user(async_client, f"page{i}@test.com")
    await _relogin_admin(async_client)
    response = await async_client.get("/api/admin/users?limit=2&offset=0")
    assert response.status_code == 200
    assert len(response.json()) == 2


async def test_delete_user(async_client):
    await _register_admin(async_client)
    await _register_user(async_client, "todelete@test.com")
    await _relogin_admin(async_client)
    users = (await async_client.get("/api/admin/users")).json()
    target = next(u for u in users if u["email"] == "todelete@test.com")
    response = await async_client.delete(f"/api/admin/users/{target['id']}")
    assert response.status_code == 204


async def test_delete_user_not_found(async_client):
    await _register_admin(async_client)
    response = await async_client.delete(f"/api/admin/users/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.json()["code"] == "user_not_found"


async def test_delete_last_admin_returns_409(async_client):
    await _register_admin(async_client)
    users = (await async_client.get("/api/admin/users")).json()
    admin = next(u for u in users if u["is_admin"])
    response = await async_client.delete(f"/api/admin/users/{admin['id']}")
    assert response.status_code == 409
    assert response.json()["code"] == "auth_last_admin_required"


async def test_non_admin_list_users_returns_403(async_client):
    await _register_user(async_client, "nonadmin@test.com")
    await async_client.post(
        "/api/auth/login",
        json={"email": "nonadmin@test.com", "password": "testpassword"},
    )
    response = await async_client.get("/api/admin/users")
    assert response.status_code == 403
    assert response.json()["code"] == "auth_forbidden"
