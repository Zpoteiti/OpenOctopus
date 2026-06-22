import uuid


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
        assert u["quota_bytes"] is None
        assert u["bytes_used"] is None
        assert u["locked"] is None


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
