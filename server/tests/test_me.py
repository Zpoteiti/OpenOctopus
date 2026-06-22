async def test_get_me_returns_user(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "me@test.com", "password": "testpassword", "name": "Me User"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "me@test.com", "password": "testpassword"},
    )
    response = await async_client.get("/api/me")
    assert response.status_code == 200
    assert response.json()["email"] == "me@test.com"


async def test_get_me_without_token_returns_401(async_client):
    response = await async_client.get("/api/me")
    assert response.status_code == 401
    assert response.json()["code"] == "auth_unauthorized"


async def test_patch_me_name(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "me@test.com", "password": "testpassword", "name": "Me User"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "me@test.com", "password": "testpassword"},
    )
    response = await async_client.patch("/api/me", json={"name": "New Name"})
    assert response.status_code == 200
    assert response.json()["name"] == "New Name"


async def test_patch_me_email_taken_returns_409(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "first@test.com", "password": "testpassword", "name": "First"},
    )
    await async_client.post(
        "/api/auth/register",
        json={"email": "second@test.com", "password": "testpassword", "name": "Second"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "second@test.com", "password": "testpassword"},
    )
    response = await async_client.patch("/api/me", json={"email": "first@test.com"})
    assert response.status_code == 409
    assert response.json()["code"] == "auth_email_taken"


async def test_patch_me_password(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "me@test.com", "password": "testpassword", "name": "Me User"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "me@test.com", "password": "testpassword"},
    )
    response = await async_client.patch("/api/me", json={"password": "newpassword123"})
    assert response.status_code == 200
    response = await async_client.post(
        "/api/auth/login",
        json={"email": "me@test.com", "password": "newpassword123"},
    )
    assert response.status_code == 200


async def test_delete_me_returns_204(async_client):
    await async_client.post(
        "/api/auth/register",
        json={"email": "delme@test.com", "password": "testpassword", "name": "Del"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "delme@test.com", "password": "testpassword"},
    )
    response = await async_client.delete("/api/me")
    assert response.status_code == 204


async def test_delete_me_last_admin_returns_409(async_client):
    await async_client.post(
        "/api/auth/register",
        json={
            "email": "lastadmin@test.com",
            "password": "testpassword",
            "name": "Last Admin",
            "admin_token": "dev-admin-token",
        },
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "lastadmin@test.com", "password": "testpassword"},
    )
    response = await async_client.delete("/api/me")
    assert response.status_code == 409
    assert response.json()["code"] == "auth_last_admin_required"
