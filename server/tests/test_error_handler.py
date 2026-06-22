from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError


async def test_error_handler_returns_code_and_message(async_client):
    # Register so we can hit a protected route and trigger AUTH_FORBIDDEN
    await async_client.post(
        "/api/auth/register",
        json={"email": "user@test.com", "password": "testpassword", "name": "User"},
    )
    # Login to get a JWT cookie
    await async_client.post(
        "/api/auth/login",
        json={"email": "user@test.com", "password": "testpassword"},
    )
    # Hit an admin-only route as a non-admin
    response = await async_client.get("/api/admin/config")
    assert response.status_code == 403
    body = response.json()
    assert "code" in body
    assert "message" in body


def test_error_status_map_covers_all_new_codes():
    from openctopus_server.errors.http import ERROR_STATUS

    assert ERROR_STATUS[ErrorCode.AUTH_UNAUTHORIZED] == 401
    assert ERROR_STATUS[ErrorCode.AUTH_INVALID_CREDENTIALS] == 401
    assert ERROR_STATUS[ErrorCode.AUTH_FORBIDDEN] == 403
    assert ERROR_STATUS[ErrorCode.AUTH_EMAIL_TAKEN] == 409
    assert ERROR_STATUS[ErrorCode.AUTH_LAST_ADMIN_REQUIRED] == 409
    assert ERROR_STATUS[ErrorCode.USER_NOT_FOUND] == 404
    assert ERROR_STATUS[ErrorCode.CONFIG_VALIDATION_FAILED] == 400
