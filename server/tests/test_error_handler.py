from fastapi import Request

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import DeviceError, WorkspaceError
from openctopus_server.errors.http import openoctopus_error_handler


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


async def test_unmatched_route_and_method_use_stable_error_envelopes(async_client):
    missing = await async_client.get("/api/does-not-exist")
    method = await async_client.put("/health")

    assert missing.status_code == 404
    assert missing.json() == {"code": "not_found", "message": "Route not found"}
    assert method.status_code == 405
    assert method.json() == {
        "code": "invalid_request",
        "message": "Method not allowed",
    }


async def test_generic_validation_uses_stable_error_envelope(user_client):
    response = await user_client.get("/api/sessions/not-a-uuid/messages")

    assert response.status_code == 400
    assert response.json() == {
        "code": "invalid_request",
        "message": "Request is invalid",
    }


async def test_admin_config_validation_uses_config_error_envelope(admin_client):
    response = await admin_client.patch(
        "/api/admin/config",
        json={"unknown_key": True},
    )

    assert response.status_code == 400
    assert response.json() == {
        "code": "config_validation_failed",
        "message": "Admin configuration is invalid",
    }


async def test_workspace_error_handler_preserves_retry_header() -> None:
    response = await openoctopus_error_handler(
        Request({"type": "http", "method": "GET", "path": "/", "headers": []}),
        WorkspaceError(
            ErrorCode.WORKSPACE_TRANSFER_BUSY,
            "Workspace transfer capacity is busy",
            headers={"Retry-After": "5"},
        ),
    )

    assert response.status_code == 429
    assert response.headers["retry-after"] == "5"


def test_error_status_map_covers_all_new_codes():
    from openctopus_server.errors.http import ERROR_STATUS

    assert ERROR_STATUS[ErrorCode.AUTH_UNAUTHORIZED] == 401
    assert ERROR_STATUS[ErrorCode.AUTH_INVALID_CREDENTIALS] == 401
    assert ERROR_STATUS[ErrorCode.AUTH_FORBIDDEN] == 403
    assert ERROR_STATUS[ErrorCode.AUTH_EMAIL_TAKEN] == 409
    assert ERROR_STATUS[ErrorCode.AUTH_LAST_ADMIN_REQUIRED] == 409
    assert ERROR_STATUS[ErrorCode.USER_NOT_FOUND] == 404
    assert ERROR_STATUS[ErrorCode.CONFIG_VALIDATION_FAILED] == 400
    assert ERROR_STATUS[ErrorCode.DEVICE_CONFIG_CONFLICT] == 409
    assert ERROR_STATUS[ErrorCode.DEVICE_OFFLINE] == 409


async def test_mcp_config_validation_uses_422_without_changing_admin_validation() -> None:
    response = await openoctopus_error_handler(
        Request(
            {
                "type": "http",
                "method": "PATCH",
                "path": "/api/devices/laptop/config",
                "headers": [],
            }
        ),
        DeviceError(ErrorCode.CONFIG_VALIDATION_FAILED, "MCP config is invalid"),
    )

    assert response.status_code == 422


def test_error_status_map_covers_workspace_codes():
    from openctopus_server.errors.http import ERROR_STATUS

    expected = {
        ErrorCode.WORKSPACE_NOT_FOUND: 404,
        ErrorCode.WORKSPACE_PERMISSION_DENIED: 403,
        ErrorCode.WORKSPACE_BLOCKED_PATH: 400,
        ErrorCode.WORKSPACE_SYMLINK_ESCAPE: 403,
        ErrorCode.WORKSPACE_SOFT_LOCKED: 409,
        ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE: 409,
        ErrorCode.WORKSPACE_QUOTA_EXCEEDED: 409,
        ErrorCode.WORKSPACE_FILE_CHANGED: 409,
        ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT: 422,
        ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT: 413,
        ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE: 413,
        ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE: 503,
        ErrorCode.WORKSPACE_STORAGE_ERROR: 503,
        ErrorCode.WORKSPACE_TRANSFER_BUSY: 429,
        ErrorCode.WORKSPACE_TRANSFER_TIMEOUT: 408,
        ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED: 502,
    }

    assert {code: ERROR_STATUS[code] for code in expected} == expected


def test_error_status_map_covers_workspace_tool_codes():
    from openctopus_server.errors.http import ERROR_STATUS

    expected = {
        ErrorCode.TOOL_NO_MATCH: 409,
        ErrorCode.TOOL_AMBIGUOUS_EDIT: 409,
        ErrorCode.TOOL_IS_DIRECTORY: 409,
        ErrorCode.TOOL_IS_FILE: 409,
        ErrorCode.TOOL_NOT_A_DIRECTORY: 409,
        ErrorCode.TOOL_INVALID_ARGS: 400,
        ErrorCode.TOOL_INVALID_REGEX: 400,
        ErrorCode.TOOL_INVALID_GLOB: 400,
        ErrorCode.TOOL_INVALID_NOTEBOOK: 400,
        ErrorCode.TOOL_CELL_INDEX_OUT_OF_RANGE: 400,
    }

    assert {code: ERROR_STATUS[code] for code in expected} == expected


def test_error_status_map_covers_mcp_config_codes():
    from openctopus_server.errors.http import ERROR_STATUS

    expected = {
        ErrorCode.MCP_SPAWN_FAILED: 422,
        ErrorCode.MCP_MESSAGE_TOO_LARGE: 422,
        ErrorCode.MCP_WITHIN_SERVER_COLLISION: 409,
        ErrorCode.MCP_SCHEMA_COLLISION: 409,
        ErrorCode.MCP_OWNER_SCHEMA_LIMIT: 409,
        ErrorCode.MCP_SECRET_TRANSPORT_INSECURE: 409,
    }

    assert {code: ERROR_STATUS[code] for code in expected} == expected
