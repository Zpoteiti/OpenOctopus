from fastapi import Request

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
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
