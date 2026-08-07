from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError, WorkspaceError

ERROR_STATUS: dict[ErrorCode, int] = {
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
    ErrorCode.WORKSPACE_INVALID_REQUEST: 400,
    ErrorCode.WORKSPACE_REF_CONFLICT: 409,
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
    ErrorCode.AUTH_UNAUTHORIZED: 401,
    ErrorCode.AUTH_INVALID_CREDENTIALS: 401,
    ErrorCode.AUTH_FORBIDDEN: 403,
    ErrorCode.AUTH_EMAIL_TAKEN: 409,
    ErrorCode.AUTH_LAST_ADMIN_REQUIRED: 409,
    ErrorCode.USER_NOT_FOUND: 404,
    ErrorCode.CONFIG_VALIDATION_FAILED: 400,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.INVALID_MESSAGE_CONTENT: 400,
    ErrorCode.INVALID_CURSOR: 400,
    ErrorCode.PROVIDER_NOT_CONFIGURED: 503,
    ErrorCode.PROVIDER_UNAVAILABLE: 503,
    ErrorCode.PROVIDER_PROTOCOL_ERROR: 502,
}


async def openoctopus_error_handler(request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, OpenOctopusError)
    status = ERROR_STATUS.get(exc.code, 500)
    return JSONResponse(
        status_code=status,
        content={"code": exc.code.value, "message": exc.message},
        headers=exc.headers if isinstance(exc, WorkspaceError) else None,
    )


def register_error_handler(app: FastAPI) -> None:
    app.add_exception_handler(OpenOctopusError, openoctopus_error_handler)
    app.add_exception_handler(RequestValidationError, message_validation_handler)


async def message_validation_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    if request.url.path.startswith("/api/workspace"):
        return JSONResponse(
            status_code=400,
            content={
                "code": ErrorCode.WORKSPACE_INVALID_REQUEST.value,
                "message": "Workspace request is invalid",
            },
        )
    is_messages_route = request.url.path.startswith("/api/sessions/") and request.url.path.endswith(
        "/messages"
    )
    if request.method == "POST" and is_messages_route:
        return JSONResponse(
            status_code=400,
            content={
                "code": ErrorCode.INVALID_MESSAGE_CONTENT.value,
                "message": "Message request is invalid",
            },
        )
    if request.method == "GET" and is_messages_route:
        errors = exc.errors()
        query_fields = {"before", "after", "limit"}
        if errors and all(
            len(error["loc"]) >= 2
            and error["loc"][0] == "query"
            and error["loc"][1] in query_fields
            for error in errors
        ):
            return JSONResponse(
                status_code=400,
                content={
                    "code": ErrorCode.INVALID_CURSOR.value,
                    "message": "Message query parameters are invalid",
                },
            )
    response = await request_validation_exception_handler(request, exc)
    assert isinstance(response, JSONResponse)
    return response
