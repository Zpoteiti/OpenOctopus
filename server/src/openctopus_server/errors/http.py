from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError

ERROR_STATUS: dict[ErrorCode, int] = {
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
    )


def register_error_handler(app: FastAPI) -> None:
    app.add_exception_handler(OpenOctopusError, openoctopus_error_handler)
    app.add_exception_handler(RequestValidationError, message_validation_handler)


async def message_validation_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    assert isinstance(exc, RequestValidationError)
    if (
        request.method == "POST"
        and request.url.path.startswith("/api/sessions/")
        and request.url.path.endswith("/messages")
    ):
        return JSONResponse(
            status_code=400,
            content={
                "code": ErrorCode.INVALID_MESSAGE_CONTENT.value,
                "message": "Message request is invalid",
            },
        )
    response = await request_validation_exception_handler(request, exc)
    assert isinstance(response, JSONResponse)
    return response
