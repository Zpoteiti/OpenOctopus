from fastapi import FastAPI, Request
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
}


async def openoctopus_error_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    assert isinstance(exc, OpenOctopusError)
    status = ERROR_STATUS.get(exc.code, 500)
    return JSONResponse(
        status_code=status,
        content={"code": exc.code.value, "message": exc.message},
    )


def register_error_handler(app: FastAPI) -> None:
    app.add_exception_handler(OpenOctopusError, openoctopus_error_handler)
