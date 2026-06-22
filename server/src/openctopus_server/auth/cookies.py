from fastapi.responses import JSONResponse

from openctopus_server.config import get_settings

COOKIE_NAME = "openoctopus_session"


def set_auth_cookie(response: JSONResponse, jwt: str) -> None:
    settings = get_settings()
    response.set_cookie(
        key=COOKIE_NAME,
        value=jwt,
        httponly=True,
        samesite="strict",
        secure=settings.cookie_secure,
        path="/",
    )


def clear_auth_cookie(response: JSONResponse) -> None:
    response.delete_cookie(key=COOKIE_NAME, path="/")
