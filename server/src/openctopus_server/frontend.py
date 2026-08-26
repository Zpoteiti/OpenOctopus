import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.types import Scope

from openctopus_server.errors.http import http_exception_handler

FRONTEND_BUILD_DIR = Path(__file__).resolve().parent / "assets" / "web"

_RESERVED_PATHS = (
    "/api",
    "/assets",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/ws",
)


class ImmutableStaticFiles(StaticFiles):
    def file_response(
        self,
        full_path: str | os.PathLike[str],
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        response = super().file_response(full_path, stat_result, scope, status_code)
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


def install_frontend(
    app: FastAPI,
    directory: Path | None = FRONTEND_BUILD_DIR,
) -> bool:
    """Serve a built browser app without changing API-only development."""
    if directory is None:
        return False

    index = directory / "index.html"
    if not index.is_file():
        return False

    assets = directory / "assets"
    if assets.is_dir():
        app.mount("/assets", ImmutableStaticFiles(directory=assets), name="frontend-assets")

    async def frontend_or_not_found(request: Request, exc: Exception) -> Response:
        if _is_frontend_navigation(request):
            return FileResponse(
                index,
                media_type="text/html",
                headers={"Cache-Control": "no-cache"},
            )
        return await http_exception_handler(request, exc)

    app.add_exception_handler(404, frontend_or_not_found)
    return True


def _is_frontend_navigation(request: Request) -> bool:
    if request.method != "GET":
        return False
    if not _accepts_html(request.headers.get("accept", "")):
        return False
    path = request.url.path
    return not any(path == prefix or path.startswith(f"{prefix}/") for prefix in _RESERVED_PATHS)


def _accepts_html(header: str) -> bool:
    for item in header.split(","):
        media_type, *parameters = item.split(";")
        if media_type.strip().lower() != "text/html":
            continue
        quality = 1.0
        for parameter in parameters:
            key, separator, value = parameter.partition("=")
            if separator and key.strip().lower() == "q":
                try:
                    quality = float(value.strip())
                except ValueError:
                    quality = 0.0
        if quality > 0:
            return True
    return False
