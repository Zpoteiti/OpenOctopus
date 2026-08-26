from pathlib import Path

from httpx import ASGITransport, AsyncClient

from openctopus_server.frontend import install_frontend
from openctopus_server.main import create_app


def _frontend_build(root: Path) -> Path:
    assets = root / "assets"
    assets.mkdir()
    (root / "index.html").write_text("<html><body>OpenOctopus SPA</body></html>", encoding="utf-8")
    (assets / "app.js").write_text("console.log('openoctopus')", encoding="utf-8")
    return root


async def test_frontend_serves_navigation_and_built_assets(
    test_app,
    async_client,
    tmp_path: Path,
) -> None:
    install_frontend(test_app, _frontend_build(tmp_path))

    root = await async_client.get("/", headers={"Accept": "text/html"})
    navigation = await async_client.get(
        "/chat/00000000-0000-4000-8000-000000000001",
        headers={"Accept": "text/html,application/xhtml+xml"},
    )
    asset = await async_client.get("/assets/app.js")

    assert root.status_code == 200
    assert root.headers["content-type"].startswith("text/html")
    assert "OpenOctopus SPA" in root.text
    assert navigation.status_code == 200
    assert navigation.text == root.text
    assert asset.status_code == 200
    assert asset.headers["content-type"].startswith("text/javascript")
    assert asset.headers["cache-control"] == "public, max-age=31536000, immutable"
    assert asset.text == "console.log('openoctopus')"
    assert root.headers["cache-control"] == "no-cache"


async def test_frontend_never_captures_reserved_or_non_navigation_requests(
    test_app,
    async_client,
    tmp_path: Path,
) -> None:
    install_frontend(test_app, _frontend_build(tmp_path))

    reserved = [
        await async_client.get(path, headers={"Accept": "text/html"})
        for path in (
            "/api/does-not-exist",
            "/assets/does-not-exist.js",
            "/health/does-not-exist",
            "/docs/does-not-exist",
            "/redoc/does-not-exist",
            "/openapi.json/does-not-exist",
            "/ws/does-not-exist",
        )
    ]
    json_request = await async_client.get(
        "/does-not-exist",
        headers={"Accept": "application/json"},
    )
    html_rejected = await async_client.get(
        "/does-not-exist",
        headers={"Accept": "text/html;q=0, application/json"},
    )
    post_navigation = await async_client.post(
        "/chat/does-not-exist",
        headers={"Accept": "text/html"},
    )
    wrong_method = await async_client.put("/health", headers={"Accept": "text/html"})

    for response in (*reserved, json_request, html_rejected, post_navigation):
        assert response.status_code == 404
        assert response.json() == {"code": "not_found", "message": "Route not found"}
    assert wrong_method.status_code == 405
    assert wrong_method.json() == {
        "code": "invalid_request",
        "message": "Method not allowed",
    }


async def test_frontend_keeps_health_and_fastapi_documentation_routes(
    test_app,
    async_client,
    tmp_path: Path,
) -> None:
    install_frontend(test_app, _frontend_build(tmp_path))

    health = await async_client.get("/health", headers={"Accept": "text/html"})
    docs = await async_client.get("/docs", headers={"Accept": "text/html"})
    redoc = await async_client.get("/redoc", headers={"Accept": "text/html"})
    openapi = await async_client.get("/openapi.json", headers={"Accept": "text/html"})

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert docs.status_code == 200
    assert "swagger-ui" in docs.text
    assert redoc.status_code == 200
    assert "redoc" in redoc.text.lower()
    assert openapi.status_code == 200
    assert openapi.json()["info"]["title"] == "OpenOctopus"


async def test_missing_frontend_build_keeps_api_only_app(tmp_path: Path) -> None:
    app = create_app(frontend_dir=tmp_path / "missing")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/", headers={"Accept": "text/html"})

    assert response.status_code == 404
    assert response.json() == {"code": "not_found", "message": "Route not found"}
