"""Opt-in real RustFS and two-Client Py8c directory transfer E2E."""

from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine
from test_device_client_e2e import (
    _start_server,
    _stop_client,
    _stop_server,
    _wait_online,
)
from test_device_client_transfer_e2e import (
    _assert_client_running,
    _start_source_and_configured_clients,
    _wait_for_transfer_cleanup,
)

from openctopus_server.api.router import router as api_router
from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.errors.http import register_error_handler
from openctopus_server.workspace.fs import WorkspaceFS, WorkspaceTarget, get_workspace_fs
from openctopus_server.workspace.service import WorkspaceService, get_workspace_service
from openctopus_server.workspace.storage import ObjectStorage, build_object_storage

pytestmark = pytest.mark.skipif(
    os.environ.get("PY8C_REAL_E2E") != "1",
    reason="set PY8C_REAL_E2E=1 to run real Py8c directory transfers",
)

_QUOTA_BYTES = 128 * 1024 * 1024
_TREE = {
    ".hidden": b"hidden\n",
    "nested/large.bin": (b"py8c-multi-chunk\x00\xff" * 8192) + b"end",
    "nested/zero.bin": b"",
}


async def _delete_prefix(storage: ObjectStorage, prefix: str) -> None:
    start_after: str | None = None
    while True:
        page = await storage.list_page(prefix, start_after=start_after)
        for item in page.items:
            await storage.delete(item.object_name)
        if page.next_start_after is None:
            return
        start_after = page.next_start_after


async def _write_server_tree(
    workspace_fs: WorkspaceFS,
    target: WorkspaceTarget,
    root: str,
) -> None:
    for relative_path, content in _TREE.items():
        await workspace_fs.write(
            target,
            f"{root}/{relative_path}",
            content,
            quota_bytes=_QUOTA_BYTES,
            if_none_match=True,
        )


def _write_client_tree(workspace: Path, root: str) -> None:
    tree_root = workspace / root
    for relative_path, content in _TREE.items():
        path = tree_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    (tree_root / "nested" / "empty").mkdir()


async def _assert_server_tree(
    workspace_fs: WorkspaceFS,
    target: WorkspaceTarget,
    root: str,
) -> None:
    for relative_path, content in _TREE.items():
        assert await workspace_fs.read(target, f"{root}/{relative_path}") == content


def _assert_client_tree(workspace: Path, root: str) -> None:
    for relative_path, content in _TREE.items():
        assert (workspace / root / relative_path).read_bytes() == content


def _assert_directory_success(response: httpx.Response) -> None:
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["kind"] == "directory"
    assert body["files_transferred"] == len(_TREE)
    assert body["bytes_transferred"] == sum(map(len, _TREE.values()))
    assert len(body["sha256"]) == 64
    assert body["warnings"] == []


async def _assert_server_root_absent(
    workspace_fs: WorkspaceFS,
    target: WorkspaceTarget,
    root: str,
) -> None:
    for relative_path in _TREE:
        with pytest.raises(WorkspaceError) as caught:
            await workspace_fs.stat(target, f"{root}/{relative_path}")
        assert caught.value.code == ErrorCode.WORKSPACE_NOT_FOUND


async def test_real_rustfs_and_two_clients_cover_five_directory_topologies(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Exercise five public routes through TCP, WS, native filesystems, and RustFS."""

    monkeypatch.setenv(
        "OPENOCTOPUS_DATABASE_URL", pg_engine.url.render_as_string(hide_password=False)
    )
    get_settings.cache_clear()
    get_engine.cache_clear()
    get_device_registry.cache_clear()

    settings = get_settings()
    object_storage = build_object_storage(settings)
    await object_storage.probe_startup()
    workspace_fs = WorkspaceFS(object_storage)
    workspace_service = WorkspaceService(workspace_fs)
    registry = get_device_registry()
    app = FastAPI()
    app.include_router(api_router)
    app.dependency_overrides[get_workspace_service] = lambda: workspace_service
    app.dependency_overrides[get_workspace_fs] = lambda: workspace_fs
    register_error_handler(app)
    server, server_task, server_url, listener = await _start_server(app)

    source_workspace = tmp_path / "source-client"
    destination_workspace = tmp_path / "destination-client"
    source_workspace.mkdir()
    destination_workspace.mkdir()
    clients: list[tuple[asyncio.subprocess.Process, str]] = []
    user_id: UUID | None = None

    try:
        async with httpx.AsyncClient(
            base_url=server_url,
            timeout=30,
            trust_env=False,
        ) as http_client:
            auth_response = await http_client.post(
                "/api/auth/register",
                json={
                    "email": f"py8c-e2e-{uuid4().hex}@example.com",
                    "password": "testpassword",
                    "name": "Py8c E2E",
                },
            )
            assert auth_response.status_code == 201, auth_response.text
            auth = auth_response.json()
            jwt = auth["jwt"]
            user_id = UUID(auth["user"]["id"])
            headers = {"Authorization": f"Bearer {jwt}"}
            server_target = WorkspaceTarget.personal(user_id)

            async def create_device(name: str, workspace: Path) -> tuple[str, str]:
                response = await http_client.post(
                    "/api/devices",
                    headers=headers,
                    json={
                        "name": name,
                        "workspace_path": str(workspace),
                        "restrict_to_workspace": True,
                        "ssrf_denylist": [],
                    },
                )
                assert response.status_code == 201, response.text
                created = response.json()
                return created["device"]["name"], created["token"]

            source_name, source_token = await create_device(
                "py8c-source",
                source_workspace,
            )
            destination_name, destination_token = await create_device(
                "py8c-destination",
                destination_workspace,
            )
            source_process, destination_process = (
                await _start_source_and_configured_clients(
                    server_url,
                    source_token,
                    destination_token,
                    monkeypatch,
                )
            )
            clients.extend(
                (
                    (source_process, source_token),
                    (destination_process, destination_token),
                )
            )
            await _wait_online(
                http_client,
                jwt,
                source_name,
                online=True,
                process=source_process,
            )
            await _wait_online(
                http_client,
                jwt,
                destination_name,
                online=True,
                process=destination_process,
            )

            async def transfer(
                source_device: str,
                source_path: str,
                destination_device: str,
                destination_path: str,
                *,
                mode: str,
            ) -> httpx.Response:
                response = await http_client.post(
                    "/api/workspace/transfer",
                    headers=headers,
                    json={
                        "openoctopus_src_device": source_device,
                        "src_path": source_path,
                        "openoctopus_dst_device": destination_device,
                        "dst_path": destination_path,
                        "mode": mode,
                    },
                )
                await _wait_for_transfer_cleanup(registry)
                await _assert_client_running(source_process, secret=source_token)
                await _assert_client_running(
                    destination_process,
                    secret=destination_token,
                )
                return response

            # Server -> Server uses real RustFS objects and its prefix coordinator.
            await _write_server_tree(workspace_fs, server_target, "s2s-source")
            s2s = await transfer(
                "server",
                "s2s-source",
                "server",
                "s2s-result",
                mode="copy",
            )
            _assert_directory_success(s2s)
            await _assert_server_tree(workspace_fs, server_target, "s2s-source")
            await _assert_server_tree(workspace_fs, server_target, "s2s-result")

            # Server -> Client move proves source cleanup happens after all children.
            await _write_server_tree(workspace_fs, server_target, "s2a-source")
            s2a = await transfer(
                "server",
                "s2a-source",
                source_name,
                "from-server",
                mode="move",
            )
            _assert_directory_success(s2a)
            await _assert_server_root_absent(workspace_fs, server_target, "s2a-source")
            _assert_client_tree(source_workspace, "from-server")

            # Client -> Server copy traverses the source manifest and real RustFS upload.
            _write_client_tree(source_workspace, "a2s-source")
            a2s = await transfer(
                source_name,
                "a2s-source",
                "server",
                "from-client",
                mode="copy",
            )
            _assert_directory_success(a2s)
            _assert_client_tree(source_workspace, "a2s-source")
            await _assert_server_tree(workspace_fs, server_target, "from-client")

            # Distinct Clients use the Py8b child bridge over two real WS connections.
            _write_client_tree(source_workspace, "a2b-source")
            a2b = await transfer(
                source_name,
                "a2b-source",
                destination_name,
                "from-source-client",
                mode="copy",
            )
            _assert_directory_success(a2b)
            _assert_client_tree(source_workspace, "a2b-source")
            _assert_client_tree(destination_workspace, "from-source-client")
            assert not (
                destination_workspace / "from-source-client" / "nested" / "empty"
            ).exists()

            # Same Client move must preserve empty directories through native rename.
            _write_client_tree(source_workspace, "local-source")
            local = await transfer(
                source_name,
                "local-source",
                source_name,
                "local-result",
                mode="move",
            )
            _assert_directory_success(local)
            assert not (source_workspace / "local-source").exists()
            _assert_client_tree(source_workspace, "local-result")
            assert (
                source_workspace / "local-result" / "nested" / "empty"
            ).is_dir()

            # A representative existing-root conflict leaves both trees unchanged.
            _write_client_tree(source_workspace, "conflict-source")
            conflict_root = destination_workspace / "conflict-result"
            conflict_root.mkdir()
            (conflict_root / "existing.txt").write_bytes(b"keep me")
            conflict = await transfer(
                source_name,
                "conflict-source",
                destination_name,
                "conflict-result",
                mode="copy",
            )
            assert conflict.status_code == 409, conflict.text
            assert conflict.json()["code"] == "workspace_file_changed"
            _assert_client_tree(source_workspace, "conflict-source")
            assert (conflict_root / "existing.txt").read_bytes() == b"keep me"

            assert registry.transfers.active_slots == 0
            assert not list(source_workspace.rglob("*.openoctopus-*.tmp"))
            assert not list(destination_workspace.rglob("*.openoctopus-*.tmp"))
            assert not (
                await object_storage.list_page("_openoctopus-transfers/", limit=1)
            ).items
    finally:
        for process, token in clients:
            if process.returncode is None:
                with contextlib.suppress(Exception):
                    await _stop_client(process, expected_returncode=0, secret=token)
        try:
            await _stop_server(
                server,
                server_task,
                listener,
                registry,
                get_engine(),
            )
        finally:
            if user_id is not None:
                with contextlib.suppress(WorkspaceError):
                    await _delete_prefix(object_storage, f"users/{user_id}/")
            await object_storage.close()
            get_settings.cache_clear()
            get_engine.cache_clear()
            get_device_registry.cache_clear()
