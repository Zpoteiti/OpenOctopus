"""Opt-in real RustFS and two-Client Py8c directory transfer E2E."""

from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
from typing import Any
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
from openctopus_server.devices.transfer import TransferError
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError, WorkspaceError
from openctopus_server.errors.http import register_error_handler
from openctopus_server.tools.file_transfer import FileTransferRequest, FileTransferTool
from openctopus_server.workspace.fs import WorkspaceFS, WorkspaceTarget, get_workspace_fs
from openctopus_server.workspace.service import WorkspaceService, get_workspace_service
from openctopus_server.workspace.storage import ObjectStorage, build_object_storage

pytestmark = pytest.mark.skipif(
    os.environ.get("PY8C_REAL_E2E") != "1",
    reason="set PY8C_REAL_E2E=1 to run real Py8c directory transfers",
)

_QUOTA_BYTES = 128 * 1024 * 1024
_SHARED_QUOTA_BYTES = 1_000_000
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


async def test_real_rustfs_and_two_clients_cover_all_directory_directions(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Exercise copy and move in every public directory-transfer direction."""

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
    shared_owner_id: UUID | None = None
    shared_workspace_id: UUID | None = None

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

            owner_response = await http_client.post(
                "/api/auth/register",
                json={
                    "email": f"py8c-owner-{uuid4().hex}@example.com",
                    "password": "testpassword",
                    "name": "Py8c Shared Owner",
                },
            )
            assert owner_response.status_code == 201, owner_response.text
            owner_auth = owner_response.json()
            shared_owner_id = UUID(owner_auth["user"]["id"])
            owner_headers = {"Authorization": f"Bearer {owner_auth['jwt']}"}
            shared_response = await http_client.post(
                "/api/workspaces",
                headers=owner_headers,
                json={"name": "Py8cShared", "quota_bytes": _SHARED_QUOTA_BYTES},
            )
            assert shared_response.status_code == 201, shared_response.text
            shared = shared_response.json()
            shared_workspace_id = UUID(shared["id"])
            shared_target = WorkspaceTarget.shared(shared_workspace_id)
            shared_ref = shared["ref"]
            member_response = await http_client.post(
                f"/api/workspaces/{shared_ref}/members",
                headers=owner_headers,
                json={"user_id": str(user_id)},
            )
            assert member_response.status_code == 201, member_response.text
            http_client.cookies.clear()
            member_view = await http_client.get(
                f"/api/workspaces/{shared_ref}",
                headers=headers,
            )
            assert member_view.status_code == 200, member_view.text
            assert member_view.json()["quota_bytes"] == _SHARED_QUOTA_BYTES

            async def create_device(name: str, workspace: Path) -> tuple[str, str, UUID]:
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
                return (
                    created["device"]["name"],
                    created["token"],
                    UUID(created["device"]["id"]),
                )

            source_name, source_token, source_id = await create_device(
                "py8c-source",
                source_workspace,
            )
            destination_name, destination_token, destination_id = await create_device(
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

            transfer_tool = FileTransferTool(
                pg_engine,
                workspace_service,
                registry,
                workspace_fs,
            )
            device_targets = {
                source_name: source_id,
                destination_name: destination_id,
            }

            async def machine_transfer(
                source_device: str,
                source_path: str,
                destination_device: str,
                destination_path: str,
            ) -> None:
                await transfer_tool.transfer(
                    FileTransferRequest(
                        openoctopus_src_device=source_device,
                        src_path=source_path,
                        openoctopus_dst_device=destination_device,
                        dst_path=destination_path,
                        mode="copy",
                    ),
                    user_id=user_id,
                    device_targets=device_targets,
                )

            async def assert_clients_healthy() -> None:
                await _wait_for_transfer_cleanup(registry)
                await _assert_client_running(source_process, secret=source_token)
                await _assert_client_running(
                    destination_process,
                    secret=destination_token,
                )

            # Personal Server -> personal Server uses real RustFS in both modes.
            await _write_server_tree(workspace_fs, server_target, "s2s-copy-source")
            s2s_copy = await transfer(
                "server",
                "s2s-copy-source",
                "server",
                "s2s-copy-result",
                mode="copy",
            )
            _assert_directory_success(s2s_copy)
            await _assert_server_tree(workspace_fs, server_target, "s2s-copy-source")
            await _assert_server_tree(workspace_fs, server_target, "s2s-copy-result")
            await _write_server_tree(workspace_fs, server_target, "s2s-move-source")
            s2s_move = await transfer(
                "server",
                "s2s-move-source",
                "server",
                "s2s-move-result",
                mode="move",
            )
            _assert_directory_success(s2s_move)
            await _assert_server_root_absent(workspace_fs, server_target, "s2s-move-source")
            await _assert_server_tree(workspace_fs, server_target, "s2s-move-result")

            # A non-owner member resolves a shared destination with its stored quota.
            await _write_server_tree(workspace_fs, server_target, "p2shared-copy-source")
            shared_copy = await transfer(
                "server",
                "p2shared-copy-source",
                "server",
                f"/{shared_ref}/copy-result",
                mode="copy",
            )
            _assert_directory_success(shared_copy)
            await _assert_server_tree(workspace_fs, server_target, "p2shared-copy-source")
            await _assert_server_tree(workspace_fs, shared_target, "copy-result")
            await _write_server_tree(workspace_fs, server_target, "p2shared-move-source")
            shared_move = await transfer(
                "server",
                "p2shared-move-source",
                "server",
                f"/{shared_ref}/move-result",
                mode="move",
            )
            _assert_directory_success(shared_move)
            await _assert_server_root_absent(
                workspace_fs, server_target, "p2shared-move-source"
            )
            await _assert_server_tree(workspace_fs, shared_target, "move-result")
            shared_usage = await http_client.get(
                f"/api/workspaces/{shared_ref}",
                headers=headers,
            )
            assert shared_usage.status_code == 200, shared_usage.text
            assert shared_usage.json()["quota_bytes"] == _SHARED_QUOTA_BYTES
            assert shared_usage.json()["bytes_used"] == 2 * sum(map(len, _TREE.values()))

            # Server -> Client traverses the real WS in copy and move modes.
            await _write_server_tree(workspace_fs, server_target, "s2a-copy-source")
            s2a_copy = await transfer(
                "server",
                "s2a-copy-source",
                source_name,
                "from-server-copy",
                mode="copy",
            )
            _assert_directory_success(s2a_copy)
            await _assert_server_tree(workspace_fs, server_target, "s2a-copy-source")
            _assert_client_tree(source_workspace, "from-server-copy")
            await _write_server_tree(workspace_fs, server_target, "s2a-move-source")
            s2a_move = await transfer(
                "server",
                "s2a-move-source",
                source_name,
                "from-server-move",
                mode="move",
            )
            _assert_directory_success(s2a_move)
            await _assert_server_root_absent(workspace_fs, server_target, "s2a-move-source")
            _assert_client_tree(source_workspace, "from-server-move")

            # Client -> Server traverses the source manifest and real RustFS upload.
            _write_client_tree(source_workspace, "a2s-copy-source")
            a2s_copy = await transfer(
                source_name,
                "a2s-copy-source",
                "server",
                "from-client-copy",
                mode="copy",
            )
            _assert_directory_success(a2s_copy)
            _assert_client_tree(source_workspace, "a2s-copy-source")
            await _assert_server_tree(workspace_fs, server_target, "from-client-copy")
            _write_client_tree(source_workspace, "a2s-move-source")
            a2s_move = await transfer(
                source_name,
                "a2s-move-source",
                "server",
                "from-client-move",
                mode="move",
            )
            _assert_directory_success(a2s_move)
            assert not (source_workspace / "a2s-move-source").exists()
            await _assert_server_tree(workspace_fs, server_target, "from-client-move")

            # Distinct Clients use the Py8b child bridge over two real WS connections.
            _write_client_tree(source_workspace, "a2b-copy-source")
            a2b_copy = await transfer(
                source_name,
                "a2b-copy-source",
                destination_name,
                "from-source-copy",
                mode="copy",
            )
            _assert_directory_success(a2b_copy)
            _assert_client_tree(source_workspace, "a2b-copy-source")
            _assert_client_tree(destination_workspace, "from-source-copy")
            assert not (
                destination_workspace / "from-source-copy" / "nested" / "empty"
            ).exists()
            _write_client_tree(source_workspace, "a2b-move-source")
            a2b_move = await transfer(
                source_name,
                "a2b-move-source",
                destination_name,
                "from-source-move",
                mode="move",
            )
            _assert_directory_success(a2b_move)
            assert not (source_workspace / "a2b-move-source").exists()
            _assert_client_tree(destination_workspace, "from-source-move")

            # Same Client copy uses the regular-file manifest; atomic move renames the tree.
            _write_client_tree(source_workspace, "local-copy-source")
            local_copy = await transfer(
                source_name,
                "local-copy-source",
                source_name,
                "local-copy-result",
                mode="copy",
            )
            _assert_directory_success(local_copy)
            _assert_client_tree(source_workspace, "local-copy-source")
            _assert_client_tree(source_workspace, "local-copy-result")
            assert not (
                source_workspace / "local-copy-result" / "nested" / "empty"
            ).exists()
            _write_client_tree(source_workspace, "local-move-source")
            local_move = await transfer(
                source_name,
                "local-move-source",
                source_name,
                "local-move-result",
                mode="move",
            )
            _assert_directory_success(local_move)
            assert not (source_workspace / "local-move-source").exists()
            _assert_client_tree(source_workspace, "local-move-result")
            assert (
                source_workspace / "local-move-result" / "nested" / "empty"
            ).is_dir()

            async def assert_failure_destination_absent(
                *,
                destination_device: str,
                destination_root: str,
                server_destination: WorkspaceTarget | None,
                client_workspace: Path | None,
            ) -> None:
                if destination_device == "server":
                    assert server_destination is not None
                    await _assert_server_root_absent(
                        workspace_fs,
                        server_destination,
                        destination_root,
                    )
                else:
                    assert client_workspace is not None
                    assert not (client_workspace / destination_root).exists()

            async def exercise_server_source_failures(
                *,
                label: str,
                destination_device: str,
                destination_path_prefix: str,
                server_destination: WorkspaceTarget | None = None,
                client_workspace: Path | None = None,
            ) -> None:
                for cause in ("drift", "cancel"):
                    source_root = f"{label}-{cause}-source"
                    destination_root = f"{label}-{cause}-destination"
                    destination_path = f"{destination_path_prefix}{destination_root}"
                    await _write_server_tree(workspace_fs, server_target, source_root)
                    blocked = asyncio.Event()
                    release = asyncio.Event()
                    original_open_stream = workspace_fs.open_stream

                    async def pause_second_source(
                        target: WorkspaceTarget,
                        relative_path: str,
                    ) -> Any:
                        if (
                            target == server_target
                            and relative_path == f"{source_root}/nested/large.bin"
                        ):
                            blocked.set()
                            await release.wait()
                        return await original_open_stream(target, relative_path)

                    with monkeypatch.context() as failure_patch:
                        failure_patch.setattr(
                            workspace_fs,
                            "open_stream",
                            pause_second_source,
                        )
                        attempt = asyncio.create_task(
                            machine_transfer(
                                "server",
                                source_root,
                                destination_device,
                                destination_path,
                            )
                        )
                        await asyncio.wait_for(blocked.wait(), timeout=5)
                        if cause == "drift":
                            await object_storage.write(
                                f"users/{user_id}/{source_root}/nested/large.bin",
                                b"changed after manifest",
                            )
                        else:
                            attempt.cancel()
                        release.set()
                        if cause == "drift":
                            with pytest.raises((OpenOctopusError, TransferError)) as caught:
                                await attempt
                            assert caught.value.code in {
                                ErrorCode.WORKSPACE_FILE_CHANGED,
                                ErrorCode.WORKSPACE_FILE_CHANGED.value,
                            }
                        else:
                            with pytest.raises(asyncio.CancelledError):
                                await attempt

                    await assert_failure_destination_absent(
                        destination_device=destination_device,
                        destination_root=destination_root,
                        server_destination=server_destination,
                        client_workspace=client_workspace,
                    )
                    await assert_clients_healthy()

            async def exercise_client_source_failures(
                *,
                label: str,
                destination_device: str,
                server_destination: WorkspaceTarget | None = None,
                client_workspace: Path | None = None,
            ) -> None:
                for cause in ("drift", "cancel"):
                    source_root = f"{label}-{cause}-source"
                    destination_root = f"{label}-{cause}-destination"
                    _write_client_tree(source_workspace, source_root)
                    blocked = asyncio.Event()
                    release = asyncio.Event()
                    original_dispatch = registry.dispatch_tool_on_snapshot
                    blocked_once = False

                    async def pause_second_source_authorization(**kwargs: Any) -> Any:
                        nonlocal blocked_once
                        args = kwargs.get("args")
                        if (
                            not blocked_once
                            and kwargs.get("expected_device_name") == source_name
                            and isinstance(args, dict)
                            and args.get("operation")
                            == "transfer_directory_authorize_source_child"
                            and args.get("relative_path") == "nested/large.bin"
                        ):
                            blocked_once = True
                            blocked.set()
                            await release.wait()
                        return await original_dispatch(**kwargs)

                    with monkeypatch.context() as failure_patch:
                        failure_patch.setattr(
                            registry,
                            "dispatch_tool_on_snapshot",
                            pause_second_source_authorization,
                        )
                        attempt = asyncio.create_task(
                            machine_transfer(
                                source_name,
                                source_root,
                                destination_device,
                                destination_root,
                            )
                        )
                        await asyncio.wait_for(blocked.wait(), timeout=5)
                        if cause == "drift":
                            (source_workspace / source_root / "nested" / "large.bin").write_bytes(
                                b"changed after manifest"
                            )
                        else:
                            attempt.cancel()
                        release.set()
                        if cause == "drift":
                            with pytest.raises((OpenOctopusError, TransferError)) as caught:
                                await attempt
                            assert caught.value.code in {
                                ErrorCode.WORKSPACE_FILE_CHANGED,
                                ErrorCode.WORKSPACE_FILE_CHANGED.value,
                            }
                        else:
                            with pytest.raises(asyncio.CancelledError):
                                await attempt

                    await assert_failure_destination_absent(
                        destination_device=destination_device,
                        destination_root=destination_root,
                        server_destination=server_destination,
                        client_workspace=client_workspace,
                    )
                    await assert_clients_healthy()

            async def exercise_same_client_failures() -> None:
                for cause in ("drift", "cancel"):
                    source_root = f"local-{cause}-source"
                    destination_root = f"local-{cause}-destination"
                    _write_client_tree(source_workspace, source_root)
                    blocked = asyncio.Event()
                    release = asyncio.Event()
                    original_dispatch = registry.dispatch_tool_on_snapshot
                    blocked_once = False

                    async def pause_local_start(**kwargs: Any) -> Any:
                        nonlocal blocked_once
                        args = kwargs.get("args")
                        if (
                            not blocked_once
                            and kwargs.get("expected_device_name") == source_name
                            and isinstance(args, dict)
                            and args.get("operation") == "transfer_local_directory_start"
                            and args.get("source_path") == source_root
                        ):
                            blocked_once = True
                            blocked.set()
                            await release.wait()
                        return await original_dispatch(**kwargs)

                    with monkeypatch.context() as failure_patch:
                        failure_patch.setattr(
                            registry,
                            "dispatch_tool_on_snapshot",
                            pause_local_start,
                        )
                        attempt = asyncio.create_task(
                            machine_transfer(
                                source_name,
                                source_root,
                                source_name,
                                destination_root,
                            )
                        )
                        await asyncio.wait_for(blocked.wait(), timeout=5)
                        if cause == "drift":
                            (source_workspace / source_root / "nested" / "large.bin").write_bytes(
                                b"changed after manifest"
                            )
                        else:
                            attempt.cancel()
                        release.set()
                        if cause == "drift":
                            with pytest.raises((OpenOctopusError, TransferError)) as caught:
                                await attempt
                            assert caught.value.code in {
                                ErrorCode.WORKSPACE_FILE_CHANGED,
                                ErrorCode.WORKSPACE_FILE_CHANGED.value,
                            }
                        else:
                            with pytest.raises(asyncio.CancelledError):
                                await attempt

                    assert not (source_workspace / destination_root).exists()
                    await assert_clients_healthy()

            # Each Direction E2E row gets deterministic post-manifest drift and
            # cancellation. Cross-site hooks stop before the second child, proving
            # conditional cleanup of the already committed first child.
            await exercise_server_source_failures(
                label="failure-s2s-personal",
                destination_device="server",
                destination_path_prefix="",
                server_destination=server_target,
            )
            await exercise_server_source_failures(
                label="failure-s2s-shared",
                destination_device="server",
                destination_path_prefix=f"/{shared_ref}/",
                server_destination=shared_target,
            )
            await exercise_server_source_failures(
                label="failure-s2a",
                destination_device=source_name,
                destination_path_prefix="",
                client_workspace=source_workspace,
            )
            await exercise_client_source_failures(
                label="failure-a2s",
                destination_device="server",
                server_destination=server_target,
            )
            await exercise_client_source_failures(
                label="failure-a2b",
                destination_device=destination_name,
                client_workspace=destination_workspace,
            )
            await exercise_same_client_failures()

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
            if shared_owner_id is not None:
                with contextlib.suppress(WorkspaceError):
                    await _delete_prefix(object_storage, f"users/{shared_owner_id}/")
            if shared_workspace_id is not None:
                with contextlib.suppress(WorkspaceError):
                    await _delete_prefix(
                        object_storage,
                        f"workspaces/{shared_workspace_id}/",
                    )
            await object_storage.close()
            get_settings.cache_clear()
            get_engine.cache_clear()
            get_device_registry.cache_clear()
