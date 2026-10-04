from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from urllib.parse import quote
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from test_workspace_files_api import workspace_api as workspace_api
from test_workspace_files_api import workspace_storage as workspace_storage
from test_workspace_prompt import _PromptWorkspace

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.workspace_backend import WorkspaceToolDispatcher
from openctopus_server.workspace.builtin_skills import (
    BuiltinSkillLibrary,
    get_builtin_skill_library,
    validate_builtin_skills,
)
from openctopus_server.workspace.file_content import DocumentParser
from openctopus_server.workspace.fs import WorkspaceFS, WorkspaceTarget
from openctopus_server.workspace.resolver import WorkspacePathResolver
from openctopus_server.workspace.service import PatchEdit, WorkspaceService
from openctopus_server.workspace.skills import SkillsCache
from openctopus_server.workspace.storage import ObjectStorage

_PATH = "/builtin/skills/create-skill/SKILL.md"
_ALIASES = (
    _PATH,
    "\\builtin\\skills\\create-skill\\SKILL.md",
    "/./builtin//skills/create-skill/SKILL.md",
)


def _service() -> tuple[WorkspaceService, AsyncMock]:
    storage = AsyncMock(spec=ObjectStorage)
    return WorkspaceService(WorkspaceFS(storage)), storage


async def test_library_is_shared_and_reads_use_no_object_storage(pg_engine) -> None:
    validate_builtin_skills()
    library = get_builtin_skill_library()
    service, storage = _service()
    async with AsyncSession(pg_engine) as db:
        for user_id in (uuid4(), uuid4()):
            resolved = await WorkspacePathResolver().resolve(db, user_id=user_id, path=_PATH)
            assert resolved.target == WorkspaceTarget.builtin()
            assert resolved.quota_bytes == 0
            assert await service.read(db, user_id=user_id, path=_PATH) == library.read(
                "skills/create-skill/SKILL.md", max_bytes=8 * 1024 * 1024
            )
            assert await service.usage(db, user_id=user_id, path="/builtin") == 0
    assert get_builtin_skill_library() is library
    assert storage.mock_calls == []


async def test_real_tool_dispatcher_reads_and_lists_packaged_skills(pg_engine) -> None:
    service, storage = _service()
    dispatcher = WorkspaceToolDispatcher(
        pg_engine, service, document_parser=AsyncMock(spec=DocumentParser)
    )
    ctx = ToolContext(user_id=uuid4(), session_id=uuid4(), openoctopus_device="server")
    result = await dispatcher(
        "read_file", {"path": _PATH, "offset": 1, "limit": 2000, "pages": None}, ctx
    )
    assert not result.is_error
    assert "# Create a personal skill" in str(result.content)
    listed = await dispatcher(
        "list_dir", {"path": "/builtin/skills", "max_entries": 3, "recursive": False}, ctx
    )
    assert "/builtin/skills/connect-mcp" in str(listed.content)
    assert "truncated, showing first 3 entries" in str(listed.content)
    async with AsyncSession(pg_engine) as db:
        first = await service.list_dir_page(db, user_id=ctx.user_id, path="/builtin/skills", limit=3)
        second = await service.list_dir_page(
            db, user_id=ctx.user_id, path="/builtin/skills", limit=3, offset=first.next_offset
        )
    assert first.next_offset == 3
    assert second.next_offset is None
    assert len({entry.path for entry in (*first.items, *second.items)}) == 6
    assert storage.mock_calls == []


@pytest.mark.parametrize("path", _ALIASES)
@pytest.mark.parametrize(
    "operation",
    [
        "write",
        "edit",
        "delete",
        "delete_folder",
        "patch",
        "upload",
        "transfer_source",
        "transfer_destination",
    ],
)
async def test_all_service_mutations_reject_builtin_aliases_before_storage(
    pg_engine, path: str, operation: str
) -> None:
    service, storage = _service()
    user_id = uuid4()
    async with AsyncSession(pg_engine) as db:
        with pytest.raises(WorkspaceError) as caught:
            if operation == "write":
                await service.write(db, user_id=user_id, path=path, data=b"replace")
            elif operation == "edit":
                await service.edit_text(
                    db, user_id=user_id, path=path, old_text="Create", new_text="Change"
                )
            elif operation == "delete":
                await service.delete_file(db, user_id=user_id, path=path)
            elif operation == "delete_folder":
                await service.delete_folder(db, user_id=user_id, path=path)
            elif operation == "patch":
                await service.apply_patch(
                    db,
                    user_id=user_id,
                    edits=(PatchEdit(path, "add", None, "extra"),),
                    dry_run=False,
                )
            elif operation == "upload":
                await service.authorize_upload(db, user_id=user_id, path=path)
            elif operation == "transfer_source":
                await service.authorize_transfer_source(db, user_id=user_id, path=path)
            else:
                await service.authorize_transfer_destination(db, user_id=user_id, path=path)
        assert caught.value.code is ErrorCode.WORKSPACE_BLOCKED_PATH
    assert storage.mock_calls == []


@pytest.mark.parametrize("mode", ["copy", "move"])
async def test_low_level_transfer_rejects_builtin_source_before_destination_side_effects(
    mode: str,
) -> None:
    service, storage = _service()
    with pytest.raises(WorkspaceError) as caught:
        await service._fs.transfer_server_to_server(
            WorkspaceTarget.builtin(),
            "skills/create-skill/SKILL.md",
            WorkspaceTarget.personal(uuid4()),
            "copied.md",
            user_id=uuid4(),
            quota_bytes=100_000,
            mode=mode,
        )
    assert caught.value.code is ErrorCode.WORKSPACE_BLOCKED_PATH
    assert storage.mock_calls == []


@pytest.mark.parametrize(
    "path", ["/builtin/../skills/x", "/builtin/skills/../x", "\\builtin\\..\\x"]
)
async def test_builtin_traversal_is_rejected(pg_engine, path: str) -> None:
    service, storage = _service()
    async with AsyncSession(pg_engine) as db:
        with pytest.raises(WorkspaceError) as caught:
            await service.read(db, user_id=uuid4(), path=path)
    assert caught.value.code is ErrorCode.WORKSPACE_BLOCKED_PATH
    assert storage.mock_calls == []


async def test_rest_can_browse_and_download_same_library_for_two_users(
    workspace_api, register_user_fn, login_fn
) -> None:
    client, storage, _ = workspace_api
    baseline_keys = set(storage.objects)
    listed = await client.get(
        f"/api/workspace/list/{quote('/builtin/skills', safe='')}",
        params={"openoctopus_device": "server"},
    )
    assert listed.status_code == 200
    assert len(listed.json()["items"]) == 6
    assert any(item["path"] == "/builtin/skills/create-skill" for item in listed.json()["items"])
    url = f"/api/workspace/files/{quote(_PATH, safe='')}"
    first = await client.get(url, params={"openoctopus_device": "server"})
    assert first.status_code == 200
    assert b"# Create a personal skill" in first.content
    assert set(storage.objects) == baseline_keys
    await register_user_fn(email="second@test.com")
    await login_fn("second@test.com")
    second = await client.get(url, params={"openoctopus_device": "server"})
    assert second.status_code == 200
    assert second.content == first.content
    assert second.headers["etag"] == first.headers["etag"]
    assert not any("builtin" in key or "/skills/" in key for key in storage.objects)


@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE", "PATCH_BATCH", "DELETE_FOLDER"])
async def test_rest_builtin_mutations_are_rejected(workspace_api, method: str) -> None:
    client, storage, _ = workspace_api
    before = dict(storage.objects)
    kwargs: dict[str, Any] = {"params": {"openoctopus_device": "server"}}
    route = f"/api/workspace/files/{quote(_PATH, safe='')}"
    if method == "PUT":
        kwargs["content"] = b"changed"
        kwargs["headers"] = {"Content-Type": "application/octet-stream"}
    elif method == "PATCH":
        kwargs["json"] = {"old_text": "Create", "new_text": "Change"}
    elif method == "PATCH_BATCH":
        method = "POST"
        route = "/api/workspace/patch"
        kwargs["json"] = {"edits": [{"path": _PATH, "action": "add", "new_text": "extra"}]}
    elif method == "DELETE_FOLDER":
        method = "DELETE"
        route = f"/api/workspace/folders/{quote('/builtin/skills', safe='')}"
    response = await client.request(method, route, **kwargs)
    assert response.status_code == 400
    assert response.json()["code"] == "workspace_blocked_path"
    assert storage.objects == before


@pytest.mark.parametrize("mode", ["copy", "move"])
@pytest.mark.parametrize("builtin_source", [True, False])
async def test_rest_transfer_rejects_both_builtin_endpoints_without_effects(
    workspace_api, mode: str, builtin_source: bool
) -> None:
    client, storage, user_id = workspace_api
    storage.seed(user_id, "source.txt", b"source")
    before = dict(storage.objects)
    response = await client.post(
        "/api/workspace/transfer",
        json={
            "openoctopus_src_device": "server",
            "src_path": _PATH if builtin_source else "source.txt",
            "openoctopus_dst_device": "server",
            "dst_path": "copied.md" if builtin_source else _PATH,
            "mode": mode,
        },
    )
    assert response.status_code == 400
    assert response.json()["code"] == "workspace_blocked_path"
    assert storage.objects == before


async def test_harness_discovers_builtins_outside_personal_limit_and_preserves_collision(pg_engine):
    from types import SimpleNamespace

    from openctopus_server.chat.scope import active_run
    from openctopus_server.chat.skills import personal_skill_id, skill_sources
    personal_files = {
        f"skills/a{i:03}/SKILL.md": f"---\nname: a{i:03}\ndescription: Personal guide\n---\nprivate body".encode()
        for i in range(201)
    }
    runtime = SimpleNamespace(workspace_service=_PromptWorkspace(personal_files), skills_cache=SkillsCache())
    token = active_run.set(SimpleNamespace(user_id=uuid4(), runtime=runtime))
    try:
        files = await skill_sources()
        for skill in get_builtin_skill_library().skills:
            assert f"/skills/builtin-{skill.name}/SKILL.md" in files
        assert f'/skills/{personal_skill_id("a199")}/SKILL.md' in files
        assert f'/skills/{personal_skill_id("a200")}/SKILL.md' not in files
        names = ('create-skill', 'builtin-create-skill', 'Review Guide_中文', 'a' * 64)
        runtime.workspace_service = _PromptWorkspace({f'skills/{name}/SKILL.md':
            f'---\nname: {name}\ndescription: Personal creation guide\n---\nprivate body'.encode() for name in names})
        runtime.skills_cache = SkillsCache()
        collision = await skill_sources()
        assert '/skills/builtin-create-skill/SKILL.md' in collision
        for name in names:
            assert f'/skills/{personal_skill_id(name)}/SKILL.md' in collision
        assert b'private body' not in collision['/skills/builtin-create-skill/SKILL.md']
    finally:
        active_run.reset(token)


@pytest.mark.parametrize("failure", ["missing", "name", "always_on", "body"])
def test_startup_validation_rejects_broken_packaged_library(tmp_path: Path, failure: str) -> None:
    for skill in get_builtin_skill_library().skills:
        path = tmp_path / skill.name / "SKILL.md"
        path.parent.mkdir()
        path.write_bytes(
            get_builtin_skill_library().read(
                f"skills/{skill.name}/SKILL.md", max_bytes=8 * 1024 * 1024
            )
        )
    path = tmp_path / "create-skill" / "SKILL.md"
    if failure == "missing":
        path.unlink()
    elif failure == "name":
        path.write_text("---\nname: wrong\ndescription: Test\n---\nBody")
    elif failure == "always_on":
        path.write_text("---\nname: create-skill\ndescription: Test\nalways_on: true\n---\nBody")
    else:
        path.write_text("---\nname: create-skill\ndescription: Test\n---\n")
    with pytest.raises((ValueError, WorkspaceError)):
        BuiltinSkillLibrary(tmp_path)
