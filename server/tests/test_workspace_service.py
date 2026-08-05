import asyncio
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User, Workspace, WorkspaceMember
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError, WorkspaceError
from openctopus_server.services import users
from openctopus_server.workspace.fs import DirectoryPage, FileMetadata, WorkspaceFS, WorkspaceTarget
from openctopus_server.workspace.search import GrepContentMatch, SearchObject
from openctopus_server.workspace.service import PatchEdit, WorkspaceService


@asynccontextmanager
async def _slot():
    yield


def _workspace_fs_mock() -> AsyncMock:
    workspace_fs = AsyncMock(spec=WorkspaceFS)
    workspace_fs.materialization_slot = Mock(side_effect=_slot)
    workspace_fs.file_operation_slot = Mock(side_effect=_slot)
    return workspace_fs


async def test_service_rejects_inaccessible_shared_path_before_file_call(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _workspace_fs_mock()
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = User(email="owner-service@test.com", password_hash="x", name="Owner")
        outsider = User(
            email="outsider-service@test.com",
            password_hash="x",
            name="Outsider",
        )
        db.add_all((owner, outsider))
        await db.flush()
        workspace = Workspace(
            name="Private",
            suffix="1234abcd",
            quota_bytes=1234,
            created_by=owner.id,
        )
        db.add(workspace)
        await db.flush()
        db.add(WorkspaceMember(workspace_id=workspace.id, user_id=owner.id))
        await db.commit()

        with pytest.raises(WorkspaceError) as caught:
            await service.write(
                db,
                user_id=outsider.id,
                path="/Private@1234abcd/secret.txt",
                data=b"blocked",
            )

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    workspace_fs.write.assert_not_awaited()
    workspace_fs.write_collected_upload.assert_not_awaited()


async def test_service_supplies_resolved_target_and_database_quota(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _workspace_fs_mock()
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        member = User(email="member-service@test.com", password_hash="x", name="Member")
        db.add(member)
        await db.flush()
        workspace = Workspace(
            name="Shared",
            suffix="abcd1234",
            quota_bytes=4321,
            created_by=member.id,
        )
        db.add(workspace)
        await db.flush()
        db.add(WorkspaceMember(workspace_id=workspace.id, user_id=member.id))
        await db.commit()

        await service.write(
            db,
            user_id=member.id,
            path="/Shared@abcd1234/file.txt",
            data=b"contents",
        )

    workspace_fs.write_collected_upload.assert_awaited_once_with(
        WorkspaceTarget.shared(workspace.id),
        "file.txt",
        b"contents",
        quota_bytes=4321,
        if_match=None,
        if_none_match=False,
    )


async def test_authorized_operation_holds_account_deletion_until_it_finishes(
    pg_engine: AsyncEngine,
) -> None:
    entered_file_call = asyncio.Event()
    release_file_call = asyncio.Event()

    async def blocking_write(*args: object, **kwargs: object) -> None:
        del args, kwargs
        entered_file_call.set()
        await release_file_call.wait()

    workspace_fs = _workspace_fs_mock()
    workspace_fs.write_collected_upload.side_effect = blocking_write
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        user = User(email="locked-service@test.com", password_hash="x", name="Locked")
        setup_db.add(user)
        await setup_db.commit()
        user_id = user.id

    operation_db = AsyncSession(pg_engine, expire_on_commit=False)
    writing = asyncio.create_task(
        service.write(
            operation_db,
            user_id=user_id,
            path="file.txt",
            data=b"contents",
        )
    )
    await entered_file_call.wait()

    async def delete_account() -> None:
        async with AsyncSession(pg_engine, expire_on_commit=False) as deletion_db:
            target = await deletion_db.get(User, user_id)
            assert target is not None
            await users.delete_user(
                deletion_db,
                target,
                workspace_fs=workspace_fs,
            )

    deleting = asyncio.create_task(delete_account())
    try:
        await asyncio.sleep(0.05)
        assert not deleting.done()
        release_file_call.set()
        await writing
        await asyncio.sleep(0.05)
        assert not deleting.done()
    finally:
        await operation_db.rollback()
        await operation_db.close()
    await deleting


async def test_text_edit_uses_shared_matcher_and_can_create_missing_file(pg_engine) -> None:
    workspace_fs = _workspace_fs_mock()

    async def edit_optional(
        target: WorkspaceTarget,
        path: str,
        transform: Any,
        **kwargs: object,
    ) -> FileMetadata:
        del target, path, kwargs
        assert transform(None) == b"created\n"
        return FileMetadata(size=8, etag="new", created=True)

    workspace_fs.edit_optional_materialized.side_effect = edit_optional
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(email="edit-create@test.com", password_hash="x", name="Editor")
        db.add(user)
        await db.commit()

        metadata, replacements = await service.edit_text(
            db,
            user_id=user.id,
            path="new.txt",
            old_text="",
            new_text="created\n",
            occurrence=None,
        )

    assert metadata.created is True
    assert replacements == 0


async def test_grep_rejects_invalid_regex_before_scanning_storage() -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    service = WorkspaceService(workspace_fs)

    with pytest.raises(ToolError) as caught:
        await service.grep(
            AsyncMock(spec=AsyncSession),
            user_id=uuid4(),
            pattern="[",
            path="",
            glob=None,
            file_type=None,
            case_insensitive=False,
            fixed_strings=False,
            output_mode="content",
            context_before=0,
            context_after=0,
            limit=10,
            offset=0,
        )

    assert caught.value.code is ErrorCode.TOOL_INVALID_REGEX
    workspace_fs.scan_objects.assert_not_awaited()


async def test_find_rejects_invalid_glob_before_scanning_storage() -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    service = WorkspaceService(workspace_fs)

    with pytest.raises(ToolError) as caught:
        await service.find_files(
            AsyncMock(spec=AsyncSession),
            user_id=uuid4(),
            path="",
            query="",
            glob="[broken",
            file_type=None,
            include_dirs=False,
            sort="path",
            limit=10,
            offset=0,
        )

    assert caught.value.code is ErrorCode.TOOL_INVALID_GLOB
    workspace_fs.scan_objects.assert_not_awaited()


async def test_tool_write_rejects_more_than_eight_mib_before_storage() -> None:
    workspace_fs = _workspace_fs_mock()
    service = WorkspaceService(workspace_fs)

    with pytest.raises(WorkspaceError) as caught:
        await service.write(
            AsyncMock(spec=AsyncSession),
            user_id=uuid4(),
            path="large.txt",
            data=b"x" * (8 * 1024 * 1024 + 1),
        )

    assert caught.value.code is ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT
    workspace_fs.write_collected_upload.assert_not_awaited()


async def test_search_reauthorizes_after_waiting_for_heavy_admission(pg_engine) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    @asynccontextmanager
    async def blocked_slot():
        entered.set()
        await release.wait()
        yield

    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=blocked_slot)
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        owner = User(email="search-owner@test.com", password_hash="x", name="Owner")
        member = User(email="search-member@test.com", password_hash="x", name="Member")
        setup_db.add_all((owner, member))
        await setup_db.flush()
        workspace = Workspace(
            name="Search",
            suffix="feed1234",
            quota_bytes=1024,
            created_by=owner.id,
        )
        setup_db.add(workspace)
        await setup_db.flush()
        setup_db.add_all(
            (
                WorkspaceMember(workspace_id=workspace.id, user_id=owner.id),
                WorkspaceMember(workspace_id=workspace.id, user_id=member.id),
            )
        )
        await setup_db.commit()
        member_id = member.id
        workspace_id = workspace.id

    operation_db = AsyncSession(pg_engine, expire_on_commit=False)
    searching = asyncio.create_task(
        service.find_files(
            operation_db,
            user_id=member_id,
            path="/Search@feed1234/",
            query="",
            glob=None,
            file_type=None,
            include_dirs=False,
            sort="path",
            limit=10,
            offset=0,
        )
    )
    await entered.wait()
    async with AsyncSession(pg_engine, expire_on_commit=False) as revoke_db:
        await revoke_db.execute(
            delete(WorkspaceMember).where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.user_id == member_id,
            )
        )
        await revoke_db.commit()
    release.set()
    try:
        with pytest.raises(WorkspaceError) as caught:
            await searching
    finally:
        await operation_db.close()

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    workspace_fs.scan_objects.assert_not_awaited()


async def test_patch_reauthorizes_after_waiting_for_materialization(pg_engine) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    @asynccontextmanager
    async def blocked_slot():
        entered.set()
        await release.wait()
        yield

    workspace_fs = _workspace_fs_mock()
    workspace_fs.materialization_slot = Mock(side_effect=blocked_slot)
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        owner = User(email="patch-owner@test.com", password_hash="x", name="Owner")
        member = User(email="patch-member@test.com", password_hash="x", name="Member")
        setup_db.add_all((owner, member))
        await setup_db.flush()
        workspace = Workspace(
            name="Patch",
            suffix="cafe1234",
            quota_bytes=1024,
            created_by=owner.id,
        )
        setup_db.add(workspace)
        await setup_db.flush()
        setup_db.add_all(
            (
                WorkspaceMember(workspace_id=workspace.id, user_id=owner.id),
                WorkspaceMember(workspace_id=workspace.id, user_id=member.id),
            )
        )
        await setup_db.commit()
        member_id = member.id
        workspace_id = workspace.id

    operation_db = AsyncSession(pg_engine, expire_on_commit=False)
    patching = asyncio.create_task(
        service.apply_patch(
            operation_db,
            user_id=member_id,
            edits=(PatchEdit("/Patch@cafe1234/a.txt", "add", None, "x"),),
            dry_run=False,
        )
    )
    await entered.wait()
    async with AsyncSession(pg_engine, expire_on_commit=False) as revoke_db:
        await revoke_db.execute(
            delete(WorkspaceMember).where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.user_id == member_id,
            )
        )
        await revoke_db.commit()
    release.set()
    try:
        with pytest.raises(WorkspaceError) as caught:
            await patching
    finally:
        await operation_db.close()

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    workspace_fs.apply_transforms_admitted.assert_not_awaited()


async def test_recursive_list_rejects_an_exact_file_path(pg_engine) -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    workspace_fs.scan_objects.return_value = (
        (SearchObject(path="file.txt", size=4),),
        False,
    )
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(email="list-file@test.com", password_hash="x", name="Reader")
        db.add(user)
        await db.commit()

        with pytest.raises(WorkspaceError) as caught:
            await service.list_recursive(
                db,
                user_id=user.id,
                path="file.txt",
                limit=10,
                offset=0,
            )

    assert caught.value.code is ErrorCode.TOOL_NOT_A_DIRECTORY


async def test_directory_page_normalizes_dot_to_workspace_root(pg_engine) -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.list_dir_page.return_value = DirectoryPage((), None, False)
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(email="list-root@test.com", password_hash="x", name="Reader")
        db.add(user)
        await db.commit()

        await service.list_dir_page(
            db,
            user_id=user.id,
            path=".",
            limit=20,
        )

    workspace_fs.list_dir_page.assert_awaited_once_with(
        WorkspaceTarget.personal(user.id),
        "",
        limit=20,
        offset=0,
        include_noise_directories=False,
    )


async def test_grep_result_cap_resumes_within_the_same_file(pg_engine) -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    content = (("needle" + "x" * 14_900 + "\n") * 100).encode()
    metadata = SearchObject(path="large.txt", size=len(content))
    workspace_fs.scan_objects.return_value = ((metadata,), False)
    workspace_fs.read_search_object.return_value = SearchObject(
        path="large.txt",
        size=len(content),
        content=content,
    )
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(email="grep-page@test.com", password_hash="x", name="Reader")
        db.add(user)
        await db.commit()

        first = await service.grep(
            db,
            user_id=user.id,
            pattern="needle",
            path=".",
            glob=None,
            file_type=None,
            case_insensitive=False,
            fixed_strings=True,
            output_mode="content",
            context_before=0,
            context_after=0,
            limit=1000,
            offset=0,
        )
        assert first.next_offset == len(first.items)
        assert first.next_offset is not None

        second = await service.grep(
            db,
            user_id=user.id,
            pattern="needle",
            path=".",
            glob=None,
            file_type=None,
            case_insensitive=False,
            fixed_strings=True,
            output_mode="content",
            context_before=0,
            context_after=0,
            limit=1000,
            offset=first.next_offset,
        )

    assert second.items
    assert isinstance(second.items[0], GrepContentMatch)
    assert second.items[0].line_number == first.next_offset + 1
