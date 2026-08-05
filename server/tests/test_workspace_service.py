import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User, Workspace, WorkspaceMember
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.services import users
from openctopus_server.workspace.fs import WorkspaceFS, WorkspaceTarget
from openctopus_server.workspace.service import WorkspaceService


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
