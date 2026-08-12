"""PostgreSQL-backed contracts for workspace byte cleanup on ownership changes."""

from __future__ import annotations

import asyncio
import importlib
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User, Workspace, WorkspaceMember
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError
from openctopus_server.services import users
from openctopus_server.workspace.fs import WorkspaceTarget


class _RecordingWorkspaceFS:
    def __init__(self) -> None:
        self.purged_prefixes: list[str] = []

    async def purge_workspace(self, target: WorkspaceTarget) -> None:
        collection = "users" if target.kind == "personal" else "workspaces"
        self.purged_prefixes.append(f"{collection}/{target.id}/")

    async def retire_workspace(self, target: WorkspaceTarget) -> None:
        pass

    async def reactivate_workspace(self, target: WorkspaceTarget) -> None:
        pass

    async def forget_workspace(self, target: WorkspaceTarget) -> None:
        pass


async def _add_user(db: AsyncSession, *, email: str, name: str) -> User:
    user = User(email=email, password_hash="not-used", name=name)
    db.add(user)
    await db.flush()
    return user


def _shared_workspace(
    *,
    workspace_id: UUID,
    creator_id: UUID,
    name: str = "Shared",
) -> Workspace:
    return Workspace(
        id=workspace_id,
        name=name,
        suffix=workspace_id.hex[:8],
        quota_bytes=1024,
        created_by=creator_id,
    )


def _workspace_lifecycle_service() -> Any:
    try:
        return importlib.import_module("openctopus_server.services.workspaces")
    except ModuleNotFoundError:
        raise AssertionError(
            "missing openctopus_server.services.workspaces.remove_member"
        ) from None


async def test_account_deletion_purges_personal_workspace_prefix(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _RecordingWorkspaceFS()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _add_user(db, email="delete@test.com", name="Delete")
        await db.commit()
        user_id = user.id

        await users.delete_user(
            db, user, workspace_fs=workspace_fs, device_registry=DeviceRegistry()
        )

        assert await db.get(User, user_id) is None

    assert workspace_fs.purged_prefixes == [f"users/{user_id}/"]


async def test_removing_last_shared_member_purges_prefix_and_workspace_row(
    pg_engine: AsyncEngine,
) -> None:
    lifecycle = _workspace_lifecycle_service()
    workspace_fs = _RecordingWorkspaceFS()
    workspace_id = UUID("10000000-0000-4000-8000-000000000001")

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        member = await _add_user(db, email="last@test.com", name="Last")
        workspace = _shared_workspace(
            workspace_id=workspace_id,
            creator_id=member.id,
        )
        db.add(workspace)
        db.add(WorkspaceMember(workspace_id=workspace_id, user_id=member.id))
        await db.commit()

        await lifecycle.remove_member(
            db,
            workspace_id=workspace_id,
            user_id=member.id,
            workspace_fs=workspace_fs,
        )

        assert await db.get(Workspace, workspace_id) is None
        member_count = await db.scalar(
            select(func.count())
            .select_from(WorkspaceMember)
            .where(WorkspaceMember.workspace_id == workspace_id)
        )

    assert member_count == 0
    assert workspace_fs.purged_prefixes == [f"workspaces/{workspace_id}/"]


async def test_deleting_creator_preserves_shared_workspace_for_remaining_member(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _RecordingWorkspaceFS()
    workspace_id = UUID("20000000-0000-4000-8000-000000000001")

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        creator = await _add_user(db, email="creator@test.com", name="Creator")
        remaining = await _add_user(db, email="remaining@test.com", name="Remaining")
        workspace = _shared_workspace(
            workspace_id=workspace_id,
            creator_id=creator.id,
        )
        db.add(workspace)
        db.add_all(
            (
                WorkspaceMember(workspace_id=workspace_id, user_id=creator.id),
                WorkspaceMember(workspace_id=workspace_id, user_id=remaining.id),
            )
        )
        await db.commit()
        creator_id = creator.id

        await users.delete_user(
            db, creator, workspace_fs=workspace_fs, device_registry=DeviceRegistry()
        )

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        persisted = await db.get(Workspace, workspace_id)
        remaining_members = list(
            (
                await db.scalars(
                    select(WorkspaceMember).where(WorkspaceMember.workspace_id == workspace_id)
                )
            ).all()
        )

    assert persisted is not None
    assert persisted.created_by is None
    assert [member.user_id for member in remaining_members] == [remaining.id]
    assert workspace_fs.purged_prefixes == [f"users/{creator_id}/"]


async def test_deleting_last_member_account_purges_orphaned_shared_workspace(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _RecordingWorkspaceFS()
    workspace_id = UUID("30000000-0000-4000-8000-000000000001")

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        creator = await _add_user(db, email="orphan@test.com", name="Orphan")
        db.add(
            _shared_workspace(
                workspace_id=workspace_id,
                creator_id=creator.id,
            )
        )
        db.add(WorkspaceMember(workspace_id=workspace_id, user_id=creator.id))
        await db.commit()
        creator_id = creator.id

        await users.delete_user(
            db, creator, workspace_fs=workspace_fs, device_registry=DeviceRegistry()
        )

        assert await db.get(Workspace, workspace_id) is None

    assert workspace_fs.purged_prefixes == [
        f"users/{creator_id}/",
        f"workspaces/{workspace_id}/",
    ]


async def test_concurrent_member_removals_cannot_leave_an_orphaned_workspace(
    pg_engine: AsyncEngine,
) -> None:
    lifecycle = _workspace_lifecycle_service()
    workspace_fs = _RecordingWorkspaceFS()
    workspace_id = UUID("40000000-0000-4000-8000-000000000001")

    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        first = await _add_user(setup_db, email="first@test.com", name="First")
        second = await _add_user(setup_db, email="second@test.com", name="Second")
        setup_db.add(
            _shared_workspace(
                workspace_id=workspace_id,
                creator_id=first.id,
            )
        )
        setup_db.add_all(
            (
                WorkspaceMember(workspace_id=workspace_id, user_id=first.id),
                WorkspaceMember(workspace_id=workspace_id, user_id=second.id),
            )
        )
        await setup_db.commit()

    async def remove(user_id: UUID) -> None:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            await lifecycle.remove_member(
                db,
                workspace_id=workspace_id,
                user_id=user_id,
                workspace_fs=workspace_fs,
            )

    blocker = AsyncSession(pg_engine)
    await blocker.execute(select(Workspace).where(Workspace.id == workspace_id).with_for_update())
    removals = [asyncio.create_task(remove(user_id)) for user_id in (first.id, second.id)]
    try:
        await asyncio.sleep(0.05)
        both_waited_for_workspace_lock = not any(task.done() for task in removals)
    finally:
        await blocker.rollback()
        await blocker.close()
        await asyncio.gather(*removals)

    assert both_waited_for_workspace_lock
    async with AsyncSession(pg_engine) as verify_db:
        assert await verify_db.get(Workspace, workspace_id) is None
    assert workspace_fs.purged_prefixes == [f"workspaces/{workspace_id}/"]


async def test_concurrent_admin_deletions_preserve_one_admin(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _RecordingWorkspaceFS()
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        first = await _add_user(setup_db, email="admin1@test.com", name="Admin One")
        second = await _add_user(setup_db, email="admin2@test.com", name="Admin Two")
        first.is_admin = True
        second.is_admin = True
        await setup_db.commit()

    async def remove(user_id: UUID) -> None:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            user = await db.get(User, user_id)
            assert user is not None
            await users.delete_user(
                db, user, workspace_fs=workspace_fs, device_registry=DeviceRegistry()
            )

    results = await asyncio.gather(
        remove(first.id),
        remove(second.id),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    failures = [result for result in results if isinstance(result, AuthError)]
    assert len(failures) == 1
    assert failures[0].code is ErrorCode.AUTH_LAST_ADMIN_REQUIRED
    async with AsyncSession(pg_engine) as verify_db:
        assert (
            await verify_db.scalar(
                select(func.count()).select_from(User).where(User.is_admin.is_(True))
            )
            == 1
        )
