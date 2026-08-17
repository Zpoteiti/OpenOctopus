import asyncio
import hmac
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.auth.password import hash_password
from openctopus_server.config import get_settings
from openctopus_server.db.models import Device, User, Workspace, WorkspaceMember
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError
from openctopus_server.services.workspace_deletions import (
    WorkspaceLifecycle,
    reactivate_if_deletion_rolled_back,
    retire_workspace_targets,
    try_finalize_workspace_deletions,
)
from openctopus_server.workspace.fs import WorkspaceTarget


async def create_user(
    db: AsyncSession,
    email: str,
    password: str,
    name: str,
    *,
    admin_token: str | None = None,
) -> User:
    existing = await db.execute(select(User).where(User.email == email))
    if existing.scalar_one_or_none() is not None:
        raise AuthError(ErrorCode.AUTH_EMAIL_TAKEN, "Email already in use")

    is_admin = False
    if admin_token is not None:
        settings = get_settings()
        is_admin = hmac.compare_digest(admin_token, settings.admin_token)

    user = User(
        email=email,
        password_hash=hash_password(password),
        name=name,
        is_admin=is_admin,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def get_user_by_id(db: AsyncSession, user_id: UUID) -> User | None:
    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    result = await db.execute(select(User).where(User.email == email))
    return result.scalar_one_or_none()


async def list_users(db: AsyncSession, limit: int, offset: int) -> list[User]:
    result = await db.execute(
        select(User).order_by(User.created_at, User.id).limit(limit).offset(offset)
    )
    return list(result.scalars().all())


async def update_user(
    db: AsyncSession,
    user: User,
    *,
    name: str | None = None,
    email: str | None = None,
    password: str | None = None,
) -> User:
    if email is not None and email != user.email:
        existing = await db.execute(select(User).where(User.email == email))
        if existing.scalar_one_or_none() is not None:
            raise AuthError(ErrorCode.AUTH_EMAIL_TAKEN, "Email already in use")
        user.email = email
    if name is not None:
        user.name = name
    if password is not None:
        user.password_hash = hash_password(password)
    await db.commit()
    await db.refresh(user)
    return user


async def delete_user(
    db: AsyncSession,
    user: User,
    *,
    workspace_fs: WorkspaceLifecycle,
    device_registry: DeviceRegistry,
) -> None:
    if user.is_admin:
        await db.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended('openoctopus:admin_deletion', 0))")
        )
        await assert_not_last_admin(db, user)

    locked_user = await db.scalar(select(User).where(User.id == user.id).with_for_update())
    if locked_user is None:
        return
    user = locked_user

    device_ids = tuple(
        (
            await db.scalars(
                select(Device.id)
                .where(Device.user_id == user.id)
                .order_by(Device.id)
                .with_for_update()
            )
        ).all()
    )

    workspace_ids = list(
        (
            await db.scalars(
                select(WorkspaceMember.workspace_id).where(WorkspaceMember.user_id == user.id)
            )
        ).all()
    )
    if workspace_ids:
        await db.scalars(
            select(Workspace)
            .where(Workspace.id.in_(workspace_ids))
            .order_by(Workspace.id)
            .with_for_update()
        )
    orphaned_workspace_ids: list[UUID] = []
    for workspace_id in workspace_ids:
        other_members = await db.scalar(
            select(func.count())
            .select_from(WorkspaceMember)
            .where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.user_id != user.id,
            )
        )
        if other_members == 0:
            orphaned_workspace_ids.append(workspace_id)

    targets = [
        WorkspaceTarget.personal(user.id),
        *(WorkspaceTarget.shared(workspace_id) for workspace_id in orphaned_workspace_ids),
    ]
    retired_targets: list[WorkspaceTarget] = []
    try:
        retired_targets = await retire_workspace_targets(db, targets, workspace_fs)
        for workspace_id in orphaned_workspace_ids:
            workspace = await db.get(Workspace, workspace_id)
            if workspace is not None:
                await db.delete(workspace)
        await db.delete(user)
    except BaseException:
        for target in retired_targets:
            await workspace_fs.reactivate_workspace(target)
        await db.rollback()
        raise

    async def commit_release_and_invalidate_devices() -> None:
        try:
            await db.commit()
        except BaseException:
            await db.rollback()
            await reactivate_if_deletion_rolled_back(db, retired_targets, workspace_fs)
            raise
        try:
            await db.close()
        finally:
            await device_registry.remove_devices(device_ids)

    invalidation = asyncio.create_task(commit_release_and_invalidate_devices())
    await await_future_cancellation_safe(invalidation)
    await try_finalize_workspace_deletions(db, retired_targets, workspace_fs)


async def count_admins(db: AsyncSession) -> int:
    result = await db.execute(select(func.count()).select_from(User).where(User.is_admin.is_(True)))
    return result.scalar_one()


async def assert_not_last_admin(db: AsyncSession, user: User) -> None:
    if user.is_admin and await count_admins(db) == 1:
        raise AuthError(
            ErrorCode.AUTH_LAST_ADMIN_REQUIRED,
            "Cannot delete the last remaining admin",
        )
