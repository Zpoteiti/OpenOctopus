from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import Workspace, WorkspaceMember
from openctopus_server.services.workspace_deletions import (
    WorkspaceLifecycle,
    reactivate_if_deletion_rolled_back,
    retire_workspace_targets,
    try_finalize_workspace_deletions,
)
from openctopus_server.workspace.fs import WorkspaceTarget


async def remove_member(
    db: AsyncSession,
    *,
    workspace_id: UUID,
    user_id: UUID,
    workspace_fs: WorkspaceLifecycle,
) -> None:
    workspace = await db.scalar(
        select(Workspace).where(Workspace.id == workspace_id).with_for_update()
    )
    if workspace is None:
        return

    result = await db.execute(
        select(WorkspaceMember).where(
            WorkspaceMember.workspace_id == workspace_id,
            WorkspaceMember.user_id == user_id,
        )
    )
    member = result.scalar_one_or_none()
    if member is None:
        return

    target = WorkspaceTarget.shared(workspace.id)
    retired_targets: list[WorkspaceTarget] = []
    try:
        await db.delete(member)
        await db.flush()
        remaining = await db.scalar(
            select(func.count())
            .select_from(WorkspaceMember)
            .where(WorkspaceMember.workspace_id == workspace_id)
        )
        if remaining == 0:
            retired_targets = await retire_workspace_targets(db, [target], workspace_fs)
            await db.delete(workspace)
    except BaseException:
        for retired_target in retired_targets:
            await workspace_fs.reactivate_workspace(retired_target)
        await db.rollback()
        raise
    try:
        await db.commit()
    except BaseException:
        await db.rollback()
        await reactivate_if_deletion_rolled_back(db, retired_targets, workspace_fs)
        raise
    await try_finalize_workspace_deletions(db, retired_targets, workspace_fs)
