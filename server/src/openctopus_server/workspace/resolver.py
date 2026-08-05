from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.advisory import lock_personal_quota_read
from openctopus_server.db.models import SystemConfig, User, Workspace, WorkspaceMember
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import WorkspaceTarget

_PERSONAL_QUOTA_DEFAULT = 500 * 1024 * 1024


@dataclass(frozen=True)
class ResolvedWorkspacePath:
    target: WorkspaceTarget
    relative_path: str
    quota_bytes: int


class WorkspacePathResolver:
    """Resolve access while retaining a PostgreSQL share lock in ``db``."""

    async def resolve(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> ResolvedWorkspacePath:
        if not path.startswith("/"):
            await _lock_personal_workspace(db, user_id)
            return ResolvedWorkspacePath(
                target=WorkspaceTarget.personal(user_id),
                relative_path=path,
                quota_bytes=await _personal_quota(db),
            )

        workspace_ref, separator, relative_path = path[1:].partition("/")
        if not separator:
            relative_path = ""

        if workspace_ref == str(user_id):
            await _lock_personal_workspace(db, user_id)
            return ResolvedWorkspacePath(
                target=WorkspaceTarget.personal(user_id),
                relative_path=relative_path,
                quota_bytes=await _personal_quota(db),
            )

        name, marker, suffix = workspace_ref.rpartition("@")
        if not marker or not name or not suffix:
            raise _not_found()

        result = await db.execute(
            select(Workspace)
            .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
            .where(
                WorkspaceMember.user_id == user_id,
                Workspace.name == name,
                Workspace.suffix == suffix,
            )
            .with_for_update(read=True, of=Workspace)
        )
        workspace = result.scalar_one_or_none()
        if workspace is None:
            raise _not_found()
        return ResolvedWorkspacePath(
            target=WorkspaceTarget.shared(workspace.id),
            relative_path=relative_path,
            quota_bytes=workspace.quota_bytes,
        )


async def _personal_quota(db: AsyncSession) -> int:
    await lock_personal_quota_read(db)
    result = await db.execute(select(SystemConfig.value).where(SystemConfig.key == "quota_bytes"))
    value = result.scalar_one_or_none()
    return _PERSONAL_QUOTA_DEFAULT if value is None else int(value)


async def _lock_personal_workspace(db: AsyncSession, user_id: UUID) -> None:
    locked_user_id = await db.scalar(
        select(User.id).where(User.id == user_id).with_for_update(read=True)
    )
    if locked_user_id is None:
        raise _not_found()


def _not_found() -> WorkspaceError:
    return WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "Workspace was not found")
