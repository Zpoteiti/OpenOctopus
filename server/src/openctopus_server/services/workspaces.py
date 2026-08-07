from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.advisory import lock_shared_quota_read, lock_workspace_refs
from openctopus_server.db.models import SystemConfig, User, Workspace, WorkspaceMember
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.identifiers import validate_display_identifier_name
from openctopus_server.services.workspace_deletions import (
    WorkspaceLifecycle,
    reactivate_if_deletion_rolled_back,
    retire_workspace_targets,
    try_finalize_workspace_deletions,
)
from openctopus_server.workspace.fs import WorkspaceTarget

_SHARED_QUOTA_DEFAULT = 500 * 1024 * 1024


@dataclass(frozen=True)
class MemberSnapshot:
    user_id: UUID
    email: str
    name: str


@dataclass(frozen=True)
class WorkspaceSnapshot:
    id: UUID
    name: str
    kind: str
    quota_bytes: int
    target: WorkspaceTarget
    suffix: str | None = None
    created_by: UUID | None = None
    members: tuple[MemberSnapshot, ...] | None = None

    @property
    def ref(self) -> str | None:
        if self.suffix is None:
            return None
        return f"{self.name}@{self.suffix}"


async def list_authorized(
    db: AsyncSession,
    *,
    user_id: UUID,
    limit: int,
    offset: int,
) -> tuple[list[WorkspaceSnapshot], int | None]:
    user = await db.scalar(select(User).where(User.id == user_id))
    if user is None:
        raise _not_found()
    quota_bytes = await _config_int(db, "quota_bytes", _SHARED_QUOTA_DEFAULT)
    personal = WorkspaceSnapshot(
        id=user.id,
        name=user.name,
        kind="personal",
        quota_bytes=quota_bytes,
        target=WorkspaceTarget.personal(user.id),
    )

    shared_offset = max(0, offset - 1)
    shared_limit = limit + 1 if offset > 0 else limit
    rows = list(
        (
            await db.scalars(
                select(Workspace)
                .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
                .where(WorkspaceMember.user_id == user_id)
                .order_by(Workspace.created_at, Workspace.id)
                .offset(shared_offset)
                .limit(shared_limit + 1)
            )
        ).all()
    )
    items = ([] if offset > 0 else [personal]) + [_snapshot(workspace, None) for workspace in rows]
    has_more = len(items) > limit
    items = items[:limit]
    return items, offset + limit if has_more else None


async def create_shared(
    db: AsyncSession,
    *,
    user_id: UUID,
    name: str,
    quota_bytes: int,
) -> WorkspaceSnapshot:
    normalized_name = _validate_name(name)
    await lock_shared_quota_read(db)
    await _validate_shared_quota(db, quota_bytes)
    creator = await _locked_user(db, user_id=user_id)
    if creator is None:
        raise _not_found()
    await lock_workspace_refs(db, [user_id])

    workspace_id = uuid4()
    suffix = await _available_suffix(db, workspace_id=workspace_id, user_ids=[user_id])
    workspace = Workspace(
        id=workspace_id,
        name=normalized_name,
        suffix=suffix,
        quota_bytes=quota_bytes,
        created_by=user_id,
    )
    db.add(workspace)
    db.add(WorkspaceMember(workspace_id=workspace_id, user_id=user_id))
    await db.flush()
    snapshot = _snapshot(
        workspace,
        (MemberSnapshot(user_id=creator.id, email=creator.email, name=creator.name),),
    )
    await db.commit()
    return snapshot


async def get_authorized(
    db: AsyncSession,
    *,
    user_id: UUID,
    workspace_ref: str,
    for_update: bool = False,
    include_members: bool = True,
) -> WorkspaceSnapshot:
    name, suffix = _parse_ref(workspace_ref)
    statement = (
        select(Workspace)
        .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
        .where(
            WorkspaceMember.user_id == user_id,
            Workspace.name == name,
            Workspace.suffix == suffix,
        )
    )
    if for_update:
        statement = statement.with_for_update(of=Workspace)
    workspace = (await db.execute(statement)).scalar_one_or_none()
    if workspace is None:
        raise _not_found()
    members = None
    if include_members:
        by_workspace = await _members_by_workspace(db, [workspace.id])
        members = by_workspace.get(workspace.id, ())
    return _snapshot(workspace, members)


async def patch_shared(
    db: AsyncSession,
    *,
    user_id: UUID,
    workspace_ref: str,
    name: str | None,
    quota_bytes: int | None,
) -> WorkspaceSnapshot:
    if name is None and quota_bytes is None:
        raise _invalid("Workspace patch must change name or quota_bytes")
    normalized_name = _validate_name(name) if name is not None else None
    if quota_bytes is not None:
        await lock_shared_quota_read(db)
        await _validate_shared_quota(db, quota_bytes)
    snapshot = await get_authorized(
        db,
        user_id=user_id,
        workspace_ref=workspace_ref,
        for_update=True,
    )
    workspace = await db.get(Workspace, snapshot.id)
    assert workspace is not None
    if normalized_name is not None:
        workspace.name = normalized_name
    if quota_bytes is not None:
        workspace.quota_bytes = quota_bytes
    await db.flush()
    assert snapshot.members is not None
    updated = _snapshot(workspace, snapshot.members)
    await db.commit()
    return updated


async def list_members(
    db: AsyncSession,
    *,
    user_id: UUID,
    workspace_ref: str,
    limit: int,
    offset: int,
) -> tuple[list[MemberSnapshot], int | None]:
    snapshot = await get_authorized(
        db,
        user_id=user_id,
        workspace_ref=workspace_ref,
        include_members=False,
    )
    rows = (
        await db.execute(
            select(User.id, User.email, User.name)
            .join(WorkspaceMember, WorkspaceMember.user_id == User.id)
            .where(WorkspaceMember.workspace_id == snapshot.id)
            .order_by(WorkspaceMember.joined_at, User.id)
            .offset(offset)
            .limit(limit + 1)
        )
    ).all()
    members = [MemberSnapshot(user_id=row.id, email=row.email, name=row.name) for row in rows]
    has_more = len(members) > limit
    return members[:limit], offset + limit if has_more else None


async def add_member(
    db: AsyncSession,
    *,
    user_id: UUID,
    workspace_ref: str,
    member_user_id: UUID | None,
    email: str | None,
) -> MemberSnapshot:
    if (member_user_id is None) == (email is None):
        raise _invalid("Exactly one of user_id or email is required")
    member = await _locked_user(db, user_id=member_user_id, email=email)
    snapshot = await get_authorized(
        db,
        user_id=user_id,
        workspace_ref=workspace_ref,
        for_update=True,
        include_members=False,
    )
    if member is None:
        raise WorkspaceError(ErrorCode.USER_NOT_FOUND, "User not found")

    await lock_workspace_refs(db, [member.id])
    workspace = await db.get(Workspace, snapshot.id)
    assert workspace is not None
    existing = await db.get(WorkspaceMember, (workspace.id, member.id))
    if existing is not None:
        await db.commit()
        return MemberSnapshot(user_id=member.id, email=member.email, name=member.name)
    collision = await db.scalar(
        select(Workspace.id)
        .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
        .where(
            WorkspaceMember.user_id == member.id,
            Workspace.id != workspace.id,
            Workspace.suffix == workspace.suffix,
        )
        .limit(1)
    )
    if collision is not None:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_REF_CONFLICT,
            "Adding this member would create a workspace reference collision",
        )
    db.add(WorkspaceMember(workspace_id=workspace.id, user_id=member.id))
    await db.commit()
    return MemberSnapshot(user_id=member.id, email=member.email, name=member.name)


async def remove_authorized_member(
    db: AsyncSession,
    *,
    user_id: UUID,
    workspace_ref: str,
    member_user_id: UUID,
    workspace_fs: WorkspaceLifecycle,
) -> None:
    snapshot = await get_authorized(
        db,
        user_id=user_id,
        workspace_ref=workspace_ref,
        for_update=True,
        include_members=False,
    )
    await remove_member(
        db,
        workspace_id=snapshot.id,
        user_id=member_user_id,
        workspace_fs=workspace_fs,
    )


async def _members_by_workspace(
    db: AsyncSession,
    workspace_ids: list[UUID],
) -> dict[UUID, tuple[MemberSnapshot, ...]]:
    if not workspace_ids:
        return {}
    rows = (
        await db.execute(
            select(WorkspaceMember.workspace_id, User.id, User.email, User.name)
            .join(User, User.id == WorkspaceMember.user_id)
            .where(WorkspaceMember.workspace_id.in_(workspace_ids))
            .order_by(WorkspaceMember.joined_at, User.id)
        )
    ).all()
    result: dict[UUID, list[MemberSnapshot]] = {}
    for workspace_id, user_id, email, name in rows:
        result.setdefault(workspace_id, []).append(
            MemberSnapshot(user_id=user_id, email=email, name=name)
        )
    return {workspace_id: tuple(items) for workspace_id, items in result.items()}


def _snapshot(
    workspace: Workspace,
    members: tuple[MemberSnapshot, ...] | None,
) -> WorkspaceSnapshot:
    return WorkspaceSnapshot(
        id=workspace.id,
        name=workspace.name,
        kind="shared",
        quota_bytes=workspace.quota_bytes,
        target=WorkspaceTarget.shared(workspace.id),
        suffix=workspace.suffix,
        created_by=workspace.created_by,
        members=members,
    )


async def _available_suffix(
    db: AsyncSession,
    *,
    workspace_id: UUID,
    user_ids: list[UUID],
) -> str:
    statement = (
        select(Workspace.suffix)
        .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
        .where(WorkspaceMember.user_id.in_(set(user_ids)))
    )
    suffixes = set((await db.scalars(statement)).all())
    full = workspace_id.hex
    for length in range(8, len(full) + 1):
        candidate = full[:length]
        if candidate not in suffixes:
            return candidate
    raise RuntimeError("UUID suffix collision")


async def _validate_shared_quota(db: AsyncSession, quota_bytes: int) -> None:
    ceiling = await _config_int(
        db,
        "shared_workspace_quota_bytes",
        _SHARED_QUOTA_DEFAULT,
    )
    if quota_bytes > ceiling:
        raise _invalid("Shared workspace quota exceeds the configured ceiling")


async def _config_int(db: AsyncSession, key: str, default: int) -> int:
    value = await db.scalar(select(SystemConfig.value).where(SystemConfig.key == key))
    return default if value is None else int(value)


async def _locked_user(
    db: AsyncSession,
    *,
    user_id: UUID | None = None,
    email: str | None = None,
) -> User | None:
    statement = select(User).with_for_update(read=True)
    if user_id is not None:
        statement = statement.where(User.id == user_id)
    else:
        statement = statement.where(User.email == email)
    return (await db.execute(statement)).scalar_one_or_none()


def _parse_ref(workspace_ref: str) -> tuple[str, str]:
    name, separator, suffix = workspace_ref.rpartition("@")
    if (
        not separator
        or not name
        or len(suffix) < 8
        or any(ch not in "0123456789abcdef" for ch in suffix)
    ):
        raise _not_found()
    return name, suffix


def _validate_name(name: str) -> str:
    try:
        return validate_display_identifier_name(name)
    except ValueError as exc:
        raise _invalid("Workspace name is invalid") from exc


def _invalid(message: str) -> WorkspaceError:
    return WorkspaceError(ErrorCode.WORKSPACE_INVALID_REQUEST, message)


def _not_found() -> WorkspaceError:
    return WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "Workspace not found")


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
