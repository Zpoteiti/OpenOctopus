import asyncio
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User, Workspace, WorkspaceMember
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.services import workspaces


async def _user(db: AsyncSession, email: str) -> User:
    user = User(email=email, password_hash="x", name=email)
    db.add(user)
    await db.flush()
    return user


async def test_concurrent_member_additions_keep_suffixes_unique_for_new_member(
    pg_engine: AsyncEngine,
) -> None:
    first_id = UUID("12345678-a000-4000-8000-000000000001")
    second_id = UUID("12345678-b000-4000-8000-000000000002")
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        first_owner = await _user(db, "first-owner@test.com")
        second_owner = await _user(db, "second-owner@test.com")
        joining = await _user(db, "joining@test.com")
        db.add_all(
            [
                Workspace(
                    id=first_id,
                    name="First",
                    suffix="12345678",
                    quota_bytes=1_000_000,
                    created_by=first_owner.id,
                ),
                Workspace(
                    id=second_id,
                    name="Second",
                    suffix="12345678",
                    quota_bytes=1_000_000,
                    created_by=second_owner.id,
                ),
                WorkspaceMember(workspace_id=first_id, user_id=first_owner.id),
                WorkspaceMember(workspace_id=second_id, user_id=second_owner.id),
            ]
        )
        await db.commit()
        first_owner_id = first_owner.id
        second_owner_id = second_owner.id
        joining_id = joining.id

    async def add(owner_id: UUID, workspace_ref: str) -> object:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            try:
                return await workspaces.add_member(
                    db,
                    user_id=owner_id,
                    workspace_ref=workspace_ref,
                    member_user_id=joining_id,
                    email=None,
                )
            except WorkspaceError as exc:
                return exc

    results = await asyncio.gather(
        add(first_owner_id, "First@12345678"),
        add(second_owner_id, "Second@12345678"),
    )

    async with AsyncSession(pg_engine) as db:
        rows = list(
            (
                await db.scalars(
                    select(Workspace)
                    .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
                    .where(WorkspaceMember.user_id == joining_id)
                    .order_by(Workspace.id)
                )
            ).all()
        )
    assert len(rows) == 1
    assert sum(isinstance(result, WorkspaceError) for result in results) == 1
    conflict = next(result for result in results if isinstance(result, WorkspaceError))
    assert conflict.code is ErrorCode.WORKSPACE_REF_CONFLICT
    assert rows[0].suffix == "12345678"
