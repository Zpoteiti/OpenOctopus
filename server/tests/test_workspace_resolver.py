"""PostgreSQL-backed contract tests for virtual workspace path resolution."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import SystemConfig, User, Workspace, WorkspaceMember
from openctopus_server.dto.config import ConfigPatch
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.services.system_config import patch_config

_PERSONAL_QUOTA = 123_456
_SHARED_QUOTA = 654_321


@pytest.fixture
def resolver() -> Any:
    """Load the proposed resolver without making the whole test module uncollectable."""
    try:
        module = importlib.import_module("openctopus_server.workspace.resolver")
    except ModuleNotFoundError:
        pytest.fail(
            "missing openctopus_server.workspace.resolver.WorkspacePathResolver",
            pytrace=False,
        )
    return module.WorkspacePathResolver()


async def _add_user(db: AsyncSession, *, email: str, name: str) -> User:
    user = User(email=email, password_hash="not-used", name=name)
    db.add(user)
    await db.flush()
    return user


def _workspace_with_suffix(
    *,
    workspace_id: UUID,
    name: str,
    suffix: str,
    created_by: UUID,
) -> Workspace:
    assert "suffix" in Workspace.__table__.columns, (
        "Workspace must persist its assigned suffix so references stay stable across renames"
    )
    return Workspace(
        id=workspace_id,
        name=name,
        suffix=suffix,
        quota_bytes=_SHARED_QUOTA,
        created_by=created_by,
    )


async def _assert_workspace_not_found(operation: Callable[[], Any]) -> None:
    with pytest.raises(WorkspaceError) as caught:
        await operation()
    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND


async def test_relative_path_resolves_to_authenticated_users_personal_workspace(
    pg_engine: AsyncEngine,
    resolver: Any,
) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _add_user(db, email="personal@test.com", name="Personal")
        db.add(SystemConfig(key="quota_bytes", value=_PERSONAL_QUOTA))
        await db.commit()

        resolved = await resolver.resolve(db, user_id=user.id, path="notes/today.md")

    assert resolved.target.kind == "personal"
    assert resolved.target.id == user.id
    assert resolved.relative_path == "notes/today.md"
    assert resolved.quota_bytes == _PERSONAL_QUOTA


async def test_explicit_own_uuid_path_resolves_to_personal_workspace(
    pg_engine: AsyncEngine,
    resolver: Any,
) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _add_user(db, email="explicit@test.com", name="Explicit")
        db.add(SystemConfig(key="quota_bytes", value=_PERSONAL_QUOTA))
        await db.commit()

        resolved = await resolver.resolve(
            db,
            user_id=user.id,
            path=f"/{user.id}/skills/example/SKILL.md",
        )

    assert resolved.target.kind == "personal"
    assert resolved.target.id == user.id
    assert resolved.relative_path == "skills/example/SKILL.md"
    assert resolved.quota_bytes == _PERSONAL_QUOTA


async def test_strict_shared_ref_resolves_for_a_member(
    pg_engine: AsyncEngine,
    resolver: Any,
) -> None:
    workspace_id = UUID("a4f7e2d1-0000-4000-8000-000000000001")
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _add_user(db, email="member@test.com", name="Member")
        workspace = _workspace_with_suffix(
            workspace_id=workspace_id,
            name="Production Department",
            suffix="a4f7e2d1",
            created_by=user.id,
        )
        db.add(workspace)
        db.add(WorkspaceMember(workspace_id=workspace_id, user_id=user.id))
        await db.commit()

        resolved = await resolver.resolve(
            db,
            user_id=user.id,
            path="/Production Department@a4f7e2d1/sprint.md",
        )

    assert resolved.target.kind == "shared"
    assert resolved.target.id == workspace_id
    assert resolved.relative_path == "sprint.md"
    assert resolved.quota_bytes == _SHARED_QUOTA


async def test_stale_shared_name_returns_not_found_after_rename(
    pg_engine: AsyncEngine,
    resolver: Any,
) -> None:
    workspace_id = UUID("b4f7e2d1-0000-4000-8000-000000000001")
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _add_user(db, email="rename@test.com", name="Rename")
        workspace = _workspace_with_suffix(
            workspace_id=workspace_id,
            name="Before",
            suffix="b4f7e2d1",
            created_by=user.id,
        )
        db.add(workspace)
        db.add(WorkspaceMember(workspace_id=workspace_id, user_id=user.id))
        await db.commit()

        workspace.name = "After"
        await db.commit()

        await _assert_workspace_not_found(
            lambda: resolver.resolve(
                db,
                user_id=user.id,
                path="/Before@b4f7e2d1/file.txt",
            )
        )
        resolved = await resolver.resolve(
            db,
            user_id=user.id,
            path="/After@b4f7e2d1/file.txt",
        )

    assert resolved.target.id == workspace_id
    assert resolved.relative_path == "file.txt"


async def test_missing_shared_ref_returns_workspace_not_found(
    pg_engine: AsyncEngine,
    resolver: Any,
) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _add_user(db, email="missing@test.com", name="Missing")
        await db.commit()

        await _assert_workspace_not_found(
            lambda: resolver.resolve(
                db,
                user_id=user.id,
                path="/Missing@deadbeef/file.txt",
            )
        )


async def test_inaccessible_shared_ref_is_indistinguishable_from_missing(
    pg_engine: AsyncEngine,
    resolver: Any,
) -> None:
    workspace_id = UUID("c4f7e2d1-0000-4000-8000-000000000001")
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _add_user(db, email="owner@test.com", name="Owner")
        outsider = await _add_user(db, email="outsider@test.com", name="Outsider")
        workspace = _workspace_with_suffix(
            workspace_id=workspace_id,
            name="Private",
            suffix="c4f7e2d1",
            created_by=owner.id,
        )
        db.add(workspace)
        db.add(WorkspaceMember(workspace_id=workspace_id, user_id=owner.id))
        await db.commit()

        await _assert_workspace_not_found(
            lambda: resolver.resolve(
                db,
                user_id=outsider.id,
                path="/Private@c4f7e2d1/secret.txt",
            )
        )


async def test_workspace_suffix_is_persisted_and_survives_rename(
    pg_engine: AsyncEngine,
) -> None:
    assert "suffix" in Workspace.__table__.columns, (
        "Workspace needs a non-null persisted suffix column; deriving it from the current "
        "accessible set cannot preserve ADR-108 rename and membership stability"
    )
    assert Workspace.__table__.columns["suffix"].nullable is False

    workspace_id = UUID("d4f7e2d1-a000-4000-8000-000000000001")
    assigned_suffix = "d4f7e2d1a"
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await _add_user(db, email="suffix@test.com", name="Suffix")
        workspace = _workspace_with_suffix(
            workspace_id=workspace_id,
            name="Original",
            suffix=assigned_suffix,
            created_by=user.id,
        )
        db.add(workspace)
        db.add(WorkspaceMember(workspace_id=workspace_id, user_id=user.id))
        await db.commit()

        workspace.name = "Renamed"
        await db.commit()

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        reloaded = await db.get(Workspace, workspace_id)

    assert reloaded is not None
    assert reloaded.name == "Renamed"
    assert reloaded.suffix == assigned_suffix


async def test_personal_quota_update_waits_for_a_resolved_operation(
    pg_engine: AsyncEngine,
    resolver: Any,
) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        user = await _add_user(setup_db, email="quota-lock@test.com", name="Quota Lock")
        setup_db.add(SystemConfig(key="quota_bytes", value=100))
        await setup_db.commit()
        user_id = user.id

    operation_db = AsyncSession(pg_engine, expire_on_commit=False)
    await resolver.resolve(operation_db, user_id=user_id, path="file.txt")

    async def lower_quota() -> None:
        async with AsyncSession(pg_engine) as admin_db:
            await patch_config(admin_db, ConfigPatch(quota_bytes=50))

    update = asyncio.create_task(lower_quota())
    try:
        await asyncio.sleep(0.05)
        assert not update.done()
    finally:
        await operation_db.rollback()
        await operation_db.close()
    await update
