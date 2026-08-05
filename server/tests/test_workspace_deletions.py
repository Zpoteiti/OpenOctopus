import asyncio
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User, WorkspaceDeletion
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.services import users
from openctopus_server.services.workspace_deletions import (
    WorkspaceDeletionWorker,
    recover_workspace_deletions,
)
from openctopus_server.workspace.fs import WorkspaceTarget


class _LifecycleFS:
    def __init__(self, *, purge_failures: int = 0) -> None:
        self.purge_failures = purge_failures
        self.retired: list[WorkspaceTarget] = []
        self.purged: list[WorkspaceTarget] = []
        self.reactivated: list[WorkspaceTarget] = []
        self.forgotten: list[WorkspaceTarget] = []

    async def retire_workspace(self, target: WorkspaceTarget) -> None:
        self.retired.append(target)

    async def purge_workspace(self, target: WorkspaceTarget) -> None:
        self.purged.append(target)
        if self.purge_failures:
            self.purge_failures -= 1
            raise WorkspaceError(
                ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
                "Object storage is unavailable",
            )

    async def reactivate_workspace(self, target: WorkspaceTarget) -> None:
        self.reactivated.append(target)

    async def forget_workspace(self, target: WorkspaceTarget) -> None:
        self.forgotten.append(target)


class _CommitThenDisconnectSession(AsyncSession):
    async def commit(self) -> None:
        await super().commit()
        raise ConnectionError("commit acknowledgement was lost")


async def test_failed_purge_leaves_durable_cleanup_after_metadata_deletion(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _LifecycleFS(purge_failures=1)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(
            email="durable-delete@test.com",
            password_hash="not-used",
            name="Durable Delete",
        )
        db.add(user)
        await db.commit()
        user_id = user.id

        await users.delete_user(db, user, workspace_fs=workspace_fs)

        assert await db.get(User, user_id) is None
        deletion = await db.scalar(select(WorkspaceDeletion))

    assert deletion is not None
    assert deletion.kind == "personal"
    assert deletion.target_id == user_id
    assert workspace_fs.retired == [WorkspaceTarget.personal(user_id)]
    assert workspace_fs.reactivated == []
    assert workspace_fs.forgotten == []


async def test_startup_recovery_purges_and_removes_durable_cleanup(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _LifecycleFS()
    target = WorkspaceTarget.shared(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()

    await recover_workspace_deletions(pg_engine, workspace_fs)

    async with AsyncSession(pg_engine) as db:
        assert await db.scalar(select(WorkspaceDeletion)) is None
    assert workspace_fs.purged == [target]
    assert workspace_fs.forgotten == [target]


async def test_ambiguous_commit_keeps_a_deleted_target_retired(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _LifecycleFS()
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        user = User(
            email="ambiguous-delete@test.com",
            password_hash="not-used",
            name="Ambiguous Delete",
        )
        setup_db.add(user)
        await setup_db.commit()
        user_id = user.id

    async with _CommitThenDisconnectSession(pg_engine, expire_on_commit=False) as db:
        user = await db.get(User, user_id)
        assert user is not None
        try:
            await users.delete_user(db, user, workspace_fs=workspace_fs)
        except ConnectionError:
            pass
        else:
            raise AssertionError("the simulated ambiguous commit must be reported")

    async with AsyncSession(pg_engine) as verify_db:
        assert await verify_db.get(User, user_id) is None
        deletion = await verify_db.get(WorkspaceDeletion, ("personal", user_id))

    assert deletion is not None
    assert workspace_fs.reactivated == []


async def test_runtime_worker_retries_a_durable_cleanup_job(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _LifecycleFS(purge_failures=1)
    target = WorkspaceTarget.personal(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()

    worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        retry_interval_seconds=0.01,
    )
    worker.start()
    try:
        async with asyncio.timeout(1):
            while target not in workspace_fs.forgotten:
                await asyncio.sleep(0.01)
    finally:
        await worker.close()

    async with AsyncSession(pg_engine) as db:
        assert await db.get(WorkspaceDeletion, (target.kind, target.id)) is None
    assert workspace_fs.purged == [target, target]
