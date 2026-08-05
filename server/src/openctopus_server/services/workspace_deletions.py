from __future__ import annotations

import asyncio
import logging
from typing import Literal, Protocol, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User, Workspace, WorkspaceDeletion
from openctopus_server.workspace.fs import WorkspaceTarget

logger = logging.getLogger(__name__)


class WorkspaceLifecycle(Protocol):
    async def retire_workspace(self, target: WorkspaceTarget) -> None: ...

    async def purge_workspace(self, target: WorkspaceTarget) -> None: ...

    async def reactivate_workspace(self, target: WorkspaceTarget) -> None: ...

    async def forget_workspace(self, target: WorkspaceTarget) -> None: ...


async def retire_workspace_targets(
    db: AsyncSession,
    targets: list[WorkspaceTarget],
    workspace_fs: WorkspaceLifecycle,
) -> list[WorkspaceTarget]:
    retired: list[WorkspaceTarget] = []
    try:
        for target in targets:
            await workspace_fs.retire_workspace(target)
            retired.append(target)
            db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.flush()
    except BaseException:
        for target in retired:
            await workspace_fs.reactivate_workspace(target)
        raise
    return retired


async def finalize_workspace_deletions(
    db: AsyncSession,
    targets: list[WorkspaceTarget],
    workspace_fs: WorkspaceLifecycle,
) -> None:
    pending: list[WorkspaceTarget] = []
    for target in sorted(targets, key=lambda item: (item.kind, item.id)):
        exists = await db.scalar(
            select(WorkspaceDeletion).where(
                WorkspaceDeletion.kind == target.kind,
                WorkspaceDeletion.target_id == target.id,
            )
        )
        if exists is not None:
            pending.append(target)
    await db.commit()

    for target in pending:
        await workspace_fs.purge_workspace(target)
    for target in pending:
        row = await db.scalar(
            select(WorkspaceDeletion)
            .where(
                WorkspaceDeletion.kind == target.kind,
                WorkspaceDeletion.target_id == target.id,
            )
            .with_for_update()
        )
        if row is not None:
            await db.delete(row)
    await db.commit()
    for target in pending:
        await workspace_fs.forget_workspace(target)


async def try_finalize_workspace_deletions(
    db: AsyncSession,
    targets: list[WorkspaceTarget],
    workspace_fs: WorkspaceLifecycle,
) -> bool:
    if not targets:
        return True
    try:
        await finalize_workspace_deletions(db, targets, workspace_fs)
    except Exception:
        await db.rollback()
        logger.warning("Workspace object cleanup deferred for retry")
        return False
    return True


async def reactivate_if_deletion_rolled_back(
    db: AsyncSession,
    targets: list[WorkspaceTarget],
    workspace_fs: WorkspaceLifecycle,
) -> None:
    """Clear a fence only when a fresh transaction proves commit did not land."""
    bind = db.bind
    if not isinstance(bind, AsyncEngine):
        return
    try:
        async with AsyncSession(bind) as verify_db:
            for target in targets:
                if (
                    await verify_db.get(
                        WorkspaceDeletion,
                        (target.kind, target.id),
                    )
                    is not None
                ):
                    return
                model = User if target.kind == "personal" else Workspace
                if await verify_db.get(model, target.id) is None:
                    return
    except Exception:
        logger.warning("Workspace deletion commit state is unknown; retaining fence")
        return
    for target in targets:
        await workspace_fs.reactivate_workspace(target)


async def recover_workspace_deletions(
    engine: AsyncEngine,
    workspace_fs: WorkspaceLifecycle,
) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        rows = list(
            (
                await db.scalars(
                    select(WorkspaceDeletion).order_by(
                        WorkspaceDeletion.kind,
                        WorkspaceDeletion.target_id,
                    )
                )
            ).all()
        )
        targets = [
            WorkspaceTarget(
                kind=cast(Literal["personal", "shared"], row.kind),
                id=row.target_id,
            )
            for row in rows
        ]
    first_error: Exception | None = None
    for target in targets:
        try:
            async with AsyncSession(engine, expire_on_commit=False) as db:
                await finalize_workspace_deletions(db, [target], workspace_fs)
        except Exception as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


class WorkspaceDeletionWorker:
    """Retry durable RustFS cleanup jobs while this process remains running."""

    def __init__(
        self,
        engine: AsyncEngine,
        workspace_fs: WorkspaceLifecycle,
        *,
        retry_interval_seconds: float = 60.0,
    ) -> None:
        self._engine = engine
        self._workspace_fs = workspace_fs
        self._retry_interval_seconds = retry_interval_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._retry_interval_seconds,
                )
            except TimeoutError:
                pass
            if self._stop.is_set():
                return
            try:
                await recover_workspace_deletions(
                    self._engine,
                    self._workspace_fs,
                )
            except Exception:
                logger.warning("Workspace object cleanup retry failed")
