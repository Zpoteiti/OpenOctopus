from __future__ import annotations

import asyncio
import logging
import multiprocessing
from collections.abc import Callable
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from typing import Literal, Protocol, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User, Workspace, WorkspaceDeletion
from openctopus_server.services.workspace_purge import (
    WorkspacePurgeStorageConfig,
    purge_workspace_child,
)
from openctopus_server.workspace.fs import WorkspaceTarget

logger = logging.getLogger(__name__)
_CHILD_POLL_INTERVAL_SECONDS = 0.05
_CHILD_TERMINATE_GRACE_SECONDS = 5.0
PurgeEntrypoint = Callable[[WorkspacePurgeStorageConfig, WorkspaceTarget, Connection], None]


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
        purge_storage: WorkspacePurgeStorageConfig,
        purge_timeout_seconds: float,
        shutdown_grace_seconds: float,
        retry_interval_seconds: float = 60.0,
        purge_entrypoint: PurgeEntrypoint = purge_workspace_child,
    ) -> None:
        self._engine = engine
        self._workspace_fs = workspace_fs
        self._purge_storage = purge_storage
        self._retry_interval_seconds = retry_interval_seconds
        self._purge_timeout_seconds = purge_timeout_seconds
        self._shutdown_grace_seconds = shutdown_grace_seconds
        self._purge_entrypoint = purge_entrypoint
        self._process_context = multiprocessing.get_context("spawn")
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._active_process: BaseProcess | None = None
        self._close_lock = asyncio.Lock()
        self._closed = False

    def start(self) -> None:
        if self._task is None and not self._closed:
            self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            cleanup = asyncio.create_task(self._close_impl())
            cancelled = False
            while True:
                try:
                    await asyncio.shield(cleanup)
                    break
                except asyncio.CancelledError:
                    cancelled = True
            if cancelled:
                raise asyncio.CancelledError

    async def _close_impl(self) -> None:
        self._stop.set()
        task = self._task
        if task is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=self._shutdown_grace_seconds,
                )
            except TimeoutError:
                await self._terminate_active_child()
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            self._task = None
        self._closed = True

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
                targets = await _load_workspace_deletion_targets(self._engine)
            except Exception:
                logger.warning("Workspace object cleanup retry failed")
                continue
            for target in targets:
                if self._stop.is_set():
                    return
                try:
                    succeeded = await self._purge_target(target)
                    if succeeded:
                        await _complete_workspace_deletion(
                            self._engine,
                            self._workspace_fs,
                            target,
                        )
                    elif not self._stop.is_set():
                        logger.warning("Workspace object cleanup retry failed")
                except Exception:
                    if not self._stop.is_set():
                        logger.warning("Workspace object cleanup retry failed")

    async def _purge_target(self, target: WorkspaceTarget) -> bool:
        receiver, sender = self._process_context.Pipe(duplex=False)
        process = self._process_context.Process(
            target=self._purge_entrypoint,
            args=(self._purge_storage, target, sender),
            daemon=False,
        )
        self._active_process = process
        started = False
        try:
            process.start()
            started = True
            sender.close()
            completed = await _wait_for_process(
                process,
                timeout=self._purge_timeout_seconds,
            )
            if not completed:
                await self._terminate_active_child()
                return False
            if process.exitcode != 0 or not receiver.poll():
                return False
            try:
                return receiver.recv() is True
            except (EOFError, OSError):
                return False
        except asyncio.CancelledError:
            await asyncio.shield(self._terminate_active_child())
            raise
        finally:
            sender.close()
            receiver.close()
            if started:
                if process.is_alive():
                    await asyncio.shield(_terminate_process(process))
                else:
                    process.join(timeout=0)
            process.close()
            if self._active_process is process:
                self._active_process = None

    async def _terminate_active_child(self) -> None:
        process = self._active_process
        if process is not None:
            try:
                await _terminate_process(process)
            except ValueError:
                # The purge task won the race and already closed its reaped handle.
                pass


async def _load_workspace_deletion_targets(
    engine: AsyncEngine,
) -> list[WorkspaceTarget]:
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
        return [
            WorkspaceTarget(
                kind=cast(Literal["personal", "shared"], row.kind),
                id=row.target_id,
            )
            for row in rows
        ]


async def _complete_workspace_deletion(
    engine: AsyncEngine,
    workspace_fs: WorkspaceLifecycle,
    target: WorkspaceTarget,
) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
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
    await workspace_fs.forget_workspace(target)


async def _wait_for_process(
    process: BaseProcess,
    *,
    timeout: float | None,
) -> bool:
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    while process.is_alive():
        if deadline is not None and loop.time() >= deadline:
            return False
        await asyncio.sleep(_CHILD_POLL_INTERVAL_SECONDS)
    process.join(timeout=0)
    return True


async def _terminate_process(process: BaseProcess) -> None:
    if not process.is_alive():
        process.join(timeout=0)
        return
    process.terminate()
    if await _wait_for_process(process, timeout=_CHILD_TERMINATE_GRACE_SECONDS):
        return
    process.kill()
    await _wait_for_process(process, timeout=None)
