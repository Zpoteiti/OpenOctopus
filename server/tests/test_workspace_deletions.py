import asyncio
import os
import signal
import time
from multiprocessing.connection import Connection
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import Device, User, WorkspaceDeletion
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.services import users, workspace_purge
from openctopus_server.services.workspace_deletions import (
    WorkspaceDeletionWorker,
    WorkspacePurgeStorageConfig,
    finalize_workspace_deletions,
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


def _purge_config(endpoint: str = "test") -> WorkspacePurgeStorageConfig:
    return WorkspacePurgeStorageConfig(
        endpoint=endpoint,
        secure=False,
        bucket="test",
        region="us-east-1",
        access_key="test-access",
        secret_key="test-secret",
    )


def _successful_purge_child(
    config: WorkspacePurgeStorageConfig,
    target: WorkspaceTarget,
    result: Connection,
) -> None:
    result.send(True)
    result.close()


def _failed_purge_child(
    config: WorkspacePurgeStorageConfig,
    target: WorkspaceTarget,
    result: Connection,
) -> None:
    result.send(False)
    result.close()


def _blocking_purge_child(
    config: WorkspacePurgeStorageConfig,
    target: WorkspaceTarget,
    result: Connection,
) -> None:
    Path(config.endpoint).write_text(str(os.getpid()), encoding="utf-8")
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(1)


def _partial_file_purge_child(
    config: WorkspacePurgeStorageConfig,
    target: WorkspaceTarget,
    result: Connection,
) -> None:
    del target, result
    root = Path(config.endpoint)
    (root / "object-1").unlink()
    (root / "partial.pid").write_text(str(os.getpid()), encoding="utf-8")
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(1)


def _complete_file_purge_child(
    config: WorkspacePurgeStorageConfig,
    target: WorkspaceTarget,
    result: Connection,
) -> None:
    del target
    root = Path(config.endpoint)
    for path in root.glob("object-*"):
        path.unlink()
    result.send(True)
    result.close()


def test_purge_child_uses_one_connection_and_only_the_workspace_prefix(monkeypatch) -> None:
    target = WorkspaceTarget.personal(uuid4())
    prefix = f"users/{target.id}/"
    object_names = {f"{prefix}a.txt", f"{prefix}nested/b.txt", "users/other/keep.txt"}
    captured: dict[str, object] = {}

    class FakeMinio:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def list_objects(self, bucket, *, prefix, recursive):
            assert bucket == "test"
            assert recursive is True
            return (
                SimpleNamespace(object_name=name)
                for name in sorted(object_names)
                if name.startswith(prefix)
            )

        def remove_object(self, bucket, object_name) -> None:
            assert bucket == "test"
            object_names.remove(object_name)

    monkeypatch.setattr(workspace_purge, "Minio", FakeMinio)

    workspace_purge._purge_workspace(_purge_config(), target)

    assert object_names == {"users/other/keep.txt"}
    http_client = captured["http_client"]
    assert http_client.connection_pool_kw["maxsize"] == 1


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

        await users.delete_user(
            db, user, workspace_fs=workspace_fs, device_registry=DeviceRegistry()
        )

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


async def test_slow_workspace_purge_does_not_hold_database_connection(
    pg_engine: AsyncEngine,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingLifecycle(_LifecycleFS):
        async def purge_workspace(self, target: WorkspaceTarget) -> None:
            entered.set()
            await release.wait()
            await super().purge_workspace(target)

    workspace_fs = BlockingLifecycle()
    target = WorkspaceTarget.shared(uuid4())
    async with AsyncSession(pg_engine) as setup_db:
        setup_db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await setup_db.commit()

    async with AsyncSession(pg_engine) as db:
        finalizing = asyncio.create_task(finalize_workspace_deletions(db, [target], workspace_fs))
        await entered.wait()
        try:
            assert pg_engine.pool.checkedout() == 0
        finally:
            release.set()
        await finalizing


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
            await users.delete_user(
                db, user, workspace_fs=workspace_fs, device_registry=DeviceRegistry()
            )
        except ConnectionError:
            pass
        else:
            raise AssertionError("the simulated ambiguous commit must be reported")

    async with AsyncSession(pg_engine) as verify_db:
        assert await verify_db.get(User, user_id) is None
        deletion = await verify_db.get(WorkspaceDeletion, ("personal", user_id))

    assert deletion is not None
    assert workspace_fs.reactivated == []


async def test_user_deletion_commit_and_device_invalidation_survive_cancellation(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_fs = _LifecycleFS()
    invalidated = asyncio.Event()

    class RecordingRegistry:
        async def remove_devices(self, device_ids: tuple[object, ...]) -> None:
            assert device_ids == (device_id,)
            invalidated.set()

    registry = RecordingRegistry()
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        user = User(
            email="cancelled-delete@test.com",
            password_hash="not-used",
            name="Cancelled Delete",
        )
        setup_db.add(user)
        await setup_db.flush()
        user_id = user.id
        device = Device(
            user_id=user_id,
            name="laptop",
            token_hash=b"c" * 32,
            token_hint="openoctopus_dev_...cancel",
            workspace_path="~/workspace",
            restrict_to_workspace=True,
            ssrf_denylist=[],
        )
        setup_db.add(device)
        await setup_db.commit()
        device_id = device.id

    commit_started = asyncio.Event()
    release_commit = asyncio.Event()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await db.get(User, user_id)
        assert user is not None
        original_commit = db.commit

        async def blocked_commit() -> None:
            commit_started.set()
            await release_commit.wait()
            await original_commit()

        monkeypatch.setattr(db, "commit", blocked_commit)
        deletion = asyncio.create_task(
            users.delete_user(
                db,
                user,
                workspace_fs=workspace_fs,
                device_registry=registry,  # type: ignore[arg-type]
            )
        )
        await asyncio.wait_for(commit_started.wait(), timeout=1)

        deletion.cancel()
        await asyncio.sleep(0)
        assert deletion.done() is False

        release_commit.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(deletion, timeout=1)

    async with AsyncSession(pg_engine) as verify_db:
        assert await verify_db.get(User, user_id) is None
    assert invalidated.is_set()


async def test_user_deletion_rollback_does_not_invalidate_devices(
    pg_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_fs = _LifecycleFS()
    invalidated = asyncio.Event()

    class RecordingRegistry:
        async def remove_devices(self, device_ids: tuple[object, ...]) -> None:
            del device_ids
            invalidated.set()

    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        user = User(
            email="rolled-back-delete@test.com",
            password_hash="not-used",
            name="Rolled Back Delete",
        )
        setup_db.add(user)
        await setup_db.commit()
        user_id = user.id

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = await db.get(User, user_id)
        assert user is not None

        async def fail_commit() -> None:
            raise RuntimeError("commit failed")

        monkeypatch.setattr(db, "commit", fail_commit)
        with pytest.raises(RuntimeError, match="commit failed"):
            await users.delete_user(
                db,
                user,
                workspace_fs=workspace_fs,
                device_registry=RecordingRegistry(),  # type: ignore[arg-type]
            )

    async with AsyncSession(pg_engine) as verify_db:
        assert await verify_db.get(User, user_id) is not None
    assert invalidated.is_set() is False


async def test_runtime_worker_finalizes_a_durable_cleanup_job(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _LifecycleFS()
    target = WorkspaceTarget.personal(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()

    worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        purge_storage=_purge_config(),
        purge_timeout_seconds=5,
        shutdown_grace_seconds=0.2,
        retry_interval_seconds=0.01,
        purge_entrypoint=_successful_purge_child,
    )
    worker.start()
    try:
        async with asyncio.timeout(10):
            while target not in workspace_fs.forgotten:
                await asyncio.sleep(0.01)
    finally:
        await worker.close()

    async with AsyncSession(pg_engine) as db:
        assert await db.get(WorkspaceDeletion, (target.kind, target.id)) is None
    assert workspace_fs.purged == []
    assert workspace_fs.forgotten == [target]


async def test_close_wakes_worker_sleeping_between_retry_passes(
    pg_engine: AsyncEngine,
) -> None:
    worker = WorkspaceDeletionWorker(
        pg_engine,
        _LifecycleFS(),
        purge_storage=_purge_config(),
        purge_timeout_seconds=1,
        shutdown_grace_seconds=0.2,
        retry_interval_seconds=60,
        purge_entrypoint=_successful_purge_child,
    )
    worker.start()
    await asyncio.sleep(0)

    async with asyncio.timeout(1):
        await worker.close()

    assert worker._task is None


async def test_runtime_worker_purge_does_not_hold_database_connection(
    pg_engine: AsyncEngine,
    tmp_path: Path,
) -> None:
    started = tmp_path / "purge.pid"
    workspace_fs = _LifecycleFS()
    target = WorkspaceTarget.personal(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()

    worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        purge_storage=_purge_config(str(started)),
        retry_interval_seconds=0.01,
        purge_timeout_seconds=60,
        shutdown_grace_seconds=0.01,
        purge_entrypoint=_blocking_purge_child,
    )
    worker.start()
    async with asyncio.timeout(2):
        while not started.exists():
            await asyncio.sleep(0.01)
    try:
        assert pg_engine.pool.checkedout() == 0
    finally:
        await worker.close()


async def test_close_kills_and_reaps_a_blocked_purge_child(
    pg_engine: AsyncEngine,
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setattr(
        "openctopus_server.services.workspace_deletions._CHILD_TERMINATE_GRACE_SECONDS",
        0.02,
    )
    started = tmp_path / "purge.pid"
    workspace_fs = _LifecycleFS()
    target = WorkspaceTarget.personal(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()

    worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        purge_storage=_purge_config(str(started)),
        retry_interval_seconds=0.01,
        purge_timeout_seconds=60,
        shutdown_grace_seconds=0.01,
        purge_entrypoint=_blocking_purge_child,
    )
    worker.start()
    async with asyncio.timeout(2):
        while not started.exists():
            await asyncio.sleep(0.01)
    pid = int(started.read_text(encoding="utf-8"))

    async with asyncio.timeout(1):
        await worker.close()
        await worker.close()

    assert worker._task is None
    assert worker._active_process is None
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert "Workspace object cleanup retry failed" not in caplog.text
    async with AsyncSession(pg_engine) as db:
        assert await db.get(WorkspaceDeletion, (target.kind, target.id)) is not None


async def test_repeated_close_cancellation_waits_for_blocked_child_reap(
    pg_engine: AsyncEngine,
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "openctopus_server.services.workspace_deletions._CHILD_TERMINATE_GRACE_SECONDS",
        0.1,
    )
    started = tmp_path / "purge.pid"
    workspace_fs = _LifecycleFS()
    target = WorkspaceTarget.personal(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()

    worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        purge_storage=_purge_config(str(started)),
        retry_interval_seconds=0.01,
        purge_timeout_seconds=60,
        shutdown_grace_seconds=0.01,
        purge_entrypoint=_blocking_purge_child,
    )
    worker.start()
    async with asyncio.timeout(2):
        while not started.exists():
            await asyncio.sleep(0.01)
    pid = int(started.read_text(encoding="utf-8"))
    closing = asyncio.create_task(worker.close())
    try:
        await asyncio.sleep(0.03)
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing

        assert worker._task is None
        assert worker._active_process is None
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    finally:
        if not closing.done():
            closing.cancel()
            await asyncio.gather(closing, return_exceptions=True)
        await worker.close()


async def test_runtime_purge_timeout_retains_the_durable_row(
    pg_engine: AsyncEngine,
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "openctopus_server.services.workspace_deletions._CHILD_TERMINATE_GRACE_SECONDS",
        0.02,
    )
    started = tmp_path / "purge.pid"
    workspace_fs = _LifecycleFS()
    target = WorkspaceTarget.personal(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()

    worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        purge_storage=_purge_config(str(started)),
        retry_interval_seconds=0.01,
        purge_timeout_seconds=2,
        shutdown_grace_seconds=0.2,
        purge_entrypoint=_blocking_purge_child,
    )
    worker.start()
    async with asyncio.timeout(5):
        while not started.exists():
            await asyncio.sleep(0.01)
    await asyncio.sleep(2.05)
    await worker.close()

    async with AsyncSession(pg_engine) as db:
        assert await db.get(WorkspaceDeletion, (target.kind, target.id)) is not None
    assert workspace_fs.forgotten == []


async def test_partial_child_purge_keeps_row_for_replay_to_completion(
    pg_engine: AsyncEngine,
    tmp_path: Path,
    monkeypatch,
) -> None:
    replay_completed = asyncio.Event()

    class SignallingLifecycleFS(_LifecycleFS):
        async def forget_workspace(self, target: WorkspaceTarget) -> None:
            await super().forget_workspace(target)
            replay_completed.set()

    monkeypatch.setattr(
        "openctopus_server.services.workspace_deletions._CHILD_TERMINATE_GRACE_SECONDS",
        0.02,
    )
    (tmp_path / "object-1").write_bytes(b"first")
    (tmp_path / "object-2").write_bytes(b"second")
    workspace_fs = SignallingLifecycleFS()
    target = WorkspaceTarget.personal(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()

    partial_worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        purge_storage=_purge_config(str(tmp_path)),
        retry_interval_seconds=0.01,
        purge_timeout_seconds=60,
        shutdown_grace_seconds=0.01,
        purge_entrypoint=_partial_file_purge_child,
    )
    partial_worker.start()
    async with asyncio.timeout(2):
        while not (tmp_path / "partial.pid").exists():
            await asyncio.sleep(0.01)
    await partial_worker.close()

    assert not (tmp_path / "object-1").exists()
    assert (tmp_path / "object-2").exists()
    async with AsyncSession(pg_engine) as db:
        assert await db.get(WorkspaceDeletion, (target.kind, target.id)) is not None

    replay_worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        purge_storage=_purge_config(str(tmp_path)),
        retry_interval_seconds=0.01,
        purge_timeout_seconds=60,
        shutdown_grace_seconds=0.2,
        purge_entrypoint=_complete_file_purge_child,
    )
    replay_worker.start()
    try:
        async with asyncio.timeout(10):
            await replay_completed.wait()
    finally:
        await replay_worker.close()

    assert not (tmp_path / "object-2").exists()
    async with AsyncSession(pg_engine) as db:
        assert await db.get(WorkspaceDeletion, (target.kind, target.id)) is None


async def test_runtime_purge_failure_does_not_log_credentials(
    pg_engine: AsyncEngine,
    caplog,
) -> None:
    workspace_fs = _LifecycleFS()
    target = WorkspaceTarget.personal(uuid4())
    async with AsyncSession(pg_engine) as db:
        db.add(WorkspaceDeletion(kind=target.kind, target_id=target.id))
        await db.commit()
    config = _purge_config()

    worker = WorkspaceDeletionWorker(
        pg_engine,
        workspace_fs,
        purge_storage=config,
        retry_interval_seconds=0.01,
        purge_timeout_seconds=1,
        shutdown_grace_seconds=0.2,
        purge_entrypoint=_failed_purge_child,
    )
    worker.start()
    await asyncio.sleep(0.1)
    await worker.close()

    assert config.access_key not in caplog.text
    assert config.secret_key not in caplog.text
