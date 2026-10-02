"""Independent checks for failure boundaries around Dream's two durable stores."""

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from test_dream import NOW, conversation, service, user

from openctopus_server.automations.dream import apply_proposal
from openctopus_server.db.models import DreamProgress, DreamRun, User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.provider.anthropic import ProviderResult


@pytest.mark.parametrize(
    "failure_boundary", ["write", "finish", "storage_write", "storage_after_write"]
)
async def test_prepared_proposal_recovers_ordinary_failures_without_repeating_models(
    pg_engine, monkeypatch, failure_boundary
):
    owner = await user(pg_engine)
    _, message = await conversation(pg_engine, owner)
    dream, memory, gate, writer = service(pg_engine)

    async def failure(*args, **kwargs):
        if failure_boundary == "finish":
            args[0].status, args[0].finished_at = "updated", NOW
        if failure_boundary == "storage_after_write":
            await original(*args, **kwargs)
        if failure_boundary.startswith("storage_"):
            raise WorkspaceError(ErrorCode.WORKSPACE_STORAGE_ERROR, "Object storage request failed")
        raise RuntimeError("Temporary storage or finalization failure")

    target = dream if failure_boundary == "finish" else memory
    attribute = "_finish" if failure_boundary == "finish" else "write"
    original = getattr(target, attribute)
    monkeypatch.setattr(target, attribute, failure)
    deferred = await dream.process_user(owner.id, now=NOW)
    assert deferred.status == "pending"
    async with AsyncSession(pg_engine) as db:
        assert await db.get(DreamProgress, message.id) is None
        saved = await db.get(DreamRun, deferred.id)
        assert saved.after is not None and saved.status == "pending"

    monkeypatch.setattr(target, attribute, original)
    gate.config_state = "not_configured"
    recovered = await dream.process_user(owner.id, now=NOW + timedelta(minutes=1))
    assert recovered.id == deferred.id and recovered.status == "updated"
    assert recovered.error is None
    assert len(gate.calls) == len(writer.calls) == len(memory.writes) == 1
    async with AsyncSession(pg_engine) as db:
        assert (await db.get(DreamProgress, message.id)).complete


@pytest.mark.parametrize("failure_boundary", ["write", "save"])
async def test_restore_recovers_transient_failure_without_rerunning_the_update(
    pg_engine, monkeypatch, failure_boundary
):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, gate, writer = service(pg_engine)
    before = memory.data
    updated = await dream.process_user(owner.id, now=NOW)
    target = memory if failure_boundary == "write" else dream
    attribute = "write" if failure_boundary == "write" else "_save"
    original = getattr(target, attribute)

    async def failure(*args, **kwargs):
        raise RuntimeError("Temporary restore failure")

    monkeypatch.setattr(target, attribute, failure)
    with pytest.raises(RuntimeError):
        await dream.restore(owner.id, updated.id)
    async with AsyncSession(pg_engine) as db:
        assert (await db.get(DreamRun, updated.id)).status == "restoring"

    monkeypatch.setattr(target, attribute, original)
    gate.config_state = "not_configured"
    recovered = await dream.process_user(owner.id, now=NOW + timedelta(minutes=1))
    assert recovered.status == "restored" and memory.data == before
    assert len(memory.writes) == 2
    assert len(gate.calls) == len(writer.calls) == 1


async def test_restore_write_conflict_preserves_successful_update_history(pg_engine, monkeypatch):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, _, _ = service(pg_engine)
    updated = await dream.process_user(owner.id, now=NOW)

    async def manual_edit_before_write(*args, **kwargs):
        memory.data = b"A newer manual memory edit"
        raise WorkspaceError(ErrorCode.WORKSPACE_FILE_CHANGED, "Memory changed")

    monkeypatch.setattr(memory, "write", manual_edit_before_write)
    with pytest.raises(WorkspaceError) as caught:
        await dream.restore(owner.id, updated.id)
    assert caught.value.code is ErrorCode.WORKSPACE_FILE_CHANGED
    assert memory.data == b"A newer manual memory edit"
    async with AsyncSession(pg_engine) as db:
        saved = await db.get(DreamRun, updated.id)
        assert saved.status == "updated" and saved.restored_at is None


async def test_restore_with_unfinished_batch_returns_controlled_conflict(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, _, _ = service(pg_engine)
    updated = await dream.process_user(owner.id, now=NOW)
    async with AsyncSession(pg_engine) as db:
        db.add(DreamRun(user_id=owner.id, status="pending", source=[], started_at=NOW))
        await db.commit()
    with pytest.raises(WorkspaceError) as caught:
        await dream.restore(owner.id, updated.id)
    assert caught.value.code is ErrorCode.WORKSPACE_FILE_CHANGED
    assert memory.data == updated.after.encode()
    async with AsyncSession(pg_engine) as db:
        assert (await db.get(DreamRun, updated.id)).status == "updated"


async def test_worker_failure_cancels_and_joins_other_workers(pg_engine, monkeypatch):
    failing = await user(pg_engine)
    waiting = await user(pg_engine)
    dream, _, _, _ = service(pg_engine)
    started = asyncio.Event()
    canceled = asyncio.Event()

    async def worker(user_id, *, now):
        if user_id == failing.id:
            await started.wait()
            raise RuntimeError("Database failure before the processing boundary")
        assert user_id == waiting.id
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            canceled.set()
            raise

    monkeypatch.setattr(dream, "process_user", worker)
    with pytest.raises(ExceptionGroup):
        await dream.tick(NOW)
    assert canceled.is_set()
    await dream.close()
    async with AsyncSession(pg_engine) as db:
        assert not list(await db.scalars(select(DreamRun)))


def test_supported_obsolete_memory_can_be_removed_with_an_empty_replacement():
    result = ProviderResult(
        content=[
            {
                "type": "tool_use",
                "name": "propose_memory_update",
                "input": {
                    "edits": [
                        {"old": "Obsolete preference.\n", "new": "", "source_ids": ["source"]}
                    ],
                    "append": "",
                    "append_source_ids": [],
                },
            }
        ],
        fingerprint="mock",
    )
    assert (
        apply_proposal(result, "# Memory\nObsolete preference.\n", [{"id": "source"}])
        == "# Memory\n"
    )


async def test_completed_record_survives_a_lost_commit_acknowledgment(pg_engine, monkeypatch):
    owner = await user(pg_engine)
    _, message = await conversation(pg_engine, owner)
    dream, memory, gate, writer = service(pg_engine)
    finish = dream._finish

    async def committed_then_disconnected(*args, **kwargs):
        await finish(*args, **kwargs)
        raise RuntimeError("The completion committed, but its acknowledgment was lost")

    monkeypatch.setattr(dream, "_finish", committed_then_disconnected)
    completed = await dream.process_user(owner.id, now=NOW)
    assert completed.status == "updated" and completed.error is None
    async with AsyncSession(pg_engine) as db:
        assert (await db.get(DreamRun, completed.id)).status == "updated"
        assert (await db.get(DreamProgress, message.id)).complete
    assert len(memory.writes) == len(gate.calls) == len(writer.calls) == 1

    memory.data = b"A manual change after the completed update"
    assert await dream.process_user(owner.id, now=NOW + timedelta(minutes=1)) is None
    async with AsyncSession(pg_engine) as db:
        assert (await db.get(DreamRun, completed.id)).status == "updated"


async def test_restored_record_survives_a_lost_commit_acknowledgment(pg_engine, monkeypatch):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, gate, writer = service(pg_engine)
    updated = await dream.process_user(owner.id, now=NOW)
    save = dream._save

    async def disconnected_before_save(*args, **kwargs):
        raise RuntimeError("The restore file write completed before the database save")

    monkeypatch.setattr(dream, "_save", disconnected_before_save)
    with pytest.raises(RuntimeError):
        await dream.restore(owner.id, updated.id)

    async def committed_then_disconnected(*args, **kwargs):
        await save(*args, **kwargs)
        raise RuntimeError("The restore committed, but its acknowledgment was lost")

    monkeypatch.setattr(dream, "_save", committed_then_disconnected)
    completed = await dream.process_user(owner.id, now=NOW + timedelta(minutes=1))
    assert completed.status == "restored" and completed.error is None
    async with AsyncSession(pg_engine) as db:
        assert (await db.get(DreamRun, updated.id)).status == "restored"
    assert len(memory.writes) == 2
    assert len(gate.calls) == len(writer.calls) == 1


async def test_deleted_user_is_not_resurrected_after_a_processing_failure(pg_engine, monkeypatch):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, _, _, _ = service(pg_engine)

    async def user_deleted_before_finalization(*args, **kwargs):
        async with AsyncSession(pg_engine) as db:
            await db.execute(delete(User).where(User.id == owner.id))
            await db.commit()
        raise RuntimeError("The owner was deleted during processing")

    monkeypatch.setattr(dream, "_finish", user_deleted_before_finalization)
    assert await dream.process_user(owner.id, now=NOW) is None
    async with AsyncSession(pg_engine) as db:
        assert await db.get(User, owner.id) is None
        assert not list(await db.scalars(select(DreamRun)))
        assert not list(await db.scalars(select(DreamProgress)))
