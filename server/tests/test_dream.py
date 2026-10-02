import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.api.dream import get_dream_service
from openctopus_server.automations.dream import (
    MAX_MEMORY_BYTES,
    SOURCE_CHUNK_CHARS,
    DreamService,
    apply_proposal,
    day_cutoff,
    next_midnight,
)
from openctopus_server.db.models import (
    DreamProgress,
    DreamRun,
    Message,
    PendingMessage,
    Session,
    TurnRun,
    User,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.provider.anthropic import ProviderResult
from openctopus_server.provider.jev import JevChoiceAnswer, JevError
from openctopus_server.workspace.storage import StoredObject

NOW = datetime(2026, 9, 30, tzinfo=UTC)


class Memory:
    def __init__(self, content=b"# Memory\n"):
        self.data = content
        self.writes = []

    @property
    def etag(self):
        return hashlib.sha256(self.data).hexdigest()

    async def stat(self, *args, **kwargs):
        return SimpleNamespace(size=len(self.data), etag=self.etag)

    async def read_with_metadata(self, *args, **kwargs):
        return StoredObject(data=self.data, etag=self.etag, truncated=False)

    async def write(self, *args, data, if_match, if_none_match=False, **kwargs):
        assert kwargs["path"] == "MEMORY.md"
        if if_none_match or if_match != self.etag:
            raise WorkspaceError(ErrorCode.WORKSPACE_FILE_CHANGED, "changed")
        self.data = data
        self.writes.append(data)
        return SimpleNamespace(etag=self.etag)


class Gate:
    def __init__(self, choice="run"):
        self.choice = choice
        self.calls = []
        self.failure = None
        self.config_state = "unchecked"

    async def status(self):
        return SimpleNamespace(state=self.config_state)

    async def evaluate(self, *, state, questions):
        self.calls.append(state)
        if self.failure:
            raise self.failure
        return {
            key: JevChoiceAnswer(
                type="choice",
                choice=self.choice,
                probabilities={
                    "run": 1.0 if self.choice == "run" else 0.0,
                    "skip": 1.0 if self.choice == "skip" else 0.0,
                },
                confidence=1.0,
            )
            for key in questions
        }


class Writer:
    def __init__(self):
        self.calls = []
        self.before_return = None
        self.empty = False

    async def propose_memory_update(self, **kwargs):
        self.calls.append(kwargs)
        state = json.loads(kwargs["messages"][0]["content"][0]["text"])
        if self.before_return:
            await self.before_return()
        proposal = {
            "edits": [],
            "append": "" if self.empty else "Prefers concise answers.\n",
            "append_source_ids": [] if self.empty else [state["conversations"][0]["id"]],
        }
        return ProviderResult(
            content=[{"type": "tool_use", "name": "propose_memory_update", "input": proposal}],
            fingerprint="mock",
        )


async def user(engine, timezone="UTC"):
    async with AsyncSession(engine, expire_on_commit=False) as db:
        result = User(
            email=f"{uuid4()}@example.test", password_hash="hash", name="Owner", timezone=timezone
        )
        db.add(result)
        await db.commit()
        return result


async def conversation(
    engine,
    owner,
    *,
    channel="web",
    content="I prefer concise answers",
    at=NOW - timedelta(hours=1),
    role="human",
    sender="owner",
    session=None,
):
    async with AsyncSession(engine, expire_on_commit=False) as db:
        if session is None:
            session = Session(
                user_id=owner.id,
                session_key=f"{channel}:{uuid4()}",
                channel=channel,
                chat_id="test",
            )
            db.add(session)
            await db.flush()
        message = Message(
            session_id=session.id,
            message_kind=role,
            content=[{"type": "text", "text": content}],
            created_at=at,
            sender_id="speaker" if role == "human" else None,
            sender_display_name="Participant" if role == "human" else None,
            sender_classification=sender if role == "human" else None,
            ingress_tool_profile=("message_only" if sender == "allowed_non_owner" else "owner_full")
            if role == "human"
            else None,
        )
        db.add(message)
        await db.commit()
        return session, message


def service(engine):
    memory, gate, writer = Memory(), Gate(), Writer()
    return (
        DreamService(engine=engine, workspace=memory, jev=gate, writer=writer),
        memory,
        gate,
        writer,
    )


async def test_empty_day_uses_no_model_calls_or_history(pg_engine):
    owner = await user(pg_engine)
    dream, memory, gate, writer = service(pg_engine)
    assert await dream.process_user(owner.id, now=NOW) is None
    assert not gate.calls and not writer.calls and not memory.writes
    async with AsyncSession(pg_engine) as db:
        assert not list(await db.scalars(select(DreamRun)))


async def test_unconfigured_jev_does_not_start_dream_and_resumes_when_configured(
    pg_engine, monkeypatch
):
    owner = await user(pg_engine)
    _, message = await conversation(pg_engine, owner)
    dream, memory, gate, writer = service(pg_engine)
    gate.config_state = "not_configured"
    original_stat = memory.stat
    original_read = memory.read_with_metadata

    async def forbidden(*args, **kwargs):
        raise AssertionError("unconfigured Dream read memory")

    monkeypatch.setattr(memory, "stat", forbidden)
    monkeypatch.setattr(memory, "read_with_metadata", forbidden)
    for hours in (0, 1, 24, 48):
        assert await dream.process_user(owner.id, now=NOW + timedelta(hours=hours)) is None
    assert not gate.calls and not writer.calls
    async with AsyncSession(pg_engine) as db:
        assert not list(await db.scalars(select(DreamRun)))
        assert await db.get(DreamProgress, message.id) is None

    gate.config_state = "unchecked"
    monkeypatch.setattr(memory, "stat", original_stat)
    monkeypatch.setattr(memory, "read_with_metadata", original_read)
    resumed = await dream.process_user(owner.id, now=NOW + timedelta(hours=49))
    assert resumed.status == "updated"
    assert len(gate.calls) == len(writer.calls) == 1
    async with AsyncSession(pg_engine) as db:
        assert (await db.get(DreamProgress, message.id)).complete


async def test_all_channels_and_speakers_are_preserved_but_other_users_today_and_summaries_excluded(
    pg_engine,
):
    owner, other = await user(pg_engine), await user(pg_engine)
    for channel in ["web", "cron", "heartbeat", "discord", "dingtalk"]:
        await conversation(pg_engine, owner, channel=channel)
    await conversation(pg_engine, owner, role="assistant", content="I will do this")
    await conversation(
        pg_engine, owner, sender="allowed_non_owner", content="My name is someone else"
    )
    await conversation(
        pg_engine, owner, role="compaction_summary", content="Do not repeat summaries"
    )
    await conversation(pg_engine, owner, at=NOW, content="today")
    await conversation(pg_engine, other, content="private other user")
    dream, _, gate, writer = service(pg_engine)
    gate.choice = "skip"
    run = await dream.process_user(owner.id, now=NOW)
    assert run.status == "skipped"
    assert len(run.source) == 7
    source = gate.calls[0]["conversations"]
    assert {item["channel"] for item in source} == {
        "web",
        "cron",
        "heartbeat",
        "discord",
        "dingtalk",
    }
    assert any(item["role"] == "assistant" for item in source)
    assert any(item["sender_classification"] == "allowed_non_owner" for item in source)
    assert not writer.calls
    assert await dream.process_user(owner.id, now=NOW) is None
    assert all("text" not in item for item in run.source)


async def test_running_and_queued_sessions_wait_until_settled(pg_engine):
    owner = await user(pg_engine)
    active, message = await conversation(pg_engine, owner)
    queued, _ = await conversation(pg_engine, owner)
    async with AsyncSession(pg_engine) as db:
        turn = TurnRun(
            session_id=active.id,
            runner_instance_id=uuid4(),
            status="running",
            tool_profile="owner_full",
            input_message_ids=[str(message.id)],
        )
        pending = PendingMessage(
            session_id=queued.id,
            user_id=owner.id,
            session_key=queued.session_key,
            content=[],
            sender_id="speaker",
            sender_classification="owner",
            ingress_tool_profile="owner_full",
        )
        db.add_all([turn, pending])
        await db.commit()
        dream, _, gate, _ = service(pg_engine)
        gate.choice = "skip"
        assert await dream.process_user(owner.id, now=NOW) is None
        turn.status = "completed"
        await db.delete(pending)
        await db.commit()
    run = await dream.process_user(owner.id, now=NOW + timedelta(minutes=1))
    assert len(run.source) == 2


async def test_large_message_continues_without_truncating_or_repeating_progress(pg_engine):
    owner = await user(pg_engine)
    body = "汉" * (SOURCE_CHUNK_CHARS + 11)
    _, message = await conversation(pg_engine, owner, content=body)
    dream, _, gate, _ = service(pg_engine)
    gate.choice = "skip"
    await dream.process_user(owner.id, now=NOW)
    async with AsyncSession(pg_engine) as db:
        progress = await db.get(DreamProgress, message.id)
        assert progress.next_offset == SOURCE_CHUNK_CHARS and not progress.complete
    await dream.process_user(owner.id, now=NOW)
    assert "".join(call["conversations"][0]["text"] for call in gate.calls) == body
    assert gate.calls[1]["conversations"][0]["start_offset"] == SOURCE_CHUNK_CHARS
    assert await dream.process_user(owner.id, now=NOW) is None


async def test_failure_keeps_work_and_retries_after_bounded_delay(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, gate, writer = service(pg_engine)
    gate.failure = JevError("unreachable")
    run = await dream.process_user(owner.id, now=NOW)
    assert run.status == "failed" and run.error == "jev_unreachable"
    assert not memory.writes and not writer.calls
    gate.config_state = "unreachable"
    assert await dream.process_user(owner.id, now=NOW + timedelta(minutes=5)) is None
    assert len(gate.calls) == 1
    gate.failure, gate.choice = None, "skip"
    retry = await dream.process_user(owner.id, now=NOW + timedelta(hours=1))
    assert retry.status == "skipped" and retry.source == run.source
    assert len(gate.calls) == 2


async def test_update_uses_restricted_writer_and_undo_preserves_progress(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, _, writer = service(pg_engine)
    before = memory.data
    run = await dream.process_user(owner.id, now=NOW)
    assert run.status == "updated" and run.before == before.decode()
    assert memory.data == run.after.encode()
    assert writer.calls[0]["tool"]["name"] == "propose_memory_update"
    restored = await dream.restore(owner.id, run.id)
    assert restored.status == "restored" and memory.data == before
    assert await dream.process_user(owner.id, now=NOW) is None
    assert (await dream.restore(owner.id, run.id)).status == "restored"


async def test_manual_edit_during_proposal_is_preserved_and_work_retained(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, _, writer = service(pg_engine)

    async def manual_edit():
        memory.data = b"My manual memory"

    writer.before_return = manual_edit
    run = await dream.process_user(owner.id, now=NOW)
    assert run.status == "failed" and run.error == "workspace_file_changed"
    assert memory.data == b"My manual memory" and not memory.writes
    async with AsyncSession(pg_engine) as db:
        assert not list(await db.scalars(select(DreamProgress)))


async def test_restore_rejects_new_manual_edit_and_other_owner(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, _, _ = service(pg_engine)
    run = await dream.process_user(owner.id, now=NOW)
    memory.data = b"A newer manual edit"
    with pytest.raises(WorkspaceError) as caught:
        await dream.restore(owner.id, run.id)
    assert caught.value.code == ErrorCode.WORKSPACE_FILE_CHANGED
    with pytest.raises(WorkspaceError) as caught:
        await dream.restore(uuid4(), run.id)
    assert caught.value.code == ErrorCode.WORKSPACE_NOT_FOUND
    assert memory.data == b"A newer manual edit"


async def test_interruption_after_write_recovers_without_second_model_or_write(
    pg_engine, monkeypatch
):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, gate, writer = service(pg_engine)
    finish = dream._finish

    async def interrupted(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(dream, "_finish", interrupted)
    with pytest.raises(asyncio.CancelledError):
        await dream.process_user(owner.id, now=NOW)
    assert len(memory.writes) == 1
    monkeypatch.setattr(dream, "_finish", finish)
    run = await dream.process_user(owner.id, now=NOW)
    assert run.status == "updated"
    assert len(memory.writes) == len(gate.calls) == len(writer.calls) == 1


async def test_unchanged_proposal_advances_without_file_write(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, _, writer = service(pg_engine)
    writer.empty = True
    run = await dream.process_user(owner.id, now=NOW)
    assert run.status == "unchanged" and not memory.writes
    assert await dream.process_user(owner.id, now=NOW) is None


async def test_oversized_memory_does_not_reach_models(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, gate, writer = service(pg_engine)
    memory.data = b"x" * (MAX_MEMORY_BYTES + 1)
    run = await dream.process_user(owner.id, now=NOW)
    assert run.error == "memory_too_large" and not gate.calls and not writer.calls


async def test_simultaneous_runs_share_one_gate_and_update(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, memory, gate, _ = service(pg_engine)
    await asyncio.gather(
        dream.process_user(owner.id, now=NOW), dream.process_user(owner.id, now=NOW)
    )
    assert len(gate.calls) == len(memory.writes) == 1


def test_timezone_boundaries_and_dst():
    assert day_cutoff(datetime(2026, 9, 29, 16, tzinfo=UTC), "Asia/Shanghai") == datetime(
        2026, 9, 29, 16, tzinfo=UTC
    )
    # New York's spring transition makes this day 23 hours, its autumn transition 25.
    spring = datetime(2026, 3, 8, 5, tzinfo=UTC)
    autumn = datetime(2026, 11, 1, 4, tzinfo=UTC)
    assert next_midnight(spring, "America/New_York") - spring == timedelta(hours=23)
    assert next_midnight(autumn, "America/New_York") - autumn == timedelta(hours=25)


@pytest.mark.parametrize(
    "proposal",
    [
        {"edits": [], "append": "invented", "append_source_ids": ["unknown"]},
        {
            "edits": [{"old": "missing", "new": "x", "source_ids": ["m"]}],
            "append": "",
            "append_source_ids": [],
        },
        {"edits": [], "append": "x", "append_source_ids": []},
        {"edits": [], "append": "", "append_source_ids": [], "path": "SOUL.md"},
    ],
)
def test_invalid_proposals_cannot_mutate_memory(proposal):
    result = ProviderResult(
        content=[{"type": "tool_use", "name": "propose_memory_update", "input": proposal}],
        fingerprint="mock",
    )
    with pytest.raises(ValueError, match="invalid_proposal"):
        apply_proposal(result, "Original memory", [{"id": "m"}])


async def test_dream_api_history_and_restore_are_owner_scoped(pg_engine, user_client, test_app):
    async with AsyncSession(pg_engine) as db:
        owner = await db.scalar(select(User).where(User.email == "user@test.com"))
    other = await user(pg_engine)
    await conversation(pg_engine, owner)
    await conversation(pg_engine, other)
    dream, _, _, _ = service(pg_engine)
    own_run = await dream.process_user(owner.id, now=NOW)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        other_run = DreamRun(user_id=other.id, status="skipped", source=[], started_at=NOW)
        db.add(other_run)
        await db.commit()
    test_app.dependency_overrides[get_dream_service] = lambda: dream
    response = await user_client.get("/api/dream")
    assert response.status_code == 200
    body = response.json()
    assert body["availability"]["state"] == "not_configured"
    assert [item["id"] for item in body["items"]] == [str(own_run.id)]
    assert "source" not in body["items"][0] and "before" not in body["items"][0]
    detail = await user_client.get(f"/api/dream/{own_run.id}")
    assert detail.json()["after"] == own_run.after
    assert (await user_client.get(f"/api/dream/{other_run.id}")).status_code == 404
    assert (await user_client.post(f"/api/dream/{other_run.id}/restore")).status_code == 404
    assert (await user_client.post(f"/api/dream/{own_run.id}/restore")).status_code == 200


async def test_dream_requires_auth(async_client):
    assert (await async_client.get("/api/dream")).status_code == 401
    assert (await async_client.get(f"/api/dream/{uuid4()}")).status_code == 401
    assert (await async_client.post(f"/api/dream/{uuid4()}/restore")).status_code == 401


@pytest.mark.skipif(
    os.environ.get("RUN_RUSTFS_INTEGRATION") != "1",
    reason="requires configured RustFS",
)
async def test_real_storage_dream_update_restore_and_manual_edit_fence(pg_engine):
    from openctopus_server.config import get_settings
    from openctopus_server.workspace.fs import WorkspaceFS, WorkspaceTarget
    from openctopus_server.workspace.service import WorkspaceService
    from openctopus_server.workspace.storage import build_object_storage

    owner = await user(pg_engine)
    storage = build_object_storage(get_settings())
    fs = WorkspaceFS(storage)
    workspace = WorkspaceService(fs)
    gate, writer = Gate(), Writer()
    dream = DreamService(engine=pg_engine, workspace=workspace, jev=gate, writer=writer)
    try:
        async with AsyncSession(pg_engine) as db:
            await workspace.write(
                db, user_id=owner.id, path="MEMORY.md", data=b"Existing memory.\n"
            )
        await conversation(pg_engine, owner)
        run = await dream.process_user(owner.id, now=NOW)
        assert run.status == "updated" and run.after_etag != run.before_etag
        actual = await fs.read_with_metadata(WorkspaceTarget.personal(owner.id), "MEMORY.md")
        assert actual.data == run.after.encode() and actual.etag == run.after_etag
        restored = await dream.restore(owner.id, run.id)
        assert restored.status == "restored"
        assert (
            await fs.read(WorkspaceTarget.personal(owner.id), "MEMORY.md") == b"Existing memory.\n"
        )
        await conversation(pg_engine, owner, content="My next durable preference")

        async def manual_edit():
            async with AsyncSession(pg_engine) as db:
                await workspace.write(
                    db, user_id=owner.id, path="MEMORY.md", data=b"New manual edit.\n"
                )

        writer.before_return = manual_edit
        conflicted = await dream.process_user(owner.id, now=NOW)
        assert conflicted.error == "workspace_file_changed"
        assert (
            await fs.read(WorkspaceTarget.personal(owner.id), "MEMORY.md") == b"New manual edit.\n"
        )
    finally:
        await fs.purge_workspace(WorkspaceTarget.personal(owner.id))
        await storage.close()


async def test_old_unprepared_run_failure_waits_an_hour_from_failure(pg_engine):
    owner = await user(pg_engine)
    await conversation(pg_engine, owner)
    dream, _, gate, _ = service(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        source = await dream._source(db, owner, NOW)
        pending = DreamRun(
            user_id=owner.id, status="pending", source=source, started_at=NOW - timedelta(days=1)
        )
        db.add(pending)
        await db.commit()
    gate.failure = JevError("unreachable")
    failed = await dream.process_user(owner.id, now=NOW)
    assert failed.id == pending.id and failed.finished_at == NOW
    assert await dream.process_user(owner.id, now=NOW + timedelta(minutes=1)) is None
    assert len(gate.calls) == 1
