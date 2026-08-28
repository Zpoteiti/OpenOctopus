from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.compaction import (
    StaleCompactionSelectionError,
    commit_stage_one,
    commit_stage_two,
    compaction_max_output_tokens,
    compaction_required,
    stage_one_source_ids,
    stage_two_source_ids,
)
from openctopus_server.chat.runtime_context import build_runtime_block
from openctopus_server.db.models import Message, PendingMessage, Session, SystemConfig, User
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.config import load_provider_config


def _message(
    session_id: UUID,
    *,
    kind: str,
    created_at: datetime,
    compacted: bool = False,
) -> Message:
    return Message(
        id=uuid4(),
        session_id=session_id,
        message_kind=kind,
        content=[{"type": "text", "text": kind}],
        delivery_refs=[],
        llm_fingerprint=None,
        is_compacted=compacted,
        created_at=created_at,
    )


async def _seed_session(db: AsyncSession) -> tuple[User, Session]:
    user = User(
        id=uuid4(),
        email=f"{uuid4()}@test.com",
        password_hash="hash",
        name="User",
        is_admin=False,
    )
    session_id = uuid4()
    session = Session(
        id=session_id,
        user_id=user.id,
        session_key=f"web:{session_id}",
        channel="web",
        chat_id=str(session_id),
        title="New chat",
    )
    db.add(user)
    await db.flush()
    db.add(session)
    await db.flush()
    return user, session


def test_compaction_trigger_and_output_budget() -> None:
    assert not compaction_required(
        input_tokens=90,
        max_context_tokens=None,
        threshold_tokens=20,
    )
    assert not compaction_required(
        input_tokens=90,
        max_context_tokens=100,
        threshold_tokens=None,
    )
    assert not compaction_required(
        input_tokens=80,
        max_context_tokens=100,
        threshold_tokens=20,
    )
    assert compaction_required(
        input_tokens=81,
        max_context_tokens=100,
        threshold_tokens=20,
    )
    assert compaction_max_output_tokens(16_000) == 12_000
    assert compaction_max_output_tokens(4001) == 1
    with pytest.raises(ValueError):
        compaction_max_output_tokens(4000)


def test_stage_selection_uses_only_active_rows_and_preserves_latest_humans() -> None:
    session_id = uuid4()
    now = datetime.now(UTC)
    old_human = _message(session_id, kind="human", created_at=now)
    old_assistant = _message(
        session_id,
        kind="assistant",
        created_at=now + timedelta(microseconds=1),
    )
    compacted = _message(
        session_id,
        kind="assistant",
        created_at=now + timedelta(microseconds=2),
        compacted=True,
    )
    latest_human_one = _message(
        session_id,
        kind="human",
        created_at=now + timedelta(microseconds=3),
    )
    latest_human_two = _message(
        session_id,
        kind="human",
        created_at=now + timedelta(microseconds=4),
    )
    assistant = _message(
        session_id,
        kind="assistant",
        created_at=now + timedelta(microseconds=5),
    )
    tool_result = _message(
        session_id,
        kind="tool_result",
        created_at=now + timedelta(microseconds=6),
    )
    rows = [
        old_human,
        old_assistant,
        compacted,
        latest_human_one,
        latest_human_two,
        assistant,
        tool_result,
    ]

    assert stage_one_source_ids(rows) == (
        old_human.id,
        old_assistant.id,
        latest_human_one.id,
        latest_human_two.id,
        assistant.id,
        tool_result.id,
    )
    assert stage_two_source_ids(rows) == (assistant.id, tool_result.id)


def test_stage_two_preserves_runtime_human_and_compacts_later_internal_marker() -> None:
    session_id = uuid4()
    user_id = uuid4()
    now = datetime.now(UTC)
    session = Session(
        id=session_id,
        user_id=user_id,
        session_key=f"web:{session_id}",
        channel="web",
        chat_id=str(session_id),
        title="New chat",
    )
    external_human = _message(session_id, kind="human", created_at=now)
    external_human.content = [
        build_runtime_block(timestamp=now.isoformat(), session=session, user_id=user_id),
        {"type": "text", "text": "original request"},
    ]
    first_assistant = _message(
        session_id,
        kind="assistant",
        created_at=now + timedelta(microseconds=1),
    )
    first_result = _message(
        session_id,
        kind="tool_result",
        created_at=now + timedelta(microseconds=2),
    )
    trap_marker = _message(
        session_id,
        kind="human",
        created_at=now + timedelta(microseconds=3),
    )
    trap_marker.content = [{"type": "text", "text": "Repeated-tool warning"}]
    later_assistant = _message(
        session_id,
        kind="assistant",
        created_at=now + timedelta(microseconds=4),
    )

    assert stage_two_source_ids(
        [external_human, first_assistant, first_result, trap_marker, later_assistant]
    ) == (
        first_assistant.id,
        first_result.id,
        trap_marker.id,
        later_assistant.id,
    )


def test_stage_two_falls_back_to_latest_human_without_a_runtime_boundary() -> None:
    session_id = uuid4()
    now = datetime.now(UTC)
    first_human = _message(session_id, kind="human", created_at=now)
    first_assistant = _message(
        session_id,
        kind="assistant",
        created_at=now + timedelta(microseconds=1),
    )
    latest_human = _message(
        session_id,
        kind="human",
        created_at=now + timedelta(microseconds=2),
    )
    latest_assistant = _message(
        session_id,
        kind="assistant",
        created_at=now + timedelta(microseconds=3),
    )

    assert stage_two_source_ids([first_human, first_assistant, latest_human, latest_assistant]) == (
        latest_assistant.id,
    )


async def test_stage_one_commit_promotes_only_the_captured_pending_prefix(pg_engine) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user, session = await _seed_session(db)
        old_human = _message(session.id, kind="human", created_at=now)
        old_human.attachment_refs = [
            {
                "openoctopus_device": "laptop-cn",
                "device_id": str(uuid4()),
                "path": "documents/report.pdf",
            }
        ]
        old_assistant = _message(
            session.id,
            kind="assistant",
            created_at=now + timedelta(microseconds=1),
        )
        already_compacted = _message(
            session.id,
            kind="assistant",
            created_at=now - timedelta(seconds=1),
            compacted=True,
        )
        first_pending = PendingMessage(
            id=uuid4(),
            session_id=session.id,
            user_id=user.id,
            session_key=session.session_key,
            content=[{"type": "text", "text": "first pending"}],
            received_at=now + timedelta(seconds=1),
        )
        later_pending = PendingMessage(
            id=uuid4(),
            session_id=session.id,
            user_id=user.id,
            session_key=session.session_key,
            content=[{"type": "text", "text": "later pending"}],
            effort="high",
            received_at=now + timedelta(seconds=2),
        )
        db.add_all([old_human, old_assistant, already_compacted, first_pending, later_pending])
        await db.commit()

        summary, promoted_ids, latest_effort = await commit_stage_one(
            db,
            session_id=session.id,
            source_ids=(old_human.id, old_assistant.id),
            pending_ids=(first_pending.id,),
            summary_content=[{"type": "text", "text": "summary"}],
        )

        rows = list(
            (
                await db.execute(
                    select(Message)
                    .where(Message.session_id == session.id)
                    .order_by(Message.created_at, Message.id)
                )
            )
            .scalars()
            .all()
        )
        pending_rows = list(
            (
                await db.execute(
                    select(PendingMessage).where(PendingMessage.session_id == session.id)
                )
            )
            .scalars()
            .all()
        )

    assert promoted_ids == (first_pending.id,)
    assert latest_effort is None
    assert [row.id for row in pending_rows] == [later_pending.id]
    assert old_human.is_compacted
    assert old_human.attachment_refs[0]["path"] == "documents/report.pdf"
    assert old_assistant.is_compacted
    assert already_compacted.is_compacted
    assert summary.attachment_refs == []
    active_rows = [row for row in rows if not row.is_compacted]
    assert [row.id for row in active_rows] == [summary.id, first_pending.id]
    assert [row.message_kind for row in active_rows] == [
        "compaction_summary",
        "human",
    ]


async def test_stage_one_commit_rejects_a_stale_active_snapshot(pg_engine) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user, session = await _seed_session(db)
        source = _message(session.id, kind="human", created_at=now)
        added_after_selection = _message(
            session.id,
            kind="assistant",
            created_at=now + timedelta(microseconds=1),
        )
        pending = PendingMessage(
            id=uuid4(),
            session_id=session.id,
            user_id=user.id,
            session_key=session.session_key,
            content=[{"type": "text", "text": "pending"}],
            received_at=now + timedelta(seconds=1),
        )
        db.add_all([source, added_after_selection, pending])
        await db.commit()
        session_id = session.id
        source_id = source.id
        added_id = added_after_selection.id
        pending_id = pending.id

        with pytest.raises(StaleCompactionSelectionError):
            await commit_stage_one(
                db,
                session_id=session_id,
                source_ids=(source_id,),
                pending_ids=(pending_id,),
                summary_content=[{"type": "text", "text": "stale summary"}],
            )

        active_ids = tuple(
            (
                await db.execute(
                    select(Message.id)
                    .where(
                        Message.session_id == session_id,
                        Message.is_compacted.is_(False),
                    )
                    .order_by(Message.created_at, Message.id)
                )
            )
            .scalars()
            .all()
        )

    assert active_ids == (source_id, added_id)


async def test_stage_two_commit_replaces_only_latest_assistant_tool_tail(pg_engine) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        _, session = await _seed_session(db)
        prior_summary = _message(session.id, kind="compaction_summary", created_at=now)
        human_one = _message(
            session.id,
            kind="human",
            created_at=now + timedelta(microseconds=1),
        )
        human_two = _message(
            session.id,
            kind="human",
            created_at=now + timedelta(microseconds=2),
        )
        assistant = _message(
            session.id,
            kind="assistant",
            created_at=now + timedelta(microseconds=3),
        )
        tool_result = _message(
            session.id,
            kind="tool_result",
            created_at=now + timedelta(microseconds=4),
        )
        db.add_all([prior_summary, human_one, human_two, assistant, tool_result])
        await db.commit()

        summary = await commit_stage_two(
            db,
            session_id=session.id,
            source_ids=(assistant.id, tool_result.id),
            summary_content=[{"type": "text", "text": "turn summary"}],
        )

        rows = list(
            (
                await db.execute(
                    select(Message)
                    .where(Message.session_id == session.id)
                    .order_by(Message.created_at, Message.id)
                )
            )
            .scalars()
            .all()
        )

    assert assistant.is_compacted
    assert tool_result.is_compacted
    active_ids = [row.id for row in rows if not row.is_compacted]
    assert active_ids == [prior_summary.id, human_one.id, human_two.id, summary.id]


async def test_stage_two_commit_defers_to_a_pending_user_boundary(pg_engine) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user, session = await _seed_session(db)
        human = _message(session.id, kind="human", created_at=now)
        assistant = _message(
            session.id,
            kind="assistant",
            created_at=now + timedelta(microseconds=1),
        )
        pending = PendingMessage(
            id=uuid4(),
            session_id=session.id,
            user_id=user.id,
            session_key=session.session_key,
            content=[{"type": "text", "text": "new boundary"}],
            received_at=now + timedelta(seconds=1),
        )
        db.add_all([human, assistant, pending])
        await db.commit()

        with pytest.raises(StaleCompactionSelectionError):
            await commit_stage_two(
                db,
                session_id=session.id,
                source_ids=(assistant.id,),
                summary_content=[{"type": "text", "text": "stale summary"}],
            )

        await db.refresh(assistant)

    assert not assistant.is_compacted


async def test_provider_config_loads_and_validates_compaction_threshold(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
                SystemConfig(key="llm_max_context_tokens", value=128_000),
                SystemConfig(key="llm_compaction_threshold_tokens", value=16_000),
            ]
        )
        await db.commit()
        config = await load_provider_config(db)
        assert config.compaction_threshold_tokens == 16_000

        threshold = await db.get(SystemConfig, "llm_compaction_threshold_tokens")
        assert threshold is not None
        threshold.value = 4000
        await db.commit()
        with pytest.raises(ChatError, match="llm_compaction_threshold_tokens is invalid"):
            await load_provider_config(db)

        threshold.value = 128_000
        await db.commit()
        with pytest.raises(ChatError, match="llm_compaction_threshold_tokens is invalid"):
            await load_provider_config(db)

        threshold.value = 16_000
        context = await db.get(SystemConfig, "llm_max_context_tokens")
        assert context is not None
        await db.delete(context)
        await db.commit()
        with pytest.raises(ChatError, match="llm_compaction_threshold_tokens is invalid"):
            await load_provider_config(db)
