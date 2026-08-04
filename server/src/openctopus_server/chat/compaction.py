import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.runtime_context import parse_runtime_block
from openctopus_server.db.models import Message, PendingMessage

_SUMMARY_HEADROOM_TOKENS = 4000


class StaleCompactionSelectionError(RuntimeError):
    """The transcript changed after summary input was selected."""


def compaction_required(
    *,
    input_tokens: int,
    max_context_tokens: int | None,
    threshold_tokens: int | None,
) -> bool:
    if input_tokens < 0:
        raise ValueError("input_tokens must not be negative")
    if max_context_tokens is None or threshold_tokens is None:
        return False
    return max_context_tokens - input_tokens < threshold_tokens


def compaction_max_output_tokens(threshold_tokens: int) -> int:
    max_output_tokens = threshold_tokens - _SUMMARY_HEADROOM_TOKENS
    if max_output_tokens < 1:
        raise ValueError("compaction threshold must reserve at least one output token")
    return max_output_tokens


def stage_one_source_ids(rows: Sequence[Message]) -> tuple[UUID, ...]:
    """Return all active rows from an already canonically ordered transcript."""
    return tuple(row.id for row in rows if not row.is_compacted)


def stage_two_source_ids(rows: Sequence[Message]) -> tuple[UUID, ...]:
    """Return the active tail after the latest external human boundary."""
    active_rows = [row for row in rows if not row.is_compacted]
    latest_human_index: int | None = None
    latest_runtime_human_index: int | None = None
    for index, row in enumerate(active_rows):
        if row.message_kind == "human":
            latest_human_index = index
            if row.content and parse_runtime_block(row.content[0]) is not None:
                latest_runtime_human_index = index
    boundary_index = (
        latest_runtime_human_index if latest_runtime_human_index is not None else latest_human_index
    )
    if boundary_index is None:
        return ()
    return tuple(row.id for row in active_rows[boundary_index + 1 :])


async def commit_stage_one(
    db: AsyncSession,
    *,
    session_id: UUID,
    source_ids: Sequence[UUID],
    pending_ids: Sequence[UUID],
    summary_content: list[dict[str, Any]],
) -> tuple[Message, tuple[UUID, ...], str | None]:
    """Atomically replace active history and promote the captured pending prefix."""
    captured_sources = tuple(source_ids)
    captured_pending = tuple(pending_ids)
    if not captured_sources:
        raise ValueError("Stage 1 requires at least one active source row")
    if not captured_pending:
        raise ValueError("Stage 1 requires a pending user boundary")
    if not summary_content:
        raise ValueError("Compaction summary content must not be empty")

    try:
        await _advisory_lock(db, session_id)
        active_rows = await _locked_active_rows(db, session_id)
        if stage_one_source_ids(active_rows) != captured_sources:
            raise StaleCompactionSelectionError("Stage 1 active rows changed")

        pending_rows = await _locked_pending_rows(db, session_id)
        current_pending_ids = tuple(row.id for row in pending_rows)
        if current_pending_ids[: len(captured_pending)] != captured_pending:
            raise StaleCompactionSelectionError("Stage 1 pending boundary changed")

        for row in active_rows:
            row.is_compacted = True

        summary_created_at = _after_rows(active_rows)
        summary = Message(
            id=uuid.uuid4(),
            session_id=session_id,
            message_kind="compaction_summary",
            content=[dict(block) for block in summary_content],
            delivery_refs=[],
            llm_fingerprint=None,
            is_compacted=False,
            created_at=summary_created_at,
        )
        db.add(summary)

        captured_pending_rows = pending_rows[: len(captured_pending)]
        promoted_ids: list[UUID] = []
        for index, pending in enumerate(captured_pending_rows, start=1):
            db.add(
                Message(
                    id=pending.id,
                    session_id=session_id,
                    message_kind="human",
                    content=[dict(block) for block in pending.content],
                    delivery_refs=[],
                    llm_fingerprint=None,
                    is_compacted=False,
                    created_at=summary_created_at + timedelta(microseconds=index),
                )
            )
            promoted_ids.append(pending.id)
            await db.delete(pending)

        latest_effort = captured_pending_rows[-1].effort
        await db.commit()
        return summary, tuple(promoted_ids), latest_effort
    except Exception:
        await db.rollback()
        raise


async def commit_stage_two(
    db: AsyncSession,
    *,
    session_id: UUID,
    source_ids: Sequence[UUID],
    summary_content: list[dict[str, Any]],
) -> Message:
    """Atomically replace the selected active tail after the latest external human."""
    captured_sources = tuple(source_ids)
    if not captured_sources:
        raise ValueError("Stage 2 requires at least one active source row")
    if not summary_content:
        raise ValueError("Compaction summary content must not be empty")

    try:
        await _advisory_lock(db, session_id)
        active_rows = await _locked_active_rows(db, session_id)
        if stage_two_source_ids(active_rows) != captured_sources:
            raise StaleCompactionSelectionError("Stage 2 active tail changed")
        if await _locked_pending_rows(db, session_id):
            raise StaleCompactionSelectionError("A pending user boundary supersedes Stage 2")

        captured_set = set(captured_sources)
        source_rows = [row for row in active_rows if row.id in captured_set]
        for row in source_rows:
            row.is_compacted = True

        summary = Message(
            id=uuid.uuid4(),
            session_id=session_id,
            message_kind="compaction_summary",
            content=[dict(block) for block in summary_content],
            delivery_refs=[],
            llm_fingerprint=None,
            is_compacted=False,
            created_at=_after_rows(source_rows),
        )
        db.add(summary)
        await db.commit()
        return summary
    except Exception:
        await db.rollback()
        raise


async def _locked_active_rows(db: AsyncSession, session_id: UUID) -> list[Message]:
    return list(
        (
            await db.execute(
                select(Message)
                .where(
                    Message.session_id == session_id,
                    Message.is_compacted.is_(False),
                )
                .order_by(Message.created_at, Message.id)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )


async def _locked_pending_rows(db: AsyncSession, session_id: UUID) -> list[PendingMessage]:
    return list(
        (
            await db.execute(
                select(PendingMessage)
                .where(PendingMessage.session_id == session_id)
                .order_by(PendingMessage.received_at, PendingMessage.id)
                .with_for_update()
            )
        )
        .scalars()
        .all()
    )


async def _advisory_lock(db: AsyncSession, session_id: UUID) -> None:
    key = session_id.int & ((1 << 63) - 1)
    await db.execute(
        text("SELECT pg_advisory_xact_lock(:key)"),
        {"key": key},
    )


def _after_rows(rows: Sequence[Message]) -> datetime:
    now = datetime.now(UTC)
    if not rows:
        return now
    latest = max(row.created_at for row in rows)
    return max(now, latest + timedelta(microseconds=1))
