import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import Message
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError

_RESTART_TEXT = (
    "[server restart: tool was not executed because the OpenOctopus server "
    "restarted before completing this tool batch]"
)


async def repair_unpaired_tool_uses(
    db: AsyncSession,
    *,
    session_id: UUID,
) -> list[Message]:
    try:
        await _advisory_lock(db, session_id)
        rows = list(
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
        missing = _tail_missing_tool_uses(rows)
        if not missing:
            await db.commit()
            return []

        created_at = max(
            datetime.now(UTC),
            rows[-1].created_at + timedelta(microseconds=1),
        )
        repaired: list[Message] = []
        for index, tool_id in enumerate(missing):
            message = Message(
                id=uuid.uuid4(),
                session_id=session_id,
                message_kind="synthetic_tool_result",
                content=[synthetic_tool_result(tool_id, code="server_restart", text=_RESTART_TEXT)],
                delivery_refs=[],
                is_compacted=False,
                created_at=created_at + timedelta(microseconds=index),
            )
            db.add(message)
            repaired.append(message)
        await db.commit()
        return repaired
    except Exception:
        await db.rollback()
        raise


def synthetic_tool_result(tool_id: str, *, code: str, text: str) -> dict[str, Any]:
    return {
        "type": "tool_result",
        "tool_use_id": tool_id,
        "content": [{"type": "text", "text": text}],
        "is_error": True,
        "code": code,
    }


def _tail_missing_tool_uses(rows: list[Message]) -> list[str]:
    unresolved: list[str] = []
    for row in rows:
        if row.message_kind == "assistant":
            if unresolved:
                _invalid_history()
            unresolved = [
                block["id"]
                for block in row.content
                if block.get("type") == "tool_use" and isinstance(block.get("id"), str)
            ]
            continue
        if row.message_kind in {"tool_result", "synthetic_tool_result"}:
            if not row.content:
                _invalid_history()
            if not unresolved:
                _invalid_history()
            for block in row.content:
                tool_id = block.get("tool_use_id")
                if (
                    block.get("type") != "tool_result"
                    or not isinstance(tool_id, str)
                    or tool_id not in unresolved
                ):
                    _invalid_history()
                unresolved.remove(tool_id)
            continue
        if unresolved:
            _invalid_history()
    return unresolved


def _invalid_history() -> NoReturn:
    raise ChatError(
        ErrorCode.PROVIDER_PROTOCOL_ERROR,
        "Invalid persisted history: incomplete tool batch is not at the active tail",
    )


async def _advisory_lock(db: AsyncSession, session_id: UUID) -> None:
    key = session_id.int & ((1 << 63) - 1)
    await db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
