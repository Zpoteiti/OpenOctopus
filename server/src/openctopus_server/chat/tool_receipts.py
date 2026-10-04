"""Persist dispatch intent before OO tools can cause an external side effect."""

import hashlib
import json
from typing import Any
from uuid import UUID, uuid5

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.chat.types import TurnStart
from openctopus_server.db.models import Message, ToolOperation
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import ToolResult


async def claim_tool(
    engine: AsyncEngine, turn: TurnStart, call: dict[str, Any],
) -> tuple[ToolResult, UUID | None] | None:
    operation_id = uuid5(turn.turn_id, f"tool:{call['id']}")
    arguments_hash = hashlib.sha256(json.dumps(
        {"name": call["name"], "args": call["input"]}, sort_keys=True,
    ).encode()).hexdigest()
    async with AsyncSession(engine, expire_on_commit=False) as db:
        previous = await db.get(ToolOperation, operation_id)
        if previous is not None and previous.arguments_hash != arguments_hash:
            raise RuntimeError("A replayed tool call changed its arguments")
        saved = await db.get(Message, operation_id)
        if saved is not None:
            block = saved.content[0]
            return ToolResult(
                block["content"], is_error=block.get("is_error", False),
                code=ErrorCode(block["code"]) if block.get("code") else None,
            ), saved.id
        claimed = await db.scalar(insert(ToolOperation).values(
            id=operation_id, session_id=turn.session_id, arguments_hash=arguments_hash,
        ).on_conflict_do_nothing().returning(ToolOperation.id))
        await db.commit()
    if claimed is not None:
        return None
    return ToolResult(
        "The server restarted after this operation was accepted but before its result was saved. "
        "The external operation may have completed. Check its current state before deciding what to do next.",
        is_error=True, code=ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
    ), None
