"""Authorized database adapters for Harness recall and tool-output storage."""

import hashlib

from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, UserPromptPart
from pydantic_ai_harness.step_persistence import RunRecord
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.scope import active_run
from openctopus_server.db.models import Session, ToolOverflow


class ConversationHistory:
    async def list_runs(self) -> list[RunRecord]:
        scope = active_run.get()
        async with AsyncSession(scope.runtime.engine) as db:
            session = await db.scalar(select(Session).where(
                Session.id == scope.turn.session_id, Session.user_id == scope.user_id,
            ))
        if session is None:
            return []
        return [RunRecord(run_id=str(session.id), conversation_id=str(session.id), started_at=session.created_at)]

    async def run_history(self, *, run_id: str) -> list[ModelMessage]:
        scope = active_run.get()
        if run_id != str(scope.turn.session_id):
            return []
        # Extract searchable text inside PostgreSQL. Attachment bytes and opaque
        # thinking never enter the corpus; the SDK bounds ranked search output.
        query = text("""
            SELECT m.id, m.message_kind, b.value->>'text' AS body
            FROM messages m JOIN sessions s ON s.id=m.session_id
            CROSS JOIN LATERAL jsonb_array_elements(m.content) WITH ORDINALITY AS b(value, n)
            WHERE m.session_id=:session AND s.user_id=:owner AND b.value->>'type'='text'
              AND (:owner_full OR (m.message_kind='human' AND m.ingress_tool_profile='message_only'))
            ORDER BY m.created_at, m.id, b.n
        """)
        async with AsyncSession(scope.runtime.engine) as db:
            rows = (await db.execute(query, {"session": scope.turn.session_id, "owner": scope.user_id, "owner_full": scope.turn.tool_profile == "owner_full"})).all()
        result: list[ModelMessage] = []
        for row in rows:
            content = f"[message {row.id}] {row.body}"
            result.append(ModelResponse([TextPart(content)]) if row.message_kind == "assistant"
                          else ModelRequest([UserPromptPart(content)]))
        return result


class ToolOutputStore:
    async def write(self, key: str, data: bytes) -> str:
        scope = active_run.get()
        if len(data) > 16 * 1024 * 1024:
            raise ValueError("Tool output exceeds the 16 MiB storage limit")
        handle = hashlib.sha256(key.encode() + b"\x00" + data).hexdigest()
        async with AsyncSession(scope.runtime.engine) as db:
            owner = await db.scalar(select(Session.user_id).where(Session.id == scope.turn.session_id))
            if owner != scope.user_id:
                raise PermissionError("Conversation no longer available")
            await db.execute(insert(ToolOverflow).values(
                session_id=scope.turn.session_id, handle=handle, data=data,
            ).on_conflict_do_nothing())
            await db.commit()
        return handle

    async def read(self, handle: str) -> bytes:
        scope = active_run.get()
        async with AsyncSession(scope.runtime.engine) as db:
            data = await db.scalar(select(ToolOverflow.data).join(Session).where(
                ToolOverflow.session_id == scope.turn.session_id, Session.user_id == scope.user_id,
                ToolOverflow.handle == handle,
            ))
        if data is None:
            raise FileNotFoundError("Tool result not found in this conversation")
        return data
