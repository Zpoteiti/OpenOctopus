"""Persist native SDK context without rewriting the user's original transcript."""

from uuid import UUID

from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import AgentContext


async def save_context(
    engine: AsyncEngine, session_id: UUID, messages: list[ModelMessage], through_message_id: UUID, tool_profile: str,
) -> None:
    value = ModelMessagesTypeAdapter.dump_python(messages, mode="json")
    async with AsyncSession(engine) as db:
        statement = insert(AgentContext).values(
            session_id=session_id, messages=value, through_message_id=through_message_id, tool_profile=tool_profile,
        )
        await db.execute(statement.on_conflict_do_update(
            index_elements=[AgentContext.session_id],
            set_={"messages": value, "through_message_id": through_message_id, "tool_profile": tool_profile},
        ))
        await db.commit()
