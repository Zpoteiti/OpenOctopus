from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.prompt import build_system_prompt
from openctopus_server.db.models import Message, Session, User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.anthropic import provider_fingerprint
from openctopus_server.provider.config import ProviderConfig


async def build_provider_context(
    db: AsyncSession,
    *,
    session_id: UUID,
    config: ProviderConfig,
) -> tuple[str, list[dict[str, Any]]]:
    session = await db.get(Session, session_id)
    if session is None:
        raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
    user = await db.get(User, session.user_id)
    if user is None:
        raise ChatError(ErrorCode.NOT_FOUND, "Session owner not found")

    rows = (
        (
            await db.execute(
                select(Message)
                .where(Message.session_id == session_id)
                .order_by(Message.created_at, Message.id)
            )
        )
        .scalars()
        .all()
    )
    current_fingerprint = provider_fingerprint(config)
    projected: list[dict[str, Any]] = []
    for row in rows:
        content = [dict(block) for block in row.content]
        if row.role == "assistant" and row.llm_fingerprint != current_fingerprint:
            content = [
                block
                for block in content
                if block.get("type") not in {"thinking", "redacted_thinking"}
            ]
        if not content:
            continue
        if projected and projected[-1]["role"] == row.role:
            projected[-1]["content"].extend(content)
        else:
            projected.append({"role": row.role, "content": content})

    system = await build_system_prompt(db, session=session, user=user)
    return system, projected
