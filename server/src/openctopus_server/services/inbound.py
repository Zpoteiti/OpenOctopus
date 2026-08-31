import uuid
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.advisory import lock_uuid_identity
from openctopus_server.db.models import User
from openctopus_server.provider.wire_types import Effort


@dataclass(frozen=True, slots=True)
class InboundMessage:
    message_id: UUID
    owner_user_id: UUID
    session_id: UUID
    session_key: str
    channel: str
    chat_id: str
    content: tuple[dict[str, Any], ...]
    attachment_refs: tuple[dict[str, Any], ...] = ()
    effort: Effort | None = None


def web_inbound(
    *,
    owner_user_id: UUID,
    session_id: UUID,
    content: list[dict[str, Any]],
    attachment_refs: list[dict[str, Any]] | None,
    effort: Effort | None,
) -> InboundMessage:
    return _inbound(
        owner_user_id=owner_user_id,
        session_id=session_id,
        channel="web",
        content=content,
        attachment_refs=attachment_refs,
        effort=effort,
    )


def cron_inbound(
    *,
    owner_user_id: UUID,
    job_id: UUID,
    content: list[dict[str, Any]],
) -> InboundMessage:
    return _inbound(
        owner_user_id=owner_user_id,
        session_id=job_id,
        channel="cron",
        content=content,
    )


def heartbeat_inbound(
    *,
    owner_user_id: UUID,
    content: list[dict[str, Any]],
) -> InboundMessage:
    return _inbound(
        owner_user_id=owner_user_id,
        session_id=owner_user_id,
        channel="heartbeat",
        content=content,
    )


async def lock_inbound_identity(
    db: AsyncSession,
    inbound: InboundMessage,
) -> User | None:
    await lock_uuid_identity(db, inbound.session_id)
    result = await db.execute(
        select(User)
        .where(User.id == inbound.owner_user_id)
        .with_for_update(read=True, key_share=True)
    )
    return result.scalar_one_or_none()


def _inbound(
    *,
    owner_user_id: UUID,
    session_id: UUID,
    channel: str,
    content: list[dict[str, Any]],
    attachment_refs: list[dict[str, Any]] | None = None,
    effort: Effort | None = None,
) -> InboundMessage:
    route = f"{channel}:{session_id}"
    return InboundMessage(
        message_id=uuid.uuid4(),
        owner_user_id=owner_user_id,
        session_id=session_id,
        session_key=route,
        channel=channel,
        chat_id=str(session_id),
        content=tuple(dict(block) for block in content),
        attachment_refs=tuple(dict(ref) for ref in (attachment_refs or [])),
        effort=effort,
    )
