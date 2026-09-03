import uuid
from dataclasses import asdict
from typing import Any, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.channels.types import (
    ChannelName,
    InboundMessage,
    InboundSender,
    SenderClassification,
)
from openctopus_server.db.advisory import lock_uuid_identity
from openctopus_server.db.models import User
from openctopus_server.provider.wire_types import Effort


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
    message_id = uuid.uuid4()
    classification = "internal" if channel in {"cron", "heartbeat"} else "owner"
    return InboundMessage(
        message_id=message_id,
        owner_user_id=owner_user_id,
        session_id=session_id,
        session_key=route,
        channel=cast(ChannelName, channel),
        chat_id=str(session_id),
        source_message_id=str(message_id),
        channel_binding_generation=None,
        sender=InboundSender(
            id=str(owner_user_id),
            display_name=None,
            classification=cast(SenderClassification, classification),
        ),
        ingress_tool_profile="owner_full",
        content=tuple(dict(block) for block in content),
        attachment_refs=tuple(dict(ref) for ref in (attachment_refs or [])),
        channel_context=(),
        effort=effort,
    )


def serialize_channel_context(inbound: InboundMessage) -> list[dict[str, object]]:
    return [asdict(item) for item in inbound.channel_context]
