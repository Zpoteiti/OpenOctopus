from typing import Any

from openctopus_server.chat.attachments import strip_provider_attachment_markers
from openctopus_server.chat.runtime_context import build_runtime_block, runtime_matches_session
from openctopus_server.db.models import Message, PendingMessage, Session
from openctopus_server.dto.message import (
    ChannelContextResponse,
    ChannelDeliveryResponse,
    MessageResponse,
    MessageSenderResponse,
    PendingMessageResponse,
)

__all__ = ["build_runtime_block", "message_response", "pending_response", "public_content"]


def public_content(
    content: list[dict[str, Any]],
    *,
    session: Session,
    human: bool,
) -> list[dict[str, Any]]:
    projected = [dict(block) for block in content]
    if human and projected and runtime_matches_session(projected[0], session=session):
        projected = projected[1:]
    for block in projected:
        if block.get("type") == "thinking":
            block.pop("signature", None)
        elif block.get("type") == "redacted_thinking":
            block.pop("data", None)
    return projected


def message_response(
    message: Message,
    *,
    session: Session,
    deliveries: list[ChannelDeliveryResponse] | None = None,
) -> MessageResponse:
    attachment_refs = [dict(item) for item in (message.attachment_refs or [])]
    return MessageResponse(
        id=message.id,
        session_id=message.session_id,
        role=provider_role(message.message_kind),
        message_kind=message.message_kind,
        content=public_content(
            strip_provider_attachment_markers(message.content, attachment_refs),
            session=session,
            human=message.message_kind == "human",
        ),
        attachment_refs=attachment_refs,
        delivery_refs=[dict(item) for item in message.delivery_refs],
        sender=_sender_response(message) if message.message_kind == "human" else None,
        source_message_id=message.source_message_id,
        channel_context=_channel_context_response(message.channel_context),
        deliveries=deliveries or [],
        is_compacted=message.is_compacted,
        created_at=message.created_at,
    )


def pending_response(
    pending: PendingMessage,
    *,
    session: Session,
) -> PendingMessageResponse:
    attachment_refs = [dict(item) for item in (pending.attachment_refs or [])]
    return PendingMessageResponse(
        id=pending.id,
        session_id=pending.session_id,
        content=public_content(
            strip_provider_attachment_markers(pending.content, attachment_refs),
            session=session,
            human=True,
        ),
        attachment_refs=attachment_refs,
        sender=_sender_response(pending),
        source_message_id=pending.source_message_id,
        channel_context=_channel_context_response(pending.channel_context),
        effort=pending.effort,
        received_at=pending.received_at,
    )


def provider_role(message_kind: str) -> str:
    if message_kind in {"human", "tool_result", "synthetic_tool_result"}:
        return "user"
    if message_kind in {
        "assistant",
        "synthetic_assistant_error",
        "compaction_summary",
    }:
        return "assistant"
    raise ValueError(f"Unsupported message kind: {message_kind}")


def _sender_response(message: Message | PendingMessage) -> MessageSenderResponse:
    if message.sender_id is None or message.sender_classification is None:
        raise RuntimeError("Human message sender authority is missing")
    return MessageSenderResponse(
        id=message.sender_id,
        display_name=message.sender_display_name,
        classification=message.sender_classification,
    )


def _channel_context_response(
    raw_entries: list[dict[str, Any]] | None,
) -> ChannelContextResponse:
    entries: list[dict[str, Any]] = []
    omitted_count = 0
    for raw in raw_entries or []:
        marker_count = raw.get("_openoctopus_omitted_count")
        if isinstance(marker_count, int) and marker_count >= 0:
            omitted_count += marker_count
            continue
        text = raw.get("text")
        if not isinstance(text, str):
            continue
        summaries = raw.get("attachment_summaries")
        entries.append(
            {
                "source_message_id": _optional_string(raw.get("source_message_id")),
                "sender_id": _optional_string(raw.get("sender_id")),
                "sender_display_name": _optional_string(raw.get("sender_display_name")),
                "sent_at": _optional_string(raw.get("sent_at")),
                "text": text,
                "attachment_summaries": [
                    value
                    for value in (summaries if isinstance(summaries, list) else [])
                    if isinstance(value, str)
                ],
            }
        )
    return ChannelContextResponse(
        entries=entries,
        included_count=len(entries),
        omitted_count=omitted_count,
    )


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None
