from typing import Any

from openctopus_server.chat.attachments import strip_provider_attachment_markers
from openctopus_server.chat.runtime_context import build_runtime_block, runtime_matches_session
from openctopus_server.db.models import Message, PendingMessage, Session
from openctopus_server.dto.message import MessageResponse, PendingMessageResponse

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


def message_response(message: Message, *, session: Session) -> MessageResponse:
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
