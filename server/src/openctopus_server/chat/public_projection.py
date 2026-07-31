import re
from typing import Any
from uuid import UUID

from openctopus_server.db.models import Message, PendingMessage, Session
from openctopus_server.dto.message import MessageResponse, PendingMessageResponse

_RUNTIME_RE = re.compile(
    r"\A<runtime>\n"
    r"time: (?P<time>[^\r\n]+)\n"
    r"channel: (?P<channel>[a-z0-9_-]+)\n"
    r"chat_id: (?P<chat_id>[^\r\n]+)\n"
    r"sender: partner:(?P<sender>[0-9a-fA-F-]{36})\n"
    r"trust: partner\n"
    r"</runtime>\Z"
)


def build_runtime_block(
    *,
    timestamp: str,
    session: Session,
    user_id: UUID,
) -> dict[str, str]:
    return {
        "type": "text",
        "text": (
            "<runtime>\n"
            f"time: {timestamp}\n"
            f"channel: {session.channel}\n"
            f"chat_id: {session.chat_id}\n"
            f"sender: partner:{user_id}\n"
            "trust: partner\n"
            "</runtime>"
        ),
    }


def public_content(
    content: list[dict[str, Any]],
    *,
    session: Session,
    human: bool,
) -> list[dict[str, Any]]:
    projected = [dict(block) for block in content]
    if human and projected and _is_matching_runtime(projected[0], session=session):
        projected = projected[1:]
    for block in projected:
        if block.get("type") == "thinking":
            block.pop("signature", None)
        elif block.get("type") == "redacted_thinking":
            block.pop("data", None)
    return projected


def message_response(message: Message, *, session: Session) -> MessageResponse:
    return MessageResponse(
        id=message.id,
        session_id=message.session_id,
        role=message.role,
        message_kind=message.message_kind,
        content=public_content(
            message.content,
            session=session,
            human=message.message_kind == "human",
        ),
        delivery_refs=[dict(item) for item in message.delivery_refs],
        is_compaction_summary=message.is_compaction_summary,
        created_at=message.created_at,
    )


def pending_response(
    pending: PendingMessage,
    *,
    session: Session,
) -> PendingMessageResponse:
    return PendingMessageResponse(
        id=pending.id,
        session_id=pending.session_id,
        content=public_content(pending.content, session=session, human=True),
        effort=pending.effort,
        received_at=pending.received_at,
    )


def _is_matching_runtime(block: dict[str, Any], *, session: Session) -> bool:
    if block.get("type") != "text" or not isinstance(block.get("text"), str):
        return False
    match = _RUNTIME_RE.fullmatch(block["text"])
    if match is None:
        return False
    return match.group("channel") == session.channel and match.group("chat_id") == session.chat_id
