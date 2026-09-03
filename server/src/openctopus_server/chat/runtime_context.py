import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from openctopus_server.db.models import Session

_RUNTIME_RE = re.compile(
    r"\A<runtime>\n"
    r"time: (?P<time>[^\r\n]+)\n"
    r"channel: (?P<channel>[a-z0-9_-]+)\n"
    r"chat_id: (?P<chat_id>[^\r\n]+)\n"
    r"sender: (?P<sender_namespace>[a-z0-9_-]+):(?P<sender>[^\r\n]+)\n"
    r"trust: (?P<trust>owner|allowed_non_owner|internal)\n"
    r"</runtime>\Z"
)


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    time: str
    channel: str
    chat_id: str
    sender_namespace: str
    sender_id: str
    trust: str


def build_runtime_block(
    *,
    timestamp: str,
    session: Session,
    user_id: UUID | None = None,
    sender_id: str | None = None,
    trust: str = "owner",
) -> dict[str, str]:
    resolved_sender_id = sender_id or (str(user_id) if user_id is not None else None)
    if resolved_sender_id is None or "\n" in resolved_sender_id or "\r" in resolved_sender_id:
        raise ValueError("Runtime sender ID is invalid")
    if trust not in {"owner", "allowed_non_owner", "internal"}:
        raise ValueError("Runtime trust classification is invalid")
    return {
        "type": "text",
        "text": (
            "<runtime>\n"
            f"time: {timestamp}\n"
            f"channel: {session.channel}\n"
            f"chat_id: {session.chat_id}\n"
            f"sender: {session.channel}:{resolved_sender_id}\n"
            f"trust: {trust}\n"
            "</runtime>"
        ),
    }


def parse_runtime_block(block: dict[str, Any]) -> RuntimeContext | None:
    if block.get("type") != "text" or not isinstance(block.get("text"), str):
        return None
    match = _RUNTIME_RE.fullmatch(block["text"])
    if match is None:
        return None
    return RuntimeContext(
        time=match.group("time"),
        channel=match.group("channel"),
        chat_id=match.group("chat_id"),
        sender_namespace=match.group("sender_namespace"),
        sender_id=match.group("sender"),
        trust=match.group("trust"),
    )


def runtime_matches_session(block: dict[str, Any], *, session: Session) -> bool:
    runtime = parse_runtime_block(block)
    return (
        runtime is not None
        and runtime.channel == session.channel
        and runtime.chat_id == session.chat_id
        and runtime.sender_namespace == session.channel
    )
