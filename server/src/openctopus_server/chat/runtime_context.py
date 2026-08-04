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
    r"sender: partner:(?P<sender>[0-9a-fA-F-]{36})\n"
    r"trust: partner\n"
    r"</runtime>\Z"
)


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    time: str
    channel: str
    chat_id: str
    sender_id: UUID
    trust: str = "partner"


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


def parse_runtime_block(block: dict[str, Any]) -> RuntimeContext | None:
    if block.get("type") != "text" or not isinstance(block.get("text"), str):
        return None
    match = _RUNTIME_RE.fullmatch(block["text"])
    if match is None:
        return None
    try:
        sender_id = UUID(match.group("sender"))
    except ValueError:
        return None
    return RuntimeContext(
        time=match.group("time"),
        channel=match.group("channel"),
        chat_id=match.group("chat_id"),
        sender_id=sender_id,
    )


def runtime_matches_session(block: dict[str, Any], *, session: Session) -> bool:
    runtime = parse_runtime_block(block)
    return (
        runtime is not None
        and runtime.channel == session.channel
        and runtime.chat_id == session.chat_id
    )
