from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from openctopus_server.provider.wire_types import Effort


@dataclass(frozen=True, slots=True)
class TurnStart:
    session_id: UUID
    turn_id: UUID
    message_ids: tuple[UUID, ...]
    effort: Effort | None


@dataclass(frozen=True, slots=True)
class AcceptedMessage:
    session_id: UUID
    message_id: UUID
    accepted_at: datetime
    disposition: str
    created_session: bool
    turn: TurnStart | None
