from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class SessionResponse(BaseModel):
    id: UUID
    session_key: str
    channel: str
    chat_id: str
    title: str | None
    unread: bool
    created_at: datetime
