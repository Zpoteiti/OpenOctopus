from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from ..provider.wire_types import ContentBlock


class PostMessageRequest(BaseModel):
    content: str


class MessageResponse(BaseModel):
    id: UUID
    role: str
    message_kind: str
    content: list[ContentBlock]
    created_at: datetime
