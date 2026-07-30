import base64
import binascii
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..provider.wire_types import (
    ContentBlock,
    Effort,
    ImageBlock,
    TextBlock,
)

UserContentBlock = Annotated[
    TextBlock | ImageBlock,
    Field(discriminator="type"),
]


class PostMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    effort: Effort | None = None
    content: list[UserContentBlock] = Field(min_length=1)
    attachments: list[dict[str, Any]] = Field(max_length=0)

    @model_validator(mode="after")
    def validate_user_content(self) -> "PostMessageRequest":
        for block in self.content:
            if isinstance(block, TextBlock):
                if not block.text.strip():
                    raise ValueError("text blocks must not be empty")
                continue
            try:
                decoded = base64.b64decode(block.source.data, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ValueError("image data must be valid base64") from exc
            if not decoded:
                raise ValueError("image data must not be empty")
        return self


class MessageResponse(BaseModel):
    id: UUID
    session_id: UUID
    role: str
    message_kind: str
    content: list[ContentBlock]
    delivery_refs: list[dict[str, Any]]
    is_compaction_summary: bool
    created_at: datetime


class PendingMessageResponse(BaseModel):
    id: UUID
    session_id: UUID
    content: list[ContentBlock]
    effort: Effort | None
    received_at: datetime


class MessagesResponse(BaseModel):
    messages: list[MessageResponse]
    pending_messages: list[PendingMessageResponse]
    status: str
    active_turn_id: UUID | None
    last_message_id: UUID | None
    pending_count: int
    has_more_before: bool
