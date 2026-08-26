from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.json_schema import SkipJsonSchema


class SessionPatchRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "anyOf": [
                {"required": ["title"]},
                {"required": ["read_through_message_id"]},
            ]
        },
    )

    title: Annotated[str, Field(min_length=1, max_length=120)] | SkipJsonSchema[
        None
    ] = None
    read_through_message_id: UUID | SkipJsonSchema[None] = None

    @model_validator(mode="after")
    def require_non_null_update(self) -> "SessionPatchRequest":
        fields = self.model_fields_set
        if not fields:
            raise ValueError("at least one session field is required")
        if "title" in fields and self.title is None:
            raise ValueError("title must not be null")
        if "read_through_message_id" in fields and self.read_through_message_id is None:
            raise ValueError("read_through_message_id must not be null")
        return self


class SessionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    user_id: UUID
    session_key: str
    channel: str
    chat_id: str
    title: str
    last_inbound_at: datetime | None
    unread: bool
    cancel_requested: bool
    created_at: datetime
