import base64
import binascii
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from ..provider.wire_types import (
    ContentBlock,
    Effort,
    ImageBlock,
    TextBlock,
)

type UserContentBlock = Annotated[
    TextBlock | ImageBlock,
    Field(discriminator="type"),
]


class MessageAttachmentRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    openoctopus_device: Literal["server"]
    path: str = Field(min_length=1, max_length=4096)


class PostMessageRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "anyOf": [
                {"properties": {"content": {"minItems": 1}}},
                {"properties": {"attachments": {"minItems": 1}}},
            ]
        },
    )

    effort: Effort | None = None
    content: list[UserContentBlock]
    attachments: list[MessageAttachmentRef] = Field(max_length=10)

    @model_validator(mode="after")
    def validate_user_content(self) -> "PostMessageRequest":
        if not self.content and not self.attachments:
            raise ValueError("content and attachments must not both be empty")
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


class WorkspaceFileDeliveryRefResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool_use_id: str = Field(min_length=1)
    type: Literal["workspace_file"]
    openoctopus_device: Literal["server"]
    path: str = Field(min_length=1, max_length=4096)
    workspace_id: UUID
    workspace_relative_path: str = Field(min_length=1, max_length=4096)
    filename: str = Field(min_length=1, max_length=4096)
    mime: str = Field(min_length=1, max_length=255)
    size: int = Field(ge=0)
    online_only: Literal[False]


class DeviceFileDeliveryRefResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool_use_id: str = Field(min_length=1)
    type: Literal["device_file"]
    device_id: UUID
    openoctopus_device: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$",
    )
    path: str = Field(min_length=1, max_length=4096)
    filename: str = Field(min_length=1, max_length=4096)
    mime: str = Field(min_length=1, max_length=255)
    size: int | None = Field(default=None, ge=0)
    online_only: Literal[True]

    @field_validator("openoctopus_device")
    @classmethod
    def reject_server(cls, value: str) -> str:
        if value == "server":
            raise ValueError("device_file cannot target the server workspace")
        return value


type DeliveryRefResponse = Annotated[
    WorkspaceFileDeliveryRefResponse | DeviceFileDeliveryRefResponse,
    Field(discriminator="type"),
]


class MessageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    session_id: UUID
    role: Literal["user", "assistant"]
    message_kind: Literal[
        "human",
        "assistant",
        "tool_result",
        "synthetic_tool_result",
        "synthetic_assistant_error",
        "compaction_summary",
    ]
    content: list[ContentBlock]
    delivery_refs: list[DeliveryRefResponse]
    is_compacted: bool
    created_at: datetime


class PendingMessageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    session_id: UUID
    content: list[ContentBlock]
    effort: Effort | None
    received_at: datetime


class MessagesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    messages: list[MessageResponse]
    pending_messages: list[PendingMessageResponse]
    status: Literal["idle", "running", "failed", "abandoned"]
    active_turn_id: UUID | None
    last_message_id: UUID | None
    pending_count: int
    has_more_before: bool
