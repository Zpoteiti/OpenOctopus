import base64
import binascii
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

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
    id: UUID
    session_id: UUID
    role: str
    message_kind: str
    content: list[ContentBlock]
    delivery_refs: list[DeliveryRefResponse]
    is_compacted: bool
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

    @model_serializer(mode="wrap")
    def serialize_required_nullable_fields(
        self,
        handler: SerializerFunctionWrapHandler,
    ) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        data["active_turn_id"] = self.active_turn_id
        data["last_message_id"] = self.last_message_id
        return data
