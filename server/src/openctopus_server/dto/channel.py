from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from openctopus_server.channels.types import ExternalChannel

type ChannelState = Literal[
    "stopped",
    "connecting",
    "awaiting_pairing",
    "ready",
    "degraded",
]

ChannelCredential = Annotated[str, Field(min_length=1)]
ChannelUserId = Annotated[str, Field(min_length=1, max_length=256)]


class ChannelConfigPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    bot_token: ChannelCredential | None = None
    client_id: ChannelCredential | None = None
    client_secret: ChannelCredential | None = None
    allow_list: Annotated[list[ChannelUserId], Field(max_length=256)] | None = None

    @model_validator(mode="after")
    def require_non_null_patch(self) -> ChannelConfigPatchRequest:
        if not self.model_fields_set or any(
            getattr(self, field) is None for field in self.model_fields_set
        ):
            raise ValueError("channel patch fields must be non-null")
        return self


class ChannelBotResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    id: str
    name: str | None
    avatar_url: str | None


class ChannelOwnerResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    id: str
    dm_chat_id: str


class ChannelPairingResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    expires_at: datetime
    code: str | None


class ChannelLastErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    code: str
    message: str = Field(max_length=512)
    at: datetime


class ChannelConfigResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    channel: ExternalChannel
    configured: bool
    state: ChannelState
    bot: ChannelBotResponse | None
    owner: ChannelOwnerResponse | None
    allow_list: list[str]
    credential_hint: Literal["Configured"] | None
    pairing: ChannelPairingResponse | None
    last_error: ChannelLastErrorResponse | None
