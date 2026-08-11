from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field


def _reject_nul(value: str) -> str:
    if "\x00" in value:
        raise ValueError("value must not contain NUL")
    return value

WorkspacePath = Annotated[str, Field(min_length=1, max_length=4096), AfterValidator(_reject_nul)]
SsrfDenyEntry = Annotated[
    str,
    Field(min_length=1, max_length=512, pattern=r".*\S.*"),
    AfterValidator(_reject_nul),
]
SsrfDenylist = Annotated[list[SsrfDenyEntry], Field(max_length=256)]


class DeviceCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    workspace_path: WorkspacePath = "~/openoctopus/workspace"
    sandbox_mode: bool = True
    ssrf_denylist: SsrfDenylist | None = None


class DevicePatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    workspace_path: WorkspacePath | None = None
    sandbox_mode: bool | None = None
    ssrf_denylist: SsrfDenylist | None = None


class DeviceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    name: str
    token_hint: str
    workspace_path: str
    sandbox_mode: bool
    ssrf_denylist: list[str]
    online: bool
    created_at: datetime


class DeviceTokenResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str
    device: DeviceResponse
