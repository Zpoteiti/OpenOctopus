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
EnvName = Annotated[str, Field(min_length=1, max_length=128, pattern=r"[^=\x00-\x1f]+")]
EnvAllowlist = Annotated[list[EnvName], Field(max_length=64)]


class DeviceCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    workspace_path: WorkspacePath = "~/openoctopus/workspace"
    restrict_to_workspace: bool = True
    ssrf_denylist: SsrfDenylist | None = None
    shell_timeout_max: int = Field(default=600, ge=0, le=86400)
    env_allowlist: EnvAllowlist | None = None


class DevicePatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    workspace_path: WorkspacePath | None = None
    restrict_to_workspace: bool | None = None
    ssrf_denylist: SsrfDenylist | None = None
    shell_timeout_max: int | None = Field(default=None, ge=0, le=86400)
    env_allowlist: EnvAllowlist | None = None


class DeviceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    name: str
    token_hint: str
    workspace_path: str
    restrict_to_workspace: bool
    ssrf_denylist: list[str]
    shell_timeout_max: int
    env_allowlist: list[str]
    config_revision: int
    mcp_config_count: int
    mcp_enabled_capability_count: int
    mcp_catalog_digest: str
    online: bool
    created_at: datetime


class DeviceTokenResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str
    device: DeviceResponse
