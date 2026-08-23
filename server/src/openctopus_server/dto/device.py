from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field

from openctopus_server.devices.mcp_models import McpServerConfig


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

    base_config_revision: int = Field(ge=1)
    name: str | None = None
    workspace_path: WorkspacePath | None = None
    restrict_to_workspace: bool | None = None
    ssrf_denylist: SsrfDenylist | None = None
    shell_timeout_max: int | None = Field(default=None, ge=0, le=86400)
    env_allowlist: EnvAllowlist | None = None
    mcp_servers: list[McpServerConfig] | None = Field(default=None, max_length=16)


class StdioMcpServerResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    transport: Literal["stdio"]
    command: str
    args: list[str]
    cwd: str | None
    env: dict[str, str]
    enabled_capabilities: list[str] | None
    effective_status: Literal["active", "shadowed_by_server"]
    shadowed_by: str | None


class RemoteMcpServerResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    transport: Literal["streamable_http", "sse"]
    url: str
    headers: dict[str, str]
    enabled_capabilities: list[str] | None
    effective_status: Literal["active", "shadowed_by_server"]
    shadowed_by: str | None


type McpServerResponse = StdioMcpServerResponse | RemoteMcpServerResponse


class McpDiscoveredCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_name: str
    final_name: str
    enabled: bool
    provider_visible: bool
    suppression_reason: Literal[
        "server_namespace_reserved",
        "server_final_name_collision",
        "provider_capacity",
    ] | None


class McpDiscoveredServer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tools: list[McpDiscoveredCapability]
    resources: list[McpDiscoveredCapability]
    resource_templates: list[McpDiscoveredCapability]
    prompts: list[McpDiscoveredCapability]


class DeviceConfigIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    online: bool
    config_revision: int


class DeviceConfigResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    device: DeviceConfigIdentity
    mcp_servers: list[McpServerResponse]
    mcp_catalog_digest: str
    mcp_discovered: dict[str, McpDiscoveredServer]


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
    mcp_provider_visible_capability_count: int
    mcp_suppressed_capability_count: int
    mcp_catalog_digest: str
    online: bool
    created_at: datetime


class DeviceTokenResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str
    device: DeviceResponse
