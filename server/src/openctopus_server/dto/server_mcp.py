from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from openctopus_server.mcp.models import (
    ServerMcpServerConfig,
    parse_server_mcp_configs,
)


class ServerMcpPutRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    base_config_revision: int = Field(strict=True, ge=1)
    mcp_servers: list[ServerMcpServerConfig] = Field(max_length=16)

    @field_validator("mcp_servers")
    @classmethod
    def _validate_whole_list(
        cls,
        value: list[ServerMcpServerConfig],
    ) -> list[ServerMcpServerConfig]:
        return list(
            parse_server_mcp_configs([config.storage_dict() for config in value])
        )


class ServerStdioMcpResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    transport: Literal["stdio"]
    command: str
    args: list[str]
    cwd: str | None
    env: dict[str, str]
    enabled_capabilities: list[str] | None
    max_concurrent_calls: int


class ServerRemoteMcpResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    transport: Literal["streamable_http", "sse"]
    url: str
    headers: dict[str, str]
    enabled_capabilities: list[str] | None
    max_concurrent_calls: int


type ServerMcpConfigResponse = Annotated[
    ServerStdioMcpResponse | ServerRemoteMcpResponse,
    Field(discriminator="transport"),
]


class ServerMcpDiscoveredCapability(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    raw_name: str
    final_name: str
    enabled: bool


class ServerMcpDiscoveredServer(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    tools: list[ServerMcpDiscoveredCapability]
    resources: list[ServerMcpDiscoveredCapability]
    resource_templates: list[ServerMcpDiscoveredCapability]
    prompts: list[ServerMcpDiscoveredCapability]


class ServerMcpRuntimeError(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    code: str
    message: str


class ServerMcpRuntimeStatus(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    state: Literal[
        "starting",
        "discovering",
        "ready",
        "unavailable",
        "backoff",
        "drifted",
        "draining",
        "cleanup_blocked",
    ]
    origin: Literal["persisted", "candidate"]
    config_revision: int | None
    catalog_digest: str | None
    runtime_generation: UUID | None
    max_concurrent_calls: int
    active_calls: int
    waiting_calls: int
    draining_calls: int
    restart_attempt: int
    last_error: ServerMcpRuntimeError | None


class ServerMcpRuntimeSlot(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    configured: bool
    active: ServerMcpRuntimeStatus | None
    draining: ServerMcpRuntimeStatus | None


class ServerMcpAdminResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    config_revision: int
    mcp_servers: list[ServerMcpConfigResponse]
    mcp_catalog_digest: str
    mcp_discovered: dict[str, ServerMcpDiscoveredServer]
    runtimes: dict[str, ServerMcpRuntimeSlot]
