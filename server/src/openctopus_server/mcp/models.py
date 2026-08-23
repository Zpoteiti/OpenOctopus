from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import Field, TypeAdapter, ValidationError, field_validator, model_validator

from openctopus_server.devices.mcp_catalog import (
    EMPTY_CATALOG_DIGEST,
    build_persisted_catalog,
    canonical_json_bytes,
    catalog_digest,
    validate_persisted_catalog_entry,
)
from openctopus_server.devices.mcp_models import (
    MCP_CONFIG_BYTES_MAX,
    MCP_SERVER_MAX,
    McpModel,
    McpServerConfig,
    PersistedMcpCatalog,
    RemoteMcpServerConfigBase,
    SourceMcpCatalog,
    SseMcpServerConfig,
    StdioMcpServerConfig,
    StreamableHttpMcpServerConfig,
    parse_mcp_server_configs,
)

_REDACTED = "<redacted>"


class ServerStdioMcpServerConfig(StdioMcpServerConfig):
    max_concurrent_calls: int = Field(default=1, strict=True, ge=1, le=32)


class ServerStreamableHttpMcpServerConfig(StreamableHttpMcpServerConfig):
    max_concurrent_calls: int = Field(default=8, strict=True, ge=1, le=32)


class ServerSseMcpServerConfig(SseMcpServerConfig):
    max_concurrent_calls: int = Field(default=8, strict=True, ge=1, le=32)


type ServerMcpServerConfig = Annotated[
    ServerStdioMcpServerConfig
    | ServerStreamableHttpMcpServerConfig
    | ServerSseMcpServerConfig,
    Field(discriminator="transport"),
]

_SERVER_CONFIG_LIST_ADAPTER = TypeAdapter(list[ServerMcpServerConfig])


def parse_server_mcp_configs(value: object) -> tuple[ServerMcpServerConfig, ...]:
    try:
        configs = _SERVER_CONFIG_LIST_ADAPTER.validate_python(value, strict=True)
    except ValidationError:
        raise ValueError("invalid Server MCP configuration") from None
    if len(configs) > MCP_SERVER_MAX:
        raise ValueError(f"at most {MCP_SERVER_MAX} Server MCP servers may be configured")
    names = [config.name for config in configs]
    if len(names) != len(set(names)):
        raise ValueError("Server MCP server names must be unique")
    configs.sort(key=lambda config: config.name)
    if len(canonical_json_bytes([config.storage_dict() for config in configs])) > MCP_CONFIG_BYTES_MAX:
        raise ValueError("Server MCP configuration is too large")
    return tuple(configs)


def _device_catalog_configs(
    configs: Sequence[ServerMcpServerConfig],
) -> tuple[McpServerConfig, ...]:
    payloads: list[dict[str, Any]] = []
    for config in configs:
        payload = config.storage_dict()
        del payload["max_concurrent_calls"]
        payloads.append(payload)
    return parse_mcp_server_configs(payloads)


def _unexpected_entry_id() -> UUID:
    raise ValueError("stored Server MCP catalog is missing an entry identity")


class ServerMcpEnvelope(McpModel):
    version: Literal[1]
    config_revision: int = Field(strict=True, ge=1)
    mcp_servers: list[ServerMcpServerConfig] = Field(max_length=MCP_SERVER_MAX)
    mcp_catalog: PersistedMcpCatalog

    @field_validator("mcp_servers")
    @classmethod
    def _validate_configs(
        cls,
        value: list[ServerMcpServerConfig],
    ) -> list[ServerMcpServerConfig]:
        return list(parse_server_mcp_configs([config.storage_dict() for config in value]))

    @model_validator(mode="after")
    def _validate_catalog_authority(self) -> ServerMcpEnvelope:
        if self.mcp_catalog.digest != catalog_digest(self.mcp_catalog):
            raise ValueError("Server MCP catalog digest does not match its content")
        try:
            for server in self.mcp_catalog.servers:
                for entry in server.entries:
                    validate_persisted_catalog_entry(entry)
        except ValueError as exc:
            raise ValueError("Server MCP catalog content is invalid") from exc
        try:
            rebuilt = build_persisted_catalog(
                _device_catalog_configs(self.mcp_servers),
                source_catalog=SourceMcpCatalog(version=1, servers=[]),
                existing_catalog=self.mcp_catalog,
                entry_id_factory=_unexpected_entry_id,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("Server MCP config and catalog are inconsistent") from exc
        if canonical_json_bytes(rebuilt) != canonical_json_bytes(self.mcp_catalog):
            raise ValueError("Server MCP catalog digest or content is invalid")
        return self


def empty_server_mcp_envelope() -> ServerMcpEnvelope:
    return ServerMcpEnvelope(
        version=1,
        config_revision=1,
        mcp_servers=[],
        mcp_catalog=PersistedMcpCatalog(
            version=1,
            digest=EMPTY_CATALOG_DIGEST,
            servers=[],
        ),
    )


def server_mcp_envelope_storage(envelope: ServerMcpEnvelope) -> dict[str, Any]:
    return {
        "version": envelope.version,
        "config_revision": envelope.config_revision,
        "mcp_servers": [config.storage_dict() for config in envelope.mcp_servers],
        "mcp_catalog": envelope.mcp_catalog.model_dump(mode="json"),
    }


def _mcp_sink(config: ServerMcpServerConfig) -> tuple[object, ...]:
    if isinstance(config, ServerStdioMcpServerConfig):
        return (
            config.name,
            config.transport,
            config.command,
            tuple(config.args),
            config.cwd,
        )
    return (config.name, config.transport, config.url)


def _resolved_secret_map(
    candidate: dict[str, object],
    current: dict[str, str],
    *,
    same_sink: bool,
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for key, value in candidate.items():
        if not isinstance(value, str):
            raise ValueError("MCP secret value is invalid")
        if value != _REDACTED:
            resolved[key] = value
            continue
        if not same_sink or key not in current:
            raise ValueError("A redacted MCP secret can only retain the same key at the same sink")
        resolved[key] = current[key]
    return resolved


def resolve_server_mcp_secret_markers(
    current: Sequence[ServerMcpServerConfig],
    candidate: Sequence[ServerMcpServerConfig],
) -> tuple[ServerMcpServerConfig, ...]:
    current_by_name = {server.name: server for server in current}
    resolved: list[dict[str, Any]] = []
    for server in candidate:
        payload = server.storage_dict()
        previous = current_by_name.get(server.name)
        same_sink = previous is not None and type(previous) is type(server) and _mcp_sink(
            previous
        ) == _mcp_sink(server)
        if isinstance(server, ServerStdioMcpServerConfig):
            values = payload["env"]
            assert isinstance(values, dict)
            current_values = (
                {key: secret.get_secret_value() for key, secret in previous.env.items()}
                if isinstance(previous, ServerStdioMcpServerConfig) and same_sink
                else {}
            )
            payload["env"] = _resolved_secret_map(
                values,
                current_values,
                same_sink=same_sink,
            )
        else:
            values = payload["headers"]
            assert isinstance(values, dict)
            current_values = (
                {key: secret.get_secret_value() for key, secret in previous.headers.items()}
                if isinstance(previous, RemoteMcpServerConfigBase) and same_sink
                else {}
            )
            payload["headers"] = _resolved_secret_map(
                values,
                current_values,
                same_sink=same_sink,
            )
        resolved.append(payload)
    return parse_server_mcp_configs(resolved)


def redacted_server_mcp_configs(
    configs: Sequence[ServerMcpServerConfig],
) -> list[dict[str, Any]]:
    redacted: list[dict[str, Any]] = []
    for config in configs:
        payload = config.storage_dict()
        if isinstance(config, ServerStdioMcpServerConfig):
            payload["env"] = {key: _REDACTED for key in config.env}
        else:
            payload["headers"] = {key: _REDACTED for key in config.headers}
        redacted.append(payload)
    return redacted


def parse_server_mcp_envelope(value: object) -> ServerMcpEnvelope:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        return ServerMcpEnvelope.model_validate_json(encoded, strict=True)
    except (TypeError, ValueError) as exc:
        raise ValueError("stored Server MCP envelope is invalid") from exc
