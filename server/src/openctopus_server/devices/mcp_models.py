from __future__ import annotations

import json
from pathlib import PurePosixPath, PureWindowsPath
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import (
    AfterValidator,
    AnyHttpUrl,
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

MCP_SERVER_MAX = 16
MCP_CONFIG_BYTES_MAX = 256 * 1024
MCP_SECRET_BYTES_MAX = 16 * 1024

_HTTP_URL_ADAPTER = TypeAdapter(AnyHttpUrl)

_SERVER_NAME_PATTERN = r"^[a-z][a-z0-9_]{0,31}$"
_FINAL_NAME_PATTERN = r"^[a-zA-Z0-9_-]{1,64}$"
_HTTP_TCHAR = frozenset(
    "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)
_FORBIDDEN_HEADERS = frozenset(
    {
        "accept",
        "connection",
        "content-length",
        "content-type",
        "host",
        "keep-alive",
        "last-event-id",
        "mcp-protocol-version",
        "mcp-session-id",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


class McpModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)


def _reject_nul(value: str) -> str:
    if "\x00" in value:
        raise ValueError("value must not contain NUL")
    return value


def _validate_secret(value: SecretStr, *, header: bool) -> SecretStr:
    raw = value.get_secret_value()
    if len(raw.encode("utf-8")) > MCP_SECRET_BYTES_MAX:
        raise ValueError("secret value exceeds UTF-8 byte limit")
    forbidden = "\r\n\x00" if header else "\x00"
    if any(character in raw for character in forbidden):
        raise ValueError("secret value contains a forbidden character")
    return value


def _validate_env_secret(value: SecretStr) -> SecretStr:
    return _validate_secret(value, header=False)


def _validate_header_secret(value: SecretStr) -> SecretStr:
    return _validate_secret(value, header=True)


EnvSecret = Annotated[SecretStr, AfterValidator(_validate_env_secret)]
HeaderSecret = Annotated[SecretStr, AfterValidator(_validate_header_secret)]
Argument = Annotated[str, Field(max_length=4096), AfterValidator(_reject_nul)]
EnabledCapability = Annotated[
    str,
    Field(min_length=1, max_length=64, pattern=_FINAL_NAME_PATTERN),
    AfterValidator(_reject_nul),
]
RawCapabilityName = Annotated[str, Field(min_length=1, max_length=256), AfterValidator(_reject_nul)]
InvocationIdentity = Annotated[
    str, Field(min_length=1, max_length=4096), AfterValidator(_reject_nul)
]


class McpServerConfigBase(McpModel):
    name: str = Field(pattern=_SERVER_NAME_PATTERN)
    enabled_capabilities: list[EnabledCapability] | None = Field(default=None, max_length=512)

    @field_validator("enabled_capabilities")
    @classmethod
    def _enabled_names_are_unique(cls, value: list[str] | None) -> list[str] | None:
        if value is not None and len(value) != len(set(value)):
            raise ValueError("enabled_capabilities entries must be unique")
        return value

    def storage_dict(self) -> dict[str, Any]:
        payload = self.model_dump(mode="python")
        if isinstance(self, StdioMcpServerConfig):
            payload["env"] = {key: secret.get_secret_value() for key, secret in self.env.items()}
        elif isinstance(self, RemoteMcpServerConfigBase):
            payload["headers"] = {
                key: secret.get_secret_value() for key, secret in self.headers.items()
            }
        return payload


class StdioMcpServerConfig(McpServerConfigBase):
    transport: Literal["stdio"]
    command: str = Field(min_length=1, max_length=4096)
    args: list[Argument] = Field(default_factory=list, max_length=64)
    cwd: str | None = Field(default=None, min_length=1, max_length=4096)
    env: dict[str, EnvSecret] = Field(default_factory=dict, max_length=64)

    @field_validator("command")
    @classmethod
    def _validate_command(cls, value: str) -> str:
        if not value.strip() or "\x00" in value:
            raise ValueError("command must be a non-blank executable string")
        return value

    @field_validator("cwd")
    @classmethod
    def _validate_cwd(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip() or "\x00" in value:
            raise ValueError("cwd must be a non-blank path")
        home_relative = value == "~" or value.startswith(("~/", "~\\"))
        if not (
            home_relative
            or PurePosixPath(value).is_absolute()
            or PureWindowsPath(value).is_absolute()
        ):
            raise ValueError("cwd must be absolute or start at the user home")
        return value

    @field_validator("env")
    @classmethod
    def _validate_env(cls, value: dict[str, EnvSecret]) -> dict[str, EnvSecret]:
        folded: set[str] = set()
        for key in value:
            normalized = key.casefold()
            if (
                not key
                or len(key) > 128
                or "=" in key
                or any(ord(character) < 0x20 or ord(character) == 0x7F for character in key)
                or normalized.startswith("openoctopus_")
                or normalized in folded
            ):
                raise ValueError("env contains an invalid or duplicate variable name")
            folded.add(normalized)
        return value


class RemoteMcpServerConfigBase(McpServerConfigBase):
    url: str = Field(min_length=1, max_length=4096)
    headers: dict[str, HeaderSecret] = Field(default_factory=dict, max_length=64)

    @field_validator("headers", mode="before")
    @classmethod
    def _canonicalize_headers(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        canonical: dict[str, object] = {}
        for raw_name, raw_value in value.items():
            if not isinstance(raw_name, str):
                raise ValueError("header names must be strings")
            name = raw_name.lower()
            if (
                not raw_name
                or len(raw_name) > 128
                or any(character not in _HTTP_TCHAR for character in raw_name)
                or name in _FORBIDDEN_HEADERS
                or name in canonical
            ):
                raise ValueError("headers contain an invalid, forbidden, or duplicate name")
            canonical[name] = raw_value
        return canonical

    @model_validator(mode="after")
    def _validate_remote_endpoint(self) -> RemoteMcpServerConfigBase:
        if any(ord(character) <= 0x20 or ord(character) == 0x7F for character in self.url):
            raise ValueError("url contains whitespace or control characters")
        try:
            _HTTP_URL_ADAPTER.validate_python(self.url, strict=True)
        except ValidationError:
            raise ValueError("url is invalid") from None
        try:
            parsed = urlsplit(self.url)
            port = parsed.port
        except ValueError as exc:
            raise ValueError("url is invalid") from exc
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or "#" in self.url
        ):
            raise ValueError("url must be a complete HTTP(S) endpoint without userinfo or fragment")
        if port is not None and not 1 <= port <= 65535:
            raise ValueError("url port is invalid")
        if self.headers and parsed.scheme != "https":
            raise ValueError("remote MCP headers require HTTPS")
        return self


class StreamableHttpMcpServerConfig(RemoteMcpServerConfigBase):
    transport: Literal["streamable_http"]


class SseMcpServerConfig(RemoteMcpServerConfigBase):
    transport: Literal["sse"]


type McpServerConfig = Annotated[
    StdioMcpServerConfig | StreamableHttpMcpServerConfig | SseMcpServerConfig,
    Field(discriminator="transport"),
]
_CONFIG_LIST_ADAPTER = TypeAdapter(list[McpServerConfig])
_ANY_URL_ADAPTER = TypeAdapter(AnyUrl)


def parse_mcp_server_configs(value: object) -> tuple[McpServerConfig, ...]:
    try:
        configs = _CONFIG_LIST_ADAPTER.validate_python(value, strict=True)
    except ValidationError:
        raise ValueError("invalid MCP server configuration") from None
    if len(configs) > MCP_SERVER_MAX:
        raise ValueError(f"at most {MCP_SERVER_MAX} MCP servers may be configured")
    names = [config.name for config in configs]
    if len(names) != len(set(names)):
        raise ValueError("MCP server names must be unique")
    payload = [config.storage_dict() for config in configs]
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > MCP_CONFIG_BYTES_MAX:
        raise ValueError("MCP server configuration is too large")
    return tuple(configs)


class PromptArgument(McpModel):
    name: RawCapabilityName
    description: str | None = Field(default=None, max_length=4096)
    required: bool = False


class SourceMcpTool(McpModel):
    raw_name: RawCapabilityName
    description: str | None = Field(default=None, max_length=4096)
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None = None


class SourceMcpResource(McpModel):
    raw_name: RawCapabilityName
    uri: InvocationIdentity
    description: str | None = Field(default=None, max_length=4096)
    mime_type: str | None = Field(default=None, max_length=256)

    @field_validator("uri")
    @classmethod
    def _normalize_uri(cls, value: str) -> str:
        try:
            return str(_ANY_URL_ADAPTER.validate_python(value, strict=True))
        except ValidationError:
            raise ValueError("resource URI is invalid") from None


class SourceMcpResourceTemplate(McpModel):
    raw_name: RawCapabilityName
    uri_template: InvocationIdentity
    description: str | None = Field(default=None, max_length=4096)
    mime_type: str | None = Field(default=None, max_length=256)


class SourceMcpPrompt(McpModel):
    raw_name: RawCapabilityName
    description: str | None = Field(default=None, max_length=4096)
    arguments: list[PromptArgument] = Field(default_factory=list, max_length=256)


class SourceMcpServerCatalog(McpModel):
    name: str = Field(pattern=_SERVER_NAME_PATTERN)
    tools: list[SourceMcpTool] = Field(default_factory=list, max_length=256)
    resources: list[SourceMcpResource] = Field(default_factory=list, max_length=256)
    resource_templates: list[SourceMcpResourceTemplate] = Field(
        default_factory=list, max_length=256
    )
    prompts: list[SourceMcpPrompt] = Field(default_factory=list, max_length=256)

    @property
    def capability_count(self) -> int:
        return (
            len(self.tools) + len(self.resources) + len(self.resource_templates) + len(self.prompts)
        )


class SourceMcpCatalog(McpModel):
    version: Literal[1]
    servers: list[SourceMcpServerCatalog] = Field(default_factory=list, max_length=16)

    @field_validator("servers")
    @classmethod
    def _server_names_are_unique(
        cls, value: list[SourceMcpServerCatalog]
    ) -> list[SourceMcpServerCatalog]:
        names = [server.name for server in value]
        if len(names) != len(set(names)):
            raise ValueError("source catalog server names must be unique")
        return value


class PersistedMcpCatalogEntry(McpModel):
    entry_id: UUID
    server: str = Field(pattern=_SERVER_NAME_PATTERN)
    surface: Literal["tool", "resource", "resource_template", "prompt"]
    raw_name: RawCapabilityName
    invocation_identity: InvocationIdentity
    final_name: str = Field(min_length=1, max_length=64, pattern=_FINAL_NAME_PATTERN)
    provider_description: str = Field(min_length=1, max_length=8192)
    input_schema: dict[str, Any]
    output_schema: dict[str, Any] | None = None
    enabled: bool

    @field_validator("entry_id")
    @classmethod
    def _entry_id_is_uuid7(cls, value: UUID) -> UUID:
        if value.version != 7:
            raise ValueError("MCP entry IDs must be UUID v7")
        return value


class PersistedMcpServerCatalog(McpModel):
    name: str = Field(pattern=_SERVER_NAME_PATTERN)
    entries: list[PersistedMcpCatalogEntry] = Field(default_factory=list, max_length=256)

    @model_validator(mode="after")
    def _entries_match_server(self) -> PersistedMcpServerCatalog:
        if any(entry.server != self.name for entry in self.entries):
            raise ValueError("persisted entries must match their server")
        return self


class PersistedMcpCatalog(McpModel):
    version: Literal[1]
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    servers: list[PersistedMcpServerCatalog] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def _catalog_identity_is_unique(self) -> PersistedMcpCatalog:
        names = [server.name for server in self.servers]
        entry_ids = [entry.entry_id for server in self.servers for entry in server.entries]
        if len(names) != len(set(names)):
            raise ValueError("persisted catalog server names must be unique")
        if len(entry_ids) != len(set(entry_ids)):
            raise ValueError("persisted catalog entry IDs must be unique")
        return self


class ProviderMcpTool(McpModel):
    name: str = Field(min_length=1, max_length=64, pattern=_FINAL_NAME_PATTERN)
    description: str = Field(min_length=1, max_length=8192)
    input_schema: dict[str, Any]
