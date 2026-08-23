from __future__ import annotations

import base64
import binascii
import json
import re
import secrets
import time
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from .mcp_models import (
    MCP_SERVER_MAX,
    parse_mcp_server_configs,
)
from .mcp_models import (
    McpServerConfig as McpServerConfig,
)
from .mcp_models import (
    McpServerConfigBase as McpServerConfigBase,
)
from .mcp_models import (
    PersistedMcpCatalog as PersistedMcpCatalog,
)
from .mcp_models import (
    PersistedMcpCatalogEntry as PersistedMcpCatalogEntry,
)
from .mcp_models import (
    PersistedMcpServerCatalog as PersistedMcpServerCatalog,
)
from .mcp_models import (
    PromptArgument as PromptArgument,
)
from .mcp_models import (
    SourceMcpCatalog as SourceMcpCatalog,
)
from .mcp_models import (
    SourceMcpPrompt as SourceMcpPrompt,
)
from .mcp_models import (
    SourceMcpResource as SourceMcpResource,
)
from .mcp_models import (
    SourceMcpResourceTemplate as SourceMcpResourceTemplate,
)
from .mcp_models import (
    SourceMcpServerCatalog as SourceMcpServerCatalog,
)
from .mcp_models import (
    SourceMcpTool as SourceMcpTool,
)
from .mcp_models import (
    SseMcpServerConfig as SseMcpServerConfig,
)
from .mcp_models import (
    StdioMcpServerConfig as StdioMcpServerConfig,
)
from .mcp_models import (
    StreamableHttpMcpServerConfig as StreamableHttpMcpServerConfig,
)

PROTOCOL_VERSION: Literal["3"] = "3"
MAX_TEXT_FRAME_BYTES = 12 * 1024 * 1024
# Pending-call admission reserves both the largest legal request frame and the
# largest legal result frame.  Configuration must permit at least one such call.
MAX_TOOL_CALL_RESERVATION_BYTES = 2 * MAX_TEXT_FRAME_BYTES
MAX_BINARY_CHUNK_BYTES = 64 * 1024
TRANSFER_PURPOSES = ("file_transfer", "workspace_upload", "http_relay")

MCP_SERVER_CAPABILITY_MAX = 256

_MCP_SERVER_NAME_PATTERN = r"^[a-z][a-z0-9_]{0,31}$"


def new_uuid7() -> UUID:
    timestamp_ms = time.time_ns() // 1_000_000
    if timestamp_ms >= 1 << 48:
        raise OverflowError("UUID v7 timestamp exceeds 48 bits")
    random_a = secrets.randbits(12)
    random_b = secrets.randbits(62)
    value = (timestamp_ms << 80) | (0x7 << 76) | (random_a << 64) | (0b10 << 62) | random_b
    return UUID(int=value)


def _require_uuid7(value: UUID) -> UUID:
    if value.version != 7:
        raise ValueError("protocol IDs must be UUID v7")
    return value


Uuid7 = Annotated[UUID, AfterValidator(_require_uuid7)]


class ProtocolModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        hide_input_in_errors=True,
    )


def _reject_nul(value: str) -> str:
    if "\x00" in value:
        raise ValueError("value must not contain NUL")
    return value


McpDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
McpCode = Annotated[str, Field(min_length=1, max_length=128), AfterValidator(_reject_nul)]


class RuntimeMcpSourceCatalog(ProtocolModel):
    tools: list[SourceMcpTool] = Field(default_factory=list, max_length=256)
    resources: list[SourceMcpResource] = Field(default_factory=list, max_length=256)
    resource_templates: list[SourceMcpResourceTemplate] = Field(
        default_factory=list, max_length=256
    )
    prompts: list[SourceMcpPrompt] = Field(default_factory=list, max_length=256)

    @model_validator(mode="after")
    def _capability_count_is_bounded(self) -> RuntimeMcpSourceCatalog:
        if (
            len(self.tools) + len(self.resources) + len(self.resource_templates) + len(self.prompts)
            > MCP_SERVER_CAPABILITY_MAX
        ):
            raise ValueError("MCP server source catalog exceeds its capability limit")
        return self


_CANONICAL_SHELL_NAMES = frozenset(
    {"bash", "sh", "zsh", "pwsh", "powershell", "powershell_x86", "cmd"}
)


class DeviceCapabilities(ProtocolModel):
    shared_tools: Literal[True] = True
    web_fetch: Literal[True] = True
    file_transfer: tuple[Literal["send", "receive"], Literal["send", "receive"]] = (
        "send",
        "receive",
    )
    http_relay: Literal[True] = True

    @field_validator("file_transfer")
    @classmethod
    def _require_both_transfer_directions(
        cls,
        value: tuple[Literal["send", "receive"], Literal["send", "receive"]],
    ) -> tuple[Literal["send", "receive"], Literal["send", "receive"]]:
        if value != ("send", "receive"):
            raise ValueError("Py6 clients must support send and receive")
        return value


class ShellMetadata(ProtocolModel):
    default: str = Field(min_length=1, max_length=32)
    available: list[str] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def _validate_shells(self) -> ShellMetadata:
        names = (self.default, *self.available)
        if any(
            len(name) > 32
            or not all(
                "a" <= character <= "z" or "0" <= character <= "9" or character == "_"
                for character in name
            )
            or name not in _CANONICAL_SHELL_NAMES
            for name in names
        ):
            raise ValueError("shell names must be canonical lowercase names")
        if len(self.available) != len(set(self.available)):
            raise ValueError("shells.available must not contain duplicates")
        if self.default not in self.available:
            raise ValueError("shells.default must be listed in shells.available")
        return self


class DeviceConfigFrame(ProtocolModel):
    workspace_path: str = Field(min_length=1, max_length=4096)
    restrict_to_workspace: bool
    ssrf_denylist: list[str] = Field(max_length=256)
    shell_timeout_max: int = Field(default=600, ge=0, le=86400)
    env_allowlist: list[str] = Field(
        default_factory=lambda: [
            "PATH",
            "HOME",
            "LANG",
            "TERM",
            "SystemRoot",
            "ComSpec",
            "PATHEXT",
            "TEMP",
            "TMP",
            "USERPROFILE",
        ],
        max_length=64,
    )
    mcp_servers: list[McpServerConfig] = Field(default_factory=list, max_length=MCP_SERVER_MAX)

    @field_validator("workspace_path")
    @classmethod
    def _workspace_path_has_content(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("workspace_path must not contain NUL")
        if not value.strip():
            raise ValueError("workspace_path must not be blank")
        return value

    @field_validator("ssrf_denylist")
    @classmethod
    def _deny_entries_have_content(cls, value: list[str]) -> list[str]:
        if any("\x00" in entry for entry in value):
            raise ValueError("ssrf_denylist entries must not contain NUL")
        if any(not entry.strip() or len(entry) > 512 for entry in value):
            raise ValueError("ssrf_denylist entries must be non-blank and bounded")
        return value

    @field_validator("env_allowlist")
    @classmethod
    def _validate_env_allowlist(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("env_allowlist entries must be unique")
        for entry in value:
            if (
                not entry
                or len(entry) > 128
                or entry.strip() != entry
                or any(ord(char) < 0x20 for char in entry)
                or "=" in entry
                or entry.upper().startswith("OPENOCTOPUS_")
            ):
                raise ValueError("env_allowlist contains an invalid variable name")
        return value

    @field_validator("mcp_servers")
    @classmethod
    def _validate_mcp_servers(cls, value: list[McpServerConfig]) -> list[McpServerConfig]:
        return list(parse_mcp_server_configs([server.storage_dict() for server in value]))


class HelloFrame(ProtocolModel):
    type: Literal["hello"] = "hello"
    id: Uuid7
    version: Literal["3"]
    client_version: str = Field(min_length=1, max_length=64)
    os: Literal["linux", "darwin", "windows"]
    caps: DeviceCapabilities
    shells: ShellMetadata


class HelloAckFrame(ProtocolModel):
    type: Literal["hello_ack"] = "hello_ack"
    id: Uuid7
    device_name: str = Field(min_length=1, max_length=64)
    config_revision: int = Field(ge=1)
    config: DeviceConfigFrame
    mcp_catalog: PersistedMcpCatalog


class ConfigUpdateFrame(ProtocolModel):
    type: Literal["config_update"] = "config_update"
    id: Uuid7
    device_name: str = Field(min_length=1, max_length=64)
    config_revision: int = Field(ge=1)
    config: DeviceConfigFrame
    mcp_catalog: PersistedMcpCatalog


class ConfigAppliedFrame(ProtocolModel):
    type: Literal["config_applied"] = "config_applied"
    id: Uuid7
    config_revision: int = Field(ge=1)


class ConfigAppliedAckFrame(ProtocolModel):
    type: Literal["config_applied_ack"] = "config_applied_ack"
    id: Uuid7
    config_revision: int = Field(ge=1)


class ConfigValidateFrame(ProtocolModel):
    type: Literal["config_validate"] = "config_validate"
    id: Uuid7
    base_config_revision: int = Field(ge=1)
    candidate_config: DeviceConfigFrame
    validate_servers: list[str] = Field(min_length=1, max_length=MCP_SERVER_MAX)
    deadline_ms: Literal[300000]

    @field_validator("validate_servers")
    @classmethod
    def _validate_server_names(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)) or any(
            re.fullmatch(_MCP_SERVER_NAME_PATTERN, name) is None for name in value
        ):
            raise ValueError("validate_servers entries must be unique MCP server names")
        return value


class McpValidationFailure(ProtocolModel):
    name: str = Field(pattern=_MCP_SERVER_NAME_PATTERN)
    stage: str = Field(min_length=1, max_length=64)
    code: McpCode
    message: str = Field(min_length=1, max_length=4096)


class ConfigValidateResultFrame(ProtocolModel):
    type: Literal["config_validate_result"] = "config_validate_result"
    id: Uuid7
    ok: bool
    source_catalog: SourceMcpCatalog | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    failures: list[McpValidationFailure] = Field(max_length=MCP_SERVER_MAX)

    @model_validator(mode="after")
    def _validate_outcome(self) -> ConfigValidateResultFrame:
        source_was_sent = "source_catalog" in self.model_fields_set
        if self.ok:
            if self.source_catalog is None or self.failures:
                raise ValueError("successful MCP validation requires only a source catalog")
        elif source_was_sent or self.source_catalog is not None or not self.failures:
            raise ValueError("failed MCP validation requires failures and no source catalog")
        return self


class ConfigValidateCancelFrame(ProtocolModel):
    type: Literal["config_validate_cancel"] = "config_validate_cancel"
    id: Uuid7


class ReadyMcpRuntimeSnapshot(ProtocolModel):
    name: str = Field(pattern=_MCP_SERVER_NAME_PATTERN)
    runtime_generation: Uuid7
    state: Literal["ready"]
    code: Literal[None]
    source_catalog: RuntimeMcpSourceCatalog


class UnavailableMcpRuntimeSnapshot(ProtocolModel):
    name: str = Field(pattern=_MCP_SERVER_NAME_PATTERN)
    runtime_generation: Uuid7
    state: Literal["unavailable"]
    code: McpCode


class DriftedMcpRuntimeSnapshot(ProtocolModel):
    name: str = Field(pattern=_MCP_SERVER_NAME_PATTERN)
    runtime_generation: Uuid7
    state: Literal["drifted"]
    code: McpCode


McpRuntimeSnapshot = Annotated[
    ReadyMcpRuntimeSnapshot | UnavailableMcpRuntimeSnapshot | DriftedMcpRuntimeSnapshot,
    Field(discriminator="state"),
]


class RegisterMcpFrame(ProtocolModel):
    type: Literal["register_mcp"] = "register_mcp"
    id: Uuid7
    config_revision: int = Field(ge=1)
    catalog_digest: McpDigest
    servers: list[McpRuntimeSnapshot] = Field(max_length=MCP_SERVER_MAX)

    @field_validator("servers")
    @classmethod
    def _server_names_are_unique(cls, value: list[McpRuntimeSnapshot]) -> list[McpRuntimeSnapshot]:
        names = [server.name for server in value]
        if len(names) != len(set(names)):
            raise ValueError("register_mcp server names must be unique")
        return value


class AcceptedMcpRegistration(ProtocolModel):
    name: str = Field(pattern=_MCP_SERVER_NAME_PATTERN)
    runtime_generation: Uuid7
    accepted: Literal[True]
    code: Literal[None]


class RejectedMcpRegistration(ProtocolModel):
    name: str = Field(pattern=_MCP_SERVER_NAME_PATTERN)
    runtime_generation: Uuid7
    accepted: Literal[False]
    code: McpCode


McpRegistrationResult = Annotated[
    AcceptedMcpRegistration | RejectedMcpRegistration,
    Field(discriminator="accepted"),
]


class RegisterMcpAckFrame(ProtocolModel):
    type: Literal["register_mcp_ack"] = "register_mcp_ack"
    id: Uuid7
    config_revision: int = Field(ge=1)
    catalog_digest: McpDigest
    results: list[McpRegistrationResult] = Field(max_length=MCP_SERVER_MAX)

    @field_validator("results")
    @classmethod
    def _result_names_are_unique(
        cls, value: list[McpRegistrationResult]
    ) -> list[McpRegistrationResult]:
        names = [result.name for result in value]
        if len(names) != len(set(names)):
            raise ValueError("register_mcp_ack result names must be unique")
        return value


class PingFrame(ProtocolModel):
    type: Literal["ping"] = "ping"
    id: Uuid7


class PongFrame(ProtocolModel):
    type: Literal["pong"] = "pong"
    id: Uuid7


class TextResultBlock(ProtocolModel):
    type: Literal["text"] = "text"
    text: str


class Base64ImageSource(ProtocolModel):
    type: Literal["base64"]
    media_type: Literal["image/jpeg", "image/png", "image/gif", "image/webp"]
    data: str = Field(min_length=4, max_length=MAX_TEXT_FRAME_BYTES)

    @field_validator("data")
    @classmethod
    def _valid_base64(cls, value: str) -> str:
        try:
            base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("image data must be valid base64") from exc
        return value


class ImageResultBlock(ProtocolModel):
    type: Literal["image"] = "image"
    source: Base64ImageSource


SafeResultBlock = Annotated[TextResultBlock | ImageResultBlock, Field(discriminator="type")]


class McpRoute(ProtocolModel):
    entry_id: Uuid7
    config_revision: int = Field(ge=1)
    catalog_digest: McpDigest
    runtime_generation: Uuid7


class ToolCallFrame(ProtocolModel):
    type: Literal["tool_call"] = "tool_call"
    id: Uuid7
    name: str = Field(min_length=1, max_length=128)
    args: dict[str, Any]
    max_result_bytes: int = Field(ge=1, le=MAX_TEXT_FRAME_BYTES)
    chat_session_id: UUID | None = None
    mcp_route: McpRoute | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    @model_validator(mode="after")
    def _validate_hidden_route(self) -> ToolCallFrame:
        if (
            self.name in {"exec", "write_stdin", "list_exec_sessions"}
            and self.chat_session_id is None
        ):
            raise ValueError("client-only exec calls require chat_session_id")
        is_mcp_name = self.name.startswith("mcp_")
        if is_mcp_name != (self.mcp_route is not None):
            raise ValueError("MCP tool names and mcp_route must appear together")
        if self.mcp_route is not None and "openoctopus_device" in self.args:
            raise ValueError("MCP tool calls must not forward openoctopus_device")
        if is_mcp_name and (
            len(self.name) > 64
            or self.name == "mcp_"
            or any(
                not ("a" <= character <= "z" or "0" <= character <= "9" or character in "_-")
                for character in self.name
            )
        ):
            raise ValueError("MCP tool call name is not canonical")
        return self


class ToolResultFrame(ProtocolModel):
    type: Literal["tool_result"] = "tool_result"
    id: Uuid7
    content: str | list[SafeResultBlock]
    is_error: bool
    code: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def _validate_error_code(self) -> ToolResultFrame:
        if self.is_error and self.code is None:
            raise ValueError("error tool results require a code")
        if not self.is_error and self.code is not None:
            raise ValueError("successful tool results must not carry a code")
        return self


class ErrorFrame(ProtocolModel):
    type: Literal["error"] = "error"
    id: Uuid7 | None = None
    code: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=4096)


TransferPurpose = Literal["file_transfer", "workspace_upload", "http_relay"]
TransferDirection = Literal["client_to_server", "server_to_client"]


def _validate_transfer_path(value: str | None) -> str | None:
    if value is None:
        return None
    if "\x00" in value or not value.strip():
        raise ValueError("transfer paths must be non-empty and contain no NUL")
    return value


# ETags are opaque protocol values.  The Py6 client currently emits a hashed
# stat fingerprint, but the server only compares the value and does not parse
# filesystem identity from it.
OpaqueTag = Annotated[str, Field(min_length=1, max_length=512, strict=True)]


def _validate_opaque_tag(value: str | None) -> str | None:
    if value is None:
        return None
    if any(character in {'"', "\x00"} or not 0x21 <= ord(character) <= 0x7E for character in value):
        raise ValueError("ETag must contain visible ASCII without quotes")
    return value


def _valid_sha256(value: str | None) -> str | None:
    if value is None:
        return None
    if len(value) != 64:
        raise ValueError("sha256 must be a 64-character hexadecimal digest")
    try:
        int(value, 16)
    except ValueError as exc:
        raise ValueError("sha256 must be hexadecimal") from exc
    return value.lower()


class TransferRequestFrame(ProtocolModel):
    type: Literal["transfer_request"] = "transfer_request"
    id: Uuid7
    purpose: TransferPurpose
    src_path: str = Field(min_length=1, max_length=4096)
    dst_path: str | None = Field(default=None, max_length=4096)

    _src_path = field_validator("src_path")(_validate_transfer_path)
    _dst_path = field_validator("dst_path")(_validate_transfer_path)

    @model_validator(mode="after")
    def _validate_purpose(self) -> TransferRequestFrame:
        if self.purpose == "workspace_upload":
            raise ValueError("workspace_upload starts with transfer_begin")
        if self.purpose == "file_transfer" and self.dst_path is None:
            raise ValueError("file_transfer request requires dst_path")
        if self.purpose == "http_relay" and self.dst_path is not None:
            raise ValueError("http_relay request must not include dst_path")
        return self


class TransferBeginFrame(ProtocolModel):
    type: Literal["transfer_begin"] = "transfer_begin"
    id: Uuid7
    direction: TransferDirection
    purpose: TransferPurpose
    src_device: str | None = Field(default=None, min_length=1, max_length=64)
    src_path: str | None = Field(default=None, min_length=1, max_length=4096)
    dst_device: str | None = Field(default=None, min_length=1, max_length=64)
    dst_path: str | None = Field(default=None, min_length=1, max_length=4096)
    total_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, min_length=64, max_length=64)
    mime: str | None = Field(default=None, min_length=1, max_length=256)
    etag: OpaqueTag | None = None
    if_match: OpaqueTag | None = None
    if_none_match: bool | None = Field(default=None, strict=True)

    _src_path = field_validator("src_path")(_validate_transfer_path)
    _dst_path = field_validator("dst_path")(_validate_transfer_path)

    @field_validator("sha256")
    @classmethod
    def _normalize_sha256(cls, value: str | None) -> str | None:
        return _valid_sha256(value)

    _etag = field_validator("etag", "if_match")(_validate_opaque_tag)

    @field_validator("total_bytes")
    @classmethod
    def _require_file_size(cls, value: int | None) -> int | None:
        # ``null`` is reserved for an HTTP request body without a length.
        # The transfer manager applies the purpose-specific restriction.
        return value

    @model_validator(mode="after")
    def _validate_purpose_fields(self) -> TransferBeginFrame:
        if self.purpose == "workspace_upload":
            if (
                self.direction != "server_to_client"
                or self.src_path is not None
                or self.dst_path is None
            ):
                raise ValueError("workspace_upload begin fields are inconsistent")
            if self.etag is not None:
                raise ValueError("workspace_upload begin must not carry source etag")
            if self.if_match is not None and self.if_none_match is not None:
                raise ValueError("workspace_upload preconditions are mutually exclusive")
            return self
        if self.purpose == "http_relay":
            if (
                self.direction != "client_to_server"
                or self.src_path is None
                or self.dst_path is not None
                or self.dst_device is not None
                or self.total_bytes is None
            ):
                raise ValueError("http_relay begin fields are inconsistent")
            if self.if_match is not None or self.if_none_match is not None:
                raise ValueError("http_relay begin must not carry destination preconditions")
            return self
        if self.src_path is None or self.dst_path is None or self.total_bytes is None:
            raise ValueError("file_transfer begin requires paths and total_bytes")
        if self.if_match is not None or self.if_none_match is not None:
            raise ValueError("file_transfer begin must not carry destination preconditions")
        return self


class TransferReadyFrame(ProtocolModel):
    type: Literal["transfer_ready"] = "transfer_ready"
    id: Uuid7


class TransferProgressFrame(ProtocolModel):
    type: Literal["transfer_progress"] = "transfer_progress"
    id: Uuid7
    bytes_sent: int = Field(ge=0)


class TransferEndFrame(ProtocolModel):
    type: Literal["transfer_end"] = "transfer_end"
    id: Uuid7
    ack: bool
    ok: bool
    code: str | None = Field(default=None, min_length=1, max_length=128)
    bytes_sent: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, min_length=64, max_length=64)
    etag: OpaqueTag | None = None
    created: bool | None = Field(default=None, strict=True)

    @field_validator("sha256")
    @classmethod
    def _normalize_sha256(cls, value: str | None) -> str | None:
        return _valid_sha256(value)

    _etag = field_validator("etag")(_validate_opaque_tag)

    @model_validator(mode="after")
    def _validate_terminal_fields(self) -> TransferEndFrame:
        if self.ok:
            if self.code is not None or self.bytes_sent is None or self.sha256 is None:
                raise ValueError("successful transfer_end requires size and digest only")
            if (self.etag is None) != (self.created is None):
                raise ValueError("transfer metadata requires both etag and created")
            if not self.ack and (self.etag is not None or self.created is not None):
                raise ValueError("transfer metadata is only valid on a successful ACK")
        elif self.code is None or self.bytes_sent is not None or self.sha256 is not None:
            raise ValueError("failed transfer_end requires only a code")
        elif self.etag is not None or self.created is not None:
            raise ValueError("failed transfer ends must not carry metadata")
        return self


TransferClientFrame = Annotated[
    TransferBeginFrame | TransferReadyFrame | TransferProgressFrame | TransferEndFrame,
    Field(discriminator="type"),
]
TransferServerFrame = Annotated[
    TransferRequestFrame
    | TransferBeginFrame
    | TransferReadyFrame
    | TransferProgressFrame
    | TransferEndFrame,
    Field(discriminator="type"),
]


ClientFrame = Annotated[
    HelloFrame
    | ConfigAppliedFrame
    | ConfigValidateResultFrame
    | RegisterMcpFrame
    | ToolResultFrame
    | PongFrame
    | ErrorFrame
    | TransferBeginFrame
    | TransferReadyFrame
    | TransferProgressFrame
    | TransferEndFrame,
    Field(discriminator="type"),
]
ServerFrame = Annotated[
    HelloAckFrame
    | ConfigUpdateFrame
    | ConfigAppliedAckFrame
    | ConfigValidateFrame
    | ConfigValidateCancelFrame
    | RegisterMcpAckFrame
    | ToolCallFrame
    | PingFrame
    | ErrorFrame
    | TransferRequestFrame
    | TransferBeginFrame
    | TransferReadyFrame
    | TransferProgressFrame
    | TransferEndFrame,
    Field(discriminator="type"),
]

_CLIENT_FRAME_ADAPTER: TypeAdapter[ClientFrame] = TypeAdapter(ClientFrame)
_SERVER_FRAME_ADAPTER: TypeAdapter[ServerFrame] = TypeAdapter(ServerFrame)


def _device_config_wire_dict(
    config: DeviceConfigFrame,
    *,
    exclude_none: bool,
) -> dict[str, Any]:
    payload = config.model_dump(mode="json", exclude_none=exclude_none)
    payload["mcp_servers"] = [
        {
            key: value
            for key, value in server.storage_dict().items()
            if value is not None or not exclude_none
        }
        for server in config.mcp_servers
    ]
    return payload


def frame_to_wire_dict(
    frame: ClientFrame | ServerFrame,
    *,
    exclude_none: bool = False,
) -> dict[str, Any]:
    """Project one validated frame to its wire form, revealing only wire-bound secrets."""

    payload = frame.model_dump(mode="json", exclude_none=exclude_none)
    if isinstance(frame, (HelloAckFrame, ConfigUpdateFrame)):
        payload["config"] = _device_config_wire_dict(
            frame.config,
            exclude_none=exclude_none,
        )
    elif isinstance(frame, ConfigValidateFrame):
        payload["candidate_config"] = _device_config_wire_dict(
            frame.candidate_config,
            exclude_none=exclude_none,
        )
    return payload


def encode_server_frame(frame: ServerFrame) -> str:
    return json.dumps(
        frame_to_wire_dict(frame),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def parse_client_frame(
    payload: str,
) -> (
    HelloFrame
    | ConfigAppliedFrame
    | ConfigValidateResultFrame
    | RegisterMcpFrame
    | ToolResultFrame
    | PongFrame
    | ErrorFrame
    | TransferBeginFrame
    | TransferReadyFrame
    | TransferProgressFrame
    | TransferEndFrame
):
    _validate_text_frame_size(payload)
    return _CLIENT_FRAME_ADAPTER.validate_json(payload)


def parse_server_frame(payload: str) -> ServerFrame:
    _validate_text_frame_size(payload)
    return _SERVER_FRAME_ADAPTER.validate_json(payload)


def _validate_text_frame_size(payload: str) -> None:
    if len(payload.encode("utf-8")) > MAX_TEXT_FRAME_BYTES:
        raise ValueError("Control frame exceeds the maximum size")


def encode_binary_chunk(slot_id: UUID, payload: bytes) -> bytes:
    if slot_id.version != 7:
        raise ValueError("transfer slot IDs must be UUID v7")
    if len(payload) > MAX_BINARY_CHUNK_BYTES:
        raise ValueError("transfer chunk exceeds 64 KiB")
    return slot_id.bytes + payload


def decode_binary_chunk(frame: bytes) -> tuple[UUID, bytes]:
    if len(frame) < 16:
        raise ValueError("binary transfer frame is missing its slot ID")
    if len(frame) > 16 + MAX_BINARY_CHUNK_BYTES:
        raise ValueError("binary transfer frame exceeds 64 KiB")
    slot_id = UUID(bytes=frame[:16])
    if slot_id.version != 7:
        raise ValueError("transfer slot IDs must be UUID v7")
    return slot_id, frame[16:]


# Short names are useful to callers that construct transfer state machines;
# the ``*Frame`` names remain the canonical wire DTOs.
TransferRequest = TransferRequestFrame
TransferBegin = TransferBeginFrame
TransferReady = TransferReadyFrame
TransferProgress = TransferProgressFrame
TransferEnd = TransferEndFrame
