from __future__ import annotations

import base64
import binascii
import json
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
    ValidationError,
    field_validator,
    model_validator,
)

CONTROL_FRAME_MAX_BYTES = 12 * 1024 * 1024
MAX_BINARY_CHUNK_BYTES = 64 * 1024
BINARY_SLOT_ID_BYTES = 16
MAX_BINARY_FRAME_BYTES = BINARY_SLOT_ID_BYTES + MAX_BINARY_CHUNK_BYTES
PROTOCOL_VERSION: Literal["2"] = "2"
EXEC_TOOL_NAMES = frozenset({"exec", "write_stdin", "list_exec_sessions"})


def _require_uuid7(value: UUID) -> UUID:
    if value.version != 7:
        raise ValueError("protocol IDs must be UUID v7")
    return value


Uuid7 = Annotated[UUID, AfterValidator(_require_uuid7)]


class ProtocolError(ValueError):
    """A peer frame did not satisfy Protocol v2."""


class _Frame(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class DeviceConfig(_Frame):
    workspace_path: Annotated[str, Field(min_length=1, max_length=4096)]
    sandbox_mode: bool
    ssrf_denylist: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=512)]], Field(max_length=256)
    ]
    shell_timeout_max: Annotated[int, Field(ge=0, le=86400)]
    env_allowlist: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=128)]], Field(max_length=64)
    ]

    @field_validator("workspace_path")
    @classmethod
    def _workspace_path_has_no_nul(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("workspace_path must not contain NUL")
        if not value.strip():
            raise ValueError("workspace_path must not be blank")
        return value

    @field_validator("ssrf_denylist")
    @classmethod
    def _denylist_has_no_nul(cls, value: list[str]) -> list[str]:
        if any("\x00" in item for item in value):
            raise ValueError("ssrf_denylist entries must not contain NUL")
        if any(not item.strip() for item in value):
            raise ValueError("ssrf_denylist entries must not be blank")
        return value

    @field_validator("env_allowlist")
    @classmethod
    def _validate_env_allowlist(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("env_allowlist entries must be unique")
        for entry in value:
            if (
                entry.strip() != entry
                or any(ord(character) < 0x20 for character in entry)
                or "=" in entry
                or entry.upper().startswith("OPENOCTOPUS_")
            ):
                raise ValueError("env_allowlist contains an invalid variable name")
        return value


class Capabilities(_Frame):
    shared_tools: Literal[True] = True
    web_fetch: Literal[True] = True
    file_transfer: list[Literal["send", "receive"]] = ["send", "receive"]
    http_relay: Literal[True] = True

    def model_post_init(self, __context: Any) -> None:
        if self.file_transfer != ["send", "receive"]:
            raise ValueError("file_transfer must be send and receive")


class ShellMetadata(_Frame):
    default: Annotated[str, Field(min_length=1, max_length=32)]
    available: Annotated[
        list[Annotated[str, Field(min_length=1, max_length=32)]],
        Field(min_length=1, max_length=16),
    ]

    @model_validator(mode="after")
    def _validate_shells(self) -> ShellMetadata:
        if len(self.available) != len(set(self.available)):
            raise ValueError("shells.available must not contain duplicates")
        if self.default not in self.available:
            raise ValueError("shells.default must be listed in shells.available")
        return self


class Hello(_Frame):
    type: Literal["hello"] = "hello"
    id: Uuid7
    version: Literal["2"] = PROTOCOL_VERSION
    client_version: Annotated[str, Field(min_length=1, max_length=64)]
    os: Literal["linux", "darwin", "windows"]
    caps: Capabilities = Capabilities()
    shells: ShellMetadata

    @classmethod
    def new(
        cls,
        *,
        client_version: str,
        operating_system: Literal["linux", "darwin", "windows"],
        shells: ShellMetadata,
    ) -> Hello:
        return cls(
            id=new_uuid7(),
            client_version=client_version,
            os=operating_system,
            shells=shells,
        )

    @classmethod
    def new_with_id(
        cls,
        frame_id: UUID,
        client_version: str,
        operating_system: Literal["linux", "darwin", "windows"],
        shells: ShellMetadata,
    ) -> Hello:
        return cls(
            id=frame_id,
            client_version=client_version,
            os=operating_system,
            shells=shells,
        )


class HelloAck(_Frame):
    type: Literal["hello_ack"] = "hello_ack"
    id: Uuid7
    device_name: Annotated[str, Field(min_length=1, max_length=64)]
    config: DeviceConfig


class ConfigUpdate(_Frame):
    type: Literal["config_update"] = "config_update"
    id: Uuid7
    device_name: Annotated[str, Field(min_length=1, max_length=64)]
    config: DeviceConfig


class Ping(_Frame):
    type: Literal["ping"] = "ping"
    id: Uuid7


class Pong(_Frame):
    type: Literal["pong"] = "pong"
    id: Uuid7


class ToolCall(_Frame):
    type: Literal["tool_call"] = "tool_call"
    id: Uuid7
    name: Annotated[str, Field(min_length=1, max_length=128)]
    args: dict[str, Any]
    max_result_bytes: Annotated[int, Field(ge=1, le=CONTROL_FRAME_MAX_BYTES)]
    chat_session_id: UUID | None = None

    @model_validator(mode="after")
    def _require_exec_owner(self) -> ToolCall:
        if self.name in EXEC_TOOL_NAMES and self.chat_session_id is None:
            raise ValueError("exec tool calls require chat_session_id")
        return self


class _TextResultBlock(_Frame):
    type: Literal["text"]
    text: str


class _Base64ImageSource(_Frame):
    type: Literal["base64"]
    media_type: Literal["image/jpeg", "image/png", "image/gif", "image/webp"]
    data: Annotated[str, Field(min_length=4, max_length=CONTROL_FRAME_MAX_BYTES)]

    @field_validator("data")
    @classmethod
    def _validate_data(cls, value: str) -> str:
        try:
            base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("image data must be valid base64") from exc
        return value


class _ImageResultBlock(_Frame):
    type: Literal["image"]
    source: _Base64ImageSource


SafeResultBlock = Annotated[
    _TextResultBlock | _ImageResultBlock,
    Field(discriminator="type"),
]


class ToolResult(_Frame):
    type: Literal["tool_result"] = "tool_result"
    id: Uuid7
    content: str | list[SafeResultBlock]
    is_error: bool
    code: Annotated[str, Field(min_length=1, max_length=128)] | None = None

    @model_validator(mode="after")
    def _validate_error_code(self) -> ToolResult:
        if self.is_error and self.code is None:
            raise ValueError("error tool results require a code")
        if not self.is_error and self.code is not None:
            raise ValueError("successful tool results must not carry a code")
        return self


class ErrorFrame(_Frame):
    type: Literal["error"] = "error"
    id: Uuid7 | None = None
    code: Annotated[str, Field(min_length=1, max_length=128)]
    message: Annotated[str, Field(min_length=1, max_length=4096)]


TransferPurpose = Literal["file_transfer", "workspace_upload", "http_relay"]
TransferDirection = Literal["client_to_server", "server_to_client"]


def _validate_path(value: str | None) -> str | None:
    if value is None:
        return None
    if "\x00" in value or not value.strip():
        raise ValueError("transfer paths must be non-empty and contain no NUL")
    return value


Digest = Annotated[str, Field(min_length=64, max_length=64, pattern=r"^[0-9a-fA-F]{64}$")]


# ETags are opaque on the wire.  The client currently emits a SHA-256
# fingerprint, but keeping the DTO opaque lets the server compare it without
# making filesystem identity part of the protocol contract.
OpaqueTag = Annotated[str, Field(min_length=1, max_length=512, strict=True)]


def _validate_opaque_tag(value: str | None) -> str | None:
    if value is None:
        return None
    if any(character in {'"', "\x00"} or not 0x21 <= ord(character) <= 0x7E for character in value):
        raise ValueError("ETag must contain visible ASCII without quotes")
    return value


class TransferRequest(_Frame):
    """A server request for the client to become the byte sender."""

    type: Literal["transfer_request"] = "transfer_request"
    id: Uuid7
    purpose: TransferPurpose
    src_path: Annotated[str, Field(min_length=1, max_length=4096)]
    dst_path: Annotated[str, Field(min_length=1, max_length=4096)] | None = None

    _src_path = field_validator("src_path")(_validate_path)
    _dst_path = field_validator("dst_path")(_validate_path)

    @model_validator(mode="after")
    def _validate_purpose(self) -> TransferRequest:
        if self.purpose == "workspace_upload":
            raise ValueError("workspace_upload starts with transfer_begin")
        if self.purpose == "file_transfer" and self.dst_path is None:
            raise ValueError("file_transfer request requires dst_path")
        if self.purpose == "http_relay" and self.dst_path is not None:
            raise ValueError("http_relay request must not include dst_path")
        return self


class TransferBegin(_Frame):
    type: Literal["transfer_begin"] = "transfer_begin"
    id: Uuid7
    direction: TransferDirection
    purpose: TransferPurpose
    src_device: Annotated[str, Field(min_length=1, max_length=64)] | None = None
    src_path: Annotated[str, Field(min_length=1, max_length=4096)] | None = None
    dst_device: Annotated[str, Field(min_length=1, max_length=64)] | None = None
    dst_path: Annotated[str, Field(min_length=1, max_length=4096)] | None = None
    total_bytes: Annotated[int, Field(ge=0)] | None = None
    sha256: Digest | None = None
    mime: Annotated[str, Field(min_length=1, max_length=256)] | None = None
    etag: OpaqueTag | None = None
    if_match: OpaqueTag | None = None
    if_none_match: bool | None = Field(default=None, strict=True)

    _src_path = field_validator("src_path")(_validate_path)
    _dst_path = field_validator("dst_path")(_validate_path)

    @field_validator("sha256")
    @classmethod
    def _normalise_digest(cls, value: str | None) -> str | None:
        return value.lower() if value is not None else None

    _etag = field_validator("etag", "if_match")(_validate_opaque_tag)

    @model_validator(mode="after")
    def _validate_purpose_fields(self) -> TransferBegin:
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


class TransferReady(_Frame):
    type: Literal["transfer_ready"] = "transfer_ready"
    id: Uuid7


class TransferProgress(_Frame):
    type: Literal["transfer_progress"] = "transfer_progress"
    id: Uuid7
    bytes_sent: Annotated[int, Field(ge=0)]


class TransferEnd(_Frame):
    type: Literal["transfer_end"] = "transfer_end"
    id: Uuid7
    ack: bool
    ok: bool
    code: Annotated[str, Field(min_length=1, max_length=128)] | None = None
    bytes_sent: Annotated[int, Field(ge=0)] | None = None
    sha256: Digest | None = None
    etag: OpaqueTag | None = None
    created: bool | None = Field(default=None, strict=True)

    @field_validator("sha256")
    @classmethod
    def _normalise_digest(cls, value: str | None) -> str | None:
        return value.lower() if value is not None else None

    _etag = field_validator("etag")(_validate_opaque_tag)

    def model_post_init(self, __context: Any) -> None:
        if self.ok:
            if self.code is not None or self.bytes_sent is None or self.sha256 is None:
                raise ValueError("successful transfer ends require size and digest only")
            if (self.etag is None) != (self.created is None):
                raise ValueError("transfer metadata requires both etag and created")
            if not self.ack and (self.etag is not None or self.created is not None):
                raise ValueError("transfer metadata is only valid on a successful ACK")
        elif self.code is None or self.bytes_sent is not None or self.sha256 is not None:
            raise ValueError("failed transfer ends require only a code")
        elif self.etag is not None or self.created is not None:
            raise ValueError("failed transfer ends must not carry metadata")


type TransferFrame = (
    TransferRequest | TransferBegin | TransferReady | TransferProgress | TransferEnd
)
type ServerFrame = HelloAck | ConfigUpdate | Ping | ToolCall | TransferFrame | ErrorFrame
type ClientFrame = (
    Hello
    | Pong
    | ToolResult
    | TransferBegin
    | TransferReady
    | TransferProgress
    | TransferEnd
    | ErrorFrame
)
_SERVER_FRAMES: TypeAdapter[ServerFrame] = TypeAdapter(
    Annotated[ServerFrame, Field(discriminator="type")]
)


def new_uuid7() -> UUID:
    """Generate a UUIDv7 without relying on a newer Python stdlib."""

    milliseconds = int(time.time_ns() // 1_000_000) & ((1 << 48) - 1)
    value = milliseconds << 80
    value |= 0x7 << 76
    value |= secrets.randbits(12) << 64
    value |= 0b10 << 62
    value |= secrets.randbits(62)
    return UUID(int=value)


def encode_frame(frame: ClientFrame) -> str:
    return frame.model_dump_json(exclude_none=True)


def decode_server_frame(payload: str) -> ServerFrame:
    if len(payload.encode("utf-8")) > CONTROL_FRAME_MAX_BYTES:
        raise ProtocolError("Control frame exceeds the maximum size")
    try:
        return _SERVER_FRAMES.validate_json(payload)
    except (ValidationError, ValueError, json.JSONDecodeError) as exc:
        raise ProtocolError("Invalid server frame") from exc


def encode_binary_chunk(slot_id: UUID, payload: bytes) -> bytes:
    """Encode one bounded transfer chunk with its 16-byte UUID header."""

    if slot_id.version != 7:
        raise ProtocolError("Transfer slot IDs must be UUID v7")
    if len(payload) > MAX_BINARY_CHUNK_BYTES:
        raise ProtocolError("Transfer chunk exceeds the maximum size")
    return slot_id.bytes + payload


def decode_binary_chunk(payload: bytes) -> tuple[UUID, bytes]:
    """Validate and split one transfer binary frame."""

    if len(payload) < BINARY_SLOT_ID_BYTES:
        raise ProtocolError("Transfer binary frame is missing its slot ID")
    if len(payload) > MAX_BINARY_FRAME_BYTES:
        raise ProtocolError("Transfer binary frame exceeds the maximum size")
    try:
        slot_id = UUID(bytes=payload[:BINARY_SLOT_ID_BYTES])
    except ValueError as exc:
        raise ProtocolError("Transfer binary frame has an invalid slot ID") from exc
    if slot_id.version != 7:
        raise ProtocolError("Transfer slot IDs must be UUID v7")
    return slot_id, payload[BINARY_SLOT_ID_BYTES:]
