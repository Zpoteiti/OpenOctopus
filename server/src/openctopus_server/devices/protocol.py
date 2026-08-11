from __future__ import annotations

import base64
import binascii
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

PROTOCOL_VERSION = "1"
MAX_TEXT_FRAME_BYTES = 12 * 1024 * 1024
MAX_BINARY_CHUNK_BYTES = 64 * 1024
TRANSFER_PURPOSES = ("file_transfer", "workspace_upload", "http_relay")


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
    model_config = ConfigDict(extra="forbid", strict=True)


class DeviceCapabilities(ProtocolModel):
    shared_tools: Literal[True] = True
    web_fetch: Literal[True] = True
    file_transfer: tuple[Literal["send", "receive"], Literal["send", "receive"]] = (
        "send",
        "receive",
    )
    http_relay: Literal[True] = True
    exec: Literal[False] = False
    mcp: Literal[False] = False

    @field_validator("file_transfer")
    @classmethod
    def _require_both_transfer_directions(
        cls,
        value: tuple[Literal["send", "receive"], Literal["send", "receive"]],
    ) -> tuple[Literal["send", "receive"], Literal["send", "receive"]]:
        if value != ("send", "receive"):
            raise ValueError("Py5 clients must support send and receive")
        return value


class DeviceConfigFrame(ProtocolModel):
    workspace_path: str = Field(min_length=1, max_length=4096)
    sandbox_mode: bool
    ssrf_denylist: list[str] = Field(max_length=256)

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


class HelloFrame(ProtocolModel):
    type: Literal["hello"] = "hello"
    id: Uuid7
    version: Literal["1"]
    client_version: str = Field(min_length=1, max_length=64)
    os: Literal["linux", "darwin", "windows"]
    caps: DeviceCapabilities


class HelloAckFrame(ProtocolModel):
    type: Literal["hello_ack"] = "hello_ack"
    id: Uuid7
    device_name: str = Field(min_length=1, max_length=64)
    config: DeviceConfigFrame


class ConfigUpdateFrame(ProtocolModel):
    type: Literal["config_update"] = "config_update"
    id: Uuid7
    device_name: str = Field(min_length=1, max_length=64)
    config: DeviceConfigFrame


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


class ToolCallFrame(ProtocolModel):
    type: Literal["tool_call"] = "tool_call"
    id: Uuid7
    name: str = Field(min_length=1, max_length=128)
    args: dict[str, Any]
    max_result_bytes: int = Field(ge=1, le=MAX_TEXT_FRAME_BYTES)


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


# ETags are opaque protocol values.  The Py5 client currently emits a hashed
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
    TransferBeginFrame
    | TransferReadyFrame
    | TransferProgressFrame
    | TransferEndFrame,
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


def parse_client_frame(
    payload: str,
) -> (
    HelloFrame
    | ToolResultFrame
    | PongFrame
    | ErrorFrame
    | TransferBeginFrame
    | TransferReadyFrame
    | TransferProgressFrame
    | TransferEndFrame
):
    return _CLIENT_FRAME_ADAPTER.validate_json(payload)


def parse_server_frame(payload: str) -> ServerFrame:
    return _SERVER_FRAME_ADAPTER.validate_json(payload)


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
