"""Private line-oriented JSON channel between the tray GUI and the core.

This is an in-package interface, not part of Server Protocol v3.  The GUI
sends exactly one startup configuration (with the in-memory token) and may
send stop commands; the core answers with structured status events and one
terminal exit result.  Diagnostics go to stderr only and are never parsed for
state.  Tokens, MCP secrets, command bodies, and file contents never travel
on these events.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)

MAX_MESSAGE_BYTES = 64 * 1024
"""Hard bound for one inbound or outbound channel message."""


class ChannelError(ValueError):
    """An inbound or outbound channel message is invalid or oversized."""


class CoreState(StrEnum):
    """Structured lifecycle states reported by the core over the channel."""

    CONNECTING = "connecting"
    ONLINE = "online"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"


class CoreFailureCode(StrEnum):
    """Stable, sanitized categories for core failures."""

    SERVER_UNREACHABLE = "server_unreachable"
    AUTH_REJECTED = "auth_rejected"
    CONNECTION_REPLACED = "connection_replaced"
    CONFIG_REJECTED = "config_rejected"
    STARTUP_CONFIG_INVALID = "startup_config_invalid"
    PIPELINE_FAILED = "pipeline_failed"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StartupConfigMessage(_StrictModel):
    """The single startup configuration from the GUI, carrying the token.

    The token exists only here, in memory, on the private stdin pipe.  It is
    never written to a command line, an environment variable, a log line, or
    any event produced afterwards.
    """

    type: Literal["startup-config"]
    generation: Annotated[int, Field(ge=0, le=2_147_483_647)]
    server_url: Annotated[str, Field(min_length=1, max_length=4096)]
    token: Annotated[str, Field(min_length=1, max_length=4096)]
    workspace_root: Annotated[str, Field(min_length=1, max_length=4096)] | None = None

    def __repr__(self) -> str:
        return (
            f"StartupConfigMessage(type={self.type!r}, generation={self.generation!r}, "
            f"server_url={self.server_url!r}, token=DeviceToken(<redacted>))"
        )

    __str__ = __repr__


class StopCommandMessage(_StrictModel):
    """Ask the core to run its existing full stop flow."""

    type: Literal["stop"]


ExitReason = Literal[
    "stopped",
    "auth_rejected",
    "connection_replaced",
    "config_rejected",
    "startup_config_invalid",
    "owner_gone",
]


class StatusEventMessage(_StrictModel):
    """Structured status event emitted by the core; never carries secrets."""

    type: Literal["status"]
    generation: Annotated[int, Field(ge=0, le=2_147_483_647)]
    state: CoreState
    device_name: Annotated[str | None, Field(default=None, max_length=256)] = None
    error_code: CoreFailureCode | None = None
    retry_in_seconds: Annotated[float | None, Field(default=None, ge=0.0, le=300.0)] = None
    attempt: Annotated[int | None, Field(default=None, ge=0)] = None

    @field_validator("retry_in_seconds")
    @classmethod
    def _finite(cls, value: float | None) -> float | None:
        if value is not None and (value != value or value in (float("inf"), float("-inf"))):
            raise ValueError("retry_in_seconds must be finite")
        return value


class ExitResultMessage(_StrictModel):
    """Terminal result reported by a core process before it exits."""

    type: Literal["exit"]
    generation: Annotated[int, Field(ge=0, le=2_147_483_647)]
    return_code: Annotated[int, Field(ge=0, le=255)]
    cleanup_complete: bool
    reason: ExitReason


GuiMessage = StartupConfigMessage | StopCommandMessage
CoreEvent = StatusEventMessage | ExitResultMessage


def encode_message(message: BaseModel) -> str:
    """Serialize one channel message as a single UTF-8 JSON line."""

    line = message.model_dump_json(exclude_none=True)
    if len(line.encode("utf-8")) + 1 > MAX_MESSAGE_BYTES:
        raise ChannelError("channel message exceeds the size bound")
    return line


def parse_gui_message(raw: bytes | str) -> GuiMessage:
    """Parse one stdin line into a GUI->core message."""

    return cast(GuiMessage, _parse(raw, _GUI_MESSAGE_TYPES, "GUI message"))


def parse_core_event(raw: bytes | str) -> CoreEvent:
    """Parse one stdout line into a core->GUI event."""

    return cast(CoreEvent, _parse(raw, _CORE_EVENT_TYPES, "core event"))


_GUI_MESSAGE_TYPES: dict[str, type[GuiMessage]] = {
    "startup-config": StartupConfigMessage,
    "stop": StopCommandMessage,
}
_CORE_EVENT_TYPES: dict[str, type[CoreEvent]] = {
    "status": StatusEventMessage,
    "exit": ExitResultMessage,
}


def _parse(raw: bytes | str, models: Mapping[str, type[BaseModel]], label: str) -> BaseModel:
    if isinstance(raw, bytes):
        if len(raw) > MAX_MESSAGE_BYTES:
            raise ChannelError(f"{label} exceeds the size bound")
        try:
            text = raw.decode("utf-8", errors="strict")
        except UnicodeError as exc:
            raise ChannelError(f"{label} is not valid UTF-8") from exc
    else:
        text = raw
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ChannelError(f"{label} is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ChannelError(f"{label} is not a JSON object")
    kind = payload.get("type")
    model = models.get(kind) if isinstance(kind, str) else None
    if model is None:
        raise ChannelError(f"{label} has an unknown type")
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        raise ChannelError(f"{label} failed validation") from exc
