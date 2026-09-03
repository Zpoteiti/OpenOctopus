from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from openctopus_server.channels.types import ChannelName, ToolProfile
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.truncate import DEFAULT_MAX_TOOL_RESULT_CHARS

type ToolResultContentBlock = dict[str, Any]
type RawToolResultContent = str | list[ToolResultContentBlock]


class ToolRoutingMode(StrEnum):
    ROUTING_ONLY = "routing_only"
    CLIENT_ONLY = "client_only"
    INTRINSIC_DEVICE = "intrinsic_device"
    PURE_SERVER = "pure_server"


@dataclass(frozen=True, slots=True)
class WorkspaceFileDeliveryRef:
    path: str
    workspace_id: UUID
    workspace_relative_path: str
    filename: str
    mime: str
    size: int
    type: Literal["workspace_file"] = "workspace_file"
    openoctopus_device: Literal["server"] = "server"
    online_only: Literal[False] = False


@dataclass(frozen=True, slots=True)
class DeviceFileDeliveryRef:
    path: str
    device_id: UUID
    openoctopus_device: str
    filename: str
    mime: str
    size: int
    fingerprint: str
    type: Literal["device_file"] = "device_file"
    online_only: Literal[True] = True


type DeliveryRef = WorkspaceFileDeliveryRef | DeviceFileDeliveryRef


@dataclass(frozen=True, slots=True)
class MessageDeliveryEffect:
    delivery_refs: tuple[DeliveryRef, ...]


@dataclass(frozen=True, slots=True)
class ToolResult:
    content: RawToolResultContent
    is_error: bool = False
    code: ErrorCode | None = None
    side_effect: MessageDeliveryEffect | None = None


@dataclass(frozen=True, slots=True)
class ToolContext:
    user_id: UUID
    session_id: UUID
    openoctopus_device: str | None = None
    device_targets: Mapping[str, UUID] | None = None
    on_issued: Callable[[], None] | None = None
    turn_id: UUID | None = None
    tool_use_id: str | None = None
    assistant_message_id: UUID | None = None
    tool_profile: ToolProfile = "owner_full"
    current_channel: ChannelName | None = None
    current_chat_id: str | None = None
    current_binding_generation: UUID | None = None


class Tool(ABC):
    routing_mode = ToolRoutingMode.ROUTING_ONLY
    manages_issue_boundary = False

    @abstractmethod
    def name(self) -> str:
        """Return the provider-visible tool name."""

    @abstractmethod
    def schema(self) -> dict[str, Any]:
        """Return the source schema before OpenOctopus routing fields are added."""

    def schema_for_profile(self, tool_profile: ToolProfile) -> dict[str, Any] | None:
        """Project this tool into a Provider-visible profile."""
        if tool_profile == "owner_full":
            return self.schema()
        return None

    def max_output_chars(self) -> int:
        return DEFAULT_MAX_TOOL_RESULT_CHARS

    @abstractmethod
    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        """Execute one validated, locally routed tool call."""
