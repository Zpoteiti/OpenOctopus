from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.truncate import DEFAULT_MAX_TOOL_RESULT_CHARS

type ToolResultContentBlock = dict[str, Any]
type RawToolResultContent = str | list[ToolResultContentBlock]


class ToolRoutingMode(StrEnum):
    ROUTING_ONLY = "routing_only"
    INTRINSIC_DEVICE = "intrinsic_device"
    PURE_SERVER = "pure_server"


@dataclass(frozen=True, slots=True)
class ToolResult:
    content: RawToolResultContent
    is_error: bool = False
    code: ErrorCode | None = None


@dataclass(frozen=True, slots=True)
class ToolContext:
    user_id: UUID
    session_id: UUID
    openoctopus_device: str | None = None


class Tool(ABC):
    routing_mode = ToolRoutingMode.ROUTING_ONLY

    @abstractmethod
    def name(self) -> str:
        """Return the provider-visible tool name."""

    @abstractmethod
    def schema(self) -> dict[str, Any]:
        """Return the source schema before OpenOctopus routing fields are added."""

    def max_output_chars(self) -> int:
        return DEFAULT_MAX_TOOL_RESULT_CHARS

    @abstractmethod
    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        """Execute one validated, locally routed tool call."""
