from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from ..attachments import AuthenticatedAttachmentStream
from ..delivery import ActionResult
from ..types import (
    ChannelCapabilities,
    ChannelContextMessage,
    ChannelEvent,
    DeliveryAction,
    DeliveryPlan,
    ExternalAttachmentDescriptor,
    ExternalChannel,
    OutboundMessage,
)

type ContextFetchStatus = Literal["available", "unsupported", "failed"]
type ActionIssueHook = Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ContextFetchResult:
    status: ContextFetchStatus
    messages: tuple[ChannelContextMessage, ...] = ()
    error_code: str | None = None
    error_message: str | None = None


class ChannelEventSink(Protocol):
    async def __call__(self, event: ChannelEvent) -> object | None: ...


class ChannelAdapter(Protocol):
    """Delivery surface whose results contain no raw platform response."""

    platform: ExternalChannel
    capabilities: ChannelCapabilities

    async def start(self, sink: ChannelEventSink) -> None: ...

    async def wait_closed(self) -> None: ...

    async def stop(self) -> None: ...

    async def open_authenticated_attachment(
        self,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream: ...

    async def fetch_recent_context(
        self,
        *,
        chat_id: str,
        before_message_id: str,
        limit: Literal[100],
    ) -> ContextFetchResult: ...

    def plan_delivery(self, message: OutboundMessage) -> DeliveryPlan: ...

    async def execute_action(
        self,
        action: DeliveryAction,
        *,
        on_issued: ActionIssueHook,
    ) -> ActionResult: ...
