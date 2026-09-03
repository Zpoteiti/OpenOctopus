from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import UUID

from openctopus_server.provider.wire_types import Effort

type ToolProfile = Literal["owner_full", "message_only"]
type SenderClassification = Literal["owner", "allowed_non_owner", "internal"]
type ChannelName = Literal["web", "cron", "heartbeat", "discord", "dingtalk"]
type ExternalChannel = Literal["discord", "dingtalk"]
type ConversationKind = Literal["dm", "group", "thread"]
type ChannelSenderKind = Literal["human", "bot", "webhook"]
type DeliveryOrigin = Literal[
    "final",
    "message_tool",
    "policy_notice",
    "pairing_confirmation",
]
type DeliveryActionKind = Literal["text_message", "file_upload", "file_message"]


@dataclass(frozen=True, slots=True)
class ExternalAttachmentDescriptor:
    """Platform attachment facts that are safe to pass beyond an SDK callback."""

    source_id: str
    filename: str
    content_type: str | None
    size: int | None


@dataclass(frozen=True, slots=True)
class ChannelContextMessage:
    source_message_id: str | None
    sender_id: str | None
    sender_display_name: str | None
    sent_at: str | None
    text: str
    attachment_summaries: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ChannelEvent:
    """External adapter facts before owner and authority resolution."""

    platform: ExternalChannel
    binding_generation: UUID
    runtime_generation: UUID
    source_message_id: str
    chat_id: str
    conversation_kind: ConversationKind
    sender_id: str
    sender_display_name: str | None
    sender_kind: ChannelSenderKind
    explicitly_mentions_bot: bool
    text: str
    attachments: tuple[ExternalAttachmentDescriptor, ...]
    reply_context: tuple[ChannelContextMessage, ...] = ()
    conversation_label: str | None = None


@dataclass(frozen=True, slots=True)
class InboundSender:
    id: str
    display_name: str | None
    classification: SenderClassification


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """Authoritative ingress envelope after Server-side route and identity resolution."""

    message_id: UUID
    owner_user_id: UUID
    session_id: UUID
    session_key: str
    channel: ChannelName
    chat_id: str
    source_message_id: str | None
    channel_binding_generation: UUID | None
    sender: InboundSender
    ingress_tool_profile: ToolProfile
    content: tuple[dict[str, object], ...]
    attachment_refs: tuple[dict[str, object], ...] = ()
    channel_context: tuple[ChannelContextMessage, ...] = ()
    effort: Effort | None = None


@dataclass(frozen=True, slots=True)
class ChannelCapabilities:
    history_backfill: bool
    file_delivery: bool


class ResolvedDeliveryFile(Protocol):
    """Metadata shared by resolved Workspace and Device delivery sources."""

    filename: str
    mime: str
    size: int | None


@dataclass(frozen=True, slots=True)
class OutboundMessage:
    delivery_key: str
    user_id: UUID
    turn_id: UUID | None
    origin: DeliveryOrigin
    channel: ExternalChannel
    chat_id: str
    binding_generation: UUID
    content: str
    media: tuple[ResolvedDeliveryFile, ...] = ()
    source_channel: ExternalChannel | None = None
    source_binding_generation: UUID | None = None


@dataclass(frozen=True, slots=True)
class DeliveryAction:
    kind: DeliveryActionKind
    visible: bool
    content: str | None = None
    media_index: int | None = None
    chat_id: str | None = None
    idempotency_key: str | None = None
    media: ResolvedDeliveryFile | None = None
    dependency_action_index: int | None = None
    dependency_artifact_id: str | None = None


@dataclass(frozen=True, slots=True)
class DeliveryPlan:
    actions: tuple[DeliveryAction, ...]
