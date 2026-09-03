from dataclasses import FrozenInstanceError, fields
from typing import get_args
from uuid import uuid4

import pytest

from openctopus_server.channels.types import (
    ChannelCapabilities,
    ChannelContextMessage,
    ChannelEvent,
    DeliveryAction,
    DeliveryActionKind,
    DeliveryOrigin,
    DeliveryPlan,
    ExternalAttachmentDescriptor,
    InboundMessage,
    InboundSender,
    OutboundMessage,
    SenderClassification,
    ToolProfile,
)


def test_authority_aliases_are_closed_literal_sets() -> None:
    assert get_args(ToolProfile.__value__) == ("owner_full", "message_only")
    assert get_args(SenderClassification.__value__) == (
        "owner",
        "allowed_non_owner",
        "internal",
    )
    assert get_args(DeliveryOrigin.__value__) == (
        "final",
        "message_tool",
        "policy_notice",
        "pairing_confirmation",
    )
    assert get_args(DeliveryActionKind.__value__) == (
        "text_message",
        "file_upload",
        "file_message",
    )


def test_channel_event_contains_only_adapter_facts() -> None:
    binding_generation = uuid4()
    runtime_generation = uuid4()
    attachment = ExternalAttachmentDescriptor(
        source_id="attachment-1",
        filename="report.pdf",
        content_type="application/pdf",
        size=123,
    )
    context = ChannelContextMessage(
        source_message_id="prior-1",
        sender_id="42",
        sender_display_name="Colleague",
        sent_at="2026-09-02T08:00:00Z",
        text="Earlier context",
        attachment_summaries=("report.pdf (application/pdf)",),
    )
    event = ChannelEvent(
        platform="discord",
        binding_generation=binding_generation,
        runtime_generation=runtime_generation,
        source_message_id="message-1",
        chat_id="channel-1",
        conversation_kind="group",
        sender_id="42",
        sender_display_name="Colleague",
        sender_kind="human",
        explicitly_mentions_bot=True,
        text="What do you think?",
        attachments=(attachment,),
        reply_context=(context,),
    )

    assert tuple(field.name for field in fields(ChannelEvent)) == (
        "platform",
        "binding_generation",
        "runtime_generation",
        "source_message_id",
        "chat_id",
        "conversation_kind",
        "sender_id",
        "sender_display_name",
        "sender_kind",
        "explicitly_mentions_bot",
        "text",
        "attachments",
        "reply_context",
        "conversation_label",
    )
    assert event.attachments == (attachment,)
    assert event.reply_context == (context,)
    with pytest.raises(FrozenInstanceError):
        event.text = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        ChannelEvent(  # type: ignore[call-arg]
            platform="discord",
            binding_generation=binding_generation,
            runtime_generation=runtime_generation,
            source_message_id="message-1",
            chat_id="channel-1",
            conversation_kind="group",
            sender_id="42",
            sender_display_name=None,
            sender_kind="human",
            explicitly_mentions_bot=True,
            text="hello",
            attachments=(),
            owner_user_id=uuid4(),
        )


def test_inbound_message_carries_structured_authority_and_context() -> None:
    user_id = uuid4()
    session_id = uuid4()
    binding_generation = uuid4()
    sender = InboundSender(
        id="42",
        display_name="Colleague",
        classification="allowed_non_owner",
    )
    context = ChannelContextMessage(
        source_message_id=None,
        sender_id=None,
        sender_display_name=None,
        sent_at=None,
        text="Quoted context",
    )
    inbound = InboundMessage(
        message_id=uuid4(),
        owner_user_id=user_id,
        session_id=session_id,
        session_key="discord:application-1:channel-1",
        channel="discord",
        chat_id="channel-1",
        source_message_id="message-1",
        channel_binding_generation=binding_generation,
        sender=sender,
        ingress_tool_profile="message_only",
        content=({"type": "text", "text": "hello"},),
        channel_context=(context,),
    )

    assert inbound.sender is sender
    assert inbound.ingress_tool_profile == "message_only"
    assert inbound.channel_context == (context,)
    assert inbound.attachment_refs == ()
    assert inbound.effort is None


def test_outbound_contract_and_delivery_plan_are_immutable() -> None:
    message = OutboundMessage(
        delivery_key="final:message-1",
        user_id=uuid4(),
        turn_id=uuid4(),
        origin="final",
        channel="dingtalk",
        chat_id="conversation-1",
        binding_generation=uuid4(),
        content="hello",
    )
    plan = DeliveryPlan(
        actions=(
            DeliveryAction(
                kind="text_message",
                visible=True,
                content="hello",
                chat_id="conversation-1",
                idempotency_key="delivery-action-0",
            ),
        )
    )
    capabilities = ChannelCapabilities(history_backfill=False, file_delivery=True)

    assert message.media == ()
    assert plan.actions[0].kind == "text_message"
    assert plan.actions[0].chat_id == "conversation-1"
    assert plan.actions[0].idempotency_key == "delivery-action-0"
    assert plan.actions[0].media is None
    assert capabilities.file_delivery is True
    with pytest.raises(FrozenInstanceError):
        plan.actions = ()  # type: ignore[misc]
