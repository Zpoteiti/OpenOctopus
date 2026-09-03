from __future__ import annotations

from uuid import UUID

from openctopus_server.chat.types import TurnStart
from openctopus_server.db.models import Message

from .ingress import PolicyNotice
from .router import ChannelDeliveryResult, ChannelDeliveryRouter
from .types import ChannelName, ExternalChannel, OutboundMessage


class ChannelOutbound:
    """Build the three Server-authored channel deliveries on the shared Router."""

    def __init__(self, router: ChannelDeliveryRouter) -> None:
        self._router = router

    async def deliver_final(
        self,
        *,
        turn: TurnStart,
        assistant: Message,
        user_id: UUID,
        channel: ChannelName,
        chat_id: str,
        binding_generation: UUID | None,
    ) -> None:
        if channel not in {"discord", "dingtalk"} or binding_generation is None:
            return
        external_channel: ExternalChannel = channel
        content = _assistant_text(assistant)
        if not content.strip():
            return
        await self._router.deliver(
            OutboundMessage(
                delivery_key=f"final:{assistant.id}",
                user_id=user_id,
                turn_id=turn.turn_id,
                origin="final",
                channel=external_channel,
                chat_id=chat_id,
                binding_generation=binding_generation,
                content=content,
            ),
            session_id=turn.session_id,
            assistant_message_id=assistant.id,
        )

    async def deliver_policy(self, notice: PolicyNotice) -> None:
        await self._router.deliver(
            OutboundMessage(
                delivery_key=notice.delivery_key,
                user_id=notice.user_id,
                turn_id=None,
                origin="policy_notice",
                channel=notice.channel,
                chat_id=notice.chat_id,
                binding_generation=notice.binding_generation,
                content=notice.content,
            )
        )

    async def deliver_pairing_confirmation(
        self,
        *,
        user_id: UUID,
        channel: ExternalChannel,
        chat_id: str,
        binding_generation: UUID,
        source_message_id: str,
        content: str,
    ) -> ChannelDeliveryResult:
        return await self._router.deliver(
            OutboundMessage(
                delivery_key=pairing_delivery_key(
                    channel=channel,
                    binding_generation=binding_generation,
                    chat_id=chat_id,
                    source_message_id=source_message_id,
                ),
                user_id=user_id,
                turn_id=None,
                origin="pairing_confirmation",
                channel=channel,
                chat_id=chat_id,
                binding_generation=binding_generation,
                content=content,
            )
        )


def pairing_delivery_key(
    *,
    channel: ExternalChannel,
    binding_generation: UUID,
    chat_id: str,
    source_message_id: str,
) -> str:
    return f"pairing:{channel}:{binding_generation}:{chat_id}:{source_message_id}"


def _assistant_text(message: Message) -> str:
    return "\n\n".join(
        text
        for block in message.content
        if block.get("type") == "text"
        and isinstance((text := block.get("text")), str)
        and text
    )
