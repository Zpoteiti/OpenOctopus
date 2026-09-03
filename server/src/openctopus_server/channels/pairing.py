from __future__ import annotations

import asyncio
import hashlib
import hmac
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.db.models import (
    ChannelDelivery,
    DingTalkConfig,
    DiscordConfig,
)

from .ingress import ExternalIngressResult, external_message_id
from .outbound import ChannelOutbound, pairing_delivery_key
from .types import ChannelEvent, ExternalChannel

PAIRING_CONFIRMATION = "Channel pairing confirmed."


class PairingRuntime(Protocol):
    async def apply(self, user_id: UUID, channel: ExternalChannel) -> None: ...

    async def mark_degraded(
        self,
        user_id: UUID,
        channel: ExternalChannel,
        *,
        binding_generation: UUID,
        code: str,
        message: str,
    ) -> None: ...


@dataclass(slots=True)
class _LockEntry:
    lock: asyncio.Lock
    users: int = 0


@dataclass(frozen=True, slots=True)
class _PairingConfig:
    binding_generation: UUID
    owner_platform_user_id: str | None
    pairing_code_hash: bytes | None
    pairing_expires_at: datetime | None


class ChannelPairing:
    def __init__(
        self,
        engine: AsyncEngine,
        outbound: ChannelOutbound,
        *,
        runtime: PairingRuntime | None = None,
    ) -> None:
        self._engine = engine
        self._outbound = outbound
        self._runtime = runtime
        self._locks: dict[tuple[UUID, ExternalChannel, UUID], _LockEntry] = {}
        self._locks_guard = asyncio.Lock()

    @property
    def active_locks(self) -> int:
        return len(self._locks)

    async def __call__(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
    ) -> ExternalIngressResult | None:
        if (
            event.sender_kind != "human"
            or event.conversation_kind != "dm"
            or event.attachments
        ):
            return None
        message_id = external_message_id(user_id, event)
        key = (user_id, event.platform, event.binding_generation)
        async with self._hold(key):
            delivery_key = pairing_delivery_key(
                channel=event.platform,
                binding_generation=event.binding_generation,
                chat_id=event.chat_id,
                source_message_id=event.source_message_id,
            )
            if await self._delivery_exists(user_id, delivery_key):
                return ExternalIngressResult(
                    "duplicate",
                    reason="pairing_event_already_processed",
                    message_id=message_id,
                )

            candidate = event.text.strip()
            config = await self._load_config(user_id, event.platform)
            if (
                config is None
                or config.binding_generation != event.binding_generation
                or config.owner_platform_user_id is not None
                or config.pairing_code_hash is None
                or config.pairing_expires_at is None
                or config.pairing_expires_at <= datetime.now(UTC)
                or not candidate
                or not hmac.compare_digest(
                    bytes(config.pairing_code_hash),
                    hashlib.sha256(candidate.encode("utf-8")).digest(),
                )
            ):
                return None

            try:
                delivery = await self._outbound.deliver_pairing_confirmation(
                    user_id=user_id,
                    channel=event.platform,
                    chat_id=event.chat_id,
                    binding_generation=event.binding_generation,
                    source_message_id=event.source_message_id,
                    content=PAIRING_CONFIRMATION,
                )
            except Exception:
                await self._mark_degraded(
                    user_id=user_id,
                    event=event,
                    code="channel_pairing_confirmation_unavailable",
                    message="Channel pairing confirmation could not be sent.",
                )
                return ExternalIngressResult(
                    "ignored",
                    reason="pairing_confirmation_unavailable",
                    message_id=message_id,
                )
            if delivery.status != "sent":
                await self._mark_degraded(
                    user_id=user_id,
                    event=event,
                    code="channel_pairing_confirmation_failed",
                    message="Channel pairing confirmation was not delivered.",
                )
                return ExternalIngressResult(
                    "ignored",
                    reason="pairing_confirmation_failed",
                    message_id=message_id,
                )

            finalize = asyncio.create_task(
                self._finalize_pairing(
                    user_id=user_id,
                    event=event,
                    expected_hash=bytes(config.pairing_code_hash),
                )
            )
            paired = await await_future_cancellation_safe(finalize)
            if not paired:
                return ExternalIngressResult(
                    "ignored",
                    reason="stale_pairing_confirmation",
                    message_id=message_id,
                )
            return ExternalIngressResult(
                "paired",
                message_id=message_id,
            )

    async def _mark_degraded(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        code: str,
        message: str,
    ) -> None:
        if self._runtime is None:
            return
        try:
            await self._runtime.mark_degraded(
                user_id,
                event.platform,
                binding_generation=event.binding_generation,
                code=code,
                message=message,
            )
        except Exception:
            pass

    async def _delivery_exists(self, user_id: UUID, delivery_key: str) -> bool:
        async with AsyncSession(self._engine) as db:
            delivery_id = await db.scalar(
                select(ChannelDelivery.id).where(
                    ChannelDelivery.user_id == user_id,
                    ChannelDelivery.delivery_key == delivery_key,
                )
            )
            await db.rollback()
        return delivery_id is not None

    async def _load_config(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> _PairingConfig | None:
        async with AsyncSession(self._engine) as db:
            if channel == "discord":
                return _pairing_snapshot(await db.get(DiscordConfig, user_id))
            return _pairing_snapshot(await db.get(DingTalkConfig, user_id))

    async def _finalize_pairing(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        expected_hash: bytes,
    ) -> bool:
        now = datetime.now(UTC)
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            async with db.begin():
                if event.platform == "discord":
                    config = await db.scalar(
                        select(DiscordConfig)
                        .where(DiscordConfig.user_id == user_id)
                        .with_for_update()
                    )
                else:
                    config = await db.scalar(
                        select(DingTalkConfig)
                        .where(DingTalkConfig.user_id == user_id)
                        .with_for_update()
                    )
                if (
                    config is None
                    or config.binding_generation != event.binding_generation
                    or config.owner_platform_user_id is not None
                    or config.pairing_code_hash is None
                    or not hmac.compare_digest(
                        bytes(config.pairing_code_hash), expected_hash
                    )
                    or config.pairing_expires_at is None
                    or config.pairing_expires_at <= now
                ):
                    return False
                config.owner_platform_user_id = event.sender_id
                config.owner_dm_chat_id = event.chat_id
                config.paired_at = now
                config.pairing_code_hash = None
                config.pairing_expires_at = None
                config.revision += 1
                config.updated_at = now
        if self._runtime is not None:
            try:
                await self._runtime.apply(user_id, event.platform)
            except Exception:
                pass
        return True

    @asynccontextmanager
    async def _hold(
        self,
        key: tuple[UUID, ExternalChannel, UUID],
    ) -> AsyncIterator[None]:
        async with self._locks_guard:
            entry = self._locks.get(key)
            if entry is None:
                entry = _LockEntry(asyncio.Lock())
                self._locks[key] = entry
            entry.users += 1
        acquired = False
        try:
            await entry.lock.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                entry.lock.release()
            async with self._locks_guard:
                entry.users -= 1
                if entry.users == 0 and self._locks.get(key) is entry:
                    self._locks.pop(key)


def _pairing_snapshot(
    config: DiscordConfig | DingTalkConfig | None,
) -> _PairingConfig | None:
    if config is None:
        return None
    return _PairingConfig(
        binding_generation=config.binding_generation,
        owner_platform_user_id=config.owner_platform_user_id,
        pairing_code_hash=(
            bytes(config.pairing_code_hash)
            if config.pairing_code_hash is not None
            else None
        ),
        pairing_expires_at=config.pairing_expires_at,
    )
