import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.channels.pairing import ChannelPairing
from openctopus_server.channels.router import ChannelDeliveryResult
from openctopus_server.channels.types import ChannelEvent
from openctopus_server.db.models import DiscordConfig, User


class _Outbound:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        status: str = "sent",
        block: asyncio.Event | None = None,
    ) -> None:
        self.engine = engine
        self.status = status
        self.block = block
        self.calls: list[dict[str, object]] = []

    async def deliver_pairing_confirmation(
        self,
        **kwargs: object,
    ) -> ChannelDeliveryResult:
        async with AsyncSession(self.engine) as db:
            config = await db.get(DiscordConfig, kwargs["user_id"])
            assert config is not None and config.owner_platform_user_id is None
        self.calls.append(kwargs)
        if self.block is not None:
            await self.block.wait()
        return ChannelDeliveryResult(
            delivery_id=uuid4(),
            status=self.status,  # type: ignore[arg-type]
            visible_sent_actions=1 if self.status == "sent" else 0,
            visible_total_actions=1,
            last_error_code=None,
            last_error_message=None,
        )


class _Runtime:
    def __init__(self) -> None:
        self.applied: list[tuple[UUID, str]] = []
        self.degraded: list[tuple[UUID, str, UUID, str, str]] = []

    async def apply(self, user_id: UUID, channel: str) -> None:
        self.applied.append((user_id, channel))

    async def mark_degraded(
        self,
        user_id: UUID,
        channel: str,
        *,
        binding_generation: UUID,
        code: str,
        message: str,
    ) -> None:
        self.degraded.append(
            (user_id, channel, binding_generation, code, message)
        )


def _event(generation: UUID, *, source: str = "event-1") -> ChannelEvent:
    return ChannelEvent(
        platform="discord",
        binding_generation=generation,
        runtime_generation=uuid4(),
        source_message_id=source,
        chat_id="dm-1",
        conversation_kind="dm",
        sender_id="owner-platform-id",
        sender_display_name="Owner",
        sender_kind="human",
        explicitly_mentions_bot=False,
        text="  pair-code  ",
        attachments=(),
    )


async def _config(
    engine: AsyncEngine,
    *,
    code: str = "pair-code",
) -> tuple[UUID, UUID]:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        user = (await db.scalars(select(User).where(User.email == "user@test.com"))).one()
        generation = uuid4()
        db.add(
            DiscordConfig(
                user_id=user.id,
                bot_token="secret",
                application_id=str(uuid4()),
                bot_user_id="bot-id",
                bot_display_name="Bot",
                binding_generation=generation,
                revision=1,
                allow_list=[],
                pairing_code_hash=hashlib.sha256(code.encode()).digest(),
                pairing_expires_at=datetime.now(UTC) + timedelta(minutes=10),
            )
        )
        await db.commit()
    return user.id, generation


async def test_pairing_confirms_before_cas_then_binds_and_refreshes_runtime(
    pg_engine,
    user_client,
) -> None:
    del user_client
    user_id, generation = await _config(pg_engine)
    outbound = _Outbound(pg_engine)
    runtime = _Runtime()
    pairing = ChannelPairing(
        pg_engine,
        outbound,  # type: ignore[arg-type]
        runtime=runtime,  # type: ignore[arg-type]
    )

    result = await pairing(user_id=user_id, event=_event(generation))

    assert result is not None and result.disposition == "paired"
    assert len(outbound.calls) == 1
    async with AsyncSession(pg_engine) as db:
        config = await db.get(DiscordConfig, user_id)
    assert config is not None
    assert config.owner_platform_user_id == "owner-platform-id"
    assert config.owner_dm_chat_id == "dm-1"
    assert config.pairing_code_hash is None
    assert config.revision == 2
    assert runtime.applied == [(user_id, "discord")]
    assert pairing.active_locks == 0


async def test_wrong_group_attachment_and_failed_confirmation_never_bind(
    pg_engine,
    user_client,
) -> None:
    del user_client
    user_id, generation = await _config(pg_engine)
    outbound = _Outbound(pg_engine, status="failed")
    runtime = _Runtime()
    pairing = ChannelPairing(
        pg_engine,
        outbound,  # type: ignore[arg-type]
        runtime=runtime,  # type: ignore[arg-type]
    )
    wrong = replace(_event(generation), text="prefix pair-code")

    assert await pairing(user_id=user_id, event=wrong) is None
    failed = await pairing(user_id=user_id, event=_event(generation, source="event-2"))

    assert failed is not None and failed.reason == "pairing_confirmation_failed"
    async with AsyncSession(pg_engine) as db:
        config = await db.get(DiscordConfig, user_id)
    assert config is not None and config.owner_platform_user_id is None
    assert runtime.degraded == [
        (
            user_id,
            "discord",
            generation,
            "channel_pairing_confirmation_failed",
            "Channel pairing confirmation was not delivered.",
        )
    ]


async def test_concurrent_pairing_events_issue_only_one_confirmation(
    pg_engine,
    user_client,
) -> None:
    del user_client
    user_id, generation = await _config(pg_engine)
    release = asyncio.Event()
    outbound = _Outbound(pg_engine, block=release)
    pairing = ChannelPairing(pg_engine, outbound)  # type: ignore[arg-type]

    first = asyncio.create_task(pairing(user_id=user_id, event=_event(generation)))
    while not outbound.calls:
        await asyncio.sleep(0)
    second = asyncio.create_task(
        pairing(user_id=user_id, event=_event(generation, source="event-2"))
    )
    release.set()
    first_result, second_result = await asyncio.gather(first, second)

    assert first_result is not None and first_result.disposition == "paired"
    assert second_result is None
    assert len(outbound.calls) == 1
    assert pairing.active_locks == 0
