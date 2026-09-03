import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.channels.attachments import OwnerAttachmentResolution
from openctopus_server.channels.ingress import (
    ChannelIngress,
    PolicyNotice,
    external_message_id,
)
from openctopus_server.channels.types import (
    ChannelContextMessage,
    ChannelEvent,
    ExternalAttachmentDescriptor,
)
from openctopus_server.chat.public_projection import pending_response
from openctopus_server.db.models import (
    ChannelMessageReceipt,
    DingTalkConfig,
    DiscordConfig,
    PendingMessage,
    Session,
    TurnRun,
    User,
)
from openctopus_server.errors.exceptions import ChatError


class _Runtime:
    def __init__(self) -> None:
        self.runner_instance_id = uuid4()
        self.scheduled: list[Any] = []
        self.schedule_started = asyncio.Event()
        self.release_schedule: asyncio.Event | None = None

    @asynccontextmanager
    async def session_operation(self, session_id: UUID):
        del session_id
        yield

    async def schedule(self, accepted: Any) -> None:
        self.scheduled.append(accepted)
        self.schedule_started.set()
        if self.release_schedule is not None:
            await self.release_schedule.wait()


class _ClosableRuntime(_Runtime):
    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    async def close(self) -> None:
        assert len(self.scheduled) == 1
        self.close_calls += 1


class _DelayedSessionRuntime(_Runtime):
    def __init__(self, delayed_session_id: UUID) -> None:
        super().__init__()
        self.delayed_session_id = delayed_session_id
        self.session_operation_started = asyncio.Event()
        self.release_session_operation = asyncio.Event()
        self._delayed = False

    @asynccontextmanager
    async def session_operation(self, session_id: UUID):
        if session_id == self.delayed_session_id and not self._delayed:
            self._delayed = True
            self.session_operation_started.set()
            await self.release_session_operation.wait()
        yield


class _RuntimeFence:
    def __init__(self, results: list[bool] | None = None) -> None:
        self.results = results or [True]
        self.calls = 0

    async def __call__(
        self,
        *,
        user_id: UUID,
        platform: str,
        binding_generation: UUID,
        runtime_generation: UUID,
    ) -> bool:
        del user_id, platform, binding_generation, runtime_generation
        index = min(self.calls, len(self.results) - 1)
        self.calls += 1
        return self.results[index]


class _ContextFetcher:
    def __init__(
        self,
        messages: tuple[ChannelContextMessage, ...] = (),
        *,
        error: Exception | None = None,
    ) -> None:
        self.messages = messages
        self.error = error
        self.calls = 0
        self.limits: list[int] = []

    async def __call__(
        self,
        user_id: UUID,
        event: ChannelEvent,
        *,
        limit: int,
    ) -> tuple[ChannelContextMessage, ...]:
        del user_id, event
        self.calls += 1
        self.limits.append(limit)
        if self.error is not None:
            raise self.error
        return self.messages


class _PolicyDelivery:
    def __init__(self) -> None:
        self.notices: list[PolicyNotice] = []

    async def __call__(self, notice: PolicyNotice) -> None:
        self.notices.append(notice)


async def _owner(db: AsyncSession) -> User:
    owner = User(
        id=uuid4(),
        email=f"{uuid4()}@test.com",
        password_hash="hash",
        name="Owner",
    )
    db.add(owner)
    await db.flush()
    return owner


async def _discord_config(
    db: AsyncSession,
    owner: User,
    *,
    binding_generation: UUID,
    owner_platform_user_id: str = "owner-1",
    allow_list: list[str] | None = None,
) -> DiscordConfig:
    now = datetime.now(UTC)
    config = DiscordConfig(
        user_id=owner.id,
        bot_token="secret",
        application_id="application-1",
        bot_user_id="bot-1",
        bot_display_name="Bob",
        binding_generation=binding_generation,
        owner_platform_user_id=owner_platform_user_id,
        owner_dm_chat_id="dm-owner-1",
        paired_at=now,
        allow_list=allow_list or [],
    )
    db.add(config)
    await db.flush()
    return config


async def _dingtalk_config(
    db: AsyncSession,
    owner: User,
    *,
    binding_generation: UUID,
    allow_list: list[str] | None = None,
) -> DingTalkConfig:
    now = datetime.now(UTC)
    config = DingTalkConfig(
        user_id=owner.id,
        client_id="client-1",
        client_secret="secret",
        bot_user_id="bot-1",
        bot_display_name="Bob",
        binding_generation=binding_generation,
        owner_platform_user_id="owner-1",
        owner_dm_chat_id="dm-owner-1",
        paired_at=now,
        allow_list=allow_list or [],
    )
    db.add(config)
    await db.flush()
    return config


def _event(
    *,
    binding_generation: UUID,
    source_message_id: str = "message-1",
    platform: str = "discord",
    sender_id: str = "owner-1",
    sender_kind: str = "human",
    conversation_kind: str = "dm",
    explicitly_mentions_bot: bool = False,
    text: str = "hello",
    attachments: tuple[ExternalAttachmentDescriptor, ...] = (),
    reply_context: tuple[ChannelContextMessage, ...] = (),
    conversation_label: str | None = None,
) -> ChannelEvent:
    return ChannelEvent(
        platform=platform,  # type: ignore[arg-type]
        binding_generation=binding_generation,
        runtime_generation=UUID("30000000-0000-4000-8000-000000000001"),
        source_message_id=source_message_id,
        chat_id="chat-1",
        conversation_kind=conversation_kind,  # type: ignore[arg-type]
        sender_id=sender_id,
        sender_display_name="Sender",
        sender_kind=sender_kind,  # type: ignore[arg-type]
        explicitly_mentions_bot=explicitly_mentions_bot,
        text=text,
        attachments=attachments,
        reply_context=reply_context,
        conversation_label=conversation_label,
    )


async def _counts(pg_engine) -> dict[str, int]:
    async with AsyncSession(pg_engine) as db:
        return {
            "sessions": await db.scalar(select(func.count()).select_from(Session)) or 0,
            "receipts": (
                await db.scalar(select(func.count()).select_from(ChannelMessageReceipt)) or 0
            ),
            "pending": await db.scalar(select(func.count()).select_from(PendingMessage)) or 0,
            "turns": await db.scalar(select(func.count()).select_from(TurnRun)) or 0,
        }


async def test_owner_event_uses_uuid5_route_and_shared_publish(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    runtime = _Runtime()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
    )
    event = _event(binding_generation=binding_generation)

    result = await ingress.accept_external(user_id=owner.id, event=event)

    expected_message_id = external_message_id(owner.id, event)
    assert result.disposition == "accepted"
    assert result.message_id == expected_message_id
    assert expected_message_id.version == 5
    assert len(runtime.scheduled) == 1
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        session = await db.scalar(select(Session))
        pending = await db.get(PendingMessage, expected_message_id)
        receipt = await db.scalar(select(ChannelMessageReceipt))
        run = await db.scalar(select(TurnRun))

    assert session is not None
    assert session.session_key == "discord:application-1:chat-1"
    assert session.channel == "discord"
    assert pending is not None
    assert pending.sender_id == "owner-1"
    assert pending.sender_classification == "owner"
    assert pending.ingress_tool_profile == "owner_full"
    assert receipt is not None and receipt.session_id == session.id
    assert receipt.disposition == "trigger"
    assert run is not None and run.input_message_ids == [str(expected_message_id)]
    assert ingress.active_event_locks == 0
    assert ingress.active_route_locks == 0


@pytest.mark.parametrize(
    ("sender_kind", "conversation_kind", "mentioned"),
    [
        ("bot", "dm", False),
        ("webhook", "dm", False),
        ("human", "group", False),
        ("human", "thread", False),
    ],
)
async def test_deterministic_ignored_events_write_nothing(
    pg_engine,
    sender_kind: str,
    conversation_kind: str,
    mentioned: bool,
) -> None:
    runtime = _Runtime()
    fence = _RuntimeFence()
    ingress = ChannelIngress(pg_engine, runtime, is_current_runtime=fence)

    result = await ingress.accept_external(
        user_id=uuid4(),
        event=_event(
            binding_generation=uuid4(),
            sender_kind=sender_kind,
            conversation_kind=conversation_kind,
            explicitly_mentions_bot=mentioned,
        ),
    )

    assert result.disposition == "ignored"
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }
    assert runtime.scheduled == []


async def test_trigger_text_over_32000_characters_is_ignored_before_lookup(
    pg_engine,
) -> None:
    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
        config_loader=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("oversized input must stop before config lookup")
        ),  # type: ignore[arg-type]
    )

    result = await ingress.accept_external(
        user_id=uuid4(),
        event=_event(binding_generation=uuid4(), text="x" * 32_001),
    )

    assert result.disposition == "ignored"
    assert result.reason == "text_too_long"
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }


async def test_mention_only_trigger_is_prompted_once_without_provider(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    event = _event(
        binding_generation=binding_generation,
        source_message_id="mention-only",
        conversation_kind="group",
        explicitly_mentions_bot=True,
        text="   ",
    )
    runtime = _Runtime()
    policy = _PolicyDelivery()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
        policy_delivery=policy,
    )

    first = await ingress.accept_external(user_id=owner.id, event=event)
    duplicate = await ingress.accept_external(user_id=owner.id, event=event)

    assert first.disposition == "ignored"
    assert first.reason == "empty_message_prompted"
    assert duplicate.disposition == "duplicate"
    assert runtime.scheduled == []
    assert len(policy.notices) == 1
    assert policy.notices[0].content == "请在@机器人后写明问题"
    assert policy.notices[0].delivery_key == (
        f"empty-policy:{external_message_id(owner.id, event)}"
    )
    async with AsyncSession(pg_engine) as db:
        receipt = await db.scalar(select(ChannelMessageReceipt))
    assert receipt is not None
    assert receipt.disposition == "trigger"
    assert receipt.session_id is None


async def test_owner_precedes_exact_allow_list_and_unauthorized_is_silent(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(
            db,
            owner,
            binding_generation=binding_generation,
            allow_list=["owner-1", "42"],
        )
        await db.commit()

    runtime = _Runtime()
    ingress = ChannelIngress(pg_engine, runtime, is_current_runtime=_RuntimeFence())
    owner_result = await ingress.accept_external(
        user_id=owner.id,
        event=_event(binding_generation=binding_generation, source_message_id="owner"),
    )
    allowed_result = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            source_message_id="allowed",
            sender_id="42",
        ),
    )
    unauthorized_result = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            source_message_id="unauthorized",
            sender_id="042",
        ),
    )

    assert owner_result.disposition == allowed_result.disposition == "accepted"
    assert unauthorized_result.disposition == "ignored"
    async with AsyncSession(pg_engine) as db:
        rows = list(
            (
                await db.scalars(
                    select(PendingMessage).order_by(PendingMessage.received_at)
                )
            ).all()
        )
    assert [(row.sender_classification, row.ingress_tool_profile) for row in rows] == [
        ("owner", "owner_full"),
        ("allowed_non_owner", "message_only"),
    ]
    assert rows[1].content[-1]["text"] == "hello"
    assert (await _counts(pg_engine))["receipts"] == 2


async def test_stale_runtime_and_binding_write_nothing(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    config_calls = 0

    async def should_not_load(*args: Any, **kwargs: Any):
        nonlocal config_calls
        config_calls += 1
        raise AssertionError("stale runtime must be rejected before config/DB loading")

    stale_runtime = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence([False]),
        config_loader=should_not_load,
    )
    runtime_result = await stale_runtime.accept_external(
        user_id=owner.id,
        event=_event(binding_generation=binding_generation),
    )
    current_runtime = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
    )
    binding_result = await current_runtime.accept_external(
        user_id=owner.id,
        event=_event(binding_generation=uuid4()),
    )

    assert runtime_result.reason == "stale_runtime"
    assert binding_result.reason == "stale_binding"
    assert config_calls == 0
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }


async def test_non_owner_attachment_policy_never_exposes_or_downloads_bytes(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(
            db,
            owner,
            binding_generation=binding_generation,
            allow_list=["42"],
        )
        await db.commit()

    attachment = ExternalAttachmentDescriptor(
        source_id="attachment-1",
        filename="secret-payroll.pdf",
        content_type="application/pdf",
        size=100,
    )
    owner_resolver_calls = 0

    async def owner_resolver(event: ChannelEvent):
        nonlocal owner_resolver_calls
        owner_resolver_calls += 1
        raise AssertionError(event)

    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
        owner_attachment_resolver=owner_resolver,
    )
    result = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            sender_id="42",
            text="Please review this",
            attachments=(attachment,),
        ),
    )

    assert result.disposition == "accepted"
    assert owner_resolver_calls == 0
    async with AsyncSession(pg_engine) as db:
        pending = await db.scalar(select(PendingMessage))
    assert pending is not None
    serialized = str(pending.content)
    assert "secret-payroll.pdf" not in serialized
    assert "attachment-1" not in serialized
    assert "not accepted" in serialized
    assert pending.attachment_refs == []


async def test_non_owner_attachment_only_is_durable_policy_once_without_session(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(
            db,
            owner,
            binding_generation=binding_generation,
            allow_list=["42"],
        )
        await db.commit()

    attachment = ExternalAttachmentDescriptor(
        source_id="attachment-1",
        filename="secret.pdf",
        content_type="application/pdf",
        size=100,
    )
    event = _event(
        binding_generation=binding_generation,
        sender_id="42",
        text="   ",
        attachments=(attachment,),
    )
    policy = _PolicyDelivery()
    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
        policy_delivery=policy,
    )

    first = await ingress.accept_external(user_id=owner.id, event=event)
    duplicate = await ingress.accept_external(user_id=owner.id, event=event)

    assert first.disposition == "attachment_rejected"
    assert duplicate.disposition == "duplicate"
    assert len(policy.notices) == 1
    assert "secret.pdf" not in policy.notices[0].content
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 1,
        "pending": 0,
        "turns": 0,
    }
    async with AsyncSession(pg_engine) as db:
        receipt = await db.scalar(select(ChannelMessageReceipt))
    assert receipt is not None
    assert receipt.session_id is None
    assert receipt.disposition == "attachment_rejected"


async def test_owner_attachment_returns_explicit_unsupported_without_acceptance(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    result = await ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
    ).accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            attachments=(
                ExternalAttachmentDescriptor(
                    source_id="attachment-1",
                    filename="owner.pdf",
                    content_type="application/pdf",
                    size=1,
                ),
            ),
        ),
    )

    assert result.disposition == "owner_attachment_unsupported"
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }


async def test_owner_attachment_resolution_persists_refs_and_expanded_content(
    pg_engine,
) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    event = _event(
        binding_generation=binding_generation,
        attachments=(
            ExternalAttachmentDescriptor(
                source_id="attachment-1",
                filename="owner.pdf",
                content_type="application/pdf",
                size=4,
            ),
        ),
    )
    message_id = external_message_id(owner.id, event)
    path = f".attachments/channels/{message_id}/0-owner.pdf"
    resolver_calls = 0

    async def resolve_owner_attachments(
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
    ) -> OwnerAttachmentResolution:
        nonlocal resolver_calls
        del user_id, event, message_id
        resolver_calls += 1
        return OwnerAttachmentResolution(
            content=(
                {
                    "type": "text",
                    "text": f'User uploaded file to device=\'server\', path="{path}"',
                },
                {"type": "text", "text": "hello"},
            ),
            attachment_refs=(
                {"openoctopus_device": "server", "path": path},
            ),
            failed_count=0,
        )

    runtime = _Runtime()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
        owner_attachment_resolver=resolve_owner_attachments,
    )

    first = await ingress.accept_external(user_id=owner.id, event=event)
    duplicate = await ingress.accept_external(user_id=owner.id, event=event)

    assert first.disposition == "accepted"
    assert duplicate.disposition == "duplicate"
    assert resolver_calls == 1
    async with AsyncSession(pg_engine) as db:
        pending = await db.get(PendingMessage, message_id)
        session = await db.get(Session, pending.session_id) if pending is not None else None
    assert pending is not None
    assert session is not None
    assert pending.attachment_refs == [
        {"openoctopus_device": "server", "path": path}
    ]
    assert pending.content[-1] == {"type": "text", "text": "hello"}
    projected = pending_response(pending, session=session)
    assert [block.model_dump() for block in projected.content] == [
        {"type": "text", "text": "hello"}
    ]
    assert [ref.model_dump() for ref in projected.attachment_refs] == (
        pending.attachment_refs
    )
    assert len(runtime.scheduled) == 1


async def test_close_gate_rejects_new_events_before_any_ingress_work(pg_engine) -> None:
    fence = _RuntimeFence()
    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=fence,
    )
    ingress.close_gate()
    ingress.close_gate()

    result = await ingress.accept_external(
        user_id=uuid4(),
        event=_event(binding_generation=uuid4()),
    )

    assert result.disposition == "shutting_down"
    assert result.reason is None
    assert fence.calls == 0
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }


async def test_close_gate_allows_an_entered_event_to_complete(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking_fence(**_kwargs: object) -> bool:
        entered.set()
        await release.wait()
        return True

    runtime = _Runtime()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=blocking_fence,
    )
    entered_task = asyncio.create_task(
        ingress.accept_external(
            user_id=owner.id,
            event=_event(
                binding_generation=binding_generation,
                source_message_id="entered",
            ),
        )
    )
    await entered.wait()

    ingress.close_gate()
    rejected = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            source_message_id="after-close",
        ),
    )
    release.set()
    accepted = await entered_task

    assert rejected.disposition == "shutting_down"
    assert accepted.disposition == "accepted"
    assert len(runtime.scheduled) == 1
    assert await _counts(pg_engine) == {
        "sessions": 1,
        "receipts": 1,
        "pending": 1,
        "turns": 1,
    }


async def test_shutdown_drains_publish_and_schedule_before_runtime_close(
    pg_engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    from openctopus_server.channels import ingress as ingress_module

    publish_started = asyncio.Event()
    release_publish = asyncio.Event()
    original_publish = ingress_module.publish_inbound_locked

    async def blocking_publish(*args: Any, **kwargs: Any):
        publish_started.set()
        await release_publish.wait()
        return await original_publish(*args, **kwargs)

    monkeypatch.setattr(ingress_module, "publish_inbound_locked", blocking_publish)
    runtime = _ClosableRuntime()
    runtime.release_schedule = asyncio.Event()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
    )
    accept_task = asyncio.create_task(
        ingress.accept_external(
            user_id=owner.id,
            event=_event(binding_generation=binding_generation),
        )
    )
    await publish_started.wait()

    from openctopus_server.main import _close_lifespan_resources

    shutdown_task = asyncio.create_task(
        _close_lifespan_resources(
            channel_ingress=ingress,
            channel_manager=None,
            heartbeat_pulse=None,
            cron_scheduler=None,
            server_mcp_supervisor=None,
            runtime=runtime,  # type: ignore[arg-type]
            device_registry=None,
            deletion_worker=None,
            object_storage=None,
            engine=None,
        )
    )
    await asyncio.sleep(0)
    assert not shutdown_task.done()
    assert ingress.active_operations == 1
    assert runtime.close_calls == 0

    release_publish.set()
    await runtime.schedule_started.wait()
    await asyncio.sleep(0)
    assert not shutdown_task.done()
    assert runtime.close_calls == 0

    runtime.release_schedule.set()
    result = await accept_task
    await shutdown_task

    assert result.disposition == "accepted"
    assert len(runtime.scheduled) == 1
    assert runtime.close_calls == 1
    assert ingress.active_operations == 0
    assert ingress.active_event_locks == 0
    assert ingress.active_route_locks == 0


async def test_owner_all_failed_attachment_only_is_policy_once_without_provider(
    pg_engine,
) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    event = _event(
        binding_generation=binding_generation,
        text="   ",
        attachments=(
            ExternalAttachmentDescriptor(
                source_id="attachment-1",
                filename="owner.pdf",
                content_type="application/pdf",
                size=None,
            ),
        ),
    )
    resolver_calls = 0

    async def resolve_owner_attachments(
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
    ) -> OwnerAttachmentResolution:
        nonlocal resolver_calls
        del user_id, event, message_id
        resolver_calls += 1
        return OwnerAttachmentResolution(
            content=(
                {"type": "text", "text": "Some attachments were not accepted."},
            ),
            attachment_refs=(),
            failed_count=1,
        )

    runtime = _Runtime()
    policy = _PolicyDelivery()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
        policy_delivery=policy,
        owner_attachment_resolver=resolve_owner_attachments,
    )

    first = await ingress.accept_external(user_id=owner.id, event=event)
    duplicate = await ingress.accept_external(user_id=owner.id, event=event)

    assert first.disposition == "attachment_rejected"
    assert first.reason == "owner_attachments_failed"
    assert duplicate.disposition == "duplicate"
    assert resolver_calls == 1
    assert len(policy.notices) == 1
    assert "owner.pdf" not in policy.notices[0].content
    assert runtime.scheduled == []
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 1,
        "pending": 0,
        "turns": 0,
    }


async def test_owner_attachment_publish_revalidates_authority_after_resolution(
    pg_engine,
) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    started = asyncio.Event()
    release = asyncio.Event()
    cleanup_calls = 0

    async def cleanup_unpublished() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1

    async def resolve_owner_attachments(
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
    ) -> OwnerAttachmentResolution:
        del user_id, event, message_id
        started.set()
        await release.wait()
        return OwnerAttachmentResolution(
            content=({"type": "text", "text": "hello"},),
            attachment_refs=(
                {"openoctopus_device": "server", "path": ".attachments/file"},
            ),
            failed_count=0,
            cleanup_unpublished=cleanup_unpublished,
        )

    runtime = _Runtime()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
        owner_attachment_resolver=resolve_owner_attachments,
    )
    task = asyncio.create_task(
        ingress.accept_external(
            user_id=owner.id,
            event=_event(
                binding_generation=binding_generation,
                attachments=(
                    ExternalAttachmentDescriptor(
                        source_id="attachment-1",
                        filename="owner.pdf",
                        content_type="application/pdf",
                        size=1,
                    ),
                ),
            ),
        )
    )
    await started.wait()
    async with AsyncSession(pg_engine) as db:
        config = await db.get(DiscordConfig, owner.id)
        assert config is not None
        config.owner_platform_user_id = "different-owner"
        await db.commit()
    release.set()

    result = await task

    assert result.disposition == "ignored"
    assert result.reason == "unauthorized"
    assert cleanup_calls == 1
    assert runtime.scheduled == []
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }


async def test_owner_attachment_cleanup_finishes_before_cancellation_propagates(
    pg_engine,
) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    context_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def resolve_owner_attachments(
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
    ) -> OwnerAttachmentResolution:
        del user_id, event, message_id

        async def cleanup_unpublished() -> None:
            cleanup_started.set()
            await release_cleanup.wait()

        return OwnerAttachmentResolution(
            content=({"type": "text", "text": "hello"},),
            attachment_refs=({"openoctopus_device": "server", "path": ".attachments/file"},),
            failed_count=0,
            cleanup_unpublished=cleanup_unpublished,
        )

    async def blocking_context(
        user_id: UUID,
        event: ChannelEvent,
        *,
        limit: int,
    ) -> tuple[ChannelContextMessage, ...]:
        del user_id, event, limit
        context_started.set()
        await asyncio.Future()

    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
        context_fetcher=blocking_context,
        owner_attachment_resolver=resolve_owner_attachments,
    )
    task = asyncio.create_task(
        ingress.accept_external(
            user_id=owner.id,
            event=_event(
                binding_generation=binding_generation,
                conversation_kind="group",
                explicitly_mentions_bot=True,
                attachments=(
                    ExternalAttachmentDescriptor(
                        source_id="attachment-1",
                        filename="owner.pdf",
                        content_type="application/pdf",
                        size=1,
                    ),
                ),
            ),
        )
    )
    await context_started.wait()
    task.cancel()
    await cleanup_started.wait()
    assert not task.done()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }


async def test_concurrent_duplicate_fetches_context_and_schedules_once(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    context_fetcher = _ContextFetcher(
        (
            ChannelContextMessage(
                source_message_id="context-1",
                sender_id="other",
                sender_display_name="Other",
                sent_at="2026-09-02T01:00:00Z",
                text="context",
            ),
        )
    )
    runtime = _Runtime()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
        context_fetcher=context_fetcher,
    )
    event = _event(
        binding_generation=binding_generation,
        conversation_kind="group",
        explicitly_mentions_bot=True,
    )

    results = await asyncio.gather(
        ingress.accept_external(user_id=owner.id, event=event),
        ingress.accept_external(user_id=owner.id, event=event),
    )

    assert {result.disposition for result in results} == {"accepted", "duplicate"}
    assert context_fetcher.calls == 1
    assert context_fetcher.limits == [100]
    assert len(runtime.scheduled) == 1
    assert await _counts(pg_engine) == {
        "sessions": 1,
        "receipts": 2,
        "pending": 1,
        "turns": 1,
    }
    assert ingress.active_event_locks == 0
    assert ingress.active_route_locks == 0


async def test_concurrent_first_events_share_one_jit_session(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    runtime = _Runtime()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
    )

    results = await asyncio.gather(
        ingress.accept_external(
            user_id=owner.id,
            event=_event(
                binding_generation=binding_generation,
                source_message_id="first",
            ),
        ),
        ingress.accept_external(
            user_id=owner.id,
            event=_event(
                binding_generation=binding_generation,
                source_message_id="second",
            ),
        ),
    )

    assert [result.disposition for result in results] == ["accepted", "accepted"]
    assert {result.session_id for result in results} == {results[0].session_id}
    assert await _counts(pg_engine) == {
        "sessions": 1,
        "receipts": 2,
        "pending": 2,
        "turns": 1,
    }
    assert ingress.active_event_locks == 0
    assert ingress.active_route_locks == 0


async def test_external_session_title_uses_first_sanitized_conversation_label(
    pg_engine,
) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
    )
    long_label = "  Project\n\x00Room  " + ("x" * 200)

    first = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            source_message_id="first-label",
            conversation_label=long_label,
        ),
    )
    second = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            source_message_id="second-label",
            conversation_label="Renamed platform conversation",
        ),
    )

    assert first.disposition == second.disposition == "accepted"
    async with AsyncSession(pg_engine) as db:
        session = await db.scalar(select(Session))
    assert session is not None
    assert session.title == ("Project Room " + ("x" * 200))[:120]
    assert 1 <= len(session.title) <= 120


async def test_external_session_title_has_stable_platform_fallback(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    result = await ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
    ).accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            conversation_label=" \n\x00 ",
        ),
    )

    assert result.disposition == "accepted"
    async with AsyncSession(pg_engine) as db:
        session = await db.scalar(select(Session))
    assert session is not None
    assert session.title == "Discord chat"


async def test_session_deleted_before_operation_gets_new_public_identity(pg_engine) -> None:
    binding_generation = uuid4()
    deleted_session_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        db.add(
            Session(
                id=deleted_session_id,
                user_id=owner.id,
                session_key="discord:application-1:chat-1",
                channel="discord",
                chat_id="chat-1",
                title="Old",
            )
        )
        await db.commit()

    runtime = _DelayedSessionRuntime(deleted_session_id)
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
    )
    task = asyncio.create_task(
        ingress.accept_external(
            user_id=owner.id,
            event=_event(binding_generation=binding_generation),
        )
    )
    await runtime.session_operation_started.wait()
    async with AsyncSession(pg_engine) as db:
        await db.execute(delete(Session).where(Session.id == deleted_session_id))
        await db.commit()
    runtime.release_session_operation.set()

    result = await task

    assert result.disposition == "accepted"
    assert result.session_id != deleted_session_id
    async with AsyncSession(pg_engine) as db:
        assert await db.get(Session, deleted_session_id) is None
        sessions = list((await db.scalars(select(Session))).all())
    assert [session.id for session in sessions] == [result.session_id]


async def test_existing_route_field_mismatch_fails_closed(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        db.add(
            Session(
                id=uuid4(),
                user_id=owner.id,
                session_key="discord:application-1:chat-1",
                channel="dingtalk",
                chat_id="different-chat",
                title="Corrupt route",
            )
        )
        await db.commit()

    runtime = _Runtime()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
    )

    with pytest.raises(ChatError):
        await ingress.accept_external(
            user_id=owner.id,
            event=_event(binding_generation=binding_generation),
        )

    assert await _counts(pg_engine) == {
        "sessions": 1,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }
    assert runtime.scheduled == []


async def test_context_dedup_and_64k_trim_keep_latest_and_register_omitted(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        db.add(
            ChannelMessageReceipt(
                id=uuid4(),
                user_id=owner.id,
                session_id=None,
                channel="discord",
                binding_generation=binding_generation,
                chat_id="chat-1",
                source_message_id="already-seen",
                disposition="context",
            )
        )
        await db.commit()

    context_fetcher = _ContextFetcher(
        (
            ChannelContextMessage(
                source_message_id="already-seen",
                sender_id="1",
                sender_display_name=None,
                sent_at="1",
                text="old duplicate",
            ),
            ChannelContextMessage(
                source_message_id="too-large",
                sender_id="2",
                sender_display_name=None,
                sent_at="2",
                text="x" * 64_001,
            ),
            ChannelContextMessage(
                source_message_id="latest",
                sender_id="3",
                sender_display_name=None,
                sent_at="3",
                text="latest context",
            ),
        )
    )
    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
        context_fetcher=context_fetcher,
    )

    result = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            conversation_kind="group",
            explicitly_mentions_bot=True,
        ),
    )

    assert result.context_included_count == 1
    assert result.context_omitted_count == 1
    async with AsyncSession(pg_engine) as db:
        pending = await db.scalar(select(PendingMessage))
        receipts = list(
            (
                await db.scalars(
                    select(ChannelMessageReceipt).order_by(
                        ChannelMessageReceipt.source_message_id
                    )
                )
            ).all()
        )
        session = await db.get(Session, pending.session_id) if pending is not None else None
    assert pending is not None
    assert session is not None
    assert pending.channel_context == [
        {
            "source_message_id": "latest",
            "sender_id": "3",
            "sender_display_name": None,
            "sent_at": "3",
            "text": "latest context",
            "attachment_summaries": [],
        },
        {"_openoctopus_omitted_count": 1},
    ]
    projected = pending_response(pending, session=session)
    assert projected.channel_context.included_count == 1
    assert projected.channel_context.omitted_count == 1
    assert {
        receipt.source_message_id: receipt.disposition for receipt in receipts
    } == {
        "already-seen": "context",
        "latest": "context",
        "message-1": "trigger",
        "too-large": "context_omitted",
    }


async def test_dingtalk_uses_reply_context_and_never_calls_history_fetcher(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _dingtalk_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    fetcher = _ContextFetcher(error=AssertionError("DingTalk must not fetch history"))
    reply = ChannelContextMessage(
        source_message_id="quote-1",
        sender_id="other",
        sender_display_name="Other",
        sent_at=None,
        text="quoted",
    )
    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
        context_fetcher=fetcher,
    )

    result = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            platform="dingtalk",
            binding_generation=binding_generation,
            conversation_kind="group",
            explicitly_mentions_bot=True,
            reply_context=(reply,),
        ),
    )

    assert result.disposition == "accepted"
    assert fetcher.calls == 0
    async with AsyncSession(pg_engine) as db:
        pending = await db.scalar(select(PendingMessage))
    assert pending is not None
    assert pending.channel_context == [
        {
            "source_message_id": "quote-1",
            "sender_id": "other",
            "sender_display_name": "Other",
            "sent_at": None,
            "text": "quoted",
            "attachment_summaries": [],
        }
    ]


async def test_context_failure_does_not_block_trigger(
    pg_engine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
        context_fetcher=_ContextFetcher(
            error=TimeoutError("raw secret-chat-id secret-source-id")
        ),
    )
    caplog.set_level("WARNING", logger="openctopus_server.channels.ingress")
    result = await ingress.accept_external(
        user_id=owner.id,
        event=_event(
            binding_generation=binding_generation,
            conversation_kind="group",
            explicitly_mentions_bot=True,
        ),
    )

    assert result.disposition == "accepted"
    assert result.context_included_count == 0
    assert (await _counts(pg_engine))["pending"] == 1
    record = next(
        record
        for record in caplog.records
        if getattr(record, "event", None) == "channel_context_fetch_failed"
    )
    assert record.platform == "discord"
    assert record.user_id == str(owner.id)
    assert record.error_code == "channel_history_fetch_exception"
    assert record.context_count == 0
    assert "secret-chat-id" not in caplog.text
    assert "secret-source-id" not in caplog.text


async def test_current_config_is_revalidated_after_context_fetch(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(
            db,
            owner,
            binding_generation=binding_generation,
            allow_list=["42"],
        )
        await db.commit()

    started = asyncio.Event()
    release = asyncio.Event()

    async def fetcher(user_id: UUID, event: ChannelEvent, *, limit: int):
        del user_id, event, limit
        started.set()
        await release.wait()
        return ()

    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
        context_fetcher=fetcher,
    )
    task = asyncio.create_task(
        ingress.accept_external(
            user_id=owner.id,
            event=_event(
                binding_generation=binding_generation,
                sender_id="42",
                conversation_kind="group",
                explicitly_mentions_bot=True,
            ),
        )
    )
    await started.wait()
    async with AsyncSession(pg_engine) as db:
        config = await db.get(DiscordConfig, owner.id)
        assert config is not None
        config.allow_list = []
        await db.commit()
    release.set()

    result = await task

    assert result.disposition == "ignored"
    assert result.reason == "unauthorized"
    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }


async def test_publish_failure_rolls_back_receipt_session_and_pending(
    pg_engine,
    monkeypatch,
) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    async def fail_publish(*args: Any, **kwargs: Any):
        raise RuntimeError("publish failed")

    monkeypatch.setattr(
        "openctopus_server.channels.ingress.publish_inbound_locked",
        fail_publish,
    )
    ingress = ChannelIngress(
        pg_engine,
        _Runtime(),
        is_current_runtime=_RuntimeFence(),
    )

    with pytest.raises(RuntimeError, match="publish failed"):
        await ingress.accept_external(
            user_id=owner.id,
            event=_event(binding_generation=binding_generation),
        )

    assert await _counts(pg_engine) == {
        "sessions": 0,
        "receipts": 0,
        "pending": 0,
        "turns": 0,
    }


async def test_commit_to_schedule_handoff_is_cancellation_safe(pg_engine) -> None:
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = await _owner(db)
        await _discord_config(db, owner, binding_generation=binding_generation)
        await db.commit()

    runtime = _Runtime()
    runtime.release_schedule = asyncio.Event()
    ingress = ChannelIngress(
        pg_engine,
        runtime,
        is_current_runtime=_RuntimeFence(),
    )
    task = asyncio.create_task(
        ingress.accept_external(
            user_id=owner.id,
            event=_event(binding_generation=binding_generation),
        )
    )
    await runtime.schedule_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()

    runtime.release_schedule.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(runtime.scheduled) == 1
    assert await _counts(pg_engine) == {
        "sessions": 1,
        "receipts": 1,
        "pending": 1,
        "turns": 1,
    }
