from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, replace
from typing import Literal, Protocol
from uuid import UUID, uuid4, uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.chat.types import AcceptedMessage
from openctopus_server.db.models import (
    ChannelMessageReceipt,
    DingTalkConfig,
    DiscordConfig,
    PendingMessage,
    Session,
)
from openctopus_server.services.inbound import lock_inbound_identity
from openctopus_server.services.messages import publish_inbound_locked

from .attachments import (
    OwnerAttachmentResolution,
    OwnerAttachmentResolutionResolver,
)
from .types import (
    ChannelContextMessage,
    ChannelEvent,
    ExternalChannel,
    InboundMessage,
    InboundSender,
    SenderClassification,
    ToolProfile,
)

type IngressDisposition = Literal[
    "accepted",
    "duplicate",
    "ignored",
    "paired",
    "attachment_rejected",
    "owner_attachment_unsupported",
    "shutting_down",
]

_EXTERNAL_MESSAGE_NAMESPACE = UUID("928482e7-ea9f-4a3d-81e8-3bca8946baa9")
_LOGGER = logging.getLogger(__name__)
_MAX_CONTEXT_MESSAGES = 100
_MAX_CONTEXT_CHARS = 64_000
_MAX_TRIGGER_TEXT_CHARS = 32_000
_ATTACHMENT_REJECTION = (
    "Attachments from this non-owner sender were not accepted."
)
_ATTACHMENT_ONLY_REJECTION = "Attachments from non-owner senders are not accepted."
_OWNER_ATTACHMENT_ONLY_REJECTION = (
    "Your attachments could not be accepted. Send a new message with supported files."
)
_MENTION_ONLY_PROMPT = "请在@机器人后写明问题"


@dataclass(frozen=True, slots=True)
class ChannelBindingSnapshot:
    user_id: UUID
    platform: ExternalChannel
    bot_identity: str
    binding_generation: UUID
    owner_platform_user_id: str | None
    allow_list: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PolicyNotice:
    delivery_key: str
    user_id: UUID
    channel: ExternalChannel
    chat_id: str
    binding_generation: UUID
    source_message_id: str
    content: str


@dataclass(frozen=True, slots=True)
class ExternalIngressResult:
    disposition: IngressDisposition
    reason: str | None = None
    message_id: UUID | None = None
    session_id: UUID | None = None
    context_included_count: int = 0
    context_omitted_count: int = 0


class IngressRuntime(Protocol):
    runner_instance_id: UUID

    def session_operation(
        self,
        session_id: UUID,
    ) -> AbstractAsyncContextManager[None]: ...

    async def schedule(self, accepted: AcceptedMessage) -> None: ...


class CurrentRuntimeFence(Protocol):
    async def __call__(
        self,
        *,
        user_id: UUID,
        platform: ExternalChannel,
        binding_generation: UUID,
        runtime_generation: UUID,
    ) -> bool: ...


class ChannelConfigLoader(Protocol):
    async def __call__(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        platform: ExternalChannel,
        for_update: bool,
    ) -> ChannelBindingSnapshot | None: ...


class ChannelContextFetcher(Protocol):
    async def __call__(
        self,
        user_id: UUID,
        event: ChannelEvent,
        *,
        limit: int,
    ) -> Sequence[ChannelContextMessage]: ...


class PolicyDelivery(Protocol):
    async def __call__(self, notice: PolicyNotice) -> None: ...


class PairingHandler(Protocol):
    async def __call__(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
    ) -> ExternalIngressResult | None: ...


@dataclass(slots=True)
class _LockEntry:
    lock: asyncio.Lock
    users: int = 0


class _KeyedLocks:
    def __init__(self) -> None:
        self._entries: dict[object, _LockEntry] = {}
        self._guard = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self._entries)

    @asynccontextmanager
    async def hold(self, key: object) -> AsyncIterator[None]:
        async with self._guard:
            entry = self._entries.get(key)
            if entry is None:
                entry = _LockEntry(asyncio.Lock())
                self._entries[key] = entry
            entry.users += 1

        acquired = False
        try:
            await entry.lock.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                entry.lock.release()
            async with self._guard:
                entry.users -= 1
                if entry.users == 0 and self._entries.get(key) is entry:
                    self._entries.pop(key)


@dataclass(frozen=True, slots=True)
class _ContextSelection:
    included: tuple[ChannelContextMessage, ...]
    receipt_dispositions: tuple[tuple[str, Literal["context", "context_omitted"]], ...]
    omitted_count: int


@dataclass(frozen=True, slots=True)
class _PublishOutcome:
    result: ExternalIngressResult
    accepted: AcceptedMessage | None = None


class ChannelIngress:
    def __init__(
        self,
        engine: AsyncEngine,
        runtime: IngressRuntime,
        *,
        is_current_runtime: CurrentRuntimeFence,
        config_loader: ChannelConfigLoader | None = None,
        context_fetcher: ChannelContextFetcher | None = None,
        policy_delivery: PolicyDelivery | None = None,
        pairing_handler: PairingHandler | None = None,
        owner_attachment_resolver: OwnerAttachmentResolutionResolver | None = None,
    ) -> None:
        self._engine = engine
        self._runtime = runtime
        self._is_current_runtime = is_current_runtime
        self._config_loader = config_loader or _load_current_config
        self._context_fetcher = context_fetcher
        self._policy_delivery = policy_delivery
        self._pairing_handler = pairing_handler
        self._owner_attachment_resolver = owner_attachment_resolver
        self._event_locks = _KeyedLocks()
        self._route_locks = _KeyedLocks()
        self._gate_open = True
        self._active_operations = 0
        self._operations_drained = asyncio.Event()
        self._operations_drained.set()

    @property
    def active_operations(self) -> int:
        return self._active_operations

    @property
    def active_event_locks(self) -> int:
        return self._event_locks.size

    @property
    def active_route_locks(self) -> int:
        return self._route_locks.size

    def close_gate(self) -> None:
        self._gate_open = False

    async def drain(self) -> None:
        if self._active_operations == 0:
            return
        waiter = asyncio.create_task(self._operations_drained.wait())
        await await_future_cancellation_safe(waiter)

    async def accept_external(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
    ) -> ExternalIngressResult:
        if not self._begin_operation():
            return ExternalIngressResult("shutting_down")
        try:
            return await self._accept_external(user_id=user_id, event=event)
        finally:
            self._finish_operation()

    async def _accept_external(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
    ) -> ExternalIngressResult:
        event = replace(event, text=event.text.strip())
        ignored_reason = _deterministic_ignore_reason(event)
        if ignored_reason is not None:
            return ExternalIngressResult("ignored", reason=ignored_reason)
        if not await self._runtime_is_current(user_id, event):
            return ExternalIngressResult("ignored", reason="stale_runtime")
        if self._pairing_handler is not None:
            pairing = await self._pairing_handler(user_id=user_id, event=event)
            if pairing is not None:
                return pairing

        event_key = (
            user_id,
            event.platform,
            event.binding_generation,
            event.chat_id,
            event.source_message_id,
        )
        async with self._event_locks.hold(event_key):
            message_id = external_message_id(user_id, event)
            if await self._receipt_exists(user_id, event):
                return ExternalIngressResult(
                    "duplicate",
                    reason="already_processed",
                    message_id=message_id,
                )

            binding = await self._read_binding(user_id, event.platform)
            binding_reason = _binding_mismatch_reason(binding, user_id, event)
            if binding_reason is not None:
                return ExternalIngressResult("ignored", reason=binding_reason)
            assert binding is not None

            classification = _classify_sender(binding, event.sender_id)
            if classification is None:
                return ExternalIngressResult("ignored", reason="unauthorized")

            attachment_resolution: OwnerAttachmentResolution | None = None
            if classification == "owner" and event.attachments:
                if self._owner_attachment_resolver is None:
                    return ExternalIngressResult(
                        "owner_attachment_unsupported",
                        reason="owner_attachment_resolver_unavailable",
                        message_id=message_id,
                    )
                attachment_resolution = await self._owner_attachment_resolver(
                    user_id=user_id,
                    event=event,
                    message_id=message_id,
                )
            return await self._complete_event(
                user_id=user_id,
                event=event,
                message_id=message_id,
                binding=binding,
                classification=classification,
                attachment_resolution=attachment_resolution,
            )

    def _begin_operation(self) -> bool:
        if not self._gate_open:
            return False
        self._active_operations += 1
        if self._active_operations == 1:
            self._operations_drained.clear()
        return True

    def _finish_operation(self) -> None:
        self._active_operations -= 1
        if self._active_operations == 0:
            self._operations_drained.set()

    async def _complete_event(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
        binding: ChannelBindingSnapshot,
        classification: SenderClassification,
        attachment_resolution: OwnerAttachmentResolution | None,
    ) -> ExternalIngressResult:
        attachments_published = False
        try:
            if attachment_resolution is not None:
                if not await self._runtime_is_current(user_id, event):
                    return ExternalIngressResult(
                        "ignored",
                        reason="stale_runtime",
                        message_id=message_id,
                    )
                if (
                    not attachment_resolution.attachment_refs
                    and not event.text.strip()
                ):
                    return await self._reject_attachment_only(
                        user_id=user_id,
                        event=event,
                        message_id=message_id,
                        expected_classification="owner",
                    )

            if (
                classification == "allowed_non_owner"
                and event.attachments
                and not event.text.strip()
            ):
                return await self._reject_attachment_only(
                    user_id=user_id,
                    event=event,
                    message_id=message_id,
                    expected_classification="allowed_non_owner",
                )

            if (
                not event.text.strip()
                and (
                    attachment_resolution is None
                    or not attachment_resolution.attachment_refs
                )
            ):
                if (
                    event.conversation_kind != "dm"
                    and event.explicitly_mentions_bot
                ):
                    return await self._prompt_mention_only(
                        user_id=user_id,
                        event=event,
                        message_id=message_id,
                        expected_classification=classification,
                    )
                return ExternalIngressResult(
                    "ignored",
                    reason="empty_message",
                    message_id=message_id,
                )

            context = await self._fetch_context(user_id, event)
            if not await self._runtime_is_current(user_id, event):
                return ExternalIngressResult(
                    "ignored",
                    reason="stale_runtime",
                    message_id=message_id,
                )

            route_key = (
                user_id,
                event.platform,
                binding.bot_identity,
                event.chat_id,
            )
            async with self._route_locks.hold(route_key):
                publishing = asyncio.create_task(
                    self._publish(
                        user_id=user_id,
                        event=event,
                        message_id=message_id,
                        bot_identity=binding.bot_identity,
                        expected_classification=classification,
                        resolved_content=(
                            attachment_resolution.content
                            if attachment_resolution is not None
                            else None
                        ),
                        attachment_refs=(
                            attachment_resolution.attachment_refs
                            if attachment_resolution is not None
                            else ()
                        ),
                        raw_context=context,
                    )
                )
                try:
                    outcome = await await_future_cancellation_safe(publishing)
                except BaseException:
                    attachments_published = _accepted_publish_result(publishing)
                    raise
                if outcome.accepted is None:
                    return outcome.result

                attachments_published = True
                handoff = asyncio.create_task(self._runtime.schedule(outcome.accepted))
                await await_future_cancellation_safe(handoff)
                return outcome.result
        finally:
            if not attachments_published:
                await _cleanup_resolution(attachment_resolution)

    async def _read_binding(
        self,
        user_id: UUID,
        platform: ExternalChannel,
    ) -> ChannelBindingSnapshot | None:
        async with AsyncSession(self._engine) as db:
            binding = await self._config_loader(
                db,
                user_id=user_id,
                platform=platform,
                for_update=False,
            )
            await db.rollback()
            return binding

    async def _receipt_exists(self, user_id: UUID, event: ChannelEvent) -> bool:
        async with AsyncSession(self._engine) as db:
            receipt_id = await db.scalar(
                select(ChannelMessageReceipt.id).where(
                    *_receipt_identity(user_id, event),
                )
            )
            await db.rollback()
            return receipt_id is not None

    async def _reject_attachment_only(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
        expected_classification: SenderClassification,
    ) -> ExternalIngressResult:
        return await self._deliver_policy_only(
            user_id=user_id,
            event=event,
            message_id=message_id,
            expected_classification=expected_classification,
            receipt_disposition="attachment_rejected",
            delivery_key_prefix="attachment-policy",
            content=(
                _OWNER_ATTACHMENT_ONLY_REJECTION
                if expected_classification == "owner"
                else _ATTACHMENT_ONLY_REJECTION
            ),
            disposition="attachment_rejected",
            reason=(
                "owner_attachments_failed"
                if expected_classification == "owner"
                else None
            ),
        )

    async def _prompt_mention_only(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
        expected_classification: SenderClassification,
    ) -> ExternalIngressResult:
        return await self._deliver_policy_only(
            user_id=user_id,
            event=event,
            message_id=message_id,
            expected_classification=expected_classification,
            receipt_disposition="trigger",
            delivery_key_prefix="empty-policy",
            content=_MENTION_ONLY_PROMPT,
            disposition="ignored",
            reason="empty_message_prompted",
        )

    async def _deliver_policy_only(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
        expected_classification: SenderClassification,
        receipt_disposition: Literal["trigger", "attachment_rejected"],
        delivery_key_prefix: str,
        content: str,
        disposition: IngressDisposition,
        reason: str | None,
    ) -> ExternalIngressResult:
        if not await self._runtime_is_current(user_id, event):
            return ExternalIngressResult(
                "ignored",
                reason="stale_runtime",
                message_id=message_id,
            )

        async with AsyncSession(self._engine) as db:
            try:
                provisional = _inbound_message(
                    user_id=user_id,
                    event=event,
                    message_id=message_id,
                    session_id=message_id,
                    bot_identity="policy-only",
                    classification=expected_classification,
                    context=(),
                )
                user = await lock_inbound_identity(db, provisional)
                if user is None:
                    await db.rollback()
                    return ExternalIngressResult("ignored", reason="missing_user")
                if not await self._runtime_is_current(user_id, event):
                    await db.rollback()
                    return ExternalIngressResult(
                        "ignored",
                        reason="stale_runtime",
                        message_id=message_id,
                    )
                binding = await self._config_loader(
                    db,
                    user_id=user_id,
                    platform=event.platform,
                    for_update=True,
                )
                binding_reason = _binding_mismatch_reason(binding, user_id, event)
                if binding_reason is not None:
                    await db.rollback()
                    return ExternalIngressResult(
                        "ignored",
                        reason=binding_reason,
                        message_id=message_id,
                    )
                assert binding is not None
                classification = _classify_sender(binding, event.sender_id)
                if classification is None:
                    await db.rollback()
                    return ExternalIngressResult(
                        "ignored",
                        reason="unauthorized",
                        message_id=message_id,
                    )
                if classification != expected_classification:
                    await db.rollback()
                    return ExternalIngressResult(
                        "ignored",
                        reason="unauthorized",
                        message_id=message_id,
                    )

                receipt_id = await db.scalar(
                    select(ChannelMessageReceipt.id).where(
                        *_receipt_identity(user_id, event),
                    )
                )
                if receipt_id is not None:
                    await db.rollback()
                    return ExternalIngressResult(
                        "duplicate",
                        reason="already_processed",
                        message_id=message_id,
                    )
                db.add(
                    _receipt(
                        user_id=user_id,
                        event=event,
                        session_id=None,
                        disposition=receipt_disposition,
                    )
                )
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

        if self._policy_delivery is not None:
            notice = PolicyNotice(
                delivery_key=f"{delivery_key_prefix}:{message_id}",
                user_id=user_id,
                channel=event.platform,
                chat_id=event.chat_id,
                binding_generation=event.binding_generation,
                source_message_id=event.source_message_id,
                content=content,
            )
            delivery = asyncio.create_task(self._policy_delivery(notice))
            await await_future_cancellation_safe(delivery)
        return ExternalIngressResult(
            disposition,
            reason=reason,
            message_id=message_id,
        )

    async def _fetch_context(
        self,
        user_id: UUID,
        event: ChannelEvent,
    ) -> tuple[ChannelContextMessage, ...]:
        if event.platform == "dingtalk":
            return _deduplicate_context(event.reply_context, event=event)
        if event.conversation_kind == "dm" or self._context_fetcher is None:
            return ()
        try:
            messages = await self._context_fetcher(
                user_id,
                event,
                limit=_MAX_CONTEXT_MESSAGES,
            )
        except Exception:
            _LOGGER.warning(
                "Channel context fetch failed",
                extra={
                    "event": "channel_context_fetch_failed",
                    "platform": event.platform,
                    "user_id": str(user_id),
                    "error_code": "channel_history_fetch_exception",
                    "context_count": 0,
                },
            )
            return ()
        return _deduplicate_context(messages, event=event)

    async def _publish(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
        bot_identity: str,
        expected_classification: SenderClassification,
        resolved_content: tuple[dict[str, object], ...] | None,
        attachment_refs: tuple[dict[str, object], ...],
        raw_context: tuple[ChannelContextMessage, ...],
    ) -> _PublishOutcome:
        session_key = f"{event.platform}:{bot_identity}:{event.chat_id}"
        existing_session_id = await self._lookup_session_id(user_id, session_key)
        session_id = existing_session_id or uuid4()
        route_was_present = existing_session_id is not None

        while True:
            async with self._runtime.session_operation(session_id):
                current_session_id = await self._lookup_session_id(user_id, session_key)
                if current_session_id != session_id:
                    if current_session_id is not None:
                        session_id = current_session_id
                        route_was_present = True
                        continue
                    if route_was_present:
                        session_id = uuid4()
                        route_was_present = False
                        continue
                return await self._publish_locked(
                    user_id=user_id,
                    event=event,
                    message_id=message_id,
                    session_id=session_id,
                    bot_identity=bot_identity,
                    expected_classification=expected_classification,
                    resolved_content=resolved_content,
                    attachment_refs=attachment_refs,
                    raw_context=raw_context,
                )

    async def _publish_locked(
        self,
        *,
        user_id: UUID,
        event: ChannelEvent,
        message_id: UUID,
        session_id: UUID,
        bot_identity: str,
        expected_classification: SenderClassification,
        resolved_content: tuple[dict[str, object], ...] | None,
        attachment_refs: tuple[dict[str, object], ...],
        raw_context: tuple[ChannelContextMessage, ...],
    ) -> _PublishOutcome:
        if not await self._runtime_is_current(user_id, event):
            return _PublishOutcome(
                ExternalIngressResult(
                    "ignored",
                    reason="stale_runtime",
                    message_id=message_id,
                )
            )
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            try:
                provisional = _inbound_message(
                    user_id=user_id,
                    event=event,
                    message_id=message_id,
                    session_id=session_id,
                    bot_identity=bot_identity,
                    classification="owner",
                    context=(),
                )
                user = await lock_inbound_identity(db, provisional)
                if user is None:
                    await db.rollback()
                    return _PublishOutcome(
                        ExternalIngressResult(
                            "ignored",
                            reason="missing_user",
                            message_id=message_id,
                        )
                    )
                if not await self._runtime_is_current(user_id, event):
                    await db.rollback()
                    return _PublishOutcome(
                        ExternalIngressResult(
                            "ignored",
                            reason="stale_runtime",
                            message_id=message_id,
                        )
                    )

                binding = await self._config_loader(
                    db,
                    user_id=user_id,
                    platform=event.platform,
                    for_update=True,
                )
                binding_reason = _binding_mismatch_reason(binding, user_id, event)
                if binding_reason is not None:
                    await db.rollback()
                    return _PublishOutcome(
                        ExternalIngressResult(
                            "ignored",
                            reason=binding_reason,
                            message_id=message_id,
                        )
                    )
                assert binding is not None
                if binding.bot_identity != bot_identity:
                    await db.rollback()
                    return _PublishOutcome(
                        ExternalIngressResult(
                            "ignored",
                            reason="stale_binding",
                            message_id=message_id,
                        )
                    )
                classification = _classify_sender(binding, event.sender_id)
                if classification is None:
                    await db.rollback()
                    return _PublishOutcome(
                        ExternalIngressResult(
                            "ignored",
                            reason="unauthorized",
                            message_id=message_id,
                        )
                    )
                if classification != expected_classification:
                    await db.rollback()
                    return _PublishOutcome(
                        ExternalIngressResult(
                            "ignored",
                            reason="unauthorized",
                            message_id=message_id,
                        )
                    )
                if (
                    classification == "owner"
                    and event.attachments
                    and resolved_content is None
                ):
                    await db.rollback()
                    return _PublishOutcome(
                        ExternalIngressResult(
                            "owner_attachment_unsupported",
                            reason="owner_attachment_resolver_unavailable",
                            message_id=message_id,
                        )
                    )

                receipt_id = await db.scalar(
                    select(ChannelMessageReceipt.id).where(
                        *_receipt_identity(user_id, event),
                    )
                )
                if receipt_id is not None:
                    await db.rollback()
                    return _PublishOutcome(
                        ExternalIngressResult(
                            "duplicate",
                            reason="already_processed",
                            message_id=message_id,
                        )
                    )

                context = await _select_context(
                    db,
                    user_id=user_id,
                    event=event,
                    messages=raw_context,
                )
                inbound = _inbound_message(
                    user_id=user_id,
                    event=event,
                    message_id=message_id,
                    session_id=session_id,
                    bot_identity=bot_identity,
                    classification=classification,
                    context=context.included,
                    content=resolved_content,
                    attachment_refs=attachment_refs,
                )
                accepted = await publish_inbound_locked(
                    db,
                    inbound=inbound,
                    title=_conversation_title(event),
                    runner_instance_id=self._runtime.runner_instance_id,
                    queue_if_busy=True,
                )
                assert accepted is not None
                if context.omitted_count:
                    pending = await db.get(PendingMessage, message_id)
                    assert pending is not None
                    pending.channel_context = [
                        *pending.channel_context,
                        {"_openoctopus_omitted_count": context.omitted_count},
                    ]
                for source_message_id, disposition in context.receipt_dispositions:
                    db.add(
                        _receipt(
                            user_id=user_id,
                            event=event,
                            source_message_id=source_message_id,
                            session_id=session_id,
                            disposition=disposition,
                        )
                    )
                db.add(
                    _receipt(
                        user_id=user_id,
                        event=event,
                        session_id=session_id,
                        disposition="trigger",
                    )
                )
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

        return _PublishOutcome(
            ExternalIngressResult(
                "accepted",
                message_id=message_id,
                session_id=session_id,
                context_included_count=len(context.included),
                context_omitted_count=context.omitted_count,
            ),
            accepted=accepted,
        )

    async def _lookup_session_id(
        self,
        user_id: UUID,
        session_key: str,
    ) -> UUID | None:
        async with AsyncSession(self._engine) as db:
            session_id = await db.scalar(
                select(Session.id).where(
                    Session.user_id == user_id,
                    Session.session_key == session_key,
                )
            )
            await db.rollback()
        return session_id

    async def _runtime_is_current(self, user_id: UUID, event: ChannelEvent) -> bool:
        return await self._is_current_runtime(
            user_id=user_id,
            platform=event.platform,
            binding_generation=event.binding_generation,
            runtime_generation=event.runtime_generation,
        )


async def _cleanup_resolution(
    resolution: OwnerAttachmentResolution | None,
) -> None:
    if resolution is None or resolution.cleanup_unpublished is None:
        return
    cleanup = asyncio.create_task(resolution.cleanup_unpublished())
    await await_future_cancellation_safe(cleanup)


def _accepted_publish_result(publishing: asyncio.Task[_PublishOutcome]) -> bool:
    if not publishing.done() or publishing.cancelled():
        return False
    try:
        outcome = publishing.result()
    except BaseException:
        return False
    return outcome.accepted is not None


def _conversation_title(event: ChannelEvent) -> str:
    label = event.conversation_label
    if isinstance(label, str):
        sanitized = " ".join(
            "".join(character if character.isprintable() else " " for character in label)
            .split()
        )
        if sanitized:
            return sanitized[:120]
    return "Discord chat" if event.platform == "discord" else "DingTalk chat"


def external_message_id(user_id: UUID, event: ChannelEvent) -> UUID:
    identity = json.dumps(
        [
            str(user_id),
            event.platform,
            str(event.binding_generation),
            event.chat_id,
            event.source_message_id,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return uuid5(_EXTERNAL_MESSAGE_NAMESPACE, identity)


async def _load_current_config(
    db: AsyncSession,
    *,
    user_id: UUID,
    platform: ExternalChannel,
    for_update: bool,
) -> ChannelBindingSnapshot | None:
    if platform == "discord":
        discord_statement = select(DiscordConfig).where(
            DiscordConfig.user_id == user_id
        )
        if for_update:
            discord_statement = discord_statement.with_for_update()
        row = await db.scalar(discord_statement)
        if row is None:
            return None
        return ChannelBindingSnapshot(
            user_id=row.user_id,
            platform="discord",
            bot_identity=row.application_id,
            binding_generation=row.binding_generation,
            owner_platform_user_id=row.owner_platform_user_id,
            allow_list=_string_allow_list(row.allow_list),
        )

    dingtalk_statement = select(DingTalkConfig).where(
        DingTalkConfig.user_id == user_id
    )
    if for_update:
        dingtalk_statement = dingtalk_statement.with_for_update()
    row = await db.scalar(dingtalk_statement)
    if row is None:
        return None
    return ChannelBindingSnapshot(
        user_id=row.user_id,
        platform="dingtalk",
        bot_identity=row.client_id,
        binding_generation=row.binding_generation,
        owner_platform_user_id=row.owner_platform_user_id,
        allow_list=_string_allow_list(row.allow_list),
    )


def _string_allow_list(values: Sequence[object]) -> tuple[str, ...]:
    return tuple(value for value in values if isinstance(value, str))


def _binding_mismatch_reason(
    binding: ChannelBindingSnapshot | None,
    user_id: UUID,
    event: ChannelEvent,
) -> str | None:
    if binding is None:
        return "stale_binding"
    if (
        binding.user_id != user_id
        or binding.platform != event.platform
        or binding.binding_generation != event.binding_generation
    ):
        return "stale_binding"
    return None


def _classify_sender(
    binding: ChannelBindingSnapshot,
    sender_id: str,
) -> SenderClassification | None:
    if sender_id == binding.owner_platform_user_id:
        return "owner"
    if sender_id in binding.allow_list:
        return "allowed_non_owner"
    return None


def _deterministic_ignore_reason(event: ChannelEvent) -> str | None:
    if event.sender_kind != "human":
        return "non_human_sender"
    if event.conversation_kind != "dm" and not event.explicitly_mentions_bot:
        return "not_addressed_to_bot"
    if len(event.text) > _MAX_TRIGGER_TEXT_CHARS:
        return "text_too_long"
    return None


def _receipt_identity(
    user_id: UUID,
    event: ChannelEvent,
) -> tuple[ColumnElement[bool], ...]:
    return (
        ChannelMessageReceipt.user_id == user_id,
        ChannelMessageReceipt.channel == event.platform,
        ChannelMessageReceipt.binding_generation == event.binding_generation,
        ChannelMessageReceipt.chat_id == event.chat_id,
        ChannelMessageReceipt.source_message_id == event.source_message_id,
    )


def _receipt(
    *,
    user_id: UUID,
    event: ChannelEvent,
    session_id: UUID | None,
    disposition: Literal[
        "context",
        "context_omitted",
        "trigger",
        "attachment_rejected",
    ],
    source_message_id: str | None = None,
) -> ChannelMessageReceipt:
    return ChannelMessageReceipt(
        id=uuid4(),
        user_id=user_id,
        session_id=session_id,
        channel=event.platform,
        binding_generation=event.binding_generation,
        chat_id=event.chat_id,
        source_message_id=(
            event.source_message_id
            if source_message_id is None
            else source_message_id
        ),
        disposition=disposition,
    )


def _inbound_message(
    *,
    user_id: UUID,
    event: ChannelEvent,
    message_id: UUID,
    session_id: UUID,
    bot_identity: str,
    classification: SenderClassification,
    context: tuple[ChannelContextMessage, ...],
    content: tuple[dict[str, object], ...] | None = None,
    attachment_refs: tuple[dict[str, object], ...] = (),
) -> InboundMessage:
    profile: ToolProfile = (
        "owner_full" if classification == "owner" else "message_only"
    )
    if content is None:
        resolved_content: list[dict[str, object]] = []
        if classification == "allowed_non_owner" and event.attachments:
            resolved_content.append({"type": "text", "text": _ATTACHMENT_REJECTION})
        resolved_content.append({"type": "text", "text": event.text})
    else:
        resolved_content = [dict(block) for block in content]
    return InboundMessage(
        message_id=message_id,
        owner_user_id=user_id,
        session_id=session_id,
        session_key=f"{event.platform}:{bot_identity}:{event.chat_id}",
        channel=event.platform,
        chat_id=event.chat_id,
        source_message_id=event.source_message_id,
        channel_binding_generation=event.binding_generation,
        sender=InboundSender(
            id=event.sender_id,
            display_name=event.sender_display_name,
            classification=classification,
        ),
        ingress_tool_profile=profile,
        content=tuple(resolved_content),
        attachment_refs=tuple(dict(ref) for ref in attachment_refs),
        channel_context=context,
    )


def _deduplicate_context(
    messages: Sequence[ChannelContextMessage],
    *,
    event: ChannelEvent,
) -> tuple[ChannelContextMessage, ...]:
    deduplicated_reversed: list[ChannelContextMessage] = []
    seen_ids: set[str] = {event.source_message_id}
    for message in reversed(messages[-_MAX_CONTEXT_MESSAGES:]):
        source_message_id = message.source_message_id
        if source_message_id is not None:
            if source_message_id in seen_ids:
                continue
            seen_ids.add(source_message_id)
        deduplicated_reversed.append(message)
    deduplicated_reversed.reverse()
    return tuple(deduplicated_reversed)


async def _select_context(
    db: AsyncSession,
    *,
    user_id: UUID,
    event: ChannelEvent,
    messages: tuple[ChannelContextMessage, ...],
) -> _ContextSelection:
    source_ids = {
        message.source_message_id
        for message in messages
        if message.source_message_id is not None
    }
    existing_ids: set[str] = set()
    if source_ids:
        existing_ids = set(
            (
                await db.scalars(
                    select(ChannelMessageReceipt.source_message_id).where(
                        ChannelMessageReceipt.user_id == user_id,
                        ChannelMessageReceipt.channel == event.platform,
                        ChannelMessageReceipt.binding_generation
                        == event.binding_generation,
                        ChannelMessageReceipt.chat_id == event.chat_id,
                        ChannelMessageReceipt.source_message_id.in_(source_ids),
                    )
                )
            ).all()
        )

    candidates = tuple(
        message
        for message in messages
        if message.source_message_id is None
        or message.source_message_id not in existing_ids
    )
    included_indices: set[int] = set()
    remaining_chars = _MAX_CONTEXT_CHARS
    for index in range(len(candidates) - 1, -1, -1):
        size = _context_chars(candidates[index])
        if size <= remaining_chars:
            included_indices.add(index)
            remaining_chars -= size

    included = tuple(
        message
        for index, message in enumerate(candidates)
        if index in included_indices
    )
    receipt_dispositions: list[
        tuple[str, Literal["context", "context_omitted"]]
    ] = []
    for index, message in enumerate(candidates):
        if message.source_message_id is None:
            continue
        disposition: Literal["context", "context_omitted"] = (
            "context" if index in included_indices else "context_omitted"
        )
        receipt_dispositions.append((message.source_message_id, disposition))
    return _ContextSelection(
        included=included,
        receipt_dispositions=tuple(receipt_dispositions),
        omitted_count=len(candidates) - len(included),
    )


def _context_chars(message: ChannelContextMessage) -> int:
    return len(message.text) + sum(len(summary) for summary in message.attachment_summaries)
