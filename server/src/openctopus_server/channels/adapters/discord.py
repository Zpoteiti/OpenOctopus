from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol, cast
from urllib.parse import quote
from uuid import UUID

import httpx

from openctopus_server.channels.attachments import AuthenticatedAttachmentStream
from openctopus_server.services.channels import (
    ChannelCredentialsInvalidError,
    ChannelCredentialsUnverifiedError,
    ValidatedBotIdentity,
)

from ..delivery import (
    MAX_DELIVERY_ACTIONS,
    ActionResult,
    DeliveryPlanTooLargeError,
    split_text_actions,
)
from ..types import (
    ChannelCapabilities,
    ChannelContextMessage,
    ChannelEvent,
    DeliveryAction,
    DeliveryPlan,
    ExternalAttachmentDescriptor,
    OutboundMessage,
    ResolvedDeliveryFile,
)
from .base import ActionIssueHook, ChannelEventSink, ContextFetchResult
from .http_attachment import open_http_attachment

DISCORD_TEXT_LIMIT = 2_000
DISCORD_NONCE_LIMIT = 25
DISCORD_MAX_REQUEST_BYTES = 25 * 1024 * 1024
DISCORD_START_TIMEOUT_SECONDS = 30.0
DISCORD_CONVERSATION_LABEL_LIMIT = 120
DISCORD_API_BASE_URL = "https://discord.com/api/v10/"

_HISTORY_ERROR_CODE = "discord_history_unavailable"
_HISTORY_ERROR_MESSAGE = "Discord conversation history is unavailable."
_REQUEST_REJECTED_CODE = "discord_request_rejected"
_RATE_LIMITED_CODE = "discord_rate_limited"
_OUTCOME_UNKNOWN_CODE = "discord_outcome_unknown"
_REQUEST_TOO_LARGE_CODE = "discord_request_too_large"
_MEDIA_SIZE_UNKNOWN_CODE = "tool_channel_media_size_unknown"


@dataclass(frozen=True, slots=True)
class DiscordRESTResponse:
    status_code: int
    message_id: str | None


@dataclass(frozen=True, slots=True)
class DiscordCreateMessageRequest:
    chat_id: str
    payload: dict[str, object]
    media: ResolvedDeliveryFile | None
    estimated_size: int


class DiscordOutcomeUnknownError(Exception):
    """A create-message request was issued but its result could not be observed."""


class DiscordRequestNotIssuedError(Exception):
    """A local prerequisite failed before the Discord request was issued."""


class DiscordRESTClient(Protocol):
    async def create_message(
        self,
        request: DiscordCreateMessageRequest,
        *,
        on_issued: ActionIssueHook,
    ) -> DiscordRESTResponse: ...

    async def open_authenticated_attachment(
        self,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream: ...

    async def close(self) -> None: ...


class DiscordMediaStream(Protocol):
    async def read(self) -> bytes: ...

    async def aclose(self) -> None: ...


class DiscordMediaOpener(Protocol):
    async def __call__(self, media: ResolvedDeliveryFile) -> DiscordMediaStream: ...


class DiscordGatewayClient(Protocol):
    async def start(self, token: str, *, reconnect: bool) -> None: ...

    async def wait_until_ready(self) -> None: ...

    async def close(self) -> None: ...

    def get_channel(self, channel_id: int) -> object | None: ...

    async def fetch_channel(self, channel_id: int) -> object: ...


type DiscordGatewayFactory = Callable[
    [Callable[[object], Awaitable[None]]],
    DiscordGatewayClient,
]


class _DiscordPyGateway:
    def __init__(self, client: Any) -> None:
        self._client = client
        self._initialized = asyncio.Event()

    async def start(self, token: str, *, reconnect: bool) -> None:
        async with self._client:
            self._initialized.set()
            await self._client.start(token, reconnect=reconnect)

    async def wait_until_ready(self) -> None:
        await self._initialized.wait()
        await self._client.wait_until_ready()

    async def close(self) -> None:
        await self._client.close()

    def get_channel(self, channel_id: int) -> object | None:
        return cast(object | None, self._client.get_channel(channel_id))

    async def fetch_channel(self, channel_id: int) -> object:
        return cast(object, await self._client.fetch_channel(channel_id))


@dataclass(frozen=True, slots=True)
class _Snowflake:
    id: int


class DiscordCredentialValidator:
    """Validate a Bot token through Discord's typed identity endpoints."""

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient | None = None,
        api_base_url: str = DISCORD_API_BASE_URL,
    ) -> None:
        self._http_client = http_client
        self._api_base_url = api_base_url

    async def validate_discord(self, bot_token: str) -> ValidatedBotIdentity:
        if (
            not bot_token
            or bot_token.strip() != bot_token
            or any(ord(character) < 32 for character in bot_token)
        ):
            raise ChannelCredentialsInvalidError("Discord Bot token is invalid")

        if self._http_client is not None:
            return await self._validate(self._http_client, bot_token)

        transport = httpx.AsyncHTTPTransport(retries=0)
        async with httpx.AsyncClient(
            base_url=self._api_base_url,
            transport=transport,
            timeout=httpx.Timeout(15.0),
            follow_redirects=False,
        ) as client:
            return await self._validate(client, bot_token)

    async def _validate(
        self,
        client: httpx.AsyncClient,
        bot_token: str,
    ) -> ValidatedBotIdentity:
        headers = {"Authorization": f"Bot {bot_token}"}
        try:
            application_response = await client.get(
                "oauth2/applications/@me",
                headers=headers,
            )
            _raise_for_identity_status(application_response.status_code)
            user_response = await client.get("users/@me", headers=headers)
            _raise_for_identity_status(user_response.status_code)
            application = application_response.json()
            user = user_response.json()
            return _validated_identity(application, user)
        except asyncio.CancelledError:
            raise
        except ChannelCredentialsInvalidError:
            raise
        except ChannelCredentialsUnverifiedError:
            raise
        except (httpx.HTTPError, TimeoutError, OSError, ValueError, TypeError):
            raise ChannelCredentialsUnverifiedError(
                "Discord Bot identity could not be verified"
            ) from None


class DiscordAdapter:
    platform: Literal["discord"] = "discord"
    capabilities = ChannelCapabilities(history_backfill=True, file_delivery=True)

    def __init__(
        self,
        *,
        bot_token: str,
        bot_user_id: str,
        binding_generation: UUID,
        runtime_generation: UUID,
        gateway_factory: DiscordGatewayFactory | None = None,
        rest_client: DiscordRESTClient | None = None,
        media_opener: DiscordMediaOpener | None = None,
    ) -> None:
        self._bot_token = bot_token
        self._bot_user_id = bot_user_id
        self.binding_generation = binding_generation
        self.runtime_generation = runtime_generation
        factory = gateway_factory or _build_discord_gateway
        self._gateway = factory(self._on_message)
        self._rest = rest_client or DiscordHTTPRESTClient(
            bot_token,
            media_opener=media_opener,
        )
        self._sink: ChannelEventSink | None = None
        self._client_task: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Task[None] | None = None
        self._lifecycle_lock = asyncio.Lock()

    async def start(self, sink: ChannelEventSink) -> None:
        async with self._lifecycle_lock:
            if self._stop_task is not None:
                raise RuntimeError("Discord adapter is stopping")
            self._sink = sink
            if self._client_task is None:
                self._client_task = asyncio.create_task(
                    self._gateway.start(self._bot_token, reconnect=True)
                )
            client_task = self._client_task

        ready_task = asyncio.create_task(self._wait_until_gateway_ready())
        try:
            done, _ = await asyncio.wait(
                (client_task, ready_task),
                return_when=asyncio.FIRST_COMPLETED,
                timeout=DISCORD_START_TIMEOUT_SECONDS,
            )
            if not done:
                ready_task.cancel()
                await asyncio.gather(ready_task, return_exceptions=True)
                cleanup = asyncio.create_task(self.stop())
                await _wait_through_cancellation(cleanup)
                raise RuntimeError(
                    "Discord Gateway did not become ready before startup timeout"
                )
            if client_task in done:
                ready_task.cancel()
                await asyncio.gather(ready_task, return_exceptions=True)
                try:
                    await client_task
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if type(exc).__name__ == "PrivilegedIntentsRequired":
                        raise RuntimeError(
                            "Discord MESSAGE_CONTENT intent is not enabled"
                        ) from None
                    raise RuntimeError("Discord Gateway failed before becoming ready") from None
                raise RuntimeError("Discord Gateway closed before becoming ready")
            try:
                await ready_task
            except asyncio.CancelledError:
                raise
            except Exception:
                cleanup = asyncio.create_task(self.stop())
                await _wait_through_cancellation(cleanup)
                raise RuntimeError("Discord Gateway failed before becoming ready") from None
        except asyncio.CancelledError:
            ready_task.cancel()
            await asyncio.gather(ready_task, return_exceptions=True)
            cleanup = asyncio.create_task(self.stop())
            await _wait_through_cancellation(cleanup)
            raise

    async def wait_closed(self) -> None:
        task = self._client_task
        if task is None:
            return
        try:
            await task
        except asyncio.CancelledError:
            raise
        except Exception:
            raise RuntimeError("Discord Gateway connection closed unexpectedly") from None

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            if self._stop_task is None:
                self._stop_task = asyncio.create_task(self._stop_once())
            task = self._stop_task
        await _wait_through_cancellation(task)

    async def open_authenticated_attachment(
        self,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream:
        if (
            event.platform != self.platform
            or event.binding_generation != self.binding_generation
            or event.runtime_generation != self.runtime_generation
            or attachment not in event.attachments
        ):
            raise ValueError("Discord attachment event is not current")
        return await self._rest.open_authenticated_attachment(event, attachment)

    async def _stop_once(self) -> None:
        self._sink = None
        gateway_failure: BaseException | None = None
        try:
            await self._gateway.close()
        except BaseException as exc:
            gateway_failure = exc
        task = self._client_task
        if task is not None:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await self._rest.close()
        finally:
            if gateway_failure is not None:
                raise RuntimeError("Discord Gateway close failed") from None

    async def _wait_until_gateway_ready(self) -> None:
        await self._gateway.wait_until_ready()

    async def _on_message(self, message: object) -> None:
        sink = self._sink
        if sink is None:
            return
        event = _normalize_event(
            message,
            bot_user_id=self._bot_user_id,
            binding_generation=self.binding_generation,
            runtime_generation=self.runtime_generation,
        )
        if event is not None:
            await sink(event)

    async def fetch_recent_context(
        self,
        *,
        chat_id: str,
        before_message_id: str,
        limit: Literal[100],
    ) -> ContextFetchResult:
        if limit != 100:
            raise ValueError("Discord context fetch limit must be exactly 100")
        try:
            channel_id = _discord_id(chat_id)
            before_id = _discord_id(before_message_id)
            channel = self._gateway.get_channel(channel_id)
            if channel is None:
                channel = await self._gateway.fetch_channel(channel_id)
            if _conversation_kind(channel) == "dm":
                return ContextFetchResult(status="unsupported")

            history = getattr(channel, "history")(
                limit=100,
                before=_Snowflake(before_id),
                oldest_first=False,
            )
            newest_first: list[ChannelContextMessage] = []
            async for message in history:
                context = _normalize_context_message(message, self._bot_user_id)
                if context is not None:
                    newest_first.append(context)
                if len(newest_first) == 100:
                    break
            return ContextFetchResult(
                status="available",
                messages=tuple(reversed(newest_first)),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return ContextFetchResult(
                status="failed",
                error_code=_HISTORY_ERROR_CODE,
                error_message=_HISTORY_ERROR_MESSAGE,
            )

    def plan_delivery(self, message: OutboundMessage) -> DeliveryPlan:
        if message.channel != "discord":
            raise ValueError("Discord planning requires a Discord outbound message")
        if len(message.media) >= MAX_DELIVERY_ACTIONS:
            raise DeliveryPlanTooLargeError(
                f"Delivery plan exceeds the {MAX_DELIVERY_ACTIONS}-action limit"
            )
        chunks = split_text_actions(
            message.content,
            max_chars=DISCORD_TEXT_LIMIT,
            max_actions=MAX_DELIVERY_ACTIONS - len(message.media),
        )
        actions: list[DeliveryAction] = [
            DeliveryAction(
                kind="text_message",
                visible=True,
                content=chunk,
                chat_id=message.chat_id,
                idempotency_key=discord_action_nonce(message.delivery_key, index),
            )
            for index, chunk in enumerate(chunks)
        ]
        for media_index, media in enumerate(message.media):
            action_index = len(actions)
            actions.append(
                DeliveryAction(
                    kind="file_message",
                    visible=True,
                    media_index=media_index,
                    chat_id=message.chat_id,
                    idempotency_key=discord_action_nonce(
                        message.delivery_key,
                        action_index,
                    ),
                    media=media,
                )
            )
        return DeliveryPlan(actions=tuple(actions))

    async def execute_action(
        self,
        action: DeliveryAction,
        *,
        on_issued: ActionIssueHook,
    ) -> ActionResult:
        prepared = _prepare_create_message(action)
        if isinstance(prepared, ActionResult):
            return prepared
        issue_error: Exception | None = None

        async def report_issued() -> None:
            nonlocal issue_error
            try:
                await on_issued()
            except Exception as exc:
                issue_error = exc
                raise

        try:
            response = await self._rest.create_message(
                prepared,
                on_issued=report_issued,
            )
        except asyncio.CancelledError:
            raise
        except DiscordRequestNotIssuedError:
            if issue_error is not None:
                raise issue_error
            return ActionResult(
                status="failed",
                error_code="discord_media_unavailable",
                error_message="Discord media could not be opened before sending.",
            )
        except (DiscordOutcomeUnknownError, httpx.TransportError, TimeoutError, OSError):
            if issue_error is not None:
                raise issue_error
            return _unknown_result()
        except Exception:
            if issue_error is not None:
                raise issue_error
            return _unknown_result()

        if 200 <= response.status_code < 300:
            return ActionResult(
                status="sent",
                platform_message_id=response.message_id,
            )
        if response.status_code == 429:
            return ActionResult(
                status="failed",
                error_code=_RATE_LIMITED_CODE,
                error_message="Discord rejected the action because it was rate limited.",
            )
        if 400 <= response.status_code < 500:
            return ActionResult(
                status="failed",
                error_code=_REQUEST_REJECTED_CODE,
                error_message="Discord definitively rejected the action.",
            )
        return _unknown_result()


def plan_discord_text_delivery(message: OutboundMessage) -> DeliveryPlan:
    """Build the bounded, ordered Discord text actions for an outbound message."""
    if message.channel != "discord":
        raise ValueError("Discord planning requires a Discord outbound message")
    chunks = split_text_actions(message.content, max_chars=DISCORD_TEXT_LIMIT)
    return DeliveryPlan(
        actions=tuple(
            DeliveryAction(
                kind="text_message",
                visible=True,
                content=chunk,
                chat_id=message.chat_id,
                idempotency_key=discord_action_nonce(message.delivery_key, index),
            )
            for index, chunk in enumerate(chunks)
        )
    )


def discord_action_nonce(delivery_key: str, action_index: int) -> str:
    if action_index < 0:
        raise ValueError("Discord action index must not be negative")
    material = f"discord\0{delivery_key}\0{action_index}".encode()
    return hashlib.sha256(material).hexdigest()[:DISCORD_NONCE_LIMIT]


def _prepare_create_message(
    action: DeliveryAction,
) -> DiscordCreateMessageRequest | ActionResult:
    if action.chat_id is None or action.idempotency_key is None:
        return _failed_action("discord_action_invalid", "Discord action facts are incomplete.")
    try:
        _discord_id(action.chat_id)
    except ValueError:
        return _failed_action("discord_action_invalid", "Discord target is invalid.")
    if not 1 <= len(action.idempotency_key) <= DISCORD_NONCE_LIMIT:
        return _failed_action("discord_action_invalid", "Discord action nonce is invalid.")

    payload: dict[str, object] = {
        "nonce": action.idempotency_key,
        "enforce_nonce": True,
        "allowed_mentions": {"parse": []},
    }
    media: ResolvedDeliveryFile | None = None
    if action.kind == "text_message":
        if (
            action.content is None
            or not action.content
            or len(action.content) > DISCORD_TEXT_LIMIT
            or action.media is not None
        ):
            return _failed_action("discord_action_invalid", "Discord text action is invalid.")
        payload["content"] = action.content
    elif action.kind == "file_message":
        media = action.media
        if media is None or not _safe_media_filename(media.filename):
            return _failed_action("discord_action_invalid", "Discord media action is invalid.")
        if media.size is None:
            return _failed_action(
                _MEDIA_SIZE_UNKNOWN_CODE,
                "Discord requires a known media size before sending.",
            )
        if media.size < 0:
            return _failed_action("discord_action_invalid", "Discord media size is invalid.")
        payload["attachments"] = [{"id": "0", "filename": media.filename}]
    else:
        return _failed_action("discord_action_invalid", "Discord action kind is unsupported.")

    estimated_size = _estimate_request_size(payload, media, action.idempotency_key)
    if estimated_size > DISCORD_MAX_REQUEST_BYTES:
        return _failed_action(
            _REQUEST_TOO_LARGE_CODE,
            "Discord create-message request exceeds 25 MiB.",
        )
    return DiscordCreateMessageRequest(
        chat_id=action.chat_id,
        payload=payload,
        media=media,
        estimated_size=estimated_size,
    )


def _failed_action(code: str, message: str) -> ActionResult:
    return ActionResult(status="failed", error_code=code, error_message=message)


def _unknown_result() -> ActionResult:
    return ActionResult(
        status="unknown",
        error_code=_OUTCOME_UNKNOWN_CODE,
        error_message="Discord action outcome is unknown after issue.",
    )


def _normalize_event(
    message: object,
    *,
    bot_user_id: str,
    binding_generation: UUID,
    runtime_generation: UUID,
) -> ChannelEvent | None:
    if _ignore_message(message):
        return None
    channel = getattr(message, "channel", None)
    author = getattr(message, "author", None)
    if channel is None or author is None:
        return None
    kind = _conversation_kind(channel)
    mentions_bot = _mentions_bot(message, bot_user_id)
    if kind != "dm" and not mentions_bot:
        return None
    source_message_id = _optional_id(getattr(message, "id", None))
    chat_id = _optional_id(getattr(channel, "id", None))
    sender_id = _optional_id(getattr(author, "id", None))
    if source_message_id is None or chat_id is None or sender_id is None:
        return None
    text = getattr(message, "content", "")
    if not isinstance(text, str):
        text = ""
    return ChannelEvent(
        platform="discord",
        binding_generation=binding_generation,
        runtime_generation=runtime_generation,
        source_message_id=source_message_id,
        chat_id=chat_id,
        conversation_kind=kind,
        sender_id=sender_id,
        sender_display_name=_sender_display_name(author),
        sender_kind="human",
        explicitly_mentions_bot=mentions_bot,
        text=_remove_bot_mention(text, bot_user_id),
        attachments=_attachment_descriptors(message),
        conversation_label=_discord_conversation_label(channel, kind),
    )


def _normalize_context_message(
    message: object,
    bot_user_id: str,
) -> ChannelContextMessage | None:
    if _ignore_message(message):
        return None
    author = getattr(message, "author", None)
    if author is None:
        return None
    source_message_id = _optional_id(getattr(message, "id", None))
    sender_id = _optional_id(getattr(author, "id", None))
    text = getattr(message, "content", "")
    if not isinstance(text, str):
        text = ""
    created_at = getattr(message, "created_at", None)
    sent_at = created_at.isoformat() if isinstance(created_at, datetime) else None
    return ChannelContextMessage(
        source_message_id=source_message_id,
        sender_id=sender_id,
        sender_display_name=_sender_display_name(author),
        sent_at=sent_at,
        text=_remove_bot_mention(text, bot_user_id),
        attachment_summaries=_context_attachment_summaries(message),
    )


def _ignore_message(message: object) -> bool:
    author = getattr(message, "author", None)
    if author is None or bool(getattr(author, "bot", False)):
        return True
    if getattr(message, "webhook_id", None) is not None:
        return True
    is_system = getattr(message, "is_system", None)
    return bool(is_system()) if callable(is_system) else False


def _mentions_bot(message: object, bot_user_id: str) -> bool:
    return any(
        _optional_id(getattr(mention, "id", None)) == bot_user_id
        for mention in (getattr(message, "mentions", ()) or ())
    )


def _remove_bot_mention(text: str, bot_user_id: str) -> str:
    return text.replace(f"<@{bot_user_id}>", "").replace(f"<@!{bot_user_id}>", "")


def _conversation_kind(channel: object) -> Literal["dm", "group", "thread"]:
    class_names = {base.__name__ for base in type(channel).__mro__}
    if "DMChannel" in class_names:
        return "dm"
    if "Thread" in class_names:
        return "thread"
    return "group"


def _discord_conversation_label(
    channel: object,
    kind: Literal["dm", "group", "thread"],
) -> str | None:
    recipient = getattr(channel, "recipient", None)
    guild = getattr(channel, "guild", None)
    if kind == "dm":
        candidates = (
            getattr(recipient, "display_name", None),
            getattr(recipient, "global_name", None),
            getattr(recipient, "name", None),
            getattr(channel, "name", None),
            getattr(guild, "name", None),
        )
    else:
        candidates = (
            getattr(channel, "name", None),
            getattr(guild, "name", None),
            getattr(recipient, "display_name", None),
            getattr(recipient, "global_name", None),
            getattr(recipient, "name", None),
        )
    return _bounded_conversation_label(
        candidates,
        limit=DISCORD_CONVERSATION_LABEL_LIMIT,
    )


def _bounded_conversation_label(
    candidates: tuple[object, ...],
    *,
    limit: int,
) -> str | None:
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        printable = "".join(
            character if character.isprintable() else " " for character in candidate
        )
        collapsed = " ".join(printable.split())
        if collapsed:
            return collapsed[:limit]
    return None


def _attachment_descriptors(
    message: object,
) -> tuple[ExternalAttachmentDescriptor, ...]:
    descriptors: list[ExternalAttachmentDescriptor] = []
    for attachment in getattr(message, "attachments", ()) or ():
        source_id = _optional_id(getattr(attachment, "id", None))
        filename = getattr(attachment, "filename", None)
        if source_id is None or not isinstance(filename, str):
            continue
        content_type = getattr(attachment, "content_type", None)
        if not isinstance(content_type, str):
            content_type = None
        size = getattr(attachment, "size", None)
        if not isinstance(size, int) or size < 0:
            size = None
        descriptors.append(
            ExternalAttachmentDescriptor(
                source_id=source_id,
                filename=filename,
                content_type=content_type,
                size=size,
            )
        )
    return tuple(descriptors)


def _context_attachment_summaries(message: object) -> tuple[str, ...]:
    summaries: list[str] = []
    for attachment in getattr(message, "attachments", ()) or ():
        filename = _bounded_conversation_label(
            (getattr(attachment, "filename", None),),
            limit=255,
        )
        content_type = _bounded_conversation_label(
            (getattr(attachment, "content_type", None),),
            limit=255,
        )
        if filename is not None and content_type is not None:
            summaries.append(f"{filename} ({content_type})")
        elif filename is not None:
            summaries.append(filename)
        elif content_type is not None:
            summaries.append(f"attachment ({content_type})")
        else:
            summaries.append("attachment")
    return tuple(summaries)


def _sender_display_name(author: object) -> str | None:
    for attribute in ("display_name", "global_name", "name"):
        value = getattr(author, attribute, None)
        if isinstance(value, str) and value:
            return value
    return None


def _optional_id(value: object) -> str | None:
    if isinstance(value, (str, int)):
        result = str(value)
        return result if result else None
    return None


def _discord_id(value: str) -> int:
    if not value.isascii() or not value.isdigit() or not 1 <= len(value) <= 20:
        raise ValueError("Discord IDs must be decimal snowflakes")
    return int(value)


def _safe_media_filename(filename: str) -> bool:
    return (
        bool(filename)
        and "\x00" not in filename
        and "\r" not in filename
        and "\n" not in filename
    )


def _estimate_request_size(
    payload: dict[str, object],
    media: ResolvedDeliveryFile | None,
    nonce: str,
) -> int:
    encoded_payload = _encode_payload(payload)
    if media is None:
        return len(encoded_payload)
    if media.size is None:
        raise ValueError("Discord media size must be known")
    prefix, suffix, _ = _multipart_parts(payload, media, nonce)
    return len(prefix) + media.size + len(suffix)


def _encode_payload(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def _multipart_parts(
    payload: dict[str, object],
    media: ResolvedDeliveryFile,
    nonce: str,
) -> tuple[bytes, bytes, str]:
    boundary = f"----openoctopus-{nonce}"
    encoded_filename = quote(media.filename, safe="")
    mime = (
        media.mime
        if "\r" not in media.mime and "\n" not in media.mime
        else "application/octet-stream"
    )
    prefix = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="payload_json"\r\n'
        "Content-Type: application/json\r\n\r\n"
    ).encode() + _encode_payload(payload) + (
        f"\r\n--{boundary}\r\n"
        'Content-Disposition: form-data; name="files[0]"; filename="upload"; '
        f"filename*=UTF-8''{encoded_filename}\r\n"
        f"Content-Type: {mime}\r\n\r\n"
    ).encode()
    suffix = f"\r\n--{boundary}--\r\n".encode()
    return prefix, suffix, boundary


class DiscordHTTPRESTClient:
    def __init__(
        self,
        bot_token: str,
        *,
        media_opener: DiscordMediaOpener | None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._bot_token = bot_token
        self._media_opener = media_opener
        self._closed = False
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            base_url=DISCORD_API_BASE_URL,
            transport=httpx.AsyncHTTPTransport(retries=0),
            timeout=httpx.Timeout(30.0),
            follow_redirects=False,
        )

    async def create_message(
        self,
        request: DiscordCreateMessageRequest,
        *,
        on_issued: ActionIssueHook,
    ) -> DiscordRESTResponse:
        if self._closed:
            raise DiscordRequestNotIssuedError
        path = f"channels/{request.chat_id}/messages"
        if request.media is None:
            await on_issued()
            try:
                response = await self._client.post(
                    path,
                    headers={"Authorization": f"Bot {self._bot_token}"},
                    json=request.payload,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise DiscordOutcomeUnknownError from None
            return _rest_response(response)

        opener = self._media_opener
        if opener is None:
            raise DiscordRequestNotIssuedError
        try:
            stream = await opener(request.media)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise DiscordRequestNotIssuedError from None

        nonce = cast(str, request.payload["nonce"])
        prefix, suffix, boundary = _multipart_parts(request.payload, request.media, nonce)
        body = _multipart_body(
            stream,
            prefix=prefix,
            suffix=suffix,
            expected_size=cast(int, request.media.size),
        )
        try:
            await on_issued()
            try:
                response = await self._client.post(
                    path,
                    headers={
                        "Authorization": f"Bot {self._bot_token}",
                        "Content-Type": f"multipart/form-data; boundary={boundary}",
                        "Content-Length": str(request.estimated_size),
                    },
                    content=body,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise DiscordOutcomeUnknownError from None
        finally:
            close_task = asyncio.create_task(stream.aclose())
            await _wait_through_cancellation(close_task)
        return _rest_response(response)

    async def open_authenticated_attachment(
        self,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream:
        if self._closed or attachment.size is None:
            raise DiscordRequestNotIssuedError
        try:
            channel_id = str(_discord_id(event.chat_id))
            message_id = str(_discord_id(event.source_message_id))
            attachment_id = str(_discord_id(attachment.source_id))
            response = await self._client.get(
                f"channels/{channel_id}/messages/{message_id}",
                headers={"Authorization": f"Bot {self._bot_token}"},
                follow_redirects=False,
            )
            if not 200 <= response.status_code < 300:
                raise ValueError
            body = response.json()
            platform_attachment = _discord_attachment_from_message(
                body,
                attachment_id=attachment_id,
            )
            if (
                platform_attachment[0] != attachment.filename
                or platform_attachment[1] != attachment.size
                or not _valid_discord_attachment_url(
                    platform_attachment[2],
                    channel_id=channel_id,
                    attachment_id=attachment_id,
                )
            ):
                raise ValueError
            return await open_http_attachment(
                self._client,
                platform_attachment[2],
                size=attachment.size,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise DiscordRequestNotIssuedError from None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            await self._client.aclose()


async def _multipart_body(
    stream: DiscordMediaStream,
    *,
    prefix: bytes,
    suffix: bytes,
    expected_size: int,
) -> AsyncIterator[bytes]:
    yield prefix
    observed = 0
    while True:
        chunk = await stream.read()
        if not isinstance(chunk, bytes):
            raise TypeError("Discord media stream returned a non-bytes chunk")
        if not chunk:
            break
        observed += len(chunk)
        if observed > expected_size:
            raise ValueError("Discord media stream exceeded its declared size")
        yield chunk
    if observed != expected_size:
        raise ValueError("Discord media stream did not match its declared size")
    yield suffix


def _rest_response(response: httpx.Response) -> DiscordRESTResponse:
    message_id: str | None = None
    if 200 <= response.status_code < 300:
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            message_id = _optional_id(body.get("id"))
    return DiscordRESTResponse(
        status_code=response.status_code,
        message_id=message_id,
    )


def _discord_attachment_from_message(
    body: object,
    *,
    attachment_id: str,
) -> tuple[str, int, str]:
    if not isinstance(body, Mapping):
        raise ValueError("Discord message response is invalid")
    attachments = body.get("attachments")
    if not isinstance(attachments, list):
        raise ValueError("Discord message attachments are invalid")
    for item in attachments:
        if not isinstance(item, Mapping) or _optional_id(item.get("id")) != attachment_id:
            continue
        filename = item.get("filename")
        size = item.get("size")
        url = item.get("url")
        if (
            not isinstance(filename, str)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or not isinstance(url, str)
        ):
            break
        return filename, size, url
    raise ValueError("Discord attachment is unavailable")


def _valid_discord_attachment_url(
    value: str,
    *,
    channel_id: str,
    attachment_id: str,
) -> bool:
    try:
        url = httpx.URL(value)
    except ValueError:
        return False
    return (
        url.scheme == "https"
        and url.host in {"cdn.discordapp.com", "media.discordapp.net"}
        and url.port in {None, 443}
        and url.userinfo == b""
        and url.path.startswith(f"/attachments/{channel_id}/{attachment_id}/")
    )


def _build_discord_gateway(
    message_callback: Callable[[object], Awaitable[None]],
) -> DiscordGatewayClient:
    discord: Any = importlib.import_module("discord")
    intents = discord.Intents.none()
    intents.guilds = True
    intents.guild_messages = True
    intents.dm_messages = True
    intents.message_content = True
    client = discord.Client(
        intents=intents,
        member_cache_flags=discord.MemberCacheFlags.none(),
        max_messages=None,
        chunk_guilds_at_startup=False,
        allowed_mentions=discord.AllowedMentions.none(),
    )

    @client.event  # type: ignore[untyped-decorator]
    async def on_message(message: object) -> None:
        await message_callback(message)

    return cast(DiscordGatewayClient, _DiscordPyGateway(client))


def _raise_for_identity_status(status_code: int) -> None:
    if 200 <= status_code < 300:
        return
    if status_code in {401, 403}:
        raise ChannelCredentialsInvalidError("Discord Bot token is invalid")
    raise ChannelCredentialsUnverifiedError("Discord Bot identity could not be verified")


def _validated_identity(application: object, user: object) -> ValidatedBotIdentity:
    if not isinstance(application, dict) or not isinstance(user, dict):
        raise ChannelCredentialsUnverifiedError("Discord Bot identity response is invalid")
    application_id = application.get("id")
    bot_user_id = user.get("id")
    username = user.get("username")
    global_name = user.get("global_name")
    if (
        not isinstance(application_id, str)
        or not isinstance(bot_user_id, str)
        or not isinstance(username, str)
        or user.get("bot") is not True
    ):
        raise ChannelCredentialsUnverifiedError("Discord Bot identity response is invalid")
    try:
        _discord_id(application_id)
        _discord_id(bot_user_id)
    except ValueError:
        raise ChannelCredentialsUnverifiedError(
            "Discord Bot identity response is invalid"
        ) from None
    display_name = global_name if isinstance(global_name, str) and global_name else username
    avatar_hash = user.get("avatar")
    avatar_url = (
        f"https://cdn.discordapp.com/avatars/{bot_user_id}/{avatar_hash}.png"
        if isinstance(avatar_hash, str) and avatar_hash
        else None
    )
    return ValidatedBotIdentity(
        identity_id=application_id,
        bot_user_id=bot_user_id,
        display_name=display_name,
        avatar_url=avatar_url,
    )


async def _wait_through_cancellation[T](task: asyncio.Task[T]) -> T:
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return await task
