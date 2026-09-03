from __future__ import annotations

import asyncio
import importlib
import json
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol, cast
from urllib.parse import quote
from uuid import UUID, uuid5

import httpx

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.channels.attachments import AuthenticatedAttachmentStream
from openctopus_server.services.channels import (
    ChannelCredentialsInvalidError,
    ChannelCredentialsUnverifiedError,
    ValidatedBotIdentity,
)

from ..delivery import MAX_DELIVERY_ACTIONS, ActionResult, DeliveryPlanTooLargeError
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

DINGTALK_CHATBOT_TOPIC = "/v1.0/im/bot/messages/get"
DINGTALK_MARKDOWN_LIMIT_BYTES = 20_000
DINGTALK_MARKDOWN_TITLE_LIMIT = 20
DINGTALK_FILE_LIMIT_BYTES = 20 * 1024 * 1024
DINGTALK_UPLOAD_CHUNK_BYTES = 64 * 1024
DINGTALK_START_TIMEOUT_SECONDS = 15.0
DINGTALK_MAX_MEDIA = 10
DINGTALK_CONVERSATION_LABEL_LIMIT = 120
DINGTALK_MARKDOWN_MESSAGE_KEY = "sampleMarkdown"
DINGTALK_FILE_MESSAGE_KEY = "sampleFile"

_DINGTALK_API_BASE = "https://api.dingtalk.com"
_DINGTALK_UPLOAD_URL = "https://oapi.dingtalk.com/media/upload"
_ACTION_NAMESPACE = UUID("373d5167-b14d-47c5-8ab3-f74d77c8df45")

type DingTalkDestinationKind = Literal["dm", "group"]


@dataclass(frozen=True, slots=True)
class DingTalkApiResponse:
    status_code: int
    message_id: str | None = None
    artifact_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class DingTalkSendRequest:
    destination_kind: DingTalkDestinationKind
    destination_id: str
    msg_key: str
    msg_param: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class DingTalkUploadRequest:
    filename: str
    mime: str
    size: int
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class DingTalkCallbackDecision:
    acknowledge: bool
    event: ChannelEvent | None = None


class DingTalkOutcomeUnknownError(Exception):
    """The platform request was issued but its outcome is not knowable."""


class _DingTalkCredentialsRejectedError(Exception):
    pass


class _DingTalkPreIssueError(Exception):
    pass


class DingTalkApi(Protocol):
    async def get_access_token(self) -> str: ...

    async def send_message(
        self,
        access_token: str,
        request: DingTalkSendRequest,
    ) -> DingTalkApiResponse: ...

    async def upload_file(
        self,
        access_token: str,
        request: DingTalkUploadRequest,
        chunks: AsyncIterator[bytes],
    ) -> DingTalkApiResponse: ...

    async def open_authenticated_attachment(
        self,
        access_token: str,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream: ...

    async def close(self) -> None: ...


class DingTalkMediaSource(Protocol):
    """Opens a bounded stream; implementations must yield chunks no larger than 64 KiB."""

    def open(
        self,
        media: ResolvedDeliveryFile,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]: ...


class DingTalkStreamClient(Protocol):
    def register_callback_handler(self, topic: str, handler: object) -> None: ...

    async def start(self) -> None: ...

    def is_online(self) -> bool: ...

    async def stop(self) -> None: ...


class DingTalkStreamFactory(Protocol):
    def __call__(self, client_id: str, client_secret: str) -> DingTalkStreamClient: ...


class DingTalkCredentialValidator:
    """Verify credentials without pretending DingTalk exposes bot profile metadata."""

    def __init__(self, *, http_client: httpx.AsyncClient | None = None) -> None:
        self._http_client = http_client

    async def validate_discord(self, bot_token: str) -> ValidatedBotIdentity:
        del bot_token
        raise ChannelCredentialsUnverifiedError

    async def validate_dingtalk(
        self,
        client_id: str,
        client_secret: str,
    ) -> ValidatedBotIdentity:
        owns_client = self._http_client is None
        client = self._http_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=0),
            timeout=10.0,
            follow_redirects=False,
        )
        try:
            await _request_access_token(client, client_id, client_secret)
        except _DingTalkCredentialsRejectedError as exc:
            raise ChannelCredentialsInvalidError from exc
        except (httpx.HTTPError, _DingTalkPreIssueError) as exc:
            raise ChannelCredentialsUnverifiedError from exc
        finally:
            if owns_client:
                await client.aclose()

        # robotCode is the configured Client ID for active Bot API delivery. The
        # token and Stream handshake do not expose a name, avatar, or chatbotUserId.
        return ValidatedBotIdentity(
            identity_id=client_id,
            bot_user_id=client_id,
            display_name=None,
            avatar_url=None,
        )


class DingTalkHttpApi:
    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._client = http_client or httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=0),
            timeout=10.0,
            follow_redirects=False,
        )
        self._owns_client = http_client is None
        self._token_lock = asyncio.Lock()
        self._access_token: str | None = None
        self._token_expires_at = 0.0

    async def get_access_token(self) -> str:
        async with self._token_lock:
            if (
                self._access_token is not None
                and time.monotonic() < self._token_expires_at
            ):
                return self._access_token
            token, expires_in = await _request_access_token(
                self._client,
                self._client_id,
                self._client_secret,
            )
            self._access_token = token
            self._token_expires_at = time.monotonic() + max(1, expires_in - 300)
            return token

    async def send_message(
        self,
        access_token: str,
        request: DingTalkSendRequest,
    ) -> DingTalkApiResponse:
        if request.destination_kind == "group":
            path = "/v1.0/robot/groupMessages/send"
            destination: dict[str, object] = {
                "openConversationId": request.destination_id
            }
        else:
            path = "/v1.0/robot/oToMessages/batchSend"
            destination = {"userIds": [request.destination_id]}
        body = {
            "robotCode": self._client_id,
            **destination,
            "msgKey": request.msg_key,
            "msgParam": request.msg_param,
        }
        try:
            response = await self._client.post(
                f"{_DINGTALK_API_BASE}{path}",
                headers=_request_headers(access_token, request.idempotency_key),
                json=body,
            )
        except httpx.HTTPError as exc:
            raise DingTalkOutcomeUnknownError from exc
        return _decode_api_response(response)

    async def upload_file(
        self,
        access_token: str,
        request: DingTalkUploadRequest,
        chunks: AsyncIterator[bytes],
    ) -> DingTalkApiResponse:
        boundary = f"openoctopus-{request.idempotency_key}"
        filename = _multipart_filename(request.filename)
        prefix = (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="type"\r\n\r\n'
            "file\r\n"
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="media"; filename="{filename}"\r\n'
            f"Content-Type: {_safe_mime(request.mime)}\r\n\r\n"
        ).encode()
        suffix = f"\r\n--{boundary}--\r\n".encode()
        stream = _MultipartStream(
            prefix=prefix,
            chunks=chunks,
            suffix=suffix,
            expected_file_size=request.size,
        )
        headers = {
            **_request_headers(access_token, request.idempotency_key),
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(prefix) + request.size + len(suffix)),
        }
        try:
            response = await self._client.post(
                f"{_DINGTALK_UPLOAD_URL}?access_token={quote(access_token, safe='')}",
                headers=headers,
                content=stream,
            )
        except (httpx.HTTPError, OSError, ValueError) as exc:
            raise DingTalkOutcomeUnknownError from exc
        return _decode_api_response(response, upload=True)

    async def open_authenticated_attachment(
        self,
        access_token: str,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream:
        if attachment.size is None:
            raise ValueError("DingTalk attachment size is unavailable")
        try:
            response = await self._client.post(
                f"{_DINGTALK_API_BASE}/v1.0/robot/messageFiles/download",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "x-acs-dingtalk-access-token": access_token,
                },
                json={
                    "robotCode": self._client_id,
                    "downloadCode": attachment.source_id,
                },
            )
            if not 200 <= response.status_code < 300:
                raise ValueError
            body = response.json()
            download_url = (
                body.get("downloadUrl") if isinstance(body, Mapping) else None
            )
            if not isinstance(download_url, str) or not _valid_dingtalk_download_url(
                download_url
            ):
                raise ValueError
            return await open_http_attachment(
                self._client,
                download_url,
                size=attachment.size,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ValueError("DingTalk attachment is unavailable") from None

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


class _MultipartStream(httpx.AsyncByteStream):
    def __init__(
        self,
        *,
        prefix: bytes,
        chunks: AsyncIterator[bytes],
        suffix: bytes,
        expected_file_size: int,
    ) -> None:
        self._prefix = prefix
        self._chunks = chunks
        self._suffix = suffix
        self._expected_file_size = expected_file_size

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._prefix
        observed = 0
        async for chunk in self._chunks:
            if not isinstance(chunk, bytes) or len(chunk) > DINGTALK_UPLOAD_CHUNK_BYTES:
                raise ValueError("Media source yielded an invalid upload chunk")
            observed += len(chunk)
            if observed > self._expected_file_size:
                raise ValueError("Media source exceeded its declared size")
            yield chunk
        if observed != self._expected_file_size:
            raise ValueError("Media source did not match its declared size")
        yield self._suffix


class _SdkStreamClient:
    def __init__(self, client_id: str, client_secret: str) -> None:
        sdk = importlib.import_module("dingtalk_stream")
        credential = sdk.Credential(client_id, client_secret)
        self._client: Any = sdk.DingTalkStreamClient(credential)

    def register_callback_handler(self, topic: str, handler: object) -> None:
        self._client.register_callback_handler(topic, handler)

    async def start(self) -> None:
        await self._client.start()

    def is_online(self) -> bool:
        return self._client.websocket is not None

    async def stop(self) -> None:
        await self._client.stop()


def _default_stream_factory(client_id: str, client_secret: str) -> DingTalkStreamClient:
    return _SdkStreamClient(client_id, client_secret)


class DingTalkCallbackHandler:
    def __init__(
        self,
        *,
        client_id: str,
        binding_generation: UUID,
        runtime_generation: UUID,
        sink: ChannelEventSink,
        ack_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._client_id = client_id
        self._binding_generation = binding_generation
        self._runtime_generation = runtime_generation
        self._sink = sink
        self._ack_factory = ack_factory or _sdk_ack
        self.dingtalk_client: object | None = None

    def pre_start(self) -> None:
        return None

    async def raw_process(self, callback: object) -> object | None:
        data = getattr(callback, "data", None)
        decision = normalize_dingtalk_callback(
            data if isinstance(data, Mapping) else {},
            client_id=self._client_id,
            binding_generation=self._binding_generation,
            runtime_generation=self._runtime_generation,
        )
        if decision.event is None:
            return self._ack(callback) if decision.acknowledge else None

        result = await self._sink(decision.event)
        disposition = getattr(result, "disposition", None)
        if (
            result is None
            or disposition in {"owner_attachment_unsupported", "shutting_down"}
            or getattr(result, "reason", None) == "shutting_down"
        ):
            return None
        return self._ack(callback)

    def _ack(self, callback: object) -> object:
        ack = self._ack_factory()
        headers = getattr(callback, "headers", None)
        ack.code = 200
        ack.headers.message_id = getattr(headers, "message_id", "")
        ack.headers.content_type = "application/json"
        ack.message = "OK"
        ack.data = {"response": None}
        return cast(object, ack)


class DingTalkAdapter:
    platform: Literal["dingtalk"] = "dingtalk"
    capabilities = ChannelCapabilities(history_backfill=False, file_delivery=True)

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        binding_generation: UUID,
        runtime_generation: UUID,
        stream_factory: DingTalkStreamFactory | None = None,
        api: DingTalkApi | None = None,
        media_source: DingTalkMediaSource | None = None,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._binding_generation = binding_generation
        self._runtime_generation = runtime_generation
        self._stream_factory = stream_factory or _default_stream_factory
        self._api = api or DingTalkHttpApi(
            client_id=client_id,
            client_secret=client_secret,
        )
        self._media_source = media_source
        self._stream: DingTalkStreamClient | None = None
        self._stream_task: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Task[None] | None = None

    async def start(self, sink: ChannelEventSink) -> None:
        if self._stop_task is not None:
            raise RuntimeError("DingTalk adapter is stopping")
        if self._stream_task is not None and not self._stream_task.done():
            raise RuntimeError("DingTalk adapter is already running")
        stream = self._stream_factory(self._client_id, self._client_secret)
        stream.register_callback_handler(
            DINGTALK_CHATBOT_TOPIC,
            DingTalkCallbackHandler(
                client_id=self._client_id,
                binding_generation=self._binding_generation,
                runtime_generation=self._runtime_generation,
                sink=sink,
            ),
        )
        self._stream = stream
        task = asyncio.create_task(stream.start(), name="dingtalk-stream-client")
        self._stream_task = task
        try:
            async with asyncio.timeout(DINGTALK_START_TIMEOUT_SECONDS):
                while not stream.is_online():
                    if task.done():
                        await task
                        raise RuntimeError("DingTalk Stream closed before becoming online")
                    await asyncio.sleep(0.01)
        except BaseException:
            cleanup = self._begin_stop()
            try:
                await await_future_cancellation_safe(cleanup)
            except asyncio.CancelledError:
                pass
            raise

    async def wait_closed(self) -> None:
        task = self._stream_task
        if task is not None:
            await task

    async def open_authenticated_attachment(
        self,
        event: ChannelEvent,
        attachment: ExternalAttachmentDescriptor,
    ) -> AuthenticatedAttachmentStream:
        if (
            event.platform != self.platform
            or event.binding_generation != self._binding_generation
            or event.runtime_generation != self._runtime_generation
            or attachment not in event.attachments
        ):
            raise ValueError("DingTalk attachment event is not current")
        access_token = await self._api.get_access_token()
        return await self._api.open_authenticated_attachment(
            access_token,
            attachment,
        )

    async def stop(self) -> None:
        await await_future_cancellation_safe(self._begin_stop())

    def _begin_stop(self) -> asyncio.Task[None]:
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(
                self._stop_once(), name="dingtalk-adapter-stop"
            )
        return self._stop_task

    async def _stop_once(self) -> None:
        try:
            if self._stream is not None:
                await self._stream.stop()
            task = self._stream_task
            if task is not None and task is not asyncio.current_task():
                await asyncio.gather(task, return_exceptions=True)
        finally:
            await self._api.close()

    async def fetch_recent_context(
        self,
        *,
        chat_id: str,
        before_message_id: str,
        limit: Literal[100],
    ) -> ContextFetchResult:
        del chat_id, before_message_id
        if limit != 100:
            raise ValueError("DingTalk context fetch limit must be exactly 100")
        return ContextFetchResult(status="unsupported")

    def plan_delivery(self, message: OutboundMessage) -> DeliveryPlan:
        if message.channel != "dingtalk":
            raise ValueError("DingTalk planning requires a DingTalk outbound message")
        _parse_chat_id(message.chat_id)
        if len(message.media) > DINGTALK_MAX_MEDIA:
            raise ValueError("DingTalk delivery accepts at most 10 media files")
        chunks = _split_utf8_text(
            message.content,
            max_bytes=DINGTALK_MARKDOWN_LIMIT_BYTES,
        )
        actions: list[DeliveryAction] = []
        for chunk in chunks:
            index = len(actions)
            actions.append(
                DeliveryAction(
                    kind="text_message",
                    visible=True,
                    content=chunk,
                    chat_id=message.chat_id,
                    idempotency_key=dingtalk_action_idempotency_key(
                        message.delivery_key, index
                    ),
                )
            )

        if message.media and self._media_source is None:
            raise ValueError("DingTalk media source is unavailable")
        for media_index, media in enumerate(message.media):
            if media.size is None:
                raise ValueError("DingTalk file delivery requires a known media size")
            if media.size < 0 or media.size > DINGTALK_FILE_LIMIT_BYTES:
                raise ValueError("DingTalk file exceeds the upload size limit")
            upload_index = len(actions)
            actions.append(
                DeliveryAction(
                    kind="file_upload",
                    visible=False,
                    media_index=media_index,
                    chat_id=message.chat_id,
                    idempotency_key=dingtalk_action_idempotency_key(
                        message.delivery_key, upload_index
                    ),
                    media=media,
                )
            )
            file_message_index = len(actions)
            actions.append(
                DeliveryAction(
                    kind="file_message",
                    visible=True,
                    media_index=media_index,
                    chat_id=message.chat_id,
                    idempotency_key=dingtalk_action_idempotency_key(
                        message.delivery_key, file_message_index
                    ),
                    media=media,
                    dependency_action_index=upload_index,
                )
            )

        if not actions:
            raise ValueError("DingTalk delivery must contain content or media")
        if len(actions) > MAX_DELIVERY_ACTIONS:
            raise DeliveryPlanTooLargeError(
                f"Delivery plan exceeds the {MAX_DELIVERY_ACTIONS}-action limit"
            )
        return DeliveryPlan(actions=tuple(actions))

    async def execute_action(
        self,
        action: DeliveryAction,
        *,
        on_issued: ActionIssueHook,
    ) -> ActionResult:
        if action.kind == "text_message":
            return await self._execute_text(action, on_issued=on_issued)
        if action.kind == "file_upload":
            return await self._execute_upload(action, on_issued=on_issued)
        if action.kind == "file_message":
            return await self._execute_file_message(action, on_issued=on_issued)
        return _failed("dingtalk_action_invalid", "DingTalk delivery action is invalid.")

    async def _execute_text(
        self,
        action: DeliveryAction,
        *,
        on_issued: ActionIssueHook,
    ) -> ActionResult:
        if (
            action.content is None
            or not action.content
            or len(action.content.encode()) > DINGTALK_MARKDOWN_LIMIT_BYTES
            or action.media is not None
        ):
            return _failed(
                "dingtalk_action_invalid", "DingTalk text action is missing content."
            )
        target = _action_target(action)
        if isinstance(target, ActionResult):
            return target
        request = DingTalkSendRequest(
            destination_kind=target[0],
            destination_id=target[1],
            msg_key=DINGTALK_MARKDOWN_MESSAGE_KEY,
            msg_param=json.dumps(
                {"title": _markdown_title(action.content), "text": action.content},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            idempotency_key=cast(str, action.idempotency_key),
        )
        return await self._send(request, on_issued=on_issued)

    async def _execute_file_message(
        self,
        action: DeliveryAction,
        *,
        on_issued: ActionIssueHook,
    ) -> ActionResult:
        media = action.media
        artifact_id = action.dependency_artifact_id
        if media is None or not artifact_id:
            return _failed(
                "dingtalk_upload_artifact_missing",
                "DingTalk file upload did not provide a media artifact.",
            )
        if media.size is None:
            return _failed(
                "dingtalk_media_size_unknown",
                "DingTalk file size is required before delivery.",
            )
        if media.size < 0 or media.size > DINGTALK_FILE_LIMIT_BYTES:
            return _failed(
                "dingtalk_media_size_invalid",
                "DingTalk file size is outside the upload limit.",
            )
        target = _action_target(action)
        if isinstance(target, ActionResult):
            return target
        request = DingTalkSendRequest(
            destination_kind=target[0],
            destination_id=target[1],
            msg_key=DINGTALK_FILE_MESSAGE_KEY,
            msg_param=json.dumps(
                {
                    "mediaId": artifact_id,
                    "fileName": media.filename,
                    "fileType": _file_type(media.filename),
                    "fileSize": media.size,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            idempotency_key=cast(str, action.idempotency_key),
        )
        return await self._send(request, on_issued=on_issued)

    async def _send(
        self,
        request: DingTalkSendRequest,
        *,
        on_issued: ActionIssueHook,
    ) -> ActionResult:
        token = await self._token()
        if isinstance(token, ActionResult):
            return token
        await on_issued()
        try:
            response = await self._api.send_message(token, request)
        except (DingTalkOutcomeUnknownError, TimeoutError, OSError):
            return _unknown(
                "dingtalk_send_outcome_unknown",
                "DingTalk message outcome is unknown.",
            )
        except Exception:
            return _unknown(
                "dingtalk_send_outcome_unknown",
                "DingTalk message outcome is unknown.",
            )
        if 200 <= response.status_code < 300:
            return ActionResult(
                status="sent", platform_message_id=response.message_id
            )
        if 400 <= response.status_code < 500:
            return _failed(
                "dingtalk_send_rejected", "DingTalk rejected the message."
            )
        return _unknown(
            "dingtalk_send_outcome_unknown",
            "DingTalk message outcome is unknown.",
        )

    async def _execute_upload(
        self,
        action: DeliveryAction,
        *,
        on_issued: ActionIssueHook,
    ) -> ActionResult:
        media = action.media
        source = self._media_source
        if media is None or media.size is None or source is None:
            return _failed(
                "dingtalk_media_read_failed",
                "DingTalk media could not be opened before upload.",
            )
        if media.size < 0 or media.size > DINGTALK_FILE_LIMIT_BYTES:
            return _failed(
                "dingtalk_media_size_invalid",
                "DingTalk file size is outside the upload limit.",
            )
        if action.idempotency_key is None:
            return _failed(
                "dingtalk_action_invalid", "DingTalk upload action is invalid."
            )
        issued = False
        issue_error: Exception | None = None
        try:
            async with source.open(media) as chunks:
                first, has_first = await _first_chunk(chunks)
                if not has_first and media.size != 0:
                    return _failed(
                        "dingtalk_media_read_failed",
                        "DingTalk media ended before upload.",
                    )
                if has_first and len(first) > media.size:
                    return _failed(
                        "dingtalk_media_read_failed",
                        "DingTalk media exceeded its declared size before upload.",
                    )
                token = await self._token()
                if isinstance(token, ActionResult):
                    return token
                request = DingTalkUploadRequest(
                    filename=media.filename,
                    mime=media.mime,
                    size=media.size,
                    idempotency_key=action.idempotency_key,
                )
                try:
                    await on_issued()
                except Exception as exc:
                    issue_error = exc
                    raise
                issued = True
                response = await self._api.upload_file(
                    token,
                    request,
                    _prepend_chunk(first, has_first, chunks),
                )
        except (DingTalkOutcomeUnknownError, TimeoutError, OSError, ValueError):
            if issue_error is not None:
                raise issue_error
            if issued:
                return _unknown(
                    "dingtalk_upload_outcome_unknown",
                    "DingTalk file upload outcome is unknown.",
                )
            return _failed(
                "dingtalk_media_read_failed",
                "DingTalk media could not be read before upload.",
            )
        except Exception:
            if issue_error is not None:
                raise issue_error
            if issued:
                return _unknown(
                    "dingtalk_upload_outcome_unknown",
                    "DingTalk file upload outcome is unknown.",
                )
            return _failed(
                "dingtalk_media_read_failed",
                "DingTalk media could not be read before upload.",
            )

        if 200 <= response.status_code < 300:
            if not response.artifact_id:
                return _unknown(
                    "dingtalk_upload_outcome_unknown",
                    "DingTalk file upload outcome is unknown.",
                )
            return ActionResult(
                status="sent",
                platform_message_id=response.artifact_id,
                artifact_id=response.artifact_id,
            )
        if 400 <= response.status_code < 500:
            return _failed(
                "dingtalk_upload_rejected", "DingTalk rejected the file upload."
            )
        return _unknown(
            "dingtalk_upload_outcome_unknown",
            "DingTalk file upload outcome is unknown.",
        )

    async def _token(self) -> str | ActionResult:
        try:
            return await self._api.get_access_token()
        except _DingTalkCredentialsRejectedError:
            return _failed(
                "dingtalk_credentials_invalid",
                "DingTalk credentials were rejected before message issue.",
            )
        except Exception:
            return _failed(
                "dingtalk_token_unavailable",
                "DingTalk access token was unavailable before message issue.",
            )


def normalize_dingtalk_callback(
    data: Mapping[str, object],
    *,
    client_id: str,
    binding_generation: UUID,
    runtime_generation: UUID,
) -> DingTalkCallbackDecision:
    robot_code = _string(data.get("robotCode"))
    if robot_code is not None and robot_code != client_id:
        return DingTalkCallbackDecision(acknowledge=True)

    conversation_type = _string(data.get("conversationType"))
    if conversation_type not in {"1", "2"}:
        return DingTalkCallbackDecision(acknowledge=True)
    source_message_id = _string(data.get("msgId"))
    conversation_id = _string(data.get("conversationId"))
    sender_id = _stable_sender_id(data)
    if source_message_id is None or conversation_id is None or sender_id is None:
        return DingTalkCallbackDecision(acknowledge=True)

    raw_sender_id = _string(data.get("senderId"))
    chatbot_user_id = _string(data.get("chatbotUserId"))
    sender_type = (_string(data.get("senderType")) or "").casefold()
    if (
        (raw_sender_id is not None and raw_sender_id == chatbot_user_id)
        or sender_type in {"bot", "robot", "system", "webhook"}
        or data.get("isRobot") is True
    ):
        return DingTalkCallbackDecision(acknowledge=True)

    explicitly_mentions_bot = data.get("isInAtList") is True
    if conversation_type == "2" and not explicitly_mentions_bot:
        return DingTalkCallbackDecision(acknowledge=True)

    message_type = (_string(data.get("msgtype")) or "").casefold()
    text = _current_message_text(data, message_type).strip()
    attachments = _attachment_descriptors(data, message_type)
    reply_context = _reply_context(data, chatbot_user_id=chatbot_user_id)
    chat_id = canonical_dingtalk_chat_id(
        conversation_type,
        sender_id,
        conversation_id,
    )
    kind: Literal["dm", "group"] = "dm" if conversation_type == "1" else "group"
    return DingTalkCallbackDecision(
        acknowledge=False,
        event=ChannelEvent(
            platform="dingtalk",
            binding_generation=binding_generation,
            runtime_generation=runtime_generation,
            source_message_id=source_message_id,
            chat_id=chat_id,
            conversation_kind=kind,
            sender_id=sender_id,
            sender_display_name=_safe_display_name(data.get("senderNick")),
            sender_kind="human",
            explicitly_mentions_bot=explicitly_mentions_bot,
            text=text,
            attachments=attachments,
            reply_context=reply_context,
            conversation_label=_dingtalk_conversation_label(data),
        ),
    )


def canonical_dingtalk_chat_id(
    conversation_type: str,
    sender_id: str,
    conversation_id: str,
) -> str:
    if conversation_type == "1":
        return f"dm:{sender_id}"
    if conversation_type == "2":
        return f"group:{conversation_id}"
    raise ValueError("Unsupported DingTalk conversation type")


def _dingtalk_conversation_label(data: Mapping[str, object]) -> str | None:
    for key in ("conversationTitle", "conversationName"):
        value = data.get(key)
        if not isinstance(value, str):
            continue
        printable = "".join(
            character if character.isprintable() else " " for character in value
        )
        collapsed = " ".join(printable.split())
        if collapsed:
            return collapsed[:DINGTALK_CONVERSATION_LABEL_LIMIT]
    return None


def dingtalk_action_idempotency_key(delivery_key: str, action_index: int) -> str:
    if action_index < 0:
        raise ValueError("Action index must not be negative")
    return str(uuid5(_ACTION_NAMESPACE, f"{delivery_key}:{action_index}"))


async def _request_access_token(
    client: httpx.AsyncClient,
    client_id: str,
    client_secret: str,
) -> tuple[str, int]:
    try:
        response = await client.post(
            f"{_DINGTALK_API_BASE}/v1.0/oauth2/accessToken",
            json={"appKey": client_id, "appSecret": client_secret},
        )
    except httpx.HTTPError:
        raise
    if response.status_code in {400, 401, 403}:
        raise _DingTalkCredentialsRejectedError
    if not 200 <= response.status_code < 300:
        raise _DingTalkPreIssueError
    try:
        body = response.json()
    except ValueError as exc:
        raise _DingTalkPreIssueError from exc
    token = _string(body.get("accessToken")) if isinstance(body, Mapping) else None
    expires = _positive_int(body.get("expireIn")) if isinstance(body, Mapping) else None
    if token is None or expires is None:
        raise _DingTalkPreIssueError
    return token, expires


def _decode_api_response(
    response: httpx.Response,
    *,
    upload: bool = False,
) -> DingTalkApiResponse:
    try:
        body = response.json()
    except ValueError:
        body = {}
    mapping = body if isinstance(body, Mapping) else {}
    errcode = mapping.get("errcode")
    status_code = response.status_code
    if status_code < 300 and isinstance(errcode, int) and errcode != 0:
        status_code = 400
    message_id = _first_string(
        mapping,
        ("processQueryKey", "messageId", "outTrackId"),
    )
    artifact_id = _first_string(mapping, ("media_id", "mediaId")) if upload else None
    return DingTalkApiResponse(
        status_code=status_code,
        message_id=message_id,
        artifact_id=artifact_id,
        error_code=_first_string(mapping, ("code", "errcode")),
        error_message=None,
    )


def _request_headers(access_token: str, idempotency_key: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-acs-dingtalk-access-token": access_token,
        "x-acs-dingtalk-request-id": idempotency_key,
    }


def _valid_dingtalk_download_url(value: str) -> bool:
    try:
        url = httpx.URL(value)
    except ValueError:
        return False
    host = url.host
    return (
        url.scheme == "https"
        and url.port in {None, 443}
        and url.userinfo == b""
        and bool(url.path)
        and host is not None
        and any(
            host == suffix or host.endswith(f".{suffix}")
            for suffix in ("dingtalk.com", "alicdn.com", "aliyuncs.com")
        )
    )


def _sdk_ack() -> Any:
    sdk = importlib.import_module("dingtalk_stream")
    return sdk.AckMessage()


def _stable_sender_id(data: Mapping[str, object]) -> str | None:
    return _first_string(
        data,
        (
            "senderStaffId",
            "senderOpenDingTalkId",
            "openDingTalkId",
            "senderId",
        ),
    )


def _current_message_text(data: Mapping[str, object], message_type: str) -> str:
    text = _nested_text(data.get("text"))
    if text is not None:
        return text
    content = data.get("content")
    if message_type == "richtext" and isinstance(content, Mapping):
        rich_text = content.get("richText")
        if isinstance(rich_text, Sequence) and not isinstance(rich_text, (str, bytes)):
            parts = [
                value
                for item in rich_text
                if isinstance(item, Mapping)
                for value in [_string(item.get("text"))]
                if value is not None
            ]
            return "\n".join(parts)
    if message_type == "audio" and isinstance(content, Mapping):
        recognition = _string(content.get("recognition"))
        if recognition is not None:
            return recognition
    return _nested_text(content) or ""


def _nested_text(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if not isinstance(value, Mapping):
        return None
    for key in ("content", "text", "title"):
        candidate = value.get(key)
        if isinstance(candidate, str):
            return candidate
        nested = _nested_text(candidate)
        if nested is not None:
            return nested
    return None


def _attachment_descriptors(
    data: Mapping[str, object],
    message_type: str,
) -> tuple[ExternalAttachmentDescriptor, ...]:
    candidates: list[Mapping[str, object]] = []
    content = data.get("content")
    if isinstance(content, Mapping):
        candidates.append(content)
        rich_text = content.get("richText")
        if isinstance(rich_text, Sequence) and not isinstance(rich_text, (str, bytes)):
            candidates.extend(item for item in rich_text if isinstance(item, Mapping))
    attachments = data.get("attachments")
    if isinstance(attachments, Sequence) and not isinstance(attachments, (str, bytes)):
        candidates.extend(item for item in attachments if isinstance(item, Mapping))
    for key in ("forwardMessages", "forwardedMessages", "forward"):
        forwarded = data.get(key)
        forwarded_items: Sequence[object]
        if isinstance(forwarded, Mapping):
            forwarded_items = (forwarded,)
        elif isinstance(forwarded, Sequence) and not isinstance(
            forwarded, (str, bytes)
        ):
            forwarded_items = forwarded
        else:
            continue
        for item in forwarded_items:
            if not isinstance(item, Mapping):
                continue
            forwarded_content = item.get("content")
            if isinstance(forwarded_content, Mapping):
                candidates.append(forwarded_content)

    descriptors: list[ExternalAttachmentDescriptor] = []
    for index, candidate in enumerate(candidates):
        source_id = _first_string(
            candidate,
            ("downloadCode", "pictureDownloadCode", "fileId", "mediaId"),
        )
        if source_id is None:
            continue
        raw_filename = _first_string(candidate, ("fileName", "filename", "name"))
        filename = _safe_filename(raw_filename or f"attachment-{index + 1}.{message_type or 'bin'}")
        descriptors.append(
            ExternalAttachmentDescriptor(
                source_id=source_id,
                filename=filename,
                content_type=_first_string(
                    candidate,
                    ("contentType", "mimeType", "mime"),
                ),
                size=_nonnegative_int(
                    candidate.get("fileSize", candidate.get("size"))
                ),
            )
        )
    return tuple(descriptors)


def _reply_context(
    data: Mapping[str, object],
    *,
    chatbot_user_id: str | None,
) -> tuple[ChannelContextMessage, ...]:
    candidates: list[Mapping[str, object]] = []
    for key in ("repliedMsg", "replyMsg", "quotedMessage", "quote"):
        value = data.get(key)
        if isinstance(value, Mapping):
            candidates.append(value)
    for key in ("forwardMessages", "forwardedMessages", "forward"):
        value = data.get(key)
        if isinstance(value, Mapping):
            candidates.append(value)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            candidates.extend(item for item in value if isinstance(item, Mapping))

    result: list[ChannelContextMessage] = []
    for candidate in candidates:
        sender_id = _stable_sender_id(candidate)
        sender_type = (_string(candidate.get("senderType")) or "").casefold()
        if sender_type in {"bot", "robot", "system", "webhook"} or (
            sender_id is not None and sender_id == chatbot_user_id
        ):
            continue
        text = _current_message_text(
            candidate,
            (_first_string(candidate, ("msgType", "msgtype")) or "").casefold(),
        ).strip()
        message_type = _first_string(candidate, ("msgType", "msgtype"))
        summaries = _context_attachment_summaries(candidate, message_type)
        if not text and not summaries:
            continue
        sent_at_value = candidate.get("createdAt", candidate.get("createAt"))
        result.append(
            ChannelContextMessage(
                source_message_id=_first_string(
                    candidate,
                    ("msgId", "messageId", "originalMsgId"),
                ),
                sender_id=sender_id,
                sender_display_name=_safe_display_name(
                    _first_string(candidate, ("senderNick", "senderName", "nick"))
                ),
                sent_at=str(sent_at_value) if sent_at_value is not None else None,
                text=text,
                attachment_summaries=summaries,
            )
        )
    return tuple(result)


def _context_attachment_summaries(
    message: Mapping[str, object],
    message_type: str | None,
) -> tuple[str, ...]:
    candidates: list[Mapping[str, object]] = [message]
    content = message.get("content")
    if isinstance(content, Mapping):
        candidates.append(content)
        rich_text = content.get("richText")
        if isinstance(rich_text, Sequence) and not isinstance(rich_text, (str, bytes)):
            candidates.extend(item for item in rich_text if isinstance(item, Mapping))
    attachments = message.get("attachments")
    if isinstance(attachments, Sequence) and not isinstance(attachments, (str, bytes)):
        candidates.extend(item for item in attachments if isinstance(item, Mapping))

    summaries: list[str] = []
    for candidate in candidates:
        raw_filename = _first_string(candidate, ("fileName", "filename", "name"))
        content_type = _safe_display_name(
            _first_string(candidate, ("contentType", "mimeType", "mime"))
        )
        if raw_filename is not None:
            filename = _safe_filename(raw_filename)
            summary = f"{filename} ({content_type})" if content_type else filename
        elif content_type is not None:
            summary = f"attachment ({content_type})"
        else:
            continue
        if summary not in summaries:
            summaries.append(summary)

    if summaries:
        return tuple(summaries)
    normalized_type = _safe_display_name(message_type)
    if normalized_type is None or normalized_type.casefold() == "text":
        return ()
    return (f"{normalized_type} attachment",)


def _split_utf8_text(content: str, *, max_bytes: int) -> tuple[str, ...]:
    if not content:
        raise ValueError("Delivery text must not be empty")
    if max_bytes <= 0:
        raise ValueError("Text byte limit must be positive")

    chunks: list[str] = []
    remaining = content
    while remaining:
        byte_count = 0
        end = 0
        for character in remaining:
            encoded_size = len(character.encode("utf-8"))
            if byte_count + encoded_size > max_bytes:
                break
            byte_count += encoded_size
            end += 1
        if end == 0:
            raise ValueError("Text byte limit cannot fit a Unicode code point")
        if end == len(remaining):
            chunks.append(remaining)
            break

        candidate = remaining[:end]
        split_at = candidate.rfind("\n")
        if split_at < 0:
            split_at = _last_whitespace(candidate)
        boundary = split_at + 1 if split_at >= 0 else end
        chunks.append(remaining[:boundary])
        remaining = remaining[boundary:]
        if len(chunks) == MAX_DELIVERY_ACTIONS and remaining:
            raise DeliveryPlanTooLargeError(
                f"Delivery plan exceeds the {MAX_DELIVERY_ACTIONS}-action limit"
            )
    return tuple(chunks)


def _last_whitespace(value: str) -> int:
    for index in range(len(value) - 1, -1, -1):
        if value[index].isspace():
            return index
    return -1


def _parse_chat_id(chat_id: str) -> tuple[DingTalkDestinationKind, str]:
    if chat_id.startswith("dm:") and len(chat_id) > 3:
        return "dm", chat_id[3:]
    if chat_id.startswith("group:") and len(chat_id) > 6:
        return "group", chat_id[6:]
    raise ValueError("DingTalk chat target is not canonical")


def _action_target(
    action: DeliveryAction,
) -> tuple[DingTalkDestinationKind, str] | ActionResult:
    if (
        action.chat_id is None
        or action.idempotency_key is None
        or not _valid_idempotency_key(action.idempotency_key)
    ):
        return _failed(
            "dingtalk_action_invalid", "DingTalk delivery action is invalid."
        )
    try:
        return _parse_chat_id(action.chat_id)
    except ValueError:
        return _failed(
            "dingtalk_target_invalid", "DingTalk delivery target is invalid."
        )


def _markdown_title(content: str) -> str:
    for line in content.splitlines():
        candidate = line.strip().lstrip("#").strip()
        if candidate:
            return candidate[:DINGTALK_MARKDOWN_TITLE_LIMIT]
    return "OpenOctopus"


def _file_type(filename: str) -> str:
    suffix = PurePosixPath(filename).suffix.lstrip(".").casefold()
    return suffix or "file"


async def _first_chunk(chunks: AsyncIterator[bytes]) -> tuple[bytes, bool]:
    try:
        chunk = await anext(chunks)
    except StopAsyncIteration:
        return b"", False
    if not isinstance(chunk, bytes) or len(chunk) > DINGTALK_UPLOAD_CHUNK_BYTES:
        raise ValueError("Media source yielded an invalid upload chunk")
    return chunk, True


async def _prepend_chunk(
    first: bytes,
    has_first: bool,
    chunks: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    if has_first:
        yield first
    async for chunk in chunks:
        yield chunk


def _multipart_filename(filename: str) -> str:
    return filename.replace("\\", "_").replace('"', "_").replace("\r", "_").replace("\n", "_")


def _safe_mime(mime: str) -> str:
    if not mime or "\r" in mime or "\n" in mime:
        return "application/octet-stream"
    return mime


def _valid_idempotency_key(value: str) -> bool:
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def _safe_filename(filename: str) -> str:
    leaf = filename.replace("\\", "/").split("/")[-1].strip()
    leaf = "".join(character for character in leaf if character.isprintable())
    return (leaf or "attachment.bin")[:255]


def _safe_display_name(value: object) -> str | None:
    name = _string(value)
    if name is None:
        return None
    sanitized = "".join(character for character in name if ord(character) >= 32).strip()
    return sanitized[:120] or None


def _string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _first_string(
    mapping: Mapping[str, object],
    keys: Sequence[str],
) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, int):
            return str(value)
    return None


def _positive_int(value: object) -> int | None:
    parsed = _integer(value)
    return parsed if parsed is not None and parsed > 0 else None


def _nonnegative_int(value: object) -> int | None:
    parsed = _integer(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _failed(code: str, message: str) -> ActionResult:
    return ActionResult(status="failed", error_code=code, error_message=message)


def _unknown(code: str, message: str) -> ActionResult:
    return ActionResult(status="unknown", error_code=code, error_message=message)
