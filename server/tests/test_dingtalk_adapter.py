from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest

from openctopus_server.channels.adapters.dingtalk import (
    DINGTALK_CHATBOT_TOPIC,
    DINGTALK_FILE_LIMIT_BYTES,
    DINGTALK_MARKDOWN_LIMIT_BYTES,
    DingTalkAdapter,
    DingTalkApiResponse,
    DingTalkCallbackHandler,
    DingTalkCredentialValidator,
    DingTalkHttpApi,
    DingTalkOutcomeUnknownError,
    DingTalkSendRequest,
    DingTalkUploadRequest,
    canonical_dingtalk_chat_id,
    dingtalk_action_idempotency_key,
    normalize_dingtalk_callback,
)
from openctopus_server.channels.types import ExternalAttachmentDescriptor, OutboundMessage
from openctopus_server.services.channels import (
    ChannelCredentialsInvalidError,
    ChannelCredentialsUnverifiedError,
)

USER_ID = UUID("10000000-0000-4000-8000-000000000001")
BINDING_GENERATION = UUID("20000000-0000-4000-8000-000000000001")
RUNTIME_GENERATION = UUID("30000000-0000-4000-8000-000000000001")


async def _issued() -> None:
    return None


@dataclass(frozen=True)
class _Media:
    filename: str
    mime: str
    size: int | None


class _AttachmentStream:
    def __init__(self, data: bytes) -> None:
        self.size = len(data)
        self._data = data
        self._position = 0
        self.closed = False

    async def read(self, max_bytes: int) -> bytes:
        chunk = self._data[self._position : self._position + max_bytes]
        self._position += len(chunk)
        return chunk

    async def aclose(self) -> None:
        self.closed = True


class _Api:
    def __init__(self) -> None:
        self.token = "access-token"
        self.token_error: BaseException | None = None
        self.send_result = DingTalkApiResponse(status_code=200, message_id="sent-1")
        self.send_error: BaseException | None = None
        self.upload_result = DingTalkApiResponse(status_code=200, artifact_id="media-1")
        self.upload_error: BaseException | None = None
        self.token_calls = 0
        self.send_requests: list[DingTalkSendRequest] = []
        self.upload_requests: list[DingTalkUploadRequest] = []
        self.uploaded = bytearray()
        self.attachment_requests: list[ExternalAttachmentDescriptor] = []
        self.closed = False

    async def get_access_token(self) -> str:
        self.token_calls += 1
        if self.token_error is not None:
            raise self.token_error
        return self.token

    async def send_message(
        self,
        access_token: str,
        request: DingTalkSendRequest,
    ) -> DingTalkApiResponse:
        assert access_token == self.token
        self.send_requests.append(request)
        if self.send_error is not None:
            raise self.send_error
        return self.send_result

    async def upload_file(
        self,
        access_token: str,
        request: DingTalkUploadRequest,
        chunks: AsyncIterator[bytes],
    ) -> DingTalkApiResponse:
        assert access_token == self.token
        self.upload_requests.append(request)
        if self.upload_error is not None:
            raise self.upload_error
        async for chunk in chunks:
            self.uploaded.extend(chunk)
        return self.upload_result

    async def close(self) -> None:
        self.closed = True

    async def open_authenticated_attachment(
        self,
        access_token: str,
        attachment: ExternalAttachmentDescriptor,
    ) -> _AttachmentStream:
        assert access_token == self.token
        self.attachment_requests.append(attachment)
        return _AttachmentStream(b"abc")


class _MediaSource:
    def __init__(
        self,
        chunks: tuple[bytes, ...] = (b"hello",),
        *,
        open_error: BaseException | None = None,
        after_first_error: BaseException | None = None,
    ) -> None:
        self.chunks = chunks
        self.open_error = open_error
        self.after_first_error = after_first_error
        self.opened: list[object] = []

    @asynccontextmanager
    async def open(self, media: object) -> AsyncIterator[AsyncIterator[bytes]]:
        self.opened.append(media)
        if self.open_error is not None:
            raise self.open_error

        async def iterate() -> AsyncIterator[bytes]:
            for index, chunk in enumerate(self.chunks):
                yield chunk
                if index == 0 and self.after_first_error is not None:
                    raise self.after_first_error

        yield iterate()


class _Stream:
    def __init__(self) -> None:
        self.online = False
        self.started = asyncio.Event()
        self.closed = asyncio.Event()
        self.handlers: dict[str, object] = {}
        self.start_calls = 0
        self.stop_calls = 0

    def register_callback_handler(self, topic: str, handler: object) -> None:
        self.handlers[topic] = handler

    async def start(self) -> None:
        self.start_calls += 1
        self.online = True
        self.started.set()
        await self.closed.wait()
        self.online = False

    def is_online(self) -> bool:
        return self.online

    async def stop(self) -> None:
        self.stop_calls += 1
        self.closed.set()


class _StreamFactory:
    def __init__(self, stream: _Stream | None = None) -> None:
        self.stream = stream or _Stream()
        self.calls: list[tuple[str, str]] = []

    def __call__(self, client_id: str, client_secret: str) -> _Stream:
        self.calls.append((client_id, client_secret))
        return self.stream


class _BlockingStopStream(_Stream):
    def __init__(self) -> None:
        super().__init__()
        self.stop_started = asyncio.Event()
        self.release_stop = asyncio.Event()

    async def stop(self) -> None:
        self.stop_calls += 1
        self.stop_started.set()
        await self.release_stop.wait()
        self.closed.set()


def _callback_data(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "conversationId": "cid-group-1",
        "conversationType": "2",
        "msgId": "msg-1",
        "msgtype": "text",
        "senderStaffId": "staff-1",
        "senderId": "open-sender-1",
        "senderNick": "Alice",
        "chatbotUserId": "open-bot-1",
        "robotCode": "client-1",
        "isInAtList": True,
        "text": {"content": "  hello DingTalk  "},
        "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?secret=raw",
    }
    data.update(overrides)
    return data


def _outbound(
    content: str = "hello",
    *,
    chat_id: str = "group:cid-group-1",
    media: tuple[_Media, ...] = (),
) -> OutboundMessage:
    return OutboundMessage(
        delivery_key="message-tool:turn-1:tool-1",
        user_id=USER_ID,
        turn_id=None,
        origin="message_tool",
        channel="dingtalk",
        chat_id=chat_id,
        binding_generation=BINDING_GENERATION,
        content=content,
        media=media,
    )


def _adapter(
    *,
    api: _Api | None = None,
    media_source: _MediaSource | None = None,
    stream_factory: _StreamFactory | None = None,
) -> tuple[DingTalkAdapter, _Api, _StreamFactory]:
    api = api or _Api()
    stream_factory = stream_factory or _StreamFactory()
    return (
        DingTalkAdapter(
            client_id="client-1",
            client_secret="secret-1",
            binding_generation=BINDING_GENERATION,
            runtime_generation=RUNTIME_GENERATION,
            stream_factory=stream_factory,
            api=api,
            media_source=media_source,
        ),
        api,
        stream_factory,
    )


async def test_credential_validation_verifies_token_and_uses_real_robot_code_identity() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"accessToken": "temporary", "expireIn": 7200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        identity = await DingTalkCredentialValidator(
            http_client=client
        ).validate_dingtalk("client-1", "secret-1")

    assert identity.identity_id == "client-1"
    assert identity.bot_user_id == "client-1"
    assert identity.display_name is None
    assert identity.avatar_url is None
    assert len(requests) == 1
    assert requests[0].url.path == "/v1.0/oauth2/accessToken"
    assert json.loads(requests[0].content) == {
        "appKey": "client-1",
        "appSecret": "secret-1",
    }


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, ChannelCredentialsInvalidError),
        (403, ChannelCredentialsInvalidError),
        (429, ChannelCredentialsUnverifiedError),
        (503, ChannelCredentialsUnverifiedError),
    ],
)
async def test_credential_validation_classifies_failure(
    status: int,
    expected: type[Exception],
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(status))
    ) as client:
        with pytest.raises(expected):
            await DingTalkCredentialValidator(http_client=client).validate_dingtalk(
                "client-1", "secret-1"
            )


async def test_credential_validation_network_failure_is_unverified() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("secret raw transport detail")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ChannelCredentialsUnverifiedError) as raised:
            await DingTalkCredentialValidator(http_client=client).validate_dingtalk(
                "client-1", "secret-1"
            )
    assert "secret raw transport detail" not in str(raised.value)


def test_callback_normalization_uses_stable_sender_and_canonical_group_target() -> None:
    decision = normalize_dingtalk_callback(
        _callback_data(),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )

    assert decision.event is not None
    assert decision.event.platform == "dingtalk"
    assert decision.event.source_message_id == "msg-1"
    assert decision.event.chat_id == "group:cid-group-1"
    assert decision.event.conversation_kind == "group"
    assert decision.event.sender_id == "staff-1"
    assert decision.event.sender_display_name == "Alice"
    assert decision.event.sender_kind == "human"
    assert decision.event.explicitly_mentions_bot is True
    assert decision.event.text == "hello DingTalk"
    assert decision.event.attachments == ()
    assert "sessionWebhook" not in repr(decision.event)
    assert "sendBySession" not in repr(decision.event)


def test_callback_conversation_label_is_safe_bounded_and_optional() -> None:
    labeled = normalize_dingtalk_callback(
        _callback_data(conversationTitle="  Team\x00\n" + ("群" * 200)),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )
    unlabeled = normalize_dingtalk_callback(
        _callback_data(),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )

    assert labeled.event is not None
    assert labeled.event.conversation_label == "Team " + ("群" * 115)
    assert unlabeled.event is not None
    assert unlabeled.event.conversation_label is None


def test_dm_normalization_uses_active_oto_target_without_requiring_mention() -> None:
    decision = normalize_dingtalk_callback(
        _callback_data(
            conversationType="1",
            conversationId="temporary-dm-conversation",
            isInAtList=False,
        ),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )

    assert decision.event is not None
    assert decision.event.chat_id == "dm:staff-1"
    assert decision.event.conversation_kind == "dm"
    assert decision.event.explicitly_mentions_bot is False
    assert canonical_dingtalk_chat_id("1", "staff-1", "ignored") == "dm:staff-1"


@pytest.mark.parametrize(
    "overrides",
    [
        {"conversationType": "2", "isInAtList": False},
        {"senderId": "open-bot-1"},
        {"senderType": "BOT"},
        {"senderType": "WEBHOOK"},
        {"robotCode": "another-client"},
    ],
)
def test_structurally_ignored_callback_never_builds_an_event(
    overrides: dict[str, object],
) -> None:
    decision = normalize_dingtalk_callback(
        _callback_data(**overrides),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )
    assert decision.acknowledge is True
    assert decision.event is None


def test_callback_quote_and_forward_are_projected_without_history_or_webhook() -> None:
    decision = normalize_dingtalk_callback(
        _callback_data(
            repliedMsg={
                "msgId": "quoted-1",
                "senderStaffId": "staff-2",
                "senderNick": "Bob",
                "createdAt": 1_725_000_000_000,
                "msgType": "text",
                "content": {"text": "quoted text"},
                "sessionWebhook": "raw-secret",
            },
            forwardMessages=[
                {
                    "msgId": "forwarded-1",
                    "senderId": "staff-3",
                    "senderName": "Carol",
                    "content": {"text": "forwarded text"},
                }
            ],
        ),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )

    assert decision.event is not None
    assert [item.source_message_id for item in decision.event.reply_context] == [
        "quoted-1",
        "forwarded-1",
    ]
    assert [item.text for item in decision.event.reply_context] == [
        "quoted text",
        "forwarded text",
    ]
    assert "raw-secret" not in repr(decision.event.reply_context)


def test_attachment_only_quote_keeps_safe_filename_and_type_as_context() -> None:
    decision = normalize_dingtalk_callback(
        _callback_data(
            repliedMsg={
                "msgId": "quoted-file-1",
                "senderStaffId": "staff-2",
                "senderNick": "Bob",
                "msgType": "file",
                "content": {
                    "fileName": "folder/report.pdf",
                    "contentType": "application/pdf",
                    "downloadUrl": SimpleNamespace(
                        __str__=lambda _self: (_ for _ in ()).throw(
                            AssertionError("context normalization must not inspect URLs")
                        )
                    ),
                },
            },
        ),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )

    assert decision.event is not None
    assert len(decision.event.reply_context) == 1
    context = decision.event.reply_context[0]
    assert context.text == ""
    assert context.attachment_summaries == ("report.pdf (application/pdf)",)


def test_callback_attachment_normalization_keeps_descriptor_and_never_downloads() -> None:
    content = {
        "downloadCode": "download-code-1",
        "fileName": "report.pdf",
        "fileSize": "123",
        "contentType": "application/pdf",
        "downloadUrl": SimpleNamespace(
            __str__=lambda _self: (_ for _ in ()).throw(
                AssertionError("adapter must not inspect download URL")
            )
        ),
    }
    decision = normalize_dingtalk_callback(
        _callback_data(msgtype="file", text=None, content=content),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )

    assert decision.event is not None
    assert decision.event.text == ""
    assert len(decision.event.attachments) == 1
    assert decision.event.attachments[0].source_id == "download-code-1"
    assert decision.event.attachments[0].filename == "report.pdf"
    assert decision.event.attachments[0].size == 123


def test_forwarded_file_descriptor_is_still_subject_to_attachment_policy() -> None:
    decision = normalize_dingtalk_callback(
        _callback_data(
            forwardMessages=[
                {
                    "msgId": "forwarded-file-1",
                    "msgType": "file",
                    "content": {
                        "downloadCode": "forward-download-1",
                        "fileName": "forward.pdf",
                    },
                }
            ]
        ),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )

    assert decision.event is not None
    assert [item.source_id for item in decision.event.attachments] == [
        "forward-download-1"
    ]


async def test_callback_acks_only_after_sink_returns_a_durable_decision() -> None:
    calls: list[object] = []

    async def accepted(event: object) -> object:
        calls.append(event)
        return SimpleNamespace(disposition="accepted")

    handler = DingTalkCallbackHandler(
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        sink=accepted,
    )
    callback = SimpleNamespace(
        data=_callback_data(),
        headers=SimpleNamespace(message_id="stream-frame-1"),
    )

    ack = await handler.raw_process(callback)

    assert len(calls) == 1
    assert ack is not None
    assert ack.code == 200
    assert ack.headers.message_id == "stream-frame-1"


async def test_callback_does_not_ack_when_manager_fence_returns_none() -> None:
    async def fenced(_event: object) -> None:
        return None

    handler = DingTalkCallbackHandler(
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        sink=fenced,
    )
    callback = SimpleNamespace(
        data=_callback_data(),
        headers=SimpleNamespace(message_id="stream-frame-1"),
    )

    assert await handler.raw_process(callback) is None


async def test_callback_does_not_ack_owner_attachment_until_ingress_supports_it() -> None:
    async def unsupported(_event: object) -> object:
        return SimpleNamespace(disposition="owner_attachment_unsupported")

    handler = DingTalkCallbackHandler(
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        sink=unsupported,
    )
    callback = SimpleNamespace(
        data=_callback_data(msgtype="file", text=None, content={"downloadCode": "d1"}),
        headers=SimpleNamespace(message_id="stream-frame-1"),
    )

    assert await handler.raw_process(callback) is None


@pytest.mark.parametrize(
    "result",
    [
        SimpleNamespace(disposition="shutting_down"),
        SimpleNamespace(disposition="ignored", reason="shutting_down"),
    ],
)
async def test_callback_does_not_ack_during_shutdown(result: object) -> None:
    async def shutting_down(_event: object) -> object:
        return result

    handler = DingTalkCallbackHandler(
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        sink=shutting_down,
    )
    callback = SimpleNamespace(
        data=_callback_data(),
        headers=SimpleNamespace(message_id="stream-frame-1"),
    )

    assert await handler.raw_process(callback) is None


async def test_callback_commit_failure_propagates_without_ack() -> None:
    async def failed(_event: object) -> object:
        raise RuntimeError("commit failed")

    handler = DingTalkCallbackHandler(
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        sink=failed,
    )
    callback = SimpleNamespace(
        data=_callback_data(),
        headers=SimpleNamespace(message_id="stream-frame-1"),
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        await handler.raw_process(callback)


async def test_callback_cancellation_propagates_without_ack() -> None:
    entered = asyncio.Event()

    async def cancelled(_event: object) -> object:
        entered.set()
        await asyncio.Future()

    handler = DingTalkCallbackHandler(
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        sink=cancelled,
    )
    callback = SimpleNamespace(
        data=_callback_data(),
        headers=SimpleNamespace(message_id="stream-frame-1"),
    )
    task = asyncio.create_task(handler.raw_process(callback))
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_deterministically_ignored_callback_acks_without_calling_sink() -> None:
    async def forbidden(_event: object) -> object:
        raise AssertionError("ignored callback must not enter ingress")

    handler = DingTalkCallbackHandler(
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        sink=forbidden,
    )
    callback = SimpleNamespace(
        data=_callback_data(isInAtList=False),
        headers=SimpleNamespace(message_id="stream-frame-1"),
    )

    ack = await handler.raw_process(callback)
    assert ack is not None
    assert ack.code == 200


async def test_stream_lifecycle_reports_online_and_stops_sdk_task() -> None:
    adapter, api, factory = _adapter()

    async def sink(_event: object) -> object:
        return SimpleNamespace(disposition="accepted")

    await adapter.start(sink)
    assert factory.calls == [("client-1", "secret-1")]
    assert factory.stream.start_calls == 1
    assert DINGTALK_CHATBOT_TOPIC in factory.stream.handlers

    closed = asyncio.create_task(adapter.wait_closed())
    await adapter.stop()
    await closed

    assert factory.stream.stop_calls == 1
    assert api.closed is True


async def test_stream_stop_finishes_cleanup_before_propagating_cancellation() -> None:
    stream = _BlockingStopStream()
    factory = _StreamFactory(stream)
    adapter, api, _ = _adapter(stream_factory=factory)

    async def sink(_event: object) -> object:
        return SimpleNamespace(disposition="accepted")

    await adapter.start(sink)
    stop = asyncio.create_task(adapter.stop())
    await stream.stop_started.wait()
    stop.cancel()
    await asyncio.sleep(0)
    assert not stop.done()

    stream.release_stop.set()
    with pytest.raises(asyncio.CancelledError):
        await stop
    await adapter.wait_closed()

    assert stream.stop_calls == 1
    assert api.closed is True


async def test_fetch_recent_context_is_explicitly_unsupported() -> None:
    adapter, _, _ = _adapter()
    result = await adapter.fetch_recent_context(
        chat_id="group:cid-group-1",
        before_message_id="msg-1",
        limit=100,
    )
    assert result.status == "unsupported"
    assert result.messages == ()

    with pytest.raises(ValueError, match="exactly 100"):
        await adapter.fetch_recent_context(
            chat_id="group:cid-group-1",
            before_message_id="msg-1",
            limit=99,  # type: ignore[arg-type]
        )


def test_markdown_delivery_splits_by_actual_utf8_byte_limit_without_loss() -> None:
    adapter, _, _ = _adapter()
    content = "开头\n" + ("界" * (DINGTALK_MARKDOWN_LIMIT_BYTES // 3 + 5))

    plan = adapter.plan_delivery(_outbound(content))

    assert len(plan.actions) == 3
    assert "".join(action.content or "" for action in plan.actions) == content
    assert all(
        len((action.content or "").encode("utf-8")) <= DINGTALK_MARKDOWN_LIMIT_BYTES
        for action in plan.actions
    )
    assert all(action.kind == "text_message" for action in plan.actions)
    assert all(action.chat_id == "group:cid-group-1" for action in plan.actions)
    assert [action.idempotency_key for action in plan.actions] == [
        dingtalk_action_idempotency_key("message-tool:turn-1:tool-1", index)
        for index in range(3)
    ]


def test_file_plan_has_upload_then_visible_message_with_explicit_dependency() -> None:
    source = _MediaSource(chunks=(b"hello",))
    adapter, _, _ = _adapter(media_source=source)
    media = _Media("report.pdf", "application/pdf", 5)

    plan = adapter.plan_delivery(_outbound(media=(media,)))

    assert [action.kind for action in plan.actions] == [
        "text_message",
        "file_upload",
        "file_message",
    ]
    upload, file_message = plan.actions[1:]
    assert upload.visible is False
    assert upload.media is media
    assert file_message.visible is True
    assert file_message.media is media
    assert file_message.dependency_action_index == 1
    assert file_message.dependency_artifact_id is None


@pytest.mark.parametrize(
    "media",
    [
        _Media("unknown.bin", "application/octet-stream", None),
        _Media("large.bin", "application/octet-stream", DINGTALK_FILE_LIMIT_BYTES + 1),
    ],
)
def test_file_plan_rejects_unknown_or_oversized_media_before_issue(media: _Media) -> None:
    adapter, _, _ = _adapter(media_source=_MediaSource())
    with pytest.raises(ValueError):
        adapter.plan_delivery(_outbound(media=(media,)))


def test_file_plan_rejects_missing_media_source_before_issue() -> None:
    adapter, _, _ = _adapter(media_source=None)
    with pytest.raises(ValueError, match="media source"):
        adapter.plan_delivery(
            _outbound(media=(_Media("report.pdf", "application/pdf", 5),))
        )


async def test_text_action_uses_active_bot_api_and_stable_request_key() -> None:
    adapter, api, _ = _adapter()
    action = adapter.plan_delivery(_outbound("## Result\nDone")).actions[0]

    issue_observations: list[tuple[int, int]] = []

    async def on_issued() -> None:
        issue_observations.append((api.token_calls, len(api.send_requests)))

    result = await adapter.execute_action(action, on_issued=on_issued)

    assert result.status == "sent"
    assert result.platform_message_id == "sent-1"
    assert issue_observations == [(1, 0)]
    assert len(api.send_requests) == 1
    request = api.send_requests[0]
    assert request.destination_kind == "group"
    assert request.destination_id == "cid-group-1"
    assert request.msg_key == "sampleMarkdown"
    assert json.loads(request.msg_param) == {
        "title": "Result",
        "text": "## Result\nDone",
    }
    assert request.idempotency_key == action.idempotency_key


async def test_text_action_propagates_issue_hook_failure_without_platform_request() -> None:
    class IssueRejectedError(Exception):
        pass

    adapter, api, _ = _adapter()
    action = adapter.plan_delivery(_outbound()).actions[0]

    async def reject_issue() -> None:
        raise IssueRejectedError

    with pytest.raises(IssueRejectedError):
        await adapter.execute_action(action, on_issued=reject_issue)

    assert api.token_calls == 1
    assert api.send_requests == []


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429])
async def test_explicit_client_failure_is_definitive_and_never_retried(status: int) -> None:
    adapter, api, _ = _adapter()
    api.send_result = DingTalkApiResponse(
        status_code=status,
        error_code="raw-platform-code",
        error_message="raw platform response",
    )
    action = adapter.plan_delivery(_outbound()).actions[0]

    result = await adapter.execute_action(action, on_issued=_issued)

    assert result.status == "failed"
    assert result.error_code == "dingtalk_send_rejected"
    assert result.error_message == "DingTalk rejected the message."
    assert len(api.send_requests) == 1


@pytest.mark.parametrize("status", [500, 502, 503])
async def test_server_response_after_issue_is_unknown_and_never_retried(status: int) -> None:
    adapter, api, _ = _adapter()
    api.send_result = DingTalkApiResponse(status_code=status)
    action = adapter.plan_delivery(_outbound()).actions[0]

    result = await adapter.execute_action(action, on_issued=_issued)

    assert result.status == "unknown"
    assert result.error_code == "dingtalk_send_outcome_unknown"
    assert len(api.send_requests) == 1


async def test_transport_failure_after_visible_issue_is_unknown_and_never_retried() -> None:
    adapter, api, _ = _adapter()
    api.send_error = DingTalkOutcomeUnknownError()
    action = adapter.plan_delivery(_outbound()).actions[0]

    result = await adapter.execute_action(action, on_issued=_issued)

    assert result.status == "unknown"
    assert len(api.send_requests) == 1


async def test_file_upload_returns_artifact_then_file_message_consumes_it() -> None:
    source = _MediaSource(chunks=(b"he", b"llo"))
    adapter, api, _ = _adapter(media_source=source)
    plan = adapter.plan_delivery(
        _outbound(media=(_Media("report.pdf", "application/pdf", 5),))
    )
    upload = plan.actions[1]

    upload_issue_observations: list[tuple[int, int, int]] = []

    async def on_upload_issued() -> None:
        upload_issue_observations.append(
            (len(source.opened), api.token_calls, len(api.upload_requests))
        )

    upload_result = await adapter.execute_action(
        upload,
        on_issued=on_upload_issued,
    )
    assert upload_result.status == "sent"
    assert upload_result.artifact_id == "media-1"
    assert upload_issue_observations == [(1, 1, 0)]
    assert bytes(api.uploaded) == b"hello"

    file_message = replace(
        plan.actions[2],
        dependency_artifact_id=upload_result.artifact_id,
    )
    send_result = await adapter.execute_action(file_message, on_issued=_issued)
    assert send_result.status == "sent"
    assert len(api.send_requests) == 1
    assert api.send_requests[0].msg_key == "sampleFile"
    assert json.loads(api.send_requests[0].msg_param) == {
        "mediaId": "media-1",
        "fileName": "report.pdf",
        "fileType": "pdf",
        "fileSize": 5,
    }


async def test_file_upload_propagates_issue_hook_failure_after_preflight() -> None:
    class IssueRejectedError(Exception):
        pass

    source = _MediaSource(chunks=(b"hello",))
    adapter, api, _ = _adapter(media_source=source)
    action = adapter.plan_delivery(
        _outbound(media=(_Media("report.pdf", "application/pdf", 5),))
    ).actions[1]

    async def reject_issue() -> None:
        raise IssueRejectedError

    with pytest.raises(IssueRejectedError):
        await adapter.execute_action(action, on_issued=reject_issue)

    assert source.opened
    assert api.token_calls == 1
    assert api.upload_requests == []


async def test_file_message_without_upload_artifact_fails_before_platform_io() -> None:
    adapter, api, _ = _adapter(media_source=_MediaSource())
    action = adapter.plan_delivery(
        _outbound(media=(_Media("report.pdf", "application/pdf", 5),))
    ).actions[2]

    issue_calls: list[bool] = []

    async def on_issued() -> None:
        issue_calls.append(True)

    result = await adapter.execute_action(action, on_issued=on_issued)

    assert result.status == "failed"
    assert result.error_code == "dingtalk_upload_artifact_missing"
    assert api.send_requests == []
    assert api.token_calls == 0
    assert issue_calls == []


async def test_file_open_failure_is_definitive_before_upload_issue() -> None:
    source = _MediaSource(open_error=OSError("workspace unavailable"))
    adapter, api, _ = _adapter(media_source=source)
    action = adapter.plan_delivery(
        _outbound(media=(_Media("report.pdf", "application/pdf", 5),))
    ).actions[1]

    issue_calls: list[bool] = []

    async def on_issued() -> None:
        issue_calls.append(True)

    result = await adapter.execute_action(action, on_issued=on_issued)

    assert result.status == "failed"
    assert result.error_code == "dingtalk_media_read_failed"
    assert api.upload_requests == []
    assert issue_calls == []


async def test_file_stream_failure_after_upload_issue_is_unknown_without_retry() -> None:
    source = _MediaSource(
        chunks=(b"he", b"llo"),
        after_first_error=OSError("client disconnected"),
    )
    adapter, api, _ = _adapter(media_source=source)
    action = adapter.plan_delivery(
        _outbound(media=(_Media("report.pdf", "application/pdf", 5),))
    ).actions[1]

    issue_calls: list[bool] = []

    async def on_issued() -> None:
        issue_calls.append(True)

    result = await adapter.execute_action(action, on_issued=on_issued)

    assert result.status == "unknown"
    assert result.error_code == "dingtalk_upload_outcome_unknown"
    assert len(api.upload_requests) == 1
    assert issue_calls == [True]


async def test_adapter_opens_only_current_authenticated_attachment_descriptor() -> None:
    decision = normalize_dingtalk_callback(
        _callback_data(
            msgtype="file",
            text=None,
            content={
                "downloadCode": "download-1",
                "fileName": "report.txt",
                "fileSize": 3,
            },
        ),
        client_id="client-1",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )
    assert decision.event is not None
    attachment = decision.event.attachments[0]
    adapter, api, _ = _adapter()

    stream = await adapter.open_authenticated_attachment(decision.event, attachment)

    assert await stream.read(2) == b"ab"
    assert await stream.read(2) == b"c"
    await stream.aclose()
    assert api.token_calls == 1
    assert api.attachment_requests == [attachment]

    with pytest.raises(ValueError, match="not current"):
        await adapter.open_authenticated_attachment(
            replace(decision.event, runtime_generation=UUID(int=0)),
            attachment,
        )
    assert api.token_calls == 1


async def test_http_api_resolves_download_code_and_streams_official_host() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "api.dingtalk.com":
            return httpx.Response(
                200,
                json={"downloadUrl": "https://download.dingtalk.com/files/report.txt"},
            )
        return httpx.Response(200, content=b"abc", headers={"Content-Length": "3"})

    attachment = ExternalAttachmentDescriptor(
        source_id="download-1",
        filename="report.txt",
        content_type="text/plain",
        size=3,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        api = DingTalkHttpApi(
            client_id="client-1",
            client_secret="secret-1",
            http_client=client,
        )
        stream = await api.open_authenticated_attachment("access-token", attachment)
        assert await stream.read(2) == b"ab"
        assert await stream.read(2) == b"c"
        assert await stream.read(2) == b""
        await stream.aclose()

    assert [request.url.host for request in requests] == [
        "api.dingtalk.com",
        "download.dingtalk.com",
    ]
    assert requests[0].headers["x-acs-dingtalk-access-token"] == "access-token"
    assert json.loads(requests[0].content) == {
        "robotCode": "client-1",
        "downloadCode": "download-1",
    }
    assert "x-acs-dingtalk-access-token" not in requests[1].headers


async def test_http_api_rejects_untrusted_download_url_without_fetching_it() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"downloadUrl": "https://evil.example/file"})

    attachment = ExternalAttachmentDescriptor(
        source_id="download-1",
        filename="report.txt",
        content_type="text/plain",
        size=3,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        api = DingTalkHttpApi(
            client_id="client-1",
            client_secret="secret-1",
            http_client=client,
        )
        with pytest.raises(ValueError, match="unavailable"):
            await api.open_authenticated_attachment("access-token", attachment)

    assert len(requests) == 1


async def test_http_api_uses_active_group_and_dm_endpoints_without_retry() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"processQueryKey": f"message-{len(requests)}"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        api = DingTalkHttpApi(
            client_id="client-1",
            client_secret="secret-1",
            http_client=client,
        )
        group = await api.send_message(
            "access-token",
            DingTalkSendRequest(
                destination_kind="group",
                destination_id="cid-group-1",
                msg_key="sampleMarkdown",
                msg_param='{"title":"Result","text":"Done"}',
                idempotency_key="10000000-0000-4000-8000-000000000001",
            ),
        )
        dm = await api.send_message(
            "access-token",
            DingTalkSendRequest(
                destination_kind="dm",
                destination_id="staff-1",
                msg_key="sampleText",
                msg_param='{"content":"Done"}',
                idempotency_key="10000000-0000-4000-8000-000000000002",
            ),
        )

    assert group.message_id == "message-1"
    assert dm.message_id == "message-2"
    assert [request.url.path for request in requests] == [
        "/v1.0/robot/groupMessages/send",
        "/v1.0/robot/oToMessages/batchSend",
    ]
    assert json.loads(requests[0].content) == {
        "robotCode": "client-1",
        "openConversationId": "cid-group-1",
        "msgKey": "sampleMarkdown",
        "msgParam": '{"title":"Result","text":"Done"}',
    }
    assert json.loads(requests[1].content) == {
        "robotCode": "client-1",
        "userIds": ["staff-1"],
        "msgKey": "sampleText",
        "msgParam": '{"content":"Done"}',
    }
    assert requests[0].headers["x-acs-dingtalk-access-token"] == "access-token"
    assert requests[0].headers["x-acs-dingtalk-request-id"] == (
        "10000000-0000-4000-8000-000000000001"
    )


async def test_http_api_streams_file_to_active_upload_endpoint_once() -> None:
    requests: list[httpx.Request] = []
    bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        bodies.append(await request.aread())
        return httpx.Response(200, json={"errcode": 0, "media_id": "media-1"})

    async def chunks() -> AsyncIterator[bytes]:
        yield b"he"
        yield b"llo"

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        api = DingTalkHttpApi(
            client_id="client-1",
            client_secret="secret-1",
            http_client=client,
        )
        result = await api.upload_file(
            "access-token",
            DingTalkUploadRequest(
                filename="report.pdf",
                mime="application/pdf",
                size=5,
                idempotency_key="10000000-0000-4000-8000-000000000003",
            ),
            chunks(),
        )

    assert result.artifact_id == "media-1"
    assert len(requests) == 1
    assert requests[0].url.host == "oapi.dingtalk.com"
    assert requests[0].url.path == "/media/upload"
    assert requests[0].url.params["access_token"] == "access-token"
    assert b"hello" in bodies[0]
    assert int(requests[0].headers["Content-Length"]) == len(bodies[0])
