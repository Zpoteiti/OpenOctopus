import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import httpx
import pytest

from openctopus_server.channels.adapters import discord as discord_adapter_module
from openctopus_server.channels.adapters.discord import (
    DISCORD_MAX_REQUEST_BYTES,
    DiscordAdapter,
    DiscordCreateMessageRequest,
    DiscordCredentialValidator,
    DiscordHTTPRESTClient,
    DiscordOutcomeUnknownError,
    DiscordRequestNotIssuedError,
    DiscordRESTResponse,
    discord_action_nonce,
)
from openctopus_server.channels.types import (
    ChannelEvent,
    ExternalAttachmentDescriptor,
    OutboundMessage,
)
from openctopus_server.services.channels import (
    ChannelCredentialsInvalidError,
    ChannelCredentialsUnverifiedError,
)

USER_ID = UUID("10000000-0000-4000-8000-000000000001")
BINDING_GENERATION = UUID("20000000-0000-4000-8000-000000000001")
RUNTIME_GENERATION = UUID("30000000-0000-4000-8000-000000000001")


class _REST:
    def __init__(
        self,
        result: DiscordRESTResponse | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.result = result or DiscordRESTResponse(status_code=201, message_id="sent-1")
        self.error = error
        self.requests: list[DiscordCreateMessageRequest] = []
        self.closed = False

    async def create_message(
        self,
        request: DiscordCreateMessageRequest,
        *,
        on_issued: Callable[[], Awaitable[None]],
    ) -> DiscordRESTResponse:
        await on_issued()
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.result

    async def close(self) -> None:
        self.closed = True


class _Gateway:
    def __init__(self, on_message: Any) -> None:
        self.on_message = on_message
        self.started = asyncio.Event()
        self.ready = asyncio.Event()
        self.closed = asyncio.Event()
        self.start_calls: list[tuple[str, bool]] = []
        self.wait_until_ready_calls = 0
        self.wait_until_ready_cancelled = False
        self.close_calls = 0
        self.channels: dict[int, object] = {}
        self.fetch_calls: list[int] = []
        self.start_error: BaseException | None = None

    async def start(self, token: str, *, reconnect: bool) -> None:
        self.start_calls.append((token, reconnect))
        self.started.set()
        if self.start_error is not None:
            raise self.start_error
        await self.closed.wait()

    async def wait_until_ready(self) -> None:
        self.wait_until_ready_calls += 1
        try:
            await self.ready.wait()
        except asyncio.CancelledError:
            self.wait_until_ready_cancelled = True
            raise

    async def close(self) -> None:
        self.close_calls += 1
        self.closed.set()

    def get_channel(self, channel_id: int) -> object | None:
        return self.channels.get(channel_id)

    async def fetch_channel(self, channel_id: int) -> object:
        self.fetch_calls.append(channel_id)
        return self.channels[channel_id]


class _GatewayFactory:
    def __init__(self) -> None:
        self.gateway: _Gateway | None = None

    def __call__(self, on_message: Any) -> _Gateway:
        self.gateway = _Gateway(on_message)
        return self.gateway


@dataclass(frozen=True)
class _Media:
    filename: str
    mime: str
    size: int | None


class _Attachment:
    id = 77
    filename = "report.txt"
    content_type = "text/plain"
    size = 123

    @property
    def url(self) -> str:
        raise AssertionError("normalization must not read or download attachment URLs")


class TextChannel:
    def __init__(self, messages: list[object] | None = None) -> None:
        self.id = 456
        self.messages = messages or []
        self.history_calls: list[dict[str, object]] = []
        self.history_error: BaseException | None = None

    def history(self, **kwargs: object) -> Any:
        self.history_calls.append(kwargs)
        error = self.history_error
        messages = self.messages

        async def iterate() -> Any:
            if error is not None:
                raise error
            for message in messages:
                yield message

        return iterate()


class DMChannel(TextChannel):
    pass


class Thread(TextChannel):
    pass


async def _issued() -> None:
    return None


def _message(
    *,
    message_id: int = 123,
    channel: object | None = None,
    content: str = "hello",
    author_id: int = 42,
    author_bot: bool = False,
    webhook_id: int | None = None,
    mentions: list[object] | None = None,
    attachments: list[object] | None = None,
    system: bool = False,
) -> object:
    author = SimpleNamespace(
        id=author_id,
        bot=author_bot,
        display_name="Alice",
        name="alice",
    )
    return SimpleNamespace(
        id=message_id,
        channel=channel or DMChannel(),
        author=author,
        webhook_id=webhook_id,
        mentions=mentions or [],
        content=content,
        attachments=attachments or [],
        created_at=datetime(2026, 9, 2, 8, 0, tzinfo=UTC),
        is_system=lambda: system,
    )


def _outbound(
    content: str = "hello",
    *,
    media: tuple[_Media, ...] = (),
) -> OutboundMessage:
    return OutboundMessage(
        delivery_key="message-tool:turn-1:tool-1",
        user_id=USER_ID,
        turn_id=None,
        origin="message_tool",
        channel="discord",
        chat_id="456",
        binding_generation=BINDING_GENERATION,
        content=content,
        media=media,
    )


def _adapter(
    *,
    rest: _REST | None = None,
    factory: _GatewayFactory | None = None,
) -> tuple[DiscordAdapter, _REST, _GatewayFactory]:
    rest = rest or _REST()
    factory = factory or _GatewayFactory()
    return (
        DiscordAdapter(
            bot_token="secret-token",
            bot_user_id="999",
            binding_generation=BINDING_GENERATION,
            runtime_generation=RUNTIME_GENERATION,
            gateway_factory=factory,
            rest_client=rest,
        ),
        rest,
        factory,
    )


async def test_credential_validation_reads_typed_bot_identity_without_leaking_token() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/oauth2/applications/@me"):
            return httpx.Response(200, json={"id": "123"})
        return httpx.Response(
            200,
            json={
                "id": "999",
                "username": "Octopus",
                "global_name": "OpenOctopus",
                "avatar": "avatar-hash",
                "bot": True,
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://discord.test/api/v10",
    ) as client:
        identity = await DiscordCredentialValidator(http_client=client).validate_discord(
            "secret-token"
        )

    assert identity.identity_id == "123"
    assert identity.bot_user_id == "999"
    assert identity.display_name == "OpenOctopus"
    assert identity.avatar_url == "https://cdn.discordapp.com/avatars/999/avatar-hash.png"
    assert [request.url.path for request in requests] == [
        "/api/v10/oauth2/applications/@me",
        "/api/v10/users/@me",
    ]
    assert all(request.headers["Authorization"] == "Bot secret-token" for request in requests)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, ChannelCredentialsInvalidError),
        (403, ChannelCredentialsInvalidError),
        (429, ChannelCredentialsUnverifiedError),
        (503, ChannelCredentialsUnverifiedError),
    ],
)
async def test_credential_validation_classifies_failure_without_raw_response(
    status: int,
    expected: type[Exception],
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(status, text="raw secret platform response")
        ),
        base_url="https://discord.test/api/v10",
    ) as client:
        with pytest.raises(expected) as raised:
            await DiscordCredentialValidator(http_client=client).validate_discord("token")

    assert "raw secret platform response" not in str(raised.value)


def test_default_gateway_enables_only_required_intents_and_disables_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    clients: list[object] = []

    class Intents:
        @staticmethod
        def none() -> SimpleNamespace:
            return SimpleNamespace(
                guilds=False,
                guild_messages=False,
                dm_messages=False,
                message_content=False,
                members=False,
                presences=False,
            )

    class MemberCacheFlags:
        @staticmethod
        def none() -> str:
            return "no-members"

    class AllowedMentions:
        @staticmethod
        def none() -> str:
            return "no-mentions"

    class Client:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)
            self.handlers: dict[str, object] = {}
            clients.append(self)

        def event(self, handler: object) -> object:
            self.handlers[getattr(handler, "__name__")] = handler
            return handler

    fake_discord = SimpleNamespace(
        Intents=Intents,
        MemberCacheFlags=MemberCacheFlags,
        AllowedMentions=AllowedMentions,
        Client=Client,
    )
    monkeypatch.setattr(
        discord_adapter_module.importlib,
        "import_module",
        lambda name: fake_discord if name == "discord" else None,
    )

    discord_adapter_module._build_discord_gateway(  # noqa: SLF001
        lambda _message: asyncio.sleep(0),
    )

    intents = captured["intents"]
    assert intents.guilds is True  # type: ignore[union-attr]
    assert intents.guild_messages is True  # type: ignore[union-attr]
    assert intents.dm_messages is True  # type: ignore[union-attr]
    assert intents.message_content is True  # type: ignore[union-attr]
    assert intents.members is False  # type: ignore[union-attr]
    assert intents.presences is False  # type: ignore[union-attr]
    assert captured["member_cache_flags"] == "no-members"
    assert captured["max_messages"] is None
    assert captured["chunk_guilds_at_startup"] is False
    assert captured["allowed_mentions"] == "no-mentions"
    assert len(clients) == 1
    assert set(clients[0].handlers) == {"on_message"}  # type: ignore[attr-defined]


async def test_default_gateway_initializes_client_before_waiting_for_ready() -> None:
    class Client:
        def __init__(self) -> None:
            self.entered = False
            self.started = asyncio.Event()
            self.ready_wait_started = asyncio.Event()
            self.ready = asyncio.Event()
            self.closed = asyncio.Event()

        async def __aenter__(self) -> Any:
            self.entered = True
            return self

        async def __aexit__(self, *_args: object) -> None:
            await self.close()

        async def start(self, _token: str, *, reconnect: bool) -> None:
            assert reconnect is True
            self.started.set()
            await self.closed.wait()

        async def wait_until_ready(self) -> None:
            assert self.entered is True
            self.ready_wait_started.set()
            await self.ready.wait()

        async def close(self) -> None:
            self.closed.set()

    client = Client()
    gateway = discord_adapter_module._DiscordPyGateway(client)  # noqa: SLF001
    ready_waiter = asyncio.create_task(gateway.wait_until_ready())
    await asyncio.sleep(0)
    assert not client.ready_wait_started.is_set()

    starting = asyncio.create_task(gateway.start("secret", reconnect=True))
    await client.started.wait()
    await client.ready_wait_started.wait()
    client.ready.set()
    await ready_waiter
    await gateway.close()
    await starting


async def test_gateway_start_waits_for_ready_and_stop_is_idempotent() -> None:
    adapter, rest, factory = _adapter()
    accepted: list[object] = []
    starting = asyncio.create_task(adapter.start(accepted.append))  # type: ignore[arg-type]
    assert factory.gateway is not None
    await factory.gateway.started.wait()
    assert not starting.done()

    factory.gateway.ready.set()
    await starting
    assert factory.gateway.start_calls == [("secret-token", True)]
    assert factory.gateway.wait_until_ready_calls == 1

    await adapter.stop()
    await adapter.stop()
    await adapter.wait_closed()
    assert factory.gateway.close_calls == 1
    assert rest.closed is True


async def test_gateway_start_timeout_stops_adapter_without_exposing_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        discord_adapter_module,
        "DISCORD_START_TIMEOUT_SECONDS",
        0.01,
    )
    adapter, rest, factory = _adapter()

    async def sink(_event: object) -> None:
        return None

    with pytest.raises(RuntimeError, match="did not become ready") as raised:
        await adapter.start(sink)  # type: ignore[arg-type]

    assert "secret-token" not in str(raised.value)
    assert factory.gateway is not None
    assert factory.gateway.close_calls == 1
    assert rest.closed is True
    await adapter.wait_closed()


async def test_gateway_start_cancellation_joins_ready_waiter_before_close() -> None:
    adapter, rest, factory = _adapter()
    starting = asyncio.create_task(adapter.start(lambda _event: None))  # type: ignore[arg-type]
    assert factory.gateway is not None
    await factory.gateway.started.wait()
    while factory.gateway.wait_until_ready_calls == 0:
        await asyncio.sleep(0)

    starting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await starting

    assert factory.gateway.wait_until_ready_cancelled is True
    assert factory.gateway.close_calls == 1
    assert rest.closed is True
    await adapter.wait_closed()


async def test_gateway_reports_stable_message_content_intent_setup_error() -> None:
    adapter, _, factory = _adapter()
    assert factory.gateway is not None
    privileged_intents_error = type("PrivilegedIntentsRequired", (Exception,), {})
    factory.gateway.start_error = privileged_intents_error("raw gateway details")

    with pytest.raises(RuntimeError, match="MESSAGE_CONTENT intent is not enabled") as raised:
        await adapter.start(lambda _event: None)  # type: ignore[arg-type]

    assert "raw gateway details" not in str(raised.value)
    await adapter.stop()


async def test_gateway_normalizes_structural_trigger_and_never_reads_attachment_url() -> None:
    adapter, _, factory = _adapter()
    accepted: list[object] = []

    async def sink(event: object) -> None:
        accepted.append(event)

    starting = asyncio.create_task(adapter.start(sink))  # type: ignore[arg-type]
    assert factory.gateway is not None
    await factory.gateway.started.wait()
    factory.gateway.ready.set()
    await starting

    group = TextChannel()
    group.name = "  Engineering\n" + ("x" * 200)
    bot = SimpleNamespace(id=999)
    await factory.gateway.on_message(
        _message(
            channel=group,
            content="<@999> please help <@123>",
            mentions=[bot],
            attachments=[_Attachment()],
        )
    )

    assert len(accepted) == 1
    event = accepted[0]
    assert event.platform == "discord"
    assert event.binding_generation == BINDING_GENERATION
    assert event.runtime_generation == RUNTIME_GENERATION
    assert event.source_message_id == "123"
    assert event.chat_id == "456"
    assert event.conversation_kind == "group"
    assert event.sender_id == "42"
    assert event.sender_kind == "human"
    assert event.explicitly_mentions_bot is True
    assert event.text == " please help <@123>"
    assert event.attachments[0].source_id == "77"
    assert event.attachments[0].filename == "report.txt"
    assert event.conversation_label == ("Engineering " + ("x" * 108))

    await adapter.stop()


def test_conversation_label_uses_dm_recipient_then_guild_fallback() -> None:
    dm = DMChannel()
    dm.recipient = SimpleNamespace(display_name="  Alice\x00 Smith  ")
    dm_event = discord_adapter_module._normalize_event(  # noqa: SLF001
        _message(channel=dm),
        bot_user_id="999",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )
    assert dm_event is not None
    assert dm_event.conversation_label == "Alice Smith"

    group = TextChannel()
    group.guild = SimpleNamespace(name="  Example\nGuild  ")
    group_event = discord_adapter_module._normalize_event(  # noqa: SLF001
        _message(channel=group, mentions=[SimpleNamespace(id=999)]),
        bot_user_id="999",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
    )
    assert group_event is not None
    assert group_event.conversation_label == "Example Guild"


async def test_gateway_ignores_plain_name_bot_webhook_system_and_unmentioned_group() -> None:
    adapter, _, factory = _adapter()
    accepted: list[object] = []

    async def sink(event: object) -> None:
        accepted.append(event)

    starting = asyncio.create_task(adapter.start(sink))  # type: ignore[arg-type]
    assert factory.gateway is not None
    await factory.gateway.started.wait()
    factory.gateway.ready.set()
    await starting
    group = TextChannel()

    for message in (
        _message(channel=group, content="@Octopus help"),
        _message(channel=group, author_bot=True, mentions=[SimpleNamespace(id=999)]),
        _message(channel=group, webhook_id=7, mentions=[SimpleNamespace(id=999)]),
        _message(channel=group, system=True, mentions=[SimpleNamespace(id=999)]),
    ):
        await factory.gateway.on_message(message)

    assert accepted == []
    await adapter.stop()


async def test_gateway_accepts_unmentioned_dm_and_classifies_thread_structurally() -> None:
    adapter, _, factory = _adapter()
    accepted: list[object] = []

    async def sink(event: object) -> None:
        accepted.append(event)

    starting = asyncio.create_task(adapter.start(sink))  # type: ignore[arg-type]
    assert factory.gateway is not None
    await factory.gateway.started.wait()
    factory.gateway.ready.set()
    await starting

    await factory.gateway.on_message(_message(channel=DMChannel()))
    await factory.gateway.on_message(
        _message(
            message_id=124,
            channel=Thread(),
            mentions=[SimpleNamespace(id=999)],
        )
    )

    assert [event.conversation_kind for event in accepted] == ["dm", "thread"]
    assert [event.explicitly_mentions_bot for event in accepted] == [False, True]
    await adapter.stop()


async def test_history_calls_once_before_trigger_filters_unsafe_rows_and_orders_oldest() -> None:
    adapter, _, factory = _adapter()
    assert factory.gateway is not None
    channel = TextChannel(
        [
            _message(
                message_id=30,
                channel=TextChannel(),
                content="newer",
                attachments=[_Attachment()],
            ),
            _message(message_id=25, channel=TextChannel(), author_bot=True),
            _message(message_id=20, channel=TextChannel(), webhook_id=8),
            _message(message_id=15, channel=TextChannel(), system=True),
            _message(message_id=10, channel=TextChannel(), content="older"),
        ]
    )
    factory.gateway.channels[456] = channel

    result = await adapter.fetch_recent_context(
        chat_id="456",
        before_message_id="100",
        limit=100,
    )

    assert result.status == "available"
    assert [message.source_message_id for message in result.messages] == ["10", "30"]
    assert [message.text for message in result.messages] == ["older", "newer"]
    assert result.messages[1].attachment_summaries == ("report.txt (text/plain)",)
    assert len(channel.history_calls) == 1
    assert channel.history_calls[0]["limit"] == 100
    assert channel.history_calls[0]["before"].id == 100  # type: ignore[union-attr]


async def test_dm_history_is_unsupported_and_history_failure_is_nonfatal() -> None:
    adapter, _, factory = _adapter()
    assert factory.gateway is not None
    dm = DMChannel()
    factory.gateway.channels[456] = dm
    unsupported = await adapter.fetch_recent_context(
        chat_id="456",
        before_message_id="100",
        limit=100,
    )
    assert unsupported.status == "unsupported"
    assert dm.history_calls == []

    group = TextChannel()
    group.history_error = TimeoutError("raw timeout")
    factory.gateway.channels[456] = group
    failed = await adapter.fetch_recent_context(
        chat_id="456",
        before_message_id="100",
        limit=100,
    )
    assert failed.status == "failed"
    assert failed.error_code == "discord_history_unavailable"
    assert "raw timeout" not in (failed.error_message or "")
    assert len(group.history_calls) == 1


def test_plan_binds_target_stable_bounded_nonces_and_media_without_reading_bytes() -> None:
    adapter, _, _ = _adapter()
    media = _Media(filename="report.bin", mime="application/octet-stream", size=10)

    first = adapter.plan_delivery(_outbound("x" * 2_001, media=(media,)))
    second = adapter.plan_delivery(_outbound("x" * 2_001, media=(media,)))

    assert first == second
    assert [action.kind for action in first.actions] == [
        "text_message",
        "text_message",
        "file_message",
    ]
    assert all(action.chat_id == "456" for action in first.actions)
    assert [action.idempotency_key for action in first.actions] == [
        discord_action_nonce("message-tool:turn-1:tool-1", 0),
        discord_action_nonce("message-tool:turn-1:tool-1", 1),
        discord_action_nonce("message-tool:turn-1:tool-1", 2),
    ]
    assert all(len(action.idempotency_key or "") <= 25 for action in first.actions)
    assert first.actions[-1].media is media
    assert first.actions[-1].media_index == 0


async def test_execute_text_disables_mentions_enforces_nonce_and_sends_once() -> None:
    rest = _REST(DiscordRESTResponse(status_code=201, message_id="discord-message"))
    adapter, _, _ = _adapter(rest=rest)
    action = adapter.plan_delivery(_outbound("@everyone <@42>" )).actions[0]

    issue_observations: list[int] = []

    async def on_issued() -> None:
        issue_observations.append(len(rest.requests))

    result = await adapter.execute_action(action, on_issued=on_issued)

    assert result.status == "sent"
    assert result.platform_message_id == "discord-message"
    assert len(rest.requests) == 1
    assert issue_observations == [0]
    request = rest.requests[0]
    assert request.chat_id == "456"
    assert request.payload == {
        "content": "@everyone <@42>",
        "nonce": action.idempotency_key,
        "enforce_nonce": True,
        "allowed_mentions": {"parse": []},
    }
    assert request.media is None


async def test_execute_propagates_issue_hook_failure_without_platform_request() -> None:
    class IssueRejectedError(Exception):
        pass

    rest = _REST()
    adapter, _, _ = _adapter(rest=rest)
    action = adapter.plan_delivery(_outbound()).actions[0]

    async def reject_issue() -> None:
        raise IssueRejectedError

    with pytest.raises(IssueRejectedError):
        await adapter.execute_action(action, on_issued=reject_issue)

    assert rest.requests == []


@pytest.mark.parametrize(
    ("status", "aggregate", "code"),
    [
        (400, "failed", "discord_request_rejected"),
        (403, "failed", "discord_request_rejected"),
        (404, "failed", "discord_request_rejected"),
        (429, "failed", "discord_rate_limited"),
        (500, "unknown", "discord_outcome_unknown"),
        (503, "unknown", "discord_outcome_unknown"),
    ],
)
async def test_execute_classifies_http_response_without_retry(
    status: int,
    aggregate: str,
    code: str,
) -> None:
    rest = _REST(DiscordRESTResponse(status_code=status, message_id=None))
    adapter, _, _ = _adapter(rest=rest)

    result = await adapter.execute_action(
        adapter.plan_delivery(_outbound()).actions[0],
        on_issued=_issued,
    )

    assert result.status == aggregate
    assert result.error_code == code
    assert len(rest.requests) == 1


async def test_execute_transport_timeout_is_unknown_and_never_retried() -> None:
    rest = _REST(error=DiscordOutcomeUnknownError())
    adapter, _, _ = _adapter(rest=rest)

    result = await adapter.execute_action(
        adapter.plan_delivery(_outbound()).actions[0],
        on_issued=_issued,
    )

    assert result.status == "unknown"
    assert result.error_code == "discord_outcome_unknown"
    assert len(rest.requests) == 1


async def test_http_transport_issues_one_request_and_does_not_retry_429() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(429, json={"retry_after": 0.001})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://discord.test/api/v10/",
    ) as client:
        transport = DiscordHTTPRESTClient(
            "secret-token",
            media_opener=None,
            http_client=client,
        )
        response = await transport.create_message(
            DiscordCreateMessageRequest(
                chat_id="456",
                payload={
                    "content": "hello",
                    "nonce": "nonce",
                    "enforce_nonce": True,
                    "allowed_mentions": {"parse": []},
                },
                media=None,
                estimated_size=100,
            ),
            on_issued=_issued,
        )
        await transport.close()

    assert response.status_code == 429
    assert len(requests) == 1
    assert requests[0].headers["Authorization"] == "Bot secret-token"


async def test_http_transport_streams_one_bounded_multipart_file_and_closes_source() -> None:
    class Stream:
        def __init__(self) -> None:
            self.chunks = [b"abc", b""]
            self.closed = False

        async def read(self) -> bytes:
            return self.chunks.pop(0)

        async def aclose(self) -> None:
            self.closed = True

    stream = Stream()
    opened: list[object] = []
    order: list[str] = []

    async def opener(media: object) -> Stream:
        opened.append(media)
        order.append("open")
        return stream

    async def on_issued() -> None:
        order.append("issue")

    bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        order.append("http")
        bodies.append(await request.aread())
        return httpx.Response(201, json={"id": "uploaded-message"})

    media = _Media(filename="报告.bin", mime="application/octet-stream", size=3)
    adapter, _, _ = _adapter()
    action = adapter.plan_delivery(_outbound(media=(media,))).actions[-1]
    prepared = discord_adapter_module._prepare_create_message(action)  # noqa: SLF001
    assert isinstance(prepared, DiscordCreateMessageRequest)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://discord.test/api/v10/",
    ) as client:
        transport = DiscordHTTPRESTClient(
            "secret-token",
            media_opener=opener,  # type: ignore[arg-type]
            http_client=client,
        )
        response = await transport.create_message(prepared, on_issued=on_issued)
        await transport.close()

    assert response == DiscordRESTResponse(
        status_code=201,
        message_id="uploaded-message",
    )
    assert opened == [media]
    assert order == ["open", "issue", "http"]
    assert stream.closed is True
    assert len(bodies) == 1
    assert b"abc" in bodies[0]
    assert len(bodies[0]) == prepared.estimated_size


async def test_http_transport_media_open_failure_stays_before_issue_hook() -> None:
    async def opener(_media: object) -> Any:
        raise OSError("workspace unavailable")

    issue_calls: list[bool] = []

    async def on_issued() -> None:
        issue_calls.append(True)

    media = _Media(filename="report.bin", mime="application/octet-stream", size=3)
    adapter, _, _ = _adapter()
    action = adapter.plan_delivery(_outbound(media=(media,))).actions[-1]
    prepared = discord_adapter_module._prepare_create_message(action)  # noqa: SLF001
    assert isinstance(prepared, DiscordCreateMessageRequest)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(201)),
        base_url="https://discord.test/api/v10/",
    ) as client:
        transport = DiscordHTTPRESTClient(
            "secret-token",
            media_opener=opener,  # type: ignore[arg-type]
            http_client=client,
        )
        with pytest.raises(DiscordRequestNotIssuedError):
            await transport.create_message(prepared, on_issued=on_issued)

    assert issue_calls == []


async def test_authenticated_attachment_refetches_message_and_streams_official_cdn() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "discord.test":
            return httpx.Response(
                200,
                json={
                    "attachments": [
                        {
                            "id": "77",
                            "filename": "report.txt",
                            "size": 3,
                            "url": (
                                "https://cdn.discordapp.com/attachments/456/77/"
                                "report.txt?ex=signed"
                            ),
                        }
                    ]
                },
            )
        return httpx.Response(200, content=b"abc", headers={"Content-Length": "3"})

    attachment = ExternalAttachmentDescriptor(
        source_id="77",
        filename="report.txt",
        content_type="text/plain",
        size=3,
    )
    event = ChannelEvent(
        platform="discord",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        source_message_id="123",
        chat_id="456",
        conversation_kind="dm",
        sender_id="42",
        sender_display_name="Owner",
        sender_kind="human",
        explicitly_mentions_bot=False,
        text="",
        attachments=(attachment,),
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://discord.test/api/v10/",
    ) as client:
        rest = DiscordHTTPRESTClient(
            "secret-token",
            media_opener=None,
            http_client=client,
        )
        adapter = DiscordAdapter(
            bot_token="secret-token",
            bot_user_id="999",
            binding_generation=BINDING_GENERATION,
            runtime_generation=RUNTIME_GENERATION,
            gateway_factory=_GatewayFactory(),
            rest_client=rest,
        )
        stream = await adapter.open_authenticated_attachment(event, attachment)
        assert await stream.read(2) == b"ab"
        assert await stream.read(2) == b"c"
        assert await stream.read(2) == b""
        await stream.aclose()

    assert [request.url.host for request in requests] == [
        "discord.test",
        "cdn.discordapp.com",
    ]
    assert requests[0].headers["Authorization"] == "Bot secret-token"
    assert "Authorization" not in requests[1].headers


async def test_authenticated_attachment_rejects_non_discord_download_host() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "attachments": [
                    {
                        "id": "77",
                        "filename": "report.txt",
                        "size": 3,
                        "url": "https://evil.example/attachments/456/77/report.txt",
                    }
                ]
            },
        )

    attachment = ExternalAttachmentDescriptor("77", "report.txt", "text/plain", 3)
    event = ChannelEvent(
        platform="discord",
        binding_generation=BINDING_GENERATION,
        runtime_generation=RUNTIME_GENERATION,
        source_message_id="123",
        chat_id="456",
        conversation_kind="dm",
        sender_id="42",
        sender_display_name=None,
        sender_kind="human",
        explicitly_mentions_bot=False,
        text="",
        attachments=(attachment,),
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://discord.test/api/v10/",
    ) as client:
        rest = DiscordHTTPRESTClient(
            "secret-token",
            media_opener=None,
            http_client=client,
        )
        adapter = DiscordAdapter(
            bot_token="secret-token",
            bot_user_id="999",
            binding_generation=BINDING_GENERATION,
            runtime_generation=RUNTIME_GENERATION,
            gateway_factory=_GatewayFactory(),
            rest_client=rest,
        )
        with pytest.raises(DiscordRequestNotIssuedError):
            await adapter.open_authenticated_attachment(event, attachment)

    assert len(requests) == 1


@pytest.mark.parametrize("size", [None, DISCORD_MAX_REQUEST_BYTES])
async def test_file_unknown_or_oversized_request_fails_before_transport(size: int | None) -> None:
    rest = _REST()
    adapter, _, _ = _adapter(rest=rest)
    media = _Media(filename="large.bin", mime="application/octet-stream", size=size)
    action = adapter.plan_delivery(_outbound(media=(media,))).actions[-1]

    issue_calls: list[bool] = []

    async def on_issued() -> None:
        issue_calls.append(True)

    result = await adapter.execute_action(action, on_issued=on_issued)

    assert result.status == "failed"
    assert result.error_code in {
        "tool_channel_media_size_unknown",
        "discord_request_too_large",
    }
    assert rest.requests == []
    assert issue_calls == []


async def test_file_action_passes_resolved_media_once_without_adapter_download() -> None:
    rest = _REST()
    adapter, _, _ = _adapter(rest=rest)
    media = _Media(filename="small.bin", mime="application/octet-stream", size=8)
    action = adapter.plan_delivery(_outbound(media=(media,))).actions[-1]

    result = await adapter.execute_action(action, on_issued=_issued)

    assert result.status == "sent"
    assert len(rest.requests) == 1
    assert rest.requests[0].media is media
    assert rest.requests[0].payload["attachments"] == [
        {"id": "0", "filename": "small.bin"}
    ]
