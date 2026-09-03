import hashlib
from collections.abc import AsyncIterator
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.channels.adapters.dingtalk import (
    DingTalkAdapter,
    DingTalkApiResponse,
)
from openctopus_server.channels.adapters.discord import (
    DiscordAdapter,
    DiscordHTTPRESTClient,
)
from openctopus_server.channels.media import ChannelMediaSource
from openctopus_server.channels.types import OutboundMessage
from openctopus_server.db.models import Device, User
from openctopus_server.devices.registry import ConnectionHandle, DeviceRouteSnapshot
from openctopus_server.devices.workspace import (
    DirectoryCommandResult,
    FileSourceProbe,
    SourceDirectoryJobStatus,
)
from openctopus_server.directory_contract import canonical_json_bytes
from openctopus_server.tools.base import DeliveryRef, ToolContext, ToolResult
from openctopus_server.tools.message import MessageTool, ResolvedMessageTarget


class _Transfers:
    idle_timeout_seconds = 1.0

    def __init__(self, *, content: bytes, fingerprint: str) -> None:
        self.content = content
        self.fingerprint = fingerprint
        self.calls: list[dict[str, object]] = []
        self.lease_closed = False

    async def acquire_operation(self, _user_id: UUID):
        transfers = self

        class _Lease:
            async def aclose(self) -> None:
                transfers.lease_closed = True

        return _Lease()

    async def start_client_to_server(self, **kwargs):
        self.calls.append(kwargs)
        sink = await kwargs["sink_factory"](
            SimpleNamespace(
                purpose="http_relay",
                direction="client_to_server",
                src_path=kwargs["src_path"],
                dst_path=None,
                total_bytes=len(self.content),
                etag=self.fingerprint,
            )
        )
        if self.content:
            await sink.write(self.content)
        await sink.finish()


class _Registry:
    def __init__(self, *, device_id: UUID, content: bytes, fingerprint: str) -> None:
        self.route = DeviceRouteSnapshot(
            handle=ConnectionHandle(device_id=device_id, generation=11),
            config_epoch=4,
            device_name="laptop",
        )
        self.transfers = _Transfers(content=content, fingerprint=fingerprint)
        self.fingerprint = fingerprint

    async def get_route_snapshot(self, device_id: UUID, **kwargs):
        assert device_id == self.route.handle.device_id
        assert kwargs["expected_device_name"] == "laptop"
        return self.route

    async def dispatch_tool_on_snapshot(self, **kwargs):
        assert kwargs["route"] is self.route
        action = kwargs["args"]
        operation = action["operation"]
        if operation == "transfer_source_probe_start":
            digest = hashlib.sha256(
                canonical_json_bytes(
                    {"role": "source", "path": action["path"], "version": 1}
                )
            ).hexdigest()
            result = DirectoryCommandResult(state="running", expected_digest=digest)
        elif operation == "transfer_source_probe_status":
            result = SourceDirectoryJobStatus(
                state="succeeded",
                expected_digest=action["expected_digest"],
                progress_seq=1,
                entries_processed=1,
                files_processed=1,
                bytes_processed=len(self.transfers.content),
                probe=FileSourceProbe(
                    size=len(self.transfers.content),
                    fingerprint=self.fingerprint,
                ),
            )
        elif operation == "transfer_source_probe_release":
            result = DirectoryCommandResult(
                state="released",
                expected_digest=action["expected_digest"],
            )
        else:
            raise AssertionError(f"unexpected operation: {operation}")
        return SimpleNamespace(is_error=False, content=result.model_dump_json())


class _TargetResolver:
    def __init__(self, *, channel: str, chat_id: str) -> None:
        self.channel = channel
        self.chat_id = chat_id
        self.binding_generation = uuid4()

    async def resolve_message_target(self, **_kwargs):
        return ResolvedMessageTarget(
            channel=self.channel,  # type: ignore[arg-type]
            chat_id=self.chat_id,
            binding_generation=self.binding_generation,
        )


class _AdapterRouter:
    def __init__(self, adapter, *, channel: str) -> None:
        self.adapter = adapter
        self.channel = channel
        self.refs: tuple[DeliveryRef, ...] = ()

    async def deliver_message(self, **kwargs):
        target = kwargs["target"]
        self.refs = kwargs["delivery_refs"]
        message = OutboundMessage(
            delivery_key=f"message-tool:{uuid4()}",
            user_id=kwargs["ctx"].user_id,
            turn_id=None,
            origin="message_tool",
            channel=self.channel,  # type: ignore[arg-type]
            chat_id=target.chat_id,
            binding_generation=target.binding_generation,
            content=kwargs["content"],
            media=self.refs,  # type: ignore[arg-type]
        )
        plan = self.adapter.plan_delivery(message)
        kind = "file_message" if self.channel == "discord" else "file_upload"
        action = next(item for item in plan.actions if item.kind == kind)

        async def issued() -> None:
            return None

        result = await self.adapter.execute_action(action, on_issued=issued)
        assert result.status == "sent"
        return ToolResult(content="sent")


class _DingTalkApi:
    def __init__(self) -> None:
        self.uploaded = bytearray()

    async def get_access_token(self) -> str:
        return "access-token"

    async def upload_file(self, _token, _request, chunks: AsyncIterator[bytes]):
        async for chunk in chunks:
            self.uploaded.extend(chunk)
        return DingTalkApiResponse(status_code=200, artifact_id="media-1")

    async def send_message(self, *_args, **_kwargs):
        raise AssertionError("file upload test must not send a visible action")

    async def open_authenticated_attachment(self, *_args, **_kwargs):
        raise AssertionError("outbound test must not open an inbound attachment")

    async def close(self) -> None:
        return None


async def _owner_device(pg_engine, *, content: bytes):
    user_id = uuid4()
    device_id = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(
            User(
                id=user_id,
                email=f"{user_id}@test.com",
                password_hash="hash",
                name="Owner",
            )
        )
        await db.flush()
        db.add(
            Device(
                id=device_id,
                user_id=user_id,
                name="laptop",
                token_hash=b"x" * 32,
                token_hint="hint",
            )
        )
        await db.commit()
    registry = _Registry(
        device_id=device_id,
        content=content,
        fingerprint="source-v1",
    )
    media_source = ChannelMediaSource(
        pg_engine,
        user_id=user_id,
        workspace_service=SimpleNamespace(),
        device_registry=registry,  # type: ignore[arg-type]
        idle_timeout_seconds=1,
    )
    return user_id, device_id, registry, media_source


async def _execute_message(
    pg_engine,
    *,
    user_id: UUID,
    device_id: UUID,
    registry: _Registry,
    target: _TargetResolver,
    router: _AdapterRouter,
) -> ToolResult:
    tool = MessageTool(
        pg_engine,
        SimpleNamespace(),  # type: ignore[arg-type]
        target_resolver=target,  # type: ignore[arg-type]
        delivery_router=router,  # type: ignore[arg-type]
        device_registry=registry,  # type: ignore[arg-type]
    )
    return await tool.execute(
        {
            "content": "attached",
            "openoctopus_device": "laptop",
            "media": ["reports/final.txt"],
        },
        ToolContext(
            user_id=user_id,
            session_id=uuid4(),
            current_channel=target.channel,  # type: ignore[arg-type]
            current_chat_id=target.chat_id,
            current_binding_generation=target.binding_generation,
            device_targets={"laptop": device_id},
        ),
    )


async def test_message_client_media_reaches_discord_upload(pg_engine) -> None:
    content = b"hello"
    user_id, device_id, registry, media_source = await _owner_device(
        pg_engine,
        content=content,
    )
    request_bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        request_bodies.append(await request.aread())
        return httpx.Response(200, json={"id": "discord-message-1"})

    async with httpx.AsyncClient(
        base_url="https://discord.test/api/v10/",
        transport=httpx.MockTransport(handler),
    ) as client:
        rest = DiscordHTTPRESTClient(
            "token",
            media_opener=media_source,
            http_client=client,
        )
        adapter = DiscordAdapter(
            bot_token="token",
            bot_user_id="999",
            binding_generation=uuid4(),
            runtime_generation=uuid4(),
            gateway_factory=lambda *_args: SimpleNamespace(),
            rest_client=rest,
        )
        target = _TargetResolver(channel="discord", chat_id="456")
        router = _AdapterRouter(adapter, channel="discord")
        result = await _execute_message(
            pg_engine,
            user_id=user_id,
            device_id=device_id,
            registry=registry,
            target=target,
            router=router,
        )

    assert result.is_error is False
    assert content in request_bodies[0]
    assert router.refs[0].size == len(content)
    assert registry.transfers.calls
    assert registry.transfers.lease_closed is True


async def test_message_client_media_reaches_dingtalk_upload(pg_engine) -> None:
    content = b"hello"
    user_id, device_id, registry, media_source = await _owner_device(
        pg_engine,
        content=content,
    )
    api = _DingTalkApi()
    adapter = DingTalkAdapter(
        client_id="client-1",
        client_secret="secret-1",
        binding_generation=uuid4(),
        runtime_generation=uuid4(),
        api=api,  # type: ignore[arg-type]
        media_source=media_source,
    )
    target = _TargetResolver(channel="dingtalk", chat_id="group:conversation-1")
    router = _AdapterRouter(adapter, channel="dingtalk")

    result = await _execute_message(
        pg_engine,
        user_id=user_id,
        device_id=device_id,
        registry=registry,
        target=target,
        router=router,
    )

    assert result.is_error is False
    assert bytes(api.uploaded) == content
    assert router.refs[0].size == len(content)
    assert registry.transfers.calls
    assert registry.transfers.lease_closed is True
