import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from typing import cast
from uuid import UUID, uuid4

import pytest

from openctopus_server.devices.mcp_catalog import EMPTY_CATALOG_DIGEST
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    StdioMcpServerConfig,
    StreamableHttpMcpServerConfig,
)
from openctopus_server.devices.mcp_routes import (
    AcceptedMcpBinding,
    FrozenMcpEntryRoute,
    McpRegistrationCandidate,
)
from openctopus_server.devices.protocol import (
    ConfigAppliedFrame,
    ConfigValidateResultFrame,
    DeviceConfigFrame,
    HelloAckFrame,
    McpValidationFailure,
    RegisterMcpAckFrame,
    ShellMetadata,
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    ToolResultFrame,
    TransferBeginFrame,
    TransferEndFrame,
    new_uuid7,
)
from openctopus_server.devices.registry import (
    BridgeRoutePair,
    ConnectionHandle,
    DeviceBusyError,
    DeviceMcpUnavailableError,
    DeviceOutcomeUnknownError,
    DeviceProtocolError,
    DeviceRegistry,
    DeviceRouteSnapshot,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import TransferUnavailableError
from openctopus_server.mcp.authority import ServerMcpAuthorityFence
from openctopus_server.mcp.models import empty_server_mcp_envelope, parse_server_mcp_configs


@dataclass
class FakeTransport:
    sent_text: list[str] = field(default_factory=list)
    closes: list[tuple[int, str]] = field(default_factory=list)
    sent: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.sent_text.append(payload)
        self.sent.set()

    async def close(self, code: int, reason: str) -> None:
        self.closes.append((code, reason))


@dataclass
class FailingTransport(FakeTransport):
    async def send_text(self, payload: str) -> None:
        del payload
        raise OSError("socket is closed")


@dataclass
class BlockingTransport(FakeTransport):
    send_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_send: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.send_started.set()
        await self.release_send.wait()
        self.sent_text.append(payload)
        self.sent.set()


@dataclass
class BlockingFailingTransport(FakeTransport):
    send_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_send: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        del payload
        self.send_started.set()
        await self.release_send.wait()
        raise OSError("socket is closed")


@dataclass
class FirstSendBlockingTransport(FakeTransport):
    attempted_text: list[str] = field(default_factory=list)
    send_started: asyncio.Event = field(default_factory=asyncio.Event)
    first_send_cancelled: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.attempted_text.append(payload)
        if len(self.attempted_text) == 1:
            self.send_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.first_send_cancelled.set()
                raise
        self.sent_text.append(payload)
        self.sent.set()


@dataclass
class EverySendBlockingTransport(FakeTransport):
    attempted_text: list[str] = field(default_factory=list)
    first_send_started: asyncio.Event = field(default_factory=asyncio.Event)
    second_send_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_second_send: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.attempted_text.append(payload)
        if len(self.attempted_text) == 1:
            self.first_send_started.set()
            await asyncio.Future()
        else:
            self.second_send_started.set()
            await self.release_second_send.wait()


@dataclass
class ImmediateConfigAppliedTransport(FakeTransport):
    registry: DeviceRegistry | None = None
    handle: ConnectionHandle | None = None

    async def send_text(self, payload: str) -> None:
        self.sent_text.append(payload)
        frame = cast(dict[str, object], json.loads(payload))
        if frame["type"] in {"hello_ack", "config_update"}:
            assert self.registry is not None
            assert self.handle is not None
            assert await self.registry.resolve_config_applied(
                self.handle,
                ConfigAppliedFrame(
                    id=UUID(str(frame["id"])),
                    config_revision=int(frame["config_revision"]),
                ),
            )
        self.sent.set()


@dataclass
class AmbiguousConfigTransport(FakeTransport):
    deliver_before_block: bool = False
    send_started: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        if self.deliver_before_block:
            self.sent_text.append(payload)
        self.send_started.set()
        await asyncio.Future()


@dataclass
class RecordingSink:
    chunks: list[bytes] = field(default_factory=list)
    finished: bool = False
    aborted: bool = False

    async def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)

    async def finish(self) -> None:
        self.finished = True

    async def abort(self) -> None:
        self.aborted = True


async def _wait_for_sent(transport: FakeTransport) -> dict[str, object]:
    await asyncio.wait_for(transport.sent.wait(), timeout=1)
    return cast(dict[str, object], json.loads(transport.sent_text[-1]))


async def _push_and_apply(
    registry: DeviceRegistry,
    handle: ConnectionHandle,
    transport: FakeTransport,
    *,
    device_id: UUID,
    user_id: UUID,
    device_name: str,
    config: DeviceConfigFrame,
) -> dict[str, object]:
    before = len(transport.sent_text)
    push = asyncio.create_task(
        registry.push_config(
            device_id=device_id,
            user_id=user_id,
            device_name=device_name,
            config=config,
        )
    )
    for _ in range(100):
        updates = [
            cast(dict[str, object], json.loads(payload))
            for payload in transport.sent_text[before:]
            if json.loads(payload)["type"] == "config_update"
        ]
        if updates:
            break
        await asyncio.sleep(0)
    assert updates
    update = updates[0]
    assert await registry.resolve_config_applied(
        handle,
        ConfigAppliedFrame(
            id=UUID(str(update["id"])),
            config_revision=int(update["config_revision"]),
        ),
    )
    assert await push
    return update


def _empty_catalog() -> PersistedMcpCatalog:
    return PersistedMcpCatalog(
        version=1,
        digest=EMPTY_CATALOG_DIGEST,
        servers=[],
    )


def _hello_ack(frame_id: UUID) -> HelloAckFrame:
    return HelloAckFrame(
        id=frame_id,
        device_name="laptop",
        config_revision=1,
        config=DeviceConfigFrame(
            workspace_path="~/workspace",
            restrict_to_workspace=True,
            ssrf_denylist=[],
        ),
        mcp_catalog=_empty_catalog(),
    )


async def test_dispatch_correlates_result_and_enforces_owner() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )

    task = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=131_072,
            timeout_seconds=1,
        )
    )
    payload = await _wait_for_sent(transport)
    result = ToolResultFrame(
        id=UUID(str(payload["id"])),
        content="hello",
        is_error=False,
    )

    assert await registry.resolve_tool_result(handle, result) is True
    assert await task == result

    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=uuid4(),
            name="read_file",
            args={},
            max_result_bytes=1024,
            timeout_seconds=1,
        )


async def test_result_must_fit_the_credit_reserved_by_its_tool_call() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="read_file",
            args={"path": "a.txt"},
            max_result_bytes=256,
            timeout_seconds=10,
        )
    )
    payload = await _wait_for_sent(transport)
    oversized = ToolResultFrame(
        id=UUID(str(payload["id"])),
        content="x" * 512,
        is_error=False,
    )

    with pytest.raises(DeviceProtocolError, match="credit"):
        await registry.resolve_tool_result(
            handle,
            oversized,
            encoded_bytes=len(oversized.model_dump_json().encode("utf-8")),
        )

    assert pending.done() is False
    assert await registry.unregister(handle) is True
    with pytest.raises(DeviceOutcomeUnknownError):
        await pending


async def test_replacement_fails_old_pending_and_stale_unregister_is_harmless() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={"path": "."},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    await _wait_for_sent(old_transport)

    new_transport = FakeTransport()
    new_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=new_transport,
    )

    with pytest.raises(DeviceOutcomeUnknownError):
        await pending
    assert old_transport.closes == [(4000, "connection_replaced")]
    assert new_handle.generation > old_handle.generation
    assert await registry.unregister(old_handle) is False
    assert await registry.is_online(device_id, user_id=user_id) is True


async def test_stale_generation_cannot_complete_new_call() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    new_transport = FakeTransport()
    new_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=new_transport,
    )
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=1,
        )
    )
    payload = await _wait_for_sent(new_transport)
    result = ToolResultFrame(
        id=UUID(str(payload["id"])),
        content="new",
        is_error=False,
    )

    assert await registry.resolve_tool_result(old_handle, result) is False
    assert pending.done() is False
    assert await registry.resolve_tool_result(new_handle, result) is True
    assert await pending == result


async def test_replaced_generation_cannot_send_a_late_tool_call() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )

    new_transport = FakeTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=new_transport,
    )

    assert await registry.send_text(old_handle, "late") is False
    assert old_transport.sent_text == []


async def test_unready_registration_is_not_online_or_routable_until_activation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()

    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        ready=False,
    )

    assert handle is not None
    assert await registry.is_online(device_id, user_id=user_id) is False
    assert await registry.get_handle(device_id, user_id=user_id) is None
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=1024,
            timeout_seconds=1,
        )

    ack = _hello_ack(new_uuid7())
    activation = asyncio.create_task(registry.activate(handle, ack, timeout_seconds=1))
    sent = await _wait_for_sent(transport)
    assert sent["type"] == "hello_ack"
    assert await registry.is_online(device_id, user_id=user_id) is False

    assert await registry.resolve_config_applied(
        handle,
        ConfigAppliedFrame(id=ack.id, config_revision=1),
    )
    assert await activation is True
    assert [json.loads(payload)["type"] for payload in transport.sent_text] == [
        "hello_ack",
        "config_applied_ack",
    ]
    assert await registry.is_online(device_id, user_id=user_id) is True


async def test_activation_accepts_config_applied_before_send_returns() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = ImmediateConfigAppliedTransport(registry=registry)
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        ready=False,
    )
    transport.handle = handle

    assert await registry.activate(handle, _hello_ack(new_uuid7()), timeout_seconds=1)
    assert [json.loads(payload)["type"] for payload in transport.sent_text] == [
        "hello_ack",
        "config_applied_ack",
    ]


async def test_config_apply_requires_matching_id_and_revision() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        ready=False,
    )
    ack = _hello_ack(new_uuid7())
    activation = asyncio.create_task(registry.activate(handle, ack, timeout_seconds=1))
    await _wait_for_sent(transport)

    assert not await registry.resolve_config_applied(
        handle,
        ConfigAppliedFrame(id=new_uuid7(), config_revision=1),
    )
    assert not await registry.resolve_config_applied(
        handle,
        ConfigAppliedFrame(id=ack.id, config_revision=2),
    )
    assert await registry.is_online(device_id, user_id=user_id) is False
    assert await registry.resolve_config_applied(
        handle,
        ConfigAppliedFrame(id=ack.id, config_revision=1),
    )
    assert await activation


async def test_config_update_is_fenced_until_applied_ack_is_sent() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="old-name",
        transport=transport,
    )
    assert await registry.begin_config_update(device_id=device_id, user_id=user_id)
    update_id = new_uuid7()
    update = asyncio.create_task(
        registry.push_config(
            device_id=device_id,
            user_id=user_id,
            device_name="new-name",
            config=DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            ),
            config_revision=2,
            mcp_catalog=_empty_catalog(),
            frame_id=update_id,
            expected_handle=handle,
            timeout_seconds=1,
        )
    )
    await _wait_for_sent(transport)
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="old-name",
            name="list_dir",
            args={},
            max_result_bytes=1024,
            timeout_seconds=1,
        )

    assert await registry.resolve_config_applied(
        handle,
        ConfigAppliedFrame(id=update_id, config_revision=2),
    )
    assert await update
    assert [json.loads(payload)["type"] for payload in transport.sent_text] == [
        "config_update",
        "config_applied_ack",
    ]


async def test_config_update_accepts_config_applied_before_send_returns() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = ImmediateConfigAppliedTransport(registry=registry)
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    transport.handle = handle
    assert await registry.begin_config_update(device_id=device_id, user_id=user_id)

    assert await registry.push_config(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        config=DeviceConfigFrame(
            workspace_path="~/workspace",
            restrict_to_workspace=True,
            ssrf_denylist=[],
        ),
        config_revision=2,
        expected_handle=handle,
        timeout_seconds=1,
    )
    assert [json.loads(payload)["type"] for payload in transport.sent_text] == [
        "config_update",
        "config_applied_ack",
    ]


async def test_mcp_binding_is_published_only_after_ack_and_dispatch_uses_runtime() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    entry_id = new_uuid7()
    runtime_generation = new_uuid7()
    transport = BlockingTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        config_revision=7,
        catalog_digest="1" * 64,
    )
    candidate = McpRegistrationCandidate(
        ack=RegisterMcpAckFrame(
            id=new_uuid7(),
            config_revision=7,
            catalog_digest="1" * 64,
            results=[],
        ),
        bindings=(
            AcceptedMcpBinding(
                name="demo",
                runtime_generation=runtime_generation,
                config_revision=7,
                catalog_digest="1" * 64,
                entry_ids=(entry_id,),
            ),
        ),
    )
    route = FrozenMcpEntryRoute(
        device_id=device_id,
        device_name="laptop",
        entry_id=entry_id,
        config_revision=7,
        catalog_digest="1" * 64,
        server="demo",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name="mcp_demo_search",
    )

    registration = asyncio.create_task(registry.publish_mcp_registration(handle, candidate))
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)
    with pytest.raises(DeviceMcpUnavailableError):
        await registry.dispatch_mcp_tool(
            route=route,
            user_id=user_id,
            name=route.final_name,
            args={"query": "x"},
            max_result_bytes=1024,
            timeout_seconds=1,
        )

    transport.release_send.set()
    assert await registration
    transport.sent.clear()
    call = asyncio.create_task(
        registry.dispatch_mcp_tool(
            route=route,
            user_id=user_id,
            name=route.final_name,
            args={"query": "x"},
            max_result_bytes=1024,
            timeout_seconds=1,
        )
    )
    payload = await _wait_for_sent(transport)
    assert payload["mcp_route"] == {
        "entry_id": str(entry_id),
        "config_revision": 7,
        "catalog_digest": "1" * 64,
        "runtime_generation": str(runtime_generation),
    }
    result = ToolResultFrame(id=UUID(str(payload["id"])), content="ok", is_error=False)
    assert await registry.resolve_tool_result(handle, result)
    assert await call == result


async def test_mcp_epoch_change_before_send_is_reported_as_route_unavailable() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    entry_id = new_uuid7()
    transport = BlockingTransport()
    transport.release_send.set()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        config_revision=7,
        catalog_digest="1" * 64,
    )

    def candidate(runtime_generation: UUID) -> McpRegistrationCandidate:
        return McpRegistrationCandidate(
            ack=RegisterMcpAckFrame(
                id=new_uuid7(),
                config_revision=7,
                catalog_digest="1" * 64,
                results=[],
            ),
            bindings=(
                AcceptedMcpBinding(
                    name="demo",
                    runtime_generation=runtime_generation,
                    config_revision=7,
                    catalog_digest="1" * 64,
                    entry_ids=(entry_id,),
                ),
            ),
        )

    assert await registry.publish_mcp_registration(handle, candidate(new_uuid7()))
    route = FrozenMcpEntryRoute(
        device_id=device_id,
        device_name="laptop",
        entry_id=entry_id,
        config_revision=7,
        catalog_digest="1" * 64,
        server="demo",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name="mcp_demo_search",
    )
    transport.release_send.clear()
    transport.send_started.clear()
    blocker = asyncio.create_task(registry.send_text(handle, "block"))
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)
    registration = asyncio.create_task(
        registry.publish_mcp_registration(handle, candidate(new_uuid7()))
    )
    await asyncio.sleep(0)
    call = asyncio.create_task(
        registry.dispatch_mcp_tool(
            route=route,
            user_id=user_id,
            name=route.final_name,
            args={"query": "x"},
            max_result_bytes=1024,
            timeout_seconds=1,
        )
    )
    await asyncio.sleep(0)

    transport.release_send.set()
    assert await blocker
    assert await registration
    with pytest.raises(DeviceMcpUnavailableError):
        await call


async def test_server_authority_commit_fences_device_mcp_issue() -> None:
    registry = DeviceRegistry()
    authority = ServerMcpAuthorityFence(empty_server_mcp_envelope())
    device_id = uuid4()
    user_id = uuid4()
    entry_id = new_uuid7()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        config_revision=7,
        catalog_digest="1" * 64,
    )
    assert await registry.publish_mcp_registration(
        handle,
        McpRegistrationCandidate(
            ack=RegisterMcpAckFrame(
                id=new_uuid7(),
                config_revision=7,
                catalog_digest="1" * 64,
                results=[],
            ),
            bindings=(
                AcceptedMcpBinding(
                    name="demo",
                    runtime_generation=new_uuid7(),
                    config_revision=7,
                    catalog_digest="1" * 64,
                    entry_ids=(entry_id,),
                ),
            ),
        ),
    )
    transport.sent_text.clear()
    route = FrozenMcpEntryRoute(
        device_id=device_id,
        device_name="laptop",
        entry_id=entry_id,
        config_revision=7,
        catalog_digest="1" * 64,
        server="demo",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name="mcp_demo_search",
        server_config_revision=1,
    )
    issued = False

    def mark_issued() -> None:
        nonlocal issued
        issued = True

    reserved = empty_server_mcp_envelope().model_copy(
        update={
            "config_revision": 2,
            "mcp_servers": list(
                parse_server_mcp_configs(
                    [
                        {
                            "name": "demo",
                            "transport": "streamable_http",
                            "url": "https://mcp.example/mcp",
                            "enabled_capabilities": [],
                        }
                    ]
                )
            ),
        }
    )
    async with authority.transition():
        call = asyncio.create_task(
            registry.dispatch_mcp_tool(
                route=route,
                user_id=user_id,
                name=route.final_name,
                args={"query": "x"},
                max_result_bytes=1024,
                timeout_seconds=1,
                on_issued=mark_issued,
                issue_guard=lambda: authority.device_issue(route),
            )
        )
        await asyncio.sleep(0)
        assert not call.done()
        authority.publish(reserved)

    with pytest.raises(DeviceMcpUnavailableError):
        await call
    assert issued is False
    assert transport.sent_text == []


async def test_config_transition_waits_for_registration_then_pushes() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = BlockingTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        config_revision=7,
        catalog_digest="1" * 64,
    )
    candidate = McpRegistrationCandidate(
        ack=RegisterMcpAckFrame(
            id=new_uuid7(),
            config_revision=7,
            catalog_digest="1" * 64,
            results=[],
        ),
        bindings=(),
    )
    registration = asyncio.create_task(
        registry.publish_mcp_registration(handle, candidate, timeout_seconds=1)
    )
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)
    transition = asyncio.create_task(
        registry.begin_config_update(
            device_id=device_id,
            user_id=user_id,
            expected_handle=handle,
        )
    )
    await asyncio.sleep(0)
    assert not transition.done()

    transport.release_send.set()
    assert await registration
    assert await asyncio.wait_for(transition, timeout=1)
    update_id = new_uuid7()
    update = asyncio.create_task(
        registry.push_config(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            config=DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            ),
            config_revision=8,
            mcp_catalog=_empty_catalog(),
            frame_id=update_id,
            expected_handle=handle,
            timeout_seconds=1,
        )
    )
    for _ in range(100):
        if any(json.loads(payload)["type"] == "config_update" for payload in transport.sent_text):
            break
        await asyncio.sleep(0)
    assert await registry.resolve_config_applied(
        handle,
        ConfigAppliedFrame(id=update_id, config_revision=8),
    )
    assert await update


async def test_cancelled_config_transition_wait_does_not_leak_a_fence() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = BlockingTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    candidate = McpRegistrationCandidate(
        ack=RegisterMcpAckFrame(
            id=new_uuid7(),
            config_revision=1,
            catalog_digest=EMPTY_CATALOG_DIGEST,
            results=[],
        ),
        bindings=(),
    )
    registration = asyncio.create_task(
        registry.publish_mcp_registration(handle, candidate, timeout_seconds=1)
    )
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)
    transition = asyncio.create_task(
        registry.begin_config_update(
            device_id=device_id,
            user_id=user_id,
            expected_handle=handle,
        )
    )
    await asyncio.sleep(0)
    transition.cancel()
    with pytest.raises(asyncio.CancelledError):
        await transition

    transport.release_send.set()
    assert await registration
    assert await registry.begin_config_update(
        device_id=device_id,
        user_id=user_id,
        expected_handle=handle,
    )
    await registry.abort_config_update(device_id=device_id, user_id=user_id)


async def test_failed_registration_releases_a_waiting_config_transition() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = BlockingFailingTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    candidate = McpRegistrationCandidate(
        ack=RegisterMcpAckFrame(
            id=new_uuid7(),
            config_revision=1,
            catalog_digest=EMPTY_CATALOG_DIGEST,
            results=[],
        ),
        bindings=(),
    )
    registration = asyncio.create_task(
        registry.publish_mcp_registration(handle, candidate, timeout_seconds=1)
    )
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)
    transition = asyncio.create_task(
        registry.begin_config_update(
            device_id=device_id,
            user_id=user_id,
            expected_handle=handle,
        )
    )
    await asyncio.sleep(0)
    assert not transition.done()

    transport.release_send.set()
    assert not await registration
    assert not await asyncio.wait_for(transition, timeout=1)
    assert not await registry.is_online(device_id, user_id=user_id)


async def test_validation_late_result_consumes_tombstone_once() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    with pytest.raises(TimeoutError):
        await registry.validate_config(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="laptop",
            base_config_revision=1,
            candidate_config=DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            ),
            validate_servers=("demo",),
            timeout_seconds=0,
        )
    validation = json.loads(transport.sent_text[0])
    late = ConfigValidateResultFrame(
        id=UUID(validation["id"]),
        ok=False,
        failures=[
            McpValidationFailure(
                name="demo",
                stage="initialize",
                code="config_validation_failed",
                message="failed",
            )
        ],
    )

    assert await registry.resolve_config_validate_result(handle, late)
    assert not await registry.resolve_config_validate_result(handle, late)


async def test_config_validation_deadline_includes_blocked_send() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FirstSendBlockingTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    validation = asyncio.create_task(
        registry.validate_config(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="laptop",
            base_config_revision=1,
            candidate_config=DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            ),
            validate_servers=("demo",),
            timeout_seconds=0.01,
        )
    )
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)
    try:
        await asyncio.sleep(0.05)
        assert validation.done()
    finally:
        if not validation.done():
            validation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await validation

    with pytest.raises(TimeoutError):
        await validation
    assert transport.first_send_cancelled.is_set()
    request = json.loads(transport.attempted_text[0])
    await asyncio.wait_for(transport.sent.wait(), timeout=1)
    assert json.loads(transport.sent_text[0]) == {
        "type": "config_validate_cancel",
        "id": request["id"],
    }
    late = ConfigValidateResultFrame(
        id=UUID(request["id"]),
        ok=False,
        failures=[
            McpValidationFailure(
                name="demo",
                stage="initialize",
                code="config_validation_failed",
                message="late",
            )
        ],
    )
    assert await registry.resolve_config_validate_result(handle, late)


async def test_blocked_validation_cancel_retires_in_background(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "openctopus_server.devices.registry.VALIDATION_CANCEL_TIMEOUT_SECONDS",
        0.01,
    )
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = EverySendBlockingTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(
            registry.validate_config(
                device_id=device_id,
                user_id=user_id,
                expected_device_name="laptop",
                base_config_revision=1,
                candidate_config=DeviceConfigFrame(
                    workspace_path="~/workspace",
                    restrict_to_workspace=True,
                    ssrf_denylist=[],
                ),
                validate_servers=("demo",),
                timeout_seconds=0.01,
            ),
            timeout=0.1,
        )
    await asyncio.wait_for(transport.second_send_started.wait(), timeout=1)
    for _ in range(100):
        if not await registry.is_online(device_id, user_id=user_id):
            break
        await asyncio.sleep(0.01)
    assert not await registry.is_online(device_id, user_id=user_id)


async def test_cancelled_validation_does_not_wait_for_the_cancel_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "openctopus_server.devices.registry.VALIDATION_CANCEL_TIMEOUT_SECONDS",
        0.01,
    )
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = EverySendBlockingTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    validation = asyncio.create_task(
        registry.validate_config(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="laptop",
            base_config_revision=1,
            candidate_config=DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            ),
            validate_servers=("demo",),
        )
    )
    await asyncio.wait_for(transport.first_send_started.wait(), timeout=1)

    validation.cancel()
    await asyncio.wait_for(transport.second_send_started.wait(), timeout=1)
    await asyncio.sleep(0.05)
    cancellation_propagated = validation.done()
    retired_in_background = not await registry.is_online(device_id, user_id=user_id)
    transport.release_second_send.set()
    with pytest.raises(asyncio.CancelledError):
        await validation

    assert cancellation_propagated
    assert retired_in_background


async def test_config_validation_wire_reveals_secret_only_to_the_wss_device() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
        secret_transport_safe=True,
    )
    candidate = DeviceConfigFrame(
        workspace_path="~/workspace",
        restrict_to_workspace=True,
        ssrf_denylist=[],
        mcp_servers=[
            StdioMcpServerConfig(
                name="demo",
                transport="stdio",
                command="mcp-demo",
                env={"TOKEN": "actual-secret"},
            )
        ],
    )
    validation = asyncio.create_task(
        registry.validate_config(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="laptop",
            base_config_revision=1,
            candidate_config=candidate,
            validate_servers=("demo",),
            timeout_seconds=1,
        )
    )
    request = await _wait_for_sent(transport)

    candidate_payload = cast(dict[str, object], request["candidate_config"])
    servers = cast(list[dict[str, object]], candidate_payload["mcp_servers"])
    assert servers[0]["env"] == {"TOKEN": "actual-secret"}
    assert "actual-secret" not in candidate.model_dump_json()
    result = ConfigValidateResultFrame(
        id=UUID(str(request["id"])),
        ok=True,
        source_catalog=SourceMcpCatalog(
            version=1,
            servers=[SourceMcpServerCatalog(name="demo")],
        ),
        failures=[],
    )
    assert await registry.resolve_config_validate_result(handle, result)
    assert (await validation).source_catalog == result.source_catalog


@pytest.mark.parametrize(
    "server",
    [
        StdioMcpServerConfig(
            name="demo",
            transport="stdio",
            command="mcp-demo",
            env={"TOKEN": ""},
        ),
        StreamableHttpMcpServerConfig(
            name="demo",
            transport="streamable_http",
            url="https://mcp.example.test",
            headers={"Authorization": ""},
        ),
    ],
)
async def test_empty_mcp_secret_values_do_not_require_secure_device_transport(
    server: StdioMcpServerConfig | StreamableHttpMcpServerConfig,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    validation = asyncio.create_task(
        registry.validate_config(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="laptop",
            base_config_revision=1,
            candidate_config=DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
                mcp_servers=[server],
            ),
            validate_servers=("demo",),
            timeout_seconds=1,
        )
    )
    request = await _wait_for_sent(transport)
    result = ConfigValidateResultFrame(
        id=UUID(str(request["id"])),
        ok=True,
        source_catalog=SourceMcpCatalog(
            version=1,
            servers=[SourceMcpServerCatalog(name="demo")],
        ),
        failures=[],
    )

    assert await registry.resolve_config_validate_result(handle, result)
    assert (await validation).source_catalog == result.source_catalog


async def test_tombstone_capacity_retires_instead_of_evicting_unknown_ids() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    assert handle is not None
    for _ in range(65):
        with pytest.raises(TimeoutError):
            await registry.validate_config(
                device_id=device_id,
                user_id=user_id,
                expected_device_name="laptop",
                base_config_revision=1,
                candidate_config=DeviceConfigFrame(
                    workspace_path="~/workspace",
                    restrict_to_workspace=True,
                    ssrf_denylist=[],
                ),
                validate_servers=("demo",),
                timeout_seconds=0,
            )
    for _ in range(100):
        if not await registry.is_online(device_id, user_id=user_id):
            break
        await asyncio.sleep(0)
    assert not await registry.is_online(device_id, user_id=user_id)


async def test_tool_late_result_consumes_tombstone_and_releases_admission() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    with pytest.raises(DeviceOutcomeUnknownError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=1024,
            timeout_seconds=0,
        )
    assert registry.pending_count == 0
    call = json.loads(transport.sent_text[0])
    late = ToolResultFrame(id=UUID(call["id"]), content="late", is_error=False)
    assert await registry.resolve_tool_result(handle, late)
    assert not await registry.resolve_tool_result(handle, late)


async def test_tool_tombstone_capacity_retires_the_generation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    for _ in range(257):
        with pytest.raises(DeviceOutcomeUnknownError):
            await registry.dispatch_tool(
                device_id=device_id,
                user_id=user_id,
                name="list_dir",
                args={},
                max_result_bytes=1024,
                timeout_seconds=0,
            )
    for _ in range(100):
        if not await registry.is_online(device_id, user_id=user_id):
            break
        await asyncio.sleep(0)
    assert not await registry.is_online(device_id, user_id=user_id)


async def test_close_releases_a_registration_waiting_on_publication_fence() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    assert await registry.begin_config_update(
        device_id=device_id,
        user_id=user_id,
        expected_handle=handle,
    )
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.sleep(0)
    assert not replacement.done()

    await asyncio.wait_for(registry.close(), timeout=1)
    assert await asyncio.wait_for(replacement, timeout=1) is None


async def test_cancelled_publication_lookup_releases_the_global_lock() -> None:
    registry = DeviceRegistry()
    await registry._lock.acquire()
    registration = asyncio.create_task(
        registry.register(
            device_id=uuid4(),
            user_id=uuid4(),
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    for _ in range(100):
        if registry._register_lock.locked():
            break
        await asyncio.sleep(0)
    assert registry._register_lock.locked()

    registration.cancel()
    with pytest.raises(asyncio.CancelledError):
        await registration
    leaked = registry._register_lock.locked()
    registry._lock.release()
    if leaked:
        registry._register_lock.release()

    assert leaked is False
    assert await asyncio.wait_for(
        registry.register(
            device_id=uuid4(),
            user_id=uuid4(),
            device_name="desktop",
            transport=FakeTransport(),
        ),
        timeout=1,
    ) is not None


async def test_live_hello_metadata_follows_the_current_connection_generation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_shells = ShellMetadata(default="bash", available=["bash", "sh"])
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
        operating_system="linux",
        shells=old_shells,
    )

    metadata = await registry.get_live_metadata(device_id, user_id=user_id)
    assert metadata is not None
    assert metadata.os == "linux"
    assert metadata.default_shell == "bash"
    assert metadata.available_shells == ("bash", "sh")

    new_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
        operating_system="darwin",
        shells=ShellMetadata(default="zsh", available=["zsh", "bash", "sh"]),
    )
    assert new_handle.generation > old_handle.generation
    metadata = await registry.get_live_metadata(device_id, user_id=user_id)
    assert metadata is not None
    assert metadata.os == "darwin"
    assert metadata.default_shell == "zsh"
    assert metadata.available_shells == ("zsh", "bash", "sh")

    assert await registry.unregister(new_handle) is True
    assert await registry.get_live_metadata(device_id, user_id=user_id) is None


async def test_replacement_waits_for_an_admitted_tool_call_send_boundary() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = BlockingTransport()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="write_file",
            args={"path": "a.txt", "content": "x"},
            max_result_bytes=1024,
            timeout_seconds=10,
        )
    )
    await asyncio.wait_for(old_transport.send_started.wait(), timeout=1)

    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.sleep(0)
    assert replacement.done() is False

    old_transport.release_send.set()
    await replacement
    with pytest.raises(DeviceOutcomeUnknownError):
        await pending


async def test_revocation_epoch_rejects_a_stale_handshake_after_token_rotation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    epoch = await registry.registration_epoch(device_id)

    assert await registry.revoke(device_id) is False
    assert (
        await registry.register(
            device_id=device_id,
            user_id=uuid4(),
            device_name="laptop",
            transport=FakeTransport(),
            expected_revocation_epoch=epoch,
        )
        is None
    )


async def test_config_push_updates_the_current_connection_name() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="old-name",
        transport=transport,
    )
    assert handle is not None

    payload = await _push_and_apply(
        registry,
        handle,
        transport,
        device_id=device_id,
        user_id=user_id,
        device_name="new-name",
        config=DeviceConfigFrame(
            workspace_path="~/workspace",
            restrict_to_workspace=True,
            ssrf_denylist=[],
        ),
    )
    assert payload["type"] == "config_update"
    assert payload["device_name"] == "new-name"
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="old-name",
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    assert len(transport.sent_text) == 2


async def test_private_dispatch_rejects_a_changed_config_snapshot() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    assert handle is not None
    route = await registry.get_route_snapshot(
        device_id,
        user_id=user_id,
        expected_device_name="laptop",
    )
    assert route is not None

    await _push_and_apply(
        registry,
        handle,
        transport,
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        config=DeviceConfigFrame(
            workspace_path="~/different-workspace",
            restrict_to_workspace=True,
            ssrf_denylist=[],
        ),
    )
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool_on_snapshot(
            route=route,
            user_id=user_id,
            expected_device_name="laptop",
            name="delete_file",
            args={"path": "source.txt"},
            max_result_bytes=1024,
            timeout_seconds=1,
        )
    assert [json.loads(payload)["type"] for payload in transport.sent_text] == [
        "config_update",
        "config_applied_ack",
    ]


async def test_bridge_route_pair_captures_both_current_routes_atomically() -> None:
    registry = DeviceRegistry()
    user_id = uuid4()
    source_id = uuid4()
    destination_id = uuid4()
    source_handle = await registry.register(
        device_id=source_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    destination_handle = await registry.register(
        device_id=destination_id,
        user_id=user_id,
        device_name="phone",
        transport=FakeTransport(),
    )
    assert source_handle is not None
    assert destination_handle is not None

    pair = await registry.get_bridge_route_pair(
        user_id=user_id,
        source_device_id=source_id,
        source_device_name="laptop",
        destination_device_id=destination_id,
        destination_device_name="phone",
    )

    assert pair == BridgeRoutePair(
        source=DeviceRouteSnapshot(source_handle, 0, "laptop"),
        destination=DeviceRouteSnapshot(destination_handle, 0, "phone"),
    )
    assert await registry.bridge_routes_current(
        pair.source,
        pair.destination,
        user_id=user_id,
    )
    assert not await registry.bridge_routes_current(
        pair.source,
        pair.source,
        user_id=user_id,
    )
    assert not await registry.bridge_routes_current(
        pair.source,
        DeviceRouteSnapshot(destination_handle, 1, "phone"),
        user_id=user_id,
    )


async def test_bridge_route_pair_fails_closed_for_identity_or_route_drift() -> None:
    registry = DeviceRegistry()
    user_id = uuid4()
    source_id = uuid4()
    destination_id = uuid4()
    source_handle = await registry.register(
        device_id=source_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    destination_handle = await registry.register(
        device_id=destination_id,
        user_id=user_id,
        device_name="phone",
        transport=FakeTransport(),
    )
    assert source_handle is not None
    assert destination_handle is not None
    pair = BridgeRoutePair(
        source=DeviceRouteSnapshot(source_handle, 0, "laptop"),
        destination=DeviceRouteSnapshot(destination_handle, 0, "phone"),
    )

    assert (
        await registry.get_bridge_route_pair(
            user_id=uuid4(),
            source_device_id=source_id,
            source_device_name="laptop",
            destination_device_id=destination_id,
            destination_device_name="phone",
        )
        is None
    )
    assert (
        await registry.get_bridge_route_pair(
            user_id=user_id,
            source_device_id=source_id,
            source_device_name="renamed",
            destination_device_id=destination_id,
            destination_device_name="phone",
        )
        is None
    )
    assert (
        await registry.get_bridge_route_pair(
            user_id=user_id,
            source_device_id=source_id,
            source_device_name="laptop",
            destination_device_id=source_id,
            destination_device_name="laptop",
        )
        is None
    )

    assert await registry.begin_config_update(
        device_id=destination_id,
        user_id=user_id,
    )
    try:
        assert (
            await registry.get_bridge_route_pair(
                user_id=user_id,
                source_device_id=source_id,
                source_device_name="laptop",
                destination_device_id=destination_id,
                destination_device_name="phone",
            )
            is None
        )
        assert not await registry.bridge_routes_current(
            pair.source,
            pair.destination,
            user_id=user_id,
        )
    finally:
        await registry.abort_config_update(
            device_id=destination_id,
            user_id=user_id,
        )

    assert await registry.unregister(destination_handle)
    assert (
        await registry.get_bridge_route_pair(
            user_id=user_id,
            source_device_id=source_id,
            source_device_name="laptop",
            destination_device_id=destination_id,
            destination_device_name="phone",
        )
        is None
    )


async def test_config_push_send_failure_marks_the_device_offline() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FailingTransport(),
    )

    assert (
        await registry.push_config(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            config=DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            ),
        )
        is False
    )
    assert await registry.is_online(device_id, user_id=user_id) is False


@pytest.mark.parametrize("deliver_before_block", [False, True])
async def test_cancelled_config_push_retires_the_ambiguous_generation(
    deliver_before_block: bool,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = AmbiguousConfigTransport(deliver_before_block=deliver_before_block)
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="old-name",
        transport=transport,
    )
    update = asyncio.create_task(
        registry.push_config(
            device_id=device_id,
            user_id=user_id,
            device_name="new-name",
            config=DeviceConfigFrame(
                workspace_path="~/new-workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            ),
        )
    )
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)

    update.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(update, timeout=1)

    assert await registry.is_online(device_id, user_id=user_id) is False
    assert await registry.send_text(handle, "late") is False


async def test_config_update_precedes_new_name_dispatch_and_blocks_stale_name() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = BlockingTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="old-name",
        transport=transport,
    )
    assert handle is not None
    config_task = asyncio.create_task(
        registry.push_config(
            device_id=device_id,
            user_id=user_id,
            device_name="new-name",
            config=DeviceConfigFrame(
                workspace_path="~/workspace",
                restrict_to_workspace=True,
                ssrf_denylist=[],
            ),
        )
    )
    await asyncio.wait_for(transport.send_started.wait(), timeout=1)

    new_name_dispatch = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="new-name",
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    with pytest.raises(DeviceUnavailableError):
        await asyncio.wait_for(new_name_dispatch, timeout=0.02)
    stale = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            expected_device_name="old-name",
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    transport.release_send.set()
    for _ in range(100):
        if transport.sent_text:
            break
        await asyncio.sleep(0)
    update = json.loads(transport.sent_text[0])
    assert await registry.resolve_config_applied(
        handle,
        ConfigAppliedFrame(
            id=UUID(update["id"]),
            config_revision=update["config_revision"],
        ),
    )
    assert await config_task is True
    with pytest.raises(DeviceUnavailableError):
        await stale
    assert [json.loads(payload)["type"] for payload in transport.sent_text] == [
        "config_update",
        "config_applied_ack",
    ]


async def test_revoke_serializes_with_replacement_and_revokes_the_current_generation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = BlockingTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    epoch = await registry.registration_epoch(device_id)
    in_flight = asyncio.create_task(registry.send_text(old_handle, "in-flight"))
    await asyncio.wait_for(old_transport.send_started.wait(), timeout=1)
    new_transport = FakeTransport()
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=new_transport,
            expected_revocation_epoch=epoch,
        )
    )
    await asyncio.sleep(0)
    assert replacement.done() is False
    revocation = asyncio.create_task(registry.revoke(device_id))
    old_transport.release_send.set()

    assert await in_flight is True
    assert await replacement is not None
    assert await revocation is True
    assert await registry.is_online(device_id, user_id=user_id) is False
    assert new_transport.closes == [(4401, '{"code":"unauthorized"}')]


async def test_cancelled_replacement_retires_its_published_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    retirement_started = asyncio.Event()
    release_retirement = asyncio.Event()
    original_retire = registry._retire

    async def blocked_retire(connection: object, **kwargs: object) -> None:
        if getattr(connection, "handle", None) == old_handle:
            retirement_started.set()
            await release_retirement.wait()
        await original_retire(connection, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(registry, "_retire", blocked_retire)
    replacement_transport = FakeTransport()
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=replacement_transport,
        )
    )
    await asyncio.wait_for(retirement_started.wait(), timeout=1)

    replacement.cancel()
    await asyncio.sleep(0)
    assert replacement.done() is False
    release_retirement.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(replacement, timeout=1)

    assert await registry.is_online(device_id, user_id=user_id) is False


async def test_remove_device_waits_for_every_retiring_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    retirement_started = asyncio.Event()
    release_retirement = asyncio.Event()
    original_retire = registry._retire

    async def blocked_retire(connection: object, **kwargs: object) -> None:
        if getattr(connection, "handle", None) == old_handle:
            retirement_started.set()
            await release_retirement.wait()
        await original_retire(connection, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(registry, "_retire", blocked_retire)
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.wait_for(retirement_started.wait(), timeout=1)

    removal = asyncio.create_task(registry.remove_device(device_id))
    await asyncio.sleep(0)
    assert removal.done() is False

    release_retirement.set()
    assert await replacement is not None
    assert await removal is True
    assert await registry.is_online(device_id, user_id=user_id) is False


async def test_replacement_waits_for_an_in_flight_binary_transfer_frame() -> None:
    @dataclass
    class BlockingBinaryTransport(FakeTransport):
        binary_started: asyncio.Event = field(default_factory=asyncio.Event)
        release_binary: asyncio.Event = field(default_factory=asyncio.Event)
        sent_binary: list[bytes] = field(default_factory=list)

        async def send_binary(self, payload: bytes) -> None:
            self.binary_started.set()
            await self.release_binary.wait()
            self.sent_binary.append(payload)

    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_transport = BlockingBinaryTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=old_transport,
    )
    payload = new_uuid7().bytes + b"chunk"
    send = asyncio.create_task(registry.send_binary(old_handle, payload))
    await asyncio.wait_for(old_transport.binary_started.wait(), timeout=1)

    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.sleep(0)
    assert replacement.done() is False

    old_transport.release_binary.set()
    assert await send is True
    assert await replacement is not None
    assert old_transport.sent_binary == [payload]


async def test_stale_generation_terminal_frame_cannot_commit_during_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    sink = RecordingSink()
    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=old_handle,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=lambda _frame: _sink(sink),
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await registry.handle_transfer_frame(
        old_handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=0,
        ),
    )

    disconnect_started = asyncio.Event()
    release_disconnect = asyncio.Event()
    original_disconnect = registry.transfers.disconnect

    async def delayed_disconnect(handle: object) -> None:
        if handle == old_handle:
            disconnect_started.set()
            await release_disconnect.wait()
        await original_disconnect(handle)

    monkeypatch.setattr(registry.transfers, "disconnect", delayed_disconnect)
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.wait_for(disconnect_started.wait(), timeout=1)
    assert await registry.get_handle(device_id, user_id=user_id) != old_handle

    digest = hashlib.sha256(b"").hexdigest()
    assert (
        await registry.handle_transfer_frame(
            old_handle,
            TransferEndFrame(
                id=slot_id,
                ack=False,
                ok=True,
                bytes_sent=0,
                sha256=digest,
            ),
        )
        is False
    )
    assert sink.finished is False

    release_disconnect.set()
    await replacement
    with pytest.raises(Exception):
        await transfer


async def test_terminal_past_registry_check_cannot_commit_after_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    assert old_handle is not None
    route = await registry.get_route_snapshot(
        device_id,
        user_id=user_id,
        expected_device_name="laptop",
    )
    assert route is not None
    sink = RecordingSink()
    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=old_handle,
            route=route,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=lambda _frame: _sink(sink),
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await registry.handle_transfer_frame(
        old_handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=0,
        ),
    )
    while not any(
        json.loads(payload)["type"] == "transfer_ready" for payload in transport.sent_text
    ):
        await asyncio.sleep(0)

    frame_entered = asyncio.Event()
    release_frame = asyncio.Event()
    original_handle_frame = registry.transfers.handle_frame

    async def blocked_handle_frame(handle: object, frame: object) -> None:
        if isinstance(frame, TransferEndFrame) and frame.ok and not frame.ack:
            frame_entered.set()
            await release_frame.wait()
        await original_handle_frame(handle, frame)

    monkeypatch.setattr(registry.transfers, "handle_frame", blocked_handle_frame)
    terminal = asyncio.create_task(
        registry.handle_transfer_frame(
            old_handle,
            TransferEndFrame(
                id=slot_id,
                ack=False,
                ok=True,
                bytes_sent=0,
                sha256=hashlib.sha256(b"").hexdigest(),
            ),
        )
    )
    await frame_entered.wait()

    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    while await registry.get_handle(device_id, user_id=user_id) == old_handle:
        await asyncio.sleep(0)
    release_frame.set()

    await asyncio.gather(terminal, return_exceptions=True)
    assert await replacement is not None
    with pytest.raises(Exception):
        await transfer
    assert sink.finished is False


async def test_config_push_preserves_active_transfer_and_rejects_stale_new_route() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    assert handle is not None
    route = await registry.get_route_snapshot(
        device_id,
        user_id=user_id,
        expected_device_name="laptop",
    )
    assert route is not None
    sink = RecordingSink()
    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=handle,
            route=route,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=lambda _frame: _sink(sink),
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await registry.handle_transfer_frame(
        handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=0,
        ),
    )
    while not any(
        json.loads(payload)["type"] == "transfer_ready" for payload in transport.sent_text
    ):
        await asyncio.sleep(0)

    await _push_and_apply(
        registry,
        handle,
        transport,
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        config=DeviceConfigFrame(
            workspace_path="~/different-workspace",
            restrict_to_workspace=True,
            ssrf_denylist=[],
        ),
    )

    await registry.handle_transfer_frame(
        handle,
        TransferEndFrame(
            id=slot_id,
            ack=False,
            ok=True,
            bytes_sent=0,
            sha256=hashlib.sha256(b"").hexdigest(),
        ),
    )
    result = await transfer
    assert result.bytes_transferred == 0
    assert sink.finished is True

    with pytest.raises(TransferUnavailableError):
        await registry.transfers.start_client_to_server(
            handle=handle,
            route=route,
            user_id=user_id,
            src_path="stale-source.txt",
            dst_path="stale-destination.txt",
            sink_factory=lambda _frame: _sink(RecordingSink()),
        )
    current_route = await registry.get_route_snapshot(
        device_id,
        user_id=user_id,
        expected_device_name="laptop",
    )
    assert current_route is not None
    assert current_route.config_epoch == route.config_epoch + 1


async def test_config_push_fences_an_unissued_transfer_preflight() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    assert handle is not None
    route = await registry.get_route_snapshot(
        device_id,
        user_id=user_id,
        expected_device_name="laptop",
    )
    assert route is not None
    source_factory_started = asyncio.Event()

    class WaitingSource:
        size = 0
        etag: str | None = None

        async def read(self) -> bytes:
            return b""

        async def aclose(self) -> None:
            return None

    async def source_factory() -> WaitingSource:
        source_factory_started.set()
        await asyncio.Event().wait()
        return WaitingSource()

    transfer = asyncio.create_task(
        registry.transfers.start_server_to_client(
            handle=handle,
            route=route,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            source_factory=source_factory,
            total_bytes=0,
        )
    )
    await source_factory_started.wait()

    await _push_and_apply(
        registry,
        handle,
        transport,
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        config=DeviceConfigFrame(
            workspace_path="~/different-workspace",
            restrict_to_workspace=True,
            ssrf_denylist=[],
        ),
    )
    with pytest.raises(TransferUnavailableError):
        await asyncio.wait_for(transfer, timeout=0.2)


async def test_stale_generation_binary_frame_cannot_write_during_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    sink = RecordingSink()
    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=old_handle,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=lambda _frame: _sink(sink),
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await registry.handle_transfer_frame(
        old_handle,
        TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=1,
        ),
    )

    disconnect_started = asyncio.Event()
    release_disconnect = asyncio.Event()
    original_disconnect = registry.transfers.disconnect

    async def delayed_disconnect(handle: object) -> None:
        if handle == old_handle:
            disconnect_started.set()
            await release_disconnect.wait()
        await original_disconnect(handle)

    monkeypatch.setattr(registry.transfers, "disconnect", delayed_disconnect)
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.wait_for(disconnect_started.wait(), timeout=1)
    assert await registry.get_handle(device_id, user_id=user_id) != old_handle

    assert await registry.handle_transfer_binary(old_handle, slot_id.bytes + b"x") is False
    assert sink.chunks == []

    release_disconnect.set()
    await replacement
    with pytest.raises(Exception):
        await transfer


async def test_blocked_sink_preparation_does_not_hold_registry_lifecycle_lock() -> None:
    registry = DeviceRegistry(transfer_idle_timeout_seconds=1.0)
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    preparation_started = asyncio.Event()
    preparation_cancelled = asyncio.Event()

    async def make_sink(_: TransferBeginFrame) -> RecordingSink:
        preparation_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            preparation_cancelled.set()
            raise
        raise AssertionError("sink preparation should remain blocked")

    transfer = asyncio.create_task(
        registry.transfers.start_client_to_server(
            handle=handle,
            user_id=user_id,
            src_path="source.txt",
            dst_path="destination.txt",
            sink_factory=make_sink,
        )
    )
    request = await _wait_for_sent(transport)
    slot_id = UUID(str(request["id"]))
    await asyncio.wait_for(
        registry.handle_transfer_frame(
            handle,
            TransferBeginFrame(
                id=slot_id,
                direction="client_to_server",
                purpose="file_transfer",
                src_path="source.txt",
                dst_path="destination.txt",
                total_bytes=0,
            ),
        ),
        timeout=0.2,
    )
    await preparation_started.wait()

    assert await asyncio.wait_for(registry.unregister(handle), timeout=0.2) is True
    await preparation_cancelled.wait()
    with pytest.raises(Exception):
        await transfer
    assert await registry.is_online(device_id, user_id=user_id) is False
    assert registry.transfers.active_slots == 0
    assert registry.transfers._admission.active_count == 0


async def _sink(sink: RecordingSink) -> RecordingSink:
    return sink


async def test_close_does_not_wait_for_inbound_transfer_backpressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    inbound_started = asyncio.Event()
    release_inbound = asyncio.Event()

    async def blocked_handle_frame(_handle: object, _frame: object) -> None:
        inbound_started.set()
        await release_inbound.wait()

    monkeypatch.setattr(registry.transfers, "handle_frame", blocked_handle_frame)
    inbound = asyncio.create_task(registry.handle_transfer_frame(handle, object()))
    await asyncio.wait_for(inbound_started.wait(), timeout=1)

    shutdown = asyncio.create_task(registry.close())
    await asyncio.wait_for(shutdown, timeout=0.2)
    assert await registry.is_online(device_id, user_id=user_id) is False

    release_inbound.set()
    assert await inbound is False


async def test_timeout_releases_issued_call_admission_and_accepts_one_late_result() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    timed_out = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=0.01,
        )
    )
    payload = await _wait_for_sent(transport)

    with pytest.raises(DeviceOutcomeUnknownError):
        await timed_out
    assert registry.pending_count == 0
    late = ToolResultFrame(
        id=UUID(str(payload["id"])),
        content="late",
        is_error=False,
    )
    assert await registry.resolve_tool_result(handle, late) is True
    assert await registry.resolve_tool_result(handle, late) is False
    assert registry.pending_count == 0
    assert (
        await registry.resolve_tool_result(
            handle,
            late.model_copy(update={"id": new_uuid7()}),
        )
        is False
    )

    transport.sent.clear()
    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    await _wait_for_sent(transport)
    assert await registry.unregister(handle) is True
    with pytest.raises(DeviceOutcomeUnknownError):
        await pending
    assert registry.pending_count == 0


async def test_pending_admission_rejects_without_retaining_or_sending() -> None:
    registry = DeviceRegistry(
        pending_calls_max=1,
        pending_calls_max_per_user=1,
        pending_bytes_max=1_000_000,
        pending_bytes_max_per_user=1_000_000,
    )
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )
    first = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    first_payload = await _wait_for_sent(transport)

    with pytest.raises(DeviceBusyError):
        await registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    assert registry.pending_count == 1
    assert len(transport.sent_text) == 1

    assert await registry.resolve_tool_result(
        handle,
        ToolResultFrame(
            id=UUID(str(first_payload["id"])),
            content="done",
            is_error=False,
        ),
    )
    await first

    transport.sent.clear()
    second = asyncio.create_task(
        registry.dispatch_tool(
            device_id=device_id,
            user_id=user_id,
            name="list_dir",
            args={},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    second_payload = await _wait_for_sent(transport)
    assert await registry.resolve_tool_result(
        handle,
        ToolResultFrame(
            id=UUID(str(second_payload["id"])),
            content="done",
            is_error=False,
        ),
    )
    await second


async def test_pending_byte_admission_is_per_user_and_releases_after_disconnect() -> None:
    registry = DeviceRegistry(
        pending_calls_max=4,
        pending_calls_max_per_user=2,
        pending_bytes_max=100_000,
        pending_bytes_max_per_user=20_000,
    )
    first_user = uuid4()
    second_user = uuid4()
    first_device = uuid4()
    second_device = uuid4()
    first_transport = FakeTransport()
    second_transport = FakeTransport()
    first_handle = await registry.register(
        device_id=first_device,
        user_id=first_user,
        device_name="first",
        transport=first_transport,
    )
    second_handle = await registry.register(
        device_id=second_device,
        user_id=second_user,
        device_name="second",
        transport=second_transport,
    )

    first = asyncio.create_task(
        registry.dispatch_tool(
            device_id=first_device,
            user_id=first_user,
            name="read_file",
            args={"path": "x"},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    await _wait_for_sent(first_transport)

    with pytest.raises(DeviceBusyError):
        await registry.dispatch_tool(
            device_id=first_device,
            user_id=first_user,
            name="read_file",
            args={"path": "x"},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )

    second = asyncio.create_task(
        registry.dispatch_tool(
            device_id=second_device,
            user_id=second_user,
            name="read_file",
            args={"path": "x"},
            max_result_bytes=16_000,
            timeout_seconds=10,
        )
    )
    await _wait_for_sent(second_transport)

    assert await registry.unregister(first_handle) is True
    with pytest.raises(DeviceOutcomeUnknownError):
        await first

    await registry.unregister(second_handle)
    with pytest.raises(DeviceOutcomeUnknownError):
        await second
    assert registry.pending_count == 0


async def test_revoke_closes_current_and_allows_later_generation() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    transport = FakeTransport()
    first = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=transport,
    )

    assert await registry.revoke(device_id) is True
    assert transport.closes == [(4401, '{"code":"unauthorized"}')]
    assert await registry.is_online(device_id, user_id=user_id) is False

    second = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    assert second.generation > first.generation


async def test_close_prevents_registration_after_shutdown() -> None:
    registry = DeviceRegistry()
    await registry.close()

    assert (
        await registry.register(
            device_id=uuid4(),
            user_id=uuid4(),
            device_name="laptop",
            transport=FakeTransport(),
        )
        is None
    )


async def test_close_serializes_with_registration_already_waiting() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    await registry._register_lock.acquire()
    registration = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.sleep(0)
    shutdown = asyncio.create_task(registry.close())
    await asyncio.sleep(0)

    registry._register_lock.release()
    await registration
    await shutdown

    assert await registry.is_online(device_id, user_id=user_id) is False


async def test_close_waits_for_a_replaced_generation_retirement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old_handle = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    retirement_started = asyncio.Event()
    release_retirement = asyncio.Event()
    current_retired = asyncio.Event()
    original_retire = registry._retire

    async def blocked_retire(connection: object, **kwargs: object) -> None:
        if getattr(connection, "handle", None) == old_handle:
            retirement_started.set()
            await release_retirement.wait()
        await original_retire(connection, **kwargs)  # type: ignore[arg-type]
        if getattr(connection, "handle", None) != old_handle:
            current_retired.set()

    monkeypatch.setattr(registry, "_retire", blocked_retire)
    replacement = asyncio.create_task(
        registry.register(
            device_id=device_id,
            user_id=user_id,
            device_name="laptop",
            transport=FakeTransport(),
        )
    )
    await asyncio.wait_for(retirement_started.wait(), timeout=1)

    shutdown = asyncio.create_task(registry.close())
    await asyncio.wait_for(current_retired.wait(), timeout=1)
    assert shutdown.done() is False

    release_retirement.set()
    await asyncio.wait_for(shutdown, timeout=1)
    assert await replacement is not None


async def test_pong_is_generation_scoped() -> None:
    registry = DeviceRegistry()
    device_id = uuid4()
    user_id = uuid4()
    old = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    current = await registry.register(
        device_id=device_id,
        user_id=user_id,
        device_name="laptop",
        transport=FakeTransport(),
    )
    ping_id = uuid4()
    assert await registry.send_ping(current, ping_id, "ping") is True

    assert await registry.mark_pong(old, ping_id, at=10.0) is False
    assert await registry.mark_pong(current, uuid4(), at=20.0) is False
    assert await registry.mark_pong(current, ping_id, at=20.0) is True
    assert await registry.last_pong(current) == 20.0


def test_connection_handle_is_immutable() -> None:
    handle = ConnectionHandle(device_id=uuid4(), generation=1)
    with pytest.raises((AttributeError, TypeError)):
        handle.generation = 2  # type: ignore[misc]
