from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from openctopus_server.api import device_ws
from openctopus_server.devices.mcp_catalog import build_persisted_catalog
from openctopus_server.devices.mcp_models import (
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SourceMcpTool,
    StdioMcpServerConfig,
)
from openctopus_server.devices.mcp_routes import FrozenMcpEntryRoute
from openctopus_server.devices.protocol import (
    ConfigAppliedFrame,
    DeviceCapabilities,
    DeviceConfigFrame,
    HelloFrame,
    PongFrame,
    ReadyMcpRuntimeSnapshot,
    RegisterMcpFrame,
    RuntimeMcpSourceCatalog,
    ShellMetadata,
    new_uuid7,
)
from openctopus_server.devices.registry import (
    ConnectionHandle,
    DeviceMcpUnavailableError,
    DeviceOutcomeUnknownError,
    DeviceRegistry,
    DeviceTransport,
    DeviceUnavailableError,
)
from openctopus_server.services.devices import DEFAULT_ENV_ALLOWLIST, DeviceSnapshot


@dataclass
class _FakeWebSocket:
    headers: dict[str, str]
    incoming: list[dict[str, object]]
    scope: dict[str, object] = field(default_factory=lambda: {"scheme": "wss"})
    accepted: bool = False
    sent: list[str] = field(default_factory=list)
    closes: list[tuple[int, str]] = field(default_factory=list)
    disconnect: asyncio.Event | None = None

    async def accept(self) -> None:
        self.accepted = True

    async def receive(self) -> dict[str, object]:
        while not self.incoming and self.disconnect is not None and not self.disconnect.is_set():
            await asyncio.sleep(0)
        if self.incoming:
            return self.incoming.pop(0)
        return {"type": "websocket.disconnect", "code": 1000}

    async def send_text(self, payload: str) -> None:
        self.sent.append(payload)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closes.append((code, reason))


@dataclass
class _BlockingSendWebSocket(_FakeWebSocket):
    send_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_send: asyncio.Event = field(default_factory=asyncio.Event)

    async def send_text(self, payload: str) -> None:
        self.send_started.set()
        await self.release_send.wait()
        self.sent.append(payload)


def _snapshot() -> DeviceSnapshot:
    return DeviceSnapshot(
        id=uuid4(),
        user_id=uuid4(),
        name="laptop",
        token_hint="openoctopus_dev_...token",
        workspace_path="~/workspace",
        restrict_to_workspace=True,
        ssrf_denylist=["127.0.0.0/8"],
        created_at=datetime.now(UTC),
    )


def _mcp_snapshot_and_register() -> tuple[DeviceSnapshot, RegisterMcpFrame]:
    config = StdioMcpServerConfig(
        name="demo",
        transport="stdio",
        command="mcp-demo",
    )
    tool = SourceMcpTool(
        raw_name="search",
        description="Search",
        input_schema={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        },
    )
    source = SourceMcpCatalog(
        version=1,
        servers=[SourceMcpServerCatalog(name="demo", tools=[tool])],
    )
    catalog = build_persisted_catalog(
        [config],
        source,
        entry_id_factory=new_uuid7,
    )
    snapshot = replace(
        _snapshot(),
        config_revision=7,
        mcp_servers=[config.storage_dict()],
        mcp_catalog=catalog.model_dump(mode="json"),
    )
    frame = RegisterMcpFrame(
        id=new_uuid7(),
        config_revision=7,
        catalog_digest=catalog.digest,
        servers=[
            ReadyMcpRuntimeSnapshot(
                name="demo",
                runtime_generation=new_uuid7(),
                state="ready",
                code=None,
                source_catalog=RuntimeMcpSourceCatalog(tools=[tool]),
            )
        ],
    )
    return snapshot, frame


def _hello(*, version: str = "3") -> str:
    return json.dumps(
        {
            **HelloFrame(
                id=new_uuid7(),
                version="3",
                client_version="0.1.0",
                os="linux",
                caps=DeviceCapabilities(),
                shells=ShellMetadata(default="bash", available=["bash", "sh"]),
            ).model_dump(mode="json"),
            "version": version,
        }
    )


def _config_applied(hello: str, *, revision: int = 1) -> str:
    return ConfigAppliedFrame(
        id=UUID(json.loads(hello)["id"]),
        config_revision=revision,
    ).model_dump_json()


async def test_invalid_device_token_is_accepted_then_closed_unauthorized(
    monkeypatch: Any,
) -> None:
    websocket = _FakeWebSocket(headers={"authorization": "Bearer bad"}, incoming=[])
    lookup = AsyncMock(return_value=None)
    monkeypatch.setattr(device_ws, "_find_device_by_token", lookup)
    engine = object()

    await device_ws.serve_device_socket(websocket, DeviceRegistry(), engine)  # type: ignore[arg-type]

    assert websocket.accepted is True
    assert websocket.closes == [(4401, '{"code":"unauthorized"}')]
    lookup.assert_awaited_once_with(engine, "bad")


async def test_handshake_rechecks_token_registers_and_acks_only_active_config(
    monkeypatch: Any,
) -> None:
    snapshot = _snapshot()
    hello = _hello()
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[
            {"type": "websocket.receive", "text": hello},
            {"type": "websocket.receive", "text": _config_applied(hello)},
        ],
    )
    lookup = AsyncMock(side_effect=[snapshot, snapshot])
    monkeypatch.setattr(device_ws, "_find_device_by_token", lookup)
    registry = DeviceRegistry()
    engine = object()

    await device_ws.serve_device_socket(websocket, registry, engine)  # type: ignore[arg-type]

    assert lookup.await_args_list == [((engine, "token"),), ((engine, "token"),)]
    assert [json.loads(payload) for payload in websocket.sent] == [
        {
            "type": "hello_ack",
            "id": json.loads(hello)["id"],
            "device_name": "laptop",
            "config_revision": 1,
            "config": {
                "workspace_path": "~/workspace",
                "restrict_to_workspace": True,
                "ssrf_denylist": ["127.0.0.0/8"],
                "shell_timeout_max": 600,
                "env_allowlist": list(DEFAULT_ENV_ALLOWLIST),
                "mcp_servers": [],
            },
            "mcp_catalog": {
                "version": 1,
                "digest": "d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf",
                "servers": [],
            },
        },
        {
            "type": "config_applied_ack",
            "id": json.loads(hello)["id"],
            "config_revision": 1,
        }
    ]
    assert websocket.closes == [(1000, "")]
    assert await registry.is_online(snapshot.id, user_id=snapshot.user_id) is False


@pytest.mark.parametrize("secret", ["actual-secret", ""])
async def test_secret_bearing_hello_requires_exact_wss_scope_and_uses_wire_secret(
    monkeypatch: Any,
    secret: str,
) -> None:
    snapshot = replace(
        _snapshot(),
        mcp_servers=[
            {
                "name": "demo",
                "transport": "stdio",
                "command": "mcp-demo",
                "env": {"TOKEN": secret},
            }
        ],
    )
    hello = _hello()
    insecure = _FakeWebSocket(
        headers={
            "authorization": "Bearer token",
            "x-forwarded-proto": "https",
        },
        scope={"scheme": "ws"},
        incoming=[{"type": "websocket.receive", "text": hello}],
    )
    monkeypatch.setattr(device_ws, "_find_device_by_token", AsyncMock(return_value=snapshot))

    await device_ws.serve_device_socket(insecure, DeviceRegistry(), object())  # type: ignore[arg-type]

    assert insecure.sent == []
    assert insecure.closes == [(4403, '{"code":"mcp_secret_transport_insecure"}')]

    secure_hello = _hello()
    secure = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        scope={"scheme": "wss"},
        incoming=[
            {"type": "websocket.receive", "text": secure_hello},
            {
                "type": "websocket.receive",
                "text": _config_applied(secure_hello),
            },
        ],
    )
    await device_ws.serve_device_socket(secure, DeviceRegistry(), object())  # type: ignore[arg-type]

    hello_ack = json.loads(secure.sent[0])
    assert hello_ack["config"]["mcp_servers"][0]["env"] == {"TOKEN": secret}
    assert "<redacted>" not in secure.sent[0]


async def test_register_mcp_is_validated_and_acked_after_config_activation(
    monkeypatch: Any,
) -> None:
    snapshot, registration = _mcp_snapshot_and_register()
    hello = _hello()
    disconnect = asyncio.Event()
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[
            {"type": "websocket.receive", "text": hello},
            {
                "type": "websocket.receive",
                "text": _config_applied(hello, revision=7),
            },
        ],
        disconnect=disconnect,
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )

    serving = asyncio.create_task(
        device_ws.serve_device_socket(websocket, DeviceRegistry(), object())  # type: ignore[arg-type]
    )
    for _ in range(100):
        if any(json.loads(payload)["type"] == "config_applied_ack" for payload in websocket.sent):
            break
        await asyncio.sleep(0)
    websocket.incoming.append(
        {
            "type": "websocket.receive",
            "text": registration.model_dump_json(),
        }
    )
    for _ in range(100):
        if any(json.loads(payload)["type"] == "register_mcp_ack" for payload in websocket.sent):
            break
        await asyncio.sleep(0)
    disconnect.set()
    await asyncio.wait_for(serving, timeout=1)

    frames = [json.loads(payload) for payload in websocket.sent]
    assert [frame["type"] for frame in frames] == [
        "hello_ack",
        "config_applied_ack",
        "register_mcp_ack",
    ]
    assert frames[-1]["id"] == str(registration.id)
    assert frames[-1]["results"] == [
        {
            "name": "demo",
            "runtime_generation": str(registration.servers[0].runtime_generation),
            "accepted": True,
            "code": None,
        }
    ]


async def test_config_precommit_waits_for_register_mcp_ack_publication(
    monkeypatch: Any,
) -> None:
    snapshot, registration = _mcp_snapshot_and_register()
    websocket = _BlockingSendWebSocket(headers={}, incoming=[])
    transport = device_ws.WebSocketTransport(websocket)
    registry = DeviceRegistry()
    handle = await registry.register(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
        device_name=snapshot.name,
        transport=transport,
        config_revision=snapshot.config_revision,
        catalog_digest=registration.catalog_digest,
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )
    registering = asyncio.create_task(
        device_ws._register_mcp(
            registry=registry,
            handle=handle,
            transport=transport,
            engine=object(),  # type: ignore[arg-type]
            token="token",
            frame=registration,
        )
    )
    await asyncio.wait_for(websocket.send_started.wait(), timeout=1)
    precommit = asyncio.create_task(
        registry.begin_config_update(
            device_id=snapshot.id,
            user_id=snapshot.user_id,
        )
    )
    await asyncio.sleep(0)
    assert not precommit.done()
    websocket.release_send.set()
    assert await registering
    assert await asyncio.wait_for(precommit, timeout=1)
    await registry.abort_config_update(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
    )
    await registry.close()


async def test_register_mcp_does_not_wait_for_candidate_validation_lock(
    monkeypatch: Any,
) -> None:
    snapshot, registration = _mcp_snapshot_and_register()
    websocket = _FakeWebSocket(headers={}, incoming=[])
    transport = device_ws.WebSocketTransport(websocket)
    registry = DeviceRegistry()
    handle = await registry.register(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
        device_name=snapshot.name,
        transport=transport,
        config_revision=snapshot.config_revision,
        catalog_digest=registration.catalog_digest,
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )

    try:
        async with registry.config_update_lock(
            user_id=snapshot.user_id,
            device_name=snapshot.name,
            device_id=snapshot.id,
        ):
            assert await asyncio.wait_for(
                device_ws._register_mcp(
                    registry=registry,
                    handle=handle,
                    transport=transport,
                    engine=object(),  # type: ignore[arg-type]
                    token="token",
                    frame=registration,
                    timeout_seconds=0.1,
                ),
                timeout=0.2,
            )
        assert json.loads(websocket.sent[-1])["type"] == "register_mcp_ack"
    finally:
        await registry.close()


async def test_register_mcp_waits_for_precommit_abort_without_becoming_stale(
    monkeypatch: Any,
) -> None:
    snapshot, registration = _mcp_snapshot_and_register()
    websocket = _FakeWebSocket(headers={}, incoming=[])
    transport = device_ws.WebSocketTransport(websocket)
    registry = DeviceRegistry()
    handle = await registry.register(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
        device_name=snapshot.name,
        transport=transport,
        config_revision=snapshot.config_revision,
        catalog_digest=registration.catalog_digest,
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )
    assert await registry.begin_config_update(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
    )

    registration_task = asyncio.create_task(
        device_ws._register_mcp(
            registry=registry,
            handle=handle,
            transport=transport,
            engine=object(),  # type: ignore[arg-type]
            token="token",
            frame=registration,
            timeout_seconds=0.1,
        )
    )
    try:
        await asyncio.sleep(0)
        assert not registration_task.done()
        await registry.abort_config_update(
            device_id=snapshot.id,
            user_id=snapshot.user_id,
        )
        assert await registration_task
        acknowledgement = json.loads(websocket.sent[-1])
        assert acknowledgement["results"][0]["accepted"] is True
        assert acknowledgement["results"][0]["code"] is None
    finally:
        await registry.abort_config_update(
            device_id=snapshot.id,
            user_id=snapshot.user_id,
        )
        await registry.close()


async def test_register_mcp_has_one_end_to_end_server_deadline(monkeypatch: Any) -> None:
    snapshot, registration = _mcp_snapshot_and_register()
    websocket = _FakeWebSocket(headers={}, incoming=[])
    transport = device_ws.WebSocketTransport(websocket)
    registry = DeviceRegistry()
    handle = await registry.register(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
        device_name=snapshot.name,
        transport=transport,
        config_revision=snapshot.config_revision,
        catalog_digest=registration.catalog_digest,
    )

    async def stalled_lookup(_engine: object, _token: str) -> DeviceSnapshot:
        await asyncio.Future()
        raise AssertionError("unreachable")

    monkeypatch.setattr(device_ws, "_find_device_by_token", stalled_lookup)
    try:
        assert not await asyncio.wait_for(
            device_ws._register_mcp(
                registry=registry,
                handle=handle,
                transport=transport,
                engine=object(),  # type: ignore[arg-type]
                token="token",
                frame=registration,
                timeout_seconds=0.01,
            ),
            timeout=0.1,
        )
        assert websocket.sent == []
        assert websocket.closes == [(1013, '{"code":"io_error"}')]
    finally:
        await registry.close()


async def test_stale_register_mcp_is_rejected_on_wire_and_clears_bindings(
    monkeypatch: Any,
) -> None:
    snapshot, registration = _mcp_snapshot_and_register()
    websocket = _FakeWebSocket(headers={}, incoming=[])
    transport = device_ws.WebSocketTransport(websocket)
    registry = DeviceRegistry()
    handle = await registry.register(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
        device_name=snapshot.name,
        transport=transport,
        config_revision=snapshot.config_revision,
        catalog_digest=registration.catalog_digest,
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )
    stale = registration.model_copy(update={"config_revision": 6})

    try:
        assert await device_ws._register_mcp(
            registry=registry,
            handle=handle,
            transport=transport,
            engine=object(),  # type: ignore[arg-type]
            token="token",
            frame=registration,
            timeout_seconds=0.1,
        )
        assert await device_ws._register_mcp(
            registry=registry,
            handle=handle,
            transport=transport,
            engine=object(),  # type: ignore[arg-type]
            token="token",
            frame=stale,
            timeout_seconds=0.1,
        )
        acknowledgement = json.loads(websocket.sent[-1])
        assert acknowledgement["id"] == str(stale.id)
        assert acknowledgement["results"] == [
            {
                "name": "demo",
                "runtime_generation": str(stale.servers[0].runtime_generation),
                "accepted": False,
                "code": "mcp_registration_stale",
            }
        ]

        catalog = device_ws.devices.parse_stored_mcp_catalog(snapshot.mcp_catalog)
        entry = catalog.servers[0].entries[0]
        route = FrozenMcpEntryRoute(
            device_id=snapshot.id,
            device_name=snapshot.name,
            entry_id=entry.entry_id,
            config_revision=snapshot.config_revision,
            catalog_digest=catalog.digest,
            server=entry.server,
            surface=entry.surface,
            raw_name=entry.raw_name,
            invocation_identity=entry.invocation_identity,
            final_name=entry.final_name,
        )
        with pytest.raises(DeviceMcpUnavailableError):
            await registry.dispatch_mcp_tool(
                route=route,
                user_id=snapshot.user_id,
                name=entry.final_name,
                args={},
                max_result_bytes=1024,
                timeout_seconds=0.1,
            )
    finally:
        await registry.close()


async def test_registration_runner_coalesces_one_latest_follow_up(
    monkeypatch: Any,
) -> None:
    snapshot, first = _mcp_snapshot_and_register()
    second = first.model_copy(update={"id": new_uuid7()})
    latest = first.model_copy(update={"id": new_uuid7()})
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[UUID] = []

    async def register(**kwargs: Any) -> bool:
        frame = kwargs["frame"]
        assert isinstance(frame, RegisterMcpFrame)
        calls.append(frame.id)
        if len(calls) == 1:
            started.set()
            await release.wait()
        return True

    monkeypatch.setattr(device_ws, "_register_mcp", register)
    runner = device_ws._McpRegistrationRunner(
        registry=DeviceRegistry(),
        handle=ConnectionHandle(snapshot.id, 1),
        transport=device_ws.WebSocketTransport(_FakeWebSocket(headers={}, incoming=[])),
        engine=object(),  # type: ignore[arg-type]
        token="token",
        stop_event=None,
    )

    assert await runner.start(first)
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await runner.start(second)
    assert await runner.start(latest)
    release.set()
    for _ in range(100):
        if len(calls) == 2:
            break
        await asyncio.sleep(0)
    assert calls == [first.id, latest.id]
    assert await runner.poll()
    await runner.close()


async def test_reader_routes_pong_while_register_mcp_waits_on_authoritative_read(
    monkeypatch: Any,
) -> None:
    snapshot, registration = _mcp_snapshot_and_register()
    disconnect = asyncio.Event()
    websocket = _FakeWebSocket(
        headers={},
        incoming=[
            {
                "type": "websocket.receive",
                "text": registration.model_dump_json(),
            }
        ],
        disconnect=disconnect,
    )
    transport = device_ws.WebSocketTransport(websocket)
    registry = DeviceRegistry()
    handle = await registry.register(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
        device_name=snapshot.name,
        transport=transport,
        config_revision=snapshot.config_revision,
        catalog_digest=registration.catalog_digest,
    )
    ping_id = new_uuid7()
    before = await registry.last_pong(handle)
    assert before is not None
    assert await registry.send_ping(
        handle,
        ping_id,
        json.dumps({"type": "ping", "id": str(ping_id)}),
    )
    read_started = asyncio.Event()
    release_read = asyncio.Event()
    calls = 0

    async def lookup(_engine: object, _token: str) -> DeviceSnapshot:
        nonlocal calls
        calls += 1
        if calls == 1:
            read_started.set()
            await release_read.wait()
        return snapshot

    monkeypatch.setattr(device_ws, "_find_device_by_token", lookup)
    routing = asyncio.create_task(
        device_ws._route_frames(
            websocket,
            registry,
            handle,
            transport,
            engine=object(),  # type: ignore[arg-type]
            token="token",
        )
    )
    await asyncio.wait_for(read_started.wait(), timeout=1)
    websocket.incoming.append(
        {
            "type": "websocket.receive",
            "text": PongFrame(id=ping_id).model_dump_json(),
        }
    )
    for _ in range(100):
        after = await registry.last_pong(handle)
        if after is not None and after > before:
            break
        await asyncio.sleep(0)
    assert after is not None and after > before

    release_read.set()
    for _ in range(100):
        if any(json.loads(payload).get("type") == "register_mcp_ack" for payload in websocket.sent):
            break
        await asyncio.sleep(0)
    disconnect.set()
    await asyncio.wait_for(routing, timeout=1)
    await registry.close()


async def test_immediate_register_mcp_after_config_ack_observes_ready(
    monkeypatch: Any,
) -> None:
    snapshot, registration = _mcp_snapshot_and_register()
    hello = _hello()
    checked = asyncio.Event()
    disconnect = asyncio.Event()

    class ProbeRegistry(DeviceRegistry):
        observed_online: bool | None = None

        async def can_register_mcp(self, handle: ConnectionHandle) -> bool:
            allowed = await super().can_register_mcp(handle)
            self.observed_online = await self.is_online(
                handle.device_id,
                user_id=snapshot.user_id,
            )
            checked.set()
            return allowed

    class ImmediateRegistrationWebSocket(_FakeWebSocket):
        injected = False

        async def send_text(self, payload: str) -> None:
            self.sent.append(payload)
            if json.loads(payload)["type"] == "config_applied_ack" and not self.injected:
                self.injected = True
                self.incoming.append(
                    {
                        "type": "websocket.receive",
                        "text": registration.model_dump_json(),
                    }
                )
                await asyncio.wait_for(checked.wait(), timeout=1)

    websocket = ImmediateRegistrationWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[
            {"type": "websocket.receive", "text": hello},
            {
                "type": "websocket.receive",
                "text": _config_applied(hello, revision=7),
            },
        ],
        disconnect=disconnect,
    )
    registry = ProbeRegistry()
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )

    serving = asyncio.create_task(
        device_ws.serve_device_socket(websocket, registry, object())  # type: ignore[arg-type]
    )
    for _ in range(100):
        if any(json.loads(payload)["type"] == "register_mcp_ack" for payload in websocket.sent):
            break
        if serving.done():
            break
        await asyncio.sleep(0)
    disconnect.set()
    await asyncio.wait_for(serving, timeout=1)

    assert registry.observed_online is False
    assert [json.loads(payload)["type"] for payload in websocket.sent] == [
        "hello_ack",
        "config_applied_ack",
        "register_mcp_ack",
    ]
    assert all(close[0] != 1002 for close in websocket.closes)


async def test_handshake_generation_is_not_routable_before_hello_ack_is_written(
    monkeypatch: Any,
) -> None:
    snapshot = _snapshot()
    disconnect = asyncio.Event()
    websocket = _BlockingSendWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[],
        disconnect=disconnect,
    )
    hello = _hello()
    websocket.incoming.extend(
        [
            {"type": "websocket.receive", "text": hello},
        ]
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )
    registry = DeviceRegistry()
    serving = asyncio.create_task(
        device_ws.serve_device_socket(websocket, registry, object())  # type: ignore[arg-type]
    )
    await asyncio.wait_for(websocket.send_started.wait(), timeout=1)

    assert await registry.is_online(snapshot.id, user_id=snapshot.user_id) is False
    assert await registry.get_live_metadata(snapshot.id, user_id=snapshot.user_id) is None
    with pytest.raises(DeviceUnavailableError):
        await registry.dispatch_tool(
            device_id=snapshot.id,
            user_id=snapshot.user_id,
            name="list_dir",
            args={},
            max_result_bytes=1024,
            timeout_seconds=1,
        )

    websocket.release_send.set()
    for _ in range(100):
        if websocket.sent:
            break
        await asyncio.sleep(0)
    websocket.incoming.append(
        {
            "type": "websocket.receive",
            "text": _config_applied(hello),
        }
    )
    for _ in range(100):
        if await registry.is_online(snapshot.id, user_id=snapshot.user_id):
            break
        await asyncio.sleep(0)
    assert await registry.is_online(snapshot.id, user_id=snapshot.user_id) is True
    metadata = await registry.get_live_metadata(snapshot.id, user_id=snapshot.user_id)
    assert metadata is not None
    assert metadata.os == "linux"
    assert metadata.default_shell == "bash"
    assert metadata.available_shells == ("bash", "sh")
    disconnect.set()
    await asyncio.wait_for(serving, timeout=1)


async def test_config_patch_cannot_be_missed_during_handshake_registration(
    monkeypatch: Any,
) -> None:
    class _BlockingRegisterRegistry(DeviceRegistry):
        def __init__(self) -> None:
            super().__init__()
            self.register_started = asyncio.Event()
            self.release_register = asyncio.Event()

        async def register(
            self,
            *,
            device_id: Any,
            user_id: Any,
            device_name: str,
            transport: DeviceTransport,
            expected_revocation_epoch: int | None = None,
            ready: bool = True,
            operating_system: str | None = None,
            shells: ShellMetadata | None = None,
            secret_transport_safe: bool = False,
            config_revision: int = 1,
            catalog_digest: str = "d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf",
        ) -> ConnectionHandle | None:
            self.register_started.set()
            await self.release_register.wait()
            return await super().register(
                device_id=device_id,
                user_id=user_id,
                device_name=device_name,
                transport=transport,
                expected_revocation_epoch=expected_revocation_epoch,
                ready=ready,
                operating_system=operating_system,
                shells=shells,
                secret_transport_safe=secret_transport_safe,
                config_revision=config_revision,
                catalog_digest=catalog_digest,
            )

    snapshot = replace(_snapshot(), restrict_to_workspace=False)
    disconnect = asyncio.Event()
    hello = _hello()
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[
            {"type": "websocket.receive", "text": hello},
            {"type": "websocket.receive", "text": _config_applied(hello)},
        ],
        disconnect=disconnect,
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )
    registry = _BlockingRegisterRegistry()
    serve_task = asyncio.create_task(
        device_ws.serve_device_socket(websocket, registry, object())  # type: ignore[arg-type]
    )
    await asyncio.wait_for(registry.register_started.wait(), timeout=1)

    patch_acquired = asyncio.Event()

    async def patch_and_push() -> bool:
        async with registry.config_update_lock(
            user_id=snapshot.user_id,
            device_name=snapshot.name,
            device_id=snapshot.id,
        ):
            patch_acquired.set()
            assert await registry.begin_config_update(
                device_id=snapshot.id,
                user_id=snapshot.user_id,
            )
            return await registry.push_config(
                device_id=snapshot.id,
                user_id=snapshot.user_id,
                device_name=snapshot.name,
                config=DeviceConfigFrame(
                    workspace_path=snapshot.workspace_path,
                    restrict_to_workspace=True,
                    ssrf_denylist=snapshot.ssrf_denylist,
                ),
            )

    patch_task = asyncio.create_task(patch_and_push())
    await asyncio.sleep(0)
    assert patch_acquired.is_set() is False

    registry.release_register.set()
    for _ in range(100):
        updates = [
            json.loads(payload)
            for payload in websocket.sent
            if json.loads(payload)["type"] == "config_update"
        ]
        if updates:
            break
        await asyncio.sleep(0)
    assert updates
    websocket.incoming.append(
        {
            "type": "websocket.receive",
            "text": ConfigAppliedFrame(
                id=UUID(str(updates[0]["id"])),
                config_revision=updates[0]["config_revision"],
            ).model_dump_json(),
        }
    )
    assert await asyncio.wait_for(patch_task, timeout=1) is True
    disconnect.set()
    await asyncio.wait_for(serve_task, timeout=1)

    frames = [json.loads(payload) for payload in websocket.sent]
    assert [frame["type"] for frame in frames] == [
        "hello_ack",
        "config_applied_ack",
        "config_update",
        "config_applied_ack",
    ]
    assert frames[0]["config"]["restrict_to_workspace"] is False
    assert frames[2]["config"]["restrict_to_workspace"] is True


async def test_revoked_token_between_hello_and_registration_is_rejected(
    monkeypatch: Any,
) -> None:
    snapshot = _snapshot()
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[{"type": "websocket.receive", "text": _hello()}],
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(side_effect=[snapshot, None]),
    )

    await device_ws.serve_device_socket(websocket, DeviceRegistry(), object())  # type: ignore[arg-type]

    assert websocket.sent == []
    assert websocket.closes == [(4401, '{"code":"unauthorized"}')]


async def test_version_mismatch_closes_with_4409(monkeypatch: Any) -> None:
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[{"type": "websocket.receive", "text": _hello(version="1")}],
    )
    monkeypatch.setattr(device_ws, "_find_device_by_token", AsyncMock(return_value=_snapshot()))

    await device_ws.serve_device_socket(websocket, DeviceRegistry(), object())  # type: ignore[arg-type]

    assert websocket.closes == [(4409, '{"code":"version_unsupported","protocol_version":"3"}')]


async def test_unknown_frame_gets_protocol_error(monkeypatch: Any) -> None:
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[{"type": "websocket.receive", "text": '{"type":"not_a_frame"}'}],
    )
    monkeypatch.setattr(device_ws, "_find_device_by_token", AsyncMock(return_value=_snapshot()))

    await device_ws.serve_device_socket(websocket, DeviceRegistry(), object())  # type: ignore[arg-type]

    assert json.loads(websocket.sent[0])["code"] == "protocol_unknown_type"
    assert websocket.closes[0][0] == 1002


async def test_unknown_frame_after_handshake_closes_current_generation(
    monkeypatch: Any,
) -> None:
    snapshot = _snapshot()
    hello = _hello()
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[
            {"type": "websocket.receive", "text": hello},
            {"type": "websocket.receive", "text": _config_applied(hello)},
            {"type": "websocket.receive", "text": '{"type":"not_a_frame"}'},
        ],
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )

    await device_ws.serve_device_socket(websocket, DeviceRegistry(), object())  # type: ignore[arg-type]

    assert [json.loads(payload)["type"] for payload in websocket.sent] == [
        "hello_ack",
        "config_applied_ack",
        "error",
    ]
    assert json.loads(websocket.sent[-1])["code"] == "protocol_unknown_type"
    assert websocket.closes == [(1002, "protocol_error")]


async def test_malformed_frame_before_config_applied_retires_unready_generation(
    monkeypatch: Any,
) -> None:
    snapshot = _snapshot()
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[
            {"type": "websocket.receive", "text": _hello()},
            {"type": "websocket.receive", "text": '{"type":"not_a_frame"}'},
        ],
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )
    registry = DeviceRegistry()

    await asyncio.wait_for(
        device_ws.serve_device_socket(websocket, registry, object()),  # type: ignore[arg-type]
        timeout=1,
    )

    assert any(json.loads(payload)["type"] == "error" for payload in websocket.sent)
    assert not any(
        json.loads(payload)["type"] == "config_applied_ack" for payload in websocket.sent
    )
    assert not await registry.is_online(snapshot.id, user_id=snapshot.user_id)


async def test_disconnect_before_config_applied_does_not_leave_handshake_tasks(
    monkeypatch: Any,
) -> None:
    snapshot = _snapshot()
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[{"type": "websocket.receive", "text": _hello()}],
    )
    monkeypatch.setattr(
        device_ws,
        "_find_device_by_token",
        AsyncMock(return_value=snapshot),
    )
    registry = DeviceRegistry()

    await asyncio.wait_for(
        device_ws.serve_device_socket(websocket, registry, object()),  # type: ignore[arg-type]
        timeout=1,
    )

    assert not await registry.is_online(snapshot.id, user_id=snapshot.user_id)


async def test_heartbeat_sends_ping_and_closes_after_liveness_timeout() -> None:
    registry = DeviceRegistry()
    websocket = _FakeWebSocket(headers={}, incoming=[])
    transport = device_ws.WebSocketTransport(websocket)
    snapshot = _snapshot()
    handle = await registry.register(
        device_id=snapshot.id,
        user_id=snapshot.user_id,
        device_name=snapshot.name,
        transport=transport,
    )

    pending = asyncio.create_task(
        registry.dispatch_tool(
            device_id=snapshot.id,
            user_id=snapshot.user_id,
            name="read_file",
            args={"path": "pending.txt"},
            max_result_bytes=1024,
            timeout_seconds=10,
        )
    )
    for _ in range(100):
        if any(json.loads(payload).get("type") == "tool_call" for payload in websocket.sent):
            break
        await asyncio.sleep(0)

    await asyncio.wait_for(
        device_ws._heartbeat(
            registry,
            handle,
            transport,
            ping_interval_seconds=0.01,
            liveness_timeout_seconds=0.025,
        ),
        timeout=1,
    )

    assert any(json.loads(payload)["type"] == "ping" for payload in websocket.sent)
    assert websocket.closes == [(4408, "")]
    assert await registry.is_online(snapshot.id, user_id=snapshot.user_id) is False
    with pytest.raises(DeviceOutcomeUnknownError):
        await pending
