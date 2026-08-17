from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from openctopus_server.api import device_ws
from openctopus_server.devices.protocol import (
    DeviceCapabilities,
    DeviceConfigFrame,
    HelloFrame,
    ShellMetadata,
    new_uuid7,
)
from openctopus_server.devices.registry import (
    ConnectionHandle,
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
    accepted: bool = False
    sent: list[str] = field(default_factory=list)
    closes: list[tuple[int, str]] = field(default_factory=list)
    disconnect: asyncio.Event | None = None

    async def accept(self) -> None:
        self.accepted = True

    async def receive(self) -> dict[str, object]:
        if self.incoming:
            return self.incoming.pop(0)
        if self.disconnect is not None:
            await self.disconnect.wait()
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
        sandbox_mode=True,
        ssrf_denylist=["127.0.0.0/8"],
        created_at=datetime.now(UTC),
    )


def _hello(*, version: str = "2") -> str:
    return json.dumps(
        {
            **HelloFrame(
                id=new_uuid7(),
                version="2",
                client_version="0.1.0",
                os="linux",
                caps=DeviceCapabilities(),
                shells=ShellMetadata(default="bash", available=["bash", "sh"]),
            ).model_dump(mode="json"),
            "version": version,
        }
    )


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
        incoming=[{"type": "websocket.receive", "text": hello}],
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
            "config": {
                "workspace_path": "~/workspace",
                "sandbox_mode": True,
                "ssrf_denylist": ["127.0.0.0/8"],
                "shell_timeout_max": 600,
                "env_allowlist": list(DEFAULT_ENV_ALLOWLIST),
            },
        }
    ]
    assert websocket.closes == [(1000, "")]
    assert await registry.is_online(snapshot.id, user_id=snapshot.user_id) is False


async def test_handshake_generation_is_not_routable_before_hello_ack_is_written(
    monkeypatch: Any,
) -> None:
    snapshot = _snapshot()
    disconnect = asyncio.Event()
    websocket = _BlockingSendWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[{"type": "websocket.receive", "text": _hello()}],
        disconnect=disconnect,
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
        if await registry.is_online(snapshot.id, user_id=snapshot.user_id):
            break
        await asyncio.sleep(0)
    assert await registry.is_online(snapshot.id, user_id=snapshot.user_id) is True
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
            )

    snapshot = replace(_snapshot(), sandbox_mode=False)
    disconnect = asyncio.Event()
    websocket = _FakeWebSocket(
        headers={"authorization": "Bearer token"},
        incoming=[{"type": "websocket.receive", "text": _hello()}],
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
        ):
            patch_acquired.set()
            return await registry.push_config(
                device_id=snapshot.id,
                user_id=snapshot.user_id,
                device_name=snapshot.name,
                config=DeviceConfigFrame(
                    workspace_path=snapshot.workspace_path,
                    sandbox_mode=True,
                    ssrf_denylist=snapshot.ssrf_denylist,
                ),
            )

    patch_task = asyncio.create_task(patch_and_push())
    await asyncio.sleep(0)
    assert patch_acquired.is_set() is False

    registry.release_register.set()
    assert await asyncio.wait_for(patch_task, timeout=1) is True
    disconnect.set()
    await asyncio.wait_for(serve_task, timeout=1)

    frames = [json.loads(payload) for payload in websocket.sent]
    assert [frame["type"] for frame in frames] == ["hello_ack", "config_update"]
    assert frames[0]["config"]["sandbox_mode"] is False
    assert frames[1]["config"]["sandbox_mode"] is True


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

    assert websocket.closes == [(4409, '{"code":"version_unsupported","protocol_version":"2"}')]


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

    await device_ws.serve_device_socket(websocket, DeviceRegistry(), object())  # type: ignore[arg-type]

    assert [json.loads(payload)["type"] for payload in websocket.sent] == [
        "hello_ack",
        "error",
    ]
    assert json.loads(websocket.sent[-1])["code"] == "protocol_unknown_type"
    assert websocket.closes == [(1002, "protocol_error")]


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
