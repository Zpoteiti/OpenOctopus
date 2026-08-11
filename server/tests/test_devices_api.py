"""Device persistence and authenticated REST contract tests."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import Device
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.services import devices


class _FakeDeviceRegistry:
    def __init__(self) -> None:
        self.online_ids: set[UUID] = set()
        self.revoked: list[UUID] = []
        self.removed: list[UUID] = []
        self.config_updates: list[dict[str, Any]] = []
        self.entered: asyncio.Event | None = None
        self.release: asyncio.Event | None = None

    async def is_online(self, device_id: UUID, *, user_id: UUID) -> bool:
        del user_id
        if self.entered is not None and self.release is not None:
            self.entered.set()
            await self.release.wait()
        return device_id in self.online_ids

    async def revoke(self, device_id: UUID) -> bool:
        self.revoked.append(device_id)
        self.online_ids.discard(device_id)
        return True

    async def push_config(self, **kwargs: Any) -> bool:
        self.config_updates.append(kwargs)
        return True

    async def remove_device(self, device_id: UUID) -> bool:
        self.removed.append(device_id)
        self.online_ids.discard(device_id)
        return True


async def _register(client: Any, *, email: str) -> dict[str, Any]:
    response = await client.post(
        "/api/auth/register",
        json={"email": email, "password": "testpassword", "name": email.split("@")[0]},
    )
    assert response.status_code == 201, response.text
    return response.json()


async def _request_as(
    client: Any,
    identity: dict[str, Any],
    method: str,
    url: str,
    **kwargs: Any,
) -> Any:
    client.cookies.clear()
    headers = dict(kwargs.pop("headers", {}))
    headers["Authorization"] = f"Bearer {identity['jwt']}"
    return await client.request(method, url, headers=headers, **kwargs)


@pytest.mark.parametrize(
    ("method", "url", "body"),
    [
        ("GET", "/api/devices", None),
        ("POST", "/api/devices", {"name": "Laptop"}),
        ("PATCH", "/api/devices/laptop/config", {"sandbox_mode": False}),
        ("POST", "/api/devices/laptop/regenerate-token", None),
        ("DELETE", "/api/devices/laptop", None),
    ],
)
async def test_device_routes_require_authenticated_user(
    async_client: Any,
    method: str,
    url: str,
    body: dict[str, Any] | None,
) -> None:
    response = await async_client.request(method, url, json=body)

    assert response.status_code == 401
    assert response.json() == {"code": "auth_unauthorized", "message": "Not authenticated"}


async def test_device_rest_lifecycle_stores_only_the_token_hash(
    async_client: Any,
    pg_engine: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com")
    created_response = await _request_as(
        async_client,
        owner,
        "POST",
        "/api/devices",
        json={"name": "  Alice  Laptop ", "workspace_path": "~/work"},
    )

    assert created_response.status_code == 201, created_response.text
    created = created_response.json()
    assert set(created) == {"token", "device"}
    token = created["token"]
    device = created["device"]
    assert token.startswith("openoctopus_dev_")
    assert device == {
        "id": device["id"],
        "name": "alice-laptop",
        "token_hint": f"{token[:16]}...{token[-6:]}",
        "workspace_path": "~/work",
        "sandbox_mode": True,
        "ssrf_denylist": [
            "0.0.0.0/8",
            "127.0.0.0/8",
            "224.0.0.0/4",
            "240.0.0.0/4",
            "::/128",
            "::1/128",
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "100.64.0.0/10",
            "169.254.0.0/16",
            "169.254.169.254/32",
            "fc00::/7",
            "fe80::/10",
            "ff00::/8",
        ],
        "online": False,
        "created_at": device["created_at"],
    }

    async with AsyncSession(pg_engine) as db:
        row = await db.scalar(select(Device).where(Device.name == "alice-laptop"))
    assert row is not None
    assert row.token_hash == hashlib.sha256(token.encode("utf-8")).digest()
    assert row.token_hint == device["token_hint"]
    assert token not in repr(row)
    row_id = row.id
    row_user_id = row.user_id
    async with AsyncSession(pg_engine) as db:
        authenticated = await devices.find_by_token(db, token)
        rejected = await devices.find_by_token(db, "openoctopus_dev_not-the-token")
    assert authenticated is not None
    assert authenticated.id == row_id
    assert authenticated.user_id == row_user_id
    assert rejected is None

    listed_response = await _request_as(async_client, owner, "GET", "/api/devices")
    assert listed_response.status_code == 200
    assert listed_response.json() == [device]
    assert token not in listed_response.text

    patched_response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/alice-laptop/config",
        json={
            "name": "Desk  PC",
            "sandbox_mode": False,
            "ssrf_denylist": ["internal.example:8443"],
        },
    )
    assert patched_response.status_code == 200, patched_response.text
    patched = patched_response.json()
    assert patched == {
        **device,
        "name": "desk-pc",
        "sandbox_mode": False,
        "ssrf_denylist": ["internal.example:8443"],
    }

    no_op_response = await _request_as(
        async_client, owner, "PATCH", "/api/devices/desk-pc/config", json={}
    )
    assert no_op_response.status_code == 200
    assert no_op_response.json() == patched

    rotation_response = await _request_as(
        async_client, owner, "POST", "/api/devices/desk-pc/regenerate-token"
    )
    assert rotation_response.status_code == 200, rotation_response.text
    rotated = rotation_response.json()
    assert set(rotated) == {"token", "device"}
    assert rotated["token"] != token
    assert rotated["device"] == {
        **patched,
        "token_hint": f"{rotated['token'][:16]}...{rotated['token'][-6:]}",
    }

    deleted_response = await _request_as(async_client, owner, "DELETE", "/api/devices/desk-pc")
    assert deleted_response.status_code == 204
    assert deleted_response.content == b""
    assert (await _request_as(async_client, owner, "GET", "/api/devices")).json() == []


async def test_device_names_are_canonical_per_user_and_unknown_config_is_rejected(
    async_client: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com")
    other_user = await _register(async_client, email="other@test.com")

    for bad_name in ("server", "../laptop", "Laptop_2", "\u00e9", ""):
        response = await _request_as(
            async_client, owner, "POST", "/api/devices", json={"name": bad_name}
        )
        assert response.status_code == 400
        assert response.json()["code"] == "device_invalid_request"

    first = await _request_as(
        async_client, owner, "POST", "/api/devices", json={"name": "Main Laptop"}
    )
    assert first.status_code == 201
    duplicate = await _request_as(
        async_client, owner, "POST", "/api/devices", json={"name": "main   laptop"}
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["code"] == "device_name_taken"

    other = await _request_as(
        async_client, other_user, "POST", "/api/devices", json={"name": "other laptop"}
    )
    assert other.status_code == 201

    unknown = await _request_as(
        async_client,
        owner,
        "POST",
        "/api/devices",
        json={"name": "Second", "shell_timeout_max": 5},
    )
    assert unknown.status_code == 400
    assert unknown.json() == {
        "code": "device_invalid_request",
        "message": "Device request is invalid",
    }

    cross_user = await _request_as(
        async_client, other_user, "PATCH", "/api/devices/main-laptop/config", json={"name": "nope"}
    )
    assert cross_user.status_code == 404
    assert cross_user.json()["code"] == "device_not_found"


@pytest.mark.parametrize(
    "config",
    [
        {"workspace_path": " "},
        {"workspace_path": "/tmp/work\x00space"},
        {"workspace_path": "x" * 4097},
        {"ssrf_denylist": [" "]},
        {"ssrf_denylist": ["127.0.0.1\x00/32"]},
        {"ssrf_denylist": ["x" * 513]},
        {"ssrf_denylist": [f"host-{index}.example" for index in range(257)]},
    ],
)
async def test_device_config_is_bounded_before_it_can_reach_the_wire(
    async_client: Any,
    config: dict[str, Any],
) -> None:
    owner = await _register(async_client, email="bounded@test.com")

    response = await _request_as(
        async_client,
        owner,
        "POST",
        "/api/devices",
        json={"name": "laptop", **config},
    )

    assert response.status_code == 400
    assert response.json()["code"] == "device_invalid_request"


async def test_device_rest_projects_registry_state_after_commits(
    async_client: Any,
    test_app: Any,
) -> None:
    registry = _FakeDeviceRegistry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    owner = await _register(async_client, email="owner@test.com")
    created_response = await _request_as(
        async_client, owner, "POST", "/api/devices", json={"name": "Laptop"}
    )
    assert created_response.status_code == 201
    created = created_response.json()
    device_id = UUID(created["device"]["id"])
    assert created["device"]["online"] is False

    registry.online_ids.add(device_id)
    entered = asyncio.Event()
    release = asyncio.Event()
    registry.entered = entered
    registry.release = release
    listing = asyncio.create_task(_request_as(async_client, owner, "GET", "/api/devices"))
    await entered.wait()
    try:
        assert get_engine().pool.checkedout() == 0
    finally:
        release.set()
    listed_response = await listing
    assert listed_response.status_code == 200
    assert listed_response.json()[0]["online"] is True
    registry.entered = None
    registry.release = None

    patched_response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={"workspace_path": "~/updated"},
    )
    assert patched_response.status_code == 200
    assert patched_response.json()["online"] is True
    assert registry.config_updates[0]["device_name"] == "laptop"
    assert registry.config_updates[0]["config"].workspace_path == "~/updated"

    no_op_response = await _request_as(
        async_client,
        owner,
        "PATCH",
        "/api/devices/laptop/config",
        json={},
    )
    assert no_op_response.status_code == 200
    assert len(registry.config_updates) == 1

    rotated_response = await _request_as(
        async_client, owner, "POST", "/api/devices/laptop/regenerate-token"
    )
    assert rotated_response.status_code == 200
    assert registry.revoked == [device_id]
    assert rotated_response.json()["device"]["online"] is False

    registry.online_ids.add(device_id)
    deleted_response = await _request_as(async_client, owner, "DELETE", "/api/devices/laptop")
    assert deleted_response.status_code == 204
    assert registry.removed == [device_id]
