"""Contract tests for shared workspace management REST APIs."""

import asyncio
import threading
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import Workspace, WorkspaceMember
from openctopus_server.workspace.storage import get_object_storage

DEFAULT_QUOTA_BYTES = 524_288_000


async def _register(
    client: Any,
    *,
    email: str,
    name: str,
) -> dict[str, Any]:
    response = await client.post(
        "/api/auth/register",
        json={"email": email, "password": "testpassword", "name": name},
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


def _workspace_url(workspace_ref: str) -> str:
    return f"/api/workspaces/{quote(workspace_ref, safe='@')}"


async def _create_workspace(
    client: Any,
    identity: dict[str, Any],
    *,
    name: str = "Project",
    quota_bytes: int = 1_000_000,
) -> dict[str, Any]:
    response = await _request_as(
        client,
        identity,
        "POST",
        "/api/workspaces",
        json={"name": name, "quota_bytes": quota_bytes},
    )
    assert response.status_code == 201, response.text
    return response.json()


def _assert_page(
    body: dict[str, Any],
    *,
    limit: int,
    offset: int,
    next_offset: int | None,
) -> None:
    assert set(body) == {"items", "limit", "offset", "next_offset", "truncated"}
    assert body["limit"] == limit
    assert body["offset"] == offset
    assert body["next_offset"] == next_offset
    assert body["truncated"] is False


def _assert_error_shape(response: Any, status_code: int) -> None:
    assert response.status_code == status_code, response.text
    assert set(response.json()) == {"code", "message"}
    assert isinstance(response.json()["code"], str)
    assert isinstance(response.json()["message"], str)


@pytest.mark.parametrize(
    ("method", "url", "json_body"),
    [
        ("GET", "/api/workspaces", None),
        (
            "POST",
            "/api/workspaces",
            {"name": "Project", "quota_bytes": 1_000_000},
        ),
        ("GET", "/api/workspaces/Project@12345678", None),
        ("PATCH", "/api/workspaces/Project@12345678", {"name": "Renamed"}),
        ("GET", "/api/workspaces/Project@12345678/members", None),
        (
            "POST",
            "/api/workspaces/Project@12345678/members",
            {"email": "member@test.com"},
        ),
        (
            "DELETE",
            f"/api/workspaces/Project@12345678/members/{uuid4()}",
            None,
        ),
    ],
)
async def test_every_workspace_management_route_requires_authentication(
    async_client: Any,
    method: str,
    url: str,
    json_body: dict[str, Any] | None,
) -> None:
    async_client.cookies.clear()
    response = await async_client.request(method, url, json=json_body)

    assert response.status_code == 401
    assert response.json() == {
        "code": "auth_unauthorized",
        "message": "Not authenticated",
    }


async def test_workspace_list_contains_only_personal_and_accessible_shared_workspaces(
    async_client: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")
    outsider = await _register(async_client, email="outsider@test.com", name="Outsider")
    alpha = await _create_workspace(async_client, owner, name="Alpha")
    beta = await _create_workspace(async_client, owner, name="Beta")
    await _create_workspace(async_client, outsider, name="Outsider Project")

    response = await _request_as(async_client, owner, "GET", "/api/workspaces")

    assert response.status_code == 200, response.text
    body = response.json()
    _assert_page(body, limit=200, offset=0, next_offset=None)
    assert body["items"][0] == {
        "id": owner["user"]["id"],
        "name": "Owner",
        "type": "personal",
        "quota_bytes": DEFAULT_QUOTA_BYTES,
        "bytes_used": 0,
        "locked": False,
    }
    shared = {item["id"]: item for item in body["items"][1:]}
    assert set(shared) == {alpha["id"], beta["id"]}
    assert {item["type"] for item in shared.values()} == {"shared"}


async def test_workspace_usage_scan_does_not_hold_database_connection(
    async_client: Any,
    test_app: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")
    storage = test_app.dependency_overrides[get_object_storage]()
    entered = threading.Event()
    release = threading.Event()

    def blocking_list(*args: Any, **kwargs: Any) -> list[Any]:
        del args, kwargs
        entered.set()
        release.wait(timeout=2)
        return []

    storage.client.list_objects.side_effect = blocking_list
    listing = asyncio.create_task(_request_as(async_client, owner, "GET", "/api/workspaces"))
    assert await asyncio.to_thread(entered.wait, 1)
    try:
        assert get_engine().pool.checkedout() == 0
    finally:
        release.set()
    response = await listing
    assert response.status_code == 200


async def test_workspace_and_member_collections_share_the_same_page_contract(
    async_client: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")
    member = await _register(async_client, email="member@test.com", name="Member")
    workspace = await _create_workspace(async_client, owner)
    workspace_url = _workspace_url(workspace["ref"])
    added = await _request_as(
        async_client,
        owner,
        "POST",
        f"{workspace_url}/members",
        json={"user_id": member["user"]["id"]},
    )
    assert added.status_code == 201, added.text

    workspace_page = await _request_as(
        async_client,
        owner,
        "GET",
        "/api/workspaces",
        params={"limit": 1, "offset": 0},
    )
    member_page = await _request_as(
        async_client,
        owner,
        "GET",
        f"{workspace_url}/members",
        params={"limit": 1, "offset": 0},
    )

    assert workspace_page.status_code == 200, workspace_page.text
    assert member_page.status_code == 200, member_page.text
    _assert_page(workspace_page.json(), limit=1, offset=0, next_offset=1)
    _assert_page(member_page.json(), limit=1, offset=0, next_offset=1)
    assert len(workspace_page.json()["items"]) == 1
    assert len(member_page.json()["items"]) == 1

    workspace_tail = await _request_as(
        async_client,
        owner,
        "GET",
        "/api/workspaces",
        params={"limit": 1, "offset": 1},
    )
    member_tail = await _request_as(
        async_client,
        owner,
        "GET",
        f"{workspace_url}/members",
        params={"limit": 1, "offset": 1},
    )
    _assert_page(workspace_tail.json(), limit=1, offset=1, next_offset=None)
    _assert_page(member_tail.json(), limit=1, offset=1, next_offset=None)


async def test_create_normalizes_name_assigns_stable_suffix_and_enforces_quota(
    async_client: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")

    created = await _create_workspace(
        async_client,
        owner,
        name="  Project   Cafe\u0301  ",
        quota_bytes=8_000_000,
    )

    assert created["name"] == "Project Caf\u00e9"
    assert created["type"] == "shared"
    assert created["quota_bytes"] == 8_000_000
    assert created["bytes_used"] == 0
    assert created["locked"] is False
    assert created["created_by"] == owner["user"]["id"]
    assert 8 <= len(created["suffix"]) <= 32
    assert set(created["suffix"]) <= set("0123456789abcdef")
    assert UUID(created["id"]).hex.startswith(created["suffix"])
    assert created["ref"] == f"Project Caf\u00e9@{created['suffix']}"
    assert created["members"] == [
        {
            "user_id": owner["user"]["id"],
            "email": "owner@test.com",
            "name": "Owner",
        }
    ]

    over_ceiling = await _request_as(
        async_client,
        owner,
        "POST",
        "/api/workspaces",
        json={"name": "Too Large", "quota_bytes": DEFAULT_QUOTA_BYTES + 1},
    )
    _assert_error_shape(over_ceiling, 400)


@pytest.mark.parametrize("name", ["", "..", "bad/name", "bad@name", "bad:name"])
async def test_create_rejects_invalid_workspace_names_with_uniform_error(
    async_client: Any,
    name: str,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")

    response = await _request_as(
        async_client,
        owner,
        "POST",
        "/api/workspaces",
        json={"name": name, "quota_bytes": 1_000_000},
    )

    _assert_error_shape(response, 400)


async def test_get_and_patch_preserve_identity_and_suffix_while_changing_ref(
    async_client: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")
    created = await _create_workspace(async_client, owner, name="Before")
    old_url = _workspace_url(created["ref"])

    fetched = await _request_as(async_client, owner, "GET", old_url)
    assert fetched.status_code == 200, fetched.text
    assert fetched.json() == created

    patched = await _request_as(
        async_client,
        owner,
        "PATCH",
        old_url,
        json={"name": "  After   Rename ", "quota_bytes": 2_000_000},
    )
    assert patched.status_code == 200, patched.text
    updated = patched.json()
    assert updated["id"] == created["id"]
    assert updated["suffix"] == created["suffix"]
    assert updated["name"] == "After Rename"
    assert updated["ref"] == f"After Rename@{created['suffix']}"
    assert updated["quota_bytes"] == 2_000_000

    stale = await _request_as(async_client, owner, "GET", old_url)
    assert stale.status_code == 404
    assert stale.json()["code"] == "workspace_not_found"
    current = await _request_as(async_client, owner, "GET", _workspace_url(updated["ref"]))
    assert current.status_code == 200
    assert current.json() == updated

    empty_patch = await _request_as(
        async_client,
        owner,
        "PATCH",
        _workspace_url(updated["ref"]),
        json={},
    )
    _assert_error_shape(empty_patch, 400)


async def test_members_can_be_added_by_email_or_id_listed_and_removed(
    async_client: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")
    member = await _register(async_client, email="member@test.com", name="Member")
    workspace = await _create_workspace(async_client, owner)
    members_url = f"{_workspace_url(workspace['ref'])}/members"

    added = await _request_as(
        async_client,
        owner,
        "POST",
        members_url,
        json={"email": "member@test.com"},
    )
    assert added.status_code == 201, added.text
    assert added.json() == {
        "user_id": member["user"]["id"],
        "email": "member@test.com",
        "name": "Member",
    }

    listed = await _request_as(async_client, member, "GET", members_url)
    assert listed.status_code == 200, listed.text
    _assert_page(listed.json(), limit=200, offset=0, next_offset=None)
    assert {item["user_id"] for item in listed.json()["items"]} == {
        owner["user"]["id"],
        member["user"]["id"],
    }

    removed = await _request_as(
        async_client,
        owner,
        "DELETE",
        f"{members_url}/{member['user']['id']}",
    )
    assert removed.status_code == 204, removed.text
    denied_after_removal = await _request_as(
        async_client,
        member,
        "GET",
        _workspace_url(workspace["ref"]),
    )
    assert denied_after_removal.status_code == 404
    assert denied_after_removal.json()["code"] == "workspace_not_found"

    readded = await _request_as(
        async_client,
        owner,
        "POST",
        members_url,
        json={"user_id": member["user"]["id"]},
    )
    assert readded.status_code == 201
    assert readded.json() == added.json()


@pytest.mark.parametrize("operation", ["get", "patch", "list", "add", "remove"])
async def test_inaccessible_shared_workspace_is_always_hidden_as_not_found(
    async_client: Any,
    operation: str,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")
    outsider = await _register(async_client, email="outsider@test.com", name="Outsider")
    workspace = await _create_workspace(async_client, owner)
    workspace_url = _workspace_url(workspace["ref"])
    requests = {
        "get": ("GET", workspace_url, None),
        "patch": ("PATCH", workspace_url, {"name": "Stolen"}),
        "list": ("GET", f"{workspace_url}/members", None),
        ("add"): ("POST", f"{workspace_url}/members", {"email": "outsider@test.com"}),
        "remove": (
            "DELETE",
            f"{workspace_url}/members/{owner['user']['id']}",
            None,
        ),
    }
    method, url, json_body = requests[operation]

    response = await _request_as(
        async_client,
        outsider,
        method,
        url,
        json=json_body,
    )

    assert response.status_code == 404
    assert response.json() == {
        "code": "workspace_not_found",
        "message": "Workspace not found",
    }


async def test_removing_last_member_deletes_workspace_at_api_boundary(
    async_client: Any,
    pg_engine: Any,
) -> None:
    owner = await _register(async_client, email="owner@test.com", name="Owner")
    workspace = await _create_workspace(async_client, owner)
    workspace_id = UUID(workspace["id"])
    workspace_url = _workspace_url(workspace["ref"])

    response = await _request_as(
        async_client,
        owner,
        "DELETE",
        f"{workspace_url}/members/{owner['user']['id']}",
    )

    assert response.status_code == 204, response.text
    async with AsyncSession(pg_engine) as db:
        assert await db.get(Workspace, workspace_id) is None
        member_count = await db.scalar(
            select(func.count())
            .select_from(WorkspaceMember)
            .where(WorkspaceMember.workspace_id == workspace_id)
        )
        assert member_count == 0

    missing = await _request_as(async_client, owner, "GET", workspace_url)
    assert missing.status_code == 404
    assert missing.json()["code"] == "workspace_not_found"
