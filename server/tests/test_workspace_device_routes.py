from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID

from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.protocol import ToolResultFrame, new_uuid7


class _WorkspaceRegistry:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def is_online(self, device_id: UUID, *, user_id: UUID) -> bool:
        del device_id, user_id
        return False

    async def dispatch_tool(self, **kwargs: Any) -> ToolResultFrame:
        self.calls.append(kwargs)
        self.entered.set()
        await self.release.wait()
        operation = kwargs["args"]["operation"]
        if operation == "edit_file":
            content = {
                "path": "a.txt",
                "size": 2,
                "etag": "opaque",
                "created": False,
                "replacements": 1,
            }
        elif operation == "apply_patch":
            content = {
                "items": [
                    {
                        "path": "a.txt",
                        "action": "add",
                        "size": 2,
                        "etag": "opaque",
                        "created": True,
                        "replacements": 0,
                    }
                ],
                "dry_run": False,
                "committed": 1,
            }
        elif operation in {"delete_file", "delete_folder"}:
            content = {"deleted": True}
        elif operation in {"list_dir", "find_files"}:
            content = {
                "items": [{"name": "a.txt", "path": "a.txt", "kind": "file", "size": 2}],
                "limit": 200,
                "offset": 0,
                "next_offset": None,
                "truncated": False,
            }
        else:
            content = {
                "items": [{"path": "a.txt", "line_number": 1, "line": "x"}],
                "limit": 200,
                "offset": 0,
                "next_offset": None,
                "truncated": False,
            }
        return ToolResultFrame(
            id=new_uuid7(),
            content=json.dumps(content),
            is_error=False,
        )


async def test_device_workspace_routes_dispatch_machine_result_and_close_db(
    test_app: Any,
    user_client: Any,
    pg_engine: Any,
) -> None:
    registry = _WorkspaceRegistry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry
    created = await user_client.post(
        "/api/devices",
        json={"name": "laptop", "workspace_path": "/tmp/workspace"},
    )
    assert created.status_code == 201

    pending = asyncio.create_task(
        user_client.patch(
            "/api/workspace/files/a.txt",
            params={"openoctopus_device": "laptop"},
            json={"old_text": "a", "new_text": "b"},
        )
    )
    await asyncio.wait_for(registry.entered.wait(), timeout=1)
    assert pg_engine.pool.checkedout() == 0
    registry.release.set()
    response = await pending
    assert response.status_code == 200
    assert response.headers["etag"] == '"opaque"'
    assert response.json() == {
        "path": "a.txt",
        "size": 2,
        "etag": "opaque",
        "created": False,
        "replacements": 1,
    }
    assert registry.calls[0]["name"] == "__workspace_rest__"
    assert registry.calls[0]["args"]["operation"] == "edit_file"


async def test_unknown_or_other_users_device_is_reported_as_unreachable(
    test_app: Any,
    user_client: Any,
) -> None:
    registry = _WorkspaceRegistry()
    test_app.dependency_overrides[get_device_registry] = lambda: registry

    response = await user_client.get(
        "/api/workspace/list/subdir",
        params={"openoctopus_device": "someone-elses-device"},
    )

    assert response.status_code == 409
    assert response.json()["code"] == "tool_device_unreachable"
    assert registry.calls == []
