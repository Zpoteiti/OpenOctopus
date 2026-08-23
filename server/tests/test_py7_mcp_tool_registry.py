from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

import openctopus_server.tools.registry as registry_module
from openctopus_server.devices.mcp_catalog import with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
)
from openctopus_server.devices.mcp_routes import (
    FrozenMcpEntryRoute,
    OwnerMcpDevice,
    OwnerMcpSnapshot,
    build_owner_mcp_snapshot,
)
from openctopus_server.devices.protocol import ToolResultFrame, new_uuid7
from openctopus_server.devices.registry import (
    DeviceMcpUnavailableError,
    DeviceUnavailableError,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.registry import ToolRegistry, build_py3_registry

_USER_ID = UUID("01890f7c-bb80-7000-8000-000000000001")
_DEVICE_ID = UUID("01890f7c-bb80-7000-8000-000000000002")
_ENTRY_ID = UUID("01890f7c-bb80-7000-8000-000000000003")
_SESSION_ID = UUID("01890f7c-bb80-7000-8000-000000000004")


def _snapshot() -> OwnerMcpSnapshot:
    catalog = with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[
                PersistedMcpServerCatalog(
                    name="demo",
                    entries=[
                        PersistedMcpCatalogEntry(
                            entry_id=_ENTRY_ID,
                            server="demo",
                            surface="tool",
                            raw_name="search",
                            invocation_identity="search",
                            final_name="mcp_demo_search",
                            provider_description="Search with demo.",
                            input_schema={
                                "type": "object",
                                "properties": {"query": {"type": "string"}},
                                "required": ["query"],
                                "additionalProperties": False,
                            },
                            enabled=True,
                        )
                    ],
                )
            ],
        )
    )
    return build_owner_mcp_snapshot(
        [
            OwnerMcpDevice(
                device_id=_DEVICE_ID,
                name="laptop",
                config_revision=7,
                catalog=catalog,
            )
        ]
    )


class _McpDispatcher:
    def __init__(
        self,
        failure: Exception | None = None,
        *,
        issue_before_failure: bool = False,
        content: str | list[dict[str, Any]] = "ok",
        error_code: str | None = None,
    ) -> None:
        self.failure = failure
        self.issue_before_failure = issue_before_failure
        self.content = content
        self.error_code = error_code
        self.calls: list[dict[str, object]] = []

    async def dispatch_mcp_tool(
        self,
        *,
        route: FrozenMcpEntryRoute,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        chat_session_id: UUID | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ToolResultFrame:
        if self.issue_before_failure and on_issued is not None:
            on_issued()
        if self.failure is not None:
            raise self.failure
        if not self.issue_before_failure and on_issued is not None:
            on_issued()
        self.calls.append(
            {
                "route": route,
                "user_id": user_id,
                "name": name,
                "args": args,
                "max_result_bytes": max_result_bytes,
                "timeout_seconds": timeout_seconds,
                "chat_session_id": chat_session_id,
            }
        )
        return ToolResultFrame(
            id=new_uuid7(),
            content=self.content,
            is_error=self.error_code is not None,
            code=self.error_code,
        )


def _ctx() -> ToolContext:
    return ToolContext(user_id=_USER_ID, session_id=_SESSION_ID)


def test_registry_appends_durable_mcp_schemas() -> None:
    registry = ToolRegistry(())

    schemas = registry.get_tool_schemas(mcp_snapshot=_snapshot())

    assert [schema["name"] for schema in schemas] == ["mcp_demo_search"]
    assert schemas[0]["input_schema"]["properties"]["openoctopus_device"]["enum"] == [
        "laptop"
    ]


def test_registry_reuses_only_matching_provider_shape() -> None:
    registry = ToolRegistry(())
    snapshot = _snapshot()
    first = registry.get_tool_schemas(mcp_snapshot=snapshot)
    same_shape = replace(
        snapshot,
        routes=(
            replace(
                snapshot.routes[0],
                entry_id=uuid4(),
                config_revision=snapshot.routes[0].config_revision + 1,
            ),
        ),
    )
    second = registry.get_tool_schemas(mcp_snapshot=same_shape)
    changed_shape = replace(snapshot, shape_key="f" * 64)
    third = registry.get_tool_schemas(mcp_snapshot=changed_shape)

    assert first is not second
    assert first[0] is second[0]
    assert third[0] is not first[0]


@pytest.mark.asyncio
async def test_registry_dispatches_the_frozen_route_and_strips_selector() -> None:
    snapshot = _snapshot()
    dispatcher = _McpDispatcher()
    checked: list[tuple[UUID, FrozenMcpEntryRoute]] = []

    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        checked.append((user_id, route))
        return True

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)
    result = await registry.execute(
        name="mcp_demo_search",
        args={"query": "octopus", "openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=snapshot,
        device_registry=dispatcher,
    )

    assert result.is_error is False
    assert checked == [(_USER_ID, snapshot.routes[0])]
    assert dispatcher.calls == [
        {
            "route": snapshot.routes[0],
            "user_id": _USER_ID,
            "name": "mcp_demo_search",
            "args": {"query": "octopus"},
            "max_result_bytes": 12 * 1024 * 1024,
            "timeout_seconds": 60.0,
            "chat_session_id": _SESSION_ID,
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current", "failure", "issue_before_failure", "expected_code"),
    [
        (False, None, False, ErrorCode.TOOL_MCP_UNAVAILABLE),
        (
            True,
            DeviceMcpUnavailableError("not registered"),
            False,
            ErrorCode.TOOL_MCP_UNAVAILABLE,
        ),
        (True, DeviceUnavailableError("offline"), False, ErrorCode.TOOL_DEVICE_UNREACHABLE),
        (
            True,
            DeviceUnavailableError("lost after issue"),
            True,
            ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
        ),
    ],
)
async def test_registry_maps_mcp_route_and_availability_failures(
    current: bool,
    failure: Exception | None,
    issue_before_failure: bool,
    expected_code: ErrorCode,
) -> None:
    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        return current

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)
    result = await registry.execute(
        name="mcp_demo_search",
        args={"query": "octopus", "openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=_McpDispatcher(
            failure,
            issue_before_failure=issue_before_failure,
        ),
    )

    assert result.is_error is True
    assert result.code is expected_code


@pytest.mark.asyncio
async def test_registry_rejects_unknown_mcp_but_forwards_schema_invalid_arguments() -> None:
    dispatcher = _McpDispatcher()

    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        return True

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)

    unknown = await registry.execute(
        name="mcp_demo_missing",
        args={"openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=dispatcher,
    )
    malformed = await registry.execute(
        name="mcp_demo_search",
        args={"openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=dispatcher,
    )

    assert unknown.code is ErrorCode.TOOL_INVALID_ARGS
    assert malformed.is_error is False
    assert dispatcher.calls[0]["args"] == {}


@pytest.mark.asyncio
async def test_registry_forwards_unknown_mcp_fields_to_mcp_server() -> None:
    dispatcher = _McpDispatcher()

    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        return True

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)
    untrusted_field = "secret-" + "x" * 20_000

    result = await registry.execute(
        name="mcp_demo_search",
        args={
            "query": "octopus",
            "openoctopus_device": "laptop",
            untrusted_field: "value",
        },
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=dispatcher,
    )

    assert result.is_error is False
    assert dispatcher.calls[0]["args"] == {"query": "octopus", untrusted_field: "value"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("issued", "expected_code"),
    [
        (False, ErrorCode.TOOL_MCP_ERROR),
        (True, ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN),
    ],
)
async def test_registry_contains_unexpected_mcp_dispatch_failures(
    issued: bool,
    expected_code: ErrorCode,
) -> None:
    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        return True

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)
    result = await registry.execute(
        name="mcp_demo_search",
        args={"query": "octopus", "openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=_McpDispatcher(
            RuntimeError("secret third-party failure"),
            issue_before_failure=issued,
        ),
    )

    assert result.is_error is True
    assert result.code is expected_code
    assert "secret third-party failure" not in str(result.content)


@pytest.mark.asyncio
async def test_registry_contains_mcp_route_resolver_failures() -> None:
    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        raise RuntimeError("secret database failure")

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)
    result = await registry.execute(
        name="mcp_demo_search",
        args={"query": "octopus", "openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=_McpDispatcher(),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_DB_ERROR
    assert "secret database failure" not in str(result.content)


@pytest.mark.asyncio
async def test_registry_bounds_provider_visible_mcp_text() -> None:
    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        return True

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)
    result = await registry.execute(
        name="mcp_demo_search",
        args={"query": "octopus", "openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=_McpDispatcher(
            content=[{"type": "text", "text": "x" * 20_000}],
        ),
    )

    assert isinstance(result.content, list)
    text = result.content[1]["text"]
    assert isinstance(text, str)
    assert text == "x" * 16_000 + "\n... (truncated)"


@pytest.mark.asyncio
async def test_registry_normalizes_unknown_mcp_error_codes() -> None:
    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        return True

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)
    result = await registry.execute(
        name="mcp_demo_search",
        args={"query": "octopus", "openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=_McpDispatcher(
            content="third-party failure",
            error_code="vendor_private_error",
        ),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_MCP_ERROR


@pytest.mark.asyncio
async def test_registry_preserves_mcp_result_too_large() -> None:
    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        return True

    registry = ToolRegistry((), mcp_route_resolver=route_is_current)
    result = await registry.execute(
        name="mcp_demo_search",
        args={"query": "octopus", "openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=_McpDispatcher(
            content="result exceeded response credit",
            error_code="tool_result_too_large",
        ),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_RESULT_TOO_LARGE


@pytest.mark.asyncio
async def test_py3_registry_with_engine_installs_mcp_route_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def route_is_current(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        del user_id, route
        return True

    engine = cast(AsyncEngine, object())
    monkeypatch.setattr(
        registry_module,
        "_owned_mcp_route_resolver",
        lambda candidate: route_is_current if candidate is engine else None,
    )
    registry = build_py3_registry(engine=engine)
    result = await registry.execute(
        name="mcp_demo_search",
        args={"query": "octopus", "openoctopus_device": "laptop"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        device_registry=_McpDispatcher(),
    )

    assert result.is_error is False
