from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

import pytest

from openctopus_server.chat import runner as chat_runner
from openctopus_server.devices.mcp_catalog import with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.mcp.authority import ServerMcpAuthorityFence
from openctopus_server.mcp.models import (
    ServerMcpEnvelope,
    empty_server_mcp_envelope,
    parse_server_mcp_configs,
)
from openctopus_server.mcp.routes import (
    FrozenServerMcpEntryRoute,
    build_composite_mcp_snapshot,
)
from openctopus_server.tools.base import ToolContext, ToolResult
from openctopus_server.tools.registry import ToolRegistry

_USER_ID = UUID("01890f7c-bb80-7000-8000-000000000001")
_SESSION_ID = UUID("01890f7c-bb80-7000-8000-000000000002")
_ENTRY_ID = UUID("01890f7c-bb80-7000-8000-000000000011")
_GENERATION = UUID("01890f7c-bb80-7000-8000-000000000021")


def _snapshot():  # type: ignore[no-untyped-def]
    catalog = with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[
                PersistedMcpServerCatalog(
                    name="search",
                    entries=[
                        PersistedMcpCatalogEntry(
                            entry_id=_ENTRY_ID,
                            server="search",
                            surface="tool",
                            raw_name="search",
                            invocation_identity="search",
                            final_name="mcp_search_search",
                            provider_description="MCP tool from 'search'. Search.",
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
    envelope = ServerMcpEnvelope(
        version=1,
        config_revision=7,
        mcp_servers=list(
            parse_server_mcp_configs(
                [
                    {
                        "name": "search",
                        "transport": "streamable_http",
                        "url": "https://mcp.example/mcp",
                        "enabled_capabilities": [],
                    }
                ]
            )
        ),
        mcp_catalog=catalog,
    )
    return build_composite_mcp_snapshot(
        envelope,
        [],
        runtime_generations={"search": _GENERATION},
    )


class _Dispatcher:
    def __init__(self, *, failure: Exception | None = None, issue: bool = True) -> None:
        self.failure = failure
        self.issue = issue
        self.calls: list[tuple[FrozenServerMcpEntryRoute, dict[str, object]]] = []

    async def dispatch_server_mcp(
        self,
        *,
        route: FrozenServerMcpEntryRoute,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        on_issued: Callable[[], None] | None = None,
        issue_guard: Callable[[], bool] | None = None,
    ) -> ToolResult:
        del user_id, name
        if issue_guard is not None and not issue_guard():
            return ToolResult(
                content="unavailable",
                is_error=True,
                code=ErrorCode.TOOL_MCP_UNAVAILABLE,
            )
        if self.issue and on_issued is not None:
            on_issued()
        if self.failure is not None:
            raise self.failure
        self.calls.append((route, args))
        return ToolResult(content="ok")


def _ctx() -> ToolContext:
    return ToolContext(user_id=_USER_ID, session_id=_SESSION_ID)


def test_registry_exposes_server_mcp_schema() -> None:
    schemas = ToolRegistry(()).get_tool_schemas(mcp_snapshot=_snapshot())

    assert [schema["name"] for schema in schemas] == ["mcp_search_search"]
    assert schemas[0]["input_schema"]["properties"]["openoctopus_device"]["enum"] == [
        "server"
    ]


@pytest.mark.asyncio
async def test_mcp_authority_snapshot_retries_when_server_changes_between_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = empty_server_mcp_envelope()
    second = first.model_copy(update={"config_revision": 2})
    envelopes = iter((first, second, second, second))
    device_reads = 0

    async def load_server(_db):  # type: ignore[no-untyped-def]
        return next(envelopes)

    async def load_devices(_db, *, user_id):  # type: ignore[no-untyped-def]
        nonlocal device_reads
        del user_id
        device_reads += 1
        return [f"devices-{device_reads}"]

    monkeypatch.setattr(chat_runner, "load_server_mcp_envelope", load_server)
    monkeypatch.setattr(chat_runner, "load_owner_device_snapshot", load_devices)

    envelope, devices = await chat_runner._load_mcp_authority_snapshot(  # noqa: SLF001
        object(),  # type: ignore[arg-type]
        user_id=_USER_ID,
    )

    assert envelope.config_revision == 2
    assert devices == ["devices-2"]
    assert device_reads == 2


@pytest.mark.asyncio
async def test_mcp_authority_snapshot_fails_after_bounded_revision_churn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revisions = iter(range(1, 7))

    async def load_server(_db):  # type: ignore[no-untyped-def]
        return empty_server_mcp_envelope().model_copy(
            update={"config_revision": next(revisions)}
        )

    async def load_devices(_db, *, user_id):  # type: ignore[no-untyped-def]
        del user_id
        return []

    monkeypatch.setattr(chat_runner, "load_server_mcp_envelope", load_server)
    monkeypatch.setattr(chat_runner, "load_owner_device_snapshot", load_devices)

    with pytest.raises(RuntimeError, match="changed repeatedly"):
        await chat_runner._load_mcp_authority_snapshot(  # noqa: SLF001
            object(),  # type: ignore[arg-type]
            user_id=_USER_ID,
        )


@pytest.mark.asyncio
async def test_registry_dispatches_server_route_and_strips_selector() -> None:
    dispatcher = _Dispatcher()
    issued = False

    def mark_issued() -> None:
        nonlocal issued
        issued = True

    result = await ToolRegistry((), server_mcp_dispatcher=dispatcher).execute(
        name="mcp_search_search",
        args={"query": "octopus", "openoctopus_device": "server"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
        on_issued=mark_issued,
    )

    assert result == ToolResult(content="ok")
    assert dispatcher.calls[0][1] == {"query": "octopus"}
    assert dispatcher.calls[0][0].runtime_generation == _GENERATION
    assert issued is True


@pytest.mark.asyncio
async def test_registry_passes_server_authority_guard_to_the_issue_boundary() -> None:
    dispatcher = _Dispatcher()
    result = await ToolRegistry(
        (),
        server_mcp_dispatcher=dispatcher,
        server_mcp_authority=ServerMcpAuthorityFence(empty_server_mcp_envelope()),
    ).execute(
        name="mcp_search_search",
        args={"query": "octopus", "openoctopus_device": "server"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
    )

    assert result.code is ErrorCode.TOOL_MCP_UNAVAILABLE
    assert dispatcher.calls == []


@pytest.mark.asyncio
async def test_registry_maps_missing_or_failed_server_dispatch_without_leaking() -> None:
    missing = await ToolRegistry(()).execute(
        name="mcp_search_search",
        args={"query": "octopus", "openoctopus_device": "server"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
    )
    failed_before_send = await ToolRegistry(
        (),
        server_mcp_dispatcher=_Dispatcher(
            failure=RuntimeError("secret failure"),
            issue=False,
        ),
    ).execute(
        name="mcp_search_search",
        args={"query": "octopus", "openoctopus_device": "server"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
    )
    failed_after_send = await ToolRegistry(
        (),
        server_mcp_dispatcher=_Dispatcher(
            failure=RuntimeError("secret failure"),
            issue=True,
        ),
    ).execute(
        name="mcp_search_search",
        args={"query": "octopus", "openoctopus_device": "server"},
        ctx=_ctx(),
        mcp_snapshot=_snapshot(),
    )

    assert missing.code is ErrorCode.TOOL_MCP_UNAVAILABLE
    assert failed_before_send.code is ErrorCode.TOOL_MCP_ERROR
    assert failed_after_send.code is ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN
    assert "secret failure" not in str(failed_before_send.content)
    assert "secret failure" not in str(failed_after_send.content)
