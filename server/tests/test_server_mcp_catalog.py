from __future__ import annotations

import gc
import weakref
from collections.abc import Awaitable, Callable
from uuid import UUID

import pytest
from mcp import types
from pydantic import AnyUrl

from openctopus_server.devices.mcp_models import (
    PromptArgument,
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SourceMcpTool,
)
from openctopus_server.mcp.catalog import (
    McpCatalogError,
    build_server_persisted_catalog,
    discover_server_catalog,
    expand_resource_template,
)
from openctopus_server.mcp.models import ServerStdioMcpServerConfig


async def _value[T](value: T) -> T:
    return value


class FakeSession:
    def __init__(
        self,
        *,
        tools: Callable[[str | None], Awaitable[types.ListToolsResult]],
    ) -> None:
        self._tools = tools
        self.calls: list[tuple[str, str | None]] = []

    def get_server_capabilities(self) -> types.ServerCapabilities:
        return types.ServerCapabilities(
            tools=types.ToolsCapability(),
            resources=types.ResourcesCapability(),
            prompts=types.PromptsCapability(),
        )

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult:
        self.calls.append(("tools", cursor))
        return await self._tools(cursor)

    async def list_resources(self, cursor: str | None = None) -> types.ListResourcesResult:
        self.calls.append(("resources", cursor))
        return types.ListResourcesResult(
            resources=[types.Resource(name="manual", uri=AnyUrl("file:///manual.txt"))]
        )

    async def list_resource_templates(
        self, cursor: str | None = None
    ) -> types.ListResourceTemplatesResult:
        self.calls.append(("resource_templates", cursor))
        return types.ListResourceTemplatesResult(
            resourceTemplates=[
                types.ResourceTemplate(name="issue", uriTemplate="https://example.test/{id}")
            ]
        )

    async def list_prompts(self, cursor: str | None = None) -> types.ListPromptsResult:
        self.calls.append(("prompts", cursor))
        return types.ListPromptsResult(
            prompts=[
                types.Prompt(
                    name="explain",
                    arguments=[types.PromptArgument(name="topic", required=True)],
                )
            ]
        )


class ReleasingPagingSession:
    def __init__(self) -> None:
        self._previous: weakref.ReferenceType[object] | None = None
        self.calls = 0

    def get_server_capabilities(self) -> types.ServerCapabilities:
        return types.ServerCapabilities(
            tools=types.ToolsCapability(),
            resources=types.ResourcesCapability(),
            prompts=types.PromptsCapability(),
        )

    def _check_previous(self) -> None:
        gc.collect()
        assert self._previous is None or self._previous() is None

    def _record(self, value: object) -> None:
        self._previous = weakref.ref(value)
        self.calls += 1

    @staticmethod
    def _meta() -> dict[str, object]:
        return {"ignored": "x" * 1_000_000}

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult:
        self._check_previous()
        result = types.ListToolsResult(
            tools=[
                types.Tool(
                    name=f"tool-{cursor or 'first'}",
                    inputSchema={"type": "object", "properties": {}},
                    _meta=self._meta(),
                )
            ],
            nextCursor="next" if cursor is None else None,
        )
        self._record(result.tools[0])
        return result

    async def list_resources(self, cursor: str | None = None) -> types.ListResourcesResult:
        self._check_previous()
        result = types.ListResourcesResult(
            resources=[
                types.Resource(
                    name=f"resource-{cursor or 'first'}",
                    uri=AnyUrl(f"file:///{cursor or 'first'}.txt"),
                    _meta=self._meta(),
                )
            ],
            nextCursor="next" if cursor is None else None,
        )
        self._record(result.resources[0])
        return result

    async def list_resource_templates(
        self, cursor: str | None = None
    ) -> types.ListResourceTemplatesResult:
        self._check_previous()
        result = types.ListResourceTemplatesResult(
            resourceTemplates=[
                types.ResourceTemplate(
                    name=f"template-{cursor or 'first'}",
                    uriTemplate=f"https://example.test/{cursor or 'first'}/{{id}}",
                    _meta=self._meta(),
                )
            ],
            nextCursor="next" if cursor is None else None,
        )
        self._record(result.resourceTemplates[0])
        return result

    async def list_prompts(self, cursor: str | None = None) -> types.ListPromptsResult:
        self._check_previous()
        result = types.ListPromptsResult(
            prompts=[
                types.Prompt(
                    name=f"prompt-{cursor or 'first'}",
                    _meta=self._meta(),
                )
            ],
            nextCursor="next" if cursor is None else None,
        )
        self._record(result.prompts[0])
        return result


@pytest.mark.asyncio
async def test_discovery_paginates_all_four_surfaces_and_canonicalizes() -> None:
    async def tools(cursor: str | None) -> types.ListToolsResult:
        if cursor is None:
            return types.ListToolsResult(
                tools=[
                    types.Tool(
                        name="zeta",
                        inputSchema={"type": "object", "properties": {}},
                    )
                ],
                nextCursor="page-2",
            )
        return types.ListToolsResult(
            tools=[
                types.Tool(
                    name="alpha",
                    inputSchema={"type": "object", "properties": {}},
                )
            ]
        )

    session = FakeSession(tools=tools)
    discovered = await discover_server_catalog("demo", session)

    assert [tool.raw_name for tool in discovered.tools] == ["alpha", "zeta"]
    assert discovered.resources[0].uri == "file:///manual.txt"
    assert discovered.prompts[0].arguments == [
        PromptArgument(name="topic", description=None, required=True)
    ]
    assert session.calls == [
        ("tools", None),
        ("tools", "page-2"),
        ("resources", None),
        ("resource_templates", None),
        ("prompts", None),
    ]


@pytest.mark.asyncio
async def test_discovery_releases_raw_metadata_after_each_projected_page() -> None:
    session = ReleasingPagingSession()

    discovered = await discover_server_catalog("demo", session)
    session._check_previous()

    assert discovered.capability_count == 8
    assert session.calls == 8


@pytest.mark.asyncio
async def test_discovery_rejects_repeated_cursor_without_partial_catalog() -> None:
    session = FakeSession(
        tools=lambda cursor: _value(types.ListToolsResult(tools=[], nextCursor="same"))
    )

    with pytest.raises(McpCatalogError, match="repeated cursor"):
        await discover_server_catalog("demo", session)


@pytest.mark.asyncio
async def test_discovery_rejects_an_invalid_json_schema() -> None:
    session = FakeSession(
        tools=lambda cursor: _value(
            types.ListToolsResult(
                tools=[
                    types.Tool(
                        name="broken",
                        inputSchema={"type": "definitely-not-a-json-schema-type"},
                    )
                ]
            )
        )
    )

    with pytest.raises(McpCatalogError, match="input schema"):
        await discover_server_catalog("demo", session)


def test_server_catalog_uses_py7_wrapping_and_reuses_entry_ids() -> None:
    config = ServerStdioMcpServerConfig(
        name="search",
        transport="stdio",
        command="search-mcp",
        enabled_capabilities=[],
    )
    source = SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name="search",
                tools=[
                    SourceMcpTool(
                        raw_name="Web Search",
                        description="Search",
                        input_schema={"type": "object", "properties": {}},
                    )
                ],
            )
        ],
    )
    first = build_server_persisted_catalog(
        (config,),
        source,
        entry_id_factory=_fixed_uuid,
    )
    second = build_server_persisted_catalog(
        (config,),
        source,
        existing_catalog=first,
        entry_id_factory=lambda: (_ for _ in ()).throw(AssertionError("must reuse")),
    )

    assert first.servers[0].entries[0].final_name == "mcp_search_web_search"
    assert first.servers[0].entries[0].enabled is True
    assert second == first


def test_resource_template_expansion_is_strict_rfc6570() -> None:
    assert str(expand_resource_template("https://example.test/{id}", {"id": "a b"})) == (
        "https://example.test/a%20b"
    )
    with pytest.raises(McpCatalogError, match="match its variables"):
        expand_resource_template("https://example.test/{id}", {})


def _fixed_uuid() -> UUID:
    return UUID("0198d6b8-03d6-7a1b-8f42-6c54fcbad921")
