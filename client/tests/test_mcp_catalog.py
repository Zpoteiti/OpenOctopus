from __future__ import annotations

from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import Literal
from uuid import UUID

import pytest
from mcp import types
from pydantic import AnyUrl, TypeAdapter

from openoctopus_client.mcp import catalog as catalog_module
from openoctopus_client.mcp.catalog import (
    EMPTY_CATALOG_DIGEST,
    McpCatalogError,
    bind_persisted_catalog,
    canonicalize_source_catalog,
    catalog_digest,
    discover_server_catalog,
    expand_resource_template,
    extract_resource_template_variables,
    normalized_resource_uri,
    with_catalog_digest,
)
from openoctopus_client.mcp.models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    PromptArgument,
    SourceMcpCatalog,
    SourceMcpPrompt,
    SourceMcpResource,
    SourceMcpResourceTemplate,
    SourceMcpServerCatalog,
    SourceMcpTool,
)


class FakeSession:
    def __init__(
        self,
        capabilities: types.ServerCapabilities,
        *,
        tools: Callable[[str | None], Awaitable[types.ListToolsResult]] | None = None,
        resources: Callable[[str | None], Awaitable[types.ListResourcesResult]] | None = None,
        templates: Callable[[str | None], Awaitable[types.ListResourceTemplatesResult]]
        | None = None,
        prompts: Callable[[str | None], Awaitable[types.ListPromptsResult]] | None = None,
    ) -> None:
        self.capabilities = capabilities
        self._tools = tools
        self._resources = resources
        self._templates = templates
        self._prompts = prompts
        self.calls: list[tuple[str, str | None]] = []

    def get_server_capabilities(self) -> types.ServerCapabilities | None:
        return self.capabilities

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult:
        self.calls.append(("tools", cursor))
        assert self._tools is not None
        return await self._tools(cursor)

    async def list_resources(self, cursor: str | None = None) -> types.ListResourcesResult:
        self.calls.append(("resources", cursor))
        assert self._resources is not None
        return await self._resources(cursor)

    async def list_resource_templates(
        self, cursor: str | None = None
    ) -> types.ListResourceTemplatesResult:
        self.calls.append(("resource_templates", cursor))
        assert self._templates is not None
        return await self._templates(cursor)

    async def list_prompts(self, cursor: str | None = None) -> types.ListPromptsResult:
        self.calls.append(("prompts", cursor))
        assert self._prompts is not None
        return await self._prompts(cursor)


async def _result[T](value: T) -> T:
    return value


@pytest.mark.asyncio
async def test_discovery_walks_all_advertised_surface_pages_and_normalizes_sdk_values() -> None:
    async def tools(cursor: str | None) -> types.ListToolsResult:
        if cursor is None:
            return types.ListToolsResult(
                tools=[
                    types.Tool(
                        name="zeta",
                        description="Last after canonical ordering",
                        inputSchema={"type": "object", "properties": {}},
                    )
                ],
                nextCursor="tools-2",
            )
        assert cursor == "tools-2"
        return types.ListToolsResult(
            tools=[
                types.Tool(
                    name="alpha",
                    inputSchema={
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"],
                    },
                    outputSchema={"type": "object"},
                )
            ]
        )

    session = FakeSession(
        types.ServerCapabilities(
            tools=types.ToolsCapability(),
            resources=types.ResourcesCapability(),
            prompts=types.PromptsCapability(),
        ),
        tools=tools,
        resources=lambda cursor: _result(
            types.ListResourcesResult(
                resources=[
                    types.Resource(
                        name="guide",
                        uri=TypeAdapter(AnyUrl).validate_python(
                            "HTTPS://EXAMPLE.COM/a/../guide",
                            strict=True,
                        ),
                        description="Guide",
                    )
                ]
            )
        ),
        templates=lambda cursor: _result(
            types.ListResourceTemplatesResult(
                resourceTemplates=[
                    types.ResourceTemplate(
                        name="record",
                        uriTemplate="docs://record/{id}",
                    )
                ]
            )
        ),
        prompts=lambda cursor: _result(
            types.ListPromptsResult(
                prompts=[
                    types.Prompt(
                        name="review",
                        arguments=[types.PromptArgument(name="language", required=True)],
                    )
                ]
            )
        ),
    )

    discovered = await discover_server_catalog("demo", session)

    assert [tool.raw_name for tool in discovered.tools] == ["alpha", "zeta"]
    assert discovered.resources[0].uri == "https://example.com/guide"
    assert discovered.resource_templates[0].uri_template == "docs://record/{id}"
    assert discovered.prompts[0].arguments == [
        PromptArgument(name="language", description=None, required=True)
    ]
    assert session.calls == [
        ("tools", None),
        ("tools", "tools-2"),
        ("resources", None),
        ("resource_templates", None),
        ("prompts", None),
    ]


@pytest.mark.asyncio
async def test_discovery_skips_unadvertised_surfaces() -> None:
    session = FakeSession(
        types.ServerCapabilities(tools=types.ToolsCapability()),
        tools=lambda cursor: _result(types.ListToolsResult(tools=[])),
    )

    discovered = await discover_server_catalog("demo", session)

    assert discovered.capability_count == 0
    assert session.calls == [("tools", None)]


@pytest.mark.asyncio
async def test_discovery_rejects_repeated_cursor_page_and_capability_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeSession(
        types.ServerCapabilities(tools=types.ToolsCapability()),
        tools=lambda cursor: _result(types.ListToolsResult(tools=[], nextCursor="same")),
    )
    with pytest.raises(McpCatalogError, match="cursor"):
        await discover_server_catalog("demo", session)

    monkeypatch.setattr(catalog_module, "CAPABILITY_BYTES_MAX", 10)
    oversized = FakeSession(
        types.ServerCapabilities(tools=types.ToolsCapability()),
        tools=lambda cursor: _result(
            types.ListToolsResult(
                tools=[
                    types.Tool(
                        name="large",
                        description="x" * 20,
                        inputSchema={"type": "object", "properties": {}},
                    )
                ]
            )
        ),
    )
    with pytest.raises(McpCatalogError, match="256 KiB"):
        await discover_server_catalog("demo", oversized)


@pytest.mark.asyncio
async def test_discovery_enforces_page_cursor_and_cross_surface_item_limits_early() -> None:
    async def endless_tools(cursor: str | None) -> types.ListToolsResult:
        page = 0 if cursor is None else int(cursor)
        return types.ListToolsResult(tools=[], nextCursor=str(page + 1))

    pages = FakeSession(
        types.ServerCapabilities(tools=types.ToolsCapability()),
        tools=endless_tools,
    )
    with pytest.raises(McpCatalogError, match="16 pages"):
        await discover_server_catalog("demo", pages)
    assert len(pages.calls) == 16

    long_cursor = FakeSession(
        types.ServerCapabilities(tools=types.ToolsCapability()),
        tools=lambda cursor: _result(
            types.ListToolsResult(tools=[], nextCursor="界" * 1366)
        ),
    )
    with pytest.raises(McpCatalogError, match="4096 bytes"):
        await discover_server_catalog("demo", long_cursor)

    tools = [
        types.Tool(
            name=f"tool_{index}",
            inputSchema={"type": "object", "properties": {}},
        )
        for index in range(256)
    ]

    async def templates_must_not_run(
        cursor: str | None,
    ) -> types.ListResourceTemplatesResult:
        del cursor
        raise AssertionError("discovery must stop as soon as the total item limit is exceeded")

    total = FakeSession(
        types.ServerCapabilities(
            tools=types.ToolsCapability(),
            resources=types.ResourcesCapability(),
        ),
        tools=lambda cursor: _result(types.ListToolsResult(tools=tools)),
        resources=lambda cursor: _result(
            types.ListResourcesResult(
                resources=[
                    types.Resource(
                        name="extra",
                        uri=TypeAdapter(AnyUrl).validate_python(
                            "docs://extra",
                            strict=True,
                        ),
                    )
                ]
            )
        ),
        templates=templates_must_not_run,
    )
    with pytest.raises(McpCatalogError, match="256"):
        await discover_server_catalog("demo", total)
    assert ("resource_templates", None) not in total.calls


@pytest.mark.parametrize(
    ("template", "variables"),
    [
        ("docs://record/{id}", ("id",)),
        ("https://example/{+path}{?q,lang:2}{&q}", ("path", "q", "lang")),
        ("urn:test/{list*}/{user.name}/{encoded%20name}", ("list", "user.name", "encoded%20name")),
    ],
)
def test_resource_template_uses_strict_rfc6570_scanner_and_uritemplate(
    template: str,
    variables: tuple[str, ...],
) -> None:
    assert extract_resource_template_variables(template) == variables
    values = {variable: "value" for variable in variables}
    assert isinstance(expand_resource_template(template, values), AnyUrl)


def test_resource_uri_helper_returns_sdk_anyurl_normalization() -> None:
    uri = normalized_resource_uri("HTTPS://EXAMPLE.COM/a/../guide")

    assert isinstance(uri, AnyUrl)
    assert str(uri) == "https://example.com/guide"


@pytest.mark.parametrize(
    "template",
    [
        "docs://record/{",
        "docs://record/}",
        "docs://record/{}",
        "docs://record/{{id}}",
        "docs://record/{ id}",
        "docs://record/{!id}",
        "docs://record/{id,}",
        "docs://record/{id**}",
        "docs://record/{id:0}",
        "docs://record/{id:10000}",
        "docs://record/{bad%2G}",
        "docs://record/{a..b}",
    ],
)
def test_resource_template_rejects_invalid_syntax(template: str) -> None:
    with pytest.raises(McpCatalogError):
        extract_resource_template_variables(template)


def test_canonical_projection_rejects_nonfinite_deep_and_non_utf8_json() -> None:
    for invalid in (float("nan"), "\ud800"):
        source = SourceMcpCatalog(
            version=1,
            servers=[
                SourceMcpServerCatalog(
                    name="demo",
                    tools=[
                        SourceMcpTool(
                            raw_name="bad",
                            input_schema={
                                "type": "object",
                                "properties": {"value": {"const": invalid}},
                            },
                        )
                    ],
                )
            ],
        )
        with pytest.raises(McpCatalogError):
            canonicalize_source_catalog(source)

    nested: object = "leaf"
    for _ in range(33):
        nested = [nested]
    deep = SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name="demo",
                tools=[
                    SourceMcpTool(
                        raw_name="deep",
                        input_schema={
                            "type": "object",
                            "properties": {"value": {"const": nested}},
                        },
                    )
                ],
            )
        ],
    )
    with pytest.raises(McpCatalogError, match="depth"):
        canonicalize_source_catalog(deep)


def _source_catalog() -> SourceMcpCatalog:
    return SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name="demo",
                tools=[
                    SourceMcpTool(
                        raw_name="search",
                        description="Search records",
                        input_schema={"type": "object", "properties": {}},
                    )
                ],
                resources=[SourceMcpResource(raw_name="guide", uri="docs://guide")],
                resource_templates=[
                    SourceMcpResourceTemplate(
                        raw_name="record",
                        uri_template="docs://record/{id}",
                    )
                ],
                prompts=[SourceMcpPrompt(raw_name="review")],
            )
        ],
    )


def test_source_catalog_enforces_device_count_and_total_byte_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def server(name: str, count: int) -> SourceMcpServerCatalog:
        return SourceMcpServerCatalog(
            name=name,
            tools=[
                SourceMcpTool(
                    raw_name=f"tool_{index}",
                    input_schema={"type": "object", "properties": {}},
                )
                for index in range(count)
            ],
        )

    exact = SourceMcpCatalog(
        version=1,
        servers=[server("one", 256), server("two", 256)],
    )
    assert sum(item.capability_count for item in canonicalize_source_catalog(exact).servers) == 512

    exceeded = SourceMcpCatalog(
        version=1,
        servers=[*exact.servers, server("three", 1)],
    )
    with pytest.raises(McpCatalogError, match="512"):
        canonicalize_source_catalog(exceeded)

    monkeypatch.setattr(catalog_module, "DEVICE_CATALOG_BYTES_MAX", 10)
    with pytest.raises(McpCatalogError, match="byte limit"):
        canonicalize_source_catalog(SourceMcpCatalog(version=1, servers=[]))


def _persisted_catalog() -> PersistedMcpCatalog:
    surface_routes: list[
        tuple[Literal["tool", "resource", "resource_template", "prompt"], str, str]
    ] = [
        ("tool", "search", "search"),
        ("resource", "guide", "docs://guide"),
        ("resource_template", "record", "docs://record/{id}"),
        ("prompt", "review", "review"),
    ]
    entries = [
        PersistedMcpCatalogEntry(
            entry_id=UUID(f"0190d5a7-0000-7000-8000-{index:012d}"),
            server="demo",
            surface=surface,
            raw_name=raw_name,
            invocation_identity=identity,
            final_name=f"mcp_demo_{raw_name}",
            provider_description=f"MCP {surface} from 'demo'.",
            input_schema={"type": "object", "properties": {}},
            output_schema=None,
            enabled=True,
        )
        for index, (surface, raw_name, identity) in enumerate(
            surface_routes,
            start=1,
        )
    ]
    return with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[PersistedMcpServerCatalog(name="demo", entries=list(reversed(entries)))],
        )
    )


def test_source_projection_is_canonical_and_persisted_digest_maps_server_entry_ids() -> None:
    source = _source_catalog()
    source.servers[0].tools.reverse()
    canonical = canonicalize_source_catalog(source)
    persisted = _persisted_catalog()

    assert canonical.servers[0].capability_count == 4
    assert catalog_digest(
        PersistedMcpCatalog(version=1, digest="0" * 64, servers=[])
    ) == EMPTY_CATALOG_DIGEST
    assert persisted.digest == "c3b9e60ae00fe37e5c796ed376acf65989e39108e0d56e9d0fa3104a6c74f903"

    routes = bind_persisted_catalog(canonical, persisted)
    assert len(routes) == 4
    assert {route.surface for route in routes.values()} == {
        "tool",
        "resource",
        "resource_template",
        "prompt",
    }

    changed_ids = deepcopy(persisted)
    changed_ids.servers[0].entries[0].entry_id = UUID(
        "0190d5a7-0000-7000-8000-000000000999"
    )
    assert catalog_digest(changed_ids) == persisted.digest

    drifted = deepcopy(persisted)
    drifted.servers[0].entries[0].invocation_identity = "different"
    drifted = with_catalog_digest(drifted)
    with pytest.raises(McpCatalogError, match="source identity"):
        bind_persisted_catalog(canonical, drifted)

    bad_digest = deepcopy(persisted)
    bad_digest.digest = "f" * 64
    with pytest.raises(McpCatalogError, match="digest"):
        bind_persisted_catalog(canonical, bad_digest)
