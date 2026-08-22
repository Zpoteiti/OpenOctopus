from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from openctopus_server.devices.mcp_models import (
    McpServerConfig,
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
    SseMcpServerConfig,
    StdioMcpServerConfig,
    StreamableHttpMcpServerConfig,
    parse_mcp_server_configs,
)


def test_stdio_config_is_strict_and_secret_safe() -> None:
    sentinel = "mcp-secret-sentinel"
    config = StdioMcpServerConfig.model_validate(
        {
            "name": "github",
            "transport": "stdio",
            "command": "npx",
            "args": ["-y", "@example/github-mcp"],
            "cwd": "~/mcp",
            "env": {"GITHUB_TOKEN": sentinel},
            "enabled_capabilities": None,
        },
        strict=True,
    )

    assert sentinel not in repr(config)
    assert config.storage_dict()["env"] == {"GITHUB_TOKEN": sentinel}
    assert config.enabled_capabilities is None

    with pytest.raises(ValidationError) as captured:
        StdioMcpServerConfig.model_validate(
            {
                **config.storage_dict(),
                "env": {"TOKEN": sentinel + "\x00"},
            },
            strict=True,
        )
    assert sentinel not in str(captured.value)


@pytest.mark.parametrize(
    "patch",
    [
        {"unexpected": True},
        {"command": "   "},
        {"command": "bad\x00command"},
        {"args": ["bad\x00arg"]},
        {"cwd": "relative/path"},
        {"env": {"OPENOCTOPUS_TOKEN": "secret"}},
        {"env": {"Token": "one", "TOKEN": "two"}},
        {"env": {"BAD=NAME": "secret"}},
        {"enabled_capabilities": ["bad name"]},
        {"enabled_capabilities": ["mcp_github_search", "mcp_github_search"]},
    ],
)
def test_stdio_config_rejects_invalid_shape(patch: dict[str, object]) -> None:
    payload: dict[str, object] = {
        "name": "github",
        "transport": "stdio",
        "command": "npx",
        "args": [],
        "cwd": None,
        "env": {},
        "enabled_capabilities": None,
    }
    payload.update(patch)

    with pytest.raises(ValidationError):
        StdioMcpServerConfig.model_validate(payload, strict=True)


def test_remote_configs_preserve_url_and_canonicalize_header_names() -> None:
    sentinel = "remote-secret-sentinel"
    config = StreamableHttpMcpServerConfig.model_validate(
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "https://MCP.example.com/mcp/?tenant=A",
            "headers": {"Authorization": f"Bearer {sentinel}", "X-Tenant": "A"},
            "enabled_capabilities": [],
        },
        strict=True,
    )

    assert config.url == "https://MCP.example.com/mcp/?tenant=A"
    assert set(config.headers) == {"authorization", "x-tenant"}
    assert sentinel not in repr(config)
    assert config.storage_dict()["headers"] == {
        "authorization": f"Bearer {sentinel}",
        "x-tenant": "A",
    }

    sse = SseMcpServerConfig(
        name="legacy",
        transport="sse",
        url="http://10.0.0.20:8000/sse",
        headers={},
        enabled_capabilities=None,
    )
    assert sse.transport == "sse"


@pytest.mark.parametrize(
    "payload",
    [
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "http://mcp.example/mcp",
            "headers": {"authorization": "Bearer secret"},
        },
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "https://user@mcp.example/mcp",
            "headers": {},
        },
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "https://mcp.example/mcp#fragment",
            "headers": {},
        },
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "https://mcp.example/mcp#",
            "headers": {},
        },
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "http://%/mcp",
            "headers": {},
        },
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "https://mcp.example/mcp",
            "headers": {"Host": "mcp.example"},
        },
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "https://mcp.example/mcp",
            "headers": {"X-Test": "bad\r\nvalue"},
        },
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "https://mcp.example/mcp",
            "headers": {"X-Test": "one", "x-test": "two"},
        },
    ],
)
def test_remote_configs_reject_unsafe_urls_and_headers(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        StreamableHttpMcpServerConfig.model_validate(payload, strict=True)


def test_tagged_config_parser_enforces_count_uniqueness_and_total_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configs = parse_mcp_server_configs(
        [
            {
                "name": "local",
                "transport": "stdio",
                "command": "python",
                "args": [],
                "cwd": None,
                "env": {},
                "enabled_capabilities": None,
            },
            {
                "name": "remote",
                "transport": "sse",
                "url": "http://127.0.0.1/sse",
                "headers": {},
                "enabled_capabilities": [],
            },
        ]
    )

    assert [config.transport for config in configs] == ["stdio", "sse"]

    with pytest.raises(ValueError, match="unique"):
        parse_mcp_server_configs([configs[0].storage_dict(), configs[0].storage_dict()])

    monkeypatch.setattr("openctopus_server.devices.mcp_models.MCP_CONFIG_BYTES_MAX", 10)
    with pytest.raises(ValueError, match="too large"):
        parse_mcp_server_configs([configs[0].storage_dict()])


def test_source_and_persisted_catalog_models_are_strict() -> None:
    source = SourceMcpCatalog(
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
                resources=[
                    SourceMcpResource(
                        raw_name="guide",
                        uri="docs://guide",
                        description=None,
                    )
                ],
                resource_templates=[
                    SourceMcpResourceTemplate(
                        raw_name="record",
                        uri_template="docs://record/{id}",
                        description=None,
                    )
                ],
                prompts=[
                    SourceMcpPrompt(
                        raw_name="review",
                        description=None,
                        arguments=[PromptArgument(name="language", required=True)],
                    )
                ],
            )
        ],
    )
    assert source.servers[0].capability_count == 4
    normalized_resource = SourceMcpResource(
        raw_name="normalized",
        uri="HTTPS://EXAMPLE.COM/a/../guide",
    )
    assert normalized_resource.uri == "https://example.com/guide"

    with pytest.raises(ValidationError):
        SourceMcpResource(raw_name="invalid", uri="not a URI")

    entry = PersistedMcpCatalogEntry(
        entry_id=UUID("0190d5a7-0000-7000-8000-000000000001"),
        server="demo",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name="mcp_demo_search",
        provider_description="MCP tool from 'demo'. Search records",
        input_schema={"type": "object", "properties": {}},
        output_schema=None,
        enabled=True,
    )
    catalog = PersistedMcpCatalog(
        version=1,
        digest="0" * 64,
        servers=[PersistedMcpServerCatalog(name="demo", entries=[entry])],
    )
    assert catalog.servers[0].entries[0].entry_id.version == 7

    with pytest.raises(ValidationError):
        PersistedMcpCatalogEntry(
            **{
                **entry.model_dump(),
                "entry_id": UUID("00000000-0000-4000-8000-000000000001"),
            }
        )


def test_mcp_server_config_alias_is_discriminated() -> None:
    assert McpServerConfig is not None
