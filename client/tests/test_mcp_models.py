from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

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
    StdioMcpServerConfig,
    StreamableHttpMcpServerConfig,
    parse_mcp_server_configs,
)


def test_config_wire_shape_matches_server_and_secrets_are_repr_safe() -> None:
    secret = "client-mcp-secret-sentinel"
    config = StdioMcpServerConfig.model_validate(
        {
            "name": "local",
            "transport": "stdio",
            "command": "python",
            "args": ["-m", "fake_mcp"],
            "cwd": "~/mcp",
            "env": {"TOKEN": secret},
            "enabled_capabilities": None,
        },
        strict=True,
    )

    assert secret not in repr(config)
    assert config.storage_dict() == {
        "name": "local",
        "enabled_capabilities": None,
        "transport": "stdio",
        "command": "python",
        "args": ["-m", "fake_mcp"],
        "cwd": "~/mcp",
        "env": {"TOKEN": secret},
    }

    parsed = parse_mcp_server_configs([config.storage_dict()])
    assert parsed == (config,)


def test_remote_config_canonicalizes_headers_and_rejects_unsafe_wire_fields() -> None:
    config = StreamableHttpMcpServerConfig.model_validate(
        {
            "name": "corp",
            "transport": "streamable_http",
            "url": "https://MCP.example.com/mcp/?tenant=A",
            "headers": {"Authorization": "Bearer secret", "X-Tenant": "A"},
            "enabled_capabilities": [],
        },
        strict=True,
    )
    assert config.url == "https://MCP.example.com/mcp/?tenant=A"
    assert config.storage_dict()["headers"] == {
        "authorization": "Bearer secret",
        "x-tenant": "A",
    }

    with pytest.raises(ValidationError):
        StreamableHttpMcpServerConfig.model_validate(
            {**config.storage_dict(), "unexpected": True},
            strict=True,
        )
    with pytest.raises(ValidationError):
        StreamableHttpMcpServerConfig.model_validate(
            {**config.storage_dict(), "headers": {"host": "example.com"}},
            strict=True,
        )


def test_source_and_persisted_catalog_wire_shapes_are_strict_and_normalized() -> None:
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
                        output_schema={"type": "object"},
                    )
                ],
                resources=[
                    SourceMcpResource(
                        raw_name="guide",
                        uri="HTTPS://EXAMPLE.COM/a/../guide",
                        description=None,
                    )
                ],
                resource_templates=[
                    SourceMcpResourceTemplate(
                        raw_name="record",
                        uri_template="docs://record/{id}",
                    )
                ],
                prompts=[
                    SourceMcpPrompt(
                        raw_name="review",
                        arguments=[PromptArgument(name="language", required=True)],
                    )
                ],
            )
        ],
    )

    assert source.servers[0].resources[0].uri == "https://example.com/guide"
    assert source.model_dump(mode="json", exclude_none=True) == {
        "version": 1,
        "servers": [
            {
                "name": "demo",
                "tools": [
                    {
                        "raw_name": "search",
                        "description": "Search records",
                        "input_schema": {"type": "object", "properties": {}},
                        "output_schema": {"type": "object"},
                    }
                ],
                "resources": [{"raw_name": "guide", "uri": "https://example.com/guide"}],
                "resource_templates": [
                    {"raw_name": "record", "uri_template": "docs://record/{id}"}
                ],
                "prompts": [
                    {
                        "raw_name": "review",
                        "arguments": [{"name": "language", "required": True}],
                    }
                ],
            }
        ],
    }

    entry = PersistedMcpCatalogEntry(
        entry_id=UUID("0190d5a7-0000-7000-8000-000000000001"),
        server="demo",
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name="mcp_demo_search",
        provider_description="MCP tool from 'demo'. Search records",
        input_schema={"type": "object", "properties": {}},
        output_schema={"type": "object"},
        enabled=True,
    )
    persisted = PersistedMcpCatalog(
        version=1,
        digest="0" * 64,
        servers=[PersistedMcpServerCatalog(name="demo", entries=[entry])],
    )
    assert persisted.servers[0].entries[0].entry_id.version == 7

    with pytest.raises(ValidationError):
        PersistedMcpCatalogEntry.model_validate(
            {**entry.model_dump(mode="python"), "unexpected": True},
            strict=True,
        )
    with pytest.raises(ValidationError):
        SourceMcpResource(raw_name="bad", uri="not a URI")
