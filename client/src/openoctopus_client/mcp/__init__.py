"""Bounded MCP catalog and transport primitives for the OpenOctopus client."""

from openoctopus_client.mcp.catalog import (
    McpCatalogError,
    McpEntryRoute,
    bind_persisted_catalog,
    bind_server_entries,
    canonical_json_bytes,
    canonicalize_source_catalog,
    catalog_digest,
    discover_server_catalog,
    expand_resource_template,
    extract_resource_template_variables,
    normalized_resource_uri,
    validate_persisted_catalog,
)
from openoctopus_client.mcp.models import (
    McpServerConfig,
    PersistedMcpCatalog,
    SourceMcpCatalog,
    SseMcpServerConfig,
    StdioMcpServerConfig,
    StreamableHttpMcpServerConfig,
    parse_mcp_server_configs,
)

__all__ = [
    "McpCatalogError",
    "McpEntryRoute",
    "McpServerConfig",
    "PersistedMcpCatalog",
    "SseMcpServerConfig",
    "SourceMcpCatalog",
    "StdioMcpServerConfig",
    "StreamableHttpMcpServerConfig",
    "bind_persisted_catalog",
    "bind_server_entries",
    "canonical_json_bytes",
    "canonicalize_source_catalog",
    "catalog_digest",
    "discover_server_catalog",
    "expand_resource_template",
    "extract_resource_template_variables",
    "normalized_resource_uri",
    "parse_mcp_server_configs",
    "validate_persisted_catalog",
]
