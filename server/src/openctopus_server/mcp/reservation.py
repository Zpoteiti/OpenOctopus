from __future__ import annotations

from collections.abc import Collection, Sequence

from openctopus_server.devices.mcp_catalog import McpCatalogError
from openctopus_server.devices.mcp_models import McpServerConfig, PersistedMcpCatalog


def _changed_candidate_names(
    current: Sequence[McpServerConfig],
    candidate: Sequence[McpServerConfig],
) -> set[str]:
    current_by_name = {config.name: config.storage_dict() for config in current}
    return {
        config.name
        for config in candidate
        if current_by_name.get(config.name) != config.storage_dict()
    }


def validate_device_mcp_candidate(
    *,
    current_configs: Sequence[McpServerConfig],
    candidate_configs: Sequence[McpServerConfig],
    candidate_catalog: PersistedMcpCatalog,
    reserved_names: Collection[str],
    server_enabled_final_names: Collection[str],
) -> None:
    """Apply Admin-first namespace rules to one Device candidate.

    Only additions and effective modifications are rejected. Exact unchanged
    configs and deletions remain legal so an Admin reservation never traps a
    user's existing Device configuration.
    """

    changed_names = _changed_candidate_names(current_configs, candidate_configs)
    reserved_change = sorted(changed_names.intersection(reserved_names))
    if reserved_change:
        raise McpCatalogError(
            "mcp_name_reserved_by_server",
            f"MCP server name is reserved by the administrator: {reserved_change[0]}",
        )

    server_names = set(server_enabled_final_names)
    for server in candidate_catalog.servers:
        if server.name not in changed_names:
            continue
        if any(entry.enabled and entry.final_name in server_names for entry in server.entries):
            raise McpCatalogError(
                "mcp_schema_collision",
                "Device MCP capability collides with an administrator Server MCP capability",
            )
