from __future__ import annotations

from uuid import UUID

import pytest

from openctopus_server.devices.mcp_catalog import McpCatalogError, with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    parse_mcp_server_configs,
)
from openctopus_server.mcp.reservation import validate_device_mcp_candidate

_ENTRY = UUID("01890f7c-bb80-7000-8000-000000000001")


def _configs(command: str = "old", *, name: str = "search"):
    return parse_mcp_server_configs(
        [{"name": name, "transport": "stdio", "command": command}]
    )


def _catalog(*, server: str = "search", final_name: str = "mcp_search_find"):
    return with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[
                PersistedMcpServerCatalog(
                    name=server,
                    entries=[
                        PersistedMcpCatalogEntry(
                            entry_id=_ENTRY,
                            server=server,
                            surface="tool",
                            raw_name="find",
                            invocation_identity="find",
                            final_name=final_name,
                            provider_description="Find.",
                            input_schema={"type": "object", "properties": {}},
                            enabled=True,
                        )
                    ],
                )
            ],
        )
    )


def test_reserved_name_rejects_add_or_effective_modify() -> None:
    with pytest.raises(McpCatalogError) as added:
        validate_device_mcp_candidate(
            current_configs=(),
            candidate_configs=_configs(),
            candidate_catalog=_catalog(),
            reserved_names={"search"},
            server_enabled_final_names=set(),
        )
    assert added.value.code == "mcp_name_reserved_by_server"

    with pytest.raises(McpCatalogError) as modified:
        validate_device_mcp_candidate(
            current_configs=_configs(),
            candidate_configs=_configs("new"),
            candidate_catalog=_catalog(),
            reserved_names={"search"},
            server_enabled_final_names=set(),
        )
    assert modified.value.code == "mcp_name_reserved_by_server"


def test_reserved_name_allows_unchanged_or_delete() -> None:
    validate_device_mcp_candidate(
        current_configs=_configs(),
        candidate_configs=_configs(),
        candidate_catalog=_catalog(),
        reserved_names={"search"},
        server_enabled_final_names=set(),
    )
    validate_device_mcp_candidate(
        current_configs=_configs(),
        candidate_configs=(),
        candidate_catalog=with_catalog_digest(
            PersistedMcpCatalog(version=1, digest="0" * 64, servers=[])
        ),
        reserved_names={"search"},
        server_enabled_final_names=set(),
    )


def test_exact_server_final_name_rejects_only_changed_device_server() -> None:
    with pytest.raises(McpCatalogError) as collision:
        validate_device_mcp_candidate(
            current_configs=(),
            candidate_configs=_configs(name="other"),
            candidate_catalog=_catalog(
                server="other",
                final_name="mcp_search_find",
            ),
            reserved_names={"search"},
            server_enabled_final_names={"mcp_search_find"},
        )
    assert collision.value.code == "mcp_schema_collision"

    validate_device_mcp_candidate(
        current_configs=_configs(name="other"),
        candidate_configs=_configs(name="other"),
        candidate_catalog=_catalog(server="other", final_name="mcp_search_find"),
        reserved_names={"search"},
        server_enabled_final_names={"mcp_search_find"},
    )
