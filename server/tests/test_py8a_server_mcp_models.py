from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from openctopus_server.devices.mcp_catalog import EMPTY_CATALOG_DIGEST, with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    StdioMcpServerConfig,
)
from openctopus_server.mcp.models import (
    ServerMcpEnvelope,
    ServerSseMcpServerConfig,
    ServerStdioMcpServerConfig,
    ServerStreamableHttpMcpServerConfig,
    empty_server_mcp_envelope,
    parse_server_mcp_configs,
    redacted_server_mcp_configs,
    resolve_server_mcp_secret_markers,
    server_mcp_envelope_storage,
)


def _empty_catalog() -> dict[str, object]:
    return {"version": 1, "digest": EMPTY_CATALOG_DIGEST, "servers": []}


def test_server_mcp_transport_defaults_are_canonical() -> None:
    stdio = ServerStdioMcpServerConfig(
        name="calc",
        transport="stdio",
        command="python",
    )
    streamable = ServerStreamableHttpMcpServerConfig(
        name="search",
        transport="streamable_http",
        url="https://mcp.example/mcp",
    )
    sse = ServerSseMcpServerConfig(
        name="legacy",
        transport="sse",
        url="https://mcp.example/sse",
    )

    assert stdio.max_concurrent_calls == 1
    assert streamable.max_concurrent_calls == 8
    assert sse.max_concurrent_calls == 8
    assert [item["max_concurrent_calls"] for item in redacted_server_mcp_configs((stdio, streamable, sse))] == [
        1,
        8,
        8,
    ]


@pytest.mark.parametrize("value", [True, False, 0, 33])
def test_server_mcp_concurrency_rejects_bool_and_out_of_range(value: object) -> None:
    with pytest.raises(ValidationError):
        ServerStdioMcpServerConfig.model_validate(
            {
                "name": "calc",
                "transport": "stdio",
                "command": "python",
                "max_concurrent_calls": value,
            },
            strict=True,
        )


def test_device_mcp_config_does_not_accept_server_concurrency() -> None:
    with pytest.raises(ValidationError):
        StdioMcpServerConfig.model_validate(
            {
                "name": "calc",
                "transport": "stdio",
                "command": "python",
                "max_concurrent_calls": 1,
            },
            strict=True,
        )


def test_server_mcp_config_parser_enforces_unique_names() -> None:
    with pytest.raises(ValueError, match="unique"):
        parse_server_mcp_configs(
            [
                {"name": "calc", "transport": "stdio", "command": "python"},
                {"name": "calc", "transport": "stdio", "command": "python3"},
            ]
        )


def test_server_mcp_config_parser_canonicalizes_whole_list_order() -> None:
    configs = parse_server_mcp_configs(
        [
            {
                "name": "search",
                "transport": "streamable_http",
                "url": "https://mcp.example/mcp",
            },
            {"name": "calc", "transport": "stdio", "command": "python"},
        ]
    )

    assert [config.name for config in configs] == ["calc", "search"]


def test_secret_marker_retains_only_the_same_stdio_sink() -> None:
    current = parse_server_mcp_configs(
        [
            {
                "name": "calc",
                "transport": "stdio",
                "command": "python",
                "args": ["server.py"],
                "env": {"TOKEN": "secret"},
                "enabled_capabilities": [],
            }
        ]
    )
    candidate = parse_server_mcp_configs(
        [
            {
                "name": "calc",
                "transport": "stdio",
                "command": "python",
                "args": ["server.py"],
                "env": {"TOKEN": "<redacted>"},
                "enabled_capabilities": None,
                "max_concurrent_calls": 2,
            }
        ]
    )

    resolved = resolve_server_mcp_secret_markers(current, candidate)

    assert resolved[0].storage_dict()["env"] == {"TOKEN": "secret"}
    assert resolved[0].enabled_capabilities is None
    assert resolved[0].max_concurrent_calls == 2

    changed_sink = parse_server_mcp_configs(
        [
            {
                "name": "calc",
                "transport": "stdio",
                "command": "python3",
                "args": ["server.py"],
                "env": {"TOKEN": "<redacted>"},
            }
        ]
    )
    with pytest.raises(ValueError, match="same key at the same sink"):
        resolve_server_mcp_secret_markers(current, changed_sink)


def test_secret_marker_retains_only_the_exact_remote_sink_and_key() -> None:
    current = parse_server_mcp_configs(
        [
            {
                "name": "search",
                "transport": "streamable_http",
                "url": "https://mcp.example/mcp?tenant=A",
                "headers": {"Authorization": "Bearer secret"},
            }
        ]
    )
    retained = parse_server_mcp_configs(
        [
            {
                "name": "search",
                "transport": "streamable_http",
                "url": "https://mcp.example/mcp?tenant=A",
                "headers": {"authorization": "<redacted>"},
            }
        ]
    )
    resolved = resolve_server_mcp_secret_markers(current, retained)
    assert resolved[0].storage_dict()["headers"] == {"authorization": "Bearer secret"}

    changed_url = parse_server_mcp_configs(
        [
            {
                "name": "search",
                "transport": "streamable_http",
                "url": "https://mcp.example/mcp?tenant=B",
                "headers": {"authorization": "<redacted>"},
            }
        ]
    )
    with pytest.raises(ValueError, match="same key at the same sink"):
        resolve_server_mcp_secret_markers(current, changed_url)


def test_redaction_and_storage_have_opposite_secret_projections() -> None:
    configs = parse_server_mcp_configs(
        [
            {
                "name": "calc",
                "transport": "stdio",
                "command": "python",
                "env": {"TOKEN": "secret"},
            },
            {
                "name": "search",
                "transport": "streamable_http",
                "url": "https://mcp.example/mcp",
                "headers": {"authorization": "Bearer secret"},
            },
        ]
    )
    catalog = with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[
                PersistedMcpServerCatalog(name="calc", entries=[]),
                PersistedMcpServerCatalog(name="search", entries=[]),
            ],
        )
    )
    envelope = ServerMcpEnvelope(
        version=1,
        config_revision=4,
        mcp_servers=list(configs),
        mcp_catalog=catalog,
    )

    assert redacted_server_mcp_configs(configs)[0]["env"] == {"TOKEN": "<redacted>"}
    assert redacted_server_mcp_configs(configs)[1]["headers"] == {
        "authorization": "<redacted>"
    }
    stored = server_mcp_envelope_storage(envelope)
    assert stored["mcp_servers"][0]["env"] == {"TOKEN": "secret"}  # type: ignore[index]
    assert stored["mcp_servers"][1]["headers"] == {  # type: ignore[index]
        "authorization": "Bearer secret"
    }


def test_nonempty_config_requires_matching_catalog_even_when_disabled() -> None:
    with pytest.raises(ValidationError, match="catalog"):
        ServerMcpEnvelope.model_validate(
            {
                "version": 1,
                "config_revision": 2,
                "mcp_servers": [
                    {
                        "name": "calc",
                        "transport": "stdio",
                        "command": "python",
                    }
                ],
                "mcp_catalog": _empty_catalog(),
            },
            strict=True,
        )


def test_empty_envelope_is_canonical_and_storage_keeps_plain_secrets() -> None:
    empty = empty_server_mcp_envelope()
    assert empty.config_revision == 1
    assert empty.mcp_servers == []
    assert empty.mcp_catalog.digest == EMPTY_CATALOG_DIGEST
    assert server_mcp_envelope_storage(empty) == {
        "version": 1,
        "config_revision": 1,
        "mcp_servers": [],
        "mcp_catalog": _empty_catalog(),
    }


def test_envelope_is_strict_and_rejects_catalog_corruption() -> None:
    with pytest.raises(ValidationError):
        ServerMcpEnvelope.model_validate(
            {
                "version": 1,
                "config_revision": 1,
                "mcp_servers": [],
                "mcp_catalog": _empty_catalog(),
                "unknown": True,
            },
            strict=True,
        )


def test_envelope_rejects_invalid_catalog_content_with_a_valid_digest() -> None:
    config = parse_server_mcp_configs(
        [
            {
                "name": "search",
                "transport": "streamable_http",
                "url": "https://mcp.example/mcp",
                "enabled_capabilities": [],
            }
        ]
    )
    invalid = with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[
                PersistedMcpServerCatalog(
                    name="search",
                    entries=[
                        PersistedMcpCatalogEntry(
                            entry_id=UUID("01890f7c-bb80-7000-8000-000000000011"),
                            server="search",
                            surface="tool",
                            raw_name="query",
                            invocation_identity="query",
                            final_name="mcp_search_query",
                            provider_description="MCP tool from 'search'.",
                            input_schema={
                                "type": "definitely-not-a-json-schema-type",
                                "properties": {},
                            },
                            enabled=True,
                        )
                    ],
                )
            ],
        )
    )

    with pytest.raises(ValidationError, match="catalog content"):
        ServerMcpEnvelope(
            version=1,
            config_revision=2,
            mcp_servers=list(config),
            mcp_catalog=invalid,
        )

    corrupt = _empty_catalog()
    corrupt["digest"] = "0" * 64
    with pytest.raises(ValidationError, match="digest"):
        ServerMcpEnvelope.model_validate(
            {
                "version": 1,
                "config_revision": 1,
                "mcp_servers": [],
                "mcp_catalog": corrupt,
            },
            strict=True,
        )
