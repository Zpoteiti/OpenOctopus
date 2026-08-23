from __future__ import annotations

from uuid import UUID

import pytest

from openctopus_server.devices.mcp_catalog import McpCatalogError, with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
)
from openctopus_server.devices.mcp_routes import OwnerMcpDevice
from openctopus_server.mcp.models import ServerMcpEnvelope, parse_server_mcp_configs
from openctopus_server.mcp.routes import (
    ServerMcpRouteSelectionError,
    build_composite_mcp_snapshot,
    select_composite_mcp_call,
)

_DEVICE_ONE = UUID("01890f7c-bb80-7000-8000-000000000001")
_DEVICE_TWO = UUID("01890f7c-bb80-7000-8000-000000000002")
_SERVER_ENTRY = UUID("01890f7c-bb80-7000-8000-000000000011")
_DEVICE_ENTRY = UUID("01890f7c-bb80-7000-8000-000000000012")
_PREFIX_ENTRY = UUID("01890f7c-bb80-7000-8000-000000000013")
_COLLISION_ENTRY = UUID("01890f7c-bb80-7000-8000-000000000014")
_GENERATION = UUID("01890f7c-bb80-7000-8000-000000000021")


def _schema(*, padding: int = 0) -> dict[str, object]:
    return {
        "type": "object",
        "properties": {"query": {"type": "string", "description": "x" * padding}},
        "required": ["query"],
        "additionalProperties": False,
    }


def _entry(
    *,
    entry_id: UUID,
    server: str,
    final_name: str,
    raw_name: str = "search",
    padding: int = 0,
    enabled: bool = True,
) -> PersistedMcpCatalogEntry:
    return PersistedMcpCatalogEntry(
        entry_id=entry_id,
        server=server,
        surface="tool",
        raw_name=raw_name,
        invocation_identity=raw_name,
        final_name=final_name,
        provider_description=f"MCP tool from '{server}'.",
        input_schema=_schema(padding=padding),
        enabled=enabled,
    )


def _catalog(*entries: PersistedMcpCatalogEntry) -> PersistedMcpCatalog:
    grouped: dict[str, list[PersistedMcpCatalogEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.server, []).append(entry)
    return with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[
                PersistedMcpServerCatalog(name=name, entries=values)
                for name, values in sorted(grouped.items())
            ],
        )
    )


def _envelope(
    *,
    revision: int = 7,
    name: str = "search",
    entries: tuple[PersistedMcpCatalogEntry, ...] | None = None,
) -> ServerMcpEnvelope:
    server_entries = entries or (
        _entry(
            entry_id=_SERVER_ENTRY,
            server=name,
            final_name=f"mcp_{name}_search",
        ),
    )
    return ServerMcpEnvelope(
        version=1,
        config_revision=revision,
        mcp_servers=list(
            parse_server_mcp_configs(
                [
                    {
                        "name": name,
                        "transport": "streamable_http",
                        "url": "https://mcp.example/mcp",
                        "enabled_capabilities": [],
                    }
                ]
            )
        ),
        mcp_catalog=_catalog(*server_entries),
    )


def _owner(
    *entries: PersistedMcpCatalogEntry,
    device_id: UUID = _DEVICE_ONE,
    name: str = "laptop",
) -> OwnerMcpDevice:
    return OwnerMcpDevice(
        device_id=device_id,
        name=name,
        config_revision=3,
        catalog=_catalog(*entries),
    )


def test_server_schema_is_first_and_available_even_without_runtime() -> None:
    envelope = _envelope()

    snapshot = build_composite_mcp_snapshot(envelope, [])

    assert [schema.name for schema in snapshot.schemas] == ["mcp_search_search"]
    selector = snapshot.schemas[0].input_schema["properties"]["openoctopus_device"]
    assert selector["enum"] == ["server"]
    assert snapshot.server_routes[0].runtime_generation is None
    assert snapshot.server_routes[0].config_revision == 7


def test_structured_reservation_shadows_exact_server_name_only() -> None:
    shadowed = _entry(
        entry_id=_DEVICE_ENTRY,
        server="search",
        final_name="mcp_search_search",
    )
    prefix = _entry(
        entry_id=_PREFIX_ENTRY,
        server="search_v2",
        final_name="mcp_search_v2_search",
    )

    snapshot = build_composite_mcp_snapshot(_envelope(), [_owner(shadowed, prefix)])

    assert [schema.name for schema in snapshot.schemas] == [
        "mcp_search_search",
        "mcp_search_v2_search",
    ]
    assert snapshot.suppression_by_entry[(_DEVICE_ONE, _DEVICE_ENTRY)] == (
        "server_namespace_reserved"
    )
    assert (_DEVICE_ONE, _PREFIX_ENTRY) not in snapshot.suppression_by_entry
    assert [route.entry_id for route in snapshot.device_routes] == [_PREFIX_ENTRY]


def test_server_exact_final_name_wins_over_other_device_namespace() -> None:
    server_entry = _entry(
        entry_id=_SERVER_ENTRY,
        server="foo",
        raw_name="bar_baz",
        final_name="mcp_foo_bar_baz",
    )
    collision = _entry(
        entry_id=_DEVICE_ENTRY,
        server="foo_bar",
        raw_name="baz",
        final_name="mcp_foo_bar_baz",
    )

    snapshot = build_composite_mcp_snapshot(
        _envelope(name="foo", entries=(server_entry,)),
        [_owner(collision)],
    )

    assert [schema.name for schema in snapshot.schemas] == ["mcp_foo_bar_baz"]
    assert snapshot.suppression_by_entry[(_DEVICE_ONE, _DEVICE_ENTRY)] == (
        "server_final_name_collision"
    )
    assert snapshot.device_routes == ()


def test_server_final_name_wins_before_conflicting_device_schemas_are_merged() -> None:
    server_entry = _entry(
        entry_id=_SERVER_ENTRY,
        server="foo",
        raw_name="bar_baz",
        final_name="mcp_foo_bar_baz",
    )
    first = _entry(
        entry_id=_DEVICE_ENTRY,
        server="foo_bar",
        raw_name="baz",
        final_name="mcp_foo_bar_baz",
    )
    second = _entry(
        entry_id=_COLLISION_ENTRY,
        server="foo_bar",
        raw_name="baz",
        final_name="mcp_foo_bar_baz",
        padding=1,
    )

    snapshot = build_composite_mcp_snapshot(
        _envelope(name="foo", entries=(server_entry,)),
        [
            _owner(first),
            _owner(second, device_id=_DEVICE_TWO, name="desktop"),
        ],
    )

    assert [schema.name for schema in snapshot.schemas] == ["mcp_foo_bar_baz"]
    assert snapshot.suppression_by_entry == {
        (_DEVICE_ONE, _DEVICE_ENTRY): "server_final_name_collision",
        (_DEVICE_TWO, _COLLISION_ENTRY): "server_final_name_collision",
    }


def test_removing_server_authority_restores_device_without_catalog_change() -> None:
    device_entry = _entry(
        entry_id=_DEVICE_ENTRY,
        server="search",
        final_name="mcp_search_search",
    )
    owner = _owner(device_entry)
    suppressed = build_composite_mcp_snapshot(_envelope(), [owner])
    empty = ServerMcpEnvelope(
        version=1,
        config_revision=8,
        mcp_servers=[],
        mcp_catalog=_catalog(),
    )

    restored = build_composite_mcp_snapshot(empty, [owner])

    assert suppressed.device_routes == ()
    assert [route.entry_id for route in restored.device_routes] == [_DEVICE_ENTRY]
    assert restored.schemas[0].input_schema["properties"]["openoctopus_device"][
        "enum"
    ] == ["laptop"]


def test_device_capacity_is_stable_greedy_and_does_not_evict_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _entry(
        entry_id=_DEVICE_ENTRY,
        server="alpha",
        final_name="mcp_alpha_search",
        padding=1000,
    )
    second = _entry(
        entry_id=_PREFIX_ENTRY,
        server="beta",
        final_name="mcp_beta_search",
    )
    monkeypatch.setattr("openctopus_server.mcp.routes.PROVIDER_SCHEMA_BYTES_MAX", 800)

    snapshot = build_composite_mcp_snapshot(_envelope(), [_owner(first, second)])

    assert [schema.name for schema in snapshot.schemas] == [
        "mcp_search_search",
        "mcp_beta_search",
    ]
    assert snapshot.suppression_by_entry[(_DEVICE_ONE, _DEVICE_ENTRY)] == (
        "provider_capacity"
    )
    assert (_DEVICE_ONE, _PREFIX_ENTRY) not in snapshot.suppression_by_entry


def test_shape_key_includes_global_revision_and_selection_is_exact() -> None:
    first = build_composite_mcp_snapshot(
        _envelope(revision=7),
        [],
        runtime_generations={"search": _GENERATION},
    )
    second = build_composite_mcp_snapshot(_envelope(revision=8), [])
    assert first.shape_key != second.shape_key

    selected = select_composite_mcp_call(
        first,
        final_name="mcp_search_search",
        provider_args={"openoctopus_device": "server", "query": "octopus"},
    )
    assert selected.route.runtime_generation == _GENERATION
    assert selected.source_args == {"query": "octopus"}

    with pytest.raises(ServerMcpRouteSelectionError):
        select_composite_mcp_call(
            first,
            final_name="mcp_search_search",
            provider_args={"openoctopus_device": "laptop", "query": "octopus"},
        )


def test_composite_snapshot_rejects_duplicate_device_authority() -> None:
    entry = _entry(
        entry_id=_DEVICE_ENTRY,
        server="other",
        final_name="mcp_other_search",
    )
    with pytest.raises(McpCatalogError, match="duplicate"):
        build_composite_mcp_snapshot(
            _envelope(),
            [_owner(entry), _owner(entry, device_id=_DEVICE_TWO)],
        )
