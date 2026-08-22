from __future__ import annotations

from copy import deepcopy
from uuid import UUID

import pytest

from openctopus_server.devices.mcp_catalog import (
    McpCatalogError,
    with_catalog_digest,
)
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    StdioMcpServerConfig,
)
from openctopus_server.devices.mcp_routes import (
    McpRegistrationError,
    McpRouteSelectionError,
    OwnerMcpDevice,
    build_owner_mcp_snapshot,
    select_mcp_call,
    validate_mcp_registration,
)
from openctopus_server.devices.protocol import (
    DriftedMcpRuntimeSnapshot,
    ReadyMcpRuntimeSnapshot,
    RegisterMcpFrame,
    RuntimeMcpSourceCatalog,
    SourceMcpTool,
    UnavailableMcpRuntimeSnapshot,
)

_DEVICE_ONE = UUID("01890f7c-bb80-7000-8000-000000000001")
_DEVICE_TWO = UUID("01890f7c-bb80-7000-8000-000000000002")
_ENTRY_ONE = UUID("01890f7c-bb80-7000-8000-000000000011")
_ENTRY_TWO = UUID("01890f7c-bb80-7000-8000-000000000012")
_GEN_ONE = UUID("01890f7c-bb80-7000-8000-000000000021")
_GEN_TWO = UUID("01890f7c-bb80-7000-8000-000000000022")
_REQUEST_ID = UUID("01890f7c-bb80-7000-8000-000000000031")


def _schema(*, extra: bool = False) -> dict[str, object]:
    return {
        "type": "object",
        "properties": {"query": {"type": "string"}},
        "required": ["query"],
        "additionalProperties": extra,
    }


def _entry(
    *,
    entry_id: UUID = _ENTRY_ONE,
    server: str = "demo",
    final_name: str = "mcp_demo_search",
    description: str = "MCP tool from 'demo'. Search",
    schema: dict[str, object] | None = None,
    enabled: bool = True,
) -> PersistedMcpCatalogEntry:
    return PersistedMcpCatalogEntry(
        entry_id=entry_id,
        server=server,
        surface="tool",
        raw_name="search",
        invocation_identity="search",
        final_name=final_name,
        provider_description=description,
        input_schema=schema or _schema(),
        enabled=enabled,
    )


def _catalog(*entries: PersistedMcpCatalogEntry) -> PersistedMcpCatalog:
    by_server: dict[str, list[PersistedMcpCatalogEntry]] = {}
    for entry in entries:
        by_server.setdefault(entry.server, []).append(entry)
    return with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[
                PersistedMcpServerCatalog(name=name, entries=server_entries)
                for name, server_entries in sorted(by_server.items())
            ],
        )
    )


def _owner(
    *,
    device_id: UUID = _DEVICE_ONE,
    name: str = "laptop",
    revision: int = 7,
    catalog: PersistedMcpCatalog | None = None,
) -> OwnerMcpDevice:
    return OwnerMcpDevice(
        device_id=device_id,
        name=name,
        config_revision=revision,
        catalog=catalog or _catalog(_entry()),
    )


def _source(*, description: str = "Search") -> RuntimeMcpSourceCatalog:
    return RuntimeMcpSourceCatalog(
        tools=[
            SourceMcpTool(
                raw_name="search",
                description=description,
                input_schema=_schema(),
            )
        ]
    )


def _config(name: str = "demo") -> StdioMcpServerConfig:
    return StdioMcpServerConfig(
        name=name,
        transport="stdio",
        command="mcp-demo",
    )


def _register(
    catalog: PersistedMcpCatalog,
    *,
    revision: int = 7,
    digest: str | None = None,
    servers: list[object] | None = None,
) -> RegisterMcpFrame:
    return RegisterMcpFrame.model_validate(
        {
            "id": _REQUEST_ID,
            "config_revision": revision,
            "catalog_digest": digest or catalog.digest,
            "servers": servers
            if servers is not None
            else [
                ReadyMcpRuntimeSnapshot(
                    name="demo",
                    runtime_generation=_GEN_ONE,
                    state="ready",
                    code=None,
                    source_catalog=_source(),
                )
            ],
        },
        strict=True,
    )


def test_owner_snapshot_merges_schemas_and_freezes_exact_routes() -> None:
    laptop = _catalog(_entry(entry_id=_ENTRY_ONE))
    desktop = _catalog(_entry(entry_id=_ENTRY_TWO))

    snapshot = build_owner_mcp_snapshot(
        [
            _owner(catalog=laptop),
            _owner(
                device_id=_DEVICE_TWO,
                name="desktop",
                revision=9,
                catalog=desktop,
            ),
        ]
    )

    assert [schema.name for schema in snapshot.schemas] == ["mcp_demo_search"]
    assert snapshot.schemas[0].input_schema["properties"]["openoctopus_device"][
        "enum"
    ] == ["desktop", "laptop"]
    assert [(route.final_name, route.device_name) for route in snapshot.routes] == [
        ("mcp_demo_search", "desktop"),
        ("mcp_demo_search", "laptop"),
    ]
    desktop_route = snapshot.routes[0]
    assert desktop_route.device_id == _DEVICE_TWO
    assert desktop_route.entry_id == _ENTRY_TWO
    assert desktop_route.config_revision == 9
    assert desktop_route.catalog_digest == desktop.digest
    assert desktop_route.server == "demo"
    assert desktop_route.source_identity == ("tool", "search", "search")


def test_owner_snapshot_delegates_collision_and_owner_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drift = _catalog(_entry(entry_id=_ENTRY_TWO, description="different"))
    with pytest.raises(McpCatalogError) as collision:
        build_owner_mcp_snapshot([_owner(), _owner(device_id=_DEVICE_TWO, name="desktop", catalog=drift)])
    assert collision.value.code == "mcp_schema_collision"

    with pytest.raises(McpCatalogError) as built_in:
        build_owner_mcp_snapshot([_owner()], built_in_names={"mcp_demo_search"})
    assert built_in.value.code == "mcp_schema_collision"

    monkeypatch.setattr("openctopus_server.devices.mcp_catalog.OWNER_CAPABILITY_MAX", 0)
    with pytest.raises(McpCatalogError) as limit:
        build_owner_mcp_snapshot([_owner()])
    assert limit.value.code == "mcp_owner_schema_limit"


def test_provider_shape_key_excludes_routes_revision_and_entry_ids() -> None:
    first = build_owner_mcp_snapshot([_owner()])
    second = build_owner_mcp_snapshot(
        [
            _owner(
                revision=999,
                catalog=_catalog(_entry(entry_id=_ENTRY_TWO)),
            )
        ]
    )

    assert first.shape_key == second.shape_key
    assert first.routes != second.routes

    changed = build_owner_mcp_snapshot(
        [_owner(catalog=_catalog(_entry(description="Changed provider shape")))]
    )
    assert changed.shape_key != first.shape_key


def test_select_call_requires_exact_install_site_and_removes_selector() -> None:
    snapshot = build_owner_mcp_snapshot([_owner()])

    selected = select_mcp_call(
        snapshot,
        final_name="mcp_demo_search",
        provider_args={"query": "octopus", "openoctopus_device": "laptop"},
    )

    assert selected.route is snapshot.routes[0]
    assert selected.source_args == {"query": "octopus"}

    with pytest.raises(McpRouteSelectionError) as missing:
        select_mcp_call(
            snapshot,
            final_name="mcp_demo_search",
            provider_args={"query": "octopus"},
        )
    assert missing.value.code == "tool_missing_required_field"

    with pytest.raises(McpRouteSelectionError) as unknown_device:
        select_mcp_call(
            snapshot,
            final_name="mcp_demo_search",
            provider_args={"query": "octopus", "openoctopus_device": "desktop"},
        )
    assert unknown_device.value.code == "tool_invalid_args"

    with pytest.raises(McpRouteSelectionError) as unknown_name:
        select_mcp_call(
            snapshot,
            final_name="mcp_demo_missing",
            provider_args={"openoctopus_device": "laptop"},
        )
    assert unknown_name.value.code == "tool_invalid_args"


def test_select_call_validates_top_level_required_and_forbidden_extra_args() -> None:
    snapshot = build_owner_mcp_snapshot([_owner()])

    with pytest.raises(McpRouteSelectionError) as missing:
        select_mcp_call(
            snapshot,
            final_name="mcp_demo_search",
            provider_args={"openoctopus_device": "laptop"},
        )
    assert missing.value.code == "tool_missing_required_field"

    with pytest.raises(McpRouteSelectionError) as extra:
        select_mcp_call(
            snapshot,
            final_name="mcp_demo_search",
            provider_args={
                "query": "octopus",
                "unexpected": True,
                "openoctopus_device": "laptop",
            },
        )
    assert extra.value.code == "tool_invalid_args"

    open_schema = _schema(extra=True)
    open_snapshot = build_owner_mcp_snapshot(
        [_owner(catalog=_catalog(_entry(schema=open_schema)))]
    )
    selected = select_mcp_call(
        open_snapshot,
        final_name="mcp_demo_search",
        provider_args={
            "query": "octopus",
            "unexpected": True,
            "openoctopus_device": "laptop",
        },
    )
    assert selected.source_args["unexpected"] is True


def test_route_selection_never_parses_a_final_name() -> None:
    ambiguous = _catalog(
        _entry(
            server="corp_tools",
            final_name="mcp_corp_tools_find_docs",
        )
    )
    snapshot = build_owner_mcp_snapshot([_owner(catalog=ambiguous)])

    selected = select_mcp_call(
        snapshot,
        final_name="mcp_corp_tools_find_docs",
        provider_args={"query": "x", "openoctopus_device": "laptop"},
    )

    assert selected.route.server == "corp_tools"
    assert selected.route.raw_name == "search"
    assert selected.route.invocation_identity == "search"


def test_registration_accepts_only_ready_exact_runtime_source() -> None:
    catalog = _catalog(_entry())
    candidate = validate_mcp_registration(
        _register(catalog),
        authoritative_config_revision=7,
        authoritative_configs=[_config()],
        authoritative_catalog=catalog,
    )

    assert candidate.ack.id == _REQUEST_ID
    assert candidate.ack.config_revision == 7
    assert candidate.ack.catalog_digest == catalog.digest
    assert candidate.ack.results[0].accepted is True
    assert candidate.ack.results[0].code is None
    assert len(candidate.bindings) == 1
    binding = candidate.bindings[0]
    assert binding.name == "demo"
    assert binding.runtime_generation == _GEN_ONE
    assert binding.entry_ids == (_ENTRY_ONE,)


@pytest.mark.parametrize(
    ("runtime", "expected_code"),
    [
        (
            UnavailableMcpRuntimeSnapshot(
                name="demo",
                runtime_generation=_GEN_ONE,
                state="unavailable",
                code="mcp_starting",
            ),
            "mcp_starting",
        ),
        (
            DriftedMcpRuntimeSnapshot(
                name="demo",
                runtime_generation=_GEN_ONE,
                state="drifted",
                code="mcp_schema_drift",
            ),
            "mcp_schema_drift",
        ),
    ],
)
def test_registration_rejects_non_ready_runtime_with_stable_code(
    runtime: object,
    expected_code: str,
) -> None:
    catalog = _catalog(_entry())
    candidate = validate_mcp_registration(
        _register(catalog, servers=[runtime]),
        authoritative_config_revision=7,
        authoritative_configs=[_config()],
        authoritative_catalog=catalog,
    )

    assert candidate.bindings == ()
    assert candidate.ack.results[0].accepted is False
    assert candidate.ack.results[0].code == expected_code


def test_registration_rejects_schema_drift_without_mutating_authoritative_catalog() -> None:
    catalog = _catalog(_entry())
    original = deepcopy(catalog)
    runtime = ReadyMcpRuntimeSnapshot(
        name="demo",
        runtime_generation=_GEN_ONE,
        state="ready",
        code=None,
        source_catalog=_source(description="Drifted"),
    )

    candidate = validate_mcp_registration(
        _register(catalog, servers=[runtime]),
        authoritative_config_revision=7,
        authoritative_configs=[_config()],
        authoritative_catalog=catalog,
    )

    assert candidate.bindings == ()
    assert candidate.ack.results[0].accepted is False
    assert candidate.ack.results[0].code == "mcp_schema_drift"
    assert catalog == original


def test_registration_ignores_persisted_entry_storage_order() -> None:
    second = _entry(
        entry_id=_ENTRY_TWO,
        final_name="mcp_demo_zebra",
        description="MCP tool from 'demo'. Zebra",
    )
    second.raw_name = "zebra"
    second.invocation_identity = "zebra"
    catalog = _catalog(_entry(), second)
    catalog.servers[0].entries.reverse()
    catalog = with_catalog_digest(catalog)
    runtime_source = _source()
    runtime_source.tools.append(
        SourceMcpTool(
            raw_name="zebra",
            description="Zebra",
            input_schema=_schema(),
        )
    )
    runtime = ReadyMcpRuntimeSnapshot(
        name="demo",
        runtime_generation=_GEN_ONE,
        state="ready",
        code=None,
        source_catalog=runtime_source,
    )

    candidate = validate_mcp_registration(
        _register(catalog, servers=[runtime]),
        authoritative_config_revision=7,
        authoritative_configs=[_config()],
        authoritative_catalog=catalog,
    )

    assert candidate.ack.results[0].accepted is True


@pytest.mark.parametrize("stale_field", ["revision", "digest"])
def test_registration_rejects_stale_aggregate_snapshot(stale_field: str) -> None:
    catalog = _catalog(_entry())
    frame = (
        _register(catalog, revision=6)
        if stale_field == "revision"
        else _register(catalog, digest="f" * 64)
    )

    candidate = validate_mcp_registration(
        frame,
        authoritative_config_revision=7,
        authoritative_configs=[_config()],
        authoritative_catalog=catalog,
    )

    assert candidate.bindings == ()
    assert candidate.ack.results[0].accepted is False
    assert candidate.ack.results[0].code == "mcp_registration_stale"


def test_registration_requires_exact_authoritative_server_coverage() -> None:
    catalog = _catalog(_entry())
    with pytest.raises(McpRegistrationError) as missing:
        validate_mcp_registration(
            _register(catalog, servers=[]),
            authoritative_config_revision=7,
            authoritative_configs=[_config()],
            authoritative_catalog=catalog,
        )
    assert missing.value.code == "protocol_malformed_frame"

    extra_runtime = UnavailableMcpRuntimeSnapshot(
        name="extra",
        runtime_generation=_GEN_TWO,
        state="unavailable",
        code="mcp_starting",
    )
    with pytest.raises(McpRegistrationError):
        validate_mcp_registration(
            _register(
                catalog,
                servers=[
                    ReadyMcpRuntimeSnapshot(
                        name="demo",
                        runtime_generation=_GEN_ONE,
                        state="ready",
                        code=None,
                        source_catalog=_source(),
                    ),
                    extra_runtime,
                ],
            ),
            authoritative_config_revision=7,
            authoritative_configs=[_config()],
            authoritative_catalog=catalog,
        )


def test_registration_rejects_invalid_authoritative_catalog() -> None:
    catalog = _catalog(_entry())
    catalog.digest = "f" * 64

    with pytest.raises(McpRegistrationError) as invalid:
        validate_mcp_registration(
            _register(catalog),
            authoritative_config_revision=7,
            authoritative_configs=[_config()],
            authoritative_catalog=catalog,
        )
    assert invalid.value.code == "config_validation_failed"
