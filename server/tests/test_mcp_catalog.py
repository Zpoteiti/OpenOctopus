from __future__ import annotations

import math
from collections.abc import Iterator
from copy import deepcopy
from uuid import UUID

import pytest

from openctopus_server.devices import mcp_catalog
from openctopus_server.devices.mcp_catalog import (
    EMPTY_CATALOG_DIGEST,
    McpCatalogError,
    build_persisted_catalog,
    canonical_json_bytes,
    extract_resource_template_variables,
    merge_owner_catalogs,
    wrapped_capability_name,
)
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PromptArgument,
    SourceMcpCatalog,
    SourceMcpPrompt,
    SourceMcpResource,
    SourceMcpResourceTemplate,
    SourceMcpServerCatalog,
    SourceMcpTool,
    StdioMcpServerConfig,
)


def _uuid7_factory() -> Iterator[UUID]:
    for suffix in range(1, 1000):
        yield UUID(f"0190d5a7-0000-7000-8000-{suffix:012d}")


def _config(*, enabled: list[str] | None = None, name: str = "demo") -> StdioMcpServerConfig:
    return StdioMcpServerConfig(
        name=name,
        transport="stdio",
        command="python",
        args=[],
        cwd=None,
        env={},
        enabled_capabilities=enabled,
    )


def _source(*, tool_name: str = "search", server: str = "demo") -> SourceMcpCatalog:
    return SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name=server,
                tools=[
                    SourceMcpTool(
                        raw_name=tool_name,
                        description="Search records",
                        input_schema={
                            "type": "object",
                            "properties": {"query": {"type": "string"}},
                            "required": ["query"],
                            "additionalProperties": False,
                        },
                        output_schema={"type": "object"},
                    )
                ],
                resources=[],
                resource_templates=[],
                prompts=[],
            )
        ],
    )


def _four_surface_source() -> SourceMcpCatalog:
    source = _source()
    server = source.servers[0]
    server.resources = [SourceMcpResource(raw_name="guide", uri="docs://guide")]
    server.resource_templates = [
        SourceMcpResourceTemplate(raw_name="record", uri_template="docs://record/{id}")
    ]
    server.prompts = [SourceMcpPrompt(raw_name="review")]
    return source


def _build(
    configs: list[StdioMcpServerConfig],
    source: SourceMcpCatalog,
    *,
    existing: PersistedMcpCatalog | None = None,
    built_in_names: set[str] | None = None,
) -> PersistedMcpCatalog:
    ids = _uuid7_factory()
    return build_persisted_catalog(
        configs,
        source,
        existing_catalog=existing,
        built_in_names=built_in_names or set(),
        entry_id_factory=lambda: next(ids),
    )


def test_wrapped_name_nfkc_normalization_and_invalid_aliases() -> None:
    assert wrapped_capability_name("demo", "Ｆｏｏ  BAR++baz") == "mcp_demo_foo_bar_baz"
    assert wrapped_capability_name("demo", "a--b__c") == "mcp_demo_a--b__c"

    with pytest.raises(McpCatalogError, match="empty"):
        wrapped_capability_name("demo", "你好")
    with pytest.raises(McpCatalogError, match="64"):
        wrapped_capability_name("demo", "x" * 60)


@pytest.mark.parametrize(
    ("template", "variables"),
    [
        ("docs://record/{id}", ("id",)),
        ("https://example/{+path}{?q,lang:2}{&q}", ("path", "q", "lang")),
        ("urn:test/{list*}/{user.name}/{encoded%20name}", ("list", "user.name", "encoded%20name")),
    ],
)
def test_resource_template_scanner_accepts_strict_rfc6570_subset(
    template: str, variables: tuple[str, ...]
) -> None:
    assert extract_resource_template_variables(template) == variables


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
        "docs://record/{id:00001}",
        "docs://record/{bad%2G}",
        "docs://record/{a..b}",
    ],
)
def test_resource_template_scanner_rejects_invalid_syntax(template: str) -> None:
    with pytest.raises(McpCatalogError):
        extract_resource_template_variables(template)


def test_build_wraps_all_four_surfaces_and_computes_stable_digest() -> None:
    source = SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name="demo",
                tools=_source().servers[0].tools,
                resources=[
                    SourceMcpResource(
                        raw_name="guide",
                        uri="docs://guide",
                        description="Read the guide",
                    )
                ],
                resource_templates=[
                    SourceMcpResourceTemplate(
                        raw_name="record",
                        uri_template="docs://record/{id}",
                        description="Read one record",
                    )
                ],
                prompts=[
                    SourceMcpPrompt(
                        raw_name="review",
                        description="Prepare a review",
                        arguments=[
                            PromptArgument(name="language", description=None, required=True),
                            PromptArgument(name="style", description="Tone", required=False),
                        ],
                    )
                ],
            )
        ],
    )

    first = _build([_config(enabled=[])], source)
    second = _build([_config(enabled=[])], source)

    entries = first.servers[0].entries
    assert [entry.final_name for entry in entries] == [
        "mcp_demo_review",
        "mcp_demo_guide",
        "mcp_demo_record",
        "mcp_demo_search",
    ]
    assert all(entry.enabled for entry in entries)
    assert entries[1].input_schema == {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }
    assert entries[2].input_schema["required"] == ["id"]
    assert entries[3].output_schema == {"type": "object"}
    assert first.digest == second.digest
    assert first.servers[0].entries[0].entry_id == second.servers[0].entries[0].entry_id
    assert first.digest != "0" * 64


def test_empty_catalog_digest_matches_contract_fixture() -> None:
    catalog = _build([], SourceMcpCatalog(version=1, servers=[]))

    assert catalog.digest == EMPTY_CATALOG_DIGEST
    assert catalog.digest == "d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf"


def test_enabled_capabilities_three_states_and_unknown_selector() -> None:
    source = _four_surface_source()
    none_enabled = _build([_config(enabled=None)], source)
    all_enabled = _build([_config(enabled=[])], source)
    exact_enabled = _build(
        [_config(enabled=["mcp_demo_search", "mcp_demo_record"])],
        source,
    )

    assert not any(entry.enabled for entry in none_enabled.servers[0].entries)
    assert all(entry.enabled for entry in all_enabled.servers[0].entries)
    assert {
        entry.final_name for entry in exact_enabled.servers[0].entries if entry.enabled
    } == {"mcp_demo_search", "mcp_demo_record"}

    with pytest.raises(McpCatalogError, match="unknown"):
        _build([_config(enabled=["mcp_demo_missing"])], source)


def test_filter_precedes_collision_and_built_in_reservation() -> None:
    source = _source(tool_name="foo bar")
    source.servers[0].tools.append(
        SourceMcpTool(
            raw_name="foo+bar",
            description="Other",
            input_schema={"type": "object", "properties": {}},
        )
    )

    disabled = _build([_config(enabled=None)], source)
    assert all(not entry.enabled for entry in disabled.servers[0].entries)

    with pytest.raises(McpCatalogError) as collision:
        _build([_config(enabled=[])], source)
    assert collision.value.code == "mcp_within_server_collision"

    with pytest.raises(McpCatalogError) as built_in:
        _build([_config(enabled=[])], _source(), built_in_names={"mcp_demo_search"})
    assert built_in.value.code == "mcp_within_server_collision"


def test_disabled_duplicate_logical_source_identity_is_rejected_on_every_build() -> None:
    source = _source()
    source.servers[0].tools.append(deepcopy(source.servers[0].tools[0]))

    for _ in range(2):
        with pytest.raises(McpCatalogError) as captured:
            _build([_config(enabled=[])], source)
        assert captured.value.code == "config_validation_failed"


def test_partial_source_catalog_reuses_unchanged_persisted_servers() -> None:
    original_source = SourceMcpCatalog(
        version=1,
        servers=[
            _source(server="demo").servers[0],
            _source(server="other", tool_name="lookup").servers[0],
        ],
    )
    original = _build(
        [_config(name="demo"), _config(name="other")],
        original_source,
    )
    changed_demo = _source(server="demo")
    changed_demo.servers[0].tools.append(
        SourceMcpTool(
            raw_name="create",
            input_schema={"type": "object", "properties": {}},
        )
    )
    ids = iter([UUID("0190d5a7-0000-7000-8000-000000000777")])

    candidate = build_persisted_catalog(
        [_config(name="demo"), _config(name="other")],
        changed_demo,
        existing_catalog=original,
        entry_id_factory=lambda: next(ids),
    )

    original_other = next(server for server in original.servers if server.name == "other")
    candidate_other = next(server for server in candidate.servers if server.name == "other")
    assert candidate_other == original_other
    assert {entry.final_name for server in candidate.servers for entry in server.entries} == {
        "mcp_demo_create",
        "mcp_demo_search",
        "mcp_other_lookup",
    }

    with pytest.raises(McpCatalogError, match="missing"):
        build_persisted_catalog(
            [_config(name="demo"), _config(name="other")],
            changed_demo,
            entry_id_factory=lambda: next(ids),
        )


@pytest.mark.parametrize(
    ("selector", "enabled"),
    [(None, False), ([], True)],
    ids=("null_disables", "empty_enables_all"),
)
def test_existing_catalog_reuses_entries_for_unchanged_filter_only_selector(
    selector: list[str] | None,
    enabled: bool,
) -> None:
    original = _build([_config(enabled=selector)], _source())
    candidate = build_persisted_catalog(
        [_config(enabled=selector)],
        SourceMcpCatalog(version=1, servers=[]),
        existing_catalog=original,
        entry_id_factory=lambda: pytest.fail("unchanged filter must not allocate an id"),
    )

    original_entry = original.servers[0].entries[0]
    candidate_entry = candidate.servers[0].entries[0]
    assert candidate_entry.entry_id == original_entry.entry_id
    assert candidate_entry.enabled is enabled


@pytest.mark.parametrize(
    ("selector", "enabled"),
    [(None, False), (["mcp_demo_search"], True)],
    ids=("null_disables", "exact_selects"),
)
def test_existing_catalog_applies_filter_only_update_and_preserves_entry_ids(
    selector: list[str] | None,
    enabled: bool,
) -> None:
    source = _source()
    original = _build([_config(enabled=[])], source)
    candidate = build_persisted_catalog(
        [_config(enabled=selector)],
        source,
        existing_catalog=original,
        entry_id_factory=lambda: pytest.fail("filter-only update must preserve entry ids"),
    )

    original_entry = original.servers[0].entries[0]
    candidate_entry = candidate.servers[0].entries[0]
    assert candidate_entry.entry_id == original_entry.entry_id
    assert candidate_entry.enabled is enabled


def test_cross_config_enabled_name_collision_is_rejected() -> None:
    source = SourceMcpCatalog(
        version=1,
        servers=[
            _source(server="a_b", tool_name="c").servers[0],
            _source(server="a", tool_name="b_c").servers[0],
        ],
    )

    with pytest.raises(McpCatalogError) as captured:
        _build(
            [_config(name="a_b", enabled=[]), _config(name="a", enabled=[])],
            source,
        )

    assert captured.value.code == "mcp_schema_collision"


def test_tool_prompt_and_template_reserved_field_validation() -> None:
    reserved_tool = _source()
    reserved_tool.servers[0].tools[0].input_schema["properties"]["openoctopus_device"] = {
        "type": "string"
    }
    with pytest.raises(McpCatalogError, match="openoctopus_device"):
        _build([_config(enabled=[])], reserved_tool)

    prompt = SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name="demo",
                tools=[],
                resources=[],
                resource_templates=[],
                prompts=[
                    SourceMcpPrompt(
                        raw_name="review",
                        arguments=[PromptArgument(name="openoctopus_device", required=True)],
                    )
                ],
            )
        ],
    )
    with pytest.raises(McpCatalogError, match="openoctopus_device"):
        _build([_config(enabled=[])], prompt)

    template = SourceMcpCatalog(
        version=1,
        servers=[
            SourceMcpServerCatalog(
                name="demo",
                tools=[],
                resources=[],
                resource_templates=[
                    SourceMcpResourceTemplate(
                        raw_name="item",
                        uri_template="docs://item/{openoctopus_device}",
                    )
                ],
                prompts=[],
            )
        ],
    )
    with pytest.raises(McpCatalogError, match="openoctopus_device"):
        _build([_config(enabled=[])], template)


def test_existing_logical_entry_keeps_uuid_and_digest_excludes_uuid() -> None:
    original = _build([_config(enabled=[])], _source())

    def unexpected_id() -> UUID:
        raise AssertionError("an unchanged entry must not allocate a replacement UUID")

    replacement = build_persisted_catalog(
        [_config(enabled=[])],
        _source(),
        existing_catalog=original,
        entry_id_factory=unexpected_id,
    )

    assert replacement.servers[0].entries[0].entry_id == original.servers[0].entries[0].entry_id

    changed_ids = deepcopy(replacement)
    changed_ids.servers[0].entries[0].entry_id = UUID("0190d5a7-0000-7000-8000-000000000999")
    rebuilt = mcp_catalog.with_catalog_digest(changed_ids)
    assert rebuilt.digest == replacement.digest


def test_digest_is_independent_of_discovery_order_for_equal_invocation_identity() -> None:
    resources = [
        SourceMcpResource(raw_name="guide", uri="docs://shared"),
        SourceMcpResource(raw_name="manual", uri="docs://shared"),
    ]
    forward = SourceMcpCatalog(
        version=1,
        servers=[SourceMcpServerCatalog(name="demo", resources=resources)],
    )
    reverse = SourceMcpCatalog(
        version=1,
        servers=[SourceMcpServerCatalog(name="demo", resources=list(reversed(resources)))],
    )

    first = _build([_config(enabled=[])], forward)
    second = _build([_config(enabled=[])], reverse)

    assert first.digest == second.digest
    assert [entry.raw_name for entry in first.servers[0].entries] == ["guide", "manual"]
    assert [entry.raw_name for entry in second.servers[0].entries] == ["guide", "manual"]


def test_canonical_json_rejects_nonfinite_values_and_excessive_depth() -> None:
    with pytest.raises(McpCatalogError):
        canonical_json_bytes({"value": math.nan})

    value: object = "leaf"
    for _ in range(33):
        value = [value]
    with pytest.raises(McpCatalogError, match="depth"):
        canonical_json_bytes(value)


def test_per_server_capability_and_catalog_byte_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    source = _source()
    source.servers[0].tools = [
        SourceMcpTool(
            raw_name=f"tool_{index}",
            input_schema={"type": "object", "properties": {}},
        )
        for index in range(257)
    ]
    with pytest.raises(McpCatalogError, match="256"):
        _build([_config(enabled=[])], source)

    monkeypatch.setattr(mcp_catalog, "DEVICE_CATALOG_BYTES_MAX", 10)
    with pytest.raises(McpCatalogError, match="catalog"):
        _build([_config(enabled=[])], _source())


def test_per_capability_and_device_count_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mcp_catalog, "CAPABILITY_BYTES_MAX", 10)
    with pytest.raises(McpCatalogError, match="capability"):
        _build([_config(enabled=[])], _source())
    monkeypatch.setattr(mcp_catalog, "CAPABILITY_BYTES_MAX", 256 * 1024)

    servers: list[SourceMcpServerCatalog] = []
    configs: list[StdioMcpServerConfig] = []
    for name, count in (("one", 256), ("two", 256), ("three", 1)):
        configs.append(_config(name=name))
        servers.append(
            SourceMcpServerCatalog(
                name=name,
                tools=[
                    SourceMcpTool(
                        raw_name=f"tool_{index}",
                        input_schema={"type": "object", "properties": {}},
                    )
                    for index in range(count)
                ],
            )
        )

    with pytest.raises(McpCatalogError, match="512"):
        _build(configs, SourceMcpCatalog(version=1, servers=servers))


def test_owner_equal_merge_injects_exact_device_enum_and_schema_drift_rejects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    laptop = _build([_config(enabled=[])], _source())
    desktop = _build([_config(enabled=[])], _source())

    merged = merge_owner_catalogs({"desktop": desktop, "laptop": laptop})

    assert len(merged) == 1
    assert merged[0].name == "mcp_demo_search"
    assert merged[0].input_schema["properties"]["openoctopus_device"]["enum"] == [
        "desktop",
        "laptop",
    ]
    assert merged[0].input_schema["required"] == ["query", "openoctopus_device"]

    drift_source = _source()
    drift_source.servers[0].tools[0].description = "Different schema identity"
    drift = _build([_config(enabled=[])], drift_source)
    with pytest.raises(McpCatalogError) as captured:
        merge_owner_catalogs({"desktop": drift, "laptop": laptop})
    assert captured.value.code == "mcp_schema_collision"

    monkeypatch.setattr(mcp_catalog, "OWNER_CAPABILITY_MAX", 0)
    with pytest.raises(McpCatalogError) as count_limit:
        merge_owner_catalogs({"laptop": laptop})
    assert count_limit.value.code == "mcp_owner_schema_limit"

    monkeypatch.setattr(mcp_catalog, "OWNER_CAPABILITY_MAX", 256)
    monkeypatch.setattr(mcp_catalog, "OWNER_PROVIDER_SCHEMA_BYTES_MAX", 10)
    with pytest.raises(McpCatalogError) as byte_limit:
        merge_owner_catalogs({"laptop": laptop})
    assert byte_limit.value.code == "mcp_owner_schema_limit"


def test_owner_merge_uses_invocation_identity_not_provider_hidden_resource_name() -> None:
    def resource_catalog(raw_name: str) -> PersistedMcpCatalog:
        source = SourceMcpCatalog(
            version=1,
            servers=[
                SourceMcpServerCatalog(
                    name="demo",
                    resources=[
                        SourceMcpResource(
                            raw_name=raw_name,
                            uri="docs://shared",
                            description="Shared guide",
                        )
                    ],
                )
            ],
        )
        return _build([_config(enabled=[])], source)

    merged = merge_owner_catalogs(
        {
            "desktop": resource_catalog("Foo Bar"),
            "laptop": resource_catalog("foo+bar"),
        }
    )

    assert [tool.name for tool in merged] == ["mcp_demo_foo_bar"]
    assert merged[0].input_schema["properties"]["openoctopus_device"]["enum"] == [
        "desktop",
        "laptop",
    ]
