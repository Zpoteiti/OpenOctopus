from __future__ import annotations

import hashlib
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from openctopus_server.tools.device_field import DEVICE_FIELD_NAME

from .mcp_catalog import (
    McpCatalogError,
    build_persisted_catalog,
    canonical_json_bytes,
    catalog_digest,
    merge_owner_catalogs,
    with_catalog_digest,
)
from .mcp_models import (
    McpServerConfig,
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    ProviderMcpTool,
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    parse_mcp_server_configs,
)
from .protocol import (
    AcceptedMcpRegistration,
    McpRuntimeSnapshot,
    ReadyMcpRuntimeSnapshot,
    RegisterMcpAckFrame,
    RegisterMcpFrame,
    RejectedMcpRegistration,
)

type McpSurface = Literal["tool", "resource", "resource_template", "prompt"]


class McpRouteSelectionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


class McpRegistrationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class OwnerMcpDevice:
    device_id: UUID
    name: str
    config_revision: int
    catalog: PersistedMcpCatalog


@dataclass(frozen=True, slots=True)
class FrozenMcpEntryRoute:
    device_id: UUID
    device_name: str
    entry_id: UUID
    config_revision: int
    catalog_digest: str
    server: str
    surface: McpSurface
    raw_name: str
    invocation_identity: str
    final_name: str
    server_config_revision: int | None = None

    @property
    def source_identity(self) -> tuple[McpSurface, str, str]:
        return self.surface, self.raw_name, self.invocation_identity


@dataclass(frozen=True, slots=True)
class OwnerMcpSnapshot:
    schemas: tuple[ProviderMcpTool, ...]
    routes: tuple[FrozenMcpEntryRoute, ...]
    shape_key: str


@dataclass(frozen=True, slots=True)
class SelectedMcpCall:
    route: FrozenMcpEntryRoute
    source_args: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AcceptedMcpBinding:
    name: str
    runtime_generation: UUID
    config_revision: int
    catalog_digest: str
    entry_ids: tuple[UUID, ...]


@dataclass(frozen=True, slots=True)
class McpRegistrationCandidate:
    ack: RegisterMcpAckFrame
    bindings: tuple[AcceptedMcpBinding, ...]


def _route_sort_key(route: FrozenMcpEntryRoute) -> tuple[str, str, str]:
    return route.final_name, route.device_name, str(route.entry_id)


def _schema_shape_key(schemas: Sequence[ProviderMcpTool]) -> str:
    projection = [
        schema.model_dump(mode="json", exclude_none=True)
        for schema in schemas
    ]
    return hashlib.sha256(canonical_json_bytes(projection)).hexdigest()


def _validate_owner_devices(devices: Sequence[OwnerMcpDevice]) -> None:
    device_ids = [device.device_id for device in devices]
    names = [device.name for device in devices]
    if len(device_ids) != len(set(device_ids)) or len(names) != len(set(names)):
        raise McpCatalogError(
            "config_validation_failed",
            "owner MCP snapshot contains a duplicate device",
        )
    if any(device.config_revision < 1 for device in devices):
        raise McpCatalogError(
            "config_validation_failed",
            "owner MCP snapshot contains an invalid config revision",
        )


def build_owner_mcp_snapshot(
    devices: Sequence[OwnerMcpDevice],
    *,
    built_in_names: Collection[str] = (),
) -> OwnerMcpSnapshot:
    """Build one provider iteration's durable schemas and immutable routes."""

    _validate_owner_devices(devices)
    by_name = {device.name: device.catalog for device in devices}
    schemas = tuple(
        merge_owner_catalogs(
            by_name,
            built_in_names=built_in_names,
        )
    )
    routes: list[FrozenMcpEntryRoute] = []
    for device in devices:
        for server in device.catalog.servers:
            for entry in server.entries:
                if not entry.enabled:
                    continue
                routes.append(
                    FrozenMcpEntryRoute(
                        device_id=device.device_id,
                        device_name=device.name,
                        entry_id=entry.entry_id,
                        config_revision=device.config_revision,
                        catalog_digest=device.catalog.digest,
                        server=entry.server,
                        surface=entry.surface,
                        raw_name=entry.raw_name,
                        invocation_identity=entry.invocation_identity,
                        final_name=entry.final_name,
                    )
                )
    return OwnerMcpSnapshot(
        schemas=schemas,
        routes=tuple(sorted(routes, key=_route_sort_key)),
        shape_key=_schema_shape_key(schemas),
    )


def _selection_error(code: str, message: str) -> McpRouteSelectionError:
    return McpRouteSelectionError(code, message)


def _schema_for_name(snapshot: OwnerMcpSnapshot, final_name: str) -> ProviderMcpTool:
    matches = [schema for schema in snapshot.schemas if schema.name == final_name]
    if len(matches) != 1:
        raise _selection_error("tool_invalid_args", "Unknown MCP capability")
    return matches[0]


def select_mcp_call(
    snapshot: OwnerMcpSnapshot,
    *,
    final_name: str,
    provider_args: Mapping[str, Any],
) -> SelectedMcpCall:
    """Resolve an exact frozen install site without interpreting the final name."""

    _schema_for_name(snapshot, final_name)
    if DEVICE_FIELD_NAME not in provider_args:
        raise _selection_error(
            "tool_missing_required_field",
            f"Missing required field: {DEVICE_FIELD_NAME}",
        )
    device_name = provider_args[DEVICE_FIELD_NAME]
    if not isinstance(device_name, str):
        raise _selection_error("tool_invalid_args", "MCP install site must be a string")
    routes = [
        route
        for route in snapshot.routes
        if route.final_name == final_name and route.device_name == device_name
    ]
    if len(routes) != 1:
        raise _selection_error("tool_invalid_args", "Unknown MCP install site")
    source_args = dict(provider_args)
    del source_args[DEVICE_FIELD_NAME]
    return SelectedMcpCall(route=routes[0], source_args=source_args)


def _authoritative_server_catalog(
    catalog: PersistedMcpCatalog,
    name: str,
) -> PersistedMcpServerCatalog:
    matches = [server for server in catalog.servers if server.name == name]
    if len(matches) != 1:
        raise McpRegistrationError(
            "config_validation_failed",
            "authoritative MCP catalog does not match configuration",
        )
    return matches[0]


def _runtime_source(snapshot: ReadyMcpRuntimeSnapshot) -> SourceMcpServerCatalog:
    payload = snapshot.source_catalog.model_dump(mode="python")
    return SourceMcpServerCatalog.model_validate(
        {"name": snapshot.name, **payload},
        strict=True,
    )


def _unexpected_entry_id() -> UUID:
    raise McpCatalogError(
        "config_validation_failed",
        "runtime source contains an unknown MCP entry",
    )


def _persisted_entry_sort_key(
    entry: PersistedMcpCatalogEntry,
) -> tuple[str, str, str, str, str, str]:
    return (
        entry.server,
        entry.surface,
        entry.invocation_identity,
        entry.raw_name,
        entry.final_name,
        str(entry.entry_id),
    )


def _server_projection(server: PersistedMcpServerCatalog) -> dict[str, object]:
    return {
        "name": server.name,
        "entries": [
            entry.model_dump(mode="json", exclude_none=True)
            for entry in sorted(server.entries, key=_persisted_entry_sort_key)
        ],
    }


def _ready_snapshot_matches(
    snapshot: ReadyMcpRuntimeSnapshot,
    *,
    config: McpServerConfig,
    catalog: PersistedMcpCatalog,
) -> bool:
    persisted_server = _authoritative_server_catalog(catalog, snapshot.name)
    existing = with_catalog_digest(
        PersistedMcpCatalog(
            version=1,
            digest="0" * 64,
            servers=[persisted_server.model_copy(deep=True)],
        )
    )
    try:
        rebuilt = build_persisted_catalog(
            [config],
            SourceMcpCatalog(version=1, servers=[_runtime_source(snapshot)]),
            existing_catalog=existing,
            entry_id_factory=_unexpected_entry_id,
        )
    except (McpCatalogError, ValueError):
        return False
    return canonical_json_bytes(_server_projection(rebuilt.servers[0])) == canonical_json_bytes(
        _server_projection(persisted_server)
    )


def _rejected(
    snapshot: McpRuntimeSnapshot,
    code: str,
) -> RejectedMcpRegistration:
    return RejectedMcpRegistration(
        name=snapshot.name,
        runtime_generation=snapshot.runtime_generation,
        accepted=False,
        code=code,
    )


def _accepted(snapshot: ReadyMcpRuntimeSnapshot) -> AcceptedMcpRegistration:
    return AcceptedMcpRegistration(
        name=snapshot.name,
        runtime_generation=snapshot.runtime_generation,
        accepted=True,
        code=None,
    )


def _validate_authoritative_state(
    *,
    config_revision: int,
    configs: Sequence[McpServerConfig],
    catalog: PersistedMcpCatalog,
) -> tuple[tuple[McpServerConfig, ...], dict[str, McpServerConfig]]:
    if config_revision < 1 or catalog.digest != catalog_digest(catalog):
        raise McpRegistrationError(
            "config_validation_failed",
            "authoritative MCP state is invalid",
        )
    try:
        normalized = parse_mcp_server_configs(
            [config.storage_dict() for config in configs]
        )
    except ValueError as exc:
        raise McpRegistrationError(
            "config_validation_failed",
            "authoritative MCP configuration is invalid",
        ) from exc
    config_by_name = {config.name: config for config in normalized}
    if set(config_by_name) != {server.name for server in catalog.servers}:
        raise McpRegistrationError(
            "config_validation_failed",
            "authoritative MCP catalog does not match configuration",
        )
    return normalized, config_by_name


def validate_mcp_registration(
    frame: RegisterMcpFrame,
    *,
    authoritative_config_revision: int,
    authoritative_configs: Sequence[McpServerConfig],
    authoritative_catalog: PersistedMcpCatalog,
) -> McpRegistrationCandidate:
    """Validate one full registration without consulting or mutating live state."""

    normalized, config_by_name = _validate_authoritative_state(
        config_revision=authoritative_config_revision,
        configs=authoritative_configs,
        catalog=authoritative_catalog,
    )
    stale = (
        frame.config_revision != authoritative_config_revision
        or frame.catalog_digest != authoritative_catalog.digest
    )
    requested_names = {snapshot.name for snapshot in frame.servers}
    expected_names = {config.name for config in normalized}
    if not stale and requested_names != expected_names:
        raise McpRegistrationError(
            "protocol_malformed_frame",
            "register_mcp must exactly cover authoritative MCP servers",
        )

    results: list[AcceptedMcpRegistration | RejectedMcpRegistration] = []
    bindings: list[AcceptedMcpBinding] = []
    for snapshot in frame.servers:
        if stale:
            results.append(_rejected(snapshot, "mcp_registration_stale"))
            continue
        if not isinstance(snapshot, ReadyMcpRuntimeSnapshot):
            results.append(_rejected(snapshot, snapshot.code))
            continue
        if not _ready_snapshot_matches(
            snapshot,
            config=config_by_name[snapshot.name],
            catalog=authoritative_catalog,
        ):
            results.append(_rejected(snapshot, "mcp_schema_drift"))
            continue
        server_catalog = _authoritative_server_catalog(
            authoritative_catalog,
            snapshot.name,
        )
        entry_ids = tuple(
            entry.entry_id
            for entry in sorted(server_catalog.entries, key=_persisted_entry_sort_key)
            if entry.enabled
        )
        results.append(_accepted(snapshot))
        bindings.append(
            AcceptedMcpBinding(
                name=snapshot.name,
                runtime_generation=snapshot.runtime_generation,
                config_revision=authoritative_config_revision,
                catalog_digest=authoritative_catalog.digest,
                entry_ids=entry_ids,
            )
        )
    ack = RegisterMcpAckFrame(
        id=frame.id,
        config_revision=frame.config_revision,
        catalog_digest=frame.catalog_digest,
        results=results,
    )
    return McpRegistrationCandidate(
        ack=ack,
        bindings=tuple(bindings),
    )
