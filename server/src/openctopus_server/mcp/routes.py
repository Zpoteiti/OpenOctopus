from __future__ import annotations

import hashlib
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal
from uuid import UUID

from openctopus_server.devices.mcp_catalog import (
    McpCatalogError,
    canonical_json_bytes,
    catalog_digest,
    merge_owner_catalogs,
    provider_tool_for_entry,
    with_catalog_digest,
)
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    ProviderMcpTool,
)
from openctopus_server.devices.mcp_routes import FrozenMcpEntryRoute, OwnerMcpDevice
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME

from .models import ServerMcpEnvelope

PROVIDER_CAPABILITY_MAX = 256
PROVIDER_SCHEMA_BYTES_MAX = 256 * 1024

type DeviceSuppressionReason = Literal[
    "server_namespace_reserved",
    "server_final_name_collision",
    "provider_capacity",
]


class ServerMcpRouteSelectionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class FrozenServerMcpEntryRoute:
    entry_id: UUID
    config_revision: int
    catalog_digest: str
    runtime_generation: UUID | None
    server: str
    surface: Literal["tool", "resource", "resource_template", "prompt"]
    raw_name: str
    invocation_identity: str
    final_name: str

    @property
    def source_identity(self) -> tuple[str, str, str]:
        return self.surface, self.raw_name, self.invocation_identity


type CompositeMcpEntryRoute = FrozenServerMcpEntryRoute | FrozenMcpEntryRoute


@dataclass(frozen=True, slots=True)
class CompositeMcpSnapshot:
    schemas: tuple[ProviderMcpTool, ...]
    server_routes: tuple[FrozenServerMcpEntryRoute, ...]
    device_routes: tuple[FrozenMcpEntryRoute, ...]
    suppression_by_entry: Mapping[tuple[UUID, UUID], DeviceSuppressionReason]
    config_revision: int
    catalog_digest: str
    shape_key: str

    @property
    def routes(self) -> tuple[CompositeMcpEntryRoute, ...]:
        return (*self.server_routes, *self.device_routes)


@dataclass(frozen=True, slots=True)
class SelectedCompositeMcpCall:
    route: CompositeMcpEntryRoute
    source_args: dict[str, Any]


def _error(code: str, message: str) -> McpCatalogError:
    return McpCatalogError(code, message)


def _schema_bytes(schemas: Sequence[ProviderMcpTool]) -> int:
    return len(
        canonical_json_bytes(
            [schema.model_dump(mode="json", exclude_none=True) for schema in schemas]
        )
    )


def _filtered_owner_catalogs(
    devices: Sequence[OwnerMcpDevice],
    *,
    reserved_names: Collection[str],
    reserved_final_names: Collection[str],
    suppression: dict[tuple[UUID, UUID], DeviceSuppressionReason],
) -> dict[str, PersistedMcpCatalog]:
    filtered: dict[str, PersistedMcpCatalog] = {}
    for device in devices:
        if device.catalog.digest != catalog_digest(device.catalog):
            raise _error(
                "config_validation_failed",
                "persisted MCP catalog digest does not match its content",
            )
        servers: list[PersistedMcpServerCatalog] = []
        for server in device.catalog.servers:
            entries: list[PersistedMcpCatalogEntry] = []
            for entry in server.entries:
                if entry.server in reserved_names:
                    if entry.enabled:
                        suppression[(device.device_id, entry.entry_id)] = (
                            "server_namespace_reserved"
                        )
                    continue
                if entry.enabled and entry.final_name in reserved_final_names:
                    suppression[(device.device_id, entry.entry_id)] = (
                        "server_final_name_collision"
                    )
                    continue
                entries.append(entry.model_copy(deep=True))
            if entries:
                servers.append(PersistedMcpServerCatalog(name=server.name, entries=entries))
        filtered[device.name] = with_catalog_digest(
            PersistedMcpCatalog(version=1, digest="0" * 64, servers=servers)
        )
    return filtered


def _server_schemas_and_routes(
    envelope: ServerMcpEnvelope,
    *,
    built_in_names: Collection[str],
    runtime_generations: Mapping[str, UUID | None],
) -> tuple[list[ProviderMcpTool], list[FrozenServerMcpEntryRoute]]:
    schemas: list[ProviderMcpTool] = []
    routes: list[FrozenServerMcpEntryRoute] = []
    for server in envelope.mcp_catalog.servers:
        for entry in server.entries:
            if not entry.enabled:
                continue
            if entry.final_name in built_in_names:
                raise _error(
                    "mcp_schema_collision",
                    f"Server MCP capability collides with a built-in tool: {entry.final_name}",
                )
            schemas.append(
                provider_tool_for_entry(
                    entry,
                    sites=("server",),
                    selector_description=(
                        "Which install site should execute this MCP capability."
                    ),
                )
            )
            routes.append(
                FrozenServerMcpEntryRoute(
                    entry_id=entry.entry_id,
                    config_revision=envelope.config_revision,
                    catalog_digest=envelope.mcp_catalog.digest,
                    runtime_generation=runtime_generations.get(entry.server),
                    server=entry.server,
                    surface=entry.surface,
                    raw_name=entry.raw_name,
                    invocation_identity=entry.invocation_identity,
                    final_name=entry.final_name,
                )
            )
    schemas.sort(key=lambda schema: schema.name)
    routes.sort(key=lambda route: (route.final_name, str(route.entry_id)))
    if len(schemas) > PROVIDER_CAPABILITY_MAX or _schema_bytes(schemas) > PROVIDER_SCHEMA_BYTES_MAX:
        raise _error(
            "mcp_server_schema_limit",
            "Server MCP provider schema exceeds its capacity",
        )
    return schemas, routes


def build_composite_mcp_snapshot(
    envelope: ServerMcpEnvelope,
    devices: Sequence[OwnerMcpDevice],
    *,
    built_in_names: Collection[str] = (),
    runtime_generations: Mapping[str, UUID | None] | None = None,
) -> CompositeMcpSnapshot:
    """Build one immutable Server-first Provider/dispatch snapshot."""

    device_ids = [device.device_id for device in devices]
    device_names = [device.name for device in devices]
    if (
        len(device_ids) != len(set(device_ids))
        or len(device_names) != len(set(device_names))
        or any(device.config_revision < 1 for device in devices)
    ):
        raise _error(
            "config_validation_failed",
            "owner MCP snapshot contains duplicate or invalid Device authority",
        )
    runtime_generations = runtime_generations or {}
    reserved_names = {config.name for config in envelope.mcp_servers}
    suppression: dict[tuple[UUID, UUID], DeviceSuppressionReason] = {}
    server_schemas, server_routes = _server_schemas_and_routes(
        envelope,
        built_in_names=built_in_names,
        runtime_generations=runtime_generations,
    )
    server_names = {schema.name for schema in server_schemas}

    filtered = _filtered_owner_catalogs(
        devices,
        reserved_names=reserved_names,
        reserved_final_names=server_names,
        suppression=suppression,
    )
    device_schemas = merge_owner_catalogs(filtered, built_in_names=built_in_names)
    devices_by_name = {device.name: device for device in devices}
    enabled_routes_by_name: dict[str, list[FrozenMcpEntryRoute]] = {}
    for device_name, catalog in filtered.items():
        owner = devices_by_name[device_name]
        for server in catalog.servers:
            for entry in server.entries:
                if not entry.enabled:
                    continue
                enabled_routes_by_name.setdefault(entry.final_name, []).append(
                    FrozenMcpEntryRoute(
                        device_id=owner.device_id,
                        device_name=device_name,
                        entry_id=entry.entry_id,
                        config_revision=owner.config_revision,
                        catalog_digest=owner.catalog.digest,
                        server=entry.server,
                        surface=entry.surface,
                        raw_name=entry.raw_name,
                        invocation_identity=entry.invocation_identity,
                        final_name=entry.final_name,
                        server_config_revision=envelope.config_revision,
                    )
                )

    selected_schemas = list(server_schemas)
    visible_device_names: set[str] = set()
    for schema in device_schemas:
        routes = enabled_routes_by_name.get(schema.name, [])
        candidate = [*selected_schemas, schema]
        if (
            len(candidate) > PROVIDER_CAPABILITY_MAX
            or _schema_bytes(candidate) > PROVIDER_SCHEMA_BYTES_MAX
        ):
            for route in routes:
                suppression[(route.device_id, route.entry_id)] = "provider_capacity"
            continue
        selected_schemas.append(schema)
        visible_device_names.add(schema.name)

    device_routes = sorted(
        (
            route
            for name in visible_device_names
            for route in enabled_routes_by_name.get(name, [])
        ),
        key=lambda route: (route.final_name, route.device_name, str(route.entry_id)),
    )
    shape_projection = {
        "config_revision": envelope.config_revision,
        "schemas": [schema.model_dump(mode="json", exclude_none=True) for schema in selected_schemas],
    }
    shape_key = hashlib.sha256(canonical_json_bytes(shape_projection)).hexdigest()
    return CompositeMcpSnapshot(
        schemas=tuple(selected_schemas),
        server_routes=tuple(server_routes),
        device_routes=tuple(device_routes),
        suppression_by_entry=MappingProxyType(dict(suppression)),
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        shape_key=shape_key,
    )


def select_composite_mcp_call(
    snapshot: CompositeMcpSnapshot,
    *,
    final_name: str,
    provider_args: Mapping[str, Any],
) -> SelectedCompositeMcpCall:
    schemas = [schema for schema in snapshot.schemas if schema.name == final_name]
    if len(schemas) != 1:
        raise ServerMcpRouteSelectionError("tool_invalid_args", "Unknown MCP capability")
    site = provider_args.get(DEVICE_FIELD_NAME)
    if site is None:
        raise ServerMcpRouteSelectionError(
            "tool_missing_required_field",
            f"Missing required field: {DEVICE_FIELD_NAME}",
        )
    if not isinstance(site, str):
        raise ServerMcpRouteSelectionError(
            "tool_invalid_args",
            "MCP install site must be a string",
        )
    routes = [
        route
        for route in snapshot.routes
        if route.final_name == final_name
        and (
            (isinstance(route, FrozenServerMcpEntryRoute) and site == "server")
            or (isinstance(route, FrozenMcpEntryRoute) and route.device_name == site)
        )
    ]
    if len(routes) != 1:
        raise ServerMcpRouteSelectionError(
            "tool_invalid_args",
            "Unknown MCP install site",
        )
    source_args = dict(provider_args)
    del source_args[DEVICE_FIELD_NAME]
    return SelectedCompositeMcpCall(route=routes[0], source_args=source_args)
