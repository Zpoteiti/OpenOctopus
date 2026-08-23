from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
from copy import deepcopy
from typing import Any
from uuid import UUID

import uritemplate
from jsonschema import SchemaError as JsonSchemaSchemaError
from jsonschema.validators import validator_for
from pydantic import BaseModel

from openctopus_server.tools.device_field import (
    DEVICE_FIELD_NAME,
    openoctopus_device_field,
)

from .mcp_models import (
    McpServerConfig,
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    ProviderMcpTool,
    SourceMcpCatalog,
    SourceMcpPrompt,
    SourceMcpResource,
    SourceMcpResourceTemplate,
    SourceMcpServerCatalog,
    parse_mcp_server_configs,
)

SURFACE_CAPABILITY_MAX = 256
DEVICE_CAPABILITY_MAX = 512
CAPABILITY_BYTES_MAX = 256 * 1024
DEVICE_CATALOG_BYTES_MAX = 2 * 1024 * 1024
JSON_NESTING_MAX = 32
OWNER_CAPABILITY_MAX = 256
OWNER_PROVIDER_SCHEMA_BYTES_MAX = 256 * 1024

EMPTY_CATALOG_DIGEST = "d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf"

_FINAL_NAME = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_DEVICE_NAME = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_VAR_SEGMENT = r"(?:[A-Za-z0-9_]|%[0-9A-Fa-f]{2})+"
_VAR_NAME = re.compile(rf"^{_VAR_SEGMENT}(?:\.{_VAR_SEGMENT})*$")
_TEMPLATE_OPERATORS = frozenset("+#./;?&")


class McpCatalogError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def _validation_error(message: str) -> McpCatalogError:
    return McpCatalogError("config_validation_failed", message)


def _selected_capabilities(
    known_names: set[str], configured: list[str] | None
) -> set[str]:
    if configured is None:
        return set()
    if not configured:
        return known_names
    return set(configured)


def _normalized_json(value: object, *, depth: int = 0) -> object:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", by_alias=True, exclude_none=True)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _validation_error("JSON contains a non-finite number")
        return value
    if isinstance(value, dict):
        next_depth = depth + 1
        if next_depth > JSON_NESTING_MAX:
            raise _validation_error(f"JSON nesting depth exceeds {JSON_NESTING_MAX}")
        normalized: dict[str, object] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise _validation_error("JSON object keys must be strings")
            normalized[key] = _normalized_json(child, depth=next_depth)
        return normalized
    if isinstance(value, (list, tuple)):
        next_depth = depth + 1
        if next_depth > JSON_NESTING_MAX:
            raise _validation_error(f"JSON nesting depth exceeds {JSON_NESTING_MAX}")
        return [_normalized_json(child, depth=next_depth) for child in value]
    raise _validation_error("value is not canonical JSON")


def canonical_json_bytes(value: object) -> bytes:
    normalized = _normalized_json(value)
    try:
        return json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise _validation_error("value cannot be encoded as canonical JSON") from exc


def wrapped_capability_name(server: str, raw_name: str) -> str:
    normalized = unicodedata.normalize("NFKC", raw_name).lower()
    alias = re.sub(r"[^a-z0-9_-]+", "_", normalized).strip("_-")
    if not alias:
        raise _validation_error("wrapped MCP alias is empty")
    final_name = f"mcp_{server}_{alias}"
    if len(final_name) > 64 or _FINAL_NAME.fullmatch(final_name) is None:
        raise _validation_error("wrapped MCP name must fit the 64-character provider limit")
    return final_name


def _parse_varspec(varspec: str) -> str:
    if not varspec:
        raise _validation_error("resource template contains an empty variable")
    if varspec.endswith("*"):
        name = varspec[:-1]
        if not name or "*" in name or ":" in name:
            raise _validation_error("resource template explode modifier is invalid")
    elif ":" in varspec:
        if varspec.count(":") != 1:
            raise _validation_error("resource template prefix modifier is invalid")
        name, prefix = varspec.split(":", 1)
        if (
            not prefix.isascii()
            or not prefix.isdigit()
            or len(prefix) > 4
            or prefix.startswith("0")
            or not 1 <= int(prefix) <= 9999
        ):
            raise _validation_error("resource template prefix must be in 1..9999")
    else:
        name = varspec
        if "*" in name:
            raise _validation_error("resource template modifier is invalid")
    if _VAR_NAME.fullmatch(name) is None:
        raise _validation_error("resource template variable name is invalid")
    return name


def extract_resource_template_variables(template: str) -> tuple[str, ...]:
    variables: list[str] = []
    seen: set[str] = set()
    index = 0
    while index < len(template):
        character = template[index]
        if character == "}":
            raise _validation_error("resource template contains an unmatched closing brace")
        if character != "{":
            index += 1
            continue
        end = template.find("}", index + 1)
        if end < 0:
            raise _validation_error("resource template contains an unmatched opening brace")
        expression = template[index + 1 : end]
        if not expression or "{" in expression or "}" in expression:
            raise _validation_error("resource template expression is invalid")
        if expression[0] in _TEMPLATE_OPERATORS:
            expression = expression[1:]
        if not expression:
            raise _validation_error("resource template expression has no variables")
        for varspec in expression.split(","):
            name = _parse_varspec(varspec)
            if name not in seen:
                seen.add(name)
                variables.append(name)
        index = end + 1
    try:
        parsed = tuple(str(value) for value in uritemplate.variables(template))
    except (TypeError, ValueError) as exc:
        raise _validation_error("resource template is not valid RFC 6570") from exc
    if parsed != tuple(variables):
        raise _validation_error("resource template variable projection is ambiguous")
    return tuple(variables)


def _validate_capability_value(value: object) -> None:
    if len(canonical_json_bytes(value)) > CAPABILITY_BYTES_MAX:
        raise _validation_error("single MCP capability exceeds 256 KiB")


def _validate_tool_schema(schema: dict[str, Any]) -> dict[str, Any]:
    _validate_capability_value(schema)
    try:
        validator_for(schema).check_schema(schema)
    except JsonSchemaSchemaError:
        raise _validation_error("MCP tool input schema is invalid") from None
    if schema.get("type") != "object":
        raise _validation_error("MCP tool input schema must have top-level type object")
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        raise _validation_error("MCP tool input schema properties must be an object")
    if DEVICE_FIELD_NAME in properties:
        raise _validation_error(f"MCP source schema reserves {DEVICE_FIELD_NAME}")
    required = schema.get("required", [])
    if (
        not isinstance(required, list)
        or any(not isinstance(name, str) for name in required)
        or len(required) != len(set(required))
        or not set(required).issubset(properties)
    ):
        raise _validation_error("MCP tool required must be a unique properties subset")
    return deepcopy(schema)


def _provider_description(server: str, surface: str, description: str | None) -> str:
    labels = {
        "tool": "tool",
        "resource": "static resource",
        "resource_template": "resource template",
        "prompt": "prompt",
    }
    prefix = f"MCP {labels[surface]} from '{server}'."
    return prefix if description is None else f"{prefix} {description}"


def _resource_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }


def _template_schema(template: SourceMcpResourceTemplate) -> dict[str, Any]:
    variables = extract_resource_template_variables(template.uri_template)
    if DEVICE_FIELD_NAME in variables:
        raise _validation_error(f"MCP resource template reserves {DEVICE_FIELD_NAME}")
    return {
        "type": "object",
        "properties": {name: {"type": "string"} for name in variables},
        "required": list(variables),
        "additionalProperties": False,
    }


def _prompt_schema(prompt: SourceMcpPrompt) -> dict[str, Any]:
    names = [argument.name for argument in prompt.arguments]
    if len(names) != len(set(names)):
        raise _validation_error("MCP prompt arguments must be unique")
    if DEVICE_FIELD_NAME in names:
        raise _validation_error(f"MCP prompt reserves {DEVICE_FIELD_NAME}")
    properties: dict[str, Any] = {}
    required: list[str] = []
    for argument in prompt.arguments:
        property_schema: dict[str, Any] = {"type": "string"}
        if argument.description is not None:
            property_schema["description"] = argument.description
        properties[argument.name] = property_schema
        if argument.required:
            required.append(argument.name)
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _entry_facts(
    server: SourceMcpServerCatalog,
) -> list[tuple[str, str, str, str, dict[str, Any], dict[str, Any] | None]]:
    facts: list[tuple[str, str, str, str, dict[str, Any], dict[str, Any] | None]] = []
    for tool in server.tools:
        _validate_capability_value(tool)
        schema = _validate_tool_schema(tool.input_schema)
        if tool.output_schema is not None:
            _validate_capability_value(tool.output_schema)
        facts.append(
            (
                "tool",
                tool.raw_name,
                tool.raw_name,
                _provider_description(server.name, "tool", tool.description),
                schema,
                deepcopy(tool.output_schema),
            )
        )
    for resource in server.resources:
        _validate_capability_value(resource)
        facts.append(
            (
                "resource",
                resource.raw_name,
                resource.uri,
                _provider_description(server.name, "resource", resource.description),
                _resource_schema(),
                None,
            )
        )
    for template in server.resource_templates:
        _validate_capability_value(template)
        facts.append(
            (
                "resource_template",
                template.raw_name,
                template.uri_template,
                _provider_description(server.name, "resource_template", template.description),
                _template_schema(template),
                None,
            )
        )
    for prompt in server.prompts:
        _validate_capability_value(prompt)
        facts.append(
            (
                "prompt",
                prompt.raw_name,
                prompt.raw_name,
                _provider_description(server.name, "prompt", prompt.description),
                _prompt_schema(prompt),
                None,
            )
        )
    return facts


def _logical_entry_key(entry: PersistedMcpCatalogEntry) -> tuple[str, str, str, str, str]:
    return (
        entry.server,
        entry.surface,
        entry.raw_name,
        entry.invocation_identity,
        entry.final_name,
    )


def _entry_sort_key(
    entry: PersistedMcpCatalogEntry,
) -> tuple[str, str, str, str, str]:
    return (
        entry.server,
        entry.surface,
        entry.invocation_identity,
        entry.raw_name,
        entry.final_name,
    )


def _build_server_entries(
    config: McpServerConfig,
    source: SourceMcpServerCatalog,
    *,
    existing_ids: Mapping[tuple[str, str, str, str, str], UUID],
    built_in_names: Collection[str],
    entry_id_factory: Callable[[], UUID],
) -> list[PersistedMcpCatalogEntry]:
    if source.capability_count > SURFACE_CAPABILITY_MAX:
        raise _validation_error(f"MCP server capability count exceeds {SURFACE_CAPABILITY_MAX}")
    facts = _entry_facts(source)
    prepared: list[tuple[str, str, str, str, str, dict[str, Any], dict[str, Any] | None]] = []
    known_names: set[str] = set()
    source_identities: set[tuple[str, str, str]] = set()
    for surface, raw_name, identity, description, input_schema, output_schema in facts:
        source_identity = (surface, raw_name, identity)
        if source_identity in source_identities:
            raise _validation_error("MCP server contains a duplicate logical capability")
        source_identities.add(source_identity)
        final_name = wrapped_capability_name(config.name, raw_name)
        known_names.add(final_name)
        prepared.append(
            (
                surface,
                raw_name,
                identity,
                final_name,
                description,
                input_schema,
                output_schema,
            )
        )
    selected = _selected_capabilities(known_names, config.enabled_capabilities)
    unknown = selected - known_names
    if unknown:
        raise _validation_error("enabled_capabilities contains an unknown wrapped name")

    enabled_names: set[str] = set()
    entries: list[PersistedMcpCatalogEntry] = []
    for (
        surface,
        raw_name,
        identity,
        final_name,
        description,
        input_schema,
        output_schema,
    ) in prepared:
        enabled = final_name in selected
        if enabled and (final_name in enabled_names or final_name in built_in_names):
            raise McpCatalogError(
                "mcp_within_server_collision",
                f"enabled MCP capability name collides: {final_name}",
            )
        if enabled:
            enabled_names.add(final_name)
        key = (config.name, surface, raw_name, identity, final_name)
        entry_id = existing_ids.get(key)
        if entry_id is None:
            entry_id = entry_id_factory()
        entries.append(
            PersistedMcpCatalogEntry(
                entry_id=entry_id,
                server=config.name,
                surface=surface,  # type: ignore[arg-type]
                raw_name=raw_name,
                invocation_identity=identity,
                final_name=final_name,
                provider_description=description,
                input_schema=input_schema,
                output_schema=output_schema,
                enabled=enabled,
            )
        )
    return sorted(
        entries,
        key=_entry_sort_key,
    )


def _catalog_projection(catalog: PersistedMcpCatalog) -> dict[str, object]:
    servers: list[dict[str, object]] = []
    for server in sorted(catalog.servers, key=lambda value: value.name):
        entries = sorted(
            server.entries,
            key=_entry_sort_key,
        )
        servers.append(
            {
                "name": server.name,
                "entries": [
                    entry.model_dump(
                        mode="json",
                        exclude={"entry_id"},
                        exclude_none=True,
                    )
                    for entry in entries
                ],
            }
        )
    return {"version": catalog.version, "servers": servers}


def catalog_digest(catalog: PersistedMcpCatalog) -> str:
    return hashlib.sha256(canonical_json_bytes(_catalog_projection(catalog))).hexdigest()


def with_catalog_digest(catalog: PersistedMcpCatalog) -> PersistedMcpCatalog:
    payload = catalog.model_dump(mode="python")
    payload["digest"] = catalog_digest(catalog)
    return PersistedMcpCatalog.model_validate(payload, strict=True)


def build_persisted_catalog(
    configs: Sequence[McpServerConfig],
    source_catalog: SourceMcpCatalog,
    *,
    existing_catalog: PersistedMcpCatalog | None = None,
    built_in_names: Collection[str] = (),
    entry_id_factory: Callable[[], UUID],
) -> PersistedMcpCatalog:
    normalized_configs = parse_mcp_server_configs([config.storage_dict() for config in configs])
    config_by_name = {config.name: config for config in normalized_configs}
    source_by_name = {server.name: server for server in source_catalog.servers}
    if not set(source_by_name).issubset(config_by_name):
        raise _validation_error("source catalog contains an unconfigured MCP server")
    if len(canonical_json_bytes(source_catalog)) > DEVICE_CATALOG_BYTES_MAX:
        raise _validation_error("source MCP catalog exceeds the device catalog byte limit")

    existing_ids: dict[tuple[str, str, str, str, str], UUID] = {}
    existing_by_name: dict[str, PersistedMcpServerCatalog] = {}
    if existing_catalog is not None:
        if existing_catalog.digest != catalog_digest(existing_catalog):
            raise _validation_error("existing MCP catalog digest does not match its content")
        existing_ids = {
            _logical_entry_key(entry): entry.entry_id
            for server in existing_catalog.servers
            for entry in server.entries
        }
        existing_by_name = {server.name: server for server in existing_catalog.servers}
    missing_sources = sorted(set(config_by_name) - set(source_by_name) - set(existing_by_name))
    if missing_sources:
        raise _validation_error(
            f"source catalog is missing configured MCP server: {missing_sources[0]}"
        )
    servers: list[PersistedMcpServerCatalog] = []
    for name in sorted(config_by_name):
        config = config_by_name[name]
        source = source_by_name.get(name)
        if source is not None:
            entries = _build_server_entries(
                config,
                source,
                existing_ids=existing_ids,
                built_in_names=built_in_names,
                entry_id_factory=entry_id_factory,
            )
            servers.append(PersistedMcpServerCatalog(name=name, entries=entries))
            continue
        existing_server = existing_by_name.get(name)
        if existing_server is None:
            raise _validation_error(f"source catalog is missing configured MCP server: {name}")
        known_names = {entry.final_name for entry in existing_server.entries}
        selected = _selected_capabilities(known_names, config.enabled_capabilities)
        if selected - known_names or any(
            entry.enabled != (entry.final_name in selected) for entry in existing_server.entries
        ):
            raise _validation_error(f"source catalog is missing a changed MCP server: {name}")
        servers.append(existing_server.model_copy(deep=True))

    total_capabilities = sum(len(server.entries) for server in servers)
    if total_capabilities > DEVICE_CAPABILITY_MAX:
        raise _validation_error(f"device MCP capability count exceeds {DEVICE_CAPABILITY_MAX}")
    enabled_names: dict[str, str] = {}
    for server in servers:
        for entry in server.entries:
            if not entry.enabled:
                continue
            if entry.final_name in built_in_names:
                raise McpCatalogError(
                    "mcp_within_server_collision",
                    f"enabled MCP capability name collides: {entry.final_name}",
                )
            previous_server = enabled_names.get(entry.final_name)
            if previous_server is not None:
                raise McpCatalogError(
                    "mcp_schema_collision",
                    f"configured MCP servers collide on name: {entry.final_name}",
                )
            enabled_names[entry.final_name] = server.name
    catalog = with_catalog_digest(PersistedMcpCatalog(version=1, digest="0" * 64, servers=servers))
    if len(canonical_json_bytes(catalog)) > DEVICE_CATALOG_BYTES_MAX:
        raise _validation_error("persisted MCP catalog exceeds the device catalog byte limit")
    return catalog


def _merge_identity(entry: PersistedMcpCatalogEntry) -> bytes:
    return canonical_json_bytes(
        {
            "server": entry.server,
            "surface": entry.surface,
            "invocation_identity": entry.invocation_identity,
            "provider_description": entry.provider_description,
            "input_schema": entry.input_schema,
            "output_schema": entry.output_schema,
        }
    )


def provider_tool_for_entry(
    entry: PersistedMcpCatalogEntry,
    *,
    sites: Sequence[str],
    selector_description: str,
) -> ProviderMcpTool:
    """Build the canonical Provider schema for one logical MCP entry."""

    input_schema = deepcopy(entry.input_schema)
    properties = input_schema.get("properties")
    required = input_schema.get("required", [])
    if not isinstance(properties, dict) or not isinstance(required, list):
        raise _validation_error("persisted MCP provider schema is invalid")
    if DEVICE_FIELD_NAME in properties:
        raise _validation_error(f"persisted MCP schema reserves {DEVICE_FIELD_NAME}")
    properties[DEVICE_FIELD_NAME] = openoctopus_device_field(
        selector_description,
        sites=tuple(sites),
    )
    input_schema["required"] = [*required, DEVICE_FIELD_NAME]
    return ProviderMcpTool(
        name=entry.final_name,
        description=entry.provider_description,
        input_schema=input_schema,
    )


def validate_persisted_catalog_entry(entry: PersistedMcpCatalogEntry) -> None:
    """Validate persisted facts even when no fresh discovery source is present."""

    _validate_capability_value(entry)
    if entry.final_name != wrapped_capability_name(entry.server, entry.raw_name):
        raise _validation_error("persisted MCP final name does not match its source identity")
    _validate_tool_schema(entry.input_schema)
    if entry.output_schema is not None:
        if entry.surface != "tool":
            raise _validation_error("only MCP tools may define an output schema")
        _validate_capability_value(entry.output_schema)
    expected_prefix = _provider_description(entry.server, entry.surface, None)
    if not entry.provider_description.startswith(expected_prefix):
        raise _validation_error("persisted MCP provider description is invalid")
    if entry.surface in {"tool", "prompt"}:
        if entry.invocation_identity != entry.raw_name:
            raise _validation_error("persisted MCP invocation identity is invalid")
        return
    if entry.surface == "resource":
        SourceMcpResource(
            raw_name=entry.raw_name,
            uri=entry.invocation_identity,
        )
        if canonical_json_bytes(entry.input_schema) != canonical_json_bytes(
            _resource_schema()
        ):
            raise _validation_error("persisted MCP resource schema is invalid")
        return
    template = SourceMcpResourceTemplate(
        raw_name=entry.raw_name,
        uri_template=entry.invocation_identity,
    )
    if canonical_json_bytes(entry.input_schema) != canonical_json_bytes(
        _template_schema(template)
    ):
        raise _validation_error("persisted MCP resource template schema is invalid")


def merge_owner_catalogs(
    catalogs_by_device: Mapping[str, PersistedMcpCatalog],
    *,
    built_in_names: Collection[str] = (),
) -> list[ProviderMcpTool]:
    grouped: dict[str, list[tuple[str, PersistedMcpCatalogEntry]]] = defaultdict(list)
    for device_name, catalog in catalogs_by_device.items():
        if (
            len(device_name) > 64
            or _DEVICE_NAME.fullmatch(device_name) is None
            or device_name == "server"
        ):
            raise _validation_error("owner catalog contains an invalid device name")
        if catalog.digest != catalog_digest(catalog):
            raise _validation_error("persisted MCP catalog digest does not match its content")
        seen_names: set[str] = set()
        for server in catalog.servers:
            for entry in server.entries:
                if not entry.enabled:
                    continue
                if entry.final_name in seen_names:
                    raise McpCatalogError(
                        "mcp_schema_collision",
                        f"device catalog repeats enabled name: {entry.final_name}",
                    )
                seen_names.add(entry.final_name)
                grouped[entry.final_name].append((device_name, entry))

    if len(grouped) > OWNER_CAPABILITY_MAX:
        raise McpCatalogError(
            "mcp_owner_schema_limit",
            f"owner MCP capability count exceeds {OWNER_CAPABILITY_MAX}",
        )
    merged: list[ProviderMcpTool] = []
    for final_name in sorted(grouped):
        if final_name in built_in_names:
            raise McpCatalogError(
                "mcp_schema_collision",
                f"MCP capability collides with a built-in tool: {final_name}",
            )
        installs = grouped[final_name]
        reference = installs[0][1]
        reference_identity = _merge_identity(reference)
        if any(_merge_identity(entry) != reference_identity for _, entry in installs[1:]):
            raise McpCatalogError(
                "mcp_schema_collision",
                f"MCP install sites disagree on schema: {final_name}",
            )
        sites = sorted({device_name for device_name, _ in installs})
        if len(sites) != len(installs):
            raise McpCatalogError(
                "mcp_schema_collision",
                f"MCP device contains duplicate install routes: {final_name}",
            )
        merged.append(
            provider_tool_for_entry(
                reference,
                sites=sites,
                selector_description=(
                    "Which paired device should execute this MCP capability."
                ),
            )
        )
    if (
        len(
            canonical_json_bytes(
                [tool.model_dump(mode="json", exclude_none=True) for tool in merged]
            )
        )
        > OWNER_PROVIDER_SCHEMA_BYTES_MAX
    ):
        raise McpCatalogError(
            "mcp_owner_schema_limit",
            "owner MCP provider schema exceeds 256 KiB",
        )
    return merged
