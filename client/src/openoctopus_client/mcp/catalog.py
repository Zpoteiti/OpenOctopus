from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from uuid import UUID

import uritemplate
from mcp import types
from pydantic import AnyUrl, BaseModel, TypeAdapter, ValidationError

from openoctopus_client.mcp.models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    PromptArgument,
    SourceMcpCatalog,
    SourceMcpPrompt,
    SourceMcpResource,
    SourceMcpResourceTemplate,
    SourceMcpServerCatalog,
    SourceMcpTool,
)

SURFACE_PAGE_MAX = 16
SERVER_CAPABILITY_MAX = 256
DEVICE_CAPABILITY_MAX = 512
CAPABILITY_BYTES_MAX = 256 * 1024
DEVICE_CATALOG_BYTES_MAX = 2 * 1024 * 1024
CURSOR_BYTES_MAX = 4096
JSON_NESTING_MAX = 32

EMPTY_CATALOG_DIGEST = "d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf"

_DEVICE_FIELD_NAME = "openoctopus_device"
_VAR_SEGMENT = r"(?:[A-Za-z0-9_]|%[0-9A-Fa-f]{2})+"
_VAR_NAME = re.compile(rf"^{_VAR_SEGMENT}(?:\.{_VAR_SEGMENT})*$")
_TEMPLATE_OPERATORS = frozenset("+#./;?&")
_ANY_URL_ADAPTER = TypeAdapter(AnyUrl)

type McpSurface = Literal["tool", "resource", "resource_template", "prompt"]
type SourceRouteKey = tuple[McpSurface, str, str]


class McpCatalogError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def _validation_error(message: str) -> McpCatalogError:
    return McpCatalogError("config_validation_failed", message)


class CatalogSession(Protocol):
    def get_server_capabilities(self) -> types.ServerCapabilities | None: ...

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult: ...

    async def list_resources(self, cursor: str | None = None) -> types.ListResourcesResult: ...

    async def list_resource_templates(
        self,
        cursor: str | None = None,
    ) -> types.ListResourceTemplatesResult: ...

    async def list_prompts(self, cursor: str | None = None) -> types.ListPromptsResult: ...


@dataclass(frozen=True, slots=True)
class McpEntryRoute:
    entry_id: UUID
    server: str
    surface: McpSurface
    raw_name: str
    invocation_identity: str
    final_name: str
    enabled: bool


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


def normalized_resource_uri(value: str) -> AnyUrl:
    if len(value) > 4096:
        raise _validation_error("resource URI exceeds 4096 characters")
    try:
        return _ANY_URL_ADAPTER.validate_python(value, strict=True)
    except ValidationError:
        raise _validation_error("resource URI is invalid") from None


def expand_resource_template(template: str, arguments: Mapping[str, str]) -> AnyUrl:
    variables = extract_resource_template_variables(template)
    if set(arguments) != set(variables) or any(
        not isinstance(value, str) or "\x00" in value for value in arguments.values()
    ):
        raise _validation_error("resource template arguments must match its variables")
    try:
        expanded = uritemplate.expand(template, dict(arguments))
    except (TypeError, ValueError) as exc:
        raise _validation_error("resource template could not be expanded") from exc
    if len(expanded) > 4096:
        raise _validation_error("expanded resource URI exceeds 4096 characters")
    return normalized_resource_uri(expanded)


async def _paginate[T](
    surface: str,
    fetch: Callable[[str | None], Awaitable[tuple[list[T], str | None]]],
) -> list[T]:
    cursor: str | None = None
    seen: set[str] = set()
    items: list[T] = []
    for page_index in range(SURFACE_PAGE_MAX):
        page, next_cursor = await fetch(cursor)
        if len(items) + len(page) > SERVER_CAPABILITY_MAX:
            raise _validation_error(
                f"MCP {surface} item count exceeds {SERVER_CAPABILITY_MAX}"
            )
        items.extend(page)
        if next_cursor is None:
            return items
        try:
            cursor_bytes = len(next_cursor.encode("utf-8"))
        except UnicodeError:
            raise _validation_error(f"MCP {surface} returned an invalid cursor") from None
        if cursor_bytes > CURSOR_BYTES_MAX:
            raise _validation_error(f"MCP {surface} cursor exceeds {CURSOR_BYTES_MAX} bytes")
        if next_cursor in seen:
            raise _validation_error(f"MCP {surface} returned a repeated cursor")
        if page_index + 1 >= SURFACE_PAGE_MAX:
            raise _validation_error(f"MCP {surface} pagination exceeds {SURFACE_PAGE_MAX} pages")
        seen.add(next_cursor)
        cursor = next_cursor
    raise AssertionError("pagination loop exhausted without returning")


def _require_server_item_limit(*counts: int) -> None:
    if sum(counts) > SERVER_CAPABILITY_MAX:
        raise _validation_error(
            f"MCP server capability count exceeds {SERVER_CAPABILITY_MAX}"
        )


def _validate_capability_value(value: object) -> None:
    if len(canonical_json_bytes(value)) > CAPABILITY_BYTES_MAX:
        raise _validation_error("single MCP capability exceeds 256 KiB")


def _validate_tool_schema(schema: dict[str, Any]) -> None:
    _validate_capability_value(schema)
    if schema.get("type") != "object":
        raise _validation_error("MCP tool input schema must have top-level type object")
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        raise _validation_error("MCP tool input schema properties must be an object")
    if _DEVICE_FIELD_NAME in properties:
        raise _validation_error(f"MCP source schema reserves {_DEVICE_FIELD_NAME}")
    required = schema.get("required", [])
    if (
        not isinstance(required, list)
        or any(not isinstance(name, str) for name in required)
        or len(required) != len(set(required))
        or not set(required).issubset(properties)
    ):
        raise _validation_error("MCP tool required must be a unique properties subset")


def _validated_model[T: BaseModel](model_type: type[T], **values: object) -> T:
    try:
        return model_type.model_validate(values, strict=True)
    except ValidationError:
        raise _validation_error("MCP capability shape is invalid") from None


def _source_tool(value: types.Tool) -> SourceMcpTool:
    return _validated_model(
        SourceMcpTool,
        raw_name=value.name,
        description=value.description,
        input_schema=value.inputSchema,
        output_schema=value.outputSchema,
    )


def _source_resource(value: types.Resource) -> SourceMcpResource:
    return _validated_model(
        SourceMcpResource,
        raw_name=value.name,
        uri=str(value.uri),
        description=value.description,
        mime_type=value.mimeType,
    )


def _source_template(value: types.ResourceTemplate) -> SourceMcpResourceTemplate:
    return _validated_model(
        SourceMcpResourceTemplate,
        raw_name=value.name,
        uri_template=value.uriTemplate,
        description=value.description,
        mime_type=value.mimeType,
    )


def _source_prompt(value: types.Prompt) -> SourceMcpPrompt:
    arguments = [
        _validated_model(
            PromptArgument,
            name=argument.name,
            description=argument.description,
            required=argument.required is True,
        )
        for argument in value.arguments or []
    ]
    return _validated_model(
        SourceMcpPrompt,
        raw_name=value.name,
        description=value.description,
        arguments=arguments,
    )


def _source_route_key(
    surface: McpSurface,
    raw_name: str,
    invocation_identity: str,
) -> SourceRouteKey:
    return surface, raw_name, invocation_identity


def _server_route_keys(server: SourceMcpServerCatalog) -> set[SourceRouteKey]:
    keys: list[SourceRouteKey] = []
    keys.extend(_source_route_key("tool", item.raw_name, item.raw_name) for item in server.tools)
    keys.extend(
        _source_route_key("resource", item.raw_name, item.uri) for item in server.resources
    )
    keys.extend(
        _source_route_key("resource_template", item.raw_name, item.uri_template)
        for item in server.resource_templates
    )
    keys.extend(
        _source_route_key("prompt", item.raw_name, item.raw_name) for item in server.prompts
    )
    if len(keys) != len(set(keys)):
        raise _validation_error("MCP source catalog contains a duplicate source identity")
    return set(keys)


def _canonicalize_server(server: SourceMcpServerCatalog) -> SourceMcpServerCatalog:
    try:
        cloned = SourceMcpServerCatalog.model_validate(
            server.model_dump(mode="python"),
            strict=True,
        )
    except ValidationError:
        raise _validation_error("MCP source catalog shape is invalid") from None
    if cloned.capability_count > SERVER_CAPABILITY_MAX:
        raise _validation_error(
            f"MCP server capability count exceeds {SERVER_CAPABILITY_MAX}"
        )
    for tool in cloned.tools:
        _validate_tool_schema(tool.input_schema)
        if tool.output_schema is not None:
            _validate_capability_value(tool.output_schema)
        _validate_capability_value(tool)
    for resource in cloned.resources:
        _validate_capability_value(resource)
    for template in cloned.resource_templates:
        variables = extract_resource_template_variables(template.uri_template)
        if _DEVICE_FIELD_NAME in variables:
            raise _validation_error(f"MCP resource template reserves {_DEVICE_FIELD_NAME}")
        _validate_capability_value(template)
    for prompt in cloned.prompts:
        names = [argument.name for argument in prompt.arguments]
        if len(names) != len(set(names)):
            raise _validation_error("MCP prompt arguments must be unique")
        if _DEVICE_FIELD_NAME in names:
            raise _validation_error(f"MCP prompt reserves {_DEVICE_FIELD_NAME}")
        _validate_capability_value(prompt)
    _server_route_keys(cloned)
    cloned.tools.sort(key=lambda item: item.raw_name)
    cloned.resources.sort(key=lambda item: (item.uri, item.raw_name))
    cloned.resource_templates.sort(key=lambda item: (item.uri_template, item.raw_name))
    cloned.prompts.sort(key=lambda item: item.raw_name)
    return cloned


def canonicalize_source_catalog(catalog: SourceMcpCatalog) -> SourceMcpCatalog:
    try:
        validated = SourceMcpCatalog.model_validate(
            catalog.model_dump(mode="python"),
            strict=True,
        )
    except ValidationError:
        raise _validation_error("MCP source catalog shape is invalid") from None
    servers = sorted(
        (_canonicalize_server(server) for server in validated.servers),
        key=lambda server: server.name,
    )
    canonical = SourceMcpCatalog(version=1, servers=servers)
    total = sum(server.capability_count for server in canonical.servers)
    if total > DEVICE_CAPABILITY_MAX:
        raise _validation_error(f"device MCP capability count exceeds {DEVICE_CAPABILITY_MAX}")
    if len(canonical_json_bytes(canonical)) > DEVICE_CATALOG_BYTES_MAX:
        raise _validation_error("source MCP catalog exceeds the device catalog byte limit")
    return canonical


async def discover_server_catalog(
    server_name: str,
    session: CatalogSession,
) -> SourceMcpServerCatalog:
    capabilities = session.get_server_capabilities()
    if capabilities is None:
        raise _validation_error("MCP session has not completed initialize")

    async def fetch_tools(cursor: str | None) -> tuple[list[types.Tool], str | None]:
        result = await session.list_tools(cursor)
        return result.tools, result.nextCursor

    async def fetch_resources(cursor: str | None) -> tuple[list[types.Resource], str | None]:
        result = await session.list_resources(cursor)
        return result.resources, result.nextCursor

    async def fetch_templates(
        cursor: str | None,
    ) -> tuple[list[types.ResourceTemplate], str | None]:
        result = await session.list_resource_templates(cursor)
        return result.resourceTemplates, result.nextCursor

    async def fetch_prompts(cursor: str | None) -> tuple[list[types.Prompt], str | None]:
        result = await session.list_prompts(cursor)
        return result.prompts, result.nextCursor

    tools = (
        await _paginate("tools", fetch_tools) if capabilities.tools is not None else []
    )
    _require_server_item_limit(len(tools))
    if capabilities.resources is not None:
        resources = await _paginate("resources", fetch_resources)
        _require_server_item_limit(len(tools), len(resources))
        templates = await _paginate("resource templates", fetch_templates)
        _require_server_item_limit(len(tools), len(resources), len(templates))
    else:
        resources = []
        templates = []
    prompts = (
        await _paginate("prompts", fetch_prompts) if capabilities.prompts is not None else []
    )
    _require_server_item_limit(len(tools), len(resources), len(templates), len(prompts))
    try:
        source = SourceMcpServerCatalog(
            name=server_name,
            tools=[_source_tool(item) for item in tools],
            resources=[_source_resource(item) for item in resources],
            resource_templates=[_source_template(item) for item in templates],
            prompts=[_source_prompt(item) for item in prompts],
        )
    except ValidationError:
        raise _validation_error("MCP source catalog shape is invalid") from None
    return canonicalize_source_catalog(
        SourceMcpCatalog(version=1, servers=[source])
    ).servers[0]


def _entry_sort_key(entry: PersistedMcpCatalogEntry) -> tuple[str, str, str, str, str]:
    return (
        entry.server,
        entry.surface,
        entry.invocation_identity,
        entry.raw_name,
        entry.final_name,
    )


def _catalog_projection(catalog: PersistedMcpCatalog) -> dict[str, object]:
    servers: list[dict[str, object]] = []
    for server in sorted(catalog.servers, key=lambda value: value.name):
        entries = sorted(server.entries, key=_entry_sort_key)
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


def validate_persisted_catalog(catalog: PersistedMcpCatalog) -> PersistedMcpCatalog:
    try:
        validated = PersistedMcpCatalog.model_validate(
            catalog.model_dump(mode="python"),
            strict=True,
        )
    except ValidationError:
        raise _validation_error("persisted MCP catalog shape is invalid") from None
    total = 0
    for server in validated.servers:
        if len(server.entries) > SERVER_CAPABILITY_MAX:
            raise _validation_error(
                f"MCP server capability count exceeds {SERVER_CAPABILITY_MAX}"
            )
        total += len(server.entries)
        for entry in server.entries:
            _validate_capability_value(entry)
    if total > DEVICE_CAPABILITY_MAX:
        raise _validation_error(f"device MCP capability count exceeds {DEVICE_CAPABILITY_MAX}")
    if len(canonical_json_bytes(validated)) > DEVICE_CATALOG_BYTES_MAX:
        raise _validation_error("persisted MCP catalog exceeds the device catalog byte limit")
    if validated.digest != catalog_digest(validated):
        raise _validation_error("persisted MCP catalog digest does not match its content")
    return validated


def _persisted_route_key(entry: PersistedMcpCatalogEntry) -> SourceRouteKey:
    return _source_route_key(
        entry.surface,
        entry.raw_name,
        entry.invocation_identity,
    )


def bind_server_entries(
    source: SourceMcpServerCatalog,
    persisted: PersistedMcpServerCatalog,
) -> dict[UUID, McpEntryRoute]:
    canonical_source = _canonicalize_server(source)
    if canonical_source.name != persisted.name:
        raise _validation_error("persisted MCP server does not match source catalog")
    source_keys = _server_route_keys(canonical_source)
    persisted_keys = [_persisted_route_key(entry) for entry in persisted.entries]
    if len(persisted_keys) != len(set(persisted_keys)):
        raise _validation_error("persisted MCP catalog repeats a source identity")
    if source_keys != set(persisted_keys):
        raise _validation_error("persisted MCP catalog does not match source identity set")
    return {
        entry.entry_id: McpEntryRoute(
            entry_id=entry.entry_id,
            server=entry.server,
            surface=entry.surface,
            raw_name=entry.raw_name,
            invocation_identity=entry.invocation_identity,
            final_name=entry.final_name,
            enabled=entry.enabled,
        )
        for entry in persisted.entries
    }


def bind_persisted_catalog(
    source: SourceMcpCatalog,
    persisted: PersistedMcpCatalog,
) -> dict[UUID, McpEntryRoute]:
    canonical_source = canonicalize_source_catalog(source)
    validated_persisted = validate_persisted_catalog(persisted)
    source_by_name = {server.name: server for server in canonical_source.servers}
    persisted_by_name = {server.name: server for server in validated_persisted.servers}
    if set(source_by_name) != set(persisted_by_name):
        raise _validation_error("persisted MCP catalog does not match source server set")
    routes: dict[UUID, McpEntryRoute] = {}
    for name in sorted(source_by_name):
        routes.update(bind_server_entries(source_by_name[name], persisted_by_name[name]))
    return routes
