"""Server-local MCP discovery and canonical catalog contracts."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from uuid import UUID

import uritemplate
from jsonschema import SchemaError as JsonSchemaSchemaError
from jsonschema.validators import validator_for
from mcp import types
from pydantic import AnyUrl, BaseModel, TypeAdapter, ValidationError

from openctopus_server.devices.mcp_catalog import (
    CAPABILITY_BYTES_MAX,
    DEVICE_CATALOG_BYTES_MAX,
    EMPTY_CATALOG_DIGEST,
    JSON_NESTING_MAX,
    McpCatalogError,
    build_persisted_catalog,
    canonical_json_bytes,
    catalog_digest,
    with_catalog_digest,
)
from openctopus_server.devices.mcp_models import (
    McpServerConfig,
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
    parse_mcp_server_configs,
)
from openctopus_server.mcp.models import ServerMcpServerConfig

SURFACE_PAGE_MAX = 16
SERVER_CAPABILITY_MAX = 256
SERVER_CATALOG_CAPABILITY_MAX = 512
CURSOR_BYTES_MAX = 4096

_DEVICE_FIELD_NAME = "openoctopus_device"
_ANY_URL_ADAPTER = TypeAdapter(AnyUrl)
_VAR_SEGMENT = r"(?:[A-Za-z0-9_]|%[0-9A-Fa-f]{2})+"
_VAR_NAME = re.compile(rf"^{_VAR_SEGMENT}(?:\.{_VAR_SEGMENT})*$")
_TEMPLATE_OPERATORS = frozenset("+#./;?&")

type McpSurface = Literal["tool", "resource", "resource_template", "prompt"]
type SourceRouteKey = tuple[McpSurface, str, str]


def _validation_error(message: str) -> McpCatalogError:
    return McpCatalogError("config_validation_failed", message)


class CatalogSession(Protocol):
    def get_server_capabilities(self) -> types.ServerCapabilities | None: ...

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult: ...

    async def list_resources(self, cursor: str | None = None) -> types.ListResourcesResult: ...

    async def list_resource_templates(
        self, cursor: str | None = None
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
    input_schema: dict[str, Any]
    enabled: bool


def normalized_resource_uri(value: str) -> AnyUrl:
    if len(value) > 4096:
        raise _validation_error("resource URI exceeds 4096 characters")
    try:
        return _ANY_URL_ADAPTER.validate_python(value, strict=True)
    except ValidationError:
        raise _validation_error("resource URI is invalid") from None


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


@dataclass(slots=True)
class _DiscoveryBudget:
    count: int = 0
    projected_bytes: int = 0

    def add(self, value: object) -> None:
        self.count += 1
        if self.count > SERVER_CAPABILITY_MAX:
            raise _validation_error(
                f"MCP server capability count exceeds {SERVER_CAPABILITY_MAX}"
            )
        self.projected_bytes += _validate_capability(value)
        if self.projected_bytes > DEVICE_CATALOG_BYTES_MAX:
            raise _validation_error("source MCP catalog exceeds the Server catalog byte limit")


async def _paginate[T, U](
    surface: str,
    fetch: Callable[[str | None], Awaitable[tuple[list[T], str | None]]],
    project: Callable[[T], U],
    budget: _DiscoveryBudget,
) -> list[U]:
    cursor: str | None = None
    seen: set[str] = set()
    items: list[U] = []
    for page_index in range(SURFACE_PAGE_MAX):
        page, next_cursor = await fetch(cursor)
        if len(items) + len(page) > SERVER_CAPABILITY_MAX:
            raise _validation_error(
                f"MCP {surface} item count exceeds {SERVER_CAPABILITY_MAX}"
            )
        projected = [project(value) for value in page]
        for item in projected:
            budget.add(item)
        del page
        items.extend(projected)
        if next_cursor is None:
            return items
        try:
            cursor_size = len(next_cursor.encode("utf-8"))
        except UnicodeError:
            raise _validation_error(f"MCP {surface} returned an invalid cursor") from None
        if cursor_size > CURSOR_BYTES_MAX:
            raise _validation_error(f"MCP {surface} cursor exceeds {CURSOR_BYTES_MAX} bytes")
        if next_cursor in seen:
            raise _validation_error(f"MCP {surface} returned a repeated cursor")
        if page_index + 1 >= SURFACE_PAGE_MAX:
            raise _validation_error(
                f"MCP {surface} pagination exceeds {SURFACE_PAGE_MAX} pages"
            )
        seen.add(next_cursor)
        cursor = next_cursor
    raise AssertionError("pagination loop exhausted without returning")


def _validate_capability(value: object) -> int:
    size = len(canonical_json_bytes(value))
    if size > CAPABILITY_BYTES_MAX:
        raise _validation_error("single MCP capability exceeds 256 KiB")
    return size


def _validate_tool_schema(schema: dict[str, Any]) -> None:
    _validate_capability(schema)
    try:
        validator_for(schema).check_schema(schema)
    except JsonSchemaSchemaError:
        raise _validation_error("MCP tool input schema is invalid") from None
    if schema.get("type") != "object" or not isinstance(schema.get("properties"), dict):
        raise _validation_error("MCP tool input schema must be an object with properties")
    properties = schema["properties"]
    assert isinstance(properties, dict)
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
    surface: McpSurface, raw_name: str, invocation_identity: str
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
            server.model_dump(mode="python"), strict=True
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
            _validate_capability(tool.output_schema)
        _validate_capability(tool)
    for resource in cloned.resources:
        normalized_resource_uri(resource.uri)
        _validate_capability(resource)
    for template in cloned.resource_templates:
        variables = extract_resource_template_variables(template.uri_template)
        if _DEVICE_FIELD_NAME in variables:
            raise _validation_error(f"MCP resource template reserves {_DEVICE_FIELD_NAME}")
        _validate_capability(template)
    for prompt in cloned.prompts:
        names = [argument.name for argument in prompt.arguments]
        if len(names) != len(set(names)):
            raise _validation_error("MCP prompt arguments must be unique")
        if _DEVICE_FIELD_NAME in names:
            raise _validation_error(f"MCP prompt reserves {_DEVICE_FIELD_NAME}")
        _validate_capability(prompt)
    _server_route_keys(cloned)
    cloned.tools.sort(key=lambda item: item.raw_name)
    cloned.resources.sort(key=lambda item: (item.uri, item.raw_name))
    cloned.resource_templates.sort(key=lambda item: (item.uri_template, item.raw_name))
    cloned.prompts.sort(key=lambda item: item.raw_name)
    return cloned


def canonicalize_source_catalog(catalog: SourceMcpCatalog) -> SourceMcpCatalog:
    try:
        validated = SourceMcpCatalog.model_validate(
            catalog.model_dump(mode="python"), strict=True
        )
    except ValidationError:
        raise _validation_error("MCP source catalog shape is invalid") from None
    canonical = SourceMcpCatalog(
        version=1,
        servers=sorted(
            (_canonicalize_server(server) for server in validated.servers),
            key=lambda server: server.name,
        ),
    )
    if sum(server.capability_count for server in canonical.servers) > SERVER_CATALOG_CAPABILITY_MAX:
        raise _validation_error(
            f"Server MCP capability count exceeds {SERVER_CATALOG_CAPABILITY_MAX}"
        )
    if len(canonical_json_bytes(canonical)) > DEVICE_CATALOG_BYTES_MAX:
        raise _validation_error("source MCP catalog exceeds the Server catalog byte limit")
    return canonical


async def discover_server_catalog(
    server_name: str, session: CatalogSession
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

    budget = _DiscoveryBudget()
    tools = (
        await _paginate("tools", fetch_tools, _source_tool, budget)
        if capabilities.tools is not None
        else []
    )
    if capabilities.resources is not None:
        resources = await _paginate("resources", fetch_resources, _source_resource, budget)
        templates = await _paginate(
            "resource templates", fetch_templates, _source_template, budget
        )
    else:
        resources = []
        templates = []
    prompts = (
        await _paginate("prompts", fetch_prompts, _source_prompt, budget)
        if capabilities.prompts is not None
        else []
    )
    try:
        source = SourceMcpServerCatalog(
            name=server_name,
            tools=tools,
            resources=resources,
            resource_templates=templates,
            prompts=prompts,
        )
    except ValidationError:
        raise _validation_error("MCP source catalog shape is invalid") from None
    return _canonicalize_server(source)


def _device_configs(
    configs: Sequence[ServerMcpServerConfig],
) -> tuple[McpServerConfig, ...]:
    values: list[dict[str, Any]] = []
    for config in configs:
        value = config.storage_dict()
        value.pop("max_concurrent_calls", None)
        values.append(value)
    return parse_mcp_server_configs(values)


def build_server_persisted_catalog(
    configs: Sequence[ServerMcpServerConfig],
    source_catalog: SourceMcpCatalog,
    *,
    existing_catalog: PersistedMcpCatalog | None = None,
    built_in_names: Collection[str] = (),
    entry_id_factory: Callable[[], UUID],
) -> PersistedMcpCatalog:
    canonical_source = canonicalize_source_catalog(source_catalog)
    return build_persisted_catalog(
        _device_configs(configs),
        canonical_source,
        existing_catalog=existing_catalog,
        built_in_names=built_in_names,
        entry_id_factory=entry_id_factory,
    )


def _persisted_key(entry: PersistedMcpCatalogEntry) -> SourceRouteKey:
    return _source_route_key(entry.surface, entry.raw_name, entry.invocation_identity)


def bind_server_entries(
    source: SourceMcpServerCatalog,
    persisted: PersistedMcpServerCatalog,
) -> dict[UUID, McpEntryRoute]:
    canonical_source = _canonicalize_server(source)
    if canonical_source.name != persisted.name:
        raise _validation_error("persisted MCP server does not match source catalog")
    source_keys = _server_route_keys(canonical_source)
    persisted_keys = [_persisted_key(entry) for entry in persisted.entries]
    if len(persisted_keys) != len(set(persisted_keys)) or source_keys != set(persisted_keys):
        raise _validation_error("persisted MCP catalog does not match source identity set")
    return {
        entry.entry_id: McpEntryRoute(
            entry_id=entry.entry_id,
            server=entry.server,
            surface=entry.surface,
            raw_name=entry.raw_name,
            invocation_identity=entry.invocation_identity,
            final_name=entry.final_name,
            input_schema=deepcopy(entry.input_schema),
            enabled=entry.enabled,
        )
        for entry in persisted.entries
    }


__all__ = [
    "EMPTY_CATALOG_DIGEST",
    "JSON_NESTING_MAX",
    "CatalogSession",
    "McpCatalogError",
    "McpEntryRoute",
    "bind_server_entries",
    "build_server_persisted_catalog",
    "canonical_json_bytes",
    "canonicalize_source_catalog",
    "catalog_digest",
    "discover_server_catalog",
    "expand_resource_template",
    "extract_resource_template_variables",
    "normalized_resource_uri",
    "with_catalog_digest",
]
