from collections.abc import Iterable
from copy import deepcopy
from dataclasses import replace
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError
from openctopus_server.tools.base import Tool, ToolContext, ToolResult, ToolRoutingMode
from openctopus_server.tools.device_field import (
    DEVICE_FIELD_MARKER,
    DEVICE_FIELD_NAME,
    openoctopus_device_field,
)
from openctopus_server.tools.result import normalize_tool_result
from openctopus_server.tools.web_fetch import Resolver, WebFetchTool
from openctopus_server.tools.workspace_backend import WorkspaceToolDispatcher
from openctopus_server.tools.workspace_files import build_workspace_file_tools
from openctopus_server.workspace.service import WorkspaceService


def inject_device_routing(
    tool_schema: dict[str, Any],
    *,
    sites: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    merged = deepcopy(tool_schema)
    input_schema = _input_schema(merged)
    properties = _properties(input_schema)
    if DEVICE_FIELD_NAME in properties:
        raise ValueError(f"source schema already defines {DEVICE_FIELD_NAME}")

    properties[DEVICE_FIELD_NAME] = openoctopus_device_field(
        "Which install site to execute on.",
        sites=sites,
    )
    required = input_schema.setdefault("required", [])
    if not isinstance(required, list):
        raise ValueError("tool input_schema.required must be a list")
    required.append(DEVICE_FIELD_NAME)
    return merged


def extend_openoctopus_device_enums(
    tool_schema: dict[str, Any],
    *,
    extra: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    merged = deepcopy(tool_schema)
    properties = _properties(_input_schema(merged))
    for value in properties.values():
        if not isinstance(value, dict) or value.get(DEVICE_FIELD_MARKER) is not True:
            continue
        current = value.get("enum")
        if not isinstance(current, list):
            raise ValueError("marked device field must define an enum list")
        value["enum"] = [*current, *(site for site in extra if site not in current)]
    return merged


class ToolRegistry:
    def __init__(self, tools: Iterable[Tool]) -> None:
        self._tools: dict[str, Tool] = {}
        for tool in tools:
            name = tool.name()
            if name in self._tools:
                raise ValueError(f"duplicate tool name: {name}")
            schema_name = tool.schema().get("name")
            if schema_name != name:
                raise ValueError(f"tool name/schema mismatch: {name!r} != {schema_name!r}")
            self._tools[name] = tool

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        schemas: list[dict[str, Any]] = []
        for tool in self._tools.values():
            if tool.routing_mode is ToolRoutingMode.ROUTING_ONLY:
                schema = inject_device_routing(tool.schema(), sites=("server",))
            elif tool.routing_mode is ToolRoutingMode.INTRINSIC_DEVICE:
                schema = extend_openoctopus_device_enums(tool.schema(), extra=())
            else:
                schema = deepcopy(tool.schema())
            schemas.append(schema)
        return schemas

    async def execute(
        self,
        *,
        name: str,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        tool = self._tools.get(name)
        if tool is None:
            return _normalized_error(
                ErrorCode.TOOL_INVALID_ARGS,
                f"Unknown tool: {name}",
            )

        routed_args = dict(args)
        routed_ctx = ctx
        if tool.routing_mode is ToolRoutingMode.ROUTING_ONLY:
            device = routed_args.pop(DEVICE_FIELD_NAME, None)
            if device is None:
                return _normalized_error(
                    ErrorCode.TOOL_MISSING_REQUIRED_FIELD,
                    f"Missing required field: {DEVICE_FIELD_NAME}",
                )
            if device != "server":
                return _normalized_error(
                    ErrorCode.TOOL_DEVICE_UNREACHABLE,
                    f"Tool install site is unavailable: {device}",
                )
            routed_ctx = replace(ctx, openoctopus_device=device)

        try:
            result = await tool.execute(routed_args, routed_ctx)
        except OpenOctopusError as exc:
            result = ToolResult(
                content=_error_text(exc.code, exc.message),
                is_error=True,
                code=exc.code,
            )
        except Exception:
            result = ToolResult(
                content=_error_text(ErrorCode.TOOL_DB_ERROR, "Tool execution failed"),
                is_error=True,
                code=ErrorCode.TOOL_DB_ERROR,
            )

        return ToolResult(
            content=normalize_tool_result(
                result.content,
                max_chars=tool.max_output_chars(),
            ),
            is_error=result.is_error,
            code=result.code,
        )


def build_py3_registry(
    *,
    resolver: Resolver | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> ToolRegistry:
    return ToolRegistry((WebFetchTool(resolver=resolver, transport=transport),))


def build_py4_registry(
    engine: AsyncEngine,
    workspace_service: WorkspaceService,
    *,
    resolver: Resolver | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> ToolRegistry:
    backend = WorkspaceToolDispatcher(engine, workspace_service)
    return ToolRegistry(
        (
            WebFetchTool(resolver=resolver, transport=transport),
            *build_workspace_file_tools(backend),
        )
    )


def _normalized_error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=normalize_tool_result(_error_text(code, message)),
        is_error=True,
        code=code,
    )


def _error_text(code: ErrorCode, message: str) -> str:
    return f"[{code.value}] {message}"


def _input_schema(tool_schema: dict[str, Any]) -> dict[str, Any]:
    value = tool_schema.get("input_schema")
    if not isinstance(value, dict):
        raise ValueError("tool schema must contain an input_schema object")
    return value


def _properties(input_schema: dict[str, Any]) -> dict[str, Any]:
    value = input_schema.get("properties")
    if not isinstance(value, dict):
        raise ValueError("tool input_schema must contain a properties object")
    return value
