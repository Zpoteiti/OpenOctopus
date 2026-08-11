import math
from collections.abc import Awaitable, Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import replace
from functools import lru_cache
from typing import Any, Protocol
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.admission import KeyedAdmission
from openctopus_server.config import get_settings
from openctopus_server.db.models import Device
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.protocol import MAX_TEXT_FRAME_BYTES, ToolResultFrame
from openctopus_server.devices.registry import (
    DeviceBusyError,
    DeviceRegistry,
    DeviceUnavailableError,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError
from openctopus_server.tools.base import Tool, ToolContext, ToolResult, ToolRoutingMode
from openctopus_server.tools.device_field import (
    DEVICE_FIELD_MARKER,
    DEVICE_FIELD_NAME,
    openoctopus_device_field,
)
from openctopus_server.tools.file_transfer import FileTransferTool
from openctopus_server.tools.message import MessageTool
from openctopus_server.tools.result import normalize_tool_result
from openctopus_server.tools.web_fetch import HtmlContentConverter, Resolver, WebFetchTool
from openctopus_server.tools.workspace_backend import WorkspaceToolDispatcher
from openctopus_server.tools.workspace_files import build_workspace_file_tools
from openctopus_server.workspace.file_content import DocumentParser
from openctopus_server.workspace.service import WorkspaceService

_DEVICE_TOOL_TIMEOUT_SECONDS = 60.0
_MAX_JSON_BYTES_PER_CHAR = 6
_RESULT_ENVELOPE_BYTES = 4096


class DeviceToolDispatcher(Protocol):
    async def dispatch_tool(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        expected_device_name: str | None = None,
    ) -> ToolResultFrame: ...


DeviceResolver = Callable[[UUID, str], Awaitable[UUID | None]]


@lru_cache
def get_content_converter() -> DocumentParser:
    settings = get_settings()
    return DocumentParser(
        admission=KeyedAdmission(
            global_limit=settings.content_conversion_max_concurrency,
            per_key_limit=1,
            timeout_seconds=settings.content_conversion_queue_timeout_seconds,
        ),
        memory_mb=settings.content_conversion_memory_mb,
        timeout_seconds=settings.content_conversion_timeout_seconds,
    )


@lru_cache
def get_web_fetch_admission() -> KeyedAdmission:
    settings = get_settings()
    return KeyedAdmission(
        global_limit=settings.web_fetch_max_concurrency,
        per_key_limit=settings.web_fetch_max_concurrency_per_user,
        timeout_seconds=settings.web_fetch_queue_timeout_seconds,
    )


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
    input_schema = _input_schema(merged)
    properties = _properties(input_schema)
    for value in properties.values():
        if not isinstance(value, dict) or value.get(DEVICE_FIELD_MARKER) is not True:
            continue
        current = value.get("enum")
        if not isinstance(current, list):
            raise ValueError("marked device field must define an enum list")
        value["enum"] = [*current, *(site for site in extra if site not in current)]
    if input_schema.get("x-openoctopus-same-device") is True:
        _extend_same_device_constraint(input_schema, extra=extra)
    return merged


def _extend_same_device_constraint(
    input_schema: dict[str, Any], *, extra: list[str] | tuple[str, ...]
) -> None:
    """Advertise server endpoints plus equal paired-client endpoints.

    JSON Schema has no portable operator for equality between two arbitrary
    string properties.  Paired device names are a finite snapshot at schema
    construction time, so adding one branch per name keeps different-client
    combinations outside the provider-visible contract.
    """

    branches = input_schema.get("anyOf")
    if not isinstance(branches, list):
        raise ValueError("same-device schema must define anyOf branches")
    pair_fields = ("openoctopus_src_device", "openoctopus_dst_device")
    for name in extra:
        branches.append(
            {
                "properties": {
                    field: {"const": name}
                    for field in pair_fields
                }
            }
        )


class ToolRegistry:
    def __init__(
        self,
        tools: Iterable[Tool],
        *,
        device_resolver: DeviceResolver | None = None,
    ) -> None:
        self._device_resolver = device_resolver
        self._tools: dict[str, Tool] = {}
        for tool in tools:
            name = tool.name()
            if name in self._tools:
                raise ValueError(f"duplicate tool name: {name}")
            schema_name = tool.schema().get("name")
            if schema_name != name:
                raise ValueError(f"tool name/schema mismatch: {name!r} != {schema_name!r}")
            self._tools[name] = tool

    def get_tool_schemas(self, *, device_names: Iterable[str] = ()) -> list[dict[str, Any]]:
        device_sites = tuple(device_names)
        schemas: list[dict[str, Any]] = []
        for tool in self._tools.values():
            if tool.routing_mode is ToolRoutingMode.ROUTING_ONLY:
                schema = inject_device_routing(tool.schema(), sites=("server", *device_sites))
            elif tool.routing_mode is ToolRoutingMode.INTRINSIC_DEVICE:
                schema = extend_openoctopus_device_enums(tool.schema(), extra=device_sites)
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
        device_targets: Mapping[str, UUID] | None = None,
        device_registry: DeviceToolDispatcher | None = None,
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
                device_id = device_targets.get(device) if device_targets is not None else None
                if device_id is None or device_registry is None:
                    return _normalized_error(
                        ErrorCode.TOOL_DEVICE_UNREACHABLE,
                        f"Tool install site is unavailable: {device}",
                    )
                if self._device_resolver is not None:
                    live_device_id = await self._device_resolver(ctx.user_id, device)
                    if live_device_id != device_id:
                        return _normalized_error(
                            ErrorCode.TOOL_DEVICE_UNREACHABLE,
                            f"Tool install site is unavailable: {device}",
                        )
                return await _execute_on_device(
                    device_registry=device_registry,
                    device_id=device_id,
                    device_name=device,
                    name=name,
                    args=routed_args,
                    ctx=ctx,
                    max_output_chars=tool.max_output_chars(),
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
            side_effect=result.side_effect if not result.is_error else None,
        )


def build_py3_registry(
    *,
    resolver: Resolver | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    web_admission: KeyedAdmission | None = None,
    content_converter: HtmlContentConverter | None = None,
) -> ToolRegistry:
    return ToolRegistry(
        (
            WebFetchTool(
                web_admission=web_admission or get_web_fetch_admission(),
                content_converter=content_converter or get_content_converter(),
                resolver=resolver,
                transport=transport,
            ),
        )
    )


def build_py4_registry(
    engine: AsyncEngine,
    workspace_service: WorkspaceService,
    *,
    resolver: Resolver | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    web_admission: KeyedAdmission | None = None,
    content_converter: DocumentParser | None = None,
    device_registry: DeviceRegistry | None = None,
) -> ToolRegistry:
    settings = get_settings()
    converter = content_converter or get_content_converter()
    backend = WorkspaceToolDispatcher(
        engine,
        workspace_service,
        document_parser=converter,
    )
    return ToolRegistry(
        (
            WebFetchTool(
                web_admission=web_admission or get_web_fetch_admission(),
                content_converter=converter,
                resolver=resolver,
                transport=transport,
            ),
            MessageTool(engine, workspace_service),
            FileTransferTool(
                engine,
                workspace_service,
                device_registry or get_device_registry(),
            ),
            *build_workspace_file_tools(
                backend,
                document_read_timeout_seconds=math.ceil(
                    5
                    + settings.content_conversion_queue_timeout_seconds
                    + 30
                    + settings.content_conversion_timeout_seconds
                    + 5
                ),
            ),
        ),
        device_resolver=_owned_device_resolver(engine),
    )


def _owned_device_resolver(engine: AsyncEngine) -> DeviceResolver:
    async def resolve(user_id: UUID, name: str) -> UUID | None:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            device_id = await db.scalar(
                select(Device.id).where(Device.user_id == user_id, Device.name == name)
            )
        return device_id if isinstance(device_id, UUID) else None

    return resolve


def _normalized_error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=normalize_tool_result(_error_text(code, message)),
        is_error=True,
        code=code,
    )


def _error_text(code: ErrorCode, message: str) -> str:
    return f"[{code.value}] {message}"


async def _execute_on_device(
    *,
    device_registry: DeviceToolDispatcher,
    device_id: UUID,
    device_name: str,
    name: str,
    args: dict[str, Any],
    ctx: ToolContext,
    max_output_chars: int,
) -> ToolResult:
    try:
        raw = await device_registry.dispatch_tool(
            device_id=device_id,
            user_id=ctx.user_id,
            name=name,
            args=args,
            max_result_bytes=_device_result_credit(name, max_output_chars),
            timeout_seconds=_DEVICE_TOOL_TIMEOUT_SECONDS,
            expected_device_name=device_name,
        )
    except DeviceBusyError:
        return _normalized_error(
            ErrorCode.TOOL_DEVICE_BUSY,
            "Tool install site is busy",
        )
    except (DeviceUnavailableError, TimeoutError):
        return _normalized_error(
            ErrorCode.TOOL_DEVICE_UNREACHABLE,
            "Tool install site became unavailable",
        )

    code: ErrorCode | None = None
    if raw.code is not None:
        try:
            code = ErrorCode(raw.code)
        except ValueError:
            pass
    content = (
        raw.content
        if isinstance(raw.content, str)
        else [block.model_dump() for block in raw.content]
    )
    return ToolResult(
        content=normalize_tool_result(content, max_chars=max_output_chars),
        is_error=raw.is_error,
        code=code,
    )


def _device_result_credit(name: str, max_output_chars: int) -> int:
    if name == "read_file":
        # An 8 MiB image is returned as base64 safe blocks rather than bounded
        # text, so it needs the full control-frame allowance.
        return MAX_TEXT_FRAME_BYTES
    return min(
        MAX_TEXT_FRAME_BYTES,
        max_output_chars * _MAX_JSON_BYTES_PER_CHAR + _RESULT_ENVELOPE_BYTES,
    )


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
