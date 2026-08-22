from __future__ import annotations

import math
from collections.abc import Awaitable, Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import replace
from functools import lru_cache
from typing import Any, Protocol
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.admission import KeyedAdmission
from openctopus_server.config import get_settings
from openctopus_server.db.models import Device
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.mcp_routes import (
    FrozenMcpEntryRoute,
    McpRouteSelectionError,
    OwnerMcpSnapshot,
    select_mcp_call,
)
from openctopus_server.devices.protocol import MAX_TEXT_FRAME_BYTES, ToolResultFrame
from openctopus_server.devices.registry import (
    DeviceBusyError,
    DeviceMcpUnavailableError,
    DeviceOutcomeUnknownError,
    DeviceProtocolError,
    DeviceRegistry,
    DeviceUnavailableError,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError
from openctopus_server.network_policy import SsrfPolicy
from openctopus_server.services.devices import parse_stored_mcp_catalog
from openctopus_server.services.system_config import load_web_fetch_policy
from openctopus_server.tools.base import Tool, ToolContext, ToolResult, ToolRoutingMode
from openctopus_server.tools.device_field import (
    DEVICE_FIELD_MARKER,
    DEVICE_FIELD_NAME,
    openoctopus_device_field,
)
from openctopus_server.tools.file_transfer import FileTransferTool
from openctopus_server.tools.message import MessageTool
from openctopus_server.tools.result import normalize_tool_result
from openctopus_server.tools.truncate import truncate_head
from openctopus_server.tools.web_fetch import (
    HtmlContentConverter,
    PolicyLoader,
    Resolver,
    WebFetchTool,
)
from openctopus_server.tools.workspace_backend import WorkspaceToolDispatcher
from openctopus_server.tools.workspace_files import build_workspace_file_tools
from openctopus_server.workspace.file_content import DocumentParser
from openctopus_server.workspace.service import WorkspaceService

_DEVICE_TOOL_TIMEOUT_SECONDS = 60.0
_EXEC_REPORT_DEFAULT_MS = 30_000
_WRITE_REPORT_DEFAULT_MS = 1_000
_WRITE_WAIT_DEFAULT_MS = 10_000
_DEVICE_TRANSPORT_GRACE_SECONDS = 5.0
_DEVICE_LIST_TIMEOUT_SECONDS = 10.0
_LIST_MAX_OUTPUT_CHARS = 16_000
_EXEC_RESULT_METADATA_CHARS = 16_384
_MAX_JSON_BYTES_PER_CHAR = 6
# Besides the requested output, exec reports may include an absolute cwd built
# from a 4096-character workspace plus a 4096-character relative path.  Reserve
# its worst-case JSON escaping and the fixed status fields without making the
# Client trim authoritative diagnostics to fit transport credit.
_RESULT_ENVELOPE_BYTES = 64 * 1024


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
        chat_session_id: UUID | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ToolResultFrame: ...

    async def dispatch_mcp_tool(
        self,
        *,
        route: FrozenMcpEntryRoute,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        chat_session_id: UUID | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ToolResultFrame: ...


DeviceResolver = Callable[[UUID, str], Awaitable[UUID | None]]
McpRouteResolver = Callable[[UUID, FrozenMcpEntryRoute], Awaitable[bool]]


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
        branches.append({"properties": {field: {"const": name} for field in pair_fields}})


class ToolRegistry:
    def __init__(
        self,
        tools: Iterable[Tool],
        *,
        device_resolver: DeviceResolver | None = None,
        mcp_route_resolver: McpRouteResolver | None = None,
    ) -> None:
        self._device_resolver = device_resolver
        self._mcp_route_resolver = mcp_route_resolver
        self._tools: dict[str, Tool] = {}
        self._provider_schema_cache_key: tuple[tuple[str, ...], str | None] | None = None
        self._provider_schema_cache: tuple[dict[str, Any], ...] = ()
        for tool in tools:
            name = tool.name()
            if name.startswith("mcp_"):
                raise ValueError("the mcp_ prefix is reserved for dynamic MCP capabilities")
            if name in self._tools:
                raise ValueError(f"duplicate tool name: {name}")
            schema_name = tool.schema().get("name")
            if schema_name != name:
                raise ValueError(f"tool name/schema mismatch: {name!r} != {schema_name!r}")
            self._tools[name] = tool

    @property
    def tool_names(self) -> frozenset[str]:
        return frozenset(self._tools)

    def get_tool_schemas(
        self,
        *,
        device_names: Iterable[str] = (),
        mcp_snapshot: OwnerMcpSnapshot | None = None,
    ) -> list[dict[str, Any]]:
        device_sites = tuple(device_names)
        cache_key = (
            device_sites,
            mcp_snapshot.shape_key if mcp_snapshot is not None else None,
        )
        if cache_key == self._provider_schema_cache_key:
            return list(self._provider_schema_cache)
        schemas: list[dict[str, Any]] = []
        for tool in self._tools.values():
            if tool.routing_mode is ToolRoutingMode.CLIENT_ONLY:
                if not device_sites:
                    continue
                schema = inject_device_routing(tool.schema(), sites=device_sites)
            elif tool.routing_mode is ToolRoutingMode.ROUTING_ONLY:
                schema = inject_device_routing(tool.schema(), sites=("server", *device_sites))
            elif tool.routing_mode is ToolRoutingMode.INTRINSIC_DEVICE:
                schema = extend_openoctopus_device_enums(tool.schema(), extra=device_sites)
            else:
                schema = deepcopy(tool.schema())
            schemas.append(schema)
        if mcp_snapshot is not None:
            schemas.extend(
                schema.model_dump(mode="json", exclude_none=True)
                for schema in mcp_snapshot.schemas
            )
        self._provider_schema_cache_key = cache_key
        self._provider_schema_cache = tuple(schemas)
        return list(self._provider_schema_cache)

    async def execute(
        self,
        *,
        name: str,
        args: dict[str, Any],
        ctx: ToolContext,
        device_targets: Mapping[str, UUID] | None = None,
        mcp_snapshot: OwnerMcpSnapshot | None = None,
        device_registry: DeviceToolDispatcher | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ToolResult:
        tool = self._tools.get(name)
        if tool is None:
            try:
                selected = (
                    select_mcp_call(
                        mcp_snapshot,
                        final_name=name,
                        provider_args=args,
                    )
                    if mcp_snapshot is not None
                    else None
                )
            except McpRouteSelectionError as exc:
                return _normalized_error(_selection_error_code(exc), exc.message)
            if selected is None:
                return _normalized_error(
                    ErrorCode.TOOL_INVALID_ARGS,
                    f"Unknown tool: {name}",
                )
            if self._mcp_route_resolver is None:
                return _normalized_error(
                    ErrorCode.TOOL_MCP_UNAVAILABLE,
                    "MCP capability is no longer available",
                )
            try:
                route_is_current = await self._mcp_route_resolver(
                    ctx.user_id,
                    selected.route,
                )
            except Exception:
                return _normalized_error(
                    ErrorCode.TOOL_DB_ERROR,
                    "MCP capability state could not be verified",
                )
            if not route_is_current:
                return _normalized_error(
                    ErrorCode.TOOL_MCP_UNAVAILABLE,
                    "MCP capability changed before it could be called",
                )
            if device_registry is None:
                return _normalized_error(
                    ErrorCode.TOOL_DEVICE_UNREACHABLE,
                    "MCP install site is unavailable",
                )
            return await _execute_mcp_on_device(
                device_registry=device_registry,
                route=selected.route,
                name=name,
                args=selected.source_args,
                ctx=ctx,
                on_issued=on_issued,
            )

        routed_args = dict(args)
        routed_ctx = ctx
        client_limits: tuple[int, float] | None = None
        if tool.routing_mode in {
            ToolRoutingMode.ROUTING_ONLY,
            ToolRoutingMode.CLIENT_ONLY,
        }:
            device = routed_args.pop(DEVICE_FIELD_NAME, None)
            if device is None:
                return _normalized_error(
                    ErrorCode.TOOL_MISSING_REQUIRED_FIELD,
                    f"Missing required field: {DEVICE_FIELD_NAME}",
                )
            if tool.routing_mode is ToolRoutingMode.CLIENT_ONLY:
                try:
                    client_limits = _client_call_limits(name, routed_args)
                except (ValidationError, ValueError):
                    return _normalized_error(
                        ErrorCode.TOOL_INVALID_ARGS,
                        "Tool arguments are invalid",
                    )
            if device != "server" or tool.routing_mode is ToolRoutingMode.CLIENT_ONLY:
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
                    max_output_chars=(
                        client_limits[0] if client_limits is not None else tool.max_output_chars()
                    ),
                    timeout_seconds=(
                        client_limits[1]
                        if client_limits is not None
                        else _DEVICE_TOOL_TIMEOUT_SECONDS
                    ),
                    on_issued=on_issued,
                )
            routed_ctx = replace(ctx, openoctopus_device=device)
        elif tool.routing_mode is ToolRoutingMode.INTRINSIC_DEVICE:
            routed_ctx = replace(ctx, device_targets=device_targets)
        if tool.manages_issue_boundary:
            routed_ctx = replace(routed_ctx, on_issued=on_issued)

        try:
            if on_issued is not None and not tool.manages_issue_boundary:
                on_issued()
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
    engine: AsyncEngine | None = None,
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
                policy_loader=_web_fetch_policy_loader(engine),
            ),
        ),
        mcp_route_resolver=(
            _owned_mcp_route_resolver(engine) if engine is not None else None
        ),
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
                policy_loader=_web_fetch_policy_loader(engine),
            ),
            MessageTool(engine, workspace_service),
            FileTransferTool(
                engine,
                workspace_service,
                device_registry or get_device_registry(),
            ),
            _ClientOnlyTool("exec", _EXEC_SCHEMA),
            _ClientOnlyTool("write_stdin", _WRITE_STDIN_SCHEMA),
            _ClientOnlyTool("list_exec_sessions", _LIST_EXEC_SESSIONS_SCHEMA),
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
        mcp_route_resolver=_owned_mcp_route_resolver(engine),
    )


def _web_fetch_policy_loader(engine: AsyncEngine | None) -> PolicyLoader | None:
    if engine is None:
        return None

    async def load() -> SsrfPolicy:
        return await load_web_fetch_policy(engine)

    return load


class _ClientOnlyTool(Tool):
    routing_mode = ToolRoutingMode.CLIENT_ONLY

    def __init__(self, tool_name: str, schema: dict[str, Any]) -> None:
        self._name = tool_name
        self._schema = schema

    def name(self) -> str:
        return self._name

    def schema(self) -> dict[str, Any]:
        return deepcopy(self._schema)

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        del args, ctx
        return _normalized_error(
            ErrorCode.TOOL_DEVICE_UNREACHABLE,
            "Client-only tool was not routed to a device",
        )


_EXEC_SCHEMA: dict[str, Any] = {
    "name": "exec",
    "description": (
        "在配对设备执行命令。默认使用 pipe；需要 REPL、TTY 检测或行式交互时设置 tty=true，"
        "并为长时间交互显式设置足够大的 timeout。yield_time_ms 不延长 hard timeout。"
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "command": {"type": "string", "minLength": 1, "maxLength": 24000},
            "working_dir": {"type": "string", "minLength": 1, "maxLength": 4096},
            "timeout": {"type": "integer", "minimum": 0, "maximum": 86400},
            "shell": {
                "type": "string",
                "enum": ["bash", "sh", "zsh", "pwsh", "powershell", "powershell_x86", "cmd"],
            },
            "login": {"type": "boolean", "default": False},
            "tty": {"type": "boolean", "default": False},
            "yield_time_ms": {"type": "integer", "minimum": 0, "maximum": 30000},
            "max_output_chars": {
                "type": "integer",
                "minimum": 1000,
                "maximum": 50000,
                "default": 10000,
            },
        },
        "required": ["command"],
        "additionalProperties": False,
    },
}

_WRITE_STDIN_SCHEMA: dict[str, Any] = {
    "name": "write_stdin",
    "description": (
        "查询或操作当前聊天拥有的 exec session。pipe 的唯一非空 chars=\\u0003 是 "
        "OS interrupt 控制操作，不会写入 ETX；tty 将 chars 写入终端，其中 \\u0003 "
        "只是 best-effort Ctrl-C。必须结束进程时使用 terminate=true。"
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "session_id": {"type": "string", "format": "uuid"},
            "chars": {
                "type": "string",
                "maxLength": 65536,
                "description": "最多 65,536 个 Unicode 字符，且 UTF-8 编码最多 65,536 bytes",
            },
            "terminate": {"type": "boolean", "default": False},
            "yield_time_ms": {"type": "integer", "minimum": 0, "maximum": 30000},
            "wait_for": {"type": "string", "minLength": 1, "maxLength": 4096},
            "wait_timeout_ms": {"type": "integer", "minimum": 0, "maximum": 30000},
            "max_output_chars": {
                "type": "integer",
                "minimum": 1000,
                "maximum": 50000,
                "default": 10000,
            },
        },
        "required": ["session_id"],
        "additionalProperties": False,
    },
}

_LIST_EXEC_SESSIONS_SCHEMA: dict[str, Any] = {
    "name": "list_exec_sessions",
    "description": "列出当前聊天在指定可信设备上拥有的 exec sessions。",
    "input_schema": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
}


class _StrictClientArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _ExecArgs(_StrictClientArgs):
    command: str = Field(min_length=1, max_length=24_000)
    working_dir: str | None = Field(default=None, min_length=1, max_length=4096)
    timeout: int | None = Field(default=None, ge=0, le=86_400)
    shell: str | None = None
    login: bool = False
    tty: bool = False
    yield_time_ms: int | None = Field(default=None, ge=0, le=30_000)
    max_output_chars: int = Field(default=10_000, ge=1000, le=50_000)

    @model_validator(mode="after")
    def _validate_cross_fields(self) -> _ExecArgs:
        if "\x00" in self.command or (self.working_dir is not None and "\x00" in self.working_dir):
            raise ValueError("exec strings must not contain NUL")
        if self.shell is not None and self.shell not in {
            "bash",
            "sh",
            "zsh",
            "pwsh",
            "powershell",
            "powershell_x86",
            "cmd",
        }:
            raise ValueError("unsupported shell")
        if self.timeout == 0 and "yield_time_ms" not in self.model_fields_set:
            raise ValueError("unlimited timeout requires explicit yield")
        if (
            self.timeout is not None
            and self.timeout > 60
            and "yield_time_ms" not in self.model_fields_set
        ):
            raise ValueError("long timeout requires explicit yield")
        return self


class _WriteArgs(_StrictClientArgs):
    session_id: str = Field(min_length=36, max_length=36)
    chars: str | None = Field(default=None, max_length=65_536)
    terminate: bool = False
    yield_time_ms: int | None = Field(default=None, ge=0, le=30_000)
    wait_for: str | None = Field(default=None, min_length=1, max_length=4096)
    wait_timeout_ms: int | None = Field(default=None, ge=0, le=30_000)
    max_output_chars: int = Field(default=10_000, ge=1000, le=50_000)

    @field_validator("session_id")
    @classmethod
    def _require_uuid7(cls, value: str) -> str:
        parsed = UUID(value)
        if parsed.version != 7:
            raise ValueError("session ID must be UUID v7")
        return value

    @model_validator(mode="after")
    def _validate_operation(self) -> _WriteArgs:
        if self.chars is not None and len(self.chars.encode("utf-8")) > 65_536:
            raise ValueError("chars exceeds UTF-8 byte limit")
        if self.terminate and ((self.chars is not None and self.chars != "") or self.wait_for):
            raise ValueError("terminate cannot be combined with input or wait_for")
        if self.wait_for is not None and "yield_time_ms" in self.model_fields_set:
            raise ValueError("wait_for and yield_time_ms are mutually exclusive")
        if self.wait_for is None and "wait_timeout_ms" in self.model_fields_set:
            raise ValueError("wait_timeout_ms requires wait_for")
        return self


class _ListArgs(_StrictClientArgs):
    pass


def _client_call_limits(name: str, args: dict[str, Any]) -> tuple[int, float]:
    if name == "exec":
        exec_args = _ExecArgs.model_validate(args, strict=True)
        report_ms = (
            exec_args.yield_time_ms
            if exec_args.yield_time_ms is not None
            else _EXEC_REPORT_DEFAULT_MS
        )
        return (
            exec_args.max_output_chars,
            report_ms / 1000 + _DEVICE_TRANSPORT_GRACE_SECONDS,
        )
    if name == "write_stdin":
        write_args = _WriteArgs.model_validate(args, strict=True)
        if write_args.wait_for is not None:
            report_ms = (
                write_args.wait_timeout_ms
                if write_args.wait_timeout_ms is not None
                else _WRITE_WAIT_DEFAULT_MS
            )
        else:
            report_ms = (
                write_args.yield_time_ms
                if write_args.yield_time_ms is not None
                else _WRITE_REPORT_DEFAULT_MS
            )
        write_grace = _DEVICE_TRANSPORT_GRACE_SECONDS if write_args.chars else 0.0
        return (
            write_args.max_output_chars,
            report_ms / 1000 + _DEVICE_TRANSPORT_GRACE_SECONDS + write_grace,
        )
    if name == "list_exec_sessions":
        _ListArgs.model_validate(args, strict=True)
        return _LIST_MAX_OUTPUT_CHARS, _DEVICE_LIST_TIMEOUT_SECONDS
    raise ValueError("unknown client-only tool")


def _owned_device_resolver(
    engine: AsyncEngine,
) -> DeviceResolver:
    async def resolve(user_id: UUID, name: str) -> UUID | None:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            device_id = await db.scalar(
                select(Device.id).where(
                    Device.user_id == user_id,
                    Device.name == name,
                )
            )
        return device_id if isinstance(device_id, UUID) else None

    return resolve


def _owned_mcp_route_resolver(engine: AsyncEngine) -> McpRouteResolver:
    async def resolve(user_id: UUID, route: FrozenMcpEntryRoute) -> bool:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            row = (
                await db.execute(
                    select(Device.mcp_catalog).where(
                        Device.id == route.device_id,
                        Device.user_id == user_id,
                        Device.name == route.device_name,
                        Device.config_revision == route.config_revision,
                    )
                )
            ).scalar_one_or_none()
        if not isinstance(row, dict):
            return False
        try:
            catalog = parse_stored_mcp_catalog(row)
        except OpenOctopusError:
            return False
        if catalog.digest != route.catalog_digest:
            return False
        return any(
            entry.enabled
            and entry.entry_id == route.entry_id
            and entry.server == route.server
            and entry.surface == route.surface
            and entry.raw_name == route.raw_name
            and entry.invocation_identity == route.invocation_identity
            and entry.final_name == route.final_name
            for server in catalog.servers
            for entry in server.entries
        )

    return resolve


def _selection_error_code(exc: McpRouteSelectionError) -> ErrorCode:
    try:
        return ErrorCode(exc.code)
    except ValueError:
        return ErrorCode.TOOL_INVALID_ARGS


def _normalized_error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=normalize_tool_result(_error_text(code, message)),
        is_error=True,
        code=code,
    )


def _error_text(code: ErrorCode, message: str) -> str:
    return f"[{code.value}] {message}"


async def _execute_mcp_on_device(
    *,
    device_registry: DeviceToolDispatcher,
    route: FrozenMcpEntryRoute,
    name: str,
    args: dict[str, Any],
    ctx: ToolContext,
    on_issued: Callable[[], None] | None,
) -> ToolResult:
    issued = False

    def mark_issued() -> None:
        nonlocal issued
        issued = True
        if on_issued is not None:
            on_issued()

    try:
        raw = await device_registry.dispatch_mcp_tool(
            route=route,
            user_id=ctx.user_id,
            name=name,
            args=args,
            max_result_bytes=MAX_TEXT_FRAME_BYTES,
            timeout_seconds=_DEVICE_TOOL_TIMEOUT_SECONDS,
            chat_session_id=ctx.session_id,
            on_issued=mark_issued,
        )
    except DeviceBusyError:
        return _normalized_error(
            ErrorCode.TOOL_DEVICE_BUSY,
            "MCP install site is busy",
        )
    except DeviceMcpUnavailableError:
        return _normalized_error(
            ErrorCode.TOOL_MCP_UNAVAILABLE,
            "MCP capability is not ready on the selected device",
        )
    except DeviceOutcomeUnknownError:
        return _normalized_error(
            ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
            "MCP call may have executed, but its outcome is unknown; do not replay it blindly",
        )
    except DeviceUnavailableError:
        if issued:
            return _normalized_error(
                ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
                "MCP call may have executed, but its outcome is unknown; do not replay it blindly",
            )
        return _normalized_error(
            ErrorCode.TOOL_DEVICE_UNREACHABLE,
            "MCP install site became unavailable",
        )
    except (DeviceProtocolError, TimeoutError):
        if issued:
            return _normalized_error(
                ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
                "MCP call may have executed, but its outcome is unknown; do not replay it blindly",
            )
        return _normalized_error(
            ErrorCode.TOOL_MCP_ERROR,
            "MCP call failed before it was issued",
        )
    except Exception:
        if issued:
            return _normalized_error(
                ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
                "MCP call may have executed, but its outcome is unknown; do not replay it blindly",
            )
        return _normalized_error(
            ErrorCode.TOOL_MCP_ERROR,
            "MCP call failed",
        )

    code: ErrorCode | None = None
    if raw.code is not None:
        try:
            code = ErrorCode(raw.code)
        except ValueError:
            pass
    if raw.is_error and code is None:
        code = ErrorCode.TOOL_MCP_ERROR
    content = _bounded_mcp_content(raw)
    return ToolResult(
        content=normalize_tool_result(content, max_chars=_LIST_MAX_OUTPUT_CHARS),
        is_error=raw.is_error,
        code=code,
    )


def _bounded_mcp_content(raw: ToolResultFrame) -> str | list[dict[str, Any]]:
    if isinstance(raw.content, str):
        return raw.content
    remaining = _LIST_MAX_OUTPUT_CHARS
    blocks: list[dict[str, Any]] = []
    for raw_block in raw.content:
        block = raw_block.model_dump()
        text = block.get("text")
        if not isinstance(text, str):
            blocks.append(block)
            continue
        if remaining <= 0:
            continue
        block["text"] = truncate_head(text, remaining)
        blocks.append(block)
        remaining -= min(len(text), remaining)
    return blocks


async def _execute_on_device(
    *,
    device_registry: DeviceToolDispatcher,
    device_id: UUID,
    device_name: str,
    name: str,
    args: dict[str, Any],
    ctx: ToolContext,
    max_output_chars: int,
    timeout_seconds: float,
    on_issued: Callable[[], None] | None,
) -> ToolResult:
    try:
        raw = await device_registry.dispatch_tool(
            device_id=device_id,
            user_id=ctx.user_id,
            name=name,
            args=args,
            max_result_bytes=_device_result_credit(name, max_output_chars),
            timeout_seconds=timeout_seconds,
            expected_device_name=device_name,
            chat_session_id=ctx.session_id,
            on_issued=on_issued,
        )
    except DeviceBusyError:
        return _normalized_error(
            ErrorCode.TOOL_DEVICE_BUSY,
            "Tool install site is busy",
        )
    except DeviceOutcomeUnknownError:
        return _normalized_error(
            ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
            "Device call may have executed, but its outcome is unknown",
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
    result_max_chars = max_output_chars
    if name in {"exec", "write_stdin"}:
        # The Client has already bounded the requested stdout/stderr/output.
        # Preserve its separate status, cwd, truncation, and cleanup fields;
        # they are part of the report contract rather than extra tool output.
        result_max_chars += _EXEC_RESULT_METADATA_CHARS
    return ToolResult(
        content=normalize_tool_result(content, max_chars=result_max_chars),
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
