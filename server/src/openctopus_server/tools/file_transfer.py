from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import Device
from openctopus_server.devices.protocol import TransferBeginFrame
from openctopus_server.devices.registry import (
    BridgeRoutePair,
    ConnectionHandle,
    DeviceBusyError,
    DeviceOutcomeUnknownError,
    DeviceRouteSnapshot,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import (
    BRIDGE_SOURCE_DELETE_TIMEOUT_SECONDS,
    TransferBusyError,
    TransferDisconnectedError,
    TransferError,
    TransferIntegrityError,
    TransferUnavailableError,
)
from openctopus_server.devices.workspace import (
    INTERNAL_WORKSPACE_ACTION,
    DeviceTransferLocalResult,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError
from openctopus_server.tools.base import (
    Tool,
    ToolContext,
    ToolResult,
    ToolRoutingMode,
)
from openctopus_server.tools.device_field import DEVICE_FIELD_MARKER
from openctopus_server.workspace.service import TransferPathTicket

FILE_TRANSFER_SCHEMA: dict[str, Any] = {
    "name": "file_transfer",
    "description": (
        "Transfer one regular file between the server or paired devices. Use mode='copy' "
        "to leave the source intact, or mode='move' to remove it after the destination "
        "commits. Destination is rejected if it already exists."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "openoctopus_src_device": {
                "type": "string",
                "enum": ["server"],
                "description": "Device where the source regular file lives.",
                DEVICE_FIELD_MARKER: True,
            },
            "src_path": {
                "type": "string",
                "description": "Path on openoctopus_src_device.",
                "minLength": 1,
                "maxLength": 4096,
            },
            "openoctopus_dst_device": {
                "type": "string",
                "enum": ["server"],
                "description": "Device where the regular file should land.",
                DEVICE_FIELD_MARKER: True,
            },
            "dst_path": {
                "type": "string",
                "description": "Path on openoctopus_dst_device. Must not already exist.",
                "minLength": 1,
                "maxLength": 4096,
            },
            "mode": {
                "type": "string",
                "enum": ["copy", "move"],
                "description": "copy: source intact. move: source deleted after successful transfer.",
                "default": "copy",
            },
        },
        "required": [
            "openoctopus_src_device",
            "src_path",
            "openoctopus_dst_device",
            "dst_path",
        ],
        "additionalProperties": False,
    },
}


class TransferRequest(BaseModel):
    """Strict request accepted by the shared file-transfer orchestrator.

    REST requires ``mode`` explicitly.  The agent tool accepts the same model
    with the protocol's ``copy`` default through ``_FileTransferArgs`` below.
    Keeping the request model here means the REST route and the tool exercise
    the same endpoint/path validation before entering the transfer machine.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    openoctopus_src_device: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^(?:server|[a-z0-9]+(?:-[a-z0-9]+)*)$",
    )
    src_path: str = Field(min_length=1, max_length=4096)
    openoctopus_dst_device: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^(?:server|[a-z0-9]+(?:-[a-z0-9]+)*)$",
    )
    dst_path: str = Field(min_length=1, max_length=4096)
    mode: Literal["copy", "move"]

    @field_validator("src_path", "dst_path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        if "\x00" in value or not value.strip():
            raise ValueError("transfer paths must be non-empty and contain no NUL")
        return value


class _FileTransferArgs(TransferRequest):
    """Agent-facing args, where omitted mode retains the tool default."""

    mode: Literal["copy", "move"] = "copy"


# Keep the descriptive internal name available to callers while the public
# OpenAPI component follows the documented ``TransferRequest`` schema name.
FileTransferRequest = TransferRequest


@dataclass(frozen=True, slots=True)
class FileTransferOutcome:
    """Typed result shared by the agent tool and Workspace REST route."""

    bytes_transferred: int
    sha256: str
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.bytes_transferred < 0:
            raise ValueError("transfer byte count must be non-negative")
        if len(self.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.sha256
        ):
            raise ValueError("transfer SHA-256 must be a lowercase hexadecimal digest")


class _TransferWorkspace(Protocol):
    async def transfer_server_to_server(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        src_path: str,
        dst_path: str,
        mode: str,
        on_issued: Callable[[], None] | None = None,
    ) -> Any: ...

    async def authorize_transfer_source(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> TransferPathTicket: ...

    async def authorize_transfer_destination(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> TransferPathTicket: ...

    async def open_transfer_source(self, ticket: TransferPathTicket) -> Any: ...

    async def delete_transfer_source(
        self,
        ticket: TransferPathTicket,
        *,
        if_match: str | None = None,
    ) -> None: ...

    async def begin_transfer_upload(self, ticket: TransferPathTicket, *, size: int) -> Any: ...

    async def commit_transfer_upload(
        self,
        ticket: TransferPathTicket,
        sink: Any,
        *,
        size: int,
        sha256: str,
    ) -> bool: ...


class _TransferRegistry(Protocol):
    transfers: Any

    async def get_handle(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str | None = None,
    ) -> ConnectionHandle | None: ...

    async def get_route_snapshot(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str,
    ) -> DeviceRouteSnapshot | None: ...

    async def get_bridge_route_pair(
        self,
        *,
        user_id: UUID,
        source_device_id: UUID,
        source_device_name: str,
        destination_device_id: UUID,
        destination_device_name: str,
    ) -> BridgeRoutePair | None: ...

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
    ) -> Any: ...

    async def dispatch_tool_on_snapshot(
        self,
        *,
        route: DeviceRouteSnapshot,
        user_id: UUID,
        expected_device_name: str,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        on_issued: Callable[[], None] | None = None,
    ) -> Any: ...


class FileTransferTool(Tool):
    """Server-owned orchestration for one regular-file transfer.

    The class only retains authorization tickets and transfer callbacks.  It
    never keeps a database session while the transfer manager waits on a
    device or storage stream.
    """

    routing_mode = ToolRoutingMode.INTRINSIC_DEVICE
    manages_issue_boundary = True

    def __init__(
        self,
        engine: AsyncEngine | None,
        workspace_service: _TransferWorkspace | None,
        device_registry: _TransferRegistry | None,
    ) -> None:
        self._engine = engine
        self._workspace = workspace_service
        self._devices = device_registry

    def name(self) -> str:
        return "file_transfer"

    def schema(self) -> dict[str, Any]:
        return deepcopy(FILE_TRANSFER_SCHEMA)

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        try:
            parsed = _FileTransferArgs.model_validate(args)
        except ValidationError as exc:
            return _error(ErrorCode.TOOL_INVALID_ARGS, f"Invalid file transfer arguments: {exc}")

        mark_issued = _once(ctx.on_issued)
        try:
            outcome = await self.transfer(
                parsed,
                user_id=ctx.user_id,
                device_targets=ctx.device_targets,
                on_issued=mark_issued,
            )
        except (DeviceBusyError, TransferBusyError):
            return _error(ErrorCode.TOOL_DEVICE_BUSY, "Device transfer capacity is exhausted")
        except (DeviceOutcomeUnknownError, TransferDisconnectedError):
            return _error(
                ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
                "Device transfer outcome is unknown; do not retry automatically",
            )
        except TimeoutError:
            return _error(ErrorCode.WORKSPACE_TRANSFER_TIMEOUT, "File transfer timed out")
        except (DeviceUnavailableError, TransferUnavailableError):
            return _error(ErrorCode.TOOL_DEVICE_UNREACHABLE, "The paired device is unavailable")
        except TransferIntegrityError:
            return _error(
                ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED,
                "Transferred bytes failed integrity verification",
            )
        except TransferError as exc:
            if exc.code == ErrorCode.WORKSPACE_TRANSFER_TIMEOUT.value:
                return _error(ErrorCode.WORKSPACE_TRANSFER_TIMEOUT, "File transfer timed out")
            error_code = _stable_client_transfer_error(exc.code)
            if error_code is not None:
                return _error(error_code, "Workspace device rejected the transfer")
            return _error(ErrorCode.WORKSPACE_STORAGE_ERROR, "File transfer failed")
        except OpenOctopusError as exc:
            return _error(exc.code, exc.message)
        except Exception:
            return _error(ErrorCode.WORKSPACE_STORAGE_ERROR, "File transfer failed")

        warning = "" if not outcome.warnings else f" Warnings: {', '.join(outcome.warnings)}."
        return ToolResult(
            content=(
                f"Transferred {parsed.src_path} to {parsed.dst_path} "
                f"({outcome.bytes_transferred} bytes, sha256={outcome.sha256}).{warning}"
            )
        )

    async def transfer(
        self,
        request: FileTransferRequest,
        *,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> FileTransferOutcome:
        """Run one validated transfer and return its machine outcome.

        Callers that need a protocol-specific error projection (the Agent tool
        or REST) use this one orchestration path and map the raised stable
        exceptions at their boundary.
        """

        if self._workspace is None and (
            request.openoctopus_src_device == "server" or request.openoctopus_dst_device == "server"
        ):
            raise OpenOctopusError(ErrorCode.TOOL_INVALID_ARGS, "File transfer is not configured")
        if (
            self._engine is None
            and not (
                request.openoctopus_src_device == "server"
                and request.openoctopus_dst_device == "server"
            )
        ) or (request.openoctopus_src_device != "server" and self._devices is None):
            raise OpenOctopusError(ErrorCode.TOOL_INVALID_ARGS, "File transfer is not configured")

        if (
            request.openoctopus_src_device == "server"
            and request.openoctopus_dst_device == "server"
        ):
            result = await self._server_to_server(request, user_id, on_issued)
        elif (
            request.openoctopus_src_device != "server"
            and request.openoctopus_dst_device == request.openoctopus_src_device
        ):
            result = await self._client_to_client(
                request,
                user_id,
                device_targets,
                on_issued,
            )
        elif request.openoctopus_src_device == "server":
            result = await self._server_to_client(
                request,
                user_id,
                device_targets,
                on_issued,
            )
        elif request.openoctopus_dst_device != "server":
            result = await self._client_to_distinct_client(
                request,
                user_id,
                device_targets,
                on_issued,
            )
        else:
            result = await self._client_to_server(
                request,
                user_id,
                device_targets,
                on_issued,
            )
        return _coerce_transfer_outcome(result)

    async def _server_to_server(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        on_issued: Callable[[], None] | None,
    ) -> Any:
        workspace = self._require_workspace()
        if self._engine is None:
            return await workspace.transfer_server_to_server(
                cast(AsyncSession, None),
                user_id=user_id,
                src_path=parsed.src_path,
                dst_path=parsed.dst_path,
                mode=parsed.mode,
                on_issued=on_issued,
            )
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            return await workspace.transfer_server_to_server(
                db,
                user_id=user_id,
                src_path=parsed.src_path,
                dst_path=parsed.dst_path,
                mode=parsed.mode,
                on_issued=on_issued,
            )

    async def _server_to_client(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None,
        on_issued: Callable[[], None] | None,
    ) -> Any:
        workspace = self._require_workspace()
        registry = self._require_registry()
        device_id = await self._device_id_for_call(
            user_id,
            parsed.openoctopus_dst_device,
            device_targets,
        )
        route = await registry.get_route_snapshot(
            device_id,
            user_id=user_id,
            expected_device_name=parsed.openoctopus_dst_device,
        )
        if route is None:
            raise DeviceUnavailableError("Destination device is offline")
        async with AsyncSession(self._require_engine(), expire_on_commit=False) as db:
            ticket = await workspace.authorize_transfer_source(
                db,
                user_id=user_id,
                path=parsed.src_path,
            )
        source: Any | None = None

        async def source_factory() -> Any:
            nonlocal source
            source = await workspace.open_transfer_source(ticket)
            return source

        async def delete_source() -> None:
            if source is None:
                raise RuntimeError("transfer source is missing")
            await workspace.delete_transfer_source(ticket, if_match=source.etag)

        return await registry.transfers.start_server_to_client(
            handle=route.handle,
            route=route,
            user_id=user_id,
            src_path=parsed.src_path,
            dst_path=parsed.dst_path,
            source_factory=source_factory,
            mode=parsed.mode,
            delete_source=delete_source if parsed.mode == "move" else None,
            src_device="server",
            dst_device=parsed.openoctopus_dst_device,
            on_issued=on_issued,
        )

    async def _client_to_server(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None,
        on_issued: Callable[[], None] | None,
    ) -> Any:
        workspace = self._require_workspace()
        registry = self._require_registry()
        device_id = await self._device_id_for_call(
            user_id,
            parsed.openoctopus_src_device,
            device_targets,
        )
        route = await registry.get_route_snapshot(
            device_id,
            user_id=user_id,
            expected_device_name=parsed.openoctopus_src_device,
        )
        if route is None:
            raise DeviceUnavailableError("Source device is offline")
        async with AsyncSession(self._require_engine(), expire_on_commit=False) as db:
            ticket = await workspace.authorize_transfer_destination(
                db,
                user_id=user_id,
                path=parsed.dst_path,
            )

        source_etag: str | None = None

        async def make_sink(begin: TransferBeginFrame) -> Any:
            nonlocal source_etag
            if begin.src_path != parsed.src_path or begin.dst_path != parsed.dst_path:
                raise ValueError("client transfer metadata does not match the request")
            if begin.total_bytes is None:
                raise ValueError("client file transfer did not declare its size")
            source_etag = begin.etag
            return await workspace.begin_transfer_upload(ticket, size=begin.total_bytes)

        async def commit_sink(
            sink: Any,
            _begin: TransferBeginFrame,
            size: int,
            digest: str,
        ) -> bool:
            return await workspace.commit_transfer_upload(
                ticket,
                sink,
                size=size,
                sha256=digest,
            )

        async def delete_source() -> None:
            if source_etag is None:
                raise RuntimeError("client source fingerprint is missing")
            result = await registry.dispatch_tool_on_snapshot(
                route=route,
                user_id=user_id,
                expected_device_name=parsed.openoctopus_src_device,
                name=INTERNAL_WORKSPACE_ACTION,
                args={
                    "operation": "delete_file",
                    "path": parsed.src_path,
                    "if_match": source_etag,
                },
                max_result_bytes=16 * 1024,
                timeout_seconds=30,
            )
            if getattr(result, "is_error", False):
                raise RuntimeError("client source deletion failed")

        return await registry.transfers.start_client_to_server(
            handle=route.handle,
            route=route,
            user_id=user_id,
            src_path=parsed.src_path,
            dst_path=parsed.dst_path,
            sink_factory=make_sink,
            commit_sink=commit_sink,
            delete_source=delete_source if parsed.mode == "move" else None,
            mode=parsed.mode,
            on_issued=on_issued,
        )

    async def _client_to_client(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None,
        on_issued: Callable[[], None] | None,
    ) -> FileTransferOutcome:
        """Ask one paired client to perform a coordinated local transfer."""

        registry = self._require_registry()
        device_id = await self._device_id_for_call(
            user_id,
            parsed.openoctopus_src_device,
            device_targets,
        )
        route = await registry.get_route_snapshot(
            device_id,
            user_id=user_id,
            expected_device_name=parsed.openoctopus_src_device,
        )
        if route is None:
            raise DeviceUnavailableError("Device is offline")
        raw = await registry.dispatch_tool_on_snapshot(
            route=route,
            user_id=user_id,
            expected_device_name=parsed.openoctopus_src_device,
            name=INTERNAL_WORKSPACE_ACTION,
            args={
                "operation": "transfer_local",
                "path": parsed.src_path,
                "dst_path": parsed.dst_path,
                "mode": parsed.mode,
            },
            max_result_bytes=64 * 1024,
            timeout_seconds=60.0,
            on_issued=on_issued,
        )
        if raw.is_error:
            _raise_transfer_client_error(raw.code)
        if not isinstance(raw.content, str):
            raise TransferError(ErrorCode.WORKSPACE_STORAGE_ERROR.value)
        try:
            result = DeviceTransferLocalResult.model_validate_json(raw.content, strict=True)
        except ValidationError as exc:
            raise TransferIntegrityError from exc
        return FileTransferOutcome(
            bytes_transferred=result.bytes_transferred,
            sha256=result.sha256,
            warnings=tuple(result.warnings),
        )

    async def _client_to_distinct_client(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None,
        on_issued: Callable[[], None] | None,
    ) -> Any:
        registry = self._require_registry()
        source_device_id, destination_device_id = await self._bridge_device_ids_for_call(
            user_id,
            parsed.openoctopus_src_device,
            parsed.openoctopus_dst_device,
            device_targets,
        )
        routes = await registry.get_bridge_route_pair(
            user_id=user_id,
            source_device_id=source_device_id,
            source_device_name=parsed.openoctopus_src_device,
            destination_device_id=destination_device_id,
            destination_device_name=parsed.openoctopus_dst_device,
        )
        if routes is None:
            raise DeviceUnavailableError("File transfer devices are unavailable")

        async def delete_source(source_fingerprint: str) -> None:
            result = await registry.dispatch_tool_on_snapshot(
                route=routes.source,
                user_id=user_id,
                expected_device_name=parsed.openoctopus_src_device,
                name=INTERNAL_WORKSPACE_ACTION,
                args={
                    "operation": "delete_file",
                    "path": parsed.src_path,
                    "if_match": source_fingerprint,
                },
                max_result_bytes=16 * 1024,
                timeout_seconds=BRIDGE_SOURCE_DELETE_TIMEOUT_SECONDS,
            )
            if getattr(result, "is_error", False):
                raise RuntimeError("client source deletion failed")

        return await registry.transfers.start_client_to_client(
            source_route=routes.source,
            destination_route=routes.destination,
            user_id=user_id,
            src_path=parsed.src_path,
            dst_path=parsed.dst_path,
            mode=parsed.mode,
            delete_source=delete_source if parsed.mode == "move" else None,
            on_issued=on_issued,
        )

    def _require_workspace(self) -> _TransferWorkspace:
        if self._workspace is None:
            raise RuntimeError("Workspace transfer service is not configured")
        return self._workspace

    def _require_engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("Database engine is not configured")
        return self._engine

    async def _device_id_for_call(
        self,
        user_id: UUID,
        name: str,
        device_targets: Mapping[str, UUID] | None,
    ) -> UUID:
        expected_device_id = None
        if device_targets is not None:
            expected_device_id = device_targets.get(name)
            if expected_device_id is None:
                raise DeviceUnavailableError("Paired device was not captured for this turn")
        return await self._device_id(user_id, name, expected_device_id)

    async def _bridge_device_ids_for_call(
        self,
        user_id: UUID,
        source_name: str,
        destination_name: str,
        device_targets: Mapping[str, UUID] | None,
    ) -> tuple[UUID, UUID]:
        expected_source_id: UUID | None = None
        expected_destination_id: UUID | None = None
        if device_targets is not None:
            expected_source_id = device_targets.get(source_name)
            expected_destination_id = device_targets.get(destination_name)
            if expected_source_id is None or expected_destination_id is None:
                raise DeviceUnavailableError("Paired devices were not captured for this turn")

        predicates = []
        source_predicate = Device.name == source_name
        if expected_source_id is not None:
            source_predicate = and_(source_predicate, Device.id == expected_source_id)
        predicates.append(source_predicate)
        destination_predicate = Device.name == destination_name
        if expected_destination_id is not None:
            destination_predicate = and_(
                destination_predicate,
                Device.id == expected_destination_id,
            )
        predicates.append(destination_predicate)

        async with AsyncSession(self._require_engine(), expire_on_commit=False) as db:
            rows = (
                await db.execute(
                    select(Device.name, Device.id).where(
                        Device.user_id == user_id,
                        or_(*predicates),
                    )
                )
            ).all()
        resolved = {name: device_id for name, device_id in rows}
        source_device_id = resolved.get(source_name)
        destination_device_id = resolved.get(destination_name)
        if (
            not isinstance(source_device_id, UUID)
            or not isinstance(destination_device_id, UUID)
            or source_device_id == destination_device_id
            or (
                expected_source_id is not None
                and source_device_id != expected_source_id
            )
            or (
                expected_destination_id is not None
                and destination_device_id != expected_destination_id
            )
        ):
            raise DeviceUnavailableError("Paired devices were not found")
        return source_device_id, destination_device_id

    async def _device_id(
        self,
        user_id: UUID,
        name: str,
        expected_device_id: UUID | None = None,
    ) -> UUID:
        assert self._engine is not None
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            statement = select(Device.id).where(Device.user_id == user_id, Device.name == name)
            if expected_device_id is not None:
                statement = statement.where(Device.id == expected_device_id)
            device_id = await db.scalar(statement)
        if device_id is None:
            raise DeviceUnavailableError("Paired device was not found")
        return device_id

    def _require_registry(self) -> _TransferRegistry:
        if self._devices is None:
            raise DeviceUnavailableError("Device transfer registry is not configured")
        return self._devices


def _error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(content=f"[{code.value}] {message}", is_error=True, code=code)


def _once(callback: Callable[[], None] | None) -> Callable[[], None] | None:
    if callback is None:
        return None
    called = False

    def call() -> None:
        nonlocal called
        if called:
            return
        called = True
        callback()

    return call


def _coerce_transfer_outcome(result: Any) -> FileTransferOutcome:
    if isinstance(result, FileTransferOutcome):
        return result
    return FileTransferOutcome(
        bytes_transferred=result.bytes_transferred,
        sha256=result.sha256,
        warnings=tuple(result.warnings),
    )


def _raise_transfer_client_error(code: str | None) -> None:
    if code == ErrorCode.TOOL_DEVICE_BUSY.value:
        raise DeviceBusyError("Device transfer capacity is exhausted")
    if code == ErrorCode.TOOL_DEVICE_UNREACHABLE.value:
        raise DeviceUnavailableError("Device is unavailable")
    if code == ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value:
        raise DeviceOutcomeUnknownError("Device transfer outcome is unknown")
    error_code = _stable_client_transfer_error(code)
    if error_code is not None:
        raise OpenOctopusError(error_code, "Workspace device rejected the transfer")
    raise TransferIntegrityError


def _stable_client_transfer_error(code: str | None) -> ErrorCode | None:
    try:
        error_code = ErrorCode(code) if code is not None else None
    except ValueError:
        error_code = None
    if error_code in {
        ErrorCode.TOOL_DEVICE_BUSY,
        ErrorCode.TOOL_DEVICE_UNREACHABLE,
        ErrorCode.WORKSPACE_NOT_FOUND,
        ErrorCode.WORKSPACE_PERMISSION_DENIED,
        ErrorCode.WORKSPACE_SYMLINK_ESCAPE,
        ErrorCode.WORKSPACE_BLOCKED_PATH,
        ErrorCode.WORKSPACE_FILE_CHANGED,
        ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
        ErrorCode.WORKSPACE_TRANSFER_TIMEOUT,
        ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED,
        ErrorCode.WORKSPACE_INVALID_REQUEST,
        ErrorCode.TOOL_INVALID_ARGS,
        ErrorCode.TOOL_IS_DIRECTORY,
        ErrorCode.TOOL_NOT_A_DIRECTORY,
        ErrorCode.TOOL_PATH_OUTSIDE_WORKSPACE,
    }:
        return error_code
    return None
