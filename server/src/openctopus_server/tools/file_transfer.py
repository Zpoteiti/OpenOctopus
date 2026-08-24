from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.db.models import Device
from openctopus_server.devices.protocol import TransferBeginFrame, new_uuid7
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
    TransferLease,
    TransferManager,
    TransferUnavailableError,
)
from openctopus_server.devices.workspace import (
    INTERNAL_WORKSPACE_ACTION,
    DeviceTransferLocalResult,
    DirectorySourceProbe,
    FileSourceProbe,
)
from openctopus_server.directory_contract import DirectoryManifest
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError, WorkspaceError
from openctopus_server.tools.base import (
    Tool,
    ToolContext,
    ToolResult,
    ToolRoutingMode,
)
from openctopus_server.tools.cross_site_directory_backend import (
    ClientToClientDirectoryBackend,
    ClientToServerDirectoryBackend,
    ServerToClientDirectoryBackend,
)
from openctopus_server.tools.device_directory_jobs import DeviceDirectoryJobController
from openctopus_server.tools.device_field import DEVICE_FIELD_MARKER
from openctopus_server.tools.directory_transfer import (
    DirectoryMutationNotAppliedError,
    DirectoryTransferCoordinator,
    DirectoryTransferResult,
)
from openctopus_server.tools.server_directory_backend import ServerDirectoryTransferBackend
from openctopus_server.workspace.fs import ServerFileSourceProbe, WorkspaceFS
from openctopus_server.workspace.service import (
    TransferPathTicket,
    WorkspaceService,
)

FILE_TRANSFER_SCHEMA: dict[str, Any] = {
    "name": "file_transfer",
    "description": (
        "Transfer one regular file or directory tree between the server or paired devices. "
        "Directories are recursive. Use mode='copy' to leave the source intact, or "
        "mode='move' to remove it after the destination commits. Destination is rejected "
        "if it already exists."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "openoctopus_src_device": {
                "type": "string",
                "enum": ["server"],
                "description": "Device where the source file or directory lives.",
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
                "description": "Device where the file or directory should land.",
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

_TRANSFER_WARNING_PRIORITY = (
    "transfer_ack_failed",
    "source_delete_failed",
    "source_changed_after_copy",
    "source_cleanup_incomplete",
)


@dataclass(frozen=True, slots=True)
class FileTransferOutcome:
    """Typed result shared by the agent tool and Workspace REST route."""

    kind: Literal["file", "directory"]
    files_transferred: int
    bytes_transferred: int
    sha256: str
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.kind == "file":
            if self.files_transferred != 1:
                raise ValueError("file transfer count must be one")
        elif self.kind == "directory":
            if not 1 <= self.files_transferred <= 10_000:
                raise ValueError("directory transfer count is invalid")
        else:
            raise ValueError("transfer kind is invalid")
        if self.bytes_transferred < 0:
            raise ValueError("transfer byte count must be non-negative")
        if len(self.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.sha256
        ):
            raise ValueError("transfer SHA-256 must be a lowercase hexadecimal digest")
        unknown = set(self.warnings).difference(_TRANSFER_WARNING_PRIORITY)
        if unknown:
            raise ValueError("transfer warning is invalid")
        normalized = tuple(
            warning for warning in _TRANSFER_WARNING_PRIORITY if warning in self.warnings
        )
        if len(normalized) > 8:
            raise ValueError("too many transfer warnings")
        object.__setattr__(self, "warnings", normalized)


class _TransferWorkspace(Protocol):
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

    async def validate_transfer_skill_staging(
        self,
        destination: TransferPathTicket,
        object_name: str,
        *,
        expected_size: int,
    ) -> None: ...

    def transfer_ticket_changed(self, ticket: TransferPathTicket) -> None: ...


class _TransferRegistry(Protocol):
    transfers: TransferManager

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


class _OperationLease(Protocol):
    async def aclose(self) -> None: ...


class FileTransferTool(Tool):
    """Server-owned orchestration for one regular-file or directory transfer.

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
        workspace_fs: WorkspaceFS | None,
    ) -> None:
        self._engine = engine
        self._workspace = workspace_service
        self._devices = device_registry
        self._fs = workspace_fs

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
        file_label = "file" if outcome.files_transferred == 1 else "files"
        return ToolResult(
            content=(
                f"Transferred {parsed.src_path} to {parsed.dst_path} "
                f"({outcome.kind}, {outcome.files_transferred} {file_label}, "
                f"{outcome.bytes_transferred} bytes, sha256={outcome.sha256}).{warning}"
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

        uses_server = (
            request.openoctopus_src_device == "server"
            or request.openoctopus_dst_device == "server"
        )
        uses_client = (
            request.openoctopus_src_device != "server"
            or request.openoctopus_dst_device != "server"
        )
        if self._engine is None:
            raise OpenOctopusError(ErrorCode.TOOL_INVALID_ARGS, "File transfer is not configured")
        if uses_server and (self._workspace is None or self._fs is None):
            raise OpenOctopusError(ErrorCode.TOOL_INVALID_ARGS, "File transfer is not configured")
        if uses_client and self._devices is None:
            raise OpenOctopusError(ErrorCode.TOOL_INVALID_ARGS, "File transfer is not configured")

        server_tickets: list[TransferPathTicket] = []
        try:
            if (
                request.openoctopus_src_device == "server"
                and request.openoctopus_dst_device == "server"
            ):
                return await self._server_to_server(
                    request,
                    user_id,
                    on_issued,
                    server_tickets,
                )
            if (
                request.openoctopus_src_device != "server"
                and request.openoctopus_dst_device == request.openoctopus_src_device
            ):
                return await self._client_to_client(
                    request,
                    user_id,
                    device_targets,
                    on_issued,
                )
            if request.openoctopus_src_device == "server":
                return await self._server_to_client(
                    request,
                    user_id,
                    device_targets,
                    on_issued,
                    server_tickets,
                )
            if request.openoctopus_dst_device != "server":
                return await self._client_to_distinct_client(
                    request,
                    user_id,
                    device_targets,
                    on_issued,
                )
            return await self._client_to_server(
                request,
                user_id,
                device_targets,
                on_issued,
                server_tickets,
            )
        finally:
            workspace = self._workspace
            if workspace is not None:
                seen: set[int] = set()
                for ticket in server_tickets:
                    identity = id(ticket)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    workspace.transfer_ticket_changed(ticket)

    async def _server_to_server(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        on_issued: Callable[[], None] | None,
        server_tickets: list[TransferPathTicket],
    ) -> FileTransferOutcome:
        workspace = self._require_workspace()
        workspace_fs = self._require_fs()
        async with AsyncSession(self._require_engine(), expire_on_commit=False) as db:
            source = await workspace.authorize_transfer_source(
                db,
                user_id=user_id,
                path=parsed.src_path,
            )
            server_tickets.append(source)
            destination = await workspace.authorize_transfer_destination(
                db,
                user_id=user_id,
                path=parsed.dst_path,
            )
            server_tickets.append(destination)

        lease = await workspace_fs.acquire_server_transfer_operation(user_id)
        handed_off = False

        async def validate_staging(object_name: str, size: int) -> None:
            await workspace.validate_transfer_skill_staging(
                destination,
                object_name,
                expected_size=size,
            )

        try:
            probe = await workspace_fs.probe_directory_source(
                source.target,
                source.relative_path,
            )
            if isinstance(probe, ServerFileSourceProbe):
                transferred, digest, warnings = (
                    await workspace_fs.transfer_server_to_server_admitted(
                        source.target,
                        source.relative_path,
                        destination.target,
                        destination.relative_path,
                        quota_bytes=destination.quota_bytes,
                        mode=parsed.mode,
                        expected_source_size=probe.size,
                        expected_source_fingerprint=probe.fingerprint,
                        on_issued=on_issued,
                        validate_staging=validate_staging,
                    )
                )
                return FileTransferOutcome(
                    kind="file",
                    files_transferred=1,
                    bytes_transferred=transferred,
                    sha256=digest,
                    warnings=warnings,
                )

            backend = ServerDirectoryTransferBackend(
                workspace_fs=workspace_fs,
                workspace_service=cast(WorkspaceService, workspace),
                source=source,
                destination=destination,
                manifest=probe.manifest,
                operation_id=new_uuid7(),
            )
            handed_off = True
            directory_result = await DirectoryTransferCoordinator().run(
                manifest=probe.manifest,
                mode=parsed.mode,
                backend=backend,
                operation_lease=lease,
                on_issued=on_issued,
            )
            return _directory_outcome(directory_result)
        finally:
            if not handed_off:
                await _close_operation_lease(lease)

    async def _server_to_client(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None,
        on_issued: Callable[[], None] | None,
        server_tickets: list[TransferPathTicket],
    ) -> FileTransferOutcome:
        workspace = self._require_workspace()
        workspace_fs = self._require_fs()
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
            server_tickets.append(ticket)

        transfers = registry.transfers
        lease = await transfers.acquire_operation(user_id)
        handed_off = False
        try:
            probe = await workspace_fs.probe_directory_source(
                ticket.target,
                ticket.relative_path,
            )
            if isinstance(probe, ServerFileSourceProbe):

                async def source_factory() -> Any:
                    source = await workspace.open_transfer_source(ticket)
                    if source.size != probe.size or source.etag != probe.fingerprint:
                        close = asyncio.create_task(source.aclose())
                        await await_future_cancellation_safe(close)
                        raise _source_changed()
                    return source

                async def delete_source() -> None:
                    await workspace.delete_transfer_source(
                        ticket,
                        if_match=probe.fingerprint,
                    )

                result = await transfers.start_server_to_client_regular_admitted(
                    handle=route.handle,
                    route=route,
                    operation_lease=lease,
                    slot_id=new_uuid7(),
                    user_id=user_id,
                    src_path=parsed.src_path,
                    dst_path=parsed.dst_path,
                    source_factory=source_factory,
                    total_bytes=probe.size,
                    mode=parsed.mode,
                    delete_source=delete_source if parsed.mode == "move" else None,
                    src_device="server",
                    dst_device=parsed.openoctopus_dst_device,
                    on_issued=on_issued,
                )
                return _coerce_transfer_outcome(result)

            operation_id = new_uuid7()
            destination = self._directory_controller(
                registry,
                route,
                user_id=user_id,
                operation_id=operation_id,
            )
            backend = ServerToClientDirectoryBackend(
                transfer_manager=transfers,
                operation_lease=lease,
                workspace_fs=workspace_fs,
                source=ticket,
                destination=destination,
                destination_root=parsed.dst_path,
                manifest=probe.manifest,
            )
            handed_off = True
            directory_result = await DirectoryTransferCoordinator().run(
                manifest=probe.manifest,
                mode=parsed.mode,
                backend=backend,
                operation_lease=lease,
                on_issued=on_issued,
            )
            return _directory_outcome(directory_result)
        finally:
            if not handed_off:
                await _close_operation_lease(lease)

    async def _client_to_server(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None,
        on_issued: Callable[[], None] | None,
        server_tickets: list[TransferPathTicket],
    ) -> FileTransferOutcome:
        workspace = self._require_workspace()
        workspace_fs = self._require_fs()
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
            server_tickets.append(ticket)

        transfers = registry.transfers
        lease = await transfers.acquire_operation(user_id)
        operation_id = new_uuid7()
        source = self._directory_controller(
            registry,
            route,
            user_id=user_id,
            operation_id=operation_id,
        )
        source_owned = False
        handed_off = False
        try:
            source_owned = True
            probe = await self._probe_client_source(source, parsed.src_path)
            if isinstance(probe, FileSourceProbe):
                await source.release_source_probe()
                source_owned = False
                result = await self._client_file_to_server(
                    parsed,
                    user_id=user_id,
                    route=route,
                    ticket=ticket,
                    probe=probe,
                    lease=lease,
                    transfers=transfers,
                    workspace=workspace,
                    registry=registry,
                    on_issued=on_issued,
                )
                return _coerce_transfer_outcome(result)

            manifest = await source.retrieve_source_manifest(probe)
            await source.hold_source_probe()
            backend = ClientToServerDirectoryBackend(
                transfer_manager=transfers,
                operation_lease=lease,
                workspace_fs=workspace_fs,
                workspace_service=cast(WorkspaceService, workspace),
                source=source,
                source_root=parsed.src_path,
                destination=ticket,
                manifest=manifest,
            )
            source_owned = False
            handed_off = True
            result = await DirectoryTransferCoordinator().run(
                manifest=manifest,
                mode=parsed.mode,
                backend=backend,
                operation_lease=lease,
                on_issued=on_issued,
            )
            return _directory_outcome(result)
        finally:
            try:
                if source_owned:
                    await _retire_client_source(source)
            finally:
                if not handed_off:
                    await _close_operation_lease(lease)

    async def _client_to_client(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None,
        on_issued: Callable[[], None] | None,
    ) -> FileTransferOutcome:
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
        transfers = registry.transfers
        lease = await transfers.acquire_operation(user_id)
        controller = self._directory_controller(
            registry,
            route,
            user_id=user_id,
            operation_id=new_uuid7(),
        )
        source_owned = False
        try:
            source_owned = True
            probe = await self._probe_client_source(controller, parsed.src_path)
            if isinstance(probe, FileSourceProbe):
                await controller.release_source_probe()
                source_owned = False
                return await self._same_client_file(
                    parsed,
                    user_id=user_id,
                    route=route,
                    probe=probe,
                    registry=registry,
                    on_issued=on_issued,
                )

            manifest = await controller.retrieve_source_manifest(probe)
            await controller.release_source_probe()
            source_owned = False
            return await self._same_client_directory(
                parsed,
                controller=controller,
                manifest=manifest,
                on_issued=on_issued,
            )
        finally:
            try:
                if source_owned:
                    await _retire_client_source(controller)
            finally:
                await _close_operation_lease(lease)

    async def _client_to_distinct_client(
        self,
        parsed: FileTransferRequest,
        user_id: UUID,
        device_targets: Mapping[str, UUID] | None,
        on_issued: Callable[[], None] | None,
    ) -> FileTransferOutcome:
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
        transfers = registry.transfers
        lease = await transfers.acquire_operation(user_id)
        operation_id = new_uuid7()
        source = self._directory_controller(
            registry,
            routes.source,
            user_id=user_id,
            operation_id=operation_id,
        )
        source_owned = False
        handed_off = False
        try:
            source_owned = True
            probe = await self._probe_client_source(source, parsed.src_path)
            if isinstance(probe, FileSourceProbe):
                await source.release_source_probe()
                source_owned = False

                async def delete_source(source_fingerprint: str) -> None:
                    if source_fingerprint != probe.fingerprint:
                        raise TransferIntegrityError(
                            "Client source fingerprint changed after probe"
                        )
                    result = await registry.dispatch_tool_on_snapshot(
                        route=routes.source,
                        user_id=user_id,
                        expected_device_name=parsed.openoctopus_src_device,
                        name=INTERNAL_WORKSPACE_ACTION,
                        args={
                            "operation": "delete_file",
                            "path": parsed.src_path,
                            "if_match": probe.fingerprint,
                        },
                        max_result_bytes=16 * 1024,
                        timeout_seconds=BRIDGE_SOURCE_DELETE_TIMEOUT_SECONDS,
                    )
                    if getattr(result, "is_error", False):
                        raise RuntimeError("client source deletion failed")

                result = await transfers.start_client_to_client_regular_admitted(
                    source_route=routes.source,
                    destination_route=routes.destination,
                    operation_lease=lease,
                    slot_id=new_uuid7(),
                    user_id=user_id,
                    src_path=parsed.src_path,
                    dst_path=parsed.dst_path,
                    expected_source_size=probe.size,
                    expected_source_fingerprint=probe.fingerprint,
                    mode=parsed.mode,
                    delete_source=delete_source if parsed.mode == "move" else None,
                    on_issued=on_issued,
                )
                return _coerce_transfer_outcome(result)

            manifest = await source.retrieve_source_manifest(probe)
            await source.hold_source_probe()
            destination = self._directory_controller(
                registry,
                routes.destination,
                user_id=user_id,
                operation_id=operation_id,
            )
            backend = ClientToClientDirectoryBackend(
                transfer_manager=transfers,
                operation_lease=lease,
                source=source,
                source_root=parsed.src_path,
                destination=destination,
                destination_root=parsed.dst_path,
                manifest=manifest,
            )
            source_owned = False
            handed_off = True
            directory_result = await DirectoryTransferCoordinator().run(
                manifest=manifest,
                mode=parsed.mode,
                backend=backend,
                operation_lease=lease,
                on_issued=on_issued,
            )
            return _directory_outcome(directory_result)
        finally:
            try:
                if source_owned:
                    await _retire_client_source(source)
            finally:
                if not handed_off:
                    await _close_operation_lease(lease)

    async def _client_file_to_server(
        self,
        parsed: FileTransferRequest,
        *,
        user_id: UUID,
        route: DeviceRouteSnapshot,
        ticket: TransferPathTicket,
        probe: FileSourceProbe,
        lease: TransferLease,
        transfers: TransferManager,
        workspace: _TransferWorkspace,
        registry: _TransferRegistry,
        on_issued: Callable[[], None] | None,
    ) -> Any:
        slot_id = new_uuid7()

        async def make_sink(begin: TransferBeginFrame) -> Any:
            if (
                begin.id != slot_id
                or begin.direction != "client_to_server"
                or begin.purpose != "file_transfer"
                or begin.src_path != parsed.src_path
                or begin.dst_path != parsed.dst_path
                or begin.total_bytes != probe.size
                or begin.etag != probe.fingerprint
            ):
                raise TransferIntegrityError(
                    "Client transfer metadata changed after source probe"
                )
            return await workspace.begin_transfer_upload(ticket, size=probe.size)

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
            result = await registry.dispatch_tool_on_snapshot(
                route=route,
                user_id=user_id,
                expected_device_name=parsed.openoctopus_src_device,
                name=INTERNAL_WORKSPACE_ACTION,
                args={
                    "operation": "delete_file",
                    "path": parsed.src_path,
                    "if_match": probe.fingerprint,
                },
                max_result_bytes=16 * 1024,
                timeout_seconds=30.0,
            )
            if getattr(result, "is_error", False):
                raise RuntimeError("client source deletion failed")

        return await transfers.start_client_to_server_regular_admitted(
            handle=route.handle,
            route=route,
            operation_lease=lease,
            slot_id=slot_id,
            user_id=user_id,
            src_path=parsed.src_path,
            dst_path=parsed.dst_path,
            sink_factory=make_sink,
            commit_sink=commit_sink,
            mode=parsed.mode,
            delete_source=delete_source if parsed.mode == "move" else None,
            on_issued=on_issued,
        )

    async def _same_client_file(
        self,
        parsed: FileTransferRequest,
        *,
        user_id: UUID,
        route: DeviceRouteSnapshot,
        probe: FileSourceProbe,
        registry: _TransferRegistry,
        on_issued: Callable[[], None] | None,
    ) -> FileTransferOutcome:
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
                "if_match": probe.fingerprint,
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
        if result.bytes_transferred != probe.size:
            raise TransferIntegrityError("Local transfer size changed after source probe")
        return FileTransferOutcome(
            kind="file",
            files_transferred=1,
            bytes_transferred=result.bytes_transferred,
            sha256=result.sha256,
            warnings=tuple(result.warnings),
        )

    async def _same_client_directory(
        self,
        parsed: FileTransferRequest,
        *,
        controller: DeviceDirectoryJobController,
        manifest: DirectoryManifest,
        on_issued: Callable[[], None] | None,
    ) -> FileTransferOutcome:
        destination_started = False
        local_started = False
        local_issued = False
        result: FileTransferOutcome | None = None
        failure: BaseException | None = None

        def mark_local_issued() -> None:
            nonlocal local_issued
            local_issued = True
            if on_issued is not None:
                on_issued()

        try:
            destination_started = True
            await controller.start_destination_preflight(parsed.dst_path, manifest)
            status = await controller.wait_destination_until(
                frozenset({"ready", "failed", "outcome_unknown"})
            )
            _require_directory_state(status, "ready")
            local_started = True
            await controller.start_local_directory(
                source_path=parsed.src_path,
                dst_path=parsed.dst_path,
                mode=parsed.mode,
                manifest_sha256=manifest.manifest_sha256,
                on_issued=mark_local_issued,
            )
            local = await controller.wait_local_until(
                frozenset({"succeeded", "failed", "outcome_unknown"})
            )
            _require_directory_state(local, "succeeded")
            terminal = local.terminal_result
            if terminal is None:
                raise TransferIntegrityError("Local directory result is missing")
            result = FileTransferOutcome(
                kind="directory",
                files_transferred=terminal.files_transferred,
                bytes_transferred=terminal.bytes_transferred,
                sha256=terminal.sha256,
                warnings=tuple(terminal.warnings),
            )
        except BaseException as exc:
            failure = exc

        cleanup_error = await _settle_same_client_directory(
            controller,
            destination_started=destination_started,
            local_started=local_started,
            succeeded=result is not None,
        )
        if result is not None:
            if cleanup_error is not None:
                result = FileTransferOutcome(
                    kind=result.kind,
                    files_transferred=result.files_transferred,
                    bytes_transferred=result.bytes_transferred,
                    sha256=result.sha256,
                    warnings=result.warnings + ("transfer_ack_failed",),
                )
            return result
        if failure is not None:
            mutation_may_have_started = local_issued and not isinstance(
                failure,
                DirectoryMutationNotAppliedError,
            )
            if cleanup_error is not None and mutation_may_have_started:
                raise DeviceOutcomeUnknownError(
                    "Local directory cleanup outcome is unknown"
                ) from failure
            raise failure
        if cleanup_error is not None:
            raise cleanup_error
        raise AssertionError("local directory transfer completed without an outcome")

    async def _probe_client_source(
        self,
        controller: DeviceDirectoryJobController,
        path: str,
    ) -> FileSourceProbe | DirectorySourceProbe:
        await controller.start_source_probe(path)
        status = await controller.wait_source_until(
            frozenset({"succeeded", "ready_retrieval", "failed", "outcome_unknown"})
        )
        if status.state == "succeeded" and isinstance(status.probe, FileSourceProbe):
            return status.probe
        if status.state == "ready_retrieval" and isinstance(
            status.probe, DirectorySourceProbe
        ):
            return status.probe
        _raise_directory_status(status)
        raise AssertionError("unreachable")

    def _directory_controller(
        self,
        registry: _TransferRegistry,
        route: DeviceRouteSnapshot,
        *,
        user_id: UUID,
        operation_id: UUID,
    ) -> DeviceDirectoryJobController:
        return DeviceDirectoryJobController(
            registry=registry,
            route=route,
            user_id=user_id,
            directory_operation_id=operation_id,
            idle_timeout_seconds=registry.transfers.idle_timeout_seconds,
        )

    def _require_workspace(self) -> _TransferWorkspace:
        if self._workspace is None:
            raise RuntimeError("Workspace transfer service is not configured")
        return self._workspace

    def _require_engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("Database engine is not configured")
        return self._engine

    def _require_fs(self) -> WorkspaceFS:
        if self._fs is None:
            raise RuntimeError("Workspace filesystem is not configured")
        return self._fs

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


async def _close_operation_lease(lease: _OperationLease) -> None:
    close = asyncio.create_task(lease.aclose())
    await await_future_cancellation_safe(close)


async def _retire_client_source(controller: DeviceDirectoryJobController) -> None:
    async def retire() -> None:
        try:
            await controller.cancel_source_probe()
            await controller.wait_source_until(
                frozenset({"succeeded", "failed", "outcome_unknown"})
            )
        except BaseException:
            pass
        try:
            await controller.release_source_probe()
        except BaseException:
            pass

    task = asyncio.create_task(retire())
    await await_future_cancellation_safe(task)


async def _settle_same_client_directory(
    controller: DeviceDirectoryJobController,
    *,
    destination_started: bool,
    local_started: bool,
    succeeded: bool,
) -> BaseException | None:
    async def settle() -> None:
        failure: BaseException | None = None
        if local_started:
            if not succeeded:
                try:
                    await controller.cancel_local_directory()
                    await controller.wait_local_until(
                        frozenset({"succeeded", "failed", "outcome_unknown"})
                    )
                except BaseException as exc:
                    failure = exc
            try:
                await controller.release_local_directory()
            except BaseException as exc:
                if failure is None:
                    failure = exc
        elif destination_started:
            try:
                await controller.cancel_destination()
                await controller.wait_destination_until(
                    frozenset({"failed", "outcome_unknown"})
                )
            except BaseException as exc:
                failure = exc
            try:
                await controller.release_destination()
            except BaseException as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure

    task = asyncio.create_task(settle())
    try:
        await await_future_cancellation_safe(task)
    except BaseException as exc:
        return exc
    return None


def _directory_outcome(result: DirectoryTransferResult) -> FileTransferOutcome:
    return FileTransferOutcome(
        kind="directory",
        files_transferred=result.files_transferred,
        bytes_transferred=result.bytes_transferred,
        sha256=result.sha256,
        warnings=result.warnings,
    )


def _require_directory_state(status: object, expected: str) -> None:
    if getattr(status, "state", None) == expected:
        return
    _raise_directory_status(status)


def _raise_directory_status(status: object) -> None:
    state = getattr(status, "state", None)
    if state == "outcome_unknown":
        raise DeviceOutcomeUnknownError("Client directory outcome is unknown")
    if state == "failed":
        error = getattr(status, "terminal_error", None)
        code = getattr(error, "code", None)
        if not isinstance(code, str):
            raise TransferIntegrityError("Client directory failure omitted its error code")
        if code == ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value:
            raise DeviceOutcomeUnknownError("Client directory outcome is unknown")
        raise TransferError(code)
    raise TransferIntegrityError("Client directory status is invalid")


def _source_changed() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_FILE_CHANGED,
        "Workspace source changed after it was probed",
    )


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
        kind=getattr(result, "kind", "file"),
        files_transferred=getattr(result, "files_transferred", 1),
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
        ErrorCode.WORKSPACE_SOFT_LOCKED,
        ErrorCode.WORKSPACE_QUOTA_EXCEEDED,
        ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT,
        ErrorCode.WORKSPACE_BLOCKED_PATH,
        ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE,
        ErrorCode.WORKSPACE_FILE_CHANGED,
        ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
        ErrorCode.WORKSPACE_TRANSFER_BUSY,
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
