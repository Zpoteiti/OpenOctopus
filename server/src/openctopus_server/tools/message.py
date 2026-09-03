from __future__ import annotations

import asyncio
import mimetypes
from copy import deepcopy
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Annotated, Any, Protocol
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.channels.types import ChannelName, ExternalChannel, ToolProfile
from openctopus_server.db.models import Device, Session
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.devices.registry import (
    DeviceBusyError,
    DeviceOutcomeUnknownError,
    DeviceRegistry,
    DeviceRouteSnapshot,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import (
    TransferBusyError,
    TransferError,
    TransferIntegrityError,
    TransferLease,
)
from openctopus_server.devices.workspace import FileSourceProbe
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import (
    DeliveryRef,
    DeviceFileDeliveryRef,
    MessageDeliveryEffect,
    Tool,
    ToolContext,
    ToolResult,
    ToolRoutingMode,
    WorkspaceFileDeliveryRef,
)
from openctopus_server.tools.device_directory_jobs import DeviceDirectoryJobController
from openctopus_server.tools.device_field import (
    DEVICE_FIELD_MARKER,
    DEVICE_FIELD_NAME,
)
from openctopus_server.workspace.service import WorkspaceService

MESSAGE_CONTENT_MAX_CHARS = 16_000
MESSAGE_MEDIA_MAX_ITEMS = 10
MESSAGE_TOOL_TIMEOUT_SECONDS = 120.0

MESSAGE_TOOL_SCHEMA: dict[str, Any] = {
    "name": "message",
    "description": (
        "Deliver a message, optionally with workspace files, to the current or an "
        "explicitly selected conversation. Other sessions do not inherit its context; "
        "confirmation must return to the original conversation."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "Message text to deliver.",
                "minLength": 1,
                "maxLength": MESSAGE_CONTENT_MAX_CHARS,
            },
            "channel": {
                "type": "string",
                "enum": ["discord", "dingtalk"],
            },
            "chat_id": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512,
            },
            DEVICE_FIELD_NAME: {
                "type": "string",
                "enum": ["server"],
                "description": "Workspace install site for media paths (default server).",
                DEVICE_FIELD_MARKER: True,
                "default": "server",
            },
            "media": {
                "type": "array",
                "description": "Optional workspace files to attach.",
                "items": {"type": "string", "minLength": 1, "maxLength": 4096},
                "maxItems": MESSAGE_MEDIA_MAX_ITEMS,
                "uniqueItems": True,
                "default": [],
            },
        },
        "required": ["content"],
        "additionalProperties": False,
    },
}


MESSAGE_ONLY_TOOL_SCHEMA: dict[str, Any] = {
    "name": "message",
    "description": (
        "Deliver plain text to the current external conversation or the owner's "
        "paired direct message. Other sessions do not inherit this conversation; "
        "ask the owner to return to the source channel and mention the Bot to continue."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "Message text to deliver.",
                "minLength": 1,
                "maxLength": MESSAGE_CONTENT_MAX_CHARS,
            },
            "channel": {
                "type": "string",
                "enum": ["discord", "dingtalk"],
            },
            "chat_id": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512,
            },
        },
        "required": ["content"],
        "additionalProperties": False,
    },
}


class _MessageTargetArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    content: str = Field(min_length=1, max_length=MESSAGE_CONTENT_MAX_CHARS)
    channel: ExternalChannel | None = None
    chat_id: Annotated[str, Field(min_length=1, max_length=512)] | None = None

    @field_validator("content")
    @classmethod
    def require_visible_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("content must not be blank")
        return value

    @model_validator(mode="after")
    def require_complete_target(self) -> _MessageTargetArgs:
        if (self.channel is None) != (self.chat_id is None):
            raise ValueError("channel and chat_id must be provided together")
        return self


class _OwnerMessageArgs(_MessageTargetArgs):
    openoctopus_device: Annotated[
        str,
        Field(
            min_length=1,
            max_length=64,
            pattern=r"^(?:server|[a-z0-9]+(?:-[a-z0-9]+)*)$",
        ),
    ] = "server"
    media: list[Annotated[str, Field(min_length=1, max_length=4096)]] = Field(
        default_factory=list,
        max_length=MESSAGE_MEDIA_MAX_ITEMS,
    )

    @field_validator("media")
    @classmethod
    def require_unique_media(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("media paths must be unique")
        return value


class _MessageOnlyArgs(_MessageTargetArgs):
    pass


@dataclass(frozen=True, slots=True)
class ResolvedMessageTarget:
    channel: ExternalChannel
    chat_id: str
    binding_generation: UUID


class MessageTargetResolver(Protocol):
    async def resolve_message_target(
        self,
        *,
        user_id: UUID,
        session_id: UUID,
        tool_profile: ToolProfile,
        current_channel: ChannelName | None,
        current_chat_id: str | None,
        current_binding_generation: UUID | None,
        requested_channel: ExternalChannel | None,
        requested_chat_id: str | None,
        has_media: bool,
    ) -> ResolvedMessageTarget | ToolResult: ...


class MessageDeliveryRouter(Protocol):
    async def deliver_message(
        self,
        *,
        target: ResolvedMessageTarget,
        content: str,
        delivery_refs: tuple[DeliveryRef, ...],
        ctx: ToolContext,
    ) -> ToolResult: ...


class MessageTool(Tool):
    routing_mode = ToolRoutingMode.INTRINSIC_DEVICE
    manages_issue_boundary = True

    def __init__(
        self,
        engine: AsyncEngine,
        workspace_service: WorkspaceService,
        *,
        target_resolver: MessageTargetResolver | None = None,
        delivery_router: MessageDeliveryRouter | None = None,
        device_registry: DeviceRegistry | None = None,
    ) -> None:
        self._engine = engine
        self._workspace_service = workspace_service
        self._target_resolver = target_resolver
        self._delivery_router = delivery_router
        self._device_registry = device_registry

    def name(self) -> str:
        return "message"

    def schema(self) -> dict[str, Any]:
        return deepcopy(MESSAGE_TOOL_SCHEMA)

    def schema_for_profile(self, tool_profile: ToolProfile) -> dict[str, Any] | None:
        if tool_profile == "owner_full":
            return self.schema()
        if tool_profile == "message_only":
            return deepcopy(MESSAGE_ONLY_TOOL_SCHEMA)
        return None

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        try:
            if ctx.tool_profile == "owner_full":
                parsed: _OwnerMessageArgs | _MessageOnlyArgs = (
                    _OwnerMessageArgs.model_validate(args)
                )
            elif ctx.tool_profile == "message_only":
                parsed = _MessageOnlyArgs.model_validate(args)
            else:
                return _not_allowed()
        except ValidationError as exc:
            return ToolResult(
                content=(f"[{ErrorCode.TOOL_INVALID_ARGS.value}] Invalid message arguments: {exc}"),
                is_error=True,
                code=ErrorCode.TOOL_INVALID_ARGS,
            )

        has_media = isinstance(parsed, _OwnerMessageArgs) and bool(parsed.media)
        if parsed.channel is not None or ctx.current_channel in {
            "discord",
            "dingtalk",
        }:
            try:
                async with asyncio.timeout(MESSAGE_TOOL_TIMEOUT_SECONDS):
                    target = await self._resolve_external_target(
                        parsed=parsed,
                        ctx=ctx,
                        has_media=has_media,
                    )
                    if isinstance(target, ToolResult):
                        return target
                    if self._delivery_router is None:
                        return _channel_not_configured(
                            "External message delivery is unavailable"
                        )
                    refs = await self._resolve_external_refs(parsed, ctx)
                    if isinstance(refs, ToolResult):
                        return refs
            except TimeoutError:
                return _timeout_result()

            # The Router owns the logical-delivery deadline and must return its
            # durable delivery identity and terminal aggregate to the agent.
            return await self._delivery_router.deliver_message(
                target=target,
                content=parsed.content,
                delivery_refs=refs,
                ctx=ctx,
            )

        try:
            async with asyncio.timeout(MESSAGE_TOOL_TIMEOUT_SECONDS):
                if ctx.tool_profile == "message_only":
                    return _channel_not_configured(
                        "message_only requires a current external conversation"
                    )
                async with AsyncSession(self._engine, expire_on_commit=False) as db:
                    current_web_session = await db.scalar(
                        select(Session.id).where(
                            Session.id == ctx.session_id,
                            Session.user_id == ctx.user_id,
                            Session.channel == "web",
                            Session.chat_id == str(ctx.session_id),
                            Session.session_key == f"web:{ctx.session_id}",
                        )
                    )
                    if current_web_session is None:
                        return ToolResult(
                            content=(
                                f"[{ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED.value}] "
                                "message is only available to the current web session"
                            ),
                            is_error=True,
                            code=ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED,
                        )
                    assert isinstance(parsed, _OwnerMessageArgs)
                    refs = await self._resolve_refs(db, parsed, ctx)
                    if isinstance(refs, ToolResult):
                        return refs
                if ctx.on_issued is not None:
                    ctx.on_issued()
                return ToolResult(
                    content="Message delivered to the current web session.",
                    side_effect=MessageDeliveryEffect(delivery_refs=refs),
                )
        except TimeoutError:
            return _timeout_result()

    async def _resolve_external_target(
        self,
        *,
        parsed: _OwnerMessageArgs | _MessageOnlyArgs,
        ctx: ToolContext,
        has_media: bool,
    ) -> ResolvedMessageTarget | ToolResult:
        if self._target_resolver is None:
            return _channel_not_configured("External message target is unavailable")
        return await self._target_resolver.resolve_message_target(
            user_id=ctx.user_id,
            session_id=ctx.session_id,
            tool_profile=ctx.tool_profile,
            current_channel=ctx.current_channel,
            current_chat_id=ctx.current_chat_id,
            current_binding_generation=ctx.current_binding_generation,
            requested_channel=parsed.channel,
            requested_chat_id=parsed.chat_id,
            has_media=has_media,
        )

    async def _resolve_external_refs(
        self,
        parsed: _OwnerMessageArgs | _MessageOnlyArgs,
        ctx: ToolContext,
    ) -> tuple[DeliveryRef, ...] | ToolResult:
        if not isinstance(parsed, _OwnerMessageArgs) or not parsed.media:
            return ()
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            return await self._resolve_refs(db, parsed, ctx)

    async def _resolve_refs(
        self,
        db: AsyncSession,
        parsed: _OwnerMessageArgs,
        ctx: ToolContext,
    ) -> tuple[DeliveryRef, ...] | ToolResult:
        if not parsed.media:
            return ()
        if parsed.openoctopus_device != "server":
            expected_id = (
                ctx.device_targets.get(parsed.openoctopus_device)
                if ctx.device_targets is not None
                else None
            )
            if ctx.device_targets is not None and expected_id is None:
                return _device_unreachable()
            query = (
                select(Device.id).where(
                    Device.user_id == ctx.user_id,
                    Device.name == parsed.openoctopus_device,
                )
            )
            if expected_id is not None:
                query = query.where(Device.id == expected_id)
            device_id = await db.scalar(query)
            if device_id is None:
                return _device_unreachable()
            try:
                paths = tuple(
                    (path, _device_filename(path), _mime_type(path))
                    for path in parsed.media
                )
            except ValueError:
                return ToolResult(
                    content=(
                        f"[{ErrorCode.TOOL_INVALID_ARGS.value}] "
                        "Device media path is invalid"
                    ),
                    is_error=True,
                    code=ErrorCode.TOOL_INVALID_ARGS,
                )
            # Device calls must not retain a database transaction or connection
            # while waiting on the current WebSocket generation.
            await db.rollback()
            return await self._resolve_device_refs(
                user_id=ctx.user_id,
                device_id=device_id,
                device_name=parsed.openoctopus_device,
                paths=paths,
            )

        files = [
            await self._workspace_service.resolve_delivery_file(
                db,
                user_id=ctx.user_id,
                path=path,
            )
            for path in parsed.media
        ]
        canonical_files = {
            (file.target.kind, file.target.id, file.relative_path) for file in files
        }
        if len(canonical_files) != len(files):
            return ToolResult(
                content=(
                    f"[{ErrorCode.TOOL_INVALID_ARGS.value}] "
                    "media paths must resolve to unique files"
                ),
                is_error=True,
                code=ErrorCode.TOOL_INVALID_ARGS,
            )
        return tuple(
            WorkspaceFileDeliveryRef(
                path=path,
                workspace_id=file.target.id,
                workspace_relative_path=file.relative_path,
                filename=PurePosixPath(file.relative_path).name,
                mime=_mime_type(file.relative_path),
                size=file.metadata.size,
            )
            for path, file in zip(parsed.media, files, strict=True)
        )

    async def _resolve_device_refs(
        self,
        *,
        user_id: UUID,
        device_id: UUID,
        device_name: str,
        paths: tuple[tuple[str, str, str], ...],
    ) -> tuple[DeliveryRef, ...] | ToolResult:
        registry = self._device_registry
        if registry is None:
            return _device_unreachable()
        route = await registry.get_route_snapshot(
            device_id,
            user_id=user_id,
            expected_device_name=device_name,
        )
        if route is None:
            return _device_unreachable()

        try:
            lease = await registry.transfers.acquire_operation(user_id)
        except TransferBusyError:
            return _message_error(
                ErrorCode.TOOL_DEVICE_BUSY,
                "Device media metadata is busy",
            )
        try:
            refs: list[DeviceFileDeliveryRef] = []
            for path, filename, mime in paths:
                try:
                    probe = await _probe_device_file(
                        registry,
                        route=route,
                        user_id=user_id,
                        path=path,
                    )
                except (DeviceUnavailableError, TimeoutError):
                    return _device_unreachable()
                except DeviceBusyError:
                    return _message_error(
                        ErrorCode.TOOL_DEVICE_BUSY,
                        "Device media metadata is busy",
                    )
                except (DeviceOutcomeUnknownError, TransferIntegrityError):
                    return _media_size_unknown()
                except TransferError as exc:
                    try:
                        code = ErrorCode(exc.code)
                    except ValueError:
                        return _media_size_unknown()
                    return _message_error(code, "Device media metadata is unavailable")

                if probe is None:
                    return _message_error(
                        ErrorCode.TOOL_IS_DIRECTORY,
                        "Device media path must reference a regular file",
                    )
                refs.append(
                    DeviceFileDeliveryRef(
                        path=path,
                        device_id=device_id,
                        openoctopus_device=device_name,
                        filename=filename,
                        mime=mime,
                        size=probe.size,
                        fingerprint=probe.fingerprint,
                    )
                )
            return tuple(refs)
        finally:
            close = asyncio.create_task(_close_transfer_lease(lease))
            await await_future_cancellation_safe(close)


def _mime_type(path: str) -> str:
    mime, _ = mimetypes.guess_type(path, strict=False)
    return mime or "application/octet-stream"


def _device_filename(path: str) -> str:
    if not path.strip() or "\x00" in path or path.endswith(("/", "\\")):
        raise ValueError("invalid device path")
    filename = PurePosixPath(path.replace("\\", "/")).name
    if filename in {"", ".", ".."}:
        raise ValueError("invalid device path")
    return filename


async def _probe_device_file(
    registry: DeviceRegistry,
    *,
    route: DeviceRouteSnapshot,
    user_id: UUID,
    path: str,
) -> FileSourceProbe | None:
    controller = DeviceDirectoryJobController(
        registry=registry,
        route=route,
        user_id=user_id,
        directory_operation_id=new_uuid7(),
        idle_timeout_seconds=registry.transfers.idle_timeout_seconds,
    )
    active = True
    try:
        await controller.start_source_probe(path)
        status = await controller.wait_source_until(
            frozenset({"succeeded", "ready_retrieval", "failed", "outcome_unknown"})
        )
        if status.state == "succeeded" and isinstance(status.probe, FileSourceProbe):
            await controller.release_source_probe()
            active = False
            return status.probe
        if status.state == "ready_retrieval":
            return None
        if status.state == "outcome_unknown":
            raise DeviceOutcomeUnknownError("Device media metadata is unknown")
        if status.state == "failed" and status.terminal_error is not None:
            raise TransferError(status.terminal_error.code)
        raise TransferIntegrityError("Device media metadata is invalid")
    finally:
        if active:
            cleanup = asyncio.create_task(_retire_device_probe(controller))
            await await_future_cancellation_safe(cleanup)


async def _retire_device_probe(controller: DeviceDirectoryJobController) -> None:
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


async def _close_transfer_lease(lease: TransferLease) -> None:
    await lease.aclose()


def _device_unreachable() -> ToolResult:
    return _message_error(
        ErrorCode.TOOL_DEVICE_UNREACHABLE,
        "Tool install site is unavailable",
    )


def _media_size_unknown() -> ToolResult:
    return _message_error(
        ErrorCode.TOOL_CHANNEL_MEDIA_SIZE_UNKNOWN,
        "Device media size could not be verified",
    )


def _message_error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=f"[{code.value}] {message}",
        is_error=True,
        code=code,
    )


def _channel_not_configured(message: str) -> ToolResult:
    return ToolResult(
        content=f"[{ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED.value}] {message}",
        is_error=True,
        code=ErrorCode.TOOL_CHANNEL_NOT_CONFIGURED,
    )


def _not_allowed() -> ToolResult:
    return ToolResult(
        content=(
            f"[{ErrorCode.TOOL_NOT_ALLOWED.value}] "
            "Tool is not allowed for this turn"
        ),
        is_error=True,
        code=ErrorCode.TOOL_NOT_ALLOWED,
    )


def _timeout_result() -> ToolResult:
    return ToolResult(
        content=(
            f"[{ErrorCode.TOOL_EXEC_TIMEOUT.value}] message timed out after "
            f"{MESSAGE_TOOL_TIMEOUT_SECONDS:g} seconds."
        ),
        is_error=True,
        code=ErrorCode.TOOL_EXEC_TIMEOUT,
    )
