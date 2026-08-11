import asyncio
import mimetypes
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import Device, Session
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
from openctopus_server.tools.device_field import (
    DEVICE_FIELD_MARKER,
    DEVICE_FIELD_NAME,
)
from openctopus_server.workspace.service import WorkspaceService

MESSAGE_CONTENT_MAX_CHARS = 16_000
MESSAGE_MEDIA_MAX_ITEMS = 10
MESSAGE_TOOL_TIMEOUT_SECONDS = 30.0

MESSAGE_TOOL_SCHEMA: dict[str, Any] = {
    "name": "message",
    "description": (
        "Deliver a message, optionally with workspace files, to the current web session."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "content": {
                "type": "string",
                "description": "Message text to deliver to the current web session.",
                "minLength": 1,
                "maxLength": MESSAGE_CONTENT_MAX_CHARS,
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


class _MessageArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    content: str = Field(min_length=1, max_length=MESSAGE_CONTENT_MAX_CHARS)
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

    @field_validator("content")
    @classmethod
    def require_visible_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("content must not be blank")
        return value

    @field_validator("media")
    @classmethod
    def require_unique_media(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("media paths must be unique")
        return value


class MessageTool(Tool):
    routing_mode = ToolRoutingMode.INTRINSIC_DEVICE

    def __init__(self, engine: AsyncEngine, workspace_service: WorkspaceService) -> None:
        self._engine = engine
        self._workspace_service = workspace_service

    def name(self) -> str:
        return "message"

    def schema(self) -> dict[str, Any]:
        return deepcopy(MESSAGE_TOOL_SCHEMA)

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        try:
            parsed = _MessageArgs.model_validate(args)
        except ValidationError as exc:
            return ToolResult(
                content=(f"[{ErrorCode.TOOL_INVALID_ARGS.value}] Invalid message arguments: {exc}"),
                is_error=True,
                code=ErrorCode.TOOL_INVALID_ARGS,
            )

        try:
            async with asyncio.timeout(MESSAGE_TOOL_TIMEOUT_SECONDS):
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
                    refs = await self._resolve_refs(db, parsed, ctx)
                    if isinstance(refs, ToolResult):
                        return refs
                return ToolResult(
                    content="Message delivered to the current web session.",
                    side_effect=MessageDeliveryEffect(delivery_refs=refs),
                )
        except TimeoutError:
            return ToolResult(
                content=(
                    f"[{ErrorCode.TOOL_EXEC_TIMEOUT.value}] message timed out after "
                    f"{MESSAGE_TOOL_TIMEOUT_SECONDS:g} seconds."
                ),
                is_error=True,
                code=ErrorCode.TOOL_EXEC_TIMEOUT,
            )

    async def _resolve_refs(
        self,
        db: AsyncSession,
        parsed: _MessageArgs,
        ctx: ToolContext,
    ) -> tuple[DeliveryRef, ...] | ToolResult:
        if not parsed.media:
            return ()
        if parsed.openoctopus_device != "server":
            exists = await db.scalar(
                select(Device.id).where(
                    Device.user_id == ctx.user_id,
                    Device.name == parsed.openoctopus_device,
                )
            )
            if exists is None:
                return ToolResult(
                    content=(
                        f"[{ErrorCode.TOOL_DEVICE_UNREACHABLE.value}] "
                        "Tool install site is unavailable"
                    ),
                    is_error=True,
                    code=ErrorCode.TOOL_DEVICE_UNREACHABLE,
                )
            try:
                return tuple(
                    DeviceFileDeliveryRef(
                        path=path,
                        openoctopus_device=parsed.openoctopus_device,
                        filename=_device_filename(path),
                        mime=_mime_type(path),
                    )
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
