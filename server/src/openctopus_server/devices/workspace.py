from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import Device, User
from openctopus_server.devices.protocol import MAX_TEXT_FRAME_BYTES, ToolResultFrame
from openctopus_server.devices.registry import (
    DeviceBusyError,
    DeviceOutcomeUnknownError,
    DeviceRegistry,
    DeviceUnavailableError,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError

INTERNAL_WORKSPACE_ACTION = "__workspace_rest__"
MAX_WORKSPACE_RESPONSE_BYTES = 5_000_000
_MAX_PATH_LENGTH = 4096
_MAX_DEVICE_RESULT_BYTES = MAX_TEXT_FRAME_BYTES


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class DevicePatchEdit(_StrictModel):
    path: str = Field(min_length=1, max_length=_MAX_PATH_LENGTH)
    action: Literal["replace", "add"]
    old_text: str | None = None
    new_text: str | None = None

    def model_post_init(self, __context: Any) -> None:
        if self.action == "replace" and (self.old_text is None or self.new_text is None):
            raise ValueError("replace requires old_text and new_text")
        if self.action == "add" and self.new_text is None:
            raise ValueError("add requires new_text")


class DeviceWorkspaceAction(_StrictModel):
    """Private server-to-client request; it is never included in provider tools."""

    operation: Literal[
        "edit_file",
        "apply_patch",
        "delete_file",
        "delete_folder",
        "list_dir",
        "find_files",
        "grep",
        "transfer_local",
    ]
    path: str | None = Field(default=None, min_length=1, max_length=_MAX_PATH_LENGTH)
    dst_path: str | None = Field(default=None, min_length=1, max_length=_MAX_PATH_LENGTH)
    mode: Literal["copy", "move"] | None = None
    if_match: str | None = Field(default=None, min_length=1, max_length=512)
    old_text: str | None = None
    new_text: str | None = None
    replace_all: bool = False
    occurrence: int | None = Field(default=None, ge=1)
    line_hint: int | None = Field(default=None, ge=1)
    expected_replacements: int | None = Field(default=None, ge=1)
    edits: list[DevicePatchEdit] | None = Field(default=None, min_length=1, max_length=20)
    dry_run: bool = False
    recursive: bool = False
    limit: int = Field(default=200, ge=1, le=1000)
    offset: int = Field(default=0, ge=0, le=10000)
    query: str = ""
    glob: str | None = None
    type: str | None = Field(default=None, min_length=1, max_length=32)
    include_dirs: bool = False
    sort: Literal["path", "modified"] = "path"
    pattern: str | None = Field(default=None, min_length=1, max_length=4096)
    case_insensitive: bool = False
    fixed_strings: bool = False
    output_mode: Literal["content", "files_with_matches", "count"] = "files_with_matches"
    context_before: int = Field(default=0, ge=0, le=20)
    context_after: int = Field(default=0, ge=0, le=20)

    @field_validator("path", "old_text", "new_text", "query", "glob", "type", "pattern")
    @classmethod
    def _no_nul(cls, value: str | None) -> str | None:
        if value is not None and "\x00" in value:
            raise ValueError("workspace action values must not contain NUL")
        return value

    def model_post_init(self, __context: Any) -> None:
        if self.operation in {"edit_file", "delete_file", "delete_folder", "list_dir"}:
            if self.path is None:
                raise ValueError("workspace action requires path")
        elif self.operation == "transfer_local":
            if self.path is None or self.dst_path is None:
                raise ValueError("local transfer requires source and destination paths")
            if self.mode is None:
                self.mode = "copy"
        elif self.operation in {"find_files", "grep"}:
            if self.operation == "grep" and self.pattern is None:
                raise ValueError("grep requires pattern")
            if self.path is None:
                self.path = "."
        elif self.operation == "apply_patch" and self.edits is None:
            raise ValueError("apply_patch requires edits")
        if self.operation == "edit_file" and (self.old_text is None or self.new_text is None):
            raise ValueError("edit_file requires old_text and new_text")
        if self.operation == "apply_patch" and self.if_match is not None:
            raise ValueError("apply_patch does not accept if_match")


class DeviceFileMutationResult(_StrictModel):
    path: str
    size: int = Field(ge=0)
    etag: str = Field(min_length=1, max_length=512)
    created: bool
    replacements: int | None = Field(default=None, ge=0)


class DeviceDeleteResult(_StrictModel):
    deleted: Literal[True] = True


class DevicePatchEditResult(_StrictModel):
    path: str
    action: Literal["replace", "add"]
    size: int = Field(ge=0)
    etag: str = Field(min_length=1, max_length=512)
    created: bool
    replacements: int = Field(ge=0)


class DevicePatchResult(_StrictModel):
    items: list[DevicePatchEditResult]
    dry_run: bool
    committed: int = Field(ge=0)


class DeviceDirectoryEntryResult(_StrictModel):
    name: str
    path: str
    kind: Literal["file", "directory"]
    size: int = Field(ge=0)


class DeviceDirectoryPageResult(_StrictModel):
    items: list[DeviceDirectoryEntryResult]
    limit: int = Field(ge=1, le=1000)
    offset: int = Field(ge=0, le=10000)
    next_offset: int | None = Field(default=None, ge=0)
    truncated: bool = False


class DeviceGrepContextLineResult(_StrictModel):
    line_number: int = Field(ge=1)
    line: str


class DeviceGrepItemResult(_StrictModel):
    path: str
    line_number: int | None = Field(default=None, ge=1)
    line: str | None = None
    count: int | None = Field(default=None, ge=1)
    before: list[DeviceGrepContextLineResult] | None = None
    after: list[DeviceGrepContextLineResult] | None = None


class DeviceGrepPageResult(_StrictModel):
    items: list[DeviceGrepItemResult]
    limit: int = Field(ge=1, le=1000)
    offset: int = Field(ge=0, le=10000)
    next_offset: int | None = Field(default=None, ge=0)
    truncated: bool = False


class DeviceTransferLocalResult(_StrictModel):
    bytes_transferred: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    warnings: list[str] = Field(default_factory=list, max_length=8)


_RESULT_ADAPTERS: dict[str, TypeAdapter[Any]] = {
    "edit_file": TypeAdapter(DeviceFileMutationResult),
    "apply_patch": TypeAdapter(DevicePatchResult),
    "delete_file": TypeAdapter(DeviceDeleteResult),
    "delete_folder": TypeAdapter(DeviceDeleteResult),
    "list_dir": TypeAdapter(DeviceDirectoryPageResult),
    "find_files": TypeAdapter(DeviceDirectoryPageResult),
    "grep": TypeAdapter(DeviceGrepPageResult),
    "transfer_local": TypeAdapter(DeviceTransferLocalResult),
}


async def dispatch_workspace_action(
    db: AsyncSession,
    *,
    user: User,
    device_name: str,
    action: DeviceWorkspaceAction,
    registry: DeviceRegistry,
) -> Any:
    """Resolve ownership, close DB state, then await the live device call."""

    device_id = await db.scalar(
        select(Device.id).where(Device.user_id == user.id, Device.name == device_name)
    )
    if not isinstance(device_id, UUID):
        raise WorkspaceError(
            ErrorCode.TOOL_DEVICE_UNREACHABLE,
            "Workspace device is unavailable",
        )
    # A DB session must never remain open while waiting on a WebSocket result.
    await db.close()
    try:
        raw = await registry.dispatch_tool(
            device_id=device_id,
            user_id=user.id,
            name=INTERNAL_WORKSPACE_ACTION,
            args=action.model_dump(exclude_none=True),
            max_result_bytes=_MAX_DEVICE_RESULT_BYTES,
            timeout_seconds=_timeout_for(action.operation),
            expected_device_name=device_name,
        )
    except DeviceBusyError as exc:
        raise WorkspaceError(
            ErrorCode.TOOL_DEVICE_BUSY,
            "Workspace device is busy; retry later",
            headers={"Retry-After": "1"},
        ) from exc
    except DeviceOutcomeUnknownError as exc:
        raise WorkspaceError(
            ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
            "Workspace device call outcome is unknown",
        ) from exc
    except (DeviceUnavailableError, TimeoutError) as exc:
        raise WorkspaceError(
            ErrorCode.TOOL_DEVICE_UNREACHABLE,
            "Workspace device is unavailable",
        ) from exc
    return _decode_result(action.operation, raw)


def _decode_result(operation: str, raw: ToolResultFrame) -> Any:
    if raw.is_error:
        _raise_client_error(raw.code)
    if not isinstance(raw.content, str):
        raise WorkspaceError(
            ErrorCode.WORKSPACE_STORAGE_ERROR,
            "Workspace device returned an invalid result",
        )
    if len(raw.content.encode("utf-8")) > MAX_WORKSPACE_RESPONSE_BYTES:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_STORAGE_ERROR,
            "Workspace device result exceeds the response limit",
        )
    try:
        return _RESULT_ADAPTERS[operation].validate_json(raw.content, strict=True)
    except (ValidationError, json.JSONDecodeError) as exc:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_STORAGE_ERROR,
            "Workspace device returned an invalid result",
        ) from exc


def _raise_client_error(code: str | None) -> None:
    try:
        error_code = ErrorCode(code) if code is not None else None
    except ValueError:
        error_code = None
    if error_code is None:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_STORAGE_ERROR,
            "Workspace device returned an invalid error",
        )
    if error_code is ErrorCode.TOOL_DEVICE_UNREACHABLE:
        raise WorkspaceError(error_code, "Workspace device is unavailable")
    if error_code is ErrorCode.TOOL_DEVICE_BUSY:
        raise WorkspaceError(
            error_code,
            "Workspace device is busy; retry later",
            headers={"Retry-After": "1"},
        )
    if error_code is ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN:
        raise WorkspaceError(
            error_code,
            "Workspace device call outcome is unknown",
        )
    # Client-side tool errors use the same stable codes as WorkspaceService.
    if error_code in {
        ErrorCode.WORKSPACE_NOT_FOUND,
        ErrorCode.WORKSPACE_PERMISSION_DENIED,
        ErrorCode.WORKSPACE_SYMLINK_ESCAPE,
        ErrorCode.WORKSPACE_BLOCKED_PATH,
        ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
        ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE,
        ErrorCode.WORKSPACE_FILE_CHANGED,
        ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE,
        ErrorCode.WORKSPACE_TRANSFER_TIMEOUT,
        ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED,
        ErrorCode.WORKSPACE_INVALID_REQUEST,
        ErrorCode.TOOL_NO_MATCH,
        ErrorCode.TOOL_AMBIGUOUS_EDIT,
        ErrorCode.TOOL_IS_DIRECTORY,
        ErrorCode.TOOL_NOT_A_DIRECTORY,
        ErrorCode.TOOL_IS_FILE,
        ErrorCode.TOOL_PATH_OUTSIDE_WORKSPACE,
        ErrorCode.TOOL_INVALID_ARGS,
        ErrorCode.TOOL_INVALID_REGEX,
        ErrorCode.TOOL_INVALID_GLOB,
    }:
        raise WorkspaceError(error_code, "Workspace device rejected the operation")
    raise WorkspaceError(
        ErrorCode.WORKSPACE_STORAGE_ERROR,
        "Workspace device returned an unsupported error",
    )


def _timeout_for(operation: str) -> float:
    return {"delete_folder": 60.0, "grep": 60.0, "transfer_local": 60.0}.get(operation, 30.0)
