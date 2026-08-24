from __future__ import annotations

import json
from typing import Annotated, Any, Literal, Self, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)
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
from openctopus_server.directory_contract import (
    MAX_DIRECTORY_ENTRIES,
    MAX_DIRECTORY_INTEGER,
    MAX_DIRECTORY_MANIFEST_BYTES,
    MAX_DIRECTORY_PAGE_BYTES,
    DirectoryManifest,
    DirectoryManifestPage,
    _validate_visible_ascii,
    canonical_json_bytes,
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
    kind: Literal["file"]
    files_transferred: Literal[1]
    bytes_transferred: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    warnings: list[str] = Field(default_factory=list, max_length=8)


_TRANSFER_WARNING_ORDER = (
    "transfer_ack_failed",
    "source_delete_failed",
    "source_changed_after_copy",
    "source_cleanup_incomplete",
)


def _ordered_transfer_warnings(value: list[str]) -> list[str]:
    if len(set(value)) != len(value) or any(item not in _TRANSFER_WARNING_ORDER for item in value):
        raise ValueError("transfer warnings must be unique symbolic values")
    if value != sorted(value, key=_TRANSFER_WARNING_ORDER.index):
        raise ValueError("transfer warnings must use the canonical order")
    return value


def _uuid7_string(value: str) -> str:
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ValueError("directory operation IDs must be UUID v7") from exc
    if parsed.version != 7 or str(parsed) != value:
        raise ValueError("directory operation IDs must be canonical UUID v7")
    return value


def _canonical_relative_path(value: str) -> str:
    if "\x00" in value or value.startswith("/"):
        raise ValueError("relative path is invalid")
    components = value.split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise ValueError("relative path is invalid")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ValueError("relative path must be valid UTF-8") from exc
    return value


def _workspace_path(value: str) -> str:
    if "\x00" in value:
        raise ValueError("workspace path is invalid")
    return value


class _DirectoryOperation(_StrictModel):
    directory_operation_id: str = Field(min_length=36, max_length=36)

    _operation_id = field_validator("directory_operation_id")(_uuid7_string)


class _ExpectedDirectoryOperation(_DirectoryOperation):
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class TransferSourceProbeStartAction(_DirectoryOperation):
    operation: Literal["transfer_source_probe_start"]
    path: str = Field(min_length=1, max_length=_MAX_PATH_LENGTH)

    _path = field_validator("path")(_workspace_path)


class TransferSourceProbeStatusAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_source_probe_status"]
    outer_progress_seq: int | None = Field(default=None, ge=0, le=MAX_DIRECTORY_INTEGER)


class TransferSourceProbePageAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_source_probe_page"]
    offset: int = Field(ge=0, le=MAX_DIRECTORY_ENTRIES)


class TransferSourceProbeHoldAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_source_probe_hold"]


class TransferSourceProbeCancelAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_source_probe_cancel"]


class TransferSourceProbeReleaseAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_source_probe_release"]


class TransferDirectoryAuthorizeSourceChildAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_directory_authorize_source_child"]
    transfer_uuid: str = Field(min_length=36, max_length=36)
    relative_path: str = Field(min_length=1, max_length=_MAX_PATH_LENGTH)
    fingerprint: str = Field(min_length=1, max_length=512)

    _transfer_uuid = field_validator("transfer_uuid")(_uuid7_string)
    _relative_path = field_validator("relative_path")(_canonical_relative_path)


class TransferSourceCleanupAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_source_cleanup"]


class TransferDirectoryPreflightAction(_DirectoryOperation):
    operation: Literal["transfer_directory_preflight"]
    dst_path: str = Field(min_length=1, max_length=_MAX_PATH_LENGTH)
    manifest: DirectoryManifest

    _destination = field_validator("dst_path")(_workspace_path)

    @field_validator("manifest", mode="before")
    @classmethod
    def _manifest_from_wire(cls, value: Any) -> DirectoryManifest:
        if isinstance(value, DirectoryManifest):
            return value
        return DirectoryManifest.model_validate_json(canonical_json_bytes(value), strict=True)


class TransferDirectoryStatusAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_directory_status"]
    outer_progress_seq: int | None = Field(default=None, ge=0, le=MAX_DIRECTORY_INTEGER)


class TransferDirectoryPrepareAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_directory_prepare"]


class TransferDirectoryAuthorizeChildAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_directory_authorize_child"]
    transfer_uuid: str = Field(min_length=36, max_length=36)
    relative_path: str = Field(min_length=1, max_length=_MAX_PATH_LENGTH)

    _transfer_uuid = field_validator("transfer_uuid")(_uuid7_string)
    _relative_path = field_validator("relative_path")(_canonical_relative_path)


class TransferDirectoryFinishAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_directory_finish"]


class TransferDirectoryCancelAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_directory_cancel"]


class TransferDirectoryReleaseAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_directory_release"]


class TransferLocalDirectoryStartAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_local_directory_start"]
    source_path: str = Field(min_length=1, max_length=_MAX_PATH_LENGTH)
    dst_path: str = Field(min_length=1, max_length=_MAX_PATH_LENGTH)
    mode: Literal["copy", "move"]
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    _paths = field_validator("source_path", "dst_path")(_workspace_path)


class TransferLocalDirectoryStatusAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_local_directory_status"]


class TransferLocalDirectoryCancelAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_local_directory_cancel"]


class TransferLocalDirectoryReleaseAction(_ExpectedDirectoryOperation):
    operation: Literal["transfer_local_directory_release"]


DirectoryDeviceAction = Annotated[
    TransferSourceProbeStartAction
    | TransferSourceProbeStatusAction
    | TransferSourceProbePageAction
    | TransferSourceProbeHoldAction
    | TransferSourceProbeCancelAction
    | TransferSourceProbeReleaseAction
    | TransferDirectoryAuthorizeSourceChildAction
    | TransferSourceCleanupAction
    | TransferDirectoryPreflightAction
    | TransferDirectoryStatusAction
    | TransferDirectoryPrepareAction
    | TransferDirectoryAuthorizeChildAction
    | TransferDirectoryFinishAction
    | TransferDirectoryCancelAction
    | TransferDirectoryReleaseAction
    | TransferLocalDirectoryStartAction
    | TransferLocalDirectoryStatusAction
    | TransferLocalDirectoryCancelAction
    | TransferLocalDirectoryReleaseAction,
    Field(discriminator="operation"),
]

_DIRECTORY_ACTION_ADAPTER: TypeAdapter[DirectoryDeviceAction] = TypeAdapter(
    DirectoryDeviceAction
)


class DirectoryStableError(_StrictModel):
    code: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=512)


class FileSourceProbe(_StrictModel):
    kind: Literal["file"] = "file"
    size: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    fingerprint: str = Field(min_length=1, max_length=512)

    _fingerprint = field_validator("fingerprint")(_validate_visible_ascii)


class DirectorySourceProbe(_StrictModel):
    kind: Literal["directory"] = "directory"
    root_identity: str = Field(min_length=1, max_length=512)
    scanned_entries: int = Field(ge=1, le=MAX_DIRECTORY_ENTRIES)
    file_count: int = Field(ge=1, le=MAX_DIRECTORY_ENTRIES)
    total_bytes: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    page_count: int = Field(ge=1, le=MAX_DIRECTORY_ENTRIES)


SourceProbe = Annotated[FileSourceProbe | DirectorySourceProbe, Field(discriminator="kind")]


class DirectoryCleanupResult(_StrictModel):
    cleanup_complete: bool
    warnings: list[str] = Field(default_factory=list, max_length=8)

    _warnings = field_validator("warnings")(_ordered_transfer_warnings)


class DirectoryCommandResult(_StrictModel):
    state: Literal["running", "accepted", "held", "released"]
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class DeviceTransferDirectoryResult(_StrictModel):
    kind: Literal["directory"]
    files_transferred: int = Field(ge=1, le=MAX_DIRECTORY_ENTRIES)
    bytes_transferred: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    warnings: list[str] = Field(default_factory=list, max_length=8)

    _warnings = field_validator("warnings")(_ordered_transfer_warnings)


class SourceDirectoryJobStatus(_StrictModel):
    state: Literal[
        "scanning",
        "ready_retrieval",
        "held",
        "source_cleanup",
        "succeeded",
        "failed",
        "outcome_unknown",
    ]
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    progress_seq: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    entries_processed: int = Field(ge=0, le=MAX_DIRECTORY_ENTRIES)
    files_processed: int = Field(ge=0, le=MAX_DIRECTORY_ENTRIES)
    bytes_processed: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    probe: SourceProbe | None = None
    terminal_result: DirectoryCleanupResult | None = None
    terminal_error: DirectoryStableError | None = None

    @model_validator(mode="after")
    def _state_payload(self) -> Self:
        if self.state in {"ready_retrieval", "held", "source_cleanup"}:
            if not isinstance(self.probe, DirectorySourceProbe):
                raise ValueError("directory source state requires a directory probe")
            if self.terminal_result is not None or self.terminal_error is not None:
                raise ValueError("nonterminal source state cannot carry terminal payload")
        elif self.state == "scanning":
            if (
                self.probe is not None
                or self.terminal_result is not None
                or self.terminal_error is not None
            ):
                raise ValueError("scanning cannot expose probe or terminal payload")
        elif self.state == "succeeded":
            file_success = isinstance(self.probe, FileSourceProbe) and self.terminal_result is None
            cleanup_success = (
                isinstance(self.probe, DirectorySourceProbe) and self.terminal_result is not None
            )
            if not (file_success or cleanup_success) or self.terminal_error is not None:
                raise ValueError("succeeded source state requires its bounded result")
        elif self.terminal_error is None or self.terminal_result is not None:
            raise ValueError("failed source state requires only a terminal error")
        return self


class DestinationDirectoryJobStatus(_StrictModel):
    state: Literal[
        "preflighting",
        "ready",
        "preparing",
        "reserved",
        "copying",
        "finalizing",
        "finalized_held",
        "cleaning",
        "failed",
        "outcome_unknown",
    ]
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    progress_seq: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    files_processed: int = Field(ge=0, le=MAX_DIRECTORY_ENTRIES)
    bytes_processed: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    cleanup_complete: bool | None = None
    terminal_result: DeviceTransferDirectoryResult | None = None
    terminal_error: DirectoryStableError | None = None

    @model_validator(mode="after")
    def _state_payload(self) -> Self:
        if self.state == "finalized_held":
            if self.terminal_result is None or self.terminal_error is not None:
                raise ValueError("finalized destination requires a result")
        elif self.state in {"failed", "outcome_unknown"}:
            if self.terminal_error is None or self.terminal_result is not None:
                raise ValueError("terminal destination failure requires an error")
        elif self.terminal_result is not None or self.terminal_error is not None:
            raise ValueError("nonterminal destination state cannot carry terminal payload")
        return self


class LocalDirectoryJobStatus(_StrictModel):
    state: Literal[
        "ready_not_started", "running", "cancelling", "succeeded", "failed", "outcome_unknown"
    ]
    phase: Literal[
        "waiting", "preparing", "hashing", "copying", "revalidating", "renaming", "cleanup"
    ]
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    progress_seq: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    files_processed: int = Field(ge=0, le=MAX_DIRECTORY_ENTRIES)
    bytes_processed: int = Field(ge=0, le=MAX_DIRECTORY_INTEGER)
    terminal_result: DeviceTransferDirectoryResult | None = None
    terminal_error: DirectoryStableError | None = None

    @model_validator(mode="after")
    def _state_payload(self) -> Self:
        if self.state == "succeeded":
            if self.terminal_result is None or self.terminal_error is not None:
                raise ValueError("successful local job requires a result")
        elif self.state in {"failed", "outcome_unknown"}:
            if self.terminal_error is None or self.terminal_result is not None:
                raise ValueError("terminal local failure requires an error")
        elif self.terminal_result is not None or self.terminal_error is not None:
            raise ValueError("nonterminal local state cannot carry terminal payload")
        return self


DirectoryDeviceResult = (
    DirectoryCommandResult
    | DirectoryManifestPage
    | SourceDirectoryJobStatus
    | DestinationDirectoryJobStatus
    | LocalDirectoryJobStatus
)

SourceDirectoryStartResult = DirectoryCommandResult | SourceDirectoryJobStatus
DestinationDirectoryStartResult = (
    DirectoryCommandResult | DestinationDirectoryJobStatus | LocalDirectoryJobStatus
)

_DIRECTORY_RESULT_ADAPTERS: dict[str, TypeAdapter[Any]] = {
    "transfer_source_probe_start": TypeAdapter(SourceDirectoryStartResult),
    "transfer_source_probe_status": TypeAdapter(SourceDirectoryJobStatus),
    "transfer_source_probe_page": TypeAdapter(DirectoryManifestPage),
    "transfer_source_probe_hold": TypeAdapter(DirectoryCommandResult),
    "transfer_source_probe_cancel": TypeAdapter(DirectoryCommandResult),
    "transfer_source_probe_release": TypeAdapter(DirectoryCommandResult),
    "transfer_directory_authorize_source_child": TypeAdapter(DirectoryCommandResult),
    "transfer_source_cleanup": TypeAdapter(DirectoryCommandResult),
    "transfer_directory_preflight": TypeAdapter(DestinationDirectoryStartResult),
    "transfer_directory_status": TypeAdapter(DestinationDirectoryJobStatus),
    "transfer_directory_prepare": TypeAdapter(DirectoryCommandResult),
    "transfer_directory_authorize_child": TypeAdapter(DirectoryCommandResult),
    "transfer_directory_finish": TypeAdapter(DirectoryCommandResult),
    "transfer_directory_cancel": TypeAdapter(DirectoryCommandResult),
    "transfer_directory_release": TypeAdapter(DirectoryCommandResult),
    "transfer_local_directory_start": TypeAdapter(DirectoryCommandResult),
    "transfer_local_directory_status": TypeAdapter(LocalDirectoryJobStatus),
    "transfer_local_directory_cancel": TypeAdapter(DirectoryCommandResult),
    "transfer_local_directory_release": TypeAdapter(DirectoryCommandResult),
}


def build_directory_action(operation: str, /, **payload: object) -> dict[str, Any]:
    """Validate and serialize one private directory action for the Device wire."""

    value = dict(payload)
    value["operation"] = operation
    action = _DIRECTORY_ACTION_ADAPTER.validate_python(value, strict=True)
    return action.model_dump(mode="json")


def parse_directory_result(operation: str, raw: str | bytes) -> DirectoryDeviceResult:
    """Strictly parse one private directory result without projecting its error."""

    adapter = _DIRECTORY_RESULT_ADAPTERS.get(operation)
    if adapter is None:
        raise ValueError(f"unsupported directory operation: {operation}")
    encoded_size = len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
    limit = (
        MAX_DIRECTORY_PAGE_BYTES
        if operation == "transfer_source_probe_page"
        else MAX_DIRECTORY_MANIFEST_BYTES
    )
    if encoded_size > limit:
        raise ValueError("directory result exceeds its wire limit")
    return cast(DirectoryDeviceResult, adapter.validate_json(raw, strict=True))


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
