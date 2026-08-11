from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

INTERNAL_WORKSPACE_ACTION = "__workspace_rest__"
MAX_WORKSPACE_RESPONSE_BYTES = 5_000_000
MAX_TEXT_EDIT_BYTES = 8 * 1024 * 1024
MAX_SCAN_OBJECTS = 10_000


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class WorkspacePatchEdit(_StrictModel):
    path: str = Field(min_length=1, max_length=4096)
    action: Literal["replace", "add"]
    old_text: str | None = None
    new_text: str | None = None

    def model_post_init(self, __context: object) -> None:
        if self.action == "replace" and (self.old_text is None or self.new_text is None):
            raise ValueError("replace requires old_text and new_text")
        if self.action == "add" and self.new_text is None:
            raise ValueError("add requires new_text")


class WorkspaceRestAction(_StrictModel):
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
    path: str | None = Field(default=None, min_length=1, max_length=4096)
    dst_path: str | None = Field(default=None, min_length=1, max_length=4096)
    mode: Literal["copy", "move"] | None = None
    if_match: str | None = Field(default=None, min_length=1, max_length=512)
    old_text: str | None = None
    new_text: str | None = None
    replace_all: bool = False
    occurrence: int | None = Field(default=None, ge=1)
    line_hint: int | None = Field(default=None, ge=1)
    expected_replacements: int | None = Field(default=None, ge=1)
    edits: list[WorkspacePatchEdit] | None = Field(default=None, min_length=1, max_length=20)
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

    def model_post_init(self, __context: object) -> None:
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


class WorkspaceFileMutation(_StrictModel):
    path: str
    size: int = Field(ge=0)
    etag: str = Field(min_length=1, max_length=512)
    created: bool
    replacements: int | None = Field(default=None, ge=0)


class WorkspaceDeleteResult(_StrictModel):
    deleted: Literal[True] = True


class WorkspacePatchEditResult(_StrictModel):
    path: str
    action: Literal["replace", "add"]
    size: int = Field(ge=0)
    etag: str = Field(min_length=1, max_length=512)
    created: bool
    replacements: int = Field(ge=0)


class WorkspacePatchResult(_StrictModel):
    items: list[WorkspacePatchEditResult]
    dry_run: bool
    committed: int = Field(ge=0)


class WorkspaceDirectoryEntry(_StrictModel):
    name: str
    path: str
    kind: Literal["file", "directory"]
    size: int = Field(ge=0)


class WorkspaceDirectoryPage(_StrictModel):
    items: list[WorkspaceDirectoryEntry]
    limit: int = Field(ge=1, le=1000)
    offset: int = Field(ge=0, le=10000)
    next_offset: int | None = Field(default=None, ge=0)
    truncated: bool = False


class WorkspaceGrepContextLine(_StrictModel):
    line_number: int = Field(ge=1)
    line: str


class WorkspaceGrepItem(_StrictModel):
    path: str
    line_number: int | None = Field(default=None, ge=1)
    line: str | None = None
    count: int | None = Field(default=None, ge=1)
    before: list[WorkspaceGrepContextLine] | None = None
    after: list[WorkspaceGrepContextLine] | None = None


class WorkspaceGrepPage(_StrictModel):
    items: list[WorkspaceGrepItem]
    limit: int = Field(ge=1, le=1000)
    offset: int = Field(ge=0, le=10000)
    next_offset: int | None = Field(default=None, ge=0)
    truncated: bool = False


class WorkspaceTransferLocalResult(_StrictModel):
    bytes_transferred: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    warnings: list[str] = Field(default_factory=list, max_length=8)
