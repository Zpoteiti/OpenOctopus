from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_TRANSFER_WARNING_ORDER = (
    "transfer_ack_failed",
    "source_delete_failed",
    "source_changed_after_copy",
    "source_cleanup_incomplete",
)


class FileEditRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    old_text: str
    new_text: str
    replace_all: bool = False
    occurrence: int | None = Field(default=None, ge=1)
    line_hint: int | None = Field(default=None, ge=1)
    expected_replacements: int | None = Field(default=None, ge=1)


class FileMutationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    size: int
    etag: str
    created: bool
    replacements: int | None = None


class DirectoryEntryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    path: str
    kind: Literal["file", "directory", "symlink", "other"]
    size: int


class DirectoryEntryPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[DirectoryEntryResponse]
    limit: int
    offset: int
    next_offset: int | None
    truncated: bool = False


class PatchEditRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    action: Literal["replace", "add"]
    old_text: str | None = None
    new_text: str | None = None

    @model_validator(mode="after")
    def validate_action(self) -> PatchEditRequest:
        if self.action == "replace" and (self.old_text is None or self.new_text is None):
            raise ValueError("replace requires old_text and new_text")
        if self.action == "add" and self.new_text is None:
            raise ValueError("add requires new_text")
        return self


class StructuredPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    edits: list[PatchEditRequest] = Field(min_length=1, max_length=20)
    dry_run: bool = False


class PatchEditResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    action: Literal["replace", "add"]
    size: int
    etag: str
    created: bool
    replacements: int


class StructuredPatchResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[PatchEditResponse]
    dry_run: bool
    committed: int


class _TransferResponseFields(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bytes_transferred: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    warnings: list[
        Literal[
            "transfer_ack_failed",
            "source_delete_failed",
            "source_changed_after_copy",
            "source_cleanup_incomplete",
        ]
    ] = Field(max_length=8, json_schema_extra={"uniqueItems": True})

    @field_validator("warnings")
    @classmethod
    def validate_warning_order(cls, value: list[str]) -> list[str]:
        if len(set(value)) != len(value) or value != sorted(
            value,
            key=_TRANSFER_WARNING_ORDER.index,
        ):
            raise ValueError("transfer warnings must be unique and canonically ordered")
        return value


class FileTransferResponse(_TransferResponseFields):
    kind: Literal["file"]
    files_transferred: Literal[1]


class DirectoryTransferResponse(_TransferResponseFields):
    kind: Literal["directory"]
    files_transferred: int = Field(ge=1, le=10_000)


type TransferResponse = Annotated[
    FileTransferResponse | DirectoryTransferResponse,
    Field(
        discriminator="kind",
        description=(
            "Bounded file or directory aggregate. File responses always report one file; "
            "directory responses report 1..10000 regular files."
        ),
    ),
]


class GrepContextLine(BaseModel):
    model_config = ConfigDict(extra="forbid")

    line_number: int
    line: str


class GrepResultResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    line_number: int | None = None
    line: str | None = None
    count: int | None = None
    before: list[GrepContextLine] | None = None
    after: list[GrepContextLine] | None = None


class GrepResultPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[GrepResultResponse]
    limit: int
    offset: int
    next_offset: int | None
    truncated: bool = False
