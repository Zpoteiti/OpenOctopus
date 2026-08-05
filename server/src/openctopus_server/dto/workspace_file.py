from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class FileEditRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    old_text: str
    new_text: str
    occurrence: int | None = Field(default=None, ge=1)


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
