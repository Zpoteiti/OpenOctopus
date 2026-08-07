from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class WorkspaceCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    quota_bytes: int = Field(ge=1)


class WorkspacePatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    quota_bytes: int | None = Field(default=None, ge=1)


class WorkspaceMemberCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: UUID | None = None
    email: EmailStr | None = None


class WorkspaceMemberResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: UUID
    email: str
    name: str


class _WorkspaceResponseBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    name: str
    quota_bytes: int
    bytes_used: int
    locked: bool


class PersonalWorkspaceResponse(_WorkspaceResponseBase):
    type: Literal["personal"]


class SharedWorkspaceResponse(_WorkspaceResponseBase):
    type: Literal["shared"]
    suffix: str
    ref: str
    created_by: UUID | None = None
    members: list[WorkspaceMemberResponse] | None = None


WorkspaceResponse = Annotated[
    PersonalWorkspaceResponse | SharedWorkspaceResponse,
    Field(discriminator="type"),
]


class WorkspacePage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[WorkspaceResponse]
    limit: int
    offset: int
    next_offset: int | None
    truncated: bool = False


class WorkspaceMemberPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[WorkspaceMemberResponse]
    limit: int
    offset: int
    next_offset: int | None
    truncated: bool = False
