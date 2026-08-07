from __future__ import annotations

import asyncio
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.workspace import (
    WorkspaceCreateRequest,
    WorkspaceMemberCreateRequest,
    WorkspaceMemberPage,
    WorkspaceMemberResponse,
    WorkspacePage,
    WorkspacePatchRequest,
    WorkspaceResponse,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.services import workspaces
from openctopus_server.workspace.fs import WorkspaceFS, get_workspace_fs
from openctopus_server.workspace.service import WorkspaceService, get_workspace_service

router = APIRouter(prefix="/api/workspaces", tags=["Workspaces"])


@router.get("", response_model=WorkspacePage, response_model_exclude_unset=True)
async def list_workspaces(
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    workspace_service: WorkspaceService = Depends(get_workspace_service),
) -> dict[str, Any]:
    snapshots, next_offset = await workspaces.list_authorized(
        db,
        user_id=user.id,
        limit=limit,
        offset=offset,
    )
    await db.commit()
    items = await _render_many(snapshots, workspace_service)
    return _page(items, limit=limit, offset=offset, next_offset=next_offset)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=WorkspaceResponse,
)
async def create_workspace(
    body: WorkspaceCreateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    workspace_service: WorkspaceService = Depends(get_workspace_service),
) -> dict[str, Any]:
    snapshot = await workspaces.create_shared(
        db,
        user_id=user.id,
        name=body.name,
        quota_bytes=body.quota_bytes,
    )
    await db.commit()
    return await _render(snapshot, workspace_service)


@router.get(
    "/{workspace_ref}",
    response_model=WorkspaceResponse,
)
async def get_workspace(
    workspace_ref: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    workspace_service: WorkspaceService = Depends(get_workspace_service),
) -> dict[str, Any]:
    snapshot = await workspaces.get_authorized(
        db,
        user_id=user.id,
        workspace_ref=workspace_ref,
    )
    await db.commit()
    return await _render(snapshot, workspace_service)


@router.patch(
    "/{workspace_ref}",
    response_model=WorkspaceResponse,
)
async def patch_workspace(
    workspace_ref: str,
    body: WorkspacePatchRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    workspace_service: WorkspaceService = Depends(get_workspace_service),
) -> dict[str, Any]:
    if not body.model_fields_set:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_INVALID_REQUEST,
            "Workspace patch must not be empty",
        )
    snapshot = await workspaces.patch_shared(
        db,
        user_id=user.id,
        workspace_ref=workspace_ref,
        name=body.name,
        quota_bytes=body.quota_bytes,
    )
    await db.commit()
    return await _render(snapshot, workspace_service)


@router.get("/{workspace_ref}/members", response_model=WorkspaceMemberPage)
async def list_workspace_members(
    workspace_ref: str,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    members, next_offset = await workspaces.list_members(
        db,
        user_id=user.id,
        workspace_ref=workspace_ref,
        limit=limit,
        offset=offset,
    )
    items = [
        WorkspaceMemberResponse(**member.__dict__).model_dump(mode="json") for member in members
    ]
    return _page(items, limit=limit, offset=offset, next_offset=next_offset)


@router.post(
    "/{workspace_ref}/members",
    status_code=status.HTTP_201_CREATED,
    response_model=WorkspaceMemberResponse,
)
async def add_workspace_member(
    workspace_ref: str,
    body: WorkspaceMemberCreateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> WorkspaceMemberResponse:
    member = await workspaces.add_member(
        db,
        user_id=user.id,
        workspace_ref=workspace_ref,
        member_user_id=body.user_id,
        email=body.email,
    )
    return WorkspaceMemberResponse(**member.__dict__)


@router.delete("/{workspace_ref}/members/{member_user_id}", status_code=204)
async def remove_workspace_member(
    workspace_ref: str,
    member_user_id: UUID,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    workspace_fs: WorkspaceFS = Depends(get_workspace_fs),
) -> Response:
    await workspaces.remove_authorized_member(
        db,
        user_id=user.id,
        workspace_ref=workspace_ref,
        member_user_id=member_user_id,
        workspace_fs=workspace_fs,
    )
    return Response(status_code=204)


async def _render_many(
    snapshots: list[workspaces.WorkspaceSnapshot],
    service: WorkspaceService,
) -> list[dict[str, Any]]:
    rendered: list[dict[str, Any]] = []
    for start in range(0, len(snapshots), 4):
        rendered.extend(
            await asyncio.gather(
                *(_render(snapshot, service) for snapshot in snapshots[start : start + 4])
            )
        )
    return rendered


async def _render(
    snapshot: workspaces.WorkspaceSnapshot,
    service: WorkspaceService,
) -> dict[str, Any]:
    bytes_used = await service.authorized_usage(snapshot.target)
    result: dict[str, Any] = {
        "id": str(snapshot.id),
        "name": snapshot.name,
        "type": snapshot.kind,
        "quota_bytes": snapshot.quota_bytes,
        "bytes_used": bytes_used,
        "locked": bytes_used > snapshot.quota_bytes,
    }
    if snapshot.kind == "shared":
        result.update(
            suffix=snapshot.suffix,
            ref=snapshot.ref,
            created_by=str(snapshot.created_by) if snapshot.created_by is not None else None,
        )
        if snapshot.members is not None:
            result["members"] = [
                WorkspaceMemberResponse(**member.__dict__).model_dump(mode="json")
                for member in snapshot.members
            ]
    return result


def _page(
    items: list[Any],
    *,
    limit: int,
    offset: int,
    next_offset: int | None,
) -> dict[str, Any]:
    return {
        "items": items,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "truncated": False,
    }
