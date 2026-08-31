from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import require_admin
from openctopus_server.db.advisory import lock_personal_quota_read
from openctopus_server.db.models import SystemConfig, User
from openctopus_server.db.session import get_db
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.dto.user import AdminUserResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError
from openctopus_server.services import users
from openctopus_server.workspace.fs import WorkspaceFS, get_workspace_fs
from openctopus_server.workspace.service import WorkspaceService, get_workspace_service

router = APIRouter(prefix="/api/admin/users", tags=["Admin"])
_PERSONAL_QUOTA_DEFAULT = 524_288_000


@dataclass(frozen=True)
class _UserSnapshot:
    id: UUID
    email: str
    name: str
    timezone: str
    is_admin: bool
    created_at: datetime


@router.get("", response_model=list[AdminUserResponse])
async def list_users(
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
    workspace_service: WorkspaceService = Depends(get_workspace_service),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[AdminUserResponse]:
    rows = await users.list_users(db, limit, offset)
    await lock_personal_quota_read(db)
    quota_value = await db.scalar(
        select(SystemConfig.value).where(SystemConfig.key == "quota_bytes")
    )
    quota_bytes = _PERSONAL_QUOTA_DEFAULT if quota_value is None else int(quota_value)
    snapshots = tuple(
        _UserSnapshot(
            id=row.id,
            email=row.email,
            name=row.name,
            timezone=row.timezone,
            is_admin=row.is_admin,
            created_at=row.created_at,
        )
        for row in rows
    )
    await db.commit()

    usage_values = await workspace_service.personal_usages([row.id for row in snapshots])
    return [
        AdminUserResponse(
            id=row.id,
            email=row.email,
            name=row.name,
            timezone=row.timezone,
            is_admin=row.is_admin,
            created_at=row.created_at,
            quota_bytes=quota_bytes,
            bytes_used=bytes_used,
            locked=bytes_used > quota_bytes,
        )
        for row, bytes_used in zip(snapshots, usage_values, strict=True)
    ]


@router.delete("/{user_id}", status_code=204)
async def delete_user(
    user_id: UUID,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
    workspace_fs: WorkspaceFS = Depends(get_workspace_fs),
    device_registry: DeviceRegistry = Depends(get_device_registry),
) -> Response:
    target = await users.get_user_by_id(db, user_id)
    if target is None:
        raise AuthError(ErrorCode.USER_NOT_FOUND, "User not found")
    await users.delete_user(
        db,
        target,
        workspace_fs=workspace_fs,
        device_registry=device_registry,
    )
    return Response(status_code=204)
