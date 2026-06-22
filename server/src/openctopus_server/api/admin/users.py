from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import require_admin
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.user import AdminUserResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError
from openctopus_server.services import users

router = APIRouter(prefix="/api/admin/users", tags=["Admin"])


@router.get("", response_model=list[AdminUserResponse])
async def list_users(
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[AdminUserResponse]:
    rows = await users.list_users(db, limit, offset)
    return [AdminUserResponse.model_validate(u) for u in rows]


@router.delete("/{user_id}", status_code=204)
async def delete_user(
    user_id: UUID,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    target = await users.get_user_by_id(db, user_id)
    if target is None:
        raise AuthError(ErrorCode.USER_NOT_FOUND, "User not found")
    await users.assert_not_last_admin(db, target)
    await users.delete_user(db, target)
    return Response(status_code=204)
