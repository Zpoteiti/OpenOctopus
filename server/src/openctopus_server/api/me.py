from fastapi import APIRouter, Depends
from fastapi.responses import Response
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.user import UserResponse
from openctopus_server.services import users

router = APIRouter(prefix="/api/me", tags=["Me"])


class PatchMeRequest(BaseModel):
    name: str | None = None
    email: EmailStr | None = None
    password: str | None = Field(default=None, min_length=8)


@router.get("", response_model=UserResponse)
async def get_me(user: User = Depends(get_current_user)) -> UserResponse:
    return UserResponse.model_validate(user)


@router.patch("", response_model=UserResponse)
async def patch_me(
    body: PatchMeRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    updated = await users.update_user(
        db, user, name=body.name, email=body.email, password=body.password
    )
    return UserResponse.model_validate(updated)


@router.delete("", status_code=204)
async def delete_me(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await users.assert_not_last_admin(db, user)
    await users.delete_user(db, user)
    return Response(status_code=204)
