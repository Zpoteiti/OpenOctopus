from uuid import UUID

from fastapi import Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.cookies import COOKIE_NAME
from openctopus_server.auth.jwt import verify_jwt
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError


def get_current_user_id(request: Request) -> UUID:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:]
    if not token:
        raise AuthError(ErrorCode.AUTH_UNAUTHORIZED, "Not authenticated")
    return verify_jwt(token)


async def get_current_user(
    user_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
) -> User:
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None:
        raise AuthError(ErrorCode.AUTH_UNAUTHORIZED, "User not found")
    return user


async def require_admin(user: User = Depends(get_current_user)) -> User:
    if not user.is_admin:
        raise AuthError(ErrorCode.AUTH_FORBIDDEN, "Admin access required")
    return user
