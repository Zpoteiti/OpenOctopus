from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.password import hash_password
from openctopus_server.config import get_settings
from openctopus_server.db.models import User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError


async def create_user(
    db: AsyncSession,
    email: str,
    password: str,
    name: str,
    *,
    admin_token: str | None = None,
) -> User:
    existing = await db.execute(select(User).where(User.email == email))
    if existing.scalar_one_or_none() is not None:
        raise AuthError(ErrorCode.AUTH_EMAIL_TAKEN, "Email already in use")

    is_admin = False
    if admin_token is not None:
        settings = get_settings()
        if settings.admin_token is not None and admin_token == settings.admin_token:
            is_admin = True

    user = User(
        email=email,
        password_hash=hash_password(password),
        name=name,
        is_admin=is_admin,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def get_user_by_id(db: AsyncSession, user_id: UUID) -> User | None:
    result = await db.execute(select(User).where(User.id == user_id))
    return result.scalar_one_or_none()


async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    result = await db.execute(select(User).where(User.email == email))
    return result.scalar_one_or_none()


async def list_users(db: AsyncSession, limit: int, offset: int) -> list[User]:
    result = await db.execute(
        select(User).order_by(User.created_at).limit(limit).offset(offset)
    )
    return list(result.scalars().all())


async def update_user(
    db: AsyncSession,
    user: User,
    *,
    name: str | None = None,
    email: str | None = None,
    password: str | None = None,
) -> User:
    if email is not None and email != user.email:
        existing = await db.execute(select(User).where(User.email == email))
        if existing.scalar_one_or_none() is not None:
            raise AuthError(ErrorCode.AUTH_EMAIL_TAKEN, "Email already in use")
        user.email = email
    if name is not None:
        user.name = name
    if password is not None:
        user.password_hash = hash_password(password)
    await db.commit()
    await db.refresh(user)
    return user


async def delete_user(db: AsyncSession, user: User) -> None:
    await db.delete(user)
    await db.commit()


async def count_admins(db: AsyncSession) -> int:
    result = await db.execute(
        select(func.count()).select_from(User).where(User.is_admin.is_(True))
    )
    return result.scalar_one()


async def assert_not_last_admin(db: AsyncSession, user: User) -> None:
    if user.is_admin and await count_admins(db) == 1:
        raise AuthError(
            ErrorCode.AUTH_LAST_ADMIN_REQUIRED,
            "Cannot delete the last remaining admin",
        )
