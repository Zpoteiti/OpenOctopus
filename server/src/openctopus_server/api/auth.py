from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.cookies import clear_auth_cookie, set_auth_cookie
from openctopus_server.auth.jwt import create_jwt
from openctopus_server.auth.password import verify_password
from openctopus_server.db.session import get_db
from openctopus_server.dto.user import UserResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import AuthError
from openctopus_server.services import users
from openctopus_server.services.system_config import get_config_view
from openctopus_server.workspace.service import WorkspaceService, get_workspace_service

router = APIRouter(prefix="/api/auth", tags=["Auth"])

INITIAL_MEMORY = """# Getting started

No personal profile has been recorded yet. In an early conversation, ask the user:

- how they would like to be addressed;
- what they would like to call you and what role or personality they want you to have;
- which stable preferences and personal facts they want you to remember.

Update SOUL.md and MEMORY.md with their answers.
"""


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    name: str
    admin_token: str | None = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class AuthResponse(BaseModel):
    jwt: str
    user: UserResponse


@router.post("/register", response_model=AuthResponse, status_code=201)
async def register(
    body: RegisterRequest,
    db: AsyncSession = Depends(get_db),
    workspace: WorkspaceService = Depends(get_workspace_service),
) -> JSONResponse:
    config = await get_config_view(db)
    user = await users.create_user(
        db,
        email=body.email,
        password=body.password,
        name=body.name,
        admin_token=body.admin_token,
    )
    await workspace.write(
        db,
        user_id=user.id,
        path="SOUL.md",
        data=config.default_soul.encode(),
        if_none_match=True,
    )
    await workspace.write(
        db,
        user_id=user.id,
        path="MEMORY.md",
        data=INITIAL_MEMORY.encode(),
        if_none_match=True,
    )
    token = create_jwt(user.id)
    response = JSONResponse(
        status_code=201,
        content={"jwt": token, "user": UserResponse.model_validate(user).model_dump(mode="json")},
    )
    set_auth_cookie(response, token)
    return response


@router.post("/login", response_model=AuthResponse)
async def login(body: LoginRequest, db: AsyncSession = Depends(get_db)) -> JSONResponse:
    user = await users.get_user_by_email(db, body.email)
    if user is None or not verify_password(body.password, user.password_hash):
        raise AuthError(ErrorCode.AUTH_INVALID_CREDENTIALS, "Invalid email or password")
    token = create_jwt(user.id)
    response = JSONResponse(
        status_code=200,
        content={"jwt": token, "user": UserResponse.model_validate(user).model_dump(mode="json")},
    )
    set_auth_cookie(response, token)
    return response


@router.post("/logout", status_code=204)
async def logout() -> Response:
    response = Response(status_code=204)
    clear_auth_cookie(response)
    return response
