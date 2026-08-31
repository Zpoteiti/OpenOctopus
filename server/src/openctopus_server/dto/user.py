from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: str
    name: str
    timezone: str
    is_admin: bool
    created_at: datetime


class AdminUserResponse(UserResponse):
    quota_bytes: int
    bytes_used: int
    locked: bool
