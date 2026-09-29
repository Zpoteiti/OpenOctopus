from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from openctopus_server.dto.config import JevStatus


class DreamRunResponse(BaseModel):
    id: UUID
    started_at: datetime
    finished_at: datetime | None
    status: Literal["pending", "skipped", "unchanged", "updated", "failed", "restoring", "restored"]
    message_count: int
    error: str | None
    restored_at: datetime | None


class DreamRunDetail(DreamRunResponse):
    before: str | None
    after: str | None


class DreamRunsResponse(BaseModel):
    availability: JevStatus
    next_run_at: datetime
    items: list[DreamRunResponse]
    next_offset: int | None
