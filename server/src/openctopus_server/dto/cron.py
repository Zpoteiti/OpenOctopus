from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt


class CronCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    message: str
    every_seconds: StrictInt | None = None
    cron_expr: str | None = None
    at: str | None = None
    tz: str | None = None


class CronPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    message: str | None = None
    every_seconds: StrictInt | None = None
    cron_expr: str | None = None
    at: str | None = None
    tz: str | None = None


class EveryScheduleResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["every"] = "every"
    every_seconds: int


class CronScheduleResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["cron"] = "cron"
    cron_expr: str
    tz: str


class AtScheduleResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["at"] = "at"
    at: datetime
    tz: str


CronScheduleProjection = Annotated[
    EveryScheduleResponse | CronScheduleResponse | AtScheduleResponse,
    Field(discriminator="type"),
]


class CronJobSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    id: UUID
    name: str
    schedule: CronScheduleProjection
    session_id: UUID | None
    last_fired_at: datetime | None
    next_fire_at: datetime
    created_at: datetime


class CronJobResponse(CronJobSummary):
    message: str


class CronJobsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[CronJobSummary]
    next_offset: int | None

