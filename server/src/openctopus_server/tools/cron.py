from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, TypeAdapter, ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.dto.cron import CronCreateRequest
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import OpenOctopusError
from openctopus_server.services import cron as cron_service
from openctopus_server.tools.base import Tool, ToolContext, ToolResult, ToolRoutingMode

CRON_TOOL_MAX_OUTPUT_CHARS = 16_000
CRON_TOOL_PAGE_SIZE = 20

CRON_TOOL_SCHEMA: dict[str, Any] = {
    "name": "cron",
    "description": "Create, list, or remove recurring and one-time Agent jobs.",
    "input_schema": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["add", "list", "remove"],
                "description": "Operation to perform.",
            },
            "name": {"type": "string", "minLength": 1, "maxLength": 120},
            "message": {
                "type": "string",
                "minLength": 1,
                "maxLength": 32_000,
                "description": "Task to run when the job fires.",
            },
            "every_seconds": {
                "type": "integer",
                "minimum": 60,
                "maximum": 31_536_000,
            },
            "cron_expr": {
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "description": "Standard five-field cron expression.",
            },
            "at": {
                "type": "string",
                "description": "RFC 3339 instant or local date-time for a one-time job.",
            },
            "tz": {
                "type": "string",
                "minLength": 1,
                "maxLength": 64,
                "description": "IANA timezone for cron or one-time local time.",
            },
            "offset": {"type": "integer", "minimum": 0},
            "job_id": {"type": "string", "format": "uuid"},
        },
        "required": ["action"],
        "additionalProperties": False,
    },
}


class _AddArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["add"]
    name: str | None = None
    message: str
    every_seconds: StrictInt | None = None
    cron_expr: str | None = None
    at: str | None = None
    tz: str | None = None


class _ListArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["list"]
    offset: StrictInt = Field(default=0, ge=0)


class _RemoveArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["remove"]
    job_id: UUID


_CronArgs = Annotated[_AddArgs | _ListArgs | _RemoveArgs, Field(discriminator="action")]
_CRON_ARGS_ADAPTER: TypeAdapter[_CronArgs] = TypeAdapter(_CronArgs)


class CronTool(Tool):
    routing_mode = ToolRoutingMode.PURE_SERVER

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        wake: Callable[[], None] | None = None,
    ) -> None:
        self._engine = engine
        self._wake = wake

    def name(self) -> str:
        return "cron"

    def schema(self) -> dict[str, Any]:
        return deepcopy(CRON_TOOL_SCHEMA)

    def max_output_chars(self) -> int:
        return CRON_TOOL_MAX_OUTPUT_CHARS

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        try:
            parsed = _CRON_ARGS_ADAPTER.validate_python(args)
        except ValidationError as exc:
            code = (
                ErrorCode.TOOL_MISSING_REQUIRED_FIELD
                if any(error["type"] == "missing" for error in exc.errors())
                else ErrorCode.TOOL_INVALID_SCHEDULE
            )
            return _error(code, "Cron arguments are invalid")

        try:
            async with AsyncSession(self._engine, expire_on_commit=False) as db:
                if isinstance(parsed, _AddArgs):
                    job = await cron_service.create_owned(
                        db,
                        user_id=ctx.user_id,
                        request=CronCreateRequest(
                            name=parsed.name,
                            message=parsed.message,
                            every_seconds=parsed.every_seconds,
                            cron_expr=parsed.cron_expr,
                            at=parsed.at,
                            tz=parsed.tz,
                        ),
                    )
                    self._notify_scheduler()
                    return ToolResult(
                        content=json.dumps(
                            {"job": job.model_dump(mode="json")},
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    )
                if isinstance(parsed, _ListArgs):
                    page = await cron_service.list_owned(
                        db,
                        user_id=ctx.user_id,
                        limit=CRON_TOOL_PAGE_SIZE,
                        offset=parsed.offset,
                    )
                    return ToolResult(
                        content=json.dumps(
                            page.model_dump(mode="json"),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    )

                await cron_service.delete_owned(
                    db,
                    user_id=ctx.user_id,
                    job_id=parsed.job_id,
                )
                self._notify_scheduler()
                return ToolResult(
                    content="Future triggers stopped; existing history retained."
                )
        except OpenOctopusError as exc:
            return _service_error(exc)
        except Exception:
            return _error(ErrorCode.TOOL_DB_ERROR, "Cron storage is unavailable")

    def _notify_scheduler(self) -> None:
        if self._wake is not None:
            self._wake()


def _service_error(exc: OpenOctopusError) -> ToolResult:
    if exc.code in {ErrorCode.CRON_INVALID_SCHEDULE, ErrorCode.TIMEZONE_INVALID}:
        return _error(ErrorCode.TOOL_INVALID_SCHEDULE, exc.message)
    if exc.code is ErrorCode.CRON_JOB_NOT_FOUND:
        return _error(ErrorCode.TOOL_CRON_JOB_NOT_FOUND, "Cron job not found")
    return _error(ErrorCode.TOOL_DB_ERROR, "Cron storage is unavailable")


def _error(code: ErrorCode, message: str) -> ToolResult:
    return ToolResult(
        content=f"[{code.value}] {message}",
        is_error=True,
        code=code,
    )
