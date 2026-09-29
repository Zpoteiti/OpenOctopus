from datetime import UTC, datetime
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.api.admin.config import get_jev_service
from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.automations.dream import (
    DreamService,
    next_midnight,
    run_detail,
    run_response,
)
from openctopus_server.db.models import DreamRun, User
from openctopus_server.db.session import get_db
from openctopus_server.dto.dream import DreamRunDetail, DreamRunsResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.provider.jev import JevService

router = APIRouter(prefix="/api/dream", tags=["Dream"])


def get_dream_service(request: Request) -> DreamService:
    return cast(DreamService, request.app.state.dream_service)


@router.get("", response_model=DreamRunsResponse)
async def list_dream_runs(
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    jev: JevService = Depends(get_jev_service),
) -> DreamRunsResponse:
    runs = list(
        (
            await db.scalars(
                select(DreamRun)
                .where(
                    DreamRun.user_id == user.id,
                )
                .order_by(DreamRun.started_at.desc(), DreamRun.id.desc())
                .limit(limit + 1)
                .offset(offset)
            )
        ).all()
    )
    await db.commit()
    return DreamRunsResponse(
        availability=await jev.status(),
        next_run_at=next_midnight(datetime.now(UTC), user.timezone),
        items=[run_response(run) for run in runs[:limit]],
        next_offset=offset + limit if len(runs) > limit else None,
    )


@router.get("/{run_id}", response_model=DreamRunDetail)
async def get_dream_run(
    run_id: UUID,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DreamRunDetail:
    run = await db.scalar(
        select(DreamRun).where(DreamRun.user_id == user.id, DreamRun.id == run_id)
    )
    if run is None:
        raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "Dream record was not found")
    return run_detail(run)


@router.post("/{run_id}/restore", response_model=DreamRunDetail)
async def restore_dream_run(
    run_id: UUID,
    user: User = Depends(get_current_user),
    service: DreamService = Depends(get_dream_service),
) -> DreamRunDetail:
    return await service.restore(user.id, run_id)
