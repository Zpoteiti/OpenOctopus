from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.cron import (
    CronCreateRequest,
    CronJobResponse,
    CronJobsResponse,
    CronPatchRequest,
)
from openctopus_server.services import cron

router = APIRouter(prefix="/api/cron", tags=["Cron"])


@router.get("", response_model=CronJobsResponse)
async def list_cron_jobs(
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CronJobsResponse:
    return await cron.list_owned(
        db,
        user_id=user.id,
        limit=limit,
        offset=offset,
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=CronJobResponse,
)
async def create_cron_job(
    body: CronCreateRequest,
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CronJobResponse:
    result = await cron.create_owned(db, user_id=user.id, request=body)
    _wake_scheduler(request)
    return result


@router.get("/{job_id}", response_model=CronJobResponse)
async def get_cron_job(
    job_id: UUID,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CronJobResponse:
    return await cron.get_owned(db, user_id=user.id, job_id=job_id)


@router.patch("/{job_id}", response_model=CronJobResponse)
async def patch_cron_job(
    job_id: UUID,
    body: CronPatchRequest,
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> CronJobResponse:
    result = await cron.patch_owned(
        db,
        user_id=user.id,
        job_id=job_id,
        request=body,
    )
    _wake_scheduler(request)
    return result


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_cron_job(
    job_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await cron.delete_owned(db, user_id=user.id, job_id=job_id)
    _wake_scheduler(request)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _wake_scheduler(request: Request) -> None:
    scheduler = getattr(request.app.state, "cron_scheduler", None)
    if scheduler is not None:
        scheduler.wake()
