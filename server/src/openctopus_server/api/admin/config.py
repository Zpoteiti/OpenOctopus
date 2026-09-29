from typing import cast

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import require_admin
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.config import AdminConfig, ConfigPatch, JevStatus
from openctopus_server.provider.jev import JevService
from openctopus_server.services import system_config

router = APIRouter(prefix="/api/admin/config", tags=["Admin"])


def get_jev_service(request: Request) -> JevService:
    service = getattr(request.app.state, "jev_service", None)
    if service is None:
        service = JevService(get_engine())
        request.app.state.jev_service = service
    return cast(JevService, service)


@router.get("", response_model=AdminConfig)
async def get_config(
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> AdminConfig:
    return await system_config.get_config_view(db)


@router.patch("", response_model=AdminConfig)
async def patch_config(
    body: ConfigPatch,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> AdminConfig:
    return await system_config.patch_config(db, body)


@router.post("/jev/check", response_model=JevStatus)
async def check_jev(
    user: User = Depends(require_admin),
    service: JevService = Depends(get_jev_service),
) -> JevStatus:
    return await service.check()
