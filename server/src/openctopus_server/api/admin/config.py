from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import require_admin
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.config import AdminConfig, ConfigPatch
from openctopus_server.services import system_config

router = APIRouter(prefix="/api/admin/config", tags=["Admin"])


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
