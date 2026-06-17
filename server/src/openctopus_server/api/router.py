from fastapi import APIRouter

from openctopus_server.api import health

router = APIRouter()
router.include_router(health.router)
