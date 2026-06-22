from fastapi import APIRouter

from openctopus_server.api import auth, health, me

router = APIRouter()
router.include_router(health.router)
router.include_router(auth.router)
router.include_router(me.router)
