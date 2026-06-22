from fastapi import APIRouter

from openctopus_server.api import auth, health, me
from openctopus_server.api.admin import config as admin_config
from openctopus_server.api.admin import users as admin_users

router = APIRouter()
router.include_router(health.router)
router.include_router(auth.router)
router.include_router(me.router)
router.include_router(admin_config.router)
router.include_router(admin_users.router)
