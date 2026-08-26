from fastapi import APIRouter

from openctopus_server.api import (
    auth,
    device_ws,
    devices,
    health,
    me,
    sessions,
    workspace_files,
    workspaces,
)
from openctopus_server.api.admin import config as admin_config
from openctopus_server.api.admin import server_mcp as admin_server_mcp
from openctopus_server.api.admin import users as admin_users

router = APIRouter()
router.include_router(health.router)
router.include_router(auth.router)
router.include_router(me.router)
router.include_router(sessions.collection_router)
router.include_router(sessions.router)
router.include_router(sessions.control_router)
router.include_router(admin_config.router)
router.include_router(admin_server_mcp.router)
router.include_router(admin_users.router)
router.include_router(devices.router)
router.include_router(device_ws.router)
router.include_router(workspaces.router)
router.include_router(workspace_files.router)
