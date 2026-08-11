from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.protocol import DeviceConfigFrame
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.dto.device import (
    DeviceCreateRequest,
    DevicePatchRequest,
    DeviceResponse,
    DeviceTokenResponse,
)
from openctopus_server.services import devices

router = APIRouter(prefix="/api/devices", tags=["Devices"])


@router.get("", response_model=list[DeviceResponse])
async def list_devices(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> list[DeviceResponse]:
    snapshots = await devices.list_owned(db, user_id=user.id)
    await db.commit()
    return [await _response(snapshot, registry) for snapshot in snapshots]


@router.post("", status_code=status.HTTP_201_CREATED, response_model=DeviceTokenResponse)
async def create_device(
    body: DeviceCreateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> DeviceTokenResponse:
    snapshot, token = await devices.create(
        db,
        user_id=user.id,
        name=body.name,
        workspace_path=body.workspace_path,
        sandbox_mode=body.sandbox_mode,
        ssrf_denylist=body.ssrf_denylist,
    )
    return DeviceTokenResponse(token=token, device=await _response(snapshot, registry))


@router.patch("/{name}/config", response_model=DeviceResponse)
async def patch_device(
    name: str,
    body: DevicePatchRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> DeviceResponse:
    async with _config_update_guard(registry, user.id, name):
        snapshot = await devices.patch(
            db,
            user_id=user.id,
            name=name,
            fields=set(body.model_fields_set),
            new_name=body.name,
            workspace_path=body.workspace_path,
            sandbox_mode=body.sandbox_mode,
            ssrf_denylist=body.ssrf_denylist,
        )
        if body.model_fields_set:
            # The commit above ended the DB transaction.  Close the session
            # before any potentially slow device transport await.
            await db.close()
            await registry.push_config(
                device_id=snapshot.id,
                user_id=snapshot.user_id,
                device_name=snapshot.name,
                config=DeviceConfigFrame(
                    workspace_path=snapshot.workspace_path,
                    sandbox_mode=snapshot.sandbox_mode,
                    ssrf_denylist=snapshot.ssrf_denylist,
                ),
            )
    return await _response(snapshot, registry)


@router.post("/{name}/regenerate-token", response_model=DeviceTokenResponse)
async def regenerate_device_token(
    name: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> DeviceTokenResponse:
    snapshot, token = await devices.regenerate_token(db, user_id=user.id, name=name)
    await registry.revoke(snapshot.id)
    return DeviceTokenResponse(token=token, device=await _response(snapshot, registry))


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_device(
    name: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> Response:
    snapshot = await devices.delete(db, user_id=user.id, name=name)
    await registry.remove_device(snapshot.id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def _response(snapshot: devices.DeviceSnapshot, registry: DeviceRegistry) -> DeviceResponse:
    return DeviceResponse(
        id=snapshot.id,
        name=snapshot.name,
        token_hint=snapshot.token_hint,
        workspace_path=snapshot.workspace_path,
        sandbox_mode=snapshot.sandbox_mode,
        ssrf_denylist=snapshot.ssrf_denylist,
        online=await registry.is_online(snapshot.id, user_id=snapshot.user_id),
        created_at=snapshot.created_at,
    )


@asynccontextmanager
async def _config_update_guard(
    registry: DeviceRegistry,
    user_id: UUID,
    device_name: str,
) -> AsyncIterator[None]:
    lock_factory = getattr(registry, "config_update_lock", None)
    if lock_factory is None:
        yield
        return
    async with lock_factory(user_id=user_id, device_name=device_name):
        yield
