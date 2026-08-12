from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
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
    body: DevicePatchRequest | None = None,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> DeviceResponse:
    patch = body or DevicePatchRequest()
    async with _config_update_guard(registry, user.id, name):
        snapshot = await devices.patch(
            db,
            user_id=user.id,
            name=name,
            fields=set(patch.model_fields_set),
            new_name=patch.name,
            workspace_path=patch.workspace_path,
            sandbox_mode=patch.sandbox_mode,
            ssrf_denylist=patch.ssrf_denylist,
        )
        if patch.model_fields_set:
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
    mutation = asyncio.create_task(
        _regenerate_token_and_invalidate(
            db,
            registry,
            user_id=user.id,
            name=name,
        )
    )
    snapshot, token = await await_future_cancellation_safe(mutation)
    return DeviceTokenResponse(token=token, device=await _response(snapshot, registry))


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_device(
    name: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> Response:
    mutation = asyncio.create_task(
        _delete_and_invalidate(
            db,
            registry,
            user_id=user.id,
            name=name,
        )
    )
    await await_future_cancellation_safe(mutation)
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


async def _after_commit[T](
    db: AsyncSession,
    invalidate: Callable[[], Awaitable[T]],
) -> T:
    try:
        await db.close()
    finally:
        result = await invalidate()
    return result


async def _regenerate_token_and_invalidate(
    db: AsyncSession,
    registry: DeviceRegistry,
    *,
    user_id: UUID,
    name: str,
) -> tuple[devices.DeviceSnapshot, str]:
    snapshot, token = await devices.regenerate_token(db, user_id=user_id, name=name)
    await _after_commit(db, lambda: registry.revoke(snapshot.id))
    return snapshot, token


async def _delete_and_invalidate(
    db: AsyncSession,
    registry: DeviceRegistry,
    *,
    user_id: UUID,
    name: str,
) -> devices.DeviceSnapshot:
    snapshot = await devices.delete(db, user_id=user_id, name=name)
    await _after_commit(db, lambda: registry.remove_device(snapshot.id))
    return snapshot


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
