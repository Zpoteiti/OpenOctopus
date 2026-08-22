from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import Device
from openctopus_server.devices.mcp_models import PersistedMcpCatalog
from openctopus_server.services.devices import parse_stored_mcp_catalog


@dataclass(frozen=True, slots=True)
class OwnerDeviceSnapshot:
    id: UUID
    name: str
    workspace_path: str
    restrict_to_workspace: bool
    shell_timeout_max: int
    config_revision: int
    mcp_catalog: PersistedMcpCatalog


async def load_owner_device_snapshot(
    db: AsyncSession,
    *,
    user_id: UUID,
) -> tuple[OwnerDeviceSnapshot, ...]:
    devices = (
        (
            await db.execute(
                select(Device)
                .where(Device.user_id == user_id)
                .order_by(Device.created_at, Device.id)
            )
        )
        .scalars()
        .all()
    )
    return tuple(
        OwnerDeviceSnapshot(
            id=device.id,
            name=device.name,
            workspace_path=device.workspace_path,
            restrict_to_workspace=device.restrict_to_workspace,
            shell_timeout_max=device.shell_timeout_max,
            config_revision=device.config_revision,
            mcp_catalog=parse_stored_mcp_catalog(device.mcp_catalog),
        )
        for device in devices
    )
