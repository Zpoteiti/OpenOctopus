from __future__ import annotations

import hashlib
import re
import secrets
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import Device
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import DeviceError

DEFAULT_SSRF_DENYLIST = (
    "0.0.0.0/8",
    "127.0.0.0/8",
    "224.0.0.0/4",
    "240.0.0.0/4",
    "::/128",
    "::1/128",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "100.64.0.0/10",
    "169.254.0.0/16",
    "169.254.169.254/32",
    "fc00::/7",
    "fe80::/10",
    "ff00::/8",
)

_DEVICE_NAME = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


@dataclass(frozen=True)
class DeviceSnapshot:
    id: UUID
    user_id: UUID
    name: str
    token_hint: str
    workspace_path: str
    sandbox_mode: bool
    ssrf_denylist: list[str]
    created_at: datetime


def canonicalize_name(raw: str) -> str:
    normalized = unicodedata.normalize("NFC", raw)
    canonical = re.sub(r"\s+", "-", normalized.strip().lower())
    if len(canonical) > 64 or canonical == "server" or _DEVICE_NAME.fullmatch(canonical) is None:
        raise _invalid("Device name is invalid")
    return canonical


def mint_token() -> str:
    return f"openoctopus_dev_{secrets.token_urlsafe(32)}"


def token_digest(token: str) -> bytes:
    return hashlib.sha256(token.encode("utf-8")).digest()


def token_hint(token: str) -> str:
    return f"{token[:16]}...{token[-6:]}"


async def list_owned(db: AsyncSession, *, user_id: UUID) -> list[DeviceSnapshot]:
    rows = list(
        (
            await db.scalars(
                select(Device)
                .where(Device.user_id == user_id)
                .order_by(Device.created_at, Device.id)
            )
        ).all()
    )
    return [_snapshot(row) for row in rows]


async def find_by_token(db: AsyncSession, token: str) -> DeviceSnapshot | None:
    row = await db.scalar(select(Device).where(Device.token_hash == token_digest(token)))
    return _snapshot(row) if row is not None else None


async def create(
    db: AsyncSession,
    *,
    user_id: UUID,
    name: str,
    workspace_path: str,
    sandbox_mode: bool,
    ssrf_denylist: list[str] | None,
) -> tuple[DeviceSnapshot, str]:
    canonical_name = canonicalize_name(name)
    _validate_workspace_path(workspace_path)
    _validate_ssrf_denylist(ssrf_denylist)
    token = mint_token()
    device = Device(
        user_id=user_id,
        name=canonical_name,
        token_hash=token_digest(token),
        token_hint=token_hint(token),
        workspace_path=workspace_path,
        sandbox_mode=sandbox_mode,
        ssrf_denylist=_initial_ssrf_denylist(sandbox_mode, ssrf_denylist),
    )
    db.add(device)
    await _commit_or_name_conflict(db)
    return _snapshot(device), token


async def patch(
    db: AsyncSession,
    *,
    user_id: UUID,
    name: str,
    fields: set[str],
    new_name: str | None,
    workspace_path: str | None,
    sandbox_mode: bool | None,
    ssrf_denylist: list[str] | None,
) -> DeviceSnapshot:
    device = await _owned_for_update(db, user_id=user_id, name=name)
    if "name" in fields:
        if new_name is None:
            raise _invalid("Device name must be a string")
        device.name = canonicalize_name(new_name)
    if "workspace_path" in fields:
        if workspace_path is None:
            raise _invalid("Workspace path must be a string")
        _validate_workspace_path(workspace_path)
        device.workspace_path = workspace_path
    if "sandbox_mode" in fields:
        if sandbox_mode is None:
            raise _invalid("Sandbox mode must be a boolean")
        device.sandbox_mode = sandbox_mode
    if "ssrf_denylist" in fields:
        if ssrf_denylist is None:
            raise _invalid("SSRF denylist must be an array")
        _validate_ssrf_denylist(ssrf_denylist)
        device.ssrf_denylist = list(ssrf_denylist)
    await _commit_or_name_conflict(db)
    return _snapshot(device)


async def regenerate_token(
    db: AsyncSession,
    *,
    user_id: UUID,
    name: str,
) -> tuple[DeviceSnapshot, str]:
    device = await _owned_for_update(db, user_id=user_id, name=name)
    token = mint_token()
    device.token_hash = token_digest(token)
    device.token_hint = token_hint(token)
    await db.commit()
    return _snapshot(device), token


async def delete(db: AsyncSession, *, user_id: UUID, name: str) -> DeviceSnapshot:
    device = await _owned_for_update(db, user_id=user_id, name=name)
    snapshot = _snapshot(device)
    await db.delete(device)
    await db.commit()
    return snapshot


async def _owned_for_update(db: AsyncSession, *, user_id: UUID, name: str) -> Device:
    device = await db.scalar(
        select(Device).where(Device.user_id == user_id, Device.name == name).with_for_update()
    )
    if device is None:
        raise DeviceError(ErrorCode.DEVICE_NOT_FOUND, "Device not found")
    return device


async def _commit_or_name_conflict(db: AsyncSession) -> None:
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise DeviceError(ErrorCode.DEVICE_NAME_TAKEN, "Device name is already in use") from exc


def _initial_ssrf_denylist(sandbox_mode: bool, supplied: list[str] | None) -> list[str]:
    if supplied is not None:
        return list(supplied)
    if sandbox_mode:
        return list(DEFAULT_SSRF_DENYLIST)
    return []


def _validate_workspace_path(path: str) -> None:
    if "\x00" in path:
        raise _invalid("Workspace path must not contain NUL")
    if not path.strip():
        raise _invalid("Workspace path must not be empty")


def _validate_ssrf_denylist(entries: list[str] | None) -> None:
    if entries is None:
        return
    if len(entries) > 256 or any(
        "\x00" in entry or not entry.strip() or len(entry) > 512 for entry in entries
    ):
        raise _invalid("SSRF denylist entries must be non-blank and bounded")


def _snapshot(device: Device) -> DeviceSnapshot:
    return DeviceSnapshot(
        id=device.id,
        user_id=device.user_id,
        name=device.name,
        token_hint=device.token_hint,
        workspace_path=device.workspace_path,
        sandbox_mode=device.sandbox_mode,
        ssrf_denylist=list(device.ssrf_denylist),
        created_at=device.created_at,
    )


def _invalid(message: str) -> DeviceError:
    return DeviceError(ErrorCode.DEVICE_INVALID_REQUEST, message)
