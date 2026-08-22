from __future__ import annotations

import hashlib
import re
import secrets
import unicodedata
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import Device
from openctopus_server.devices.mcp_catalog import EMPTY_CATALOG_DIGEST
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import DeviceError
from openctopus_server.network_policy import DEFAULT_SSRF_DENYLIST

DEFAULT_ENV_ALLOWLIST = (
    "PATH",
    "HOME",
    "LANG",
    "TERM",
    "SystemRoot",
    "ComSpec",
    "PATHEXT",
    "TEMP",
    "TMP",
    "USERPROFILE",
)

_DEVICE_NAME = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
EMPTY_MCP_CATALOG = {
    "version": 1,
    "digest": EMPTY_CATALOG_DIGEST,
    "servers": [],
}


@dataclass(frozen=True)
class DeviceSnapshot:
    id: UUID
    user_id: UUID
    name: str
    token_hint: str
    workspace_path: str
    restrict_to_workspace: bool
    ssrf_denylist: list[str]
    created_at: datetime
    shell_timeout_max: int = 600
    env_allowlist: list[str] = field(default_factory=lambda: list(DEFAULT_ENV_ALLOWLIST))
    mcp_servers: list[dict[str, object]] = field(default_factory=list)
    mcp_catalog: dict[str, object] = field(default_factory=lambda: deepcopy(EMPTY_MCP_CATALOG))
    config_revision: int = 1


class DevicePatchCommitOutcomeUnknownError(Exception):
    """Wrap a policy commit whose durable outcome cannot be established."""

    def __init__(self, cause: BaseException, *, device_id: UUID) -> None:
        super().__init__("Device policy commit outcome is unknown")
        self.cause = cause
        self.device_id = device_id


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


async def owned_id(db: AsyncSession, *, user_id: UUID, name: str) -> UUID | None:
    device_id = await db.scalar(
        select(Device.id).where(Device.user_id == user_id, Device.name == name)
    )
    return device_id if isinstance(device_id, UUID) else None


async def create(
    db: AsyncSession,
    *,
    user_id: UUID,
    name: str,
    workspace_path: str,
    restrict_to_workspace: bool,
    ssrf_denylist: list[str] | None,
    shell_timeout_max: int = 600,
    env_allowlist: list[str] | None = None,
) -> tuple[DeviceSnapshot, str]:
    canonical_name = canonicalize_name(name)
    _validate_workspace_path(workspace_path)
    _validate_ssrf_denylist(ssrf_denylist)
    _validate_shell_timeout_max(shell_timeout_max)
    _validate_env_allowlist(env_allowlist)
    token = mint_token()
    device = Device(
        user_id=user_id,
        name=canonical_name,
        token_hash=token_digest(token),
        token_hint=token_hint(token),
        workspace_path=workspace_path,
        restrict_to_workspace=restrict_to_workspace,
        ssrf_denylist=_initial_ssrf_denylist(ssrf_denylist),
        shell_timeout_max=shell_timeout_max,
        env_allowlist=_initial_env_allowlist(env_allowlist),
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
    restrict_to_workspace: bool | None,
    ssrf_denylist: list[str] | None,
    shell_timeout_max: int | None = None,
    env_allowlist: list[str] | None = None,
) -> tuple[DeviceSnapshot, bool]:
    device = await _owned_for_update(db, user_id=user_id, name=name)
    changed = False
    if "name" in fields:
        if new_name is None:
            raise _invalid("Device name must be a string")
        canonical_name = canonicalize_name(new_name)
        if device.name != canonical_name:
            device.name = canonical_name
            changed = True
    if "workspace_path" in fields:
        if workspace_path is None:
            raise _invalid("Workspace path must be a string")
        _validate_workspace_path(workspace_path)
        if device.workspace_path != workspace_path:
            device.workspace_path = workspace_path
            changed = True
    if "restrict_to_workspace" in fields:
        if restrict_to_workspace is None:
            raise _invalid("Workspace restriction must be a boolean")
        if device.restrict_to_workspace != restrict_to_workspace:
            device.restrict_to_workspace = restrict_to_workspace
            changed = True
    if "ssrf_denylist" in fields:
        if ssrf_denylist is None:
            raise _invalid("SSRF denylist must be an array")
        _validate_ssrf_denylist(ssrf_denylist)
        candidate_denylist = list(ssrf_denylist)
        if device.ssrf_denylist != candidate_denylist:
            device.ssrf_denylist = candidate_denylist
            changed = True
    if "shell_timeout_max" in fields:
        if shell_timeout_max is None:
            raise _invalid("Shell timeout max must be an integer")
        _validate_shell_timeout_max(shell_timeout_max)
        if device.shell_timeout_max != shell_timeout_max:
            device.shell_timeout_max = shell_timeout_max
            changed = True
    if "env_allowlist" in fields:
        if env_allowlist is None:
            raise _invalid("Environment allowlist must be an array")
        _validate_env_allowlist(env_allowlist)
        candidate_allowlist = list(env_allowlist)
        if device.env_allowlist != candidate_allowlist:
            device.env_allowlist = candidate_allowlist
            changed = True
    if changed:
        device.config_revision += 1
    await _commit_patch_or_name_conflict(db, device_id=device.id)
    return _snapshot(device), changed


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


async def _commit_patch_or_name_conflict(db: AsyncSession, *, device_id: UUID) -> None:
    try:
        await db.commit()
    except IntegrityError as exc:
        await db.rollback()
        raise DeviceError(ErrorCode.DEVICE_NAME_TAKEN, "Device name is already in use") from exc
    except BaseException as exc:
        raise DevicePatchCommitOutcomeUnknownError(exc, device_id=device_id) from exc


def _initial_ssrf_denylist(supplied: list[str] | None) -> list[str]:
    if supplied is not None:
        return list(supplied)
    return list(DEFAULT_SSRF_DENYLIST)


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


def _validate_shell_timeout_max(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 86400:
        raise _invalid("Shell timeout max must be between 0 and 86400 seconds")


def _validate_env_allowlist(entries: list[str] | None) -> None:
    if entries is None:
        return
    if (
        len(entries) > 64
        or len(entries) != len(set(entries))
        or any(
            not entry
            or len(entry) > 128
            or entry.strip() != entry
            or "=" in entry
            or any(ord(char) < 0x20 for char in entry)
            or entry.upper().startswith("OPENOCTOPUS_")
            for entry in entries
        )
    ):
        raise _invalid("Environment allowlist contains an invalid variable name")


def _initial_env_allowlist(supplied: list[str] | None) -> list[str]:
    return list(DEFAULT_ENV_ALLOWLIST if supplied is None else supplied)


def _snapshot(device: Device) -> DeviceSnapshot:
    return DeviceSnapshot(
        id=device.id,
        user_id=device.user_id,
        name=device.name,
        token_hint=device.token_hint,
        workspace_path=device.workspace_path,
        restrict_to_workspace=device.restrict_to_workspace,
        ssrf_denylist=list(device.ssrf_denylist),
        shell_timeout_max=device.shell_timeout_max,
        env_allowlist=list(device.env_allowlist),
        mcp_servers=deepcopy(device.mcp_servers),
        mcp_catalog=deepcopy(device.mcp_catalog),
        config_revision=device.config_revision,
        created_at=device.created_at,
    )


def _invalid(message: str) -> DeviceError:
    return DeviceError(ErrorCode.DEVICE_INVALID_REQUEST, message)
