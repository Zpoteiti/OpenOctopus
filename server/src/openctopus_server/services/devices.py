from __future__ import annotations

import hashlib
import json
import re
import secrets
import unicodedata
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.advisory import lock_global_mcp_catalog_write
from openctopus_server.db.models import Device
from openctopus_server.devices.mcp_catalog import (
    EMPTY_CATALOG_DIGEST,
    McpCatalogError,
    build_persisted_catalog,
    merge_owner_catalogs,
)
from openctopus_server.devices.mcp_models import (
    McpServerConfig,
    PersistedMcpCatalog,
    RemoteMcpServerConfigBase,
    SourceMcpCatalog,
    StdioMcpServerConfig,
    parse_mcp_server_configs,
)
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import DeviceError
from openctopus_server.mcp.models import ServerMcpEnvelope
from openctopus_server.mcp.reservation import validate_device_mcp_candidate
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


async def get_owned(db: AsyncSession, *, user_id: UUID, name: str) -> DeviceSnapshot:
    device = await db.scalar(select(Device).where(Device.user_id == user_id, Device.name == name))
    if device is None:
        raise DeviceError(ErrorCode.DEVICE_NOT_FOUND, "Device not found")
    return _snapshot(device)


async def get_owned_by_id(
    db: AsyncSession,
    *,
    user_id: UUID,
    device_id: UUID,
) -> DeviceSnapshot:
    device = await db.scalar(
        select(Device).where(Device.user_id == user_id, Device.id == device_id)
    )
    if device is None:
        raise DeviceError(ErrorCode.DEVICE_NOT_FOUND, "Device not found")
    return _snapshot(device)


def parse_stored_mcp_servers(value: object) -> tuple[McpServerConfig, ...]:
    try:
        return parse_mcp_server_configs(value)
    except ValueError as exc:
        raise DeviceError(
            ErrorCode.CONFIG_VALIDATION_FAILED, "Stored MCP config is invalid"
        ) from exc


def parse_stored_mcp_catalog(value: object) -> PersistedMcpCatalog:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        catalog = PersistedMcpCatalog.model_validate_json(encoded, strict=True)
    except (TypeError, ValueError) as exc:
        raise DeviceError(
            ErrorCode.CONFIG_VALIDATION_FAILED, "Stored MCP catalog is invalid"
        ) from exc
    return catalog


def resolve_mcp_secret_markers(
    current: tuple[McpServerConfig, ...],
    candidate: tuple[McpServerConfig, ...],
) -> tuple[McpServerConfig, ...]:
    """Resolve REST redaction markers without allowing them to cross a secret sink."""
    current_by_name = {server.name: server for server in current}
    resolved: list[dict[str, object]] = []
    for server in candidate:
        payload = server.storage_dict()
        previous = current_by_name.get(server.name)
        if isinstance(server, StdioMcpServerConfig):
            marker_values = payload["env"]
            assert isinstance(marker_values, dict)
            old_values: dict[str, str] = {}
            same_sink = isinstance(previous, StdioMcpServerConfig) and _mcp_sink(
                previous
            ) == _mcp_sink(server)
            if isinstance(previous, StdioMcpServerConfig) and same_sink:
                old_values = {key: value.get_secret_value() for key, value in previous.env.items()}
            payload["env"] = _resolve_secret_map(marker_values, old_values, same_sink=same_sink)
        elif isinstance(server, RemoteMcpServerConfigBase):
            marker_values = payload["headers"]
            assert isinstance(marker_values, dict)
            old_values = {}
            same_sink = isinstance(previous, RemoteMcpServerConfigBase) and _mcp_sink(
                previous
            ) == _mcp_sink(server)
            if isinstance(previous, RemoteMcpServerConfigBase) and same_sink:
                old_values = {
                    key: value.get_secret_value() for key, value in previous.headers.items()
                }
            payload["headers"] = _resolve_secret_map(marker_values, old_values, same_sink=same_sink)
        resolved.append(payload)
    try:
        return parse_mcp_server_configs(resolved)
    except ValueError as exc:
        raise DeviceError(
            ErrorCode.DEVICE_INVALID_REQUEST, "MCP server configuration is invalid"
        ) from exc


def _resolve_secret_map(
    candidate: dict[str, object],
    current: dict[str, str],
    *,
    same_sink: bool,
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for key, value in candidate.items():
        if not isinstance(value, str):
            raise DeviceError(ErrorCode.DEVICE_INVALID_REQUEST, "MCP secret value is invalid")
        if value != "<redacted>":
            resolved[key] = value
            continue
        if not same_sink or key not in current:
            raise DeviceError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                "A redacted MCP secret can only retain the same key at the same sink",
            )
        resolved[key] = current[key]
    return resolved


def _mcp_sink(server: McpServerConfig) -> tuple[object, ...]:
    if isinstance(server, StdioMcpServerConfig):
        return (
            server.name,
            server.transport,
            server.command,
            tuple(server.args),
            server.cwd,
        )
    assert isinstance(server, RemoteMcpServerConfigBase)
    return (server.name, server.transport, server.url)


def changed_mcp_servers(
    current: tuple[McpServerConfig, ...],
    candidate: tuple[McpServerConfig, ...],
) -> tuple[str, ...]:
    current_by_name = {server.name: server.storage_dict() for server in current}
    return tuple(
        server.name
        for server in candidate
        if current_by_name.get(server.name) != server.storage_dict()
    )


def mcp_configs_storage(configs: tuple[McpServerConfig, ...]) -> list[dict[str, object]]:
    return [server.storage_dict() for server in configs]


def validate_mcp_candidate_against_server(
    *,
    current_configs: tuple[McpServerConfig, ...],
    candidate_configs: tuple[McpServerConfig, ...],
    candidate_catalog: PersistedMcpCatalog,
    server_envelope: ServerMcpEnvelope,
) -> None:
    reserved_names = {config.name for config in server_envelope.mcp_servers}
    enabled_final_names = {
        entry.final_name
        for server in server_envelope.mcp_catalog.servers
        for entry in server.entries
        if entry.enabled
    }
    try:
        validate_device_mcp_candidate(
            current_configs=current_configs,
            candidate_configs=candidate_configs,
            candidate_catalog=candidate_catalog,
            reserved_names=reserved_names,
            server_enabled_final_names=enabled_final_names,
        )
    except McpCatalogError as exc:
        raise _mcp_catalog_device_error(exc) from exc


def validate_mcp_candidate_reservations(
    *,
    current_configs: tuple[McpServerConfig, ...],
    candidate_configs: tuple[McpServerConfig, ...],
    server_envelope: ServerMcpEnvelope,
) -> None:
    """Reject reserved config mutations before remote Device discovery."""
    try:
        validate_device_mcp_candidate(
            current_configs=current_configs,
            candidate_configs=candidate_configs,
            candidate_catalog=PersistedMcpCatalog(
                version=1,
                digest=EMPTY_CATALOG_DIGEST,
                servers=[],
            ),
            reserved_names={config.name for config in server_envelope.mcp_servers},
            server_enabled_final_names=(),
        )
    except McpCatalogError as exc:
        raise _mcp_catalog_device_error(exc) from exc


async def commit_config_candidate(
    db: AsyncSession,
    *,
    user_id: UUID,
    device_id: UUID,
    base_config_revision: int,
    fields: set[str],
    new_name: str | None,
    workspace_path: str | None,
    restrict_to_workspace: bool | None,
    ssrf_denylist: list[str] | None,
    shell_timeout_max: int | None,
    env_allowlist: list[str] | None,
    mcp_servers: tuple[McpServerConfig, ...] | None,
    source_catalog: SourceMcpCatalog,
    built_in_names: tuple[str, ...] = (),
) -> tuple[DeviceSnapshot, bool]:
    """Commit a prevalidated full Device candidate in one short transaction."""
    if mcp_servers is not None or "name" in fields:
        await lock_global_mcp_catalog_write(db)
    await _lock_owner_mcp_catalogs(db, user_id)
    device = await _owned_by_id_for_update(db, user_id=user_id, device_id=device_id)
    if device.config_revision != base_config_revision:
        raise DeviceError(ErrorCode.DEVICE_CONFIG_CONFLICT, "Device config revision is stale")
    current_mcp_servers = parse_stored_mcp_servers(device.mcp_servers)
    server_envelope: ServerMcpEnvelope | None = None
    if mcp_servers is not None:
        from openctopus_server.services.server_mcp import load_envelope

        server_envelope = await load_envelope(db)

    changed = _apply_non_mcp_fields(
        device,
        fields=fields,
        new_name=new_name,
        workspace_path=workspace_path,
        restrict_to_workspace=restrict_to_workspace,
        ssrf_denylist=ssrf_denylist,
        shell_timeout_max=shell_timeout_max,
        env_allowlist=env_allowlist,
    )
    catalog_for_device: PersistedMcpCatalog | None = None
    if mcp_servers is not None:
        catalog_for_device = parse_stored_mcp_catalog(device.mcp_catalog)
        try:
            candidate_catalog = build_persisted_catalog(
                mcp_servers,
                source_catalog,
                existing_catalog=catalog_for_device,
                built_in_names=built_in_names,
                entry_id_factory=new_uuid7,
            )
        except McpCatalogError as exc:
            raise _mcp_catalog_device_error(exc) from exc
        stored = mcp_configs_storage(mcp_servers)
        catalog_payload = candidate_catalog.model_dump(mode="json")
        if device.mcp_servers != stored or device.mcp_catalog != catalog_payload:
            device.mcp_servers = stored
            device.mcp_catalog = catalog_payload
            changed = True
        catalog_for_device = candidate_catalog
        assert server_envelope is not None
        validate_mcp_candidate_against_server(
            current_configs=current_mcp_servers,
            candidate_configs=mcp_servers,
            candidate_catalog=candidate_catalog,
            server_envelope=server_envelope,
        )

    if mcp_servers is not None or "name" in fields:
        if catalog_for_device is None:
            catalog_for_device = parse_stored_mcp_catalog(device.mcp_catalog)
        with db.no_autoflush:
            owner_rows = list(
                (
                    await db.scalars(
                        select(Device).where(Device.user_id == user_id).order_by(Device.id)
                    )
                ).all()
            )
        owner_catalogs: dict[str, PersistedMcpCatalog] = {}
        try:
            for row in owner_rows:
                owner_catalogs[row.name] = (
                    catalog_for_device
                    if row.id == device.id
                    else parse_stored_mcp_catalog(row.mcp_catalog)
                )
            merge_owner_catalogs(owner_catalogs, built_in_names=built_in_names)
        except McpCatalogError as exc:
            raise _mcp_catalog_device_error(exc) from exc

    if not changed:
        snapshot = _snapshot(device)
        await db.rollback()
        return snapshot, False
    device.config_revision += 1
    await _commit_patch_or_name_conflict(db, device_id=device.id)
    return _snapshot(device), True


def _apply_non_mcp_fields(
    device: Device,
    *,
    fields: set[str],
    new_name: str | None,
    workspace_path: str | None,
    restrict_to_workspace: bool | None,
    ssrf_denylist: list[str] | None,
    shell_timeout_max: int | None,
    env_allowlist: list[str] | None,
) -> bool:
    changed = False
    if "name" in fields:
        if new_name is None:
            raise _invalid("Device name must be a string")
        candidate = canonicalize_name(new_name)
        if device.name != candidate:
            device.name = candidate
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
    return changed


async def _lock_owner_mcp_catalogs(db: AsyncSession, user_id: UUID) -> None:
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"openoctopus:mcp_owner:{user_id}"},
    )


def _mcp_catalog_device_error(exc: McpCatalogError) -> DeviceError:
    try:
        code = ErrorCode(exc.code)
    except ValueError:
        code = ErrorCode.CONFIG_VALIDATION_FAILED
    return DeviceError(code, exc.message)


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
    await _lock_owner_mcp_catalogs(db, user_id)
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
    await lock_global_mcp_catalog_write(db)
    await _lock_owner_mcp_catalogs(db, user_id)
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


async def _owned_by_id_for_update(
    db: AsyncSession,
    *,
    user_id: UUID,
    device_id: UUID,
) -> Device:
    device = await db.scalar(
        select(Device)
        .where(Device.user_id == user_id, Device.id == device_id)
        .with_for_update()
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
