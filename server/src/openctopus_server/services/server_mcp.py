from __future__ import annotations

import contextlib
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.advisory import (
    lock_global_mcp_catalog_write,
    lock_server_mcp_config_write,
)
from openctopus_server.db.models import SystemConfig
from openctopus_server.devices.mcp_catalog import McpCatalogError
from openctopus_server.devices.mcp_models import SourceMcpCatalog
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.mcp.catalog import build_server_persisted_catalog
from openctopus_server.mcp.models import (
    ServerMcpEnvelope,
    ServerMcpServerConfig,
    empty_server_mcp_envelope,
    parse_server_mcp_envelope,
    server_mcp_envelope_storage,
)
from openctopus_server.mcp.routes import build_composite_mcp_snapshot

SERVER_MCP_CONFIG_KEY = "server_mcp"


class ServerMcpStorageCorruptionError(RuntimeError):
    """The authoritative JSONB envelope does not satisfy the persisted contract."""


class ServerMcpCommitOutcomeUnknownError(RuntimeError):
    """The database may have committed, but its acknowledgement was not received."""

    def __init__(self, cause: BaseException | None = None) -> None:
        super().__init__("Server MCP commit outcome is unknown")
        self.interruption = cause if cause is not None and not isinstance(cause, Exception) else None


class ServerMcpCommitFailedError(RuntimeError):
    """A sanitized failure raised after an ambiguous commit is resolved."""

    def __init__(self) -> None:
        super().__init__("Server MCP configuration commit failed")


def parse_stored_envelope(value: object) -> ServerMcpEnvelope:
    try:
        envelope = parse_server_mcp_envelope(value)
        build_composite_mcp_snapshot(envelope, [])
        return envelope
    except (McpCatalogError, ValueError) as exc:
        raise ServerMcpStorageCorruptionError("Stored Server MCP envelope is invalid") from exc


async def load_envelope(
    db: AsyncSession,
    *,
    for_update: bool = False,
) -> ServerMcpEnvelope:
    statement = select(SystemConfig).where(SystemConfig.key == SERVER_MCP_CONFIG_KEY)
    if for_update:
        statement = statement.with_for_update()
    row = await db.scalar(statement)
    if row is None:
        return empty_server_mcp_envelope()
    return parse_stored_envelope(row.value)


async def load_envelope_after_commit_boundary(db: AsyncSession) -> ServerMcpEnvelope:
    """Wait for any ambiguous Server MCP commit before reading its outcome."""

    await lock_global_mcp_catalog_write(db)
    await lock_server_mcp_config_write(db)
    return await load_envelope(db, for_update=True)


def config_storage(
    configs: Sequence[ServerMcpServerConfig],
) -> list[dict[str, object]]:
    return [config.storage_dict() for config in configs]


def changed_server_names(
    current: Sequence[ServerMcpServerConfig],
    candidate: Sequence[ServerMcpServerConfig],
) -> tuple[str, ...]:
    current_by_name = {config.name: config.storage_dict() for config in current}
    return tuple(
        config.name
        for config in candidate
        if current_by_name.get(config.name) != config.storage_dict()
    )


def transition_server_names(
    current: Sequence[ServerMcpServerConfig],
    candidate: Sequence[ServerMcpServerConfig],
) -> tuple[str, ...]:
    """Return every added, changed, or removed runtime name."""

    current_by_name = {config.name: config.storage_dict() for config in current}
    candidate_by_name = {config.name: config.storage_dict() for config in candidate}
    return tuple(
        name
        for name in sorted(set(current_by_name) | set(candidate_by_name))
        if current_by_name.get(name) != candidate_by_name.get(name)
    )


def build_candidate_envelope(
    current: ServerMcpEnvelope,
    candidate: Sequence[ServerMcpServerConfig],
    *,
    validate_servers: tuple[str, ...],
    source_catalog: SourceMcpCatalog,
) -> ServerMcpEnvelope:
    expected_sources = set(validate_servers)
    actual_sources = {server.name for server in source_catalog.servers}
    if actual_sources != expected_sources:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Server MCP discovery result does not match the validated candidate",
        )
    try:
        catalog = build_server_persisted_catalog(
            candidate,
            source_catalog,
            existing_catalog=current.mcp_catalog,
            entry_id_factory=new_uuid7,
        )
        envelope = ServerMcpEnvelope(
            version=1,
            config_revision=current.config_revision + 1,
            mcp_servers=list(candidate),
            mcp_catalog=catalog,
        )
        build_composite_mcp_snapshot(envelope, [])
        return envelope
    except McpCatalogError as exc:
        try:
            code = ErrorCode(exc.code)
        except ValueError:
            code = ErrorCode.CONFIG_VALIDATION_FAILED
        raise ConfigError(code, exc.message) from exc
    except ValueError as exc:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Server MCP candidate is invalid",
        ) from exc


async def commit_candidate(
    db: AsyncSession,
    *,
    base_config_revision: int,
    candidate: ServerMcpEnvelope,
) -> ServerMcpEnvelope:
    """Commit one prevalidated whole envelope under the final cross-process CAS."""
    await lock_global_mcp_catalog_write(db)
    await lock_server_mcp_config_write(db)
    row = await db.scalar(
        select(SystemConfig).where(SystemConfig.key == SERVER_MCP_CONFIG_KEY).with_for_update()
    )
    current = empty_server_mcp_envelope() if row is None else parse_stored_envelope(row.value)
    if current.config_revision != base_config_revision:
        await db.rollback()
        raise ConfigError(
            ErrorCode.SERVER_MCP_CONFIG_CONFLICT,
            "Server MCP config revision is stale",
        )
    if candidate.config_revision != base_config_revision + 1:
        await db.rollback()
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Server MCP candidate revision is invalid",
        )

    payload = server_mcp_envelope_storage(candidate)
    now = datetime.now(UTC)
    if row is None:
        db.add(SystemConfig(key=SERVER_MCP_CONFIG_KEY, value=payload, updated_at=now))
    else:
        row.value = payload
        row.updated_at = now
    commit_conflict = False
    commit_failed = False
    interruption: BaseException | None = None
    try:
        await db.commit()
    except IntegrityError:
        commit_conflict = True
    except Exception:
        commit_failed = True
    except BaseException as exc:
        commit_failed = True
        interruption = exc

    if commit_conflict:
        await db.rollback()
        raise ConfigError(
            ErrorCode.SERVER_MCP_CONFIG_CONFLICT,
            "Server MCP config revision is stale",
        ) from None
    if commit_failed:
        with contextlib.suppress(BaseException):
            await db.rollback()
        raise ServerMcpCommitOutcomeUnknownError(interruption) from None
    return candidate


async def fence_current_authority(
    db: AsyncSession,
    *,
    expected: ServerMcpEnvelope,
) -> None:
    """Hold the final Server authority fence for a no-write runtime refresh."""

    await lock_global_mcp_catalog_write(db)
    await lock_server_mcp_config_write(db)
    current = await load_envelope(db, for_update=True)
    if server_mcp_envelope_storage(current) != server_mcp_envelope_storage(expected):
        await db.rollback()
        raise ConfigError(
            ErrorCode.SERVER_MCP_CONFIG_CONFLICT,
            "Server MCP config revision is stale",
        )
