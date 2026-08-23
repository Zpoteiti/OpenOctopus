from __future__ import annotations

import asyncio
from typing import Never, Protocol, cast

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.auth.dependencies import require_admin
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.devices.mcp_models import SourceMcpCatalog
from openctopus_server.dto.server_mcp import (
    ServerMcpAdminResponse,
    ServerMcpConfigResponse,
    ServerMcpDiscoveredCapability,
    ServerMcpDiscoveredServer,
    ServerMcpPutRequest,
    ServerMcpRuntimeSlot,
    ServerRemoteMcpResponse,
    ServerStdioMcpResponse,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.mcp.authority import ServerMcpAuthorityFence
from openctopus_server.mcp.models import (
    ServerMcpEnvelope,
    ServerMcpServerConfig,
    redacted_server_mcp_configs,
    resolve_server_mcp_secret_markers,
    server_mcp_envelope_storage,
)
from openctopus_server.services import server_mcp

router = APIRouter(prefix="/api/admin/server-mcp", tags=["Admin"])
_MUTATION_GUARD = asyncio.Lock()
_COMMIT_RECOVERY_TIMEOUT_SECONDS = 30.0


class ServerMcpCandidateValidator(Protocol):
    async def preflight(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
    ) -> None: ...

    async def validate(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
        validate_servers: tuple[str, ...],
    ) -> ValidatedServerMcpCandidate: ...

    async def discard(self, candidate: ValidatedServerMcpCandidate) -> None: ...

    async def publish(
        self,
        candidate: ValidatedServerMcpCandidate,
        envelope: ServerMcpEnvelope,
    ) -> None: ...

    def refresh_names(self, envelope: ServerMcpEnvelope) -> tuple[str, ...]: ...

    def runtime_snapshot(
        self, envelope: ServerMcpEnvelope
    ) -> dict[str, ServerMcpRuntimeSlot]: ...

    async def reconcile(self, envelope: ServerMcpEnvelope) -> None: ...


class ValidatedServerMcpCandidate(Protocol):
    @property
    def source_catalog(self) -> SourceMcpCatalog: ...


class _UnavailableCandidateValidator:
    async def preflight(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
    ) -> None:
        del configs, changed_names

    async def validate(
        self,
        *,
        configs: tuple[ServerMcpServerConfig, ...],
        changed_names: tuple[str, ...],
        validate_servers: tuple[str, ...],
    ) -> ValidatedServerMcpCandidate:
        del configs, changed_names, validate_servers
        raise ConfigError(
            ErrorCode.MCP_SPAWN_FAILED,
            "Server MCP candidate validation is unavailable",
        )

    async def discard(self, candidate: ValidatedServerMcpCandidate) -> None:
        return None

    async def publish(
        self,
        candidate: ValidatedServerMcpCandidate,
        envelope: ServerMcpEnvelope,
    ) -> None:
        return None

    def refresh_names(self, envelope: ServerMcpEnvelope) -> tuple[str, ...]:
        del envelope
        return ()

    def runtime_snapshot(
        self, envelope: ServerMcpEnvelope
    ) -> dict[str, ServerMcpRuntimeSlot]:
        del envelope
        return {}

    async def reconcile(self, envelope: ServerMcpEnvelope) -> None:
        del envelope


_UNAVAILABLE_VALIDATOR = _UnavailableCandidateValidator()


def get_server_mcp_candidate_validator(request: Request) -> ServerMcpCandidateValidator:
    validator = getattr(request.app.state, "server_mcp_supervisor", None)
    return validator if validator is not None else _UNAVAILABLE_VALIDATOR


def get_server_mcp_authority(request: Request) -> ServerMcpAuthorityFence:
    return cast(ServerMcpAuthorityFence, request.app.state.server_mcp_authority)


@router.get("", response_model=ServerMcpAdminResponse)
async def get_server_mcp(
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
    validator: ServerMcpCandidateValidator = Depends(
        get_server_mcp_candidate_validator
    ),
) -> ServerMcpAdminResponse:
    envelope = await server_mcp.load_envelope(db)
    await db.rollback()
    return _response(envelope, validator)


@router.put("", response_model=ServerMcpAdminResponse)
async def put_server_mcp(
    body: ServerMcpPutRequest,
    user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
    validator: ServerMcpCandidateValidator = Depends(
        get_server_mcp_candidate_validator
    ),
    authority: ServerMcpAuthorityFence = Depends(get_server_mcp_authority),
    engine: AsyncEngine = Depends(get_engine),
) -> ServerMcpAdminResponse:
    async with _MUTATION_GUARD:
        current = await server_mcp.load_envelope(db)
        if current.config_revision != body.base_config_revision:
            await db.rollback()
            raise ConfigError(
                ErrorCode.SERVER_MCP_CONFIG_CONFLICT,
                "Server MCP config revision is stale",
            )
        try:
            candidate_configs = resolve_server_mcp_secret_markers(
                current.mcp_servers,
                body.mcp_servers,
            )
        except ValueError as exc:
            await db.rollback()
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                "Server MCP secret marker is invalid",
            ) from exc

        same_config = server_mcp.config_storage(
            current.mcp_servers
        ) == server_mcp.config_storage(candidate_configs)
        validate_servers = (
            validator.refresh_names(current)
            if same_config
            else server_mcp.changed_server_names(
                current.mcp_servers,
                candidate_configs,
            )
        )
        if same_config and not validate_servers:
            await db.rollback()
            repair = asyncio.create_task(
                _repair_exact_noop_authority(
                    db,
                    validator,
                    current=current,
                    authority=authority,
                )
            )
            await await_future_cancellation_safe(repair)
            return _response(current, validator)

        changed_names = (
            validate_servers
            if same_config
            else server_mcp.transition_server_names(
                current.mcp_servers,
                candidate_configs,
            )
        )
        await validator.preflight(
            configs=candidate_configs,
            changed_names=changed_names,
        )
        await db.rollback()
        validated = await validator.validate(
            configs=candidate_configs,
            changed_names=changed_names,
            validate_servers=validate_servers,
        )
        try:
            candidate = server_mcp.build_candidate_envelope(
                current,
                candidate_configs,
                validate_servers=validate_servers,
                source_catalog=validated.source_catalog,
            )
        except BaseException:
            cleanup = asyncio.create_task(validator.discard(validated))
            try:
                await await_future_cancellation_safe(cleanup)
            finally:
                raise
        if same_config and candidate.mcp_catalog.digest == current.mcp_catalog.digest:
            transition = asyncio.create_task(
                _publish_refresh(
                    db,
                    validator,
                    validated=validated,
                    current=current,
                    changed_names=changed_names,
                    authority=authority,
                )
            )
        else:
            transition = asyncio.create_task(
                _commit_and_publish(
                    db,
                    validator,
                    validated=validated,
                    base_config_revision=body.base_config_revision,
                    candidate=candidate,
                    changed_names=changed_names,
                    authority=authority,
                    engine=engine,
                )
            )
        committed = await await_future_cancellation_safe(transition)
        return _response(committed, validator)


async def _publish_refresh(
    db: AsyncSession,
    validator: ServerMcpCandidateValidator,
    *,
    validated: ValidatedServerMcpCandidate,
    current: ServerMcpEnvelope,
    changed_names: tuple[str, ...],
    authority: ServerMcpAuthorityFence,
) -> ServerMcpEnvelope:
    async with authority.transition():
        try:
            await db.rollback()
            await validator.preflight(
                configs=tuple(current.mcp_servers),
                changed_names=changed_names,
            )
            await server_mcp.fence_current_authority(db, expected=current)
            authority.publish(current)
            try:
                await validator.publish(validated, current)
            finally:
                await db.rollback()
        except BaseException:
            await db.rollback()
            await validator.discard(validated)
            await validator.reconcile(current)
            raise
    return current


async def _repair_exact_noop_authority(
    db: AsyncSession,
    validator: ServerMcpCandidateValidator,
    *,
    current: ServerMcpEnvelope,
    authority: ServerMcpAuthorityFence,
) -> None:
    async with authority.transition():
        try:
            await server_mcp.fence_current_authority(db, expected=current)
            await validator.reconcile(current)
            authority.publish(current)
        finally:
            await db.rollback()


async def _commit_and_publish(
    db: AsyncSession,
    validator: ServerMcpCandidateValidator,
    *,
    validated: ValidatedServerMcpCandidate,
    base_config_revision: int,
    candidate: ServerMcpEnvelope,
    changed_names: tuple[str, ...],
    authority: ServerMcpAuthorityFence,
    engine: AsyncEngine,
) -> ServerMcpEnvelope:
    async with authority.transition():
        try:
            await validator.preflight(
                configs=tuple(candidate.mcp_servers),
                changed_names=changed_names,
            )
            committed = await server_mcp.commit_candidate(
                db,
                base_config_revision=base_config_revision,
                candidate=candidate,
            )
        except server_mcp.ServerMcpCommitOutcomeUnknownError as exc:
            recovery = asyncio.create_task(
                _recover_unknown_commit(
                    engine,
                    validator,
                    validated=validated,
                    candidate=candidate,
                    authority=authority,
                    error=exc,
                )
            )
            return await await_future_cancellation_safe(recovery)
        except BaseException:
            cleanup = asyncio.create_task(validator.discard(validated))
            try:
                await await_future_cancellation_safe(cleanup)
            finally:
                raise
        await _publish_committed(
            validator,
            validated=validated,
            committed=committed,
            authority=authority,
        )
    return committed


async def _recover_unknown_commit(
    engine: AsyncEngine,
    validator: ServerMcpCandidateValidator,
    *,
    validated: ValidatedServerMcpCandidate,
    candidate: ServerMcpEnvelope,
    authority: ServerMcpAuthorityFence,
    error: server_mcp.ServerMcpCommitOutcomeUnknownError,
) -> ServerMcpEnvelope:
    recovery_failed = False
    recovery_interruption: BaseException | None = None
    try:
        async with AsyncSession(engine, expire_on_commit=False) as verify_db:
            async with asyncio.timeout(_COMMIT_RECOVERY_TIMEOUT_SECONDS):
                durable = await server_mcp.load_envelope_after_commit_boundary(
                    verify_db
                )
                await verify_db.rollback()
    except Exception:
        recovery_failed = True
    except BaseException as exc:
        recovery_failed = True
        recovery_interruption = exc

    if recovery_failed:
        authority.invalidate()
        try:
            await validator.discard(validated)
        finally:
            _raise_commit_failure(error, recovery_interruption)

    if server_mcp_envelope_storage(durable) == server_mcp_envelope_storage(candidate):
        await _publish_committed(
            validator,
            validated=validated,
            committed=durable,
            authority=authority,
        )
        if error.interruption is not None:
            raise error.interruption
        return durable

    authority.publish(durable)
    try:
        await validator.discard(validated)
        await validator.reconcile(durable)
    finally:
        _raise_commit_failure(error)


def _raise_commit_failure(
    error: server_mcp.ServerMcpCommitOutcomeUnknownError,
    recovery_interruption: BaseException | None = None,
) -> Never:
    if error.interruption is not None:
        raise error.interruption
    if recovery_interruption is not None:
        raise recovery_interruption
    raise server_mcp.ServerMcpCommitFailedError() from None


async def _publish_committed(
    validator: ServerMcpCandidateValidator,
    *,
    validated: ValidatedServerMcpCandidate,
    committed: ServerMcpEnvelope,
    authority: ServerMcpAuthorityFence,
) -> None:
    authority.publish(committed)
    try:
        await validator.publish(validated, committed)
    except BaseException:
        await validator.discard(validated)
        await validator.reconcile(committed)
        raise


def _response(
    envelope: ServerMcpEnvelope,
    validator: ServerMcpCandidateValidator,
) -> ServerMcpAdminResponse:
    discovered = {
        config.name: ServerMcpDiscoveredServer(
            tools=[],
            resources=[],
            resource_templates=[],
            prompts=[],
        )
        for config in envelope.mcp_servers
    }
    surface_fields = {
        "tool": "tools",
        "resource": "resources",
        "resource_template": "resource_templates",
        "prompt": "prompts",
    }
    for catalog_server in envelope.mcp_catalog.servers:
        projection = discovered.setdefault(
            catalog_server.name,
            ServerMcpDiscoveredServer(
                tools=[],
                resources=[],
                resource_templates=[],
                prompts=[],
            ),
        )
        for entry in catalog_server.entries:
            target = getattr(projection, surface_fields[entry.surface])
            target.append(
                ServerMcpDiscoveredCapability(
                    raw_name=entry.raw_name,
                    final_name=entry.final_name,
                    enabled=entry.enabled,
                )
            )
    redacted: list[ServerMcpConfigResponse] = []
    for payload in redacted_server_mcp_configs(envelope.mcp_servers):
        if payload["transport"] == "stdio":
            redacted.append(ServerStdioMcpResponse.model_validate(payload))
        else:
            redacted.append(ServerRemoteMcpResponse.model_validate(payload))
    return ServerMcpAdminResponse(
        config_revision=envelope.config_revision,
        mcp_servers=redacted,
        mcp_catalog_digest=envelope.mcp_catalog.digest,
        mcp_discovered=discovered,
        runtimes=validator.runtime_snapshot(envelope),
    )
