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
from openctopus_server.devices.mcp_models import SourceMcpCatalog
from openctopus_server.devices.protocol import DeviceConfigFrame
from openctopus_server.devices.registry import (
    ConfigValidation,
    DeviceBusyError,
    DeviceRegistry,
    DeviceSecretTransportError,
    DeviceUnavailableError,
    DeviceValidationError,
)
from openctopus_server.dto.device import (
    DeviceConfigIdentity,
    DeviceConfigResponse,
    DeviceCreateRequest,
    DevicePatchRequest,
    DeviceResponse,
    DeviceTokenResponse,
    McpDiscoveredCapability,
    McpDiscoveredServer,
    McpServerResponse,
    RemoteMcpServerResponse,
    StdioMcpServerResponse,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import DeviceError
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
        restrict_to_workspace=body.restrict_to_workspace,
        ssrf_denylist=body.ssrf_denylist,
        shell_timeout_max=body.shell_timeout_max,
        env_allowlist=body.env_allowlist,
    )
    return DeviceTokenResponse(token=token, device=await _response(snapshot, registry))


@router.get("/{name}/config", response_model=DeviceConfigResponse)
async def get_device_config(
    name: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> DeviceConfigResponse:
    snapshot = await devices.get_owned(db, user_id=user.id, name=name)
    await db.commit()
    return await _config_response(snapshot, registry)


@router.patch("/{name}/config", response_model=DeviceConfigResponse)
async def patch_device(
    name: str,
    body: DevicePatchRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> DeviceConfigResponse:
    patch = body
    user_id = user.id
    device_id = await devices.owned_id(db, user_id=user_id, name=name)
    if device_id is None:
        raise DeviceError(ErrorCode.DEVICE_NOT_FOUND, "Device not found")
    await db.rollback()
    async with _config_update_guard(registry, user_id, name, device_id=device_id):
        snapshot = await _build_validate_and_commit(
            db,
            registry,
            user_id=user_id,
            name=name,
            patch=patch,
        )
    return await _config_response(snapshot, registry)


async def _build_validate_and_commit(
    db: AsyncSession,
    registry: DeviceRegistry,
    *,
    user_id: UUID,
    name: str,
    patch: DevicePatchRequest,
) -> devices.DeviceSnapshot:
    current = await devices.get_owned(db, user_id=user_id, name=name)
    if current.config_revision != patch.base_config_revision:
        raise DeviceError(ErrorCode.DEVICE_CONFIG_CONFLICT, "Device config revision is stale")
    if "mcp_servers" in patch.model_fields_set and patch.mcp_servers is None:
        raise DeviceError(ErrorCode.DEVICE_INVALID_REQUEST, "MCP servers must be an array")

    current_mcp = devices.parse_stored_mcp_servers(current.mcp_servers)
    candidate_mcp = current_mcp
    if "mcp_servers" in patch.model_fields_set:
        assert patch.mcp_servers is not None
        candidate_mcp = devices.resolve_mcp_secret_markers(
            current_mcp,
            tuple(patch.mcp_servers),
        )
    validate_servers = devices.changed_mcp_servers(current_mcp, candidate_mcp)
    mcp_changed = "mcp_servers" in patch.model_fields_set and (
        devices.mcp_configs_storage(current_mcp) != devices.mcp_configs_storage(candidate_mcp)
    )
    if not mcp_changed and _non_mcp_noop(current, patch):
        await db.rollback()
        return current

    await db.rollback()
    validation: ConfigValidation | None = None
    if validate_servers:
        candidate_frame = _device_config_frame(current, patch, candidate_mcp)
        try:
            validation = await registry.validate_config(
                device_id=current.id,
                user_id=user_id,
                expected_device_name=current.name,
                base_config_revision=current.config_revision,
                candidate_config=candidate_frame,
                validate_servers=validate_servers,
            )
        except DeviceSecretTransportError as exc:
            raise DeviceError(
                ErrorCode.MCP_SECRET_TRANSPORT_INSECURE,
                "Secret-bearing MCP config requires a secure Device transport",
            ) from exc
        except DeviceUnavailableError as exc:
            raise DeviceError(ErrorCode.DEVICE_OFFLINE, "Device is offline") from exc
        except DeviceBusyError as exc:
            raise DeviceError(
                ErrorCode.DEVICE_CONFIG_CONFLICT,
                "Device configuration is already updating",
            ) from exc
        except DeviceValidationError as exc:
            failure = exc.failures[0]
            try:
                code = ErrorCode(failure.code)
            except ValueError:
                code = ErrorCode.CONFIG_VALIDATION_FAILED
            raise DeviceError(
                code,
                f"MCP validation failed for '{failure.name}'",
            ) from exc

    transition = asyncio.create_task(
        _commit_and_activate_candidate(
            db,
            registry,
            user_id=user_id,
            name=name,
            patch=patch,
            current=current,
            candidate_mcp=candidate_mcp,
            source_catalog=(
                validation.source_catalog
                if validation is not None
                else SourceMcpCatalog(version=1, servers=[])
            ),
            validation=validation,
        )
    )
    return await await_future_cancellation_safe(transition)


async def _commit_and_activate_candidate(
    db: AsyncSession,
    registry: DeviceRegistry,
    *,
    user_id: UUID,
    name: str,
    patch: DevicePatchRequest,
    current: devices.DeviceSnapshot,
    candidate_mcp: tuple[object, ...],
    source_catalog: SourceMcpCatalog,
    validation: ConfigValidation | None,
) -> devices.DeviceSnapshot:
    # The tuple is produced only by parse_mcp_server_configs; keeping the cast
    # local prevents REST DTO details from leaking into the persistence API.
    from typing import cast

    from openctopus_server.devices.mcp_models import McpServerConfig

    parsed_candidate = cast(tuple[McpServerConfig, ...], candidate_mcp)
    fence_installed = await registry.begin_config_update(
        device_id=current.id,
        user_id=user_id,
        expected_handle=(validation.handle if validation is not None else None),
    )
    if validation is not None and not fence_installed:
        await registry.discard_validated_config(validation)
        raise DeviceError(ErrorCode.DEVICE_CONFIG_CONFLICT, "Device connection was replaced")
    try:
        snapshot, changed = await devices.commit_config_candidate(
            db,
            user_id=user_id,
            name=name,
            base_config_revision=patch.base_config_revision,
            fields=set(patch.model_fields_set) - {"base_config_revision", "mcp_servers"},
            new_name=patch.name,
            workspace_path=patch.workspace_path,
            restrict_to_workspace=patch.restrict_to_workspace,
            ssrf_denylist=patch.ssrf_denylist,
            shell_timeout_max=patch.shell_timeout_max,
            env_allowlist=patch.env_allowlist,
            mcp_servers=(parsed_candidate if "mcp_servers" in patch.model_fields_set else None),
            source_catalog=source_catalog,
        )
    except devices.DevicePatchCommitOutcomeUnknownError as exc:
        try:
            await db.close()
        finally:
            await registry.retire_config_update(device_id=exc.device_id, user_id=user_id)
        raise exc.cause
    except BaseException:
        if fence_installed:
            await registry.abort_config_update(device_id=current.id, user_id=user_id)
        if validation is not None:
            await registry.discard_validated_config(validation)
        raise
    if not changed:
        if fence_installed:
            await registry.abort_config_update(device_id=current.id, user_id=user_id)
        if validation is not None:
            await registry.discard_validated_config(validation)
        return snapshot
    try:
        await db.close()
    finally:
        await registry.push_config(
            device_id=snapshot.id,
            user_id=snapshot.user_id,
            device_name=snapshot.name,
            config=_snapshot_config_frame(snapshot),
            config_revision=snapshot.config_revision,
            mcp_catalog=devices.parse_stored_mcp_catalog(snapshot.mcp_catalog),
            frame_id=(validation.id if validation is not None else None),
            expected_handle=(validation.handle if validation is not None else None),
        )
    return snapshot


def _non_mcp_noop(snapshot: devices.DeviceSnapshot, patch: DevicePatchRequest) -> bool:
    for field in (
        "workspace_path",
        "restrict_to_workspace",
        "ssrf_denylist",
        "shell_timeout_max",
        "env_allowlist",
    ):
        if field in patch.model_fields_set and getattr(patch, field) != getattr(snapshot, field):
            return False
    if "name" in patch.model_fields_set:
        if patch.name is None or devices.canonicalize_name(patch.name) != snapshot.name:
            return False
    return True


def _device_config_frame(
    current: devices.DeviceSnapshot,
    patch: DevicePatchRequest,
    mcp_servers: tuple[object, ...],
) -> DeviceConfigFrame:
    from typing import cast

    from openctopus_server.devices.mcp_models import McpServerConfig

    def selected(field: str) -> object:
        if field not in patch.model_fields_set:
            return getattr(current, field)
        value = getattr(patch, field)
        if value is None:
            raise DeviceError(ErrorCode.DEVICE_INVALID_REQUEST, f"{field} cannot be null")
        return value

    return DeviceConfigFrame(
        workspace_path=cast(str, selected("workspace_path")),
        restrict_to_workspace=cast(bool, selected("restrict_to_workspace")),
        ssrf_denylist=list(cast(list[str], selected("ssrf_denylist"))),
        shell_timeout_max=cast(int, selected("shell_timeout_max")),
        env_allowlist=list(cast(list[str], selected("env_allowlist"))),
        mcp_servers=list(cast(tuple[McpServerConfig, ...], mcp_servers)),
    )


def _snapshot_config_frame(snapshot: devices.DeviceSnapshot) -> DeviceConfigFrame:
    return DeviceConfigFrame(
        workspace_path=snapshot.workspace_path,
        restrict_to_workspace=snapshot.restrict_to_workspace,
        ssrf_denylist=snapshot.ssrf_denylist,
        shell_timeout_max=snapshot.shell_timeout_max,
        env_allowlist=snapshot.env_allowlist,
        mcp_servers=list(devices.parse_stored_mcp_servers(snapshot.mcp_servers)),
    )


async def _config_response(
    snapshot: devices.DeviceSnapshot,
    registry: DeviceRegistry,
) -> DeviceConfigResponse:
    configs = devices.parse_stored_mcp_servers(snapshot.mcp_servers)
    catalog = devices.parse_stored_mcp_catalog(snapshot.mcp_catalog)
    discovered = {
        config.name: McpDiscoveredServer(
            tools=[],
            resources=[],
            resource_templates=[],
            prompts=[],
        )
        for config in configs
    }
    surface_fields = {
        "tool": "tools",
        "resource": "resources",
        "resource_template": "resource_templates",
        "prompt": "prompts",
    }
    for server in catalog.servers:
        projection = discovered.setdefault(
            server.name,
            McpDiscoveredServer(
                tools=[],
                resources=[],
                resource_templates=[],
                prompts=[],
            ),
        )
        for entry in server.entries:
            target = getattr(projection, surface_fields[entry.surface])
            target.append(
                McpDiscoveredCapability(
                    raw_name=entry.raw_name,
                    final_name=entry.final_name,
                    enabled=entry.enabled,
                )
            )
    redacted: list[McpServerResponse] = []
    for config in configs:
        payload = config.storage_dict()
        if config.transport == "stdio":
            payload["env"] = {key: "<redacted>" for key in config.env}
            redacted.append(StdioMcpServerResponse.model_validate(payload))
        else:
            payload["headers"] = {key: "<redacted>" for key in config.headers}
            redacted.append(RemoteMcpServerResponse.model_validate(payload))
    return DeviceConfigResponse(
        device=DeviceConfigIdentity(
            name=snapshot.name,
            online=await registry.is_online(snapshot.id, user_id=snapshot.user_id),
            config_revision=snapshot.config_revision,
        ),
        mcp_servers=redacted,
        mcp_catalog_digest=catalog.digest,
        mcp_discovered=discovered,
    )


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
        restrict_to_workspace=snapshot.restrict_to_workspace,
        ssrf_denylist=snapshot.ssrf_denylist,
        shell_timeout_max=snapshot.shell_timeout_max,
        env_allowlist=snapshot.env_allowlist,
        config_revision=snapshot.config_revision,
        mcp_config_count=len(snapshot.mcp_servers),
        mcp_enabled_capability_count=_enabled_capability_count(snapshot.mcp_catalog),
        mcp_catalog_digest=str(snapshot.mcp_catalog["digest"]),
        online=await registry.is_online(snapshot.id, user_id=snapshot.user_id),
        created_at=snapshot.created_at,
    )


def _enabled_capability_count(catalog: dict[str, object]) -> int:
    servers = catalog.get("servers")
    if not isinstance(servers, list):
        return 0
    count = 0
    for server in servers:
        if not isinstance(server, dict):
            continue
        entries = server.get("entries")
        if not isinstance(entries, list):
            continue
        count += sum(isinstance(entry, dict) and entry.get("enabled") is True for entry in entries)
    return count


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
    *,
    device_id: UUID | None = None,
) -> AsyncIterator[None]:
    lock_factory = getattr(registry, "config_update_lock", None)
    if lock_factory is None:
        yield
        return
    async with lock_factory(
        user_id=user_id,
        device_name=device_name,
        device_id=device_id,
    ):
        yield
