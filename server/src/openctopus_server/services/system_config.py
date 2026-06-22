import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import SystemConfig
from openctopus_server.dto.config import AdminConfig, ConfigPatch
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError

_QUOTA_DEFAULT = 524288000  # 500 MiB
_REDACTED = "<redacted>"

_CONFIG_KEYS = {
    "quota_bytes",
    "shared_workspace_quota_bytes",
    "llm_endpoint",
    "llm_api_key",
    "llm_model",
    "llm_max_context_tokens",
    "llm_compaction_threshold_tokens",
    "llm_max_concurrent_requests",
}


async def _get_all_rows(db: AsyncSession) -> dict[str, object]:
    result = await db.execute(select(SystemConfig))
    return {row.key: row.value for row in result.scalars().all()}


async def get_config_view(db: AsyncSession) -> AdminConfig:
    rows = await _get_all_rows(db)
    return AdminConfig(
        quota_bytes=rows.get("quota_bytes", _QUOTA_DEFAULT),  # type: ignore[arg-type]
        shared_workspace_quota_bytes=rows.get(
            "shared_workspace_quota_bytes", _QUOTA_DEFAULT
        ),  # type: ignore[arg-type]
        llm_endpoint=rows.get("llm_endpoint"),  # type: ignore[arg-type]
        llm_api_key=_REDACTED if "llm_api_key" in rows else None,
        llm_model=rows.get("llm_model"),  # type: ignore[arg-type]
        llm_max_context_tokens=rows.get("llm_max_context_tokens"),  # type: ignore[arg-type]
        llm_compaction_threshold_tokens=rows.get(
            "llm_compaction_threshold_tokens"
        ),  # type: ignore[arg-type]
        llm_max_concurrent_requests=rows.get(
            "llm_max_concurrent_requests"
        ),  # type: ignore[arg-type]
    )


async def patch_config(db: AsyncSession, payload: ConfigPatch) -> AdminConfig:
    data = payload.model_dump(exclude_none=True)

    if data.get("llm_api_key") == _REDACTED:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Cannot set llm_api_key to the redaction marker",
        )

    llm_identity_keys = {"llm_endpoint", "llm_api_key", "llm_model"}
    if llm_identity_keys & data.keys():
        existing = await _get_all_rows(db)
        endpoint = data.get("llm_endpoint", existing.get("llm_endpoint"))
        api_key = data.get("llm_api_key", existing.get("llm_api_key"))
        model = data.get("llm_model", existing.get("llm_model"))
        if not (endpoint and api_key and model):
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                "First-time LLM setup requires llm_endpoint, llm_api_key, and llm_model together",
            )
        await validate_llm_identity(str(endpoint), str(api_key), str(model))

    for key, value in data.items():
        if key not in _CONFIG_KEYS:
            continue
        result = await db.execute(select(SystemConfig).where(SystemConfig.key == key))
        row = result.scalar_one_or_none()
        if row is None:
            db.add(SystemConfig(key=key, value=value))
        else:
            row.value = value
    await db.commit()

    return await get_config_view(db)


async def validate_llm_identity(
    endpoint: str,
    api_key: str,
    model: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> None:
    own_client = client is None
    if client is None:
        client = httpx.AsyncClient(timeout=10.0)
    try:
        response = await client.get(
            f"{endpoint}/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        if response.status_code != 200:
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                f"LLM endpoint returned HTTP {response.status_code}",
            )
        body = response.json()
        model_ids = [m.get("id") for m in body.get("data", [])]
        if model not in model_ids:
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                f"Model '{model}' not found in endpoint models list",
            )
    except httpx.HTTPError as exc:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            f"LLM endpoint unreachable: {exc}",
        ) from exc
    finally:
        if own_client:
            await client.aclose()
