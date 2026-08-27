from typing import Any

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.advisory import (
    lock_personal_quota_write,
    lock_shared_quota_write,
)
from openctopus_server.db.models import SystemConfig
from openctopus_server.dto.config import AdminConfig, ConfigPatch
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError, ToolError
from openctopus_server.network_policy import (
    DEFAULT_SSRF_DENYLIST,
    SsrfPolicy,
    canonicalize_ssrf_denylist,
    compile_ssrf_policy,
)

_QUOTA_DEFAULT = 524288000  # 500 MiB
LLM_MAX_OUTPUT_TOKENS_DEFAULT = 16_384
DEFAULT_SOUL = "You are OpenOctopus, the user's personal AI partner."
_REDACTED = "<redacted>"
_TOKEN_LIMIT_KEYS = {
    "llm_max_context_tokens",
    "llm_compaction_threshold_tokens",
    "llm_max_output_tokens",
}

_CONFIG_KEYS = {
    "quota_bytes",
    "shared_workspace_quota_bytes",
    "llm_endpoint",
    "llm_api_key",
    "llm_model",
    "llm_max_context_tokens",
    "llm_compaction_threshold_tokens",
    "llm_max_concurrent_requests",
    "llm_max_output_tokens",
    "default_soul",
    "web_fetch_denylist",
}


async def _get_all_rows(db: AsyncSession) -> dict[str, Any]:
    result = await db.execute(select(SystemConfig))
    return {row.key: row.value for row in result.scalars().all()}


async def get_config_view(db: AsyncSession) -> AdminConfig:
    rows = await _get_all_rows(db)
    return AdminConfig(
        quota_bytes=rows.get("quota_bytes", _QUOTA_DEFAULT),
        shared_workspace_quota_bytes=rows.get("shared_workspace_quota_bytes", _QUOTA_DEFAULT),
        llm_endpoint=rows.get("llm_endpoint"),
        llm_api_key=_REDACTED if "llm_api_key" in rows else None,
        llm_model=rows.get("llm_model"),
        llm_max_context_tokens=rows.get("llm_max_context_tokens"),
        llm_compaction_threshold_tokens=rows.get("llm_compaction_threshold_tokens"),
        llm_max_concurrent_requests=rows.get("llm_max_concurrent_requests"),
        llm_max_output_tokens=rows.get("llm_max_output_tokens", LLM_MAX_OUTPUT_TOKENS_DEFAULT),
        default_soul=rows.get("default_soul", DEFAULT_SOUL),
        web_fetch_denylist=list(
            canonicalize_ssrf_denylist(
                rows.get("web_fetch_denylist", DEFAULT_SSRF_DENYLIST)
            )
        ),
    )


async def patch_config(db: AsyncSession, payload: ConfigPatch) -> AdminConfig:
    if any(getattr(payload, field) is None for field in payload.model_fields_set):
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Config values cannot be null",
        )

    data = payload.model_dump(exclude_unset=True)
    existing = await _get_all_rows(db)

    if data.get("llm_api_key") == _REDACTED:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Cannot set llm_api_key to the redaction marker",
        )

    if "web_fetch_denylist" in data:
        data["web_fetch_denylist"] = list(
            canonicalize_ssrf_denylist(data["web_fetch_denylist"])
        )

    # Validate LLM identity before opening the write transaction.
    llm_identity_keys = {"llm_endpoint", "llm_api_key", "llm_model"}
    if llm_identity_keys & data.keys():
        endpoint = data.get("llm_endpoint", existing.get("llm_endpoint"))
        api_key = data.get("llm_api_key", existing.get("llm_api_key"))
        model = data.get("llm_model", existing.get("llm_model"))
        if not (endpoint and api_key and model):
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                "First-time LLM setup requires llm_endpoint, llm_api_key, and llm_model together",
            )
        await validate_llm_identity(str(endpoint), str(api_key), str(model))

    if _TOKEN_LIMIT_KEYS & data.keys():
        await db.execute(
            text(
                "SELECT pg_advisory_xact_lock(hashtextextended('openoctopus:llm_token_limits', 0))"
            )
        )
        existing = await _get_all_rows(db)

    if "quota_bytes" in data:
        await lock_personal_quota_write(db)
        existing = await _get_all_rows(db)

    if "shared_workspace_quota_bytes" in data:
        await lock_shared_quota_write(db)
        existing = await _get_all_rows(db)

    max_output_tokens = int(
        data.get(
            "llm_max_output_tokens",
            existing.get("llm_max_output_tokens", LLM_MAX_OUTPUT_TOKENS_DEFAULT),
        )
    )
    raw_context_tokens = data.get("llm_max_context_tokens", existing.get("llm_max_context_tokens"))
    if raw_context_tokens is not None and max_output_tokens > int(raw_context_tokens):
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "llm_max_output_tokens must not exceed llm_max_context_tokens",
        )
    raw_compaction_threshold = data.get(
        "llm_compaction_threshold_tokens",
        existing.get("llm_compaction_threshold_tokens"),
    )
    if raw_compaction_threshold is not None and (
        raw_context_tokens is None or int(raw_compaction_threshold) >= int(raw_context_tokens)
    ):
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "llm_compaction_threshold_tokens must be less than llm_max_context_tokens",
        )

    # Upsert rows after validation succeeds.
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


async def load_web_fetch_policy(engine: AsyncEngine) -> SsrfPolicy:
    try:
        async with AsyncSession(engine, expire_on_commit=False) as db:
            value = await db.scalar(
                select(SystemConfig.value).where(SystemConfig.key == "web_fetch_denylist")
            )
        if value is None:
            return compile_ssrf_policy(None)
        if not isinstance(value, list) or any(not isinstance(entry, str) for entry in value):
            raise ValueError("stored web_fetch_denylist is invalid")
        return compile_ssrf_policy(value)
    except Exception as exc:
        raise ToolError(
            ErrorCode.TOOL_DB_ERROR,
            "web_fetch network policy is unavailable",
        ) from exc


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
            f"{endpoint.rstrip('/')}/v1/models",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
        )
        if response.status_code != 200:
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                f"LLM endpoint returned HTTP {response.status_code}",
            )
        if model not in response.text:
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                f"Model '{model}' not found in endpoint models response",
            )
    except httpx.HTTPError as exc:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            f"LLM endpoint unreachable: {exc}",
        ) from exc
    finally:
        if own_client:
            await client.aclose()
