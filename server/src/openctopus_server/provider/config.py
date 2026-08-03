from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import SystemConfig
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.services.system_config import LLM_MAX_OUTPUT_TOKENS_DEFAULT


@dataclass(frozen=True, slots=True)
class ProviderConfig:
    endpoint: str
    api_key: str
    model: str
    max_output_tokens: int
    max_concurrent_requests: int
    max_context_tokens: int | None
    compaction_threshold_tokens: int | None = None


async def load_provider_config(db: AsyncSession) -> ProviderConfig:
    result = await db.execute(
        select(SystemConfig).where(
            SystemConfig.key.in_(
                {
                    "llm_endpoint",
                    "llm_api_key",
                    "llm_model",
                    "llm_max_output_tokens",
                    "llm_max_concurrent_requests",
                    "llm_max_context_tokens",
                    "llm_compaction_threshold_tokens",
                }
            )
        )
    )
    rows: dict[str, Any] = {row.key: row.value for row in result.scalars().all()}
    endpoint = rows.get("llm_endpoint")
    api_key = rows.get("llm_api_key")
    model = rows.get("llm_model")
    if not (
        isinstance(endpoint, str)
        and endpoint
        and isinstance(api_key, str)
        and api_key
        and isinstance(model, str)
        and model
    ):
        raise ChatError(
            ErrorCode.PROVIDER_NOT_CONFIGURED,
            "Configure llm_endpoint, llm_api_key, and llm_model before sending messages",
        )

    max_output_tokens = rows.get("llm_max_output_tokens", LLM_MAX_OUTPUT_TOKENS_DEFAULT)
    max_concurrent_requests = rows.get("llm_max_concurrent_requests", 0)
    max_context_tokens = rows.get("llm_max_context_tokens")
    compaction_threshold_tokens = rows.get("llm_compaction_threshold_tokens")
    if not isinstance(max_output_tokens, int) or not 1 <= max_output_tokens <= 1_000_000:
        raise ChatError(
            ErrorCode.PROVIDER_NOT_CONFIGURED,
            "llm_max_output_tokens is invalid",
        )
    if not isinstance(max_concurrent_requests, int) or not (
        0 <= max_concurrent_requests <= 1_000_000
    ):
        raise ChatError(
            ErrorCode.PROVIDER_NOT_CONFIGURED,
            "llm_max_concurrent_requests is invalid",
        )
    if max_context_tokens is not None and (
        not isinstance(max_context_tokens, int)
        or max_context_tokens < 1
        or max_output_tokens > max_context_tokens
    ):
        raise ChatError(
            ErrorCode.PROVIDER_NOT_CONFIGURED,
            "LLM context/output token configuration is invalid",
        )
    if compaction_threshold_tokens is not None:
        if (
            not isinstance(compaction_threshold_tokens, int)
            or compaction_threshold_tokens < 4001
            or max_context_tokens is None
            or compaction_threshold_tokens >= max_context_tokens
        ):
            raise ChatError(
                ErrorCode.PROVIDER_NOT_CONFIGURED,
                "llm_compaction_threshold_tokens is invalid",
            )

    return ProviderConfig(
        endpoint=endpoint,
        api_key=api_key,
        model=model,
        max_output_tokens=max_output_tokens,
        max_concurrent_requests=max_concurrent_requests,
        max_context_tokens=max_context_tokens,
        compaction_threshold_tokens=compaction_threshold_tokens,
    )
