from __future__ import annotations

from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema


def _bounded_deny_entry(value: str) -> str:
    if "\x00" in value or len(value.encode("utf-8")) > 512:
        raise ValueError("denylist entry must be at most 512 UTF-8 bytes and contain no NUL")
    return value


WebFetchDenyEntry = Annotated[
    str,
    Field(min_length=1, pattern=r".*\S.*"),
    AfterValidator(_bounded_deny_entry),
]
WebFetchDenylist = Annotated[list[WebFetchDenyEntry], Field(max_length=256)]
PositiveInt = Annotated[int, Field(ge=1)]
NonEmptyString = Annotated[str, Field(min_length=1)]
CompactionThreshold = Annotated[int, Field(ge=4001)]
ConcurrencyLimit = Annotated[int, Field(ge=0, le=1_000_000)]
OutputTokenLimit = Annotated[int, Field(ge=1, le=1_000_000)]


class ConfigPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quota_bytes: PositiveInt | SkipJsonSchema[None] = Field(
        default=None,
        description="Per-user personal workspace quota in bytes.",
        examples=[524_288_000],
    )
    shared_workspace_quota_bytes: PositiveInt | SkipJsonSchema[None] = Field(
        default=None,
        description="Maximum quota for one shared workspace in bytes.",
        examples=[524_288_000],
    )
    llm_endpoint: NonEmptyString | SkipJsonSchema[None] = Field(
        default=None,
        description="Unversioned Anthropic-compatible API base URL; omit /v1.",
        examples=["https://api.siliconflow.cn"],
    )
    llm_api_key: NonEmptyString | SkipJsonSchema[None] = Field(
        default=None,
        description="New provider credential; omit to retain the configured key.",
        examples=["sk-your-provider-key"],
    )
    llm_model: NonEmptyString | SkipJsonSchema[None] = Field(
        default=None,
        description="Provider model identifier.",
        examples=["Qwen/Qwen3.5-4B"],
    )
    llm_max_context_tokens: PositiveInt | SkipJsonSchema[None] = Field(
        default=None,
        description="Provider context-window size.",
        examples=[131_072],
    )
    llm_compaction_threshold_tokens: CompactionThreshold | SkipJsonSchema[None] = Field(
        default=None,
        description="Remaining-token headroom that triggers history compaction.",
        examples=[16_000],
    )
    llm_max_concurrent_requests: ConcurrencyLimit | SkipJsonSchema[None] = Field(
        default=None,
        description="Maximum in-flight provider calls; zero means unlimited.",
        examples=[8],
    )
    llm_max_output_tokens: OutputTokenLimit | SkipJsonSchema[None] = Field(
        default=None,
        description="Provider output budget, including thinking tokens.",
        examples=[16_384],
    )
    web_fetch_denylist: WebFetchDenylist | SkipJsonSchema[None] = Field(
        default=None,
        description=(
            "Complete canonical Server web_fetch denylist; an empty list permits "
            "every otherwise-valid HTTP(S) target."
        ),
        examples=[["127.0.0.0/8", "169.254.169.254/32"]],
    )


class AdminConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quota_bytes: int = Field(
        description="Per-user personal workspace quota in bytes.",
        examples=[524_288_000],
    )
    shared_workspace_quota_bytes: int = Field(
        description="Maximum quota for one shared workspace in bytes.",
        examples=[524_288_000],
    )
    llm_endpoint: str | None = Field(
        description="Unversioned Anthropic-compatible API base URL; omit /v1.",
        examples=["https://api.siliconflow.cn"],
    )
    llm_api_key: str | None = Field(
        description='Null when unset; "<redacted>" when configured.',
        examples=["<redacted>"],
    )
    llm_model: str | None = Field(
        description="Provider model identifier.",
        examples=["Qwen/Qwen3.5-4B"],
    )
    llm_max_context_tokens: int | None = Field(
        description="Configured provider context-window size.",
        examples=[131_072],
    )
    llm_compaction_threshold_tokens: int | None = Field(
        description="Remaining-token headroom that triggers history compaction.",
        examples=[16_000],
    )
    llm_max_concurrent_requests: int | None = Field(
        description="Maximum in-flight provider calls; zero means unlimited.",
        examples=[8],
    )
    llm_max_output_tokens: int = Field(
        description="Provider output budget, including thinking tokens.",
        examples=[16_384],
    )
    web_fetch_denylist: list[str] = Field(
        description=(
            "Canonical Server web_fetch CIDR, IP, hostname, or host:port entries; "
            "an empty list permits every otherwise-valid HTTP(S) target."
        ),
        examples=[["127.0.0.0/8", "169.254.169.254/32"]],
    )
