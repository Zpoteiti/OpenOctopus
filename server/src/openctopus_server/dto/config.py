from __future__ import annotations

from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator


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


class ConfigPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quota_bytes: int | None = Field(default=None, ge=1)
    shared_workspace_quota_bytes: int | None = Field(default=None, ge=1)
    llm_endpoint: str | None = Field(default=None, min_length=1)
    llm_api_key: str | None = Field(default=None, min_length=1)
    llm_model: str | None = Field(default=None, min_length=1)
    llm_max_context_tokens: int | None = Field(default=None, ge=1)
    llm_compaction_threshold_tokens: int | None = Field(default=None, ge=4001)
    llm_max_concurrent_requests: int | None = Field(default=None, ge=0, le=1_000_000)
    llm_max_output_tokens: int | None = Field(default=None, ge=1, le=1_000_000)
    web_fetch_denylist: WebFetchDenylist | None = None

    @model_validator(mode="after")
    def _denylist_cannot_be_null(self) -> ConfigPatch:
        if "web_fetch_denylist" in self.model_fields_set and self.web_fetch_denylist is None:
            raise ValueError("web_fetch_denylist must be an array")
        return self


class AdminConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quota_bytes: int
    shared_workspace_quota_bytes: int
    llm_endpoint: str | None = None
    llm_api_key: str | None = None
    llm_model: str | None = None
    llm_max_context_tokens: int | None = None
    llm_compaction_threshold_tokens: int | None = None
    llm_max_concurrent_requests: int | None = None
    llm_max_output_tokens: int
    web_fetch_denylist: list[str]
