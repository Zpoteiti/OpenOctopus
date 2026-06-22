from pydantic import BaseModel, ConfigDict, Field


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
