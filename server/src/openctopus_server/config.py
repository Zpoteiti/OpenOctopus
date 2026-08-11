import os
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parent.parent.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="forbid",
        env_prefix="OPENOCTOPUS_",
    )

    # PostgreSQL (Py0) — all required, no defaults
    database_url: str
    database_pool_size: int
    database_max_overflow: int
    database_pool_timeout: int
    database_pool_pre_ping: bool

    # Server — required
    host: str
    port: int

    # Auth (Py1 — read, Py0 placeholder)
    jwt_secret: str
    cookie_secure: bool
    admin_token: Annotated[str, Field(min_length=1)]

    # Object Storage (Py4 — read, Py0 placeholder)
    object_storage_endpoint: str
    object_storage_bucket: str
    object_storage_region: Annotated[str, Field(min_length=1, pattern=r".*\S.*")]
    object_storage_access_key: str
    object_storage_secret_key: str
    object_storage_max_connections: Annotated[int, Field(ge=5, le=256)]

    # REST workspace transfer admission — all required
    rest_upload_max_concurrency: Annotated[int, Field(ge=2, le=256)]
    rest_download_max_concurrency: Annotated[int, Field(ge=2, le=255)]
    rest_transfer_max_concurrency_per_user: Annotated[int, Field(ge=1, le=255)]
    rest_transfer_queue_timeout_seconds: Annotated[float, Field(ge=0.1, le=300)]
    rest_transfer_idle_timeout_seconds: Annotated[float, Field(ge=1, le=600)]

    # Isolated content conversion — all required
    content_conversion_memory_mb: Annotated[int, Field(ge=256, le=4096)]
    content_conversion_timeout_seconds: Annotated[int, Field(ge=1, le=120)]
    content_conversion_max_concurrency: Annotated[int, Field(ge=1, le=32)]
    content_conversion_queue_timeout_seconds: Annotated[float, Field(ge=0.1, le=60)]

    # Web-fetch admission — all required
    web_fetch_max_concurrency: Annotated[int, Field(ge=2, le=256)]
    web_fetch_max_concurrency_per_user: Annotated[int, Field(ge=1, le=255)]
    web_fetch_queue_timeout_seconds: Annotated[float, Field(ge=0.1, le=30)]

    # Provider-context memory admission — all required
    chat_context_max_concurrency: Annotated[int, Field(ge=2, le=256)]
    chat_context_max_concurrency_per_user: Annotated[int, Field(ge=1, le=255)]
    chat_context_queue_timeout_seconds: Annotated[float, Field(ge=0.1, le=300)]

    # Device pending-call admission — all required
    device_pending_calls_max: Annotated[int, Field(ge=64, le=65536)]
    device_pending_calls_max_per_user: Annotated[int, Field(ge=1, le=1024)]
    device_pending_bytes_max: Annotated[int, Field(ge=16 * 1024 * 1024, le=1024 * 1024 * 1024)]
    device_pending_bytes_max_per_user: Annotated[
        int, Field(ge=1024 * 1024, le=256 * 1024 * 1024)
    ]

    # Device transfer admission — all required
    device_transfer_max_concurrency: Annotated[int, Field(ge=2, le=256)]
    device_transfer_max_concurrency_per_user: Annotated[int, Field(ge=1, le=32)]
    device_transfer_queue_timeout_seconds: Annotated[float, Field(ge=0.1, le=60)]
    device_transfer_idle_timeout_seconds: Annotated[float, Field(ge=1, le=600)]

    # Runtime workspace deletion — all required
    workspace_deletion_purge_timeout_seconds: Annotated[float, Field(ge=1, le=3600)]
    workspace_deletion_shutdown_grace_seconds: Annotated[float, Field(ge=0.1, le=60)]

    @model_validator(mode="after")
    def _validate_capacity_relations(self) -> "Settings":
        if (
            self.rest_upload_max_concurrency + self.rest_download_max_concurrency
            >= self.object_storage_max_connections
        ):
            raise ValueError(
                "REST upload and download concurrency must leave a storage connection reserve"
            )
        per_user = self.rest_transfer_max_concurrency_per_user
        if per_user >= self.rest_upload_max_concurrency or per_user >= (
            self.rest_download_max_concurrency
        ):
            raise ValueError("per-user REST concurrency must be below both direction limits")
        if self.web_fetch_max_concurrency_per_user >= self.web_fetch_max_concurrency:
            raise ValueError("per-user web concurrency must be below the global limit")
        if self.chat_context_max_concurrency_per_user >= self.chat_context_max_concurrency:
            raise ValueError("per-user context concurrency must be below the global limit")
        if self.device_pending_calls_max_per_user >= self.device_pending_calls_max:
            raise ValueError("per-user device pending calls must be below the global limit")
        if self.device_pending_bytes_max_per_user >= self.device_pending_bytes_max:
            raise ValueError("per-user device pending bytes must be below the global limit")
        if self.device_transfer_max_concurrency_per_user >= self.device_transfer_max_concurrency:
            raise ValueError("per-user device transfer concurrency must be below the global limit")
        return self

    @model_validator(mode="after")
    def _reject_unknown_prefixed_env_vars(self) -> "Settings":
        """Enforce ``extra='forbid'`` for environment variables too."""
        prefix = self.model_config.get("env_prefix", "")
        if not prefix:
            return self
        allowed = {f"{prefix}{name.upper()}" for name in self.__class__.model_fields}
        for key in os.environ:
            if key.startswith(prefix) and key not in allowed:
                raise ValueError(f"Extra environment variable not permitted: {key}")
        return self


@lru_cache
def get_settings() -> Settings:
    # Settings values are populated from environment variables / .env at runtime.
    return Settings()  # type: ignore[call-arg,unused-ignore]
