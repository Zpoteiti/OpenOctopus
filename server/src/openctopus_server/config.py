import os
from functools import lru_cache
from pathlib import Path

from pydantic import model_validator
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

    # Object Storage (Py4 — read, Py0 placeholder)
    object_storage_endpoint: str
    object_storage_bucket: str
    object_storage_region: str
    object_storage_access_key: str
    object_storage_secret_key: str

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
    return Settings()  # type: ignore[call-arg]
