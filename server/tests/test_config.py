import os

import pytest
from pydantic import ValidationError

from openctopus_server.config import Settings, get_settings


def test_settings_rejects_typo_env_var(monkeypatch):
    # Provide all required vars
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_SIZE", "5")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_MAX_OVERFLOW", "10")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_TIMEOUT", "30")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_PRE_PING", "true")
    monkeypatch.setenv("OPENOCTOPUS_HOST", "127.0.0.1")
    monkeypatch.setenv("OPENOCTOPUS_PORT", "8080")
    monkeypatch.setenv("OPENOCTOPUS_JWT_SECRET", "secret")
    monkeypatch.setenv("OPENOCTOPUS_COOKIE_SECURE", "false")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY", "key")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY", "secret")
    # Typo
    monkeypatch.setenv("OPENOCTOPUS_HTST", "127.0.0.1")
    with pytest.raises(ValidationError):
        get_settings()


def test_settings_loads_with_openoctopus_prefix(monkeypatch):
    monkeypatch.setenv("OPENOCTOPUS_HOST", "0.0.0.0")
    monkeypatch.setenv("OPENOCTOPUS_PORT", "9000")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_SIZE", "5")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_MAX_OVERFLOW", "10")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_TIMEOUT", "30")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_PRE_PING", "true")
    monkeypatch.setenv("OPENOCTOPUS_JWT_SECRET", "secret")
    monkeypatch.setenv("OPENOCTOPUS_COOKIE_SECURE", "false")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY", "key")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY", "secret")
    settings = Settings()
    assert settings.host == "0.0.0.0"
    assert settings.port == 9000
