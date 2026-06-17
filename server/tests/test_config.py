import pytest
from pydantic import ValidationError

from openctopus_server.config import Settings, get_settings


REQUIRED_ENV_VARS = {
    "OPENOCTOPUS_DATABASE_URL": "postgresql+asyncpg://u:p@localhost/db",
    "OPENOCTOPUS_DATABASE_POOL_SIZE": "5",
    "OPENOCTOPUS_DATABASE_MAX_OVERFLOW": "10",
    "OPENOCTOPUS_DATABASE_POOL_TIMEOUT": "30",
    "OPENOCTOPUS_DATABASE_POOL_PRE_PING": "true",
    "OPENOCTOPUS_HOST": "127.0.0.1",
    "OPENOCTOPUS_PORT": "8080",
    "OPENOCTOPUS_JWT_SECRET": "secret",
    "OPENOCTOPUS_COOKIE_SECURE": "false",
    "OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT": "localhost:9000",
    "OPENOCTOPUS_OBJECT_STORAGE_BUCKET": "bucket",
    "OPENOCTOPUS_OBJECT_STORAGE_REGION": "us-east-1",
    "OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY": "key",
    "OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY": "secret",
}


@pytest.fixture
def valid_env(monkeypatch):
    for key, value in REQUIRED_ENV_VARS.items():
        monkeypatch.setenv(key, value)


def test_settings_rejects_typo_env_var(monkeypatch, valid_env):
    # Unknown/misspelled variable that should trigger extra="forbid"
    monkeypatch.setenv("OPENOCTOPUS_HTST", "127.0.0.1")
    with pytest.raises(ValidationError):
        get_settings()


def test_settings_loads_with_openoctopus_prefix(monkeypatch, valid_env):
    monkeypatch.setenv("OPENOCTOPUS_HOST", "0.0.0.0")
    monkeypatch.setenv("OPENOCTOPUS_PORT", "9000")

    direct = Settings()
    assert direct.host == "0.0.0.0"
    assert direct.port == 9000
    assert direct.database_url == REQUIRED_ENV_VARS["OPENOCTOPUS_DATABASE_URL"]

    cached = get_settings()
    assert cached.host == "0.0.0.0"
    assert cached.port == 9000
