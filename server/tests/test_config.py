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
    "OPENOCTOPUS_ADMIN_TOKEN": "dev-admin-token",
    "OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT": "http://localhost:9000",
    "OPENOCTOPUS_OBJECT_STORAGE_BUCKET": "bucket",
    "OPENOCTOPUS_OBJECT_STORAGE_REGION": "us-east-1",
    "OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY": "key",
    "OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY": "secret",
    "OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS": "32",
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


@pytest.mark.parametrize("required_var", REQUIRED_ENV_VARS)
def test_settings_requires_every_documented_variable(monkeypatch, valid_env, required_var):
    monkeypatch.delenv(required_var)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_settings_rejects_empty_admin_token(monkeypatch, valid_env):
    monkeypatch.setenv("OPENOCTOPUS_ADMIN_TOKEN", "")

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", ["0", "257", "not-an-integer"])
def test_settings_rejects_invalid_object_storage_max_connections(monkeypatch, valid_env, value):
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS", value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", ["1", "256"])
def test_settings_accepts_object_storage_connection_boundaries(monkeypatch, valid_env, value):
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS", value)

    settings = Settings(_env_file=None)

    assert settings.object_storage_max_connections == int(value)
