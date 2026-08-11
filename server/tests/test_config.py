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
    "OPENOCTOPUS_REST_UPLOAD_MAX_CONCURRENCY": "8",
    "OPENOCTOPUS_REST_DOWNLOAD_MAX_CONCURRENCY": "16",
    "OPENOCTOPUS_REST_TRANSFER_MAX_CONCURRENCY_PER_USER": "2",
    "OPENOCTOPUS_REST_TRANSFER_QUEUE_TIMEOUT_SECONDS": "5",
    "OPENOCTOPUS_REST_TRANSFER_IDLE_TIMEOUT_SECONDS": "30",
    "OPENOCTOPUS_CONTENT_CONVERSION_MEMORY_MB": "1024",
    "OPENOCTOPUS_CONTENT_CONVERSION_TIMEOUT_SECONDS": "20",
    "OPENOCTOPUS_CONTENT_CONVERSION_MAX_CONCURRENCY": "2",
    "OPENOCTOPUS_CONTENT_CONVERSION_QUEUE_TIMEOUT_SECONDS": "5",
    "OPENOCTOPUS_WEB_FETCH_MAX_CONCURRENCY": "16",
    "OPENOCTOPUS_WEB_FETCH_MAX_CONCURRENCY_PER_USER": "2",
    "OPENOCTOPUS_WEB_FETCH_QUEUE_TIMEOUT_SECONDS": "5",
    "OPENOCTOPUS_CHAT_CONTEXT_MAX_CONCURRENCY": "32",
    "OPENOCTOPUS_CHAT_CONTEXT_MAX_CONCURRENCY_PER_USER": "2",
    "OPENOCTOPUS_CHAT_CONTEXT_QUEUE_TIMEOUT_SECONDS": "30",
    "OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX": "4096",
    "OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX_PER_USER": "64",
    "OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX": "268435456",
    "OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX_PER_USER": "33554432",
    "OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY": "32",
    "OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY_PER_USER": "2",
    "OPENOCTOPUS_DEVICE_TRANSFER_QUEUE_TIMEOUT_SECONDS": "5",
    "OPENOCTOPUS_DEVICE_TRANSFER_IDLE_TIMEOUT_SECONDS": "30",
    "OPENOCTOPUS_WORKSPACE_DELETION_PURGE_TIMEOUT_SECONDS": "300",
    "OPENOCTOPUS_WORKSPACE_DELETION_SHUTDOWN_GRACE_SECONDS": "5",
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


@pytest.mark.parametrize("value", ["", "   "])
def test_settings_rejects_blank_object_storage_region(monkeypatch, valid_env, value):
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_REGION", value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", ["4", "257", "not-an-integer"])
def test_settings_rejects_invalid_object_storage_max_connections(monkeypatch, valid_env, value):
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS", value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("value", ["25", "256"])
def test_settings_accepts_object_storage_connection_boundaries(monkeypatch, valid_env, value):
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS", value)

    settings = Settings(_env_file=None)

    assert settings.object_storage_max_connections == int(value)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("OPENOCTOPUS_REST_UPLOAD_MAX_CONCURRENCY", "1"),
        ("OPENOCTOPUS_REST_DOWNLOAD_MAX_CONCURRENCY", "1"),
        ("OPENOCTOPUS_REST_TRANSFER_QUEUE_TIMEOUT_SECONDS", "0"),
        ("OPENOCTOPUS_REST_TRANSFER_IDLE_TIMEOUT_SECONDS", "0"),
        ("OPENOCTOPUS_CONTENT_CONVERSION_MEMORY_MB", "255"),
        ("OPENOCTOPUS_CONTENT_CONVERSION_TIMEOUT_SECONDS", "1.5"),
        ("OPENOCTOPUS_CONTENT_CONVERSION_MAX_CONCURRENCY", "0"),
        ("OPENOCTOPUS_CONTENT_CONVERSION_QUEUE_TIMEOUT_SECONDS", "0"),
        ("OPENOCTOPUS_WEB_FETCH_MAX_CONCURRENCY", "1"),
        ("OPENOCTOPUS_WEB_FETCH_QUEUE_TIMEOUT_SECONDS", "0"),
        ("OPENOCTOPUS_CHAT_CONTEXT_MAX_CONCURRENCY", "1"),
        ("OPENOCTOPUS_CHAT_CONTEXT_QUEUE_TIMEOUT_SECONDS", "0"),
        ("OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX", "63"),
        ("OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX_PER_USER", "0"),
        ("OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX", "16777215"),
        ("OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX_PER_USER", "1048575"),
        ("OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY", "1"),
        ("OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY_PER_USER", "0"),
        ("OPENOCTOPUS_DEVICE_TRANSFER_QUEUE_TIMEOUT_SECONDS", "0"),
        ("OPENOCTOPUS_DEVICE_TRANSFER_IDLE_TIMEOUT_SECONDS", "0"),
        ("OPENOCTOPUS_WORKSPACE_DELETION_PURGE_TIMEOUT_SECONDS", "0"),
        ("OPENOCTOPUS_WORKSPACE_DELETION_SHUTDOWN_GRACE_SECONDS", "0"),
    ],
)
def test_settings_rejects_out_of_range_capacity_values(
    monkeypatch,
    valid_env,
    name,
    value,
):
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {
                "OPENOCTOPUS_REST_UPLOAD_MAX_CONCURRENCY": "16",
                "OPENOCTOPUS_REST_DOWNLOAD_MAX_CONCURRENCY": "16",
            },
            "storage connection",
        ),
        (
            {"OPENOCTOPUS_REST_TRANSFER_MAX_CONCURRENCY_PER_USER": "8"},
            "per-user REST",
        ),
        (
            {"OPENOCTOPUS_WEB_FETCH_MAX_CONCURRENCY_PER_USER": "16"},
            "per-user web",
        ),
        (
            {"OPENOCTOPUS_CHAT_CONTEXT_MAX_CONCURRENCY_PER_USER": "32"},
            "per-user context",
        ),
        (
            {
                "OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX": "64",
                "OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX_PER_USER": "64",
            },
            "per-user device pending calls",
        ),
        (
            {
                "OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX": "16777216",
                "OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX_PER_USER": "16777216",
            },
            "per-user device pending bytes",
        ),
        (
            {
                "OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY": "2",
                "OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY_PER_USER": "2",
            },
            "per-user device transfer concurrency",
        ),
    ],
)
def test_settings_rejects_invalid_capacity_relations(
    monkeypatch,
    valid_env,
    updates,
    message,
):
    for name, value in updates.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError, match=message):
        Settings(_env_file=None)
