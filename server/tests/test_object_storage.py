from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import urllib3
from minio.error import ServerError

from openctopus_server.config import Settings
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.storage import (
    STARTUP_PROBE_KEY,
    ObjectStorage,
    build_object_storage,
)


class _Response:
    def __init__(self, data: bytes, *, etag: str | None = "response-etag"):
        self._data = data
        self.headers = {} if etag is None else {"ETag": f'"{etag}"'}
        self.closed = False
        self.released = False

    def read(self, size: int = -1) -> bytes:
        return self._data if size < 0 else self._data[:size]

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://u:p@localhost/db",
        "database_pool_size": 5,
        "database_max_overflow": 10,
        "database_pool_timeout": 30,
        "database_pool_pre_ping": True,
        "host": "127.0.0.1",
        "port": 8080,
        "jwt_secret": "secret",
        "cookie_secure": False,
        "admin_token": "admin",
        "object_storage_endpoint": "https://rustfs.internal:9000",
        "object_storage_bucket": "openoctopus",
        "object_storage_region": "us-east-1",
        "object_storage_access_key": "key",
        "object_storage_secret_key": "secret",
        "object_storage_max_connections": 17,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[arg-type]


def test_build_object_storage_uses_one_bounded_pool() -> None:
    pool = Mock()
    client = Mock()

    with (
        patch(
            "openctopus_server.workspace.storage.urllib3.PoolManager", return_value=pool
        ) as make_pool,
        patch("openctopus_server.workspace.storage.Minio", return_value=client) as make_client,
    ):
        storage = build_object_storage(_settings())

    pool_kwargs = make_pool.call_args.kwargs
    assert pool_kwargs["num_pools"] == 1
    assert pool_kwargs["maxsize"] == 17
    assert pool_kwargs["block"] is True
    assert pool_kwargs["timeout"].connect_timeout == 5
    assert pool_kwargs["timeout"].read_timeout == 30
    assert pool_kwargs["retries"].total == 2
    make_client.assert_called_once_with(
        endpoint="rustfs.internal:9000",
        access_key="key",
        secret_key="secret",
        secure=True,
        region="us-east-1",
        http_client=pool,
    )
    assert storage.client is client
    assert storage.max_connections == 17


@pytest.mark.parametrize(
    "endpoint",
    ["rustfs.internal:9000", "ftp://rustfs.internal", "https://rustfs.internal/path"],
)
def test_build_object_storage_rejects_invalid_endpoint(endpoint: str) -> None:
    with pytest.raises(ValueError, match="object storage endpoint"):
        build_object_storage(_settings(object_storage_endpoint=endpoint))


async def test_startup_probe_verifies_exact_bytes_and_cleans_up() -> None:
    client = Mock()
    response = _Response(b"probe-data")
    client.bucket_exists.return_value = True
    client.put_object.return_value = SimpleNamespace(etag="probe-etag")
    client.stat_object.return_value = SimpleNamespace(size=10, etag="probe-etag")
    client.get_object.return_value = response
    storage = ObjectStorage(client, "openoctopus", max_connections=2)

    with patch(
        "openctopus_server.workspace.storage.secrets.token_bytes", return_value=b"probe-data"
    ):
        await storage.probe_startup()

    client.put_object.assert_called_once()
    put_args = client.put_object.call_args.args
    assert put_args[:2] == ("openoctopus", STARTUP_PROBE_KEY)
    assert isinstance(put_args[2], BytesIO)
    assert put_args[2].getvalue() == b"probe-data"
    assert put_args[3] == 10
    client.stat_object.assert_called_once_with("openoctopus", STARTUP_PROBE_KEY)
    client.get_object.assert_called_once_with(
        "openoctopus",
        STARTUP_PROBE_KEY,
        offset=0,
        length=11,
    )
    client.remove_object.assert_called_once_with("openoctopus", STARTUP_PROBE_KEY)
    assert response.closed
    assert response.released


async def test_startup_probe_rejects_missing_bucket_without_creating_it() -> None:
    client = Mock()
    client.bucket_exists.return_value = False
    storage = ObjectStorage(client, "openoctopus", max_connections=2)

    with pytest.raises(WorkspaceError) as exc_info:
        await storage.probe_startup()

    assert exc_info.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    client.make_bucket.assert_not_called()
    client.put_object.assert_not_called()


async def test_startup_probe_attempts_cleanup_after_read_failure() -> None:
    client = Mock()
    client.bucket_exists.return_value = True
    client.put_object.return_value = SimpleNamespace(etag="probe-etag")
    client.stat_object.return_value = SimpleNamespace(size=32, etag="probe-etag")
    client.get_object.side_effect = OSError("connection lost")
    storage = ObjectStorage(client, "openoctopus", max_connections=2)

    with pytest.raises(WorkspaceError) as exc_info:
        await storage.probe_startup()

    assert exc_info.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    client.remove_object.assert_called_once_with("openoctopus", STARTUP_PROBE_KEY)


async def test_health_check_is_non_mutating() -> None:
    client = Mock()
    client.bucket_exists.return_value = True
    storage = ObjectStorage(client, "openoctopus", max_connections=2)

    await storage.check_health()

    client.bucket_exists.assert_called_once_with("openoctopus")
    client.put_object.assert_not_called()
    client.remove_object.assert_not_called()


@pytest.mark.parametrize(
    "failure",
    [
        urllib3.exceptions.MaxRetryError(None, "internal/key/secret.txt"),
        urllib3.exceptions.ReadTimeoutError(None, "request", "internal/key/secret.txt"),
        urllib3.exceptions.ProtocolError("internal/key/secret.txt"),
    ],
)
async def test_transport_failures_are_storage_unavailable(failure: Exception) -> None:
    storage = ObjectStorage(Mock(), "openoctopus", max_connections=1)

    with pytest.raises(WorkspaceError) as caught:
        await storage.execute(Mock(side_effect=failure))

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE
    assert "internal/key/secret.txt" not in str(caught.value)


@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
async def test_retry_exhausted_server_errors_are_storage_unavailable(
    status_code: int,
) -> None:
    storage = ObjectStorage(Mock(), "openoctopus", max_connections=1)

    with pytest.raises(WorkspaceError) as caught:
        await storage.execute(Mock(side_effect=ServerError("internal/key/secret.txt", status_code)))

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE


@pytest.mark.parametrize("operation", ["stat", "list_page", "read", "write"])
async def test_malformed_storage_metadata_is_normalized(operation: str) -> None:
    client = Mock()
    client.stat_object.return_value = SimpleNamespace()
    client.list_objects.return_value = [SimpleNamespace(object_name="key")]
    client.get_object.return_value = _Response(b"data", etag=None)
    client.put_object.return_value = SimpleNamespace()
    storage = ObjectStorage(client, "openoctopus", max_connections=1)

    with pytest.raises(WorkspaceError) as caught:
        if operation == "stat":
            await storage.stat("key")
        elif operation == "list_page":
            await storage.list_page("prefix")
        elif operation == "read":
            await storage.read("key", max_bytes=4)
        else:
            await storage.write("key", b"data")

    assert caught.value.code is ErrorCode.WORKSPACE_STORAGE_ERROR
