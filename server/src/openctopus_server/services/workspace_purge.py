from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from multiprocessing.connection import Connection

import urllib3
from minio import Minio

from openctopus_server.config import Settings
from openctopus_server.workspace.fs import WorkspaceTarget
from openctopus_server.workspace.storage import _parse_endpoint

_PURGE_BATCH_SIZE = 1000


@dataclass(frozen=True, slots=True)
class WorkspacePurgeStorageConfig:
    endpoint: str
    secure: bool
    bucket: str
    region: str
    access_key: str
    secret_key: str

    @classmethod
    def from_settings(cls, settings: Settings) -> WorkspacePurgeStorageConfig:
        endpoint, secure = _parse_endpoint(settings.object_storage_endpoint)
        return cls(
            endpoint=endpoint,
            secure=secure,
            bucket=settings.object_storage_bucket,
            region=settings.object_storage_region,
            access_key=settings.object_storage_access_key,
            secret_key=settings.object_storage_secret_key,
        )


def purge_workspace_child(
    config: WorkspacePurgeStorageConfig,
    target: WorkspaceTarget,
    result: Connection,
) -> None:
    try:
        _purge_workspace(config, target)
    except BaseException:
        _send_result(result, False)
    else:
        _send_result(result, True)
    finally:
        result.close()


def _purge_workspace(
    config: WorkspacePurgeStorageConfig,
    target: WorkspaceTarget,
) -> None:
    retries = urllib3.Retry(
        total=2,
        connect=2,
        read=0,
        status=2,
        backoff_factor=0.2,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"DELETE", "GET"}),
    )
    http_client = urllib3.PoolManager(
        num_pools=1,
        maxsize=1,
        block=True,
        timeout=urllib3.Timeout(connect=5, read=30),
        retries=retries,
    )
    client = Minio(
        endpoint=config.endpoint,
        access_key=config.access_key,
        secret_key=config.secret_key,
        secure=config.secure,
        region=config.region,
        http_client=http_client,
    )
    prefix = _workspace_prefix(target)
    try:
        while True:
            items = tuple(
                islice(
                    client.list_objects(
                        config.bucket,
                        prefix=prefix,
                        recursive=True,
                    ),
                    _PURGE_BATCH_SIZE,
                )
            )
            if not items:
                return
            for item in items:
                object_name = getattr(item, "object_name", None)
                if not isinstance(object_name, str) or not object_name.startswith(prefix):
                    raise ValueError("object listing entry is outside the workspace prefix")
                client.remove_object(config.bucket, object_name)
    finally:
        http_client.clear()


def _workspace_prefix(target: WorkspaceTarget) -> str:
    collection = "users" if target.kind == "personal" else "workspaces"
    return f"{collection}/{target.id}/"


def _send_result(result: Connection, succeeded: bool) -> None:
    try:
        result.send(succeeded)
    except (BrokenPipeError, EOFError, OSError):
        pass
