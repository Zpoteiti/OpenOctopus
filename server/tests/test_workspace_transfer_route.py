from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError

from openctopus_server.api.workspace_files import transfer_workspace_file
from openctopus_server.config import get_settings
from openctopus_server.devices.transfer import TransferBusyError, TransferError
from openctopus_server.dto.workspace_file import TransferResponse
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.main import create_app
from openctopus_server.tools.file_transfer import (
    FileTransferOutcome,
    FileTransferRequest,
    FileTransferTool,
)

_TRANSFER_RESPONSE_ADAPTER = TypeAdapter(TransferResponse)


class _Workspace:
    async def transfer_server_to_server(self, db: object, **kwargs: object) -> object:
        del db, kwargs
        return SimpleNamespace(
            kind="file",
            files_transferred=1,
            bytes_transferred=12,
            sha256="a" * 64,
            warnings=("source_delete_failed",),
        )


@pytest.mark.asyncio
async def test_transfer_route_projects_the_shared_machine_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = FileTransferRequest.model_validate(
        {
            "openoctopus_src_device": "server",
            "src_path": "from.bin",
            "openoctopus_dst_device": "server",
            "dst_path": "to.bin",
            "mode": "move",
        }
    )

    transfer = AsyncMock(
        return_value=FileTransferOutcome(
            kind="directory",
            files_transferred=2,
            bytes_transferred=12,
            sha256="a" * 64,
            warnings=("source_delete_failed",),
        )
    )
    monkeypatch.setattr(FileTransferTool, "transfer", transfer)
    workspace_fs = object()

    response = await transfer_workspace_file(
        body=body,
        user=SimpleNamespace(id=uuid4()),
        engine=object(),  # type: ignore[arg-type]
        service=_Workspace(),
        workspace_fs=workspace_fs,  # type: ignore[arg-type]
        registry=None,  # type: ignore[arg-type]
        settings=get_settings(),
    )

    assert response.model_dump() == {
        "kind": "directory",
        "files_transferred": 2,
        "bytes_transferred": 12,
        "sha256": "a" * 64,
        "warnings": ["source_delete_failed"],
    }


@pytest.mark.asyncio
async def test_transfer_route_maps_busy_to_retryable_device_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        FileTransferTool,
        "transfer",
        AsyncMock(side_effect=WorkspaceError(ErrorCode.TOOL_DEVICE_BUSY, "busy")),
    )

    body = FileTransferRequest.model_validate(
        {
            "openoctopus_src_device": "server",
            "src_path": "from.bin",
            "openoctopus_dst_device": "server",
            "dst_path": "to.bin",
            "mode": "copy",
        }
    )

    with pytest.raises(WorkspaceError) as raised:
        await transfer_workspace_file(
            body=body,
            user=SimpleNamespace(id=uuid4()),
            engine=object(),  # type: ignore[arg-type]
            service=_Workspace(),
            workspace_fs=object(),  # type: ignore[arg-type]
            registry=None,  # type: ignore[arg-type]
            settings=get_settings(),
        )

    assert raised.value.code is ErrorCode.TOOL_DEVICE_BUSY


@pytest.mark.asyncio
async def test_transfer_route_busy_retry_after_uses_device_transfer_queue_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        FileTransferTool,
        "transfer",
        AsyncMock(side_effect=TransferBusyError),
    )

    body = FileTransferRequest.model_validate(
        {
            "openoctopus_src_device": "server",
            "src_path": "from.bin",
            "openoctopus_dst_device": "server",
            "dst_path": "to.bin",
            "mode": "copy",
        }
    )

    with pytest.raises(WorkspaceError) as raised:
        await transfer_workspace_file(
            body=body,
            user=SimpleNamespace(id=uuid4()),
            engine=object(),  # type: ignore[arg-type]
            service=_Workspace(),
            workspace_fs=object(),  # type: ignore[arg-type]
            registry=None,  # type: ignore[arg-type]
            settings=SimpleNamespace(
                rest_transfer_queue_timeout_seconds=1.1,
                device_transfer_queue_timeout_seconds=4.2,
            ),  # type: ignore[arg-type]
        )

    assert raised.value.code is ErrorCode.TOOL_DEVICE_BUSY
    assert raised.value.headers == {"Retry-After": "5"}


@pytest.mark.asyncio
async def test_transfer_route_preserves_directory_too_large_for_http_413(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        FileTransferTool,
        "transfer",
        AsyncMock(
            side_effect=TransferError(ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE.value)
        ),
    )
    body = FileTransferRequest(
        openoctopus_src_device="server",
        src_path="from",
        openoctopus_dst_device="server",
        dst_path="to",
        mode="copy",
    )

    with pytest.raises(WorkspaceError) as raised:
        await transfer_workspace_file(
            body=body,
            user=SimpleNamespace(id=uuid4()),
            engine=object(),  # type: ignore[arg-type]
            service=_Workspace(),
            workspace_fs=object(),  # type: ignore[arg-type]
            registry=None,  # type: ignore[arg-type]
            settings=get_settings(),
        )

    assert raised.value.code is ErrorCode.WORKSPACE_DIRECTORY_TOO_LARGE


@pytest.mark.parametrize(
    "payload",
    [
        {
            "openoctopus_src_device": "server",
            "src_path": "a",
            "openoctopus_dst_device": "server",
            "dst_path": "b",
        },
        {
            "openoctopus_src_device": "server",
            "src_path": "a",
            "openoctopus_dst_device": "server",
            "dst_path": "b",
            "mode": "copy",
            "extra": True,
        },
        {
            "openoctopus_src_device": "server",
            "src_path": "a" * 4097,
            "openoctopus_dst_device": "server",
            "dst_path": "b",
            "mode": "copy",
        },
        {
            "openoctopus_src_device": "server",
            "src_path": " \t",
            "openoctopus_dst_device": "server",
            "dst_path": "b",
            "mode": "copy",
        },
        {
            "openoctopus_src_device": "server",
            "src_path": "a",
            "openoctopus_dst_device": "server",
            "dst_path": " \t",
            "mode": "copy",
        },
    ],
)
def test_transfer_request_is_strict_and_bounded(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        FileTransferRequest.model_validate(payload)


def test_runtime_openapi_documents_transfer_result_and_strict_request() -> None:
    schema = create_app().openapi()
    operation = schema["paths"]["/api/workspace/transfer"]["post"]
    request_schema = schema["components"]["schemas"]["TransferRequest"]
    response_schema = schema["components"]["schemas"]["TransferResponse"]

    assert request_schema["additionalProperties"] is False
    assert request_schema["required"] == [
        "openoctopus_src_device",
        "src_path",
        "openoctopus_dst_device",
        "dst_path",
        "mode",
    ]
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/TransferResponse"
    }
    assert {"400", "408", "409", "413", "422", "429", "502", "503"} <= operation[
        "responses"
    ].keys()
    for status in ("400", "408", "409", "413", "422", "429", "502", "503"):
        assert operation["responses"][status]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/ErrorResponse"
        }
    assert operation["responses"]["429"]["headers"]["Retry-After"] == {
        "description": "Seconds to wait before retrying",
        "schema": {"type": "integer", "minimum": 1},
    }
    assert response_schema["discriminator"] == {
        "propertyName": "kind",
        "mapping": {
            "directory": "#/components/schemas/DirectoryTransferResponse",
            "file": "#/components/schemas/FileTransferResponse",
        },
    }
    assert response_schema["oneOf"] == [
        {"$ref": "#/components/schemas/FileTransferResponse"},
        {"$ref": "#/components/schemas/DirectoryTransferResponse"},
    ]
    file_schema = schema["components"]["schemas"]["FileTransferResponse"]
    directory_schema = schema["components"]["schemas"]["DirectoryTransferResponse"]
    assert file_schema["additionalProperties"] is False
    assert file_schema["properties"]["kind"]["const"] == "file"
    assert file_schema["properties"]["files_transferred"]["const"] == 1
    assert directory_schema["additionalProperties"] is False
    assert directory_schema["properties"]["kind"]["const"] == "directory"
    assert directory_schema["properties"]["files_transferred"]["minimum"] == 1
    assert directory_schema["properties"]["files_transferred"]["maximum"] == 10_000
    for variant in (file_schema, directory_schema):
        warnings = variant["properties"]["warnings"]
        assert warnings["uniqueItems"] is True
        assert warnings["items"] == {
            "enum": [
                "transfer_ack_failed",
                "source_delete_failed",
                "source_changed_after_copy",
                "source_cleanup_incomplete",
            ],
            "type": "string",
        }


def test_transfer_response_rejects_unknown_warning() -> None:
    with pytest.raises(ValidationError):
        _TRANSFER_RESPONSE_ADAPTER.validate_python(
            {
                "kind": "file",
                "files_transferred": 1,
                "bytes_transferred": 12,
                "sha256": "a" * 64,
                "warnings": ["unexpected_warning"],
            }
        )


@pytest.mark.parametrize(
    "warnings",
    [
        ["source_cleanup_incomplete", "source_changed_after_copy"],
        ["source_changed_after_copy", "source_changed_after_copy"],
    ],
)
def test_transfer_response_rejects_noncanonical_warnings(
    warnings: list[str],
) -> None:
    with pytest.raises(ValidationError):
        _TRANSFER_RESPONSE_ADAPTER.validate_python(
            {
                "kind": "directory",
                "files_transferred": 2,
                "bytes_transferred": 12,
                "sha256": "a" * 64,
                "warnings": warnings,
            }
        )


@pytest.mark.parametrize(
    ("kind", "files_transferred"),
    [("file", 0), ("file", 2), ("directory", 0), ("directory", 10_001)],
)
def test_transfer_response_enforces_kind_count(
    kind: str,
    files_transferred: int,
) -> None:
    with pytest.raises(ValidationError):
        _TRANSFER_RESPONSE_ADAPTER.validate_python(
            {
                "kind": kind,
                "files_transferred": files_transferred,
                "bytes_transferred": 12,
                "sha256": "a" * 64,
                "warnings": [],
            }
        )


async def test_transfer_route_requires_authentication(async_client) -> None:
    response = await async_client.post(
        "/api/workspace/transfer",
        json={
            "openoctopus_src_device": "server",
            "src_path": "from.bin",
            "openoctopus_dst_device": "server",
            "dst_path": "to.bin",
            "mode": "copy",
        },
    )

    assert response.status_code == 401
    assert response.json()["code"] == "auth_unauthorized"


async def test_transfer_route_hides_missing_distinct_clients(user_client) -> None:
    response = await user_client.post(
        "/api/workspace/transfer",
        json={
            "openoctopus_src_device": "laptop",
            "src_path": "from.bin",
            "openoctopus_dst_device": "desktop",
            "dst_path": "to.bin",
            "mode": "copy",
        },
    )

    assert response.status_code == 409
    assert response.json()["code"] == "tool_device_unreachable"


async def test_transfer_route_hides_missing_or_offline_devices(user_client) -> None:
    response = await user_client.post(
        "/api/workspace/transfer",
        json={
            "openoctopus_src_device": "server",
            "src_path": "from.bin",
            "openoctopus_dst_device": "missing-device",
            "dst_path": "to.bin",
            "mode": "copy",
        },
    )

    assert response.status_code == 409
    assert response.json()["code"] == "tool_device_unreachable"
