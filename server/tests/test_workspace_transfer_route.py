from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from openctopus_server.api.workspace_files import transfer_workspace_file
from openctopus_server.config import get_settings
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.main import create_app
from openctopus_server.tools.file_transfer import FileTransferRequest


class _Workspace:
    async def transfer_server_to_server(self, db: object, **kwargs: object) -> object:
        del db, kwargs
        return SimpleNamespace(
            bytes_transferred=12,
            sha256="a" * 64,
            warnings=("source_delete_failed",),
        )


@pytest.mark.asyncio
async def test_transfer_route_projects_the_shared_machine_outcome() -> None:
    body = FileTransferRequest.model_validate(
        {
            "openoctopus_src_device": "server",
            "src_path": "from.bin",
            "openoctopus_dst_device": "server",
            "dst_path": "to.bin",
            "mode": "move",
        }
    )

    response = await transfer_workspace_file(
        body=body,
        user=SimpleNamespace(id=uuid4()),
        engine=None,
        service=_Workspace(),
        registry=None,  # type: ignore[arg-type]
        settings=get_settings(),
    )

    assert response.model_dump() == {
        "bytes_transferred": 12,
        "sha256": "a" * 64,
        "warnings": ["source_delete_failed"],
    }


@pytest.mark.asyncio
async def test_transfer_route_maps_busy_to_retryable_device_error() -> None:
    class BusyWorkspace:
        async def transfer_server_to_server(self, db: object, **kwargs: object) -> object:
            del db, kwargs
            raise WorkspaceError(ErrorCode.TOOL_DEVICE_BUSY, "busy")

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
            engine=None,
            service=BusyWorkspace(),
            registry=None,  # type: ignore[arg-type]
            settings=get_settings(),
        )

    assert raised.value.code is ErrorCode.TOOL_DEVICE_BUSY


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
    assert response_schema["required"] == ["bytes_transferred", "sha256", "warnings"]
    assert response_schema["additionalProperties"] is False


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


async def test_transfer_route_rejects_different_clients_before_io(user_client) -> None:
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

    assert response.status_code == 400
    assert response.json()["code"] == "tool_invalid_args"


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
