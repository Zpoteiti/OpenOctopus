from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

import openctopus_server.tools.file_transfer as file_transfer_module
from openctopus_server.devices.protocol import TransferBeginFrame, new_uuid7
from openctopus_server.devices.transfer import TransferError
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.file_transfer import FileTransferRequest, FileTransferTool
from openctopus_server.tools.registry import ToolRegistry
from openctopus_server.workspace.fs import WorkspaceTarget
from openctopus_server.workspace.service import TransferPathTicket


def _ctx() -> ToolContext:
    return ToolContext(user_id=uuid4(), session_id=uuid4())


def test_file_transfer_schema_marks_both_device_fields() -> None:
    schema = ToolRegistry((FileTransferTool(None, None, None),)).get_tool_schemas()[0]

    assert schema["name"] == "file_transfer"
    properties = schema["input_schema"]["properties"]
    assert properties["openoctopus_src_device"]["enum"] == ["server"]
    assert properties["openoctopus_dst_device"]["enum"] == ["server"]
    assert properties["openoctopus_src_device"]["x-openoctopus-device"] is True
    assert properties["openoctopus_dst_device"]["x-openoctopus-device"] is True
    assert schema["input_schema"]["anyOf"] == [
        {"properties": {"openoctopus_src_device": {"const": "server"}}},
        {"properties": {"openoctopus_dst_device": {"const": "server"}}},
    ]
    assert schema["input_schema"]["required"] == [
        "openoctopus_src_device",
        "src_path",
        "openoctopus_dst_device",
        "dst_path",
    ]


def test_file_transfer_schema_adds_equal_paired_device_branches() -> None:
    schema = ToolRegistry((FileTransferTool(None, None, None),)).get_tool_schemas(
        device_names=["laptop", "phone"]
    )[0]["input_schema"]

    assert schema["properties"]["openoctopus_src_device"]["enum"] == [
        "server",
        "laptop",
        "phone",
    ]
    assert {
        "properties": {
            "openoctopus_src_device": {"const": "laptop"},
            "openoctopus_dst_device": {"const": "laptop"},
        }
    } in schema["anyOf"]
    assert {
        "properties": {
            "openoctopus_src_device": {"const": "phone"},
            "openoctopus_dst_device": {"const": "phone"},
        }
    } in schema["anyOf"]
@pytest.mark.asyncio
async def test_client_to_client_is_rejected_before_any_io() -> None:
    workspace = _NoIoWorkspace()
    registry = _NoIoRegistry()
    tool = FileTransferTool(None, workspace, registry)

    result = await tool.execute(
        {
            "openoctopus_src_device": "laptop",
            "src_path": "a.txt",
            "openoctopus_dst_device": "phone",
            "dst_path": "b.txt",
        },
        _ctx(),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_INVALID_ARGS
    assert "client-to-client" in str(result.content).lower()
    assert workspace.calls == []
    assert registry.calls == []


@pytest.mark.asyncio
async def test_same_client_dispatches_one_private_local_action_without_transfer_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    device_id = uuid4()
    registry = _SameClientRegistry()
    tool = FileTransferTool(object(), None, registry)  # type: ignore[arg-type]

    async def resolve(_: UUID, name: str) -> UUID:
        assert name == "laptop"
        return device_id

    monkeypatch.setattr(tool, "_device_id", resolve)
    result = await tool.execute(
        {
            "openoctopus_src_device": "laptop",
            "src_path": "a.txt",
            "openoctopus_dst_device": "laptop",
            "dst_path": "b.txt",
            "mode": "move",
        },
        _ctx(),
    )

    assert result.is_error is False
    assert "12 bytes" in str(result.content)
    assert registry.calls == [
        (
            device_id,
            "__workspace_rest__",
            {
                "operation": "transfer_local",
                "path": "a.txt",
                "dst_path": "b.txt",
                "mode": "move",
            },
            "laptop",
        )
    ]


@pytest.mark.asyncio
async def test_server_to_server_uses_workspace_transfer_without_device_registry() -> None:
    workspace = _Workspace()
    tool = FileTransferTool(None, workspace, None)

    result = await tool.execute(
        {
            "openoctopus_src_device": "server",
            "src_path": "a.txt",
            "openoctopus_dst_device": "server",
            "dst_path": "b.txt",
            "mode": "move",
        },
        _ctx(),
    )

    assert result.is_error is False
    assert "12 bytes" in str(result.content)
    assert workspace.calls == [("a.txt", "b.txt", "move")]


@pytest.mark.asyncio
async def test_client_to_server_move_uses_private_conditional_source_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = uuid4()
    device_id = uuid4()
    workspace = _DestinationWorkspace(user_id)
    registry = _ClientToServerRegistry()
    tool = FileTransferTool(object(), workspace, registry)  # type: ignore[arg-type]

    async def resolve(_: UUID, name: str) -> UUID:
        assert name == "laptop"
        return device_id

    monkeypatch.setattr(tool, "_device_id", resolve)

    class _Session:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *_: object) -> None:
            pass

    monkeypatch.setattr(file_transfer_module, "AsyncSession", _Session)
    outcome = await tool.transfer(
        FileTransferRequest(
            openoctopus_src_device="laptop",
            src_path="source.txt",
            openoctopus_dst_device="server",
            dst_path="destination.txt",
            mode="move",
        ),
        user_id=user_id,
    )

    assert outcome.warnings == ()
    assert registry.delete_call == (
        device_id,
        "__workspace_rest__",
        {
            "operation": "delete_file",
            "path": "source.txt",
            "if_match": "source-v1",
        },
    )


@pytest.mark.parametrize(
    "code",
    [
        ErrorCode.WORKSPACE_NOT_FOUND,
        ErrorCode.WORKSPACE_FILE_CHANGED,
        ErrorCode.TOOL_PATH_OUTSIDE_WORKSPACE,
    ],
)
@pytest.mark.asyncio
async def test_remote_transfer_preserves_stable_workspace_errors(
    monkeypatch: pytest.MonkeyPatch,
    code: ErrorCode,
) -> None:
    tool = FileTransferTool(None, None, None)
    monkeypatch.setattr(
        tool,
        "transfer",
        AsyncMock(side_effect=TransferError(code.value)),
    )

    result = await tool.execute(
        {
            "openoctopus_src_device": "laptop",
            "src_path": "a.txt",
            "openoctopus_dst_device": "server",
            "dst_path": "b.txt",
        },
        _ctx(),
    )

    assert result.is_error is True
    assert result.code is code


@pytest.mark.asyncio
async def test_remote_transfer_maps_unknown_error_to_storage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tool = FileTransferTool(None, None, None)
    monkeypatch.setattr(
        tool,
        "transfer",
        AsyncMock(side_effect=TransferError("client_internal_detail")),
    )

    result = await tool.execute(
        {
            "openoctopus_src_device": "laptop",
            "src_path": "a.txt",
            "openoctopus_dst_device": "server",
            "dst_path": "b.txt",
        },
        _ctx(),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.WORKSPACE_STORAGE_ERROR


@dataclass
class _Workspace:
    calls: list[tuple[str, str, str]] | None = None

    def __post_init__(self) -> None:
        self.calls = []

    async def transfer_server_to_server(
        self,
        db: object,
        *,
        user_id: UUID,
        src_path: str,
        dst_path: str,
        mode: str,
    ) -> Any:
        del db, user_id
        assert self.calls is not None
        self.calls.append((src_path, dst_path, mode))
        return _TransferResult(12, "a" * 64, ())


class _NoIoWorkspace:
    calls: list[object] = []


class _NoIoRegistry:
    calls: list[object] = []


class _SameClientRegistry:
    def __init__(self) -> None:
        self.calls: list[tuple[object, str, dict[str, object], str]] = []
        self.transfers = object()

    async def get_handle(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str | None = None,
    ) -> object:
        del user_id
        assert expected_device_name == "laptop"
        return SimpleNamespace(device_id=device_id, generation=1)

    async def dispatch_tool(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        expected_device_name: str | None = None,
    ) -> object:
        del user_id, max_result_bytes, timeout_seconds
        assert expected_device_name == "laptop"
        self.calls.append((device_id, name, args, expected_device_name))
        return SimpleNamespace(
            is_error=False,
            code=None,
            content='{"bytes_transferred":12,"sha256":"%s","warnings":[]}' % ("a" * 64),
        )


class _DestinationWorkspace:
    def __init__(self, user_id: UUID) -> None:
        self.ticket = TransferPathTicket(
            user_id=user_id,
            display_path="destination.txt",
            target=WorkspaceTarget.personal(user_id),
            relative_path="destination.txt",
            quota_bytes=100,
        )

    async def authorize_transfer_destination(
        self,
        db: object,
        *,
        user_id: UUID,
        path: str,
    ) -> TransferPathTicket:
        del db, user_id
        assert path == "destination.txt"
        return self.ticket

    async def begin_transfer_upload(self, ticket: TransferPathTicket, *, size: int) -> object:
        assert ticket is self.ticket
        assert size == 7
        return object()


class _ClientToServerRegistry:
    def __init__(self) -> None:
        self.transfers = self
        self.delete_call: tuple[UUID, str, dict[str, object]] | None = None

    async def get_handle(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str | None = None,
    ) -> object:
        del user_id
        assert expected_device_name == "laptop"
        return SimpleNamespace(device_id=device_id, generation=1)

    async def start_client_to_server(self, **kwargs: object) -> _TransferResult:
        begin = TransferBeginFrame(
            id=new_uuid7(),
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source.txt",
            dst_path="destination.txt",
            total_bytes=7,
            etag="source-v1",
        )
        sink_factory = kwargs["sink_factory"]
        assert callable(sink_factory)
        await sink_factory(begin)
        delete_source = kwargs["delete_source"]
        assert callable(delete_source)
        await delete_source()
        return _TransferResult(7, "a" * 64, ())

    async def dispatch_tool(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        expected_device_name: str | None = None,
    ) -> object:
        del user_id, max_result_bytes, timeout_seconds
        assert expected_device_name == "laptop"
        self.delete_call = (device_id, name, args)
        return SimpleNamespace(is_error=False, code=None)


@dataclass(frozen=True)
class _TransferResult:
    bytes_transferred: int
    sha256: str
    warnings: tuple[str, ...]
