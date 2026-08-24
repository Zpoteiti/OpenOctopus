from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from jsonschema import Draft202012Validator
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

import openctopus_server.tools.file_transfer as file_transfer_module
from openctopus_server.db.models import Device, User
from openctopus_server.devices.protocol import TransferBeginFrame, new_uuid7
from openctopus_server.devices.registry import (
    BridgeRoutePair,
    ConnectionHandle,
    DeviceRouteSnapshot,
)
from openctopus_server.devices.transfer import TransferError, TransferUnavailableError
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.file_transfer import (
    FileTransferOutcome,
    FileTransferRequest,
    FileTransferTool,
)
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
    assert "anyOf" not in schema["input_schema"]
    assert "x-openoctopus-same-device" not in schema["input_schema"]
    assert schema["input_schema"]["required"] == [
        "openoctopus_src_device",
        "src_path",
        "openoctopus_dst_device",
        "dst_path",
    ]


@pytest.mark.parametrize(
    ("source", "destination"),
    [
        ("server", "server"),
        ("server", "laptop"),
        ("laptop", "server"),
        ("laptop", "laptop"),
        ("laptop", "phone"),
    ],
)
def test_file_transfer_schema_accepts_every_install_site_combination(
    source: str,
    destination: str,
) -> None:
    schema = ToolRegistry((FileTransferTool(None, None, None),)).get_tool_schemas(
        device_names=["laptop", "phone"]
    )[0]["input_schema"]

    assert schema["properties"]["openoctopus_src_device"]["enum"] == [
        "server",
        "laptop",
        "phone",
    ]
    Draft202012Validator(schema).validate(
        {
            "openoctopus_src_device": source,
            "src_path": "source.txt",
            "openoctopus_dst_device": destination,
            "dst_path": "destination.txt",
        }
    )


@pytest.mark.asyncio
async def test_distinct_clients_resolve_once_then_dispatch_one_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = uuid4()
    source_id = uuid4()
    destination_id = uuid4()
    session = _BridgeLookupSession(
        [("laptop", source_id), ("phone", destination_id)]
    )
    registry = _DistinctClientRegistry(
        user_id=user_id,
        source_id=source_id,
        destination_id=destination_id,
        lookup_session=session,
    )
    workspace = _NoIoWorkspace()
    tool = FileTransferTool(object(), workspace, registry)  # type: ignore[arg-type]
    monkeypatch.setattr(file_transfer_module, "AsyncSession", lambda *_args, **_kwargs: session)

    result = await tool.execute(
        {
            "openoctopus_src_device": "laptop",
            "src_path": "a.txt",
            "openoctopus_dst_device": "phone",
            "dst_path": "b.txt",
        },
        ToolContext(
            user_id=user_id,
            session_id=uuid4(),
            device_targets={"laptop": source_id, "phone": destination_id},
        ),
    )

    assert result.is_error is False
    assert "12 bytes" in str(result.content)
    assert session.execute_calls == 1
    assert session.closed is True
    assert workspace.calls == []
    assert registry.bridge_calls == [
        {
            "source_route": registry.routes.source,
            "destination_route": registry.routes.destination,
            "user_id": user_id,
            "src_path": "a.txt",
            "dst_path": "b.txt",
            "mode": "copy",
            "delete_source": None,
        }
    ]


@pytest.mark.asyncio
async def test_distinct_client_move_uses_source_snapshot_conditional_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = uuid4()
    source_id = uuid4()
    destination_id = uuid4()
    session = _BridgeLookupSession(
        [("laptop", source_id), ("phone", destination_id)]
    )
    registry = _DistinctClientRegistry(
        user_id=user_id,
        source_id=source_id,
        destination_id=destination_id,
        lookup_session=session,
        source_fingerprint="source-v1",
    )
    tool = FileTransferTool(object(), None, registry)  # type: ignore[arg-type]
    monkeypatch.setattr(file_transfer_module, "AsyncSession", lambda *_args, **_kwargs: session)

    outcome = await tool.transfer(
        FileTransferRequest(
            openoctopus_src_device="laptop",
            src_path="source.txt",
            openoctopus_dst_device="phone",
            dst_path="destination.txt",
            mode="move",
        ),
        user_id=user_id,
    )

    assert outcome.bytes_transferred == 12
    assert registry.delete_call == {
        "route": registry.routes.source,
        "user_id": user_id,
        "expected_device_name": "laptop",
        "name": "__workspace_rest__",
        "args": {
            "operation": "delete_file",
            "path": "source.txt",
            "if_match": "source-v1",
        },
        "max_result_bytes": 16 * 1024,
        "timeout_seconds": 30.0,
    }


@pytest.mark.asyncio
async def test_distinct_client_turn_snapshot_name_reuse_fails_before_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = uuid4()
    captured_source_id = uuid4()
    captured_destination_id = uuid4()
    session = _BridgeLookupSession(
        [("laptop", uuid4()), ("phone", captured_destination_id)]
    )
    registry = _NoIoRegistry()
    tool = FileTransferTool(object(), None, registry)  # type: ignore[arg-type]
    monkeypatch.setattr(file_transfer_module, "AsyncSession", lambda *_args, **_kwargs: session)

    result = await tool.execute(
        {
            "openoctopus_src_device": "laptop",
            "src_path": "a.txt",
            "openoctopus_dst_device": "phone",
            "dst_path": "b.txt",
        },
        ToolContext(
            user_id=user_id,
            session_id=uuid4(),
            device_targets={
                "laptop": captured_source_id,
                "phone": captured_destination_id,
            },
        ),
    )

    assert result.code is ErrorCode.TOOL_DEVICE_UNREACHABLE
    assert session.execute_calls == 1
    assert session.closed is True
    assert registry.calls == []


@pytest.mark.asyncio
async def test_distinct_client_db_lookup_is_scoped_to_one_owner(
    pg_engine: AsyncEngine,
) -> None:
    owner = User(
        email=f"bridge-owner-{uuid4().hex}@example.com",
        password_hash="hash",
        name="Bridge Owner",
    )
    other = User(
        email=f"bridge-other-{uuid4().hex}@example.com",
        password_hash="hash",
        name="Bridge Other",
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all([owner, other])
        await db.flush()
        owner_source = Device(
            user_id=owner.id,
            name="laptop",
            token_hash=b"a" * 32,
            token_hint="owner-source",
        )
        owner_destination = Device(
            user_id=owner.id,
            name="phone",
            token_hash=b"b" * 32,
            token_hint="owner-destination",
        )
        other_source = Device(
            user_id=other.id,
            name="laptop",
            token_hash=b"c" * 32,
            token_hint="other-source",
        )
        other_destination = Device(
            user_id=other.id,
            name="phone",
            token_hash=b"d" * 32,
            token_hint="other-destination",
        )
        db.add_all(
            [
                owner_source,
                owner_destination,
                other_source,
                other_destination,
            ]
        )
        await db.commit()

    tool = FileTransferTool(pg_engine, None, _NoIoRegistry())
    assert await tool._bridge_device_ids_for_call(
        owner.id,
        "laptop",
        "phone",
        None,
    ) == (owner_source.id, owner_destination.id)
    with pytest.raises(file_transfer_module.DeviceUnavailableError):
        await tool._bridge_device_ids_for_call(
            owner.id,
            "laptop",
            "phone",
            {"laptop": other_source.id, "phone": other_destination.id},
        )


@pytest.mark.asyncio
async def test_same_client_dispatches_one_private_local_action_without_transfer_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    device_id = uuid4()
    registry = _SameClientRegistry()
    tool = FileTransferTool(object(), None, registry)  # type: ignore[arg-type]

    async def resolve(_: UUID, name: str, _expected: UUID | None = None) -> UUID:
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

    async def resolve(_: UUID, name: str, _expected: UUID | None = None) -> UUID:
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
    assert registry.route is not None
    assert registry.route.handle.device_id == device_id
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
        ErrorCode.TOOL_DEVICE_BUSY,
        ErrorCode.TOOL_DEVICE_UNREACHABLE,
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


@pytest.mark.asyncio
async def test_preissue_transfer_fence_maps_to_device_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tool = FileTransferTool(None, None, None)
    monkeypatch.setattr(
        tool,
        "transfer",
        AsyncMock(side_effect=TransferUnavailableError("route was fenced before send")),
    )

    result = await tool.execute(
        {
            "openoctopus_src_device": "server",
            "src_path": "a.txt",
            "openoctopus_dst_device": "laptop",
            "dst_path": "b.txt",
        },
        _ctx(),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_DEVICE_UNREACHABLE


@pytest.mark.asyncio
async def test_registry_leaves_file_transfer_unissued_during_tool_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tool = FileTransferTool(None, None, None)
    entered = asyncio.Event()
    issued = asyncio.Event()
    mark_issued = issued.set

    async def preflight(
        *_args: object,
        on_issued: Callable[[], None] | None = None,
        **_kwargs: object,
    ) -> FileTransferOutcome:
        assert on_issued is not None
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(tool, "transfer", preflight)
    task = asyncio.create_task(
        ToolRegistry((tool,)).execute(
            name="file_transfer",
            args={
                "openoctopus_src_device": "server",
                "src_path": "a.txt",
                "openoctopus_dst_device": "server",
                "dst_path": "b.txt",
            },
            ctx=_ctx(),
            on_issued=mark_issued,
        )
    )
    await entered.wait()
    assert issued.is_set() is False
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_native_transfer_timeout_maps_to_stable_timeout_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tool = FileTransferTool(None, None, None)
    monkeypatch.setattr(tool, "transfer", AsyncMock(side_effect=TimeoutError))

    result = await tool.execute(
        {
            "openoctopus_src_device": "server",
            "src_path": "a.txt",
            "openoctopus_dst_device": "server",
            "dst_path": "b.txt",
        },
        _ctx(),
    )

    assert result.is_error is True
    assert result.code is ErrorCode.WORKSPACE_TRANSFER_TIMEOUT


@pytest.mark.asyncio
async def test_intrinsic_device_transfer_rejects_a_reused_provider_turn_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_device_id = uuid4()
    registry = _NoIoRegistry()
    tool = FileTransferTool(object(), _NoIoWorkspace(), registry)  # type: ignore[arg-type]

    async def resolve(
        _user_id: UUID,
        name: str,
        expected_device_id: UUID | None,
    ) -> UUID:
        assert name == "laptop"
        assert expected_device_id == original_device_id
        raise file_transfer_module.DeviceUnavailableError("captured device was replaced")

    monkeypatch.setattr(tool, "_device_id", resolve)
    ctx = ToolContext(
        user_id=uuid4(),
        session_id=uuid4(),
        device_targets={"laptop": original_device_id},
    )

    result = await tool.execute(
        {
            "openoctopus_src_device": "laptop",
            "src_path": "a.txt",
            "openoctopus_dst_device": "server",
            "dst_path": "b.txt",
        },
        ctx,
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_DEVICE_UNREACHABLE
    assert registry.calls == []


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
        on_issued: Callable[[], None] | None = None,
    ) -> Any:
        del db, user_id
        if on_issued is not None:
            on_issued()
        assert self.calls is not None
        self.calls.append((src_path, dst_path, mode))
        return _TransferResult(12, "a" * 64, ())


class _NoIoWorkspace:
    calls: list[object] = []


class _NoIoRegistry:
    calls: list[object] = []


class _BridgeLookupSession:
    def __init__(self, rows: list[tuple[str, UUID]]) -> None:
        self._rows = rows
        self.execute_calls = 0
        self.closed = False

    async def __aenter__(self) -> _BridgeLookupSession:
        return self

    async def __aexit__(self, *_args: object) -> None:
        self.closed = True

    async def execute(self, _statement: object) -> object:
        self.execute_calls += 1
        return SimpleNamespace(all=lambda: list(self._rows))


class _DistinctClientRegistry:
    def __init__(
        self,
        *,
        user_id: UUID,
        source_id: UUID,
        destination_id: UUID,
        lookup_session: _BridgeLookupSession,
        source_fingerprint: str | None = None,
    ) -> None:
        self.user_id = user_id
        self.lookup_session = lookup_session
        self.routes = BridgeRoutePair(
            source=DeviceRouteSnapshot(
                ConnectionHandle(source_id, 1),
                2,
                "laptop",
            ),
            destination=DeviceRouteSnapshot(
                ConnectionHandle(destination_id, 3),
                4,
                "phone",
            ),
        )
        self.source_fingerprint = source_fingerprint
        self.bridge_calls: list[dict[str, object]] = []
        self.delete_call: dict[str, object] | None = None
        self.transfers = self

    async def get_bridge_route_pair(
        self,
        *,
        user_id: UUID,
        source_device_id: UUID,
        source_device_name: str,
        destination_device_id: UUID,
        destination_device_name: str,
    ) -> BridgeRoutePair:
        assert self.lookup_session.closed is True
        assert user_id == self.user_id
        assert source_device_id == self.routes.source.handle.device_id
        assert source_device_name == "laptop"
        assert destination_device_id == self.routes.destination.handle.device_id
        assert destination_device_name == "phone"
        return self.routes

    async def start_client_to_client(self, **kwargs: object) -> _TransferResult:
        call = dict(kwargs)
        on_issued = call.pop("on_issued")
        if callable(on_issued):
            on_issued()
        delete_source = call.get("delete_source")
        if self.source_fingerprint is not None:
            assert callable(delete_source)
            await delete_source(self.source_fingerprint)
        self.bridge_calls.append(call)
        return _TransferResult(12, "a" * 64, ())

    async def dispatch_tool_on_snapshot(self, **kwargs: object) -> object:
        self.delete_call = dict(kwargs)
        return SimpleNamespace(is_error=False, code=None)


class _SameClientRegistry:
    def __init__(self) -> None:
        self.calls: list[tuple[object, str, dict[str, object], str]] = []
        self.transfers = object()

    async def get_route_snapshot(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str | None = None,
    ) -> DeviceRouteSnapshot:
        del user_id
        assert expected_device_name == "laptop"
        return DeviceRouteSnapshot(ConnectionHandle(device_id, 1), 0)

    async def dispatch_tool_on_snapshot(
        self,
        *,
        route: DeviceRouteSnapshot,
        user_id: UUID,
        expected_device_name: str,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        on_issued: Callable[[], None] | None = None,
    ) -> object:
        del user_id, max_result_bytes, timeout_seconds
        if on_issued is not None:
            on_issued()
        assert expected_device_name == "laptop"
        device_id = route.handle.device_id
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
        self.route: DeviceRouteSnapshot | None = None

    async def get_route_snapshot(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str,
    ) -> DeviceRouteSnapshot:
        del user_id
        assert expected_device_name == "laptop"
        return DeviceRouteSnapshot(ConnectionHandle(device_id, 1), 0)

    async def start_client_to_server(self, **kwargs: object) -> _TransferResult:
        route = kwargs.get("route")
        assert isinstance(route, DeviceRouteSnapshot)
        self.route = route
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

    async def dispatch_tool_on_snapshot(
        self,
        *,
        route: DeviceRouteSnapshot,
        user_id: UUID,
        expected_device_name: str,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
    ) -> object:
        del user_id, max_result_bytes, timeout_seconds
        assert expected_device_name == "laptop"
        device_id = route.handle.device_id
        self.delete_call = (device_id, name, args)
        return SimpleNamespace(is_error=False, code=None)


@dataclass(frozen=True)
class _TransferResult:
    bytes_transferred: int
    sha256: str
    warnings: tuple[str, ...]
