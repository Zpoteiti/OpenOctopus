from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest

import openctopus_server.tools.file_transfer as file_transfer_module
from openctopus_server.devices.protocol import TransferBeginFrame
from openctopus_server.devices.registry import (
    BridgeRoutePair,
    ConnectionHandle,
    DeviceOutcomeUnknownError,
    DeviceRouteSnapshot,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import TransferError
from openctopus_server.devices.workspace import DirectorySourceProbe, FileSourceProbe
from openctopus_server.directory_contract import (
    DirectoryManifest,
    DirectoryManifestEntry,
    create_directory_manifest,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.tools.directory_transfer import (
    DirectoryMutationNotAppliedError,
    DirectoryTransferResult,
)
from openctopus_server.tools.file_transfer import FileTransferRequest, FileTransferTool
from openctopus_server.workspace.fs import (
    ServerDirectorySourceProbe,
    ServerFileSourceProbe,
    WorkspaceTarget,
)
from openctopus_server.workspace.service import TransferPathTicket


@dataclass
class _Lease:
    events: list[str]

    async def aclose(self) -> None:
        self.events.append("outer_release")


class _Session:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def __aenter__(self) -> _Session:
        self._events.append("db_enter")
        return self

    async def __aexit__(self, *_args: object) -> None:
        self._events.append("db_exit")


class _Workspace:
    def __init__(self, user_id: UUID, events: list[str]) -> None:
        self._events = events
        target = WorkspaceTarget.personal(user_id)
        self.source = TransferPathTicket(user_id, "source", target, "source", 1024)
        self.destination = TransferPathTicket(
            user_id, "destination", target, "destination", 1024
        )

    async def authorize_transfer_source(self, _db: object, **_kwargs: object) -> TransferPathTicket:
        self._events.append("auth_source")
        return self.source

    async def authorize_transfer_destination(
        self, _db: object, **_kwargs: object
    ) -> TransferPathTicket:
        self._events.append("auth_destination")
        return self.destination

    def transfer_ticket_changed(self, ticket: TransferPathTicket) -> None:
        self._events.append(
            "cache_source" if ticket is self.source else "cache_destination"
        )

    async def open_transfer_source(self, _ticket: TransferPathTicket) -> object:
        self._events.append("source_open")
        return SimpleNamespace(size=12, etag="source-v1", aclose=_noop)

    async def delete_transfer_source(
        self,
        _ticket: TransferPathTicket,
        *,
        if_match: str | None = None,
    ) -> None:
        assert if_match == "source-v1"
        self._events.append("source_delete")

    async def begin_transfer_upload(
        self,
        _ticket: TransferPathTicket,
        *,
        size: int,
    ) -> object:
        assert size == 12
        self._events.append("sink_create")
        return object()

    async def commit_transfer_upload(
        self,
        _ticket: TransferPathTicket,
        _sink: object,
        *,
        size: int,
        sha256: str,
    ) -> bool:
        assert size == 12
        assert sha256 == "a" * 64
        self._events.append("sink_commit")
        return True


class _WorkspaceFS:
    def __init__(self, events: list[str], *, kind: str = "file") -> None:
        self._events = events
        self._kind = kind

    async def acquire_server_transfer_operation(self, _user_id: UUID) -> _Lease:
        self._events.append("outer_acquire")
        return _Lease(self._events)

    async def probe_directory_source(
        self, _target: WorkspaceTarget, _relative_path: str
    ) -> ServerFileSourceProbe | ServerDirectorySourceProbe:
        self._events.append("probe")
        if self._kind == "directory":
            return ServerDirectorySourceProbe(manifest=_manifest())
        return ServerFileSourceProbe(size=12, fingerprint="source-v1")

    async def transfer_server_to_server_admitted(self, *_args: object, **_kwargs: object) -> Any:
        self._events.append("file_admitted")
        return 12, "a" * 64, ()


@dataclass(frozen=True)
class _RegularResult:
    bytes_transferred: int = 12
    sha256: str = "a" * 64
    warnings: tuple[str, ...] = ()


class _Transfers:
    idle_timeout_seconds = 0.1

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.acquire_count = 0

    async def acquire_operation(self, _user_id: UUID) -> _Lease:
        self.acquire_count += 1
        self.events.append("outer_acquire")
        return _Lease(self.events)

    async def start_server_to_client_regular_admitted(
        self,
        **kwargs: object,
    ) -> _RegularResult:
        self.events.append("file_admitted")
        source_factory = kwargs["source_factory"]
        assert callable(source_factory)
        source = await source_factory()
        await source.aclose()
        return _RegularResult()

    async def start_client_to_server_regular_admitted(
        self,
        **kwargs: object,
    ) -> _RegularResult:
        self.events.append("file_admitted")
        slot_id = kwargs["slot_id"]
        sink_factory = kwargs["sink_factory"]
        commit_sink = kwargs["commit_sink"]
        assert isinstance(slot_id, UUID)
        assert callable(sink_factory)
        assert callable(commit_sink)
        begin = TransferBeginFrame(
            id=slot_id,
            direction="client_to_server",
            purpose="file_transfer",
            src_path="source",
            dst_path="destination",
            total_bytes=12,
            etag="source-v1",
        )
        sink = await sink_factory(begin)
        assert await commit_sink(sink, begin, 12, "a" * 64) is True
        return _RegularResult()

    async def start_client_to_client_regular_admitted(
        self,
        **kwargs: object,
    ) -> _RegularResult:
        assert kwargs["expected_source_size"] == 12
        assert kwargs["expected_source_fingerprint"] == "source-v1"
        self.events.append("file_admitted")
        return _RegularResult()


class _Registry:
    def __init__(self, events: list[str], *, kind: str) -> None:
        self.events = events
        self.kind = kind
        self.manifest = _manifest()
        self.transfers = _Transfers(events)
        self.source_id = uuid4()
        self.destination_id = uuid4()
        self.source_route = DeviceRouteSnapshot(
            ConnectionHandle(self.source_id, 1),
            1,
            "source-client",
        )
        self.destination_route = DeviceRouteSnapshot(
            ConnectionHandle(self.destination_id, 1),
            1,
            "destination-client",
        )
        self.route_calls = 0
        self.pair_calls = 0

    async def get_route_snapshot(
        self,
        device_id: UUID,
        *,
        user_id: UUID,
        expected_device_name: str,
    ) -> DeviceRouteSnapshot:
        del user_id
        self.route_calls += 1
        self.events.append("route")
        if expected_device_name == "source-client":
            assert device_id == self.source_id
            return self.source_route
        assert expected_device_name == "destination-client"
        assert device_id == self.destination_id
        return self.destination_route

    async def get_bridge_route_pair(self, **kwargs: object) -> BridgeRoutePair:
        del kwargs
        self.pair_calls += 1
        self.events.append("pair")
        return BridgeRoutePair(
            source=self.source_route,
            destination=self.destination_route,
        )

    async def dispatch_tool_on_snapshot(self, **kwargs: object) -> object:
        args = kwargs["args"]
        assert isinstance(args, dict)
        if args["operation"] == "transfer_local":
            assert args["if_match"] == "source-v1"
            self.events.append("file_admitted")
            return SimpleNamespace(
                is_error=False,
                code=None,
                content=(
                    '{"kind":"file","files_transferred":1,'
                    '"bytes_transferred":12,"sha256":"%s","warnings":[]}'
                    % ("a" * 64)
                ),
            )
        self.events.append("source_delete")
        return SimpleNamespace(is_error=False, code=None)


class _Controller:
    def __init__(
        self,
        *,
        registry: _Registry,
        route: DeviceRouteSnapshot,
        user_id: UUID,
        directory_operation_id: UUID,
        idle_timeout_seconds: float,
    ) -> None:
        del idle_timeout_seconds
        self.registry = registry
        self.route = route
        self._user_id = user_id
        self.directory_operation_id = directory_operation_id

    @property
    def user_id(self) -> UUID:
        return self._user_id

    async def start_source_probe(self, _path: str) -> None:
        self.registry.events.append("probe")

    async def wait_source_until(self, _states: frozenset[str], **_kwargs: object) -> object:
        if self.registry.kind == "file":
            return SimpleNamespace(
                state="succeeded",
                probe=FileSourceProbe(size=12, fingerprint="source-v1"),
            )
        manifest = self.registry.manifest
        return SimpleNamespace(
            state="ready_retrieval",
            probe=DirectorySourceProbe(
                root_identity="root-v1",
                scanned_entries=manifest.scanned_entries,
                file_count=len(manifest.entries),
                total_bytes=manifest.total_bytes,
                manifest_sha256=manifest.manifest_sha256,
                page_count=1,
            ),
        )

    async def retrieve_source_manifest(
        self,
        _probe: DirectorySourceProbe,
    ) -> DirectoryManifest:
        self.registry.events.append("source_page")
        return self.registry.manifest

    async def hold_source_probe(self) -> None:
        self.registry.events.append("source_hold")

    async def cancel_source_probe(self) -> None:
        self.registry.events.append("source_cancel")

    async def release_source_probe(self) -> None:
        self.registry.events.append("source_release")

    async def start_destination_preflight(
        self,
        _dst_path: str,
        _manifest: DirectoryManifest,
    ) -> None:
        self.registry.events.append("destination_preflight")

    async def wait_destination_until(
        self,
        _states: frozenset[str],
        **_kwargs: object,
    ) -> object:
        return SimpleNamespace(state="ready")

    async def start_local_directory(self, **_kwargs: object) -> None:
        self.registry.events.append("local_directory")

    async def wait_local_until(self, _states: frozenset[str]) -> object:
        return SimpleNamespace(
            state="succeeded",
            terminal_result=SimpleNamespace(
                files_transferred=1,
                bytes_transferred=12,
                sha256="b" * 64,
                warnings=(),
            ),
        )

    async def release_local_directory(self) -> None:
        self.registry.events.append("local_release")


class _CancellingProbeController(_Controller):
    started: asyncio.Event

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._cancelled = False

    async def start_source_probe(self, _path: str) -> None:
        self.registry.events.append("probe")
        type(self).started.set()

    async def wait_source_until(self, _states: frozenset[str], **_kwargs: object) -> object:
        if self._cancelled:
            return SimpleNamespace(state="failed", probe=None)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def cancel_source_probe(self) -> None:
        self._cancelled = True
        self.registry.events.append("source_cancel")


class _MissingProbeController(_Controller):
    original: TransferError

    async def start_source_probe(self, _path: str) -> None:
        self.registry.events.append("probe")
        raise type(self).original

    async def cancel_source_probe(self) -> None:
        self.registry.events.append("source_cancel")
        raise TransferError(ErrorCode.WORKSPACE_NOT_FOUND.value)

    async def release_source_probe(self) -> None:
        self.registry.events.append("source_release")
        raise TransferError(ErrorCode.WORKSPACE_NOT_FOUND.value)


class _UnavailableCleanupProbeController(_CancellingProbeController):
    original: BaseException | None = None

    async def wait_source_until(
        self,
        _states: frozenset[str],
        **_kwargs: object,
    ) -> object:
        if self._cancelled:
            raise DeviceUnavailableError("cleanup route unavailable")
        if type(self).original is not None:
            raise type(self).original
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def cancel_source_probe(self) -> None:
        self._cancelled = True
        self.registry.events.append("source_cancel")
        raise DeviceUnavailableError("cleanup route unavailable")

    async def release_source_probe(self) -> None:
        self.registry.events.append("source_release")
        raise DeviceUnavailableError("cleanup route unavailable")


class _RepeatedCancellationProbeController(_CancellingProbeController):
    cleanup_started: asyncio.Event
    allow_cleanup: asyncio.Event

    async def cancel_source_probe(self) -> None:
        self.registry.events.append("source_cancel")
        type(self).cleanup_started.set()
        await type(self).allow_cleanup.wait()
        self._cancelled = True


class _LocalLifecycleController:
    def __init__(self, *, cancellation: bool) -> None:
        self.events: list[str] = []
        self._cancellation = cancellation
        self._cancelled = False
        self.local_started = asyncio.Event()

    async def start_destination_preflight(
        self,
        _dst_path: str,
        _manifest: DirectoryManifest,
    ) -> None:
        self.events.append("destination_preflight")

    async def wait_destination_until(self, _states: frozenset[str]) -> object:
        return SimpleNamespace(state="ready")

    async def start_local_directory(self, **_kwargs: object) -> None:
        self.events.append("local_start")
        self.local_started.set()

    async def wait_local_until(self, _states: frozenset[str]) -> object:
        if self._cancellation and not self._cancelled:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        return SimpleNamespace(
            state="failed",
            terminal_error=SimpleNamespace(
                code=ErrorCode.WORKSPACE_QUOTA_EXCEEDED.value
            ),
        )

    async def cancel_local_directory(self) -> None:
        self._cancelled = True
        self.events.append("local_cancel")

    async def release_local_directory(self) -> None:
        self.events.append("local_release")


class _LocalStartEvidenceController(_LocalLifecycleController):
    def __init__(
        self,
        *,
        error: BaseException,
        issued: bool,
        proven_not_applied: bool = False,
    ) -> None:
        super().__init__(cancellation=False)
        self._error = error
        self._issued = issued
        self._proven_not_applied = proven_not_applied

    async def start_local_directory(self, **kwargs: object) -> None:
        self.events.append("local_start")
        callback = kwargs.get("on_issued")
        if self._issued:
            assert callable(callback)
            callback()
        if self._proven_not_applied:
            raise DirectoryMutationNotAppliedError("local start was not applied") from self._error
        raise self._error

    async def cancel_local_directory(self) -> None:
        self.events.append("local_cancel")
        raise DeviceUnavailableError("cleanup route unavailable")

    async def release_local_directory(self) -> None:
        self.events.append("local_release")
        raise DeviceUnavailableError("cleanup route unavailable")


class _Coordinator:
    async def run(self, **kwargs: object) -> DirectoryTransferResult:
        backend = kwargs["backend"]
        lease = kwargs["operation_lease"]
        events = _backend_events(backend)
        events.append(f"directory:{type(backend).__name__}")
        await backend.release()
        await lease.aclose()
        return DirectoryTransferResult(
            kind="directory",
            files_transferred=1,
            bytes_transferred=12,
            sha256="b" * 64,
        )


def _backend_events(backend: object) -> list[str]:
    for name in ("_source", "_destination"):
        endpoint = getattr(backend, name, None)
        registry = getattr(endpoint, "registry", None)
        if registry is not None:
            return registry.events
    workspace_fs = getattr(backend, "_fs", None)
    return workspace_fs._events


def _manifest() -> DirectoryManifest:
    return create_directory_manifest(
        root_identity="root-v1",
        directories=(),
        entries=(
            DirectoryManifestEntry(
                relative_path="child.txt",
                size=12,
                fingerprint="child-v1",
            ),
        ),
    )


async def _noop() -> None:
    return None


@pytest.mark.asyncio
async def test_server_file_authorizes_then_uses_one_outer_admitted_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    user_id = uuid4()
    workspace = _Workspace(user_id, events)
    workspace_fs = _WorkspaceFS(events)
    monkeypatch.setattr(
        file_transfer_module,
        "AsyncSession",
        lambda *_args, **_kwargs: _Session(events),
    )
    tool = FileTransferTool(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        None,
        workspace_fs,  # type: ignore[arg-type]
    )

    outcome = await tool.transfer(
        FileTransferRequest(
            openoctopus_src_device="server",
            src_path="source",
            openoctopus_dst_device="server",
            dst_path="destination",
            mode="copy",
        ),
        user_id=user_id,
    )

    assert outcome.kind == "file"
    assert events == [
        "db_enter",
        "auth_source",
        "auth_destination",
        "db_exit",
        "outer_acquire",
        "probe",
        "file_admitted",
        "outer_release",
        "cache_source",
        "cache_destination",
    ]


@pytest.mark.parametrize(
    ("topology", "source_device", "destination_device", "directory_backend"),
    [
        ("server-server", "server", "server", "ServerDirectoryTransferBackend"),
        (
            "server-client",
            "server",
            "destination-client",
            "ServerToClientDirectoryBackend",
        ),
        (
            "client-server",
            "source-client",
            "server",
            "ClientToServerDirectoryBackend",
        ),
        ("same-client", "source-client", "source-client", "local_directory"),
        (
            "client-client",
            "source-client",
            "destination-client",
            "ClientToClientDirectoryBackend",
        ),
    ],
)
@pytest.mark.parametrize("kind", ["file", "directory"])
@pytest.mark.asyncio
async def test_public_transfer_routes_five_topologies_after_one_outer_admission(
    monkeypatch: pytest.MonkeyPatch,
    topology: str,
    source_device: str,
    destination_device: str,
    directory_backend: str,
    kind: str,
) -> None:
    events: list[str] = []
    user_id = uuid4()
    workspace = _Workspace(user_id, events)
    workspace_fs = _WorkspaceFS(events, kind=kind)
    registry = _Registry(events, kind=kind)
    monkeypatch.setattr(
        file_transfer_module,
        "AsyncSession",
        lambda *_args, **_kwargs: _Session(events),
    )
    monkeypatch.setattr(file_transfer_module, "DeviceDirectoryJobController", _Controller)
    monkeypatch.setattr(file_transfer_module, "DirectoryTransferCoordinator", _Coordinator)
    tool = FileTransferTool(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        None if topology == "server-server" else registry,  # type: ignore[arg-type]
        workspace_fs,  # type: ignore[arg-type]
    )

    async def resolve_device(
        _user_id: UUID,
        name: str,
        _targets: object,
    ) -> UUID:
        events.extend(("device_db_enter", "device_db_exit"))
        return registry.source_id if name == "source-client" else registry.destination_id

    async def resolve_pair(
        _user_id: UUID,
        _source_name: str,
        _destination_name: str,
        _targets: object,
    ) -> tuple[UUID, UUID]:
        events.extend(("device_db_enter", "device_db_exit"))
        return registry.source_id, registry.destination_id

    monkeypatch.setattr(tool, "_device_id_for_call", resolve_device)
    monkeypatch.setattr(tool, "_bridge_device_ids_for_call", resolve_pair)

    outcome = await tool.transfer(
        FileTransferRequest(
            openoctopus_src_device=source_device,
            src_path="source",
            openoctopus_dst_device=destination_device,
            dst_path="destination",
            mode="copy",
        ),
        user_id=user_id,
    )

    assert outcome.kind == kind
    assert events.count("outer_acquire") == 1
    assert events.count("outer_release") == 1
    outer_index = events.index("outer_acquire")
    assert events.index("probe") > outer_index
    assert all(
        index < outer_index for index, event in enumerate(events) if event == "db_exit"
    )
    if topology in {"server-client", "client-server", "same-client"}:
        assert registry.route_calls == 1
        assert registry.pair_calls == 0
    elif topology == "client-client":
        assert registry.route_calls == 0
        assert registry.pair_calls == 1
    else:
        assert registry.route_calls == registry.pair_calls == 0

    if kind == "file":
        assert events.count("file_admitted") == 1
        assert not any(event.startswith("directory:") for event in events)
    elif topology == "same-client":
        assert events.count("source_page") == 1
        assert events.count("source_release") == 1
        assert events.count("destination_preflight") == 1
        assert events.count("local_directory") == 1
        assert events.count("local_release") == 1
    else:
        assert events.count(f"directory:{directory_backend}") == 1

    if source_device != "server":
        assert events.count("source_release") == 1
        if kind == "directory" and topology != "same-client":
            assert events.count("source_page") == 1
            assert events.count("source_hold") == 1
    if topology == "server-server":
        assert events[-2:] == ["cache_source", "cache_destination"]
    elif topology == "server-client":
        assert events[-1] == "cache_source"
    elif topology == "client-server":
        assert events[-1] == "cache_destination"
    else:
        assert not any(event.startswith("cache_") for event in events)


@pytest.mark.asyncio
async def test_preissue_missing_probe_cleanup_preserves_original_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    registry = _Registry(events, kind="file")
    original = TransferError(ErrorCode.WORKSPACE_NOT_FOUND.value)
    _MissingProbeController.original = original
    monkeypatch.setattr(
        file_transfer_module,
        "DeviceDirectoryJobController",
        _MissingProbeController,
    )
    tool = FileTransferTool(
        object(),  # type: ignore[arg-type]
        None,
        registry,  # type: ignore[arg-type]
        None,
    )

    async def resolve_device(
        _user_id: UUID,
        _name: str,
        _targets: object,
    ) -> UUID:
        return registry.source_id

    monkeypatch.setattr(tool, "_device_id_for_call", resolve_device)

    with pytest.raises(TransferError) as raised:
        await tool.transfer(
            FileTransferRequest(
                openoctopus_src_device="source-client",
                src_path="missing",
                openoctopus_dst_device="source-client",
                dst_path="destination",
                mode="copy",
            ),
            user_id=uuid4(),
        )

    assert raised.value is original
    assert events == [
        "route",
        "outer_acquire",
        "probe",
        "source_cancel",
        "source_release",
        "outer_release",
    ]


@pytest.mark.parametrize(
    ("topology", "source_device", "destination_device"),
    [
        ("client-server", "source-client", "server"),
        ("same-client", "source-client", "source-client"),
        ("client-client", "source-client", "destination-client"),
    ],
)
@pytest.mark.asyncio
async def test_cancelled_client_probe_is_retired_before_outer_release(
    monkeypatch: pytest.MonkeyPatch,
    topology: str,
    source_device: str,
    destination_device: str,
) -> None:
    events: list[str] = []
    user_id = uuid4()
    workspace = _Workspace(user_id, events)
    workspace_fs = _WorkspaceFS(events)
    registry = _Registry(events, kind="file")
    _CancellingProbeController.started = asyncio.Event()
    monkeypatch.setattr(
        file_transfer_module,
        "DeviceDirectoryJobController",
        _CancellingProbeController,
    )
    monkeypatch.setattr(
        file_transfer_module,
        "AsyncSession",
        lambda *_args, **_kwargs: _Session(events),
    )
    tool = FileTransferTool(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        registry,  # type: ignore[arg-type]
        workspace_fs,  # type: ignore[arg-type]
    )

    async def resolve_device(
        _user_id: UUID,
        _name: str,
        _targets: object,
    ) -> UUID:
        return registry.source_id

    async def resolve_pair(
        _user_id: UUID,
        _source_name: str,
        _destination_name: str,
        _targets: object,
    ) -> tuple[UUID, UUID]:
        return registry.source_id, registry.destination_id

    monkeypatch.setattr(tool, "_device_id_for_call", resolve_device)
    monkeypatch.setattr(tool, "_bridge_device_ids_for_call", resolve_pair)
    task = asyncio.create_task(
        tool.transfer(
            FileTransferRequest(
                openoctopus_src_device=source_device,
                src_path="source",
                openoctopus_dst_device=destination_device,
                dst_path="destination",
                mode="copy",
            ),
            user_id=user_id,
        )
    )
    await _CancellingProbeController.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert events.count("source_cancel") == 1
    assert events.count("source_release") == 1
    assert events.index("source_release") < events.index("outer_release")
    if topology == "client-server":
        assert events[-1] == "cache_destination"


@pytest.mark.parametrize(
    ("source_device", "destination_device"),
    [
        ("source-client", "server"),
        ("source-client", "source-client"),
        ("source-client", "destination-client"),
    ],
)
@pytest.mark.asyncio
async def test_repeated_probe_cancellation_still_releases_outer_admission(
    monkeypatch: pytest.MonkeyPatch,
    source_device: str,
    destination_device: str,
) -> None:
    events: list[str] = []
    user_id = uuid4()
    workspace = _Workspace(user_id, events)
    registry = _Registry(events, kind="file")
    _RepeatedCancellationProbeController.started = asyncio.Event()
    _RepeatedCancellationProbeController.cleanup_started = asyncio.Event()
    _RepeatedCancellationProbeController.allow_cleanup = asyncio.Event()
    monkeypatch.setattr(
        file_transfer_module,
        "DeviceDirectoryJobController",
        _RepeatedCancellationProbeController,
    )
    monkeypatch.setattr(
        file_transfer_module,
        "AsyncSession",
        lambda *_args, **_kwargs: _Session(events),
    )
    tool = FileTransferTool(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        registry,  # type: ignore[arg-type]
        _WorkspaceFS(events),  # type: ignore[arg-type]
    )

    async def resolve_device(
        _user_id: UUID,
        _name: str,
        _targets: object,
    ) -> UUID:
        return registry.source_id

    async def resolve_pair(
        _user_id: UUID,
        _source_name: str,
        _destination_name: str,
        _targets: object,
    ) -> tuple[UUID, UUID]:
        return registry.source_id, registry.destination_id

    monkeypatch.setattr(tool, "_device_id_for_call", resolve_device)
    monkeypatch.setattr(tool, "_bridge_device_ids_for_call", resolve_pair)
    task = asyncio.create_task(
        tool.transfer(
            FileTransferRequest(
                openoctopus_src_device=source_device,
                src_path="source",
                openoctopus_dst_device=destination_device,
                dst_path="destination",
                mode="copy",
            ),
            user_id=user_id,
        )
    )
    await _RepeatedCancellationProbeController.started.wait()
    task.cancel()
    await _RepeatedCancellationProbeController.cleanup_started.wait()
    task.cancel()
    _RepeatedCancellationProbeController.allow_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert events.count("source_cancel") == 1
    assert events.count("source_release") == 1
    assert events.count("outer_release") == 1
    assert events.index("source_release") < events.index("outer_release")


@pytest.mark.parametrize("failure_kind", ["cancel", "timeout"])
@pytest.mark.asyncio
async def test_read_only_probe_cleanup_failure_preserves_original_outcome(
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    events: list[str] = []
    registry = _Registry(events, kind="file")
    _UnavailableCleanupProbeController.started = asyncio.Event()
    original_timeout = TimeoutError("source probe timed out")
    _UnavailableCleanupProbeController.original = (
        original_timeout if failure_kind == "timeout" else None
    )
    monkeypatch.setattr(
        file_transfer_module,
        "DeviceDirectoryJobController",
        _UnavailableCleanupProbeController,
    )
    tool = FileTransferTool(
        object(),  # type: ignore[arg-type]
        None,
        registry,  # type: ignore[arg-type]
        None,
    )

    async def resolve_device(
        _user_id: UUID,
        _name: str,
        _targets: object,
    ) -> UUID:
        return registry.source_id

    monkeypatch.setattr(tool, "_device_id_for_call", resolve_device)
    task = asyncio.create_task(
        tool.transfer(
            FileTransferRequest(
                openoctopus_src_device="source-client",
                src_path="source",
                openoctopus_dst_device="source-client",
                dst_path="destination",
                mode="copy",
            ),
            user_id=uuid4(),
        )
    )
    await _UnavailableCleanupProbeController.started.wait()
    if failure_kind == "cancel":
        task.cancel()

    if failure_kind == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(TimeoutError) as raised:
            await task
        assert raised.value is original_timeout

    assert events[-3:] == ["source_cancel", "source_release", "outer_release"]


@pytest.mark.parametrize(
    ("issued", "proven_not_applied", "expected"),
    [
        (False, False, "unavailable"),
        (True, True, "not_applied"),
        (True, False, "outcome_unknown"),
    ],
)
@pytest.mark.asyncio
async def test_same_client_cleanup_failure_uses_mutation_evidence(
    issued: bool,
    proven_not_applied: bool,
    expected: str,
) -> None:
    original = DeviceUnavailableError("local start route unavailable")
    controller = _LocalStartEvidenceController(
        error=original,
        issued=issued,
        proven_not_applied=proven_not_applied,
    )
    tool = FileTransferTool(None, None, None, None)

    if expected == "outcome_unknown":
        with pytest.raises(DeviceOutcomeUnknownError) as raised:
            await tool._same_client_directory(  # type: ignore[arg-type]
                FileTransferRequest(
                    openoctopus_src_device="source-client",
                    src_path="source",
                    openoctopus_dst_device="source-client",
                    dst_path="destination",
                    mode="copy",
                ),
                controller=controller,
                manifest=_manifest(),
                on_issued=None,
            )
        assert raised.value.__cause__ is original
    else:
        expected_error = (
            DirectoryMutationNotAppliedError
            if expected == "not_applied"
            else DeviceUnavailableError
        )
        with pytest.raises(expected_error) as raised:
            await tool._same_client_directory(  # type: ignore[arg-type]
                FileTransferRequest(
                    openoctopus_src_device="source-client",
                    src_path="source",
                    openoctopus_dst_device="source-client",
                    dst_path="destination",
                    mode="copy",
                ),
                controller=controller,
                manifest=_manifest(),
                on_issued=None,
            )
        if expected == "unavailable":
            assert raised.value is original

    assert controller.events == [
        "destination_preflight",
        "local_start",
        "local_cancel",
        "local_release",
    ]


@pytest.mark.asyncio
async def test_same_client_directory_failure_cancels_and_releases_local_job() -> None:
    controller = _LocalLifecycleController(cancellation=False)
    tool = FileTransferTool(None, None, None, None)
    request = FileTransferRequest(
        openoctopus_src_device="source-client",
        src_path="source",
        openoctopus_dst_device="source-client",
        dst_path="destination",
        mode="copy",
    )

    with pytest.raises(TransferError) as raised:
        await tool._same_client_directory(  # type: ignore[arg-type]
            request,
            controller=controller,
            manifest=_manifest(),
            on_issued=None,
        )

    assert raised.value.code == ErrorCode.WORKSPACE_QUOTA_EXCEEDED.value
    assert controller.events == [
        "destination_preflight",
        "local_start",
        "local_cancel",
        "local_release",
    ]


@pytest.mark.asyncio
async def test_same_client_directory_cancellation_settles_local_job() -> None:
    controller = _LocalLifecycleController(cancellation=True)
    tool = FileTransferTool(None, None, None, None)
    task = asyncio.create_task(
        tool._same_client_directory(  # type: ignore[arg-type]
            FileTransferRequest(
                openoctopus_src_device="source-client",
                src_path="source",
                openoctopus_dst_device="source-client",
                dst_path="destination",
                mode="copy",
            ),
            controller=controller,
            manifest=_manifest(),
            on_issued=None,
        )
    )
    await controller.local_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert controller.events == [
        "destination_preflight",
        "local_start",
        "local_cancel",
        "local_release",
    ]


@pytest.mark.asyncio
async def test_server_ticket_cache_is_invalidated_when_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    user_id = uuid4()
    workspace = _Workspace(user_id, events)
    workspace_fs = _WorkspaceFS(events)

    async def fail_probe(*_args: object) -> object:
        events.append("probe")
        raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "missing")

    monkeypatch.setattr(workspace_fs, "probe_directory_source", fail_probe)
    monkeypatch.setattr(
        file_transfer_module,
        "AsyncSession",
        lambda *_args, **_kwargs: _Session(events),
    )
    tool = FileTransferTool(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        None,
        workspace_fs,  # type: ignore[arg-type]
    )

    with pytest.raises(WorkspaceError):
        await tool.transfer(
            FileTransferRequest(
                openoctopus_src_device="server",
                src_path="source",
                openoctopus_dst_device="server",
                dst_path="destination",
                mode="copy",
            ),
            user_id=user_id,
        )

    assert events[-3:] == ["outer_release", "cache_source", "cache_destination"]


@pytest.mark.asyncio
async def test_server_ticket_cache_is_invalidated_after_issued_file_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    issued: list[str] = []
    user_id = uuid4()
    workspace = _Workspace(user_id, events)
    workspace_fs = _WorkspaceFS(events)
    registry = _Registry(events, kind="file")

    async def fail_after_issue(**kwargs: object) -> object:
        callback = kwargs["on_issued"]
        assert callable(callback)
        callback()
        events.append("file_issued")
        raise TransferError(ErrorCode.WORKSPACE_STORAGE_ERROR.value)

    monkeypatch.setattr(
        registry.transfers,
        "start_server_to_client_regular_admitted",
        fail_after_issue,
    )
    monkeypatch.setattr(
        file_transfer_module,
        "AsyncSession",
        lambda *_args, **_kwargs: _Session(events),
    )
    tool = FileTransferTool(
        object(),  # type: ignore[arg-type]
        workspace,  # type: ignore[arg-type]
        registry,  # type: ignore[arg-type]
        workspace_fs,  # type: ignore[arg-type]
    )

    async def resolve_device(
        _user_id: UUID,
        _name: str,
        _targets: object,
    ) -> UUID:
        return registry.destination_id

    monkeypatch.setattr(tool, "_device_id_for_call", resolve_device)

    with pytest.raises(TransferError):
        await tool.transfer(
            FileTransferRequest(
                openoctopus_src_device="server",
                src_path="source",
                openoctopus_dst_device="destination-client",
                dst_path="destination",
                mode="copy",
            ),
            user_id=user_id,
            on_issued=lambda: issued.append("issued"),
        )

    assert issued == ["issued"]
    assert events[-3:] == ["file_issued", "outer_release", "cache_source"]
