from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Literal, cast
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from openctopus_server.devices.protocol import TransferBeginFrame, new_uuid7
from openctopus_server.devices.registry import (
    ConnectionHandle,
    DeviceOutcomeUnknownError,
    DeviceRouteSnapshot,
)
from openctopus_server.devices.transfer import (
    TransferCommitResult,
    TransferCommittedAfterCancellation,
    TransferIntegrityError,
    TransferLease,
    TransferManager,
    TransferResult,
)
from openctopus_server.directory_contract import (
    DirectoryContentEntry,
    DirectoryManifest,
    DirectoryManifestDirectory,
    DirectoryManifestEntry,
    create_directory_manifest,
    directory_content_sha256,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.tools.cross_site_directory_backend import (
    ClientToClientDirectoryBackend,
    ClientToServerDirectoryBackend,
    ServerToClientDirectoryBackend,
)
from openctopus_server.tools.device_directory_jobs import DeviceDirectoryJobController
from openctopus_server.tools.directory_transfer import (
    DirectoryChildCommittedAfterCancellation,
    DirectoryTransferCoordinator,
)
from openctopus_server.workspace.fs import (
    DirectoryDestinationPlan,
    FileMetadata,
    WorkspaceFS,
    WorkspaceTarget,
)
from openctopus_server.workspace.service import TransferPathTicket, WorkspaceService


def _server_manifest() -> DirectoryManifest:
    return create_directory_manifest(
        root_identity=None,
        directories=(DirectoryManifestDirectory(relative_path="nested", identity=None),),
        entries=(
            DirectoryManifestEntry(relative_path="a.txt", size=1, fingerprint="source-a"),
            DirectoryManifestEntry(relative_path="nested/b.txt", size=2, fingerprint="source-b"),
        ),
    )


def _client_manifest() -> DirectoryManifest:
    return create_directory_manifest(
        root_identity="root-v1",
        directories=(DirectoryManifestDirectory(relative_path="nested", identity="nested-v1"),),
        entries=(
            DirectoryManifestEntry(relative_path="a.txt", size=1, fingerprint="source-a"),
            DirectoryManifestEntry(relative_path="nested/b.txt", size=2, fingerprint="source-b"),
        ),
    )


def _payloads() -> dict[str, bytes]:
    return {"a.txt": b"a", "nested/b.txt": b"bb"}


def _final_result(manifest: DirectoryManifest) -> SimpleNamespace:
    payloads = _payloads()
    content = tuple(
        DirectoryContentEntry(
            relative_path=entry.relative_path,
            size=entry.size,
            sha256=hashlib.sha256(payloads.get(entry.relative_path, b"x" * entry.size)).hexdigest(),
        )
        for entry in manifest.entries
    )
    return SimpleNamespace(
        files_transferred=len(content),
        bytes_transferred=sum(item.size for item in content),
        sha256=directory_content_sha256(content),
        warnings=[],
    )


@dataclass
class _OperationLease:
    close_count: int = 0

    async def aclose(self) -> None:
        self.close_count += 1


@dataclass
class _SubtreeLease:
    release_count: int = 0

    async def release(self) -> None:
        self.release_count += 1


@dataclass
class _QuotaReservation:
    release_count: int = 0

    async def release(self) -> None:
        self.release_count += 1


@dataclass
class _Stream:
    data: bytes
    etag: str
    closed: bool = False
    chunks: list[bytes] = field(init=False)

    def __post_init__(self) -> None:
        self.size = len(self.data)
        self.chunks = [self.data, b""]

    async def read(self) -> bytes:
        return self.chunks.pop(0)

    async def aclose(self) -> None:
        self.closed = True


@dataclass
class _Sink:
    chunks: list[bytes] = field(default_factory=list)
    finished: bool = False

    async def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)

    async def finish(self) -> None:
        self.finished = True

    async def abort(self) -> None:
        pass


class _Controller:
    def __init__(
        self,
        *,
        operation_id,
        user_id,
        name: str,
        manifest: DirectoryManifest,
        events: list[str],
    ) -> None:
        self.directory_operation_id = operation_id
        self.user_id = user_id
        self.route = DeviceRouteSnapshot(
            ConnectionHandle(uuid4(), 1),
            0,
            name,
        )
        self.events = events
        self.final_result = _final_result(manifest)
        self.cleanup_complete = True
        self.destination_cancelled = False
        self.source_warnings: list[str] = []
        self.release_source_count = 0
        self.release_destination_count = 0
        self.fail_source_authorization = False
        self.fail_destination_authorization = False
        self.fail_destination_release = False

    async def start_destination_preflight(self, _path: str, _manifest: object) -> object:
        self.events.append("destination-preflight")
        return object()

    async def prepare_destination(self, *, on_issued=None) -> object:
        self.events.append("destination-prepare")
        if on_issued is not None:
            on_issued()
        return object()

    async def finish_destination(self) -> object:
        self.events.append("destination-finish")
        return object()

    async def cancel_destination(self) -> object:
        self.events.append("destination-cancel")
        self.destination_cancelled = True
        return object()

    async def wait_destination_until(
        self,
        states: frozenset[str],
        *,
        progress_callback=None,
    ) -> SimpleNamespace:
        del progress_callback
        if "ready" in states:
            return SimpleNamespace(state="ready")
        if "reserved" in states:
            return SimpleNamespace(state="reserved")
        if "finalized_held" in states:
            return SimpleNamespace(
                state="finalized_held",
                terminal_result=self.final_result,
            )
        assert self.destination_cancelled
        return SimpleNamespace(
            state="failed" if self.cleanup_complete else "outcome_unknown",
            cleanup_complete=self.cleanup_complete,
        )

    async def get_destination_status(self, *, outer_progress_seq=None) -> SimpleNamespace:
        del outer_progress_seq
        return SimpleNamespace(state="finalized_held", progress_seq=0)

    async def authorize_destination_child(self, _slot_id, relative_path: str) -> object:
        self.events.append(f"destination-authorize:{relative_path}")
        if self.fail_destination_authorization:
            raise DeviceOutcomeUnknownError("authorization ACK was lost")
        return object()

    async def release_destination(self) -> object:
        self.events.append("destination-release")
        self.release_destination_count += 1
        if self.fail_destination_release:
            raise DeviceOutcomeUnknownError("release result was lost")
        return object()

    async def authorize_source_child(
        self, _slot_id, relative_path: str, _fingerprint: str
    ) -> object:
        self.events.append(f"source-authorize:{relative_path}")
        if self.fail_source_authorization:
            raise DeviceOutcomeUnknownError("authorization ACK was lost")
        return object()

    async def start_source_cleanup(self) -> object:
        self.events.append("source-cleanup")
        return object()

    async def cancel_source_probe(self) -> object:
        self.events.append("source-cancel")
        return object()

    async def wait_source_until(
        self,
        _states: frozenset[str],
        *,
        progress_callback=None,
    ) -> SimpleNamespace:
        del progress_callback
        return SimpleNamespace(
            state="succeeded",
            terminal_result=SimpleNamespace(
                cleanup_complete=not self.source_warnings,
                warnings=self.source_warnings,
            ),
        )

    async def release_source_probe(self) -> object:
        self.events.append("source-release")
        self.release_source_count += 1
        return object()


class _BlockingFinalizeController(_Controller):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.finish_started = asyncio.Event()
        self.release_finish = asyncio.Event()

    async def finish_destination(self) -> object:
        self.events.append("destination-finish")
        self.finish_started.set()
        await self.release_finish.wait()
        return object()


class _StatefulSourceController(_Controller):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.source_authorization_pending = False
        self.source_cancel_count = 0
        self.source_terminal = False

    async def authorize_source_child(self, slot_id, relative_path: str, fingerprint: str) -> object:
        result = await super().authorize_source_child(slot_id, relative_path, fingerprint)
        self.source_authorization_pending = True
        return result

    async def cancel_source_probe(self) -> object:
        self.events.append("source-cancel")
        self.source_cancel_count += 1
        self.source_authorization_pending = False
        self.source_terminal = True
        return object()

    async def wait_source_until(
        self,
        states: frozenset[str],
        *,
        progress_callback=None,
    ) -> SimpleNamespace:
        del progress_callback
        if self.source_terminal:
            assert "failed" in states
            return SimpleNamespace(state="failed", terminal_result=None)
        return await super().wait_source_until(states)

    async def release_source_probe(self) -> object:
        if self.source_authorization_pending:
            raise RuntimeError("source authorization was not retired")
        return await super().release_source_probe()


class _ProgressController(_Controller):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.destination_progress = iter(((1, 1, 2), (3, 3, 4), (5, 5, 6)))
        self.source_cleanup_progress = (7, 7, 8)
        self.forwarded_source_progress: list[int] = []
        self.forwarded_destination_progress: list[int] = []

    async def wait_destination_until(
        self,
        states: frozenset[str],
        *,
        progress_callback=None,
    ) -> SimpleNamespace:
        if progress_callback is not None and (
            "ready" in states or "reserved" in states or "finalized_held" in states
        ):
            previous = -1
            for value in next(self.destination_progress):
                if value > previous:
                    previous = value
                    await progress_callback(value)
        return await super().wait_destination_until(states)

    async def wait_source_until(
        self,
        states: frozenset[str],
        *,
        progress_callback=None,
    ) -> SimpleNamespace:
        if progress_callback is not None:
            previous = -1
            for value in self.source_cleanup_progress:
                if value > previous:
                    previous = value
                    await progress_callback(value)
        return await super().wait_source_until(states)

    async def get_source_status(self, *, outer_progress_seq=None) -> SimpleNamespace:
        assert outer_progress_seq is not None
        self.forwarded_source_progress.append(outer_progress_seq)
        return SimpleNamespace(state="held", progress_seq=0)

    async def get_destination_status(self, *, outer_progress_seq=None) -> SimpleNamespace:
        assert outer_progress_seq is not None
        self.forwarded_destination_progress.append(outer_progress_seq)
        return SimpleNamespace(state="finalized_held", progress_seq=0)


@dataclass
class _ManualClock:
    value: float = 0.0

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _ExpiringFinalizedController(_Controller):
    def __init__(
        self,
        *,
        clock: _ManualClock,
        idle_timeout: float,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._clock = clock
        self._idle_timeout = idle_timeout
        self._last_progress_at = clock.value
        self._last_outer_progress_seq = 0
        self.forwarded_destination_progress: list[int] = []
        self.expired = False

    async def finish_destination(self) -> object:
        result = await super().finish_destination()
        self._last_progress_at = self._clock.value
        return result

    async def get_destination_status(self, *, outer_progress_seq=None) -> SimpleNamespace:
        self._expire_if_idle()
        if self.expired:
            raise DeviceOutcomeUnknownError("finalized destination lease expired")
        assert outer_progress_seq is not None
        if outer_progress_seq > self._last_outer_progress_seq:
            self._last_outer_progress_seq = outer_progress_seq
            self._last_progress_at = self._clock.value
            self.forwarded_destination_progress.append(outer_progress_seq)
        return SimpleNamespace(state="finalized_held", progress_seq=0)

    async def release_destination(self) -> object:
        self._expire_if_idle()
        if self.expired:
            raise DeviceOutcomeUnknownError("finalized destination lease expired")
        return await super().release_destination()

    def _expire_if_idle(self) -> None:
        if self._clock.value - self._last_progress_at >= self._idle_timeout:
            self.expired = True


class _Transfers:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.fail_on_path: str | None = None
        self.missing_metadata = False
        self.committed_cancel_path: str | None = None

    async def start_server_to_client_admitted(self, **kwargs: Any) -> TransferResult:
        relative_path = kwargs["dst_path"].removeprefix("backup/")
        self.events.append(f"transfer:{relative_path}")
        source = await kwargs["source_factory"]()
        digest = hashlib.sha256()
        size = 0
        while chunk := await source.read():
            digest.update(chunk)
            size += len(chunk)
        await source.aclose()
        if self.fail_on_path == relative_path:
            raise RuntimeError("child failed")
        result = TransferResult(
            size,
            digest.hexdigest(),
            etag=None if self.missing_metadata else f"destination-{relative_path}",
            created=None if self.missing_metadata else True,
        )
        if self.committed_cancel_path == relative_path:
            raise TransferCommittedAfterCancellation(result)
        return result

    async def start_client_to_server_admitted(self, **kwargs: Any) -> TransferResult:
        src_path = kwargs["src_path"]
        relative_path = src_path.removeprefix("source/")
        self.events.append(f"transfer:{relative_path}")
        if self.fail_on_path == relative_path:
            raise RuntimeError("child failed")
        data = _payloads().get(relative_path, b"invalid")
        fingerprint = next(
            (
                entry.fingerprint
                for entry in _client_manifest().entries
                if entry.relative_path == relative_path
            ),
            "skill-v1",
        )
        begin = TransferBeginFrame(
            id=kwargs["slot_id"],
            direction="client_to_server",
            purpose="file_transfer",
            src_path=src_path,
            dst_path=kwargs["dst_path"],
            total_bytes=len(data),
            etag=fingerprint,
        )
        sink = await kwargs["sink_factory"](begin)
        await sink.write(data)
        await sink.finish()
        commit = await kwargs["commit_sink"](
            sink,
            begin,
            len(data),
            hashlib.sha256(data).hexdigest(),
        )
        assert isinstance(commit, TransferCommitResult)
        result = TransferResult(
            len(data),
            hashlib.sha256(data).hexdigest(),
            etag=commit.etag,
            created=True,
        )
        if self.committed_cancel_path == relative_path:
            raise TransferCommittedAfterCancellation(result)
        return result

    async def start_client_to_client_admitted(self, **kwargs: Any) -> TransferResult:
        relative_path = kwargs["src_path"].removeprefix("source/")
        self.events.append(f"transfer:{relative_path}")
        if self.fail_on_path == relative_path:
            raise RuntimeError("child failed")
        data = _payloads()[relative_path]
        assert kwargs["expected_source_size"] == len(data)
        assert kwargs["expected_source_fingerprint"].startswith("source-")
        result = TransferResult(
            len(data),
            hashlib.sha256(data).hexdigest(),
            etag=None if self.missing_metadata else f"destination-{relative_path}",
            created=None if self.missing_metadata else True,
        )
        if self.committed_cancel_path == relative_path:
            raise TransferCommittedAfterCancellation(result)
        return result


def _ticket(user_id, *, target: WorkspaceTarget, path: str) -> TransferPathTicket:
    return TransferPathTicket(user_id, path, target, path, 1024)


async def test_server_to_client_backend_serializes_authorization_before_each_child() -> None:
    manifest = _server_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="laptop",
        manifest=manifest,
        events=events,
    )
    fs = AsyncMock(spec=WorkspaceFS)
    source_lease = _SubtreeLease()
    fs.acquire_subtree_lease.return_value = source_lease
    fs.open_stream.side_effect = (
        _Stream(b"a", "source-a"),
        _Stream(b"bb", "source-b"),
    )
    operation_lease = _OperationLease()
    transfers = _Transfers(events)
    issued = 0

    def mark_issued() -> None:
        nonlocal issued
        issued += 1

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="copy",
        backend=ServerToClientDirectoryBackend(
            transfer_manager=cast(TransferManager, transfers),
            operation_lease=cast(TransferLease, operation_lease),
            workspace_fs=fs,
            source=_ticket(
                user_id,
                target=WorkspaceTarget.personal(user_id),
                path="source",
            ),
            destination=cast(DeviceDirectoryJobController, destination),
            destination_root="backup",
            manifest=manifest,
        ),
        operation_lease=operation_lease,
        on_issued=mark_issued,
    )

    assert result.files_transferred == 2
    assert issued == 1
    assert events.index("destination-authorize:a.txt") < events.index("transfer:a.txt")
    assert events.index("destination-authorize:nested/b.txt") < events.index(
        "transfer:nested/b.txt"
    )
    assert events[-1] == "destination-release"
    assert source_lease.release_count == 1
    assert operation_lease.close_count == 1


async def test_server_to_client_rechecks_source_fingerprint_before_each_open() -> None:
    manifest = _server_manifest()
    events: list[str] = []
    user_id = uuid4()
    destination = _Controller(
        operation_id=new_uuid7(),
        user_id=user_id,
        name="laptop",
        manifest=manifest,
        events=events,
    )
    fs = AsyncMock(spec=WorkspaceFS)
    fs.acquire_subtree_lease.return_value = _SubtreeLease()
    changed = _Stream(b"a", "source-a-changed")
    fs.open_stream.return_value = changed

    with pytest.raises(WorkspaceError) as caught:
        await DirectoryTransferCoordinator().run(
            manifest=manifest,
            mode="copy",
            backend=ServerToClientDirectoryBackend(
                transfer_manager=cast(TransferManager, _Transfers(events)),
                operation_lease=cast(TransferLease, _OperationLease()),
                workspace_fs=fs,
                source=_ticket(
                    user_id,
                    target=WorkspaceTarget.personal(user_id),
                    path="source",
                ),
                destination=cast(DeviceDirectoryJobController, destination),
                destination_root="backup",
                manifest=manifest,
            ),
            operation_lease=_OperationLease(),
        )

    assert caught.value.code is ErrorCode.WORKSPACE_FILE_CHANGED
    assert changed.closed is True
    assert "destination-cancel" in events


async def test_server_to_client_move_conditionally_cleans_manifest_source() -> None:
    manifest = _server_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="laptop",
        manifest=manifest,
        events=events,
    )
    target = WorkspaceTarget.personal(user_id)
    fs = AsyncMock(spec=WorkspaceFS)
    fs.acquire_subtree_lease.return_value = _SubtreeLease()
    fs.open_stream.side_effect = (
        _Stream(b"a", "source-a"),
        _Stream(b"bb", "source-b"),
    )
    fs.conditional_delete_file.side_effect = ("deleted", "mismatch")
    fs.directory_root_is_absent.return_value = False

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="move",
        backend=ServerToClientDirectoryBackend(
            transfer_manager=cast(TransferManager, _Transfers(events)),
            operation_lease=cast(TransferLease, _OperationLease()),
            workspace_fs=fs,
            source=_ticket(user_id, target=target, path="source"),
            destination=cast(DeviceDirectoryJobController, destination),
            destination_root="backup",
            manifest=manifest,
        ),
        operation_lease=_OperationLease(),
    )

    assert result.warnings == (
        "source_changed_after_copy",
        "source_cleanup_incomplete",
    )
    assert fs.conditional_delete_file.await_args_list[0].kwargs == {
        "expected_etag": "source-a",
        "subtree_owner": operation_id,
    }
    assert fs.conditional_delete_file.await_args_list[1].kwargs == {
        "expected_etag": "source-b",
        "subtree_owner": operation_id,
    }
    assert "destination-cancel" not in events


async def test_server_to_client_move_keeps_finalized_destination_alive_during_cleanup() -> None:
    manifest = _server_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    clock = _ManualClock()
    destination = _ExpiringFinalizedController(
        operation_id=operation_id,
        user_id=user_id,
        name="laptop",
        manifest=manifest,
        events=events,
        clock=clock,
        idle_timeout=1.0,
    )
    target = WorkspaceTarget.personal(user_id)
    fs = AsyncMock(spec=WorkspaceFS)
    fs.acquire_subtree_lease.return_value = _SubtreeLease()
    fs.open_stream.side_effect = (
        _Stream(b"a", "source-a"),
        _Stream(b"bb", "source-b"),
    )

    async def delete_file(*_args, **_kwargs) -> str:
        clock.advance(0.6)
        await asyncio.sleep(0)
        return "deleted"

    async def root_is_absent(*_args, **_kwargs) -> bool:
        clock.advance(0.6)
        await asyncio.sleep(0)
        return True

    fs.conditional_delete_file.side_effect = delete_file
    fs.directory_root_is_absent.side_effect = root_is_absent

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="move",
        backend=ServerToClientDirectoryBackend(
            transfer_manager=cast(TransferManager, _Transfers(events)),
            operation_lease=cast(TransferLease, _OperationLease()),
            workspace_fs=fs,
            source=_ticket(user_id, target=target, path="source"),
            destination=cast(DeviceDirectoryJobController, destination),
            destination_root="backup",
            manifest=manifest,
        ),
        operation_lease=_OperationLease(),
    )

    assert result.warnings == ()
    assert destination.forwarded_destination_progress == [1, 2, 3]
    assert destination.expired is False


async def test_client_to_server_backend_authorizes_before_begin_and_commits_serially() -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    source = _ProgressController(
        operation_id=new_uuid7(),
        user_id=user_id,
        name="laptop",
        manifest=manifest,
        events=events,
    )
    target = WorkspaceTarget.personal(user_id)
    destination = _ticket(user_id, target=target, path="backup")
    plan = DirectoryDestinationPlan(
        target=target,
        destination_root="backup",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("backup/a.txt", "backup/nested/b.txt"),
    )
    fs = AsyncMock(spec=WorkspaceFS)
    reservation = _QuotaReservation()

    async def preflight(*_args, on_progress):
        on_progress()
        await asyncio.sleep(0)
        on_progress()
        await asyncio.sleep(0)
        return plan

    async def reserve(*_args, on_progress, **_kwargs):
        on_progress()
        await asyncio.sleep(0)
        on_progress()
        await asyncio.sleep(0)
        return reservation

    async def verify(*_args, on_progress, **_kwargs):
        on_progress()
        await asyncio.sleep(0)
        on_progress()
        await asyncio.sleep(0)

    fs.preflight_directory_destination.side_effect = preflight
    fs.reserve_directory_quota.side_effect = reserve
    fs.verify_directory_destination.side_effect = verify
    subtree_lease = _SubtreeLease()
    fs.acquire_subtree_lease.return_value = subtree_lease
    fs.begin_directory_child_upload.side_effect = (
        (_Sink(), "temporary-a"),
        (_Sink(), "temporary-b"),
    )

    async def commit(_reservation, path, _temporary, *, size, on_issued):
        on_issued()
        return FileMetadata(size=size, etag=f"etag-{path}", created=True)

    fs.commit_directory_child_upload.side_effect = commit
    workspace = AsyncMock(spec=WorkspaceService)
    operation_lease = _OperationLease()
    transfers = _Transfers(events)

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="copy",
        backend=ClientToServerDirectoryBackend(
            transfer_manager=cast(TransferManager, transfers),
            operation_lease=cast(TransferLease, operation_lease),
            workspace_fs=fs,
            workspace_service=workspace,
            source=cast(DeviceDirectoryJobController, source),
            source_root="source",
            destination=destination,
            manifest=manifest,
        ),
        operation_lease=operation_lease,
    )

    assert result.files_transferred == 2
    assert events.index("source-authorize:a.txt") < events.index("transfer:a.txt")
    assert events.index("source-authorize:nested/b.txt") < events.index("transfer:nested/b.txt")
    fs.verify_directory_destination.assert_awaited_once()
    workspace.directory_transfer_committed.assert_called_once_with(destination)
    assert source.release_source_count == 1
    assert source.forwarded_source_progress == list(range(1, 9))
    assert reservation.release_count == 1
    assert subtree_lease.release_count == 1


async def test_client_to_server_failure_conditionally_removes_exact_commits() -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    source = _Controller(
        operation_id=new_uuid7(),
        user_id=user_id,
        name="laptop",
        manifest=manifest,
        events=events,
    )
    target = WorkspaceTarget.personal(user_id)
    destination = _ticket(user_id, target=target, path="backup")
    plan = DirectoryDestinationPlan(
        target=target,
        destination_root="backup",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("backup/a.txt", "backup/nested/b.txt"),
    )
    fs = AsyncMock(spec=WorkspaceFS)
    fs.preflight_directory_destination.return_value = plan
    reservation = _QuotaReservation()
    fs.reserve_directory_quota.return_value = reservation
    subtree_lease = _SubtreeLease()
    fs.acquire_subtree_lease.return_value = subtree_lease
    fs.begin_directory_child_upload.return_value = (_Sink(), "temporary-a")

    async def commit(_reservation, path, _temporary, *, size, on_issued):
        on_issued()
        return FileMetadata(size=size, etag=f"etag-{path}", created=True)

    fs.commit_directory_child_upload.side_effect = commit
    fs.conditional_delete_file.return_value = "deleted"
    fs.directory_root_is_absent.return_value = True
    transfers = _Transfers(events)
    transfers.fail_on_path = "nested/b.txt"

    with pytest.raises(RuntimeError, match="child failed"):
        await DirectoryTransferCoordinator().run(
            manifest=manifest,
            mode="copy",
            backend=ClientToServerDirectoryBackend(
                transfer_manager=cast(TransferManager, transfers),
                operation_lease=cast(TransferLease, _OperationLease()),
                workspace_fs=fs,
                workspace_service=AsyncMock(spec=WorkspaceService),
                source=cast(DeviceDirectoryJobController, source),
                source_root="source",
                destination=destination,
                manifest=manifest,
            ),
            operation_lease=_OperationLease(),
        )

    fs.conditional_delete_file.assert_awaited_once_with(
        target,
        "backup/a.txt",
        expected_etag="etag-backup/a.txt",
        subtree_owner=source.directory_operation_id,
    )
    fs.directory_root_is_absent.assert_awaited_once_with(
        target,
        "backup",
        owner=source.directory_operation_id,
    )
    assert reservation.release_count == 1
    assert subtree_lease.release_count == 1


async def test_client_to_client_requires_both_authorizations_before_begin() -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )
    transfers = _Transfers(events)
    operation_lease = _OperationLease()

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="copy",
        backend=ClientToClientDirectoryBackend(
            transfer_manager=cast(TransferManager, transfers),
            operation_lease=cast(TransferLease, operation_lease),
            source=cast(DeviceDirectoryJobController, source),
            source_root="source",
            destination=cast(DeviceDirectoryJobController, destination),
            destination_root="backup",
            manifest=manifest,
        ),
        operation_lease=operation_lease,
    )

    assert result.files_transferred == 2
    for path in ("a.txt", "nested/b.txt"):
        assert events.index(f"source-authorize:{path}") < events.index(f"transfer:{path}")
        assert events.index(f"destination-authorize:{path}") < events.index(f"transfer:{path}")


async def test_server_to_client_preserves_commit_metadata_across_cancellation() -> None:
    manifest = _server_manifest()
    events: list[str] = []
    user_id = uuid4()
    destination = _Controller(
        operation_id=new_uuid7(),
        user_id=user_id,
        name="laptop",
        manifest=manifest,
        events=events,
    )
    fs = AsyncMock(spec=WorkspaceFS)
    fs.open_stream.return_value = _Stream(b"a", "source-a")
    transfers = _Transfers(events)
    transfers.committed_cancel_path = "a.txt"
    backend = ServerToClientDirectoryBackend(
        transfer_manager=cast(TransferManager, transfers),
        operation_lease=cast(TransferLease, _OperationLease()),
        workspace_fs=fs,
        source=_ticket(
            user_id,
            target=WorkspaceTarget.personal(user_id),
            path="source",
        ),
        destination=cast(DeviceDirectoryJobController, destination),
        destination_root="backup",
        manifest=manifest,
    )

    with pytest.raises(DirectoryChildCommittedAfterCancellation) as caught:
        await backend.copy_child(manifest.entries[0], new_uuid7(), lambda: None)

    assert caught.value.result.destination_fingerprint == "destination-a.txt"
    assert caught.value.result.verified_sha256 == hashlib.sha256(b"a").hexdigest()


async def test_client_to_client_preserves_commit_metadata_across_cancellation() -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )
    transfers = _Transfers(events)
    transfers.committed_cancel_path = "a.txt"
    backend = ClientToClientDirectoryBackend(
        transfer_manager=cast(TransferManager, transfers),
        operation_lease=cast(TransferLease, _OperationLease()),
        source=cast(DeviceDirectoryJobController, source),
        source_root="source",
        destination=cast(DeviceDirectoryJobController, destination),
        destination_root="backup",
        manifest=manifest,
    )

    with pytest.raises(DirectoryChildCommittedAfterCancellation) as caught:
        await backend.copy_child(manifest.entries[0], new_uuid7(), lambda: None)

    assert caught.value.result.destination_fingerprint == "destination-a.txt"
    assert caught.value.result.verified_sha256 == hashlib.sha256(b"a").hexdigest()


async def test_client_to_server_preserves_commit_metadata_across_cancellation() -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    source = _Controller(
        operation_id=new_uuid7(),
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    target = WorkspaceTarget.personal(user_id)
    destination = _ticket(user_id, target=target, path="backup")
    plan = DirectoryDestinationPlan(
        target=target,
        destination_root="backup",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("backup/a.txt", "backup/nested/b.txt"),
    )
    fs = AsyncMock(spec=WorkspaceFS)
    fs.preflight_directory_destination.return_value = plan
    fs.reserve_directory_quota.return_value = _QuotaReservation()
    fs.acquire_subtree_lease.return_value = _SubtreeLease()
    fs.begin_directory_child_upload.return_value = (_Sink(), "temporary-a")

    async def commit(_reservation, path, _temporary, *, size, on_issued):
        on_issued()
        return FileMetadata(size=size, etag=f"etag-{path}", created=True)

    fs.commit_directory_child_upload.side_effect = commit
    transfers = _Transfers(events)
    transfers.committed_cancel_path = "a.txt"
    backend = ClientToServerDirectoryBackend(
        transfer_manager=cast(TransferManager, transfers),
        operation_lease=cast(TransferLease, _OperationLease()),
        workspace_fs=fs,
        workspace_service=AsyncMock(spec=WorkspaceService),
        source=cast(DeviceDirectoryJobController, source),
        source_root="source",
        destination=destination,
        manifest=manifest,
    )
    await backend.preflight(manifest)
    await backend.prepare_destination(lambda: None)

    with pytest.raises(DirectoryChildCommittedAfterCancellation) as caught:
        await backend.copy_child(manifest.entries[0], new_uuid7(), lambda: None)

    assert caught.value.result.destination_fingerprint == "etag-backup/a.txt"
    assert caught.value.result.verified_sha256 == hashlib.sha256(b"a").hexdigest()
    await backend.release()


@pytest.mark.parametrize("failed_endpoint", ["source", "destination"])
async def test_lost_child_authorization_never_sends_child_begin(
    failed_endpoint: str,
) -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )
    source.fail_source_authorization = failed_endpoint == "source"
    destination.fail_destination_authorization = failed_endpoint == "destination"

    with pytest.raises(DeviceOutcomeUnknownError):
        await DirectoryTransferCoordinator().run(
            manifest=manifest,
            mode="copy",
            backend=ClientToClientDirectoryBackend(
                transfer_manager=cast(TransferManager, _Transfers(events)),
                operation_lease=cast(TransferLease, _OperationLease()),
                source=cast(DeviceDirectoryJobController, source),
                source_root="source",
                destination=cast(DeviceDirectoryJobController, destination),
                destination_root="backup",
                manifest=manifest,
            ),
            operation_lease=_OperationLease(),
        )

    assert not any(event.startswith("transfer:") for event in events)


@pytest.mark.parametrize("failure_point", ["destination_authorization", "initial_send"])
async def test_failed_client_child_retires_authorizations_without_waiting_for_idle_expiry(
    failure_point: str,
) -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _StatefulSourceController(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )
    transfers = _Transfers(events)
    destination.fail_destination_authorization = failure_point == "destination_authorization"
    transfers.fail_on_path = "a.txt" if failure_point == "initial_send" else None
    operation_lease = _OperationLease()

    expected_error = (
        DeviceOutcomeUnknownError if failure_point == "destination_authorization" else RuntimeError
    )
    with pytest.raises(expected_error):
        await DirectoryTransferCoordinator().run(
            manifest=manifest,
            mode="copy",
            backend=ClientToClientDirectoryBackend(
                transfer_manager=cast(TransferManager, transfers),
                operation_lease=cast(TransferLease, operation_lease),
                source=cast(DeviceDirectoryJobController, source),
                source_root="source",
                destination=cast(DeviceDirectoryJobController, destination),
                destination_root="backup",
                manifest=manifest,
            ),
            operation_lease=operation_lease,
        )

    assert destination.destination_cancelled is True
    assert source.source_authorization_pending is False
    assert source.release_source_count == 1
    assert operation_lease.close_count == 1
    if failure_point == "destination_authorization":
        assert not any(event.startswith("source-authorize:") for event in events)
        assert source.source_cancel_count == 0
    else:
        assert events.index("destination-authorize:a.txt") < events.index("source-authorize:a.txt")
        assert source.source_cancel_count == 1
        assert events.index("source-cancel") < events.index("source-release")


@pytest.mark.parametrize(
    ("cleanup_complete", "expected_error"),
    [
        (True, TransferIntegrityError),
        (False, DeviceOutcomeUnknownError),
    ],
)
async def test_client_ack_without_directory_commit_metadata_triggers_cleanup(
    cleanup_complete: bool,
    expected_error: type[BaseException],
) -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )
    destination.cleanup_complete = cleanup_complete
    transfers = _Transfers(events)
    transfers.missing_metadata = True

    with pytest.raises(expected_error):
        await DirectoryTransferCoordinator().run(
            manifest=manifest,
            mode="copy",
            backend=ClientToClientDirectoryBackend(
                transfer_manager=cast(TransferManager, transfers),
                operation_lease=cast(TransferLease, _OperationLease()),
                source=cast(DeviceDirectoryJobController, source),
                source_root="source",
                destination=cast(DeviceDirectoryJobController, destination),
                destination_root="backup",
                manifest=manifest,
            ),
            operation_lease=_OperationLease(),
        )

    assert "destination-cancel" in events


async def test_client_move_keeps_destination_and_aggregates_source_warnings() -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    source.source_warnings = ["source_changed_after_copy", "source_cleanup_incomplete"]
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="move",
        backend=ClientToClientDirectoryBackend(
            transfer_manager=cast(TransferManager, _Transfers(events)),
            operation_lease=cast(TransferLease, _OperationLease()),
            source=cast(DeviceDirectoryJobController, source),
            source_root="source",
            destination=cast(DeviceDirectoryJobController, destination),
            destination_root="backup",
            manifest=manifest,
        ),
        operation_lease=_OperationLease(),
    )

    assert result.warnings == (
        "source_changed_after_copy",
        "source_cleanup_incomplete",
    )
    assert events.index("source-cleanup") < events.index("destination-release")
    assert "destination-cancel" not in events


@pytest.mark.parametrize(
    ("mode", "expected_warnings"),
    [
        ("copy", ()),
        ("move", ("source_cleanup_incomplete",)),
    ],
)
async def test_cancellation_after_client_destination_finalize_returns_success(
    mode: Literal["copy", "move"],
    expected_warnings: tuple[str, ...],
) -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    destination = _BlockingFinalizeController(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )
    operation_lease = _OperationLease()
    task = asyncio.create_task(
        DirectoryTransferCoordinator().run(
            manifest=manifest,
            mode=mode,
            backend=ClientToClientDirectoryBackend(
                transfer_manager=cast(TransferManager, _Transfers(events)),
                operation_lease=cast(TransferLease, operation_lease),
                source=cast(DeviceDirectoryJobController, source),
                source_root="source",
                destination=cast(DeviceDirectoryJobController, destination),
                destination_root="backup",
                manifest=manifest,
            ),
            operation_lease=operation_lease,
        )
    )
    await destination.finish_started.wait()

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    destination.release_finish.set()

    result = await task
    assert result.files_transferred == 2
    assert result.warnings == expected_warnings
    assert "destination-cancel" not in events
    assert "source-cleanup" not in events
    assert source.release_source_count == 1
    assert destination.release_destination_count == 1
    assert operation_lease.close_count == 1


async def test_client_release_loss_after_finalized_destination_keeps_success() -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    destination = _Controller(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )
    destination.fail_destination_release = True
    operation_lease = _OperationLease()

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="copy",
        backend=ClientToClientDirectoryBackend(
            transfer_manager=cast(TransferManager, _Transfers(events)),
            operation_lease=cast(TransferLease, operation_lease),
            source=cast(DeviceDirectoryJobController, source),
            source_root="source",
            destination=cast(DeviceDirectoryJobController, destination),
            destination_root="backup",
            manifest=manifest,
        ),
        operation_lease=operation_lease,
    )

    assert result.files_transferred == 2
    assert result.warnings == ("transfer_ack_failed",)
    assert source.release_source_count == 1
    assert destination.release_destination_count == 1
    assert operation_lease.close_count == 1


async def test_client_to_client_forwards_only_observed_cross_side_progress() -> None:
    manifest = _client_manifest()
    events: list[str] = []
    user_id = uuid4()
    operation_id = new_uuid7()
    source = _ProgressController(
        operation_id=operation_id,
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    destination = _ProgressController(
        operation_id=operation_id,
        user_id=user_id,
        name="destination-client",
        manifest=manifest,
        events=events,
    )
    operation_lease = _OperationLease()

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="move",
        backend=ClientToClientDirectoryBackend(
            transfer_manager=cast(TransferManager, _Transfers(events)),
            operation_lease=cast(TransferLease, operation_lease),
            source=cast(DeviceDirectoryJobController, source),
            source_root="source",
            destination=cast(DeviceDirectoryJobController, destination),
            destination_root="backup",
            manifest=manifest,
        ),
        operation_lease=operation_lease,
    )

    assert result.warnings == ()
    assert source.forwarded_source_progress == [1, 2, 3, 4, 5, 6]
    assert destination.forwarded_destination_progress == [7, 8]
    assert operation_lease.close_count == 1


async def test_invalid_client_skill_prevalidation_never_commits_destination() -> None:
    content = b"not a valid skill"
    manifest = create_directory_manifest(
        root_identity="root-v1",
        directories=(DirectoryManifestDirectory(relative_path="demo", identity="demo-v1"),),
        entries=(
            DirectoryManifestEntry(
                relative_path="demo/SKILL.md",
                size=len(content),
                fingerprint="skill-v1",
            ),
        ),
    )
    events: list[str] = []
    user_id = uuid4()
    source = _Controller(
        operation_id=new_uuid7(),
        user_id=user_id,
        name="source-client",
        manifest=manifest,
        events=events,
    )
    target = WorkspaceTarget.personal(user_id)
    destination = _ticket(user_id, target=target, path="skills")
    plan = DirectoryDestinationPlan(
        target=target,
        destination_root="skills",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("skills/demo/SKILL.md",),
    )
    fs = AsyncMock(spec=WorkspaceFS)
    fs.preflight_directory_destination.return_value = plan
    fs.reserve_directory_quota.return_value = _QuotaReservation()
    fs.begin_directory_validation_staging = Mock(
        return_value=(_Sink(), "_openoctopus-transfers/00000000000000000000000000000000")
    )
    workspace = AsyncMock(spec=WorkspaceService)

    async def validate_selected(_destination, _manifest, _plan, *, validate_source):
        await validate_source(manifest.entries[0], "skills/demo/SKILL.md")

    workspace.validate_client_directory_skill_manifests.side_effect = validate_selected
    workspace.validate_staged_directory_skill_manifest.side_effect = WorkspaceError(
        ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT,
        "invalid skill",
    )
    transfers = _Transfers(events)
    transfers.start_client_to_server_admitted = _skill_transfer(content, transfers.events)
    issued = 0

    def mark_issued() -> None:
        nonlocal issued
        issued += 1

    with pytest.raises(WorkspaceError) as caught:
        await DirectoryTransferCoordinator().run(
            manifest=manifest,
            mode="copy",
            backend=ClientToServerDirectoryBackend(
                transfer_manager=cast(TransferManager, transfers),
                operation_lease=cast(TransferLease, _OperationLease()),
                workspace_fs=fs,
                workspace_service=workspace,
                source=cast(DeviceDirectoryJobController, source),
                source_root="source",
                destination=destination,
                manifest=manifest,
            ),
            operation_lease=_OperationLease(),
            on_issued=mark_issued,
        )

    assert caught.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT
    assert issued == 0
    fs.commit_directory_child_upload.assert_not_awaited()
    fs.acquire_subtree_lease.assert_not_awaited()
    fs.delete_directory_validation_staging.assert_awaited_once()
    assert events == [
        "source-authorize:demo/SKILL.md",
        "skill-transfer",
        "source-cancel",
        "source-release",
    ]


def _skill_transfer(content: bytes, events: list[str]):
    async def transfer(**kwargs: Any) -> TransferResult:
        events.append("skill-transfer")
        begin = TransferBeginFrame(
            id=kwargs["slot_id"],
            direction="client_to_server",
            purpose="file_transfer",
            src_path=kwargs["src_path"],
            dst_path=kwargs["dst_path"],
            total_bytes=len(content),
            etag="skill-v1",
        )
        sink = await kwargs["sink_factory"](begin)
        await sink.write(content)
        await sink.finish()
        await kwargs["commit_sink"](
            sink,
            begin,
            len(content),
            hashlib.sha256(content).hexdigest(),
        )
        raise AssertionError("invalid Skill validation must fail")

    return transfer
