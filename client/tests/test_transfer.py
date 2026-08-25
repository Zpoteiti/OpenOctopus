from __future__ import annotations

import asyncio
import errno
import hashlib
import json
import os
import subprocess
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest
from pydantic import ValidationError

import openoctopus_client.transfer as transfer_module
from openoctopus_client.protocol import (
    ProtocolError,
    TransferBegin,
    TransferEnd,
    TransferReady,
    TransferRequest,
    decode_binary_chunk,
    encode_binary_chunk,
    new_uuid7,
)
from openoctopus_client.tools.common import ToolFailure
from openoctopus_client.tools.fingerprints import opaque_stat_fingerprint
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.tools.paths import WorkspacePaths
from openoctopus_client.transfer import (
    TOMBSTONE_MAX_ENTRIES,
    TransferConfigSnapshot,
    TransferManager,
    TransferState,
)
from openoctopus_client.transfer_admission import (
    LocalTransferAdmission,
    LocalTransferDrainRegistry,
)
from openoctopus_client.writer import SerializedWriter, WriterOverflowError

SLOT = UUID("0190d5a7-0000-7000-8000-000000000002")
SLOT_2 = UUID("0190d5a7-0000-7000-8000-000000000003")


class Socket:
    def __init__(self) -> None:
        self.sent: list[str | bytes] = []

    async def send(self, payload: str | bytes) -> None:
        self.sent.append(payload)


async def _manager(
    workspace: Path,
    *,
    admission: LocalTransferAdmission | None = None,
    drains: LocalTransferDrainRegistry | None = None,
    path_locks: PathLocks | None = None,
    directory_managers: Any = None,
) -> tuple[TransferManager, SerializedWriter, Socket, asyncio.Task[None]]:
    socket = Socket()
    writer = SerializedWriter()
    writer_task = asyncio.create_task(writer.run(socket))
    return (
        TransferManager(
            workspace,
            writer,
            path_locks=path_locks,
            admission=admission,
            drain_registry=drains,
            directory_managers=directory_managers,
        ),
        writer,
        socket,
        writer_task,
    )


async def _stop(
    manager: TransferManager, writer: SerializedWriter, writer_task: asyncio.Task[None]
) -> None:
    await manager.shutdown()
    await writer.stop()
    await writer_task


async def _wait_receiver_ready(manager: TransferManager, slot_id: UUID = SLOT) -> None:
    for _ in range(100):
        if manager.slot_state(slot_id) is TransferState.READY:
            return
        await asyncio.sleep(0.001)
    raise AssertionError(f"receiver slot {slot_id} did not become ready")


async def _wait_slot_closed(manager: TransferManager, slot_id: UUID) -> None:
    async with asyncio.timeout(5):
        while manager.slot_state(slot_id) is not None:
            await asyncio.sleep(0.001)


async def _wait_for_transfer_end(
    socket: Socket,
    slot_id: UUID,
    *,
    ack: bool,
    ok: bool,
    code: str | None = None,
) -> dict[str, object]:
    async with asyncio.timeout(5):
        while True:
            for payload in socket.sent:
                if not isinstance(payload, str):
                    continue
                frame = cast(dict[str, object], json.loads(payload))
                if (
                    frame.get("type") == "transfer_end"
                    and frame.get("id") == str(slot_id)
                    and frame.get("ack") is ack
                    and frame.get("ok") is ok
                    and (code is None or frame.get("code") == code)
                ):
                    return frame
            await asyncio.sleep(0.001)


def _make_directory_link(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
        return
    except OSError as exc:
        if os.name != "nt" or getattr(exc, "winerror", None) != 1314:
            raise
    cmd = Path(os.environ.get("SystemRoot", r"C:\Windows"), "System32", "cmd.exe")
    completed = subprocess.run(
        [str(cmd), "/D", "/C", "mklink", "/J", str(link), str(target)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
        timeout=5,
    )
    if completed.returncode != 0:
        raise OSError("unable to create a Windows directory junction")


def _fingerprint(path: Path) -> str:
    info = path.stat(follow_symlinks=False)
    return opaque_stat_fingerprint((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns))


class _DirectoryTransferHooks:
    def __init__(
        self,
        *,
        source_id: UUID | None = None,
        destination_id: UUID | None = None,
        source_path: Path | None = None,
        destination_path: Path | None = None,
        expected_size: int | None = None,
        fingerprint: str | None = None,
        commit_started: asyncio.Event | None = None,
        release_commit: asyncio.Event | None = None,
    ) -> None:
        self.operation_id = new_uuid7()
        self.source_id = source_id
        self.destination_id = destination_id
        self.source_path = source_path
        self.destination_path = destination_path
        self.expected_size = expected_size
        self.fingerprint = fingerprint
        self.commit_started = commit_started
        self.release_commit = release_commit
        self.source_progress: list[int] = []
        self.destination_progress: list[int] = []
        self.source_terminal: list[bool] = []
        self.destination_terminal: list[bool] = []
        self.source_completed = asyncio.Event()
        self.destination_completed = asyncio.Event()
        self.commits: list[dict[str, object]] = []

    def claims_source_transfer(self, transfer_uuid: UUID) -> bool:
        return transfer_uuid == self.source_id

    def claims_destination_transfer(self, transfer_uuid: UUID) -> bool:
        return transfer_uuid == self.destination_id

    async def consume_source_authorization(self, transfer_uuid: UUID, source_path: Path) -> object:
        if transfer_uuid != self.source_id or source_path != self.source_path:
            raise ToolFailure(
                "workspace_transfer_integrity_failed", "Source child authorization mismatched"
            )
        assert self.fingerprint is not None
        return SimpleNamespace(
            directory_operation_id=self.operation_id,
            transfer_uuid=transfer_uuid,
            source_path=source_path,
            relative_path=source_path.name,
            fingerprint=self.fingerprint,
        )

    async def report_source_child_progress(
        self, transfer_uuid: UUID, *, byte_count: int = 0
    ) -> None:
        assert transfer_uuid == self.source_id
        self.source_progress.append(byte_count)

    async def complete_source_authorization(self, transfer_uuid: UUID, *, success: bool) -> None:
        assert transfer_uuid == self.source_id
        self.source_terminal.append(success)
        self.source_completed.set()

    async def consume_destination_authorization(self, transfer_uuid: UUID) -> object:
        if transfer_uuid != self.destination_id:
            raise ToolFailure(
                "workspace_transfer_integrity_failed",
                "Destination child authorization mismatched",
            )
        assert self.destination_path is not None
        assert self.expected_size is not None
        return SimpleNamespace(
            directory_operation_id=self.operation_id,
            transfer_uuid=transfer_uuid,
            destination_path=self.destination_path,
            relative_path=self.destination_path.name,
            expected_size=self.expected_size,
        )

    async def report_destination_child_progress(
        self, transfer_uuid: UUID, *, byte_count: int = 0
    ) -> None:
        assert transfer_uuid == self.destination_id
        self.destination_progress.append(byte_count)

    def validate_destination_child_parent(self, transfer_uuid: UUID) -> None:
        assert transfer_uuid == self.destination_id

    async def complete_destination_authorization(
        self, transfer_uuid: UUID, *, success: bool
    ) -> None:
        assert transfer_uuid == self.destination_id
        self.destination_terminal.append(success)
        self.destination_completed.set()

    async def record_destination_commit(
        self,
        directory_operation_id: UUID,
        transfer_uuid: UUID,
        **metadata: object,
    ) -> None:
        assert directory_operation_id == self.operation_id
        assert transfer_uuid == self.destination_id
        if self.commit_started is not None:
            self.commit_started.set()
        if self.release_commit is not None:
            await self.release_commit.wait()
        self.commits.append(dict(metadata))
        self.destination_terminal.append(True)
        self.destination_completed.set()


def test_binary_header_is_exactly_bounded_and_uuidv7_checked() -> None:
    payload = encode_binary_chunk(SLOT, b"abc")
    assert decode_binary_chunk(payload) == (SLOT, b"abc")
    with pytest.raises(ProtocolError):
        decode_binary_chunk(payload + b"x" * (64 * 1024))
    with pytest.raises(ProtocolError):
        decode_binary_chunk(b"short")


def test_transfer_metadata_is_purpose_scoped_and_terminal_metadata_is_ack_only() -> None:
    etag = "a" * 64
    with pytest.raises(ValidationError):
        TransferBegin(
            id=SLOT,
            direction="server_to_client",
            purpose="workspace_upload",
            dst_path="result.txt",
            total_bytes=1,
            etag=etag,
        )
    with pytest.raises(ValidationError):
        TransferBegin(
            id=SLOT,
            direction="server_to_client",
            purpose="workspace_upload",
            dst_path="result.txt",
            total_bytes=1,
            if_match=etag,
            if_none_match=True,
        )
    begin = TransferBegin(
        id=SLOT,
        direction="client_to_server",
        purpose="file_transfer",
        src_path="source.txt",
        dst_path="result.txt",
        total_bytes=1,
        etag=etag,
    )
    assert begin.etag == etag
    with pytest.raises(ValidationError):
        TransferEnd(
            id=SLOT,
            ack=False,
            ok=True,
            bytes_sent=1,
            sha256="a" * 64,
            etag=etag,
            created=True,
        )
    with pytest.raises(ValidationError):
        TransferEnd(
            id=SLOT,
            ack=True,
            ok=False,
            code="workspace_file_changed",
            etag=etag,
            created=True,
        )


def test_client_tombstones_are_bounded_and_evict_oldest(tmp_path: Path) -> None:
    async def exercise() -> tuple[UUID, UUID, int]:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            first = new_uuid7()
            for index in range(TOMBSTONE_MAX_ENTRIES + 1):
                slot_id = first if index == 0 else new_uuid7()
                manager._remember_tombstone(
                    slot_id,
                    TransferEnd(id=slot_id, ack=False, ok=False, code="cancelled"),
                )
            last = next(reversed(manager._tombstones))
            return first, last, manager.tombstone_count
        finally:
            await _stop(manager, writer, writer_task)

    first, last, count = asyncio.run(exercise())
    assert count == TOMBSTONE_MAX_ENTRIES
    assert first != last


def test_third_local_transfer_start_is_rejected_without_allocating_a_slot(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        third_slot = new_uuid7()
        try:
            for slot_id, destination in (
                (SLOT, "first.bin"),
                (SLOT_2, "second.bin"),
                (third_slot, "third.bin"),
            ):
                await manager.handle_control(
                    TransferBegin(
                        id=slot_id,
                        direction="server_to_client",
                        purpose="file_transfer",
                        src_path="source.bin",
                        dst_path=destination,
                        total_bytes=1,
                    )
                )
            await writer.drain()

            assert manager.active_count == 2
            assert manager.active_slot_ids == {SLOT, SLOT_2}
            rejection = next(
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str)
                and json.loads(item).get("type") == "transfer_end"
                and json.loads(item).get("id") == str(third_slot)
            )
            assert rejection == {
                "type": "transfer_end",
                "id": str(third_slot),
                "ack": False,
                "ok": False,
                "code": "tool_device_busy",
            }
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_wire_managers_share_runtime_local_transfer_capacity(tmp_path: Path) -> None:
    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=2)
        drains = LocalTransferDrainRegistry()
        first = await _manager(tmp_path, admission=admission, drains=drains)
        second = await _manager(tmp_path, admission=admission, drains=drains)
        first_manager, first_writer, _, first_writer_task = first
        second_manager, second_writer, second_socket, second_writer_task = second
        third_slot = new_uuid7()
        try:
            await first_manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="first.bin",
                    total_bytes=1,
                )
            )
            await second_manager.handle_control(
                TransferBegin(
                    id=SLOT_2,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="second.bin",
                    total_bytes=1,
                )
            )
            await _wait_receiver_ready(first_manager, SLOT)
            await _wait_receiver_ready(second_manager, SLOT_2)

            await second_manager.handle_control(
                TransferBegin(
                    id=third_slot,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="third.bin",
                    total_bytes=1,
                )
            )
            await second_writer.drain()

            assert admission.active_count == 2
            assert second_manager.active_slot_ids == {SLOT_2}
            rejection = next(
                json.loads(item)
                for item in second_socket.sent
                if isinstance(item, str) and json.loads(item).get("id") == str(third_slot)
            )
            assert rejection["code"] == "tool_device_busy"
        finally:
            await _stop(first_manager, first_writer, first_writer_task)
            await _stop(second_manager, second_writer, second_writer_task)
        assert await drains.wait(timeout_seconds=1)
        assert admission.active_count == 0

    asyncio.run(exercise())


def test_directory_source_child_consumes_authorization_and_reports_progress(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        payload = b"directory child payload"
        source = tmp_path / "child.bin"
        source.write_bytes(payload)
        hooks = _DirectoryTransferHooks(
            source_id=SLOT,
            source_path=source,
            fingerprint=_fingerprint(source),
        )
        manager, writer, socket, writer_task = await _manager(
            tmp_path, directory_managers=lambda: (hooks,)
        )
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="child.bin",
                    dst_path="copy.bin",
                )
            )
            async with asyncio.timeout(5):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            sent_end = await _wait_for_transfer_end(socket, SLOT, ack=False, ok=True)
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=True,
                    bytes_sent=cast(int, sent_end["bytes_sent"]),
                    sha256=cast(str, sent_end["sha256"]),
                )
            )
            await _wait_slot_closed(manager, SLOT)
            await asyncio.wait_for(hooks.source_completed.wait(), timeout=1)

            assert sum(hooks.source_progress) == len(payload)
            assert hooks.source_terminal == [True]
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_directory_source_child_rejects_a_changed_opened_fingerprint(tmp_path: Path) -> None:
    async def exercise() -> None:
        source = tmp_path / "child.bin"
        source.write_bytes(b"new version")
        hooks = _DirectoryTransferHooks(
            source_id=SLOT,
            source_path=source,
            fingerprint="stale-fingerprint",
        )
        manager, writer, socket, writer_task = await _manager(
            tmp_path, directory_managers=lambda: (hooks,)
        )
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="child.bin",
                    dst_path="copy.bin",
                )
            )
            end = await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_file_changed",
            )
            await _wait_slot_closed(manager, SLOT)
            await asyncio.wait_for(hooks.source_completed.wait(), timeout=1)
            assert end["code"] == "workspace_file_changed"
            assert not any(
                isinstance(item, str) and json.loads(item).get("type") == "transfer_begin"
                for item in socket.sent
            )
            assert hooks.source_terminal == [False]
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_directory_destination_child_bypasses_owner_subtree_and_returns_commit_metadata(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        destination_root = tmp_path / "reserved"
        destination_root.mkdir()
        destination = destination_root / "child.bin"
        payload = b"destination child payload"
        digest = hashlib.sha256(payload).hexdigest()
        hooks = _DirectoryTransferHooks(
            destination_id=SLOT,
            destination_path=destination,
            expected_size=len(payload),
        )
        locks = PathLocks()
        manager, writer, socket, writer_task = await _manager(
            tmp_path,
            path_locks=locks,
            directory_managers=lambda: (hooks,),
        )
        try:
            async with locks.reserve_subtree(hooks.operation_id, str(destination_root)):
                await manager.handle_control(
                    TransferBegin(
                        id=SLOT,
                        direction="server_to_client",
                        purpose="file_transfer",
                        src_path="source.bin",
                        dst_path="reserved/child.bin",
                        total_bytes=len(payload),
                    )
                )
                await _wait_receiver_ready(manager)
                await manager.handle_binary(encode_binary_chunk(SLOT, payload))
                await manager.handle_control(
                    TransferEnd(
                        id=SLOT,
                        ack=False,
                        ok=True,
                        bytes_sent=len(payload),
                        sha256=digest,
                    )
                )
                ack = await _wait_for_transfer_end(socket, SLOT, ack=True, ok=True)

            assert destination.read_bytes() == payload
            assert ack["created"] is True
            assert isinstance(ack["etag"], str)
            assert hooks.destination_progress == [len(payload)]
            assert hooks.destination_terminal == [True]
            assert hooks.commits == [
                {
                    "relative_path": "child.bin",
                    "destination_fingerprint": ack["etag"],
                    "verified_size": len(payload),
                    "verified_sha256": digest,
                }
            ]
            await _wait_slot_closed(manager, SLOT)
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/child.bin",
                    total_bytes=len(payload),
                )
            )
            await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_transfer_integrity_failed",
            )
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_directory_destination_authorization_path_mismatch_is_rejected(tmp_path: Path) -> None:
    async def exercise() -> None:
        expected = tmp_path / "reserved" / "expected.bin"
        expected.parent.mkdir()
        hooks = _DirectoryTransferHooks(
            destination_id=SLOT,
            destination_path=expected,
            expected_size=1,
        )
        manager, writer, socket, writer_task = await _manager(
            tmp_path, directory_managers=lambda: (hooks,)
        )
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/wrong.bin",
                    total_bytes=1,
                )
            )
            failure = await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_transfer_integrity_failed",
            )
            assert not (tmp_path / "reserved" / "wrong.bin").exists()
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=False,
                    code=cast(str, failure["code"]),
                )
            )
            await _wait_slot_closed(manager, SLOT)
            await asyncio.wait_for(hooks.destination_completed.wait(), timeout=1)
            assert hooks.destination_terminal == [False]
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_directory_destination_parent_validation_runs_before_parent_creation(
    tmp_path: Path,
) -> None:
    class RejectingHooks(_DirectoryTransferHooks):
        def validate_destination_child_parent(self, transfer_uuid: UUID) -> None:
            super().validate_destination_child_parent(transfer_uuid)
            raise ToolFailure("workspace_file_changed", "Destination parent changed")

    async def exercise() -> None:
        destination = tmp_path / "reserved" / "nested" / "child.bin"
        hooks = RejectingHooks(
            destination_id=SLOT,
            destination_path=destination,
            expected_size=1,
        )
        manager, writer, socket, writer_task = await _manager(
            tmp_path, directory_managers=lambda: (hooks,)
        )
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/nested/child.bin",
                    total_bytes=1,
                )
            )
            failure = await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_file_changed",
            )
            assert not destination.parent.exists()
            assert not tuple(tmp_path.rglob("*.tmp"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=False,
                    code=cast(str, failure["code"]),
                )
            )
            await _wait_slot_closed(manager, SLOT)
            await asyncio.wait_for(hooks.destination_completed.wait(), timeout=1)
            assert hooks.destination_terminal == [False]
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


@pytest.mark.parametrize("declared_size", [None, 3])
def test_directory_destination_rejects_missing_or_wrong_manifest_size_before_fs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    declared_size: int | None,
) -> None:
    original_resolve = transfer_module._resolve_destination
    resolution_attempted = threading.Event()

    def tracked_resolve(
        snapshot: TransferConfigSnapshot, destination_path: str
    ) -> tuple[WorkspacePaths, Path]:
        resolution_attempted.set()
        return original_resolve(snapshot, destination_path)

    monkeypatch.setattr(transfer_module, "_resolve_destination", tracked_resolve)

    async def exercise() -> None:
        destination = tmp_path / "reserved" / "child.bin"
        hooks = _DirectoryTransferHooks(
            destination_id=SLOT,
            destination_path=destination,
            expected_size=4,
        )
        manager, writer, socket, writer_task = await _manager(
            tmp_path, directory_managers=lambda: (hooks,)
        )
        try:
            begin = (
                TransferBegin.model_construct(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/child.bin",
                    total_bytes=None,
                )
                if declared_size is None
                else TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/child.bin",
                    total_bytes=declared_size,
                )
            )
            await manager.handle_control(begin)
            failure = await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_transfer_integrity_failed",
            )
            assert not destination.parent.exists()
            assert resolution_attempted.is_set() is False
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=False,
                    code=cast(str, failure["code"]),
                )
            )
            await _wait_slot_closed(manager, SLOT)
            await asyncio.wait_for(hooks.destination_completed.wait(), timeout=1)
            assert hooks.destination_terminal == [False]
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_directory_destination_publish_records_commit_before_propagating_cancellation(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        destination_root = tmp_path / "reserved"
        destination_root.mkdir()
        destination = destination_root / "child.bin"
        payload = b"committed before cancellation"
        digest = hashlib.sha256(payload).hexdigest()
        commit_started = asyncio.Event()
        release_commit = asyncio.Event()
        hooks = _DirectoryTransferHooks(
            destination_id=SLOT,
            destination_path=destination,
            expected_size=len(payload),
            commit_started=commit_started,
            release_commit=release_commit,
        )
        manager, writer, _socket, writer_task = await _manager(
            tmp_path, directory_managers=lambda: (hooks,)
        )
        shutdown: asyncio.Task[None] | None = None
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/child.bin",
                    total_bytes=len(payload),
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, payload))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=len(payload),
                    sha256=digest,
                )
            )
            await asyncio.wait_for(commit_started.wait(), timeout=1)
            assert destination.read_bytes() == payload

            shutdown = asyncio.create_task(manager.shutdown())
            await asyncio.sleep(0)
            assert not shutdown.done()
            release_commit.set()
            await asyncio.wait_for(shutdown, timeout=1)

            assert hooks.destination_terminal == [True]
            assert hooks.commits[0]["verified_sha256"] == digest
        finally:
            release_commit.set()
            if shutdown is not None:
                await asyncio.gather(shutdown, return_exceptions=True)
            await writer.stop()
            await writer_task

    asyncio.run(exercise())


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory fsync semantics")
def test_directory_destination_parent_fsync_eio_never_records_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original_fsync = os.fsync

    def fail_directory_fsync(descriptor: int) -> None:
        if os.path.samestat(os.fstat(descriptor), os.stat(tmp_path / "reserved")):
            raise OSError(errno.EIO, "injected directory fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_directory_fsync)

    async def exercise() -> None:
        destination_root = tmp_path / "reserved"
        destination_root.mkdir()
        destination = destination_root / "child.bin"
        payload = b"durability"
        digest = hashlib.sha256(payload).hexdigest()
        hooks = _DirectoryTransferHooks(
            destination_id=SLOT,
            destination_path=destination,
            expected_size=len(payload),
        )
        manager, writer, socket, writer_task = await _manager(
            tmp_path, directory_managers=lambda: (hooks,)
        )
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/child.bin",
                    total_bytes=len(payload),
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, payload))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=len(payload),
                    sha256=digest,
                )
            )
            await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=True,
                ok=False,
                code="workspace_storage_unavailable",
            )
            await _wait_slot_closed(manager, SLOT)
            await asyncio.wait_for(hooks.destination_completed.wait(), timeout=1)
            assert not destination.exists()
            assert hooks.commits == []
            assert hooks.destination_terminal == [False]
            assert not any(
                isinstance(item, str)
                and json.loads(item).get("type") == "transfer_end"
                and json.loads(item).get("ack") is True
                and json.loads(item).get("ok") is True
                for item in socket.sent
            )
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory fsync semantics")
def test_directory_cleanup_uses_full_published_identity(tmp_path: Path) -> None:
    owned = tmp_path / "owned.bin"
    owned.write_bytes(b"owned")
    owned_identity = transfer_module._identity(os.lstat(owned))

    assert transfer_module._unlink_regular_if_identity(owned, owned_identity) is True
    assert not owned.exists()

    external = tmp_path / "external.bin"
    external.write_bytes(b"external")
    info = os.lstat(external)
    recycled_identity = (
        info.st_dev,
        info.st_ino,
        info.st_size + 1,
        info.st_mtime_ns - 1,
        info.st_ctime_ns - 1,
    )

    assert transfer_module._unlink_regular_if_identity(external, recycled_identity) is False
    assert external.read_bytes() == b"external"


@pytest.mark.skipif(os.name == "nt", reason="POSIX directory fsync semantics")
def test_directory_destination_fsync_cleanup_preserves_external_replacement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination_root = tmp_path / "reserved"
    destination_root.mkdir()
    destination = destination_root / "child.bin"
    external = b"external replacement"

    def replace_then_fail(_parent: Path) -> None:
        destination.unlink()
        destination.write_bytes(external)
        raise OSError(errno.EIO, "injected directory fsync failure")

    monkeypatch.setattr(transfer_module, "_fsync_parent_strict", replace_then_fail)

    async def exercise() -> None:
        payload = b"durability"
        digest = hashlib.sha256(payload).hexdigest()
        hooks = _DirectoryTransferHooks(
            destination_id=SLOT,
            destination_path=destination,
            expected_size=len(payload),
        )
        manager, writer, socket, writer_task = await _manager(
            tmp_path, directory_managers=lambda: (hooks,)
        )
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/child.bin",
                    total_bytes=len(payload),
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, payload))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=len(payload),
                    sha256=digest,
                )
            )
            await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=True,
                ok=False,
                code="workspace_storage_unavailable",
            )
            await _wait_slot_closed(manager, SLOT)
            await asyncio.wait_for(hooks.destination_completed.wait(), timeout=1)
            assert destination.read_bytes() == external
            assert hooks.commits == []
            assert hooks.destination_terminal == [False]
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_directory_destination_failure_completes_after_abandoned_filesystem_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original_fsync = os.fsync
    started = threading.Event()
    release = threading.Event()

    def delayed_fsync(descriptor: int) -> None:
        started.set()
        release.wait(timeout=2)
        original_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", delayed_fsync)

    async def exercise() -> None:
        destination_root = tmp_path / "reserved"
        destination_root.mkdir()
        destination = destination_root / "child.bin"
        locks = PathLocks()
        temporary_holder: list[Path] = []
        handle_holder: list[object] = []

        class ObservingHooks(_DirectoryTransferHooks):
            async def complete_destination_authorization(
                self, transfer_uuid: UUID, *, success: bool
            ) -> None:
                assert temporary_holder[0].exists() is False
                assert getattr(handle_holder[0], "closed", False) is True
                assert locks.reservation_count == 0
                await super().complete_destination_authorization(transfer_uuid, success=success)

        hooks = ObservingHooks(
            destination_id=SLOT,
            destination_path=destination,
            expected_size=0,
        )
        admission = LocalTransferAdmission(capacity=1)
        drains = LocalTransferDrainRegistry()
        manager, writer, _socket, writer_task = await _manager(
            tmp_path,
            admission=admission,
            drains=drains,
            path_locks=locks,
            directory_managers=lambda: (hooks,),
        )
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="reserved/child.bin",
                    total_bytes=0,
                )
            )
            await _wait_receiver_ready(manager)
            temporary = manager._slots[SLOT].temporary
            assert temporary is not None
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=0,
                    sha256=hashlib.sha256(b"").hexdigest(),
                )
            )
            assert await asyncio.to_thread(started.wait, 1)
            handle = manager._slots[SLOT].destination_handle
            assert handle is not None
            temporary_holder.append(temporary)
            handle_holder.append(handle)

            await manager.shutdown()
            assert hooks.destination_terminal == []
            assert temporary.exists()
            assert manager.path_locks.reservation_count == 1
            assert admission.active_count == 1

            release.set()
            assert await drains.wait(timeout_seconds=1)
            assert hooks.destination_terminal == [False]
            assert temporary.exists() is False
            assert manager.path_locks.reservation_count == 0
            assert admission.active_count == 0
        finally:
            release.set()
            await drains.wait(timeout_seconds=1)
            await writer.stop()
            await writer_task

    asyncio.run(exercise())


def test_writer_round_robins_binary_lanes_and_prioritizes_controls() -> None:
    async def exercise() -> list[str | bytes]:
        socket = Socket()
        writer = SerializedWriter()
        writer.register_binary_lane(SLOT)
        writer.register_binary_lane(SLOT_2)
        task = asyncio.create_task(writer.run(socket))
        writer.enqueue_binary(SLOT, b"a")
        writer.enqueue_binary(SLOT, b"b")
        writer.enqueue_binary(SLOT_2, b"c")
        writer.enqueue_critical('{"type":"pong"}')
        await writer.drain()
        await writer.stop()
        await task
        return socket.sent

    sent = asyncio.run(exercise())
    assert sent[0] == '{"type":"pong"}'
    binary_frames = [item for item in sent[1:] if isinstance(item, bytes)]
    assert len(binary_frames) == 3
    assert [decode_binary_chunk(item)[0] for item in binary_frames] == [SLOT, SLOT_2, SLOT]


def test_writer_binary_lane_is_four_chunks_bounded() -> None:
    writer = SerializedWriter()
    writer.register_binary_lane(SLOT)
    for _ in range(4):
        writer.enqueue_binary(SLOT, b"x" * (64 * 1024))
    with pytest.raises(WriterOverflowError):
        writer.enqueue_binary(SLOT, b"x")


def test_receiver_reservation_filesystem_does_not_block_event_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_create_temp = transfer_module._create_temp
    finished = threading.Event()

    def slow_create_temp(parent: Path, name: str) -> Path:
        time.sleep(0.15)
        try:
            return original_create_temp(parent, name)
        finally:
            finished.set()

    monkeypatch.setattr(transfer_module, "_create_temp", slow_create_temp)

    async def exercise() -> int:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            task = asyncio.create_task(
                manager.handle_control(
                    TransferBegin(
                        id=SLOT,
                        direction="server_to_client",
                        purpose="file_transfer",
                        src_path="source.txt",
                        dst_path="nested/result.txt",
                        total_bytes=0,
                    )
                )
            )
            ticks = 0
            while not finished.is_set():
                await asyncio.sleep(0)
                ticks += 1
            await task
            return ticks
        finally:
            await _stop(manager, writer, writer_task)

    assert asyncio.run(exercise()) > 0


def test_sender_preparation_filesystem_does_not_block_event_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    original_resolve = WorkspacePaths.resolve
    finished = threading.Event()

    def slow_resolve(
        paths: WorkspacePaths,
        value: str,
        *,
        directory: bool | None = None,
    ) -> Path:
        time.sleep(0.15)
        try:
            return original_resolve(paths, value, directory=directory)
        finally:
            finished.set()

    monkeypatch.setattr(WorkspacePaths, "resolve", slow_resolve)

    async def exercise() -> int:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(id=SLOT, purpose="http_relay", src_path="source.bin")
            )
            ticks = 0
            while not finished.is_set():
                await asyncio.sleep(0)
                ticks += 1
            for _ in range(100):
                if manager.slot_state(SLOT) is TransferState.BEGUN:
                    break
                await asyncio.sleep(0.001)
            return ticks
        finally:
            await _stop(manager, writer, writer_task)

    assert asyncio.run(exercise()) > 0


def test_cancelled_reservation_cleans_a_late_temp_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_create_temp = transfer_module._create_temp
    started = threading.Event()
    created = threading.Event()
    release = threading.Event()

    def delayed_create_temp(parent: Path, name: str) -> Path:
        started.set()
        release.wait(timeout=1)
        temporary = original_create_temp(parent, name)
        created.set()
        return temporary

    monkeypatch.setattr(transfer_module, "_create_temp", delayed_create_temp)

    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="result.txt",
                    total_bytes=0,
                )
            )
            assert await asyncio.to_thread(started.wait, 1)
            await manager.shutdown()
            release.set()
            assert await asyncio.to_thread(created.wait, 1)
            for _ in range(100):
                if not list(tmp_path.glob(".*.tmp")):
                    break
                await asyncio.sleep(0.001)
            assert not list(tmp_path.glob(".*.tmp"))
        finally:
            release.set()
            await writer.stop()
            await writer_task

    asyncio.run(exercise())


def test_cancelled_sender_closes_a_late_source_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    original_open_source = transfer_module._open_source
    started = threading.Event()
    release = threading.Event()
    result_holder: list[tuple[int, tuple[int, int, int, int, int]]] = []

    def delayed_open_source(path: Path) -> tuple[int, tuple[int, int, int, int, int]]:
        result = original_open_source(path)
        result_holder.append(result)
        started.set()
        release.wait(timeout=1)
        return result

    monkeypatch.setattr(transfer_module, "_open_source", delayed_open_source)

    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=1)
        drains = LocalTransferDrainRegistry()
        manager, writer, _, writer_task = await _manager(
            tmp_path, admission=admission, drains=drains
        )
        try:
            await manager.handle_control(
                TransferRequest(id=SLOT, purpose="http_relay", src_path="source.bin")
            )
            assert await asyncio.to_thread(started.wait, 1)
            await manager.shutdown()
            assert admission.active_count == 1
            assert admission.try_acquire() is None
            release.set()
            fd = result_holder[0][0]
            for _ in range(100):
                try:
                    os.fstat(fd)
                except OSError:
                    break
                await asyncio.sleep(0.001)
            with pytest.raises(OSError):
                os.fstat(fd)
            assert await drains.wait(timeout_seconds=1)
            assert admission.active_count == 0
        finally:
            release.set()
            await writer.stop()
            await writer_task

    asyncio.run(exercise())


def test_cancelled_receiver_closes_a_late_temp_handle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_open_temp = transfer_module._open_temp
    started = threading.Event()
    release = threading.Event()
    result_holder: list[tuple[object, tuple[int, int, int, int, int]]] = []

    def delayed_open_temp(
        path: Path,
    ) -> tuple[object, tuple[int, int, int, int, int]]:
        result = original_open_temp(path)
        result_holder.append(result)
        started.set()
        release.wait(timeout=1)
        return result

    monkeypatch.setattr(transfer_module, "_open_temp", delayed_open_temp)

    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="result.txt",
                    total_bytes=0,
                )
            )
            await _wait_receiver_ready(manager)
            assert await asyncio.to_thread(started.wait, 1)
            await manager.shutdown()
            release.set()
            handle = result_holder[0][0]
            for _ in range(100):
                if getattr(handle, "closed", False):
                    break
                await asyncio.sleep(0.001)
            assert getattr(handle, "closed", False)
        finally:
            release.set()
            await writer.stop()
            await writer_task

    asyncio.run(exercise())


def test_cancelled_receiver_keeps_temp_until_blocked_fsync_drains(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_fsync = os.fsync
    started = threading.Event()
    release = threading.Event()

    def delayed_fsync(descriptor: int) -> None:
        started.set()
        release.wait(timeout=2)
        original_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", delayed_fsync)

    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=1)
        drains = LocalTransferDrainRegistry()
        manager, writer, _, writer_task = await _manager(
            tmp_path, admission=admission, drains=drains
        )
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="result.txt",
                    total_bytes=0,
                )
            )
            await _wait_receiver_ready(manager)
            temporary = manager._slots[SLOT].temporary
            assert temporary is not None
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=0,
                    sha256=hashlib.sha256(b"").hexdigest(),
                )
            )
            assert await asyncio.to_thread(started.wait, 1)

            await manager.shutdown()
            assert temporary.exists()
            assert manager.slot_state(SLOT) is None
            assert admission.active_count == 1
            assert manager.path_locks.reservation_count == 1

            release.set()
            assert await drains.wait(timeout_seconds=1)
            assert temporary.exists() is False
            assert admission.active_count == 0
            assert manager.path_locks.reservation_count == 0
        finally:
            release.set()
            await writer.stop()
            await writer_task

    asyncio.run(exercise())


def test_terminal_overflow_is_fatal_and_does_not_record_unsent_end(tmp_path: Path) -> None:
    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        slot = transfer_module._Slot(
            slot_id=SLOT,
            role="receiver",
            purpose="file_transfer",
            snapshot=TransferConfigSnapshot.from_values(
                tmp_path,
                restrict_to_workspace=True,
            ),
            state="READY",
        )
        manager._slots[SLOT] = slot
        try:
            for _ in range(writer._CRITICAL_MAX):
                writer.enqueue_critical("{}")
            with pytest.raises(WriterOverflowError):
                manager._enqueue_end(slot, ok=False, code="failed", ack=False)
            assert slot.final_end is None
            with pytest.raises(WriterOverflowError):
                await manager.failed
        finally:
            with pytest.raises(WriterOverflowError):
                await manager.shutdown()
            await writer.stop()
            await writer_task

    asyncio.run(exercise())


def test_receive_file_commits_only_after_digest_and_cleans_temp(tmp_path: Path) -> None:
    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="nested/result.txt",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"abc"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=3,
                    sha256="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                )
            )
            await _wait_slot_closed(manager, SLOT)
            await writer.drain()
            assert (tmp_path / "nested/result.txt").read_bytes() == b"abc"
            assert not list((tmp_path / "nested").glob(".*.tmp"))
            frames = [json.loads(item) for item in socket.sent if isinstance(item, str)]
            assert [frame["type"] for frame in frames] == ["transfer_ready", "transfer_end"]
            assert frames[-1]["ack"] is True
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_workspace_upload_create_and_conditional_overwrite_return_metadata(
    tmp_path: Path,
) -> None:
    async def exercise() -> list[dict[str, object]]:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            digest_one = hashlib.sha256(b"one").hexdigest()
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="workspace_upload",
                    dst_path="result.txt",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"one"))
            await manager.handle_control(
                TransferEnd(id=SLOT, ack=False, ok=True, bytes_sent=3, sha256=digest_one)
            )
            await _wait_slot_closed(manager, SLOT)
            await writer.drain()
            frames = [json.loads(item) for item in socket.sent if isinstance(item, str)]
            created_ack = next(
                frame
                for frame in frames
                if frame["type"] == "transfer_end" and frame.get("ack") is True
            )
            assert created_ack["created"] is True
            etag = created_ack["etag"]
            assert isinstance(etag, str) and len(etag) == 64

            digest_two = hashlib.sha256(b"two").hexdigest()
            await manager.handle_control(
                TransferBegin(
                    id=SLOT_2,
                    direction="server_to_client",
                    purpose="workspace_upload",
                    dst_path="result.txt",
                    total_bytes=3,
                    if_match=etag,
                )
            )
            await _wait_receiver_ready(manager, SLOT_2)
            await manager.handle_binary(encode_binary_chunk(SLOT_2, b"two"))
            await manager.handle_control(
                TransferEnd(id=SLOT_2, ack=False, ok=True, bytes_sent=3, sha256=digest_two)
            )
            await _wait_slot_closed(manager, SLOT_2)
            await writer.drain()
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert (tmp_path / "result.txt").read_bytes() == b"two"
    overwrite_acks = [
        frame
        for frame in frames
        if frame["type"] == "transfer_end"
        and frame.get("ack") is True
        and frame.get("ok") is True
        and frame.get("created") is False
    ]
    assert len(overwrite_acks) == 1


def test_workspace_upload_if_match_mismatch_does_not_write(tmp_path: Path) -> None:
    target = tmp_path / "result.txt"
    target.write_bytes(b"old")

    async def exercise() -> list[dict[str, object]]:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="workspace_upload",
                    dst_path="result.txt",
                    total_bytes=3,
                    if_match="stale",
                )
            )
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
            await writer.drain()
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert target.read_bytes() == b"old"
    assert frames[-1]["type"] == "transfer_end"
    assert frames[-1]["code"] == "workspace_file_changed"


def test_workspace_upload_rechecks_symlinked_parent_before_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent = tmp_path / "nested"
    outside = tmp_path / "outside"
    parent.mkdir()
    outside.mkdir()
    commit_check_started = threading.Event()
    release_commit_check = threading.Event()
    check_calls = 0
    original_check = transfer_module._destination_parent_unchanged

    def gated_parent_check(paths: WorkspacePaths, path: str, destination: Path) -> bool:
        nonlocal check_calls
        check_calls += 1
        if check_calls == 2:
            commit_check_started.set()
            assert release_commit_check.wait(timeout=5)
        return original_check(paths, path, destination)

    monkeypatch.setattr(
        transfer_module,
        "_destination_parent_unchanged",
        gated_parent_check,
    )

    async def exercise() -> list[dict[str, object]]:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="workspace_upload",
                    dst_path="nested/result.txt",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"abc"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=3,
                    sha256=hashlib.sha256(b"abc").hexdigest(),
                )
            )
            assert await asyncio.to_thread(commit_check_started.wait, 5)
            moved = tmp_path / "moved"
            parent.rename(moved)
            _make_directory_link(parent, outside)
            release_commit_check.set()
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
            await writer.drain()
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            release_commit_check.set()
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert not (outside / "result.txt").exists()
    assert frames[-1]["code"] == "workspace_symlink_escape"


def test_workspace_upload_rejects_temp_symlink_swap(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    outside = tmp_path / "outside.txt"
    outside.write_text("keep")
    probe = tmp_path / "probe"
    try:
        probe.symlink_to(outside)
    except OSError:
        pytest.skip("symbolic links are unavailable on this platform")
    probe.unlink()

    original_identity_after_close = transfer_module._identity_after_close

    def swap_temp_after_close(
        path: Path,
        open_identity: tuple[int, int, int, int, int],
        expected_bytes: int,
        expected_sha256: str,
    ) -> tuple[int, int, int, int, int]:
        path.unlink()
        path.symlink_to(outside)
        return original_identity_after_close(
            path,
            open_identity,
            expected_bytes,
            expected_sha256,
        )

    monkeypatch.setattr(
        transfer_module,
        "_identity_after_close",
        swap_temp_after_close,
    )

    async def exercise() -> list[dict[str, object]]:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="workspace_upload",
                    dst_path="result.txt",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"abc"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=3,
                    sha256=hashlib.sha256(b"abc").hexdigest(),
                )
            )
            await _wait_slot_closed(manager, SLOT)
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert outside.read_text() == "keep"
    assert not (tmp_path / "result.txt").exists()
    assert not list(tmp_path.glob(".*.tmp"))
    assert frames[-1]["code"] == "workspace_file_changed"


def test_receiver_queue_timeout_sends_terminal_and_cleans_resources(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def exercise() -> list[dict[str, object]]:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        manager._idle_timeout = 0.1

        async def stalled(slot: object) -> None:
            try:
                await asyncio.Event().wait()
            finally:
                await manager._cleanup_slot(slot)  # type: ignore[arg-type]

        monkeypatch.setattr(manager, "_receive_destination", stalled)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="result.txt",
                    total_bytes=5,
                )
            )
            await _wait_receiver_ready(manager)
            for _ in range(4):
                await manager.handle_binary(encode_binary_chunk(SLOT, b"x"))
            await manager.handle_binary(encode_binary_chunk(SLOT, b"x"))
            for _ in range(100):
                if (
                    manager.active_count == 0
                    and not manager._tasks
                    and manager.path_locks.reservation_count == 0
                ):
                    break
                await asyncio.sleep(0.001)
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=False,
                    code="workspace_transfer_timeout",
                )
            )
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert frames[-1] == {
        "type": "transfer_end",
        "id": str(SLOT),
        "ack": False,
        "ok": False,
        "code": "workspace_transfer_timeout",
    }
    assert not (tmp_path / "result.txt").exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_receiver_failure_before_ready_rejects_binary_and_accepts_late_ack(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail_open(_path: Path) -> tuple[object, tuple[int, int, int, int, int]]:
        raise transfer_module.TransferOperationError(
            "workspace_storage_unavailable",
            "synthetic local destination failure",
        )

    monkeypatch.setattr(transfer_module, "_open_temp", fail_open)

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                    total_bytes=7,
                )
            )
            failure: dict[str, object] | None = None
            for _ in range(100):
                await asyncio.sleep(0.001)
                failure = next(
                    (
                        json.loads(item)
                        for item in socket.sent
                        if isinstance(item, str) and json.loads(item)["type"] == "transfer_end"
                    ),
                    None,
                )
                if failure is not None and manager.active_count == 0:
                    break
            assert failure is not None
            assert failure["ack"] is False
            assert failure["ok"] is False
            assert failure["code"] == "workspace_storage_unavailable"

            with pytest.raises(ProtocolError):
                await manager.handle_binary(encode_binary_chunk(SLOT, b"binary-before-ready"))

            matching_ack = TransferEnd(
                id=SLOT,
                ack=True,
                ok=False,
                code="workspace_storage_unavailable",
            )
            await manager.handle_control(matching_ack)
            with pytest.raises(ProtocolError):
                await manager.handle_binary(encode_binary_chunk(SLOT, b"after-ack"))
            with pytest.raises(ProtocolError):
                await manager.handle_control(
                    matching_ack.model_copy(update={"code": "workspace_file_changed"})
                )
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_failed_active_receiver_drops_only_ready_bounded_nonempty_binary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        original_cleanup = manager._cleanup_slot

        async def blocked_cleanup(slot: object) -> None:
            cleanup_started.set()
            await release_cleanup.wait()
            await original_cleanup(slot)  # type: ignore[arg-type]

        monkeypatch.setattr(manager, "_cleanup_slot", blocked_cleanup)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"a"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=False,
                    code="peer_disconnected",
                )
            )
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)

            await manager.handle_binary(encode_binary_chunk(SLOT, b"bc"))
            with pytest.raises(ProtocolError):
                await manager.handle_binary(encode_binary_chunk(SLOT, b""))
            with pytest.raises(ProtocolError):
                await manager.handle_binary(encode_binary_chunk(SLOT, b"d"))

            release_cleanup.set()
            await _wait_slot_closed(manager, SLOT)
            assert manager._tombstones[SLOT].binary_bytes_seen == 3
        finally:
            release_cleanup.set()
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_failed_receiver_tombstone_bounds_binary_without_extending_ttl(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                    total_bytes=4,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"a"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=False,
                    code="peer_disconnected",
                )
            )
            await _wait_slot_closed(manager, SLOT)
            expires_at = manager._tombstones[SLOT].expires_at

            await manager.handle_binary(encode_binary_chunk(SLOT, b"bc"))
            assert manager._tombstones[SLOT].binary_bytes_seen == 3
            assert manager._tombstones[SLOT].expires_at == expires_at
            with pytest.raises(ProtocolError):
                await manager.handle_binary(encode_binary_chunk(SLOT, b""))
            with pytest.raises(ProtocolError):
                await manager.handle_binary(encode_binary_chunk(SLOT, b"de"))

            manager._tombstones[SLOT] = replace(
                manager._tombstones[SLOT],
                expires_at=asyncio.get_running_loop().time() - 1,
            )
            with pytest.raises(ProtocolError, match="unknown transfer"):
                await manager.handle_binary(encode_binary_chunk(SLOT, b"d"))
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_no_replace_commit_fsyncs_parent_after_link_and_unlink(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    temporary = tmp_path / ".temporary"
    destination = tmp_path / "destination"
    temporary.write_bytes(b"payload")
    calls: list[Path] = []
    monkeypatch.setattr(transfer_module, "_fsync_parent", calls.append)

    transfer_module._commit_no_replace(
        temporary,
        destination,
        transfer_module._identity(temporary.stat()),
    )

    assert destination.read_bytes() == b"payload"
    assert not temporary.exists()
    assert calls == [tmp_path]


def test_receive_accepts_later_chunks_after_consumer_enters_streaming(tmp_path: Path) -> None:
    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="result.txt",
                    total_bytes=2,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"a"))
            for _ in range(100):
                if manager.slot_state(SLOT) == "STREAMING":
                    break
                await asyncio.sleep(0.001)
            assert manager.slot_state(SLOT) == "STREAMING"

            await manager.handle_binary(encode_binary_chunk(SLOT, b"b"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=2,
                    sha256=hashlib.sha256(b"ab").hexdigest(),
                )
            )
            for _ in range(100):
                if (tmp_path / "result.txt").exists():
                    break
                await asyncio.sleep(0.001)
            assert (tmp_path / "result.txt").read_bytes() == b"ab"
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_receive_rejects_existing_destination_and_unknown_binary_slot(tmp_path: Path) -> None:
    (tmp_path / "existing.txt").write_text("old")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="workspace_upload",
                    src_path=None,
                    dst_path="existing.txt",
                    total_bytes=3,
                    if_none_match=True,
                )
            )
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
            await writer.drain()
            frame = next(json.loads(item) for item in socket.sent if isinstance(item, str))
            assert frame["type"] == "transfer_end"
            assert frame["ack"] is False
            assert frame["code"] == "workspace_file_changed"
            with pytest.raises(ProtocolError):
                await manager.handle_binary(encode_binary_chunk(SLOT_2, b"x"))
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_file_transfer_never_overwrites_existing_destination(tmp_path: Path) -> None:
    target = tmp_path / "existing.txt"
    target.write_text("old")

    async def exercise() -> list[dict[str, object]]:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="existing.txt",
                    total_bytes=3,
                )
            )
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
            await writer.drain()
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert target.read_text() == "old"
    assert frames[-1]["ack"] is False
    assert frames[-1]["code"] == "workspace_file_changed"


def test_receive_rechecks_destination_before_exposing_completed_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "result.txt"
    original_resolve = WorkspacePaths.resolve
    resolve_count = 0

    def resolve_with_external_writer(
        paths: WorkspacePaths,
        value: str,
        *,
        directory: bool | None = None,
    ) -> Path:
        nonlocal resolve_count
        resolve_count += 1
        resolved = original_resolve(paths, value, directory=directory)
        if resolve_count == 2:
            target.write_text("external")
        return resolved

    monkeypatch.setattr(WorkspacePaths, "resolve", resolve_with_external_writer)

    async def exercise() -> list[dict[str, object]]:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="workspace_upload",
                    dst_path="result.txt",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"abc"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=3,
                    sha256="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                )
            )
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
            await writer.drain()
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert target.read_text() == "external"
    assert frames[-1]["code"] == "workspace_file_changed"
    assert frames[-1]["ack"] is True


def test_receive_rejects_same_size_temp_change_after_close(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "result.txt"
    original_identity_after_close = transfer_module._identity_after_close

    def change_before_identity_check(
        path: Path,
        open_identity: tuple[int, int, int, int, int],
        expected_bytes: int,
        expected_sha256: str,
    ) -> tuple[int, int, int, int, int]:
        path.write_bytes(b"xyz")
        info = path.stat()
        os.utime(
            path,
            ns=(info.st_atime_ns, open_identity[3] + 1_000_000_000),
        )
        return original_identity_after_close(
            path,
            open_identity,
            expected_bytes,
            expected_sha256,
        )

    monkeypatch.setattr(
        transfer_module,
        "_identity_after_close",
        change_before_identity_check,
    )

    async def exercise() -> list[dict[str, object]]:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.txt",
                    dst_path="result.txt",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"abc"))
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=True,
                    bytes_sent=3,
                    sha256=hashlib.sha256(b"abc").hexdigest(),
                )
            )
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
            await writer.drain()
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert target.exists() is False
    assert frames[-1]["code"] == "workspace_file_changed"


def test_closed_temp_recheck_accepts_timestamp_skew_when_payload_matches(
    tmp_path: Path,
) -> None:
    temporary = tmp_path / "temporary.bin"
    payload = b"abc"
    temporary.write_bytes(payload)
    open_identity = transfer_module._identity(temporary.stat())
    info = temporary.stat()
    os.utime(
        temporary,
        ns=(info.st_atime_ns, open_identity[3] + 1_000_000_000),
    )

    closed_identity = transfer_module._identity_after_close(
        temporary,
        open_identity,
        len(payload),
        hashlib.sha256(payload).hexdigest(),
    )

    assert closed_identity == transfer_module._identity(temporary.stat())


def test_source_unchanged_accepts_path_ctime_skew_with_stable_open_handle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    descriptor = os.open(source, os.O_RDONLY | int(getattr(os, "O_BINARY", 0)))
    initial = (1, 2, 7, 4, 5)
    identities = iter((initial, (1, 2, 7, 4, 6)))
    monkeypatch.setattr(transfer_module, "_identity", lambda _info: next(identities))

    try:
        assert transfer_module._source_unchanged(source, descriptor, initial)
    finally:
        os.close(descriptor)


def test_send_file_waits_for_ready_and_uses_bounded_chunks(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"x" * (64 * 1024 + 7))

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(id=SLOT, purpose="http_relay", src_path="source.bin")
            )
            for _ in range(100):
                if manager.slot_state(SLOT) is TransferState.BEGUN:
                    break
                await asyncio.sleep(0.001)
            assert manager.slot_state(SLOT) is TransferState.BEGUN
            await writer.drain()
            assert not any(isinstance(item, bytes) for item in socket.sent)
            begin = next(
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str) and json.loads(item)["type"] == "transfer_begin"
            )
            assert len(begin["etag"]) == 64
            assert begin["etag"] not in {str(source.stat().st_ino), str(source.stat().st_dev)}
            await manager.handle_control(TransferReady(id=SLOT))
            for _ in range(100):
                await asyncio.sleep(0.01)
                if any(
                    isinstance(item, str) and json.loads(item)["type"] == "transfer_end"
                    for item in socket.sent
                ):
                    break
            binary = [item for item in socket.sent if isinstance(item, bytes)]
            assert binary
            assert all(len(item) <= 16 + 64 * 1024 for item in binary)
            ends = [
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str) and json.loads(item)["type"] == "transfer_end"
            ]
            assert ends and ends[-1].get("ok") is True, [item.get("code") for item in ends]
            end = ends[-1]
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=True,
                    bytes_sent=end["bytes_sent"],
                    sha256=end["sha256"],
                )
            )
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
            assert manager.active_count == 0
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_slot_stays_visible_until_resource_cleanup_finishes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        original_cleanup = manager._cleanup_slot_resources

        async def pause_cleanup(slot: Any) -> None:
            cleanup_started.set()
            await release_cleanup.wait()
            await original_cleanup(slot)

        monkeypatch.setattr(manager, "_cleanup_slot_resources", pause_cleanup)
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            success = await _wait_for_transfer_end(socket, SLOT, ack=False, ok=True)
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=True,
                    bytes_sent=cast(int, success["bytes_sent"]),
                    sha256=cast(str, success["sha256"]),
                )
            )
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)

            assert manager.slot_state(SLOT) is not None

            release_cleanup.set()
            await _wait_slot_closed(manager, SLOT)
            assert manager.path_locks.reservation_count == 0
        finally:
            release_cleanup.set()
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_shutdown_waits_for_visible_slot_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        cleanup_started = asyncio.Event()
        cleanup_cancelled = asyncio.Event()
        release_cleanup = asyncio.Event()
        original_cleanup = manager._cleanup_slot_resources
        slot: Any = None
        shutdown: asyncio.Task[None] | None = None

        async def pause_cleanup(current: Any) -> None:
            cleanup_started.set()
            try:
                await release_cleanup.wait()
            except asyncio.CancelledError:
                cleanup_cancelled.set()
                raise
            await original_cleanup(current)

        monkeypatch.setattr(manager, "_cleanup_slot_resources", pause_cleanup)
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            slot = manager._slots[SLOT]
            await manager.handle_control(TransferReady(id=SLOT))
            success = await _wait_for_transfer_end(socket, SLOT, ack=False, ok=True)
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=True,
                    bytes_sent=cast(int, success["bytes_sent"]),
                    sha256=cast(str, success["sha256"]),
                )
            )
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            assert manager.path_locks.reservation_count == 1

            shutdown = asyncio.create_task(manager.shutdown())
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert cleanup_cancelled.is_set() is False
            assert shutdown.done() is False

            release_cleanup.set()
            await asyncio.wait_for(shutdown, timeout=1)
            assert manager.slot_state(SLOT) is None
            assert manager.path_locks.reservation_count == 0
        finally:
            release_cleanup.set()
            if shutdown is not None:
                await asyncio.gather(shutdown, return_exceptions=True)
            if slot is not None and slot.lock_stack is not None:
                await original_cleanup(slot)
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("after_timeout", "ack_ok"),
    (
        pytest.param(False, True, id="success-before-timeout"),
        pytest.param(False, False, id="failure-before-timeout"),
        pytest.param(True, True, id="success-after-timeout"),
        pytest.param(True, False, id="failure-after-timeout"),
    ),
)
def test_sender_accepts_destination_ack_before_or_after_local_timeout(
    tmp_path: Path,
    *,
    after_timeout: bool,
    ack_ok: bool,
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        manager._idle_timeout = 0.1 if after_timeout else 1.0
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            success = await _wait_for_transfer_end(socket, SLOT, ack=False, ok=True)

            if after_timeout:
                await _wait_for_transfer_end(
                    socket,
                    SLOT,
                    ack=False,
                    ok=False,
                    code="workspace_transfer_timeout",
                )

            if ack_ok:
                acknowledgement = TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=True,
                    bytes_sent=cast(int, success["bytes_sent"]),
                    sha256=cast(str, success["sha256"]),
                )
            else:
                acknowledgement = TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=False,
                    code="workspace_file_changed",
                )
            await manager.handle_control(acknowledgement)
            await _wait_slot_closed(manager, SLOT)

            assert manager.path_locks.reservation_count == 0
            assert writer.has_binary_lane(SLOT) is False

            # A valid chosen ACK must leave the connection usable for another slot.
            await manager.handle_control(
                TransferRequest(
                    id=SLOT_2,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="next.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT_2) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(
                TransferEnd(
                    id=SLOT_2,
                    ack=False,
                    ok=False,
                    code="workspace_file_changed",
                )
            )
            await _wait_slot_closed(manager, SLOT_2)
            await writer.drain()
            assert writer_task.done() is False
            assert manager.failed.done() is False
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_sender_rejects_wrong_success_digest_without_closing_slot(tmp_path: Path) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            success = await _wait_for_transfer_end(socket, SLOT, ack=False, ok=True)

            with pytest.raises(ProtocolError, match="acknowledgement conflicts"):
                await manager.handle_control(
                    TransferEnd(
                        id=SLOT,
                        ack=True,
                        ok=True,
                        bytes_sent=cast(int, success["bytes_sent"]),
                        sha256="0" * 64,
                    )
                )
            assert manager.slot_state(SLOT) is TransferState.SENDER_ENDED

            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=True,
                    bytes_sent=cast(int, success["bytes_sent"]),
                    sha256=cast(str, success["sha256"]),
                )
            )
            await _wait_slot_closed(manager, SLOT)
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


@pytest.mark.parametrize("ack_ok", [True, False], ids=["success", "failure"])
def test_sender_tombstone_accepts_late_chosen_ack_after_timeout_cleanup(
    tmp_path: Path,
    *,
    ack_ok: bool,
) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        manager._idle_timeout = 0.05
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            success = await _wait_for_transfer_end(socket, SLOT, ack=False, ok=True)
            await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_transfer_timeout",
            )
            await _wait_slot_closed(manager, SLOT)

            acknowledgement = (
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=True,
                    bytes_sent=cast(int, success["bytes_sent"]),
                    sha256=cast(str, success["sha256"]),
                )
                if ack_ok
                else TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=False,
                    code="workspace_file_changed",
                )
            )
            await manager.handle_control(acknowledgement)
            assert manager.failed.done() is False
            assert writer_task.done() is False
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_sender_ack_during_timeout_cleanup_fixes_the_chosen_tombstone(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        manager._idle_timeout = 0.05
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        original_cleanup = manager._cleanup_slot_resources

        async def blocked_cleanup(slot: Any) -> None:
            cleanup_started.set()
            await release_cleanup.wait()
            await original_cleanup(slot)

        monkeypatch.setattr(manager, "_cleanup_slot_resources", blocked_cleanup)
        first_ack = TransferEnd(
            id=SLOT,
            ack=True,
            ok=False,
            code="workspace_file_changed",
        )
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            await _wait_for_transfer_end(socket, SLOT, ack=False, ok=True)
            await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_transfer_timeout",
            )
            await asyncio.wait_for(cleanup_started.wait(), 1)

            await manager.handle_control(first_ack)
            release_cleanup.set()
            await _wait_slot_closed(manager, SLOT)
            assert manager._tombstones[SLOT].ack is True

            with pytest.raises(ProtocolError, match="unknown transfer"):
                await manager.handle_control(
                    first_ack.model_copy(update={"code": "workspace_storage_unavailable"})
                )
            await manager.handle_control(first_ack)
            assert manager.failed.done() is False
            assert writer_task.done() is False
        finally:
            release_cleanup.set()
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_sender_local_failure_without_a_success_terminal_requires_matching_ack(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")
    monkeypatch.setattr(transfer_module, "_source_unchanged", lambda *_args: False)

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_file_changed",
            )
            assert manager.slot_state(SLOT) is TransferState.ABORTED

            with pytest.raises(ProtocolError, match="acknowledgement conflicts"):
                await manager.handle_control(
                    TransferEnd(
                        id=SLOT,
                        ack=True,
                        ok=False,
                        code="workspace_storage_unavailable",
                    )
                )
            assert manager.slot_state(SLOT) is TransferState.ABORTED

            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=False,
                    code="workspace_file_changed",
                )
            )
            await _wait_slot_closed(manager, SLOT)
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


@pytest.mark.parametrize("role", ["sender", "receiver"])
def test_active_local_failure_accepts_exact_crossing_peer_failure_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    role: str,
) -> None:
    if role == "sender":
        (tmp_path / "source.bin").write_bytes(b"abc")
        monkeypatch.setattr(transfer_module, "_source_unchanged", lambda *_args: False)
    else:
        (tmp_path / "result.bin").write_bytes(b"occupied")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            if role == "sender":
                await manager.handle_control(
                    TransferRequest(
                        id=SLOT,
                        purpose="file_transfer",
                        src_path="source.bin",
                        dst_path="result.bin",
                    )
                )
                async with asyncio.timeout(1):
                    while manager.slot_state(SLOT) is not TransferState.BEGUN:
                        await asyncio.sleep(0.001)
                await manager.handle_control(TransferReady(id=SLOT))
            else:
                await manager.handle_control(
                    TransferBegin(
                        id=SLOT,
                        direction="server_to_client",
                        purpose="file_transfer",
                        src_path="source.bin",
                        dst_path="result.bin",
                        total_bytes=3,
                    )
                )

            await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_file_changed",
            )
            assert manager.slot_state(SLOT) is TransferState.ABORTED

            crossing = TransferEnd(
                id=SLOT,
                ack=False,
                ok=False,
                code="workspace_file_changed",
            )
            await manager.handle_control(crossing)
            await manager.handle_control(crossing)
            await writer.drain()

            acknowledgements = [
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str)
                and json.loads(item).get("type") == "transfer_end"
                and json.loads(item).get("ack") is True
            ]
            assert acknowledgements == [
                {
                    "type": "transfer_end",
                    "id": str(SLOT),
                    "ack": True,
                    "ok": False,
                    "code": "workspace_file_changed",
                }
            ]
            assert manager.slot_state(SLOT) is TransferState.ABORTED

            await manager.handle_control(crossing.model_copy(update={"ack": True}))
            await _wait_slot_closed(manager, SLOT)
            assert manager.failed.done() is False
            assert writer_task.done() is False
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_remote_failure_duplicate_is_idempotent_while_active_and_tombstoned(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        original_cleanup = manager._cleanup_slot

        async def blocked_cleanup(slot: object) -> None:
            cleanup_started.set()
            await release_cleanup.wait()
            await original_cleanup(slot)  # type: ignore[arg-type]

        monkeypatch.setattr(manager, "_cleanup_slot", blocked_cleanup)
        failure = TransferEnd(
            id=SLOT,
            ack=False,
            ok=False,
            code="peer_disconnected",
        )
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)

            await manager.handle_control(failure)
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            await manager.handle_control(failure)
            await writer.drain()
            acknowledgements = [
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str)
                and json.loads(item).get("type") == "transfer_end"
                and json.loads(item).get("ack") is True
            ]
            assert len(acknowledgements) == 1

            release_cleanup.set()
            await _wait_slot_closed(manager, SLOT)
            await manager.handle_control(failure)
            await writer.drain()
            acknowledgements = [
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str)
                and json.loads(item).get("type") == "transfer_end"
                and json.loads(item).get("ack") is True
            ]
            assert len(acknowledgements) == 1
            with pytest.raises(ProtocolError, match="unknown transfer"):
                await manager.handle_control(
                    failure.model_copy(update={"code": "workspace_file_changed"})
                )
        finally:
            release_cleanup.set()
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


@pytest.mark.parametrize("ack_first", [False, True], ids=["failure-first", "ack-first"])
def test_local_failure_tombstone_accepts_exact_crossing_peer_failure_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    ack_first: bool,
) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")
    monkeypatch.setattr(transfer_module, "_source_unchanged", lambda *_args: False)

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        manager._idle_timeout = 0.05
        acknowledgement = TransferEnd(
            id=SLOT,
            ack=True,
            ok=False,
            code="workspace_file_changed",
        )
        crossing = acknowledgement.model_copy(update={"ack": False})
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            await _wait_for_transfer_end(
                socket,
                SLOT,
                ack=False,
                ok=False,
                code="workspace_file_changed",
            )
            if ack_first:
                await manager.handle_control(acknowledgement)
            await _wait_slot_closed(manager, SLOT)

            await manager.handle_control(crossing)
            await manager.handle_control(crossing)
            if not ack_first:
                await manager.handle_control(acknowledgement)
            await writer.drain()

            acknowledgements = [
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str)
                and json.loads(item).get("type") == "transfer_end"
                and json.loads(item).get("ack") is True
            ]
            assert acknowledgements == [
                {
                    "type": "transfer_end",
                    "id": str(SLOT),
                    "ack": True,
                    "ok": False,
                    "code": "workspace_file_changed",
                }
            ]
            assert manager.failed.done() is False
            assert writer_task.done() is False
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_peer_failure_discards_sender_binary_lane_and_keeps_writer_healthy(
    tmp_path: Path,
) -> None:
    (tmp_path / "source.bin").write_bytes(b"x" * (8 * 64 * 1024))

    class BlockingBinarySocket(Socket):
        def __init__(self) -> None:
            super().__init__()
            self.binary_started = asyncio.Event()
            self.release_binary = asyncio.Event()

        async def send(self, payload: str | bytes) -> None:
            self.sent.append(payload)
            if isinstance(payload, bytes) and not self.release_binary.is_set():
                self.binary_started.set()
                await self.release_binary.wait()

    async def exercise() -> None:
        socket = BlockingBinarySocket()
        writer = SerializedWriter()
        writer_task = asyncio.create_task(writer.run(socket))
        manager = TransferManager(tmp_path, writer)
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            async with asyncio.timeout(1):
                while manager.slot_state(SLOT) is not TransferState.BEGUN:
                    await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            await asyncio.wait_for(socket.binary_started.wait(), timeout=1)
            async with asyncio.timeout(1):
                while writer.binary_queued_chunks != 4:
                    await asyncio.sleep(0.001)

            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=False,
                    ok=False,
                    code="workspace_file_changed",
                )
            )
            assert writer.has_binary_lane(SLOT) is False
            assert writer.binary_queued_chunks == 0

            socket.release_binary.set()
            await _wait_slot_closed(manager, SLOT)
            await writer.drain()
            matching_acks = [
                frame
                for frame in (
                    json.loads(payload) for payload in socket.sent if isinstance(payload, str)
                )
                if frame["type"] == "transfer_end"
                and frame["ack"] is True
                and frame["code"] == "workspace_file_changed"
            ]
            assert len(matching_acks) == 1
            assert len([payload for payload in socket.sent if isinstance(payload, bytes)]) == 1
            assert manager.path_locks.reservation_count == 0
            assert writer_task.done() is False
            assert manager.failed.done() is False
        finally:
            socket.release_binary.set()
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_disconnect_cancels_receiver_and_removes_temp_and_reservation(tmp_path: Path) -> None:
    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            await manager.handle_binary(encode_binary_chunk(SLOT, b"abc"))
            await manager.shutdown()
            assert manager.active_count == 0
            assert manager.path_locks.entry_count == 0
            assert not (tmp_path / "result.bin").exists()
            assert not list(tmp_path.glob(".*.tmp"))
        finally:
            await writer.stop()
            await writer_task

    asyncio.run(exercise())


def test_late_identical_ack_is_ignored_but_conflicting_terminal_is_protocol_error(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(id=SLOT, purpose="http_relay", src_path="source.bin")
            )
            for _ in range(100):
                if manager.slot_state(SLOT) == "BEGUN":
                    break
                await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            for _ in range(100):
                await asyncio.sleep(0.01)
                ends = [
                    json.loads(item)
                    for item in socket.sent
                    if isinstance(item, str) and json.loads(item)["type"] == "transfer_end"
                ]
                if ends:
                    break
            end = ends[-1]
            ack = TransferEnd(
                id=SLOT,
                ack=True,
                ok=True,
                bytes_sent=end["bytes_sent"],
                sha256=end["sha256"],
            )
            await manager.handle_control(ack)
            await asyncio.sleep(0.02)
            await manager.handle_control(ack)
            with pytest.raises(ProtocolError):
                await manager.handle_control(
                    TransferEnd(id=SLOT, ack=True, ok=False, code="peer_disconnected")
                )
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_sender_failure_tombstone_accepts_matching_ack_and_rejects_conflict(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(id=SLOT, purpose="file_transfer", src_path="missing", dst_path="x")
            )
            await asyncio.sleep(0.02)
            failure = next(
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str) and json.loads(item)["type"] == "transfer_end"
            )
            assert failure["ack"] is False
            failure_code = failure["code"]
            assert isinstance(failure_code, str)
            matching_ack = TransferEnd(
                id=SLOT,
                ack=True,
                ok=False,
                code=failure_code,
            )
            await manager.handle_control(matching_ack)
            with pytest.raises(ProtocolError):
                await manager.handle_control(
                    matching_ack.model_copy(update={"code": "workspace_file_changed"})
                )
            assert manager.active_count == 0
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_sender_acknowledges_remote_failure_while_source_is_still_requested(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            async with manager.path_locks.hold(str(source.resolve())):
                await manager.handle_control(
                    TransferRequest(
                        id=SLOT,
                        purpose="file_transfer",
                        src_path="source.bin",
                        dst_path="destination.bin",
                    )
                )
                assert manager.slot_state(SLOT) is TransferState.REQUESTED
                await manager.handle_control(
                    TransferEnd(
                        id=SLOT,
                        ack=False,
                        ok=False,
                        code="workspace_transfer_timeout",
                    )
                )
            for _ in range(100):
                await asyncio.sleep(0.001)
                if manager.active_count == 0:
                    break
            frames = [json.loads(item) for item in socket.sent if isinstance(item, str)]
            assert any(
                frame["type"] == "transfer_end"
                and frame["ack"] is True
                and frame["code"] == "workspace_transfer_timeout"
                for frame in frames
            )
            assert manager.active_count == 0
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_sender_acknowledges_remote_failure_end(tmp_path: Path) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(id=SLOT, purpose="http_relay", src_path="source.bin")
            )
            for _ in range(100):
                if manager.slot_state(SLOT) == "BEGUN":
                    break
                await asyncio.sleep(0.001)
            await manager.handle_control(TransferReady(id=SLOT))
            for _ in range(100):
                await asyncio.sleep(0.005)
                if any(
                    isinstance(item, str) and json.loads(item)["type"] == "transfer_end"
                    for item in socket.sent
                ):
                    break
            await manager.handle_control(
                TransferEnd(id=SLOT, ack=False, ok=False, code="workspace_file_changed")
            )
            await asyncio.sleep(0.02)
            acks = [
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str)
                and json.loads(item)["type"] == "transfer_end"
                and json.loads(item).get("ack") is True
            ]
            assert acks[-1]["code"] == "workspace_file_changed"
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_sender_accepts_destination_rejection_before_ready(tmp_path: Path) -> None:
    (tmp_path / "source.bin").write_bytes(b"abc")

    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(
                    id=SLOT,
                    purpose="file_transfer",
                    src_path="source.bin",
                    dst_path="result.bin",
                )
            )
            for _ in range(100):
                if manager.slot_state(SLOT) == "BEGUN":
                    break
                await asyncio.sleep(0.001)
            assert manager.slot_state(SLOT) == "BEGUN"

            await manager.handle_control(
                TransferEnd(id=SLOT, ack=False, ok=False, code="workspace_file_changed")
            )
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)

            frames = [json.loads(item) for item in socket.sent if isinstance(item, str)]
            assert any(
                frame["type"] == "transfer_end"
                and frame["ack"] is True
                and frame["code"] == "workspace_file_changed"
                for frame in frames
            )
            assert not any(isinstance(item, bytes) for item in socket.sent)
            assert manager.active_count == 0
            assert (tmp_path / "source.bin").read_bytes() == b"abc"
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_empty_chunks_do_not_extend_receiver_idle_deadline(tmp_path: Path) -> None:
    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        manager._idle_timeout = 0.04
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="a",
                    dst_path="empty.txt",
                    total_bytes=0,
                )
            )
            await _wait_receiver_ready(manager)
            for _ in range(5):
                if manager.active_count == 0:
                    break
                await manager.handle_binary(encode_binary_chunk(SLOT, b""))
                await asyncio.sleep(0.015)
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
            assert manager.active_count == 0
            await writer.drain()
            assert not (tmp_path / "empty.txt").exists()
            ends = [
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str) and json.loads(item)["type"] == "transfer_end"
            ]
            assert ends[-1]["code"] == "workspace_transfer_timeout"
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_receiver_end_wakes_empty_stream_and_commits_zero_byte_file(tmp_path: Path) -> None:
    async def exercise() -> None:
        manager, writer, socket, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="workspace_upload",
                    dst_path="empty.txt",
                    total_bytes=None,
                )
            )
            await _wait_receiver_ready(manager)
            digest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            await manager.handle_control(
                TransferEnd(id=SLOT, ack=False, ok=True, bytes_sent=0, sha256=digest)
            )
            await _wait_slot_closed(manager, SLOT)
            assert manager.active_count == 0
            await writer.drain()
            assert (tmp_path / "empty.txt").read_bytes() == b""
            assert any(
                isinstance(item, str)
                and json.loads(item)["type"] == "transfer_end"
                and json.loads(item)["ack"] is True
                for item in socket.sent
            )
        finally:
            await _stop(manager, writer, writer_task)

    asyncio.run(exercise())


def test_declared_size_is_rejected_before_receiver_queue_accepts_bytes(tmp_path: Path) -> None:
    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferBegin(
                    id=SLOT,
                    direction="server_to_client",
                    purpose="file_transfer",
                    src_path="a",
                    dst_path="too-large.txt",
                    total_bytes=3,
                )
            )
            await _wait_receiver_ready(manager)
            with pytest.raises(ProtocolError):
                await manager.handle_binary(encode_binary_chunk(SLOT, b"abcd"))
            assert manager.active_count == 1
            await manager.shutdown()
            assert not (tmp_path / "too-large.txt").exists()
        finally:
            await writer.stop()
            await writer_task

    asyncio.run(exercise())
