from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import threading
import time
from pathlib import Path
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
from openoctopus_client.tools.paths import WorkspacePaths
from openoctopus_client.transfer import (
    TOMBSTONE_MAX_ENTRIES,
    TransferConfigSnapshot,
    TransferManager,
    TransferState,
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
) -> tuple[TransferManager, SerializedWriter, Socket, asyncio.Task[None]]:
    socket = Socket()
    writer = SerializedWriter()
    writer_task = asyncio.create_task(writer.run(socket))
    return TransferManager(workspace, writer), writer, socket, writer_task


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
        manager, writer, _, writer_task = await _manager(tmp_path)
        try:
            await manager.handle_control(
                TransferRequest(id=SLOT, purpose="http_relay", src_path="source.bin")
            )
            assert await asyncio.to_thread(started.wait, 1)
            await manager.shutdown()
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


def test_terminal_overflow_is_fatal_and_does_not_record_unsent_end(tmp_path: Path) -> None:
    async def exercise() -> None:
        manager, writer, _, writer_task = await _manager(tmp_path)
        slot = transfer_module._Slot(
            slot_id=SLOT,
            role="receiver",
            purpose="file_transfer",
            snapshot=TransferConfigSnapshot.from_values(
                tmp_path,
                sandbox_mode=True,
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
            await asyncio.sleep(0.05)
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
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
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
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
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
            await asyncio.sleep(0.02)
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

    def gated_parent_check(
        paths: WorkspacePaths, path: str, destination: Path
    ) -> bool:
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

    original_open_temp = transfer_module._open_temp

    def swap_temp(path: Path) -> tuple[object, tuple[int, int, int, int, int]]:
        handle, identity = original_open_temp(path)
        path.unlink()
        path.symlink_to(outside)
        return handle, identity

    monkeypatch.setattr(transfer_module, "_open_temp", swap_temp)

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
            for _ in range(100):
                if manager.active_count == 0:
                    break
                await asyncio.sleep(0.001)
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


def test_receiver_local_failure_before_sender_end_uses_non_ack_and_accepts_late_ack(
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
                        if isinstance(item, str)
                        and json.loads(item)["type"] == "transfer_end"
                    ),
                    None,
                )
                if failure is not None and manager.active_count == 0:
                    break
            assert failure is not None
            assert failure["ack"] is False
            assert failure["ok"] is False
            assert failure["code"] == "workspace_storage_unavailable"

            # The server may already have queued bounded binary frames before
            # it receives the failure terminal.  They are expected drain for
            # this failed slot, not a connection-level protocol violation.
            await manager.handle_binary(encode_binary_chunk(SLOT, b"queued-before-failure"))

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
            await asyncio.sleep(0.01)
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
            await asyncio.sleep(0.01)
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
            await asyncio.sleep(0.05)
            return [json.loads(item) for item in socket.sent if isinstance(item, str)]
        finally:
            await _stop(manager, writer, writer_task)

    frames = asyncio.run(exercise())
    assert target.read_text() == "external"
    assert frames[-1]["code"] == "workspace_file_changed"
    assert frames[-1]["ack"] is True


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
            await asyncio.sleep(0.02)
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
            end = next(
                json.loads(item)
                for item in socket.sent
                if isinstance(item, str) and json.loads(item)["type"] == "transfer_end"
            )
            await manager.handle_control(
                TransferEnd(
                    id=SLOT,
                    ack=True,
                    ok=True,
                    bytes_sent=end["bytes_sent"],
                    sha256=end["sha256"],
                )
            )
            await asyncio.sleep(0.02)
            assert manager.active_count == 0
        finally:
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
            await asyncio.sleep(0.03)
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
            await asyncio.sleep(0.03)
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
