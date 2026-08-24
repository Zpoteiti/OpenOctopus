from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import threading
from collections.abc import Hashable
from pathlib import Path
from typing import Any, cast

import pytest

import openoctopus_client.tools.dispatcher as dispatcher_module
import openoctopus_client.transfer as transfer_module
from openoctopus_client.tools import ClientToolDispatcher
from openoctopus_client.tools.common import ToolFailure, ToolOutput
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.tools.workspace_rest import INTERNAL_WORKSPACE_ACTION
from openoctopus_client.transfer_admission import (
    LocalTransferAdmission,
    LocalTransferDrainRegistry,
)


class _RecordingLocks(PathLocks):
    def __init__(self) -> None:
        super().__init__()
        self.reservations: list[tuple[str, ...]] = []

    def hold(  # type: ignore[no-untyped-def]
        self, *paths: str, owner: Hashable | None = None
    ):
        self.reservations.append(paths)
        return super().hold(*paths, owner=owner)


def _run(dispatcher: ClientToolDispatcher, **args: object) -> ToolOutput:
    return asyncio.run(dispatcher.execute("__workspace_rest__", args))


def _json(output: ToolOutput) -> dict[str, Any]:
    assert isinstance(output.content, str)
    return cast(dict[str, Any], json.loads(output.content))


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


def test_workspace_rest_returns_machine_results_and_etag_guard(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("one\ntwo\n", encoding="utf-8")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])

    edited = _run(
        dispatcher,
        operation="edit_file",
        path="notes.txt",
        old_text="one",
        new_text="ONE",
    )
    assert edited.is_error is False
    payload = _json(edited)
    assert set(payload) == {"path", "size", "etag", "created", "replacements"}

    stale = _run(
        dispatcher,
        operation="edit_file",
        path="notes.txt",
        old_text="two",
        new_text="TWO",
        if_match="stale",
    )
    assert stale.code == "workspace_file_changed"


def test_workspace_rest_patch_delete_and_search_are_structured(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("print('hello')\n", encoding="utf-8")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])

    patched = _run(
        dispatcher,
        operation="apply_patch",
        edits=[{"path": "a.py", "action": "add", "new_text": "# tail\n"}],
    )
    assert _json(patched)["items"][0]["path"] == "a.py"

    found = _run(dispatcher, operation="find_files", path=".", type="py")
    assert _json(found)["items"][0]["path"] == "a.py"
    matched = _run(
        dispatcher,
        operation="grep",
        path=".",
        pattern="hello",
        output_mode="files_with_matches",
    )
    assert _json(matched)["items"][0]["path"] == "a.py"

    (tmp_path / "folder").mkdir()
    deleted = _run(dispatcher, operation="delete_folder", path="folder")
    assert _json(deleted) == {"deleted": True}


def test_workspace_rest_grep_preserves_lookahead_for_next_offset(tmp_path: Path) -> None:
    (tmp_path / "matches.txt").write_text("hit one\nhit two\nhit three\n", encoding="utf-8")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])

    first = _json(
        _run(
            dispatcher,
            operation="grep",
            path=".",
            pattern="hit",
            output_mode="content",
            limit=2,
        )
    )
    second = _json(
        _run(
            dispatcher,
            operation="grep",
            path=".",
            pattern="hit",
            output_mode="content",
            limit=2,
            offset=2,
        )
    )

    assert [item["line_number"] for item in first["items"]] == [1, 2]
    assert first["next_offset"] == 2
    assert first["truncated"] is False
    assert [item["line_number"] for item in second["items"]] == [3]
    assert second["next_offset"] is None
    assert second["truncated"] is False


@pytest.mark.parametrize("operation", ["list_dir", "find_files"])
def test_workspace_rest_scan_cap_keeps_pages_within_the_retained_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])
    entries = [(f"directory-{index:05d}", 0.0, True) for index in range(10_000)]
    monkeypatch.setattr(dispatcher, "_walk", lambda _root: entries)
    extra = {"recursive": True} if operation == "list_dir" else {"include_dirs": True}

    first = _json(
        _run(
            dispatcher,
            operation=operation,
            path=".",
            limit=100,
            offset=0,
            **extra,
        )
    )
    second = _json(
        _run(
            dispatcher,
            operation=operation,
            path=".",
            limit=100,
            offset=100,
            **extra,
        )
    )
    last = _json(
        _run(
            dispatcher,
            operation=operation,
            path=".",
            limit=100,
            offset=9_900,
            **extra,
        )
    )

    assert first["truncated"] is True
    assert first["next_offset"] == 100
    assert second["next_offset"] == 200
    assert last["next_offset"] is None


def test_workspace_rest_rejects_deleting_the_workspace_root(tmp_path: Path) -> None:
    (tmp_path / "keep.txt").write_text("keep", encoding="utf-8")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])

    deleted = _run(dispatcher, operation="delete_folder", path=".")

    assert deleted.code == "workspace_invalid_request"
    assert (tmp_path / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_workspace_rest_rejects_unknown_fields_and_notebook_action(tmp_path: Path) -> None:
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])
    unknown = _run(dispatcher, operation="list_dir", path=".", untrusted=True)
    notebook = _run(dispatcher, operation="notebook_edit", path="book.ipynb")
    assert unknown.code == "tool_invalid_args"
    assert notebook.code == "tool_invalid_args"


def test_workspace_rest_read_only_queries_do_not_reserve_every_scanned_path(
    tmp_path: Path,
) -> None:
    (tmp_path / "a.py").write_text("print('hello')\n", encoding="utf-8")
    locks = _RecordingLocks()
    dispatcher = ClientToolDispatcher(
        tmp_path,
        restrict_to_workspace=True,
        ssrf_denylist=[],
        path_locks=locks,
    )

    assert _run(dispatcher, operation="list_dir", path=".").is_error is False
    assert _run(dispatcher, operation="find_files", path=".").is_error is False
    assert _run(
        dispatcher,
        operation="grep",
        path=".",
        pattern="hello",
    ).is_error is False
    assert locks.reservations == []


def test_workspace_rest_local_copy_and_move_return_digest_without_overwrite(
    tmp_path: Path,
) -> None:
    payload = b"local transfer\n" * 10000
    (tmp_path / "source.bin").write_bytes(payload)
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])

    copied = _run(
        dispatcher,
        operation="transfer_local",
        path="source.bin",
        dst_path="copy.bin",
        mode="copy",
    )
    assert copied.is_error is False
    result = _json(copied)
    assert result == {
        "kind": "file",
        "files_transferred": 1,
        "bytes_transferred": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "warnings": [],
    }
    assert (tmp_path / "source.bin").read_bytes() == payload
    assert (tmp_path / "copy.bin").read_bytes() == payload

    moved = _run(
        dispatcher,
        operation="transfer_local",
        path="copy.bin",
        dst_path="moved.bin",
        mode="move",
    )
    assert moved.is_error is False
    assert (tmp_path / "copy.bin").exists() is False
    assert (tmp_path / "moved.bin").read_bytes() == payload

    conflict = _run(
        dispatcher,
        operation="transfer_local",
        path="source.bin",
        dst_path="moved.bin",
    )
    assert conflict.code == "workspace_file_changed"
    assert (tmp_path / "moved.bin").read_bytes() == payload


def test_workspace_rest_local_transfer_waits_for_shared_runtime_capacity(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        (tmp_path / "source.bin").write_bytes(b"payload")
        admission = LocalTransferAdmission(capacity=2)
        drains = LocalTransferDrainRegistry()
        first = admission.try_acquire()
        second = admission.try_acquire()
        assert first is not None and second is not None
        dispatcher = ClientToolDispatcher(
            tmp_path,
            restrict_to_workspace=True,
            ssrf_denylist=[],
            transfer_admission=admission,
            transfer_drains=drains,
        )

        task = asyncio.create_task(
            dispatcher.execute(
                "__workspace_rest__",
                {
                    "operation": "transfer_local",
                    "path": "source.bin",
                    "dst_path": "copy.bin",
                    "mode": "copy",
                },
            )
        )
        while admission.waiting_count != 1:
            await asyncio.sleep(0)
        assert task.done() is False

        first.release()
        result = await asyncio.wait_for(task, timeout=1)
        assert result.is_error is False
        assert admission.active_count == 1
        second.release()
        assert admission.active_count == 0

    asyncio.run(exercise())


def test_cancelled_local_transfer_hands_late_open_and_slot_to_runtime_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    original_open = dispatcher_module._open_transfer_source
    started = threading.Event()
    release = threading.Event()
    opened: list[tuple[int, tuple[int, int, int, int, int]]] = []

    def delayed_open(
        path: Path, delete_access: bool = False
    ) -> tuple[int, tuple[int, int, int, int, int]]:
        result = original_open(path, delete_access)
        opened.append(result)
        started.set()
        release.wait(timeout=2)
        return result

    monkeypatch.setattr(dispatcher_module, "_open_transfer_source", delayed_open)

    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=1)
        drains = LocalTransferDrainRegistry()
        dispatcher = ClientToolDispatcher(
            tmp_path,
            restrict_to_workspace=True,
            ssrf_denylist=[],
            transfer_admission=admission,
            transfer_drains=drains,
        )
        task = asyncio.create_task(
            dispatcher.execute(
                "__workspace_rest__",
                {
                    "operation": "transfer_local",
                    "path": "source.bin",
                    "dst_path": "copy.bin",
                    "mode": "copy",
                },
            )
        )
        assert await asyncio.to_thread(started.wait, 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)

        assert admission.active_count == 1
        assert admission.try_acquire() is None
        assert dispatcher.has_pending_blocking() is False
        assert dispatcher._locks.reservation_count == 2

        release.set()
        assert await drains.wait(timeout_seconds=1)
        assert admission.active_count == 0
        assert dispatcher._locks.reservation_count == 0
        with pytest.raises(OSError):
            os.fstat(opened[0][0])

    try:
        asyncio.run(exercise())
    finally:
        release.set()


def test_cancelled_local_transfer_retains_fd_lock_and_slot_until_check_drains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    original_unchanged = dispatcher_module._source_unchanged
    started = threading.Event()
    release = threading.Event()
    descriptor: list[int] = []

    def delayed_unchanged(
        path: Path, source_fd: int, initial: tuple[int, int, int, int, int]
    ) -> bool:
        descriptor.append(source_fd)
        started.set()
        release.wait(timeout=2)
        return original_unchanged(path, source_fd, initial)

    monkeypatch.setattr(dispatcher_module, "_source_unchanged", delayed_unchanged)

    async def transfer(dispatcher: ClientToolDispatcher) -> ToolOutput:
        return await dispatcher.execute(
            "__workspace_rest__",
            {
                "operation": "transfer_local",
                "path": "source.bin",
                "dst_path": "copy.bin",
                "mode": "copy",
            },
        )

    async def exercise() -> None:
        admission = LocalTransferAdmission(capacity=2)
        drains = LocalTransferDrainRegistry()
        dispatcher = ClientToolDispatcher(
            tmp_path,
            restrict_to_workspace=True,
            ssrf_denylist=[],
            transfer_admission=admission,
            transfer_drains=drains,
        )
        first = asyncio.create_task(transfer(dispatcher))
        assert await asyncio.to_thread(started.wait, 1)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(first, timeout=1)

        assert os.fstat(descriptor[0]).st_size == len(b"payload")
        assert dispatcher._locks.reservation_count == 2
        assert admission.active_count == 1

        second = asyncio.create_task(transfer(dispatcher))
        while admission.active_count != 2:
            await asyncio.sleep(0)
        await asyncio.sleep(0.01)
        assert second.done() is False
        assert dispatcher._locks.reservation_count == 2

        release.set()
        result = await asyncio.wait_for(second, timeout=1)
        assert result.is_error is False
        assert await drains.wait(timeout_seconds=1)
        assert admission.active_count == 0
        assert dispatcher._locks.reservation_count == 0
        with pytest.raises(OSError):
            os.fstat(descriptor[0])

    try:
        asyncio.run(exercise())
    finally:
        release.set()


def test_workspace_rest_local_transfer_rejects_same_path_links_and_special_files(
    tmp_path: Path,
) -> None:
    (tmp_path / "source.txt").write_text("payload", encoding="utf-8")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])

    same = _run(
        dispatcher,
        operation="transfer_local",
        path="source.txt",
        dst_path="source.txt",
    )
    assert same.code == "workspace_invalid_request"

    if hasattr(os, "mkfifo"):
        os.mkfifo(tmp_path / "pipe")
        special = _run(
            dispatcher,
            operation="transfer_local",
            path="pipe",
            dst_path="pipe-copy",
        )
        assert special.code == "workspace_blocked_path"

    link_target = tmp_path / "link-target"
    link_target.mkdir()
    (link_target / "source.txt").write_text("payload", encoding="utf-8")
    _make_directory_link(tmp_path / "link", link_target)
    linked = _run(
        dispatcher,
        operation="transfer_local",
        path="link/source.txt",
        dst_path="link-copy.txt",
    )
    assert linked.code == "workspace_symlink_escape"


def test_workspace_rest_local_transfer_detects_external_source_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"before")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])
    original_unchanged = dispatcher_module._source_unchanged

    def change_before_commit(
        path: Path, descriptor: int, initial: tuple[int, int, int, int, int]
    ) -> bool:
        source.write_bytes(b"after")
        return original_unchanged(path, descriptor, initial)

    monkeypatch.setattr(dispatcher_module, "_source_unchanged", change_before_commit)
    result = _run(
        dispatcher,
        operation="transfer_local",
        path="source.bin",
        dst_path="destination.bin",
    )
    assert result.code == "workspace_file_changed"
    assert (tmp_path / "destination.bin").exists() is False
    assert not list(tmp_path.glob(".destination.bin.openoctopus-*") )


def test_workspace_rest_local_move_uses_native_rename_without_hard_link(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])
    monkeypatch.setattr(
        dispatcher_module,
        "_link_transfer_no_replace",
        lambda *_args: (_ for _ in ()).throw(AssertionError("move used hard link")),
    )
    result = _run(
        dispatcher,
        operation="transfer_local",
        path="source.bin",
        dst_path="destination.bin",
        mode="move",
    )

    assert result.is_error is False
    assert _json(result)["warnings"] == []
    assert source.exists() is False
    assert (tmp_path / "destination.bin").read_bytes() == b"payload"


def test_workspace_rest_local_move_hashes_the_content_that_was_renamed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"before")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])
    native_rename = dispatcher_module._rename_transfer_no_replace

    def change_then_rename(
        source_path: Path, destination_path: Path, source_fd: int
    ) -> None:
        source.write_bytes(b"after")
        native_rename(source_path, destination_path, source_fd)

    monkeypatch.setattr(
        dispatcher_module,
        "_rename_transfer_no_replace",
        change_then_rename,
    )

    result = _run(
        dispatcher,
        operation="transfer_local",
        path="source.bin",
        dst_path="destination.bin",
        mode="move",
    )

    assert result.is_error is False
    assert destination.read_bytes() == b"after"
    assert _json(result)["sha256"] == hashlib.sha256(b"after").hexdigest()


def test_workspace_rest_local_move_rehashes_windows_identity_stable_commit_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    before = b"before"
    after = b"after!"
    source.write_bytes(before)
    host_is_windows = os.name == "nt"
    source_fd = (
        dispatcher_module._open_windows_transfer_source(source, delete_access=False)
        if host_is_windows
        else os.open(source, os.O_RDONLY | int(getattr(os, "O_BINARY", 0)))
    )
    native_rename = dispatcher_module._rename_transfer_no_replace
    monkeypatch.setattr(os, "name", "nt")
    initial_info = os.fstat(source_fd)
    initial = dispatcher_module._transfer_identity(initial_info)

    def change_then_rename(
        source_path: Path, destination_path: Path, descriptor: int
    ) -> None:
        source_path.write_bytes(after)
        os.utime(
            source_path,
            ns=(initial_info.st_atime_ns, initial_info.st_mtime_ns),
        )
        if host_is_windows:
            native_rename(source_path, destination_path, descriptor)
        else:
            os.rename(source_path, destination_path)

    monkeypatch.setattr(
        dispatcher_module,
        "_rename_transfer_no_replace",
        change_then_rename,
    )
    try:
        bytes_transferred, digest = dispatcher_module._rename_verify_and_hash_fd(
            source,
            destination,
            source_fd,
            initial,
            len(before),
            hashlib.sha256(before).hexdigest(),
        )
    finally:
        os.close(source_fd)

    assert destination.read_bytes() == after
    assert bytes_transferred == len(after)
    assert digest == hashlib.sha256(after).hexdigest()


def test_workspace_rest_local_move_returns_result_after_cancel_follows_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"payload")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])
    native_rename = dispatcher_module._rename_transfer_no_replace
    rename_completed = threading.Event()
    release_hash = threading.Event()

    def rename_then_signal(
        source_path: Path, destination_path: Path, source_fd: int
    ) -> None:
        source.write_bytes(b"updated")
        native_rename(source_path, destination_path, source_fd)
        rename_completed.set()

    original_read = os.read

    def slow_read(descriptor: int, size: int) -> bytes:
        if rename_completed.is_set():
            release_hash.wait(timeout=2)
        return original_read(descriptor, size)

    monkeypatch.setattr(dispatcher_module, "_rename_transfer_no_replace", rename_then_signal)
    monkeypatch.setattr(os, "read", slow_read)

    async def exercise() -> ToolOutput:
        task = asyncio.create_task(
            dispatcher.execute(
                INTERNAL_WORKSPACE_ACTION,
                {
                    "operation": "transfer_local",
                    "path": "source.bin",
                    "dst_path": "destination.bin",
                    "mode": "move",
                },
            )
        )
        assert await asyncio.to_thread(rename_completed.wait, 1)
        await asyncio.sleep(0)
        assert task.done() is False
        task.cancel()
        await asyncio.sleep(0)
        assert task.done() is False
        release_hash.set()
        return await asyncio.wait_for(task, timeout=1)

    result = asyncio.run(exercise())

    assert result.is_error is False
    assert source.exists() is False
    assert destination.read_bytes() == b"updated"
    assert _json(result)["sha256"] == hashlib.sha256(b"updated").hexdigest()


def test_workspace_rest_local_move_cross_volume_failure_leaves_both_paths_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])

    def reject_cross_volume(
        source_path: Path, destination_path: Path, source_fd: int
    ) -> None:
        del source_path, destination_path, source_fd
        raise ToolFailure(
            "workspace_storage_unavailable",
            "Same-volume exclusive move is required",
        )

    monkeypatch.setattr(
        dispatcher_module,
        "_rename_transfer_no_replace",
        reject_cross_volume,
    )
    result = _run(
        dispatcher,
        operation="transfer_local",
        path="source.bin",
        dst_path="destination.bin",
        mode="move",
    )

    assert result.code == "workspace_storage_unavailable"
    assert source.read_bytes() == b"payload"
    assert (tmp_path / "destination.bin").exists() is False


def test_rest_and_transfer_etags_use_the_same_opaque_stat_fingerprint(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")

    assert dispatcher_module._stat_fingerprint(source) == transfer_module._stat_fingerprint(source)


def test_workspace_rest_local_transfer_cancellation_cleans_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "source.bin").write_bytes(b"payload")
    dispatcher = ClientToolDispatcher(tmp_path, restrict_to_workspace=True, ssrf_denylist=[])

    async def exercise() -> None:
        started = asyncio.Event()

        async def blocked_stream(source_fd: int, destination_fd: int) -> tuple[int, str]:
            del source_fd, destination_fd
            started.set()
            await asyncio.sleep(60)
            raise AssertionError("cancelled transfer resumed")

        monkeypatch.setattr(dispatcher_module, "_stream_fd", blocked_stream)
        task = asyncio.create_task(
            dispatcher.execute(
                "__workspace_rest__",
                {
                    "operation": "transfer_local",
                    "path": "source.bin",
                    "dst_path": "destination.bin",
                },
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(exercise())
    assert not (tmp_path / "destination.bin").exists()
    assert not list(tmp_path.glob(".destination.bin.openoctopus-*"))
