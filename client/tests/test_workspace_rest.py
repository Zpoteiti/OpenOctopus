from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import Any, cast

import pytest

import openoctopus_client.tools.dispatcher as dispatcher_module
import openoctopus_client.transfer as transfer_module
from openoctopus_client.tools import ClientToolDispatcher
from openoctopus_client.tools.common import ToolOutput
from openoctopus_client.tools.locks import PathLocks


class _RecordingLocks(PathLocks):
    def __init__(self) -> None:
        super().__init__()
        self.reservations: list[tuple[str, ...]] = []

    def hold(self, *paths: str):  # type: ignore[no-untyped-def]
        self.reservations.append(paths)
        return super().hold(*paths)


def _run(dispatcher: ClientToolDispatcher, **args: object) -> ToolOutput:
    return asyncio.run(dispatcher.execute("__workspace_rest__", args))


def _json(output: ToolOutput) -> dict[str, Any]:
    assert isinstance(output.content, str)
    return cast(dict[str, Any], json.loads(output.content))


def test_workspace_rest_returns_machine_results_and_etag_guard(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("one\ntwo\n", encoding="utf-8")
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])

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
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])

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


def test_workspace_rest_rejects_deleting_the_workspace_root(tmp_path: Path) -> None:
    (tmp_path / "keep.txt").write_text("keep", encoding="utf-8")
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])

    deleted = _run(dispatcher, operation="delete_folder", path=".")

    assert deleted.code == "workspace_invalid_request"
    assert (tmp_path / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_workspace_rest_rejects_unknown_fields_and_notebook_action(tmp_path: Path) -> None:
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])
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
        sandbox_mode=True,
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
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])

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


def test_workspace_rest_local_transfer_rejects_same_path_links_and_special_files(
    tmp_path: Path,
) -> None:
    (tmp_path / "source.txt").write_text("payload", encoding="utf-8")
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])

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

    if hasattr(os, "symlink"):
        os.symlink(tmp_path / "source.txt", tmp_path / "link.txt")
        linked = _run(
            dispatcher,
            operation="transfer_local",
            path="link.txt",
            dst_path="link-copy.txt",
        )
        assert linked.code == "workspace_symlink_escape"


def test_workspace_rest_local_transfer_detects_external_source_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"before")
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])
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


def test_workspace_rest_local_move_retains_source_if_it_changes_after_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])
    calls = 0

    def change_after_commit(
        _path: Path, _descriptor: int, _initial: tuple[int, int, int, int, int]
    ) -> bool:
        nonlocal calls
        calls += 1
        return calls == 1

    monkeypatch.setattr(dispatcher_module, "_source_unchanged", change_after_commit)
    result = _run(
        dispatcher,
        operation="transfer_local",
        path="source.bin",
        dst_path="destination.bin",
        mode="move",
    )

    assert result.is_error is False
    payload = _json(result)
    assert payload["warnings"] == ["source_delete_failed"]
    assert source.read_bytes() == b"payload"
    assert (tmp_path / "destination.bin").read_bytes() == b"payload"


def test_rest_and_transfer_etags_use_the_same_opaque_stat_fingerprint(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")

    assert dispatcher_module._stat_fingerprint(source) == transfer_module._stat_fingerprint(source)


@pytest.mark.asyncio
async def test_workspace_rest_local_transfer_cancellation_cleans_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "source.bin").write_bytes(b"payload")
    dispatcher = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])
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
    assert not (tmp_path / "destination.bin").exists()
    assert not list(tmp_path.glob(".destination.bin.openoctopus-*") )
