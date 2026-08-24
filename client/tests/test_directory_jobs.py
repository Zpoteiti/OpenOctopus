from __future__ import annotations

import asyncio
import hashlib
import os
import threading
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

import openoctopus_client.tools.directory_jobs as directory_jobs_module
from openoctopus_client.protocol import new_uuid7
from openoctopus_client.tools.common import ToolFailure
from openoctopus_client.tools.directory_contract import (
    DirectoryManifest,
    DirectoryManifestDirectory,
    DirectoryManifestEntry,
    create_directory_manifest,
)
from openoctopus_client.tools.directory_jobs import DirectoryJobManager
from openoctopus_client.tools.fingerprints import opaque_stat_fingerprint
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.tools.workspace_rest import (
    DestinationDirectoryJobStatus,
    DirectoryCleanupResult,
    DirectorySourceProbe,
    DirectoryStableError,
    FileSourceProbe,
    LocalDirectoryJobStatus,
    SourceDirectoryJobStatus,
    WorkspaceTransferDirectoryResult,
    parse_directory_action,
)
from openoctopus_client.transfer_admission import LocalTransferAdmission


def _operation_id() -> str:
    return str(new_uuid7())


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fingerprint(path: Path) -> str:
    info = path.stat(follow_symlinks=False)
    return opaque_stat_fingerprint((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns))


def _manager(
    workspace: Path,
    *,
    admission: LocalTransferAdmission | None = None,
    idle_timeout_seconds: float = 5,
    queue_timeout_seconds: float = 1,
) -> DirectoryJobManager:
    return DirectoryJobManager(
        workspace,
        restrict_to_workspace=True,
        path_locks=PathLocks(),
        admission=admission or LocalTransferAdmission(capacity=2),
        idle_timeout_seconds=idle_timeout_seconds,
        queue_timeout_seconds=queue_timeout_seconds,
        terminal_ttl_seconds=60,
    )


async def _source_status(
    manager: DirectoryJobManager,
    operation_id: str,
    expected_digest: str,
    states: set[str],
) -> Any:
    for _ in range(200):
        status = await manager.handle(
            {
                "operation": "transfer_source_probe_status",
                "directory_operation_id": operation_id,
                "expected_digest": expected_digest,
            }
        )
        expected_digest = status.expected_digest
        if status.state in states:
            return status
        await asyncio.sleep(0.01)
    raise AssertionError("source job did not reach the expected state")


async def _destination_status(
    manager: DirectoryJobManager,
    operation_id: str,
    expected_digest: str,
    states: set[str],
    *,
    local: bool = False,
) -> Any:
    operation = "transfer_local_directory_status" if local else "transfer_directory_status"
    for _ in range(300):
        status = await manager.handle(
            {
                "operation": operation,
                "directory_operation_id": operation_id,
                "expected_digest": expected_digest,
            }
        )
        expected_digest = status.expected_digest
        if status.state in states:
            return status
        await asyncio.sleep(0.01)
    raise AssertionError("destination job did not reach the expected state")


async def _probe_directory(
    manager: DirectoryJobManager,
    operation_id: str,
    path: str,
) -> tuple[DirectoryManifest, str]:
    started = await manager.handle(
        {
            "operation": "transfer_source_probe_start",
            "directory_operation_id": operation_id,
            "path": path,
        }
    )
    status = await _source_status(
        manager,
        operation_id,
        started.expected_digest,
        {"ready_retrieval", "failed"},
    )
    assert status.state == "ready_retrieval"
    assert status.probe is not None and status.probe.kind == "directory"

    directories: list[DirectoryManifestDirectory] = []
    entries: list[DirectoryManifestEntry] = []
    offset = 0
    while True:
        page = await manager.handle(
            {
                "operation": "transfer_source_probe_page",
                "directory_operation_id": operation_id,
                "expected_digest": status.expected_digest,
                "offset": offset,
            }
        )
        for item in page.items:
            if item.kind == "directory":
                directories.append(
                    DirectoryManifestDirectory(
                        relative_path=item.relative_path,
                        identity=item.identity,
                    )
                )
            else:
                entries.append(
                    DirectoryManifestEntry(
                        relative_path=item.relative_path,
                        size=item.size,
                        fingerprint=item.fingerprint,
                    )
                )
        if page.next_offset is None:
            break
        offset = page.next_offset

    manifest = create_directory_manifest(
        root_identity=status.probe.root_identity,
        directories=directories,
        entries=entries,
    )
    assert manifest.manifest_sha256 == status.expected_digest
    return manifest, status.expected_digest


def test_directory_action_union_is_strict_and_requires_uuid7() -> None:
    operation_id = _operation_id()
    parsed = parse_directory_action(
        {
            "operation": "transfer_source_probe_start",
            "directory_operation_id": operation_id,
            "path": "source",
        }
    )
    assert parsed.directory_operation_id == operation_id

    with pytest.raises(ValidationError):
        parse_directory_action(
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": str(UUID(int=1)),
                "path": "source",
            }
        )


def test_directory_status_models_reject_impossible_state_payloads() -> None:
    digest = "0" * 64
    directory_probe = DirectorySourceProbe(
        root_identity="root",
        scanned_entries=1,
        file_count=1,
        total_bytes=1,
        manifest_sha256=digest,
        page_count=1,
    )
    error = DirectoryStableError(code="workspace_file_changed", message="changed")
    result = WorkspaceTransferDirectoryResult(
        files_transferred=1,
        bytes_transferred=1,
        sha256=digest,
    )
    with pytest.raises(ValidationError):
        SourceDirectoryJobStatus(
            state="ready_retrieval",
            expected_digest=digest,
            progress_seq=1,
            entries_processed=1,
            files_processed=1,
            bytes_processed=1,
        )
    with pytest.raises(ValidationError):
        SourceDirectoryJobStatus(
            state="scanning",
            expected_digest=digest,
            progress_seq=0,
            entries_processed=0,
            files_processed=0,
            bytes_processed=0,
            probe=directory_probe,
        )
    SourceDirectoryJobStatus(
        state="succeeded",
        expected_digest=digest,
        progress_seq=1,
        entries_processed=1,
        files_processed=1,
        bytes_processed=1,
        probe=FileSourceProbe(size=1, fingerprint="f"),
    )
    with pytest.raises(ValidationError):
        DestinationDirectoryJobStatus(
            state="finalized_held",
            expected_digest=digest,
            progress_seq=1,
            files_processed=1,
            bytes_processed=1,
        )
    DestinationDirectoryJobStatus(
        state="finalized_held",
        expected_digest=digest,
        progress_seq=1,
        files_processed=1,
        bytes_processed=1,
        terminal_result=result,
    )
    with pytest.raises(ValidationError):
        LocalDirectoryJobStatus(
            state="failed",
            phase="cleanup",
            expected_digest=digest,
            progress_seq=1,
            files_processed=0,
            bytes_processed=0,
        )
    LocalDirectoryJobStatus(
        state="failed",
        phase="cleanup",
        expected_digest=digest,
        progress_seq=1,
        files_processed=0,
        bytes_processed=0,
        terminal_error=error,
    )
    with pytest.raises(ValidationError):
        WorkspaceTransferDirectoryResult(
            files_transferred=1,
            bytes_transferred=1,
            sha256=digest,
            warnings=["source_cleanup_incomplete", "source_changed_after_copy"],
        )
    DirectoryCleanupResult(
        cleanup_complete=False,
        warnings=["source_changed_after_copy", "source_cleanup_incomplete"],
    )
    with pytest.raises(ValidationError):
        parse_directory_action(
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": _operation_id(),
                "path": "source",
                "unexpected": True,
            }
        )


@pytest.mark.asyncio
async def test_source_walk_keeps_hidden_noise_zero_byte_and_scan_only_directories(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    (source / ".git").mkdir(parents=True)
    (source / "node_modules" / "pkg").mkdir(parents=True)
    (source / "empty").mkdir()
    (source / ".git" / "config").write_bytes(b"")
    (source / "node_modules" / "pkg" / "index.js").write_bytes(b"module")

    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, digest = await _probe_directory(manager, operation_id, "source")
        assert [item.relative_path for item in manifest.directories] == [
            ".git",
            "empty",
            "node_modules",
            "node_modules/pkg",
        ]
        assert [(item.relative_path, item.size) for item in manifest.entries] == [
            (".git/config", 0),
            ("node_modules/pkg/index.js", 6),
        ]
        held = await manager.handle(
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        assert held.state == "held"
        released = await manager.handle(
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        assert released.state == "released"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_source_probe_rejects_empty_tree_and_links(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    (tmp_path / "linked-target").mkdir()
    (tmp_path / "linked-target" / "file").write_text("x", encoding="utf-8")
    (tmp_path / "linked").symlink_to(tmp_path / "linked-target", target_is_directory=True)
    manager = _manager(tmp_path)
    try:
        for path, code in (
            ("empty", "workspace_invalid_request"),
            ("linked", "workspace_symlink_escape"),
        ):
            operation_id = _operation_id()
            started = await manager.handle(
                {
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": operation_id,
                    "path": path,
                }
            )
            status = await _source_status(
                manager, operation_id, started.expected_digest, {"failed"}
            )
            assert status.terminal_error.code == code
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_source_hold_authorization_and_conditional_cleanup(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "a").write_bytes(b"a")
    (source / "b").write_bytes(b"b")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, digest = await _probe_directory(manager, operation_id, "source")
        await manager.handle(
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        entry = manifest.entries[0]
        transfer_uuid = _operation_id()
        accepted = await manager.handle(
            {
                "operation": "transfer_directory_authorize_source_child",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": entry.relative_path,
                "fingerprint": entry.fingerprint,
            }
        )
        assert accepted.state == "accepted"
        grant = await manager.consume_source_authorization(
            UUID(transfer_uuid), source / entry.relative_path
        )
        assert grant.directory_operation_id == UUID(operation_id)
        await manager.complete_source_authorization(UUID(transfer_uuid), success=True)

        (source / "b").write_bytes(b"changed")
        await manager.handle(
            {
                "operation": "transfer_source_cleanup",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        status = await _source_status(
            manager, operation_id, digest, {"succeeded", "outcome_unknown"}
        )
        assert status.state == "succeeded"
        assert status.terminal_result.warnings == [
            "source_changed_after_copy",
            "source_cleanup_incomplete",
        ]
        assert not (source / "a").exists()
        assert (source / "b").read_bytes() == b"changed"
    finally:
        await manager.aclose()


def _small_manifest() -> DirectoryManifest:
    return create_directory_manifest(
        root_identity="source-root",
        directories=(DirectoryManifestDirectory(relative_path="nested", identity="dir"),),
        entries=(
            DirectoryManifestEntry(relative_path="nested/file", size=4, fingerprint="source"),
        ),
    )


@pytest.mark.asyncio
async def test_destination_prepare_authorize_finish_and_release(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await manager.handle(
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            }
        )
        status = await _destination_status(
            manager, operation_id, started.expected_digest, {"ready", "failed"}
        )
        assert status.state == "ready"
        assert not (tmp_path / "destination").exists()

        await manager.handle(
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        status = await _destination_status(
            manager, operation_id, started.expected_digest, {"reserved", "failed"}
        )
        assert status.state == "reserved"
        destination = tmp_path / "destination" / "nested" / "file"
        destination.parent.mkdir()

        transfer_uuid = _operation_id()
        await manager.handle(
            {
                "operation": "transfer_directory_authorize_child",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "nested/file",
            }
        )
        grant = await manager.consume_destination_authorization(UUID(transfer_uuid), destination)
        assert grant.relative_path == "nested/file"
        destination.write_bytes(b"data")
        await manager.record_destination_commit(
            UUID(operation_id),
            UUID(transfer_uuid),
            relative_path="nested/file",
            destination_fingerprint=_fingerprint(destination),
            verified_size=4,
            verified_sha256=_sha(b"data"),
        )

        await manager.handle(
            {
                "operation": "transfer_directory_finish",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        status = await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"finalized_held", "outcome_unknown"},
        )
        assert status.state == "finalized_held"
        assert status.terminal_result.sha256 == _sha(
            b"openoctopus-directory-content-v1\0"
            + len(b"nested/file").to_bytes(4, "big")
            + b"nested/file"
            + (4).to_bytes(8, "big")
            + bytes.fromhex(_sha(b"data"))
        )

        released = await manager.handle(
            {
                "operation": "transfer_directory_release",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        assert released.state == "released"
        assert destination.read_bytes() == b"data"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_destination_cancel_conditionally_removes_owned_tree(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await manager.handle(
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            }
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await manager.handle(
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        await manager.handle(
            {
                "operation": "transfer_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        status = await _destination_status(
            manager, operation_id, started.expected_digest, {"failed", "outcome_unknown"}
        )
        assert status.state == "failed"
        assert not (tmp_path / "destination").exists()
    finally:
        await manager.aclose()


async def _prepare_local_job(
    manager: DirectoryJobManager,
    operation_id: str,
    source_path: str,
    destination_path: str,
) -> tuple[DirectoryManifest, str, str]:
    manifest, source_digest = await _probe_directory(manager, operation_id, source_path)
    await manager.handle(
        {
            "operation": "transfer_source_probe_release",
            "directory_operation_id": operation_id,
            "expected_digest": source_digest,
        }
    )
    preflight = await manager.handle(
        {
            "operation": "transfer_directory_preflight",
            "directory_operation_id": operation_id,
            "dst_path": destination_path,
            "manifest": manifest.model_dump(mode="json"),
        }
    )
    await _destination_status(manager, operation_id, preflight.expected_digest, {"ready"})
    return manifest, source_digest, preflight.expected_digest


@pytest.mark.asyncio
async def test_local_directory_copy_is_recursive_atomic_per_file_and_omits_empty_dirs(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    (source / "nested").mkdir(parents=True)
    (source / "empty").mkdir()
    (source / ".hidden").write_bytes(b"")
    (source / "nested" / "file").write_bytes(b"payload")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, _, destination_digest = await _prepare_local_job(
            manager, operation_id, "source", "copied"
        )
        await manager.handle(
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "copied",
                "mode": "copy",
                "manifest_sha256": manifest.manifest_sha256,
            }
        )
        status = await _destination_status(
            manager,
            operation_id,
            destination_digest,
            {"succeeded", "failed", "outcome_unknown"},
            local=True,
        )
        assert status.state == "succeeded"
        assert status.terminal_result.files_transferred == 2
        assert (source / "nested" / "file").read_bytes() == b"payload"
        assert (tmp_path / "copied" / "nested" / "file").read_bytes() == b"payload"
        assert (tmp_path / "copied" / ".hidden").read_bytes() == b""
        assert not (tmp_path / "copied" / "empty").exists()
        assert not list((tmp_path / "copied").rglob("*.openoctopus-*"))
    finally:
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.sys.platform.startswith("linux"), reason="Linux native proof")
async def test_local_directory_move_uses_native_no_replace_and_preserves_empty_dirs(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    (source / "empty").mkdir(parents=True)
    (source / "file").write_bytes(b"payload")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, _, destination_digest = await _prepare_local_job(
            manager, operation_id, "source", "moved"
        )
        await manager.handle(
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "moved",
                "mode": "move",
                "manifest_sha256": manifest.manifest_sha256,
            }
        )
        status = await _destination_status(
            manager,
            operation_id,
            destination_digest,
            {"succeeded", "failed", "outcome_unknown"},
            local=True,
        )
        assert status.state == "succeeded"
        assert not source.exists()
        assert (tmp_path / "moved" / "file").read_bytes() == b"payload"
        assert (tmp_path / "moved" / "empty").is_dir()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_local_cancel_stops_forward_work_but_not_cleanup(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"payload")
    admission = LocalTransferAdmission(capacity=1)
    manager = _manager(tmp_path, admission=admission)
    operation_id = _operation_id()
    try:
        manifest, _, destination_digest = await _prepare_local_job(
            manager, operation_id, "source", "destination"
        )
        blocker = admission.try_acquire()
        assert blocker is not None
        await manager.handle(
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "destination",
                "mode": "copy",
                "manifest_sha256": manifest.manifest_sha256,
            }
        )
        await manager.handle(
            {
                "operation": "transfer_local_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
            }
        )
        record = manager._active[(UUID(operation_id), "destination")]
        assert record.stop_forward_work.is_set()
        assert not record.stop_cleanup.is_set()
        blocker.release()
        status = await _destination_status(
            manager,
            operation_id,
            destination_digest,
            {"failed", "outcome_unknown"},
            local=True,
        )
        assert status.state == "failed"
        assert not (tmp_path / "destination").exists()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_wrong_digest_is_rejected_and_release_is_idempotent(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"x")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, digest = await _probe_directory(manager, operation_id, "source")
        assert manifest.entries
        with pytest.raises(ToolFailure) as raised:
            await manager.handle(
                {
                    "operation": "transfer_source_probe_release",
                    "directory_operation_id": operation_id,
                    "expected_digest": "0" * 64,
                }
            )
        assert raised.value.code == "workspace_transfer_integrity_failed"

        first = await manager.handle(
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        second = await manager.handle(
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        assert first.model_dump() == second.model_dump()
        assert manager.active_count == 0
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_source_authorization_is_one_shot_and_expires_on_release(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"x")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, digest = await _probe_directory(manager, operation_id, "source")
        await manager.handle(
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        transfer_uuid = _operation_id()
        action = {
            "operation": "transfer_directory_authorize_source_child",
            "directory_operation_id": operation_id,
            "expected_digest": digest,
            "transfer_uuid": transfer_uuid,
            "relative_path": "file",
            "fingerprint": manifest.entries[0].fingerprint,
        }
        assert (await manager.handle(action)).state == "accepted"
        assert (await manager.handle(action)).state == "accepted"
        with pytest.raises(ToolFailure):
            await manager.consume_source_authorization(UUID(transfer_uuid), source / "wrong")
        with pytest.raises(ToolFailure):
            await manager.consume_source_authorization(UUID(transfer_uuid), source / "file")

        second_uuid = _operation_id()
        action["transfer_uuid"] = second_uuid
        await manager.handle(action)
        await manager.consume_source_authorization(UUID(second_uuid), source / "file")
        await manager.complete_source_authorization(UUID(second_uuid), success=False)
        await manager.handle(
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        with pytest.raises(ToolFailure):
            await manager.consume_source_authorization(UUID(second_uuid), source / "file")
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_directory_job_active_capacity_rejects_third_before_work(tmp_path: Path) -> None:
    for name in ("one", "two", "three"):
        directory = tmp_path / name
        directory.mkdir()
        (directory / "file").write_bytes(b"x")
    admission = LocalTransferAdmission(capacity=1)
    blocker = admission.try_acquire()
    assert blocker is not None
    manager = _manager(tmp_path, admission=admission)
    try:
        for name in ("one", "two"):
            await manager.handle(
                {
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": _operation_id(),
                    "path": name,
                }
            )
        with pytest.raises(ToolFailure) as raised:
            await manager.handle(
                {
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": _operation_id(),
                    "path": "three",
                }
            )
        assert raised.value.code == "workspace_transfer_busy"
        assert manager.active_count == 2
    finally:
        blocker.release()
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(os.name == "nt", reason="mkfifo is a POSIX primitive")
async def test_source_probe_rejects_special_file(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    os.mkfifo(source / "pipe")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        started = await manager.handle(
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": operation_id,
                "path": "source",
            }
        )
        status = await _source_status(manager, operation_id, started.expected_digest, {"failed"})
        assert status.terminal_error.code == "workspace_blocked_path"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_manifest_page_retry_is_stable_and_page_skip_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for index in range(257):
        (source / f"{index:04d}").write_bytes(b"")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        started = await manager.handle(
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": operation_id,
                "path": "source",
            }
        )
        status = await _source_status(
            manager,
            operation_id,
            started.expected_digest,
            {"ready_retrieval"},
        )
        page_action = {
            "operation": "transfer_source_probe_page",
            "directory_operation_id": operation_id,
            "expected_digest": status.expected_digest,
            "offset": 256,
        }
        with pytest.raises(ToolFailure):
            await manager.handle(page_action)
        page_action["offset"] = 0
        first = await manager.handle(page_action)
        retry = await manager.handle(page_action)
        assert first.model_dump() == retry.model_dump()
        page_action["offset"] = 256
        final = await manager.handle(page_action)
        assert final.next_offset is None
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_consumed_source_authorization_blocks_next_child_cleanup_and_release(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"x")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, digest = await _probe_directory(manager, operation_id, "source")
        await manager.handle(
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        transfer_uuid = _operation_id()
        await manager.handle(
            {
                "operation": "transfer_directory_authorize_source_child",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "file",
                "fingerprint": manifest.entries[0].fingerprint,
            }
        )
        await manager.consume_source_authorization(UUID(transfer_uuid), source / "file")

        for operation in ("transfer_source_cleanup", "transfer_source_probe_release"):
            with pytest.raises(ToolFailure) as raised:
                await manager.handle(
                    {
                        "operation": operation,
                        "directory_operation_id": operation_id,
                        "expected_digest": digest,
                    }
                )
            assert raised.value.code == "workspace_transfer_busy"
        with pytest.raises(ToolFailure) as raised:
            await manager.handle(
                {
                    "operation": "transfer_directory_authorize_source_child",
                    "directory_operation_id": operation_id,
                    "expected_digest": digest,
                    "transfer_uuid": _operation_id(),
                    "relative_path": "file",
                    "fingerprint": manifest.entries[0].fingerprint,
                }
            )
        assert raised.value.code == "workspace_transfer_busy"

        await manager.complete_source_authorization(UUID(transfer_uuid), success=True)
        released = await manager.handle(
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        assert released.state == "released"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_unconsumed_authorization_and_ready_job_expire_in_background(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"x")
    manager = _manager(tmp_path, idle_timeout_seconds=0.05)
    operation_id = _operation_id()
    try:
        manifest, digest = await _probe_directory(manager, operation_id, "source")
        await manager.handle(
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            }
        )
        transfer_uuid = _operation_id()
        await manager.handle(
            {
                "operation": "transfer_directory_authorize_source_child",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "file",
                "fingerprint": manifest.entries[0].fingerprint,
            }
        )
        await asyncio.sleep(0.12)
        with pytest.raises(ToolFailure):
            await manager.consume_source_authorization(UUID(transfer_uuid), source / "file")
        status = await _source_status(manager, operation_id, digest, {"failed"})
        assert status.terminal_error.code == "workspace_transfer_timeout"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_manager_close_is_bounded_and_exposes_drain_tasks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"x")
    entered = threading.Event()
    release = threading.Event()
    original = directory_jobs_module._scan_source_path

    def blocked_scan(*args: Any, **kwargs: Any) -> Any:
        entered.set()
        release.wait(timeout=5)
        return original(*args, **kwargs)

    monkeypatch.setattr(directory_jobs_module, "_scan_source_path", blocked_scan)
    manager = _manager(tmp_path)
    await manager.handle(
        {
            "operation": "transfer_source_probe_start",
            "directory_operation_id": _operation_id(),
            "path": "source",
        }
    )
    assert await asyncio.to_thread(entered.wait, 1)
    started = asyncio.get_running_loop().time()
    assert await manager.aclose(grace_seconds=0.01) is False
    assert asyncio.get_running_loop().time() - started < 0.2
    assert manager.drain_tasks
    release.set()
    assert await manager.wait_for_drain(timeout_seconds=1)


@pytest.mark.asyncio
async def test_finalize_failure_reuses_current_capacity_for_cleanup(tmp_path: Path) -> None:
    admission = LocalTransferAdmission(capacity=1)
    manager = _manager(tmp_path, admission=admission)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await manager.handle(
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            }
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await manager.handle(
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        destination = tmp_path / "destination" / "nested" / "file"
        destination.parent.mkdir()
        transfer_uuid = _operation_id()
        await manager.handle(
            {
                "operation": "transfer_directory_authorize_child",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "nested/file",
            }
        )
        await manager.consume_destination_authorization(UUID(transfer_uuid), destination)
        destination.write_bytes(b"data")
        await manager.record_destination_commit(
            UUID(operation_id),
            UUID(transfer_uuid),
            relative_path="nested/file",
            destination_fingerprint=_fingerprint(destination),
            verified_size=4,
            verified_sha256=_sha(b"data"),
        )
        (tmp_path / "destination" / "extra").write_bytes(b"race")
        await manager.handle(
            {
                "operation": "transfer_directory_finish",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        status = await asyncio.wait_for(
            _destination_status(
                manager,
                operation_id,
                started.expected_digest,
                {"failed", "outcome_unknown"},
            ),
            timeout=1,
        )
        assert status.state == "outcome_unknown"
        assert admission.active_count == 0
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_destination_cancel_clears_consumed_authorization(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await manager.handle(
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            }
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await manager.handle(
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        transfer_uuid = _operation_id()
        await manager.handle(
            {
                "operation": "transfer_directory_authorize_child",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "nested/file",
            }
        )
        await manager.consume_destination_authorization(
            UUID(transfer_uuid), tmp_path / "destination" / "nested" / "file"
        )
        await manager.handle(
            {
                "operation": "transfer_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            }
        )
        await _destination_status(
            manager, operation_id, started.expected_digest, {"failed", "outcome_unknown"}
        )
        assert not manager._consumed_destination
    finally:
        await manager.aclose()
