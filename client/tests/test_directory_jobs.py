from __future__ import annotations

import asyncio
import errno
import hashlib
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, cast
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
from openoctopus_client.tools.directory_jobs import (
    DirectoryJobManager,
    DirectoryLifecycleCredits,
)
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


def _manager(
    workspace: Path,
    *,
    admission: LocalTransferAdmission | None = None,
    path_locks: PathLocks | None = None,
    idle_timeout_seconds: float = 5,
    queue_timeout_seconds: float = 1,
    lifecycle_credits: DirectoryLifecycleCredits | None = None,
    terminal_ttl_seconds: float = 60,
) -> DirectoryJobManager:
    return DirectoryJobManager(
        workspace,
        restrict_to_workspace=True,
        path_locks=path_locks or PathLocks(),
        admission=admission or LocalTransferAdmission(capacity=2),
        idle_timeout_seconds=idle_timeout_seconds,
        queue_timeout_seconds=queue_timeout_seconds,
        terminal_ttl_seconds=terminal_ttl_seconds,
        lifecycle_credits=lifecycle_credits,
    )


async def _handle(manager: DirectoryJobManager, raw_action: object) -> Any:
    """Keep dynamic action/result narrowing local to this black-box test module."""

    return await manager.handle(raw_action)


async def _source_status(
    manager: DirectoryJobManager,
    operation_id: str,
    expected_digest: str,
    states: set[str],
) -> Any:
    for _ in range(200):
        status = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_status",
                "directory_operation_id": operation_id,
                "expected_digest": expected_digest,
            },
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
        status = await _handle(
            manager,
            {
                "operation": operation,
                "directory_operation_id": operation_id,
                "expected_digest": expected_digest,
            },
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
    started = await _handle(
        manager,
        {
            "operation": "transfer_source_probe_start",
            "directory_operation_id": operation_id,
            "path": path,
        },
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
        page = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_page",
                "directory_operation_id": operation_id,
                "expected_digest": status.expected_digest,
                "offset": offset,
            },
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
        held = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        assert held.state == "held"
        released = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        assert released.state == "released"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_cancelled_destination_release_finishes_reservation_cleanup_and_can_retry(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path)
    try:
        operation_id, digest, _ = await _prepare_committed_destination(manager, tmp_path)
        await _handle(
            manager,
            {
                "operation": "transfer_directory_finish",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        await _destination_status(manager, operation_id, digest, {"finalized_held"})
        record = manager._active[(UUID(operation_id), "destination")]
        assert record.reservation_held

        await manager._locks._condition.acquire()
        try:
            release = asyncio.create_task(
                _handle(
                    manager,
                    {
                        "operation": "transfer_directory_release",
                        "directory_operation_id": operation_id,
                        "expected_digest": digest,
                    },
                )
            )
            while record.reservation is not None:
                await asyncio.sleep(0)

            release.cancel()
            await asyncio.sleep(0)
            release.cancel()
            await asyncio.sleep(0)
            assert release.done() is False
        finally:
            manager._locks._condition.release()

        with pytest.raises(asyncio.CancelledError):
            await release
        assert manager._locks.entry_count == 0
        assert manager._locks.reservation_count == 0

        retried = await _handle(
            manager,
            {
                "operation": "transfer_directory_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        assert retried.state == "released"
        assert manager.active_count == 0
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_source_probe_rejects_empty_tree_and_links(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    (tmp_path / "linked-target").mkdir()
    (tmp_path / "linked-target" / "file").write_text("x", encoding="utf-8")
    _make_directory_link(tmp_path / "linked", tmp_path / "linked-target")
    manager = _manager(tmp_path)
    try:
        for path, code in (
            ("empty", "workspace_invalid_request"),
            ("linked", "workspace_symlink_escape"),
        ):
            operation_id = _operation_id()
            started = await _handle(
                manager,
                {
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": operation_id,
                    "path": path,
                },
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
        await _handle(
            manager,
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        entry = manifest.entries[0]
        transfer_uuid = _operation_id()
        accepted = await _handle(
            manager,
            {
                "operation": "transfer_directory_authorize_source_child",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": entry.relative_path,
                "fingerprint": entry.fingerprint,
            },
        )
        assert accepted.state == "accepted"
        grant = await manager.consume_source_authorization(
            UUID(transfer_uuid), source / entry.relative_path
        )
        assert grant.directory_operation_id == UUID(operation_id)
        await manager.complete_source_authorization(UUID(transfer_uuid), success=True)

        (source / "b").write_bytes(b"changed")
        await _handle(
            manager,
            {
                "operation": "transfer_source_cleanup",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
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


@pytest.mark.asyncio
async def test_source_probe_path_reservation_conflict_terminalizes_busy(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"data")
    locks = PathLocks()
    manager = _manager(tmp_path, path_locks=locks)
    operation_id = _operation_id()
    try:
        async with locks.reserve_subtree("other-operation", str(source)):
            started = await _handle(
                manager,
                {
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": operation_id,
                    "path": "source",
                },
            )
            status = await _source_status(
                manager,
                operation_id,
                started.expected_digest,
                {"failed"},
            )
            assert status.terminal_error.code == "workspace_transfer_busy"
            assert manager.active_count == 0
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_source_cleanup_path_reservation_conflict_returns_warning(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"data")
    locks = PathLocks()
    manager = _manager(tmp_path, path_locks=locks)
    operation_id = _operation_id()
    try:
        _, digest = await _probe_directory(manager, operation_id, "source")
        await _handle(
            manager,
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        async with locks.reserve_subtree("other-operation", str(source)):
            await _handle(
                manager,
                {
                    "operation": "transfer_source_cleanup",
                    "directory_operation_id": operation_id,
                    "expected_digest": digest,
                },
            )
            status = await _source_status(manager, operation_id, digest, {"succeeded"})
            assert status.terminal_result.cleanup_complete is False
            assert status.terminal_result.warnings == ["source_cleanup_incomplete"]
            assert manager.active_count == 0
            assert (source / "file").read_bytes() == b"data"
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


@pytest.mark.parametrize(
    "relative_path",
    [
        "..\\escape",
        "bad<name",
        "bad>name",
        "bad:name",
        'bad"name',
        "bad|name",
        "bad?name",
        "bad*name",
        "bad\x01name",
        "trailing.",
        "trailing ",
        "CON",
        "prn.txt",
        "AUX.tar.gz",
        "nul",
        "CONIN$",
        "conout$.txt",
        "COM1.log",
        "COM1 .log",
        "com¹.txt",
        "LPT9",
        "lpt³.log",
    ],
)
def test_windows_destination_components_reject_unsupported_names(
    relative_path: str,
) -> None:
    with pytest.raises(ToolFailure) as raised:
        directory_jobs_module._validate_windows_components(relative_path)

    assert raised.value.code == "workspace_invalid_request"


@pytest.mark.parametrize(
    "path",
    [
        r"safe\root",
        "safe/root",
        r"C:\safe\root",
        "C:/safe/root",
        r"\\server\share\safe\root",
        "//server/share/safe/root",
    ],
)
def test_windows_caller_path_accepts_drive_unc_and_both_separators(path: str) -> None:
    directory_jobs_module._validate_windows_caller_path(path)


@pytest.mark.parametrize(
    "path",
    [
        r"C:\safe\CON",
        "C:/safe/trailing.",
        r"\\server\share\safe\COM1.txt",
        "//server/share/safe/bad<name",
        r"C:safe\root",
        r"\safe\root",
        r"\\?\C:\safe\root",
        r"\\.\CON",
    ],
)
def test_windows_caller_path_rejects_invalid_components_and_path_forms(path: str) -> None:
    with pytest.raises(ToolFailure) as raised:
        directory_jobs_module._validate_windows_caller_path(path)

    assert raised.value.code == "workspace_invalid_request"


@pytest.mark.parametrize(
    "source_path",
    [r"source\CON", "source/trailing.", r"source\bad<name"],
)
@pytest.mark.asyncio
async def test_windows_source_root_rejects_invalid_component_before_resolve(
    source_path: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(directory_jobs_module, "_current_destination_platform", lambda: "windows")
    if os.name != "nt":
        source = tmp_path / source_path
        source.mkdir(parents=True)
        (source / "file").write_bytes(b"unchanged")
    before = sorted(
        (
            path.relative_to(tmp_path).as_posix(),
            path.is_dir(),
            path.read_bytes() if path.is_file() else b"",
        )
        for path in tmp_path.rglob("*")
    )
    manager = _manager(tmp_path)
    resolve_calls = 0
    original_resolve = manager._paths.resolve

    def observed_resolve(*args: Any, **kwargs: Any) -> Path:
        nonlocal resolve_calls
        resolve_calls += 1
        return original_resolve(*args, **kwargs)

    monkeypatch.setattr(cast(Any, manager._paths), "resolve", observed_resolve)
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": operation_id,
                "path": source_path,
            },
        )
        status = await _source_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed", "ready_retrieval"},
        )

        assert status.state == "failed"
        assert status.terminal_error.code == "workspace_invalid_request"
        assert resolve_calls == 0
        assert sorted(
            (
                path.relative_to(tmp_path).as_posix(),
                path.is_dir(),
                path.read_bytes() if path.is_file() else b"",
            )
            for path in tmp_path.rglob("*")
        ) == before
    finally:
        await manager.aclose()


@pytest.mark.parametrize(
    "destination_path",
    [r"destination\CON", "destination/trailing.", r"destination\bad<name"],
)
@pytest.mark.asyncio
async def test_windows_destination_root_rejects_invalid_component_before_mutation(
    destination_path: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(directory_jobs_module, "_current_destination_platform", lambda: "windows")
    manager = _manager(tmp_path)
    resolve_calls = 0
    original_resolve = manager._paths.resolve

    def observed_resolve(*args: Any, **kwargs: Any) -> Path:
        nonlocal resolve_calls
        resolve_calls += 1
        return original_resolve(*args, **kwargs)

    monkeypatch.setattr(cast(Any, manager._paths), "resolve", observed_resolve)
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": destination_path,
                "manifest": _small_manifest().model_dump(mode="json"),
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed", "ready"},
        )

        assert status.state == "failed"
        assert status.terminal_error.code == "workspace_invalid_request"
        assert resolve_calls == 0
        assert not list(tmp_path.iterdir())
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_windows_destination_preflight_rejects_separator_escape_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(directory_jobs_module, "_current_destination_platform", lambda: "windows")
    manifest = create_directory_manifest(
        root_identity="source-root",
        directories=(),
        entries=(
            DirectoryManifestEntry(
                relative_path="..\\outside",
                size=1,
                fingerprint="source",
            ),
        ),
    )
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed"},
        )

        assert status.terminal_error.code == "workspace_invalid_request"
        assert not (tmp_path / "destination").exists()
        assert not (tmp_path / "outside").exists()
    finally:
        await manager.aclose()


@pytest.mark.parametrize(
    ("platform", "relative_paths"),
    [
        (
            "macos",
            (
                "cafe\N{COMBINING ACUTE ACCENT}.txt",
                "caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt",
            ),
        ),
        ("windows", ("Name.txt", "name.TXT")),
    ],
)
@pytest.mark.asyncio
async def test_destination_platform_collision_is_invalid_request(
    platform: str,
    relative_paths: tuple[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(directory_jobs_module, "_current_destination_platform", lambda: platform)
    manifest = create_directory_manifest(
        root_identity="source-root",
        directories=(),
        entries=tuple(
            DirectoryManifestEntry(
                relative_path=relative_path,
                size=1,
                fingerprint=f"source-{index}",
            )
            for index, relative_path in enumerate(relative_paths)
        ),
    )
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed"},
        )

        assert status.terminal_error.code == "workspace_invalid_request"
        assert not list(tmp_path.iterdir())
    finally:
        await manager.aclose()


async def _prepare_committed_destination(
    manager: DirectoryJobManager,
    workspace: Path,
) -> tuple[str, str, Path]:
    manifest = _small_manifest()
    operation_id = _operation_id()
    started = await _handle(
        manager,
        {
            "operation": "transfer_directory_preflight",
            "directory_operation_id": operation_id,
            "dst_path": "destination",
            "manifest": manifest.model_dump(mode="json"),
        },
    )
    await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
    await _handle(
        manager,
        {
            "operation": "transfer_directory_prepare",
            "directory_operation_id": operation_id,
            "expected_digest": started.expected_digest,
        },
    )
    await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
    destination = workspace / "destination" / "nested" / "file"
    transfer_uuid = _operation_id()
    await _handle(
        manager,
        {
            "operation": "transfer_directory_authorize_child",
            "directory_operation_id": operation_id,
            "expected_digest": started.expected_digest,
            "transfer_uuid": transfer_uuid,
            "relative_path": "nested/file",
        },
    )
    grant = await manager.consume_destination_authorization(UUID(transfer_uuid))
    assert grant.destination_path == destination
    assert grant.expected_size == 4
    destination.write_bytes(b"data")
    await manager.record_destination_commit(
        UUID(operation_id),
        UUID(transfer_uuid),
        relative_path="nested/file",
        destination_fingerprint=_fingerprint(destination),
        verified_size=4,
        verified_sha256=_sha(b"data"),
    )
    return operation_id, started.expected_digest, destination


@pytest.mark.asyncio
async def test_destination_prepare_authorize_finish_and_release(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        status = await _destination_status(
            manager, operation_id, started.expected_digest, {"ready", "failed"}
        )
        assert status.state == "ready"
        assert not (tmp_path / "destination").exists()

        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        status = await _destination_status(
            manager, operation_id, started.expected_digest, {"reserved", "failed"}
        )
        assert status.state == "reserved"
        destination = tmp_path / "destination" / "nested" / "file"

        transfer_uuid = _operation_id()
        destination_record = manager._active[(UUID(operation_id), "destination")]
        assert destination_record.manifest is not None and destination_record.entries_by_path
        saved_manifest = destination_record.manifest
        destination_record.manifest = saved_manifest.model_copy(update={"entries": ()})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_authorize_child",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "nested/file",
            },
        )
        destination_record.manifest = saved_manifest
        grant = await manager.consume_destination_authorization(UUID(transfer_uuid))
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

        await _handle(
            manager,
            {
                "operation": "transfer_directory_finish",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
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

        destination_record = manager._active[(UUID(operation_id), "destination")]
        assert destination_record.reservation_held
        await manager.request_close(preserve_finalized=True, final=False)
        assert not destination_record.reservation_held
        assert await manager.wait_for_drain(timeout_seconds=0.1)
        assert destination.read_bytes() == b"data"

        released = await _handle(
            manager,
            {
                "operation": "transfer_directory_release",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        assert released.state == "released"
        assert destination.read_bytes() == b"data"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ["delete_root", "replace_root", "delete_parent"])
async def test_destination_child_parent_validation_rejects_identity_races(
    tmp_path: Path,
    race: str,
) -> None:
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    transfer_uuid = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_authorize_child",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "nested/file",
            },
        )
        await manager.consume_destination_authorization(UUID(transfer_uuid))

        root = tmp_path / "destination"
        parent = root / "nested"
        if race == "delete_parent":
            parent.rmdir()
        else:
            parent.rmdir()
            root.rmdir()
            if race == "replace_root":
                root.mkdir()

        with pytest.raises(ToolFailure) as raised:
            manager.validate_destination_child_parent(UUID(transfer_uuid))
        assert raised.value.code == "workspace_file_changed"
        await manager.complete_destination_authorization(UUID(transfer_uuid), success=False)
        await _handle(
            manager,
            {
                "operation": "transfer_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed", "outcome_unknown"},
        )
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_destination_cancel_conditionally_removes_owned_tree(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        status = await _destination_status(
            manager, operation_id, started.expected_digest, {"failed", "outcome_unknown"}
        )
        assert status.state == "failed"
        assert not (tmp_path / "destination").exists()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_destination_prepare_never_claims_or_deletes_an_externally_raced_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_claim = directory_jobs_module._claim_destination_root

    def race_parent(
        record: Any,
        destination: Path,
    ) -> None:
        original_claim(record, destination)
        (destination / "nested").mkdir()

    monkeypatch.setattr(directory_jobs_module, "_claim_destination_root", race_parent)
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed", "outcome_unknown"},
        )
        assert status.state == "outcome_unknown"
        assert (tmp_path / "destination" / "nested").is_dir()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_destination_prepare_retains_ambiguous_mkdir_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_identity = directory_jobs_module._directory_identity

    def fail_root_identity(path: Path) -> str:
        if path.name == "destination":
            raise OSError("injected identity failure")
        return original_identity(path)

    monkeypatch.setattr(directory_jobs_module, "_directory_identity", fail_root_identity)
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "created/destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed", "outcome_unknown"},
        )
        assert status.state == "outcome_unknown"
        assert status.cleanup_complete is False
        assert (tmp_path / "created" / "destination").is_dir()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(os.name == "nt", reason="symlink replacement proof is POSIX-specific")
async def test_destination_cleanup_treats_owned_directory_link_replacement_as_incomplete(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        replaced = tmp_path / "destination" / "nested"
        replaced.rmdir()
        replaced.symlink_to(outside, target_is_directory=True)

        await _handle(
            manager,
            {
                "operation": "transfer_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed", "outcome_unknown"},
        )
        assert status.state == "outcome_unknown"
        assert status.cleanup_complete is False
        assert replaced.is_symlink()
        assert outside.is_dir()
        assert manager._locks.reservation_count == 0
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_cross_site_prepare_omits_scan_only_empty_directories(tmp_path: Path) -> None:
    manifest = create_directory_manifest(
        root_identity="source-root",
        directories=(
            DirectoryManifestDirectory(relative_path="empty", identity="empty-id"),
            DirectoryManifestDirectory(relative_path="nested", identity="nested-id"),
        ),
        entries=(
            DirectoryManifestEntry(relative_path="nested/file", size=4, fingerprint="source"),
        ),
    )
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        assert (tmp_path / "destination" / "nested").is_dir()
        assert not (tmp_path / "destination" / "empty").exists()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_repeated_file_phases_do_not_exceed_manifest_file_count(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    manifest = _small_manifest()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        record = manager._active[(UUID(operation_id), "destination")]
        initial_seq = record.progress_seq
        record.bump(files=1, phase="copying")
        record.bump(files=1, phase="revalidating")
        record.bump(files=1, phase="cleanup")
        status = await _handle(
            manager,
            {
                "operation": "transfer_directory_status",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        assert status.files_processed == len(manifest.entries)
        assert status.progress_seq == initial_seq + 3
    finally:
        await manager.aclose()


async def _prepare_local_job(
    manager: DirectoryJobManager,
    operation_id: str,
    source_path: str,
    destination_path: str,
) -> tuple[DirectoryManifest, str, str]:
    manifest, source_digest = await _probe_directory(manager, operation_id, source_path)
    await _handle(
        manager,
        {
            "operation": "transfer_source_probe_release",
            "directory_operation_id": operation_id,
            "expected_digest": source_digest,
        },
    )
    preflight = await _handle(
        manager,
        {
            "operation": "transfer_directory_preflight",
            "directory_operation_id": operation_id,
            "dst_path": destination_path,
            "manifest": manifest.model_dump(mode="json"),
        },
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
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "copied",
                "mode": "copy",
                "manifest_sha256": manifest.manifest_sha256,
            },
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
async def test_local_copy_does_not_delete_a_competing_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"payload")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    original_link = cast(Any, directory_jobs_module).os.link

    def competing_link(
        source_path: Path,
        destination_path: Path,
        *,
        follow_symlinks: bool,
    ) -> None:
        destination_path.write_bytes(b"competitor")
        original_link(
            source_path,
            destination_path,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(cast(Any, directory_jobs_module).os, "link", competing_link)
    try:
        manifest, _, destination_digest = await _prepare_local_job(
            manager, operation_id, "source", "copied"
        )
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "copied",
                "mode": "copy",
                "manifest_sha256": manifest.manifest_sha256,
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            destination_digest,
            {"failed", "outcome_unknown"},
            local=True,
        )
        assert status.state == "outcome_unknown"
        assert (tmp_path / "copied" / "file").read_bytes() == b"competitor"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_local_copy_cleans_its_published_link_when_parent_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"payload")
    manager = _manager(tmp_path)
    operation_id = _operation_id()

    def fail_fsync(_path: Path) -> None:
        raise OSError("injected directory fsync failure")

    monkeypatch.setattr(directory_jobs_module, "_fsync_directory", fail_fsync)
    try:
        manifest, _, destination_digest = await _prepare_local_job(
            manager, operation_id, "source", "copied"
        )
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "copied",
                "mode": "copy",
                "manifest_sha256": manifest.manifest_sha256,
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            destination_digest,
            {"failed", "outcome_unknown"},
            local=True,
        )
        assert status.state == "failed"
        assert not (tmp_path / "copied").exists()
    finally:
        await manager.aclose()


def test_directory_fsync_is_a_noop_when_windows_cannot_open_directories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cast(Any, directory_jobs_module).os, "name", "nt")

    def unexpected_open(*_args: Any, **_kwargs: Any) -> int:
        raise AssertionError("Windows directory fsync must not call os.open")

    monkeypatch.setattr(cast(Any, directory_jobs_module).os, "open", unexpected_open)
    directory_jobs_module._fsync_directory(Path("unused"))


def test_directory_fsync_propagates_real_posix_io_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    if os.name == "nt":
        pytest.skip("POSIX fsync error proof")

    def fail_fsync(_descriptor: int) -> None:
        raise OSError(errno.EIO, "injected directory I/O failure")

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(OSError) as raised:
        directory_jobs_module._fsync_directory(tmp_path)
    assert raised.value.errno == errno.EIO


@pytest.mark.asyncio
async def test_local_move_cleans_owned_parents_when_publish_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"payload")
    manager = _manager(tmp_path)
    operation_id = _operation_id()

    def fail_publish(_source: Path, _destination: Path) -> None:
        raise ToolFailure("workspace_storage_unavailable", "injected publish failure")

    monkeypatch.setattr(directory_jobs_module, "_rename_directory_no_replace", fail_publish)
    try:
        manifest, _, destination_digest = await _prepare_local_job(
            manager, operation_id, "source", "created/parent/moved"
        )
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "created/parent/moved",
                "mode": "move",
                "manifest_sha256": manifest.manifest_sha256,
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            destination_digest,
            {"failed", "outcome_unknown"},
            local=True,
        )
        assert status.state == "failed"
        assert not (tmp_path / "created").exists()
        assert source.is_dir()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not (sys.platform.startswith("linux") or sys.platform in {"darwin", "win32"}),
    reason="native exclusive directory rename is unsupported on this platform",
)
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
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "moved",
                "mode": "move",
                "manifest_sha256": manifest.manifest_sha256,
            },
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
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_start",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
                "source_path": "source",
                "dst_path": "destination",
                "mode": "copy",
                "manifest_sha256": manifest.manifest_sha256,
            },
        )
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": destination_digest,
            },
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
            await _handle(
                manager,
                {
                    "operation": "transfer_source_probe_release",
                    "directory_operation_id": operation_id,
                    "expected_digest": "0" * 64,
                },
            )
        assert raised.value.code == "workspace_transfer_integrity_failed"

        first = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        second = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
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
        await _handle(
            manager,
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
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
        assert (await _handle(manager, action)).state == "accepted"
        assert (await _handle(manager, action)).state == "accepted"
        with pytest.raises(ToolFailure):
            await manager.consume_source_authorization(UUID(transfer_uuid), source / "wrong")
        with pytest.raises(ToolFailure):
            await manager.consume_source_authorization(UUID(transfer_uuid), source / "file")

        second_uuid = _operation_id()
        action["transfer_uuid"] = second_uuid
        await _handle(manager, action)
        await manager.consume_source_authorization(UUID(second_uuid), source / "file")
        await manager.complete_source_authorization(UUID(second_uuid), success=False)
        await _handle(
            manager,
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        with pytest.raises(ToolFailure):
            await manager.consume_source_authorization(UUID(second_uuid), source / "file")
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_source_child_uuid_is_tombstoned_and_success_counts_each_path_once(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    file = source / "file"
    file.write_bytes(b"x")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, digest = await _probe_directory(manager, operation_id, "source")
        await _handle(
            manager,
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        initial_status = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_status",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        source_record = manager._active[(UUID(operation_id), "source")]
        assert source_record.manifest is not None and source_record.entries_by_path
        source_record.manifest = source_record.manifest.model_copy(update={"entries": ()})
        first_uuid = _operation_id()
        action = {
            "operation": "transfer_directory_authorize_source_child",
            "directory_operation_id": operation_id,
            "expected_digest": digest,
            "transfer_uuid": first_uuid,
            "relative_path": "file",
            "fingerprint": manifest.entries[0].fingerprint,
        }
        await _handle(manager, action)
        await manager.consume_source_authorization(UUID(first_uuid), file)
        await manager.complete_source_authorization(UUID(first_uuid), success=True)

        with pytest.raises(ToolFailure) as reused:
            await _handle(manager, action)
        assert reused.value.code == "workspace_transfer_integrity_failed"
        assert manager.claims_source_transfer(UUID(first_uuid))

        second_uuid = _operation_id()
        action["transfer_uuid"] = second_uuid
        await _handle(manager, action)
        await manager.consume_source_authorization(UUID(second_uuid), file)
        await manager.complete_source_authorization(UUID(second_uuid), success=True)
        status = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_status",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        assert status.files_processed == initial_status.files_processed
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
            await _handle(
                manager,
                {
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": _operation_id(),
                    "path": name,
                },
            )
        with pytest.raises(ToolFailure) as raised:
            await _handle(
                manager,
                {
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": _operation_id(),
                    "path": "three",
                },
            )
        assert raised.value.code == "workspace_transfer_busy"
        assert manager.active_count == 2
    finally:
        blocker.release()
        await manager.aclose()


@pytest.mark.asyncio
async def test_lifecycle_capacity_is_shared_across_retired_and_current_managers(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"x")
    credits = DirectoryLifecycleCredits(capacity=1)
    first = _manager(tmp_path, lifecycle_credits=credits)
    second = _manager(tmp_path, lifecycle_credits=credits)
    try:
        await first.handle(
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": _operation_id(),
                "path": "source",
            }
        )
        with pytest.raises(ToolFailure) as raised:
            await second.handle(
                {
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": _operation_id(),
                    "path": "source",
                }
            )
        assert raised.value.code == "workspace_transfer_busy"
        assert credits.active_count == 1
    finally:
        await first.aclose()
        await second.aclose()


@pytest.mark.asyncio
async def test_config_retire_keeps_janitor_until_lifecycle_tombstones_expire(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(directory_jobs_module, "TOMBSTONE_TTL_SECONDS", 0.02)
    source = tmp_path / "file"
    source.write_bytes(b"x")
    manager = _manager(tmp_path, terminal_ttl_seconds=0.02)
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": operation_id,
                "path": "file",
            },
        )
        await _source_status(
            manager,
            operation_id,
            started.expected_digest,
            {"succeeded"},
        )
        await manager.request_close(preserve_finalized=True, final=False)
        assert await manager.wait_for_lifecycle_empty(timeout_seconds=1)
        assert manager.lifecycle_count == 0
    finally:
        await manager.aclose(final=True)


@pytest.mark.asyncio
async def test_retired_manager_reconciles_exact_starts_and_rejects_new_work(
    tmp_path: Path,
) -> None:
    source = tmp_path / "file"
    source.write_bytes(b"x")
    source_manager = _manager(tmp_path)
    source_operation_id = _operation_id()
    source_action = {
        "operation": "transfer_source_probe_start",
        "directory_operation_id": source_operation_id,
        "path": "file",
    }
    try:
        started = await _handle(source_manager, source_action)
        await _source_status(
            source_manager,
            source_operation_id,
            started.expected_digest,
            {"succeeded"},
        )
        await source_manager.request_close(final=False)
        retried = await _handle(source_manager, source_action)
        assert retried.state == "succeeded"
        with pytest.raises(ToolFailure) as new_source:
            await _handle(
                source_manager,
                {
                    **source_action,
                    "directory_operation_id": _operation_id(),
                },
            )
        assert new_source.value.code == "tool_device_unreachable"
    finally:
        await source_manager.aclose(final=True)

    destination_manager = _manager(tmp_path)
    destination_operation_id = _operation_id()
    manifest = _small_manifest()
    preflight_action = {
        "operation": "transfer_directory_preflight",
        "directory_operation_id": destination_operation_id,
        "dst_path": "destination",
        "manifest": manifest.model_dump(mode="json"),
    }
    try:
        preflight = await _handle(destination_manager, preflight_action)
        await _destination_status(
            destination_manager,
            destination_operation_id,
            preflight.expected_digest,
            {"ready"},
        )
        await destination_manager.request_close(final=False)
        retried = await _handle(destination_manager, preflight_action)
        local = await _handle(
            destination_manager,
            {
                "operation": "transfer_local_directory_status",
                "directory_operation_id": destination_operation_id,
                "expected_digest": preflight.expected_digest,
            },
        )
        assert retried.state == "ready"
        assert local.state == "ready_not_started"
        with pytest.raises(ToolFailure) as retired_local_start:
            await _handle(
                destination_manager,
                {
                    "operation": "transfer_local_directory_start",
                    "directory_operation_id": destination_operation_id,
                    "expected_digest": preflight.expected_digest,
                    "source_path": "source",
                    "dst_path": "destination",
                    "mode": "copy",
                    "manifest_sha256": manifest.manifest_sha256,
                },
            )
        assert retired_local_start.value.code == "tool_device_unreachable"
        with pytest.raises(ToolFailure) as new_destination:
            await _handle(
                destination_manager,
                {
                    **preflight_action,
                    "directory_operation_id": _operation_id(),
                },
            )
        assert new_destination.value.code == "tool_device_unreachable"
    finally:
        await destination_manager.aclose(final=True)


@pytest.mark.asyncio
async def test_retired_manager_allows_prepare_and_finish_result_retries(
    tmp_path: Path,
) -> None:
    prepare_manager = _manager(tmp_path)
    manifest = _small_manifest()
    prepare_operation_id = _operation_id()
    try:
        preflight = await _handle(
            prepare_manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": prepare_operation_id,
                "dst_path": "prepared",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(
            prepare_manager, prepare_operation_id, preflight.expected_digest, {"ready"}
        )
        prepare_action = {
            "operation": "transfer_directory_prepare",
            "directory_operation_id": prepare_operation_id,
            "expected_digest": preflight.expected_digest,
        }
        await _handle(prepare_manager, prepare_action)
        await _destination_status(
            prepare_manager, prepare_operation_id, preflight.expected_digest, {"reserved"}
        )
        prepare_manager._closed = True
        assert (await _handle(prepare_manager, prepare_action)).state == "accepted"
    finally:
        await prepare_manager.aclose(final=True)

    finish_manager = _manager(tmp_path)
    try:
        operation_id, digest, _destination = await _prepare_committed_destination(
            finish_manager, tmp_path
        )
        finish_action = {
            "operation": "transfer_directory_finish",
            "directory_operation_id": operation_id,
            "expected_digest": digest,
        }
        await _handle(finish_manager, finish_action)
        await _destination_status(finish_manager, operation_id, digest, {"finalized_held"})
        await finish_manager.request_close(preserve_finalized=True, final=False)
        assert (await _handle(finish_manager, finish_action)).state == "accepted"
    finally:
        await finish_manager.aclose(final=True)


@pytest.mark.asyncio
@pytest.mark.skipif(os.name == "nt", reason="mkfifo is a POSIX primitive")
async def test_source_probe_rejects_special_file(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    os.mkfifo(source / "pipe")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": operation_id,
                "path": "source",
            },
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
        started = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": operation_id,
                "path": "source",
            },
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
            await _handle(manager, page_action)
        page_action["offset"] = 0
        first = await _handle(manager, page_action)
        retry = await _handle(manager, page_action)
        assert first.model_dump() == retry.model_dump()
        page_action["offset"] = 256
        final = await _handle(manager, page_action)
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
        await _handle(
            manager,
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        transfer_uuid = _operation_id()
        await _handle(
            manager,
            {
                "operation": "transfer_directory_authorize_source_child",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "file",
                "fingerprint": manifest.entries[0].fingerprint,
            },
        )
        await manager.consume_source_authorization(UUID(transfer_uuid), source / "file")

        for operation in ("transfer_source_cleanup", "transfer_source_probe_release"):
            with pytest.raises(ToolFailure) as raised:
                await _handle(
                    manager,
                    {
                        "operation": operation,
                        "directory_operation_id": operation_id,
                        "expected_digest": digest,
                    },
                )
            assert raised.value.code == "workspace_transfer_busy"
        with pytest.raises(ToolFailure) as raised:
            await _handle(
                manager,
                {
                    "operation": "transfer_directory_authorize_source_child",
                    "directory_operation_id": operation_id,
                    "expected_digest": digest,
                    "transfer_uuid": _operation_id(),
                    "relative_path": "file",
                    "fingerprint": manifest.entries[0].fingerprint,
                },
            )
        assert raised.value.code == "workspace_transfer_busy"

        assert await manager.aclose(grace_seconds=0.01) is False
        await manager.complete_source_authorization(UUID(transfer_uuid), success=True)
        assert await manager.wait_for_drain(timeout_seconds=1)
        released = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_release",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
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
        await _handle(
            manager,
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        transfer_uuid = _operation_id()
        await _handle(
            manager,
            {
                "operation": "transfer_directory_authorize_source_child",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "file",
                "fingerprint": manifest.entries[0].fingerprint,
            },
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
    await _handle(
        manager,
        {
            "operation": "transfer_source_probe_start",
            "directory_operation_id": _operation_id(),
            "path": "source",
        },
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
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        destination = tmp_path / "destination" / "nested" / "file"
        transfer_uuid = _operation_id()
        await _handle(
            manager,
            {
                "operation": "transfer_directory_authorize_child",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "nested/file",
            },
        )
        grant = await manager.consume_destination_authorization(UUID(transfer_uuid))
        assert grant.destination_path == destination
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
        await _handle(
            manager,
            {
                "operation": "transfer_directory_finish",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
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
@pytest.mark.parametrize("cleanup_complete", [True, False])
async def test_finalize_timeout_cleanup_preserves_cause_only_when_complete(
    tmp_path: Path,
    cleanup_complete: bool,
) -> None:
    class TimeoutThenAdmission:
        def __init__(self) -> None:
            self.calls = 0
            self.delegate = LocalTransferAdmission(capacity=1)

        async def acquire(self, *, timeout_seconds: float) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError
            return await self.delegate.acquire(timeout_seconds=timeout_seconds)

    manager = _manager(tmp_path)
    try:
        operation_id, digest, _destination = await _prepare_committed_destination(manager, tmp_path)
        if not cleanup_complete:
            (tmp_path / "destination" / "external").write_bytes(b"race")
        manager._admission = TimeoutThenAdmission()  # type: ignore[assignment]
        await _handle(
            manager,
            {
                "operation": "transfer_directory_finish",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        status = await _destination_status(
            manager,
            operation_id,
            digest,
            {"failed", "outcome_unknown"},
        )
        if cleanup_complete:
            assert status.state == "failed"
            assert status.cleanup_complete is True
            assert status.terminal_error.code == "workspace_transfer_timeout"
            assert not (tmp_path / "destination").exists()
        else:
            assert status.state == "outcome_unknown"
            assert status.cleanup_complete is False
            assert status.terminal_error.code == "tool_execution_outcome_unknown"
            assert (tmp_path / "destination" / "external").is_file()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_idle_reserved_destination_cleanup_preserves_timeout_error(tmp_path: Path) -> None:
    manager = _manager(tmp_path, idle_timeout_seconds=0.05)
    operation_id = _operation_id()
    manifest = _small_manifest()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        status = await _destination_status(
            manager,
            operation_id,
            started.expected_digest,
            {"failed", "outcome_unknown"},
        )
        assert status.state == "failed"
        assert status.cleanup_complete is True
        assert status.terminal_error.code == "workspace_transfer_timeout"
        assert not (tmp_path / "destination").exists()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_ready_not_started_local_cancel_then_release_frees_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(directory_jobs_module, "TOMBSTONE_TTL_SECONDS", 0.02)
    credits = DirectoryLifecycleCredits(capacity=1)
    manager = _manager(
        tmp_path,
        lifecycle_credits=credits,
        terminal_ttl_seconds=0.02,
    )
    operation_id = _operation_id()
    manifest = _small_manifest()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        ready = await _handle(
            manager,
            {
                "operation": "transfer_local_directory_status",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        assert ready.state == "ready_not_started"
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        cancelled = await _handle(
            manager,
            {
                "operation": "transfer_local_directory_status",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        assert cancelled.state == "failed"
        assert cancelled.terminal_error.code == "tool_execution_cancelled"
        await _handle(
            manager,
            {
                "operation": "transfer_local_directory_release",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        assert manager.active_count == 0
        assert await manager.wait_for_lifecycle_empty(timeout_seconds=1)
        assert credits.active_count == 0
    finally:
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cause", "expected_code"),
    [
        ("cancel", "tool_execution_cancelled"),
        ("idle", "workspace_transfer_timeout"),
    ],
)
async def test_destination_terminal_action_waits_for_consumed_child(
    tmp_path: Path,
    cause: str,
    expected_code: str,
) -> None:
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        transfer_uuid = _operation_id()
        await _handle(
            manager,
            {
                "operation": "transfer_directory_authorize_child",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "nested/file",
            },
        )
        await manager.consume_destination_authorization(UUID(transfer_uuid))
        if cause == "cancel":
            await _handle(
                manager,
                {
                    "operation": "transfer_directory_cancel",
                    "directory_operation_id": operation_id,
                    "expected_digest": started.expected_digest,
                },
            )
        else:
            record = manager._active[(UUID(operation_id), "destination")]
            record.last_progress_at = time.monotonic() - manager._idle_timeout - 1
            await manager._expire_idle_jobs()

        in_flight = await _handle(
            manager,
            {
                "operation": "transfer_directory_status",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        assert in_flight.state == "copying"
        assert manager._consumed_destination
        assert (tmp_path / "destination").is_dir()

        await manager.complete_destination_authorization(UUID(transfer_uuid), success=False)
        terminal = await _destination_status(
            manager, operation_id, started.expected_digest, {"failed", "outcome_unknown"}
        )
        assert terminal.state == "failed"
        assert terminal.terminal_error.code == expected_code
        assert not manager._consumed_destination
        assert not (tmp_path / "destination").exists()
    finally:
        await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cause", "expected_code"),
    [
        ("cancel", "tool_execution_cancelled"),
        ("idle", "workspace_transfer_timeout"),
    ],
)
async def test_source_terminal_action_waits_for_consumed_child(
    tmp_path: Path,
    cause: str,
    expected_code: str,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    child = source / "file"
    child.write_bytes(b"data")
    manager = _manager(tmp_path)
    operation_id = _operation_id()
    try:
        manifest, digest = await _probe_directory(manager, operation_id, "source")
        await _handle(
            manager,
            {
                "operation": "transfer_source_probe_hold",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        transfer_uuid = _operation_id()
        await _handle(
            manager,
            {
                "operation": "transfer_directory_authorize_source_child",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
                "transfer_uuid": transfer_uuid,
                "relative_path": "file",
                "fingerprint": manifest.entries[0].fingerprint,
            },
        )
        await manager.consume_source_authorization(UUID(transfer_uuid), child)

        if cause == "cancel":
            await _handle(
                manager,
                {
                    "operation": "transfer_source_probe_cancel",
                    "directory_operation_id": operation_id,
                    "expected_digest": digest,
                },
            )
        else:
            record = manager._active[(UUID(operation_id), "source")]
            record.last_progress_at = time.monotonic() - manager._idle_timeout - 1
            await manager._expire_idle_jobs()

        in_flight = await _handle(
            manager,
            {
                "operation": "transfer_source_probe_status",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        assert in_flight.state == "held"
        assert manager._consumed_source

        await manager.complete_source_authorization(UUID(transfer_uuid), success=False)
        terminal = await _source_status(manager, operation_id, digest, {"failed"})
        assert terminal.terminal_error.code == expected_code
        assert not manager._consumed_source
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_late_destination_cancel_preserves_finalized_tree_and_result(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path)
    try:
        operation_id, digest, destination = await _prepare_committed_destination(manager, tmp_path)
        await _handle(
            manager,
            {
                "operation": "transfer_directory_finish",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        finalized = await _destination_status(manager, operation_id, digest, {"finalized_held"})

        cancelled = await _handle(
            manager,
            {
                "operation": "transfer_directory_cancel",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )
        after_cancel = await _handle(
            manager,
            {
                "operation": "transfer_directory_status",
                "directory_operation_id": operation_id,
                "expected_digest": digest,
            },
        )

        assert cancelled.state == "accepted"
        assert after_cancel.state == "finalized_held"
        assert after_cancel.terminal_result == finalized.terminal_result
        assert destination.read_bytes() == b"data"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_failed_destination_child_uuid_cannot_be_reauthorized(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    manifest = _small_manifest()
    operation_id = _operation_id()
    try:
        started = await _handle(
            manager,
            {
                "operation": "transfer_directory_preflight",
                "directory_operation_id": operation_id,
                "dst_path": "destination",
                "manifest": manifest.model_dump(mode="json"),
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"ready"})
        await _handle(
            manager,
            {
                "operation": "transfer_directory_prepare",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
        )
        await _destination_status(manager, operation_id, started.expected_digest, {"reserved"})
        destination = tmp_path / "destination" / "nested" / "file"
        transfer_uuid = _operation_id()
        action = {
            "operation": "transfer_directory_authorize_child",
            "directory_operation_id": operation_id,
            "expected_digest": started.expected_digest,
            "transfer_uuid": transfer_uuid,
            "relative_path": "nested/file",
        }
        await _handle(manager, action)
        grant = await manager.consume_destination_authorization(UUID(transfer_uuid))
        assert grant.destination_path == destination
        await manager.complete_destination_authorization(UUID(transfer_uuid), success=False)

        with pytest.raises(ToolFailure) as reused:
            await _handle(manager, action)
        assert reused.value.code == "workspace_transfer_integrity_failed"
        assert manager.claims_destination_transfer(UUID(transfer_uuid))
    finally:
        await manager.aclose()
