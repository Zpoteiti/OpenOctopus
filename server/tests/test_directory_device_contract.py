from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.devices.workspace import (
    DestinationDirectoryJobStatus,
    DirectoryCleanupResult,
    DirectoryCommandResult,
    DirectorySourceProbe,
    DirectoryStableError,
    FileSourceProbe,
    LocalDirectoryJobStatus,
    SourceDirectoryJobStatus,
    build_directory_action,
    parse_directory_result,
)
from openctopus_server.directory_contract import (
    DirectoryManifestDirectory,
    DirectoryManifestEntry,
    DirectoryManifestPage,
    create_directory_manifest,
)


def _operation_id() -> str:
    return str(new_uuid7())


def _digest() -> str:
    return "0" * 64


def _manifest() -> Any:
    return create_directory_manifest(
        root_identity="root-id",
        directories=(DirectoryManifestDirectory(relative_path="nested", identity="dir-id"),),
        entries=(
            DirectoryManifestEntry(
                relative_path="nested/a.txt",
                size=1,
                fingerprint="source-etag",
            ),
        ),
    )


def _directory_result() -> dict[str, object]:
    return {
        "kind": "directory",
        "files_transferred": 1,
        "bytes_transferred": 1,
        "sha256": _digest(),
        "warnings": [],
    }


@pytest.mark.parametrize("fingerprint", ["line\nbreak", "non-ascii-é", "delete-\x7f"])
def test_file_source_probe_rejects_non_visible_ascii_fingerprint(fingerprint: str) -> None:
    with pytest.raises(ValidationError, match="visible ASCII"):
        FileSourceProbe(size=1, fingerprint=fingerprint)


def test_build_directory_action_returns_strict_wire_payload() -> None:
    operation_id = _operation_id()
    action = build_directory_action(
        "transfer_directory_preflight",
        directory_operation_id=operation_id,
        dst_path="destination",
        manifest=_manifest(),
    )

    assert action["operation"] == "transfer_directory_preflight"
    assert action["directory_operation_id"] == operation_id
    assert action["manifest"]["entries"][0]["relative_path"] == "nested/a.txt"


@pytest.mark.parametrize(
    ("operation", "extra"),
    [
        ("transfer_source_probe_start", {"path": "source"}),
        ("transfer_source_probe_status", {"expected_digest": _digest()}),
        (
            "transfer_source_probe_page",
            {"expected_digest": _digest(), "offset": 0},
        ),
        ("transfer_source_probe_hold", {"expected_digest": _digest()}),
        ("transfer_source_probe_cancel", {"expected_digest": _digest()}),
        ("transfer_source_probe_release", {"expected_digest": _digest()}),
        (
            "transfer_directory_authorize_source_child",
            {
                "expected_digest": _digest(),
                "transfer_uuid": _operation_id(),
                "relative_path": "nested/a.txt",
                "fingerprint": "source-etag",
            },
        ),
        ("transfer_source_cleanup", {"expected_digest": _digest()}),
        (
            "transfer_directory_preflight",
            {"dst_path": "destination", "manifest": _manifest()},
        ),
        ("transfer_directory_status", {"expected_digest": _digest()}),
        ("transfer_directory_prepare", {"expected_digest": _digest()}),
        (
            "transfer_directory_authorize_child",
            {
                "expected_digest": _digest(),
                "transfer_uuid": _operation_id(),
                "relative_path": "nested/a.txt",
            },
        ),
        ("transfer_directory_finish", {"expected_digest": _digest()}),
        ("transfer_directory_cancel", {"expected_digest": _digest()}),
        ("transfer_directory_release", {"expected_digest": _digest()}),
        (
            "transfer_local_directory_start",
            {
                "expected_digest": _digest(),
                "source_path": "source",
                "dst_path": "destination",
                "mode": "copy",
                "manifest_sha256": _digest(),
            },
        ),
        ("transfer_local_directory_status", {"expected_digest": _digest()}),
        ("transfer_local_directory_cancel", {"expected_digest": _digest()}),
        ("transfer_local_directory_release", {"expected_digest": _digest()}),
    ],
)
def test_build_directory_action_supports_every_private_operation(
    operation: str,
    extra: dict[str, object],
) -> None:
    action = build_directory_action(
        operation,
        directory_operation_id=_operation_id(),
        **extra,
    )
    assert action["operation"] == operation


@pytest.mark.parametrize(
    ("operation", "payload"),
    [
        (
            "transfer_source_probe_start",
            {"directory_operation_id": str(UUID(int=1)), "path": "source"},
        ),
        (
            "transfer_source_probe_status",
            {
                "directory_operation_id": _operation_id(),
                "expected_digest": "A" * 64,
            },
        ),
        (
            "transfer_directory_authorize_child",
            {
                "directory_operation_id": _operation_id(),
                "expected_digest": _digest(),
                "transfer_uuid": _operation_id(),
                "relative_path": "../escape",
            },
        ),
        (
            "transfer_source_probe_page",
            {
                "directory_operation_id": _operation_id(),
                "expected_digest": _digest(),
                "offset": 10_001,
            },
        ),
        (
            "transfer_local_directory_start",
            {
                "directory_operation_id": _operation_id(),
                "expected_digest": _digest(),
                "source_path": "source\x00bad",
                "dst_path": "destination",
                "mode": "copy",
                "manifest_sha256": _digest(),
            },
        ),
    ],
)
def test_build_directory_action_rejects_invalid_uuid_digest_path_and_bounds(
    operation: str,
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        build_directory_action(operation, **payload)


def test_build_directory_action_forbids_extra_fields_and_coercion() -> None:
    with pytest.raises(ValidationError):
        build_directory_action(
            "transfer_source_probe_start",
            directory_operation_id=_operation_id(),
            path="source",
            unexpected=True,
        )
    with pytest.raises(ValidationError):
        build_directory_action(
            "transfer_source_probe_page",
            directory_operation_id=_operation_id(),
            expected_digest=_digest(),
            offset="0",
        )


def test_build_directory_action_validates_manifest_contract() -> None:
    invalid = _manifest().model_dump(mode="json")
    invalid["total_bytes"] = 2

    with pytest.raises(ValidationError):
        build_directory_action(
            "transfer_directory_preflight",
            directory_operation_id=_operation_id(),
            dst_path="destination",
            manifest=invalid,
        )


def test_parse_directory_command_and_manifest_page_results() -> None:
    command = parse_directory_result(
        "transfer_source_probe_start",
        json.dumps({"state": "running", "expected_digest": _digest()}),
    )
    assert isinstance(command, DirectoryCommandResult)

    page = parse_directory_result(
        "transfer_source_probe_page",
        json.dumps(
            {
                "offset": 0,
                "next_offset": None,
                "items": [
                    {
                        "kind": "file",
                        "relative_path": "a.txt",
                        "size": 1,
                        "fingerprint": "etag",
                    }
                ],
            }
        ),
    )
    assert isinstance(page, DirectoryManifestPage)
    assert page.items[0].relative_path == "a.txt"


@pytest.mark.parametrize(
    "operation",
    [
        "transfer_source_probe_start",
        "transfer_source_probe_hold",
        "transfer_source_probe_cancel",
        "transfer_source_probe_release",
        "transfer_directory_authorize_source_child",
        "transfer_source_cleanup",
        "transfer_directory_preflight",
        "transfer_directory_prepare",
        "transfer_directory_authorize_child",
        "transfer_directory_finish",
        "transfer_directory_cancel",
        "transfer_directory_release",
        "transfer_local_directory_start",
        "transfer_local_directory_cancel",
        "transfer_local_directory_release",
    ],
)
def test_parse_directory_result_routes_every_command_operation(operation: str) -> None:
    result = parse_directory_result(
        operation,
        json.dumps({"state": "accepted", "expected_digest": _digest()}),
    )
    assert isinstance(result, DirectoryCommandResult)


def test_parse_directory_source_status_is_discriminated_and_strict() -> None:
    status = parse_directory_result(
        "transfer_source_probe_status",
        json.dumps(
            {
                "state": "succeeded",
                "expected_digest": _digest(),
                "progress_seq": 1,
                "entries_processed": 0,
                "files_processed": 0,
                "bytes_processed": 0,
                "probe": {"kind": "file", "size": 1, "fingerprint": "etag"},
                "terminal_result": None,
                "terminal_error": None,
            }
        ),
    )
    assert isinstance(status, SourceDirectoryJobStatus)
    assert isinstance(status.probe, FileSourceProbe)

    with pytest.raises(ValidationError):
        parse_directory_result(
            "transfer_source_probe_status",
            json.dumps(
                {
                    "state": "ready_retrieval",
                    "expected_digest": _digest(),
                    "progress_seq": 1,
                    "entries_processed": 1,
                    "files_processed": 1,
                    "bytes_processed": 1,
                }
            ),
        )


def test_parse_directory_start_accepts_exact_duplicate_status_snapshot() -> None:
    source = parse_directory_result(
        "transfer_source_probe_start",
        json.dumps(
            {
                "state": "succeeded",
                "expected_digest": _digest(),
                "progress_seq": 1,
                "entries_processed": 0,
                "files_processed": 0,
                "bytes_processed": 0,
                "probe": {"kind": "file", "size": 1, "fingerprint": "etag"},
                "terminal_result": None,
                "terminal_error": None,
            }
        ),
    )
    assert isinstance(source, SourceDirectoryJobStatus)

    destination = parse_directory_result(
        "transfer_directory_preflight",
        json.dumps(
            {
                "state": "ready",
                "expected_digest": _digest(),
                "progress_seq": 1,
                "files_processed": 0,
                "bytes_processed": 0,
                "cleanup_complete": None,
                "terminal_result": None,
                "terminal_error": None,
            }
        ),
    )
    assert isinstance(destination, DestinationDirectoryJobStatus)


def test_parse_directory_destination_and_local_terminal_results() -> None:
    destination = parse_directory_result(
        "transfer_directory_status",
        json.dumps(
            {
                "state": "finalized_held",
                "expected_digest": _digest(),
                "progress_seq": 2,
                "files_processed": 1,
                "bytes_processed": 1,
                "cleanup_complete": None,
                "terminal_result": _directory_result(),
                "terminal_error": None,
            }
        ),
    )
    assert isinstance(destination, DestinationDirectoryJobStatus)

    local = parse_directory_result(
        "transfer_local_directory_status",
        json.dumps(
            {
                "state": "succeeded",
                "phase": "cleanup",
                "expected_digest": _digest(),
                "progress_seq": 4,
                "files_processed": 1,
                "bytes_processed": 1,
                "terminal_result": _directory_result(),
                "terminal_error": None,
            }
        ),
    )
    assert isinstance(local, LocalDirectoryJobStatus)


def test_terminal_state_payload_invariants_reject_impossible_snapshots() -> None:
    error = DirectoryStableError(code="workspace_file_changed", message="changed")
    probe = DirectorySourceProbe(
        root_identity="root-id",
        scanned_entries=1,
        file_count=1,
        total_bytes=1,
        manifest_sha256=_digest(),
        page_count=1,
    )
    cleanup = DirectoryCleanupResult(cleanup_complete=False, warnings=[])

    with pytest.raises(ValidationError):
        SourceDirectoryJobStatus(
            state="scanning",
            expected_digest=_digest(),
            progress_seq=0,
            entries_processed=0,
            files_processed=0,
            bytes_processed=0,
            probe=probe,
        )
    SourceDirectoryJobStatus(
        state="succeeded",
        expected_digest=_digest(),
        progress_seq=2,
        entries_processed=1,
        files_processed=1,
        bytes_processed=1,
        probe=probe,
        terminal_result=cleanup,
    )
    with pytest.raises(ValidationError):
        DestinationDirectoryJobStatus(
            state="failed",
            expected_digest=_digest(),
            progress_seq=1,
            files_processed=0,
            bytes_processed=0,
        )
    DestinationDirectoryJobStatus(
        state="failed",
        expected_digest=_digest(),
        progress_seq=1,
        files_processed=0,
        bytes_processed=0,
        terminal_error=error,
    )
    with pytest.raises(ValidationError):
        LocalDirectoryJobStatus(
            state="running",
            phase="copying",
            expected_digest=_digest(),
            progress_seq=1,
            files_processed=0,
            bytes_processed=0,
            terminal_error=error,
        )


def test_parse_directory_result_rejects_malformed_and_wrong_operation() -> None:
    with pytest.raises(ValidationError):
        parse_directory_result(
            "transfer_source_probe_status",
            json.dumps(
                {
                    "state": "scanning",
                    "expected_digest": _digest(),
                    "progress_seq": True,
                    "entries_processed": 0,
                    "files_processed": 0,
                    "bytes_processed": 0,
                    "unexpected": 1,
                }
            ),
        )
    with pytest.raises(ValueError, match="unsupported directory operation"):
        parse_directory_result("edit_file", "{}")
