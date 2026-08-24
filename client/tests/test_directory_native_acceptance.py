from __future__ import annotations

import asyncio
import ctypes
import os
import struct
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, cast

import pytest

import openoctopus_client.tools.directory_jobs as directory_jobs
from openoctopus_client.protocol import new_uuid7
from openoctopus_client.tools.common import ToolFailure
from openoctopus_client.tools.directory_contract import (
    DirectoryManifest,
    DirectoryManifestEntry,
    create_directory_manifest,
)
from openoctopus_client.tools.directory_jobs import DirectoryJobManager
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.transfer_admission import LocalTransferAdmission

MACOS_ONLY = pytest.mark.skipif(sys.platform != "darwin", reason="native macOS contract")
WINDOWS_ONLY = pytest.mark.skipif(sys.platform != "win32", reason="native Windows contract")
NATIVE_RENAME_ONLY = pytest.mark.skipif(
    sys.platform not in {"darwin", "win32"},
    reason="native macOS/Windows exclusive directory rename contract",
)

_WINDOWS_NATIVE_UNAVAILABLE_ERRORS = frozenset({1, 5, 50, 1314})


def _progress(**_values: object) -> None:
    pass


def _manifest(*relative_paths: str) -> DirectoryManifest:
    entries = tuple(
        DirectoryManifestEntry(relative_path=path, size=1, fingerprint=f"source-{index}")
        for index, path in enumerate(
            sorted(relative_paths, key=lambda value: value.encode("utf-8"))
        )
    )
    return create_directory_manifest(
        root_identity="source-root",
        directories=(),
        entries=entries,
    )


async def _destination_preflight_error(
    workspace: Path,
    manifest: DirectoryManifest,
) -> str:
    manager = DirectoryJobManager(
        workspace,
        restrict_to_workspace=True,
        path_locks=PathLocks(),
        admission=LocalTransferAdmission(capacity=2),
        idle_timeout_seconds=5,
        queue_timeout_seconds=1,
    )
    operation_id = str(new_uuid7())
    try:
        started = cast(
            Any,
            await manager.handle(
                {
                    "operation": "transfer_directory_preflight",
                    "directory_operation_id": operation_id,
                    "dst_path": "destination",
                    "manifest": manifest.model_dump(mode="json"),
                }
            )
        )
        expected_digest = started.expected_digest
        for _ in range(200):
            status = cast(
                Any,
                await manager.handle(
                    {
                        "operation": "transfer_directory_status",
                        "directory_operation_id": operation_id,
                        "expected_digest": expected_digest,
                    }
                )
            )
            expected_digest = status.expected_digest
            if status.state == "failed":
                return str(status.terminal_error.code)
            await asyncio.sleep(0.01)
        raise AssertionError("destination preflight did not fail")
    finally:
        await manager.aclose()


def _windows_last_error() -> int:
    return int(getattr(ctypes, "get_last_error")())


def _handle_windows_fixture_error(error: int, purpose: str) -> None:
    if error in _WINDOWS_NATIVE_UNAVAILABLE_ERRORS:
        pytest.skip(f"Windows runner cannot {purpose} ({error})")
    pytest.fail(f"Windows native fixture failed to {purpose} ({error})", pytrace=False)


def _open_windows_path(
    path: Path,
    *,
    desired_access: int,
    share_mode: int,
    flags: int,
    purpose: str,
) -> tuple[Any, int]:
    from ctypes import wintypes

    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.DeviceIoControl.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPVOID,
    ]
    kernel32.DeviceIoControl.restype = wintypes.BOOL
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    handle = create_file(
        str(path),
        desired_access,
        share_mode,
        None,
        3,
        flags,
        None,
    )
    if handle == ctypes.c_void_p(-1).value:
        _handle_windows_fixture_error(_windows_last_error(), purpose)
    return kernel32, int(handle)


def _assert_link_rejected(path: Path) -> None:
    with pytest.raises(ToolFailure) as raised:
        directory_jobs._scan_source_path(path, threading.Event(), _progress)
    assert raised.value.code == "workspace_symlink_escape"


def _make_directory_junction(link: Path, target: Path) -> None:
    command = Path(os.environ.get("SystemRoot", r"C:\Windows"), "System32", "cmd.exe")
    completed = subprocess.run(
        [str(command), "/D", "/C", "mklink", "/J", str(link), str(target)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
        timeout=10,
    )
    if completed.returncode != 0:
        pytest.fail(
            "Windows native fixture failed to create a directory junction "
            f"(exit {completed.returncode}): "
            f"{completed.stderr.decode(errors='replace').strip()}",
            pytrace=False,
        )


def _set_generic_reparse_point(path: Path) -> tuple[Any, int, int, bytes]:
    from ctypes import wintypes

    kernel32, handle = _open_windows_path(
        path,
        desired_access=0x40000000,
        share_mode=0x7,
        flags=0x02200000,
        purpose="open a reparse handle",
    )

    tag = 0x00000042
    reparse_guid = bytes.fromhex("27f4fbc72f694d0eaa0314511d898981")
    payload = b"openoctopus-native-test"
    raw = struct.pack("<IHH", tag, len(payload), 0) + reparse_guid + payload
    buffer = ctypes.create_string_buffer(raw)
    returned = wintypes.DWORD()
    success = kernel32.DeviceIoControl(
        handle,
        0x000900A4,
        buffer,
        len(raw),
        None,
        0,
        ctypes.byref(returned),
        None,
    )
    if not success:
        error = _windows_last_error()
        kernel32.CloseHandle(handle)
        _handle_windows_fixture_error(error, "create a generic reparse point")
    return kernel32, int(handle), tag, reparse_guid


def _delete_generic_reparse_point(
    kernel32: Any,
    handle: int,
    tag: int,
    reparse_guid: bytes,
    path: Path,
) -> None:
    from ctypes import wintypes

    raw = struct.pack("<IHH", tag, 0, 0) + reparse_guid
    buffer = ctypes.create_string_buffer(raw)
    returned = wintypes.DWORD()
    if kernel32.DeviceIoControl(
        handle,
        0x000900AC,
        buffer,
        len(raw),
        None,
        0,
        ctypes.byref(returned),
        None,
    ):
        return
    error = _windows_last_error()
    command = Path(os.environ.get("SystemRoot", r"C:\Windows"), "System32", "fsutil.exe")
    completed = subprocess.run(
        [str(command), "reparsepoint", "delete", str(path)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
        timeout=10,
    )
    if completed.returncode != 0:
        raise OSError(error, "failed to remove native test reparse point")


def _open_windows_delete_lock(path: Path) -> tuple[Any, int]:
    return _open_windows_path(
        path,
        desired_access=0x80000000,
        share_mode=0x3,
        flags=0x80,
        purpose="lock a test file",
    )


@MACOS_ONLY
@pytest.mark.asyncio
async def test_macos_native_nfd_nfc_collision_is_invalid_request(tmp_path: Path) -> None:
    code = await _destination_preflight_error(
        tmp_path,
        _manifest(
            "cafe\N{COMBINING ACUTE ACCENT}.txt",
            "caf\N{LATIN SMALL LETTER E WITH ACUTE}.txt",
        ),
    )
    assert code == "workspace_invalid_request"


@WINDOWS_ONLY
def test_windows_native_file_symlink_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target.txt"
    target.write_bytes(b"data")
    link = tmp_path / "file-link"
    try:
        link.symlink_to(target)
    except OSError as exc:
        error = getattr(exc, "winerror", None)
        if isinstance(error, int):
            _handle_windows_fixture_error(error, "create a file symlink")
        raise
    _assert_link_rejected(link)


@WINDOWS_ONLY
def test_windows_native_directory_junction_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "file").write_bytes(b"data")
    junction = tmp_path / "junction"
    _make_directory_junction(junction, target)
    _assert_link_rejected(junction)


@WINDOWS_ONLY
def test_windows_native_generic_reparse_point_is_rejected(tmp_path: Path) -> None:
    reparse = tmp_path / "generic-reparse"
    reparse.mkdir()
    kernel32, handle, tag, reparse_guid = _set_generic_reparse_point(reparse)
    try:
        _assert_link_rejected(reparse)
    finally:
        try:
            _delete_generic_reparse_point(kernel32, handle, tag, reparse_guid, reparse)
        finally:
            kernel32.CloseHandle(handle)


@WINDOWS_ONLY
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manifest",
    [
        pytest.param(_manifest("CON.txt"), id="reserved-name"),
        pytest.param(_manifest("Name.txt", "name.TXT"), id="case-collision"),
    ],
)
async def test_windows_native_destination_collision_is_invalid_request(
    tmp_path: Path,
    manifest: DirectoryManifest,
) -> None:
    assert await _destination_preflight_error(tmp_path, manifest) == "workspace_invalid_request"


@NATIVE_RENAME_ONLY
def test_native_directory_rename_is_exclusive(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "source-file").write_bytes(b"source")
    destination = tmp_path / "destination"
    destination.mkdir()
    (destination / "destination-file").write_bytes(b"destination")

    with pytest.raises(ToolFailure) as raised:
        directory_jobs._rename_directory_no_replace(source, destination)
    assert raised.value.code == "workspace_file_changed"
    assert (source / "source-file").read_bytes() == b"source"
    assert (destination / "destination-file").read_bytes() == b"destination"

    (destination / "destination-file").unlink()
    destination.rmdir()
    directory_jobs._rename_directory_no_replace(source, destination)
    assert not source.exists()
    assert (destination / "source-file").read_bytes() == b"source"


@WINDOWS_ONLY
def test_windows_native_locked_source_cleanup_is_incomplete(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    locked_file = source / "locked.txt"
    locked_file.write_bytes(b"locked")
    scanned = directory_jobs._scan_directory(
        source,
        threading.Event(),
        _progress,
        hash_contents=False,
    )
    kernel32, handle = _open_windows_delete_lock(locked_file)
    try:
        result = directory_jobs._conditional_source_cleanup(
            source,
            scanned.manifest,
            threading.Event(),
            _progress,
        )
        assert result.cleanup_complete is False
        assert result.warnings == ["source_cleanup_incomplete"]
        assert locked_file.read_bytes() == b"locked"
    finally:
        kernel32.CloseHandle(handle)
