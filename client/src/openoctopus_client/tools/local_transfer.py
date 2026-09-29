from __future__ import annotations

import asyncio
import contextlib
import ctypes
import errno
import hashlib
import os
import stat
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import cast

from openoctopus_client.tools.common import ToolFailure
from openoctopus_client.tools.fingerprints import opaque_stat_fingerprint
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.tools.paths import WorkspacePaths
from openoctopus_client.tools.workspace_rest import (
    WorkspaceRestAction,
    WorkspaceTransferLocalResult,
)
from openoctopus_client.transfer_admission import (
    LocalTransferAdmission,
    LocalTransferDrainRegistry,
)

from .blocking import BlockingWork, _run_irreversible_mutation, _run_mutation


class LocalFileTransfers:
    """Copy/move ownership, path locks, and late worker cleanup for local files."""

    def __init__(
        self,
        paths: WorkspacePaths,
        locks: PathLocks,
        work: BlockingWork,
        admission: LocalTransferAdmission,
        drains: LocalTransferDrainRegistry,
    ) -> None:
        self._paths = paths
        self._locks = locks
        self._work = work
        self._transfer_admission = admission
        self._transfer_drains = drains

    async def execute(self, action: WorkspaceRestAction) -> WorkspaceTransferLocalResult:
        assert action.path is not None and action.dst_path is not None
        source = await self._work.run(self._paths.resolve, action.path, directory=False)
        destination = await self._work.run(self._paths.resolve, action.dst_path, directory=None)
        if source == destination:
            raise ToolFailure(
                "workspace_invalid_request", "Transfer source and destination must differ"
            )
        lease = await self._transfer_admission.acquire()
        abandoned_drains: set[asyncio.Task[None]] = set()
        lock_stack = contextlib.AsyncExitStack()
        source_fd: int | None = None
        try:
            await lock_stack.enter_async_context(self._locks.hold(str(source), str(destination)))
            opened_source = await self._work.run_transfer(
                abandoned_drains,
                _open_transfer_source,
                source,
                action.mode == "move",
                on_abandoned=_close_transfer_source_result,
            )
            active_source_fd, initial = opened_source
            source_fd = active_source_fd
            try:
                if action.if_match is not None and action.if_match != opaque_stat_fingerprint(
                    initial[:4]
                ):
                    raise ToolFailure(
                        "workspace_file_changed",
                        "Source changed before transfer",
                    )
                await self._work.run_transfer(
                    abandoned_drains, _check_transfer_destination, destination
                )
                await self._work.run_transfer(
                    abandoned_drains, self._paths.prepare_parent, destination
                )
                if not await self._work.run_transfer(abandoned_drains, destination.parent.is_dir):
                    raise ToolFailure(
                        "tool_not_a_directory",
                        "Destination parent is not a directory",
                    )
                if action.mode == "move":
                    result = await self._move_local(
                        source,
                        destination,
                        active_source_fd,
                        initial,
                        abandoned_drains,
                    )
                else:
                    result = await self._copy_local(
                        source,
                        destination,
                        active_source_fd,
                        initial,
                        abandoned_drains,
                    )
                return result
            finally:
                if not any(not task.done() for task in abandoned_drains):
                    with contextlib.suppress(OSError):
                        await self._work.mutate(os.close, active_source_fd)
                    source_fd = None
        finally:
            pending = tuple(task for task in abandoned_drains if not task.done())
            if pending:
                cleanup = asyncio.create_task(
                    _drain_local_transfer_resources(pending, source_fd, lock_stack)
                )
                self._transfer_drains.adopt(lease, (cleanup,), owner=self)
            else:
                try:
                    if source_fd is not None:
                        with contextlib.suppress(OSError):
                            await self._work.mutate(os.close, source_fd)
                    await lock_stack.aclose()
                finally:
                    lease.release()

    async def _copy_local(
        self,
        source: Path,
        destination: Path,
        source_fd: int,
        initial: tuple[int, int, int, int, int],
        abandoned_drains: set[asyncio.Task[None]],
    ) -> WorkspaceTransferLocalResult:
        temporary_fd, temporary = await self._work.run_transfer(
            abandoned_drains,
            _create_transfer_temp,
            destination.parent,
            destination.name,
            on_abandoned=_discard_transfer_temp_result,
        )
        committed = False
        try:
            bytes_transferred, digest = await _stream_fd(
                source_fd,
                temporary_fd,
            )
            await self._work.mutate(os.fsync, temporary_fd)
            await self._work.mutate(os.close, temporary_fd)
            temporary_fd = -1
            if not await self._work.run_transfer(
                abandoned_drains, _source_unchanged, source, source_fd, initial
            ):
                raise ToolFailure("workspace_file_changed", "Source changed during transfer")
            await _commit_transfer_no_replace(temporary, destination)
            committed = True
            return WorkspaceTransferLocalResult(
                kind="file",
                files_transferred=1,
                bytes_transferred=bytes_transferred,
                sha256=digest,
            )
        finally:
            if temporary_fd >= 0:
                with contextlib.suppress(OSError):
                    await self._work.mutate(os.close, temporary_fd)
            if not committed:
                with contextlib.suppress(OSError):
                    await self._work.mutate(temporary.unlink, missing_ok=True)

    async def _move_local(
        self,
        source: Path,
        destination: Path,
        source_fd: int,
        initial: tuple[int, int, int, int, int],
        abandoned_drains: set[asyncio.Task[None]],
    ) -> WorkspaceTransferLocalResult:
        bytes_transferred, digest = await _hash_fd(source_fd)
        if not await self._work.run_transfer(
            abandoned_drains, _source_unchanged, source, source_fd, initial
        ):
            raise ToolFailure("workspace_file_changed", "Source changed during transfer")
        bytes_transferred, digest = await _run_irreversible_mutation(
            self._work.tasks,
            _rename_verify_and_hash_fd,
            source,
            destination,
            source_fd,
            initial,
            bytes_transferred,
            digest,
        )
        return WorkspaceTransferLocalResult(
            kind="file",
            files_transferred=1,
            bytes_transferred=bytes_transferred,
            sha256=digest,
        )


async def _drain_local_transfer_resources(
    drains: tuple[asyncio.Task[None], ...],
    source_fd: int | None,
    lock_stack: contextlib.AsyncExitStack,
) -> None:
    await asyncio.gather(
        *(asyncio.shield(task) for task in drains),
        return_exceptions=True,
    )
    if source_fd is not None:
        with contextlib.suppress(OSError):
            await asyncio.to_thread(os.close, source_fd)
    await lock_stack.aclose()


def _open_transfer_source(
    path: Path, delete_access: bool = False
) -> tuple[int, tuple[int, int, int, int, int]]:
    try:
        initial = os.lstat(path)
    except FileNotFoundError as exc:
        raise ToolFailure("workspace_not_found", "Source file was not found") from exc
    except OSError as exc:
        raise ToolFailure("workspace_permission_denied", "Source file is unavailable") from exc
    if stat.S_ISLNK(initial.st_mode):
        raise ToolFailure("workspace_symlink_escape", "Source path is a symbolic link")
    if not stat.S_ISREG(initial.st_mode):
        raise ToolFailure("workspace_blocked_path", "Source is not a regular file")
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_NONBLOCK", 0))
    )
    descriptor: int | None = None
    try:
        descriptor = (
            # Hold an identity-stable handle that shares DELETE while writers
            # are still allowed.  Acquire DELETE access only at commit time.
            _open_windows_transfer_source(path, delete_access=False)
            if os.name == "nt" and delete_access
            else os.open(path, flags)
        )
        opened = os.fstat(descriptor)
    except FileNotFoundError as exc:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise ToolFailure("workspace_not_found", "Source file was not found") from exc
    except OSError as exc:
        if descriptor is not None:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        raise ToolFailure("workspace_permission_denied", "Source file is unavailable") from exc
    assert descriptor is not None
    if not stat.S_ISREG(opened.st_mode):
        with contextlib.suppress(OSError):
            os.close(descriptor)
        raise ToolFailure("workspace_blocked_path", "Source is not a regular file")
    identity = _transfer_identity(opened)
    if identity != _transfer_identity(initial):
        with contextlib.suppress(OSError):
            os.close(descriptor)
        raise ToolFailure("workspace_file_changed", "Source changed during transfer")
    return descriptor, identity


def _open_windows_transfer_source(path: Path, *, delete_access: bool) -> int:
    import msvcrt
    from ctypes import wintypes

    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    create_file.restype = wintypes.HANDLE
    desired_access = 0x80000000  # GENERIC_READ
    if delete_access:
        desired_access |= 0x00010000  # DELETE
    handle = create_file(
        str(path),
        desired_access,
        0x00000001 | 0x00000002 | 0x00000004,  # FILE_SHARE_READ | WRITE | DELETE
        None,
        3,  # OPEN_EXISTING
        0x00000080 | 0x00200000,  # FILE_ATTRIBUTE_NORMAL | OPEN_REPARSE_POINT
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if not handle or handle == invalid_handle:
        error = getattr(ctypes, "get_last_error")()
        if error in {2, 3}:
            raise ToolFailure("workspace_not_found", "Source file was not found")
        raise ToolFailure("workspace_permission_denied", "Source file is unavailable")
    try:
        open_osfhandle = cast(Callable[[int, int], int], getattr(msvcrt, "open_osfhandle"))
        return int(open_osfhandle(int(handle), os.O_RDONLY | int(getattr(os, "O_BINARY", 0))))
    except (OSError, OverflowError):
        kernel32.CloseHandle(handle)
        raise


def _create_transfer_temp(parent: Path, name: str) -> tuple[int, Path]:
    try:
        descriptor, raw_path = tempfile.mkstemp(prefix=f".{name}.openoctopus-", dir=parent)
    except OSError as exc:
        raise ToolFailure(
            "workspace_storage_unavailable", "Temporary destination unavailable"
        ) from exc
    return descriptor, Path(raw_path)


def _close_transfer_source_result(
    result: tuple[int, tuple[int, int, int, int, int]],
) -> None:
    with contextlib.suppress(OSError):
        os.close(result[0])


def _discard_transfer_temp_result(result: tuple[int, Path]) -> None:
    with contextlib.suppress(OSError):
        os.close(result[0])
    with contextlib.suppress(OSError):
        result[1].unlink()


async def _stream_fd(source_fd: int, destination_fd: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    bytes_transferred = 0
    while True:
        chunk = await _run_mutation(os.read, source_fd, 64 * 1024)
        if not chunk:
            break
        digest.update(chunk)
        bytes_transferred += len(chunk)
        view = memoryview(chunk)
        while view:
            written = await _run_mutation(os.write, destination_fd, view)
            if written <= 0:
                raise ToolFailure(
                    "workspace_storage_unavailable", "Destination could not be written"
                )
            view = view[written:]
    return bytes_transferred, digest.hexdigest()


async def _hash_fd(source_fd: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    bytes_transferred = 0
    while True:
        chunk = await _run_mutation(os.read, source_fd, 64 * 1024)
        if not chunk:
            break
        digest.update(chunk)
        bytes_transferred += len(chunk)
    return bytes_transferred, digest.hexdigest()


def _check_transfer_destination(path: Path) -> None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ToolFailure("workspace_permission_denied", "Destination is unavailable") from exc
    if stat.S_ISLNK(info.st_mode):
        raise ToolFailure("workspace_symlink_escape", "Destination path is a symbolic link")
    if stat.S_ISDIR(info.st_mode):
        raise ToolFailure("tool_is_directory", "Destination is a directory")
    if not stat.S_ISREG(info.st_mode):
        raise ToolFailure("workspace_blocked_path", "Destination is not a regular file")
    raise ToolFailure("workspace_file_changed", "Destination already exists")


async def _commit_transfer_no_replace(temporary: Path, destination: Path) -> None:
    try:
        await _run_mutation(_link_transfer_no_replace, temporary, destination)
    finally:
        with contextlib.suppress(OSError):
            await _run_mutation(temporary.unlink)


def _link_transfer_no_replace(source: Path, destination: Path) -> None:
    try:
        os.link(source, destination, follow_symlinks=False)
    except FileExistsError as exc:
        raise ToolFailure("workspace_file_changed", "Destination already exists") from exc
    except OSError as exc:
        raise ToolFailure(
            "workspace_storage_unavailable", "Atomic no-overwrite commit is unavailable"
        ) from exc
    _fsync_directory(destination.parent)


def _rename_transfer_no_replace(
    source: Path,
    destination: Path,
    source_fd: int,
) -> None:
    """Move one file with the platform's exclusive, same-volume rename primitive."""

    if sys.platform.startswith("linux"):
        try:
            rename = ctypes.CDLL(None, use_errno=True).renameat2
        except AttributeError as exc:
            raise ToolFailure(
                "workspace_storage_unavailable",
                "Exclusive same-volume move is unavailable",
            ) from exc
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        ctypes.set_errno(0)
        result = rename(
            -100,
            os.fsencode(source),
            -100,
            os.fsencode(destination),
            1,
        )
        if result != 0:
            _raise_exclusive_move_error(ctypes.get_errno())
    elif sys.platform == "darwin":
        try:
            rename = ctypes.CDLL(None, use_errno=True).renameatx_np
        except AttributeError as exc:
            raise ToolFailure(
                "workspace_storage_unavailable",
                "Exclusive same-volume move is unavailable",
            ) from exc
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        ctypes.set_errno(0)
        result = rename(
            -2,
            os.fsencode(source),
            -2,
            os.fsencode(destination),
            0x00000004,
        )
        if result != 0:
            _raise_exclusive_move_error(ctypes.get_errno())
    elif os.name == "nt":
        rename_fd = _open_windows_transfer_source(source, delete_access=True)
        try:
            opened_identity = _transfer_identity(os.fstat(rename_fd))
            source_identity = _transfer_identity(os.fstat(source_fd))
            if opened_identity[:2] != source_identity[:2]:
                raise ToolFailure("workspace_file_changed", "Source changed during transfer")
            _rename_windows_handle_no_replace(rename_fd, destination)
        finally:
            with contextlib.suppress(OSError):
                os.close(rename_fd)
    else:
        raise ToolFailure(
            "workspace_storage_unavailable",
            "Exclusive same-volume move is unavailable",
        )
    with contextlib.suppress(OSError):
        _fsync_directory(destination.parent)
    if source.parent != destination.parent:
        with contextlib.suppress(OSError):
            _fsync_directory(source.parent)


def _raise_exclusive_move_error(error: int) -> None:
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise ToolFailure("workspace_file_changed", "Destination already exists")
    if error == errno.EXDEV:
        raise ToolFailure(
            "workspace_storage_unavailable",
            "Same-volume exclusive move is required",
        )
    if error in {
        errno.EINVAL,
        getattr(errno, "ENOSYS", -1),
        getattr(errno, "ENOTSUP", -1),
        getattr(errno, "EOPNOTSUPP", -1),
    }:
        raise ToolFailure(
            "workspace_storage_unavailable",
            "Exclusive same-volume move is unavailable",
        )
    raise ToolFailure(
        "workspace_storage_unavailable",
        "Workspace move could not be completed",
    )


def _rename_windows_handle_no_replace(source_fd: int, destination: Path) -> None:
    import msvcrt
    from ctypes import wintypes

    class FileRenameInfo(ctypes.Structure):
        _fields_ = [
            ("flags", wintypes.DWORD),
            ("root_directory", wintypes.HANDLE),
            ("file_name_length", wintypes.DWORD),
            ("file_name", wintypes.WCHAR * 1),
        ]

    encoded = str(destination).encode("utf-16-le")
    file_name_offset = FileRenameInfo.file_name.offset
    buffer = ctypes.create_string_buffer(
        file_name_offset + len(encoded) + ctypes.sizeof(wintypes.WCHAR)
    )
    info = ctypes.cast(buffer, ctypes.POINTER(FileRenameInfo)).contents
    info.flags = 0
    info.root_directory = None
    info.file_name_length = len(encoded)
    ctypes.memmove(ctypes.addressof(buffer) + file_name_offset, encoded, len(encoded))
    kernel32 = getattr(ctypes, "WinDLL")("kernel32", use_last_error=True)
    set_file_information = kernel32.SetFileInformationByHandle
    set_file_information.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    set_file_information.restype = wintypes.BOOL
    get_osfhandle = cast(Callable[[int], int], getattr(msvcrt, "get_osfhandle"))
    handle = wintypes.HANDLE(get_osfhandle(source_fd))
    if not set_file_information(handle, 22, buffer, len(buffer)):
        error = getattr(ctypes, "get_last_error")()
        if error in {80, 183}:
            raise ToolFailure("workspace_file_changed", "Destination already exists")
        if error == 17:
            raise ToolFailure(
                "workspace_storage_unavailable",
                "Same-volume exclusive move is required",
            )
        if error in {1, 50, 87}:
            raise ToolFailure(
                "workspace_storage_unavailable",
                "Exclusive same-volume move is unavailable",
            )
        raise ToolFailure(
            "workspace_storage_unavailable",
            "Workspace move could not be completed",
        )


def _rename_verify_and_hash_fd(
    source: Path,
    destination: Path,
    source_fd: int,
    initial: tuple[int, int, int, int, int],
    bytes_transferred: int,
    digest: str,
) -> tuple[int, str]:
    """Rename exclusively and repair a digest only if the commit-race changed content."""

    _rename_transfer_no_replace(source, destination, source_fd)
    if os.name != "nt" and _transfer_identity(os.fstat(source_fd)) == initial:
        return bytes_transferred, digest
    os.lseek(source_fd, 0, os.SEEK_SET)
    updated_digest = hashlib.sha256()
    updated_bytes = 0
    while chunk := os.read(source_fd, 64 * 1024):
        updated_digest.update(chunk)
        updated_bytes += len(chunk)
    return updated_bytes, updated_digest.hexdigest()


def _source_unchanged(path: Path, descriptor: int, initial: tuple[int, int, int, int, int]) -> bool:
    try:
        return (
            _transfer_identity(os.fstat(descriptor)) == initial
            and _transfer_identity(os.lstat(path)) == initial
        )
    except OSError:
        return False


def _transfer_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    # On Windows, path-based stat reports creation time for st_ctime while
    # descriptor stat may report last-write time for the same file.
    change_time = 0 if os.name == "nt" else getattr(info, "st_ctime_ns", 0)
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        change_time,
    )


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
