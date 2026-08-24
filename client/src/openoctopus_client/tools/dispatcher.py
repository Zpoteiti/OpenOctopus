from __future__ import annotations

import asyncio
import base64
import contextlib
import ctypes
import errno
import fnmatch
import hashlib
import heapq
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

import httpx
import pathspec
import regex
from pydantic import BaseModel

from openoctopus_client.document_convert import (
    MAX_INPUT_BYTES,
    ConversionError,
    convert_html_bytes_async,
    convert_path_async,
)
from openoctopus_client.tools.common import ToolFailure, ToolOutput, fail
from openoctopus_client.tools.fingerprints import opaque_stat_fingerprint
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.tools.paths import WorkspacePaths
from openoctopus_client.tools.workspace_rest import (
    INTERNAL_WORKSPACE_ACTION,
    MAX_WORKSPACE_RESPONSE_BYTES,
    WorkspaceDeleteResult,
    WorkspaceDirectoryEntry,
    WorkspaceDirectoryPage,
    WorkspaceFileMutation,
    WorkspaceGrepContextLine,
    WorkspaceGrepItem,
    WorkspaceGrepPage,
    WorkspacePatchEditResult,
    WorkspacePatchResult,
    WorkspaceRestAction,
    WorkspaceTransferLocalResult,
)
from openoctopus_client.tools.workspace_rest import MAX_SCAN_OBJECTS as REST_MAX_SCAN_OBJECTS
from openoctopus_client.tools.workspace_rest import MAX_TEXT_EDIT_BYTES as REST_MAX_TEXT_EDIT_BYTES
from openoctopus_client.transfer_admission import (
    LOCAL_TRANSFER_CAPACITY,
    LocalTransferAdmission,
    LocalTransferDrainRegistry,
)

MAX_READ_CHARS = 128_000
MAX_TEXT_EDIT_BYTES = 8 * 1024 * 1024
MAX_GREP_BYTES = 2 * 1024 * 1024
MAX_SCAN_OBJECTS = 10_000
MAX_RESPONSE_BYTES = 5_000_000
MAX_REDIRECTS = 10
_LOCAL_TRANSFER_CANCEL_GRACE_SECONDS = 0.1
type FileFingerprint = tuple[int, int, int, int, int, str]
_NOISE = frozenset(
    {
        ".git",
        "node_modules",
        "__pycache__",
        ".venv",
        "venv",
        "dist",
        "build",
        ".tox",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".coverage",
        "htmlcov",
    }
)
_TYPES: dict[str, frozenset[str]] = {
    "c": frozenset({".c", ".h"}),
    "cpp": frozenset({".cc", ".cpp", ".cxx", ".hh", ".hpp", ".hxx"}),
    "js": frozenset({".js", ".jsx", ".mjs", ".cjs"}),
    "json": frozenset({".json", ".jsonl"}),
    "md": frozenset({".md", ".markdown"}),
    "py": frozenset({".py", ".pyi"}),
    "rs": frozenset({".rs"}),
    "ts": frozenset({".ts", ".tsx", ".mts", ".cts"}),
    "yaml": frozenset({".yaml", ".yml"}),
}
_IMAGE_TYPES = {
    b"\x89PNG\r\n\x1a\n": "image/png",
    b"\xff\xd8\xff": "image/jpeg",
    b"GIF87a": "image/gif",
    b"GIF89a": "image/gif",
    b"RIFF": "image/webp",
}
_TOOL_ARGUMENTS: dict[str, frozenset[str]] = {
    "read_file": frozenset({"path", "offset", "limit", "pages"}),
    "write_file": frozenset({"path", "content"}),
    "edit_file": frozenset(
        {
            "path",
            "old_text",
            "new_text",
            "replace_all",
            "occurrence",
            "line_hint",
            "expected_replacements",
        }
    ),
    "apply_patch": frozenset({"edits", "dry_run"}),
    "delete_file": frozenset({"path"}),
    "delete_folder": frozenset({"path"}),
    "list_dir": frozenset({"path", "recursive", "max_entries"}),
    "find_files": frozenset(
        {"path", "query", "glob", "type", "include_dirs", "sort", "head_limit", "offset"}
    ),
    "grep": frozenset(
        {
            "pattern",
            "path",
            "glob",
            "type",
            "case_insensitive",
            "fixed_strings",
            "output_mode",
            "context_before",
            "context_after",
            "max_matches",
            "max_results",
            "head_limit",
            "offset",
        }
    ),
    "notebook_edit": frozenset({"path", "cell_index", "new_source", "cell_type", "edit_mode"}),
    "web_fetch": frozenset({"url", "extractMode", "maxChars"}),
}
_PATCH_ARGUMENTS = frozenset({"path", "action", "old_text", "new_text"})


class ClientToolDispatcher:
    """Independent, bounded client-side implementation of the shared tool surface."""

    def __init__(
        self,
        workspace: Path,
        *,
        restrict_to_workspace: bool,
        ssrf_denylist: list[str],
        path_locks: PathLocks | None = None,
        transfer_admission: LocalTransferAdmission | None = None,
        transfer_drains: LocalTransferDrainRegistry | None = None,
    ) -> None:
        self._paths = WorkspacePaths(
            workspace,
            restrict_to_workspace=restrict_to_workspace,
        )
        self._locks = path_locks or PathLocks()
        self._denylist = tuple(ssrf_denylist)
        self._blocking_tasks: set[asyncio.Task[Any]] = set()
        self._transfer_admission = transfer_admission or LocalTransferAdmission(
            capacity=LOCAL_TRANSFER_CAPACITY
        )
        self._transfer_drains = transfer_drains or LocalTransferDrainRegistry()

    def has_pending_blocking(self) -> bool:
        """Whether a local worker thread is still running for this dispatcher."""

        return any(not task.done() for task in self._blocking_tasks)

    async def wait_for_pending_blocking(self) -> None:
        """Wait until a timed-out worker thread finishes before the next FIFO call."""

        pending = tuple(task for task in self._blocking_tasks if not task.done())
        if pending:
            await asyncio.gather(
                *(asyncio.shield(task) for task in pending),
                return_exceptions=True,
            )

    async def _run_blocking[T](
        self, function: Callable[..., T], *args: Any, **kwargs: Any
    ) -> T:
        return await _run_blocking(self._blocking_tasks, function, *args, **kwargs)

    async def _run_mutation(self, function: Any, *args: Any, **kwargs: Any) -> Any:
        return await _run_mutation(function, *args, tracker=self._blocking_tasks, **kwargs)

    async def _run_transfer_blocking[T](
        self,
        abandoned_drains: set[asyncio.Task[None]],
        function: Callable[..., T],
        *args: Any,
        on_abandoned: Callable[[T], Any] | None = None,
        **kwargs: Any,
    ) -> T:
        return await _run_blocking_with_drain(
            self._blocking_tasks,
            abandoned_drains,
            function,
            *args,
            on_abandoned=on_abandoned,
            **kwargs,
        )

    async def _resolve_path(self, path: str, *, directory: bool | None) -> Path:
        return await self._run_blocking(self._paths.resolve, path, directory=directory)

    async def execute(self, name: str, args: dict[str, Any]) -> ToolOutput:
        try:
            self._validate_args(name, args)
            return await asyncio.wait_for(self._execute(name, args), timeout=_timeout_for(name))
        except TimeoutError:
            code = "network_timeout" if name == "web_fetch" else "tool_exec_timeout"
            message = "web_fetch timed out" if name == "web_fetch" else f"{name} timed out"
            return fail(code, message)
        except ToolFailure as exc:
            return fail(exc.code, exc.message)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EPERM}:
                return fail("workspace_permission_denied", "Workspace path is unavailable")
            return fail(
                "workspace_storage_unavailable",
                "Workspace filesystem operation failed",
            )
        except (ValueError, TypeError, json.JSONDecodeError):
            return fail("tool_invalid_args", "Tool arguments are invalid")

    async def _execute(self, name: str, args: dict[str, Any]) -> ToolOutput:
        if name == INTERNAL_WORKSPACE_ACTION:
            return await self._workspace_rest(args)
        if name == "web_fetch":
            return await self._web_fetch(args)
        if name == "read_file":
            return await self._read_file(args)
        if name == "write_file":
            return await self._write_file(args)
        if name == "edit_file":
            return await self._edit_file(args)
        if name == "apply_patch":
            return await self._apply_patch(args)
        if name == "delete_file":
            return await self._delete_file(args)
        if name == "delete_folder":
            return await self._delete_folder(args)
        if name == "list_dir":
            return await self._list_dir(args)
        if name == "find_files":
            return await self._find_files(args)
        if name == "grep":
            return await self._grep(args)
        if name == "notebook_edit":
            return await self._notebook_edit(args)
        return fail("tool_not_available", f"This client does not implement {name}")

    async def _workspace_rest(self, args: dict[str, Any]) -> ToolOutput:
        action = WorkspaceRestAction.model_validate(args, strict=True)
        if action.operation == "edit_file":
            return await self._workspace_rest_edit(action)
        if action.operation == "apply_patch":
            return await self._workspace_rest_patch(action)
        if action.operation == "delete_file":
            return await self._workspace_rest_delete(action, directory=False)
        if action.operation == "delete_folder":
            return await self._workspace_rest_delete(action, directory=True)
        if action.operation == "list_dir":
            return await self._workspace_rest_list(action)
        if action.operation == "find_files":
            return await self._workspace_rest_find(action)
        if action.operation == "transfer_local":
            return await self._workspace_rest_transfer_local(action)
        return await self._workspace_rest_grep(action)

    async def _workspace_rest_edit(self, action: WorkspaceRestAction) -> ToolOutput:
        assert action.path is not None
        assert action.old_text is not None and action.new_text is not None
        target = await self._resolve_path(action.path, directory=False)
        async with self._locks.hold(str(target)):
            initial = await self._run_blocking(
                _capture_regular, target, REST_MAX_TEXT_EDIT_BYTES
            )
            initial_etag = None if initial is None else _opaque_fingerprint(initial[1])
            if action.if_match is not None and action.if_match != initial_etag:
                raise ToolFailure("workspace_file_changed", "File changed during the edit")
            if initial is None:
                current = None
            else:
                try:
                    current = initial[0].decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ToolFailure(
                        "workspace_invalid_request", "Workspace file is not UTF-8 text"
                    ) from exc
            updated, replacements, created = await self._run_blocking(
                _apply_text_edit,
                current,
                old_text=action.old_text,
                new_text=action.new_text,
                replace_all=action.replace_all,
                occurrence=action.occurrence,
                line_hint=action.line_hint,
                expected_replacements=action.expected_replacements,
            )
            if await self._run_blocking(_stat_fingerprint, target) != initial_etag:
                raise ToolFailure("workspace_file_changed", "File changed during the edit")
            await self._run_mutation(self._atomic_write, target, updated.encode("utf-8"))
            etag = await self._run_blocking(_stat_fingerprint, target)
        assert etag is not None
        return _workspace_json(
            WorkspaceFileMutation(
                path=action.path,
                size=len(updated.encode("utf-8")),
                etag=etag,
                created=created,
                replacements=replacements,
            )
        )

    async def _workspace_rest_patch(self, action: WorkspaceRestAction) -> ToolOutput:
        assert action.edits is not None
        targets: list[tuple[str, Path, Literal["replace", "add"], str | None, str]] = []
        for item in action.edits:
            target = await self._resolve_path(item.path, directory=False)
            old = item.old_text
            new = item.new_text
            assert new is not None
            targets.append((item.path, target, item.action, old, new))
        if len({str(item[1]) for item in targets}) != len(targets):
            raise ToolFailure("tool_invalid_args", "Patch paths must be unique")
        async with self._locks.hold(*(str(item[1]) for item in targets)):
            prepared: list[tuple[str, Path, str, int, bool, str | None]] = []
            total_bytes = 0
            for display, target, operation, old, new in targets:
                initial = await self._run_blocking(
                    _capture_regular, target, REST_MAX_TEXT_EDIT_BYTES
                )
                if initial is None:
                    current = None
                else:
                    try:
                        current = initial[0].decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise ToolFailure(
                            "workspace_invalid_request", "Workspace file is not UTF-8 text"
                        ) from exc
                if operation == "add":
                    updated, replacements, created = (current or "") + new, 0, current is None
                    _size(updated)
                else:
                    assert old is not None
                    updated, replacements, created = await self._run_blocking(
                        _apply_text_edit,
                        current,
                        old_text=old,
                        new_text=new,
                        replace_all=False,
                        occurrence=None,
                        line_hint=None,
                        expected_replacements=None,
                    )
                total_bytes += len(updated.encode("utf-8"))
                if total_bytes > REST_MAX_TEXT_EDIT_BYTES:
                    raise ToolFailure(
                        "workspace_file_too_large_to_edit",
                        "Patch content exceeds the 8 MiB edit limit",
                    )
                prepared.append(
                    (
                        display,
                        target,
                        updated,
                        replacements,
                        created,
                        None if initial is None else _opaque_fingerprint(initial[1]),
                    )
                )
            if not action.dry_run:
                for _, target, _, _, _, expected in prepared:
                    if await self._run_blocking(_stat_fingerprint, target) != expected:
                        raise ToolFailure("workspace_file_changed", "File changed during the patch")
                for _, target, updated, _, _, _ in prepared:
                    await self._run_mutation(self._atomic_write, target, updated.encode("utf-8"))
            results = [
                WorkspacePatchEditResult(
                    path=display,
                    action=targets[index][2],
                    size=len(updated.encode("utf-8")),
                    etag="dry-run"
                    if action.dry_run
                    else await self._run_blocking(_require_etag, target),
                    created=created,
                    replacements=replacements,
                )
                for index, (display, target, updated, replacements, created, _) in enumerate(
                    prepared
                )
            ]
        return _workspace_json(
            WorkspacePatchResult(
                items=results,
                dry_run=action.dry_run,
                committed=0 if action.dry_run else len(results),
            )
        )

    async def _workspace_rest_delete(
        self, action: WorkspaceRestAction, *, directory: bool
    ) -> ToolOutput:
        assert action.path is not None
        target = await self._resolve_path(action.path, directory=directory)
        if directory:
            _reject_protected_directory_delete(target, self._paths.root)
        async with self._locks.hold(str(target)):
            if directory:
                if not await self._run_blocking(target.exists):
                    raise ToolFailure("workspace_not_found", "Path does not exist")
            else:
                current = await self._run_blocking(_stat_fingerprint, target)
                if current is None:
                    raise ToolFailure("workspace_not_found", "Path does not exist")
                if action.if_match is not None and action.if_match != current:
                    raise ToolFailure("workspace_file_changed", "File changed during the delete")
            mutation = shutil.rmtree if directory else target.unlink
            await self._run_mutation(mutation, target)
        return _workspace_json(WorkspaceDeleteResult())

    async def _workspace_rest_transfer_local(
        self, action: WorkspaceRestAction
    ) -> ToolOutput:
        assert action.path is not None and action.dst_path is not None
        source = await self._resolve_path(action.path, directory=False)
        destination = await self._resolve_path(action.dst_path, directory=None)
        if source == destination:
            raise ToolFailure(
                "workspace_invalid_request", "Transfer source and destination must differ"
            )
        lease = await self._transfer_admission.acquire()
        abandoned_drains: set[asyncio.Task[None]] = set()
        lock_stack = contextlib.AsyncExitStack()
        source_fd: int | None = None
        try:
            await lock_stack.enter_async_context(
                self._locks.hold(str(source), str(destination))
            )
            opened_source = await self._run_transfer_blocking(
                abandoned_drains,
                _open_transfer_source,
                source,
                action.mode == "move",
                on_abandoned=_close_transfer_source_result,
            )
            active_source_fd, initial = opened_source
            source_fd = active_source_fd
            try:
                await self._run_transfer_blocking(
                    abandoned_drains, _check_transfer_destination, destination
                )
                await self._run_transfer_blocking(
                    abandoned_drains, self._paths.prepare_parent, destination
                )
                if not await self._run_transfer_blocking(
                    abandoned_drains, destination.parent.is_dir
                ):
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
                return _workspace_json(result)
            finally:
                if not any(not task.done() for task in abandoned_drains):
                    with contextlib.suppress(OSError):
                        await self._run_mutation(os.close, active_source_fd)
                    source_fd = None
        finally:
            pending = tuple(task for task in abandoned_drains if not task.done())
            if pending:
                cleanup = asyncio.create_task(
                    _drain_local_transfer_resources(
                        pending, source_fd, lock_stack
                    )
                )
                self._transfer_drains.adopt(lease, (cleanup,), owner=self)
            else:
                try:
                    if source_fd is not None:
                        with contextlib.suppress(OSError):
                            await self._run_mutation(os.close, source_fd)
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
        temporary_fd, temporary = await self._run_transfer_blocking(
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
            await self._run_mutation(os.fsync, temporary_fd)
            await self._run_mutation(os.close, temporary_fd)
            temporary_fd = -1
            if not await self._run_transfer_blocking(
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
                    await self._run_mutation(os.close, temporary_fd)
            if not committed:
                with contextlib.suppress(OSError):
                    await self._run_mutation(temporary.unlink, missing_ok=True)

    async def _move_local(
        self,
        source: Path,
        destination: Path,
        source_fd: int,
        initial: tuple[int, int, int, int, int],
        abandoned_drains: set[asyncio.Task[None]],
    ) -> WorkspaceTransferLocalResult:
        bytes_transferred, digest = await _hash_fd(source_fd)
        if not await self._run_transfer_blocking(
            abandoned_drains, _source_unchanged, source, source_fd, initial
        ):
            raise ToolFailure("workspace_file_changed", "Source changed during transfer")
        bytes_transferred, digest = await _run_irreversible_mutation(
            self._blocking_tasks,
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

    async def _workspace_rest_list(self, action: WorkspaceRestAction) -> ToolOutput:
        assert action.path is not None
        root = await self._resolve_path(action.path, directory=True)
        entries, truncated = await self._run_blocking(
            self._workspace_list_entries, root, action.recursive
        )
        values = [
            _directory_entry(action.path, relative, is_directory, size)
            for relative, size, is_directory in entries
        ]
        return _workspace_json(
            _directory_page(
                values, limit=action.limit, offset=action.offset, truncated=truncated
            )
        )

    async def _workspace_rest_find(self, action: WorkspaceRestAction) -> ToolOutput:
        assert action.path is not None
        root = await self._resolve_path(action.path, directory=True)
        if action.sort not in {"path", "modified"} or action.type not in {None, *_TYPES}:
            raise ToolFailure("tool_invalid_args", "Find filter is invalid")
        _validate_glob(action.glob)
        query = action.query.casefold().split()
        entries = await self._run_blocking(self._walk, root)
        scan_truncated = len(entries) >= REST_MAX_SCAN_OBJECTS
        valid_entries: list[tuple[str, float, bool]] = []
        for item in entries:
            if item[2]:
                valid_entries.append(item)
                continue
            try:
                await self._run_blocking(_safe_size, root / item[0])
            except ToolFailure as exc:
                if exc.code == "workspace_blocked_path":
                    continue
                raise
            valid_entries.append(item)
        filtered = [
            item
            for item in valid_entries
            if (action.include_dirs or not item[2])
            and all(term in item[0].casefold() for term in query)
            and (action.glob is None or fnmatch.fnmatchcase(item[0], action.glob))
            and (
                action.type is None
                or (
                    not item[2]
                    and PurePosixPath(item[0]).suffix.lower() in _TYPES[action.type]
                )
            )
        ]
        if action.sort == "path":
            filtered.sort(key=lambda item: item[0])
        else:
            filtered.sort(key=lambda item: item[1], reverse=True)
        values = [
            _directory_entry(
                action.path,
                relative,
                is_directory,
                0
                if is_directory
                else await self._run_blocking(_safe_size, root / relative),
            )
            for relative, _, is_directory in filtered
        ]
        return _workspace_json(
            _directory_page(
                values,
                limit=action.limit,
                offset=action.offset,
                truncated=scan_truncated,
            )
        )

    async def _workspace_rest_grep(self, action: WorkspaceRestAction) -> ToolOutput:
        assert action.path is not None and action.pattern is not None
        root = await self._resolve_path(action.path, directory=True)
        if action.type not in {None, *_TYPES}:
            raise ToolFailure("tool_invalid_args", "Grep filter is invalid")
        _validate_glob(action.glob)
        source = re.escape(action.pattern) if action.fixed_strings else action.pattern
        try:
            compiled = regex.compile(source, regex.IGNORECASE if action.case_insensitive else 0)
        except regex.error as exc:
            raise ToolFailure("tool_invalid_regex", "Regex pattern is invalid") from exc
        entries = await self._run_blocking(self._walk, root)
        scan_truncated = len(entries) >= REST_MAX_SCAN_OBJECTS
        values, truncated = await self._run_blocking(
            self._workspace_grep_entries,
            root,
            entries,
            compiled,
            action,
        )
        truncated = truncated or scan_truncated
        return _workspace_json(
            _grep_page(values, limit=action.limit, offset=action.offset, truncated=truncated)
        )

    def _workspace_list_entries(
        self, root: Path, recursive: bool
    ) -> tuple[list[tuple[str, int, bool]], bool]:
        if recursive:
            values: list[tuple[str, int, bool]] = []
            for relative, _, is_directory in self._walk(root):
                if is_directory:
                    values.append((relative, 0, True))
                    continue
                try:
                    values.append((relative, _safe_size(root / relative), False))
                except ToolFailure as exc:
                    if exc.code == "workspace_blocked_path":
                        continue
                    raise
            return values, len(values) >= REST_MAX_SCAN_OBJECTS
        try:
            children = heapq.nsmallest(
                REST_MAX_SCAN_OBJECTS + 1,
                (item for item in root.iterdir() if item.name not in _NOISE),
                key=lambda item: item.name,
            )
        except OSError as exc:
            raise ToolFailure("workspace_permission_denied", "Directory could not be read") from exc
        child_values: list[tuple[str, int, bool]] = []
        for item in children[:REST_MAX_SCAN_OBJECTS]:
            try:
                mode = item.lstat().st_mode
            except OSError as exc:
                raise ToolFailure(
                    "workspace_permission_denied", "Path could not be inspected"
                ) from exc
            if stat.S_ISLNK(mode):
                continue
            is_directory = stat.S_ISDIR(mode)
            if is_directory:
                child_values.append((item.name, 0, True))
                continue
            try:
                child_values.append((item.name, _safe_size(item), False))
            except ToolFailure as exc:
                if exc.code == "workspace_blocked_path":
                    continue
                raise
        return child_values, len(children) > REST_MAX_SCAN_OBJECTS

    def _workspace_grep_entries(
        self,
        root: Path,
        entries: list[tuple[str, float, bool]],
        compiled: regex.Pattern[str],
        action: WorkspaceRestAction,
    ) -> tuple[list[WorkspaceGrepItem], bool]:
        assert action.path is not None
        ignored = _gitignore(root)
        produced: list[WorkspaceGrepItem] = []
        retained_bytes = 0
        truncated = False
        page_complete = False

        def append(value: WorkspaceGrepItem) -> bool:
            nonlocal retained_bytes, truncated, page_complete
            if len(produced) >= action.offset + action.limit + 1:
                page_complete = True
                return False
            size = len(value.model_dump_json().encode("utf-8"))
            if retained_bytes + size > MAX_WORKSPACE_RESPONSE_BYTES:
                truncated = True
                return False
            produced.append(value)
            retained_bytes += size
            return True

        for relative, _, is_directory in entries:
            if truncated or page_complete:
                break
            if is_directory or ignored.match_file(relative):
                continue
            if action.glob and not fnmatch.fnmatchcase(relative, action.glob):
                continue
            if action.type and PurePosixPath(relative).suffix.lower() not in _TYPES[action.type]:
                continue
            try:
                data = _read_regular(root / relative, MAX_GREP_BYTES)
                text = data.decode("utf-8")
            except (ToolFailure, UnicodeDecodeError):
                continue
            if b"\x00" in data:
                continue
            lines = text.splitlines()
            matches: list[int] = []
            try:
                matches = [
                    index
                    for index, line in enumerate(lines)
                    if len(line) <= 16_000 and compiled.search(line, timeout=0.05)
                ]
            except (TimeoutError, regex.error) as exc:
                raise ToolFailure("tool_invalid_regex", "Regex pattern is invalid") from exc
            if action.output_mode == "files_with_matches":
                if matches:
                    append(WorkspaceGrepItem(path=_public_path(action.path, relative)))
            elif action.output_mode == "count":
                if matches:
                    append(
                        WorkspaceGrepItem(
                            path=_public_path(action.path, relative), count=len(matches)
                        )
                    )
            else:
                for index in matches:
                    if not append(
                        WorkspaceGrepItem(
                            path=_public_path(action.path, relative),
                            line_number=index + 1,
                            line=lines[index],
                            before=[
                                WorkspaceGrepContextLine(
                                    line_number=item + 1, line=lines[item][:16_000]
                                )
                                for item in range(max(0, index - action.context_before), index)
                            ],
                            after=[
                                WorkspaceGrepContextLine(
                                    line_number=item + 1, line=lines[item][:16_000]
                                )
                                for item in range(
                                    index + 1,
                                    min(len(lines), index + 1 + action.context_after),
                                )
                            ],
                        )
                    ):
                        break
        return produced, truncated

    @staticmethod
    def _validate_args(name: str, args: dict[str, Any]) -> None:
        if _contains_nul(args):
            raise ToolFailure("tool_invalid_args", "Tool arguments must not contain NUL")
        allowed = _TOOL_ARGUMENTS.get(name)
        if allowed is None:
            return
        unknown = set(args) - allowed
        if unknown:
            raise ToolFailure("tool_invalid_args", "Tool arguments contain unknown fields")
        if name == "apply_patch":
            edits = args.get("edits")
            if isinstance(edits, list):
                for edit in edits:
                    if not isinstance(edit, dict) or set(edit) - _PATCH_ARGUMENTS:
                        raise ToolFailure("tool_invalid_args", "Patch edit contains unknown fields")

    async def _read_file(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        offset = _int_arg(args, "offset", 1, minimum=1)
        limit = _int_arg(args, "limit", 2000, minimum=1)
        pages = _optional_str(args, "pages")
        resolved = await self._resolve_path(path, directory=False)
        async with self._locks.hold(str(resolved)):
            if resolved.suffix.lower() in {".pdf", ".docx", ".xlsx", ".pptx"}:
                try:
                    return ToolOutput(await convert_path_async(resolved, pages=pages))
                except ConversionError as exc:
                    return fail(exc.code, exc.message)
            return await self._run_blocking(self._read_sync, path, resolved, offset, limit)

    def _read_sync(
        self, display_path: str, path: Path, offset: int, limit: int
    ) -> ToolOutput:
        data = _read_regular(path, MAX_INPUT_BYTES)
        media = _image_media_type(data)
        if media is not None:
            return ToolOutput(
                [
                    {"type": "text", "text": f"Image: {display_path}"},
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media,
                            "data": base64.b64encode(data).decode("ascii"),
                        },
                    },
                ]
            )
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return fail("workspace_invalid_request", "File is not UTF-8 text")
        if "\x00" in text:
            return fail("tool_invalid_args", "Workspace file is binary and cannot be read as text")
        lines = text.splitlines()
        selected = lines[offset - 1 : offset - 1 + limit]
        rendered = [f"{index}|{line}" for index, line in enumerate(selected, start=offset)]
        content = _cap("\n".join(rendered), MAX_READ_CHARS)
        end = offset + len(selected) - 1
        if end < len(lines):
            footer = (
                f"(Showing lines {offset}-{end} of {len(lines)}. Use offset={end + 1} to continue.)"
            )
            content = _cap(
                f"{content}\n\n{footer}",
                MAX_READ_CHARS,
            )
        return ToolOutput(content)

    async def _write_file(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        content = _required_str(args, "content")
        resolved = await self._resolve_path(path, directory=False)
        async with self._locks.hold(str(resolved)):
            await self._run_mutation(self._atomic_write, resolved, content.encode("utf-8"))
        return ToolOutput(f"Wrote {path} ({len(content.encode('utf-8'))} bytes).")

    async def _edit_file(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        old_text = _required_str(args, "old_text")
        new_text = _required_str(args, "new_text")
        replace_all = _bool_arg(args, "replace_all", False)
        occurrence = _optional_int(args, "occurrence", minimum=1)
        line_hint = _optional_int(args, "line_hint", minimum=1)
        expected = _optional_int(args, "expected_replacements", minimum=1)
        if (occurrence is not None and line_hint is not None) or (
            replace_all and (occurrence or line_hint)
        ):
            raise ToolFailure("tool_invalid_args", "Edit selectors are mutually exclusive")
        resolved = await self._resolve_path(path, directory=False)
        async with self._locks.hold(str(resolved)):
            text: str | None
            initial = await self._run_blocking(_capture_regular, resolved, MAX_TEXT_EDIT_BYTES)
            if initial is None:
                text = None
            else:
                text = initial[0].decode("utf-8")
            updated, replacements, created = await self._run_blocking(
                _apply_text_edit,
                text,
                old_text=old_text,
                new_text=new_text,
                replace_all=replace_all,
                occurrence=occurrence,
                line_hint=line_hint,
                expected_replacements=expected,
            )
            if await self._run_blocking(_fingerprint, resolved, MAX_TEXT_EDIT_BYTES) != (
                initial[1] if initial is not None else None
            ):
                raise ToolFailure("workspace_file_changed", "File changed during the edit")
            await self._run_mutation(self._atomic_write, resolved, updated.encode("utf-8"))
        verb = "Created" if created else "Edited"
        return ToolOutput(
            f"{verb} {path}: {replacements} replacement(s), {len(updated.encode('utf-8'))} bytes."
        )

    async def _apply_patch(self, args: dict[str, Any]) -> ToolOutput:
        edits = args.get("edits")
        if (
            not isinstance(edits, list)
            or not 1 <= len(edits) <= 20
            or not all(isinstance(item, dict) for item in edits)
        ):
            raise ToolFailure("tool_invalid_args", "edits must contain 1 to 20 edits")
        dry_run = _bool_arg(args, "dry_run", False)
        parsed: list[tuple[str, Path, str, str | None, str]] = []
        for item in cast(list[dict[str, Any]], edits):
            path = self._path_arg(item)
            action = _required_str(item, "action")
            if action not in {"replace", "add"}:
                raise ToolFailure("tool_invalid_args", "Patch action is invalid")
            old = _optional_str(item, "old_text")
            new = _optional_str(item, "new_text")
            if action == "replace" and (old is None or new is None):
                raise ToolFailure("tool_invalid_args", "replace requires old_text and new_text")
            if action == "add" and new is None:
                raise ToolFailure("tool_invalid_args", "add requires new_text")
            parsed.append(
                (
                    path,
                    await self._resolve_path(path, directory=False),
                    action,
                    old,
                    cast(str, new),
                )
            )
        if len({str(item[1]) for item in parsed}) != len(parsed):
            raise ToolFailure("tool_invalid_args", "Patch paths must be unique")
        async with self._locks.hold(*(str(item[1]) for item in parsed)):
            prepared: list[tuple[str, Path, str, int, FileFingerprint | None]] = []
            total_bytes = 0
            for display, target, action, old, new in parsed:
                initial = await self._run_blocking(_capture_regular, target, MAX_TEXT_EDIT_BYTES)
                current = initial[0].decode("utf-8") if initial is not None else None
                if action == "add":
                    updated = (current or "") + new
                    count = 0
                else:
                    updated, count, _ = await self._run_blocking(
                        _apply_text_edit,
                        current,
                        old_text=cast(str, old),
                        new_text=new,
                        replace_all=False,
                        occurrence=None,
                        line_hint=None,
                        expected_replacements=None,
                    )
                total_bytes += len(updated.encode("utf-8"))
                if total_bytes > MAX_TEXT_EDIT_BYTES:
                    raise ToolFailure(
                        "workspace_file_too_large_to_edit",
                        "Patch content exceeds the 8 MiB edit limit",
                    )
                prepared.append((display, target, updated, count, initial[1] if initial else None))
            if not dry_run:
                for _, target, _, _, expected in prepared:
                    if (
                        await self._run_blocking(_fingerprint, target, MAX_TEXT_EDIT_BYTES)
                        != expected
                    ):
                        raise ToolFailure(
                            "workspace_file_changed", "File changed during the patch"
                        )
                for _, target, updated, _, _ in prepared:
                    await self._run_mutation(self._atomic_write, target, updated.encode("utf-8"))
        verb = "Validated" if dry_run else "Applied"
        lines = [f"{verb} {len(prepared)} workspace edit(s)."]
        lines.extend(
            (
                f"- {parsed[index][2]} {display}: {len(updated.encode('utf-8'))} bytes, "
                f"{count} replacement(s)"
            )
            for index, (display, _, updated, count, _) in enumerate(prepared)
        )
        return ToolOutput("\n".join(lines))

    async def _delete_file(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        resolved = await self._resolve_path(path, directory=False)
        async with self._locks.hold(str(resolved)):
            await self._run_mutation(resolved.unlink)
        return ToolOutput(f"Deleted file {path}.")

    async def _delete_folder(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        resolved = await self._resolve_path(path, directory=True)
        _reject_protected_directory_delete(resolved, self._paths.root)
        async with self._locks.hold(str(resolved)):
            await self._run_mutation(shutil.rmtree, resolved)
        return ToolOutput(f"Deleted folder {path}.")

    async def _list_dir(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        recursive = _bool_arg(args, "recursive", False)
        limit = _int_arg(args, "max_entries", 200, minimum=1, maximum=1000)
        root = await self._resolve_path(path, directory=True)
        return await self._run_blocking(self._list_dir_sync, root, recursive, limit)

    def _list_dir_sync(self, root: Path, recursive: bool, limit: int) -> ToolOutput:
        if not recursive:
            direct_entries = heapq.nsmallest(
                limit + 1,
                (item for item in root.iterdir() if item.name not in _NOISE),
                key=lambda item: item.name,
            )
            lines = [
                f"{'📁' if item.is_dir() else '📄'} {item.name}" for item in direct_entries[:limit]
            ]
            entry_count = len(direct_entries)
        else:
            entries = self._walk(root)
            lines = [
                f"{relative}{'/' if is_dir else ''}" for relative, _, is_dir in entries[:limit]
            ]
            entry_count = len(entries)
        if entry_count > limit:
            lines.append(f"(truncated, showing first {limit} entries)")
        return ToolOutput("\n".join(lines) or "(empty directory)")

    async def _find_files(self, args: dict[str, Any]) -> ToolOutput:
        path = _optional_str(args, "path") or "."
        root = await self._resolve_path(path, directory=True)
        query = (_optional_str(args, "query") or "").casefold().split()
        glob = _optional_str(args, "glob")
        file_type = _optional_str(args, "type")
        include_dirs = _bool_arg(args, "include_dirs", False)
        sort = _optional_str(args, "sort") or "path"
        head = _int_arg(args, "head_limit", 200, minimum=0, maximum=1000)
        offset = _int_arg(args, "offset", 0, minimum=0, maximum=100_000)
        if sort not in {"path", "modified"} or file_type not in {None, *_TYPES}:
            raise ToolFailure("tool_invalid_args", "Find filter is invalid")
        return await self._run_blocking(
            self._find_files_sync, root, query, glob, file_type, include_dirs, sort, head, offset
        )

    def _find_files_sync(
        self,
        root: Path,
        query: list[str],
        glob: str | None,
        file_type: str | None,
        include_dirs: bool,
        sort: str,
        head: int,
        offset: int,
    ) -> ToolOutput:
        items = self._walk(root)
        filtered = [
            item
            for item in items
            if (include_dirs or not item[2])
            and all(term in item[0].casefold() for term in query)
            and (glob is None or fnmatch.fnmatchcase(item[0], glob))
            and (
                file_type is None
                or (not item[2] and PurePosixPath(item[0]).suffix.lower() in _TYPES[file_type])
            )
        ]
        if sort == "path":
            filtered.sort(key=lambda item: item[0])
        else:
            filtered.sort(key=lambda item: item[1], reverse=True)
        result = filtered[offset:] if head == 0 else filtered[offset : offset + head]
        lines = [f"{item[0]}{'/' if item[2] else ''}" for item in result]
        if len(filtered) > offset + len(result):
            lines.append(f"(truncated, showing {len(result)} entries)")
        return ToolOutput("\n".join(lines) or "(no matching files)")

    async def _grep(self, args: dict[str, Any]) -> ToolOutput:
        pattern = _required_str(args, "pattern")
        if len(pattern) > 4096:
            raise ToolFailure("tool_invalid_regex", "Regex pattern is invalid")
        root = await self._resolve_path(
            _optional_str(args, "path") or ".", directory=True
        )
        glob = _optional_str(args, "glob")
        file_type = _optional_str(args, "type")
        mode = _optional_str(args, "output_mode") or "files_with_matches"
        if mode not in {"content", "files_with_matches", "count"} or file_type not in {
            None,
            *_TYPES,
        }:
            raise ToolFailure("tool_invalid_args", "Grep filter is invalid")
        before = _int_arg(args, "context_before", 0, minimum=0, maximum=20)
        after = _int_arg(args, "context_after", 0, minimum=0, maximum=20)
        head = _int_arg(args, "head_limit", 250, minimum=0, maximum=1000)
        alias = "max_matches" if mode == "content" else "max_results"
        head = _optional_int(args, alias, minimum=1, maximum=1000) or head
        offset = _int_arg(args, "offset", 0, minimum=0, maximum=100_000)
        flags = regex.IGNORECASE if _bool_arg(args, "case_insensitive", False) else 0
        source = regex.escape(pattern) if _bool_arg(args, "fixed_strings", False) else pattern
        try:
            compiled = regex.compile(source, flags)
        except regex.error as exc:
            raise ToolFailure("tool_invalid_regex", "Regex pattern is invalid") from exc
        return await self._run_blocking(
            self._grep_sync,
            root,
            compiled,
            glob,
            file_type,
            cast(Literal["content", "files_with_matches", "count"], mode),
            before,
            after,
            head,
            offset,
        )

    def _grep_sync(
        self,
        root: Path,
        compiled: regex.Pattern[str],
        glob: str | None,
        file_type: str | None,
        mode: Literal["content", "files_with_matches", "count"],
        before: int,
        after: int,
        head: int,
        offset: int,
    ) -> ToolOutput:
        ignored = _gitignore(root)
        selected: list[str] = []
        result_index = 0
        has_more = False
        hard_truncated = False
        output_bytes = 0

        def add_result(value: str) -> bool:
            """Append one result only when it belongs to the requested page."""

            nonlocal result_index, output_bytes, has_more, hard_truncated
            if result_index < offset:
                result_index += 1
                return False
            if head and len(selected) >= head:
                has_more = True
                return True
            encoded_size = len(value.encode("utf-8"))
            separator_size = 1 if selected else 0
            if output_bytes + separator_size + encoded_size > MAX_RESPONSE_BYTES:
                hard_truncated = True
                return True
            selected.append(value)
            output_bytes += separator_size + encoded_size
            result_index += 1
            return False

        for relative, _, is_dir in self._walk(root):
            if (
                is_dir
                or ignored.match_file(relative)
                or (glob and not fnmatch.fnmatchcase(relative, glob))
            ):
                continue
            if file_type and PurePosixPath(relative).suffix.lower() not in _TYPES[file_type]:
                continue
            path = root / relative
            try:
                data = _read_regular(path, MAX_GREP_BYTES)
                text = data.decode("utf-8")
            except (ToolFailure, UnicodeDecodeError):
                continue
            if b"\x00" in data:
                continue
            lines = text.splitlines()
            if mode == "files_with_matches":
                try:
                    matched = any(
                        len(line) <= 16_000 and compiled.search(line, timeout=0.05)
                        for line in lines
                    )
                except (TimeoutError, regex.error) as exc:
                    raise ToolFailure("tool_invalid_regex", "Regex pattern is invalid") from exc
                if matched and add_result(relative):
                    break
            elif mode == "count":
                count = 0
                try:
                    for line in lines:
                        if len(line) <= 16_000 and compiled.search(line, timeout=0.05):
                            count += 1
                except (TimeoutError, regex.error) as exc:
                    raise ToolFailure("tool_invalid_regex", "Regex pattern is invalid") from exc
                if count and add_result(f"{relative}:{count}"):
                    break
            else:
                try:
                    for index, line in enumerate(lines):
                        if len(line) > 16_000 or not compiled.search(line, timeout=0.05):
                            continue
                        for context in range(max(0, index - before), index):
                            if add_result(f"{relative}-{context + 1}-{lines[context][:16_000]}"):
                                break
                        else:
                            if add_result(f"{relative}:{index + 1}:{line}"):
                                break
                            for context in range(index + 1, min(len(lines), index + 1 + after)):
                                if add_result(
                                    f"{relative}-{context + 1}-{lines[context][:16_000]}"
                                ):
                                    break
                            else:
                                continue
                        break
                except (TimeoutError, regex.error) as exc:
                    raise ToolFailure("tool_invalid_regex", "Regex pattern is invalid") from exc
                if has_more or hard_truncated:
                    break
        if hard_truncated:
            selected.append(f"(truncated at response byte limit, showing {len(selected)} results)")
        elif has_more:
            selected.append(
                f"(more results available; use offset={offset + len(selected)} to continue)"
            )
        return ToolOutput("\n".join(selected) or "(no matches)")

    async def _notebook_edit(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        if not path.lower().endswith(".ipynb"):
            raise ToolFailure("tool_invalid_args", "notebook_edit requires an .ipynb path")
        index = _int_arg(args, "cell_index", minimum=0)
        mode = _optional_str(args, "edit_mode") or "replace"
        cell_type = _optional_str(args, "cell_type") or "code"
        source = _optional_str(args, "new_source")
        if mode not in {"replace", "insert", "delete"} or cell_type not in {"code", "markdown"}:
            raise ToolFailure("tool_invalid_args", "Notebook edit mode is invalid")
        if mode != "delete" and source is None:
            raise ToolFailure("tool_invalid_args", f"{mode} requires new_source")
        target = await self._resolve_path(path, directory=False)
        async with self._locks.hold(str(target)):
            initial = await self._run_blocking(_capture_regular, target, MAX_TEXT_EDIT_BYTES)
            updated = await self._run_blocking(
                self._edit_notebook_sync, target, index, mode, cell_type, source
            )
            if await self._run_blocking(_fingerprint, target, MAX_TEXT_EDIT_BYTES) != (
                initial[1] if initial is not None else None
            ):
                raise ToolFailure("workspace_file_changed", "Notebook changed during the edit")
            await self._run_mutation(self._atomic_write, target, updated.encode("utf-8"))
        return ToolOutput(f"Edited notebook {path} ({len(updated.encode('utf-8'))} bytes).")

    @staticmethod
    def _edit_notebook_sync(
        path: Path, index: int, mode: str, cell_type: str, source: str | None
    ) -> str:
        try:
            notebook = json.loads(_read_regular(path, MAX_TEXT_EDIT_BYTES).decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise ToolFailure("tool_invalid_notebook", "Notebook is not UTF-8") from exc
        except json.JSONDecodeError as exc:
            raise ToolFailure("tool_invalid_notebook", "Notebook is not valid JSON") from exc
        _validate_notebook(notebook)
        assert isinstance(notebook, dict)
        cells = notebook["cells"]
        assert isinstance(cells, list)
        if not 0 <= index < len(cells):
            raise ToolFailure("tool_cell_index_out_of_range", "cell_index is outside the notebook")
        if mode == "delete":
            del cells[index]
        elif mode == "insert":
            cell: dict[str, Any] = {"cell_type": cell_type, "metadata": {}, "source": source}
            if cell_type == "code":
                cell.update(execution_count=None, outputs=[])
            cells.insert(index + 1, cell)
        else:
            if not isinstance(cells[index], dict):
                raise ToolFailure("tool_invalid_notebook", "Notebook cells must be objects")
            cells[index]["source"] = source
        return json.dumps(notebook, ensure_ascii=False)

    async def _web_fetch(self, args: dict[str, Any]) -> ToolOutput:
        url = _required_str(args, "url")
        mode = _optional_str(args, "extractMode") or "markdown"
        chars = _int_arg(args, "maxChars", 50_000, minimum=100, maximum=50_000)
        if mode not in {"markdown", "text"}:
            raise ToolFailure("tool_invalid_args", "extractMode is invalid")
        try:
            content, final_url, content_type, charset = await _fetch_bounded(url, self._denylist)
        except ToolFailure as exc:
            return fail(exc.code, exc.message)
        except httpx.TimeoutException:
            return fail("network_timeout", "web_fetch timed out")
        except httpx.HTTPError:
            return fail("network_http_error", "web_fetch request failed")
        if "text/html" in content_type or "application/xhtml+xml" in content_type:
            return await self._convert_html(content, final_url, mode, chars)
        return ToolOutput(_cap(_decode(content, charset), chars))

    @staticmethod
    async def _convert_html(data: bytes, url: str, mode: str, chars: int) -> ToolOutput:
        try:
            markdown = await convert_html_bytes_async(data, base_url=url)
        except ConversionError as exc:
            return fail(exc.code, exc.message)
        if mode == "text":
            markdown = re.sub(r"\[([^]]+)]\([^)]+\)", r"\1", markdown)
        return ToolOutput(_cap(markdown, chars))

    def _walk(self, root: Path) -> list[tuple[str, float, bool]]:
        entries: list[tuple[str, float, bool]] = []
        for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
            dirs[:] = sorted(item for item in dirs if item not in _NOISE)
            relative_root = Path(current).relative_to(root)
            for name in [*dirs, *sorted(files)]:
                candidate = Path(current) / name
                try:
                    info = candidate.lstat()
                except OSError:
                    continue
                if stat.S_ISLNK(info.st_mode):
                    continue
                relative = (relative_root / name).as_posix()
                entries.append((relative, info.st_mtime, stat.S_ISDIR(info.st_mode)))
                if len(entries) >= MAX_SCAN_OBJECTS:
                    return entries
        return entries

    def _path_arg(self, args: dict[str, Any]) -> str:
        return _required_str(args, "path")

    def _atomic_write(self, path: Path, data: bytes) -> None:
        if len(data) > MAX_TEXT_EDIT_BYTES:
            raise ToolFailure(
                "workspace_file_too_large_to_edit", "File exceeds the 8 MiB edit limit"
            )
        self._paths.prepare_parent(path)
        descriptor, temporary = tempfile.mkstemp(prefix=".openoctopus-", dir=path.parent)
        temp_path = Path(temporary)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            self._paths.resolve(str(path), directory=False)
            os.replace(temp_path, path)
            _fsync_directory(path.parent)
        finally:
            temp_path.unlink(missing_ok=True)


def _validate_notebook(value: object) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("cells"), list):
        raise ToolFailure("tool_invalid_notebook", "Notebook must contain a cells array")
    if "metadata" in value and not isinstance(value["metadata"], dict):
        raise ToolFailure("tool_invalid_notebook", "Notebook metadata must be an object")
    for cell in value["cells"]:
        if not isinstance(cell, dict):
            raise ToolFailure("tool_invalid_notebook", "Notebook cells must be objects")
        if cell.get("cell_type") not in {"code", "markdown", "raw"}:
            raise ToolFailure("tool_invalid_notebook", "Notebook cell_type is invalid")
        source = cell.get("source")
        if not isinstance(source, str) and not (
            isinstance(source, list) and all(isinstance(part, str) for part in source)
        ):
            raise ToolFailure("tool_invalid_notebook", "Notebook cell source is invalid")
        if "metadata" in cell and not isinstance(cell["metadata"], dict):
            raise ToolFailure("tool_invalid_notebook", "Notebook cell metadata must be an object")
        if cell["cell_type"] == "code" and not isinstance(cell.get("outputs"), list):
            raise ToolFailure("tool_invalid_notebook", "Code cell outputs must be an array")


async def _fetch_bounded(url: str, denylist: tuple[str, ...]) -> tuple[bytes, str, str, str]:
    current = url
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=False, trust_env=False
    ) as client:
        for index in range(MAX_REDIRECTS + 1):
            target = httpx.URL(current)
            if (
                target.scheme not in {"http", "https"}
                or target.host is None
                or target.username
                or target.password
            ):
                raise ToolFailure(
                    "tool_invalid_args", "url must be an http(s) URL without credentials"
                )
            host = target.host.rstrip(".").lower()
            port = target.port or (443 if target.scheme == "https" else 80)
            addresses = await _validated_addresses(host, port, denylist)
            address = addresses[0]
            pinned = target.copy_with(host=address)
            headers = {
                "host": target.netloc.decode(),
                "accept-encoding": "identity",
                "user-agent": "OpenOctopus/0.0.1 web_fetch",
            }
            response = await client.send(
                client.build_request(
                    "GET",
                    pinned,
                    headers=headers,
                    extensions={"sni_hostname": host},
                ),
                stream=True,
            )
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                await response.aclose()
                if location is None or index == MAX_REDIRECTS:
                    raise ToolFailure("network_http_error", "web_fetch redirect is invalid")
                current = str(target.join(location))
                continue
            if response.status_code >= 400:
                status = response.status_code
                await response.aclose()
                raise ToolFailure("network_http_error", f"web_fetch received HTTP {status}")
            if response.headers.get("content-encoding", "").strip().lower() not in {"", "identity"}:
                await response.aclose()
                raise ToolFailure(
                    "network_http_error", "web_fetch does not support compressed responses"
                )
            body = bytearray()
            try:
                async for chunk in response.aiter_raw():
                    body.extend(chunk[: MAX_RESPONSE_BYTES - len(body)])
                    if len(body) >= MAX_RESPONSE_BYTES:
                        break
            finally:
                await response.aclose()
            return (
                bytes(body),
                current,
                response.headers.get("content-type", "").lower(),
                response.encoding or "utf-8",
            )
    raise ToolFailure("network_http_error", "web_fetch redirect loop terminated unexpectedly")


async def _validated_addresses(host: str, port: int, denylist: tuple[str, ...]) -> list[str]:
    import ipaddress
    import socket

    if _denied_host(host, port, denylist):
        raise ToolFailure("network_ssrf_blocked", f"Blocked address for {host}")
    try:
        records = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ToolFailure("network_dns_failed", f"Could not resolve {host}") from exc
    addresses: list[str] = []
    for record in records:
        address = str(record[4][0])
        parsed = ipaddress.ip_address(address)
        if _denied_ip(parsed, denylist):
            raise ToolFailure("network_ssrf_blocked", f"Blocked address for {host}")
        if address not in addresses:
            addresses.append(address)
    if not addresses:
        raise ToolFailure("network_dns_failed", f"Could not resolve {host}")
    return addresses


def _denied_host(host: str, port: int, denylist: tuple[str, ...]) -> bool:
    endpoint = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    return any(entry.lower() in {host, endpoint} for entry in denylist)


def _denied_ip(address: object, denylist: tuple[str, ...]) -> bool:
    import ipaddress

    assert isinstance(address, ipaddress.IPv4Address | ipaddress.IPv6Address)
    candidates = [address]
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        candidates.append(address.ipv4_mapped)
    for entry in denylist:
        try:
            network = ipaddress.ip_network(entry, strict=False)
            if any(
                candidate.version == network.version and candidate in network
                for candidate in candidates
            ):
                return True
        except ValueError:
            continue
    return False


def _track_blocking_task(
    tracker: set[asyncio.Task[Any]], task: asyncio.Task[Any]
) -> None:
    tracker.add(task)

    def complete(done: asyncio.Task[Any]) -> None:
        tracker.discard(done)
        # A cancellation may leave the worker task un-awaited.  Consume its
        # exception so a blocked or failed filesystem call cannot produce a
        # "Task exception was never retrieved" warning after shutdown.
        if not done.cancelled():
            with contextlib.suppress(BaseException):
                done.exception()

    task.add_done_callback(complete)


async def _run_blocking[T](
    tracker: set[asyncio.Task[Any]],
    function: Callable[..., T],
    *args: Any,
    **kwargs: Any,
) -> T:
    """Run local IO/CPU work without letting cancellation hide its thread."""

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    _track_blocking_task(tracker, task)
    return await asyncio.shield(task)


async def _run_blocking_with_drain[T](
    tracker: set[asyncio.Task[Any]],
    abandoned_drains: set[asyncio.Task[None]],
    function: Callable[..., T],
    *args: Any,
    on_abandoned: Callable[[T], Any] | None = None,
    **kwargs: Any,
) -> T:
    """Transfer blocking work to a runtime drain when its caller is cancelled."""

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    _track_blocking_task(tracker, task)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # The runtime drain now owns this operation; it must not hold the
        # connection's FIFO tool worker open while the OS syscall finishes.
        tracker.discard(task)
        drain = asyncio.create_task(_drain_blocking_result(task, on_abandoned))
        abandoned_drains.add(drain)

        def finish(completed: asyncio.Task[None]) -> None:
            abandoned_drains.discard(completed)
            if not completed.cancelled():
                with contextlib.suppress(BaseException):
                    completed.exception()

        drain.add_done_callback(finish)
        with contextlib.suppress(BaseException):
            await asyncio.wait(
                {drain}, timeout=_LOCAL_TRANSFER_CANCEL_GRACE_SECONDS
            )
        raise


async def _drain_blocking_result[T](
    task: asyncio.Task[T], on_abandoned: Callable[[T], Any] | None
) -> None:
    try:
        result = await asyncio.shield(task)
    except BaseException:
        return
    if on_abandoned is None:
        return
    cleanup = asyncio.create_task(asyncio.to_thread(on_abandoned, result))
    try:
        await asyncio.shield(cleanup)
    except BaseException:
        if not cleanup.cancelled():
            with contextlib.suppress(BaseException):
                cleanup.exception()


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


async def _run_mutation(
    function: Any,
    *args: Any,
    tracker: set[asyncio.Task[Any]] | None = None,
    **kwargs: Any,
) -> Any:
    """Finish a worker-thread mutation before releasing its path lock."""

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    if tracker is not None:
        _track_blocking_task(tracker, task)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # Cancellation of ``to_thread`` only cancels the asyncio wrapper; the
        # underlying filesystem operation keeps running.  Wait for it while
        # the caller still owns PathLocks, then propagate the cancellation.
        with contextlib.suppress(BaseException):
            await asyncio.shield(task)
        raise


async def _run_irreversible_mutation[T](
    tracker: set[asyncio.Task[Any]],
    function: Callable[..., T],
    *args: Any,
) -> T:
    """Return the true result once a no-rollback filesystem operation starts."""

    task = asyncio.create_task(asyncio.to_thread(function, *args))
    _track_blocking_task(tracker, task)
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            continue


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
        open_osfhandle = cast(
            Callable[[int, int], int], getattr(msvcrt, "open_osfhandle")
        )
        return int(
            open_osfhandle(int(handle), os.O_RDONLY | int(getattr(os, "O_BINARY", 0)))
        )
    except (OSError, OverflowError):
        kernel32.CloseHandle(handle)
        raise


def _create_transfer_temp(parent: Path, name: str) -> tuple[int, Path]:
    try:
        descriptor, raw_path = tempfile.mkstemp(
            prefix=f".{name}.openoctopus-", dir=parent
        )
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


def _source_unchanged(
    path: Path, descriptor: int, initial: tuple[int, int, int, int, int]
) -> bool:
    try:
        return _transfer_identity(os.fstat(descriptor)) == initial and _transfer_identity(
            os.lstat(path)
        ) == initial
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


def _capture_regular(path: Path, limit: int) -> tuple[bytes, FileFingerprint] | None:
    try:
        return _read_regular_with_fingerprint(path, limit)
    except ToolFailure as exc:
        if exc.code == "workspace_not_found":
            return None
        raise


def _fingerprint(path: Path, limit: int) -> FileFingerprint | None:
    captured = _capture_regular(path, limit)
    return captured[1] if captured is not None else None


def _read_regular(path: Path, limit: int) -> bytes:
    return _read_regular_fd(path, limit)[0]


def _read_regular_with_fingerprint(path: Path, limit: int) -> tuple[bytes, FileFingerprint]:
    data, after = _read_regular_fd(path, limit)
    digest = hashlib.sha256(data).hexdigest()
    fingerprint: FileFingerprint = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_mode,
        digest,
    )
    return data, fingerprint


def _read_regular_fd(path: Path, limit: int) -> tuple[bytes, os.stat_result]:
    flags = (
        os.O_RDONLY
        | int(getattr(os, "O_BINARY", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
        | int(getattr(os, "O_NONBLOCK", 0))
    )
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError as exc:
        raise ToolFailure("workspace_not_found", "Path does not exist") from exc
    except OSError as exc:
        raise ToolFailure("workspace_permission_denied", "Path could not be read") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ToolFailure("workspace_blocked_path", "Path is not a regular file")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            data = stream.read(limit + 1)
            after = os.fstat(stream.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(data) > limit:
        raise ToolFailure("workspace_file_too_large_to_edit", "File exceeds the size limit")
    return data, after


def _apply_text_edit(
    text: str | None,
    *,
    old_text: str,
    new_text: str,
    replace_all: bool,
    occurrence: int | None,
    line_hint: int | None,
    expected_replacements: int | None,
) -> tuple[str, int, bool]:
    if text is None:
        if old_text:
            raise ToolFailure("tool_no_match", "Text to replace was not found")
        _expected(expected_replacements, 0)
        _size(new_text)
        return new_text, 0, True
    if not old_text:
        raise ToolFailure(
            "tool_invalid_args", "old_text may be empty only when creating a missing file"
        )
    original_crlf = "\r\n" in text
    text, old_text, new_text = (
        item.replace("\r\n", "\n").replace("\r", "\n") for item in (text, old_text, new_text)
    )
    matches = _matches(text, old_text)
    level = "exact"
    if not matches:
        matches = _trimmed_matches(text, old_text)
        level = "trimmed"
    if not matches:
        matches = _quote_matches(text, old_text)
        level = "quote"
    if not matches:
        raise ToolFailure("tool_no_match", "Text to replace was not found")
    selected = _select(matches, replace_all, occurrence, line_hint)
    _expected(expected_replacements, len(selected))
    for start, end, _ in reversed(selected):
        replacement = new_text
        matched = text[start:end]
        if level == "trimmed":
            replacement = _indented(old_text, new_text, matched)
        elif level == "quote":
            replacement = _quote_style(new_text, matched)
        text = text[:start] + replacement + text[end:]
    if original_crlf:
        text = text.replace("\n", "\r\n")
    _size(text)
    return text, len(selected), False


def _matches(text: str, needle: str) -> list[tuple[int, int, int]]:
    result: list[tuple[int, int, int]] = []
    start = 0
    line = 1
    counted_to = 0
    while (index := text.find(needle, start)) >= 0:
        line += text.count("\n", counted_to, index)
        result.append((index, index + len(needle), line))
        if len(result) > 1000:
            raise ToolFailure("tool_ambiguous_edit", "Fuzzy match candidate limit exceeded")
        start = index + len(needle)
        counted_to = index
    return result


def _trimmed_matches(text: str, needle: str) -> list[tuple[int, int, int]]:
    wanted = needle.splitlines()
    if not wanted:
        return []
    result: list[tuple[int, int, int]] = []
    lines = text.splitlines(keepends=True)
    offsets: list[int] = []
    position = 0
    for line in lines:
        offsets.append(position)
        position += len(line)
    for index in range(len(lines) - len(wanted) + 1):
        candidate = [line.rstrip("\n").strip() for line in lines[index : index + len(wanted)]]
        if candidate == [line.strip() for line in wanted]:
            result.append(
                (
                    offsets[index],
                    offsets[index + len(wanted) - 1]
                    + len(lines[index + len(wanted) - 1].rstrip("\n")),
                    index + 1,
                )
            )
            if len(result) > 1000:
                raise ToolFailure(
                    "tool_ambiguous_edit", "Fuzzy match candidate limit exceeded"
                )
    return result


def _quote_matches(text: str, needle: str) -> list[tuple[int, int, int]]:
    translation = str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"'})
    normalized = text.translate(translation)
    return _matches(normalized, needle.translate(translation))


def _select(
    matches: list[tuple[int, int, int]],
    replace_all: bool,
    occurrence: int | None,
    line_hint: int | None,
) -> list[tuple[int, int, int]]:
    if len(matches) > 1000:
        raise ToolFailure("tool_ambiguous_edit", "Fuzzy match candidate limit exceeded")
    if replace_all:
        return matches
    if occurrence is not None:
        if occurrence > len(matches):
            raise ToolFailure("tool_no_match", "Requested occurrence was not found")
        return [matches[occurrence - 1]]
    if line_hint is not None:
        distance = min(abs(item[2] - line_hint) for item in matches)
        nearest = [item for item in matches if abs(item[2] - line_hint) == distance]
        if len(nearest) == 1:
            return nearest
        raise ToolFailure("tool_ambiguous_edit", "line_hint is equally close to multiple matches")
    if len(matches) != 1:
        raise ToolFailure("tool_ambiguous_edit", "Text to replace appears more than once")
    return matches


def _indented(old: str, new: str, matched: str) -> str:
    old_indent = _base_indent(old)
    target_indent = _base_indent(matched)
    return "\n".join(
        ""
        if not line
        else target_indent
        + (line[len(old_indent) :] if old_indent and line.startswith(old_indent) else line)
        for line in new.split("\n")
    )


def _base_indent(value: str) -> str:
    indents = [
        line[: len(line) - len(line.lstrip())] for line in value.splitlines() if line.strip()
    ]
    return min(indents, key=len) if indents else ""


def _quote_style(new: str, matched: str) -> str:
    doubles = [item for item in matched if item in '“”"']
    singles = [item for item in matched if item in "‘’'"]
    double_index = single_index = 0
    rendered: list[str] = []
    for item in new:
        if item == '"' and doubles:
            rendered.append(doubles[min(double_index, len(doubles) - 1)])
            double_index += 1
        elif item == "'" and singles:
            rendered.append(singles[min(single_index, len(singles) - 1)])
            single_index += 1
        else:
            rendered.append(item)
    return "".join(rendered)


def _expected(value: int | None, actual: int) -> None:
    if value is not None and value != actual:
        raise ToolFailure(
            "tool_invalid_args",
            f"expected_replacements was {value}, but the edit would replace {actual}",
        )


def _size(value: str) -> None:
    if len(value.encode("utf-8")) > MAX_TEXT_EDIT_BYTES:
        raise ToolFailure("workspace_file_too_large_to_edit", "File exceeds the 8 MiB edit limit")


def _gitignore(root: Path) -> pathspec.GitIgnoreSpec:
    lines: list[str] = []
    candidate = root / ".gitignore"
    if candidate.is_file():
        try:
            lines = (
                _read_regular(candidate, MAX_GREP_BYTES)
                .decode("utf-8", errors="replace")
                .splitlines()
            )
        except ToolFailure:
            pass
    return pathspec.GitIgnoreSpec.from_lines(lines)


def _image_media_type(data: bytes) -> str | None:
    for signature, media_type in _IMAGE_TYPES.items():
        if data.startswith(signature):
            if media_type == "image/webp" and len(data) < 12:
                continue
            if media_type == "image/webp" and data[8:12] != b"WEBP":
                continue
            return media_type
    return None


def _decode(data: bytes, charset: str) -> str:
    try:
        return data.decode(charset, errors="replace")
    except LookupError:
        return data.decode("utf-8", errors="replace")


def _cap(value: str, maximum: int) -> str:
    return value if len(value) <= maximum else value[:maximum] + "\n... (truncated)"


def _timeout_for(name: str) -> float:
    return {
        "delete_file": 10.0,
        "delete_folder": 60.0,
        "web_fetch": 30.0,
        INTERNAL_WORKSPACE_ACTION: 60.0,
    }.get(name, 30.0)


def _workspace_json(value: object) -> ToolOutput:
    if not isinstance(value, BaseModel):
        raise TypeError("workspace result must be a Pydantic model")
    encoded = value.model_dump_json()
    if len(encoded.encode("utf-8")) > MAX_WORKSPACE_RESPONSE_BYTES:
        raise ToolFailure("tool_result_too_large", "Workspace result exceeds the response limit")
    return ToolOutput(encoded)


def _directory_page(
    values: list[WorkspaceDirectoryEntry], *, limit: int, offset: int, truncated: bool = False
) -> WorkspaceDirectoryPage:
    page = values[offset : offset + limit]
    return WorkspaceDirectoryPage(
        items=page,
        limit=limit,
        offset=offset,
        next_offset=offset + limit if len(values) > offset + limit else None,
        truncated=truncated,
    )


def _grep_page(
    values: list[WorkspaceGrepItem], *, limit: int, offset: int, truncated: bool = False
) -> WorkspaceGrepPage:
    page = values[offset : offset + limit]
    return WorkspaceGrepPage(
        items=page,
        limit=limit,
        offset=offset,
        next_offset=offset + limit if len(values) > offset + limit else None,
        truncated=truncated,
    )


def _directory_entry(
    request_path: str,
    relative: str,
    is_directory: bool,
    size: int,
) -> WorkspaceDirectoryEntry:
    public_path = _public_path(request_path, relative)
    return WorkspaceDirectoryEntry(
        name=PurePosixPath(relative).name,
        path=public_path,
        kind="directory" if is_directory else "file",
        size=0 if is_directory else size,
    )


def _public_path(request_path: str, relative_path: str) -> str:
    if request_path in {"", "."}:
        return relative_path
    return f"{request_path.rstrip('/')}/{relative_path}"


def _reject_protected_directory_delete(target: Path, workspace_root: Path) -> None:
    if target == workspace_root or target.parent == target:
        raise ToolFailure(
            "workspace_invalid_request",
            "Deleting the workspace or filesystem root is not allowed",
        )


def _safe_size(path: Path) -> int:
    try:
        info = path.lstat()
    except OSError as exc:
        raise ToolFailure("workspace_permission_denied", "Path could not be inspected") from exc
    if not stat.S_ISREG(info.st_mode):
        raise ToolFailure("workspace_blocked_path", "Path is not a regular file")
    return info.st_size


def _validate_glob(pattern: str | None) -> None:
    if pattern is None:
        return
    depth = 0
    for character in pattern:
        if character == "[":
            depth += 1
        elif character == "]":
            if depth == 0:
                raise ToolFailure("tool_invalid_glob", "Glob pattern is invalid")
            depth -= 1
    if depth:
        raise ToolFailure("tool_invalid_glob", "Glob pattern is invalid")


def _stat_fingerprint(path: Path) -> str | None:
    try:
        info = path.stat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ToolFailure("workspace_permission_denied", "Path could not be inspected") from exc
    if not stat.S_ISREG(info.st_mode):
        raise ToolFailure("workspace_blocked_path", "Path is not a regular file")
    return opaque_stat_fingerprint((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns))


def _opaque_fingerprint(value: FileFingerprint) -> str:
    device, inode, size, modified_ns, _, _ = value
    return opaque_stat_fingerprint((device, inode, size, modified_ns))


def _require_etag(path: Path) -> str:
    etag = _stat_fingerprint(path)
    if etag is None:
        raise ToolFailure("workspace_not_found", "Path does not exist")
    return etag


def _required_str(args: dict[str, Any], name: str) -> str:
    value = args.get(name)
    if not isinstance(value, str):
        raise ToolFailure("tool_invalid_args", f"{name} is required and must be a string")
    return value


def _contains_nul(value: object) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_contains_nul(key) or _contains_nul(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_nul(item) for item in value)
    return False


def _optional_str(args: dict[str, Any], name: str) -> str | None:
    value = args.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ToolFailure("tool_invalid_args", f"{name} must be a string")
    return value


def _bool_arg(args: dict[str, Any], name: str, default: bool) -> bool:
    value = args.get(name, default)
    if not isinstance(value, bool):
        raise ToolFailure("tool_invalid_args", f"{name} must be a boolean")
    return value


def _int_arg(
    args: dict[str, Any],
    name: str,
    default: int | None = None,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    value = args.get(name, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        raise ToolFailure("tool_invalid_args", f"{name} is invalid")
    return value


def _optional_int(
    args: dict[str, Any], name: str, *, minimum: int, maximum: int | None = None
) -> int | None:
    if name not in args or args[name] is None:
        return None
    return _int_arg(args, name, minimum=minimum, maximum=maximum)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
