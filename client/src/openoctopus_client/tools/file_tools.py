from __future__ import annotations

import base64
import fnmatch
import hashlib
import heapq
import json
import os
import re
import shutil
import stat
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

import pathspec
import regex
from pydantic import BaseModel

from openoctopus_client.document_convert import (
    MAX_INPUT_BYTES,
    ConversionError,
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
)
from openoctopus_client.tools.workspace_rest import MAX_SCAN_OBJECTS as REST_MAX_SCAN_OBJECTS
from openoctopus_client.tools.workspace_rest import MAX_TEXT_EDIT_BYTES as REST_MAX_TEXT_EDIT_BYTES
from openoctopus_client.transfer_admission import (
    LOCAL_TRANSFER_CAPACITY,
    LocalTransferAdmission,
    LocalTransferDrainRegistry,
)

from .blocking import BlockingWork
from .common import (
    _bool_arg,
    _cap,
    _int_arg,
    _optional_int,
    _optional_str,
    _required_str,
)
from .local_transfer import LocalFileTransfers, _fsync_directory

MAX_READ_CHARS = 128_000
MAX_TEXT_EDIT_BYTES = 8 * 1024 * 1024
MAX_GREP_BYTES = 2 * 1024 * 1024
MAX_SCAN_OBJECTS = 10_000
MAX_RESPONSE_BYTES = 5_000_000
_FILE_RESULT_JSON_MAX_CHARS = 49_500
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


class FileTools:
    """File tools and workspace REST operations sharing paths and worker ownership."""

    def __init__(
        self,
        workspace: Path,
        *,
        restrict_to_workspace: bool,
        path_locks: PathLocks | None = None,
        transfer_admission: LocalTransferAdmission | None = None,
        transfer_drains: LocalTransferDrainRegistry | None = None,
    ) -> None:
        self._paths = WorkspacePaths(
            workspace,
            restrict_to_workspace=restrict_to_workspace,
        )
        self._locks = path_locks or PathLocks()
        self.work = BlockingWork()
        self._transfers = LocalFileTransfers(
            self._paths,
            self._locks,
            self.work,
            transfer_admission or LocalTransferAdmission(capacity=LOCAL_TRANSFER_CAPACITY),
            transfer_drains or LocalTransferDrainRegistry(),
        )

    async def _resolve_path(self, path: str, *, directory: bool | None) -> Path:
        return await self.work.run(self._paths.resolve, path, directory=directory)

    async def execute(self, name: str, args: dict[str, Any]) -> ToolOutput:
        if name == INTERNAL_WORKSPACE_ACTION:
            return await self._workspace_rest(args)
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
            return _workspace_json(await self._transfers.execute(action))
        return await self._workspace_rest_grep(action)

    async def _workspace_rest_edit(self, action: WorkspaceRestAction) -> ToolOutput:
        assert action.path is not None
        assert action.old_text is not None and action.new_text is not None
        target = await self._resolve_path(action.path, directory=False)
        async with self._locks.hold(str(target)):
            initial = await self.work.run(_capture_regular, target, REST_MAX_TEXT_EDIT_BYTES)
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
            updated, replacements, created = await self.work.run(
                _apply_text_edit,
                current,
                old_text=action.old_text,
                new_text=action.new_text,
                replace_all=action.replace_all,
                occurrence=action.occurrence,
                line_hint=action.line_hint,
                expected_replacements=action.expected_replacements,
            )
            if await self.work.run(_stat_fingerprint, target) != initial_etag:
                raise ToolFailure("workspace_file_changed", "File changed during the edit")
            await self.work.mutate(self._atomic_write, target, updated.encode("utf-8"))
            etag = await self.work.run(_stat_fingerprint, target)
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
        if len({item[1] for item in targets}) != len(targets):
            raise ToolFailure("tool_invalid_args", "Patch paths must be unique")
        async with self._locks.hold(*(str(item[1]) for item in targets)):
            prepared: list[tuple[str, Path, str, int, bool, str | None]] = []
            total_bytes = 0
            for display, target, operation, old, new in targets:
                initial = await self.work.run(_capture_regular, target, REST_MAX_TEXT_EDIT_BYTES)
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
                    updated, replacements, created = await self.work.run(
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
                    if await self.work.run(_stat_fingerprint, target) != expected:
                        raise ToolFailure("workspace_file_changed", "File changed during the patch")
                for _, target, updated, _, _, _ in prepared:
                    await self.work.mutate(self._atomic_write, target, updated.encode("utf-8"))
            results = [
                WorkspacePatchEditResult(
                    path=display,
                    action=targets[index][2],
                    size=len(updated.encode("utf-8")),
                    etag="dry-run"
                    if action.dry_run
                    else await self.work.run(_require_etag, target),
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
                if not await self.work.run(target.exists):
                    raise ToolFailure("workspace_not_found", "Path does not exist")
            else:
                current = await self.work.run(_stat_fingerprint, target)
                if current is None:
                    raise ToolFailure("workspace_not_found", "Path does not exist")
                if action.if_match is not None and action.if_match != current:
                    raise ToolFailure("workspace_file_changed", "File changed during the delete")
            mutation = shutil.rmtree if directory else target.unlink
            await self.work.mutate(mutation, target)
        return _workspace_json(WorkspaceDeleteResult())

    async def _workspace_rest_list(self, action: WorkspaceRestAction) -> ToolOutput:
        assert action.path is not None
        root = await self._resolve_path(action.path, directory=True)
        entries, truncated = await self.work.run(
            self._workspace_list_entries, root, action.recursive
        )
        values = [
            _directory_entry(action.path, relative, is_directory, size)
            for relative, size, is_directory in entries
        ]
        return _workspace_json(
            _directory_page(values, limit=action.limit, offset=action.offset, truncated=truncated)
        )

    async def _workspace_rest_find(self, action: WorkspaceRestAction) -> ToolOutput:
        assert action.path is not None
        root = await self._resolve_path(action.path, directory=True)
        if action.sort not in {"path", "modified"} or action.type not in {None, *_TYPES}:
            raise ToolFailure("tool_invalid_args", "Find filter is invalid")
        _validate_glob(action.glob)
        query = action.query.casefold().split()
        entries = await self.work.run(self._walk, root)
        scan_truncated = len(entries) >= REST_MAX_SCAN_OBJECTS
        valid_entries: list[tuple[str, float, bool]] = []
        for item in entries:
            if item[2]:
                valid_entries.append(item)
                continue
            try:
                await self.work.run(_safe_size, root / item[0])
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
                or (not item[2] and PurePosixPath(item[0]).suffix.lower() in _TYPES[action.type])
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
                0 if is_directory else await self.work.run(_safe_size, root / relative),
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
        entries = await self.work.run(self._walk, root)
        scan_truncated = len(entries) >= REST_MAX_SCAN_OBJECTS
        values, truncated = await self.work.run(
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
            return await self.work.run(self._read_sync, path, resolved, offset, limit)

    def _read_sync(self, display_path: str, path: Path, offset: int, limit: int) -> ToolOutput:
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
        encoded = content.encode("utf-8")
        resolved = await self._resolve_path(path, directory=False)
        async with self._locks.hold(str(resolved)):
            await self.work.mutate(self._atomic_write, resolved, encoded)
        return self._file_mutation_result(
            "write_file",
            requested_path=path,
            resolved_path=resolved,
            bytes_written=len(encoded),
        )

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
            initial = await self.work.run(_capture_regular, resolved, MAX_TEXT_EDIT_BYTES)
            if initial is None:
                text = None
            else:
                text = initial[0].decode("utf-8")
            updated, replacements, created = await self.work.run(
                _apply_text_edit,
                text,
                old_text=old_text,
                new_text=new_text,
                replace_all=replace_all,
                occurrence=occurrence,
                line_hint=line_hint,
                expected_replacements=expected,
            )
            if await self.work.run(_fingerprint, resolved, MAX_TEXT_EDIT_BYTES) != (
                initial[1] if initial is not None else None
            ):
                raise ToolFailure("workspace_file_changed", "File changed during the edit")
            await self.work.mutate(self._atomic_write, resolved, updated.encode("utf-8"))
        verb = "Created" if created else "Edited"
        return self._file_mutation_result(
            "edit_file",
            requested_path=path,
            resolved_path=resolved,
            result=verb.lower(),
            replacements=replacements,
            size_bytes=len(updated.encode("utf-8")),
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
        if len({item[1] for item in parsed}) != len(parsed):
            raise ToolFailure("tool_invalid_args", "Patch paths must be unique")
        async with self._locks.hold(*(str(item[1]) for item in parsed)):
            prepared: list[tuple[str, Path, str, int, FileFingerprint | None]] = []
            total_bytes = 0
            for display, target, action, old, new in parsed:
                initial = await self.work.run(_capture_regular, target, MAX_TEXT_EDIT_BYTES)
                current = initial[0].decode("utf-8") if initial is not None else None
                if action == "add":
                    updated = (current or "") + new
                    count = 0
                else:
                    updated, count, _ = await self.work.run(
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
                    if await self.work.run(_fingerprint, target, MAX_TEXT_EDIT_BYTES) != expected:
                        raise ToolFailure("workspace_file_changed", "File changed during the patch")
                for _, target, updated, _, _ in prepared:
                    await self.work.mutate(self._atomic_write, target, updated.encode("utf-8"))
        return ToolOutput(
            _file_patch_result(
                dry_run=dry_run,
                edits=[
                    {
                        "action": parsed[index][2],
                        "requested_path": display,
                        "canonical_path": self._paths.canonical(target),
                        "size_bytes": len(updated.encode("utf-8")),
                        "replacements": count,
                    }
                    for index, (display, target, updated, count, _) in enumerate(prepared)
                ],
            )
        )

    async def _delete_file(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        resolved = await self._resolve_path(path, directory=False)
        async with self._locks.hold(str(resolved)):
            await self.work.mutate(resolved.unlink)
        return self._file_mutation_result(
            "delete_file",
            requested_path=path,
            resolved_path=resolved,
        )

    async def _delete_folder(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        resolved = await self._resolve_path(path, directory=True)
        _reject_protected_directory_delete(resolved, self._paths.root)
        async with self._locks.hold(str(resolved)):
            await self.work.mutate(shutil.rmtree, resolved)
        return self._file_mutation_result(
            "delete_folder",
            requested_path=path,
            resolved_path=resolved,
        )

    async def _list_dir(self, args: dict[str, Any]) -> ToolOutput:
        path = self._path_arg(args)
        recursive = _bool_arg(args, "recursive", False)
        limit = _int_arg(args, "max_entries", 200, minimum=1, maximum=1000)
        root = await self._resolve_path(path, directory=True)
        return await self.work.run(self._list_dir_sync, root, recursive, limit)

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
        return await self.work.run(
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
        root = await self._resolve_path(_optional_str(args, "path") or ".", directory=True)
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
        return await self.work.run(
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
            initial = await self.work.run(_capture_regular, target, MAX_TEXT_EDIT_BYTES)
            updated = await self.work.run(
                self._edit_notebook_sync, target, index, mode, cell_type, source
            )
            if await self.work.run(_fingerprint, target, MAX_TEXT_EDIT_BYTES) != (
                initial[1] if initial is not None else None
            ):
                raise ToolFailure("workspace_file_changed", "Notebook changed during the edit")
            await self.work.mutate(self._atomic_write, target, updated.encode("utf-8"))
        return self._file_mutation_result(
            "notebook_edit",
            requested_path=path,
            resolved_path=target,
            size_bytes=len(updated.encode("utf-8")),
        )

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

    def _file_mutation_result(
        self,
        operation: str,
        *,
        requested_path: str,
        resolved_path: Path,
        **details: Any,
    ) -> ToolOutput:
        return ToolOutput(
            json.dumps(
                {
                    "ok": True,
                    "operation": operation,
                    "requested_path": requested_path,
                    "canonical_path": self._paths.canonical(resolved_path),
                    **details,
                },
                ensure_ascii=False,
            )
        )

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
                raise ToolFailure("tool_ambiguous_edit", "Fuzzy match candidate limit exceeded")
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


def _file_patch_result(*, dry_run: bool, edits: list[dict[str, Any]]) -> str:
    total_edits = len(edits)
    retained = list(edits)
    while True:
        payload: dict[str, Any] = {
            "ok": True,
            "operation": "apply_patch",
            "dry_run": dry_run,
            "total_edits": total_edits,
            "edits": retained,
        }
        if len(retained) != total_edits:
            payload["omitted_edits"] = total_edits - len(retained)
        encoded = json.dumps(payload, ensure_ascii=False)
        if len(encoded) <= _FILE_RESULT_JSON_MAX_CHARS:
            return encoded
        if not retained:
            raise ValueError("patch summary cannot fit the structured result bound")
        retained.pop()
