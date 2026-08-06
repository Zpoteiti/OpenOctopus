from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.file_content import (
    DocumentParser,
    render_file_content,
    render_streamed_text,
)
from openctopus_server.workspace.fs import (
    MAX_EDIT_BYTES,
    DirectoryEntry,
    DirectoryPage,
    FileMetadata,
    FileTransform,
    WorkspaceFS,
    WorkspaceTarget,
    get_workspace_fs,
)
from openctopus_server.workspace.resolver import ResolvedWorkspacePath, WorkspacePathResolver
from openctopus_server.workspace.search import (
    MAX_GREP_BYTES,
    MAX_GREP_RESULT_CHARS,
    GrepCount,
    GrepItem,
    ResultPage,
    SearchEntry,
    SearchObject,
    grep_result_chars,
    list_recursive,
)
from openctopus_server.workspace.search import (
    find_files as filter_files,
)
from openctopus_server.workspace.search import (
    grep_files as search_contents,
)
from openctopus_server.workspace.skills import (
    SkillsCache,
    get_skills_cache,
    is_skill_manifest,
    is_under_skills,
    validate_skill_manifest,
)
from openctopus_server.workspace.storage import ObjectStream, StoredObject
from openctopus_server.workspace.text_edit import apply_text_edit

REST_UPLOAD_MAX_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class DownloadTicket:
    target: WorkspaceTarget
    relative_path: str


@dataclass(frozen=True, slots=True)
class PatchEdit:
    path: str
    action: Literal["replace", "add"]
    old_text: str | None
    new_text: str


@dataclass(frozen=True, slots=True)
class PatchEditResult:
    path: str
    action: Literal["replace", "add"]
    size: int
    etag: str
    created: bool
    replacements: int


@dataclass(frozen=True, slots=True)
class ToolFileRead:
    etag: str
    content: str | list[dict[str, Any]]
    size: int


@dataclass(frozen=True, slots=True)
class AuthorizedWorkspaceFile:
    target: WorkspaceTarget
    relative_path: str
    metadata: FileMetadata


class WorkspaceService:
    """Authorized virtual-path façade for REST handlers and agent tools."""

    def __init__(
        self, workspace_fs: WorkspaceFS, *, skills_cache: SkillsCache | None = None
    ) -> None:
        self._fs = workspace_fs
        self._resolver = WorkspacePathResolver()
        self._skills_cache = skills_cache or get_skills_cache()

    async def stat(self, db: AsyncSession, *, user_id: UUID, path: str) -> FileMetadata:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.stat(resolved.target, resolved.relative_path)

    async def resolve_delivery_file(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> AuthorizedWorkspaceFile:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            metadata = await self._fs.stat(resolved.target, resolved.relative_path)
            return AuthorizedWorkspaceFile(
                target=resolved.target,
                relative_path=resolved.relative_path,
                metadata=metadata,
            )

    async def read(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        offset: int = 0,
        length: int = 0,
    ) -> bytes:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.materialization_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.read(
                resolved.target,
                resolved.relative_path,
                offset=offset,
                length=length,
            )

    async def read_with_metadata(
        self, db: AsyncSession, *, user_id: UUID, path: str
    ) -> StoredObject:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.materialization_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.read_with_metadata(resolved.target, resolved.relative_path)

    async def read_for_tool(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        offset: int,
        limit: int,
        pages: str | None,
        parser: DocumentParser,
    ) -> ToolFileRead:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.materialization_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            metadata = await self._fs.stat(resolved.target, resolved.relative_path)
            suffix = PurePosixPath(path).suffix.lower()
            if metadata.size > MAX_EDIT_BYTES:
                if suffix in {".pdf", ".docx", ".xlsx", ".pptx"}:
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
                        "Workspace document exceeds the 8 MiB materialization limit",
                    )
                stream = await self._fs.open_stream(resolved.target, resolved.relative_path)
                try:
                    streamed_content = await render_streamed_text(
                        stream.read,
                        offset=offset,
                        limit=limit,
                    )
                    return ToolFileRead(
                        etag=stream.etag,
                        content=streamed_content,
                        size=stream.size,
                    )
                finally:
                    await stream.aclose()
            stream = await self._fs.open_stream(resolved.target, resolved.relative_path)
            try:
                chunks: list[bytes] = []
                collected = 0
                while chunk := await stream.read():
                    collected += len(chunk)
                    if collected > MAX_EDIT_BYTES:
                        raise WorkspaceError(
                            ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
                            "Workspace file exceeds the 8 MiB materialization limit",
                        )
                    chunks.append(chunk)
                data = b"".join(chunks)
                if suffix in {".pdf", ".docx", ".xlsx", ".pptx"}:
                    materialized_content: str | list[dict[str, Any]] = await parser.parse(
                        path,
                        data,
                        pages=pages,
                    )
                else:
                    materialized_content = await _run_cpu(
                        render_file_content,
                        path,
                        data,
                        offset=offset,
                        limit=limit,
                        pages=pages,
                    )
                return ToolFileRead(
                    etag=stream.etag,
                    content=materialized_content,
                    size=stream.size,
                )
            finally:
                await stream.aclose()

    async def list_dir(self, db: AsyncSession, *, user_id: UUID, path: str) -> list[DirectoryEntry]:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            return await self._fs.list_dir(resolved.target, resolved.relative_path)

    async def list_dir_page(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        limit: int,
        offset: int = 0,
        include_noise_directories: bool = False,
    ) -> DirectoryPage:
        normalized_path = _search_path(path)
        await self._preflight(db, user_id=user_id, path=normalized_path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(
                db,
                user_id=user_id,
                path=normalized_path,
            )
            return await self._fs.list_dir_page(
                resolved.target,
                resolved.relative_path,
                limit=limit,
                offset=offset,
                include_noise_directories=include_noise_directories,
            )

    async def usage(self, db: AsyncSession, *, user_id: UUID, path: str = "") -> int:
        resolved = await self._preflight(db, user_id=user_id, path=path)
        return await self._fs.usage(resolved.target)

    async def authorized_usage(self, target: WorkspaceTarget) -> int:
        return await self._fs.usage(target)

    async def authorize_download(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> DownloadTicket:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return DownloadTicket(
            target=resolved.target,
            relative_path=resolved.relative_path,
        )

    async def open_download(self, ticket: DownloadTicket) -> ObjectStream:
        return await self._fs.open_stream(ticket.target, ticket.relative_path)

    async def list_authorized(self, ticket: DownloadTicket) -> list[DirectoryEntry]:
        async with self._fs.file_operation_slot():
            return await self._fs.list_dir(ticket.target, ticket.relative_path)

    async def list_authorized_page(
        self,
        ticket: DownloadTicket,
        *,
        limit: int,
        offset: int,
    ) -> DirectoryPage:
        async with self._fs.file_operation_slot():
            return await self._fs.list_dir_page(
                ticket.target,
                ticket.relative_path,
                limit=limit,
                offset=offset,
            )

    async def list_recursive(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        limit: int,
        offset: int,
    ) -> ResultPage[SearchEntry]:
        normalized_path = _search_path(path)
        await self._preflight(db, user_id=user_id, path=normalized_path)
        async with self._fs.heavy_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=normalized_path)
            objects, truncated = await self._fs.scan_objects(
                resolved.target,
                resolved.relative_path,
            )
            if (
                resolved.relative_path
                and len(objects) == 1
                and objects[0].path == resolved.relative_path
            ):
                raise WorkspaceError(
                    ErrorCode.TOOL_NOT_A_DIRECTORY,
                    "Workspace path is not a directory",
                )
            page = await _run_cpu(
                list_recursive,
                objects,
                root=resolved.relative_path,
                limit=limit,
                offset=offset,
            )
        return _merge_scan_truncation(page, truncated)

    async def find_files(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        query: str,
        glob: str | None,
        file_type: str | None,
        include_dirs: bool,
        sort: Literal["path", "modified"],
        limit: int,
        offset: int,
    ) -> ResultPage[SearchEntry]:
        normalized_path = _search_path(path)
        filter_files(
            (),
            root=normalized_path,
            query=query,
            glob=glob,
            file_type=file_type,
            include_dirs=include_dirs,
            sort=sort,
            limit=limit,
            offset=offset,
        )
        await self._preflight(db, user_id=user_id, path=normalized_path)
        async with self._fs.heavy_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=normalized_path)
            objects, truncated = await self._fs.scan_objects(
                resolved.target,
                resolved.relative_path,
            )
            page = await _run_cpu(
                filter_files,
                objects,
                root=resolved.relative_path,
                query=query,
                glob=glob,
                file_type=file_type,
                include_dirs=include_dirs,
                sort=sort,
                limit=limit,
                offset=offset,
            )
        return _merge_scan_truncation(page, truncated)

    async def grep(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        pattern: str,
        path: str,
        glob: str | None,
        file_type: str | None,
        case_insensitive: bool,
        fixed_strings: bool,
        output_mode: Literal["content", "files_with_matches", "count"],
        context_before: int,
        context_after: int,
        limit: int,
        offset: int,
    ) -> ResultPage[GrepItem]:
        search_contents(
            (),
            pattern=pattern,
            root=_search_path(path),
            glob=glob,
            file_type=file_type,
            case_insensitive=case_insensitive,
            fixed_strings=fixed_strings,
            output_mode=output_mode,
            context_before=context_before,
            context_after=context_after,
            limit=limit,
            offset=offset,
        )
        normalized_path = _search_path(path)
        await self._preflight(db, user_id=user_id, path=normalized_path)
        async with self._fs.heavy_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=normalized_path)
            objects, truncated = await self._fs.scan_objects(
                resolved.target,
                resolved.relative_path,
            )
            page = await self._grep_objects(
                resolved.target,
                objects,
                root=resolved.relative_path,
                pattern=pattern,
                glob=glob,
                file_type=file_type,
                case_insensitive=case_insensitive,
                fixed_strings=fixed_strings,
                output_mode=output_mode,
                context_before=context_before,
                context_after=context_after,
                limit=limit,
                offset=offset,
            )
        return _merge_scan_truncation(page, truncated)

    async def _grep_objects(
        self,
        target: WorkspaceTarget,
        objects: tuple[SearchObject, ...],
        *,
        root: str,
        pattern: str,
        glob: str | None,
        file_type: str | None,
        case_insensitive: bool,
        fixed_strings: bool,
        output_mode: Literal["content", "files_with_matches", "count"],
        context_before: int,
        context_after: int,
        limit: int,
        offset: int,
    ) -> ResultPage[GrepItem]:
        effective_limit = 10_000 if limit == 0 else limit
        results: list[GrepItem] = []
        retained_chars = 0
        result_memory_has_more = False
        remaining_offset = offset
        manifests: list[SearchObject] = []
        manifest_bytes = 0
        eligible_page = await _run_cpu(
            filter_files,
            objects,
            root=root,
            glob=glob,
            file_type=file_type,
            limit=10_000,
        )
        eligible_paths = {item.path for item in eligible_page.items}
        for item in sorted(objects, key=lambda candidate: candidate.path):
            if item.path.endswith("/.gitignore") or item.path == ".gitignore":
                if item.size <= 256 * 1024 and manifest_bytes + item.size <= 2 * 1024 * 1024:
                    loaded = await self._fs.read_search_object(
                        target,
                        item,
                        max_bytes=256 * 1024,
                    )
                    if loaded.content is not None:
                        manifests.append(loaded)
                        manifest_bytes += item.size
                continue
            if item.path not in eligible_paths or item.size > MAX_GREP_BYTES:
                continue
            loaded = await self._fs.read_search_object(
                target,
                item,
                max_bytes=MAX_GREP_BYTES,
            )
            if loaded.content is None:
                continue
            if output_mode == "content":
                counts = await _run_cpu(
                    search_contents,
                    (*manifests, loaded),
                    pattern=pattern,
                    root=root,
                    glob=glob,
                    file_type=file_type,
                    case_insensitive=case_insensitive,
                    fixed_strings=fixed_strings,
                    output_mode="count",
                    context_before=context_before,
                    context_after=context_after,
                    limit=1,
                )
                count_item = counts.items[-1] if counts.items else None
                match_count = count_item.count if isinstance(count_item, GrepCount) else 0
                if remaining_offset >= match_count:
                    remaining_offset -= match_count
                    continue
                page = await _run_cpu(
                    search_contents,
                    (*manifests, loaded),
                    pattern=pattern,
                    root=root,
                    glob=glob,
                    file_type=file_type,
                    case_insensitive=case_insensitive,
                    fixed_strings=fixed_strings,
                    output_mode="content",
                    context_before=context_before,
                    context_after=context_after,
                    limit=effective_limit + 1 - len(results),
                    offset=remaining_offset,
                )
                remaining_offset = 0
                for result in page.items:
                    size = grep_result_chars(result)
                    if retained_chars + size > MAX_GREP_RESULT_CHARS:
                        result_memory_has_more = True
                        break
                    results.append(result)
                    retained_chars += size
                if page.next_offset is not None:
                    result_memory_has_more = True
            else:
                page = await _run_cpu(
                    search_contents,
                    (*manifests, loaded),
                    pattern=pattern,
                    root=root,
                    glob=glob,
                    file_type=file_type,
                    case_insensitive=case_insensitive,
                    fixed_strings=fixed_strings,
                    output_mode=output_mode,
                    context_before=context_before,
                    context_after=context_after,
                    limit=1,
                )
                if not page.items:
                    continue
                if remaining_offset:
                    remaining_offset -= 1
                    continue
                result = page.items[0]
                size = grep_result_chars(result)
                if retained_chars + size > MAX_GREP_RESULT_CHARS:
                    result_memory_has_more = True
                else:
                    results.append(result)
                    retained_chars += size
            if result_memory_has_more or len(results) > effective_limit:
                break
        has_more = result_memory_has_more or len(results) > effective_limit
        returned_count = min(len(results), effective_limit)
        return ResultPage(
            items=tuple(results[:returned_count]),
            next_offset=offset + returned_count if has_more else None,
            truncated=False,
        )

    async def upload_limit(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> int:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return min(REST_UPLOAD_MAX_BYTES, resolved.quota_bytes * 4 // 5)

    def collect_upload(
        self,
        chunks: AsyncIterator[bytes],
        *,
        max_bytes: int,
    ) -> AbstractAsyncContextManager[bytes]:
        return self._fs.collect_upload(chunks, max_bytes=max_bytes)

    async def write(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        data: bytes,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> FileMetadata:
        if len(data) > MAX_EDIT_BYTES:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
                "Workspace file exceeds the 8 MiB server edit limit",
            )
        await self._preflight(db, user_id=user_id, path=path)
        try:
            async with self._fs.materialization_slot():
                await _run_cpu(_validate_skill_write, path, data, user_id=user_id)
                resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
                return await self._fs.write_collected_upload(
                    resolved.target,
                    resolved.relative_path,
                    data,
                    quota_bytes=resolved.quota_bytes,
                    if_match=if_match,
                    if_none_match=if_none_match,
                )
        finally:
            self._invalidate_skills(user_id, path)

    async def write_collected_upload(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        data: bytes,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> FileMetadata:
        await _run_cpu(_validate_skill_write, path, data, user_id=user_id)
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        try:
            return await self._fs.write_collected_upload(
                resolved.target,
                resolved.relative_path,
                data,
                quota_bytes=resolved.quota_bytes,
                if_match=if_match,
                if_none_match=if_none_match,
            )
        finally:
            self._invalidate_skills(user_id, path)

    async def edit(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        transform: Callable[[bytes], bytes],
        if_match: str | None = None,
    ) -> FileMetadata:
        def validated_transform(data: bytes) -> bytes:
            updated = transform(data)
            _validate_skill_write(path, updated, user_id=user_id)
            return updated

        await self._preflight(db, user_id=user_id, path=path)
        try:
            async with self._fs.materialization_slot():
                resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
                return await self._fs.edit_materialized(
                    resolved.target,
                    resolved.relative_path,
                    validated_transform,
                    quota_bytes=resolved.quota_bytes,
                    if_match=if_match,
                )
        finally:
            self._invalidate_skills(user_id, path)

    async def edit_text(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        old_text: str,
        new_text: str,
        occurrence: int | None = None,
        replace_all: bool = False,
        line_hint: int | None = None,
        expected_replacements: int | None = None,
        if_match: str | None = None,
    ) -> tuple[FileMetadata, int]:
        replacements = 0

        def transform(data: bytes | None) -> bytes:
            nonlocal replacements
            text = None
            if data is not None:
                try:
                    text = data.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise WorkspaceError(
                        ErrorCode.WORKSPACE_INVALID_REQUEST,
                        "Workspace file is not UTF-8 text",
                    ) from exc
            result = apply_text_edit(
                text,
                old_text=old_text,
                new_text=new_text,
                replace_all=replace_all,
                occurrence=occurrence,
                line_hint=line_hint,
                expected_replacements=expected_replacements,
            )
            replacements = result.replacements
            updated = result.text.encode("utf-8")
            _validate_skill_write(path, updated, user_id=user_id)
            return updated

        await self._preflight(db, user_id=user_id, path=path)
        try:
            async with self._fs.materialization_slot():
                resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
                metadata = await self._fs.edit_optional_materialized(
                    resolved.target,
                    resolved.relative_path,
                    transform,
                    quota_bytes=resolved.quota_bytes,
                    if_match=if_match,
                )
            return metadata, replacements
        finally:
            self._invalidate_skills(user_id, path)

    async def delete_file(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
        if_match: str | None = None,
    ) -> None:
        await self._preflight(db, user_id=user_id, path=path)
        try:
            async with self._fs.file_operation_slot():
                resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
                await self._fs.delete_file(
                    resolved.target,
                    resolved.relative_path,
                    if_match=if_match,
                )
        finally:
            self._invalidate_skills(user_id, path)

    async def apply_patch(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        edits: tuple[PatchEdit, ...],
        dry_run: bool,
    ) -> tuple[PatchEditResult, ...]:
        if not 1 <= len(edits) <= 20:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_INVALID_REQUEST,
                "Workspace patch must contain between 1 and 20 edits",
            )
        for edit in edits:
            await self._resolver.resolve(db, user_id=user_id, path=edit.path)
        await db.commit()
        async with self._fs.materialization_slot():
            resolved = [
                await self._resolver.resolve(db, user_id=user_id, path=edit.path) for edit in edits
            ]
            replacement_counts = [0] * len(edits)
            transforms: list[FileTransform] = []
            for index, (edit, target) in enumerate(zip(edits, resolved, strict=True)):

                def transform(
                    data: bytes | None, *, index: int = index, edit: PatchEdit = edit
                ) -> bytes:
                    text = _decode_optional_text(data)
                    if edit.action == "add":
                        updated = (text or "") + edit.new_text
                    else:
                        if text is None or not edit.old_text:
                            raise WorkspaceError(
                                ErrorCode.TOOL_NO_MATCH, "Text to replace was not found"
                            )
                        matches = text.count(edit.old_text)
                        if matches == 0:
                            raise WorkspaceError(
                                ErrorCode.TOOL_NO_MATCH, "Text to replace was not found"
                            )
                        if matches > 1:
                            raise WorkspaceError(
                                ErrorCode.TOOL_AMBIGUOUS_EDIT,
                                "Text to replace appears more than once",
                            )
                        updated = text.replace(edit.old_text, edit.new_text, 1)
                        replacement_counts[index] = 1
                    encoded = updated.encode("utf-8")
                    _validate_skill_write(edit.path, encoded, user_id=user_id)
                    return encoded

                transforms.append(
                    FileTransform(
                        target=target.target,
                        relative_path=target.relative_path,
                        quota_bytes=target.quota_bytes,
                        transform=transform,
                    )
                )
            try:
                metadata = await self._fs.apply_transforms_admitted(
                    tuple(transforms),
                    dry_run=dry_run,
                )
            finally:
                if not dry_run:
                    for edit in edits:
                        self._invalidate_skills(user_id, edit.path)
        return tuple(
            PatchEditResult(
                path=edit.path,
                action=edit.action,
                size=item.size,
                etag=item.etag,
                created=item.created,
                replacements=replacement_counts[index],
            )
            for index, (edit, item) in enumerate(zip(edits, metadata, strict=True))
        )

    async def delete_folder(self, db: AsyncSession, *, user_id: UUID, path: str) -> None:
        await self._preflight(db, user_id=user_id, path=path)
        try:
            async with self._fs.file_operation_slot():
                resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
                await self._fs.delete_folder(resolved.target, resolved.relative_path)
        finally:
            self._invalidate_skills(user_id, path)

    def _invalidate_skills(self, user_id: UUID, path: str) -> None:
        if is_under_skills(_personal_relative_path(path, user_id)):
            self._skills_cache.invalidate(user_id)

    async def _preflight(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> ResolvedWorkspacePath:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        await db.commit()
        return resolved


def get_workspace_service(
    workspace_fs: Annotated[WorkspaceFS, Depends(get_workspace_fs)],
) -> WorkspaceService:
    return WorkspaceService(workspace_fs)


def _validate_skill_write(path: str, data: bytes, *, user_id: UUID) -> None:
    relative_path = _personal_relative_path(path, user_id)
    if is_skill_manifest(relative_path):
        validate_skill_manifest(relative_path, data)


def _decode_optional_text(data: bytes | None) -> str | None:
    if data is None:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_INVALID_REQUEST,
            "Workspace file is not UTF-8 text",
        ) from exc


def _search_path(path: str) -> str:
    return "" if path in {"", "."} else path


def _personal_relative_path(path: str, user_id: UUID) -> str:
    if not path.startswith("/"):
        return path
    return path.removeprefix(f"/{user_id}/") if path.startswith(f"/{user_id}/") else ""


async def _run_cpu[T](function: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    worker = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        try:
            await worker
        except Exception:
            pass
        raise


def _merge_scan_truncation[T](page: ResultPage[T], truncated: bool) -> ResultPage[T]:
    if not truncated:
        return page
    return ResultPage(items=page.items, next_offset=None, truncated=True)
