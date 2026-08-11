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

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError, WorkspaceError
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
from openctopus_server.workspace.storage import ObjectStream, ObjectUpload, StoredObject
from openctopus_server.workspace.text_edit import apply_text_edit

REST_UPLOAD_MAX_BYTES = 64 * 1024 * 1024
TOOL_MATERIALIZATION_TIMEOUT_SECONDS = 30.0
TOOL_AUTHORIZATION_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class DownloadTicket:
    target: WorkspaceTarget
    relative_path: str


@dataclass(frozen=True, slots=True)
class UploadTicket:
    target: WorkspaceTarget
    relative_path: str
    quota_bytes: int
    max_bytes: int
    user_id: UUID
    display_path: str


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
class ToolReadTicket:
    target: WorkspaceTarget
    relative_path: str
    display_path: str
    suffix: str


@dataclass(frozen=True, slots=True)
class _MaterializedToolFile:
    etag: str
    content: bytes | str
    size: int


@dataclass(frozen=True, slots=True)
class AuthorizedWorkspaceFile:
    target: WorkspaceTarget
    relative_path: str
    metadata: FileMetadata


@dataclass(frozen=True, slots=True)
class TransferPathTicket:
    """Authorized workspace identity retained after the DB transaction closes."""

    user_id: UUID
    display_path: str
    target: WorkspaceTarget
    relative_path: str
    quota_bytes: int


@dataclass(frozen=True, slots=True)
class WorkspaceTransferResult:
    bytes_transferred: int
    sha256: str
    warnings: tuple[str, ...] = ()


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
            await db.commit()
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
            await db.commit()
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
            await db.commit()
            return await self._fs.read(
                resolved.target,
                resolved.relative_path,
                offset=offset,
                length=length,
            )

    async def read_personal_for_prompt(
        self,
        *,
        user_id: UUID,
        path: str,
        offset: int = 0,
        length: int = 0,
    ) -> bytes:
        async with self._fs.materialization_slot():
            return await self._fs.read(
                WorkspaceTarget.personal(user_id),
                path,
                offset=offset,
                length=length,
            )

    async def read_with_metadata(
        self, db: AsyncSession, *, user_id: UUID, path: str
    ) -> StoredObject:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.materialization_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            await db.commit()
            return await self._fs.read_with_metadata(resolved.target, resolved.relative_path)

    async def authorize_tool_read(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> ToolReadTicket:
        try:
            async with asyncio.timeout(TOOL_AUTHORIZATION_TIMEOUT_SECONDS):
                resolved = await self._preflight(db, user_id=user_id, path=path)
        except TimeoutError as exc:
            raise ToolError(
                ErrorCode.TOOL_EXEC_TIMEOUT,
                "Workspace file authorization timed out after 5 seconds",
            ) from exc
        return ToolReadTicket(
            target=resolved.target,
            relative_path=resolved.relative_path,
            display_path=path,
            suffix=PurePosixPath(path).suffix.lower(),
        )

    async def read_for_tool(
        self,
        ticket: ToolReadTicket,
        *,
        user_id: UUID,
        offset: int,
        limit: int,
        pages: str | None,
        parser: DocumentParser,
        unchanged_etag: Callable[[str], bool],
    ) -> ToolFileRead | None:
        is_document = ticket.suffix in {".pdf", ".docx", ".xlsx", ".pptx"}
        if is_document:
            async with parser.admit(user_id) as conversion:
                materialized = await self._materialize_tool_file(
                    ticket,
                    offset=offset,
                    limit=limit,
                    unchanged_etag=unchanged_etag,
                    document=True,
                )
                if materialized is None:
                    return None
                if isinstance(materialized.content, str):
                    return ToolFileRead(
                        etag=materialized.etag,
                        content=materialized.content,
                        size=materialized.size,
                    )
                content = await conversion.parse(
                    ticket.display_path,
                    materialized.content,
                    pages=pages,
                )
                return ToolFileRead(
                    etag=materialized.etag,
                    content=content,
                    size=materialized.size,
                )
        materialized = await self._materialize_tool_file(
            ticket,
            offset=offset,
            limit=limit,
            unchanged_etag=unchanged_etag,
            document=False,
        )
        if materialized is None:
            return None
        if isinstance(materialized.content, str):
            return ToolFileRead(
                etag=materialized.etag,
                content=materialized.content,
                size=materialized.size,
            )
        rendered_content = await _run_cpu(
            render_file_content,
            ticket.display_path,
            materialized.content,
            offset=offset,
            limit=limit,
            pages=pages,
        )
        return ToolFileRead(
            etag=materialized.etag,
            content=rendered_content,
            size=materialized.size,
        )

    async def _materialize_tool_file(
        self,
        ticket: ToolReadTicket,
        *,
        offset: int,
        limit: int,
        unchanged_etag: Callable[[str], bool],
        document: bool,
    ) -> _MaterializedToolFile | None:
        try:
            async with asyncio.timeout(TOOL_MATERIALIZATION_TIMEOUT_SECONDS):
                async with self._fs.materialization_slot():
                    stream = await self._fs.open_stream(ticket.target, ticket.relative_path)
                    try:
                        if unchanged_etag(stream.etag):
                            return None
                        if stream.size > MAX_EDIT_BYTES:
                            if document:
                                raise WorkspaceError(
                                    ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
                                    "Workspace document exceeds the 8 MiB materialization limit",
                                )
                            content = await render_streamed_text(
                                stream.read,
                                offset=offset,
                                limit=limit,
                            )
                            return _MaterializedToolFile(
                                etag=stream.etag,
                                content=content,
                                size=stream.size,
                            )
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
                        return _MaterializedToolFile(
                            etag=stream.etag,
                            content=b"".join(chunks),
                            size=stream.size,
                        )
                    finally:
                        await stream.aclose()
        except TimeoutError as exc:
            raise ToolError(
                ErrorCode.TOOL_EXEC_TIMEOUT,
                "Workspace file materialization timed out after 30 seconds",
            ) from exc

    async def list_dir(self, db: AsyncSession, *, user_id: UUID, path: str) -> list[DirectoryEntry]:
        await self._preflight(db, user_id=user_id, path=path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
            await db.commit()
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
        scan_limit: int = 10_000,
    ) -> DirectoryPage:
        normalized_path = _search_path(path)
        await self._preflight(db, user_id=user_id, path=normalized_path)
        async with self._fs.file_operation_slot():
            resolved = await self._resolver.resolve(
                db,
                user_id=user_id,
                path=normalized_path,
            )
            await db.commit()
            return await self._fs.list_dir_page(
                resolved.target,
                resolved.relative_path,
                limit=limit,
                offset=offset,
                scan_limit=scan_limit,
                include_noise_directories=include_noise_directories,
            )

    async def list_personal_for_prompt(
        self,
        *,
        user_id: UUID,
        path: str,
        limit: int,
        offset: int = 0,
        include_noise_directories: bool = False,
        scan_limit: int = 10_000,
    ) -> DirectoryPage:
        normalized_path = _search_path(path)
        async with self._fs.file_operation_slot():
            return await self._fs.list_dir_page(
                WorkspaceTarget.personal(user_id),
                normalized_path,
                limit=limit,
                offset=offset,
                scan_limit=scan_limit,
                include_noise_directories=include_noise_directories,
            )

    async def usage(self, db: AsyncSession, *, user_id: UUID, path: str = "") -> int:
        resolved = await self._preflight(db, user_id=user_id, path=path)
        return await self._fs.usage(resolved.target)

    async def authorized_usage(self, target: WorkspaceTarget) -> int:
        return await self._fs.usage(target)

    async def personal_usages(self, user_ids: list[UUID]) -> list[int]:
        usages: list[int] = []
        for start in range(0, len(user_ids), 4):
            tasks = [
                asyncio.create_task(self._fs.usage(WorkspaceTarget.personal(user_id)))
                for user_id in user_ids[start : start + 4]
            ]
            try:
                usages.extend(await asyncio.gather(*tasks))
            except BaseException:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
        return usages

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
            await db.commit()
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
            await db.commit()
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
            await db.commit()
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

    async def authorize_upload(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> UploadTicket:
        resolved = await self._resolver.resolve(db, user_id=user_id, path=path)
        return UploadTicket(
            target=resolved.target,
            relative_path=resolved.relative_path,
            quota_bytes=resolved.quota_bytes,
            max_bytes=min(REST_UPLOAD_MAX_BYTES, resolved.quota_bytes * 4 // 5),
            user_id=user_id,
            display_path=path,
        )

    async def authorize_transfer_source(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> TransferPathTicket:
        resolved = await self._preflight(db, user_id=user_id, path=path)
        return TransferPathTicket(
            user_id=user_id,
            display_path=path,
            target=resolved.target,
            relative_path=resolved.relative_path,
            quota_bytes=resolved.quota_bytes,
        )

    async def authorize_transfer_destination(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        path: str,
    ) -> TransferPathTicket:
        resolved = await self._preflight(db, user_id=user_id, path=path)
        return TransferPathTicket(
            user_id=user_id,
            display_path=path,
            target=resolved.target,
            relative_path=resolved.relative_path,
            quota_bytes=resolved.quota_bytes,
        )

    async def open_transfer_source(self, ticket: TransferPathTicket) -> ObjectStream:
        return await self._fs.open_stream(ticket.target, ticket.relative_path)

    async def delete_transfer_source(
        self,
        ticket: TransferPathTicket,
        *,
        if_match: str | None = None,
    ) -> None:
        try:
            await self._fs.delete_file(
                ticket.target,
                ticket.relative_path,
                if_match=if_match,
            )
        finally:
            self._invalidate_skills(ticket.user_id, ticket.display_path)

    async def begin_transfer_upload(
        self,
        ticket: TransferPathTicket,
        *,
        size: int,
    ) -> ObjectUpload:
        sink, _temporary_object = await self._fs.begin_transfer_upload(
            ticket.target,
            ticket.relative_path,
            size=size,
            quota_bytes=ticket.quota_bytes,
        )
        return sink

    async def commit_transfer_upload(
        self,
        ticket: TransferPathTicket,
        sink: ObjectUpload,
        *,
        size: int,
        sha256: str,
    ) -> None:
        del sha256
        try:
            await self._fs.commit_uploaded_object(
                ticket.target,
                ticket.relative_path,
                sink.object_name,
                size=size,
                quota_bytes=ticket.quota_bytes,
            )
        finally:
            self._invalidate_skills(ticket.user_id, ticket.display_path)

    async def transfer_server_to_server(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        src_path: str,
        dst_path: str,
        mode: str,
    ) -> WorkspaceTransferResult:
        source = await self.authorize_transfer_source(db, user_id=user_id, path=src_path)
        destination = await self.authorize_transfer_destination(
            db,
            user_id=user_id,
            path=dst_path,
        )
        await db.commit()
        # Authorization is complete.  Do not retain a PostgreSQL connection
        # while the transfer streams through object storage or waits for the
        # source-delete cleanup.
        await db.close()
        try:
            transferred, digest, warnings = await self._fs.transfer_server_to_server(
                source.target,
                source.relative_path,
                destination.target,
                destination.relative_path,
                quota_bytes=destination.quota_bytes,
                mode=mode,
            )
            return WorkspaceTransferResult(transferred, digest, warnings)
        finally:
            self._invalidate_skills(user_id, src_path)
            self._invalidate_skills(user_id, dst_path)

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
                await db.commit()
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

    async def write_authorized_upload(
        self,
        ticket: UploadTicket,
        *,
        data: bytes,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> FileMetadata:
        await _run_cpu(
            _validate_skill_write,
            ticket.display_path,
            data,
            user_id=ticket.user_id,
        )
        try:
            return await self._fs.write_collected_upload(
                ticket.target,
                ticket.relative_path,
                data,
                quota_bytes=ticket.quota_bytes,
                if_match=if_match,
                if_none_match=if_none_match,
            )
        finally:
            self._invalidate_skills(ticket.user_id, ticket.display_path)

    async def commit_authorized_upload_object(
        self,
        ticket: UploadTicket,
        temporary_object: str,
        *,
        size: int,
    ) -> FileMetadata:
        try:
            return await self._fs.commit_uploaded_object(
                ticket.target,
                ticket.relative_path,
                temporary_object,
                size=size,
                quota_bytes=ticket.quota_bytes,
            )
        finally:
            self._invalidate_skills(ticket.user_id, ticket.display_path)

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
                await db.commit()
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
                await db.commit()
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
                await db.commit()
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
            await db.commit()
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
                await db.commit()
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
    return await await_future_cancellation_safe(worker)


def _merge_scan_truncation[T](page: ResultPage[T], truncated: bool) -> ResultPage[T]:
    if not truncated:
        return page
    return ResultPage(items=page.items, next_offset=None, truncated=True)
