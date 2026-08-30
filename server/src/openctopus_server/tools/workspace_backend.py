from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError
from openctopus_server.tools.base import ToolContext, ToolResult
from openctopus_server.tools.file_results import (
    canonical_server_path,
    file_mutation_result,
    file_patch_result,
)
from openctopus_server.workspace.file_content import DocumentParser
from openctopus_server.workspace.fs import DirectoryEntry
from openctopus_server.workspace.notebook import CellType, EditMode, edit_notebook
from openctopus_server.workspace.search import GrepContentMatch, GrepCount, SearchEntry
from openctopus_server.workspace.service import PatchEdit, WorkspaceService


class WorkspaceToolDispatcher:
    def __init__(
        self,
        engine: AsyncEngine,
        service: WorkspaceService,
        *,
        document_parser: DocumentParser,
    ) -> None:
        self._engine = engine
        self._service = service
        self._parser = document_parser

    async def __call__(
        self,
        name: str,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            if name == "read_file":
                return await self._read_file(db, args, ctx)
            if name == "write_file":
                metadata = await self._service.write(
                    db,
                    user_id=ctx.user_id,
                    path=cast(str, args["path"]),
                    data=cast(str, args["content"]).encode("utf-8"),
                )
                await db.commit()
                requested_path = cast(str, args["path"])
                return ToolResult(
                    content=file_mutation_result(
                        "write_file",
                        device="server",
                        requested_path=requested_path,
                        canonical_path=canonical_server_path(requested_path),
                        bytes_written=metadata.size,
                    )
                )
            if name == "edit_file":
                metadata, replacements = await self._service.edit_text(
                    db,
                    user_id=ctx.user_id,
                    path=cast(str, args["path"]),
                    old_text=cast(str, args["old_text"]),
                    new_text=cast(str, args["new_text"]),
                    replace_all=cast(bool, args["replace_all"]),
                    occurrence=cast(int | None, args["occurrence"]),
                    line_hint=cast(int | None, args["line_hint"]),
                    expected_replacements=cast(int | None, args["expected_replacements"]),
                )
                await db.commit()
                requested_path = cast(str, args["path"])
                return ToolResult(
                    content=file_mutation_result(
                        "edit_file",
                        device="server",
                        requested_path=requested_path,
                        canonical_path=canonical_server_path(requested_path),
                        replacements=replacements,
                        size_bytes=metadata.size,
                    )
                )
            if name == "apply_patch":
                return await self._apply_patch(db, args, ctx)
            if name == "delete_file":
                await self._service.delete_file(
                    db,
                    user_id=ctx.user_id,
                    path=cast(str, args["path"]),
                )
                await db.commit()
                requested_path = cast(str, args["path"])
                return ToolResult(
                    content=file_mutation_result(
                        "delete_file",
                        device="server",
                        requested_path=requested_path,
                        canonical_path=canonical_server_path(requested_path),
                    )
                )
            if name == "delete_folder":
                await self._service.delete_folder(
                    db,
                    user_id=ctx.user_id,
                    path=cast(str, args["path"]),
                )
                await db.commit()
                requested_path = cast(str, args["path"])
                return ToolResult(
                    content=file_mutation_result(
                        "delete_folder",
                        device="server",
                        requested_path=requested_path,
                        canonical_path=canonical_server_path(requested_path),
                    )
                )
            if name == "list_dir":
                return await self._list_dir(db, args, ctx)
            if name == "find_files":
                return await self._find_files(db, args, ctx)
            if name == "grep":
                return await self._grep(db, args, ctx)
            if name == "notebook_edit":
                return await self._notebook_edit(db, args, ctx)
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, f"Unknown workspace tool: {name}")

    async def _read_file(
        self,
        db: AsyncSession,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        path = cast(str, args["path"])
        offset = cast(int, args["offset"])
        limit = cast(int, args["limit"])
        pages = cast(str | None, args["pages"])
        ticket = await self._service.authorize_tool_read(
            db,
            user_id=ctx.user_id,
            path=path,
        )
        await db.commit()

        result = await self._service.read_for_tool(
            ticket,
            user_id=ctx.user_id,
            offset=offset,
            limit=limit,
            pages=pages,
            parser=self._parser,
        )
        return ToolResult(content=result.content)

    async def _apply_patch(
        self,
        db: AsyncSession,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        edits = tuple(
            PatchEdit(
                path=cast(str, item["path"]),
                action=cast(Any, item["action"]),
                old_text=cast(str | None, item["old_text"]),
                new_text=cast(str, item["new_text"]),
            )
            for item in cast(list[dict[str, Any]], args["edits"])
        )
        dry_run = cast(bool, args["dry_run"])
        results = await self._service.apply_patch(
            db,
            user_id=ctx.user_id,
            edits=edits,
            dry_run=dry_run,
        )
        await db.commit()
        return ToolResult(
            content=file_patch_result(
                device="server",
                dry_run=dry_run,
                edits=[
                    {
                        "action": item.action,
                        "requested_path": item.path,
                        "canonical_path": canonical_server_path(item.path),
                        "size_bytes": item.size,
                        "replacements": item.replacements,
                    }
                    for item in results
                ],
            )
        )

    async def _list_dir(
        self,
        db: AsyncSession,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        path = cast(str, args["path"])
        limit = cast(int, args["max_entries"])
        recursive = cast(bool, args["recursive"])
        items: Sequence[DirectoryEntry | SearchEntry]
        if recursive:
            recursive_page = await self._service.list_recursive(
                db,
                user_id=ctx.user_id,
                path=path,
                limit=limit,
                offset=0,
            )
            items = recursive_page.items
            next_offset = recursive_page.next_offset
            truncated = recursive_page.truncated
        else:
            directory_page = await self._service.list_dir_page(
                db,
                user_id=ctx.user_id,
                path=path,
                limit=limit,
            )
            items = directory_page.items
            next_offset = directory_page.next_offset
            truncated = directory_page.truncated
        lines = []
        for entry in items:
            rendered = _virtual_path(path, entry.path)
            if recursive:
                lines.append(f"{rendered}/" if entry.is_directory else rendered)
            else:
                lines.append(f"{'📁' if entry.is_directory else '📄'} {rendered}")
        if next_offset is not None or truncated:
            lines.append(f"(truncated, showing first {len(items)} entries)")
        return ToolResult(content="\n".join(lines) or "(empty directory)")

    async def _find_files(
        self,
        db: AsyncSession,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        limit = cast(int, args["head_limit"])
        page = await self._service.find_files(
            db,
            user_id=ctx.user_id,
            path=cast(str, args["path"]),
            query=cast(str | None, args["query"]) or "",
            glob=cast(str | None, args["glob"]),
            file_type=cast(str | None, args["type"]),
            include_dirs=cast(bool, args["include_dirs"]),
            sort=cast(Any, args["sort"]),
            limit=1000 if limit == 0 else limit,
            offset=cast(int, args["offset"]),
        )
        lines = [
            f"{_virtual_path(cast(str, args['path']), item.path)}{'/' if item.is_directory else ''}"
            for item in page.items
        ]
        if page.next_offset is not None or page.truncated:
            lines.append(f"(truncated, showing {len(page.items)} entries)")
        return ToolResult(content="\n".join(lines) or "(no matching files)")

    async def _grep(
        self,
        db: AsyncSession,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        output_mode = cast(Any, args["output_mode"])
        alias = args["max_matches"] if output_mode == "content" else args["max_results"]
        limit = cast(int, alias if alias is not None else args["head_limit"])
        page = await self._service.grep(
            db,
            user_id=ctx.user_id,
            pattern=cast(str, args["pattern"]),
            path=cast(str, args["path"]),
            glob=cast(str | None, args["glob"]),
            file_type=cast(str | None, args["type"]),
            case_insensitive=cast(bool, args["case_insensitive"]),
            fixed_strings=cast(bool, args["fixed_strings"]),
            output_mode=output_mode,
            context_before=cast(int, args["context_before"]),
            context_after=cast(int, args["context_after"]),
            limit=limit,
            offset=cast(int, args["offset"]),
        )
        root = cast(str, args["path"])
        lines: list[str] = []
        for item in page.items:
            if isinstance(item, str):
                lines.append(_virtual_path(root, item))
            elif isinstance(item, GrepCount):
                lines.append(f"{_virtual_path(root, item.path)}:{item.count}")
            else:
                assert isinstance(item, GrepContentMatch)
                lines.extend(
                    f"{_virtual_path(root, item.path)}-{number}-{text}"
                    for number, text in item.before
                )
                lines.append(f"{_virtual_path(root, item.path)}:{item.line_number}:{item.line}")
                lines.extend(
                    f"{_virtual_path(root, item.path)}-{number}-{text}"
                    for number, text in item.after
                )
        if page.next_offset is not None or page.truncated:
            lines.append(f"(truncated, showing {len(page.items)} results)")
        return ToolResult(content="\n".join(lines) or "(no matches)")

    async def _notebook_edit(
        self,
        db: AsyncSession,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        def transform(data: bytes) -> bytes:
            try:
                content = data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Notebook is not UTF-8") from exc
            return edit_notebook(
                content,
                cell_index=cast(int, args["cell_index"]),
                new_source=cast(str | None, args["new_source"]),
                cell_type=cast(CellType, args["cell_type"]),
                edit_mode=cast(EditMode, args["edit_mode"]),
            ).encode("utf-8")

        metadata = await self._service.edit(
            db,
            user_id=ctx.user_id,
            path=cast(str, args["path"]),
            transform=transform,
        )
        await db.commit()
        requested_path = cast(str, args["path"])
        return ToolResult(
            content=file_mutation_result(
                "notebook_edit",
                device="server",
                requested_path=requested_path,
                canonical_path=canonical_server_path(requested_path),
                size_bytes=metadata.size,
            )
        )


def _virtual_path(request_path: str, relative_path: str) -> str:
    if not request_path.startswith("/"):
        return relative_path
    workspace_ref = request_path[1:].partition("/")[0]
    return f"/{workspace_ref}/{relative_path}"
