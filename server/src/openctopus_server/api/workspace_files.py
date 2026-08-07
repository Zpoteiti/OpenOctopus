from __future__ import annotations

import asyncio
import math
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from functools import lru_cache
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from urllib.parse import quote
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.types import Receive, Scope, Send

from openctopus_server.admission import (
    AdmissionLease,
    AdmissionTimeoutError,
    KeyedDirectionalAdmission,
)
from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.config import Settings, get_settings
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.workspace_file import (
    DirectoryEntryPage,
    FileEditRequest,
    FileMutationResponse,
    GrepResultPage,
    StructuredPatchRequest,
    StructuredPatchResponse,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.search import GrepContentMatch, GrepCount
from openctopus_server.workspace.service import PatchEdit, WorkspaceService, get_workspace_service

router = APIRouter(prefix="/api/workspace", tags=["Workspace Files"])
_STRONG_ETAG = re.compile(r'^"([\x21\x23-\x7e]+)"$')


@lru_cache
def get_rest_transfer_admission() -> KeyedDirectionalAdmission:
    settings = get_settings()
    return KeyedDirectionalAdmission(
        direction_limits={
            "upload": settings.rest_upload_max_concurrency,
            "download": settings.rest_download_max_concurrency,
        },
        per_key_limit=settings.rest_transfer_max_concurrency_per_user,
        timeout_seconds=settings.rest_transfer_queue_timeout_seconds,
    )


class _ClosingStreamingResponse(StreamingResponse):
    def __init__(
        self,
        content: AsyncIterator[bytes],
        *,
        closer: Callable[[], Awaitable[None]],
        send_timeout_seconds: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(content, **kwargs)
        self._closer = closer
        self._send_timeout_seconds = send_timeout_seconds

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def bounded_send(message: Any) -> None:
            if self._send_timeout_seconds is None or message["type"] != "http.response.body":
                await send(message)
                return
            async with asyncio.timeout(self._send_timeout_seconds):
                await send(message)

        try:
            await super().__call__(scope, receive, bounded_send)
        finally:
            await self._closer()


def require_server_device(
    openoctopus_device: Annotated[Literal["server"], Query()],
) -> None:
    del openoctopus_device


@router.get(
    "/files/{path:path}",
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Backpressured raw file stream",
            "headers": {
                "ETag": {"schema": {"type": "string"}},
                "Content-Length": {"schema": {"type": "integer"}},
                "Content-Disposition": {"schema": {"type": "string"}},
                "X-Content-Type-Options": {"schema": {"type": "string"}},
            },
            "content": {
                "application/octet-stream": {"schema": {"type": "string", "format": "binary"}}
            },
        }
    },
)
async def download_file(
    path: str,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    admission: KeyedDirectionalAdmission = Depends(get_rest_transfer_admission),
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    ticket = await service.authorize_download(db, user_id=user.id, path=path)
    await db.commit()
    lease = await _acquire_transfer(admission, user.id, "download", settings)
    try:
        stream = await service.open_download(ticket)
    except BaseException:
        await lease.aclose()
        raise

    async def close_download() -> None:
        try:
            await stream.aclose()
        finally:
            await lease.aclose()

    async def body() -> AsyncIterator[bytes]:
        while True:
            async with asyncio.timeout(settings.rest_transfer_idle_timeout_seconds):
                chunk = await stream.read()
            if not chunk:
                return
            yield chunk

    try:
        return _ClosingStreamingResponse(
            body(),
            closer=close_download,
            send_timeout_seconds=settings.rest_transfer_idle_timeout_seconds,
            media_type="application/octet-stream",
            headers={
                "Content-Length": str(stream.size),
                "ETag": _quote_etag(stream.etag),
                "Content-Disposition": _content_disposition(path),
                "X-Content-Type-Options": "nosniff",
            },
        )
    except BaseException:
        await close_download()
        raise


@router.put(
    "/files/{path:path}",
    response_model=FileMutationResponse,
    response_model_exclude_none=True,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/octet-stream": {"schema": {"type": "string", "format": "binary"}}
            },
        }
    },
)
async def upload_file(
    path: str,
    request: Request,
    response: Response,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    admission: KeyedDirectionalAdmission = Depends(get_rest_transfer_admission),
    settings: Settings = Depends(get_settings),
    if_match_header: Annotated[str | None, Header(alias="If-Match")] = None,
    if_none_match_header: Annotated[str | None, Header(alias="If-None-Match")] = None,
) -> FileMutationResponse:
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != (
        "application/octet-stream"
    ):
        raise _invalid("Workspace uploads require application/octet-stream")
    if_match, if_none_match = _conditions(if_match_header, if_none_match_header)
    ticket = await service.authorize_upload(db, user_id=user.id, path=path)
    await db.commit()
    try:
        slot = admission.slot(user.id, "upload")
        async with slot:
            content_length = _content_length(request)
            if content_length is not None and content_length > ticket.max_bytes:
                raise _too_large()
            chunks = _idle_upload_chunks(
                request.stream(),
                timeout_seconds=settings.rest_transfer_idle_timeout_seconds,
            )
            async with service.collect_upload(chunks, max_bytes=ticket.max_bytes) as data:
                fresh_ticket = await service.authorize_upload(db, user_id=user.id, path=path)
                await db.commit()
                if len(data) > fresh_ticket.max_bytes:
                    raise _too_large()
                metadata = await service.write_authorized_upload(
                    fresh_ticket,
                    data=data,
                    if_match=if_match,
                    if_none_match=if_none_match,
                )
    except AdmissionTimeoutError as exc:
        raise _transfer_busy(settings) from exc
    return _mutation_response(response, path=path, metadata=metadata)


@router.patch(
    "/files/{path:path}",
    response_model=FileMutationResponse,
    response_model_exclude_none=True,
)
async def edit_file(
    path: str,
    body: FileEditRequest,
    response: Response,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    if_match_header: Annotated[str | None, Header(alias="If-Match")] = None,
) -> FileMutationResponse:
    if_match = _parse_if_match(if_match_header)
    metadata, replacements = await service.edit_text(
        db,
        user_id=user.id,
        path=path,
        old_text=body.old_text,
        new_text=body.new_text,
        replace_all=body.replace_all,
        occurrence=body.occurrence,
        line_hint=body.line_hint,
        expected_replacements=body.expected_replacements,
        if_match=if_match,
    )
    await db.commit()
    return _mutation_response(
        response,
        path=path,
        metadata=metadata,
        replacements=replacements,
    )


@router.delete("/files/{path:path}", status_code=204)
async def delete_file(
    path: str,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    if_match_header: Annotated[str | None, Header(alias="If-Match")] = None,
) -> Response:
    await service.delete_file(
        db,
        user_id=user.id,
        path=path,
        if_match=_parse_if_match(if_match_header),
    )
    await db.commit()
    return Response(status_code=204)


@router.delete("/folders/{path:path}", status_code=204)
async def delete_folder(
    path: str,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
) -> Response:
    await service.delete_folder(db, user_id=user.id, path=path)
    await db.commit()
    return Response(status_code=204)


@router.post("/patch", response_model=StructuredPatchResponse)
async def apply_workspace_patch(
    body: StructuredPatchRequest,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict[str, Any]:
    edits = tuple(
        PatchEdit(
            path=edit.path,
            action=edit.action,
            old_text=edit.old_text,
            new_text=edit.new_text or "",
        )
        for edit in body.edits
    )
    results = await service.apply_patch(
        db,
        user_id=user.id,
        edits=edits,
        dry_run=body.dry_run,
    )
    await db.commit()
    return {
        "items": [
            {
                "path": item.path,
                "action": item.action,
                "size": item.size,
                "etag": item.etag,
                "created": item.created,
                "replacements": item.replacements,
            }
            for item in results
        ],
        "dry_run": body.dry_run,
        "committed": 0 if body.dry_run else len(results),
    }


@router.get("/list/{path:path}", response_model=DirectoryEntryPage)
async def list_directory(
    path: str,
    recursive: bool = False,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict[str, Any]:
    if recursive:
        recursive_page = await service.list_recursive(
            db,
            user_id=user.id,
            path=path,
            limit=limit,
            offset=offset,
        )
        rendered_items = [_search_entry(path, entry) for entry in recursive_page.items]
        next_offset = recursive_page.next_offset
        truncated = recursive_page.truncated
    else:
        directory_page = await service.list_dir_page(
            db,
            user_id=user.id,
            path=path,
            limit=limit,
            offset=offset,
        )
        rendered_items = [_search_entry(path, entry) for entry in directory_page.items]
        next_offset = directory_page.next_offset
        truncated = directory_page.truncated
    return {
        "items": rendered_items,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "truncated": truncated,
    }


@router.get("/find-files", response_model=DirectoryEntryPage)
async def find_workspace_files(
    path: str = ".",
    query: str = "",
    glob: str | None = None,
    file_type: Annotated[str | None, Query(alias="type")] = None,
    include_dirs: bool = False,
    sort: Literal["path", "modified"] = "path",
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict[str, Any]:
    page = await service.find_files(
        db,
        user_id=user.id,
        path=path,
        query=query,
        glob=glob,
        file_type=file_type,
        include_dirs=include_dirs,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    return {
        "items": [_search_entry(path, item) for item in page.items],
        "limit": limit,
        "offset": offset,
        "next_offset": page.next_offset,
        "truncated": page.truncated,
    }


@router.get("/grep", response_model=GrepResultPage, response_model_exclude_none=True)
async def grep_workspace_files(
    pattern: Annotated[str, Query(min_length=1)],
    path: str = ".",
    glob: str | None = None,
    file_type: Annotated[str | None, Query(alias="type")] = None,
    case_insensitive: bool = False,
    fixed_strings: bool = False,
    output_mode: Literal["content", "files_with_matches", "count"] = "files_with_matches",
    context_before: Annotated[int, Query(ge=0, le=20)] = 0,
    context_after: Annotated[int, Query(ge=0, le=20)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    _: None = Depends(require_server_device),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
) -> dict[str, Any]:
    page = await service.grep(
        db,
        user_id=user.id,
        pattern=pattern,
        path=path,
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
    return {
        "items": [_grep_item(path, item) for item in page.items],
        "limit": limit,
        "offset": offset,
        "next_offset": page.next_offset,
        "truncated": page.truncated,
    }


async def _acquire_transfer(
    admission: KeyedDirectionalAdmission,
    user_id: UUID,
    direction: str,
    settings: Settings,
) -> AdmissionLease:
    try:
        return await admission.acquire(user_id, direction)
    except AdmissionTimeoutError as exc:
        raise _transfer_busy(settings) from exc


async def _idle_upload_chunks(
    chunks: AsyncIterator[bytes],
    *,
    timeout_seconds: float,
) -> AsyncIterator[bytes]:
    iterator = aiter(chunks)
    while True:
        try:
            async with asyncio.timeout(timeout_seconds):
                chunk = await anext(iterator)
        except StopAsyncIteration:
            return
        except TimeoutError as exc:
            raise WorkspaceError(
                ErrorCode.WORKSPACE_TRANSFER_TIMEOUT,
                "Workspace upload timed out while waiting for request data",
            ) from exc
        yield chunk


def _transfer_busy(settings: Settings) -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_TRANSFER_BUSY,
        "Workspace transfer capacity is busy; retry later",
        headers={"Retry-After": str(math.ceil(settings.rest_transfer_queue_timeout_seconds))},
    )


def _conditions(if_match: str | None, if_none_match: str | None) -> tuple[str | None, bool]:
    if if_match is not None and if_none_match is not None:
        raise _invalid("If-Match and If-None-Match cannot be combined")
    parsed_match = _parse_if_match(if_match)
    if if_none_match is not None and if_none_match != "*":
        raise _invalid("If-None-Match must be *")
    return parsed_match, if_none_match == "*"


def _parse_if_match(value: str | None) -> str | None:
    if value is None:
        return None
    match = _STRONG_ETAG.fullmatch(value)
    if match is None:
        raise _invalid("If-Match must contain exactly one strong ETag")
    return match.group(1)


def _content_length(request: Request) -> int | None:
    value = request.headers.get("content-length")
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise _invalid("Content-Length is invalid") from exc
    if parsed < 0:
        raise _invalid("Content-Length is invalid")
    return parsed


def _quote_etag(etag: str) -> str:
    if not etag or any(
        character == '"' or not 0x21 <= ord(character) <= 0x7E for character in etag
    ):
        raise WorkspaceError(
            ErrorCode.WORKSPACE_STORAGE_ERROR,
            "Object storage returned an invalid ETag",
        )
    return f'"{etag}"'


def _content_disposition(path: str) -> str:
    filename = PurePosixPath(path).name or "download"
    fallback = re.sub(r"[^A-Za-z0-9._-]", "_", filename)
    if fallback in {"", ".", ".."}:
        fallback = "download"
    encoded = quote(filename, safe="")
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"


def _virtual_entry_path(request_path: str, relative_path: str) -> str:
    if not request_path.startswith("/"):
        return relative_path
    workspace_ref = request_path[1:].partition("/")[0]
    return f"/{workspace_ref}/{relative_path}"


def _search_entry(request_path: str, entry: Any) -> dict[str, Any]:
    return {
        "name": PurePosixPath(entry.path).name,
        "path": _virtual_entry_path(request_path, entry.path),
        "kind": "directory" if entry.is_directory else "file",
        "size": 0 if entry.size is None else entry.size,
    }


def _grep_item(request_path: str, item: Any) -> dict[str, Any]:
    if isinstance(item, str):
        return {"path": _virtual_entry_path(request_path, item)}
    if isinstance(item, GrepCount):
        return {
            "path": _virtual_entry_path(request_path, item.path),
            "count": item.count,
        }
    assert isinstance(item, GrepContentMatch)
    return {
        "path": _virtual_entry_path(request_path, item.path),
        "line_number": item.line_number,
        "line": item.line,
        "before": [{"line_number": line_number, "line": line} for line_number, line in item.before],
        "after": [{"line_number": line_number, "line": line} for line_number, line in item.after],
    }


def _mutation_response(
    response: Response,
    *,
    path: str,
    metadata: Any,
    replacements: int | None = None,
) -> FileMutationResponse:
    response.headers["ETag"] = _quote_etag(metadata.etag)
    return FileMutationResponse(
        path=path,
        size=metadata.size,
        etag=metadata.etag,
        created=metadata.created,
        replacements=replacements,
    )


def _too_large() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_UPLOAD_TOO_LARGE,
        "Workspace upload exceeds the REST upload limit",
    )


def _invalid(message: str) -> WorkspaceError:
    return WorkspaceError(ErrorCode.WORKSPACE_INVALID_REQUEST, message)
