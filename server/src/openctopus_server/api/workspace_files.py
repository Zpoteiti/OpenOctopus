from __future__ import annotations

import asyncio
import math
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, NoReturn, cast
from urllib.parse import quote
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from starlette.requests import ClientDisconnect
from starlette.types import Receive, Scope, Send

from openctopus_server.admission import (
    AdmissionLease,
    AdmissionTimeoutError,
    KeyedDirectionalAdmission,
)
from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.config import Settings, get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import Device, User
from openctopus_server.db.session import get_db
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.devices.protocol import TransferBeginFrame
from openctopus_server.devices.registry import (
    DeviceBusyError,
    DeviceOutcomeUnknownError,
    DeviceRegistry,
    DeviceRouteSnapshot,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import TransferDisconnectedError, TransferError
from openctopus_server.devices.workspace import (
    DeviceDirectoryPageResult,
    DeviceFileMutationResult,
    DeviceGrepPageResult,
    DevicePatchEdit,
    DevicePatchResult,
    DeviceWorkspaceAction,
    dispatch_workspace_action,
)
from openctopus_server.dto.error import ErrorResponse
from openctopus_server.dto.workspace_file import (
    DirectoryEntryPage,
    FileEditRequest,
    FileMutationResponse,
    GrepResultPage,
    StructuredPatchRequest,
    StructuredPatchResponse,
    TransferResponse,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.tools.file_transfer import FileTransferRequest, FileTransferTool
from openctopus_server.workspace.search import GrepContentMatch, GrepCount
from openctopus_server.workspace.service import PatchEdit, WorkspaceService, get_workspace_service

router = APIRouter(prefix="/api/workspace", tags=["Workspace Files"])
_STRONG_ETAG = re.compile(r'^"([\x21\x23-\x7e]+)"$')
_DEVICE_UPLOAD_MAX_BYTES = 64 * 1024 * 1024
_RELAY_QUEUE_CHUNKS = 4


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


@dataclass(frozen=True, slots=True)
class _RelayMetadata:
    begin: TransferBeginFrame
    sink: _RelaySink


@dataclass(frozen=True, slots=True)
class _DeviceMutationMetadata:
    size: int
    etag: str
    created: bool


class _RelaySink:
    """Bounded sink joining a device transfer to a streaming HTTP body."""

    def __init__(self, *, idle_timeout_seconds: float) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=_RELAY_QUEUE_CHUNKS)
        self._idle_timeout_seconds = idle_timeout_seconds
        self._closed = False

    async def write(self, chunk: bytes) -> None:
        if not isinstance(chunk, bytes):
            raise TransferError("workspace_transfer_integrity_failed")
        if self._closed:
            raise TransferError("workspace_transfer_timeout")
        try:
            async with asyncio.timeout(self._idle_timeout_seconds):
                await self.queue.put(chunk)
        except TimeoutError as exc:
            raise TransferError("workspace_transfer_timeout") from exc

    async def finish(self) -> None:
        if self._closed:
            return
        try:
            async with asyncio.timeout(self._idle_timeout_seconds):
                await self.queue.put(None)
        except TimeoutError as exc:
            raise TransferError("workspace_transfer_timeout") from exc
        self._closed = True

    async def abort(self) -> None:
        if self._closed:
            return
        self._closed = True
        # The browser may have disconnected and no longer be consuming.  Drop
        # queued relay bytes so the terminal marker can always wake the body.
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            pass


class _RequestBodySource:
    """Read a request body incrementally with an idle and byte ceiling."""

    def __init__(
        self,
        request: Request,
        *,
        max_bytes: int,
        idle_timeout_seconds: float,
    ) -> None:
        self._iterator = aiter(request.stream())
        self._max_bytes = max_bytes
        self._idle_timeout_seconds = idle_timeout_seconds
        self._read_bytes = 0

    async def read(self) -> bytes:
        try:
            async with asyncio.timeout(self._idle_timeout_seconds):
                chunk = await anext(self._iterator)
        except StopAsyncIteration:
            return b""
        except ClientDisconnect as exc:
            raise TransferError("workspace_transfer_timeout") from exc
        except TimeoutError as exc:
            raise TransferError("workspace_transfer_timeout") from exc
        if not isinstance(chunk, bytes):
            raise TransferError("workspace_transfer_integrity_failed")
        self._read_bytes += len(chunk)
        if self._read_bytes > self._max_bytes:
            raise TransferError("workspace_upload_too_large")
        return chunk

    async def aclose(self) -> None:
        close = getattr(self._iterator, "aclose", None)
        if close is not None:
            await close()


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
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    openoctopus_device_id: UUID | None = None,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
    admission: KeyedDirectionalAdmission = Depends(get_rest_transfer_admission),
    settings: Settings = Depends(get_settings),
) -> StreamingResponse:
    if openoctopus_device != "server":
        return await _download_device(
            path,
            openoctopus_device=openoctopus_device,
            expected_device_id=openoctopus_device_id,
            user=user,
            db=db,
            registry=registry,
            admission=admission,
            settings=settings,
        )
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


async def _download_device(
    path: str,
    *,
    openoctopus_device: str,
    expected_device_id: UUID | None,
    user: User,
    db: AsyncSession,
    registry: DeviceRegistry,
    admission: KeyedDirectionalAdmission,
    settings: Settings,
) -> StreamingResponse:
    route = await _owned_device_route(
        db,
        user=user,
        device_name=openoctopus_device,
        expected_device_id=expected_device_id,
        registry=registry,
    )
    lease = await _acquire_transfer(admission, user.id, "download", settings)
    try:
        metadata_future: asyncio.Future[_RelayMetadata] = asyncio.get_running_loop().create_future()

        async def make_sink(begin: TransferBeginFrame) -> _RelaySink:
            if (
                begin.purpose != "http_relay"
                or begin.src_path != path
                or begin.total_bytes is None
                or begin.dst_path is not None
            ):
                raise TransferError("workspace_transfer_integrity_failed")
            sink = _RelaySink(idle_timeout_seconds=settings.rest_transfer_idle_timeout_seconds)
            if not metadata_future.done():
                metadata_future.set_result(_RelayMetadata(begin=begin, sink=sink))
            return sink

        transfer_task = asyncio.create_task(
            registry.transfers.start_client_to_server(
                handle=route.handle,
                route=route,
                user_id=user.id,
                src_path=path,
                dst_path=None,
                sink_factory=make_sink,
                purpose="http_relay",
            )
        )
        try:
            metadata = await _wait_for_relay_metadata(
                metadata_future,
                transfer_task,
                timeout_seconds=settings.rest_transfer_idle_timeout_seconds,
            )
        except asyncio.CancelledError:
            await _cancel_transfer(transfer_task, None)
            raise
        except BaseException as exc:
            await _cancel_transfer(transfer_task, None)
            _raise_device_transfer(exc, settings)

        async def close_relay() -> None:
            try:
                await _cancel_transfer(transfer_task, metadata.sink)
            finally:
                await lease.aclose()

        async def body() -> AsyncIterator[bytes]:
            while True:
                try:
                    async with asyncio.timeout(settings.rest_transfer_idle_timeout_seconds):
                        chunk = await metadata.sink.queue.get()
                except TimeoutError:
                    await _cancel_transfer(transfer_task, metadata.sink)
                    return
                if chunk is None:
                    try:
                        async with asyncio.timeout(settings.rest_transfer_idle_timeout_seconds):
                            await asyncio.shield(transfer_task)
                    except TimeoutError:
                        await _cancel_transfer(transfer_task, metadata.sink)
                        raise TransferError("workspace_transfer_timeout") from None
                    return
                yield chunk

        headers: dict[str, str] = {
            "Content-Disposition": _content_disposition(path),
            "X-Content-Type-Options": "nosniff",
        }
        try:
            if metadata.begin.total_bytes is not None:
                headers["Content-Length"] = str(metadata.begin.total_bytes)
            if metadata.begin.etag is not None:
                headers["ETag"] = _quote_etag(metadata.begin.etag)
            return _ClosingStreamingResponse(
                body(),
                closer=close_relay,
                send_timeout_seconds=settings.rest_transfer_idle_timeout_seconds,
                media_type=_safe_device_mime(metadata.begin.mime),
                headers=headers,
            )
        except BaseException:
            await _cancel_transfer(transfer_task, metadata.sink)
            raise
    except BaseException:
        await lease.aclose()
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
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
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
    if openoctopus_device != "server":
        return await _upload_device(
            path,
            request=request,
            response=response,
            openoctopus_device=openoctopus_device,
            user=user,
            db=db,
            registry=registry,
            admission=admission,
            settings=settings,
            if_match=if_match,
            if_none_match=if_none_match,
        )
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


async def _upload_device(
    path: str,
    *,
    request: Request,
    response: Response,
    openoctopus_device: str,
    user: User,
    db: AsyncSession,
    registry: DeviceRegistry,
    admission: KeyedDirectionalAdmission,
    settings: Settings,
    if_match: str | None,
    if_none_match: bool,
) -> FileMutationResponse:
    route = await _owned_device_route(
        db,
        user=user,
        device_name=openoctopus_device,
        registry=registry,
    )
    content_length = _content_length(request)
    if content_length is not None and content_length > _DEVICE_UPLOAD_MAX_BYTES:
        raise _too_large()
    lease = await _acquire_transfer(admission, user.id, "upload", settings)
    source: _RequestBodySource | None = None
    try:
        source = _RequestBodySource(
            request,
            max_bytes=_DEVICE_UPLOAD_MAX_BYTES,
            idle_timeout_seconds=settings.rest_transfer_idle_timeout_seconds,
        )
        result = await registry.transfers.start_server_to_client(
            handle=route.handle,
            route=route,
            user_id=user.id,
            src_path=None,
            dst_path=path,
            source=source,
            total_bytes=content_length,
            purpose="workspace_upload",
            if_match=if_match,
            if_none_match=if_none_match,
        )
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        _raise_device_transfer(exc, settings)
    finally:
        # TransferManager owns normal cleanup; this is idempotent and covers
        # admission/rejection paths before it has installed the slot.
        if source is not None:
            await _close_request_source(source)
        await lease.aclose()
    etag = getattr(result, "etag", None)
    created = getattr(result, "created", None)
    size = getattr(result, "size", getattr(result, "bytes_transferred", None))
    if not isinstance(etag, str) or not isinstance(created, bool) or not isinstance(size, int):
        raise WorkspaceError(
            ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED,
            "Workspace device did not return committed file metadata",
        )
    return _mutation_response(
        response,
        path=path,
        metadata=_DeviceMutationMetadata(size=size, etag=etag, created=created),
    )


@router.patch(
    "/files/{path:path}",
    response_model=FileMutationResponse,
    response_model_exclude_none=True,
)
async def edit_file(
    path: str,
    body: FileEditRequest,
    response: Response,
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
    if_match_header: Annotated[str | None, Header(alias="If-Match")] = None,
) -> FileMutationResponse:
    if_match = _parse_if_match(if_match_header)
    if openoctopus_device != "server":
        action = DeviceWorkspaceAction(
            operation="edit_file",
            path=path,
            old_text=body.old_text,
            new_text=body.new_text,
            replace_all=body.replace_all,
            occurrence=body.occurrence,
            line_hint=body.line_hint,
            expected_replacements=body.expected_replacements,
            if_match=if_match,
        )
        result = await dispatch_workspace_action(
            db,
            user=user,
            device_name=openoctopus_device,
            action=action,
            registry=registry,
        )
        assert isinstance(result, DeviceFileMutationResult)
        return _mutation_response(
            response, path=path, metadata=result, replacements=result.replacements
        )
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
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
    if_match_header: Annotated[str | None, Header(alias="If-Match")] = None,
) -> Response:
    if openoctopus_device != "server":
        action = DeviceWorkspaceAction(
            operation="delete_file",
            path=path,
            if_match=_parse_if_match(if_match_header),
        )
        await dispatch_workspace_action(
            db,
            user=user,
            device_name=openoctopus_device,
            action=action,
            registry=registry,
        )
        return Response(status_code=204)
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
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> Response:
    if openoctopus_device != "server":
        action = DeviceWorkspaceAction(operation="delete_folder", path=path)
        await dispatch_workspace_action(
            db,
            user=user,
            device_name=openoctopus_device,
            action=action,
            registry=registry,
        )
        return Response(status_code=204)
    await service.delete_folder(db, user_id=user.id, path=path)
    await db.commit()
    return Response(status_code=204)


@router.post("/patch", response_model=StructuredPatchResponse)
async def apply_workspace_patch(
    body: StructuredPatchRequest,
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> dict[str, Any]:
    if openoctopus_device != "server":
        action = DeviceWorkspaceAction(
            operation="apply_patch",
            edits=[
                DevicePatchEdit(
                    path=edit.path,
                    action=edit.action,
                    old_text=edit.old_text,
                    new_text=edit.new_text,
                )
                for edit in body.edits
            ],
            dry_run=body.dry_run,
        )
        result = await dispatch_workspace_action(
            db,
            user=user,
            device_name=openoctopus_device,
            action=action,
            registry=registry,
        )
        assert isinstance(result, DevicePatchResult)
        return result.model_dump()
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
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> dict[str, Any]:
    if openoctopus_device != "server":
        action = DeviceWorkspaceAction(
            operation="list_dir",
            path=path,
            recursive=recursive,
            limit=limit,
            offset=offset,
        )
        result = await dispatch_workspace_action(
            db,
            user=user,
            device_name=openoctopus_device,
            action=action,
            registry=registry,
        )
        assert isinstance(result, DeviceDirectoryPageResult)
        return result.model_dump()
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
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> dict[str, Any]:
    if openoctopus_device != "server":
        action = DeviceWorkspaceAction(
            operation="find_files",
            path=path,
            query=query,
            glob=glob,
            type=file_type,
            include_dirs=include_dirs,
            sort=sort,
            limit=limit,
            offset=offset,
        )
        result = await dispatch_workspace_action(
            db,
            user=user,
            device_name=openoctopus_device,
            action=action,
            registry=registry,
        )
        assert isinstance(result, DeviceDirectoryPageResult)
        return result.model_dump()
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
    openoctopus_device: str = Query(..., min_length=1, max_length=64),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
) -> dict[str, Any]:
    if openoctopus_device != "server":
        action = DeviceWorkspaceAction(
            operation="grep",
            path=path,
            pattern=pattern,
            glob=glob,
            type=file_type,
            case_insensitive=case_insensitive,
            fixed_strings=fixed_strings,
            output_mode=output_mode,
            context_before=context_before,
            context_after=context_after,
            limit=limit,
            offset=offset,
        )
        result = await dispatch_workspace_action(
            db,
            user=user,
            device_name=openoctopus_device,
            action=action,
            registry=registry,
        )
        assert isinstance(result, DeviceGrepPageResult)
        return result.model_dump()
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


@router.post(
    "/transfer",
    response_model=TransferResponse,
    response_model_exclude_none=True,
    responses={
        400: {
            "model": ErrorResponse,
            "description": "The transfer request or path is invalid.",
        },
        408: {
            "model": ErrorResponse,
            "description": "Transfer made no progress before the idle timeout.",
        },
        409: {
            "model": ErrorResponse,
            "description": "The transfer conflicts or a target device is unavailable.",
        },
        429: {
            "model": ErrorResponse,
            "description": "Transfer capacity is busy; retry after the indicated delay.",
            "headers": {
                "Retry-After": {
                    "description": "Seconds to wait before retrying",
                    "schema": {"type": "integer", "minimum": 1},
                }
            },
        },
        502: {
            "model": ErrorResponse,
            "description": "Streamed transfer failed integrity verification.",
        },
        503: {
            "model": ErrorResponse,
            "description": "Transfer storage or service is unavailable.",
        },
    },
)
async def transfer_workspace_file(
    body: FileTransferRequest,
    user: User = Depends(get_current_user),
    engine: AsyncEngine = Depends(get_engine),
    service: WorkspaceService = Depends(get_workspace_service),
    registry: DeviceRegistry = Depends(get_device_registry),
    settings: Settings = Depends(get_settings),
) -> TransferResponse:
    """Run the shared single-file transfer machine for a REST caller."""

    tool = FileTransferTool(engine, service, registry)
    try:
        outcome = await tool.transfer(body, user_id=user.id)
    except Exception as exc:
        _raise_device_transfer(exc, settings)
    return TransferResponse(
        bytes_transferred=outcome.bytes_transferred,
        sha256=outcome.sha256,
        warnings=list(outcome.warnings),
    )


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


async def _owned_device_route(
    db: AsyncSession,
    *,
    user: User,
    device_name: str,
    expected_device_id: UUID | None = None,
    registry: DeviceRegistry,
) -> DeviceRouteSnapshot:
    query = select(Device.id).where(Device.user_id == user.id, Device.name == device_name)
    if expected_device_id is not None:
        query = query.where(Device.id == expected_device_id)
    device_id = await db.scalar(query)
    await db.close()
    if not isinstance(device_id, UUID):
        # Use one response for a missing or another user's name.  The server
        # must not turn the device table into a cross-user oracle.
        raise WorkspaceError(
            ErrorCode.TOOL_DEVICE_UNREACHABLE,
            "Workspace device is unavailable",
        )
    route = await registry.get_route_snapshot(
        device_id,
        user_id=user.id,
        expected_device_name=device_name,
    )
    if route is None:
        raise WorkspaceError(
            ErrorCode.TOOL_DEVICE_UNREACHABLE,
            "Workspace device is unavailable",
        )
    return route


async def _wait_for_relay_metadata(
    metadata: asyncio.Future[_RelayMetadata],
    transfer: asyncio.Task[object],
    *,
    timeout_seconds: float,
) -> _RelayMetadata:
    metadata_wait: asyncio.Future[_RelayMetadata] = metadata
    try:
        done, _ = await asyncio.wait(
            {cast(asyncio.Future[object], metadata_wait), cast(asyncio.Future[object], transfer)},
            timeout=timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        if not metadata_wait.done():
            metadata_wait.cancel()
            await asyncio.gather(metadata_wait, return_exceptions=True)
    if not done:
        raise TransferError("workspace_transfer_timeout")
    if cast(asyncio.Future[object], metadata_wait) in done:
        return metadata_wait.result()
    # Awaiting the task propagates a pre-header failure, while avoiding an
    # unobserved exception if the manager completed before its begin frame.
    await transfer
    raise TransferError("workspace_transfer_timeout")


async def _cancel_transfer(
    transfer: asyncio.Task[object],
    sink: _RelaySink | None,
) -> None:
    if sink is not None:
        await sink.abort()
    if not transfer.done():
        transfer.cancel()
    await asyncio.gather(transfer, return_exceptions=True)


async def _close_request_source(source: _RequestBodySource) -> None:
    try:
        await source.aclose()
    except Exception:
        pass


def _raise_device_transfer(exc: BaseException, settings: Settings) -> NoReturn:
    if isinstance(exc, WorkspaceError):
        raise exc
    if isinstance(exc, (DeviceOutcomeUnknownError, TransferDisconnectedError)):
        code: object = ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value
    else:
        raw_code = getattr(exc, "code", None)
        code = raw_code.value if isinstance(raw_code, ErrorCode) else raw_code
    if not isinstance(code, str):
        if isinstance(exc, DeviceOutcomeUnknownError):
            code = ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value
        elif isinstance(exc, (DeviceUnavailableError, ConnectionError)):
            code = ErrorCode.TOOL_DEVICE_UNREACHABLE.value
        elif isinstance(exc, DeviceBusyError):
            code = ErrorCode.TOOL_DEVICE_BUSY.value
        elif isinstance(exc, TimeoutError):
            code = ErrorCode.WORKSPACE_TRANSFER_TIMEOUT.value
        else:
            code = ErrorCode.WORKSPACE_STORAGE_ERROR.value
    if code in {"workspace_transfer_busy", ErrorCode.TOOL_DEVICE_BUSY.value}:
        raise _device_busy(settings) from exc
    if code == ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN.value:
        raise WorkspaceError(
            ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN,
            "Device transfer outcome is unknown; do not retry automatically",
        ) from exc
    if code in {"peer_disconnected", ErrorCode.TOOL_DEVICE_UNREACHABLE.value}:
        raise WorkspaceError(
            ErrorCode.TOOL_DEVICE_UNREACHABLE,
            "Workspace device is unavailable",
        ) from exc
    if code in {
        ErrorCode.WORKSPACE_TRANSFER_TIMEOUT.value,
        "cancelled",
    }:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_TRANSFER_TIMEOUT,
            "Workspace transfer timed out",
        ) from exc
    if code == ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED.value:
        raise WorkspaceError(
            ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED,
            "Workspace transfer integrity verification failed",
        ) from exc
    try:
        error_code = ErrorCode(code)
    except ValueError:
        error_code = ErrorCode.WORKSPACE_STORAGE_ERROR
    raise WorkspaceError(error_code, "Workspace device rejected the transfer") from exc


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


def _device_busy(settings: Settings) -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.TOOL_DEVICE_BUSY,
        "Workspace device is busy; retry later",
        headers={"Retry-After": str(math.ceil(settings.device_transfer_queue_timeout_seconds))},
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


def _safe_device_mime(mime: str | None) -> str:
    if not isinstance(mime, str):
        return "application/octet-stream"
    normalized = mime.strip().lower()
    safe = {
        "application/json",
        "application/pdf",
        "application/zip",
        "application/octet-stream",
        "image/gif",
        "image/jpeg",
        "image/png",
        "image/webp",
        "text/csv",
        "text/markdown",
        "text/plain",
    }
    return normalized if normalized in safe else "application/octet-stream"


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
