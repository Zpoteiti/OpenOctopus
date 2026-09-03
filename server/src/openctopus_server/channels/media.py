from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.db.models import Device
from openctopus_server.devices.protocol import TransferBeginFrame
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.tools.base import (
    DeviceFileDeliveryRef,
    WorkspaceFileDeliveryRef,
)
from openctopus_server.workspace.service import WorkspaceService

from .types import ResolvedDeliveryFile

_DEVICE_QUEUE_CHUNKS = 4
_MAX_STREAM_CHUNK_BYTES = 64 * 1024


class _ReadableMediaStream(Protocol):
    size: int

    async def read(self) -> bytes: ...

    async def aclose(self) -> None: ...


class ChannelMediaSource:
    """Reauthorize frozen media facts and expose one bounded, uncached stream."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        user_id: UUID,
        workspace_service: WorkspaceService,
        device_registry: DeviceRegistry,
        idle_timeout_seconds: float,
    ) -> None:
        if idle_timeout_seconds <= 0:
            raise ValueError("Channel media idle timeout must be positive")
        self._engine = engine
        self._user_id = user_id
        self._workspace_service = workspace_service
        self._device_registry = device_registry
        self._idle_timeout_seconds = idle_timeout_seconds

    async def __call__(
        self,
        media: ResolvedDeliveryFile,
    ) -> _ReadableMediaStream:
        if isinstance(media, WorkspaceFileDeliveryRef):
            return await self._open_workspace(media)
        if isinstance(media, DeviceFileDeliveryRef):
            return await self._open_device(media)
        raise TypeError("Channel media identity is unsupported")

    @asynccontextmanager
    async def open(
        self,
        media: ResolvedDeliveryFile,
    ) -> AsyncIterator[AsyncIterator[bytes]]:
        stream = await self(media)

        async def chunks() -> AsyncIterator[bytes]:
            while chunk := await stream.read():
                yield chunk

        try:
            yield chunks()
        finally:
            close_task = asyncio.create_task(stream.aclose())
            await await_future_cancellation_safe(close_task)

    async def _open_workspace(
        self,
        media: WorkspaceFileDeliveryRef,
    ) -> _ReadableMediaStream:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            ticket = await self._workspace_service.authorize_download(
                db,
                user_id=self._user_id,
                path=media.path,
            )
            await db.rollback()
        if (
            ticket.target.id != media.workspace_id
            or ticket.relative_path != media.workspace_relative_path
        ):
            raise ValueError("Workspace media identity changed before delivery")
        stream = await self._workspace_service.open_download(ticket)
        if stream.size != media.size:
            close_task = asyncio.create_task(stream.aclose())
            await await_future_cancellation_safe(close_task)
            raise ValueError("Workspace media size changed before delivery")
        return stream

    async def _open_device(
        self,
        media: DeviceFileDeliveryRef,
    ) -> _ReadableMediaStream:
        if media.size is None:
            raise ValueError("Device media requires a known size before delivery")
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            device_id = await db.scalar(
                select(Device.id).where(
                    Device.id == media.device_id,
                    Device.user_id == self._user_id,
                    Device.name == media.openoctopus_device,
                )
            )
            await db.rollback()
        if device_id is None:
            raise ValueError("Device media authority changed before delivery")
        route = await self._device_registry.get_route_snapshot(
            media.device_id,
            user_id=self._user_id,
            expected_device_name=media.openoctopus_device,
        )
        if route is None:
            raise ValueError("Device media source is unavailable")

        loop = asyncio.get_running_loop()
        ready: asyncio.Future[_DeviceRelaySink] = loop.create_future()

        async def make_sink(begin: TransferBeginFrame) -> _DeviceRelaySink:
            if (
                begin.purpose != "http_relay"
                or begin.direction != "client_to_server"
                or begin.src_path != media.path
                or begin.dst_path is not None
                or begin.total_bytes != media.size
                or begin.etag != media.fingerprint
            ):
                raise ValueError("Device media identity changed before delivery")
            sink = _DeviceRelaySink(
                idle_timeout_seconds=self._idle_timeout_seconds
            )
            if not ready.done():
                ready.set_result(sink)
            return sink

        transfer = asyncio.create_task(
            self._device_registry.transfers.start_client_to_server(
                handle=route.handle,
                route=route,
                user_id=self._user_id,
                src_path=media.path,
                dst_path=None,
                sink_factory=make_sink,
                purpose="http_relay",
            )
        )
        try:
            done, _ = await asyncio.wait(
                (ready, transfer),
                timeout=self._idle_timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if ready in done:
                return _DeviceMediaStream(
                    ready.result(),
                    transfer,
                    size=media.size,
                    idle_timeout_seconds=self._idle_timeout_seconds,
                )
            if transfer in done:
                await transfer
            raise TimeoutError("Device media did not provide metadata")
        except BaseException:
            cleanup = asyncio.create_task(_stop_transfer(transfer, None))
            await await_future_cancellation_safe(cleanup)
            raise
        finally:
            if not ready.done():
                ready.cancel()


class _DeviceRelaySink:
    def __init__(self, *, idle_timeout_seconds: float) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue(
            maxsize=_DEVICE_QUEUE_CHUNKS
        )
        self._idle_timeout_seconds = idle_timeout_seconds
        self._closed = False

    async def write(self, chunk: bytes) -> None:
        if (
            self._closed
            or not isinstance(chunk, bytes)
            or not chunk
            or len(chunk) > _MAX_STREAM_CHUNK_BYTES
        ):
            raise ValueError("Device media stream returned an invalid chunk")
        async with asyncio.timeout(self._idle_timeout_seconds):
            await self.queue.put(chunk)

    async def finish(self) -> None:
        if self._closed:
            return
        async with asyncio.timeout(self._idle_timeout_seconds):
            await self.queue.put(None)
        self._closed = True

    async def abort(self) -> None:
        if self._closed:
            return
        self._closed = True
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self.queue.put_nowait(None)


class _DeviceMediaStream:
    def __init__(
        self,
        sink: _DeviceRelaySink,
        transfer: asyncio.Task[object],
        *,
        size: int,
        idle_timeout_seconds: float,
    ) -> None:
        self.size = size
        self._sink = sink
        self._transfer = transfer
        self._idle_timeout_seconds = idle_timeout_seconds
        self._closed = False

    async def read(self) -> bytes:
        if self._closed:
            return b""
        try:
            async with asyncio.timeout(self._idle_timeout_seconds):
                chunk = await self._sink.queue.get()
            if chunk is not None:
                return chunk
            async with asyncio.timeout(self._idle_timeout_seconds):
                await asyncio.shield(self._transfer)
            self._closed = True
            return b""
        except BaseException:
            cleanup = asyncio.create_task(self.aclose())
            await await_future_cancellation_safe(cleanup)
            raise

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await _stop_transfer(self._transfer, self._sink)


async def _stop_transfer(
    transfer: asyncio.Task[object],
    sink: _DeviceRelaySink | None,
) -> None:
    if sink is not None:
        await sink.abort()
    if not transfer.done():
        transfer.cancel()
    await asyncio.gather(transfer, return_exceptions=True)
