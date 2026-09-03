from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.channels.media import ChannelMediaSource
from openctopus_server.db.models import Device, User
from openctopus_server.tools.base import (
    DeviceFileDeliveryRef,
    WorkspaceFileDeliveryRef,
)
from openctopus_server.workspace.fs import WorkspaceTarget
from openctopus_server.workspace.service import DownloadTicket


class _Stream:
    def __init__(self, chunks: list[bytes], *, size: int) -> None:
        self._chunks = chunks
        self.size = size
        self.closed = False

    async def read(self) -> bytes:
        return self._chunks.pop(0) if self._chunks else b""

    async def aclose(self) -> None:
        self.closed = True


async def test_workspace_media_is_reauthorized_and_streamed(pg_engine) -> None:
    user_id = uuid4()
    stream = _Stream([b"ab", b"c"], size=3)
    service = AsyncMock()
    service.authorize_download.return_value = DownloadTicket(
        target=WorkspaceTarget.personal(user_id),
        relative_path="report.txt",
    )
    service.open_download.return_value = stream
    source = ChannelMediaSource(
        pg_engine,
        user_id=user_id,
        workspace_service=service,
        device_registry=AsyncMock(),
        idle_timeout_seconds=1,
    )
    media = WorkspaceFileDeliveryRef(
        path="report.txt",
        workspace_id=user_id,
        workspace_relative_path="report.txt",
        filename="report.txt",
        mime="text/plain",
        size=3,
    )

    opened = await source(media)

    assert await opened.read() == b"ab"
    assert await opened.read() == b"c"
    await opened.aclose()
    assert stream.closed is True
    service.authorize_download.assert_awaited_once()


async def test_workspace_media_fails_closed_when_frozen_identity_changed(
    pg_engine,
) -> None:
    user_id = uuid4()
    service = AsyncMock()
    service.authorize_download.return_value = DownloadTicket(
        target=WorkspaceTarget.personal(user_id),
        relative_path="different.txt",
    )
    source = ChannelMediaSource(
        pg_engine,
        user_id=user_id,
        workspace_service=service,
        device_registry=AsyncMock(),
        idle_timeout_seconds=1,
    )
    media = WorkspaceFileDeliveryRef(
        path="report.txt",
        workspace_id=user_id,
        workspace_relative_path="report.txt",
        filename="report.txt",
        mime="text/plain",
        size=3,
    )

    with pytest.raises(ValueError, match="identity changed"):
        await source(media)

    service.open_download.assert_not_awaited()


class _Transfers:
    def __init__(
        self,
        chunks: tuple[bytes, ...],
        *,
        declared_size: int,
        declared_fingerprint: str = "source-v1",
    ) -> None:
        self._chunks = chunks
        self._declared_size = declared_size
        self._declared_fingerprint = declared_fingerprint
        self.calls: list[dict[str, object]] = []

    async def start_client_to_server(self, **kwargs):
        self.calls.append(kwargs)
        begin = SimpleNamespace(
            purpose="http_relay",
            direction="client_to_server",
            src_path=kwargs["src_path"],
            dst_path=None,
            total_bytes=self._declared_size,
            etag=self._declared_fingerprint,
        )
        sink = await kwargs["sink_factory"](begin)
        for chunk in self._chunks:
            await sink.write(chunk)
        await sink.finish()


async def test_known_device_media_relays_without_server_copy(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(
            id=uuid4(),
            email=f"{uuid4()}@test.com",
            password_hash="hash",
            name="Owner",
        )
        db.add(user)
        await db.flush()
        device = Device(
            id=uuid4(),
            user_id=user.id,
            name="laptop",
            token_hash=b"x" * 32,
            token_hint="hint",
        )
        db.add(device)
        await db.commit()
    transfers = _Transfers((b"ab", b"c"), declared_size=3)
    registry = SimpleNamespace(
        transfers=transfers,
        get_route_snapshot=AsyncMock(
            return_value=SimpleNamespace(
                handle=object(),
                config_epoch=1,
                device_name="laptop",
            )
        ),
    )
    source = ChannelMediaSource(
        pg_engine,
        user_id=user.id,
        workspace_service=AsyncMock(),
        device_registry=registry,
        idle_timeout_seconds=1,
    )
    media = DeviceFileDeliveryRef(
        path="report.txt",
        device_id=device.id,
        openoctopus_device="laptop",
        filename="report.txt",
        mime="text/plain",
        size=3,
        fingerprint="source-v1",
    )

    async with source.open(media) as chunks:
        assert [chunk async for chunk in chunks] == [b"ab", b"c"]

    assert len(transfers.calls) == 1
    assert transfers.calls[0]["purpose"] == "http_relay"
    assert transfers.calls[0]["dst_path"] is None


async def test_device_media_rejects_unknown_or_mismatched_size(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(
            id=uuid4(),
            email=f"{uuid4()}@test.com",
            password_hash="hash",
            name="Owner",
        )
        db.add(user)
        await db.flush()
        device = Device(
            id=uuid4(),
            user_id=user.id,
            name="tablet",
            token_hash=b"y" * 32,
            token_hint="hint",
        )
        db.add(device)
        await db.commit()
    transfers = _Transfers((b"abc",), declared_size=4)
    registry = SimpleNamespace(
        transfers=transfers,
        get_route_snapshot=AsyncMock(
            return_value=SimpleNamespace(
                handle=object(),
                config_epoch=1,
                device_name="tablet",
            )
        ),
    )
    source = ChannelMediaSource(
        pg_engine,
        user_id=user.id,
        workspace_service=AsyncMock(),
        device_registry=registry,
        idle_timeout_seconds=1,
    )
    base = dict(
        path="report.txt",
        device_id=device.id,
        openoctopus_device="tablet",
        filename="report.txt",
        mime="text/plain",
        fingerprint="source-v1",
    )

    with pytest.raises(ValueError, match="known size"):
        await source(DeviceFileDeliveryRef(size=None, **base))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="identity changed"):
        await source(DeviceFileDeliveryRef(size=3, **base))


async def test_device_media_rechecks_preflight_fingerprint_at_begin(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(
            id=uuid4(),
            email=f"{uuid4()}@test.com",
            password_hash="hash",
            name="Owner",
        )
        db.add(user)
        await db.flush()
        device = Device(
            id=uuid4(),
            user_id=user.id,
            name="laptop",
            token_hash=b"z" * 32,
            token_hint="hint",
        )
        db.add(device)
        await db.commit()
    transfers = _Transfers(
        (b"abc",),
        declared_size=3,
        declared_fingerprint="source-v2",
    )
    registry = SimpleNamespace(
        transfers=transfers,
        get_route_snapshot=AsyncMock(
            return_value=SimpleNamespace(
                handle=object(),
                config_epoch=1,
                device_name="laptop",
            )
        ),
    )
    source = ChannelMediaSource(
        pg_engine,
        user_id=user.id,
        workspace_service=AsyncMock(),
        device_registry=registry,
        idle_timeout_seconds=1,
    )

    with pytest.raises(ValueError, match="identity changed"):
        await source(
            DeviceFileDeliveryRef(
                path="report.txt",
                device_id=device.id,
                openoctopus_device="laptop",
                filename="report.txt",
                mime="text/plain",
                size=3,
                fingerprint="source-v1",
            )
        )
