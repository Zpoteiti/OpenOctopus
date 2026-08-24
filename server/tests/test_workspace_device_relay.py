from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from starlette.requests import Request

from openctopus_server.admission import KeyedDirectionalAdmission
from openctopus_server.api.workspace_files import download_file, upload_file
from openctopus_server.devices.protocol import (
    TransferBeginFrame,
    TransferEndFrame,
    TransferReadyFrame,
    TransferRequestFrame,
    new_uuid7,
    parse_server_frame,
)
from openctopus_server.devices.registry import ConnectionHandle, DeviceRouteSnapshot
from openctopus_server.devices.transfer import (
    FairTransferAdmission,
    TransferBusyError,
    TransferError,
    TransferManager,
    TransferResult,
    TransferUnavailableError,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.errors.http import ERROR_STATUS


class _DB:
    def __init__(self, device_id: UUID | None) -> None:
        self.device_id = device_id
        self.closed = False

    async def scalar(self, _statement: object) -> UUID | None:
        return self.device_id

    async def close(self) -> None:
        self.closed = True


class _Transfers:
    def __init__(self, payload: bytes = b"payload") -> None:
        self.payload = payload
        self.begin: TransferBeginFrame | None = None
        self.kwargs: dict[str, object] = {}
        self.task: asyncio.Task[object] | None = None
        self.error: BaseException | None = None

    async def start_client_to_server(self, **kwargs: object) -> TransferResult:
        self.kwargs = kwargs
        if self.error is not None:
            raise self.error
        begin = TransferBeginFrame(
            id=new_uuid7(),
            direction="client_to_server",
            purpose="http_relay",
            src_path="reports/report.txt",
            total_bytes=len(self.payload),
            etag="etag-1",
            mime="text/plain",
        )
        self.begin = begin
        sink_factory = kwargs["sink_factory"]
        sink = await sink_factory(begin)  # type: ignore[operator]
        await sink.write(self.payload)  # type: ignore[union-attr]
        await sink.finish()  # type: ignore[union-attr]
        return TransferResult(len(self.payload), hashlib.sha256(self.payload).hexdigest())


class _UploadTransfers:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.kwargs: dict[str, object] = {}
        self.source_reads = 0

    async def start_server_to_client(self, **kwargs: object) -> SimpleNamespace:
        self.kwargs = kwargs
        if self.error is not None:
            raise self.error
        source = kwargs["source"]
        chunks: list[bytes] = []
        while True:
            self.source_reads += 1
            chunk = await source.read()  # type: ignore[union-attr]
            if not chunk:
                break
            chunks.append(chunk)
        await source.aclose()  # type: ignore[union-attr]
        return SimpleNamespace(
            bytes_transferred=sum(map(len, chunks)),
            sha256=hashlib.sha256(b"".join(chunks)).hexdigest(),
            etag="etag-uploaded",
            created=True,
        )


class _Registry:
    def __init__(self, transfers: object, handle: object | None = object()) -> None:
        self.transfers = transfers
        self.handle = handle

    async def get_route_snapshot(
        self, device_id: UUID, *_args: object, **_kwargs: object
    ) -> DeviceRouteSnapshot | None:
        if self.handle is None:
            return None
        return DeviceRouteSnapshot(ConnectionHandle(device_id, 1), 0, "laptop")


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        rest_transfer_idle_timeout_seconds=0.2,
        rest_transfer_queue_timeout_seconds=0.1,
        device_transfer_queue_timeout_seconds=0.1,
    )


def _admission(*, per_key_limit: int = 2) -> KeyedDirectionalAdmission:
    return KeyedDirectionalAdmission(
        direction_limits={"upload": 2, "download": 2},
        per_key_limit=per_key_limit,
        timeout_seconds=0.01,
    )


def _request(body: bytes, *, content_length: bool = True) -> Request:
    headers = [(b"content-type", b"application/octet-stream")]
    if content_length:
        headers.append((b"content-length", str(len(body)).encode()))
    sent = False

    async def receive() -> dict[str, object]:
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "PUT",
            "path": "/api/workspace/files/reports/report.txt",
            "headers": headers,
            "query_string": b"openoctopus_device=laptop",
        },
        receive,
    )


async def _collect_body(response: object) -> bytes:
    return b"".join([chunk async for chunk in response.body_iterator])  # type: ignore[attr-defined]


async def test_device_get_closes_db_before_relay_and_never_uses_storage() -> None:
    transfers = _Transfers()
    db = _DB(uuid4())
    response = await download_file(
        "reports/report.txt",
        openoctopus_device="laptop",
        user=SimpleNamespace(id=uuid4()),
        db=db,  # type: ignore[arg-type]
        service=None,  # type: ignore[arg-type]
        registry=_Registry(transfers),  # type: ignore[arg-type]
        admission=_admission(),
        settings=_settings(),  # type: ignore[arg-type]
    )

    assert db.closed is True
    assert response.headers["content-length"] == "7"
    assert response.headers["etag"] == '"etag-1"'
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert response.headers["x-content-type-options"] == "nosniff"
    chunks = [chunk async for chunk in response.body_iterator]
    await response._closer()  # type: ignore[attr-defined]
    assert b"".join(chunks) == b"payload"


async def test_device_get_holds_rest_admission_until_response_closes() -> None:
    admission = _admission()
    response = await download_file(
        "reports/report.txt",
        openoctopus_device="laptop",
        user=SimpleNamespace(id=uuid4()),
        db=_DB(uuid4()),  # type: ignore[arg-type]
        service=None,  # type: ignore[arg-type]
        registry=_Registry(_Transfers()),  # type: ignore[arg-type]
        admission=admission,
        settings=_settings(),  # type: ignore[arg-type]
    )

    assert admission.entry_count == 1
    await response._closer()  # type: ignore[attr-defined]
    assert admission.entry_count == 0


async def test_device_get_busy_does_not_start_relay() -> None:
    admission = _admission(per_key_limit=1)
    user_id = uuid4()
    held = await admission.acquire(user_id, "download")
    transfers = _Transfers()
    try:
        with pytest.raises(WorkspaceError) as raised:
            await download_file(
                "reports/report.txt",
                openoctopus_device="laptop",
                user=SimpleNamespace(id=user_id),
                db=_DB(uuid4()),  # type: ignore[arg-type]
                service=None,  # type: ignore[arg-type]
                registry=_Registry(transfers),  # type: ignore[arg-type]
                admission=admission,
                settings=_settings(),  # type: ignore[arg-type]
            )
        assert raised.value.code is ErrorCode.WORKSPACE_TRANSFER_BUSY
        assert transfers.kwargs == {}
    finally:
        await held.aclose()


async def test_device_get_backpressures_a_slow_consumer() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class SlowTransfers(_Transfers):
        async def start_client_to_server(self, **kwargs: object) -> TransferResult:
            self.kwargs = kwargs
            begin = TransferBeginFrame(
                id=new_uuid7(),
                direction="client_to_server",
                purpose="http_relay",
                src_path="reports/report.txt",
                total_bytes=6 * 65536,
                etag="etag-1",
            )
            sink = await kwargs["sink_factory"](begin)  # type: ignore[operator]
            self.task = asyncio.current_task()
            started.set()
            for _ in range(6):
                await sink.write(b"x" * 65536)  # type: ignore[union-attr]
            await sink.finish()  # type: ignore[union-attr]
            release.set()
            return TransferResult(6 * 65536, hashlib.sha256(b"x" * (6 * 65536)).hexdigest())

    transfers = SlowTransfers()
    response = await download_file(
        "reports/report.txt",
        openoctopus_device="laptop",
        user=SimpleNamespace(id=uuid4()),
        db=_DB(uuid4()),  # type: ignore[arg-type]
        service=None,  # type: ignore[arg-type]
        registry=_Registry(transfers),  # type: ignore[arg-type]
        admission=_admission(),
        settings=_settings(),  # type: ignore[arg-type]
    )
    await started.wait()
    await asyncio.sleep(0)
    assert release.is_set() is False
    body = b"".join([chunk async for chunk in response.body_iterator])
    await response._closer()  # type: ignore[attr-defined]
    assert len(body) == 6 * 65536
    assert release.is_set() is True


async def test_device_get_waits_for_the_success_ack_after_the_last_body_chunk() -> None:
    finished = asyncio.Event()
    release_ack = asyncio.Event()
    cancelled = asyncio.Event()

    class AckingTransfers(_Transfers):
        async def start_client_to_server(self, **kwargs: object) -> TransferResult:
            begin = TransferBeginFrame(
                id=new_uuid7(),
                direction="client_to_server",
                purpose="http_relay",
                src_path="reports/report.txt",
                total_bytes=1,
            )
            sink = await kwargs["sink_factory"](begin)  # type: ignore[operator]
            await sink.write(b"x")  # type: ignore[union-attr]
            await sink.finish()  # type: ignore[union-attr]
            finished.set()
            try:
                await release_ack.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return TransferResult(1, hashlib.sha256(b"x").hexdigest())

    response = await download_file(
        "reports/report.txt",
        openoctopus_device="laptop",
        user=SimpleNamespace(id=uuid4()),
        db=_DB(uuid4()),  # type: ignore[arg-type]
        service=None,  # type: ignore[arg-type]
        registry=_Registry(AckingTransfers()),  # type: ignore[arg-type]
        admission=_admission(),
        settings=_settings(),  # type: ignore[arg-type]
    )
    consume = asyncio.create_task(_collect_body(response))
    await asyncio.wait_for(finished.wait(), timeout=1)
    await asyncio.sleep(0)

    assert consume.done() is False
    assert cancelled.is_set() is False

    release_ack.set()
    assert await asyncio.wait_for(consume, timeout=1) == b"x"
    await response._closer()  # type: ignore[attr-defined]
    assert cancelled.is_set() is False


async def test_device_get_cancellation_cancels_pending_transfer() -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    class BlockingTransfers:
        async def start_client_to_server(self, **_kwargs: object) -> TransferResult:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            raise AssertionError("unreachable")

    pending = asyncio.create_task(
        download_file(
            "reports/report.txt",
            openoctopus_device="laptop",
            user=SimpleNamespace(id=uuid4()),
            db=_DB(uuid4()),  # type: ignore[arg-type]
            service=None,  # type: ignore[arg-type]
            registry=_Registry(BlockingTransfers()),  # type: ignore[arg-type]
            admission=_admission(),
            settings=_settings(),  # type: ignore[arg-type]
        )
    )
    await entered.wait()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert cancelled.is_set() is True


async def test_device_get_disconnect_after_commit_accepts_late_client_timeout() -> None:
    body_sent = asyncio.Event()

    class BlockingAckTransport:
        def __init__(self) -> None:
            self.frames: list[object] = []
            self.frame_sent = asyncio.Event()
            self.success_ack_blocked = asyncio.Event()

        async def send_text(
            self,
            _handle: object,
            payload: str,
            *,
            expected_device_name: str | None = None,
            expected_config_epoch: int | None = None,
            on_issued: Callable[[], None] | None = None,
        ) -> bool:
            del expected_device_name, expected_config_epoch
            if on_issued is not None:
                on_issued()
            frame = parse_server_frame(payload)
            self.frames.append(frame)
            self.frame_sent.set()
            if isinstance(frame, TransferEndFrame) and frame.ack and frame.ok:
                # The transfer is committed immediately before this ACK send.
                # Wait until ASGI has delivered the declared response bytes so
                # the disconnect lands inside that post-commit ACK window.
                await body_sent.wait()
                self.success_ack_blocked.set()
                await asyncio.Event().wait()
            return True

        async def send_binary(
            self,
            _handle: object,
            _payload: bytes,
            *,
            expected_device_name: str | None = None,
            expected_config_epoch: int | None = None,
        ) -> bool:
            del expected_device_name, expected_config_epoch
            raise AssertionError("a client-to-server relay must not send binary data")

    transport = BlockingAckTransport()
    manager = TransferManager(
        transport,
        admission=FairTransferAdmission(
            max_concurrency=2,
            max_concurrency_per_user=1,
            queue_timeout_seconds=0.1,
        ),
        idle_timeout_seconds=0.05,
    )
    device_id = uuid4()
    handle = ConnectionHandle(device_id, 1)
    response_task = asyncio.create_task(
        download_file(
            "reports/report.txt",
            openoctopus_device="laptop",
            user=SimpleNamespace(id=uuid4()),
            db=_DB(device_id),  # type: ignore[arg-type]
            service=None,  # type: ignore[arg-type]
            registry=_Registry(manager),  # type: ignore[arg-type]
            admission=_admission(),
            settings=_settings(),  # type: ignore[arg-type]
        )
    )
    while not any(isinstance(frame, TransferRequestFrame) for frame in transport.frames):
        await transport.frame_sent.wait()
        transport.frame_sent.clear()
    request = next(frame for frame in transport.frames if isinstance(frame, TransferRequestFrame))
    payload = b"x"
    digest = hashlib.sha256(payload).hexdigest()
    await manager.handle_frame(
        handle,
        TransferBeginFrame(
            id=request.id,
            direction="client_to_server",
            purpose="http_relay",
            src_path="reports/report.txt",
            total_bytes=len(payload),
        ),
    )
    while not any(
        isinstance(frame, TransferReadyFrame) and frame.id == request.id
        for frame in transport.frames
    ):
        await transport.frame_sent.wait()
        transport.frame_sent.clear()
    response = await asyncio.wait_for(response_task, timeout=1)

    sent: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        await transport.success_ack_blocked.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, object]) -> None:
        sent.append(message)
        if message["type"] == "http.response.body" and message.get("body") == payload:
            body_sent.set()

    stream = asyncio.create_task(
        response(
            {"type": "http", "asgi": {"spec_version": "2.3"}},  # type: ignore[arg-type]
            receive,  # type: ignore[arg-type]
            send,  # type: ignore[arg-type]
        )
    )
    await manager.handle_binary(handle, request.id.bytes + payload)
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=True,
            bytes_sent=len(payload),
            sha256=digest,
        ),
    )
    await asyncio.wait_for(transport.success_ack_blocked.wait(), timeout=1)
    await asyncio.wait_for(stream, timeout=1)

    assert any(message.get("body") == payload for message in sent)
    assert manager.active_slots == 0
    await manager.handle_frame(
        handle,
        TransferEndFrame(
            id=request.id,
            ack=False,
            ok=False,
            code="workspace_transfer_timeout",
        ),
    )


@pytest.mark.parametrize(
    ("error", "status", "code"),
    [
        (TransferError("tool_device_busy"), 429, "tool_device_busy"),
        (TransferUnavailableError("route fenced"), 409, "tool_device_unreachable"),
        (TransferError("workspace_transfer_timeout"), 408, "workspace_transfer_timeout"),
        (TransferError("workspace_transfer_integrity_failed"), 502, "workspace_transfer_integrity_failed"),
    ],
)
async def test_device_get_maps_preheader_transfer_failures(
    error: BaseException, status: int, code: str
) -> None:
    transfers = _Transfers()
    transfers.error = error
    with pytest.raises(WorkspaceError) as raised:
        await download_file(
            "reports/report.txt",
            openoctopus_device="laptop",
            user=SimpleNamespace(id=uuid4()),
            db=_DB(uuid4()),  # type: ignore[arg-type]
            service=None,  # type: ignore[arg-type]
            registry=_Registry(transfers),  # type: ignore[arg-type]
            admission=_admission(),
            settings=_settings(),  # type: ignore[arg-type]
        )
    assert raised.value.code.value == code
    assert {
        409: ErrorCode.TOOL_DEVICE_UNREACHABLE,
        429: ErrorCode.TOOL_DEVICE_BUSY,
        408: ErrorCode.WORKSPACE_TRANSFER_TIMEOUT,
        502: ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED,
    }[status] is raised.value.code
    assert ERROR_STATUS[raised.value.code] == status


async def test_device_put_admission_failure_does_not_read_http_body() -> None:
    transfers = _UploadTransfers(TransferBusyError())
    request = _request(b"not-read")
    with pytest.raises(WorkspaceError) as raised:
        await upload_file(
            "reports/report.txt",
            request,
            SimpleNamespace(),  # type: ignore[arg-type]
            openoctopus_device="laptop",
            user=SimpleNamespace(id=uuid4()),
            db=_DB(uuid4()),  # type: ignore[arg-type]
            service=None,  # type: ignore[arg-type]
            registry=_Registry(transfers),  # type: ignore[arg-type]
            admission=_admission(),
            settings=_settings(),  # type: ignore[arg-type]
            if_match_header='"old"',
            if_none_match_header=None,
        )
    assert raised.value.code is ErrorCode.TOOL_DEVICE_BUSY
    assert transfers.source_reads == 0


async def test_device_put_rest_admission_busy_does_not_read_body_or_start_relay() -> None:
    admission = _admission(per_key_limit=1)
    user_id = uuid4()
    held = await admission.acquire(user_id, "upload")
    transfers = _UploadTransfers()
    try:
        with pytest.raises(WorkspaceError) as raised:
            await upload_file(
                "reports/report.txt",
                _request(b"not-read"),
                SimpleNamespace(),  # type: ignore[arg-type]
                openoctopus_device="laptop",
                user=SimpleNamespace(id=user_id),
                db=_DB(uuid4()),  # type: ignore[arg-type]
                service=None,  # type: ignore[arg-type]
                registry=_Registry(transfers),  # type: ignore[arg-type]
                admission=admission,
                settings=_settings(),  # type: ignore[arg-type]
                if_match_header=None,
                if_none_match_header=None,
            )
        assert raised.value.code is ErrorCode.WORKSPACE_TRANSFER_BUSY
        assert transfers.kwargs == {}
        assert transfers.source_reads == 0
    finally:
        await held.aclose()


async def test_device_put_supports_unknown_length_and_returns_ack_metadata() -> None:
    transfers = _UploadTransfers()
    response = SimpleNamespace(headers={})
    result = await upload_file(
        "reports/report.txt",
        _request(b"body", content_length=False),
        response,  # type: ignore[arg-type]
        openoctopus_device="laptop",
        user=SimpleNamespace(id=uuid4()),
        db=_DB(uuid4()),  # type: ignore[arg-type]
        service=None,  # type: ignore[arg-type]
        registry=_Registry(transfers),  # type: ignore[arg-type]
        admission=_admission(),
        settings=_settings(),  # type: ignore[arg-type]
        if_match_header='"old"',
        if_none_match_header=None,
    )
    assert transfers.kwargs["total_bytes"] is None
    assert transfers.kwargs["if_match"] == "old"
    assert result.etag == "etag-uploaded"
    assert result.created is True
    assert response.headers["ETag"] == '"etag-uploaded"'


async def test_device_name_not_owned_is_indistinguishable_from_missing() -> None:
    for device_id in (None,):
        with pytest.raises(WorkspaceError) as raised:
            await download_file(
                "secret.txt",
                openoctopus_device="someone-else",
                user=SimpleNamespace(id=uuid4()),
                db=_DB(device_id),  # type: ignore[arg-type]
                service=None,  # type: ignore[arg-type]
                registry=_Registry(_Transfers()),  # type: ignore[arg-type]
                admission=_admission(),
                settings=_settings(),  # type: ignore[arg-type]
            )
        assert raised.value.code is ErrorCode.TOOL_DEVICE_UNREACHABLE
