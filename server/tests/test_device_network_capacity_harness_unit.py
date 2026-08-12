"""Fast unit checks for the real-network capacity peer lifecycle."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from websockets.exceptions import ConnectionClosedError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from device_network_capacity_harness import (  # noqa: E402
    _Identity,
    _run_transfer_batch,
    _SourcePeer,
)


class _EndedSocket:
    def __aiter__(self) -> _EndedSocket:
        return self

    async def __anext__(self) -> str:
        raise StopAsyncIteration


class _DroppedSocket:
    def __aiter__(self) -> _DroppedSocket:
        return self

    async def __anext__(self) -> str:
        raise ConnectionClosedError(None, None, None)


@pytest.mark.asyncio
async def test_source_peer_records_unexpected_connection_end() -> None:
    peer = _SourcePeer(
        _Identity(uuid4(), uuid4(), "device", "token"),
        delay=0,
        queue_capacity=2,
    )
    peer.websocket = cast(Any, _EndedSocket())

    await peer.read_frames()

    assert peer.unexpected_disconnect is True
    assert peer.error == "connection ended before harness shutdown"


@pytest.mark.asyncio
async def test_source_peer_records_an_abnormal_websocket_drop() -> None:
    peer = _SourcePeer(
        _Identity(uuid4(), uuid4(), "device", "token"),
        delay=0,
        queue_capacity=2,
    )
    peer.websocket = cast(Any, _DroppedSocket())

    await peer.read_frames()

    assert peer.unexpected_disconnect is True
    assert peer.error == "connection closed unexpectedly: 1006"


@pytest.mark.asyncio
async def test_transfer_batch_bounds_launches_below_admission_queue_capacity() -> None:
    max_concurrency = 2
    connection_count = max_concurrency * 2 + 1
    active = 0
    active_high_water = 0

    async def transfer(index: int) -> int:
        nonlocal active, active_high_water
        active += 1
        active_high_water = max(active_high_water, active)
        try:
            await asyncio.sleep(0)
            return index
        finally:
            active -= 1

    results = await _run_transfer_batch(
        connection_count,
        max_concurrency=max_concurrency,
        transfer=transfer,
    )

    assert results == list(range(connection_count))
    assert active_high_water == max_concurrency
