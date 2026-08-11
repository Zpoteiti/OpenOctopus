from __future__ import annotations

import asyncio
from collections import deque
from typing import Protocol
from uuid import UUID

from openoctopus_client.protocol import MAX_BINARY_CHUNK_BYTES, encode_binary_chunk


class TextWebSocket(Protocol):
    async def send(self, payload: str | bytes) -> None: ...


class WriterOverflowError(RuntimeError):
    """A connection cannot retain another outbound frame safely."""


class SerializedWriter:
    """One bounded writer for control frames and interleaved transfer chunks.

    Critical controls are always considered first.  Normal controls and active
    transfer lanes take turns, so a stream of chunks from one file cannot hold
    a heartbeat or a completed tool result behind the whole file.
    """

    _CRITICAL_MAX = 16
    _NORMAL_MAX = 64
    _NORMAL_BYTES_MAX = 32 * 1024 * 1024
    _BINARY_LANE_MAX = 4
    _BINARY_LANE_BYTES_MAX = _BINARY_LANE_MAX * MAX_BINARY_CHUNK_BYTES
    _BINARY_LANES_MAX = 2

    def __init__(self) -> None:
        self._critical: deque[str] = deque()
        self._normal: deque[str] = deque()
        self._normal_bytes = 0
        self._binary: dict[UUID, deque[bytes]] = {}
        self._binary_bytes: dict[UUID, int] = {}
        self._binary_round_robin: deque[UUID] = deque()
        self._available = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._binary_changed = asyncio.Condition()
        self._stopping = False

    @property
    def binary_lane_count(self) -> int:
        return len(self._binary)

    def has_binary_lane(self, slot_id: UUID) -> bool:
        return slot_id in self._binary

    def register_binary_lane(self, slot_id: UUID) -> None:
        if slot_id in self._binary:
            return
        if len(self._binary) >= self._BINARY_LANES_MAX:
            raise WriterOverflowError("Too many active transfer lanes")
        self._binary[slot_id] = deque()
        self._binary_bytes[slot_id] = 0

    def unregister_binary_lane(self, slot_id: UUID) -> None:
        lane = self._binary.get(slot_id)
        if lane is None:
            return
        if lane:
            raise WriterOverflowError("Cannot remove a non-empty transfer lane")
        self._binary.pop(slot_id, None)
        self._binary_bytes.pop(slot_id, None)
        try:
            self._binary_round_robin.remove(slot_id)
        except ValueError:
            pass
        self._set_idle_if_empty()

    async def discard_binary_lane(self, slot_id: UUID) -> None:
        """Drop a failed transfer's queued chunks and wake its producer."""

        async with self._binary_changed:
            self._binary.pop(slot_id, None)
            self._binary_bytes.pop(slot_id, None)
            self._binary_round_robin = deque(
                queued_id for queued_id in self._binary_round_robin if queued_id != slot_id
            )
            self._set_idle_if_empty()
            self._binary_changed.notify_all()

    @property
    def binary_queued_chunks(self) -> int:
        return sum(len(lane) for lane in self._binary.values())

    def enqueue_critical(self, payload: str) -> None:
        if self._stopping or len(self._critical) >= self._CRITICAL_MAX:
            raise WriterOverflowError("Critical writer lane is full")
        self._critical.append(payload)
        self._idle.clear()
        self._available.set()

    def enqueue_normal(self, payload: str) -> None:
        payload_bytes = len(payload.encode("utf-8"))
        if (
            self._stopping
            or len(self._normal) >= self._NORMAL_MAX
            or self._normal_bytes + payload_bytes > self._NORMAL_BYTES_MAX
        ):
            raise WriterOverflowError("Normal writer lane is full")
        self._normal.append(payload)
        self._normal_bytes += payload_bytes
        self._idle.clear()
        self._available.set()

    def enqueue_binary(self, slot_id: UUID, payload: bytes) -> None:
        """Queue one raw chunk, adding its slot header.

        ``enqueue_binary`` is intentionally non-blocking.  Transfer senders
        use :meth:`wait_enqueue_binary` to obtain bounded backpressure.
        """

        if len(payload) > MAX_BINARY_CHUNK_BYTES:
            raise WriterOverflowError("Transfer chunk exceeds the maximum size")
        lane = self._binary.get(slot_id)
        if self._stopping or lane is None:
            raise WriterOverflowError("Transfer lane is unavailable")
        queued_bytes = self._binary_bytes[slot_id]
        if (
            len(lane) >= self._BINARY_LANE_MAX
            or queued_bytes + len(payload) > self._BINARY_LANE_BYTES_MAX
        ):
            raise WriterOverflowError("Transfer binary lane is full")
        lane.append(encode_binary_chunk(slot_id, payload))
        self._binary_bytes[slot_id] = queued_bytes + len(payload)
        if slot_id not in self._binary_round_robin:
            self._binary_round_robin.append(slot_id)
        self._idle.clear()
        self._available.set()

    async def wait_enqueue_binary(self, slot_id: UUID, payload: bytes) -> None:
        """Queue a chunk, waiting only while this slot's bounded lane is full."""

        if len(payload) > MAX_BINARY_CHUNK_BYTES:
            raise WriterOverflowError("Transfer chunk exceeds the maximum size")
        async with self._binary_changed:
            while True:
                lane = self._binary.get(slot_id)
                if self._stopping or lane is None:
                    raise WriterOverflowError("Transfer lane is unavailable")
                if (
                    len(lane) < self._BINARY_LANE_MAX
                    and self._binary_bytes[slot_id] + len(payload) <= self._BINARY_LANE_BYTES_MAX
                ):
                    self.enqueue_binary(slot_id, payload)
                    return
                await self._binary_changed.wait()

    async def drain_binary(self, slot_id: UUID) -> None:
        async with self._binary_changed:
            while slot_id in self._binary and self._binary[slot_id]:
                await self._binary_changed.wait()

    def _set_idle_if_empty(self) -> None:
        if not self._critical and not self._normal and not self._binary_round_robin:
            self._available.clear()
            self._idle.set()

    def _pop_binary(self) -> bytes | None:
        while self._binary_round_robin:
            slot_id = self._binary_round_robin.popleft()
            lane = self._binary.get(slot_id)
            if not lane:
                continue
            payload = lane.popleft()
            self._binary_bytes[slot_id] -= len(payload) - 16
            if lane:
                self._binary_round_robin.append(slot_id)
            return payload
        return None

    async def run(self, websocket: TextWebSocket) -> None:
        normal_turn = True
        try:
            while True:
                await self._available.wait()
                while self._critical or self._normal or self._binary_round_robin:
                    if self._critical:
                        payload: str | bytes = self._critical.popleft()
                    elif self._normal and (normal_turn or not self._binary_round_robin):
                        payload = self._normal.popleft()
                        self._normal_bytes -= len(payload.encode("utf-8"))
                        normal_turn = False
                    else:
                        binary_payload = self._pop_binary()
                        if binary_payload is None:
                            continue
                        payload = binary_payload
                        normal_turn = True
                        async with self._binary_changed:
                            self._binary_changed.notify_all()
                    await websocket.send(payload)
                self._set_idle_if_empty()
                if self._stopping:
                    return
        except BaseException:
            self._critical.clear()
            self._normal.clear()
            self._normal_bytes = 0
            self._binary_round_robin.clear()
            for slot_id in self._binary:
                self._binary[slot_id].clear()
                self._binary_bytes[slot_id] = 0
            self._available.clear()
            self._idle.set()
            async with self._binary_changed:
                self._binary_changed.notify_all()
            raise

    async def drain(self) -> None:
        await self._idle.wait()

    async def stop(self) -> None:
        self._stopping = True
        self._available.set()
        async with self._binary_changed:
            self._binary_changed.notify_all()
        await self._idle.wait()
