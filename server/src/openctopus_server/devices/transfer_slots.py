"""One slot namespace and bounded terminal-record budget for direct and relay transfers.

All mutations and *_locked methods use ``lock``. A relay reserves two endpoint
records before issue while consuming one operation admission lease.
"""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

from openctopus_server.devices.protocol import (
    TransferEndFrame,
)

from .transfer_admission import TransferBusyError
from .transfer_types import (
    BridgeRole,
    _BridgeSlot,
    _BridgeTombstone,
    _TransferSlot,
)

TOMBSTONE_MAX_ENTRIES = 4096


class TransferSlots:
    def __init__(self) -> None:
        self.slots: dict[tuple[UUID, int, UUID], _TransferSlot] = {}
        self.bridges: dict[UUID, _BridgeSlot] = {}
        self.bridge_endpoints: dict[tuple[UUID, int, UUID], tuple[_BridgeSlot, BridgeRole]] = {}
        self.bridge_tombstones: dict[tuple[UUID, int, UUID], _BridgeTombstone] = {}
        self.reserved_tombstone_credits = 0
        self.tombstones: dict[
            tuple[UUID, int, UUID], tuple[float, TransferEndFrame | None, bool]
        ] = {}
        self.acknowledged_failure_tombstones: set[tuple[UUID, int, UUID]] = set()
        self.lock = asyncio.Lock()

    def key_in_use_locked(self, key: tuple[UUID, int, UUID]) -> bool:
        return (
            key in self.slots
            or key in self.bridge_endpoints
            or key in self.tombstones
            or key in self.bridge_tombstones
        )

    def reserve_tombstone_credits_locked(self, count: int) -> None:
        while self.tombstone_occupancy_locked() + count > TOMBSTONE_MAX_ENTRIES:
            if not self.evict_one_final_tombstone_locked():
                raise TransferBusyError("transfer tombstone capacity is exhausted")
        self.reserved_tombstone_credits += count

    def tombstone_occupancy_locked(self) -> int:
        return len(self.tombstones) + len(self.bridge_tombstones) + self.reserved_tombstone_credits

    def evict_one_final_tombstone_locked(self) -> bool:
        if self.tombstones:
            key = next(iter(self.tombstones))
            self.tombstones.pop(key, None)
            self.acknowledged_failure_tombstones.discard(key)
            return True
        for key, tombstone in tuple(self.bridge_tombstones.items()):
            if not tombstone.pinned and not tombstone.source_timeout_ack_in_flight:
                self.bridge_tombstones.pop(key, None)
                return True
        return False

    def remember_tombstone_locked(
        self,
        key: tuple[UUID, int, UUID],
        value: tuple[float, TransferEndFrame | None, bool],
    ) -> None:
        self.tombstones.pop(key, None)
        self.acknowledged_failure_tombstones.discard(key)
        self.tombstones[key] = value
        while self.tombstone_occupancy_locked() > TOMBSTONE_MAX_ENTRIES:
            if not self.evict_one_final_tombstone_locked():
                raise RuntimeError("transfer tombstone capacity invariant was violated")

    def expire_tombstones_locked(self) -> None:
        now = time.monotonic()
        for key, (expires_at, _, _) in tuple(self.tombstones.items()):
            if expires_at <= now:
                self.tombstones.pop(key, None)
                self.acknowledged_failure_tombstones.discard(key)
        for key, tombstone in tuple(self.bridge_tombstones.items()):
            if (
                not tombstone.pinned
                and not tombstone.source_timeout_ack_in_flight
                and not tombstone.simultaneous_failure_ack_in_flight
                and tombstone.expires_at is not None
                and tombstone.expires_at <= now
            ):
                self.bridge_tombstones.pop(key, None)
