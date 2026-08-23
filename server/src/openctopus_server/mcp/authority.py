from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from openctopus_server.devices.mcp_routes import FrozenMcpEntryRoute
from openctopus_server.mcp.models import ServerMcpEnvelope
from openctopus_server.mcp.routes import FrozenServerMcpEntryRoute


@dataclass(frozen=True, slots=True)
class ServerMcpAuthoritySnapshot:
    config_revision: int
    catalog_digest: str
    reserved_names: frozenset[str]
    valid: bool = True


class ServerMcpAuthorityFence:
    """Linearize durable Server MCP transitions with MCP issue boundaries."""

    def __init__(self, envelope: ServerMcpEnvelope) -> None:
        self._lock = asyncio.Lock()
        self._snapshot = _snapshot(envelope)
        self._transitioning = False

    @property
    def snapshot(self) -> ServerMcpAuthoritySnapshot:
        return self._snapshot

    @asynccontextmanager
    async def transition(self) -> AsyncIterator[None]:
        async with self._lock:
            self._transitioning = True
            try:
                yield
            finally:
                self._transitioning = False

    @asynccontextmanager
    async def device_issue(self, route: FrozenMcpEntryRoute) -> AsyncIterator[bool]:
        async with self._lock:
            snapshot = self._snapshot
            yield bool(
                snapshot.valid
                and route.server_config_revision == snapshot.config_revision
                and route.server not in snapshot.reserved_names
            )

    def server_issue_allowed(self, route: FrozenServerMcpEntryRoute) -> bool:
        snapshot = self._snapshot
        return bool(
            not self._transitioning
            and snapshot.valid
            and route.config_revision == snapshot.config_revision
            and route.catalog_digest == snapshot.catalog_digest
        )

    def matches(self, envelope: ServerMcpEnvelope) -> bool:
        return self._snapshot == _snapshot(envelope)

    def publish(self, envelope: ServerMcpEnvelope) -> None:
        """Publish only while holding ``transition`` or during startup."""

        self._snapshot = _snapshot(envelope)

    def invalidate(self) -> None:
        """Fail closed when a durable transition cannot be resolved."""

        self._snapshot = ServerMcpAuthoritySnapshot(
            config_revision=self._snapshot.config_revision,
            catalog_digest=self._snapshot.catalog_digest,
            reserved_names=self._snapshot.reserved_names,
            valid=False,
        )


def _snapshot(envelope: ServerMcpEnvelope) -> ServerMcpAuthoritySnapshot:
    return ServerMcpAuthoritySnapshot(
        config_revision=envelope.config_revision,
        catalog_digest=envelope.mcp_catalog.digest,
        reserved_names=frozenset(config.name for config in envelope.mcp_servers),
    )
