"""Connection ownership and authenticated paths for the official memory store."""

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast
from uuid import UUID

import asyncpg  # type: ignore[import-untyped]
from pydantic_ai_harness.memory import PostgresConnection, PostgresMemoryStore
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError

MEMORY_MAX_CHARS = 65_536


def memory_path(user_id: UUID, path: str = "MEMORY.md") -> str:
    # Namespace is always supplied by authentication, never by model arguments.
    if any(".." in part or part == "." or re.fullmatch(r"[A-Za-z0-9_.-]{1,200}", part) is None for part in path.split("/")):
        raise ValueError("Invalid memory path")
    return f"{user_id}/main/{path}"


class MemoryDatabase:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.url = engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
        self.pool: Any = None
        self.lock = asyncio.Lock()
        self.store = OwnedMemoryStore(self)

    async def purge(self, user_id: UUID) -> None:
        # Account deletion uses the official CAS API after the user lock has
        # excluded concurrent writers. Operation receipts contain no note text.
        store = PostgresMemoryStore(self)
        while paths := await store.list_paths(f"{user_id}/", limit=100):
            for path in paths:
                note = await store.read(path, max_chars=1)
                if note is not None:
                    await store.delete(path, expected_version=note.version)

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[PostgresConnection]:
        async with self.lock:
            if self.pool is None:
                self.pool = await asyncpg.create_pool(self.url, min_size=1, max_size=5)
        async with self.pool.acquire() as connection:
            yield cast(PostgresConnection, connection)

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None


class OwnedMemoryStore(PostgresMemoryStore):
    """Keep account deletion atomic with respect to authorized notebook operations."""

    def __init__(self, database: MemoryDatabase) -> None:
        super().__init__(database)
        self.database = database

    @asynccontextmanager
    async def owner(self, path: str) -> AsyncIterator[None]:
        owner = UUID(path.split("/", 1)[0])
        async with AsyncSession(self.database.engine) as db:
            row = await db.scalar(select(User.id).where(User.id == owner).with_for_update(read=True, key_share=True))
            if row is None:
                raise WorkspaceError(ErrorCode.NOT_FOUND, "Memory owner no longer exists")
            yield

    async def read(self, path: str, **kwargs: Any) -> Any:
        async with self.owner(path):
            return await super().read(path, **kwargs)

    async def write(self, path: str, content: str, **kwargs: Any) -> Any:
        async with self.owner(path):
            return await super().write(path, content, **kwargs)

    async def delete(self, path: str, **kwargs: Any) -> Any:
        async with self.owner(path):
            return await super().delete(path, **kwargs)

    async def list_paths(self, prefix: str = "", **kwargs: Any) -> list[str]:
        async with self.owner(prefix):
            return await super().list_paths(prefix, **kwargs)

    async def search(self, prefix: str, query: str, **kwargs: Any) -> Any:
        async with self.owner(prefix):
            return await super().search(prefix, query, **kwargs)
