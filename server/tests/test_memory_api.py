from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.api.sessions import get_chat_runtime
from openctopus_server.chat.memory import memory_path
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import User


async def test_memory_editor_uses_official_store_and_detects_conflicts(user_client, test_app, pg_engine):
    runtime = ChatRuntime(pg_engine)
    test_app.dependency_overrides[get_chat_runtime] = lambda: runtime
    try:
        owner = (await user_client.get('/api/me')).json()
        path = memory_path(UUID(owner['id']))
        first = await user_client.put('/api/memory/MEMORY.md', json={'content': 'first', 'expected_version': None})
        assert first.status_code == 200
        version = first.json()['version']
        assert (await runtime.memory.store.read(path, max_chars=100)).content == 'first'
        agent_write = await runtime.memory.store.write(path, 'agent update', expected_version=version)
        conflict = await user_client.put('/api/memory/MEMORY.md', json={'content': 'stale editor', 'expected_version': version})
        assert conflict.status_code == 409
        current = (await user_client.get('/api/memory/MEMORY.md')).json()
        assert current == {'path': 'MEMORY.md', 'content': 'agent update', 'version': agent_write.version}
        assert (await user_client.delete('/api/memory/MEMORY.md', params={'expected_version': version})).status_code == 409
        assert (await user_client.delete('/api/memory/MEMORY.md', params={'expected_version': current['version']})).status_code == 204
    finally:
        await runtime.close()


async def test_memory_is_scoped_to_authenticated_user(user_client, test_app, pg_engine):
    runtime = ChatRuntime(pg_engine)
    test_app.dependency_overrides[get_chat_runtime] = lambda: runtime
    try:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            other = User(email='other@memory.test', name='other', password_hash='unused')
            db.add(other)
            await db.commit()
        await runtime.memory.store.write(memory_path(other.id), 'private to other', expected_version=None)
        assert (await user_client.get('/api/memory')).json()['paths'] == []
        assert (await user_client.get('/api/memory/MEMORY.md')).json()['content'] == ''
        escaped = await user_client.put('/api/memory/%2E%2E/other.md', json={'content': 'bad', 'expected_version': None})
        assert escaped.status_code == 400
        assert (await runtime.memory.store.read(memory_path(other.id), max_chars=100)).content == 'private to other'
    finally:
        await runtime.close()


async def test_account_deletion_waits_for_memory_write_and_blocks_resurrection(pg_engine, monkeypatch):
    import asyncio

    import pytest
    from pydantic_ai_harness.memory import PostgresMemoryStore

    from openctopus_server.chat.memory import MemoryDatabase
    from openctopus_server.errors.exceptions import WorkspaceError
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = User(email='deleting@memory.test', name='Deleting', password_hash='unused')
        db.add(owner)
        await db.commit()
    memory = MemoryDatabase(pg_engine)
    entered, release = asyncio.Event(), asyncio.Event()
    original = PostgresMemoryStore.write
    async def held_write(store, *args, **kwargs):
        entered.set()
        await release.wait()
        return await original(store, *args, **kwargs)
    monkeypatch.setattr(PostgresMemoryStore, 'write', held_write)
    async def delete_owner():
        async with AsyncSession(pg_engine) as db:
            user = await db.get(User, owner.id, with_for_update=True)
            await db.delete(user)
            await db.commit()
    write = asyncio.create_task(memory.store.write(memory_path(owner.id), 'last write', expected_version=None))
    await asyncio.wait_for(entered.wait(), 5)
    deletion = asyncio.create_task(delete_owner())
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(deletion), 0.05)
        release.set()
        await asyncio.wait_for(asyncio.gather(write, deletion), 5)
        with pytest.raises(WorkspaceError, match='Memory owner no longer exists'):
            await memory.store.write(memory_path(owner.id), 'resurrection', expected_version=None)
        await memory.purge(owner.id)
        assert await PostgresMemoryStore(memory).read(memory_path(owner.id), max_chars=100) is None
    finally:
        release.set()
        await asyncio.gather(write, deletion, return_exceptions=True)
        await memory.close()
