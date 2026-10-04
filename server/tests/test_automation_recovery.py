import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import text
from test_harness_recovery import wait_for_file

from openctopus_server.chat.memory import MemoryDatabase, memory_path


@pytest.mark.parametrize('kind', ['heartbeat', 'dream'])
async def test_periodic_wakeup_recovers_without_duplicate_effects(pg_engine, tmp_path, kind):
    env = {**os.environ, 'HARNESS_TEST_ROOT': str(tmp_path), 'HARNESS_TEST_CASE': kind,
           'HARNESS_TEST_DATABASE': pg_engine.url.render_as_string(hide_password=False), 'HARNESS_TEST_SUBMIT': '1'}
    log = tmp_path / 'worker.log'
    worker = Path(__file__).parent / 'harness/automation_worker.py'
    with log.open('w') as output:
        process = subprocess.Popen([sys.executable, str(worker)], env=env, stdout=output, stderr=output)
        try:
            await wait_for_file(tmp_path / 'effect_committed', process, log)
            process.kill()
            await asyncio.to_thread(process.wait, timeout=10)
            (tmp_path / 'continue').touch()
            env['HARNESS_TEST_SUBMIT'] = '0'
            process = subprocess.Popen([sys.executable, str(worker)], env=env, stdout=output, stderr=output)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                async with pg_engine.connect() as conn:
                    if kind == 'dream':
                        done = await conn.scalar(text("SELECT count(*) FROM dream_runs WHERE status='updated'"))
                    else:
                        done = await conn.scalar(text("SELECT count(*) FROM messages WHERE message_kind='assistant' AND content::text LIKE '%completed wake-up%'"))
                if done == 1:
                    break
                if process.poll() is not None:
                    pytest.fail(log.read_text()[-10000:])
                await asyncio.sleep(0.05)
            else:
                pytest.fail(log.read_text()[-16000:])
            async with pg_engine.connect() as conn:
                if kind == 'dream':
                    memory = MemoryDatabase(pg_engine)
                    try:
                        note = await memory.store.read(memory_path(UUID((tmp_path / 'owner').read_text())), max_chars=1000)
                        assert note.content == 'Durable preference.\n'
                    finally:
                        await memory.close()
                    assert await conn.scalar(text('SELECT count(*) FROM dream_runs')) == 1
                else:
                    assert await conn.scalar(text('SELECT count(*) FROM recovery_effects')) == 1
                    assert await conn.scalar(text("SELECT count(*) FROM messages WHERE message_kind='human'")) == 1
        finally:
            if process.poll() is None:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=10)
            async with pg_engine.begin() as conn:
                await conn.execute(text('DROP SCHEMA IF EXISTS dbos CASCADE'))
                await conn.execute(text('DROP TABLE IF EXISTS recovery_effects'))
