import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from test_harness_recovery import wait_for_file

from openctopus_server.services.messages import request_cancel


@pytest.mark.parametrize("case, barrier, stop", [
    ("model", "model_running", False), ("tool_receipt", "effect_committed", False),
    ("handoff", "handoff_committed", False), ("model", "model_running", True),
    ("tool_receipt", "effect_committed", True), ("cron", "effect_committed", False),
    ("skills", "skills_running", False), ("graceful", "model_running", False),
])
async def test_production_runtime_reconciles_execution_after_restart(pg_engine, tmp_path, case, barrier, stop):
    env = {**os.environ, "HARNESS_TEST_ROOT": str(tmp_path), "HARNESS_TEST_CASE": case,
           "HARNESS_TEST_DATABASE": pg_engine.url.render_as_string(hide_password=False), "HARNESS_TEST_SUBMIT": "1"}
    log = tmp_path / "worker.log"
    worker = Path(__file__).parent / "harness" / "runtime_worker.py"
    with log.open("w") as output:
        process = subprocess.Popen([sys.executable, str(worker)], env=env, stdout=output, stderr=output)
        try:
            await wait_for_file(tmp_path / barrier, process, log)
            accepted = json.loads((tmp_path / "accepted").read_text())
            if case == "graceful":
                (tmp_path / "shutdown_live").touch()
                await wait_for_file(tmp_path / "closed", process, log)
                await asyncio.to_thread(process.wait, timeout=10)
                (tmp_path / "shutdown_live").unlink()
            else:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=10)
            if stop:
                async with AsyncSession(pg_engine) as db:
                    owner = await db.scalar(text("SELECT user_id FROM sessions WHERE id=:id"), {"id": accepted["session"]})
                    assert await request_cancel(db, user_id=owner, session_id=UUID(accepted["session"]))
            (tmp_path / "continue").touch()
            env["HARNESS_TEST_SUBMIT"] = "0"
            process = subprocess.Popen([sys.executable, str(worker)], env=env, stdout=output, stderr=output)
            deadline = time.monotonic() + 40
            messages = []
            while time.monotonic() < deadline:
                async with pg_engine.connect() as conn:
                    messages = (await conn.execute(text("SELECT content FROM messages WHERE session_id = :session ORDER BY created_at, id"), {"session": accepted["session"]})).scalars().all()
                    running = await conn.scalar(text("SELECT count(*) FROM turn_runs WHERE status='running'"))
                if not running and any(("User pressed stop" if stop else "completed") in json.dumps(message) for message in messages):
                    break
                if process.poll() is not None:
                    pytest.fail(log.read_text()[-15000:])
                await asyncio.sleep(0.1)
            else:
                pytest.fail(json.dumps(messages) + "\n" + log.read_text()[-20000:])
            async with pg_engine.connect() as conn:
                assert await conn.scalar(text("SELECT count(*) FROM recovery_effects")) == 1
                assert await conn.scalar(text("SELECT count(*) FROM messages WHERE message_kind='assistant'")) == (1 if stop else 3 if case in {"handoff", "skills"} else 2)
                assert await conn.scalar(text("SELECT count(*) FROM messages WHERE message_kind IN ('tool_result','synthetic_tool_result')")) == (2 if case == "skills" else 1)
                if stop:
                    assert await conn.scalar(text("SELECT count(*) FROM turn_runs WHERE status='cancelled'")) == 1
                    assert await conn.scalar(text("SELECT status FROM dbos.workflow_status WHERE workflow_uuid=:id"), {"id": accepted["turn"]}) == "CANCELLED"
            if case in {"tool_receipt", "cron"}:
                assert "tool_execution_outcome_unknown" in json.dumps(messages)
        finally:
            if process.poll() is None:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=10)
            async with pg_engine.begin() as conn:
                await conn.execute(text("DROP SCHEMA IF EXISTS dbos CASCADE"))
                await conn.execute(text("DROP TABLE IF EXISTS recovery_effects"))


@pytest.mark.parametrize("case", ["child_foreground", "child_background"])
@pytest.mark.parametrize("action", ["resume", "stop", "delete"])
async def test_production_child_resumes_and_reports_to_parent(pg_engine, tmp_path, case, action):
    env = {**os.environ, "HARNESS_TEST_ROOT": str(tmp_path), "HARNESS_TEST_CASE": case,
           "HARNESS_TEST_DATABASE": pg_engine.url.render_as_string(hide_password=False), "HARNESS_TEST_SUBMIT": "1"}
    log = tmp_path / "worker.log"
    worker = Path(__file__).parent / "harness" / "runtime_worker.py"
    with log.open("w") as output:
        process = subprocess.Popen([sys.executable, str(worker)], env=env, stdout=output, stderr=output)
        try:
            await wait_for_file(tmp_path / "child_running", process, log)
            accepted = json.loads((tmp_path / "accepted").read_text())
            from openctopus_server.services.messages import get_messages_response
            async with AsyncSession(pg_engine) as db:
                owner = await db.scalar(text("SELECT user_id FROM sessions WHERE id=:id"), {'id': accepted['session']})
                snapshot = await get_messages_response(db, user_id=owner, session_id=UUID(accepted['session']), before=None, after=None, limit=10)
                assert snapshot.active_delegate_count == 1
            process.kill()
            await asyncio.to_thread(process.wait, timeout=10)
            if action != "resume":
                async with AsyncSession(pg_engine) as db:
                    owner = await db.scalar(text("SELECT user_id FROM sessions WHERE id=:id"), {'id': accepted['session']})
                    if action == "stop":
                        assert await request_cancel(db, user_id=owner, session_id=UUID(accepted['session']))
                    else:
                        from openctopus_server.chat.runner import ChatRuntime
                        from openctopus_server.services.sessions import delete_owned
                        runtime = ChatRuntime(pg_engine)
                        try:
                            await delete_owned(db, user_id=owner, session_id=UUID(accepted['session']), runtime=runtime)
                        finally:
                            await runtime.close()
            (tmp_path / "continue").touch()
            env["HARNESS_TEST_SUBMIT"] = "0"
            process = subprocess.Popen([sys.executable, str(worker)], env=env, stdout=output, stderr=output)
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                async with pg_engine.connect() as conn:
                    pending = await conn.scalar(text("SELECT count(*) FROM dbos.workflow_status WHERE status IN ('PENDING','ENQUEUED')"))
                    done = await conn.scalar(text("SELECT count(*) FROM agent_tasks WHERE status=:status"), {'status': 'completed' if action == 'resume' else 'cancelled'})
                    if action == 'delete':
                        done = 1 - await conn.scalar(text("SELECT count(*) FROM sessions"))
                if not pending and done == 1:
                    break
                if process.poll() is not None:
                    pytest.fail(log.read_text()[-15000:])
                await asyncio.sleep(0.1)
            else:
                pytest.fail(log.read_text()[-20000:])
            async with pg_engine.connect() as conn:
                assert await conn.scalar(text("SELECT count(*) FROM recovery_effects")) == 1
                assert await conn.scalar(text("SELECT count(*) FROM turn_runs WHERE status='running'")) == 0
                if action == 'delete':
                    assert await conn.scalar(text("SELECT count(*) FROM messages")) == 0
                    return
                task = (await conn.execute(text("SELECT * FROM agent_tasks"))).mappings().one()
                if action == 'stop':
                    assert task['status'] == 'cancelled'
                    assert await conn.scalar(text("SELECT count(*) FROM messages WHERE content::text LIKE '%child completed%'")) == 0
                    return
                assert str(task['parent_session_id']) == accepted['session']
                child_text = await conn.scalar(text("SELECT string_agg(content::text,' ') FROM messages WHERE session_id=:id"), {'id': task['session_id']})
                assert 'independent child task' in child_text and 'child completed' in child_text
                parent_text = await conn.scalar(text("SELECT string_agg(content::text,' ') FROM messages WHERE session_id=:id"), {'id': accepted['session']})
                assert 'completed after delegate' in parent_text
                if case == 'child_background':
                    assert await conn.scalar(text("SELECT count(*) FROM messages WHERE sender_id='openoctopus:delegate' AND session_id=:id"), {'id': accepted['session']}) == 1
                    assert await conn.scalar(text("SELECT count(*) FROM messages WHERE message_kind='assistant' AND session_id=:id"), {'id': accepted['session']}) == 3
        finally:
            if process.poll() is None:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=10)
            async with pg_engine.begin() as conn:
                await conn.execute(text("DROP SCHEMA IF EXISTS dbos CASCADE"))
                await conn.execute(text("DROP TABLE IF EXISTS recovery_effects"))


@pytest.mark.parametrize('case, barrier', [('model', 'model_running'), ('child_background', 'child_running')])
async def test_live_cancellation_stops_late_model_output(pg_engine, tmp_path, case, barrier):
    env = {**os.environ, 'HARNESS_TEST_ROOT': str(tmp_path), 'HARNESS_TEST_CASE': case,
           'HARNESS_TEST_DATABASE': pg_engine.url.render_as_string(hide_password=False), 'HARNESS_TEST_SUBMIT': '1'}
    log = tmp_path / 'worker.log'
    with log.open('w') as output:
        process = subprocess.Popen([sys.executable, str(Path(__file__).parent / 'harness/runtime_worker.py')], env=env, stdout=output, stderr=output)
        try:
            await wait_for_file(tmp_path / barrier, process, log)
            (tmp_path / 'stop_live').touch()
            await wait_for_file(tmp_path / 'cancelled_live', process, log)
            (tmp_path / 'continue').touch()
            # Cancellation is observed by DBOS at durable boundaries; allow the
            # released model coroutine to reach that boundary before inspecting.
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                async with pg_engine.connect() as conn:
                    pending = await conn.scalar(text("SELECT count(*) FROM turn_runs WHERE status='running'"))
                if not pending:
                    break
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.3)
            async with pg_engine.connect() as conn:
                assert await conn.scalar(text("SELECT count(*) FROM turn_runs WHERE status='running'")) == 0
                assert await conn.scalar(text("SELECT count(*) FROM messages WHERE message_kind='assistant' AND content::text LIKE '%child completed%'")) == 0
                if case == 'model':
                    assert await conn.scalar(text("SELECT count(*) FROM messages WHERE message_kind='assistant' AND content::text LIKE '%completed%'")) == 0
                assert await conn.scalar(text("SELECT count(*) FROM recovery_effects")) == 1
        finally:
            if process.poll() is None:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=10)
            async with pg_engine.begin() as conn:
                await conn.execute(text('DROP SCHEMA IF EXISTS dbos CASCADE'))
                await conn.execute(text('DROP TABLE IF EXISTS recovery_effects'))
