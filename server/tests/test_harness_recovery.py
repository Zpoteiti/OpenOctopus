"""M1 acceptance against released packages and a real PostgreSQL database."""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text

WORKER = Path(__file__).parent / "harness" / "worker.py"


async def wait_for_file(path: Path, process: subprocess.Popen, log: Path) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        failure = path.parent / "failure"
        if failure.exists():
            pytest.fail(failure.read_text() + "\n" + log.read_text()[-6000:])
        if list(path.parent.glob(path.name)):
            return
        if process.poll() is not None:
            pytest.fail(log.read_text()[-8000:])
        await asyncio.sleep(0.05)
    pytest.fail(f"Timed out waiting for {path.name}:\n{log.read_text()[-8000:]}")


@pytest.mark.parametrize(
    ("case", "barrier"),
    [("model", "model_running-alice"), ("memory_receipt", "memory_committed-alice"),
     ("foreground", "child_running-*"), ("background", "child_running-*")],
)
async def test_harness_process_restart(pg_engine, tmp_path, case, barrier):
    schema = "harness_" + uuid.uuid4().hex
    env = {
        **os.environ,
        "HARNESS_TEST_ROOT": str(tmp_path),
        "HARNESS_TEST_CASE": case,
        "HARNESS_TEST_SCHEMA": schema,
        "HARNESS_TEST_DATABASE": pg_engine.url.render_as_string(hide_password=False),
        "HARNESS_TEST_SUBMIT": "1",
    }
    log = tmp_path / "worker.log"
    with log.open("w") as output:
        process = subprocess.Popen([sys.executable, str(WORKER)], env=env, stdout=output, stderr=output)
        try:
            await wait_for_file(tmp_path / barrier, process, log)
            if case == "background":
                for user in ("alice", "bob"):
                    await wait_for_file(tmp_path / f"result-{user}", process, log)
                assert not (tmp_path / "result-child-alice").exists()
            # A model barrier is reached after a streamed preview. That preview
            # must already be delivered, before the model step can complete.
            if case == "model":
                await wait_for_file(tmp_path / "events-alice", process, log)
                assert "parent preview" in (tmp_path / "events-alice").read_text()
                assert not (tmp_path / "result-alice").exists()
            process.kill()
            await asyncio.to_thread(process.wait, timeout=10)
            (tmp_path / "continue").touch()
            env["HARNESS_TEST_SUBMIT"] = "0"
            process = subprocess.Popen([sys.executable, str(WORKER)], env=env, stdout=output, stderr=output)
            for user in ("alice", "bob"):
                await wait_for_file(tmp_path / f"result-{user}", process, log)
                result = json.loads((tmp_path / f"result-{user}").read_text())
                assert result["output"] == "parent preview parent completed"
                history = json.dumps(result["messages"])
                assert f"checkpoint-{user}" in history
                assert f"checkpoint-{'bob' if user == 'alice' else 'alice'}" not in history
                if case == "foreground":
                    assert "child completed" in history
                if case == "background":
                    await wait_for_file(tmp_path / f"result-wake-{user}", process, log)
                    wake = json.loads((tmp_path / f"result-wake-{user}").read_text())
                    assert "child preview child completed" in json.dumps(wake["messages"])
            async with pg_engine.connect() as conn:
                effects = (await conn.execute(text(
                    f'SELECT owner, count(*) FROM "{schema}".effects GROUP BY owner ORDER BY owner'
                ))).all()
                assert effects == [("alice", 1), ("bob", 1)]
                inputs = (await conn.execute(text(
                    f'SELECT owner FROM "{schema}".inputs ORDER BY owner'
                ))).scalars().all()
                assert inputs == ["alice", "bob"]
                accepted = (await conn.execute(text(
                    f'SELECT workflow_uuid FROM "{schema}".workflow_status ORDER BY workflow_uuid'
                ))).scalars().all()
                assert "rolled-back" not in accepted
                if case == "background":
                    counts = (await conn.execute(text(
                        f'SELECT name, count(*) FROM "{schema}".workflow_status GROUP BY name ORDER BY name'
                    ))).all()
                    assert counts == [("child", 2), ("main", 2), ("wake", 2)]
                memories = (await conn.execute(text(
                    f'SELECT path, content FROM "memory_{schema}" ORDER BY path'
                ))).all()
                assert memories == [
                    ("alice/main/MEMORY.md", "remember once\n"),
                    ("bob/main/MEMORY.md", "remember once\n"),
                ]
        finally:
            if process.poll() is None:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=10)
            async with pg_engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                await conn.execute(text(f'DROP TABLE IF EXISTS "memory_{schema}" CASCADE'))
                await conn.execute(text(f'DROP TABLE IF EXISTS "memory_{schema}_operations" CASCADE'))
                await conn.execute(text(f'DROP TABLE IF EXISTS "memory_{schema}_metadata" CASCADE'))
                await conn.execute(text(f'DROP SEQUENCE IF EXISTS "memory_{schema}_versions" CASCADE'))
