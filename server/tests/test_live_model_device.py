"""Opt-in real model -> Server -> paired core read/convert/write acceptance."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine
from test_device_client_e2e import (
    _start_client,
    _start_server,
    _stop_client,
    _stop_server,
    _wait_online,
)
from test_harness_context import configure

from openctopus_server.api.router import router as api_router
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.config import get_settings
from openctopus_server.db.engine import get_engine
from openctopus_server.devices.dependencies import get_device_registry
from openctopus_server.errors.http import register_error_handler
from openctopus_server.tools.registry import build_py4_registry
from openctopus_server.workspace.fs import get_workspace_fs
from openctopus_server.workspace.service import WorkspaceService
from openctopus_server.workspace.storage import get_object_storage

pytestmark = pytest.mark.skipif(
    not os.environ.get("OO_TEST_MODEL_ENDPOINT") or os.environ.get("PY5_REAL_E2E") != "1",
    reason="requires explicit live model configuration and PY5_REAL_E2E=1",
)


async def test_live_model_reads_converts_and_writes_on_paired_client(
    pg_engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        "OPENOCTOPUS_DATABASE_URL", pg_engine.url.render_as_string(hide_password=False),
    )
    get_settings.cache_clear()
    get_engine.cache_clear()
    registry = get_device_registry()
    await configure(
        pg_engine, llm_protocol="openai", llm_endpoint=os.environ["OO_TEST_MODEL_ENDPOINT"],
        llm_model=os.environ["OO_TEST_MODEL_NAME"],
        llm_api_key=os.environ.get("OO_TEST_MODEL_KEY", "local"),
        llm_max_output_tokens=2048, llm_max_context_tokens=262144,
    )
    marker = f"TRAY_SMOKE_{uuid4().hex}"
    (tmp_path / "seed.txt").write_text(marker, encoding="utf-8")
    shutil.copyfile(Path(__file__).parent / "fixtures/documents/sample.pdf", tmp_path / "sample.pdf")
    app = FastAPI()
    app.include_router(api_router)
    register_error_handler(app)
    storage = get_object_storage()
    workspace_fs = get_workspace_fs(storage)
    service = WorkspaceService(workspace_fs)
    runtime = ChatRuntime(
        pg_engine, device_registry=registry, workspace_service=service,
        tool_registry=build_py4_registry(pg_engine, service, workspace_fs, device_registry=registry),
    )
    app.state.chat_runtime = runtime
    server, task, url, listener = await _start_server(app)
    process = None
    token = None
    try:
        async with httpx.AsyncClient(base_url=url, timeout=120, trust_env=False) as client:
            auth = await client.post("/api/auth/register", json={
                "email": f"tray-smoke-{uuid4().hex}@example.com",
                "password": "testpassword", "name": "Tray smoke",
            })
            assert auth.status_code == 201
            jwt = auth.json()["jwt"]
            client.headers["Authorization"] = f"Bearer {jwt}"
            device = await client.post("/api/devices", json={
                "name": "tray-smoke", "workspace_path": str(tmp_path),
                "restrict_to_workspace": True, "ssrf_denylist": [],
            })
            assert device.status_code == 201
            token = device.json()["token"]
            process = await _start_client(url, token)
            await _wait_online(client, jwt, "tray-smoke", online=True, process=process)
            events = []
            async with asyncio.timeout(60), client.stream(
                "POST", f"/api/sessions/{uuid4()}/messages", json={
                    "content": [{"type": "text", "text": (
                        "On device tray-smoke use read_file to read seed.txt and sample.pdf. "
                        "Then use write_file to write result.txt containing ONLY the exact "
                        "marker from seed.txt, without line numbers. In your final reply, "
                        "briefly describe what the PDF says. Do not use exec or other tools."
                    )}], "attachments": [],
                },
            ) as response:
                assert response.status_code == 200
                persisted = 0
                async for line in response.aiter_lines():
                    event = json.loads(line)
                    events.append(event)
                    if event["type"] == "message_persisted":
                        persisted += 1
                        assert persisted <= 20, events[-5:]
            assert events[-1]["type"] == "turn_finished"
            assert events[-1]["status"] == "completed", events
            assert (tmp_path / "result.txt").read_text(encoding="utf-8").strip() == marker
            calls = [
                block for event in events if event["type"] == "message_persisted"
                for block in event["message"]["content"] if block["type"] == "tool_use"
            ]
            assert any(
                call["name"] == "read_file" and call["input"].get("path") == "sample.pdf"
                for call in calls
            )
            assert any(event["type"] == "token_delta" for event in events)
    finally:
        if process is not None:
            await _stop_client(process, expected_returncode=0, secret=token)
        await _stop_server(server, task, listener, registry, pg_engine, runtime)
        await storage.close()
