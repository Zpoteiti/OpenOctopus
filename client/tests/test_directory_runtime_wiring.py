from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Any, cast

import pytest

import openoctopus_client.tools.directory_jobs as directory_jobs_module
from openoctopus_client.config import load_config
from openoctopus_client.connection import (
    ClientRuntime,
    _DirectoryControlWorker,
    _is_directory_tool_call,
    _ToolWorker,
)
from openoctopus_client.protocol import DeviceConfig, McpRoute, ToolCall, ToolResult, new_uuid7
from openoctopus_client.tools import ToolOutput
from openoctopus_client.tools.directory_jobs import DirectoryJobManager
from openoctopus_client.tools.workspace_rest import DirectoryCommandResult


def _environment() -> dict[str, str]:
    return {
        "OPENOCTOPUS_SERVER_URL": "https://openoctopus.example:8443",
        "OPENOCTOPUS_DEVICE_TOKEN": "openoctopus_dev_secret-value",
    }


def _call(operation: str, *, route: McpRoute | None = None) -> ToolCall:
    return ToolCall(
        id=new_uuid7(),
        name="mcp_files_status" if route is not None else "__workspace_rest__",
        args={
            "operation": operation,
            "directory_operation_id": str(new_uuid7()),
            "expected_digest": "a" * 64,
        },
        max_result_bytes=4096,
        mcp_route=route,
    )


async def _directory_handle(manager: DirectoryJobManager, raw_action: object) -> Any:
    return await manager.handle(raw_action)


def test_directory_discriminator_is_exact_and_never_claims_mcp_calls() -> None:
    assert _is_directory_tool_call(_call("transfer_directory_status"))
    assert not _is_directory_tool_call(_call("transfer_directory_status_typo"))
    malformed = _call("transfer_directory_status").model_copy(update={"args": {"operation": []}})
    assert not _is_directory_tool_call(malformed)
    assert not _is_directory_tool_call(
        _call(
            "transfer_directory_status",
            route=McpRoute(
                entry_id=new_uuid7(),
                config_revision=1,
                catalog_digest="a" * 64,
                runtime_generation=new_uuid7(),
            ),
        )
    )


def test_directory_control_lane_is_not_blocked_by_the_ordinary_tool_lane() -> None:
    async def exercise() -> None:
        ordinary_started = asyncio.Event()
        release_ordinary = asyncio.Event()
        directory_seen = asyncio.Event()

        class Writer:
            def __init__(self) -> None:
                self.frames: list[dict[str, Any]] = []

            def enqueue_normal(self, payload: str) -> None:
                self.frames.append(cast(dict[str, Any], json.loads(payload)))
                if self.frames[-1].get("id") == str(directory_call.id):
                    directory_seen.set()

        class OrdinaryDispatcher:
            async def execute(self, name: str, args: dict[str, Any]) -> ToolOutput:
                del name, args
                ordinary_started.set()
                await release_ordinary.wait()
                return ToolOutput("ordinary finished")

        class DirectoryManager:
            async def handle(self, raw_action: object) -> DirectoryCommandResult:
                del raw_action
                return DirectoryCommandResult(state="accepted", expected_digest="a" * 64)

        runtime = ClientRuntime(load_config(_environment()))
        writer = Writer()
        ordinary_worker = _ToolWorker(runtime, cast(Any, writer))
        directory_worker = _DirectoryControlWorker(runtime, cast(Any, writer))
        ordinary_call = ToolCall(
            id=new_uuid7(),
            name="read_file",
            args={"path": "blocked.txt"},
            max_result_bytes=4096,
        )
        directory_call = _call("transfer_directory_status")
        try:
            assert ordinary_worker.enqueue(ordinary_call, OrdinaryDispatcher())
            await asyncio.wait_for(ordinary_started.wait(), timeout=1)
            assert directory_worker.enqueue(directory_call, cast(Any, DirectoryManager()))
            await asyncio.wait_for(directory_seen.wait(), timeout=1)
            result = next(frame for frame in writer.frames if frame["id"] == str(directory_call.id))
            assert result["type"] == "tool_result"
            assert result["is_error"] is False
            assert json.loads(cast(str, result["content"])) == {
                "state": "accepted",
                "expected_digest": "a" * 64,
            }
        finally:
            release_ordinary.set()
            await ordinary_worker.stop()
            await directory_worker.stop()

    asyncio.run(exercise())


def test_directory_control_worker_bounds_eight_waiting_calls() -> None:
    async def exercise() -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        class Runtime:
            async def _run_directory_tool(self, call: ToolCall, manager: object) -> ToolResult:
                del call, manager
                started.set()
                await release.wait()
                return ToolResult(id=new_uuid7(), content="finished", is_error=False)

        worker = _DirectoryControlWorker(cast(Any, Runtime()), cast(Any, object()))
        manager: Any = object()
        try:
            assert worker.enqueue(_call("transfer_directory_status"), manager)
            await asyncio.wait_for(started.wait(), timeout=1)
            assert all(
                worker.enqueue(_call("transfer_directory_status"), manager) for _ in range(8)
            )
            assert not worker.enqueue(_call("transfer_directory_status"), manager)
        finally:
            release.set()
            await worker.stop()

    asyncio.run(exercise())


def test_runtime_retires_config_bound_directory_managers_but_keeps_old_reconcile(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        source = tmp_path / "source.txt"
        source.write_text("content", encoding="utf-8")
        runtime = ClientRuntime(load_config(_environment()))
        config = DeviceConfig(
            workspace_path=str(tmp_path),
            restrict_to_workspace=True,
            ssrf_denylist=[],
            shell_timeout_max=60,
            env_allowlist=[],
            mcp_servers=[],
        )
        old = runtime._new_directory_manager(tmp_path, config, generation=7)
        runtime._directory_manager = old
        operation_id = str(new_uuid7())
        started = await _directory_handle(
            old,
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": operation_id,
                "path": "source.txt",
            },
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5
        while loop.time() < deadline:
            status = await _directory_handle(
                old,
                {
                    "operation": "transfer_source_probe_status",
                    "directory_operation_id": operation_id,
                    "expected_digest": started.expected_digest,
                },
            )
            if status.state == "succeeded":
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("source probe did not finish")

        replacement = runtime._new_directory_manager(tmp_path, config, generation=7)
        runtime._directory_manager = replacement
        await runtime._retire_directory_manager(
            old,
            preserve_finalized=True,
            drop_when_drained=False,
        )
        reconcile = ToolCall(
            id=new_uuid7(),
            name="__workspace_rest__",
            args={
                "operation": "transfer_source_probe_status",
                "directory_operation_id": operation_id,
                "expected_digest": started.expected_digest,
            },
            max_result_bytes=4096,
        )
        new_calls = (
            ToolCall(
                id=new_uuid7(),
                name="__workspace_rest__",
                args={
                    "operation": "transfer_source_probe_start",
                    "directory_operation_id": str(new_uuid7()),
                    "path": "source.txt",
                },
                max_result_bytes=4096,
            ),
            ToolCall(
                id=new_uuid7(),
                name="__workspace_rest__",
                args={
                    "operation": "transfer_directory_preflight",
                    "directory_operation_id": str(new_uuid7()),
                    "dst_path": "destination",
                    "manifest": {},
                },
                max_result_bytes=4096,
            ),
        )

        assert replacement.generation == old.generation == 7
        assert old in runtime._retired_directory_managers
        assert runtime._directory_manager_for_call(reconcile, generation=7, allow_new=False) is old
        for new_call in new_calls:
            assert (
                runtime._directory_manager_for_call(new_call, generation=7, allow_new=False) is None
            )
            assert (
                runtime._directory_manager_for_call(new_call, generation=7, allow_new=True)
                is replacement
            )
        result = await runtime._run_directory_tool(reconcile, old)
        assert result.is_error is False
        assert json.loads(cast(str, result.content))["state"] == "succeeded"

        await runtime._retire_directory_generation(7)
        assert runtime._directory_manager is None
        assert not runtime._directory_managers_for_generation(7)

    asyncio.run(exercise())


def test_runtime_releases_retired_manager_after_its_lifecycle_expires(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(directory_jobs_module, "TOMBSTONE_TTL_SECONDS", 0.2)

    async def exercise() -> None:
        source = tmp_path / "source.txt"
        source.write_text("content", encoding="utf-8")
        runtime = ClientRuntime(load_config(_environment()))
        config = DeviceConfig(
            workspace_path=str(tmp_path),
            restrict_to_workspace=True,
            ssrf_denylist=[],
            shell_timeout_max=60,
            env_allowlist=[],
            mcp_servers=[],
        )
        retired = runtime._new_directory_manager(tmp_path, config, generation=9)
        retired._terminal_ttl = 0.2
        runtime._directory_manager = retired
        operation_id = str(new_uuid7())
        started = await _directory_handle(
            retired,
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": operation_id,
                "path": "source.txt",
            },
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5
        while loop.time() < deadline:
            status = await _directory_handle(
                retired,
                {
                    "operation": "transfer_source_probe_status",
                    "directory_operation_id": operation_id,
                    "expected_digest": started.expected_digest,
                },
            )
            if status.state == "succeeded":
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("source probe did not finish")

        await runtime._retire_directory_manager(
            retired,
            preserve_finalized=True,
            drop_when_drained=False,
        )
        assert retired in runtime._retired_directory_managers
        async with asyncio.timeout(1):
            while retired in runtime._retired_directory_managers:
                await asyncio.sleep(0.05)
        assert runtime._directory_lifecycle_credits.active_count == 0

    asyncio.run(exercise())


def test_generation_retire_is_bounded_while_blocking_work_drains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    original_scan = directory_jobs_module._scan_source_path

    def blocked_scan(*args: Any, **kwargs: Any) -> Any:
        entered.set()
        release.wait(timeout=5)
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(directory_jobs_module, "_scan_source_path", blocked_scan)

    async def exercise() -> None:
        source = tmp_path / "source"
        source.mkdir()
        (source / "file").write_text("content", encoding="utf-8")
        runtime = ClientRuntime(load_config(_environment()))
        config = DeviceConfig(
            workspace_path=str(tmp_path),
            restrict_to_workspace=True,
            ssrf_denylist=[],
            shell_timeout_max=60,
            env_allowlist=[],
            mcp_servers=[],
        )
        manager = runtime._new_directory_manager(tmp_path, config, generation=11)
        runtime._directory_manager = manager
        await manager.handle(
            {
                "operation": "transfer_source_probe_start",
                "directory_operation_id": str(new_uuid7()),
                "path": "source",
            }
        )
        assert await asyncio.to_thread(entered.wait, 1)

        started = asyncio.get_running_loop().time()
        await runtime._retire_directory_manager(
            manager,
            preserve_finalized=False,
            drop_when_drained=True,
        )
        assert asyncio.get_running_loop().time() - started < 0.3
        assert manager in runtime._retired_directory_managers
        assert manager in runtime._directory_retirement_tasks
        assert runtime._directory_lifecycle_credits.active_count == 1

        release.set()
        async with asyncio.timeout(1):
            while manager in runtime._retired_directory_managers:
                await asyncio.sleep(0.05)
        assert runtime._directory_lifecycle_credits.active_count == 0

    try:
        asyncio.run(exercise())
    finally:
        release.set()
