#!/usr/bin/env python3
"""Measure bounded private Server MCP clients against a loopback HTTP server."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID, uuid5

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from openctopus_server.devices.mcp_models import (
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SourceMcpTool,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.mcp.catalog import discover_server_catalog
from openctopus_server.mcp.models import empty_server_mcp_envelope, parse_server_mcp_configs
from openctopus_server.mcp.routes import build_composite_mcp_snapshot
from openctopus_server.mcp.supervisor import ServerMcpSupervisor
from openctopus_server.services import server_mcp
from openctopus_server.tools.base import ToolResult

_NAMESPACE = UUID("f78285ed-5b83-4f87-8871-e647d3b95a1b")
_DEFAULT_USERS = 500
_DEFAULT_CLIENTS = 8
_MAX_RSS_GROWTH_BYTES = 128 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    users: int = _DEFAULT_USERS
    max_clients: int = _DEFAULT_CLIENTS
    sample_interval_seconds: float = 0.005

    def normalized(self) -> HarnessConfig:
        if not 1 <= self.max_clients <= 32:
            raise ValueError("max clients must be in 1..32")
        if self.users <= self.max_clients:
            raise ValueError("users must exceed the private client cap")
        if self.sample_interval_seconds <= 0:
            raise ValueError("sample interval must be positive")
        return self


@dataclass(frozen=True, slots=True)
class _ProcessSample:
    rss_bytes: int | None
    fd_count: int | None
    task_count: int


def _process_sample() -> _ProcessSample:
    rss_bytes: int | None = None
    fd_count: int | None = None
    try:
        with open("/proc/self/statm", encoding="ascii") as statm:
            rss_bytes = int(statm.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, OSError, ValueError):
        try:
            peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            rss_bytes = peak if sys.platform == "darwin" else peak * 1024
        except (OSError, ValueError):
            pass
    try:
        fd_count = len(os.listdir("/proc/self/fd"))
    except (FileNotFoundError, OSError):
        fd_count = None
    return _ProcessSample(rss_bytes, fd_count, len(asyncio.all_tasks()))


class _SearchMcpApplication:
    """Observable MCP endpoint with one live server session per private client."""

    def __init__(self, expected_parallel_searches: int) -> None:
        self.expected_parallel_searches = expected_parallel_searches
        self.initialize_requests = 0
        self.session_ids: set[str] = set()
        self.closed_session_ids: set[str] = set()
        self.search_requests = 0
        self.active_search_requests = 0
        self.active_search_requests_high_water = 0
        self.search_sessions: set[str] = set()
        self.all_searches_started = asyncio.Event()
        self.release_searches = asyncio.Event()
        self.app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
        self.app.add_api_route("/mcp", self.handle, methods=["POST", "DELETE"])

    async def handle(self, request: Request) -> Response:
        if request.method == "DELETE":
            session_id = request.headers.get("mcp-session-id")
            if session_id is not None:
                self.closed_session_ids.add(session_id)
            return Response(status_code=200)
        payload = cast(dict[str, Any], await request.json())
        if "id" not in payload:
            return Response(status_code=202)
        request_id = payload["id"]
        method = payload.get("method")
        headers: dict[str, str] = {}
        if method == "initialize":
            self.initialize_requests += 1
            session_id = f"capacity-session-{self.initialize_requests}"
            self.session_ids.add(session_id)
            headers["mcp-session-id"] = session_id
            params = cast(dict[str, Any], payload["params"])
            result: dict[str, object] = {
                "protocolVersion": params["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "capacity-search", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [{
                    "name": "search",
                    "description": "Search the local capacity fixture",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"],
                        "additionalProperties": False,
                    },
                }]
            }
        elif method == "tools/call":
            params = cast(dict[str, Any], payload["params"])
            if params.get("name") != "search":
                raise ValueError("unknown MCP tool")
            arguments = cast(dict[str, Any], params["arguments"])
            query = arguments.get("query")
            if not isinstance(query, str):
                raise ValueError("search query must be a string")
            session_id = request.headers.get("mcp-session-id")
            if session_id is None:
                raise ValueError("private MCP session header is missing")
            self.search_sessions.add(session_id)
            self.search_requests += 1
            self.active_search_requests += 1
            self.active_search_requests_high_water = max(
                self.active_search_requests_high_water, self.active_search_requests
            )
            if self.active_search_requests >= self.expected_parallel_searches:
                self.all_searches_started.set()
            try:
                await self.release_searches.wait()
            finally:
                self.active_search_requests -= 1
            result = {"content": [{"type": "text", "text": f"capacity result for {query}"}]}
        else:
            raise ValueError(f"unexpected MCP method {method!r}")
        return JSONResponse(
            {"jsonrpc": "2.0", "id": request_id, "result": result},
            headers=headers,
        )


class _LoopbackMcpServer:
    def __init__(self, application: _SearchMcpApplication) -> None:
        self.application = application
        self.listener: socket.socket | None = None
        self.server: uvicorn.Server | None = None
        self.task: asyncio.Task[None] | None = None

    @property
    def connection_count(self) -> int:
        return len(self.server.server_state.connections) if self.server is not None else 0

    async def start(self) -> str:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(512)
        listener.setblocking(False)
        port = cast(tuple[str, int], listener.getsockname())[1]
        server = uvicorn.Server(uvicorn.Config(
            self.application.app, host="127.0.0.1", port=port,
            lifespan="off", log_config=None, access_log=False,
        ))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        self.listener, self.server, self.task = listener, server, task
        while not server.started:
            if task.done():
                await task
                raise RuntimeError("loopback MCP server stopped before startup")
            await asyncio.sleep(0.001)
        return f"http://127.0.0.1:{port}/mcp"

    async def stop(self) -> None:
        if self.server is not None:
            self.server.should_exit = True
        if self.task is not None:
            await asyncio.wait_for(self.task, timeout=5)
        if self.listener is not None:
            self.listener.close()


async def _sample_until_stopped(
    stop: asyncio.Event,
    server: _LoopbackMcpServer,
    interval: float,
    peaks: dict[str, int],
) -> None:
    while not stop.is_set():
        sample = _process_sample()
        peaks["tasks"] = max(peaks["tasks"], sample.task_count)
        if sample.rss_bytes is not None:
            peaks["rss"] = max(peaks["rss"], sample.rss_bytes)
        if sample.fd_count is not None:
            peaks["fds"] = max(peaks["fds"], sample.fd_count)
        peaks["http_connections"] = max(peaks["http_connections"], server.connection_count)
        await asyncio.sleep(interval)


async def run_harness(config: HarnessConfig = HarnessConfig()) -> dict[str, Any]:
    config = config.normalized()
    baseline = _process_sample()
    started = time.perf_counter()
    application = _SearchMcpApplication(config.max_clients)
    server = _LoopbackMcpServer(application)
    supervisor = ServerMcpSupervisor(
        discoverer=discover_server_catalog,
        max_clients=config.max_clients,
        max_stdio_clients=config.max_clients,
        max_starting=config.max_clients,
    )
    peaks = {"rss": 0, "fds": 0, "tasks": 0, "http_connections": 0}
    stop_sampling = asyncio.Event()
    sampler: asyncio.Task[None] | None = None
    server_started = False
    first_tasks: list[asyncio.Task[ToolResult]] = []
    failure: str | None = None
    outcomes = {"issued": 0, "busy": 0, "completed": 0, "reused": 0, "evicted": 0}
    active_high_water = 0
    idle_after_calls = -1
    initial_search_sessions = -1
    sessions_after_shutdown = -1
    connections_after_shutdown = -1
    try:
        url = await server.start()
        server_started = True
        sampler = asyncio.create_task(_sample_until_stopped(
            stop_sampling, server, config.sample_interval_seconds, peaks
        ))
        configs = parse_server_mcp_configs([{
            "name": "search", "transport": "streamable_http", "url": url,
            "headers": {}, "enabled_capabilities": [],
            "max_concurrent_calls": config.max_clients,
        }])
        source = SourceMcpCatalog(version=1, servers=[SourceMcpServerCatalog(
            name="search", tools=[SourceMcpTool(
                raw_name="search",
                description="Search the local capacity fixture",
                input_schema={
                    "type": "object", "properties": {"query": {"type": "string"}},
                    "required": ["query"], "additionalProperties": False,
                },
            )],
        )])
        envelope = server_mcp.build_candidate_envelope(
            empty_server_mcp_envelope(), configs,
            validate_servers=("search",), source_catalog=source,
        )
        await supervisor.start(envelope)

        async def call(index: int) -> ToolResult:
            user_id = uuid5(_NAMESPACE, f"user-{index}")
            session_id = uuid5(_NAMESPACE, f"conversation-{index}")
            async with supervisor.run(user_id=user_id, session_id=session_id):
                private, generations = await supervisor.prepare(
                    user_id=user_id, session_id=session_id, envelope=envelope
                )
                route = build_composite_mcp_snapshot(
                    private, [], runtime_generations=generations
                ).server_routes[0]
                return await supervisor.dispatch_server_mcp(
                    route=route, user_id=user_id, session_id=session_id,
                    name=route.final_name, args={"query": f"query-{index}"},
                )

        first_tasks = [
            asyncio.create_task(call(index))
            for index in range(config.max_clients)
        ]
        await asyncio.wait_for(application.all_searches_started.wait(), timeout=15)
        active_high_water = supervisor.runtime_snapshot(envelope)["search"].active_sessions
        overflow = await asyncio.gather(
            *(call(index) for index in range(config.max_clients, config.users)),
            return_exceptions=True,
        )
        outcomes["busy"] = sum(
            isinstance(value, ConfigError) and value.code is ErrorCode.TOOL_MCP_BUSY
            for value in overflow
        )
        if outcomes["busy"] != len(overflow):
            raise RuntimeError(f"overflow calls were not rejected: {overflow[:3]!r}")
        application.release_searches.set()
        completed = await asyncio.gather(*first_tasks)
        outcomes["issued"] = len(first_tasks)
        outcomes["completed"] = sum(not value.is_error for value in completed)
        idle_after_calls = supervisor.runtime_snapshot(envelope)["search"].idle_sessions
        initial_search_sessions = len(application.search_sessions)

        initialized = application.initialize_requests
        await call(0)
        outcomes["reused"] = int(application.initialize_requests == initialized)
        await call(config.max_clients)
        outcomes["evicted"] = int(application.initialize_requests == initialized + 1)
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
    finally:
        application.release_searches.set()
        if first_tasks:
            await asyncio.gather(*first_tasks, return_exceptions=True)
        try:
            await supervisor.begin_shutdown()
            await supervisor.shutdown()
        except Exception as exc:
            failure = failure or f"{type(exc).__name__}: {exc}"
        sessions_after_shutdown = sum(
            slot.active_sessions + slot.idle_sessions + slot.closing_sessions
            for slot in supervisor.runtime_snapshot(None).values()
        )
        if sampler is not None:
            stop_sampling.set()
            await sampler
        if server_started:
            await server.stop()
        connections_after_shutdown = server.connection_count
    await asyncio.sleep(0)
    after_cleanup = _process_sample()
    rss_growth = (
        max(0, peaks["rss"] - baseline.rss_bytes)
        if baseline.rss_bytes is not None else None
    )
    fd_growth = (
        max(0, peaks["fds"] - baseline.fd_count)
        if baseline.fd_count is not None else None
    )
    limits = {
        "private_clients": config.max_clients,
        "rss_growth_bytes": _MAX_RSS_GROWTH_BYTES,
        "fd_growth": 4 * config.max_clients + 32,
        "task_high_water": baseline.task_count + config.users + 10 * config.max_clients + 64,
    }
    checks = {
        "harness_completed": failure is None,
        "active_sessions_bounded": active_high_water == config.max_clients,
        "overflow_is_immediately_busy": outcomes["busy"] == config.users - config.max_clients,
        "no_queue": outcomes["issued"] == outcomes["completed"] == config.max_clients,
        "private_http_sessions_observed": initial_search_sessions == config.max_clients,
        "conversation_reuses_client": outcomes["reused"] == 1,
        "idle_lru_evicts_client": outcomes["evicted"] == 1,
        "idle_sessions_observed": idle_after_calls == config.max_clients,
        "sessions_close_at_shutdown": sessions_after_shutdown == 0,
        "remote_http_sessions_close": application.closed_session_ids == application.session_ids,
        "connections_close_at_shutdown": connections_after_shutdown == 0,
        "rss_bounded": rss_growth is not None and rss_growth <= limits["rss_growth_bytes"],
        "fds_bounded": fd_growth is not None and fd_growth <= limits["fd_growth"],
        "tasks_bounded": peaks["tasks"] <= limits["task_high_water"],
        "fds_return_to_baseline": baseline.fd_count is not None
        and after_cleanup.fd_count is not None
        and after_cleanup.fd_count <= baseline.fd_count,
    }
    return {
        "ok": all(checks.values()), "failure": failure,
        "transport": "real_loopback_streamable_http",
        "users": config.users, "outcomes": outcomes, "limits": limits,
        "metrics": {
            "wall_time_seconds": round(time.perf_counter() - started, 6),
            "private_http_sessions_initialized": application.initialize_requests,
            "private_http_sessions_used": len(application.search_sessions),
            "initial_private_http_sessions_used": initial_search_sessions,
            "private_http_sessions_closed": len(application.closed_session_ids),
            "active_session_high_water": active_high_water,
            "idle_sessions_after_calls": idle_after_calls,
            "http_search_requests": application.search_requests,
            "http_active_request_high_water": application.active_search_requests_high_water,
            "http_connection_high_water": peaks["http_connections"],
            "peak_rss_bytes": peaks["rss"], "peak_fd_count": peaks["fds"],
            "peak_task_count": peaks["tasks"], "rss_growth_bytes": rss_growth,
            "fd_growth": fd_growth,
            "baseline": {
                "rss_bytes": baseline.rss_bytes, "fd_count": baseline.fd_count,
                "task_count": baseline.task_count,
            },
            "after_cleanup": {
                "rss_bytes": after_cleanup.rss_bytes,
                "fd_count": after_cleanup.fd_count,
                "task_count": after_cleanup.task_count,
                "http_connections": connections_after_shutdown,
                "private_sessions": sessions_after_shutdown,
            },
        },
        "checks": checks,
        "limitations": ["The endpoint is a deterministic local search fixture."],
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=_DEFAULT_USERS)
    parser.add_argument("--max-clients", type=int, default=_DEFAULT_CLIENTS)
    parser.add_argument("--sample-interval-ms", type=float, default=5.0)
    parser.add_argument("--indent", type=int, default=2)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        result = asyncio.run(run_harness(HarnessConfig(
            users=args.users,
            max_clients=args.max_clients,
            sample_interval_seconds=args.sample_interval_ms / 1000,
        )))
    except (OSError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    print(json.dumps(result, indent=args.indent, sort_keys=True))
    return 0 if result["ok"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
