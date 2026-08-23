#!/usr/bin/env python3
"""Run the Py8a 500-user Server MCP capacity gate.

The harness drives the production ``ServerMcpCoordinator`` and one real,
shared FastMCP Streamable HTTP client/session against a loopback MCP search
server.  It emits one JSON document suitable for a merge-gate artifact.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import socket
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any, cast
from uuid import UUID, uuid5

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastmcp.client.transports import StreamableHttpTransport
from mcp import types

from openctopus_server.mcp.scheduler import (
    GLOBAL_MAX_RESERVED,
    PER_USER_MAX_RESERVED,
    QUEUE_DEADLINE_SECONDS,
    AdmissionClock,
    AdmissionLease,
    AdmissionTicket,
    IssuedAdmission,
    RuntimeAdmission,
    ServerMcpBusyError,
    ServerMcpCoordinator,
    runtime_waiting_capacity,
)
from openctopus_server.mcp.transport import (
    RuntimeClient,
    RuntimeSession,
    create_fastmcp_client,
    create_mcp_http_client,
)

_NAMESPACE = UUID("f78285ed-5b83-4f87-8871-e647d3b95a1b")
_DEFAULT_USERS = 500
_DEFAULT_RUNTIME_CONCURRENCY = 8
_DEFAULT_SAMPLE_INTERVAL_SECONDS = 0.005
_MAX_RSS_GROWTH_BYTES = 128 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    users: int = _DEFAULT_USERS
    runtime_concurrency: int = _DEFAULT_RUNTIME_CONCURRENCY
    sample_interval_seconds: float = _DEFAULT_SAMPLE_INTERVAL_SECONDS

    def normalized(self) -> HarnessConfig:
        if not 1 <= self.runtime_concurrency <= 32:
            raise ValueError("runtime concurrency must be in 1..32")
        minimum_users = self.runtime_concurrency + runtime_waiting_capacity(
            self.runtime_concurrency
        )
        if self.users <= minimum_users:
            raise ValueError(
                "users must exceed runtime active plus waiting capacity to exercise busy admission"
            )
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
            resident_pages = int(statm.read().split()[1])
        rss_bytes = resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (FileNotFoundError, IndexError, OSError, ValueError):
        try:
            peak_rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            rss_bytes = peak_rss if sys.platform == "darwin" else peak_rss * 1024
        except (OSError, ValueError):
            pass
    try:
        fd_count = len(os.listdir("/proc/self/fd"))
    except (FileNotFoundError, OSError):
        pass
    return _ProcessSample(
        rss_bytes=rss_bytes,
        fd_count=fd_count,
        task_count=len(asyncio.all_tasks()),
    )


class _GateClock(AdmissionClock):
    def __init__(self) -> None:
        self.current = 0.0
        self._changed = asyncio.Event()

    def now(self) -> float:
        return self.current

    async def sleep_until(self, deadline: float) -> None:
        while self.current < deadline:
            await self._changed.wait()
            self._changed.clear()

    def advance(self, seconds: float) -> None:
        self.current += seconds
        self._changed.set()


class _SearchMcpApplication:
    """Small real Streamable HTTP MCP endpoint with observable request load."""

    def __init__(self, expected_parallel_searches: int) -> None:
        self.expected_parallel_searches = expected_parallel_searches
        self.active_search_requests = 0
        self.active_search_requests_high_water = 0
        self.search_requests = 0
        self.search_queries: set[str] = set()
        self.all_searches_started = asyncio.Event()
        self.release_searches = asyncio.Event()
        self.app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
        self.app.add_api_route("/mcp", self.handle, methods=["POST"])

    async def handle(self, request: Request) -> Response:
        payload = cast(dict[str, Any], await request.json())
        if "id" not in payload:
            return Response(status_code=202)

        request_id = payload["id"]
        method = payload.get("method")
        if method == "initialize":
            params = cast(dict[str, Any], payload["params"])
            result: dict[str, object] = {
                "protocolVersion": params["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "capacity-search", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "search",
                        "description": "Search the local capacity fixture",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"query": {"type": "string"}},
                            "required": ["query"],
                            "additionalProperties": False,
                        },
                    }
                ]
            }
        elif method == "tools/call":
            params = cast(dict[str, Any], payload["params"])
            if params.get("name") != "search":
                raise ValueError("capacity MCP received an unknown tool")
            arguments = cast(dict[str, Any], params["arguments"])
            query = arguments.get("query")
            if not isinstance(query, str):
                raise ValueError("capacity MCP search query must be a string")
            self.search_requests += 1
            self.search_queries.add(query)
            self.active_search_requests += 1
            self.active_search_requests_high_water = max(
                self.active_search_requests_high_water,
                self.active_search_requests,
            )
            if self.active_search_requests_high_water >= self.expected_parallel_searches:
                self.all_searches_started.set()
            try:
                await self.release_searches.wait()
            finally:
                self.active_search_requests -= 1
            result = {
                "content": [
                    {"type": "text", "text": f"capacity result for {query}"},
                ]
            }
        else:
            raise ValueError(f"capacity MCP received unexpected method {method!r}")

        return JSONResponse({"jsonrpc": "2.0", "id": request_id, "result": result})


class _LoopbackMcpServer:
    def __init__(self, application: _SearchMcpApplication) -> None:
        self.application = application
        self.listener: socket.socket | None = None
        self.server: uvicorn.Server | None = None
        self.task: asyncio.Task[None] | None = None
        self.url: str | None = None

    @property
    def connection_count(self) -> int:
        server = self.server
        return len(server.server_state.connections) if server is not None else 0

    async def start(self) -> str:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(512)
        listener.setblocking(False)
        port = cast(tuple[str, int], listener.getsockname())[1]
        server = uvicorn.Server(
            uvicorn.Config(
                self.application.app,
                host="127.0.0.1",
                port=port,
                lifespan="off",
                log_config=None,
                access_log=False,
            )
        )
        task = asyncio.create_task(server.serve(sockets=[listener]))
        self.listener = listener
        self.server = server
        self.task = task
        self.url = f"http://127.0.0.1:{port}/mcp"
        while not server.started:
            if task.done():
                await task
                raise RuntimeError("loopback MCP server stopped before startup")
            await asyncio.sleep(0.001)
        return self.url

    async def stop(self) -> None:
        server = self.server
        task = self.task
        listener = self.listener
        if server is not None:
            server.should_exit = True
        if task is not None:
            await asyncio.wait_for(task, timeout=5)
        if listener is not None:
            listener.close()
        self.server = None
        self.task = None
        self.listener = None


@dataclass(slots=True)
class _HighWaterMetrics:
    peak_rss_bytes: int | None = None
    peak_fd_count: int | None = None
    peak_task_count: int = 0
    queue_high_water: int = 0
    pending_future_high_water: int = 0
    runtime_reserved_high_water: int = 0
    global_reserved_high_water: int = 0
    per_user_reserved_high_water: int = 0
    http_connection_high_water: int = 0

    def record_process(self, *, http_connections: int) -> None:
        sample = _process_sample()
        if sample.rss_bytes is not None:
            self.peak_rss_bytes = max(self.peak_rss_bytes or 0, sample.rss_bytes)
        if sample.fd_count is not None:
            self.peak_fd_count = max(self.peak_fd_count or 0, sample.fd_count)
        self.peak_task_count = max(self.peak_task_count, sample.task_count)
        self.http_connection_high_water = max(
            self.http_connection_high_water,
            http_connections,
        )

    def record_scheduler(
        self,
        coordinator: ServerMcpCoordinator,
        runtimes: list[RuntimeAdmission],
        *,
        pending_futures: int = 0,
        include_runtime_reserved: bool = True,
    ) -> None:
        snapshot = coordinator.snapshot()
        self.global_reserved_high_water = max(
            self.global_reserved_high_water,
            snapshot.reserved,
        )
        self.per_user_reserved_high_water = max(
            self.per_user_reserved_high_water,
            max(snapshot.reserved_by_user.values(), default=0),
        )
        if include_runtime_reserved:
            self.runtime_reserved_high_water = max(
                self.runtime_reserved_high_water,
                max((runtime.reserved_count for runtime in runtimes), default=0),
            )
        self.queue_high_water = max(
            self.queue_high_water,
            max((runtime.waiting_count for runtime in runtimes), default=0),
        )
        self.pending_future_high_water = max(
            self.pending_future_high_water,
            pending_futures,
        )


class _MetricsSampler:
    def __init__(
        self,
        metrics: _HighWaterMetrics,
        server: _LoopbackMcpServer,
        interval_seconds: float,
    ) -> None:
        self.metrics = metrics
        self.server = server
        self.interval_seconds = interval_seconds
        self.task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.metrics.record_process(http_connections=self.server.connection_count)
        self.task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        task = self.task
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.task = None
        self.metrics.record_process(http_connections=self.server.connection_count)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.interval_seconds)
            self.metrics.record_process(http_connections=self.server.connection_count)


def _start_value(value: object) -> Callable[[AdmissionLease], object]:
    def start(_lease: AdmissionLease) -> object:
        return value

    return start


async def _close_admissions(admissions: list[IssuedAdmission]) -> None:
    await asyncio.gather(*(admission.lease.aclose() for admission in admissions))


async def _run_fixed_boundary_probes(metrics: _HighWaterMetrics) -> None:
    global_coordinator = ServerMcpCoordinator(clock=_GateClock())
    global_runtimes = [
        global_coordinator.create_runtime(max_concurrent_calls=32) for _ in range(16)
    ]
    global_admissions: list[IssuedAdmission] = []
    for index in range(GLOBAL_MAX_RESERVED):
        runtime = global_runtimes[index % len(global_runtimes)]
        ticket = await runtime.submit(
            uuid5(_NAMESPACE, f"global-user-{index}"),
            _start_value(index),
        )
        global_admissions.append(await ticket.wait())
        metrics.record_scheduler(
            global_coordinator,
            global_runtimes,
            include_runtime_reserved=False,
        )
    overflow = await global_runtimes[0].submit(
        uuid5(_NAMESPACE, "global-overflow"),
        _start_value("overflow"),
    )
    metrics.record_scheduler(
        global_coordinator,
        global_runtimes,
        pending_futures=1,
        include_runtime_reserved=False,
    )
    if overflow.issued or global_coordinator.snapshot().reserved != GLOBAL_MAX_RESERVED:
        raise RuntimeError("global Server MCP admission boundary was not enforced")
    await overflow.cancel()
    await _close_admissions(global_admissions)
    await global_coordinator.close()

    user_coordinator = ServerMcpCoordinator(clock=_GateClock())
    user_runtimes = [
        user_coordinator.create_runtime(max_concurrent_calls=32),
        user_coordinator.create_runtime(max_concurrent_calls=32),
    ]
    user_id = uuid5(_NAMESPACE, "per-user-boundary")
    user_tickets = [
        await user_runtimes[index % 2].submit(user_id, _start_value(index))
        for index in range(PER_USER_MAX_RESERVED + 1)
    ]
    user_admissions = [await ticket.wait() for ticket in user_tickets if ticket.issued]
    queued = [ticket for ticket in user_tickets if not ticket.issued]
    metrics.record_scheduler(
        user_coordinator,
        user_runtimes,
        pending_futures=len(queued),
        include_runtime_reserved=False,
    )
    if (
        len(user_admissions) != PER_USER_MAX_RESERVED
        or len(queued) != 1
        or user_coordinator.snapshot().reserved_by_user.get(user_id)
        != PER_USER_MAX_RESERVED
    ):
        raise RuntimeError("per-user Server MCP admission boundary was not enforced")
    await queued[0].cancel()
    await _close_admissions(user_admissions)
    await user_coordinator.close()


async def _call_search(session: RuntimeSession, query: str) -> types.CallToolResult:
    return await session.send_request(
        types.ClientRequest(
            types.CallToolRequest(
                params=types.CallToolRequestParams(
                    name="search",
                    arguments={"query": query},
                )
            )
        ),
        types.CallToolResult,
    )


async def _wait_for_expiry(ticket: AdmissionTicket) -> bool:
    try:
        await ticket.wait()
    except ServerMcpBusyError:
        return True
    return False


async def _run_pressure(
    config: HarnessConfig,
    client: RuntimeClient,
    application: _SearchMcpApplication,
    server: _LoopbackMcpServer,
    metrics: _HighWaterMetrics,
) -> tuple[dict[str, int], int, int]:
    clock = _GateClock()
    coordinator = ServerMcpCoordinator(clock=clock)
    runtime = coordinator.create_runtime(
        max_concurrent_calls=config.runtime_concurrency
    )
    launch = asyncio.Event()
    invocation_tasks: list[asyncio.Task[types.CallToolResult]] = []
    issued_admissions: list[IssuedAdmission] = []
    queued_tickets: list[AdmissionTicket] = []

    def start(query: str) -> Callable[[AdmissionLease], object]:
        def issue(_lease: AdmissionLease) -> object:
            task = asyncio.create_task(_call_search(client.session, query))
            invocation_tasks.append(task)
            return task

        return issue

    async def submit(index: int) -> AdmissionTicket | None:
        await launch.wait()
        try:
            return await runtime.submit(
                uuid5(_NAMESPACE, f"pressure-user-{index}"),
                start(f"query-{index}"),
            )
        except ServerMcpBusyError:
            return None

    submission_tasks = [asyncio.create_task(submit(index)) for index in range(config.users)]
    await asyncio.sleep(0)
    metrics.record_process(http_connections=server.connection_count)
    launch.set()
    submitted = await asyncio.gather(*submission_tasks)
    accepted_tickets = [ticket for ticket in submitted if ticket is not None]
    busy = sum(ticket is None for ticket in submitted)
    immediate_tickets = [ticket for ticket in accepted_tickets if ticket.issued]
    queued_tickets = [ticket for ticket in accepted_tickets if not ticket.issued]
    issued_admissions = [await ticket.wait() for ticket in immediate_tickets]
    metrics.record_scheduler(
        coordinator,
        [runtime],
        pending_futures=len(queued_tickets),
    )

    try:
        await asyncio.wait_for(application.all_searches_started.wait(), timeout=5)
        metrics.record_process(http_connections=server.connection_count)

        clock.advance(QUEUE_DEADLINE_SECONDS)
        expiry_tasks = [asyncio.create_task(_wait_for_expiry(ticket)) for ticket in queued_tickets]
        await asyncio.sleep(0)
        metrics.record_process(http_connections=server.connection_count)
        expired = sum(await asyncio.gather(*expiry_tasks))
        metrics.record_scheduler(coordinator, [runtime])

        application.release_searches.set()
        invocation_results = await asyncio.gather(*invocation_tasks)
        completed = sum(result.isError is not True for result in invocation_results)
        await _close_admissions(issued_admissions)
        metrics.record_scheduler(coordinator, [runtime])
        final_snapshot = coordinator.snapshot()
        final_waiting = runtime.waiting_count
        outcomes = {
            "accepted": len(accepted_tickets),
            "issued": len(immediate_tickets),
            "queued": len(queued_tickets),
            "busy": busy,
            "expired": expired,
            "completed": completed,
        }
        return outcomes, final_snapshot.reserved, final_waiting
    finally:
        application.release_searches.set()
        for ticket in queued_tickets:
            await ticket.cancel()
        await asyncio.gather(*invocation_tasks, return_exceptions=True)
        await _close_admissions(issued_admissions)
        await runtime.retire()
        await coordinator.close()


def _sample_dict(sample: _ProcessSample) -> dict[str, int | None]:
    return {
        "rss_bytes": sample.rss_bytes,
        "fd_count": sample.fd_count,
        "task_count": sample.task_count,
    }


async def _wait_for_no_http_connections(server: _LoopbackMcpServer) -> None:
    deadline = asyncio.get_running_loop().time() + 2
    while server.connection_count and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)


async def run_harness(config: HarnessConfig = HarnessConfig()) -> dict[str, object]:
    config = config.normalized()
    baseline = _process_sample()
    metrics = _HighWaterMetrics()
    application = _SearchMcpApplication(config.runtime_concurrency)
    server = _LoopbackMcpServer(application)
    sampler = _MetricsSampler(metrics, server, config.sample_interval_seconds)
    client: RuntimeClient | None = None
    client_entered = False
    server_started = False
    clients_created = 0
    sessions_entered = 0
    connections_after_client_close = -1
    outcomes: dict[str, int] = {}
    final_scheduler_reserved = -1
    final_scheduler_waiting = -1
    failure: str | None = None
    started = time.perf_counter()

    try:
        url = await server.start()
        server_started = True
        await sampler.start()
        transport = StreamableHttpTransport(
            url,
            httpx_client_factory=partial(create_mcp_http_client),
        )
        client = cast(RuntimeClient, create_fastmcp_client(transport))
        clients_created = 1
        await client.__aenter__()
        client_entered = True
        sessions_entered = 1
        tools = await client.session.list_tools()
        if [tool.name for tool in tools.tools] != ["search"]:
            raise RuntimeError("loopback MCP did not expose the expected search tool")
        await _run_fixed_boundary_probes(metrics)
        outcomes, final_scheduler_reserved, final_scheduler_waiting = await _run_pressure(
            config,
            client,
            application,
            server,
            metrics,
        )
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
    finally:
        application.release_searches.set()
        if client is not None and client_entered:
            try:
                await client.close()
            except Exception as exc:
                if failure is None:
                    failure = f"{type(exc).__name__}: {exc}"
        await _wait_for_no_http_connections(server)
        connections_after_client_close = server.connection_count
        await sampler.stop()
        try:
            await server.stop()
        except Exception as exc:
            if failure is None:
                failure = f"{type(exc).__name__}: {exc}"
    await asyncio.sleep(0)
    after_cleanup = _process_sample()
    elapsed = time.perf_counter() - started

    waiting_limit = runtime_waiting_capacity(config.runtime_concurrency)
    expected_accepted = config.runtime_concurrency + waiting_limit
    expected_busy = config.users - expected_accepted
    peak_rss_growth = (
        max(0, metrics.peak_rss_bytes - baseline.rss_bytes)
        if metrics.peak_rss_bytes is not None and baseline.rss_bytes is not None
        else None
    )
    peak_fd_growth = (
        max(0, metrics.peak_fd_count - baseline.fd_count)
        if metrics.peak_fd_count is not None and baseline.fd_count is not None
        else None
    )
    task_limit = baseline.task_count + config.users + (5 * config.runtime_concurrency) + 64
    fd_growth_limit = (2 * config.runtime_concurrency) + 32
    limits = {
        "runtime_reserved": config.runtime_concurrency,
        "runtime_waiting": waiting_limit,
        "global_reserved": GLOBAL_MAX_RESERVED,
        "per_user_reserved": PER_USER_MAX_RESERVED,
        "http_connections": config.runtime_concurrency,
        "task_high_water": task_limit,
        "fd_growth_high_water": fd_growth_limit,
        "rss_growth_high_water_bytes": _MAX_RSS_GROWTH_BYTES,
    }
    checks = {
        "harness_completed": failure is None,
        "accepted_is_active_plus_bounded_queue": outcomes.get("accepted")
        == expected_accepted,
        "immediate_issue_matches_runtime_limit": outcomes.get("issued")
        == config.runtime_concurrency,
        "queue_reaches_but_does_not_exceed_limit": outcomes.get("queued")
        == waiting_limit
        and metrics.queue_high_water == waiting_limit,
        "overflow_is_immediately_busy": outcomes.get("busy") == expected_busy,
        "queued_calls_expire_without_issue": outcomes.get("expired") == waiting_limit,
        "issued_searches_complete": outcomes.get("completed")
        == config.runtime_concurrency,
        "runtime_reserved_is_bounded": metrics.runtime_reserved_high_water
        == config.runtime_concurrency,
        "global_reserved_is_bounded": metrics.global_reserved_high_water
        == GLOBAL_MAX_RESERVED,
        "per_user_reserved_is_bounded": metrics.per_user_reserved_high_water
        == PER_USER_MAX_RESERVED,
        "pending_futures_are_bounded": metrics.pending_future_high_water
        <= waiting_limit,
        "one_shared_client_reaches_real_http": application.search_requests
        == config.runtime_concurrency
        and len(application.search_queries) == config.runtime_concurrency,
        "http_requests_are_bounded": application.active_search_requests_high_water
        == config.runtime_concurrency,
        "http_connections_are_observed_and_bounded": 1
        <= metrics.http_connection_high_water
        <= config.runtime_concurrency,
        "tasks_are_bounded": metrics.peak_task_count <= task_limit,
        "fds_are_observed_and_bounded": peak_fd_growth is not None
        and peak_fd_growth <= fd_growth_limit,
        "rss_is_observed_and_bounded": peak_rss_growth is not None
        and peak_rss_growth <= _MAX_RSS_GROWTH_BYTES,
        "scheduler_is_empty_after_pressure": final_scheduler_reserved == 0
        and final_scheduler_waiting == 0,
        "http_connections_close": connections_after_client_close == 0,
        "tasks_return_to_baseline": after_cleanup.task_count <= baseline.task_count,
        "fds_return_to_baseline": baseline.fd_count is not None
        and after_cleanup.fd_count is not None
        and after_cleanup.fd_count <= baseline.fd_count,
    }
    return {
        "ok": all(checks.values()),
        "failure": failure,
        "mode": "source",
        "transport": "real_loopback_streamable_http",
        "network_exercised": server_started,
        "http_transport_exercised": application.search_requests > 0,
        "mcp_fixture": "local equivalent search wrapper",
        "fastmcp_clients": clients_created,
        "fastmcp_sessions": sessions_entered,
        "users": config.users,
        "outcomes": outcomes,
        "limits": limits,
        "metrics": {
            "wall_time_seconds": round(elapsed, 6),
            "peak_rss_bytes": metrics.peak_rss_bytes,
            "peak_fd_count": metrics.peak_fd_count,
            "peak_task_count": metrics.peak_task_count,
            "pending_future_high_water": metrics.pending_future_high_water,
            "queue_high_water": metrics.queue_high_water,
            "runtime_reserved_high_water": metrics.runtime_reserved_high_water,
            "global_reserved_high_water": metrics.global_reserved_high_water,
            "per_user_reserved_high_water": metrics.per_user_reserved_high_water,
            "http_connection_high_water": metrics.http_connection_high_water,
            "http_active_request_high_water": (
                application.active_search_requests_high_water
            ),
            "http_search_requests": application.search_requests,
            "rss_growth_high_water_bytes": peak_rss_growth,
            "fd_growth_high_water": peak_fd_growth,
            "measurement": {
                "http_connections": "live Uvicorn TCP protocol objects",
                "pending_futures": "accepted tickets still waiting for scheduler issue",
                "rss": "current procfs RSS with resource fallback",
                "fds": "procfs open descriptor count",
                "tasks": "asyncio.all_tasks in the harness process",
            },
            "baseline": _sample_dict(baseline),
            "after_cleanup": {
                **_sample_dict(after_cleanup),
                "http_connections": connections_after_client_close,
                "scheduler_reserved": final_scheduler_reserved,
                "scheduler_waiting": final_scheduler_waiting,
            },
        },
        "checks": checks,
        "limitations": [
            "The MCP endpoint is a local deterministic search wrapper, not public SearXNG.",
            "The harness exercises the scheduler and shared FastMCP HTTP session, not an Agent/provider turn.",
        ],
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=_DEFAULT_USERS)
    parser.add_argument(
        "--runtime-concurrency",
        type=int,
        default=_DEFAULT_RUNTIME_CONCURRENCY,
    )
    parser.add_argument(
        "--sample-interval-ms",
        type=float,
        default=_DEFAULT_SAMPLE_INTERVAL_SECONDS * 1000,
    )
    parser.add_argument("--indent", type=int, default=2)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        result = asyncio.run(
            run_harness(
                HarnessConfig(
                    users=args.users,
                    runtime_concurrency=args.runtime_concurrency,
                    sample_interval_seconds=args.sample_interval_ms / 1000,
                )
            )
        )
    except (OSError, ValueError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    print(json.dumps(result, indent=args.indent, sort_keys=True))
    return 0 if result["ok"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
