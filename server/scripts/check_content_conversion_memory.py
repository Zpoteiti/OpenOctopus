#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import errno
import gc
import json
import multiprocessing
import signal
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib import metadata
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path
from threading import BrokenBarrierError
from typing import Literal, Protocol, cast
from uuid import UUID, uuid4

from openctopus_server.admission import KeyedAdmission
from openctopus_server.config import get_settings
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError
from openctopus_server.workspace.content_conversion_worker import (
    PROTOCOL_VERSION,
    WorkerRequest,
    _apply_resource_limits,
    run_conversion_worker,
)
from openctopus_server.workspace.file_content import DocumentParser

MiB = 1024 * 1024
ACCEPTANCE_MEMORY_MB = 1024
ACCEPTANCE_CONCURRENCY = 2
ACCEPTANCE_REPEAT = 20
PARENT_RSS_GROWTH_BYTES = 64 * MiB
CONCURRENT_PROTOCOL_OVERHEAD_BYTES = 256 * MiB
POLL_INTERVAL_SECONDS = 0.005
MEASUREMENT_PROTOCOL_VERSION = 1
DOCUMENT_MAX_BYTES = 8 * MiB
HTML_MAX_BYTES = 5_000_000

type ConversionFormat = Literal["pdf", "docx", "xlsx", "pptx", "html"]
type Gate = dict[str, object]

_FORMAT_BY_SUFFIX: dict[str, ConversionFormat] = {
    ".pdf": "pdf",
    ".docx": "docx",
    ".xlsx": "xlsx",
    ".pptx": "pptx",
    ".html": "html",
}
_REQUIRED_FORMATS: tuple[ConversionFormat, ...] = ("pdf", "docx", "xlsx", "pptx", "html")
_REQUIRED_DISTRIBUTIONS = (
    "beautifulsoup4",
    "charset-normalizer",
    "magika",
    "mammoth",
    "markdownify",
    "markitdown",
    "openpyxl",
    "pandas",
    "pdfminer-six",
    "pdfplumber",
    "pypdf",
    "python-pptx",
    "requests",
)


class RunnerError(RuntimeError):
    pass


class StartBarrier(Protocol):
    def wait(self, timeout: float | None = None) -> int: ...


@dataclass(frozen=True, slots=True)
class Arguments:
    fixtures: Path
    memory_mb: int
    concurrency: int
    repeat: int
    output: Path


@dataclass(frozen=True, slots=True)
class Fixture:
    path: Path
    relative_path: str
    conversion_format: ConversionFormat
    data: bytes

    def validate_size(self) -> None:
        maximum = HTML_MAX_BYTES if self.conversion_format == "html" else DOCUMENT_MAX_BYTES
        if len(self.data) > maximum:
            label = "5,000,000 bytes" if self.conversion_format == "html" else "8 MiB"
            raise RunnerError(f"fixture {self.relative_path} exceeds the {label} input limit")

    def request(self, *, memory_mb: int, timeout_seconds: float) -> WorkerRequest:
        common: WorkerRequest = {
            "data": self.data,
            "memory_mb": memory_mb,
            "timeout_seconds": timeout_seconds,
        }
        if self.conversion_format == "html":
            return {
                **common,
                "operation": "html",
                "charset": "utf-8",
                "base_url": "https://example.invalid/fixture/",
                "mode": "markdown",
                "max_chars": 50_000,
            }
        return {
            **common,
            "operation": "document",
            "path": self.relative_path,
            "pages": None,
        }


@dataclass(frozen=True, slots=True)
class ProcessRecord:
    label: str
    fixture: str | None
    ok: bool
    code: str
    output_chars: int
    wall_seconds: float
    pid: int
    exit_code: int | None
    ru_maxrss_bytes: int | None
    proc_high_water_bytes: int | None
    max_sampled_rss_bytes: int | None

    @property
    def high_water_bytes(self) -> int | None:
        values = (
            self.ru_maxrss_bytes,
            self.proc_high_water_bytes,
            self.max_sampled_rss_bytes,
        )
        present = [value for value in values if value is not None]
        return max(present) if present else None

    def to_json(self) -> dict[str, object]:
        value = asdict(self)
        value["high_water_bytes"] = self.high_water_bytes
        return value


@dataclass(frozen=True, slots=True)
class ProtocolExercise:
    rounds_requested: int
    rounds_completed: int
    success_count: int
    malformed_count: int
    timeout_count: int
    cancellation_count: int
    permit_acquisitions: int
    admission_entries_after: int
    leaked_child_pids: tuple[int, ...]
    file_descriptors_before: int
    file_descriptors_after: int
    failure: str | None

    @property
    def passed(self) -> bool:
        expected = self.rounds_requested
        return (
            self.failure is None
            and self.rounds_completed == expected
            and self.success_count == expected
            and self.malformed_count == expected
            and self.timeout_count == expected
            and self.cancellation_count == expected
            and self.permit_acquisitions == expected * 4
            and self.admission_entries_after == 0
            and not self.leaked_child_pids
            and self.file_descriptors_after <= self.file_descriptors_before
        )


@dataclass(frozen=True, slots=True)
class EvaluationInput:
    normal_records: tuple[ProcessRecord, ...]
    over_limit_record: ProcessRecord
    recovery_record: ProcessRecord
    concurrent_records: tuple[ProcessRecord, ...]
    concurrent_peak_rss_bytes: int
    concurrent_synchronized_participants: int
    baseline_parent_rss_bytes: int
    final_parent_rss_bytes: int
    memory_limit_bytes: int
    timeout_seconds: float
    owned_resources_released: bool
    protocol_exercise: ProtocolExercise


@dataclass(frozen=True, slots=True)
class ChildSpec:
    label: str
    fixture: str | None
    target: Callable[..., None]
    args: tuple[object, ...]


@dataclass(slots=True)
class _RunningChild:
    spec: ChildSpec
    process: BaseProcess
    receiver: Connection
    started_at: float
    pid: int
    message: object | None = None
    timed_out: bool = False
    max_sampled_rss_bytes: int | None = None
    proc_high_water_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class MeasurementGroup:
    records: tuple[ProcessRecord, ...]
    maximum_same_sample_rss_bytes: int
    pids: tuple[int, ...]
    parent_rss_samples_bytes: tuple[int, ...]
    resources_released: bool
    synchronized_participants: int = 0


def parse_proc_status(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    wanted = {"VmRSS:": "rss_bytes", "VmHWM:": "high_water_bytes"}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 3 or parts[0] not in wanted or parts[2] != "kB":
            continue
        try:
            result[wanted[parts[0]]] = int(parts[1]) * 1024
        except ValueError:
            continue
    return result


def discover_fixtures(root: Path) -> tuple[Fixture, ...]:
    if not root.is_dir():
        raise RunnerError(f"fixture directory does not exist: {root}")
    fixtures: list[Fixture] = []
    for path in sorted((item for item in root.rglob("*") if item.is_file()), key=str):
        conversion_format = _FORMAT_BY_SUFFIX.get(path.suffix.lower())
        if conversion_format is None:
            continue
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise RunnerError(f"could not read fixture {path}: {exc}") from exc
        fixture = Fixture(
            path=path,
            relative_path=path.relative_to(root).as_posix(),
            conversion_format=conversion_format,
            data=data,
        )
        fixture.validate_size()
        fixtures.append(fixture)

    present = {fixture.conversion_format for fixture in fixtures}
    for conversion_format in _REQUIRED_FORMATS:
        if conversion_format not in present:
            raise RunnerError(f"missing required fixture format: {conversion_format}")
    return tuple(sorted(fixtures, key=lambda item: item.relative_path))


def evaluate_gates(value: EvaluationInput) -> dict[str, Gate]:
    deadline = value.timeout_seconds + 5
    all_normal_records = (*value.normal_records, value.recovery_record, *value.concurrent_records)
    normal_high_water = [record.high_water_bytes for record in all_normal_records]
    concurrent_limit = (
        value.baseline_parent_rss_bytes
        + ACCEPTANCE_CONCURRENCY * value.memory_limit_bytes
        + CONCURRENT_PROTOCOL_OVERHEAD_BYTES
    )
    parent_growth = value.final_parent_rss_bytes - value.baseline_parent_rss_bytes
    return {
        "normal_corpus_success": _gate(
            bool(value.normal_records) and all(record.ok for record in value.normal_records),
            observed=sum(record.ok for record in value.normal_records),
            limit=len(value.normal_records),
        ),
        "normal_wall_time": _gate(
            bool(all_normal_records)
            and all(record.wall_seconds <= deadline for record in all_normal_records),
            observed=max((record.wall_seconds for record in all_normal_records), default=None),
            limit=deadline,
        ),
        "normal_child_memory": _gate(
            bool(normal_high_water)
            and all(
                high_water is not None and high_water <= value.memory_limit_bytes
                for high_water in normal_high_water
            ),
            observed=max(
                (high_water for high_water in normal_high_water if high_water is not None),
                default=None,
            ),
            limit=value.memory_limit_bytes,
        ),
        "over_limit_classification": _gate(
            not value.over_limit_record.ok
            and value.over_limit_record.code == "tool_content_conversion_resource_exceeded",
            observed=value.over_limit_record.code,
            limit="tool_content_conversion_resource_exceeded",
        ),
        "post_over_limit_recovery": _gate(
            value.recovery_record.ok,
            observed=value.recovery_record.code or "ok",
            limit="ok",
        ),
        "concurrent_success": _gate(
            len(value.concurrent_records) == ACCEPTANCE_CONCURRENCY
            and all(record.ok for record in value.concurrent_records),
            observed=sum(record.ok for record in value.concurrent_records),
            limit=ACCEPTANCE_CONCURRENCY,
        ),
        "concurrent_start_barrier": _gate(
            value.concurrent_synchronized_participants == ACCEPTANCE_CONCURRENCY,
            observed=value.concurrent_synchronized_participants,
            limit=ACCEPTANCE_CONCURRENCY,
        ),
        "concurrent_combined_rss": _gate(
            value.concurrent_peak_rss_bytes <= concurrent_limit,
            observed=value.concurrent_peak_rss_bytes,
            limit=concurrent_limit,
        ),
        "parent_rss_growth": _gate(
            parent_growth <= PARENT_RSS_GROWTH_BYTES,
            observed=parent_growth,
            limit=PARENT_RSS_GROWTH_BYTES,
        ),
        "owned_resources_released": _gate(
            value.owned_resources_released,
            observed=value.owned_resources_released,
            limit=True,
        ),
        "mixed_parent_protocol": _gate(
            value.protocol_exercise.passed,
            observed={
                "rounds_completed": value.protocol_exercise.rounds_completed,
                "permit_acquisitions": value.protocol_exercise.permit_acquisitions,
                "admission_entries_after": value.protocol_exercise.admission_entries_after,
                "leaked_child_pids": list(value.protocol_exercise.leaked_child_pids),
                "file_descriptors_before": value.protocol_exercise.file_descriptors_before,
                "file_descriptors_after": value.protocol_exercise.file_descriptors_after,
                "failure": value.protocol_exercise.failure,
            },
            limit={
                "rounds_completed": value.protocol_exercise.rounds_requested,
                "permit_acquisitions": value.protocol_exercise.rounds_requested * 4,
                "admission_entries_after": 0,
                "leaked_child_pids": [],
                "file_descriptors_after": "<= file_descriptors_before",
                "failure": None,
            },
        ),
    }


def _gate(passed: bool, *, observed: object, limit: object) -> Gate:
    return {"passed": passed, "observed": observed, "limit": limit}


def resolved_package_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    missing: list[str] = []
    for distribution in _REQUIRED_DISTRIBUTIONS:
        try:
            versions[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            missing.append(distribution)
    if missing:
        raise RunnerError(f"missing required conversion dependencies: {', '.join(missing)}")
    if versions["markitdown"] != "0.1.7":
        raise RunnerError(f"markitdown 0.1.7 is required, found {versions['markitdown']}")
    return versions


def _measured_conversion_target(connection: Connection, request: WorkerRequest) -> None:
    captured = _CapturedWorkerConnection()
    try:
        run_conversion_worker(cast(Connection, captured), request)
        ok, code, output_chars = _summarize_production_message(captured.message)
        connection.send(
            (
                MEASUREMENT_PROTOCOL_VERSION,
                ok,
                code,
                output_chars,
                _self_high_water_bytes(),
            )
        )
    finally:
        connection.close()


def _synchronized_measured_conversion_target(
    connection: Connection,
    barrier: StartBarrier,
    request: WorkerRequest,
) -> None:
    try:
        barrier.wait(timeout=5)
    except BrokenBarrierError:
        connection.close()
        return
    _measured_conversion_target(connection, request)


class _CapturedWorkerConnection:
    def __init__(self) -> None:
        self.message: object | None = None

    def send(self, message: object) -> None:
        self.message = message

    def close(self) -> None:
        pass


def _over_limit_target(
    connection: Connection,
    memory_mb: int,
    timeout_seconds: float,
) -> None:
    ok = False
    code = "runner_over_limit_not_enforced"
    try:
        _apply_resource_limits(memory_mb=memory_mb, timeout_seconds=timeout_seconds)
        allocated = bytearray((memory_mb + 256) * MiB)
        del allocated
    except MemoryError:
        code = "tool_content_conversion_resource_exceeded"
    except OSError as exc:
        code = (
            "tool_content_conversion_resource_exceeded"
            if exc.errno == errno.ENOMEM
            else "runner_over_limit_os_error"
        )
    try:
        connection.send(
            (
                MEASUREMENT_PROTOCOL_VERSION,
                ok,
                code,
                0,
                _self_high_water_bytes(),
            )
        )
    finally:
        connection.close()


def _summarize_production_message(message: object) -> tuple[bool, str, int]:
    if (
        not isinstance(message, tuple)
        or len(message) != 4
        or message[0] != PROTOCOL_VERSION
        or not isinstance(message[1], bool)
        or not isinstance(message[2], str)
        or not isinstance(message[3], str)
    ):
        return False, "runner_invalid_production_protocol", 0
    _, ok, code, output = cast(tuple[int, bool, str, str], message)
    return ok, code, len(output)


def _self_high_water_bytes() -> int:
    import resource

    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


def measure_group(
    specs: Sequence[ChildSpec],
    *,
    timeout_seconds: float,
    start_barrier: StartBarrier | None = None,
) -> MeasurementGroup:
    if not specs:
        return MeasurementGroup(
            records=(),
            maximum_same_sample_rss_bytes=0,
            pids=(),
            parent_rss_samples_bytes=(),
            resources_released=True,
        )
    context = multiprocessing.get_context("spawn")
    running: list[_RunningChild] = []
    records: tuple[ProcessRecord, ...] = ()
    maximum_same_sample = 0
    synchronized_participants = 0
    parent_rss_samples: list[int] = []
    try:
        for spec in specs:
            receiver, sender = context.Pipe(duplex=False)
            process = context.Process(
                target=spec.target,
                args=(sender, *spec.args),
                daemon=True,
            )
            started_at = time.monotonic()
            try:
                process.start()
            except BaseException:
                receiver.close()
                sender.close()
                raise
            sender.close()
            if process.pid is None:
                receiver.close()
                _stop_process(process)
                raise RunnerError("conversion child started without a PID")
            running.append(
                _RunningChild(
                    spec=spec,
                    process=process,
                    receiver=receiver,
                    started_at=started_at,
                    pid=process.pid,
                )
            )

        if start_barrier is not None:
            try:
                start_barrier.wait(timeout=5)
            except BrokenBarrierError as exc:
                raise RunnerError("concurrent conversion start barrier failed") from exc
            synchronized_participants = len(running)

        deadline_seconds = timeout_seconds + 5
        while any(child.process.is_alive() for child in running):
            now = time.monotonic()
            child_rss_sum = 0
            for child in running:
                status = _read_process_memory(child.pid)
                rss = status.get("rss_bytes")
                high_water = status.get("high_water_bytes")
                if rss is not None and child.process.is_alive():
                    child_rss_sum += rss
                    child.max_sampled_rss_bytes = max(
                        child.max_sampled_rss_bytes or 0,
                        rss,
                    )
                if high_water is not None:
                    child.proc_high_water_bytes = max(
                        child.proc_high_water_bytes or 0,
                        high_water,
                    )
                if child.message is None and child.receiver.poll():
                    try:
                        child.message = child.receiver.recv()
                    except (EOFError, OSError):
                        pass
                if child.process.is_alive() and now - child.started_at > deadline_seconds:
                    child.timed_out = True
                    _stop_process(child.process)
            parent_rss = _current_parent_rss_bytes()
            parent_rss_samples.append(parent_rss)
            maximum_same_sample = max(maximum_same_sample, parent_rss + child_rss_sum)
            if any(child.process.is_alive() for child in running):
                time.sleep(POLL_INTERVAL_SECONDS)

        collected_records: list[ProcessRecord] = []
        for child in running:
            child.process.join(timeout=0)
            if child.message is None and child.receiver.poll():
                try:
                    child.message = child.receiver.recv()
                except (EOFError, OSError):
                    pass
            collected_records.append(_record_from_child(child))
        records = tuple(collected_records)
    finally:
        for child in running:
            _stop_process(child.process)
            child.receiver.close()
            child.process.close()
    owned_pids = {child.pid for child in running}
    active_pids = {process.pid for process in multiprocessing.active_children()}
    resources_released = all(child.receiver.closed for child in running) and not bool(
        owned_pids & active_pids
    )
    return MeasurementGroup(
        records=records,
        maximum_same_sample_rss_bytes=maximum_same_sample,
        pids=tuple(child.pid for child in running),
        parent_rss_samples_bytes=tuple(parent_rss_samples),
        resources_released=resources_released,
        synchronized_participants=synchronized_participants,
    )


def _record_from_child(child: _RunningChild) -> ProcessRecord:
    exit_code = child.process.exitcode
    ru_maxrss_bytes: int | None = None
    output_chars = 0
    if child.timed_out:
        ok = False
        code = "tool_exec_timeout"
    elif child.message is not None:
        ok, code, output_chars, ru_maxrss_bytes = _parse_measurement_message(child.message)
    elif exit_code == -signal.SIGXCPU:
        ok = False
        code = "tool_exec_timeout"
    else:
        ok = False
        code = "runner_child_exited_without_result"
    return ProcessRecord(
        label=child.spec.label,
        fixture=child.spec.fixture,
        ok=ok,
        code=code,
        output_chars=output_chars,
        wall_seconds=time.monotonic() - child.started_at,
        pid=child.pid,
        exit_code=exit_code,
        ru_maxrss_bytes=ru_maxrss_bytes,
        proc_high_water_bytes=child.proc_high_water_bytes,
        max_sampled_rss_bytes=child.max_sampled_rss_bytes,
    )


def _parse_measurement_message(message: object) -> tuple[bool, str, int, int | None]:
    if (
        not isinstance(message, tuple)
        or len(message) != 5
        or message[0] != MEASUREMENT_PROTOCOL_VERSION
        or not isinstance(message[1], bool)
        or not isinstance(message[2], str)
        or not isinstance(message[3], int)
        or not isinstance(message[4], int)
    ):
        return False, "runner_invalid_measurement_protocol", 0, None
    _, ok, code, output_chars, ru_maxrss_bytes = cast(
        tuple[int, bool, str, int, int],
        message,
    )
    return ok, code, output_chars, ru_maxrss_bytes


def _stop_process(process: BaseProcess) -> None:
    if not process.is_alive():
        process.join(timeout=0)
        return
    process.terminate()
    process.join(timeout=1)
    if process.is_alive():
        process.kill()
        process.join(timeout=1)


def _read_process_memory(pid: int) -> dict[str, int]:
    return _read_process_memory_from_path(Path(f"/proc/{pid}/status"))


def _current_parent_rss_bytes() -> int:
    value = _read_process_memory_from_path(Path("/proc/self/status"))
    rss = value.get("rss_bytes")
    if rss is None:
        raise RunnerError("could not read parent RSS from /proc/self/status")
    return rss


def _read_process_memory_from_path(path: Path) -> dict[str, int]:
    try:
        return parse_proc_status(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return {}


def _settled_parent_rss() -> tuple[int, tuple[int, ...]]:
    gc.collect()
    time.sleep(0.05)
    samples: list[int] = []
    for _ in range(5):
        samples.append(_current_parent_rss_bytes())
        time.sleep(0.02)
    return int(statistics.median(samples)), tuple(samples)


def _conversion_spec(
    fixture: Fixture,
    *,
    label: str,
    memory_mb: int,
    timeout_seconds: float,
) -> ChildSpec:
    return ChildSpec(
        label=label,
        fixture=fixture.relative_path,
        target=_measured_conversion_target,
        args=(fixture.request(memory_mb=memory_mb, timeout_seconds=timeout_seconds),),
    )


def _synchronized_conversion_spec(
    fixture: Fixture,
    *,
    label: str,
    memory_mb: int,
    timeout_seconds: float,
    barrier: StartBarrier,
) -> ChildSpec:
    return ChildSpec(
        label=label,
        fixture=fixture.relative_path,
        target=_synchronized_measured_conversion_target,
        args=(
            barrier,
            fixture.request(memory_mb=memory_mb, timeout_seconds=timeout_seconds),
        ),
    )


def _over_limit_spec(*, memory_mb: int, timeout_seconds: float) -> ChildSpec:
    return ChildSpec(
        label="deterministic-over-limit",
        fixture=None,
        target=_over_limit_target,
        args=(memory_mb, timeout_seconds),
    )


def _blocking_protocol_target(connection: Connection, request: WorkerRequest) -> None:
    try:
        memory_mb = request.get("memory_mb")
        timeout_seconds = request.get("timeout_seconds")
        if not isinstance(memory_mb, int) or not isinstance(timeout_seconds, (int, float)):
            return
        _apply_resource_limits(
            memory_mb=memory_mb,
            timeout_seconds=float(timeout_seconds),
        )
        time.sleep(60)
    finally:
        connection.close()


def _stubborn_protocol_target(connection: Connection, request: WorkerRequest) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    _blocking_protocol_target(connection, request)


async def _parse_fixture_with_parent(
    parser: DocumentParser,
    fixture: Fixture,
    *,
    user_id: UUID,
) -> str:
    if fixture.conversion_format == "html":
        return await parser.parse_html(
            fixture.data,
            user_id=user_id,
            charset="utf-8",
            base_url="https://example.invalid/fixture/",
            mode="markdown",
            max_chars=50_000,
        )
    return await parser.parse(
        fixture.relative_path,
        fixture.data,
        user_id=user_id,
    )


def _active_child_pids() -> set[int]:
    return {process.pid for process in multiprocessing.active_children() if process.pid is not None}


def _open_file_descriptor_count() -> int:
    try:
        return len(tuple(Path("/proc/self/fd").iterdir()))
    except OSError as exc:
        raise RunnerError("could not inspect parent file descriptors") from exc


async def _wait_for_new_child(before: set[int]) -> set[int]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5
    while loop.time() < deadline:
        started = _active_child_pids() - before
        if started:
            return started
        await asyncio.sleep(0.01)
    raise RunnerError("parent-protocol cancellation child did not start")


async def _wait_for_sigterm_ignore(pids: set[int]) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5
    sigterm_bit = 1 << (signal.SIGTERM - 1)
    while loop.time() < deadline:
        ignored: list[bool] = []
        for pid in pids:
            try:
                lines = Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines()
            except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
                ignored.append(False)
                continue
            sig_ign = next((line.split()[1] for line in lines if line.startswith("SigIgn:")), "0")
            ignored.append(bool(int(sig_ign, 16) & sigterm_bit))
        if ignored and all(ignored):
            return
        await asyncio.sleep(0.01)
    raise RunnerError("parent-protocol cancellation child did not initialize")


async def _exercise_parent_protocol(
    fixture: Fixture,
    *,
    rounds: int,
    memory_mb: int,
    timeout_seconds: float,
) -> ProtocolExercise:
    admission = KeyedAdmission(
        global_limit=ACCEPTANCE_CONCURRENCY,
        per_key_limit=1,
        timeout_seconds=max(1.0, timeout_seconds),
    )
    normal_parser = DocumentParser(
        admission=admission,
        memory_mb=memory_mb,
        timeout_seconds=timeout_seconds,
    )
    timeout_parser = DocumentParser(
        admission=admission,
        memory_mb=memory_mb,
        timeout_seconds=0.05,
        worker_target=_blocking_protocol_target,
    )
    cancellation_parser = DocumentParser(
        admission=admission,
        memory_mb=memory_mb,
        timeout_seconds=timeout_seconds,
        worker_target=_stubborn_protocol_target,
    )
    user_id = uuid4()
    active_before = _active_child_pids()
    file_descriptors_before = _open_file_descriptor_count()
    observed_pids: set[int] = set()
    rounds_completed = 0
    success_count = 0
    malformed_count = 0
    timeout_count = 0
    cancellation_count = 0
    permit_acquisitions = 0
    failure: str | None = None

    for _ in range(rounds):
        cancellation_task: asyncio.Task[str] | None = None
        try:
            await _parse_fixture_with_parent(normal_parser, fixture, user_id=user_id)
            success_count += 1
            permit_acquisitions += 1

            try:
                await normal_parser.parse("malformed.docx", b"not a zip", user_id=user_id)
            except ToolError as malformed_error:
                if malformed_error.code is not ErrorCode.TOOL_CONTENT_CONVERSION_FAILED:
                    raise RunnerError(
                        "malformed conversion returned unexpected code "
                        f"{malformed_error.code.value}"
                    ) from malformed_error
            else:
                raise RunnerError("malformed conversion unexpectedly succeeded")
            malformed_count += 1
            permit_acquisitions += 1

            try:
                await timeout_parser.parse("timeout.docx", b"unused", user_id=user_id)
            except ToolError as timeout_error:
                if timeout_error.code is not ErrorCode.TOOL_EXEC_TIMEOUT:
                    raise RunnerError(
                        f"timed conversion returned unexpected code {timeout_error.code.value}"
                    ) from timeout_error
            else:
                raise RunnerError("timed conversion unexpectedly succeeded")
            timeout_count += 1
            permit_acquisitions += 1

            before_cancel = _active_child_pids()
            cancellation_task = asyncio.create_task(
                cancellation_parser.parse("cancel.docx", b"unused", user_id=user_id)
            )
            started = await _wait_for_new_child(before_cancel)
            observed_pids.update(started)
            permit_acquisitions += 1
            await _wait_for_sigterm_ignore(started)
            cancellation_task.cancel()
            await asyncio.sleep(0.15)
            cancellation_task.cancel()
            await asyncio.sleep(0.01)
            cancellation_task.cancel()
            try:
                await cancellation_task
            except asyncio.CancelledError:
                pass
            else:
                raise RunnerError("cancelled conversion unexpectedly completed")
            cancellation_count += 1
            cancellation_task = None

            await asyncio.sleep(0)
            leaked = observed_pids & _active_child_pids()
            if leaked or admission.entry_count:
                raise RunnerError("parent-protocol resources remained after a round")
            rounds_completed += 1
        except Exception as exc:
            failure = f"{type(exc).__name__}: {exc}"
            break
        finally:
            if cancellation_task is not None:
                if not cancellation_task.done():
                    cancellation_task.cancel()
                try:
                    await cancellation_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

    leaked_child_pids = tuple(sorted(_active_child_pids() - active_before))
    file_descriptors_after = _open_file_descriptor_count()
    return ProtocolExercise(
        rounds_requested=rounds,
        rounds_completed=rounds_completed,
        success_count=success_count,
        malformed_count=malformed_count,
        timeout_count=timeout_count,
        cancellation_count=cancellation_count,
        permit_acquisitions=permit_acquisitions,
        admission_entries_after=admission.entry_count,
        leaked_child_pids=leaked_child_pids,
        file_descriptors_before=file_descriptors_before,
        file_descriptors_after=file_descriptors_after,
        failure=failure,
    )


def run_acceptance(
    fixtures: tuple[Fixture, ...],
    *,
    arguments: Arguments,
    timeout_seconds: float,
    package_versions: dict[str, str],
) -> tuple[dict[str, object], bool]:
    baseline_parent_rss, baseline_parent_rss_samples = _settled_parent_rss()
    owned_pids: set[int] = set()
    all_resources_released = True
    normal_records: list[ProcessRecord] = []

    for repetition in range(arguments.repeat):
        for fixture in fixtures:
            group = measure_group(
                (
                    _conversion_spec(
                        fixture,
                        label=f"normal-{repetition + 1}-{fixture.relative_path}",
                        memory_mb=arguments.memory_mb,
                        timeout_seconds=timeout_seconds,
                    ),
                ),
                timeout_seconds=timeout_seconds,
            )
            normal_records.extend(group.records)
            owned_pids.update(group.pids)
            all_resources_released = all_resources_released and group.resources_released
        if not all(record.ok for record in normal_records):
            break

    over_limit_group = measure_group(
        (_over_limit_spec(memory_mb=arguments.memory_mb, timeout_seconds=timeout_seconds),),
        timeout_seconds=timeout_seconds,
    )
    owned_pids.update(over_limit_group.pids)
    all_resources_released = all_resources_released and over_limit_group.resources_released
    over_limit_record = over_limit_group.records[0]

    successful_normal = [record for record in normal_records if record.ok]
    worst_fixture: Fixture | None = None
    if successful_normal:
        worst_record = max(successful_normal, key=lambda record: record.high_water_bytes or 0)
        worst_fixture = next(
            fixture for fixture in fixtures if fixture.relative_path == worst_record.fixture
        )

    recovery_group = MeasurementGroup((), 0, (), (), True)
    concurrent_group = MeasurementGroup((), 0, (), (), True)
    if worst_fixture is not None:
        recovery_group = measure_group(
            (
                _conversion_spec(
                    worst_fixture,
                    label="post-over-limit-recovery",
                    memory_mb=arguments.memory_mb,
                    timeout_seconds=timeout_seconds,
                ),
            ),
            timeout_seconds=timeout_seconds,
        )
        owned_pids.update(recovery_group.pids)
        all_resources_released = all_resources_released and recovery_group.resources_released
        start_barrier = multiprocessing.get_context("spawn").Barrier(arguments.concurrency + 1)
        concurrent_group = measure_group(
            tuple(
                _synchronized_conversion_spec(
                    worst_fixture,
                    label=f"concurrent-{index + 1}",
                    memory_mb=arguments.memory_mb,
                    timeout_seconds=timeout_seconds,
                    barrier=start_barrier,
                )
                for index in range(arguments.concurrency)
            ),
            timeout_seconds=timeout_seconds,
            start_barrier=start_barrier,
        )
        owned_pids.update(concurrent_group.pids)
        all_resources_released = all_resources_released and concurrent_group.resources_released

    protocol_fixture = next(fixture for fixture in fixtures if fixture.conversion_format == "html")
    protocol_exercise = asyncio.run(
        _exercise_parent_protocol(
            protocol_fixture,
            rounds=arguments.repeat,
            memory_mb=arguments.memory_mb,
            timeout_seconds=timeout_seconds,
        )
    )
    final_parent_rss, final_parent_rss_samples = _settled_parent_rss()
    active_after = {process.pid for process in multiprocessing.active_children()}
    owned_resources_released = all_resources_released and not bool(owned_pids & active_after)
    recovery_record = (
        recovery_group.records[0]
        if recovery_group.records
        else _not_run_record("post-over-limit-recovery")
    )
    evaluation = EvaluationInput(
        normal_records=tuple(normal_records),
        over_limit_record=over_limit_record,
        recovery_record=recovery_record,
        concurrent_records=concurrent_group.records,
        concurrent_peak_rss_bytes=concurrent_group.maximum_same_sample_rss_bytes,
        concurrent_synchronized_participants=concurrent_group.synchronized_participants,
        baseline_parent_rss_bytes=baseline_parent_rss,
        final_parent_rss_bytes=final_parent_rss,
        memory_limit_bytes=arguments.memory_mb * MiB,
        timeout_seconds=timeout_seconds,
        owned_resources_released=owned_resources_released,
        protocol_exercise=protocol_exercise,
    )
    gates = evaluate_gates(evaluation)
    passed = all(bool(gate["passed"]) for gate in gates.values())
    evidence: dict[str, object] = {
        "schema_version": 2,
        "status": "passed" if passed else "failed",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "command": [sys.executable, *sys.argv],
        "package_versions": package_versions,
        "config": {
            "memory_mb": arguments.memory_mb,
            "concurrency": arguments.concurrency,
            "repeat": arguments.repeat,
            "timeout_seconds": timeout_seconds,
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
            "rlimit_as_bytes": arguments.memory_mb * MiB,
        },
        "fixtures": [
            {
                "path": fixture.relative_path,
                "format": fixture.conversion_format,
                "size_bytes": len(fixture.data),
            }
            for fixture in fixtures
        ],
        "baseline_parent_rss_bytes": baseline_parent_rss,
        "baseline_parent_rss_samples_bytes": list(baseline_parent_rss_samples),
        "final_parent_rss_bytes": final_parent_rss,
        "final_parent_rss_samples_bytes": list(final_parent_rss_samples),
        "normal_records": [record.to_json() for record in normal_records],
        "over_limit_record": over_limit_record.to_json(),
        "recovery_record": recovery_record.to_json(),
        "concurrent_records": [record.to_json() for record in concurrent_group.records],
        "concurrent_peak_rss_bytes": concurrent_group.maximum_same_sample_rss_bytes,
        "concurrent_synchronized_participants": concurrent_group.synchronized_participants,
        "concurrent_parent_rss_samples_bytes": list(concurrent_group.parent_rss_samples_bytes),
        "cgroup": _read_cgroup_memory(),
        "parent_protocol_exercise": asdict(protocol_exercise),
        "gates": gates,
        "failed_gates": [name for name, gate in gates.items() if not bool(gate["passed"])],
    }
    return evidence, passed


def _not_run_record(label: str) -> ProcessRecord:
    return ProcessRecord(
        label=label,
        fixture=None,
        ok=False,
        code="runner_not_run",
        output_chars=0,
        wall_seconds=0,
        pid=0,
        exit_code=None,
        ru_maxrss_bytes=None,
        proc_high_water_bytes=None,
        max_sampled_rss_bytes=None,
    )


def _read_cgroup_memory() -> dict[str, int | None]:
    return {
        "memory_current_bytes": _read_optional_int(Path("/sys/fs/cgroup/memory.current")),
        "memory_peak_bytes": _read_optional_int(Path("/sys/fs/cgroup/memory.peak")),
    }


def _read_optional_int(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, PermissionError, OSError, ValueError):
        return None


def _parse_arguments(argv: Sequence[str] | None) -> Arguments:
    parser = argparse.ArgumentParser(description="Measure isolated content-conversion memory")
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--memory-mb", type=int, required=True)
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--repeat", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    namespace = parser.parse_args(argv)
    return Arguments(
        fixtures=cast(Path, namespace.fixtures),
        memory_mb=cast(int, namespace.memory_mb),
        concurrency=cast(int, namespace.concurrency),
        repeat=cast(int, namespace.repeat),
        output=cast(Path, namespace.output),
    )


def _validate_acceptance_configuration(
    arguments: Arguments,
    *,
    configured_memory_mb: int,
    configured_concurrency: int,
) -> None:
    if arguments.memory_mb != ACCEPTANCE_MEMORY_MB:
        raise RunnerError(f"--memory-mb must be {ACCEPTANCE_MEMORY_MB} for Py4 acceptance")
    if arguments.concurrency != ACCEPTANCE_CONCURRENCY:
        raise RunnerError(f"--concurrency must be {ACCEPTANCE_CONCURRENCY} for Py4 acceptance")
    if arguments.repeat != ACCEPTANCE_REPEAT:
        raise RunnerError(f"--repeat must be {ACCEPTANCE_REPEAT} for Py4 acceptance")
    if configured_memory_mb != arguments.memory_mb:
        raise RunnerError("--memory-mb does not match OPENOCTOPUS_CONTENT_CONVERSION_MEMORY_MB")
    if configured_concurrency != arguments.concurrency:
        raise RunnerError(
            "--concurrency does not match OPENOCTOPUS_CONTENT_CONVERSION_MAX_CONCURRENCY"
        )


def _write_json(path: Path, evidence: dict[str, object]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        raise RunnerError(f"could not write evidence file {path}: {exc}") from exc


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parse_arguments(argv)
    try:
        if sys.platform != "linux":
            raise RunnerError("content conversion memory acceptance requires Linux")
        fixtures = discover_fixtures(arguments.fixtures)
        package_versions = resolved_package_versions()
        try:
            settings = get_settings()
        except Exception as exc:
            raise RunnerError(f"could not load required server configuration: {exc}") from exc
        _validate_acceptance_configuration(
            arguments,
            configured_memory_mb=settings.content_conversion_memory_mb,
            configured_concurrency=settings.content_conversion_max_concurrency,
        )
        evidence, passed = run_acceptance(
            fixtures,
            arguments=arguments,
            timeout_seconds=float(settings.content_conversion_timeout_seconds),
            package_versions=package_versions,
        )
        _write_json(arguments.output, evidence)
        if not passed:
            failed = cast(list[str], evidence["failed_gates"])
            print(f"content conversion memory check failed: {', '.join(failed)}", file=sys.stderr)
            return 1
        print(f"content conversion memory evidence written to {arguments.output}")
        return 0
    except RunnerError as exc:
        failure: dict[str, object] = {
            "schema_version": 1,
            "status": "error",
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "command": [sys.executable, *sys.argv],
            "errors": [str(exc)],
        }
        try:
            _write_json(arguments.output, failure)
        except RunnerError as write_exc:
            print(f"content conversion memory check failed: {write_exc}", file=sys.stderr)
        print(f"content conversion memory check failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
