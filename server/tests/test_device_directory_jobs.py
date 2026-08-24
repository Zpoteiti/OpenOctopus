from __future__ import annotations

import asyncio
import hashlib
import json
from collections import deque
from dataclasses import dataclass, field
from types import SimpleNamespace
from uuid import uuid4

import pytest

from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.devices.registry import (
    ConnectionHandle,
    DeviceBusyError,
    DeviceOutcomeUnknownError,
    DeviceRouteSnapshot,
    DeviceUnavailableError,
)
from openctopus_server.devices.transfer import TransferIntegrityError
from openctopus_server.devices.workspace import (
    DestinationDirectoryJobStatus,
    DirectoryCommandResult,
    DirectorySourceProbe,
    SourceDirectoryJobStatus,
)
from openctopus_server.directory_contract import DirectoryManifest, canonical_json_bytes
from openctopus_server.tools.device_directory_jobs import DeviceDirectoryJobController


def _source_request_digest(path: str) -> str:
    payload = json.dumps(
        {"path": path, "role": "source", "version": 1},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _destination_request_digest(path: str, manifest: DirectoryManifest) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "role": "destination",
                "dst_path": path,
                "manifest": manifest,
                "version": 1,
            }
        )
    ).hexdigest()


def _success(content: dict[str, object]) -> SimpleNamespace:
    return SimpleNamespace(is_error=False, code=None, content=json.dumps(content))


def _error(code: str) -> SimpleNamespace:
    return SimpleNamespace(is_error=True, code=code, content="error")


def _command(state: str, digest: str) -> SimpleNamespace:
    return _success({"state": state, "expected_digest": digest})


def _source_status(
    *,
    digest: str,
    state: str = "scanning",
    progress_seq: int = 0,
    probe: dict[str, object] | None = None,
    terminal_error: dict[str, object] | None = None,
) -> SimpleNamespace:
    return _success(
        {
            "state": state,
            "expected_digest": digest,
            "progress_seq": progress_seq,
            "entries_processed": 0 if probe is None else 1,
            "files_processed": 0 if probe is None else 1,
            "bytes_processed": 0 if probe is None else 1,
            "probe": probe,
            "terminal_result": None,
            "terminal_error": terminal_error,
        }
    )


def _destination_status(
    *,
    digest: str,
    state: str,
    progress_seq: int = 0,
    terminal_result: dict[str, object] | None = None,
    terminal_error: dict[str, object] | None = None,
) -> SimpleNamespace:
    return _success(
        {
            "state": state,
            "expected_digest": digest,
            "progress_seq": progress_seq,
            "files_processed": 0,
            "bytes_processed": 0,
            "cleanup_complete": state == "failed",
            "terminal_result": terminal_result,
            "terminal_error": terminal_error,
        }
    )


def _local_status(*, digest: str, state: str) -> SimpleNamespace:
    return _success(
        {
            "state": state,
            "phase": "waiting",
            "expected_digest": digest,
            "progress_seq": 0,
            "files_processed": 0,
            "bytes_processed": 0,
            "terminal_result": None,
            "terminal_error": None,
        }
    )


@dataclass(slots=True)
class _Registry:
    outcomes: deque[object]
    calls: list[dict[str, object]] = field(default_factory=list)
    active_calls: int = 0
    max_active_calls: int = 0

    async def dispatch_tool_on_snapshot(self, **kwargs: object) -> object:
        self.active_calls += 1
        self.max_active_calls = max(self.max_active_calls, self.active_calls)
        try:
            self.calls.append(dict(kwargs))
            outcome = self.outcomes.popleft()
            issued = False
            if isinstance(outcome, _TimedOutcome):
                outcome.clock.value += outcome.seconds
                outcome = outcome.result
            if isinstance(outcome, _BlockingOutcome):
                on_issued = kwargs.get("on_issued")
                if callable(on_issued):
                    on_issued()
                    issued = True
                outcome.started.set()
                await outcome.release.wait()
                outcome = outcome.result
            if isinstance(outcome, BaseException):
                if not isinstance(outcome, DeviceUnavailableError):
                    on_issued = kwargs.get("on_issued")
                    if callable(on_issued) and not issued:
                        on_issued()
                raise outcome
            on_issued = kwargs.get("on_issued")
            if callable(on_issued) and not issued:
                on_issued()
            return outcome
        finally:
            self.active_calls -= 1


@dataclass(slots=True)
class _BlockingOutcome:
    result: object
    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(slots=True)
class _TimedOutcome:
    result: object
    clock: _Clock
    seconds: float


@dataclass(slots=True)
class _Clock:
    value: float = 0.0

    def now(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.value += seconds
        await asyncio.sleep(0)


@dataclass(slots=True)
class _TickClock:
    step: float
    value: float = 0.0

    def now(self) -> float:
        current = self.value
        self.value += self.step
        return current

    async def sleep(self, seconds: float) -> None:
        self.value += seconds
        await asyncio.sleep(0)


def _controller(
    outcomes: list[object],
    *,
    idle_timeout_seconds: float = 3.0,
    clock: _Clock | _TickClock | None = None,
) -> tuple[DeviceDirectoryJobController, _Registry]:
    registry = _Registry(deque(outcomes))
    route = DeviceRouteSnapshot(ConnectionHandle(new_uuid7(), 7), 4, "laptop")
    controller = DeviceDirectoryJobController(
        registry=registry,
        route=route,
        user_id=new_uuid7(),
        directory_operation_id=new_uuid7(),
        idle_timeout_seconds=idle_timeout_seconds,
        poll_interval_seconds=1.0,
        monotonic=(clock.now if clock is not None else None),
        sleep=(clock.sleep if clock is not None else None),
    )
    return controller, registry


def _operations(registry: _Registry) -> list[object]:
    return [call["args"]["operation"] for call in registry.calls]  # type: ignore[index]


def _timeouts(registry: _Registry, operation: str) -> list[float]:
    return [
        call["timeout_seconds"]  # type: ignore[misc]
        for call in registry.calls
        if call["args"]["operation"] == operation  # type: ignore[index]
    ]


@pytest.mark.asyncio
async def test_source_start_result_loss_reconciles_by_status_without_replay() -> None:
    digest = _source_request_digest("source")
    controller, registry = _controller(
        [
            DeviceOutcomeUnknownError("lost"),
            _source_status(digest=digest, progress_seq=1),
        ]
    )

    result = await controller.start_source_probe("source")
    repeated = await controller.start_source_probe("source")

    assert isinstance(result, SourceDirectoryJobStatus)
    assert repeated is result
    assert _operations(registry) == [
        "transfer_source_probe_start",
        "transfer_source_probe_status",
    ]


@pytest.mark.asyncio
async def test_source_start_reconcile_retries_transient_status_capacity_busy() -> None:
    clock = _Clock()
    digest = _source_request_digest("source")
    controller, registry = _controller(
        [
            DeviceOutcomeUnknownError("lost"),
            _error("workspace_transfer_busy"),
            _source_status(digest=digest, progress_seq=1),
        ],
        idle_timeout_seconds=2.0,
        clock=clock,
    )

    result = await controller.start_source_probe("source")

    assert isinstance(result, SourceDirectoryJobStatus)
    assert _operations(registry).count("transfer_source_probe_status") == 2
    assert _timeouts(registry, "transfer_source_probe_status") == [2.0, 1.0]


@pytest.mark.asyncio
async def test_source_page_retry_clamps_each_call_to_the_remaining_window() -> None:
    clock = _Clock()
    manifest = _manifest()
    request_digest = _source_request_digest("source")
    probe = DirectorySourceProbe(
        root_identity=manifest.root_identity,
        scanned_entries=manifest.scanned_entries,
        file_count=len(manifest.entries),
        total_bytes=manifest.total_bytes,
        manifest_sha256=manifest.manifest_sha256,
        page_count=1,
    )
    page = {
        "offset": 0,
        "next_offset": None,
        "items": [
            {
                "kind": "directory",
                "relative_path": "nested",
                "identity": "dir-etag",
            },
            {
                "kind": "file",
                "relative_path": "nested/a.txt",
                "size": 1,
                "fingerprint": "file-etag",
            },
        ],
    }
    controller, registry = _controller(
        [
            _command("running", request_digest),
            _source_status(
                digest=manifest.manifest_sha256,
                state="ready_retrieval",
                progress_seq=1,
                probe=probe.model_dump(mode="json"),
            ),
            DeviceOutcomeUnknownError("page result lost"),
            _success(page),
        ],
        idle_timeout_seconds=2.0,
        clock=clock,
    )
    await controller.start_source_probe("source")
    await controller.get_source_status()

    result = await controller.get_source_page(0)

    assert result.offset == 0
    assert _timeouts(registry, "transfer_source_probe_page") == [2.0, 1.0]


@pytest.mark.asyncio
async def test_prepare_result_loss_reconciles_and_never_replays_mutation() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    controller, registry = _controller(
        [
            _command("running", digest),
            DeviceOutcomeUnknownError("lost"),
            _destination_status(digest=digest, state="reserved", progress_seq=2),
        ]
    )
    await controller.start_destination_preflight("destination", manifest)

    result = await controller.prepare_destination()
    repeated = await controller.prepare_destination()

    assert isinstance(result, DestinationDirectoryJobStatus)
    assert result.state == "reserved"
    assert repeated is result
    assert _operations(registry).count("transfer_directory_prepare") == 1


@pytest.mark.asyncio
async def test_prepare_loss_with_ready_status_is_stably_not_applied() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    controller, registry = _controller(
        [
            _command("running", digest),
            DeviceOutcomeUnknownError("lost"),
            _destination_status(digest=digest, state="ready"),
        ]
    )
    await controller.start_destination_preflight("destination", manifest)

    with pytest.raises(DeviceUnavailableError):
        await controller.prepare_destination()
    with pytest.raises(DeviceUnavailableError):
        await controller.prepare_destination()

    assert _operations(registry).count("transfer_directory_prepare") == 1
    assert _operations(registry).count("transfer_directory_status") == 1


@pytest.mark.asyncio
async def test_authorization_result_loss_is_not_treated_as_authorized_or_replayed() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    controller, registry = _controller(
        [
            _command("running", digest),
            DeviceOutcomeUnknownError("lost"),
            _destination_status(digest=digest, state="reserved"),
        ]
    )
    await controller.start_destination_preflight("destination", manifest)
    transfer_uuid = new_uuid7()

    with pytest.raises(DeviceOutcomeUnknownError):
        await controller.authorize_destination_child(transfer_uuid, "a.txt")
    with pytest.raises(DeviceOutcomeUnknownError):
        await controller.authorize_destination_child(transfer_uuid, "a.txt")

    assert _operations(registry).count("transfer_directory_authorize_child") == 1
    assert _operations(registry).count("transfer_directory_status") == 1


@pytest.mark.asyncio
async def test_duplicate_progress_does_not_extend_no_progress_deadline() -> None:
    clock = _Clock()
    digest = _source_request_digest("source")
    cancelled = {
        "code": "workspace_transfer_timeout",
        "message": "timed out",
    }
    controller, registry = _controller(
        [
            _command("running", digest),
            _source_status(digest=digest, progress_seq=1),
            _source_status(digest=digest, progress_seq=1),
            _command("accepted", digest),
            _source_status(
                digest=digest,
                state="failed",
                progress_seq=1,
                terminal_error=cancelled,
            ),
        ],
        idle_timeout_seconds=2.0,
        clock=clock,
    )
    await controller.start_source_probe("source")

    result = await controller.wait_source_until(
        frozenset({"ready_retrieval", "succeeded", "failed", "outcome_unknown"})
    )

    assert result.state == "failed"
    assert _operations(registry).count("transfer_source_probe_cancel") == 1
    assert clock.value == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_wait_uses_previously_observed_progress_as_its_baseline() -> None:
    clock = _Clock()
    digest = _source_request_digest("source")
    timed_repeat = _TimedOutcome(
        _source_status(digest=digest, progress_seq=1),
        clock,
        1.5,
    )
    timeout_error = {
        "code": "workspace_transfer_timeout",
        "message": "timed out",
    }
    controller, registry = _controller(
        [
            _command("running", digest),
            _source_status(digest=digest, progress_seq=1),
            timed_repeat,
            _command("accepted", digest),
            _source_status(
                digest=digest,
                state="failed",
                progress_seq=1,
                terminal_error=timeout_error,
            ),
        ],
        idle_timeout_seconds=2.0,
        clock=clock,
    )
    await controller.start_source_probe("source")
    await controller.get_source_status()

    result = await controller.wait_source_until(
        frozenset({"ready_retrieval", "succeeded", "failed", "outcome_unknown"})
    )

    assert result.state == "failed"
    assert clock.value == pytest.approx(2.0)
    assert _operations(registry).count("transfer_source_probe_cancel") == 1


@pytest.mark.asyncio
async def test_wait_retries_ambiguous_status_without_extending_deadline() -> None:
    digest = _source_request_digest("source")
    controller, registry = _controller(
        [
            _command("running", digest),
            DeviceOutcomeUnknownError("poll lost"),
            _source_status(
                digest=digest,
                state="succeeded",
                progress_seq=1,
                probe={"kind": "file", "size": 1, "fingerprint": "etag"},
            ),
        ]
    )
    await controller.start_source_probe("source")

    result = await controller.wait_source_until(frozenset({"succeeded"}))

    assert result.state == "succeeded"
    assert _operations(registry).count("transfer_source_probe_status") == 2


@pytest.mark.asyncio
async def test_status_calls_are_serialized_to_one_pending_poll() -> None:
    digest = _source_request_digest("source")
    blocked = _BlockingOutcome(_source_status(digest=digest))
    controller, registry = _controller(
        [
            _command("running", digest),
            blocked,
            _source_status(digest=digest),
        ]
    )
    await controller.start_source_probe("source")

    first = asyncio.create_task(controller.get_source_status())
    await blocked.started.wait()
    second = asyncio.create_task(controller.get_source_status())
    await asyncio.sleep(0)
    assert registry.max_active_calls == 1
    blocked.release.set()
    await asyncio.gather(first, second)
    assert registry.max_active_calls == 1


@pytest.mark.asyncio
async def test_malformed_result_maps_to_transfer_integrity_error() -> None:
    controller, _ = _controller([_success({"state": "scanning", "progress_seq": "not-an-int"})])

    with pytest.raises(TransferIntegrityError):
        await controller.start_source_probe("source")


@pytest.mark.asyncio
async def test_wrong_command_acknowledgement_state_is_an_integrity_error() -> None:
    digest = _source_request_digest("source")
    controller, _ = _controller([_command("accepted", digest)])

    with pytest.raises(TransferIntegrityError, match="invalid acknowledgement"):
        await controller.start_source_probe("source")


@pytest.mark.asyncio
async def test_acknowledged_command_error_is_cached_without_status_reconcile() -> None:
    controller, registry = _controller([_error("tool_device_busy")])

    with pytest.raises(DeviceBusyError, match="capacity is exhausted"):
        await controller.start_source_probe("source")
    with pytest.raises(DeviceBusyError, match="capacity is exhausted"):
        await controller.start_source_probe("source")

    assert _operations(registry) == ["transfer_source_probe_start"]


@pytest.mark.asyncio
async def test_read_only_route_loss_is_unreachable_but_mutation_loss_is_unknown() -> None:
    read_only, _ = _controller(
        [
            DeviceOutcomeUnknownError("lost"),
            DeviceUnavailableError("replaced"),
        ]
    )
    with pytest.raises(DeviceUnavailableError):
        await read_only.start_source_probe("source")

    mutation, _ = _controller(
        [
            _command(
                "running",
                _destination_request_digest("destination", _manifest()),
            ),
            DeviceOutcomeUnknownError("lost"),
            DeviceUnavailableError("replaced"),
        ]
    )
    await mutation.start_destination_preflight("destination", _manifest())
    with pytest.raises(DeviceOutcomeUnknownError):
        await mutation.prepare_destination()


@pytest.mark.asyncio
async def test_wait_route_loss_after_prepare_is_outcome_unknown() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    controller, _ = _controller(
        [
            _command("running", digest),
            _command("accepted", digest),
            DeviceUnavailableError("replaced"),
        ]
    )
    await controller.start_destination_preflight("destination", manifest)
    await controller.prepare_destination()

    with pytest.raises(DeviceOutcomeUnknownError):
        await controller.wait_destination_until(frozenset({"reserved"}))


@pytest.mark.asyncio
async def test_wait_deadline_expiry_between_precheck_and_dispatch_reconciles_mutation() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    clock = _TickClock(step=0.6)
    controller, registry = _controller(
        [
            _command("running", digest),
            _command("accepted", digest),
            _command("accepted", digest),
        ],
        idle_timeout_seconds=1.0,
        clock=clock,
    )
    await controller.start_destination_preflight("destination", manifest)
    await controller.prepare_destination()

    with pytest.raises(DeviceOutcomeUnknownError):
        await controller.wait_destination_until(frozenset({"finalized_held"}))

    assert _operations(registry).count("transfer_directory_status") == 0
    assert _operations(registry).count("transfer_directory_cancel") == 1


@pytest.mark.asyncio
async def test_local_start_loss_uses_ready_not_started_without_replay() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    controller, registry = _controller(
        [
            _command("running", digest),
            DeviceOutcomeUnknownError("lost"),
            _local_status(digest=digest, state="ready_not_started"),
        ]
    )
    await controller.start_destination_preflight("destination", manifest)

    with pytest.raises(DeviceUnavailableError):
        await controller.start_local_directory(
            source_path="source",
            dst_path="destination",
            mode="copy",
            manifest_sha256=manifest.manifest_sha256,
        )
    assert _operations(registry).count("transfer_local_directory_start") == 1


@pytest.mark.asyncio
async def test_finish_loss_without_finalizing_evidence_is_outcome_unknown() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    controller, registry = _controller(
        [
            _command("running", digest),
            _command("accepted", digest),
            DeviceOutcomeUnknownError("finish lost"),
            _destination_status(digest=digest, state="copying", progress_seq=2),
        ]
    )
    await controller.start_destination_preflight("destination", manifest)
    await controller.prepare_destination()

    with pytest.raises(DeviceOutcomeUnknownError):
        await controller.finish_destination()

    assert _operations(registry).count("transfer_directory_finish") == 1


@pytest.mark.asyncio
async def test_source_cleanup_loss_with_held_evidence_is_not_treated_as_started() -> None:
    manifest = _manifest()
    request_digest = _source_request_digest("source")
    probe = DirectorySourceProbe(
        root_identity=manifest.root_identity,
        scanned_entries=manifest.scanned_entries,
        file_count=len(manifest.entries),
        total_bytes=manifest.total_bytes,
        manifest_sha256=manifest.manifest_sha256,
        page_count=1,
    )
    held = _source_status(
        digest=manifest.manifest_sha256,
        state="held",
        progress_seq=2,
        probe=probe.model_dump(mode="json"),
    )
    controller, registry = _controller(
        [
            _command("running", request_digest),
            held,
            DeviceOutcomeUnknownError("cleanup lost"),
            held,
        ]
    )
    await controller.start_source_probe("source")
    await controller.get_source_status()

    with pytest.raises(DeviceUnavailableError):
        await controller.start_source_cleanup()

    assert _operations(registry).count("transfer_source_cleanup") == 1


@pytest.mark.asyncio
async def test_terminal_status_can_be_released_exactly_once() -> None:
    terminal_error = {"code": "workspace_file_changed", "message": "changed"}
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    controller, registry = _controller(
        [
            _command("running", digest),
            _destination_status(
                digest=digest,
                state="failed",
                progress_seq=1,
                terminal_error=terminal_error,
            ),
            _command("released", digest),
        ]
    )
    await controller.start_destination_preflight("destination", manifest)

    terminal = await controller.wait_destination_until(
        frozenset({"ready", "failed", "outcome_unknown"})
    )
    release = await controller.release_destination()
    repeated = await controller.release_destination()

    assert terminal.state == "failed"
    assert isinstance(release, DirectoryCommandResult)
    assert release.state == "released"
    assert repeated is release
    assert _operations(registry).count("transfer_directory_release") == 1


@pytest.mark.asyncio
async def test_lost_release_result_retries_only_the_exact_release() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    clock = _Clock()
    controller, registry = _controller(
        [
            _command("running", digest),
            DeviceOutcomeUnknownError("release result lost"),
            _command("released", digest),
        ],
        clock=clock,
    )
    await controller.start_destination_preflight("destination", manifest)

    result = await controller.release_destination()

    assert result.state == "released"
    assert _operations(registry).count("transfer_directory_release") == 2
    assert _timeouts(registry, "transfer_directory_release") == [3.0, 2.0]


@pytest.mark.asyncio
async def test_release_deadline_expiry_after_mutation_is_outcome_unknown() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    clock = _TickClock(step=0.6)
    controller, registry = _controller(
        [
            _command("running", digest),
            _command("accepted", digest),
        ],
        idle_timeout_seconds=1.0,
        clock=clock,
    )
    await controller.start_destination_preflight("destination", manifest)
    await controller.prepare_destination()

    with pytest.raises(DeviceOutcomeUnknownError):
        await controller.release_destination()

    assert "transfer_directory_release" not in _operations(registry)


@pytest.mark.asyncio
async def test_timeout_reconciliation_preserves_finalized_destination() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    clock = _Clock()
    finalized = {
        "kind": "directory",
        "files_transferred": 1,
        "bytes_transferred": 1,
        "sha256": "a" * 64,
        "warnings": [],
    }
    controller, registry = _controller(
        [
            _command("running", digest),
            _destination_status(digest=digest, state="finalizing", progress_seq=1),
            _destination_status(digest=digest, state="finalizing", progress_seq=1),
            _command("accepted", digest),
            _destination_status(
                digest=digest,
                state="finalized_held",
                progress_seq=2,
                terminal_result=finalized,
            ),
        ],
        idle_timeout_seconds=2.0,
        clock=clock,
    )
    await controller.start_destination_preflight("destination", manifest)

    result = await controller.wait_destination_until(
        frozenset({"finalized_held", "failed", "outcome_unknown"})
    )

    assert result.state == "finalized_held"
    assert _operations(registry).count("transfer_directory_cancel") == 1
    assert _timeouts(registry, "transfer_directory_status") == [2.0, 1.0, 2.0]


@pytest.mark.asyncio
async def test_timeout_reconciliation_does_not_accept_reserved_as_cancelled() -> None:
    manifest = _manifest()
    digest = _destination_request_digest("destination", manifest)
    clock = _Clock()
    terminal_error = {
        "code": "workspace_transfer_timeout",
        "message": "timed out",
    }
    controller, _ = _controller(
        [
            _command("running", digest),
            _command("accepted", digest),
            _destination_status(digest=digest, state="copying", progress_seq=1),
            _destination_status(digest=digest, state="copying", progress_seq=1),
            _command("accepted", digest),
            _destination_status(digest=digest, state="reserved", progress_seq=2),
            _destination_status(
                digest=digest,
                state="failed",
                progress_seq=3,
                terminal_error=terminal_error,
            ),
        ],
        idle_timeout_seconds=2.0,
        clock=clock,
    )
    await controller.start_destination_preflight("destination", manifest)
    await controller.prepare_destination()

    result = await controller.wait_destination_until(
        frozenset({"reserved", "finalized_held", "failed", "outcome_unknown"})
    )

    assert result.state == "failed"


@pytest.mark.asyncio
async def test_source_manifest_pages_are_rebuilt_and_verified() -> None:
    manifest = _manifest()
    probe = DirectorySourceProbe(
        root_identity=manifest.root_identity,
        scanned_entries=manifest.scanned_entries,
        file_count=len(manifest.entries),
        total_bytes=manifest.total_bytes,
        manifest_sha256=manifest.manifest_sha256,
        page_count=1,
    )
    controller, registry = _controller(
        [
            _command("running", _source_request_digest("source")),
            _source_status(
                digest=manifest.manifest_sha256,
                state="ready_retrieval",
                progress_seq=1,
                probe=probe.model_dump(mode="json"),
            ),
            _success(
                {
                    "offset": 0,
                    "next_offset": None,
                    "items": [
                        {
                            "kind": "directory",
                            "relative_path": "nested",
                            "identity": "dir-etag",
                        },
                        {
                            "kind": "file",
                            "relative_path": "nested/a.txt",
                            "size": 1,
                            "fingerprint": "file-etag",
                        },
                    ],
                }
            ),
        ]
    )
    await controller.start_source_probe("source")
    status = await controller.get_source_status()
    assert isinstance(status.probe, DirectorySourceProbe)

    rebuilt = await controller.retrieve_source_manifest(status.probe)

    assert rebuilt == manifest
    assert _operations(registry)[-1] == "transfer_source_probe_page"


@pytest.mark.asyncio
async def test_manifest_rebuild_rejects_nonadvancing_constructed_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _manifest()
    request_digest = _source_request_digest("source")
    probe = DirectorySourceProbe(
        root_identity=manifest.root_identity,
        scanned_entries=manifest.scanned_entries,
        file_count=len(manifest.entries),
        total_bytes=manifest.total_bytes,
        manifest_sha256=manifest.manifest_sha256,
        page_count=1,
    )
    controller, _ = _controller(
        [
            _command("running", request_digest),
            _source_status(
                digest=manifest.manifest_sha256,
                state="ready_retrieval",
                progress_seq=1,
                probe=probe.model_dump(mode="json"),
            ),
        ]
    )
    await controller.start_source_probe("source")
    await controller.get_source_status()

    async def invalid_page(offset: int) -> object:
        del offset
        from openctopus_server.directory_contract import DirectoryManifestPage

        return DirectoryManifestPage.model_construct(offset=0, next_offset=0, items=())

    monkeypatch.setattr(controller, "get_source_page", invalid_page)
    with pytest.raises(TransferIntegrityError, match="advance contiguously"):
        await controller.retrieve_source_manifest(probe)


@pytest.mark.asyncio
async def test_cancellation_after_issue_keeps_marker_and_reconciles_without_replay() -> None:
    digest = _source_request_digest("source")
    blocked = _BlockingOutcome(_command("running", digest))
    controller, registry = _controller(
        [
            blocked,
            _source_status(digest=digest, progress_seq=1),
        ]
    )

    started = asyncio.create_task(controller.start_source_probe("source"))
    await blocked.started.wait()
    started.cancel()
    with pytest.raises(asyncio.CancelledError):
        await started

    result = await controller.start_source_probe("source")

    assert isinstance(result, SourceDirectoryJobStatus)
    assert _operations(registry).count("transfer_source_probe_start") == 1


@pytest.mark.asyncio
async def test_cancellation_during_reconcile_does_not_cache_cancelled_error() -> None:
    digest = _source_request_digest("source")
    blocked = _BlockingOutcome(_source_status(digest=digest, progress_seq=1))
    controller, registry = _controller(
        [
            DeviceOutcomeUnknownError("start result lost"),
            blocked,
            _source_status(digest=digest, progress_seq=2),
        ]
    )

    started = asyncio.create_task(controller.start_source_probe("source"))
    await blocked.started.wait()
    started.cancel()
    with pytest.raises(asyncio.CancelledError):
        await started

    result = await controller.start_source_probe("source")

    assert isinstance(result, SourceDirectoryJobStatus)
    assert result.progress_seq == 2
    assert _operations(registry).count("transfer_source_probe_start") == 1
    assert _operations(registry).count("transfer_source_probe_status") == 2


def test_controller_requires_uuid7_operation_id() -> None:
    registry = _Registry(deque())
    route = DeviceRouteSnapshot(ConnectionHandle(new_uuid7(), 1), 0, "laptop")

    with pytest.raises(ValueError, match="UUID v7"):
        DeviceDirectoryJobController(
            registry=registry,
            route=route,
            user_id=new_uuid7(),
            directory_operation_id=uuid4(),
            idle_timeout_seconds=3.0,
        )


def _manifest() -> DirectoryManifest:
    from openctopus_server.directory_contract import (
        DirectoryManifestDirectory,
        DirectoryManifestEntry,
        create_directory_manifest,
    )

    return create_directory_manifest(
        root_identity="root-etag",
        directories=(DirectoryManifestDirectory(relative_path="nested", identity="dir-etag"),),
        entries=(
            DirectoryManifestEntry(
                relative_path="nested/a.txt",
                size=1,
                fingerprint="file-etag",
            ),
        ),
    )
