import asyncio
import hashlib
from dataclasses import dataclass, field
from uuid import UUID

import pytest

from openctopus_server.devices.registry import DeviceOutcomeUnknownError
from openctopus_server.devices.transfer import TransferIntegrityError
from openctopus_server.directory_contract import (
    DirectoryManifest,
    DirectoryManifestEntry,
    create_directory_manifest,
)
from openctopus_server.tools.directory_transfer import (
    DirectoryChildResult,
    DirectoryTransferCoordinator,
)


def _manifest() -> DirectoryManifest:
    return create_directory_manifest(
        root_identity="root-v1",
        directories=(),
        entries=(
            DirectoryManifestEntry(
                relative_path="a.txt",
                size=1,
                fingerprint="source-a",
            ),
            DirectoryManifestEntry(
                relative_path="nested.txt",
                size=2,
                fingerprint="source-b",
            ),
        ),
    )


@dataclass
class _Lease:
    close_count: int = 0

    async def aclose(self) -> None:
        self.close_count += 1


@dataclass
class _Backend:
    fail_preflight: BaseException | None = None
    fail_child: int | None = None
    cleanup_complete: bool = True
    source_warnings: tuple[str, ...] = ()
    issued_callbacks: list[object] = field(default_factory=list)
    child_ids: list[UUID] = field(default_factory=list)
    child_paths: list[str] = field(default_factory=list)
    committed: list[DirectoryChildResult] = field(default_factory=list)
    destination_cleanup_calls: int = 0
    source_cleanup_calls: int = 0
    release_calls: int = 0
    active_children: int = 0
    peak_children: int = 0

    async def preflight(self, _manifest: DirectoryManifest) -> None:
        if self.fail_preflight is not None:
            raise self.fail_preflight

    async def prepare_destination(self, mark_issued: object) -> None:
        self.issued_callbacks.append(mark_issued)

    async def copy_child(
        self,
        entry: DirectoryManifestEntry,
        slot_id: UUID,
        mark_issued: object,
    ) -> DirectoryChildResult:
        callback = mark_issued
        assert callable(callback)
        callback()
        self.active_children += 1
        self.peak_children = max(self.peak_children, self.active_children)
        try:
            index = len(self.child_ids)
            self.child_ids.append(slot_id)
            self.child_paths.append(entry.relative_path)
            if self.fail_child == index:
                raise RuntimeError("child failed")
            digest = hashlib.sha256(entry.relative_path.encode()).hexdigest()
            result = DirectoryChildResult(
                relative_path=entry.relative_path,
                verified_size=entry.size,
                verified_sha256=digest,
                destination_fingerprint=f"destination-{index}",
            )
            self.committed.append(result)
            return result
        finally:
            self.active_children -= 1

    async def finalize_destination(
        self, committed: tuple[DirectoryChildResult, ...]
    ) -> None:
        assert committed == tuple(self.committed)

    async def cleanup_destination(
        self, committed: tuple[DirectoryChildResult, ...]
    ) -> bool:
        self.destination_cleanup_calls += 1
        assert committed == tuple(self.committed)
        return self.cleanup_complete

    async def cleanup_source(self, _manifest: DirectoryManifest) -> tuple[str, ...]:
        self.source_cleanup_calls += 1
        return self.source_warnings

    async def release(self) -> None:
        self.release_calls += 1


async def test_coordinator_copies_children_serially_and_aggregates_content_digest() -> None:
    manifest = _manifest()
    backend = _Backend()
    lease = _Lease()
    issued = 0

    def mark_issued() -> None:
        nonlocal issued
        issued += 1

    result = await DirectoryTransferCoordinator().run(
        manifest=manifest,
        mode="copy",
        backend=backend,
        operation_lease=lease,
        on_issued=mark_issued,
    )

    assert result.kind == "directory"
    assert result.files_transferred == 2
    assert result.bytes_transferred == 3
    assert result.warnings == ()
    assert result.sha256 == "d9a7c8ffee91f10961ac8e8c44ec3a36588e720a3b6b8f48ddbdf1d31bc20a2a"
    assert backend.child_paths == ["a.txt", "nested.txt"]
    assert backend.peak_children == 1
    assert len(set(backend.child_ids)) == 2
    assert all(slot_id.version == 7 for slot_id in backend.child_ids)
    assert issued == 1
    assert backend.destination_cleanup_calls == 0
    assert backend.source_cleanup_calls == 0
    assert backend.release_calls == 1
    assert lease.close_count == 1


async def test_preflight_failure_has_no_destination_cleanup_or_issue() -> None:
    backend = _Backend(fail_preflight=RuntimeError("invalid source"))
    lease = _Lease()
    issued = 0

    def mark_issued() -> None:
        nonlocal issued
        issued += 1

    with pytest.raises(RuntimeError, match="invalid source"):
        await DirectoryTransferCoordinator().run(
            manifest=_manifest(),
            mode="copy",
            backend=backend,
            operation_lease=lease,
            on_issued=mark_issued,
        )

    assert issued == 0
    assert backend.destination_cleanup_calls == 0
    assert backend.release_calls == 1
    assert lease.close_count == 1


async def test_prepare_failure_after_issue_cleans_destination() -> None:
    @dataclass
    class FailingPrepareBackend(_Backend):
        async def prepare_destination(self, mark_issued: object) -> None:
            callback = mark_issued
            assert callable(callback)
            callback()
            raise RuntimeError("prepare failed")

    backend = FailingPrepareBackend()
    with pytest.raises(RuntimeError, match="prepare failed"):
        await DirectoryTransferCoordinator().run(
            manifest=_manifest(),
            mode="copy",
            backend=backend,
            operation_lease=_Lease(),
        )

    assert backend.destination_cleanup_calls == 1


async def test_child_failure_cleans_committed_destination_before_reraising() -> None:
    backend = _Backend(fail_child=1)

    with pytest.raises(RuntimeError, match="child failed"):
        await DirectoryTransferCoordinator().run(
            manifest=_manifest(),
            mode="copy",
            backend=backend,
            operation_lease=_Lease(),
        )

    assert len(backend.committed) == 1
    assert backend.destination_cleanup_calls == 1
    assert backend.release_calls == 1


async def test_mismatched_child_result_is_integrity_failure_and_cleans_destination() -> None:
    @dataclass
    class MismatchedBackend(_Backend):
        async def copy_child(
            self,
            entry: DirectoryManifestEntry,
            slot_id: UUID,
            mark_issued: object,
        ) -> DirectoryChildResult:
            result = await super().copy_child(entry, slot_id, mark_issued)
            return DirectoryChildResult(
                relative_path=result.relative_path,
                verified_size=result.verified_size + 1,
                verified_sha256=result.verified_sha256,
                destination_fingerprint=result.destination_fingerprint,
            )

        async def cleanup_destination(
            self, _committed: tuple[DirectoryChildResult, ...]
        ) -> bool:
            self.destination_cleanup_calls += 1
            return True

    backend = MismatchedBackend()
    with pytest.raises(TransferIntegrityError):
        await DirectoryTransferCoordinator().run(
            manifest=_manifest(),
            mode="copy",
            backend=backend,
            operation_lease=_Lease(),
        )

    assert backend.destination_cleanup_calls == 1


async def test_unconfirmed_destination_cleanup_becomes_outcome_unknown() -> None:
    backend = _Backend(fail_child=1, cleanup_complete=False)

    with pytest.raises(DeviceOutcomeUnknownError, match="check destination"):
        await DirectoryTransferCoordinator().run(
            manifest=_manifest(),
            mode="copy",
            backend=backend,
            operation_lease=_Lease(),
        )


async def test_move_starts_source_cleanup_only_after_destination_finalize() -> None:
    backend = _Backend(source_warnings=("source_changed_after_copy",))

    result = await DirectoryTransferCoordinator().run(
        manifest=_manifest(),
        mode="move",
        backend=backend,
        operation_lease=_Lease(),
    )

    assert len(backend.committed) == 2
    assert backend.source_cleanup_calls == 1
    assert backend.destination_cleanup_calls == 0
    assert result.warnings == ("source_changed_after_copy",)


async def test_cancellation_after_issue_cleans_destination_and_releases_once() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    @dataclass
    class BlockingBackend(_Backend):
        async def copy_child(
            self,
            entry: DirectoryManifestEntry,
            slot_id: UUID,
            mark_issued: object,
        ) -> DirectoryChildResult:
            callback = mark_issued
            assert callable(callback)
            callback()
            entered.set()
            await release.wait()
            return await super().copy_child(entry, slot_id, mark_issued)

    backend = BlockingBackend()
    lease = _Lease()
    task = asyncio.create_task(
        DirectoryTransferCoordinator().run(
            manifest=_manifest(),
            mode="copy",
            backend=backend,
            operation_lease=lease,
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert backend.destination_cleanup_calls == 1
    assert backend.release_calls == 1
    assert lease.close_count == 1
    release.set()


async def test_cancellation_during_started_source_cleanup_returns_destination_success() -> None:
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    @dataclass
    class BlockingCleanupBackend(_Backend):
        async def cleanup_source(
            self, _manifest: DirectoryManifest
        ) -> tuple[str, ...]:
            self.source_cleanup_calls += 1
            cleanup_started.set()
            await finish_cleanup.wait()
            return ("source_cleanup_incomplete",)

    backend = BlockingCleanupBackend()
    lease = _Lease()
    task = asyncio.create_task(
        DirectoryTransferCoordinator().run(
            manifest=_manifest(),
            mode="move",
            backend=backend,
            operation_lease=lease,
        )
    )
    await cleanup_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    finish_cleanup.set()

    result = await task
    assert result.files_transferred == 2
    assert result.warnings == ("source_cleanup_incomplete",)
    assert backend.destination_cleanup_calls == 0
    assert backend.source_cleanup_calls == 1
    assert backend.release_calls == 1
    assert lease.close_count == 1


async def test_backend_release_failure_still_closes_operation_lease() -> None:
    @dataclass
    class FailingReleaseBackend(_Backend):
        async def release(self) -> None:
            self.release_calls += 1
            raise RuntimeError("release failed")

    backend = FailingReleaseBackend()
    lease = _Lease()
    with pytest.raises(RuntimeError, match="release failed"):
        await DirectoryTransferCoordinator().run(
            manifest=_manifest(),
            mode="copy",
            backend=backend,
            operation_lease=lease,
        )

    assert backend.release_calls == 1
    assert lease.close_count == 1
