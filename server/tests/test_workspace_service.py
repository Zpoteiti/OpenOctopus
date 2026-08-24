import asyncio
import threading
from collections.abc import Coroutine
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.db.models import User, Workspace, WorkspaceMember
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.directory_contract import (
    DirectoryManifestDirectory,
    DirectoryManifestEntry,
    create_directory_manifest,
)
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError, WorkspaceError
from openctopus_server.services import users
from openctopus_server.workspace.fs import (
    MAX_EDIT_BYTES,
    DirectoryDestinationPlan,
    DirectoryPage,
    FileMetadata,
    WorkspaceFS,
    WorkspaceTarget,
)
from openctopus_server.workspace.resolver import ResolvedWorkspacePath
from openctopus_server.workspace.search import GrepContentMatch, SearchObject
from openctopus_server.workspace.service import (
    PatchEdit,
    ToolReadTicket,
    TransferPathTicket,
    WorkspaceService,
)


@asynccontextmanager
async def _slot():
    yield


def _workspace_fs_mock() -> AsyncMock:
    workspace_fs = AsyncMock(spec=WorkspaceFS)
    workspace_fs.materialization_slot = Mock(side_effect=_slot)
    workspace_fs.file_operation_slot = Mock(side_effect=_slot)
    return workspace_fs


class _FakeSession:
    def __init__(self) -> None:
        self._in_transaction = False
        self.checked_out_connections = 0
        self.commits = 0

    def open_transaction(self) -> None:
        assert not self._in_transaction
        self._in_transaction = True
        self.checked_out_connections = 1

    def in_transaction(self) -> bool:
        return self._in_transaction

    async def commit(self) -> None:
        assert self._in_transaction
        self._in_transaction = False
        self.checked_out_connections = 0
        self.commits += 1


def _install_fake_resolver(service: WorkspaceService, db: _FakeSession) -> None:
    target = WorkspaceTarget.personal(uuid4())

    async def resolve(*args: object, **kwargs: object) -> ResolvedWorkspacePath:
        del args, kwargs
        db.open_transaction()
        return ResolvedWorkspacePath(
            target=target,
            relative_path="file.txt",
            quota_bytes=1024,
        )

    resolver: Any = SimpleNamespace(resolve=resolve)
    service._resolver = resolver


async def test_document_admission_precedes_storage_and_outlives_materialization() -> None:
    events: list[str] = []
    workspace_fs = _workspace_fs_mock()

    @asynccontextmanager
    async def materialization_slot():
        events.append("materialization-enter")
        try:
            yield
        finally:
            events.append("materialization-exit")

    stream = SimpleNamespace(
        size=4,
        etag="revision-1",
        read=AsyncMock(side_effect=[b"docx", b""]),
        aclose=AsyncMock(),
    )

    async def open_stream(*args: object) -> object:
        del args
        events.append("open")
        return stream

    class _Conversion:
        async def parse(self, path: str, data: bytes, *, pages: str | None) -> str:
            del path, data, pages
            events.append("parse")
            return "converted"

    class _Parser:
        @asynccontextmanager
        async def admit(self, user_id):
            del user_id
            events.append("admit-enter")
            try:
                yield _Conversion()
            finally:
                events.append("admit-exit")

    workspace_fs.materialization_slot = Mock(side_effect=materialization_slot)
    workspace_fs.open_stream.side_effect = open_stream
    service = WorkspaceService(workspace_fs)

    result = await service.read_for_tool(
        ToolReadTicket(
            target=WorkspaceTarget.personal(uuid4()),
            relative_path="report.docx",
            display_path="report.docx",
            suffix=".docx",
        ),
        user_id=uuid4(),
        offset=1,
        limit=2000,
        pages=None,
        parser=_Parser(),  # type: ignore[arg-type]
    )

    assert result is not None and result.content == "converted"
    assert events == [
        "admit-enter",
        "materialization-enter",
        "open",
        "materialization-exit",
        "parse",
        "admit-exit",
    ]


@pytest.mark.parametrize("suffix", [".pdf", ".docx", ".xlsx", ".pptx"])
async def test_document_exact_materialization_limit_reaches_parser(suffix: str) -> None:
    workspace_fs = _workspace_fs_mock()
    chunk = b"x" * (64 * 1024)
    stream = SimpleNamespace(
        size=MAX_EDIT_BYTES,
        etag="revision-1",
        read=AsyncMock(side_effect=[chunk] * (MAX_EDIT_BYTES // len(chunk)) + [b""]),
        aclose=AsyncMock(),
    )
    workspace_fs.open_stream.return_value = stream
    parsed_sizes: list[int] = []

    class _Conversion:
        async def parse(self, path: str, data: bytes, *, pages: str | None) -> str:
            assert path == f"report{suffix}"
            assert pages is None
            parsed_sizes.append(len(data))
            return "converted"

    class _Parser:
        @asynccontextmanager
        async def admit(self, user_id):
            del user_id
            yield _Conversion()

    service = WorkspaceService(workspace_fs)
    result = await service.read_for_tool(
        ToolReadTicket(
            target=WorkspaceTarget.personal(uuid4()),
            relative_path=f"report{suffix}",
            display_path=f"report{suffix}",
            suffix=suffix,
        ),
        user_id=uuid4(),
        offset=1,
        limit=2000,
        pages=None,
        parser=_Parser(),  # type: ignore[arg-type]
    )

    assert result is not None and result.content == "converted"
    assert result.size == MAX_EDIT_BYTES
    assert parsed_sizes == [MAX_EDIT_BYTES]
    stream.aclose.assert_awaited_once()


@pytest.mark.parametrize("suffix", [".pdf", ".docx", ".xlsx", ".pptx"])
async def test_oversized_document_is_rejected_before_parser(suffix: str) -> None:
    workspace_fs = _workspace_fs_mock()
    stream = SimpleNamespace(
        size=MAX_EDIT_BYTES + 1,
        etag="revision-1",
        read=AsyncMock(),
        aclose=AsyncMock(),
    )
    workspace_fs.open_stream.return_value = stream
    parse = AsyncMock(return_value="must not run")

    class _Conversion:
        async def parse(self, path: str, data: bytes, *, pages: str | None) -> str:
            return await parse(path, data, pages=pages)

    class _Parser:
        @asynccontextmanager
        async def admit(self, user_id):
            del user_id
            yield _Conversion()

    service = WorkspaceService(workspace_fs)
    with pytest.raises(WorkspaceError) as caught:
        await service.read_for_tool(
            ToolReadTicket(
                target=WorkspaceTarget.personal(uuid4()),
                relative_path=f"report{suffix}",
                display_path=f"report{suffix}",
                suffix=suffix,
            ),
            user_id=uuid4(),
            offset=1,
            limit=2000,
            pages=None,
            parser=_Parser(),  # type: ignore[arg-type]
        )

    assert caught.value.code == ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT
    stream.read.assert_not_awaited()
    parse.assert_not_awaited()
    stream.aclose.assert_awaited_once()


async def test_service_rejects_inaccessible_shared_path_before_file_call(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _workspace_fs_mock()
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = User(email="owner-service@test.com", password_hash="x", name="Owner")
        outsider = User(
            email="outsider-service@test.com",
            password_hash="x",
            name="Outsider",
        )
        db.add_all((owner, outsider))
        await db.flush()
        workspace = Workspace(
            name="Private",
            suffix="1234abcd",
            quota_bytes=1234,
            created_by=owner.id,
        )
        db.add(workspace)
        await db.flush()
        db.add(WorkspaceMember(workspace_id=workspace.id, user_id=owner.id))
        await db.commit()

        with pytest.raises(WorkspaceError) as caught:
            await service.write(
                db,
                user_id=outsider.id,
                path="/Private@1234abcd/secret.txt",
                data=b"blocked",
            )

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    workspace_fs.write.assert_not_awaited()
    workspace_fs.write_collected_upload.assert_not_awaited()


async def test_service_supplies_resolved_target_and_database_quota(
    pg_engine: AsyncEngine,
) -> None:
    workspace_fs = _workspace_fs_mock()
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        member = User(email="member-service@test.com", password_hash="x", name="Member")
        db.add(member)
        await db.flush()
        workspace = Workspace(
            name="Shared",
            suffix="abcd1234",
            quota_bytes=4321,
            created_by=member.id,
        )
        db.add(workspace)
        await db.flush()
        db.add(WorkspaceMember(workspace_id=workspace.id, user_id=member.id))
        await db.commit()

        await service.write(
            db,
            user_id=member.id,
            path="/Shared@abcd1234/file.txt",
            data=b"contents",
        )

    workspace_fs.write_collected_upload.assert_awaited_once_with(
        WorkspaceTarget.shared(workspace.id),
        "file.txt",
        b"contents",
        quota_bytes=4321,
        if_match=None,
        if_none_match=False,
    )


async def test_authorized_operation_releases_database_lock_before_storage(
    pg_engine: AsyncEngine,
) -> None:
    entered_file_call = asyncio.Event()
    release_file_call = asyncio.Event()

    async def blocking_write(*args: object, **kwargs: object) -> None:
        del args, kwargs
        entered_file_call.set()
        await release_file_call.wait()

    workspace_fs = _workspace_fs_mock()
    workspace_fs.write_collected_upload.side_effect = blocking_write
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        user = User(email="locked-service@test.com", password_hash="x", name="Locked")
        setup_db.add(user)
        await setup_db.commit()
        user_id = user.id

    operation_db = AsyncSession(pg_engine, expire_on_commit=False)
    writing = asyncio.create_task(
        service.write(
            operation_db,
            user_id=user_id,
            path="file.txt",
            data=b"contents",
        )
    )
    await entered_file_call.wait()

    async def delete_account() -> None:
        async with AsyncSession(pg_engine, expire_on_commit=False) as deletion_db:
            target = await deletion_db.get(User, user_id)
            assert target is not None
            await users.delete_user(
                deletion_db,
                target,
                workspace_fs=workspace_fs,
                device_registry=DeviceRegistry(),
            )

    deleting = asyncio.create_task(delete_account())
    try:
        await asyncio.wait_for(asyncio.shield(deleting), timeout=1)
        release_file_call.set()
        await writing
    finally:
        release_file_call.set()
        await operation_db.rollback()
        await operation_db.close()


async def test_text_edit_uses_shared_matcher_and_can_create_missing_file(pg_engine) -> None:
    workspace_fs = _workspace_fs_mock()

    async def edit_optional(
        target: WorkspaceTarget,
        path: str,
        transform: Any,
        **kwargs: object,
    ) -> FileMetadata:
        del target, path, kwargs
        assert transform(None) == b"created\n"
        return FileMetadata(size=8, etag="new", created=True)

    workspace_fs.edit_optional_materialized.side_effect = edit_optional
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(email="edit-create@test.com", password_hash="x", name="Editor")
        db.add(user)
        await db.commit()

        metadata, replacements = await service.edit_text(
            db,
            user_id=user.id,
            path="new.txt",
            old_text="",
            new_text="created\n",
            occurrence=None,
        )

    assert metadata.created is True
    assert replacements == 0


async def test_grep_rejects_invalid_regex_before_scanning_storage() -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    service = WorkspaceService(workspace_fs)

    with pytest.raises(ToolError) as caught:
        await service.grep(
            AsyncMock(spec=AsyncSession),
            user_id=uuid4(),
            pattern="[",
            path="",
            glob=None,
            file_type=None,
            case_insensitive=False,
            fixed_strings=False,
            output_mode="content",
            context_before=0,
            context_after=0,
            limit=10,
            offset=0,
        )

    assert caught.value.code is ErrorCode.TOOL_INVALID_REGEX
    workspace_fs.scan_objects.assert_not_awaited()


async def test_find_rejects_invalid_glob_before_scanning_storage() -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    service = WorkspaceService(workspace_fs)

    with pytest.raises(ToolError) as caught:
        await service.find_files(
            AsyncMock(spec=AsyncSession),
            user_id=uuid4(),
            path="",
            query="",
            glob="[broken",
            file_type=None,
            include_dirs=False,
            sort="path",
            limit=10,
            offset=0,
        )

    assert caught.value.code is ErrorCode.TOOL_INVALID_GLOB
    workspace_fs.scan_objects.assert_not_awaited()


async def test_tool_write_rejects_more_than_eight_mib_before_storage() -> None:
    workspace_fs = _workspace_fs_mock()
    service = WorkspaceService(workspace_fs)

    with pytest.raises(WorkspaceError) as caught:
        await service.write(
            AsyncMock(spec=AsyncSession),
            user_id=uuid4(),
            path="large.txt",
            data=b"x" * (8 * 1024 * 1024 + 1),
        )

    assert caught.value.code is ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT
    workspace_fs.write_collected_upload.assert_not_awaited()


@pytest.mark.parametrize("mode", ["stat", "write", "search"])
async def test_single_target_storage_does_not_hold_database_transaction(mode: str) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    service = WorkspaceService(workspace_fs)
    fake_db = _FakeSession()
    db: Any = fake_db
    _install_fake_resolver(service, fake_db)

    async def blocking_storage(*args: object, **kwargs: object) -> object:
        del args, kwargs
        entered.set()
        await release.wait()
        if mode == "search":
            return (), False
        return FileMetadata(size=4, etag="etag", created=False)

    operation: Coroutine[Any, Any, object]
    if mode == "stat":
        workspace_fs.stat.side_effect = blocking_storage
        operation = service.stat(db, user_id=uuid4(), path="file.txt")
    elif mode == "write":
        workspace_fs.write_collected_upload.side_effect = blocking_storage
        operation = service.write(db, user_id=uuid4(), path="file.txt", data=b"data")
    else:
        workspace_fs.scan_objects.side_effect = blocking_storage
        operation = service.find_files(
            db,
            user_id=uuid4(),
            path="file.txt",
            query="",
            glob=None,
            file_type=None,
            include_dirs=False,
            sort="path",
            limit=10,
            offset=0,
        )

    running = asyncio.create_task(operation)
    await entered.wait()
    try:
        assert not fake_db.in_transaction()
        assert fake_db.checked_out_connections == 0
    finally:
        release.set()
    await running
    assert fake_db.commits == 2


async def test_multi_target_patch_commits_all_resolutions_before_storage() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    workspace_fs = _workspace_fs_mock()
    service = WorkspaceService(workspace_fs)
    fake_db = _FakeSession()
    db: Any = fake_db
    resolved_paths: list[str] = []
    target = WorkspaceTarget.personal(uuid4())

    async def resolve(*args: object, **kwargs: object) -> ResolvedWorkspacePath:
        del args
        path = kwargs["path"]
        assert isinstance(path, str)
        if not fake_db.in_transaction():
            fake_db.open_transaction()
        resolved_paths.append(path)
        return ResolvedWorkspacePath(
            target=target,
            relative_path=path,
            quota_bytes=1024,
        )

    resolver: Any = SimpleNamespace(resolve=resolve)
    service._resolver = resolver

    async def blocking_patch(*args: object, **kwargs: object) -> tuple[FileMetadata, ...]:
        del args, kwargs
        entered.set()
        await release.wait()
        return (
            FileMetadata(size=1, etag="a", created=True),
            FileMetadata(size=1, etag="b", created=True),
        )

    workspace_fs.apply_transforms_admitted.side_effect = blocking_patch
    edits = (
        PatchEdit("a.txt", "add", None, "a"),
        PatchEdit("b.txt", "add", None, "b"),
    )
    running = asyncio.create_task(
        service.apply_patch(db, user_id=uuid4(), edits=edits, dry_run=False)
    )
    await entered.wait()
    try:
        assert resolved_paths == ["a.txt", "b.txt", "a.txt", "b.txt"]
        assert fake_db.commits == 2
        assert not fake_db.in_transaction()
        assert fake_db.checked_out_connections == 0
    finally:
        release.set()
    await running


async def test_search_reauthorizes_after_waiting_for_heavy_admission(pg_engine) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    @asynccontextmanager
    async def blocked_slot():
        entered.set()
        await release.wait()
        yield

    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=blocked_slot)
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        owner = User(email="search-owner@test.com", password_hash="x", name="Owner")
        member = User(email="search-member@test.com", password_hash="x", name="Member")
        setup_db.add_all((owner, member))
        await setup_db.flush()
        workspace = Workspace(
            name="Search",
            suffix="feed1234",
            quota_bytes=1024,
            created_by=owner.id,
        )
        setup_db.add(workspace)
        await setup_db.flush()
        setup_db.add_all(
            (
                WorkspaceMember(workspace_id=workspace.id, user_id=owner.id),
                WorkspaceMember(workspace_id=workspace.id, user_id=member.id),
            )
        )
        await setup_db.commit()
        member_id = member.id
        workspace_id = workspace.id

    operation_db = AsyncSession(pg_engine, expire_on_commit=False)
    searching = asyncio.create_task(
        service.find_files(
            operation_db,
            user_id=member_id,
            path="/Search@feed1234/",
            query="",
            glob=None,
            file_type=None,
            include_dirs=False,
            sort="path",
            limit=10,
            offset=0,
        )
    )
    await entered.wait()
    async with AsyncSession(pg_engine, expire_on_commit=False) as revoke_db:
        await revoke_db.execute(
            delete(WorkspaceMember).where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.user_id == member_id,
            )
        )
        await revoke_db.commit()
    release.set()
    try:
        with pytest.raises(WorkspaceError) as caught:
            await searching
    finally:
        await operation_db.close()

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    workspace_fs.scan_objects.assert_not_awaited()


async def test_patch_reauthorizes_after_waiting_for_materialization(pg_engine) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    @asynccontextmanager
    async def blocked_slot():
        entered.set()
        await release.wait()
        yield

    workspace_fs = _workspace_fs_mock()
    workspace_fs.materialization_slot = Mock(side_effect=blocked_slot)
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as setup_db:
        owner = User(email="patch-owner@test.com", password_hash="x", name="Owner")
        member = User(email="patch-member@test.com", password_hash="x", name="Member")
        setup_db.add_all((owner, member))
        await setup_db.flush()
        workspace = Workspace(
            name="Patch",
            suffix="cafe1234",
            quota_bytes=1024,
            created_by=owner.id,
        )
        setup_db.add(workspace)
        await setup_db.flush()
        setup_db.add_all(
            (
                WorkspaceMember(workspace_id=workspace.id, user_id=owner.id),
                WorkspaceMember(workspace_id=workspace.id, user_id=member.id),
            )
        )
        await setup_db.commit()
        member_id = member.id
        workspace_id = workspace.id

    operation_db = AsyncSession(pg_engine, expire_on_commit=False)
    patching = asyncio.create_task(
        service.apply_patch(
            operation_db,
            user_id=member_id,
            edits=(PatchEdit("/Patch@cafe1234/a.txt", "add", None, "x"),),
            dry_run=False,
        )
    )
    await entered.wait()
    async with AsyncSession(pg_engine, expire_on_commit=False) as revoke_db:
        await revoke_db.execute(
            delete(WorkspaceMember).where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.user_id == member_id,
            )
        )
        await revoke_db.commit()
    release.set()
    try:
        with pytest.raises(WorkspaceError) as caught:
            await patching
    finally:
        await operation_db.close()

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    workspace_fs.apply_transforms_admitted.assert_not_awaited()


async def test_recursive_list_rejects_an_exact_file_path(pg_engine) -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    workspace_fs.scan_objects.return_value = (
        (SearchObject(path="file.txt", size=4),),
        False,
    )
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(email="list-file@test.com", password_hash="x", name="Reader")
        db.add(user)
        await db.commit()

        with pytest.raises(WorkspaceError) as caught:
            await service.list_recursive(
                db,
                user_id=user.id,
                path="file.txt",
                limit=10,
                offset=0,
            )

    assert caught.value.code is ErrorCode.TOOL_NOT_A_DIRECTORY


async def test_directory_page_normalizes_dot_to_workspace_root(pg_engine) -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.list_dir_page.return_value = DirectoryPage((), None, False)
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(email="list-root@test.com", password_hash="x", name="Reader")
        db.add(user)
        await db.commit()

        await service.list_dir_page(
            db,
            user_id=user.id,
            path=".",
            limit=20,
        )

    workspace_fs.list_dir_page.assert_awaited_once_with(
        WorkspaceTarget.personal(user.id),
        "",
        limit=20,
        offset=0,
        scan_limit=10_000,
        include_noise_directories=False,
    )


async def test_prompt_personal_reads_need_no_database_session() -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.read.return_value = b"prompt"
    workspace_fs.list_dir_page.return_value = DirectoryPage((), None, False)
    service = WorkspaceService(workspace_fs)
    user_id = uuid4()

    assert (
        await service.read_personal_for_prompt(
            user_id=user_id,
            path="SOUL.md",
            length=100,
        )
        == b"prompt"
    )
    await service.list_personal_for_prompt(
        user_id=user_id,
        path="skills",
        limit=1_000,
        scan_limit=1_000,
        include_noise_directories=True,
    )

    workspace_fs.read.assert_awaited_once_with(
        WorkspaceTarget.personal(user_id),
        "SOUL.md",
        offset=0,
        length=100,
    )
    workspace_fs.list_dir_page.assert_awaited_once_with(
        WorkspaceTarget.personal(user_id),
        "skills",
        limit=1_000,
        offset=0,
        scan_limit=1_000,
        include_noise_directories=True,
    )


async def test_personal_usages_preserves_order_and_limits_scans_to_four() -> None:
    workspace_fs = _workspace_fs_mock()
    active = 0
    peak = 0

    async def usage(target: WorkspaceTarget) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.01)
            return target.id.int
        finally:
            active -= 1

    workspace_fs.usage.side_effect = usage
    service = WorkspaceService(workspace_fs)
    user_ids = [uuid4() for _ in range(10)]

    result = await service.personal_usages(user_ids)

    assert result == [user_id.int for user_id in user_ids]
    assert peak == 4


async def test_personal_usages_cancels_and_drains_siblings_before_failure() -> None:
    workspace_fs = _workspace_fs_mock()
    user_ids = [uuid4() for _ in range(4)]
    all_started = asyncio.Event()
    blocked = asyncio.Event()
    cancelled: set[UUID] = set()
    active = 0
    expected = WorkspaceError(ErrorCode.WORKSPACE_STORAGE_UNAVAILABLE, "scan failed")

    async def usage(target: WorkspaceTarget) -> int:
        nonlocal active
        active += 1
        if active == 4:
            all_started.set()
        try:
            await all_started.wait()
            if target.id == user_ids[0]:
                raise expected
            try:
                await blocked.wait()
            except asyncio.CancelledError:
                cancelled.add(target.id)
                raise
            return target.id.int
        finally:
            active -= 1

    workspace_fs.usage.side_effect = usage
    service = WorkspaceService(workspace_fs)

    with pytest.raises(WorkspaceError) as caught:
        await service.personal_usages(user_ids)

    assert caught.value is expected
    assert cancelled == set(user_ids[1:])
    assert active == 0


async def test_repeated_cpu_cancellation_waits_for_worker_thread() -> None:
    from openctopus_server.workspace.service import _run_cpu

    entered = threading.Event()
    release = threading.Event()

    def blocking_work() -> str:
        entered.set()
        assert release.wait(timeout=2)
        return "done"

    work = asyncio.create_task(_run_cpu(blocking_work))
    assert await asyncio.to_thread(entered.wait, 1)
    work.cancel()
    await asyncio.sleep(0)
    work.cancel()
    await asyncio.sleep(0)
    work.cancel()
    await asyncio.sleep(0)

    try:
        assert not work.done()
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await work


async def test_directory_personal_skill_validation_is_sequential_and_skips_other_files() -> None:
    user_id = uuid4()
    target = WorkspaceTarget.personal(user_id)
    destination = TransferPathTicket(
        user_id=user_id,
        display_path="skills",
        target=target,
        relative_path="skills",
        quota_bytes=1024 * 1024,
    )
    contents = {
        "a/SKILL.md": b"---\nname: a\ndescription: first\n---\nbody",
        "b/SKILL.md": b"---\nname: b\ndescription: second\n---\nbody",
        "other.txt": b"other",
    }
    entries = tuple(
        DirectoryManifestEntry(
            relative_path=path,
            size=len(data),
            fingerprint=f"etag-{index}",
        )
        for index, (path, data) in enumerate(contents.items())
    )
    manifest = create_directory_manifest(
        root_identity=None,
        directories=(
            DirectoryManifestDirectory(relative_path="a", identity=None),
            DirectoryManifestDirectory(relative_path="b", identity=None),
        ),
        entries=entries,
    )
    plan = DirectoryDestinationPlan(
        target=target,
        destination_root="skills",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=tuple(f"skills/{path}" for path in contents),
    )
    active = 0
    max_active = 0
    opened: list[str] = []
    closed: list[str] = []

    class _Stream:
        def __init__(self, entry: DirectoryManifestEntry) -> None:
            nonlocal active, max_active
            self.size = entry.size
            self.etag = entry.fingerprint
            self._path = entry.relative_path
            self._chunks = [contents[self._path], b""]
            active += 1
            max_active = max(max_active, active)

        async def read(self) -> bytes:
            return self._chunks.pop(0)

        async def aclose(self) -> None:
            nonlocal active
            if self._path not in closed:
                closed.append(self._path)
                active -= 1

    async def open_source(entry: DirectoryManifestEntry):
        opened.append(entry.relative_path)
        return _Stream(entry)

    await WorkspaceService(_workspace_fs_mock()).validate_directory_skill_manifests(
        destination,
        manifest,
        plan,
        open_source=open_source,
    )

    expected = ["a/SKILL.md", "b/SKILL.md"]
    assert opened == expected
    assert closed == expected
    assert max_active == 1


async def test_directory_skill_validation_rejects_first_invalid_or_drifted_source() -> None:
    user_id = uuid4()
    target = WorkspaceTarget.personal(user_id)
    destination = TransferPathTicket(
        user_id=user_id,
        display_path="skills",
        target=target,
        relative_path="skills",
        quota_bytes=1024,
    )
    malformed = b"not a skill"
    entries = (
        DirectoryManifestEntry(
            relative_path="a/SKILL.md",
            size=len(malformed),
            fingerprint="etag-a",
        ),
        DirectoryManifestEntry(
            relative_path="b/SKILL.md",
            size=1,
            fingerprint="etag-b",
        ),
    )
    manifest = create_directory_manifest(
        root_identity=None,
        directories=(
            DirectoryManifestDirectory(relative_path="a", identity=None),
            DirectoryManifestDirectory(relative_path="b", identity=None),
        ),
        entries=entries,
    )
    plan = DirectoryDestinationPlan(
        target=target,
        destination_root="skills",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("skills/a/SKILL.md", "skills/b/SKILL.md"),
    )
    opened: list[str] = []
    closed: list[str] = []

    async def open_source(entry: DirectoryManifestEntry):
        opened.append(entry.relative_path)
        return SimpleNamespace(
            size=entry.size,
            etag=entry.fingerprint,
            read=AsyncMock(side_effect=[malformed, b""]),
            aclose=AsyncMock(side_effect=lambda: closed.append(entry.relative_path)),
        )

    with pytest.raises(WorkspaceError) as caught:
        await WorkspaceService(_workspace_fs_mock()).validate_directory_skill_manifests(
            destination,
            manifest,
            plan,
            open_source=open_source,
        )
    assert caught.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT
    assert opened == ["a/SKILL.md"]
    assert closed == ["a/SKILL.md"]

    async def open_drifted(entry: DirectoryManifestEntry):
        return SimpleNamespace(
            size=entry.size,
            etag="changed",
            read=AsyncMock(),
            aclose=AsyncMock(side_effect=lambda: closed.append("drifted")),
        )

    with pytest.raises(WorkspaceError) as caught:
        await WorkspaceService(_workspace_fs_mock()).validate_directory_skill_manifests(
            destination,
            manifest,
            plan,
            open_source=open_drifted,
        )
    assert caught.value.code is ErrorCode.WORKSPACE_FILE_CHANGED
    assert closed[-1] == "drifted"


async def test_directory_skill_validation_does_not_apply_to_shared_workspace() -> None:
    user_id = uuid4()
    target = WorkspaceTarget.shared(uuid4())
    destination = TransferPathTicket(
        user_id=user_id,
        display_path="/shared@00000000/skills",
        target=target,
        relative_path="skills",
        quota_bytes=1024,
    )
    content = b"not a skill"
    manifest = create_directory_manifest(
        root_identity=None,
        directories=(DirectoryManifestDirectory(relative_path="bad", identity=None),),
        entries=(
            DirectoryManifestEntry(
                relative_path="bad/SKILL.md",
                size=len(content),
                fingerprint="etag",
            ),
        ),
    )
    plan = DirectoryDestinationPlan(
        target=target,
        destination_root="skills",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=("skills/bad/SKILL.md",),
    )
    open_source = AsyncMock()

    await WorkspaceService(_workspace_fs_mock()).validate_directory_skill_manifests(
        destination,
        manifest,
        plan,
        open_source=open_source,
    )

    open_source.assert_not_awaited()


async def test_client_directory_skill_selection_is_sequential_and_personal_only() -> None:
    user_id = uuid4()
    target = WorkspaceTarget.personal(user_id)
    destination = TransferPathTicket(
        user_id=user_id,
        display_path="skills",
        target=target,
        relative_path="skills",
        quota_bytes=1024,
    )
    entries = (
        DirectoryManifestEntry(relative_path="a/SKILL.md", size=1, fingerprint="a"),
        DirectoryManifestEntry(relative_path="other.txt", size=1, fingerprint="other"),
        DirectoryManifestEntry(relative_path="b/SKILL.md", size=1, fingerprint="b"),
    )
    manifest = create_directory_manifest(
        root_identity="root-v1",
        directories=(
            DirectoryManifestDirectory(relative_path="a", identity="dir-a"),
            DirectoryManifestDirectory(relative_path="b", identity="dir-b"),
        ),
        entries=entries,
    )
    plan = DirectoryDestinationPlan(
        target=target,
        destination_root="skills",
        manifest_sha256=manifest.manifest_sha256,
        mapped_paths=tuple(f"skills/{entry.relative_path}" for entry in manifest.entries),
    )
    observed: list[tuple[str, str]] = []

    async def validate_source(entry: DirectoryManifestEntry, path: str) -> None:
        observed.append((entry.relative_path, path))

    await WorkspaceService(_workspace_fs_mock()).validate_client_directory_skill_manifests(
        destination,
        manifest,
        plan,
        validate_source=validate_source,
    )

    assert observed == [
        ("a/SKILL.md", "skills/a/SKILL.md"),
        ("b/SKILL.md", "skills/b/SKILL.md"),
    ]


async def test_staged_directory_skill_validation_streams_and_closes() -> None:
    content = b"---\nname: demo\ndescription: staged\n---\nbody"
    stream = SimpleNamespace(
        size=len(content),
        read=AsyncMock(side_effect=[content[:10], content[10:], b""]),
        aclose=AsyncMock(),
    )
    workspace_fs = _workspace_fs_mock()
    workspace_fs.open_directory_validation_staging.return_value = stream
    service = WorkspaceService(workspace_fs)

    await service.validate_staged_directory_skill_manifest(
        "skills/demo/SKILL.md",
        "_openoctopus-transfers/staged",
        expected_size=len(content),
    )

    stream.aclose.assert_awaited_once()

    stream.size = len(content) + 1
    with pytest.raises(WorkspaceError) as caught:
        await service.validate_staged_directory_skill_manifest(
            "skills/demo/SKILL.md",
            "_openoctopus-transfers/staged",
            expected_size=len(content),
        )
    assert caught.value.code is ErrorCode.WORKSPACE_TRANSFER_INTEGRITY_FAILED


async def test_single_file_transfer_validates_personal_skill_staging_before_commit() -> None:
    user_id = uuid4()
    content = b"not a Skill manifest"
    stream = SimpleNamespace(
        size=len(content),
        read=AsyncMock(side_effect=[content, b""]),
        aclose=AsyncMock(),
    )
    workspace_fs = _workspace_fs_mock()
    workspace_fs.open_directory_validation_staging.return_value = stream
    service = WorkspaceService(workspace_fs)
    ticket = TransferPathTicket(
        user_id=user_id,
        display_path="skills/demo/SKILL.md",
        target=WorkspaceTarget.personal(user_id),
        relative_path="skills/demo/SKILL.md",
        quota_bytes=1024,
    )
    sink = SimpleNamespace(object_name="_openoctopus-transfers/staged")

    with pytest.raises(WorkspaceError) as caught:
        await service.commit_transfer_upload(
            ticket,
            sink,
            size=len(content),
            sha256="0" * 64,
        )

    assert caught.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT
    workspace_fs.commit_uploaded_object.assert_not_awaited()
    stream.aclose.assert_awaited_once()


async def test_single_file_transfer_publishes_valid_personal_skill_staging() -> None:
    user_id = uuid4()
    content = b"---\nname: demo\ndescription: valid\n---\nbody"
    stream = SimpleNamespace(
        size=len(content),
        read=AsyncMock(side_effect=[content, b""]),
        aclose=AsyncMock(),
    )
    workspace_fs = _workspace_fs_mock()
    workspace_fs.open_directory_validation_staging.return_value = stream
    service = WorkspaceService(workspace_fs)
    ticket = TransferPathTicket(
        user_id=user_id,
        display_path="skills/demo/SKILL.md",
        target=WorkspaceTarget.personal(user_id),
        relative_path="skills/demo/SKILL.md",
        quota_bytes=1024,
    )
    sink = SimpleNamespace(object_name="_openoctopus-transfers/staged")

    cancelled_after_commit = await service.commit_transfer_upload(
        ticket,
        sink,
        size=len(content),
        sha256="0" * 64,
    )

    assert cancelled_after_commit is False
    workspace_fs.commit_uploaded_object.assert_awaited_once_with(
        ticket.target,
        ticket.relative_path,
        sink.object_name,
        size=len(content),
        quota_bytes=ticket.quota_bytes,
    )
    stream.aclose.assert_awaited_once()


async def test_single_file_transfer_does_not_validate_shared_skill_namespace() -> None:
    user_id = uuid4()
    workspace_fs = _workspace_fs_mock()
    service = WorkspaceService(workspace_fs)
    ticket = TransferPathTicket(
        user_id=user_id,
        display_path="/shared@12345678/skills/demo/SKILL.md",
        target=WorkspaceTarget.shared(uuid4()),
        relative_path="skills/demo/SKILL.md",
        quota_bytes=1024,
    )
    sink = SimpleNamespace(object_name="_openoctopus-transfers/staged")

    await service.commit_transfer_upload(
        ticket,
        sink,
        size=1,
        sha256="0" * 64,
    )

    workspace_fs.open_directory_validation_staging.assert_not_awaited()
    workspace_fs.commit_uploaded_object.assert_awaited_once()


@pytest.mark.parametrize(
    "relative_path",
    ["", "skills", "skills/demo", "skills/demo/SKILL.md"],
)
def test_transfer_ticket_changed_invalidates_personal_skills_by_immutable_path(
    relative_path: str,
) -> None:
    user_id = uuid4()
    cache = Mock()
    service = WorkspaceService(_workspace_fs_mock(), skills_cache=cache)
    ticket = TransferPathTicket(
        user_id=user_id,
        display_path="unrelated-display-alias",
        target=WorkspaceTarget.personal(user_id),
        relative_path=relative_path,
        quota_bytes=1024,
    )

    service.transfer_ticket_changed(ticket)

    cache.invalidate.assert_called_once_with(user_id)


@pytest.mark.parametrize(
    ("target_kind", "relative_path"),
    [
        ("shared", "skills/demo/SKILL.md"),
        ("foreign-personal", "skills/demo/SKILL.md"),
        ("own-personal", "notes/skills/demo/SKILL.md"),
    ],
)
def test_transfer_ticket_changed_ignores_nonpersonal_skill_namespaces(
    target_kind: str,
    relative_path: str,
) -> None:
    user_id = uuid4()
    target = {
        "shared": WorkspaceTarget.shared(uuid4()),
        "foreign-personal": WorkspaceTarget.personal(uuid4()),
        "own-personal": WorkspaceTarget.personal(user_id),
    }[target_kind]
    cache = Mock()
    service = WorkspaceService(_workspace_fs_mock(), skills_cache=cache)
    ticket = TransferPathTicket(
        user_id=user_id,
        display_path="skills/demo/SKILL.md",
        target=target,
        relative_path=relative_path,
        quota_bytes=1024,
    )

    service.transfer_ticket_changed(ticket)

    cache.invalidate.assert_not_called()


async def test_grep_result_cap_resumes_within_the_same_file(pg_engine) -> None:
    workspace_fs = _workspace_fs_mock()
    workspace_fs.heavy_operation_slot = Mock(side_effect=_slot)
    content = (("needle" + "x" * 14_900 + "\n") * 100).encode()
    metadata = SearchObject(path="large.txt", size=len(content))
    workspace_fs.scan_objects.return_value = ((metadata,), False)
    workspace_fs.read_search_object.return_value = SearchObject(
        path="large.txt",
        size=len(content),
        content=content,
    )
    service = WorkspaceService(workspace_fs)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(email="grep-page@test.com", password_hash="x", name="Reader")
        db.add(user)
        await db.commit()

        first = await service.grep(
            db,
            user_id=user.id,
            pattern="needle",
            path=".",
            glob=None,
            file_type=None,
            case_insensitive=False,
            fixed_strings=True,
            output_mode="content",
            context_before=0,
            context_after=0,
            limit=1000,
            offset=0,
        )
        assert first.next_offset == len(first.items)
        assert first.next_offset is not None

        second = await service.grep(
            db,
            user_id=user.id,
            pattern="needle",
            path=".",
            glob=None,
            file_type=None,
            case_insensitive=False,
            fixed_strings=True,
            output_mode="content",
            context_before=0,
            context_after=0,
            limit=1000,
            offset=first.next_offset,
        )

    assert second.items
    assert isinstance(second.items[0], GrepContentMatch)
    assert second.items[0].line_number == first.next_offset + 1
