from types import SimpleNamespace
from uuid import uuid4

from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.workspace_backend import WorkspaceToolDispatcher
from openctopus_server.workspace.fs import FileMetadata
from openctopus_server.workspace.service import PatchEditResult, ToolFileRead


class _WorkspaceService:
    def __init__(self) -> None:
        self.writes: list[tuple[object, str, bytes]] = []
        self.reads = 0
        self.stats = 0
        self.calls: list[str] = []
        self.etag = "revision-1"

    async def stat(self, db, *, user_id, path):
        del db, user_id, path
        self.stats += 1
        self.calls.append("stat")
        return FileMetadata(size=5, etag=self.etag)

    async def read_for_tool(self, db, *, user_id, path, offset, limit, pages, parser):
        del db, user_id, path, limit, parser
        self.reads += 1
        self.calls.append("read")
        content = f"{offset}|hello" if pages is None else f"pages={pages}"
        return ToolFileRead(etag=self.etag, content=content, size=5)

    async def write(self, db, *, user_id, path, data):
        del db
        self.writes.append((user_id, path, data))
        return SimpleNamespace(size=len(data), created=True)


def _ctx() -> ToolContext:
    return ToolContext(
        user_id=uuid4(),
        session_id=uuid4(),
        openoctopus_device="server",
    )


async def test_backend_dispatches_write_through_workspace_service(pg_engine) -> None:
    service = _WorkspaceService()
    backend = WorkspaceToolDispatcher(pg_engine, service)  # type: ignore[arg-type]
    ctx = _ctx()

    result = await backend(
        "write_file",
        {"path": "notes/a.txt", "content": "hello"},
        ctx,
    )

    assert result.is_error is False
    assert result.content == "Wrote notes/a.txt (5 bytes)."
    assert service.writes == [(ctx.user_id, "notes/a.txt", b"hello")]


async def test_backend_read_cache_is_scoped_by_session_and_force_bypasses(pg_engine) -> None:
    service = _WorkspaceService()
    backend = WorkspaceToolDispatcher(pg_engine, service)  # type: ignore[arg-type]
    ctx = _ctx()
    args = {"path": "notes/a.txt", "offset": 1, "limit": 20, "pages": None, "force": False}

    first = await backend("read_file", args, ctx)
    unchanged = await backend("read_file", args, ctx)
    forced = await backend("read_file", {**args, "force": True}, ctx)
    other_session = await backend(
        "read_file",
        args,
        ToolContext(user_id=ctx.user_id, session_id=uuid4(), openoctopus_device="server"),
    )

    assert first.content == "1|hello"
    assert unchanged.content == "[File unchanged since last read: notes/a.txt]"
    assert forced.content == "1|hello"
    assert other_session.content == "1|hello"
    assert service.reads == 3
    assert service.stats == 4
    assert service.calls[:3] == ["stat", "read", "stat"]


async def test_backend_read_cache_includes_pdf_pages_in_identity(pg_engine) -> None:
    service = _WorkspaceService()
    backend = WorkspaceToolDispatcher(pg_engine, service)  # type: ignore[arg-type]
    ctx = _ctx()
    args = {"path": "paper.pdf", "offset": 1, "limit": 20, "pages": "1", "force": False}

    first = await backend("read_file", args, ctx)
    unchanged = await backend("read_file", args, ctx)
    second_page = await backend("read_file", {**args, "pages": "2"}, ctx)

    assert first.content == "pages=1"
    assert unchanged.content == "[File unchanged since last read: paper.pdf]"
    assert second_page.content == "pages=2"
    assert service.reads == 2
    assert service.stats == 3


async def test_backend_read_cache_miss_when_etag_changes(pg_engine) -> None:
    service = _WorkspaceService()
    backend = WorkspaceToolDispatcher(pg_engine, service)  # type: ignore[arg-type]
    ctx = _ctx()
    args = {"path": "notes/a.txt", "offset": 1, "limit": 20, "pages": None, "force": False}

    await backend("read_file", args, ctx)
    service.etag = "revision-2"
    changed = await backend("read_file", args, ctx)

    assert changed.content == "1|hello"
    assert service.reads == 2


def test_patch_result_type_stays_importable_for_backend_contract() -> None:
    result = PatchEditResult("a.txt", "add", 1, "etag", True, 0)
    assert result.path == "a.txt"
