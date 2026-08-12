from types import SimpleNamespace
from uuid import uuid4

from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.workspace_backend import WorkspaceToolDispatcher
from openctopus_server.workspace.service import PatchEditResult, ToolFileRead


class _WorkspaceService:
    def __init__(self) -> None:
        self.writes: list[tuple[object, str, bytes]] = []
        self.reads = 0
        self.authorizations = 0
        self.calls: list[str] = []
        self.etag = "revision-1"

    async def authorize_tool_read(self, db, *, user_id, path):
        del db, user_id, path
        self.authorizations += 1
        self.calls.append("authorize")
        return SimpleNamespace()

    async def read_for_tool(
        self,
        ticket,
        *,
        user_id,
        offset,
        limit,
        pages,
        parser,
    ):
        del ticket, user_id, limit, parser
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


def _backend(pg_engine, service: _WorkspaceService) -> WorkspaceToolDispatcher:
    return WorkspaceToolDispatcher(
        pg_engine,
        service,  # type: ignore[arg-type]
        document_parser=SimpleNamespace(),  # type: ignore[arg-type]
    )


async def test_backend_dispatches_write_through_workspace_service(pg_engine) -> None:
    service = _WorkspaceService()
    backend = _backend(pg_engine, service)
    ctx = _ctx()

    result = await backend(
        "write_file",
        {"path": "notes/a.txt", "content": "hello"},
        ctx,
    )

    assert result.is_error is False
    assert result.content == "Wrote notes/a.txt (5 bytes)."
    assert service.writes == [(ctx.user_id, "notes/a.txt", b"hello")]


async def test_backend_repeated_reads_always_return_content(pg_engine) -> None:
    service = _WorkspaceService()
    backend = _backend(pg_engine, service)
    ctx = _ctx()
    args = {"path": "notes/a.txt", "offset": 1, "limit": 20, "pages": None}

    first = await backend("read_file", args, ctx)
    second = await backend("read_file", args, ctx)

    assert first.content == "1|hello"
    assert second.content == "1|hello"
    assert service.reads == 2
    assert service.authorizations == 2
    assert service.calls[:4] == ["authorize", "read", "authorize", "read"]


async def test_backend_read_preserves_pdf_pages(pg_engine) -> None:
    service = _WorkspaceService()
    backend = _backend(pg_engine, service)
    ctx = _ctx()
    args = {"path": "paper.pdf", "offset": 1, "limit": 20, "pages": "1"}

    first = await backend("read_file", args, ctx)
    second_page = await backend("read_file", {**args, "pages": "2"}, ctx)

    assert first.content == "pages=1"
    assert second_page.content == "pages=2"
    assert service.reads == 2
    assert service.authorizations == 2


def test_patch_result_type_stays_importable_for_backend_contract() -> None:
    result = PatchEditResult("a.txt", "add", 1, "etag", True, 0)
    assert result.path == "a.txt"
