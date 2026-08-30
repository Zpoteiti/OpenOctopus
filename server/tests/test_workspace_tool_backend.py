import json
from types import SimpleNamespace
from uuid import uuid4

from openctopus_server.tools.base import ToolContext
from openctopus_server.tools.file_results import (
    FILE_RESULT_MAX_OUTPUT_CHARS,
    canonical_server_path,
    file_patch_result,
)
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
    assert json.loads(str(result.content)) == {
        "ok": True,
        "operation": "write_file",
        "device": "server",
        "requested_path": "notes/a.txt",
        "canonical_path": "~/notes/a.txt",
        "bytes_written": 5,
    }
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


def test_patch_result_omits_trailing_details_instead_of_truncating_json() -> None:
    encoded = file_patch_result(
        device="server",
        dry_run=False,
        edits=[
            {
                "action": "add",
                "requested_path": f"{index}-" + "\\" * 4090,
                "canonical_path": f"~/{index}-" + "\\" * 4090,
                "size_bytes": 1,
                "replacements": 0,
            }
            for index in range(20)
        ],
    )

    payload = json.loads(encoded)
    assert len(encoded) < FILE_RESULT_MAX_OUTPUT_CHARS
    assert payload["total_edits"] == 20
    assert payload["omitted_edits"] > 0
    assert len(payload["edits"]) + payload["omitted_edits"] == 20


def test_patch_result_can_omit_one_unrepresentable_path_detail() -> None:
    path = "\x01" * 5000

    encoded = file_patch_result(
        device="server",
        dry_run=False,
        edits=[
            {
                "action": "add",
                "requested_path": path,
                "canonical_path": path,
                "size_bytes": 1,
                "replacements": 0,
            }
        ],
    )

    payload = json.loads(encoded)
    assert len(encoded) <= FILE_RESULT_MAX_OUTPUT_CHARS
    assert payload["edits"] == []
    assert payload["omitted_edits"] == 1


def test_server_home_alias_has_one_stable_canonical_form() -> None:
    assert canonical_server_path("~//notes/a.txt") == "~/notes/a.txt"
    assert canonical_server_path(r"~\\notes\a.txt") == "~/notes/a.txt"
    assert canonical_server_path(r"notes\a.txt") == "~/notes/a.txt"
