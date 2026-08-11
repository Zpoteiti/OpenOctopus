from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Any

import httpx
import pytest

from openoctopus_client.tools import ClientToolDispatcher
from openoctopus_client.tools import dispatcher as dispatcher_module
from openoctopus_client.tools.common import ToolFailure, ToolOutput


def _run(dispatcher: ClientToolDispatcher, name: str, **args: Any) -> ToolOutput:
    return asyncio.run(dispatcher.execute(name, args))


def test_file_tools_are_workspace_confined_atomic_and_fuzzy(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    tools = ClientToolDispatcher(workspace, sandbox_mode=True, ssrf_denylist=[])

    written = _run(tools, "write_file", path="notes/a.txt", content="one\n  two\n")
    assert written.is_error is False
    read = _run(tools, "read_file", path="notes/a.txt")
    assert isinstance(read.content, str)
    assert read.content.startswith("1|one\n2|  two")

    edited = _run(
        tools,
        "edit_file",
        path="notes/a.txt",
        old_text="one\ntwo",
        new_text="three\nfour",
    )
    assert edited.is_error is False
    assert "three\nfour" in (workspace / "notes/a.txt").read_text()

    patched = _run(
        tools,
        "apply_patch",
        edits=[{"path": "notes/a.txt", "action": "add", "new_text": "five\n"}],
    )
    assert patched.is_error is False
    assert (workspace / "notes/a.txt").read_text().endswith("five\n")


def test_paths_reject_escape_and_symlink_traversal(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    outside = tmp_path / "outside"
    workspace.mkdir()
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")
    (workspace / "link").symlink_to(outside, target_is_directory=True)
    tools = ClientToolDispatcher(workspace, sandbox_mode=True, ssrf_denylist=[])

    escaped = _run(tools, "read_file", path="../outside/secret.txt")
    symlinked = _run(tools, "read_file", path="link/secret.txt")
    assert escaped.code == "tool_path_outside_workspace"
    assert symlinked.code == "workspace_symlink_escape"


def test_workspace_root_and_nul_text_are_rejected(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    link = tmp_path / "workspace-link"
    link.symlink_to(workspace, target_is_directory=True)
    with pytest.raises(ToolFailure, match="symbolic link"):
        ClientToolDispatcher(link, sandbox_mode=True, ssrf_denylist=[])

    tools = ClientToolDispatcher(workspace, sandbox_mode=True, ssrf_denylist=[])
    assert _run(tools, "write_file", path="nul.txt", content="a\x00b").code == "tool_invalid_args"
    (workspace / "binary.txt").write_bytes(b"a\x00b")
    assert _run(tools, "read_file", path="binary.txt").code == "tool_invalid_args"


def test_delete_folder_rejects_workspace_and_filesystem_roots(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "keep.txt").write_text("keep", encoding="utf-8")

    sandboxed = ClientToolDispatcher(workspace, sandbox_mode=True, ssrf_denylist=[])
    trusted = ClientToolDispatcher(workspace, sandbox_mode=False, ssrf_denylist=[])

    assert _run(sandboxed, "delete_folder", path=".").code == "workspace_invalid_request"
    assert _run(trusted, "delete_folder", path=workspace.anchor).code == (
        "workspace_invalid_request"
    )
    assert (workspace / "keep.txt").read_text(encoding="utf-8") == "keep"


def test_discovery_grep_and_notebook_edit(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "a.py").write_text("print('hello')\nprint('again')\n")
    (workspace / ".git").mkdir()
    (workspace / ".git" / "noise.py").write_text("hello")
    (workspace / "book.ipynb").write_text(
        '{"cells":[{"cell_type":"code","metadata":{},"source":"x=1","outputs":[]}]}'
    )
    tools = ClientToolDispatcher(workspace, sandbox_mode=True, ssrf_denylist=[])

    found = _run(tools, "find_files", path=".", type="py")
    assert found.content == "a.py"
    matched = _run(tools, "grep", pattern="hello", path=".", output_mode="content")
    assert "a.py:1:print('hello')" in matched.content
    changed = _run(
        tools,
        "notebook_edit",
        path="book.ipynb",
        cell_index=0,
        new_source="x=2",
    )
    assert changed.is_error is False
    assert "x=2" in (workspace / "book.ipynb").read_text()


@pytest.mark.parametrize(
    "content",
    [
        "not json",
        json.dumps({"cells": [{"cell_type": "code", "metadata": {}, "source": "x"}]}),
        json.dumps({"metadata": [], "cells": []}),
        json.dumps({"cells": [None]}),
    ],
)
def test_notebook_edit_rejects_invalid_complete_notebook_shapes(
    tmp_path: Path, content: str
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "book.ipynb").write_text(content)
    tools = ClientToolDispatcher(workspace, sandbox_mode=True, ssrf_denylist=[])

    result = _run(tools, "notebook_edit", path="book.ipynb", cell_index=0, new_source="new")

    assert result.code == "tool_invalid_notebook"


def test_invalid_riff_is_not_reported_as_webp(tmp_path: Path) -> None:
    assert dispatcher_module._image_media_type(b"RIFF" + b"x" * 20) is None
    assert dispatcher_module._image_media_type(b"RIFF" + b"x" * 4 + b"WEBP") == "image/webp"


def test_web_fetch_maps_httpx_timeout_and_transport_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def timeout(*args: object, **kwargs: object) -> object:
        raise httpx.ReadTimeout("slow")

    async def transport(*args: object, **kwargs: object) -> object:
        raise httpx.ConnectError("failed")

    monkeypatch.setattr(dispatcher_module, "_fetch_bounded", timeout)
    tools = ClientToolDispatcher(Path.cwd(), sandbox_mode=False, ssrf_denylist=[])
    timed_out = _run(tools, "web_fetch", url="https://example.com")
    assert timed_out.code == "network_timeout"

    monkeypatch.setattr(dispatcher_module, "_fetch_bounded", transport)
    failed = _run(tools, "web_fetch", url="https://example.com")
    assert failed.code == "network_http_error"


def test_mutations_detect_external_changes_before_commit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    target = workspace / "notes.txt"
    target.write_text("old")
    tools = ClientToolDispatcher(workspace, sandbox_mode=True, ssrf_denylist=[])
    original = dispatcher_module._apply_text_edit

    def apply_and_race(*args: Any, **kwargs: Any) -> tuple[str, int, bool]:
        result = original(*args, **kwargs)
        target.write_text("external")
        return result

    monkeypatch.setattr(dispatcher_module, "_apply_text_edit", apply_and_race)
    result = _run(tools, "edit_file", path="notes.txt", old_text="old", new_text="new")

    assert result.code == "workspace_file_changed"
    assert target.read_text() == "external"


def test_cancelled_mutation_keeps_path_lock_until_worker_finishes(tmp_path: Path) -> None:
    async def exercise() -> None:
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        tools = ClientToolDispatcher(workspace, sandbox_mode=True, ssrf_denylist=[])
        started = threading.Event()
        release = threading.Event()
        original = tools._atomic_write

        def blocking(path: Path, data: bytes) -> None:
            started.set()
            release.wait(timeout=1)
            original(path, data)

        tools._atomic_write = blocking  # type: ignore[method-assign]
        first = asyncio.create_task(
            tools.execute("write_file", {"path": "same.txt", "content": "first"})
        )
        for _ in range(100):
            if started.is_set():
                break
            await asyncio.sleep(0.001)
        assert started.is_set()
        first.cancel()
        second = asyncio.create_task(
            tools.execute("write_file", {"path": "same.txt", "content": "second"})
        )
        await asyncio.sleep(0.02)
        assert not second.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert (await second).is_error is False
        assert (workspace / "same.txt").read_text() == "second"

    asyncio.run(exercise())


def test_unknown_tool_is_not_available(tmp_path: Path) -> None:
    tools = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])
    output = _run(tools, "exec", command="id")
    assert output.code == "tool_not_available"


def test_tool_arguments_reject_unknown_fields_including_patch_items(tmp_path: Path) -> None:
    tools = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])

    read = _run(tools, "read_file", path="notes.txt", untrusted=True)
    patch = _run(
        tools,
        "apply_patch",
        edits=[{"path": "notes.txt", "action": "add", "new_text": "x", "untrusted": True}],
    )

    assert read.code == "tool_invalid_args"
    assert patch.code == "tool_invalid_args"


def test_apply_patch_rejects_repeated_canonical_paths(tmp_path: Path) -> None:
    tools = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=[])

    output = _run(
        tools,
        "apply_patch",
        edits=[
            {"path": "notes.txt", "action": "add", "new_text": "one"},
            {"path": "./notes.txt", "action": "add", "new_text": "two"},
        ],
    )

    assert output.code == "tool_invalid_args"


def test_web_fetch_enforces_client_denylist_before_connecting(tmp_path: Path) -> None:
    tools = ClientToolDispatcher(tmp_path, sandbox_mode=True, ssrf_denylist=["127.0.0.0/8"])

    output = _run(tools, "web_fetch", url="http://127.0.0.1:9/", maxChars=100)

    assert output.code == "network_ssrf_blocked"


def test_web_fetch_hostname_denylist_is_case_insensitive(tmp_path: Path) -> None:
    tools = ClientToolDispatcher(
        tmp_path,
        sandbox_mode=True,
        ssrf_denylist=["LOCALHOST"],
    )

    output = _run(tools, "web_fetch", url="http://localhost:9/", maxChars=100)

    assert output.code == "network_ssrf_blocked"


@pytest.mark.parametrize(
    ("url", "resolved"),
    [
        ("http://[::ffff:127.0.0.1]:9/", "::ffff:127.0.0.1"),
        ("http://public.example/", "::ffff:169.254.169.254"),
    ],
)
def test_web_fetch_blocks_ipv4_mapped_ipv6_literals_and_dns_answers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    url: str,
    resolved: str,
) -> None:
    async def fake_getaddrinfo(
        loop: asyncio.BaseEventLoop,
        host: str,
        port: int,
        **kwargs: object,
    ) -> list[tuple[int, int, int, str, tuple[str, int, int, int]]]:
        del loop, host, kwargs
        return [(10, 1, 6, "", (resolved, port, 0, 0))]

    monkeypatch.setattr(asyncio.BaseEventLoop, "getaddrinfo", fake_getaddrinfo)
    tools = ClientToolDispatcher(
        tmp_path,
        sandbox_mode=True,
        ssrf_denylist=["127.0.0.0/8", "169.254.0.0/16"],
    )

    output = _run(tools, "web_fetch", url=url, maxChars=100)

    assert output.code == "network_ssrf_blocked"


def test_web_fetch_revalidates_original_host_on_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_validate(host: str, port: int, denylist: tuple[str, ...]) -> list[str]:
        calls.append(host)
        return ["203.0.113.7"]

    class Response:
        def __init__(self, status: int, headers: dict[str, str], body: bytes = b"") -> None:
            self.status_code = status
            self.headers = headers
            self.encoding = "utf-8"
            self._body = body

        async def aclose(self) -> None:
            return None

        async def aiter_raw(self) -> object:
            yield self._body

    class Client:
        def __init__(self) -> None:
            self.responses = iter(
                [
                    Response(302, {"location": "child"}),
                    Response(200, {"content-type": "text/plain"}, b"ok"),
                ]
            )

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        def build_request(self, *args: object, **kwargs: object) -> object:
            return object()

        async def send(self, request: object, *, stream: bool) -> Response:
            return next(self.responses)

    monkeypatch.setattr(dispatcher_module, "_validated_addresses", fake_validate)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: Client())

    result = asyncio.run(dispatcher_module._fetch_bounded("https://public.example/root/", ()))

    assert result[0] == b"ok"
    assert calls == ["public.example", "public.example", "public.example", "public.example"]
