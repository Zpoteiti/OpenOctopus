import asyncio
from typing import Any
from unittest.mock import Mock
from uuid import uuid4

import pytest

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import ToolContext, ToolResult
from openctopus_server.tools.device_field import DEVICE_FIELD_NAME
from openctopus_server.tools.registry import ToolRegistry, build_py4_registry
from openctopus_server.tools.workspace_files import (
    WORKSPACE_FILE_TOOL_SCHEMAS,
    WORKSPACE_FILE_TOOL_TIMEOUT_SECONDS,
    build_workspace_file_tools,
)
from openctopus_server.workspace.service import WorkspaceService

EXPECTED_TOOL_NAMES = [
    "read_file",
    "write_file",
    "edit_file",
    "apply_patch",
    "delete_file",
    "delete_folder",
    "list_dir",
    "find_files",
    "grep",
    "notebook_edit",
]


class _RecordingDispatcher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], ToolContext]] = []

    async def __call__(
        self,
        name: str,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        self.calls.append((name, args, ctx))
        return ToolResult(content=f"ran {name}")


class _BlockingDispatcher:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def __call__(
        self,
        name: str,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult:
        del name, args, ctx
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def _ctx() -> ToolContext:
    return ToolContext(user_id=uuid4(), session_id=uuid4())


def test_workspace_file_tool_schema_snapshot() -> None:
    dispatcher = _RecordingDispatcher()
    schemas = ToolRegistry(build_workspace_file_tools(dispatcher)).get_tool_schemas()

    assert [schema["name"] for schema in schemas] == EXPECTED_TOOL_NAMES
    assert list(WORKSPACE_FILE_TOOL_SCHEMAS) == EXPECTED_TOOL_NAMES
    for schema in schemas:
        input_schema = schema["input_schema"]
        assert input_schema["type"] == "object"
        assert input_schema["additionalProperties"] is False
        assert input_schema["properties"][DEVICE_FIELD_NAME]["enum"] == ["server"]
        assert DEVICE_FIELD_NAME in input_schema["required"]

    by_name = {schema["name"]: schema for schema in schemas}
    assert by_name["read_file"]["input_schema"]["required"] == [
        "path",
        DEVICE_FIELD_NAME,
    ]
    assert by_name["write_file"]["input_schema"]["required"] == [
        "path",
        "content",
        DEVICE_FIELD_NAME,
    ]
    assert by_name["edit_file"]["input_schema"]["properties"]["occurrence"]["minimum"] == 1
    assert by_name["apply_patch"]["input_schema"]["properties"]["edits"]["maxItems"] == 20
    assert by_name["list_dir"]["input_schema"]["properties"]["max_entries"]["maximum"] == 1000
    assert by_name["find_files"]["input_schema"]["properties"]["sort"]["enum"] == [
        "path",
        "modified",
    ]
    assert by_name["grep"]["input_schema"]["properties"]["output_mode"]["enum"] == [
        "content",
        "files_with_matches",
        "count",
    ]
    assert by_name["notebook_edit"]["input_schema"]["properties"]["edit_mode"]["enum"] == [
        "replace",
        "insert",
        "delete",
    ]


def test_workspace_file_tools_own_their_adr_075_timeouts() -> None:
    assert WORKSPACE_FILE_TOOL_TIMEOUT_SECONDS == {
        "read_file": 30,
        "write_file": 30,
        "edit_file": 30,
        "apply_patch": 30,
        "delete_file": 10,
        "delete_folder": 60,
        "list_dir": 10,
        "find_files": 30,
        "grep": 60,
        "notebook_edit": 30,
    }


async def test_workspace_file_tool_normalizes_its_internal_timeout(monkeypatch) -> None:
    dispatcher = _BlockingDispatcher()
    registry = ToolRegistry(build_workspace_file_tools(dispatcher))
    monkeypatch.setitem(WORKSPACE_FILE_TOOL_TIMEOUT_SECONDS, "read_file", 0.01)

    result = await registry.execute(
        name="read_file",
        args={"path": "a.txt", "openoctopus_device": "server"},
        ctx=_ctx(),
    )

    assert result.is_error is True
    assert result.code == ErrorCode.TOOL_EXEC_TIMEOUT
    assert isinstance(result.content, list)
    assert result.content[1]["text"].startswith("[tool_exec_timeout]")


async def test_workspace_file_tool_does_not_swallow_external_cancellation(monkeypatch) -> None:
    dispatcher = _BlockingDispatcher()
    registry = ToolRegistry(build_workspace_file_tools(dispatcher))
    monkeypatch.setitem(WORKSPACE_FILE_TOOL_TIMEOUT_SECONDS, "read_file", 30)
    task = asyncio.create_task(
        registry.execute(
            name="read_file",
            args={"path": "a.txt", "openoctopus_device": "server"},
            ctx=_ctx(),
        )
    )
    await dispatcher.started.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_workspace_file_tool_validates_and_dispatches_normalized_args() -> None:
    dispatcher = _RecordingDispatcher()
    registry = ToolRegistry(build_workspace_file_tools(dispatcher))
    ctx = _ctx()

    result = await registry.execute(
        name="list_dir",
        args={"path": ".", "openoctopus_device": "server"},
        ctx=ctx,
    )

    assert result.is_error is False
    assert dispatcher.calls == [
        (
            "list_dir",
            {"path": ".", "recursive": False, "max_entries": 200},
            ToolContext(
                user_id=ctx.user_id,
                session_id=ctx.session_id,
                openoctopus_device="server",
            ),
        )
    ]


async def test_workspace_file_tool_rejects_unknown_or_invalid_arguments_before_dispatch() -> None:
    dispatcher = _RecordingDispatcher()
    registry = ToolRegistry(build_workspace_file_tools(dispatcher))

    extra = await registry.execute(
        name="delete_file",
        args={"path": "a.txt", "extra": True, "openoctopus_device": "server"},
        ctx=_ctx(),
    )
    invalid = await registry.execute(
        name="read_file",
        args={"path": "a.txt", "offset": 0, "openoctopus_device": "server"},
        ctx=_ctx(),
    )

    assert extra.is_error is True
    assert extra.code == ErrorCode.TOOL_INVALID_ARGS
    assert invalid.is_error is True
    assert invalid.code == ErrorCode.TOOL_INVALID_ARGS
    assert dispatcher.calls == []


async def test_edit_and_patch_cross_field_validation_happens_before_dispatch() -> None:
    dispatcher = _RecordingDispatcher()
    registry = ToolRegistry(build_workspace_file_tools(dispatcher))

    conflicting_edit = await registry.execute(
        name="edit_file",
        args={
            "path": "a.txt",
            "old_text": "a",
            "new_text": "b",
            "replace_all": True,
            "occurrence": 1,
            "openoctopus_device": "server",
        },
        ctx=_ctx(),
    )
    incomplete_patch = await registry.execute(
        name="apply_patch",
        args={
            "edits": [{"path": "a.txt", "action": "replace", "new_text": "b"}],
            "openoctopus_device": "server",
        },
        ctx=_ctx(),
    )

    assert conflicting_edit.code == ErrorCode.TOOL_INVALID_ARGS
    assert incomplete_patch.code == ErrorCode.TOOL_INVALID_ARGS
    assert dispatcher.calls == []


async def test_notebook_edit_requires_new_source_except_for_delete() -> None:
    dispatcher = _RecordingDispatcher()
    registry = ToolRegistry(build_workspace_file_tools(dispatcher))

    missing_source = await registry.execute(
        name="notebook_edit",
        args={"path": "a.ipynb", "cell_index": 0, "openoctopus_device": "server"},
        ctx=_ctx(),
    )
    deleted = await registry.execute(
        name="notebook_edit",
        args={
            "path": "a.ipynb",
            "cell_index": 0,
            "edit_mode": "delete",
            "openoctopus_device": "server",
        },
        ctx=_ctx(),
    )

    assert missing_source.code == ErrorCode.TOOL_INVALID_ARGS
    assert deleted.is_error is False
    assert dispatcher.calls[0][0:2] == (
        "notebook_edit",
        {
            "path": "a.ipynb",
            "cell_index": 0,
            "new_source": None,
            "cell_type": "code",
            "edit_mode": "delete",
        },
    )


async def test_notebook_edit_rejects_non_notebook_paths() -> None:
    dispatcher = _RecordingDispatcher()
    registry = ToolRegistry(build_workspace_file_tools(dispatcher))

    result = await registry.execute(
        name="notebook_edit",
        args={
            "path": "notes.txt",
            "cell_index": 0,
            "new_source": "print('x')",
            "openoctopus_device": "server",
        },
        ctx=_ctx(),
    )

    assert result.code == ErrorCode.TOOL_INVALID_ARGS
    assert dispatcher.calls == []


def test_py4_registry_includes_message_and_ten_workspace_tools(pg_engine) -> None:
    registry = build_py4_registry(pg_engine, Mock(spec=WorkspaceService))

    assert [schema["name"] for schema in registry.get_tool_schemas()] == [
        "web_fetch",
        "message",
        *EXPECTED_TOOL_NAMES,
    ]
