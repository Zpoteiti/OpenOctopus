import json
from collections.abc import Callable
from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

import pytest

from openctopus_server.devices.protocol import MAX_TEXT_FRAME_BYTES, ToolResultFrame, new_uuid7
from openctopus_server.devices.registry import DeviceBusyError, DeviceUnavailableError
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import (
    MessageDeliveryEffect,
    Tool,
    ToolContext,
    ToolResult,
    ToolRoutingMode,
)
from openctopus_server.tools.device_field import DEVICE_FIELD_MARKER, DEVICE_FIELD_NAME
from openctopus_server.tools.registry import (
    ToolRegistry,
    _device_result_credit,
    build_py3_registry,
    extend_openoctopus_device_enums,
    inject_device_routing,
)
from openctopus_server.tools.result import UNTRUSTED_TOOL_RESULT_WARNING


class _FakeDeviceRegistry:
    def __init__(self, result: ToolResultFrame) -> None:
        self.result = result
        self.calls: list[dict[str, object]] = []

    async def dispatch_tool(
        self,
        *,
        device_id: UUID,
        user_id: UUID,
        name: str,
        args: dict[str, object],
        max_result_bytes: int,
        timeout_seconds: float,
        expected_device_name: str | None = None,
        chat_session_id: UUID | None = None,
        on_issued: Callable[[], None] | None = None,
    ) -> ToolResultFrame:
        del chat_session_id
        if on_issued is not None:
            on_issued()
        self.calls.append(
            {
                "device_id": device_id,
                "user_id": user_id,
                "name": name,
                "args": args,
                "max_result_bytes": max_result_bytes,
                "timeout_seconds": timeout_seconds,
                "expected_device_name": expected_device_name,
            }
        )
        return self.result


class _FailingDeviceRegistry:
    def __init__(self, failure: Exception) -> None:
        self.failure = failure

    async def dispatch_tool(
        self,
        **kwargs: object,
    ) -> ToolResultFrame:
        del kwargs
        raise self.failure


class _EchoTool(Tool):
    def __init__(self) -> None:
        self.received_args: dict[str, Any] | None = None
        self.received_ctx: ToolContext | None = None

    def name(self) -> str:
        return "echo"

    def schema(self) -> dict[str, Any]:
        return {
            "name": "echo",
            "description": "Echo input",
            "input_schema": {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            },
        }

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        self.received_args = args
        self.received_ctx = ctx
        return ToolResult(content=str(args["value"]))


class _PureServerTool(_EchoTool):
    routing_mode = ToolRoutingMode.PURE_SERVER


class _ClientWriteTool(_EchoTool):
    def name(self) -> str:
        return "write_file"

    def schema(self) -> dict[str, Any]:
        return {
            "name": self.name(),
            "description": "Write a file",
            "input_schema": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["path", "content"],
                "additionalProperties": False,
            },
        }


class _ReservedMcpPrefixTool(_EchoTool):
    def name(self) -> str:
        return "mcp_reserved"

    def schema(self) -> dict[str, Any]:
        schema = super().schema()
        schema["name"] = self.name()
        return schema


class _IntrinsicDeviceTool(_EchoTool):
    routing_mode = ToolRoutingMode.INTRINSIC_DEVICE

    def schema(self) -> dict[str, Any]:
        schema = super().schema()
        schema["input_schema"]["properties"][DEVICE_FIELD_NAME] = {
            "type": "string",
            "enum": ["server"],
            DEVICE_FIELD_MARKER: True,
        }
        schema["input_schema"]["required"].append(DEVICE_FIELD_NAME)
        return schema


class _ErrorWithSideEffectTool(_EchoTool):
    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        del args, ctx
        return ToolResult(
            content="failed",
            is_error=True,
            code=ErrorCode.TOOL_DELIVERY_FAILED,
            side_effect=MessageDeliveryEffect(delivery_refs=()),
        )


def _ctx() -> ToolContext:
    return ToolContext(
        user_id=uuid4(),
        session_id=uuid4(),
    )


def test_py3_registry_exposes_only_server_web_fetch() -> None:
    schemas = build_py3_registry().get_tool_schemas()

    assert [schema["name"] for schema in schemas] == ["web_fetch"]
    input_schema = schemas[0]["input_schema"]
    assert input_schema["properties"][DEVICE_FIELD_NAME] == {
        "type": "string",
        "enum": ["server"],
        "description": "Which install site to execute on.",
        DEVICE_FIELD_MARKER: True,
    }
    assert input_schema["required"] == ["url", DEVICE_FIELD_NAME]


def test_registry_reserves_the_mcp_prefix_for_dynamic_capabilities() -> None:
    with pytest.raises(ValueError, match="mcp_ prefix"):
        ToolRegistry((_ReservedMcpPrefixTool(),))


def test_inject_device_routing_changes_only_a_copy_of_the_inner_schema() -> None:
    source = _EchoTool().schema()

    merged = inject_device_routing(source, sites=("server", "laptop"))

    assert DEVICE_FIELD_NAME not in source["input_schema"]["properties"]
    assert source["input_schema"]["required"] == ["value"]
    assert merged["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"] == [
        "server",
        "laptop",
    ]
    assert merged["input_schema"]["required"] == ["value", DEVICE_FIELD_NAME]


def test_registry_includes_paired_offline_device_names_in_shared_schemas() -> None:
    registry = ToolRegistry((_EchoTool(),))

    schema = registry.get_tool_schemas(device_names=("laptop", "offline-phone"))[0]

    assert schema["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"] == [
        "server",
        "laptop",
        "offline-phone",
    ]


def test_extend_device_enums_uses_marker_and_preserves_native_device_argument() -> None:
    source = {
        "name": "transfer",
        "description": "Transfer",
        "input_schema": {
            "type": "object",
            "properties": {
                "device": {"type": "string", "enum": ["gpu-0"]},
                "openoctopus_src_device": {
                    "type": "string",
                    "enum": ["server"],
                    DEVICE_FIELD_MARKER: True,
                },
                "openoctopus_dst_device": {
                    "type": "string",
                    "enum": ["server"],
                    DEVICE_FIELD_MARKER: True,
                },
            },
            "required": ["openoctopus_src_device", "openoctopus_dst_device"],
        },
    }

    merged = extend_openoctopus_device_enums(source, extra=("laptop", "phone"))
    properties = merged["input_schema"]["properties"]

    assert properties["device"]["enum"] == ["gpu-0"]
    assert properties["openoctopus_src_device"]["enum"] == [
        "server",
        "laptop",
        "phone",
    ]
    assert properties["openoctopus_dst_device"]["enum"] == [
        "server",
        "laptop",
        "phone",
    ]
    assert source["input_schema"]["properties"]["openoctopus_src_device"]["enum"] == ["server"]


async def test_registry_consumes_routing_and_normalizes_real_result() -> None:
    tool = _EchoTool()
    registry = ToolRegistry((tool,))
    ctx = _ctx()

    result = await registry.execute(
        name="echo",
        args={"value": "hello", DEVICE_FIELD_NAME: "server"},
        ctx=ctx,
    )

    assert result == ToolResult(
        content=[
            {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
            {"type": "text", "text": "hello"},
        ]
    )
    assert tool.received_args == {"value": "hello"}
    assert tool.received_ctx == replace(ctx, openoctopus_device="server")


async def test_registry_rejects_missing_or_unavailable_routing() -> None:
    registry = ToolRegistry((_EchoTool(),))

    missing = await registry.execute(name="echo", args={"value": "x"}, ctx=_ctx())
    unavailable = await registry.execute(
        name="echo",
        args={"value": "x", DEVICE_FIELD_NAME: "laptop"},
        ctx=_ctx(),
    )

    assert missing.is_error is True
    assert missing.code == ErrorCode.TOOL_MISSING_REQUIRED_FIELD
    assert unavailable.is_error is True
    assert unavailable.code == ErrorCode.TOOL_DEVICE_UNREACHABLE


async def test_registry_dispatches_non_server_routing_to_captured_owned_device() -> None:
    user_id = uuid4()
    device_id = uuid4()
    ctx = ToolContext(user_id=user_id, session_id=uuid4())
    device_registry = _FakeDeviceRegistry(
        ToolResultFrame(
            id=new_uuid7(),
            content=[{"type": "text", "text": "from client"}],
            is_error=False,
        )
    )
    registry = ToolRegistry((_EchoTool(),))

    result = await registry.execute(
        name="echo",
        args={"value": "hello", DEVICE_FIELD_NAME: "laptop"},
        ctx=ctx,
        device_targets={"laptop": device_id},
        device_registry=device_registry,
    )

    assert device_registry.calls == [
        {
            "device_id": device_id,
            "user_id": user_id,
            "name": "echo",
            "args": {"value": "hello"},
            "max_result_bytes": 161_536,
            "timeout_seconds": 60.0,
            "expected_device_name": "laptop",
        }
    ]
    assert result == ToolResult(
        content=[
            {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
            {"type": "text", "text": "from client"},
        ]
    )
    tool = registry._tools["echo"]
    assert isinstance(tool, _EchoTool)
    assert tool.received_args is None


async def test_registry_adds_trusted_device_to_client_file_mutation_result() -> None:
    device_id = uuid4()
    device_registry = _FakeDeviceRegistry(
        ToolResultFrame(
            id=new_uuid7(),
            content=json.dumps(
                {
                    "ok": True,
                    "operation": "write_file",
                    "requested_path": "notes/a.txt",
                    "canonical_path": "~/workspace/notes/a.txt",
                    "bytes_written": 5,
                    "device": "spoofed",
                }
            ),
            is_error=False,
        )
    )
    registry = ToolRegistry((_ClientWriteTool(),))

    result = await registry.execute(
        name="write_file",
        args={
            "path": "notes/a.txt",
            "content": "hello",
            DEVICE_FIELD_NAME: "laptop",
        },
        ctx=_ctx(),
        device_targets={"laptop": device_id},
        device_registry=device_registry,
    )

    assert isinstance(result.content, list)
    assert json.loads(result.content[1]["text"]) == {
        "ok": True,
        "operation": "write_file",
        "device": "laptop",
        "requested_path": "notes/a.txt",
        "canonical_path": "~/workspace/notes/a.txt",
        "bytes_written": 5,
    }


@pytest.mark.parametrize(
    "content",
    [
        "Wrote notes/a.txt",
        json.dumps(
            {
                "ok": True,
                "operation": "delete_file",
                "requested_path": "notes/a.txt",
                "canonical_path": "~/notes/a.txt",
            }
        ),
        json.dumps(
            {
                "ok": True,
                "operation": "write_file",
                "requested_path": "notes/a.txt",
            }
        ),
        [{"type": "text", "text": "not a mutation envelope"}],
    ],
)
async def test_registry_rejects_invalid_successful_client_file_mutation_result(
    content: str | list[dict[str, str]],
) -> None:
    device_id = uuid4()
    registry = ToolRegistry((_ClientWriteTool(),))

    result = await registry.execute(
        name="write_file",
        args={
            "path": "notes/a.txt",
            "content": "hello",
            DEVICE_FIELD_NAME: "laptop",
        },
        ctx=_ctx(),
        device_targets={"laptop": device_id},
        device_registry=_FakeDeviceRegistry(
            ToolResultFrame(
                id=new_uuid7(),
                content=content,
                is_error=False,
            )
        ),
    )

    assert result.is_error is True
    assert result.code == ErrorCode.TOOL_EXECUTION_OUTCOME_UNKNOWN


async def test_registry_rechecks_live_device_identity_before_ws_io() -> None:
    user_id = uuid4()
    captured_device_id = uuid4()
    recreated_device_id = uuid4()
    device_registry = _FakeDeviceRegistry(
        ToolResultFrame(id=new_uuid7(), content="must not send", is_error=False)
    )

    async def resolve(_user_id: UUID, _name: str) -> UUID | None:
        return recreated_device_id

    registry = ToolRegistry((_EchoTool(),), device_resolver=resolve)

    result = await registry.execute(
        name="echo",
        args={"value": "hello", DEVICE_FIELD_NAME: "laptop"},
        ctx=ToolContext(user_id=user_id, session_id=uuid4()),
        device_targets={"laptop": captured_device_id},
        device_registry=device_registry,
    )

    assert result.is_error is True
    assert result.code == ErrorCode.TOOL_DEVICE_UNREACHABLE
    assert device_registry.calls == []


def test_device_result_credit_covers_json_escaping_and_image_blocks() -> None:
    assert _device_result_credit("echo", 16_000) == 161_536
    assert _device_result_credit("read_file", 128_000) == MAX_TEXT_FRAME_BYTES


async def test_registry_never_routes_a_non_server_name_outside_captured_user_devices() -> None:
    device_registry = _FakeDeviceRegistry(
        ToolResultFrame(id=new_uuid7(), content="unexpected", is_error=False)
    )
    registry = ToolRegistry((_EchoTool(),))

    result = await registry.execute(
        name="echo",
        args={"value": "hello", DEVICE_FIELD_NAME: "other-users-laptop"},
        ctx=_ctx(),
        device_targets={},
        device_registry=device_registry,
    )

    assert result.is_error is True
    assert result.code == ErrorCode.TOOL_DEVICE_UNREACHABLE
    assert device_registry.calls == []


async def test_registry_normalizes_disconnected_or_timed_out_device_calls() -> None:
    registry = ToolRegistry((_EchoTool(),))
    device_id = uuid4()

    for failure in (DeviceUnavailableError("replaced"), TimeoutError(), DeviceBusyError("busy")):
        result = await registry.execute(
            name="echo",
            args={"value": "hello", DEVICE_FIELD_NAME: "laptop"},
            ctx=_ctx(),
            device_targets={"laptop": device_id},
            device_registry=_FailingDeviceRegistry(failure),
        )

        assert result.is_error is True
        assert result.code == (
            ErrorCode.TOOL_DEVICE_BUSY
            if isinstance(failure, DeviceBusyError)
            else ErrorCode.TOOL_DEVICE_UNREACHABLE
        )


async def test_pure_server_tool_has_no_device_argument() -> None:
    tool = _PureServerTool()
    registry = ToolRegistry((tool,))
    ctx = _ctx()

    schema = registry.get_tool_schemas()[0]
    result = await registry.execute(name="echo", args={"value": "hello"}, ctx=ctx)

    assert DEVICE_FIELD_NAME not in schema["input_schema"]["properties"]
    assert tool.received_args == {"value": "hello"}
    assert tool.received_ctx == ctx
    assert result.is_error is False


async def test_intrinsic_device_tool_keeps_its_marked_device_argument() -> None:
    tool = _IntrinsicDeviceTool()
    registry = ToolRegistry((tool,))
    ctx = _ctx()

    schema = registry.get_tool_schemas()[0]
    result = await registry.execute(
        name="echo",
        args={"value": "hello", DEVICE_FIELD_NAME: "server"},
        ctx=ctx,
    )

    assert schema["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"] == ["server"]
    assert tool.received_args == {"value": "hello", DEVICE_FIELD_NAME: "server"}
    assert tool.received_ctx == ctx
    assert result.is_error is False


async def test_intrinsic_device_tool_extends_marked_device_enum() -> None:
    registry = ToolRegistry((_IntrinsicDeviceTool(),))

    schema = registry.get_tool_schemas(device_names=("laptop",))[0]

    assert schema["input_schema"]["properties"][DEVICE_FIELD_NAME]["enum"] == [
        "server",
        "laptop",
    ]


async def test_intrinsic_tool_receives_the_provider_turn_device_snapshot() -> None:
    tool = _IntrinsicDeviceTool()
    registry = ToolRegistry((tool,))
    device_id = uuid4()

    result = await registry.execute(
        name="echo",
        args={"value": "hello", DEVICE_FIELD_NAME: "laptop"},
        ctx=_ctx(),
        device_targets={"laptop": device_id},
    )

    assert result.is_error is False
    assert tool.received_ctx is not None
    assert tool.received_ctx.device_targets == {"laptop": device_id}


async def test_registry_discards_side_effects_from_error_results() -> None:
    registry = ToolRegistry((_ErrorWithSideEffectTool(),))

    result = await registry.execute(
        name="echo",
        args={"value": "hello", DEVICE_FIELD_NAME: "server"},
        ctx=_ctx(),
    )

    assert result.is_error is True
    assert result.side_effect is None
