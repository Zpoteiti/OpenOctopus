from dataclasses import replace
from typing import Any
from uuid import uuid4

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import Tool, ToolContext, ToolResult, ToolRoutingMode
from openctopus_server.tools.device_field import DEVICE_FIELD_MARKER, DEVICE_FIELD_NAME
from openctopus_server.tools.registry import (
    ToolRegistry,
    build_py3_registry,
    extend_openoctopus_device_enums,
    inject_device_routing,
)
from openctopus_server.tools.result import UNTRUSTED_TOOL_RESULT_WARNING


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
