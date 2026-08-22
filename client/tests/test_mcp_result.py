from __future__ import annotations

import base64
from uuid import UUID

from mcp import types
from pydantic import AnyUrl

from openoctopus_client.mcp.result import (
    enforce_result_credit,
    map_prompt_result,
    map_resource_result,
    map_tool_result,
)
from openoctopus_client.tools.common import ToolOutput

_REQUEST_ID = UUID("0198d6b8-03d6-7a1b-8f42-6c54fcbad921")
_PNG = base64.b64encode(b"\x89PNG\r\n\x1a\nsmall").decode("ascii")


def _text(text: str) -> dict[str, object]:
    return {"type": "text", "text": text}


def _image(data: str = _PNG, media_type: str = "image/png") -> dict[str, object]:
    return {
        "type": "image",
        "source": {"type": "base64", "media_type": media_type, "data": data},
    }


def test_tool_result_maps_content_then_structured_content_canonically() -> None:
    result = types.CallToolResult(
        content=[
            types.TextContent(type="text", text="hello"),
            types.ImageContent(type="image", data=_PNG, mimeType="image/png"),
        ],
        structuredContent={"z": 1, "a": "snow 雪"},
    )

    assert map_tool_result(result) == ToolOutput(
        [
            _text("hello"),
            _image(),
            _text('[mcp_structured_content]\n{"a":"snow 雪","z":1}'),
        ]
    )


def test_tool_result_preserves_resource_link_without_following_it() -> None:
    link = types.ResourceLink(
        type="resource_link",
        name="manual",
        uri=AnyUrl("https://example.test/manual"),
        mimeType="text/plain",
        size=42,
    )

    output = map_tool_result(types.CallToolResult(content=[link]))

    assert output == ToolOutput(
        [
            _text(
                '[mcp_resource_link]\n{"mimeType":"text/plain","name":"manual",'
                '"size":42,"type":"resource_link","uri":"https://example.test/manual"}'
            )
        ]
    )


def test_resource_result_maps_text_and_image_blob_in_order() -> None:
    result = types.ReadResourceResult(
        contents=[
            types.TextResourceContents(
                uri=AnyUrl("file:///notes.txt"),
                mimeType="text/plain",
                text="notes",
            ),
            types.BlobResourceContents(
                uri=AnyUrl("file:///pixel.png"),
                mimeType="image/png",
                blob=_PNG,
            ),
        ]
    )

    assert map_resource_result(result) == ToolOutput(
        [
            _text('[mcp_resource]\n{"mimeType":"text/plain","uri":"file:///notes.txt"}\nnotes'),
            _text(
                '[mcp_resource_image]\n{"mimeType":"image/png","uri":"file:///pixel.png"}'
            ),
            _image(),
        ]
    )


def test_prompt_result_preserves_message_and_content_order() -> None:
    result = types.GetPromptResult(
        messages=[
            types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text="question"),
            ),
            types.PromptMessage(
                role="assistant",
                content=types.ImageContent(type="image", data=_PNG, mimeType="image/png"),
            ),
        ]
    )

    assert map_prompt_result(result) == ToolOutput(
        [
            _text("[mcp_prompt_message role=user]"),
            _text("question"),
            _text("[mcp_prompt_message role=assistant]"),
            _image(),
        ]
    )


def test_empty_success_and_mcp_error_are_normal_tool_results() -> None:
    assert map_tool_result(types.CallToolResult(content=[])) == ToolOutput([_text("(no output)")])

    error = map_tool_result(
        types.CallToolResult(
            content=[types.TextContent(type="text", text="safe explanation")],
            isError=True,
        )
    )
    assert error == ToolOutput(
        [_text("safe explanation")],
        is_error=True,
        code="tool_mcp_error",
    )


def test_unsupported_media_discards_all_preceding_blocks() -> None:
    output = map_tool_result(
        types.CallToolResult(
            content=[
                types.TextContent(type="text", text="must not leak as partial"),
                types.AudioContent(type="audio", data="YWJj", mimeType="audio/wav"),
            ]
        )
    )

    assert output.is_error is True
    assert output.code == "tool_unsupported_media"
    assert "must not leak" not in str(output.content)


def test_invalid_base64_and_nonfinite_json_are_all_or_nothing() -> None:
    invalid_image = map_tool_result(
        types.CallToolResult(
            content=[
                types.TextContent(type="text", text="partial"),
                types.ImageContent(type="image", data="%%%", mimeType="image/png"),
            ]
        )
    )
    nonfinite = map_tool_result(
        types.CallToolResult(content=[], structuredContent={"bad": float("nan")})
    )

    assert invalid_image.code == "tool_mcp_invalid_result"
    assert "partial" not in str(invalid_image.content)
    assert nonfinite.code == "tool_mcp_invalid_result"


def test_blob_with_non_image_mime_is_unsupported_media() -> None:
    output = map_resource_result(
        types.ReadResourceResult(
            contents=[
                types.BlobResourceContents(
                    uri=AnyUrl("file:///archive.zip"),
                    mimeType="application/zip",
                    blob="YWJj",
                )
            ]
        )
    )

    assert output.code == "tool_unsupported_media"


def test_result_credit_uses_the_final_tool_result_frame() -> None:
    output = ToolOutput([_text("x" * 128)])

    assert enforce_result_credit(
        output,
        request_id=_REQUEST_ID,
        max_result_bytes=4096,
    ) is output
    reduced = enforce_result_credit(
        output,
        request_id=_REQUEST_ID,
        max_result_bytes=248,
    )

    assert reduced.code == "tool_result_too_large"
    assert "x" * 32 not in str(reduced.content)
