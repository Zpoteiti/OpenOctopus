from __future__ import annotations

import base64
from typing import Any, cast
from uuid import UUID

import pytest
from mcp import types
from pydantic import AnyUrl

from openoctopus_client.mcp.result import (
    enforce_result_credit,
    map_prompt_result,
    map_resource_result,
    map_tool_result,
)
from openoctopus_client.protocol import ToolResult, encode_frame
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


def _map_tool(
    result: types.CallToolResult,
    *,
    max_result_bytes: int = 4096,
) -> ToolOutput:
    return map_tool_result(
        result,
        request_id=_REQUEST_ID,
        max_result_bytes=max_result_bytes,
    )


def _map_resource(
    result: types.ReadResourceResult,
    *,
    max_result_bytes: int = 4096,
) -> ToolOutput:
    return map_resource_result(
        result,
        request_id=_REQUEST_ID,
        max_result_bytes=max_result_bytes,
    )


def _map_prompt(
    result: types.GetPromptResult,
    *,
    max_result_bytes: int = 4096,
) -> ToolOutput:
    return map_prompt_result(
        result,
        request_id=_REQUEST_ID,
        max_result_bytes=max_result_bytes,
    )


def test_tool_result_maps_content_then_structured_content_canonically() -> None:
    result = types.CallToolResult(
        content=[
            types.TextContent(type="text", text="hello"),
            types.ImageContent(type="image", data=_PNG, mimeType="image/png"),
        ],
        structuredContent={"z": 1, "a": "snow 雪"},
    )

    assert _map_tool(result) == ToolOutput(
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

    output = _map_tool(types.CallToolResult(content=[link]))

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

    assert _map_resource(result) == ToolOutput(
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

    assert _map_prompt(result) == ToolOutput(
        [
            _text("[mcp_prompt_message role=user]"),
            _text("question"),
            _text("[mcp_prompt_message role=assistant]"),
            _image(),
        ]
    )


def test_empty_success_and_mcp_error_are_normal_tool_results() -> None:
    assert _map_tool(types.CallToolResult(content=[])) == ToolOutput([_text("(no output)")])

    error = _map_tool(
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
    output = _map_tool(
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
    invalid_image = _map_tool(
        types.CallToolResult(
            content=[
                types.TextContent(type="text", text="partial"),
                types.ImageContent(type="image", data="%%%", mimeType="image/png"),
            ]
        )
    )
    nonfinite = _map_tool(
        types.CallToolResult(content=[], structuredContent={"bad": float("nan")})
    )

    assert invalid_image.code == "tool_mcp_invalid_result"
    assert "partial" not in str(invalid_image.content)
    assert nonfinite.code == "tool_mcp_invalid_result"


def test_blob_with_non_image_mime_is_unsupported_media() -> None:
    output = _map_resource(
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


def test_tool_mapper_accepts_the_exact_final_frame_credit() -> None:
    result = types.CallToolResult(
        content=[types.TextContent(type="text", text='snow 雪\n"quoted"')]
    )
    expected = _map_tool(result)
    frame = ToolResult(
        id=_REQUEST_ID,
        content=cast(Any, expected.content),
        is_error=expected.is_error,
        code=expected.code,
    )
    encoded_size = len(encode_frame(frame).encode("utf-8"))

    assert _map_tool(result, max_result_bytes=encoded_size) == expected
    assert _map_tool(result, max_result_bytes=encoded_size - 1) == ToolOutput(
        content="[tool_result_too_large] Tool result exceeded its response credit",
        is_error=True,
        code="tool_result_too_large",
    )


@pytest.mark.parametrize(
    "result",
    [
        types.CallToolResult(
            content=[
                types.TextContent(type="text", text="must not leak"),
                types.TextContent(type="text", text="x" * 1024),
            ]
        ),
        types.CallToolResult(content=[], structuredContent={"payload": "x" * 1024}),
    ],
)
def test_tool_mapper_rejects_oversized_text_and_structured_results_atomically(
    result: types.CallToolResult,
) -> None:
    output = _map_tool(result, max_result_bytes=256)

    assert output.code == "tool_result_too_large"
    assert "must not leak" not in str(output.content)
    assert "x" * 32 not in str(output.content)


def test_image_mapper_rejects_budget_before_base64_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = base64.b64encode(b"x" * 1024).decode("ascii")

    def unexpected_decode(*args: Any, **kwargs: Any) -> bytes:
        del args, kwargs
        raise AssertionError("oversized image must not be decoded")

    monkeypatch.setattr("openoctopus_client.mcp.result.base64.b64decode", unexpected_decode)

    output = _map_tool(
        types.CallToolResult(
            content=[types.ImageContent(type="image", data=data, mimeType="image/png")]
        ),
        max_result_bytes=256,
    )

    assert output.code == "tool_result_too_large"


def test_resource_mapper_rejects_oversized_text_atomically() -> None:
    output = _map_resource(
        types.ReadResourceResult(
            contents=[
                types.TextResourceContents(
                    uri=AnyUrl("file:///large.txt"),
                    mimeType="text/plain",
                    text="x" * 1024,
                )
            ]
        ),
        max_result_bytes=256,
    )

    assert output.code == "tool_result_too_large"
    assert "x" * 32 not in str(output.content)


def test_prompt_mapper_rejects_oversized_message_atomically() -> None:
    output = _map_prompt(
        types.GetPromptResult(
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.TextContent(type="text", text="x" * 1024),
                )
            ]
        ),
        max_result_bytes=256,
    )

    assert output.code == "tool_result_too_large"
    assert "x" * 32 not in str(output.content)
