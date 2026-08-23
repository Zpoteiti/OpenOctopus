from __future__ import annotations

import base64

from mcp import types

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.mcp import result as result_module
from openctopus_server.mcp.catalog import canonical_json_bytes
from openctopus_server.mcp.result import (
    map_prompt_result,
    map_resource_result,
    map_tool_result,
)
from openctopus_server.tools.result import (
    UNTRUSTED_TOOL_RESULT_WARNING,
    normalize_tool_result,
)

_PNG = base64.b64encode(b"\x89PNG\r\n\x1a\nsmall").decode("ascii")


def test_tool_result_maps_all_content_then_adds_untrusted_warning() -> None:
    result = map_tool_result(
        types.CallToolResult(
            content=[types.TextContent(type="text", text="hello")],
            structuredContent={"z": 1, "a": "snow 雪"},
        )
    )

    assert result.content == [
        {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
        {"type": "text", "text": "hello"},
        {
            "type": "text",
            "text": '[mcp_structured_content]\n{"a":"snow 雪","z":1}',
        },
    ]


def test_unsupported_media_discards_every_preceding_block() -> None:
    result = map_tool_result(
        types.CallToolResult(
            content=[
                types.TextContent(type="text", text="must not leak"),
                types.AudioContent(type="audio", data="YWJj", mimeType="audio/wav"),
            ]
        )
    )

    assert result.is_error is True
    assert result.code is ErrorCode.TOOL_UNSUPPORTED_MEDIA
    assert "must not leak" not in str(result.content)
    assert result.content[0]["text"] == UNTRUSTED_TOOL_RESULT_WARNING


def test_invalid_image_and_oversized_text_are_all_or_nothing() -> None:
    invalid = map_tool_result(
        types.CallToolResult(
            content=[types.ImageContent(type="image", data="%%%", mimeType="image/png")]
        )
    )
    oversized = map_tool_result(
        types.CallToolResult(content=[types.TextContent(type="text", text="x" * 5)]),
        max_text_chars=4,
    )

    assert invalid.code is ErrorCode.TOOL_MCP_INVALID_RESULT
    assert oversized.code is ErrorCode.TOOL_RESULT_TOO_LARGE
    assert "xxxxx" not in str(oversized.content)


def test_resource_and_prompt_results_preserve_order() -> None:
    resource = map_resource_result(
        types.ReadResourceResult(
            contents=[
                types.TextResourceContents(
                    uri="file:///notes.txt",
                    mimeType="text/plain",
                    text="notes",
                )
            ]
        )
    )
    prompt = map_prompt_result(
        types.GetPromptResult(
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.ImageContent(type="image", data=_PNG, mimeType="image/png"),
                )
            ]
        )
    )

    assert resource.content[1]["text"].endswith("\nnotes")
    assert prompt.content[1]["text"] == "[mcp_prompt_message role=user]"
    assert prompt.content[2]["type"] == "image"


def test_mcp_error_is_sanitized_and_empty_success_has_output() -> None:
    error = map_tool_result(
        types.CallToolResult(
            content=[types.TextContent(type="text", text="third-party detail")],
            isError=True,
        )
    )
    empty = map_tool_result(types.CallToolResult(content=[]))

    assert error.code is ErrorCode.TOOL_MCP_ERROR
    assert "third-party detail" not in str(error.content)
    assert empty.content[1] == {"type": "text", "text": "(no output)"}


def test_result_byte_credit_matches_exact_canonical_array_encoding() -> None:
    mapped_blocks = [
        {"type": "text", "text": 'quote " slash \\ snow 雪'},
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": _PNG,
            },
        },
    ]
    exact_bytes = len(canonical_json_bytes(normalize_tool_result(mapped_blocks)))
    source = types.CallToolResult(
        content=[
            types.TextContent(type="text", text='quote " slash \\ snow 雪'),
            types.ImageContent(type="image", data=_PNG, mimeType="image/png"),
        ]
    )

    accepted = map_tool_result(source, max_result_bytes=exact_bytes)
    rejected = map_tool_result(source, max_result_bytes=exact_bytes - 1)

    assert accepted.is_error is False
    assert accepted.content == normalize_tool_result(mapped_blocks)
    assert rejected.code is ErrorCode.TOOL_RESULT_TOO_LARGE


def test_many_small_blocks_do_not_renormalize_the_growing_result(
    monkeypatch,
) -> None:
    normalize_calls = 0
    real_normalize = result_module.normalize_tool_result

    def tracked_normalize(raw):
        nonlocal normalize_calls
        normalize_calls += 1
        return real_normalize(raw)

    monkeypatch.setattr(result_module, "normalize_tool_result", tracked_normalize)
    block_count = 2_000
    result = map_tool_result(
        types.CallToolResult(
            content=[types.TextContent(type="text", text="") for _ in range(block_count)]
        )
    )

    assert result.is_error is False
    assert len(result.content) == block_count + 1
    assert normalize_calls == 1
