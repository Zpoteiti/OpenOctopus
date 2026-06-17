import json

import pydantic
import pytest

from openctopus_server.provider.wire_types import (
    ContentBlock,
    Effort,
    ImageBlock,
    TextBlock,
    ToolResultBlock,
)


def test_effort_enum_values():
    assert Effort.LOW == "low"
    assert Effort.MAX == "max"


def test_text_block_round_trip():
    block = TextBlock(text="hello")
    data = json.loads(block.model_dump_json())
    parsed = pydantic.TypeAdapter(ContentBlock).validate_python(data)
    assert isinstance(parsed, TextBlock)
    assert parsed.text == "hello"


def test_image_block_round_trip():
    block = ImageBlock(
        source={"type": "base64", "media_type": "image/png", "data": "abc123"}
    )
    data = json.loads(block.model_dump_json())
    parsed = pydantic.TypeAdapter(ContentBlock).validate_python(data)
    assert isinstance(parsed, ImageBlock)
    assert parsed.source.data == "abc123"


def test_tool_result_block_accepts_string_or_list():
    block_str = ToolResultBlock(tool_use_id="1", content="plain text")
    assert block_str.content == "plain text"
    block_list = ToolResultBlock(tool_use_id="1", content=[{"type": "text", "text": "hi"}])
    assert block_list.content[0].text == "hi"


def test_content_block_discriminator_rejects_unknown_type():
    with pytest.raises(pydantic.ValidationError):
        pydantic.TypeAdapter(ContentBlock).validate_python({"type": "unknown"})
