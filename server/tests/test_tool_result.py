from openctopus_server.tools.result import (
    UNTRUSTED_TOOL_RESULT_WARNING,
    normalize_tool_result,
)
from openctopus_server.tools.truncate import TRUNCATION_MARKER


def test_normalize_string_prepends_warning() -> None:
    assert normalize_tool_result("hello") == [
        {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING},
        {"type": "text", "text": "hello"},
    ]


def test_normalize_safe_blocks_preserves_image_bytes_and_does_not_mutate_input() -> None:
    raw = [
        {"type": "text", "text": "caption"},
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": "aW1hZ2U=",
            },
        },
    ]

    normalized = normalize_tool_result(raw)
    normalized[2]["source"]["data"] = "changed"

    assert normalized[0] == {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING}
    assert raw[1]["source"]["data"] == "aW1hZ2U="


def test_normalize_truncates_raw_text_before_wrapping() -> None:
    normalized = normalize_tool_result("abcdef", max_chars=3)

    assert normalized[1] == {"type": "text", "text": f"abc{TRUNCATION_MARKER}"}
