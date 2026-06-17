from openctopus_server.tools.truncate import TRUNCATION_MARKER, truncate_head


def test_truncate_head_noop_when_under_limit():
    assert truncate_head("hello", 10) == "hello"


def test_truncate_head_truncates_with_marker():
    text = "x" * 20000
    result = truncate_head(text, 100)
    assert len(result) == 100 + len(TRUNCATION_MARKER)
    assert result.endswith(TRUNCATION_MARKER)
    assert result.startswith("x" * 100)
