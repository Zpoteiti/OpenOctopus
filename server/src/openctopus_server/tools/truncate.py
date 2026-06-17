DEFAULT_MAX_TOOL_RESULT_CHARS: int = 16_000
TRUNCATION_MARKER: str = "\n... (truncated)"


def truncate_head(text: str, max_chars: int = DEFAULT_MAX_TOOL_RESULT_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + TRUNCATION_MARKER
