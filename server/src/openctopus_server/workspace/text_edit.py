from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from io import StringIO

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError


class MatchLevel(StrEnum):
    EXACT = "exact"
    TRIMMED = "trimmed"
    NORMALIZED = "normalized"


@dataclass(frozen=True, slots=True)
class TextEditResult:
    text: str
    replacements: int
    match_level: MatchLevel | None
    created: bool = False


@dataclass(frozen=True, slots=True)
class _Match:
    start: int
    end: int
    line: int


MAX_FUZZY_MATCH_CANDIDATES = 1_000
MAX_TEXT_EDIT_BYTES = 8 * 1024 * 1024


_QUOTE_NORMALIZATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
    }
)


def apply_text_edit(
    text: str | None,
    *,
    old_text: str,
    new_text: str,
    replace_all: bool = False,
    occurrence: int | None = None,
    line_hint: int | None = None,
    expected_replacements: int | None = None,
) -> TextEditResult:
    _validate_selectors(
        replace_all=replace_all,
        occurrence=occurrence,
        line_hint=line_hint,
        expected_replacements=expected_replacements,
    )

    if text is None:
        if old_text:
            raise ToolError(ErrorCode.TOOL_NO_MATCH, "Text to replace was not found")
        _check_expected(expected_replacements, 0)
        _ensure_output_size(_utf8_size(new_text))
        return TextEditResult(new_text, 0, None, created=True)
    if not old_text:
        raise ToolError(
            ErrorCode.TOOL_INVALID_ARGS,
            "old_text may be empty only when creating a missing file",
        )

    line_ending = "\r\n" if "\r\n" in text else "\n"
    normalized_text = _normalize_line_endings(text)
    normalized_old = _normalize_line_endings(old_text)
    normalized_new = _normalize_line_endings(new_text)
    restore_crlf = line_ending == "\r\n"
    current_size = _rendered_utf8_size(normalized_text, restore_crlf=restore_crlf)

    exact_result = _apply_exact_edit(
        normalized_text,
        old_text=normalized_old,
        new_text=normalized_new,
        replace_all=replace_all,
        occurrence=occurrence,
        line_hint=line_hint,
        expected_replacements=expected_replacements,
        current_size=current_size,
        restore_crlf=restore_crlf,
    )
    if exact_result is not None:
        if line_ending == "\r\n":
            return TextEditResult(
                exact_result.text.replace("\n", "\r\n"),
                exact_result.replacements,
                exact_result.match_level,
            )
        return exact_result

    matches = _trimmed_matches(normalized_text, normalized_old)
    level = MatchLevel.TRIMMED
    if not matches:
        matches = _normalized_matches(normalized_text, normalized_old)
        level = MatchLevel.NORMALIZED
    if not matches:
        raise ToolError(ErrorCode.TOOL_NO_MATCH, "Text to replace was not found")

    selected = _select_matches(
        matches,
        replace_all=replace_all,
        occurrence=occurrence,
        line_hint=line_hint,
    )
    _check_expected(expected_replacements, len(selected))

    edited = _apply_fuzzy_replacements(
        normalized_text,
        old_text=normalized_old,
        new_text=normalized_new,
        selected=selected,
        level=level,
        current_size=current_size,
        restore_crlf=restore_crlf,
    )

    if line_ending == "\r\n":
        edited = edited.replace("\n", "\r\n")
    return TextEditResult(edited, len(selected), level)


def _validate_selectors(
    *,
    replace_all: bool,
    occurrence: int | None,
    line_hint: int | None,
    expected_replacements: int | None,
) -> None:
    if occurrence is not None and occurrence < 1:
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, "occurrence must be at least 1")
    if line_hint is not None and line_hint < 1:
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, "line_hint must be at least 1")
    if expected_replacements is not None and expected_replacements < 1:
        raise ToolError(
            ErrorCode.TOOL_INVALID_ARGS,
            "expected_replacements must be at least 1",
        )
    if replace_all and occurrence is not None:
        raise ToolError(
            ErrorCode.TOOL_INVALID_ARGS,
            "occurrence cannot be used with replace_all",
        )
    if replace_all and line_hint is not None:
        raise ToolError(
            ErrorCode.TOOL_INVALID_ARGS,
            "line_hint cannot be used with replace_all",
        )
    if occurrence is not None and line_hint is not None:
        raise ToolError(
            ErrorCode.TOOL_INVALID_ARGS,
            "line_hint cannot be used with occurrence",
        )


def _normalize_line_endings(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _apply_exact_edit(
    text: str,
    *,
    old_text: str,
    new_text: str,
    replace_all: bool,
    occurrence: int | None,
    line_hint: int | None,
    expected_replacements: int | None,
    current_size: int,
    restore_crlf: bool,
) -> TextEditResult | None:
    count = text.count(old_text)
    if count == 0:
        return None
    if replace_all:
        _check_expected(expected_replacements, count)
        projected_size = current_size + count * (
            _rendered_utf8_size(new_text, restore_crlf=restore_crlf)
            - _rendered_utf8_size(old_text, restore_crlf=restore_crlf)
        )
        _ensure_output_size(projected_size)
        return TextEditResult(
            text.replace(old_text, new_text),
            count,
            MatchLevel.EXACT,
        )
    if occurrence is not None:
        if occurrence > count:
            raise ToolError(ErrorCode.TOOL_NO_MATCH, "Requested occurrence was not found")
        start = _exact_occurrence_start(text, old_text, occurrence)
    elif line_hint is not None:
        start = _nearest_exact_start(text, old_text, line_hint)
    else:
        if count > 1:
            raise ToolError(
                ErrorCode.TOOL_AMBIGUOUS_EDIT,
                "Text to replace appears more than once",
            )
        start = text.find(old_text)
    _check_expected(expected_replacements, 1)
    end = start + len(old_text)
    projected_size = (
        current_size
        - _rendered_utf8_size(old_text, restore_crlf=restore_crlf)
        + _rendered_utf8_size(new_text, restore_crlf=restore_crlf)
    )
    _ensure_output_size(projected_size)
    return TextEditResult(
        f"{text[:start]}{new_text}{text[end:]}",
        1,
        MatchLevel.EXACT,
    )


def _apply_fuzzy_replacements(
    text: str,
    *,
    old_text: str,
    new_text: str,
    selected: list[_Match],
    level: MatchLevel,
    current_size: int,
    restore_crlf: bool,
) -> str:
    pieces = [text]
    for match in reversed(selected):
        matched_text = text[match.start : match.end]
        removed_size = _pieces_range_size(
            pieces,
            match.start,
            match.end,
            restore_crlf=restore_crlf,
        )
        if level is MatchLevel.TRIMMED:
            replacement_size = _indented_replacement_size(
                old_text,
                new_text,
                matched_text,
                restore_crlf=restore_crlf,
            )
        else:
            replacement_size = _quote_style_replacement_size(
                new_text,
                matched_text,
                restore_crlf=restore_crlf,
            )
        projected_size = current_size - removed_size + replacement_size
        _ensure_output_size(projected_size)
        if level is MatchLevel.TRIMMED:
            replacement = _apply_target_indentation(old_text, new_text, matched_text)
        else:
            replacement = _apply_target_quote_style(new_text, matched_text)
        pieces = _splice_pieces(pieces, match.start, match.end, replacement)
        current_size = projected_size
    return "".join(pieces)


def _splice_pieces(pieces: list[str], start: int, end: int, replacement: str) -> list[str]:
    prefix: list[str] = []
    suffix: list[str] = []
    position = 0
    for piece in pieces:
        piece_end = position + len(piece)
        if piece_end <= start:
            prefix.append(piece)
        elif position < start:
            prefix.append(piece[: start - position])
        if position >= end:
            suffix.append(piece)
        elif piece_end > end:
            suffix.append(piece[end - position :])
        position = piece_end
    return [*prefix, replacement, *suffix]


def _pieces_range_size(
    pieces: list[str],
    start: int,
    end: int,
    *,
    restore_crlf: bool,
) -> int:
    total = 0
    position = 0
    for piece in pieces:
        piece_end = position + len(piece)
        overlap_start = max(start, position)
        overlap_end = min(end, piece_end)
        if overlap_start < overlap_end:
            total += _utf8_size_range(
                piece,
                overlap_start - position,
                overlap_end - position,
                restore_crlf=restore_crlf,
            )
        position = piece_end
    return total


def _ensure_output_size(size: int) -> None:
    if size > MAX_TEXT_EDIT_BYTES:
        raise ToolError(
            ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT,
            "Workspace file exceeds the 8 MiB server edit limit",
        )


def _rendered_utf8_size(value: str, *, restore_crlf: bool) -> int:
    size = _utf8_size(value)
    if restore_crlf:
        size += value.count("\n")
    return size


def _utf8_size(value: str) -> int:
    if value.isascii():
        return len(value)
    return sum(_utf8_character_size(character) for character in value)


def _utf8_size_range(
    value: str,
    start: int,
    end: int,
    *,
    restore_crlf: bool,
) -> int:
    total = 0
    for index in range(start, end):
        character = value[index]
        total += _utf8_character_size(character)
        if restore_crlf and character == "\n":
            total += 1
    return total


def _utf8_character_size(character: str) -> int:
    codepoint = ord(character)
    if codepoint <= 0x7F:
        return 1
    if codepoint <= 0x7FF:
        return 2
    if codepoint <= 0xFFFF:
        return 3
    return 4


def _exact_occurrence_start(text: str, needle: str, occurrence: int) -> int:
    search_from = 0
    found = -1
    for _ in range(occurrence):
        found = text.find(needle, search_from)
        search_from = found + len(needle)
    return found


def _nearest_exact_start(text: str, needle: str, line_hint: int) -> int:
    search_from = 0
    line_cursor = 0
    line = 1
    best_start = -1
    best_distance: int | None = None
    tied = False
    while (found := text.find(needle, search_from)) >= 0:
        line += text.count("\n", line_cursor, found)
        distance = abs(line - line_hint)
        if best_distance is None or distance < best_distance:
            best_start = found
            best_distance = distance
            tied = False
        elif distance == best_distance:
            tied = True
        line_cursor = found
        search_from = found + len(needle)
    if tied:
        raise ToolError(
            ErrorCode.TOOL_AMBIGUOUS_EDIT,
            "line_hint is equally close to multiple matches",
        )
    return best_start


def _trimmed_matches(text: str, needle: str) -> list[_Match]:
    old_lines = needle.splitlines()
    if not old_lines:
        return []

    count = len(old_lines)
    wanted = [line.strip() for line in old_lines]
    prefix = _kmp_prefix(wanted)
    window: deque[tuple[int, int, str, int]] = deque(maxlen=count)

    def candidates() -> Iterator[_Match]:
        matched = 0
        for line_number, (start, end, body) in enumerate(_line_records(text), start=1):
            token = body.strip()
            window.append((start, end, token, line_number))
            while matched > 0 and not _tokens_equal(wanted[matched], token):
                matched = prefix[matched - 1]
            if _tokens_equal(wanted[matched], token):
                matched += 1
            if matched == count:
                yield _Match(window[0][0], window[-1][1], window[0][3])
                matched = prefix[matched - 1]

    return _bounded_fuzzy_matches(candidates())


def _kmp_prefix(tokens: list[str]) -> list[int]:
    prefix = [0] * len(tokens)
    matched = 0
    for index in range(1, len(tokens)):
        while matched > 0 and not _tokens_equal(tokens[matched], tokens[index]):
            matched = prefix[matched - 1]
        if _tokens_equal(tokens[matched], tokens[index]):
            matched += 1
        prefix[index] = matched
    return prefix


def _tokens_equal(left: str, right: str) -> bool:
    return left == right


def _line_records(text: str) -> Iterator[tuple[int, int, str]]:
    start = 0
    found_line = False
    for index, character in enumerate(text):
        if character not in "\n\v\f\x1c\x1d\x1e\x85\u2028\u2029":
            continue
        end = index if character == "\n" else index + 1
        yield start, end, text[start:end]
        found_line = True
        start = index + 1
    if start < len(text) or not found_line:
        yield start, len(text), text[start:]


def _normalized_matches(text: str, needle: str) -> list[_Match]:
    normalized_text = text.translate(_QUOTE_NORMALIZATION)
    normalized_needle = needle.translate(_QUOTE_NORMALIZATION)
    return _bounded_fuzzy_matches(_iter_substring_matches(normalized_text, normalized_needle))


def _iter_substring_matches(text: str, needle: str) -> Iterator[_Match]:
    search_from = 0
    line_cursor = 0
    line = 1
    while (found := text.find(needle, search_from)) >= 0:
        line += text.count("\n", line_cursor, found)
        yield _Match(found, found + len(needle), line)
        line_cursor = found
        search_from = found + len(needle)


def _bounded_fuzzy_matches(matches: Iterator[_Match]) -> list[_Match]:
    bounded: list[_Match] = []
    for match in matches:
        if len(bounded) == MAX_FUZZY_MATCH_CANDIDATES:
            raise ToolError(
                ErrorCode.TOOL_AMBIGUOUS_EDIT,
                f"Fuzzy match candidate limit ({MAX_FUZZY_MATCH_CANDIDATES}) exceeded",
            )
        bounded.append(match)
    return bounded


def _select_matches(
    matches: list[_Match],
    *,
    replace_all: bool,
    occurrence: int | None,
    line_hint: int | None,
) -> list[_Match]:
    if replace_all:
        return matches
    if occurrence is not None:
        if occurrence > len(matches):
            raise ToolError(ErrorCode.TOOL_NO_MATCH, "Requested occurrence was not found")
        return [matches[occurrence - 1]]
    if line_hint is not None:
        nearest_distance = min(abs(match.line - line_hint) for match in matches)
        nearest = [match for match in matches if abs(match.line - line_hint) == nearest_distance]
        if len(nearest) == 1:
            return nearest
        raise ToolError(
            ErrorCode.TOOL_AMBIGUOUS_EDIT,
            "line_hint is equally close to multiple matches",
        )
    if len(matches) > 1:
        raise ToolError(
            ErrorCode.TOOL_AMBIGUOUS_EDIT,
            "Text to replace appears more than once",
        )
    return matches


def _check_expected(expected: int | None, actual: int) -> None:
    if expected is not None and expected != actual:
        raise ToolError(
            ErrorCode.TOOL_INVALID_ARGS,
            f"expected_replacements was {expected}, but the edit would replace {actual}",
        )


def _leading_whitespace(value: str) -> str:
    return value[: len(value) - len(value.lstrip())]


def _base_indentation(value: str) -> str:
    smallest: str | None = None
    for line in _splitline_values(value):
        if not line.strip():
            continue
        indent = _leading_whitespace(line)
        if smallest is None or len(indent) < len(smallest):
            smallest = indent
    return smallest or ""


def _apply_target_indentation(old_text: str, new_text: str, matched_text: str) -> str:
    old_indent = _base_indentation(old_text)
    target_indent = _base_indentation(matched_text)
    rendered = StringIO()
    for index, line in enumerate(_newline_values(new_text)):
        if index:
            rendered.write("\n")
        if not line:
            continue
        rendered.write(target_indent)
        if old_indent and line.startswith(old_indent):
            rendered.write(line[len(old_indent) :])
        else:
            rendered.write(line)
    return rendered.getvalue()


def _indented_replacement_size(
    old_text: str,
    new_text: str,
    matched_text: str,
    *,
    restore_crlf: bool,
) -> int:
    old_indent = _base_indentation(old_text)
    target_indent_size = _utf8_size(_base_indentation(matched_text))
    total = 0
    for index, line in enumerate(_newline_values(new_text)):
        if index:
            total += 2 if restore_crlf else 1
        if not line:
            continue
        total += target_indent_size
        if old_indent and line.startswith(old_indent):
            total += _utf8_size_range(
                line,
                len(old_indent),
                len(line),
                restore_crlf=False,
            )
        else:
            total += _utf8_size(line)
    return total


def _apply_target_quote_style(new_text: str, matched_text: str) -> str:
    double_styles, single_styles = _quote_styles(matched_text)
    return _render_quote_style(new_text, double_styles, single_styles)


def _quote_style_replacement_size(
    new_text: str,
    matched_text: str,
    *,
    restore_crlf: bool,
) -> int:
    double_styles, single_styles = _quote_styles(matched_text)
    double_index = 0
    single_index = 0
    total = 0
    for character in new_text:
        if character == '"' and double_styles:
            rendered = double_styles[min(double_index, len(double_styles) - 1)]
            double_index += 1
        elif character == "'" and single_styles:
            rendered = single_styles[min(single_index, len(single_styles) - 1)]
            single_index += 1
        else:
            rendered = character
        total += _utf8_character_size(rendered)
        if restore_crlf and rendered == "\n":
            total += 1
    return total


def _quote_styles(matched_text: str) -> tuple[str, str]:
    double_styles = StringIO()
    single_styles = StringIO()
    for character in matched_text:
        if character in '\u201c\u201d"':
            double_styles.write(character)
        elif character in "\u2018\u2019'":
            single_styles.write(character)
    return double_styles.getvalue(), single_styles.getvalue()


def _render_quote_style(new_text: str, double_styles: str, single_styles: str) -> str:
    double_index = 0
    single_index = 0
    rendered = StringIO()
    for character in new_text:
        if character == '"' and double_styles:
            rendered.write(double_styles[min(double_index, len(double_styles) - 1)])
            double_index += 1
        elif character == "'" and single_styles:
            rendered.write(single_styles[min(single_index, len(single_styles) - 1)])
            single_index += 1
        else:
            rendered.write(character)
    return rendered.getvalue()


def _newline_values(value: str) -> Iterator[str]:
    start = 0
    while (end := value.find("\n", start)) >= 0:
        yield value[start:end]
        start = end + 1
    yield value[start:]


def _splitline_values(value: str) -> Iterator[str]:
    start = 0
    for index, character in enumerate(value):
        if character not in "\n\v\f\x1c\x1d\x1e\x85\u2028\u2029":
            continue
        yield value[start:index]
        start = index + 1
    if start < len(value):
        yield value[start:]
