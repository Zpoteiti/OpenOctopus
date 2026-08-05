from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError
from openctopus_server.workspace.search import (
    MAX_GREP_BYTES,
    MAX_GREP_LINE_CHARS,
    MAX_REGEX_PATTERN_CHARS,
    GrepContentMatch,
    GrepCount,
    SearchEntry,
    SearchObject,
    find_files,
    grep_files,
    list_recursive,
)


def _object(
    path: str,
    content: bytes = b"",
    *,
    modified: datetime | None = None,
) -> SearchObject:
    return SearchObject(
        path=path,
        size=len(content),
        modified=modified,
        content=content,
    )


def test_recursive_list_synthesizes_directories_and_skips_noise() -> None:
    objects = [
        _object(".git/config"),
        _object("README.md"),
        _object("node_modules/library/index.js"),
        _object("src/app.py"),
        _object("src/lib/helpers.py"),
    ]

    page = list_recursive(objects, limit=20)

    assert page.items == (
        SearchEntry(path="README.md", is_directory=False, size=0),
        SearchEntry(path="src", is_directory=True, size=None),
        SearchEntry(path="src/app.py", is_directory=False, size=0),
        SearchEntry(path="src/lib", is_directory=True, size=None),
        SearchEntry(path="src/lib/helpers.py", is_directory=False, size=0),
    )
    assert page.next_offset is None
    assert page.truncated is False


def test_recursive_list_pages_with_one_lookahead() -> None:
    objects = [_object(f"src/{name}.py") for name in ("a", "b", "c")]

    page = list_recursive(objects, limit=2, offset=1)

    assert [item.path for item in page.items] == ["src/a.py", "src/b.py"]
    assert page.next_offset == 3
    assert page.truncated is False


def test_scan_ceiling_is_terminal_for_offset_paging() -> None:
    objects = [_object(f"{index:02}.txt") for index in range(5)]

    page = list_recursive(objects, limit=2, scan_limit=3)

    assert [item.path for item in page.items] == ["00.txt", "01.txt"]
    assert page.next_offset is None
    assert page.truncated is True


def test_find_supports_fragment_glob_and_type_filters() -> None:
    objects = [
        _object("docs/api.md"),
        _object("node_modules/api.py"),
        _object("src/api.py"),
        _object("src/api.tsx"),
        _object("src/worker.py"),
    ]

    page = find_files(
        objects,
        query="SRC API",
        glob="*.py",
        file_type="py",
        limit=20,
    )

    assert [item.path for item in page.items] == ["src/api.py"]


def test_find_and_grep_accept_an_exact_file_as_the_search_root() -> None:
    objects = [_object("docs/a.txt", b"needle\n")]

    found = find_files(objects, root="docs/a.txt", limit=20)
    matched = grep_files(
        objects,
        root="docs/a.txt",
        pattern="needle",
        output_mode="files_with_matches",
    )

    assert [item.path for item in found.items] == ["docs/a.txt"]
    assert matched.items == ("docs/a.txt",)


def test_find_can_include_synthesized_directories_and_page_by_path() -> None:
    objects = [_object("src/app.py"), _object("src/lib/helpers.py")]

    page = find_files(objects, include_dirs=True, limit=2, offset=1)

    assert [(item.path, item.is_directory) for item in page.items] == [
        ("src/app.py", False),
        ("src/lib", True),
    ]
    assert page.next_offset == 3


def test_find_modified_sort_is_newest_first_with_stable_path_tiebreak() -> None:
    now = datetime.now(UTC)
    objects = [
        _object("old.py", modified=now - timedelta(days=1)),
        _object("z-new.py", modified=now),
        _object("a-new.py", modified=now),
    ]

    page = find_files(objects, sort="modified", limit=20)

    assert [item.path for item in page.items] == ["a-new.py", "z-new.py", "old.py"]


def test_invalid_glob_is_a_stable_tool_error() -> None:
    with pytest.raises(ToolError) as caught:
        find_files([_object("a.py")], glob="[broken", limit=20)

    assert caught.value.code is ErrorCode.TOOL_INVALID_GLOB


def test_grep_skips_noise_gitignored_binary_and_oversized_objects() -> None:
    objects = [
        _object(".gitignore", b"ignored/\n*.log\n!keep.log\n"),
        _object("binary.txt", b"needle\x00binary"),
        SearchObject(
            path="huge.txt",
            size=MAX_GREP_BYTES + 1,
            modified=None,
            content=b"needle",
        ),
        _object("ignored/secret.txt", b"needle\n"),
        _object("keep.log", b"needle\n"),
        _object("node_modules/package.txt", b"needle\n"),
        _object("server.log", b"needle\n"),
        _object("src/app.py", b"needle\n"),
    ]

    page = grep_files(objects, pattern="needle", output_mode="files_with_matches")

    assert page.items == ("keep.log", "src/app.py")


def test_nested_gitignore_rules_are_relative_and_can_be_overridden() -> None:
    objects = [
        _object("project/.gitignore", b"generated/\n*.tmp\n"),
        _object("project/generated/result.txt", b"needle\n"),
        _object("project/keep.tmp", b"needle\n"),
        _object("project/src/.gitignore", b"!keep.tmp\n"),
        _object("project/src/keep.tmp", b"needle\n"),
    ]

    page = grep_files(objects, pattern="needle", output_mode="files_with_matches")

    assert page.items == ("project/src/keep.tmp",)


def test_grep_result_modes_and_content_context() -> None:
    objects = [
        _object("a.txt", b"before\nneedle one\nafter\nneedle two\n"),
        _object("b.txt", b"needle\n"),
    ]

    counts = grep_files(objects, pattern="needle", output_mode="count")
    content = grep_files(
        objects,
        pattern="needle",
        output_mode="content",
        context_before=1,
        context_after=1,
    )

    assert counts.items == (GrepCount(path="a.txt", count=2), GrepCount(path="b.txt", count=1))
    assert content.items[0] == GrepContentMatch(
        path="a.txt",
        line_number=2,
        line="needle one",
        before=((1, "before"),),
        after=((3, "after"),),
    )
    assert len(content.items) == 3


def test_grep_content_has_an_aggregate_result_memory_bound() -> None:
    objects = [
        _object(f"{index:03}.txt", ("needle" + "x" * 14_900).encode()) for index in range(100)
    ]

    page = grep_files(objects, pattern="needle", output_mode="content", limit=1000)

    assert 0 < len(page.items) < len(objects)
    assert page.next_offset == len(page.items)


def test_grep_fixed_strings_case_insensitive_and_result_paging() -> None:
    objects = [
        _object("a.txt", b"literal [VALUE]\n"),
        _object("b.txt", b"literal [value]\n"),
        _object("c.txt", b"literal value\n"),
    ]

    page = grep_files(
        objects,
        pattern="[value]",
        fixed_strings=True,
        case_insensitive=True,
        output_mode="files_with_matches",
        limit=1,
        offset=1,
    )

    assert page.items == ("b.txt",)
    assert page.next_offset is None
    assert page.truncated is False


def test_grep_scan_ceiling_is_terminal_and_overlong_lines_are_skipped() -> None:
    objects = [
        _object("a.txt", ("x" * (MAX_GREP_LINE_CHARS + 1) + " needle").encode()),
        _object("b.txt", b"needle\n"),
        _object("c.txt", b"needle\n"),
    ]

    page = grep_files(
        objects,
        pattern="needle",
        output_mode="files_with_matches",
        limit=10,
        scan_limit=2,
    )

    assert page.items == ("b.txt",)
    assert page.next_offset is None
    assert page.truncated is True


def test_invalid_and_timed_out_regexes_use_the_same_stable_error() -> None:
    with pytest.raises(ToolError) as invalid:
        grep_files([_object("a.txt", b"text")], pattern="(")

    with pytest.raises(ToolError) as timed_out:
        grep_files(
            [_object("a.txt", ("a" * 15_000 + "!").encode())],
            pattern="(a+)+$",
            regex_timeout=0.000_001,
        )

    with pytest.raises(ToolError) as too_long:
        grep_files([_object("a.txt", b"text")], pattern="x" * (MAX_REGEX_PATTERN_CHARS + 1))

    assert invalid.value.code is ErrorCode.TOOL_INVALID_REGEX
    assert timed_out.value.code is ErrorCode.TOOL_INVALID_REGEX
    assert too_long.value.code is ErrorCode.TOOL_INVALID_REGEX
