from __future__ import annotations

import fnmatch
import heapq
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import PurePosixPath
from typing import Any, Literal

import pathspec
import regex  # type: ignore[import-untyped]

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError

MAX_SCAN_OBJECTS = 10_000
MAX_GREP_BYTES = 2 * 1024 * 1024
MAX_REGEX_PATTERN_CHARS = 4_096
MAX_GREP_LINE_CHARS = 16_000
MAX_GREP_RESULT_CHARS = 1_000_000
DEFAULT_REGEX_TIMEOUT_SECONDS = 0.05

NOISE_DIRECTORIES = frozenset(
    {
        ".git",
        "node_modules",
        "__pycache__",
        ".venv",
        "venv",
        "dist",
        "build",
        ".tox",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".coverage",
        "htmlcov",
    }
)

_TYPE_EXTENSIONS: dict[str, frozenset[str]] = {
    "c": frozenset({".c", ".h"}),
    "cpp": frozenset({".cc", ".cpp", ".cxx", ".hh", ".hpp", ".hxx"}),
    "js": frozenset({".js", ".jsx", ".mjs", ".cjs"}),
    "json": frozenset({".json", ".jsonl"}),
    "md": frozenset({".md", ".markdown"}),
    "py": frozenset({".py", ".pyi"}),
    "rs": frozenset({".rs"}),
    "ts": frozenset({".ts", ".tsx", ".mts", ".cts"}),
    "yaml": frozenset({".yaml", ".yml"}),
}


@dataclass(frozen=True)
class SearchObject:
    """Workspace-relative object metadata and optional bounded body."""

    path: str
    size: int
    modified: datetime | None = None
    content: bytes | None = None


@dataclass(frozen=True)
class SearchEntry:
    path: str
    is_directory: bool
    size: int | None
    modified: datetime | None = None


@dataclass(frozen=True)
class GrepContentMatch:
    path: str
    line_number: int
    line: str
    before: tuple[tuple[int, str], ...] = ()
    after: tuple[tuple[int, str], ...] = ()


@dataclass(frozen=True)
class GrepCount:
    path: str
    count: int


@dataclass(frozen=True)
class ResultPage[T]:
    items: tuple[T, ...]
    next_offset: int | None
    truncated: bool


def list_recursive(
    objects: Sequence[SearchObject],
    *,
    root: str = "",
    limit: int = 200,
    offset: int = 0,
    scan_limit: int = MAX_SCAN_OBJECTS,
) -> ResultPage[SearchEntry]:
    """Build a bounded recursive page, synthesizing ordinary-prefix directories."""

    _validate_page(limit=limit, offset=offset, scan_limit=scan_limit)
    root = _normalize_root(root)
    scanned, truncated = _bounded_objects(objects, scan_limit)
    entries = _recursive_entries(scanned, root=root)
    return _sorted_page(
        entries,
        key=lambda entry: entry.path,
        limit=limit,
        offset=offset,
        truncated=truncated,
    )


def find_files(
    objects: Sequence[SearchObject],
    *,
    root: str = "",
    query: str = "",
    glob: str | None = None,
    file_type: str | None = None,
    include_dirs: bool = False,
    sort: Literal["path", "modified"] = "path",
    limit: int = 200,
    offset: int = 0,
    scan_limit: int = MAX_SCAN_OBJECTS,
) -> ResultPage[SearchEntry]:
    """Filter one bounded object scan without retaining all matches."""

    _validate_page(limit=limit, offset=offset, scan_limit=scan_limit)
    root = _normalize_root(root)
    glob_pattern = _validate_glob(glob)
    extensions = _extensions_for(file_type)
    terms = tuple(term.casefold() for term in query.split() if term)
    scanned, truncated = _bounded_objects(objects, scan_limit)

    def matching_entries() -> Iterable[SearchEntry]:
        source = (
            _recursive_entries(scanned, root=root) if include_dirs else _file_entries(scanned, root)
        )
        return (
            entry
            for entry in source
            if _matches_find(
                entry,
                terms=terms,
                glob_pattern=glob_pattern,
                extensions=extensions,
            )
        )

    if sort == "path":
        key: Callable[[SearchEntry], Any] = _path_sort_key
    elif sort == "modified":
        key = _modified_sort_key
    else:
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, "Find sort mode is invalid")
    return _sorted_page(
        matching_entries(),
        key=key,
        limit=limit,
        offset=offset,
        truncated=truncated,
    )


GrepItem = str | GrepCount | GrepContentMatch


def grep_files(
    objects: Sequence[SearchObject],
    *,
    pattern: str,
    root: str = "",
    glob: str | None = None,
    file_type: str | None = None,
    case_insensitive: bool = False,
    fixed_strings: bool = False,
    output_mode: Literal["content", "files_with_matches", "count"] = "files_with_matches",
    context_before: int = 0,
    context_after: int = 0,
    limit: int = 250,
    offset: int = 0,
    scan_limit: int = MAX_SCAN_OBJECTS,
    regex_timeout: float = DEFAULT_REGEX_TIMEOUT_SECONDS,
) -> ResultPage[GrepItem]:
    """Search bounded in-memory object bodies with gitignore and regex timeouts."""

    _validate_page(limit=limit, offset=offset, scan_limit=scan_limit, allow_zero_limit=True)
    if context_before < 0 or context_after < 0:
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, "Grep context must not be negative")
    if output_mode not in {"content", "files_with_matches", "count"}:
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, "Grep output mode is invalid")
    compiled = _compile_pattern(
        pattern,
        case_insensitive=case_insensitive,
        fixed_strings=fixed_strings,
        timeout=regex_timeout,
    )
    root = _normalize_root(root)
    glob_pattern = _validate_glob(glob)
    extensions = _extensions_for(file_type)
    scanned, truncated = _bounded_objects(objects, scan_limit)
    ignore_rules = _GitIgnoreRules()
    effective_limit = scan_limit if limit == 0 else limit
    page_items: list[GrepItem] = []
    matched_results = 0
    retained_chars = 0
    has_more = False
    stopped_early = False

    for item in sorted(scanned, key=lambda candidate: candidate.path):
        relative = _relative_to_root(item.path, root)
        if relative is None or _contains_noise(relative):
            continue
        if ignore_rules.matches(item.path):
            continue
        if PurePosixPath(item.path).name == ".gitignore":
            if item.content is not None and item.size <= MAX_GREP_BYTES:
                ignore_rules.add(item.path, item.content)
            continue
        if not _matches_path_filters(item.path, glob_pattern, extensions):
            continue
        text = _searchable_text(item)
        if text is None:
            continue
        file_matches = _iter_matching_lines(
            text,
            compiled=compiled,
            timeout=regex_timeout,
            path=item.path,
            context_before=context_before,
            context_after=context_after,
        )
        if output_mode == "files_with_matches":
            first_match = next(file_matches, None)
            produced: Iterable[GrepItem] = () if first_match is None else (item.path,)
        elif output_mode == "count":
            match_count = sum(1 for _ in file_matches)
            produced = () if match_count == 0 else (GrepCount(path=item.path, count=match_count),)
        else:
            produced = file_matches

        for result in produced:
            if matched_results < offset:
                matched_results += 1
                continue
            if len(page_items) == effective_limit:
                has_more = True
                stopped_early = True
                break
            result_chars = grep_result_chars(result)
            if retained_chars + result_chars > MAX_GREP_RESULT_CHARS:
                has_more = True
                stopped_early = True
                break
            page_items.append(result)
            retained_chars += result_chars
            matched_results += 1
        if stopped_early:
            break

    reached_scan_ceiling = truncated and not stopped_early
    return ResultPage(
        items=tuple(page_items),
        next_offset=(None if reached_scan_ceiling or not has_more else matched_results),
        truncated=reached_scan_ceiling,
    )


def _recursive_entries(objects: Sequence[SearchObject], *, root: str) -> tuple[SearchEntry, ...]:
    files: list[SearchEntry] = []
    directories: dict[str, datetime | None] = {}
    for item in sorted(objects, key=lambda candidate: candidate.path):
        relative = _relative_to_root(item.path, root)
        if relative is None or not relative or _contains_noise(relative):
            continue
        parts = PurePosixPath(relative).parts
        for index in range(1, len(parts)):
            child = "/".join(parts[:index])
            directory_path = f"{root}/{child}" if root else child
            directories[directory_path] = _latest(directories.get(directory_path), item.modified)
        files.append(
            SearchEntry(
                path=item.path,
                is_directory=False,
                size=item.size,
                modified=item.modified,
            )
        )
    entries = files + [
        SearchEntry(path=path, is_directory=True, size=None, modified=modified)
        for path, modified in directories.items()
    ]
    return tuple(entries)


def _file_entries(objects: Sequence[SearchObject], root: str) -> tuple[SearchEntry, ...]:
    return tuple(
        SearchEntry(
            path=item.path,
            is_directory=False,
            size=item.size,
            modified=item.modified,
        )
        for item in objects
        if (relative := _relative_to_root(item.path, root)) and not _contains_noise(relative)
    )


def _matches_find(
    entry: SearchEntry,
    *,
    terms: tuple[str, ...],
    glob_pattern: str | None,
    extensions: frozenset[str] | None,
) -> bool:
    folded_path = entry.path.casefold()
    if any(term not in folded_path for term in terms):
        return False
    if glob_pattern is not None and not fnmatch.fnmatchcase(entry.path, glob_pattern):
        return False
    if extensions is not None:
        if entry.is_directory or PurePosixPath(entry.path).suffix.casefold() not in extensions:
            return False
    return True


def _matches_path_filters(
    path: str,
    glob_pattern: str | None,
    extensions: frozenset[str] | None,
) -> bool:
    if glob_pattern is not None and not fnmatch.fnmatchcase(path, glob_pattern):
        return False
    return extensions is None or PurePosixPath(path).suffix.casefold() in extensions


def _compile_pattern(
    pattern: str,
    *,
    case_insensitive: bool,
    fixed_strings: bool,
    timeout: float,
) -> regex.Pattern[str]:
    if not pattern or len(pattern) > MAX_REGEX_PATTERN_CHARS or timeout <= 0:
        raise _invalid_regex()
    source = regex.escape(pattern) if fixed_strings else pattern
    flags = regex.IGNORECASE if case_insensitive else 0
    try:
        return regex.compile(source, flags)
    except regex.error as exc:
        raise _invalid_regex() from exc


def _searchable_text(item: SearchObject) -> str | None:
    content = item.content
    if content is None or item.size > MAX_GREP_BYTES or len(content) > MAX_GREP_BYTES:
        return None
    if b"\x00" in content:
        return None
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        return None


@dataclass
class _PendingMatch:
    path: str
    line_number: int
    line: str
    before: tuple[tuple[int, str], ...]
    after: list[tuple[int, str]]
    remaining_after: int

    def complete(self) -> GrepContentMatch:
        return GrepContentMatch(
            path=self.path,
            line_number=self.line_number,
            line=self.line,
            before=self.before,
            after=tuple(self.after),
        )


def _iter_matching_lines(
    text: str,
    *,
    compiled: regex.Pattern[str],
    timeout: float,
    path: str,
    context_before: int,
    context_after: int,
) -> Iterator[GrepContentMatch]:
    before: deque[tuple[int, str]] = deque(maxlen=context_before)
    pending: list[_PendingMatch] = []
    for line_number, raw_line in enumerate(StringIO(text), start=1):
        line = raw_line.rstrip("\r\n")
        context_line = line[:MAX_GREP_LINE_CHARS]

        for match in pending:
            if match.remaining_after > 0:
                match.after.append((line_number, context_line))
                match.remaining_after -= 1

        try:
            matched = len(line) <= MAX_GREP_LINE_CHARS and bool(
                compiled.search(line, timeout=timeout)
            )
        except (TimeoutError, regex.error) as exc:
            raise _invalid_regex() from exc
        if matched:
            match = _PendingMatch(
                path=path,
                line_number=line_number,
                line=line,
                before=tuple(before),
                after=[],
                remaining_after=context_after,
            )
            pending.append(match)
        while pending and pending[0].remaining_after <= 0:
            yield pending.pop(0).complete()
        before.append((line_number, context_line))

    yield from (match.complete() for match in pending)


class _GitIgnoreRules:
    def __init__(self) -> None:
        self._rules: list[tuple[str, pathspec.GitIgnoreSpec]] = []

    def add(self, path: str, content: bytes) -> None:
        if b"\x00" in content:
            return
        base_path = PurePosixPath(path).parent.as_posix()
        base = "" if base_path == "." else base_path
        lines = content.decode("utf-8", errors="replace").splitlines()
        self._rules.append((base, pathspec.GitIgnoreSpec.from_lines(lines)))

    def matches(self, path: str) -> bool:
        ignored = False
        for base, spec in self._rules:
            relative = _relative_to_root(path, base)
            if relative is None:
                continue
            decision = spec.check_file(relative).include
            if decision is not None:
                ignored = decision
        return ignored


def _bounded_objects(
    objects: Sequence[SearchObject],
    scan_limit: int,
) -> tuple[Sequence[SearchObject], bool]:
    return objects[:scan_limit], len(objects) > scan_limit


def _sorted_page[T](
    entries: Iterable[T],
    *,
    key: Callable[[T], Any],
    limit: int,
    offset: int,
    truncated: bool,
) -> ResultPage[T]:
    wanted = offset + limit + 1
    selected = heapq.nsmallest(wanted, entries, key=key)
    return _sequence_page(selected, limit=limit, offset=offset, truncated=truncated)


def _sequence_page[T](
    entries: Sequence[T],
    *,
    limit: int,
    offset: int,
    truncated: bool,
) -> ResultPage[T]:
    items = tuple(entries[offset : offset + limit])
    has_more = len(entries) > offset + limit
    return ResultPage(
        items=items,
        next_offset=None if truncated or not has_more else offset + limit,
        truncated=truncated,
    )


def _validate_page(
    *,
    limit: int,
    offset: int,
    scan_limit: int,
    allow_zero_limit: bool = False,
) -> None:
    minimum = 0 if allow_zero_limit else 1
    if limit < minimum or offset < 0 or scan_limit < 1:
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, "Search pagination is invalid")


def _normalize_root(root: str) -> str:
    normalized = root.strip("/")
    return "" if normalized in {"", "."} else PurePosixPath(normalized).as_posix()


def _relative_to_root(path: str, root: str) -> str | None:
    if not root:
        return path
    if path == root:
        return PurePosixPath(path).name
    prefix = f"{root}/"
    return path.removeprefix(prefix) if path.startswith(prefix) else None


def grep_result_chars(item: GrepItem) -> int:
    if isinstance(item, str):
        return len(item)
    if isinstance(item, GrepCount):
        return len(item.path) + 32
    return (
        len(item.path)
        + len(item.line)
        + sum(len(line) + 16 for _, line in item.before)
        + sum(len(line) + 16 for _, line in item.after)
        + 32
    )


def _contains_noise(relative_path: str) -> bool:
    return any(part in NOISE_DIRECTORIES for part in PurePosixPath(relative_path).parts[:-1])


def _validate_glob(pattern: str | None) -> str | None:
    if pattern is None:
        return None
    depth = 0
    for character in pattern:
        if character == "[":
            depth += 1
        elif character == "]":
            if depth == 0:
                raise _invalid_glob()
            depth -= 1
    if depth:
        raise _invalid_glob()
    return pattern


def _extensions_for(file_type: str | None) -> frozenset[str] | None:
    if file_type is None:
        return None
    normalized = file_type.casefold().lstrip(".")
    return _TYPE_EXTENSIONS.get(normalized, frozenset({f".{normalized}"}))


def _modified_sort_key(entry: SearchEntry) -> tuple[int, float, str]:
    if entry.modified is None:
        return (1, 0.0, entry.path)
    return (0, -entry.modified.timestamp(), entry.path)


def _path_sort_key(entry: SearchEntry) -> tuple[str]:
    return (entry.path,)


def _latest(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _invalid_regex() -> ToolError:
    return ToolError(ErrorCode.TOOL_INVALID_REGEX, "Regular expression is invalid or timed out")


def _invalid_glob() -> ToolError:
    return ToolError(ErrorCode.TOOL_INVALID_GLOB, "Glob pattern is invalid")
