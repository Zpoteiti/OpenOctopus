from __future__ import annotations

import errno
import math
import ntpath
import os
import re
import signal
import sys
from collections.abc import Sequence
from importlib import import_module
from io import BytesIO
from multiprocessing.connection import Connection
from pathlib import PurePosixPath
from typing import Any, Literal, cast
from urllib.parse import urljoin
from zipfile import BadZipFile, ZipFile, ZipInfo

PROTOCOL_VERSION = 1
MAX_CONVERTED_CHARS = 128_000
MAX_OOXML_MEMBERS = 10_000
MAX_OOXML_MEMBER_BYTES = 128 * 1024 * 1024
MAX_OOXML_TOTAL_BYTES = 256 * 1024 * 1024
MAX_OOXML_COMPRESSION_RATIO = 100
MIN_RATIO_CHECK_BYTES = 1024 * 1024
_TRUNCATION_MARKER = "\n[truncated]"
_TOOL_TRUNCATION_MARKER = "\n... (truncated)"
_SKIP_HTML_TAGS = ("script", "style", "noscript", "template", "svg")
_BLOCK_HTML_TAGS = (
    "article",
    "aside",
    "blockquote",
    "div",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tr",
    "ul",
)

type WorkerRequest = dict[str, object]

_NATIVE_THREAD_ENV = (
    "BLIS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)


class WorkerInputError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def run_conversion_worker(connection: Connection, request: WorkerRequest) -> None:
    try:
        memory_mb = _required_int(request, "memory_mb")
        timeout_seconds = _required_float(request, "timeout_seconds")
        _configure_worker_environment()
        _apply_resource_limits(memory_mb=memory_mb, timeout_seconds=timeout_seconds)
        operation = request.get("operation")
        if operation == "document":
            result = _convert_document(request)
        elif operation == "html":
            result = _convert_html(request)
        elif operation == "probe":
            from openctopus_server.workspace.markitdown_adapter import probe_dependencies

            probe_dependencies()
            result = "ok"
        else:
            raise WorkerInputError(
                "tool_content_conversion_failed",
                "Content conversion request was invalid",
            )
        _send(connection, True, "", result)
    except WorkerInputError as exc:
        _send(connection, False, exc.code, exc.message)
    except Exception as exc:
        if _is_resource_error(exc):
            _send(
                connection,
                False,
                "tool_content_conversion_resource_exceeded",
                "Content conversion exceeded the configured memory limit",
            )
        else:
            _send(
                connection,
                False,
                "tool_content_conversion_failed",
                "Content conversion failed",
            )
    finally:
        connection.close()


def _configure_worker_environment() -> None:
    for name in _NATIVE_THREAD_ENV:
        os.environ[name] = "1"


def _convert_document(request: WorkerRequest) -> str:
    path = _required_str(request, "path")
    data = _required_bytes(request, "data")
    pages = request.get("pages")
    if pages is not None and not isinstance(pages, str):
        raise WorkerInputError("tool_invalid_args", "PDF pages must be a page number or range")
    suffix = PurePosixPath(path).suffix.lower()
    if suffix not in {".pdf", ".docx", ".xlsx", ".pptx"}:
        raise WorkerInputError(
            "tool_content_conversion_failed",
            "Workspace document format is not supported",
        )

    selected_start = selected_end = total_pages = 0
    if suffix == ".pdf":
        data, selected_start, selected_end, total_pages = _prepare_pdf(data, pages)
    else:
        _preflight_ooxml(data)

    from openctopus_server.workspace.markitdown_adapter import convert_bytes

    conversion_format = cast(Literal["pdf", "docx", "xlsx", "pptx"], suffix[1:])
    rendered = convert_bytes(data, conversion_format)
    if suffix == ".pdf":
        return _render_pdf_markdown(rendered, selected_start, selected_end, total_pages)
    return _cap(rendered, MAX_CONVERTED_CHARS, _TRUNCATION_MARKER)


def _convert_html(request: WorkerRequest) -> str:
    data = _required_bytes(request, "data")
    charset = _required_str(request, "charset")
    base_url = _required_str(request, "base_url")
    mode = request.get("mode")
    if mode not in {"markdown", "text"}:
        raise WorkerInputError("tool_invalid_args", "HTML extraction mode is invalid")
    max_chars = _required_int(request, "max_chars")
    if not 1 <= max_chars <= MAX_CONVERTED_CHARS:
        raise WorkerInputError("tool_invalid_args", "HTML output limit is invalid")
    normalized, plain_text = _preprocess_html(data, charset=charset, base_url=base_url)
    if mode == "text":
        rendered = plain_text
    else:
        from openctopus_server.workspace.markitdown_adapter import convert_bytes

        rendered = convert_bytes(normalized, "html")
    return _cap(rendered, max_chars, _TOOL_TRUNCATION_MARKER, marker_inside_limit=False)


def _apply_resource_limits(*, memory_mb: int, timeout_seconds: float) -> None:
    import resource

    if sys.platform != "linux":
        raise RuntimeError("Content conversion requires Linux resource limits")
    if memory_mb <= 0 or timeout_seconds <= 0:
        raise ValueError("conversion resource limits must be positive")
    address_space_bytes = memory_mb * 1024 * 1024
    cpu_soft_seconds = max(1, math.ceil(timeout_seconds))
    resource.setrlimit(resource.RLIMIT_AS, (address_space_bytes, address_space_bytes))
    resource.setrlimit(resource.RLIMIT_CPU, (cpu_soft_seconds, cpu_soft_seconds + 1))


def _preflight_ooxml(data: bytes) -> None:
    try:
        with ZipFile(BytesIO(data)) as archive:
            members = archive.infolist()
    except (BadZipFile, OSError) as exc:
        raise WorkerInputError(
            "tool_content_conversion_failed",
            "Workspace document is not a valid OOXML file",
        ) from exc

    _validate_ooxml_members(members)


def _validate_ooxml_members(members: Sequence[ZipInfo]) -> None:
    if len(members) > MAX_OOXML_MEMBERS:
        raise _unsafe_ooxml()
    total_size = 0
    for member in members:
        name = member.orig_filename
        normalized_name = name.replace("\\", "/")
        path = PurePosixPath(normalized_name)
        drive, _ = ntpath.splitdrive(name)
        if (
            not name
            or "\x00" in name
            or drive
            or name.startswith(("/", "\\"))
            or path.is_absolute()
            or ".." in path.parts
        ):
            raise _unsafe_ooxml()
        if member.flag_bits & 0x1:
            raise _unsafe_ooxml()
        if member.file_size < 0 or member.compress_size < 0:
            raise _unsafe_ooxml()
        if member.file_size > MAX_OOXML_MEMBER_BYTES:
            raise _unsafe_ooxml()
        total_size += member.file_size
        if total_size > MAX_OOXML_TOTAL_BYTES:
            raise _unsafe_ooxml()
        if (
            member.file_size >= MIN_RATIO_CHECK_BYTES
            and member.file_size / max(1, member.compress_size) > MAX_OOXML_COMPRESSION_RATIO
        ):
            raise _unsafe_ooxml()


def _prepare_pdf(data: bytes, pages: str | None) -> tuple[bytes, int, int, int]:
    try:
        from pypdf import PdfReader, PdfWriter

        reader = PdfReader(BytesIO(data))
        if reader.is_encrypted:
            raise WorkerInputError(
                "tool_content_conversion_failed",
                "Encrypted PDF documents are not supported",
            )
        total = len(reader.pages)
        start, end = _parse_page_range(pages, total)
        writer = PdfWriter()
        for index in range(start - 1, end):
            writer.add_page(reader.pages[index])
        output = BytesIO()
        writer.write(output)
        return output.getvalue(), start, end, total
    except WorkerInputError:
        raise
    except (MemoryError, OSError):
        raise
    except Exception as exc:
        raise WorkerInputError(
            "tool_content_conversion_failed",
            "Workspace PDF could not be parsed",
        ) from exc


def _parse_page_range(value: str | None, total: int) -> tuple[int, int]:
    if total < 1:
        raise WorkerInputError(
            "tool_content_conversion_failed",
            "Workspace PDF does not contain any pages",
        )
    if value is None:
        return 1, min(20, total)
    if re.fullmatch(r"[1-9]\d*(?:-[1-9]\d*)?", value) is None:
        raise WorkerInputError(
            "tool_invalid_args",
            "PDF pages must be a page number or range such as 1 or 1-5",
        )
    start_text, separator, end_text = value.partition("-")
    start = int(start_text)
    end = int(end_text) if separator else start
    if end < start or end - start + 1 > 20 or end > total:
        raise WorkerInputError(
            "tool_invalid_args",
            "PDF page range is invalid or exceeds 20 pages",
        )
    return start, end


def _render_pdf_markdown(markdown: str, start: int, end: int, total: int) -> str:
    header = f"[PDF pages {start}-{end} of {total}]"
    footer = ""
    if end < total:
        next_end = min(total, end + 20)
        footer = f'[More pages available. Call read_file with pages="{end + 1}-{next_end}".]'
    body = markdown.strip()
    if not body:
        body = "No extractable text was found. OCR is not enabled."

    separators = 2 + (2 if footer else 0)
    body_budget = MAX_CONVERTED_CHARS - len(header) - len(footer) - separators
    body = _cap(body, body_budget, _TRUNCATION_MARKER)
    parts = [header, body]
    if footer:
        parts.append(footer)
    return "\n\n".join(parts)


def _preprocess_html(data: bytes, *, charset: str, base_url: str) -> tuple[bytes, str]:
    try:
        source = data.decode(charset, errors="replace")
    except LookupError:
        source = data.decode("utf-8", errors="replace")
    beautiful_soup = import_module("bs4").BeautifulSoup
    soup = beautiful_soup(source, "html.parser")
    for element in soup.find_all(_SKIP_HTML_TAGS):
        element.decompose()
    for image in soup.find_all("img"):
        source_values = (image.get("src"), image.get("data-src"))
        if any(
            isinstance(value, str) and value.lstrip().lower().startswith("data:")
            for value in source_values
        ):
            image.decompose()
    for element in soup.find_all(True):
        for attribute in ("href", "src", "data-src"):
            value = element.get(attribute)
            if isinstance(value, str) and value:
                element[attribute] = urljoin(base_url, value)
    normalized = str(soup).encode("utf-8")
    body = soup.body or soup
    plain_text = _extract_plain_text(body)
    return normalized, plain_text


def _extract_plain_text(body: Any) -> str:
    for line_break in body.find_all("br"):
        line_break.replace_with("\n")
    for element in body.find_all(_BLOCK_HTML_TAGS):
        element.insert_before("\n\n")
        element.insert_after("\n\n")
    for cell in body.find_all(("th", "td")):
        cell.insert_after("\t")
    return _normalize_plain_text(str(body.get_text()))


def _normalize_plain_text(value: str) -> str:
    value = value.replace("\r", "\n")
    value = re.sub(r"[\t\f\v ]+", " ", value)
    value = re.sub(r" *\n *", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def _cap(
    value: str,
    max_chars: int,
    marker: str,
    *,
    marker_inside_limit: bool = True,
) -> str:
    if len(value) <= max_chars:
        return value
    if marker_inside_limit:
        if max_chars <= len(marker):
            return marker[-max_chars:]
        return value[: max_chars - len(marker)] + marker
    return value[:max_chars] + marker


def _is_resource_error(exc: BaseException) -> bool:
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, MemoryError):
            return True
        if isinstance(current, OSError) and current.errno == errno.ENOMEM:
            return True
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
        attempts = getattr(current, "attempts", None)
        if isinstance(attempts, list):
            for attempt in attempts:
                exc_info = getattr(attempt, "exc_info", None)
                if (
                    isinstance(exc_info, tuple)
                    and len(exc_info) >= 2
                    and isinstance(exc_info[1], BaseException)
                ):
                    pending.append(exc_info[1])
    return False


def _unsafe_ooxml() -> WorkerInputError:
    return WorkerInputError(
        "tool_content_conversion_failed",
        "Workspace OOXML document failed safety validation",
    )


def _send(connection: Connection, ok: bool, code: str, value: str) -> None:
    try:
        connection.send((PROTOCOL_VERSION, ok, code, value))
    except (BrokenPipeError, EOFError, OSError):
        pass


def _required_str(request: WorkerRequest, key: str) -> str:
    value = request.get(key)
    if not isinstance(value, str):
        raise ValueError(f"conversion request field {key} must be a string")
    return value


def _required_bytes(request: WorkerRequest, key: str) -> bytes:
    value = request.get(key)
    if not isinstance(value, bytes):
        raise ValueError(f"conversion request field {key} must be bytes")
    return value


def _required_int(request: WorkerRequest, key: str) -> int:
    value = request.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"conversion request field {key} must be an integer")
    return value


def _required_float(request: WorkerRequest, key: str) -> float:
    value = request.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"conversion request field {key} must be numeric")
    return float(value)


def exited_for_cpu_limit(exit_code: int | None) -> bool:
    return exit_code == -signal.SIGXCPU
