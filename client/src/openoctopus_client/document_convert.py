from __future__ import annotations

import asyncio
import base64
import binascii
import json
import ntpath
import os
import re
import stat
import subprocess
import sys
import traceback
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Literal, cast
from zipfile import BadZipFile, ZipFile

MAX_INPUT_BYTES = 8 * 1024 * 1024
TIMEOUT_SECONDS = 20
MAX_OUTPUT_CHARS = 128_000
MAX_OOXML_MEMBERS = 10_000
MAX_OOXML_MEMBER_BYTES = 128 * 1024 * 1024
MAX_OOXML_TOTAL_BYTES = 256 * 1024 * 1024
MAX_OOXML_COMPRESSION_RATIO = 100
MIN_RATIO_CHECK_BYTES = 1024 * 1024
_WORKER_TERMINATE_GRACE_SECONDS = 0.25

type ConversionFormat = Literal["pdf", "docx", "xlsx", "pptx", "html"]


class ConversionError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def convert_path(path: Path, *, pages: str | None) -> str:
    return _run_worker_request({"path": str(path), "pages": pages})


def convert_html_bytes(data: bytes, *, base_url: str | None = None) -> str:
    if len(data) > MAX_INPUT_BYTES:
        raise ConversionError(
            "tool_content_conversion_failed", "Document exceeds the 8 MiB input limit"
        )
    return _run_worker_request(
        {
            "name": "document.html",
            "data": base64.b64encode(data).decode("ascii"),
            "base_url": base_url,
            "pages": None,
        }
    )


async def convert_path_async(path: Path, *, pages: str | None) -> str:
    return await _run_worker_request_async({"path": str(path), "pages": pages})


async def convert_html_bytes_async(data: bytes, *, base_url: str | None = None) -> str:
    if len(data) > MAX_INPUT_BYTES:
        raise ConversionError(
            "tool_content_conversion_failed", "Document exceeds the 8 MiB input limit"
        )
    return await _run_worker_request_async(
        {
            "name": "document.html",
            "data": base64.b64encode(data).decode("ascii"),
            "base_url": base_url,
            "pages": None,
        }
    )


def _run_worker_request(request: dict[str, object]) -> str:
    try:
        completed = subprocess.run(
            _worker_command(),
            input=json.dumps(request),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            env=_minimal_worker_environment(),
            timeout=TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise ConversionError("tool_exec_timeout", "Document conversion timed out") from exc
    except (OSError, UnicodeError) as exc:
        raise ConversionError(
            "tool_content_conversion_failed",
            "Document conversion worker could not be started",
        ) from exc
    return _parse_worker_result(completed.stdout)


async def _run_worker_request_async(request: dict[str, object]) -> str:
    try:
        process = await asyncio.create_subprocess_exec(
            *_worker_command(),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=_minimal_worker_environment(),
        )
    except OSError as exc:
        raise ConversionError(
            "tool_content_conversion_failed",
            "Document conversion worker could not be started",
        ) from exc
    try:
        async with asyncio.timeout(TIMEOUT_SECONDS):
            stdout, _ = await process.communicate(json.dumps(request).encode("utf-8"))
    except asyncio.CancelledError:
        await _stop_worker(process)
        raise
    except TimeoutError as exc:
        await _stop_worker(process)
        raise ConversionError("tool_exec_timeout", "Document conversion timed out") from exc
    except OSError as exc:
        await _stop_worker(process)
        raise ConversionError(
            "tool_content_conversion_failed",
            "Document conversion worker could not be started",
        ) from exc
    try:
        output = stdout.decode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise ConversionError(
            "tool_content_conversion_failed",
            "Document conversion worker returned an invalid result",
        ) from exc
    return _parse_worker_result(output)


async def _stop_worker(process: asyncio.subprocess.Process) -> None:
    """Terminate, force-kill, and reap a conversion child within a fixed bound."""

    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), _WORKER_TERMINATE_GRACE_SECONDS)
            return
        except TimeoutError:
            pass
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
    await process.wait()


def _parse_worker_result(output: str) -> str:
    try:
        message = json.loads(output)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ConversionError(
            "tool_content_conversion_failed",
            "Document conversion worker returned an invalid result",
        ) from exc
    if not isinstance(message, dict):
        raise ConversionError(
            "tool_content_conversion_failed",
            "Document conversion worker returned an invalid result",
        )
    ok = message.get("ok")
    value = message.get("text")
    if ok is True and isinstance(value, str):
        return value
    code = message.get("code")
    value = message.get("message")
    if ok is False and isinstance(code, str) and isinstance(value, str):
        raise ConversionError(code, value)
    raise ConversionError(
        "tool_content_conversion_failed",
        "Document conversion worker returned an invalid result",
    )


def _worker_command() -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "_conversion-worker"]
    package_root = str(Path(__file__).resolve().parents[1])
    bootstrap = (
        "import runpy,sys;"
        "sys.path.insert(0,sys.argv.pop(1));"
        "runpy.run_module('openoctopus_client',run_name='__main__')"
    )
    return [sys.executable, "-I", "-c", bootstrap, package_root, "_conversion-worker"]


def _minimal_worker_environment() -> dict[str, str]:
    allowed = ("LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "TEMP", "TMP", "SYSTEMROOT", "WINDIR")
    return {name: value for name in allowed if (value := os.environ.get(name)) is not None}


def conversion_worker_main() -> int:
    try:
        request = json.load(sys.stdin)
        if not isinstance(request, dict):
            raise ConversionError("tool_invalid_args", "Document conversion request is invalid")
        pages = request.get("pages")
        if not (pages is None or isinstance(pages, str)):
            raise ConversionError("tool_invalid_args", "Document conversion request is invalid")
        _apply_linux_limits()
        base_url: str | None = None
        if set(request) == {"path", "pages"} and isinstance(request["path"], str):
            path = Path(request["path"])
            name = path.name
            data = _read_limited(path)
        elif (
            set(request) == {"name", "data", "base_url", "pages"}
            and request["name"] == "document.html"
            and isinstance(request["data"], str)
            and (request["base_url"] is None or isinstance(request["base_url"], str))
        ):
            name = "document.html"
            base_url = request["base_url"]
            try:
                data = base64.b64decode(request["data"], validate=True)
            except (ValueError, binascii.Error) as exc:
                raise ConversionError(
                    "tool_invalid_args", "Document conversion request is invalid"
                ) from exc
            if len(data) > MAX_INPUT_BYTES:
                raise ConversionError(
                    "tool_content_conversion_failed",
                    "Document exceeds the 8 MiB input limit",
                )
        else:
            raise ConversionError("tool_invalid_args", "Document conversion request is invalid")
        payload = {"ok": True, "text": _convert_bytes(name, data, pages, base_url=base_url)}
        return_code = 0
    except ConversionError as exc:
        payload = {"code": exc.code, "message": exc.message, "ok": False}
        return_code = 1
    except Exception:
        if os.environ.get("OPENOCTOPUS_CONVERSION_DEBUG") == "1":
            traceback.print_exc(file=sys.stderr)
        payload = {
            "code": "tool_content_conversion_failed",
            "message": "Document conversion failed",
            "ok": False,
        }
        return_code = 1
    sys.stdout.buffer.write(
        (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
    )
    return return_code


def _read_limited(path: Path) -> bytes:
    flags = os.O_RDONLY
    for name in ("O_BINARY", "O_NOFOLLOW", "O_NONBLOCK"):
        flags |= int(getattr(os, name, 0))
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError as exc:
        raise ConversionError("tool_content_conversion_failed", "Document does not exist") from exc
    except OSError as exc:
        raise ConversionError(
            "tool_content_conversion_failed", "Document could not be read"
        ) from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ConversionError(
                "tool_content_conversion_failed", "Document is not a regular file"
            )
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            data = stream.read(MAX_INPUT_BYTES + 1)
    except ConversionError:
        raise
    except OSError as exc:
        raise ConversionError(
            "tool_content_conversion_failed", "Document could not be read"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(data) > MAX_INPUT_BYTES:
        raise ConversionError(
            "tool_content_conversion_failed", "Document exceeds the 8 MiB input limit"
        )
    return data


def _apply_linux_limits() -> None:
    if sys.platform != "linux":
        return
    import resource

    memory_limit = 2 * 1024 * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
    resource.setrlimit(resource.RLIMIT_CPU, (TIMEOUT_SECONDS, TIMEOUT_SECONDS + 1))


def _convert_bytes(
    name: str,
    data: bytes,
    pages: str | None,
    *,
    base_url: str | None = None,
) -> str:
    conversion_format = _format_for(name)
    if conversion_format == "pdf":
        data, start, end, total = _prepare_pdf(data, pages)
    elif conversion_format in {"docx", "xlsx", "pptx"}:
        _preflight_ooxml(data)
    elif pages is not None:
        raise ConversionError("tool_invalid_args", "PDF pages are only supported for PDF documents")

    from markitdown import StreamInfo
    from markitdown.converters import (
        DocxConverter,
        HtmlConverter,
        PdfConverter,
        PptxConverter,
        XlsxConverter,
    )

    stream_info = {
        "pdf": StreamInfo(extension=".pdf", mimetype="application/pdf"),
        "docx": StreamInfo(
            extension=".docx",
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        "xlsx": StreamInfo(
            extension=".xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        "pptx": StreamInfo(
            extension=".pptx",
            mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ),
        "html": StreamInfo(
            extension=".html",
            mimetype="text/html",
            charset="utf-8",
            url=base_url,
        ),
    }[conversion_format]
    converter = {
        "pdf": PdfConverter,
        "docx": DocxConverter,
        "xlsx": XlsxConverter,
        "pptx": PptxConverter,
        "html": HtmlConverter,
    }[conversion_format]()
    text = _normalize_markdown(str(converter.convert(BytesIO(data), stream_info).text_content))
    if conversion_format == "pdf":
        return _render_pdf(text, start, end, total)
    return _cap(text, MAX_OUTPUT_CHARS)


def _format_for(name: str) -> ConversionFormat:
    suffix = Path(name).suffix.lower()
    if suffix == ".htm":
        return "html"
    if suffix[1:] in {"pdf", "docx", "xlsx", "pptx", "html"}:
        return cast(ConversionFormat, suffix[1:])
    raise ConversionError("tool_content_conversion_failed", "Document format is not supported")


def _prepare_pdf(data: bytes, pages: str | None) -> tuple[bytes, int, int, int]:
    try:
        from pypdf import PdfReader, PdfWriter

        reader = PdfReader(BytesIO(data))
        if reader.is_encrypted:
            raise ConversionError(
                "tool_content_conversion_failed", "Encrypted PDF documents are not supported"
            )
        total = len(reader.pages)
        start, end = _parse_page_range(pages, total)
        writer = PdfWriter()
        for index in range(start - 1, end):
            writer.add_page(reader.pages[index])
        output = BytesIO()
        writer.write(output)
        return output.getvalue(), start, end, total
    except ConversionError:
        raise
    except Exception as exc:
        raise ConversionError(
            "tool_content_conversion_failed", "Document PDF could not be parsed"
        ) from exc


def _parse_page_range(value: str | None, total: int) -> tuple[int, int]:
    if total < 1:
        raise ConversionError(
            "tool_content_conversion_failed", "Document PDF does not contain any pages"
        )
    if value is None:
        return 1, min(20, total)
    if re.fullmatch(r"[1-9]\d*(?:-[1-9]\d*)?", value) is None:
        raise ConversionError(
            "tool_invalid_args", "PDF pages must be a page number or range such as 1 or 1-5"
        )
    start_text, separator, end_text = value.partition("-")
    start = int(start_text)
    end = int(end_text) if separator else start
    if end < start or end - start + 1 > 20 or end > total:
        raise ConversionError("tool_invalid_args", "PDF page range is invalid or exceeds 20 pages")
    return start, end


def _preflight_ooxml(data: bytes) -> None:
    try:
        with ZipFile(BytesIO(data)) as archive:
            members = archive.infolist()
    except (BadZipFile, OSError) as exc:
        raise ConversionError(
            "tool_content_conversion_failed", "Document is not a valid OOXML file"
        ) from exc
    if len(members) > MAX_OOXML_MEMBERS:
        raise ConversionError(
            "tool_content_conversion_failed", "Document OOXML safety validation failed"
        )
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
            raise ConversionError(
                "tool_content_conversion_failed", "Document OOXML safety validation failed"
            )
        if (
            member.flag_bits & 0x1
            or member.file_size < 0
            or member.compress_size < 0
            or member.file_size > MAX_OOXML_MEMBER_BYTES
        ):
            raise ConversionError(
                "tool_content_conversion_failed", "Document OOXML safety validation failed"
            )
        total_size += member.file_size
        if total_size > MAX_OOXML_TOTAL_BYTES or (
            member.file_size >= MIN_RATIO_CHECK_BYTES
            and member.file_size / max(1, member.compress_size) > MAX_OOXML_COMPRESSION_RATIO
        ):
            raise ConversionError(
                "tool_content_conversion_failed", "Document OOXML safety validation failed"
            )


def _render_pdf(text: str, start: int, end: int, total: int) -> str:
    header = f"[PDF pages {start}-{end} of {total}]"
    footer = ""
    if end < total:
        footer = (
            f'[More pages available. Call read_file with pages="{end + 1}-{min(total, end + 20)}".]'
        )
    body = text.strip() or "No extractable text was found. OCR is not enabled."
    body = _cap(body, MAX_OUTPUT_CHARS - len(header) - len(footer) - 4)
    return "\n\n".join(part for part in (header, body, footer) if part)


def _normalize_markdown(value: str) -> str:
    return re.sub(r"\n{3,}", "\n\n", "\n".join(line.rstrip() for line in value.splitlines()))


def _cap(value: str, maximum: int) -> str:
    marker = "\n[truncated]"
    if len(value) <= maximum:
        return value
    return value[: maximum - len(marker)] + marker
