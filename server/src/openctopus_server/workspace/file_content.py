from __future__ import annotations

import asyncio
import base64
import codecs
import multiprocessing
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import PurePosixPath
from typing import Any, cast
from uuid import UUID

from openctopus_server.admission import AdmissionTimeoutError, KeyedAdmission
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError
from openctopus_server.workspace.content_conversion_worker import (
    PROTOCOL_VERSION,
    WorkerRequest,
    exited_for_cpu_limit,
    run_conversion_worker,
)

MAX_READ_CHARS = 128_000
_IMAGE_SIGNATURES = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
    (b"RIFF", "image/webp"),
)


class DocumentParser:
    def __init__(
        self,
        *,
        admission: KeyedAdmission,
        memory_mb: int,
        timeout_seconds: float,
        worker_target: Callable[[Connection, WorkerRequest], None] | None = None,
    ) -> None:
        if memory_mb <= 0 or timeout_seconds <= 0:
            raise ValueError("Document parser limits must be positive")
        self._admission = admission
        self._memory_mb = memory_mb
        self._timeout_seconds = timeout_seconds
        self._worker_target = worker_target

    @asynccontextmanager
    async def admit(self, user_id: UUID) -> AsyncIterator[_AdmittedConversion]:
        try:
            async with self._admission.slot(user_id):
                yield _AdmittedConversion(self)
        except AdmissionTimeoutError as exc:
            raise ToolError(
                ErrorCode.TOOL_CONTENT_CONVERSION_BUSY,
                "Content conversion is busy; try again",
            ) from exc

    async def parse(
        self,
        path: str,
        data: bytes,
        *,
        user_id: UUID,
        pages: str | None = None,
    ) -> str:
        async with self.admit(user_id) as conversion:
            return await conversion.parse(path, data, pages=pages)

    async def parse_html(
        self,
        data: bytes,
        *,
        user_id: UUID,
        charset: str,
        base_url: str,
        mode: str,
        max_chars: int,
    ) -> str:
        async with self.admit(user_id) as conversion:
            return await conversion.parse_html(
                data,
                charset=charset,
                base_url=base_url,
                mode=mode,
                max_chars=max_chars,
            )

    async def probe(self) -> None:
        await self._run({"operation": "probe"})

    async def _run(self, request: WorkerRequest) -> str:
        request = {
            **request,
            "memory_mb": self._memory_mb,
            "timeout_seconds": self._timeout_seconds,
        }
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe(duplex=False)
        process = context.Process(
            target=self._worker_target or run_conversion_worker,
            args=(child, request),
            daemon=True,
        )
        started = False
        try:
            process.start()
            started = True
            child.close()
            return await self._receive(parent, process)
        finally:
            child.close()
            parent.close()
            if started:
                await _reap_process_cancellation_safe(process)

    async def _receive(self, parent: Connection, process: BaseProcess) -> str:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout_seconds
        while True:
            if parent.poll():
                try:
                    message: object = parent.recv()
                except EOFError as exc:
                    raise _conversion_failed(
                        "Content conversion worker exited unexpectedly"
                    ) from exc
                return _parse_worker_message(message)
            if not process.is_alive():
                process.join(timeout=0)
                if parent.poll():
                    continue
                if exited_for_cpu_limit(process.exitcode):
                    raise ToolError(ErrorCode.TOOL_EXEC_TIMEOUT, "Content conversion timed out")
                raise _conversion_failed("Content conversion worker exited unexpectedly")
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise ToolError(ErrorCode.TOOL_EXEC_TIMEOUT, "Content conversion timed out")
            await asyncio.sleep(min(0.01, remaining))


class _AdmittedConversion:
    def __init__(self, parser: DocumentParser) -> None:
        self._parser = parser

    async def parse(self, path: str, data: bytes, *, pages: str | None = None) -> str:
        return await self._parser._run(
            {
                "operation": "document",
                "path": path,
                "data": data,
                "pages": pages,
            }
        )

    async def parse_html(
        self,
        data: bytes,
        *,
        charset: str,
        base_url: str,
        mode: str,
        max_chars: int,
    ) -> str:
        return await self._parser._run(
            {
                "operation": "html",
                "data": data,
                "charset": charset,
                "base_url": base_url,
                "mode": mode,
                "max_chars": max_chars,
            }
        )


async def _reap_process(process: BaseProcess) -> None:
    for _ in range(10):
        process.join(timeout=0)
        if not process.is_alive():
            process.close()
            return
        await asyncio.sleep(0.01)
    process.terminate()
    for _ in range(100):
        process.join(timeout=0)
        if not process.is_alive():
            process.close()
            return
        await asyncio.sleep(0.01)
    process.kill()
    while process.is_alive():
        process.join(timeout=0)
        await asyncio.sleep(0.01)
    process.close()


async def _reap_process_cancellation_safe(process: BaseProcess) -> None:
    reap_task = asyncio.create_task(_reap_process(process))
    cancelled = False
    while True:
        try:
            await asyncio.shield(reap_task)
            break
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError


def _parse_worker_message(message: object) -> str:
    if (
        not isinstance(message, tuple)
        or len(message) != 4
        or message[0] != PROTOCOL_VERSION
        or not isinstance(message[1], bool)
        or not isinstance(message[2], str)
        or not isinstance(message[3], str)
    ):
        raise _conversion_failed("Content conversion worker returned invalid content")
    _, ok, code, value = cast(tuple[int, bool, str, str], message)
    if ok:
        return value
    try:
        error_code = ErrorCode(code)
    except ValueError as exc:
        raise _conversion_failed("Content conversion worker returned an unknown error") from exc
    raise ToolError(error_code, value)


def render_file_content(
    path: str,
    data: bytes,
    *,
    offset: int = 1,
    limit: int = 2000,
    pages: str | None = None,
) -> str | list[dict[str, Any]]:
    if offset < 1 or limit < 1:
        raise _invalid("File offset and limit must be positive")
    media_type = image_media_type(data)
    if media_type is not None:
        return [
            {"type": "text", "text": f"Image: {path}"},
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": base64.b64encode(data).decode("ascii"),
                },
            },
        ]

    suffix = PurePosixPath(path).suffix.lower()
    if suffix in {".pdf", ".docx", ".xlsx", ".pptx"}:
        raise _invalid("Workspace documents must be read through the isolated parser")
    if b"\x00" in data:
        raise _invalid("Workspace file is binary and cannot be read as text")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _invalid("Workspace file is not readable UTF-8 text") from exc
    lines = text.splitlines()
    selected = lines[offset - 1 : offset - 1 + limit]
    rendered = "\n".join(
        f"{line_number}|{line}" for line_number, line in enumerate(selected, start=offset)
    )
    if selected and offset - 1 + len(selected) < len(lines):
        end = offset + len(selected) - 1
        rendered += (
            f"\n\n(Showing lines {offset}-{end} of {len(lines)}. Use offset={end + 1} to continue.)"
        )
    return _cap(rendered)


async def render_streamed_text(
    read: Callable[[], Awaitable[bytes]],
    *,
    offset: int,
    limit: int,
) -> str:
    if offset < 1 or limit < 1:
        raise _invalid("File offset and limit must be positive")
    decoder = codecs.getincrementaldecoder("utf-8")()
    line_number = 0
    current = ""
    current_truncated = False
    selected: list[str] = []
    selected_chars = 0
    content_truncated = False

    def consume(value: str) -> None:
        nonlocal current, current_truncated
        wanted = offset <= line_number + 1 < offset + limit
        if not wanted or current_truncated:
            return
        remaining = MAX_READ_CHARS - selected_chars - len(current)
        current += value[:remaining]
        if len(value) > remaining:
            current_truncated = True

    def finish_line() -> None:
        nonlocal line_number, current, current_truncated, selected_chars, content_truncated
        line_number += 1
        if offset <= line_number < offset + limit:
            rendered_line = f"{line_number}|{current.rstrip(chr(13))}"
            separator_size = 1 if selected else 0
            remaining = MAX_READ_CHARS - selected_chars - separator_size
            if remaining > 0:
                selected.append(rendered_line[:remaining])
                selected_chars += separator_size + min(len(rendered_line), remaining)
            content_truncated = (
                content_truncated or current_truncated or len(rendered_line) > remaining
            )
        current = ""
        current_truncated = False

    try:
        while chunk := await read():
            text = decoder.decode(chunk)
            if "\x00" in text:
                raise _invalid("Workspace file is binary and cannot be read as text")
            parts = text.split("\n")
            for part in parts[:-1]:
                consume(part)
                finish_line()
            consume(parts[-1])
        tail = decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise _invalid("Workspace file is not readable UTF-8 text") from exc
    if "\x00" in tail:
        raise _invalid("Workspace file is binary and cannot be read as text")
    consume(tail)
    if current or current_truncated:
        finish_line()

    rendered = "\n".join(selected)
    if selected and offset - 1 + len(selected) < line_number:
        end = offset + len(selected) - 1
        rendered += (
            f"\n\n(Showing lines {offset}-{end} of {line_number}. "
            f"Use offset={end + 1} to continue.)"
        )
    if content_truncated:
        rendered += "\n[truncated]"
    return _cap(rendered)


def image_media_type(data: bytes) -> str | None:
    for signature, media_type in _IMAGE_SIGNATURES:
        if data.startswith(signature):
            if media_type == "image/webp" and data[8:12] != b"WEBP":
                continue
            return media_type
    return None


def _cap(value: str) -> str:
    marker = "\n[truncated]"
    if len(value) <= MAX_READ_CHARS:
        return value
    return value[: MAX_READ_CHARS - len(marker)] + marker


def _invalid(message: str) -> ToolError:
    return ToolError(ErrorCode.TOOL_INVALID_ARGS, message)


def _conversion_failed(message: str) -> ToolError:
    return ToolError(ErrorCode.TOOL_CONTENT_CONVERSION_FAILED, message)
