"""Atomic MCP result mapping into Server tool results."""

from __future__ import annotations

import base64
import binascii
from typing import Any, cast

from mcp import types

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.mcp.catalog import McpCatalogError, canonical_json_bytes
from openctopus_server.tools.base import ToolResult
from openctopus_server.tools.result import (
    UNTRUSTED_TOOL_RESULT_WARNING,
    normalize_tool_result,
)

DEFAULT_MCP_TEXT_CHARS = 16_000
DEFAULT_MCP_RESULT_BYTES = 12 * 1024 * 1024
_IMAGE_MEDIA_TYPES = frozenset({"image/jpeg", "image/png", "image/gif", "image/webp"})


class _MappingError(ValueError):
    def __init__(self, code: ErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


def _failure(code: ErrorCode) -> ToolResult:
    messages = {
        ErrorCode.TOOL_RESULT_TOO_LARGE: "Tool result exceeded its response credit",
        ErrorCode.TOOL_UNSUPPORTED_MEDIA: (
            "The MCP result contains unsupported media; all content was discarded"
        ),
        ErrorCode.TOOL_MCP_ERROR: "The MCP server returned an error",
        ErrorCode.TOOL_MCP_INVALID_RESULT: (
            "The MCP server returned an invalid result; all content was discarded"
        ),
    }
    return ToolResult(
        content=normalize_tool_result(f"[{code.value}] {messages[code]}"),
        is_error=True,
        code=code,
    )


class _ResultBuilder:
    def __init__(self, *, max_text_chars: int, max_result_bytes: int) -> None:
        if max_text_chars < 0 or max_result_bytes < 1:
            raise ValueError("MCP result credits must be positive")
        self.max_text_chars = max_text_chars
        self.max_result_bytes = max_result_bytes
        self.text_chars = 0
        self.blocks: list[dict[str, Any]] = []
        warning = {"type": "text", "text": UNTRUSTED_TOOL_RESULT_WARNING}
        self._encoded_size = 2 + len(canonical_json_bytes(warning))
        if self._encoded_size > self.max_result_bytes:
            raise _MappingError(ErrorCode.TOOL_RESULT_TOO_LARGE)

    def _size_after(self, block: dict[str, Any]) -> int:
        size = self._encoded_size + 1 + len(canonical_json_bytes(block))
        if size > self.max_result_bytes:
            raise _MappingError(ErrorCode.TOOL_RESULT_TOO_LARGE)
        return size

    def _append(self, block: dict[str, Any]) -> None:
        self._encoded_size = self._size_after(block)
        self.blocks.append(block)

    def add_text(self, *parts: str) -> None:
        text = "".join(parts)
        if self.text_chars + len(text) > self.max_text_chars:
            raise _MappingError(ErrorCode.TOOL_RESULT_TOO_LARGE)
        block = {"type": "text", "text": text}
        self._append(block)
        self.text_chars += len(text)

    def add_canonical_text(self, prefix: str, value: object) -> None:
        self.add_text(prefix, canonical_json_bytes(value).decode("utf-8"))

    def add_image(self, data: str, mime_type: str | None) -> None:
        if mime_type not in _IMAGE_MEDIA_TYPES:
            raise _MappingError(ErrorCode.TOOL_UNSUPPORTED_MEDIA)
        if not data:
            raise _MappingError(ErrorCode.TOOL_MCP_INVALID_RESULT)
        block = {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": mime_type,
                "data": data,
            },
        }
        encoded_size = self._size_after(block)
        try:
            decoded = base64.b64decode(data, validate=True)
        except (ValueError, binascii.Error):
            raise _MappingError(ErrorCode.TOOL_MCP_INVALID_RESULT) from None
        if not decoded:
            raise _MappingError(ErrorCode.TOOL_MCP_INVALID_RESULT)
        self._encoded_size = encoded_size
        self.blocks.append(block)

    def finish(self) -> list[dict[str, Any]]:
        if not self.blocks:
            self.add_text("(no output)")
        return self.blocks


def _resource_descriptor(resource: object) -> dict[str, str]:
    uri = getattr(resource, "uri", None)
    if uri is None:
        raise _MappingError(ErrorCode.TOOL_MCP_INVALID_RESULT)
    descriptor = {"uri": str(uri)}
    mime_type = getattr(resource, "mimeType", None)
    if mime_type is not None:
        if not isinstance(mime_type, str):
            raise _MappingError(ErrorCode.TOOL_MCP_INVALID_RESULT)
        descriptor["mimeType"] = mime_type
    return descriptor


def _map_resource_contents(
    builder: _ResultBuilder,
    resource: types.TextResourceContents | types.BlobResourceContents,
) -> None:
    descriptor = canonical_json_bytes(_resource_descriptor(resource)).decode("utf-8")
    if isinstance(resource, types.TextResourceContents):
        builder.add_text("[mcp_resource]\n", descriptor, "\n", resource.text)
        return
    if isinstance(resource, types.BlobResourceContents):
        builder.add_text("[mcp_resource_image]\n", descriptor)
        builder.add_image(resource.blob, resource.mimeType)
        return
    raise _MappingError(ErrorCode.TOOL_MCP_INVALID_RESULT)


def _map_block(builder: _ResultBuilder, block: object) -> None:
    if isinstance(block, types.TextContent):
        builder.add_text(block.text)
        return
    if isinstance(block, types.ImageContent):
        builder.add_image(block.data, block.mimeType)
        return
    if isinstance(block, types.AudioContent):
        raise _MappingError(ErrorCode.TOOL_UNSUPPORTED_MEDIA)
    if isinstance(block, types.ResourceLink):
        builder.add_canonical_text(
            "[mcp_resource_link]\n",
            block.model_dump(mode="json", by_alias=True, exclude_none=True),
        )
        return
    if isinstance(block, types.EmbeddedResource):
        _map_resource_contents(builder, block.resource)
        return
    raise _MappingError(ErrorCode.TOOL_MCP_INVALID_RESULT)


def _success(builder: _ResultBuilder) -> ToolResult:
    return ToolResult(content=normalize_tool_result(builder.finish()))


def _builder(max_text_chars: int, max_result_bytes: int) -> _ResultBuilder:
    return _ResultBuilder(
        max_text_chars=max_text_chars,
        max_result_bytes=max_result_bytes,
    )


def map_tool_result(
    result: types.CallToolResult,
    *,
    max_text_chars: int = DEFAULT_MCP_TEXT_CHARS,
    max_result_bytes: int = DEFAULT_MCP_RESULT_BYTES,
) -> ToolResult:
    """Map one complete tool result without exposing partial or error content."""

    if result.isError:
        return _failure(ErrorCode.TOOL_MCP_ERROR)
    try:
        builder = _builder(max_text_chars, max_result_bytes)
        for block in result.content:
            _map_block(builder, block)
        if result.structuredContent is not None:
            builder.add_canonical_text("[mcp_structured_content]\n", result.structuredContent)
        return _success(builder)
    except _MappingError as exc:
        return _failure(exc.code)
    except (McpCatalogError, TypeError, UnicodeError, ValueError):
        return _failure(ErrorCode.TOOL_MCP_INVALID_RESULT)


def map_resource_result(
    result: types.ReadResourceResult,
    *,
    max_text_chars: int = DEFAULT_MCP_TEXT_CHARS,
    max_result_bytes: int = DEFAULT_MCP_RESULT_BYTES,
) -> ToolResult:
    try:
        builder = _builder(max_text_chars, max_result_bytes)
        for resource in result.contents:
            _map_resource_contents(builder, resource)
        return _success(builder)
    except _MappingError as exc:
        return _failure(exc.code)
    except (McpCatalogError, TypeError, UnicodeError, ValueError):
        return _failure(ErrorCode.TOOL_MCP_INVALID_RESULT)


def map_prompt_result(
    result: types.GetPromptResult,
    *,
    max_text_chars: int = DEFAULT_MCP_TEXT_CHARS,
    max_result_bytes: int = DEFAULT_MCP_RESULT_BYTES,
) -> ToolResult:
    try:
        builder = _builder(max_text_chars, max_result_bytes)
        for message in result.messages:
            builder.add_text("[mcp_prompt_message role=", cast(str, message.role), "]")
            _map_block(builder, message.content)
        return _success(builder)
    except _MappingError as exc:
        return _failure(exc.code)
    except (McpCatalogError, TypeError, UnicodeError, ValueError):
        return _failure(ErrorCode.TOOL_MCP_INVALID_RESULT)
