"""Deterministic MCP result mapping into OpenOctopus safe result blocks."""

from __future__ import annotations

import base64
import binascii
import json
import math
from typing import Any, Literal, cast
from uuid import UUID

from mcp import types

from openoctopus_client.mcp.catalog import (
    JSON_NESTING_MAX,
    McpCatalogError,
    canonical_json_bytes,
)
from openoctopus_client.tools.common import ToolOutput, fail

_IMAGE_MEDIA_TYPES = frozenset(
    {"image/jpeg", "image/png", "image/gif", "image/webp"}
)

type SafeImageMediaType = Literal["image/jpeg", "image/png", "image/gif", "image/webp"]


class _McpMappingError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _mapping_failure(code: str) -> ToolOutput:
    if code == "tool_result_too_large":
        return fail(code, "Tool result exceeded its response credit")
    if code == "tool_unsupported_media":
        return fail(code, "The MCP result contains unsupported media; all content was discarded")
    return fail(code, "The MCP server returned an invalid result; all content was discarded")


def _canonical_json(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _add_size(current: int, addition: int, limit: int) -> int:
    total = current + addition
    if total > limit:
        raise _McpMappingError("tool_result_too_large")
    return total


def _json_string_content_size(value: str, limit: int) -> int:
    size = 0
    for character in value:
        codepoint = ord(character)
        if character in {'"', "\\", "\b", "\f", "\n", "\r", "\t"}:
            width = 2
        elif codepoint < 0x20:
            width = 6
        elif codepoint < 0x80:
            width = 1
        elif codepoint < 0x800:
            width = 2
        elif 0xD800 <= codepoint <= 0xDFFF:
            raise UnicodeError("JSON strings must contain valid Unicode")
        elif codepoint < 0x10000:
            width = 3
        else:
            width = 4
        size = _add_size(size, width, limit)
    return size


def _json_size(value: object, limit: int, *, depth: int = 0) -> int:
    if value is None:
        return _add_size(0, 4, limit)
    if isinstance(value, str):
        content_limit = limit - 2
        if content_limit < 0:
            raise _McpMappingError("tool_result_too_large")
        return _add_size(2, _json_string_content_size(value, content_limit), limit)
    if isinstance(value, bool):
        return _add_size(0, 4 if value else 5, limit)
    if isinstance(value, int):
        return _add_size(0, len(str(value)), limit)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON contains a non-finite number")
        encoded = json.dumps(value, allow_nan=False)
        return _add_size(0, len(encoded), limit)
    if isinstance(value, dict):
        next_depth = depth + 1
        if next_depth > JSON_NESTING_MAX:
            raise ValueError("JSON nesting depth exceeds its limit")
        size = _add_size(0, 2, limit)
        for index, (key, child) in enumerate(value.items()):
            if not isinstance(key, str):
                raise TypeError("JSON object keys must be strings")
            if index:
                size = _add_size(size, 1, limit)
            size = _add_size(size, _json_size(key, limit - size), limit)
            size = _add_size(size, 1, limit)
            size = _add_size(
                size,
                _json_size(child, limit - size, depth=next_depth),
                limit,
            )
        return size
    if isinstance(value, (list, tuple)):
        next_depth = depth + 1
        if next_depth > JSON_NESTING_MAX:
            raise ValueError("JSON nesting depth exceeds its limit")
        size = _add_size(0, 2, limit)
        for index, child in enumerate(value):
            if index:
                size = _add_size(size, 1, limit)
            size = _add_size(
                size,
                _json_size(child, limit - size, depth=next_depth),
                limit,
            )
        return size
    raise TypeError("value is not canonical JSON")


def _bounded_canonical_json(value: object, max_bytes: int) -> str:
    _json_size(value, max_bytes)
    return _canonical_json(value)


class _ResultBuilder:
    def __init__(
        self,
        *,
        request_id: UUID,
        max_result_bytes: int,
        is_error: bool,
        code: str | None,
    ) -> None:
        from openoctopus_client.protocol import ToolResult, encode_frame

        empty = ToolResult(id=request_id, content=[], is_error=is_error, code=code)
        self._max_result_bytes = max_result_bytes
        self._encoded_size = len(encode_frame(empty).encode("utf-8"))
        if self._encoded_size > max_result_bytes:
            raise _McpMappingError("tool_result_too_large")
        self.blocks: list[dict[str, Any]] = []

    def remaining_block_bytes(self) -> int:
        separator = 1 if self.blocks else 0
        remaining = self._max_result_bytes - self._encoded_size - separator
        if remaining < 0:
            raise _McpMappingError("tool_result_too_large")
        return remaining

    def _append(self, block: dict[str, Any], encoded_size: int) -> None:
        separator = 1 if self.blocks else 0
        self._encoded_size = _add_size(
            self._encoded_size,
            separator + encoded_size,
            self._max_result_bytes,
        )
        self.blocks.append(block)

    def add_text(self, *parts: str) -> None:
        remaining = self.remaining_block_bytes()
        empty_size = _json_size({"type": "text", "text": ""}, remaining)
        content_limit = remaining - empty_size
        content_size = 0
        for part in parts:
            content_size = _add_size(
                content_size,
                _json_string_content_size(part, content_limit - content_size),
                content_limit,
            )
        text = parts[0] if len(parts) == 1 else "".join(parts)
        self._append({"type": "text", "text": text}, empty_size + content_size)

    def add_canonical_text(self, prefix: str, value: object) -> None:
        remaining = self.remaining_block_bytes()
        empty_size = _json_size({"type": "text", "text": ""}, remaining)
        prefix_size = _json_string_content_size(prefix, remaining - empty_size)
        canonical = _bounded_canonical_json(
            value,
            remaining - empty_size - prefix_size,
        )
        self.add_text(prefix, canonical)

    def add_image(self, data: str, mime_type: str | None) -> None:
        if mime_type not in _IMAGE_MEDIA_TYPES:
            raise _McpMappingError("tool_unsupported_media")
        if not data:
            raise _McpMappingError("tool_mcp_invalid_result")
        block = {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": cast(SafeImageMediaType, mime_type),
                "data": data,
            },
        }
        encoded_size = _json_size(block, self.remaining_block_bytes())
        try:
            decoded = base64.b64decode(data, validate=True)
        except (ValueError, binascii.Error):
            raise _McpMappingError("tool_mcp_invalid_result") from None
        if not decoded:
            raise _McpMappingError("tool_mcp_invalid_result")
        self._append(block, encoded_size)

    def finish(self) -> list[dict[str, Any]]:
        if not self.blocks:
            self.add_text("(no output)")
        return self.blocks


def _resource_descriptor(resource: object) -> dict[str, str]:
    uri = getattr(resource, "uri", None)
    if uri is None:
        raise _McpMappingError("tool_mcp_invalid_result")
    descriptor = {"uri": str(uri)}
    mime_type = getattr(resource, "mimeType", None)
    if mime_type is not None:
        if not isinstance(mime_type, str):
            raise _McpMappingError("tool_mcp_invalid_result")
        descriptor["mimeType"] = mime_type
    return descriptor


def _map_resource_contents(
    builder: _ResultBuilder,
    resource: types.TextResourceContents | types.BlobResourceContents,
) -> None:
    descriptor = _bounded_canonical_json(
        _resource_descriptor(resource),
        builder.remaining_block_bytes(),
    )
    if isinstance(resource, types.TextResourceContents):
        builder.add_text("[mcp_resource]\n", descriptor, "\n", resource.text)
        return
    if isinstance(resource, types.BlobResourceContents):
        builder.add_text("[mcp_resource_image]\n", descriptor)
        builder.add_image(resource.blob, resource.mimeType)
        return
    raise _McpMappingError("tool_mcp_invalid_result")


def _map_content_block(builder: _ResultBuilder, block: object) -> None:
    if isinstance(block, types.TextContent):
        builder.add_text(block.text)
        return
    if isinstance(block, types.ImageContent):
        builder.add_image(block.data, block.mimeType)
        return
    if isinstance(block, types.AudioContent):
        raise _McpMappingError("tool_unsupported_media")
    if isinstance(block, types.ResourceLink):
        payload = block.model_dump(mode="json", by_alias=True, exclude_none=True)
        builder.add_canonical_text("[mcp_resource_link]\n", payload)
        return
    if isinstance(block, types.EmbeddedResource):
        _map_resource_contents(builder, block.resource)
        return
    raise _McpMappingError("tool_mcp_invalid_result")


def map_tool_result(
    result: types.CallToolResult,
    *,
    request_id: UUID,
    max_result_bytes: int,
) -> ToolOutput:
    """Map a complete tool result without ever returning partial content."""

    try:
        builder = _ResultBuilder(
            request_id=request_id,
            max_result_bytes=max_result_bytes,
            is_error=result.isError,
            code="tool_mcp_error" if result.isError else None,
        )
        for block in result.content:
            _map_content_block(builder, block)
        if result.structuredContent is not None:
            builder.add_canonical_text(
                "[mcp_structured_content]\n",
                result.structuredContent,
            )
        blocks = builder.finish()
    except _McpMappingError as exc:
        return _mapping_failure(exc.code)
    except (McpCatalogError, TypeError, UnicodeError, ValueError):
        return _mapping_failure("tool_mcp_invalid_result")
    return ToolOutput(
        blocks,
        is_error=result.isError,
        code="tool_mcp_error" if result.isError else None,
    )


def map_resource_result(
    result: types.ReadResourceResult,
    *,
    request_id: UUID,
    max_result_bytes: int,
) -> ToolOutput:
    """Map all returned resource contents atomically."""

    try:
        builder = _ResultBuilder(
            request_id=request_id,
            max_result_bytes=max_result_bytes,
            is_error=False,
            code=None,
        )
        for resource in result.contents:
            _map_resource_contents(builder, resource)
        blocks = builder.finish()
    except _McpMappingError as exc:
        return _mapping_failure(exc.code)
    except (McpCatalogError, TypeError, UnicodeError, ValueError):
        return _mapping_failure("tool_mcp_invalid_result")
    return ToolOutput(blocks)


def map_prompt_result(
    result: types.GetPromptResult,
    *,
    request_id: UUID,
    max_result_bytes: int,
) -> ToolOutput:
    """Map prompt messages while preserving role, message, and content order."""

    try:
        builder = _ResultBuilder(
            request_id=request_id,
            max_result_bytes=max_result_bytes,
            is_error=False,
            code=None,
        )
        for message in result.messages:
            builder.add_text("[mcp_prompt_message role=", message.role, "]")
            _map_content_block(builder, message.content)
        blocks = builder.finish()
    except _McpMappingError as exc:
        return _mapping_failure(exc.code)
    except (McpCatalogError, TypeError, UnicodeError, ValueError):
        return _mapping_failure("tool_mcp_invalid_result")
    return ToolOutput(blocks)


def enforce_result_credit(
    output: ToolOutput,
    *,
    request_id: UUID,
    max_result_bytes: int,
) -> ToolOutput:
    """Apply credit to the encoded final ToolResult frame, not only MCP content."""

    from openoctopus_client.protocol import ToolResult, encode_frame

    frame = ToolResult(
        id=request_id,
        content=cast(Any, output.content),
        is_error=output.is_error,
        code=output.code,
    )
    if len(encode_frame(frame).encode("utf-8")) <= max_result_bytes:
        return output
    return fail(
        "tool_result_too_large",
        "Tool result exceeded its response credit",
    )
