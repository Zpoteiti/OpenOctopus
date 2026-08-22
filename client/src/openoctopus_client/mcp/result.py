"""Deterministic MCP result mapping into OpenOctopus safe result blocks."""

from __future__ import annotations

import base64
import binascii
from typing import Any, Literal, cast
from uuid import UUID

from mcp import types

from openoctopus_client.mcp.catalog import McpCatalogError, canonical_json_bytes
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
    if code == "tool_unsupported_media":
        return fail(code, "The MCP result contains unsupported media; all content was discarded")
    return fail(code, "The MCP server returned an invalid result; all content was discarded")


def _canonical_json(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _text_block(value: str) -> dict[str, Any]:
    return {"type": "text", "text": value}


def _image_block(data: str, mime_type: str | None) -> dict[str, Any]:
    if mime_type not in _IMAGE_MEDIA_TYPES:
        raise _McpMappingError("tool_unsupported_media")
    try:
        decoded = base64.b64decode(data, validate=True)
    except (ValueError, binascii.Error):
        raise _McpMappingError("tool_mcp_invalid_result") from None
    if not data or not decoded:
        raise _McpMappingError("tool_mcp_invalid_result")
    return {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": cast(SafeImageMediaType, mime_type),
            "data": data,
        },
    }


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
    resource: types.TextResourceContents | types.BlobResourceContents,
) -> list[dict[str, Any]]:
    descriptor = _canonical_json(_resource_descriptor(resource))
    if isinstance(resource, types.TextResourceContents):
        return [_text_block(f"[mcp_resource]\n{descriptor}\n{resource.text}")]
    if isinstance(resource, types.BlobResourceContents):
        return [
            _text_block(f"[mcp_resource_image]\n{descriptor}"),
            _image_block(resource.blob, resource.mimeType),
        ]
    raise _McpMappingError("tool_mcp_invalid_result")


def _map_content_block(block: object) -> list[dict[str, Any]]:
    if isinstance(block, types.TextContent):
        return [_text_block(block.text)]
    if isinstance(block, types.ImageContent):
        return [_image_block(block.data, block.mimeType)]
    if isinstance(block, types.AudioContent):
        raise _McpMappingError("tool_unsupported_media")
    if isinstance(block, types.ResourceLink):
        payload = block.model_dump(mode="json", by_alias=True, exclude_none=True)
        return [_text_block(f"[mcp_resource_link]\n{_canonical_json(payload)}")]
    if isinstance(block, types.EmbeddedResource):
        return _map_resource_contents(block.resource)
    raise _McpMappingError("tool_mcp_invalid_result")


def _finish_mapping(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return blocks or [_text_block("(no output)")]


def map_tool_result(result: types.CallToolResult) -> ToolOutput:
    """Map a complete tool result without ever returning partial content."""

    try:
        blocks: list[dict[str, Any]] = []
        for block in result.content:
            blocks.extend(_map_content_block(block))
        if result.structuredContent is not None:
            blocks.append(
                _text_block(
                    "[mcp_structured_content]\n" + _canonical_json(result.structuredContent)
                )
            )
        blocks = _finish_mapping(blocks)
    except _McpMappingError as exc:
        return _mapping_failure(exc.code)
    except (McpCatalogError, TypeError, UnicodeError, ValueError):
        return _mapping_failure("tool_mcp_invalid_result")
    return ToolOutput(
        blocks,
        is_error=result.isError,
        code="tool_mcp_error" if result.isError else None,
    )


def map_resource_result(result: types.ReadResourceResult) -> ToolOutput:
    """Map all returned resource contents atomically."""

    try:
        blocks: list[dict[str, Any]] = []
        for resource in result.contents:
            blocks.extend(_map_resource_contents(resource))
        blocks = _finish_mapping(blocks)
    except _McpMappingError as exc:
        return _mapping_failure(exc.code)
    except (McpCatalogError, TypeError, UnicodeError, ValueError):
        return _mapping_failure("tool_mcp_invalid_result")
    return ToolOutput(blocks)


def map_prompt_result(result: types.GetPromptResult) -> ToolOutput:
    """Map prompt messages while preserving role, message, and content order."""

    try:
        blocks: list[dict[str, Any]] = []
        for message in result.messages:
            blocks.append(_text_block(f"[mcp_prompt_message role={message.role}]"))
            blocks.extend(_map_content_block(message.content))
        blocks = _finish_mapping(blocks)
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
