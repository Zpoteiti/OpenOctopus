"""Translate OO's stored/UI blocks at the native model message boundary."""

import base64
from collections.abc import Sequence
from typing import Any

from pydantic_ai.messages import (
    BinaryContent,
    ModelMessage,
    ModelRequest,
    ModelRequestPart,
    ModelResponse,
    ModelResponsePart,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)


def prompt_content(content: str | list[dict[str, Any]]) -> str | list[str | BinaryContent]:
    if isinstance(content, str):
        return content
    result: list[str | BinaryContent] = []
    for block in content:
        if block["type"] == "text":
            result.append(block["text"])
        elif block["type"] == "image":
            source = block["source"]
            result.append(BinaryContent(base64.b64decode(source["data"]), media_type=source["media_type"]))
        else:
            raise ValueError(f"Unsupported prompt block: {block['type']}")
    return result


def model_messages(
    messages: Sequence[dict[str, Any]], *, model_name: str, provider_name: str, provider_url: str,
    previous: Sequence[ModelMessage] = (),
) -> list[ModelMessage]:
    """History has already been authorized and projected for this provider identity."""
    result: list[ModelMessage] = []
    tool_names = {part.tool_call_id: part.tool_name for message in previous for part in message.parts
                  if isinstance(part, ToolCallPart)}
    for message in messages:
        if message["role"] == "assistant":
            response: list[ModelResponsePart] = []
            for block in message["content"]:
                kind = block["type"]
                if kind == "text":
                    response.append(TextPart(block["text"]))
                elif kind in {"thinking", "redacted_thinking"}:
                    response.append(ThinkingPart(
                        block.get("thinking", ""),
                        signature=block.get("signature") if kind == "thinking" else block.get("data"),
                        id="redacted_thinking" if kind == "redacted_thinking" else block.get("_sdk", {}).get("id"),
                        provider_name=provider_name,
                        provider_details=block.get("_sdk", {}).get("provider_details"),
                    ))
                elif kind == "tool_use":
                    tool_names[block["id"]] = block["name"]
                    response.append(ToolCallPart(
                        block["name"], block["input"], tool_call_id=block["id"],
                    ))
                else:
                    raise ValueError(f"Unsupported response block: {kind}")
            result.append(ModelResponse(
                response, model_name=model_name, provider_name=provider_name, provider_url=provider_url,
            ))
        else:
            request: list[ModelRequestPart] = []
            prompt: list[dict[str, Any]] = []
            for block in message["content"]:
                if block["type"] == "tool_result":
                    if prompt:
                        request.append(UserPromptPart(prompt_content(prompt)))
                        prompt = []
                    request.append(ToolReturnPart(
                        tool_names[block["tool_use_id"]], prompt_content(block["content"]),
                        tool_call_id=block["tool_use_id"],
                        outcome="failed" if block.get("is_error") else "success",
                    ))
                else:
                    prompt.append(block)
            if prompt:
                request.append(UserPromptPart(prompt_content(prompt)))
            result.append(ModelRequest(request))
    return result


def response_blocks(response: ModelResponse) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for part in response.parts:
        if isinstance(part, TextPart):
            blocks.append({"type": "text", "text": part.content})
        elif isinstance(part, ThinkingPart):
            if part.id == "redacted_thinking":
                blocks.append({"type": "redacted_thinking", "data": part.signature})
            else:
                block: dict[str, Any] = {
                    "type": "thinking", "thinking": part.content, "signature": part.signature,
                }
                if part.id or part.provider_details:
                    block["_sdk"] = {"id": part.id, "provider_details": part.provider_details}
                blocks.append(block)
        elif isinstance(part, ToolCallPart):
            blocks.append({"type": "tool_use", "id": part.tool_call_id,
                           "name": part.tool_name, "input": part.args_as_dict()})
        else:
            raise ValueError(f"Unsupported response part: {part.part_kind}")
    if not blocks:
        raise ValueError("Provider returned an empty assistant message")
    return blocks
