"""Run existing product scenarios through real SDK models and the SDK agent loop."""

import asyncio
import base64
import json

from pydantic_ai.messages import (
    BinaryContent,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import DeltaThinkingPart, DeltaToolCall, FunctionModel

from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.messages import response_blocks
from openctopus_server.provider.wire_types import Effort


def blocks(content):
    if isinstance(content, str):
        return content
    return [
        {"type": "image", "source": {"type": "base64", "media_type": item.media_type,
                                    "data": base64.b64encode(item.data).decode()}}
        if isinstance(item, BinaryContent) else {"type": "text", "text": item}
        for item in content
    ]


def wire_messages(messages):
    result = []
    for message in messages:
        if isinstance(message, ModelResponse):
            result.append({"role": "assistant", "content": response_blocks(message)})
        elif isinstance(message, ModelRequest):
            content = []
            for part in message.parts:
                if isinstance(part, UserPromptPart):
                    value = blocks(part.content)
                    content.extend([{"type": "text", "text": value}] if isinstance(value, str) else value)
                elif isinstance(part, (ToolReturnPart, RetryPromptPart)):
                    content.append({"type": "tool_result", "tool_use_id": part.tool_call_id,
                                    "content": part.model_response() if isinstance(part, RetryPromptPart) else blocks(part.content),
                                    "is_error": isinstance(part, RetryPromptPart) or part.outcome == "failed"})
            if content:
                result.append({"role": "user", "content": content})
    return result


class NativeProviderFixture:
    def native_model(self, config):
        async def stream(messages, info):
            queue = asyncio.Queue()
            emitted = {"text": "", "thinking": ""}
            async def delta(channel, text):
                emitted[channel] += text
                await queue.put((channel, text))
            settings = info.model_settings or {}
            effort = Effort(settings["anthropic_effort"]) if settings.get("anthropic_effort") else None
            tools = [{"name": tool.name, "description": tool.description or "", "input_schema": tool.parameters_json_schema} for tool in info.function_tools]
            task = asyncio.create_task(self.stream_turn(
                config=config, system=info.instructions or "", messages=wire_messages(messages), effort=effort,
                limiter=ProviderLimiter(), on_delta=delta, tools=tools,
            ))
            task.add_done_callback(lambda _: queue.put_nowait(None))
            try:
                while (item := await queue.get()) is not None:
                    channel, value = item
                    yield {0: DeltaThinkingPart(content=value)} if channel == "thinking" else value
                result = task.result()
                for index, block in enumerate(result.content):
                    if block["type"] == "text":
                        text = block["text"]
                        prefix = emitted["text"]
                        if text.startswith(prefix):
                            text = text[len(prefix):]
                        if text:
                            yield text
                    elif block["type"] == "tool_use":
                        yield {index + 1: DeltaToolCall(name=block["name"], json_args=json.dumps(block["input"]), tool_call_id=block["id"])}
                    elif block["type"] == "thinking":
                        thinking = block["thinking"]
                        prefix = emitted["thinking"]
                        if thinking.startswith(prefix):
                            thinking = thinking[len(prefix):]
                        yield {index: DeltaThinkingPart(content=thinking, signature=block.get("signature"))}
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        return FunctionModel(stream_function=stream, model_name=config.model)
