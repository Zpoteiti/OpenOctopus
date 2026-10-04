from uuid import uuid4

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelResponse,
    RetryPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import FunctionModel

from openctopus_server.tools.sdk import toolset_from_schemas


@pytest.mark.parametrize("bad_args", [
    {"device": "another-users-device", "count": 1},
    {"device": "server", "count": "not an integer"},
    {"device": "server", "count": 0},
    {"device": "server", "count": 1, "unexpected": True},
])
async def test_sdk_validates_before_dispatch_and_preserves_trusted_identity(bad_args):
    calls = []
    user_id = str(uuid4())
    schema = {
        "name": "routed",
        "input_schema": {
            "type": "object", "additionalProperties": False,
            "properties": {"device": {"enum": ["server"]}, "count": {"type": "integer", "minimum": 1}},
            "required": ["device", "count"],
        },
    }

    async def execute(name, args, ctx):
        calls.append((name, args, ctx.deps, ctx.tool_call_id))
        return "success"

    def model(messages, info):
        if any(isinstance(part, ToolReturnPart) for message in messages for part in message.parts):
            return ModelResponse([TextPart("done")])
        retry = any(isinstance(part, RetryPromptPart) for message in messages for part in message.parts)
        return ModelResponse([ToolCallPart(
            "routed", {"device": "server", "count": 1} if retry else bad_args,
            tool_call_id="corrected" if retry else "bad",
        )])

    agent = Agent(FunctionModel(model), deps_type=str, toolsets=[toolset_from_schemas([schema], execute)])
    result = await agent.run("work", deps=user_id)
    assert result.output == "done"
    assert calls == [("routed", {"device": "server", "count": 1}, user_id, "corrected")]
