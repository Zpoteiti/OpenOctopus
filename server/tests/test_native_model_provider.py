import copy
import json

import httpx2
import pytest
from pydantic_ai import Agent
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import (
    BinaryContent,
    ModelRequest,
    ModelResponse,
    SystemPromptPart,
    TextPart,
    ThinkingPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models import ModelRequestParameters
from pydantic_ai.models.function import FunctionModel

from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.models import build_model, compatible_history
from openctopus_server.provider.policy import PolicyModel, without_images


@pytest.mark.parametrize("protocol", ["anthropic", "openai", "openrouter"])
async def test_native_provider_uses_configured_endpoint(protocol):
    requests = []
    model_name = "openai/test-model" if protocol == "openrouter" else "test-model"

    def respond(request):
        requests.append(request)
        if protocol == "anthropic":
            body = {"id": "m1", "type": "message", "role": "assistant", "model": model_name,
                    "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn",
                    "usage": {"input_tokens": 1, "output_tokens": 1}}
        else:
            body = {"id": "m1", "object": "chat.completion", "created": 1, "model": model_name,
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"},
                                 "finish_reason": "stop"}]}
            if protocol == "openrouter":
                body["provider"] = "OpenAI"
        return httpx2.Response(200, json=body)

    config = ProviderConfig(
        endpoint="https://enterprise.test/gateway", api_key="test-key", model=model_name,
        max_output_tokens=1024, max_concurrent_requests=0, max_context_tokens=None,
        protocol=protocol,
    )
    # Supply an offline transport to the actual provider SDK; no monkeypatched
    # model request/parser and no call to an external endpoint.
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(respond))
    model = build_model(config, http_client=client)
    try:
        response = await model.request([ModelRequest([SystemPromptPart("first instruction"), SystemPromptPart("second instruction"), UserPromptPart("hello")])], {"max_tokens": 1024}, ModelRequestParameters())
        assert response.parts == [TextPart("ok")]
        assert requests[0].url.host == "enterprise.test"
        assert requests[0].url.path == (
            "/gateway/v1/messages" if protocol == "anthropic" else "/gateway/v1/chat/completions"
        )
        assert json.loads(requests[0].content)["model"] == model_name
        if protocol != "anthropic":
            wire = json.loads(requests[0].content)["messages"]
            assert [m["role"] for m in wire] == ["system", "user"]
            assert "first instruction" in wire[0]["content"] and "second instruction" in wire[0]["content"]
    finally:
        await client.aclose()


def test_reasoning_is_retained_only_for_its_endpoint_and_model():
    message = ModelResponse(
        [ThinkingPart("reason", signature="opaque"), TextPart("answer")],
        model_name="m", provider_url="https://tenant-a/v1/", provider_name="openai",
    )
    assert compatible_history([message], provider_url="https://tenant-a/v1", model_name="m", provider_name="openai") == [message]
    for endpoint, model, provider in [
        ("https://tenant-b/v1", "m", "openai"),
        ("https://tenant-a/v1", "other", "openai"),
        ("https://tenant-a/v1", "m", "openrouter"),
    ]:
        result = compatible_history([message], provider_url=endpoint, model_name=model, provider_name=provider)
        assert result[0].parts == [TextPart("answer")]
    assert message.parts[0].signature == "opaque"


def test_image_projection_preserves_original_media_and_image_shaped_arguments():
    image = BinaryContent(b"image bytes", media_type="image/png")
    messages = [
        ModelRequest([UserPromptPart(["prompt", image])]),
        ModelResponse([ToolCallPart("tool", {"type": "image", "source": "keep"}, tool_call_id="t")]),
        ModelRequest([ToolReturnPart("tool", ["result", image], tool_call_id="t")]),
    ]
    original = copy.deepcopy(messages)
    result = without_images(messages)
    assert result[0].parts[0].content == ["prompt"]
    assert result[2].parts[0].content == ["result"]
    assert result[1] is messages[1]
    assert messages == original


async def test_native_stream_image_fallback_has_fresh_retry_budget():
    observed = []
    image = BinaryContent(b"image", media_type="image/png")
    history = [ModelRequest([UserPromptPart(["hello", image])])]

    async def stream(messages, info):
        observed.append(copy.deepcopy(messages))
        attempt = len(observed)
        if attempt in {1, 2, 4, 5}:
            raise ModelHTTPError(503, "m", {"error": "busy"})
        if attempt == 3:
            raise ModelHTTPError(400, "m", {"error": "images unsupported"})
        yield "ok"

    model = PolicyModel(FunctionModel(stream_function=stream))
    agent = Agent(model)
    result = await agent.run(message_history=history, event_stream_handler=_consume)
    assert result.output == "ok"
    assert len(observed) == 6
    assert image in observed[0][0].parts[0].content
    assert observed[-1][0].parts[0].content == ["hello"]
    assert image in history[0].parts[0].content


@pytest.mark.parametrize("thinking", [False, True])
async def test_native_stream_never_retries_after_visible_output(thinking):
    calls = 0

    async def stream(messages, info):
        from pydantic_ai.models.function import DeltaThinkingPart

        nonlocal calls
        calls += 1
        yield {0: DeltaThinkingPart(content="visible")} if thinking else "visible"
        raise ModelHTTPError(503, "m", {"error": "busy"})

    agent = Agent(PolicyModel(FunctionModel(stream_function=stream)))
    with pytest.raises(ModelHTTPError):
        await agent.run("hello", event_stream_handler=_consume)
    assert calls == 1


async def _consume(ctx, stream):
    async for _ in stream:
        pass
