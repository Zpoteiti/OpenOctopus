import json
from copy import deepcopy
from typing import Any

import httpx2 as httpx
import pytest

from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.runtime import ModelProvider, ProviderInvocationError


def _sse_event(payload: dict[str, Any]) -> str:
    return f"event: {payload['type']}\ndata: {json.dumps(payload)}\n\n"


async def test_real_sdk_streaming_wire_and_max_tokens():
    captured: dict[str, Any] = {}
    sse = "".join(
        [
            _sse_event(
                {
                    "type": "message_start",
                    "message": {
                        "id": "msg_test",
                        "type": "message",
                        "role": "assistant",
                        "content": [],
                        "model": "fake-model",
                        "stop_reason": None,
                        "stop_sequence": None,
                        "usage": {"input_tokens": 3, "output_tokens": 0},
                    },
                }
            ),
            _sse_event(
                {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                }
            ),
            _sse_event(
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {"type": "text_delta", "text": "hello"},
                }
            ),
            _sse_event({"type": "content_block_stop", "index": 0}),
            _sse_event(
                {
                    "type": "content_block_start",
                    "index": 1,
                    "content_block": {
                        "type": "tool_use",
                        "id": "toolu_1",
                        "name": "web_fetch",
                        "input": {},
                    },
                }
            ),
            _sse_event(
                {
                    "type": "content_block_delta",
                    "index": 1,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": '{"url":"https://example.com"}',
                    },
                }
            ),
            _sse_event({"type": "content_block_stop", "index": 1}),
            _sse_event(
                {
                    "type": "message_delta",
                    "delta": {
                        "stop_reason": "tool_use",
                        "stop_sequence": None,
                    },
                    "usage": {"output_tokens": 1},
                }
            ),
            _sse_event({"type": "message_stop"}),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            text=sse,
            headers={"content-type": "text/event-stream"},
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    config = ProviderConfig(
        endpoint="http://fake.test",
        api_key="fake-key",
        model="fake-model",
        max_output_tokens=16384,
        max_concurrent_requests=0,
        max_context_tokens=None,
    )
    provider = ModelProvider(config, http_client=http_client)
    deltas: list[tuple[str, str]] = []

    async def on_delta(channel: str, text: str) -> None:
        deltas.append((channel, text))

    tool_schema = {
        "name": "web_fetch",
        "description": "Fetch a URL",
        "input_schema": {
            "type": "object",
            "properties": {"url": {"type": "string"}},
            "required": ["url"],
        },
    }
    result = await provider.stream_turn(
        config=config,
        system="system",
        messages=[{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
        effort=None,
        limiter=ProviderLimiter(),
        on_delta=on_delta,  # type: ignore[arg-type]
        tools=[tool_schema],
    )

    assert captured["path"] == "/v1/messages"
    assert captured["body"]["max_tokens"] == 16384
    assert captured["body"]["thinking"] == {"type": "disabled"}
    assert captured["body"]["tools"] == [tool_schema]
    assert deltas == [("text", "hello")]
    assert result.content == [
        {"type": "text", "text": "hello"},
        {
            "type": "tool_use",
            "id": "toolu_1",
            "name": "web_fetch",
            "input": {"url": "https://example.com"},
        },
    ]
    await provider.close()


@pytest.mark.parametrize("include_text", [True, False])
async def test_image_fallback_strips_tool_result_images_on_wire_without_mutating_history(include_text):
    image = {
        "type": "image",
        "source": {"type": "base64", "media_type": "image/png", "data": "aW1hZ2U="},
    }
    text_content = [{"type": "text", "text": "screenshot description"}] if include_text else []
    messages = [
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "image_call",
                    "name": "screenshot",
                    "input": {"example": image},
                },
                {"type": "tool_use", "id": "text_call", "name": "read_file", "input": {}},
            ],
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "image_call",
                    "content": [*text_content, image],
                    "is_error": False,
                },
                {"type": "tool_result", "tool_use_id": "text_call", "content": "plain text"},
            ],
        },
    ]
    original = deepcopy(messages)
    requests: list[dict[str, Any]] = []
    sse = "".join(
        _sse_event(event)
        for event in [
            {
                "type": "message_start",
                "message": {
                    "id": "msg_fallback",
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": "fake-model",
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 3, "output_tokens": 0},
                },
            },
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": "done"},
            },
            {"type": "content_block_stop", "index": 0},
            {
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
            {"type": "message_stop"},
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            return httpx.Response(
                400,
                json={"error": {"type": "invalid_request_error", "message": "images unsupported"}},
            )
        return httpx.Response(200, text=sse, headers={"content-type": "text/event-stream"})

    config = ProviderConfig(
        endpoint="http://fake.test",
        api_key="fake-key",
        model="fake-model",
        max_output_tokens=16384,
        max_concurrent_requests=0,
        max_context_tokens=None,
    )
    provider = ModelProvider(config, http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False))
    try:
        result = await provider.stream_turn(
            config=config,
            system="system",
            messages=messages,
            effort=None,
            limiter=ProviderLimiter(),
            on_delta=lambda channel, text: _noop_delta(),
        )
    finally:
        await provider.close()

    first_blocks = requests[0]["messages"][1]["content"]
    second_blocks = requests[1]["messages"][1]["content"]
    assert first_blocks[0]["content"] == [*text_content, image]
    assert second_blocks[0]["content"] == (text_content or "")
    assert first_blocks[1] == second_blocks[1]
    assert requests[0]["messages"][0]["content"][0]["input"] == {"example": image}
    assert messages == original
    assert result.content == [{"type": "text", "text": "done"}]


async def _noop_delta() -> None:
    return None


@pytest.mark.parametrize("message, safe", [
    ("prompt exceeds this model's context window", True),
    ("Authorization: Bearer secret-key at http://secret-provider.test", False),
    ("internal customer record: unrelated response data", False),
    ("Error code: 400 - {'error': {'message': 'context window'}}", False),
    ("context window " + "x" * 2000, True),
])
async def test_sdk_rejection_exposes_only_bounded_context_errors(message, safe):
    config = ProviderConfig("http://secret-provider.test", "secret-key", "fake-model", 1024, 0, None)
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(
        400, json={"error": {"type": "invalid_request_error", "message": message},
                   "customer_record": "must-not-leak"},
    )))
    provider = ModelProvider(config, http_client=client)
    try:
        with pytest.raises(ProviderInvocationError) as rejected:
            await provider.stream_turn(
                config=config, system="system", messages=[{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
                effort=None, limiter=ProviderLimiter(), on_delta=lambda *_: _noop_delta(),
            )
        detail = rejected.value.safe_message
        if safe:
            assert detail.startswith("Provider rejected the request (HTTP 400): context window" if message.startswith("context") else "Provider rejected the request (HTTP 400): prompt exceeds")
            assert len(detail) <= 1000
            assert "customer_record" not in detail
        else:
            assert detail is None
    finally:
        await provider.close()


async def test_forced_tool_choice_and_reasoning_round_trip_through_sdk():
    requests = []
    parts = [
        {"type": "thinking", "thinking": "reason", "signature": "opaque"},
        {"type": "redacted_thinking", "data": "encrypted"},
        {"type": "tool_use", "id": "toolu_decision", "name": "decision", "input": {"action": "skip"}},
    ]
    events = [{"type": "message_start", "message": {
        "id": "m1", "type": "message", "role": "assistant", "model": "fake-model", "content": [],
        "stop_reason": None, "usage": {"input_tokens": 1, "output_tokens": 0},
    }}]
    for index, block in enumerate(parts):
        events.extend([
            {"type": "content_block_start", "index": index, "content_block": block},
            {"type": "content_block_stop", "index": index},
        ])
    events.extend([
        {"type": "message_delta", "delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 5}},
        {"type": "message_stop"},
    ])
    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, text="".join(_sse_event(event) for event in events), headers={"content-type": "text/event-stream"})
    config = ProviderConfig("http://provider.test", "secret", "fake-model", 1024, 0, None)
    provider = ModelProvider(config, http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond)))
    history = [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
    try:
        result = await provider.stream_turn(
            config=config, system="system", messages=history, effort=None,
            limiter=ProviderLimiter(), on_delta=lambda *_: _noop_delta(),
            tools=[{"name": "decision", "input_schema": {"type": "object"}}],
            tool_choice={"type": "tool", "name": "decision"},
        )
        assert result.content == parts
        assert requests[0]["tool_choice"] == {"type": "any"}
        assert [tool["name"] for tool in requests[0]["tools"]] == ["decision"]
        history.extend([
            {"role": "assistant", "content": result.content},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_decision", "content": "ok"}]},
        ])
        await provider.stream_turn(config=config, system="system", messages=history, effort=None,
                                   limiter=ProviderLimiter(), on_delta=lambda *_: _noop_delta())
        assert requests[1]["messages"][1]["content"][:2] == parts[:2]
    finally:
        await provider.close()
