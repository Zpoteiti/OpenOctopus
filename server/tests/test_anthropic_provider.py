import json
from typing import Any

import anthropic
import httpx
import pytest
from anthropic import AsyncAnthropic

from openctopus_server.provider.anthropic import (
    AnthropicProvider,
    ProviderInvocationError,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter


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
                    "type": "message_delta",
                    "delta": {
                        "stop_reason": "end_turn",
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
    client = AsyncAnthropic(
        api_key="fake-key",
        base_url="http://fake.test/v1",
        max_retries=0,
        http_client=http_client,
    )
    config = ProviderConfig(
        endpoint="http://fake.test/v1",
        api_key="fake-key",
        model="fake-model",
        max_output_tokens=16384,
        max_concurrent_requests=0,
        max_context_tokens=None,
    )
    provider = AnthropicProvider(config, client=client)
    deltas: list[tuple[str, str]] = []

    async def on_delta(channel: str, text: str) -> None:
        deltas.append((channel, text))

    result = await provider.stream_turn(
        config=config,
        system="system",
        messages=[{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
        effort=None,
        limiter=ProviderLimiter(),
        on_delta=on_delta,  # type: ignore[arg-type]
    )

    assert captured["path"].endswith("/messages")
    assert captured["body"]["max_tokens"] == 16384
    assert captured["body"]["thinking"] == {"type": "disabled"}
    assert deltas == [("text", "hello")]
    assert result.content == [{"type": "text", "text": "hello"}]
    await provider.close()


async def test_transient_retry_stops_after_first_delta(monkeypatch):
    config = ProviderConfig(
        endpoint="http://fake.test/v1",
        api_key="fake-key",
        model="fake-model",
        max_output_tokens=16384,
        max_concurrent_requests=0,
        max_context_tokens=None,
    )
    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500, request=request)),
        trust_env=False,
    )
    provider = AnthropicProvider(
        config,
        client=AsyncAnthropic(
            api_key=config.api_key,
            base_url=config.endpoint,
            max_retries=0,
            http_client=http_client,
        ),
    )
    calls = 0

    async def failing_attempt(**kwargs):
        nonlocal calls
        calls += 1
        await kwargs["on_delta"]("text", "visible")
        request = httpx.Request("POST", "http://fake.test/v1/messages")
        raise anthropic.APIConnectionError(request=request)

    monkeypatch.setattr(provider, "_stream_attempt", failing_attempt)
    with pytest.raises(ProviderInvocationError):
        await provider.stream_turn(
            config=config,
            system="system",
            messages=[{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
            effort=None,
            limiter=ProviderLimiter(),
            on_delta=lambda channel, text: _noop_delta(),
        )
    assert calls == 1
    await provider.close()


async def test_transient_failure_retries_before_first_delta(monkeypatch):
    config = ProviderConfig(
        endpoint="http://fake.test/v1",
        api_key="fake-key",
        model="fake-model",
        max_output_tokens=16384,
        max_concurrent_requests=0,
        max_context_tokens=None,
    )
    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500, request=request)),
        trust_env=False,
    )
    provider = AnthropicProvider(
        config,
        client=AsyncAnthropic(
            api_key=config.api_key,
            base_url=config.endpoint,
            max_retries=0,
            http_client=http_client,
        ),
    )
    calls = 0

    async def retrying_attempt(**kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            request = httpx.Request("POST", "http://fake.test/v1/messages")
            raise anthropic.APIConnectionError(request=request)
        return [{"type": "text", "text": "done"}]

    async def no_sleep(delay: float) -> None:
        return None

    monkeypatch.setattr(provider, "_stream_attempt", retrying_attempt)
    monkeypatch.setattr(
        "openctopus_server.provider.anthropic.asyncio.sleep",
        no_sleep,
    )
    result = await provider.stream_turn(
        config=config,
        system="system",
        messages=[{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
        effort=None,
        limiter=ProviderLimiter(),
        on_delta=lambda channel, text: _noop_delta(),
    )
    assert calls == 3
    assert result.content == [{"type": "text", "text": "done"}]
    await provider.close()


async def test_image_fallback_starts_a_fresh_retry_budget(monkeypatch):
    config = ProviderConfig(
        endpoint="http://fake.test/v1",
        api_key="fake-key",
        model="fake-model",
        max_output_tokens=16384,
        max_concurrent_requests=0,
        max_context_tokens=None,
    )
    http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500, request=request)),
        trust_env=False,
    )
    provider = AnthropicProvider(
        config,
        client=AsyncAnthropic(
            api_key=config.api_key,
            base_url=config.endpoint,
            max_retries=0,
            http_client=http_client,
        ),
    )
    projected_block_types: list[list[str]] = []
    delays: list[float] = []

    class ImageCompatibilityError(Exception):
        status_code = 415

    async def failing_attempt(**kwargs):
        projected_block_types.append([block["type"] for block in kwargs["messages"][0]["content"]])
        attempt = len(projected_block_types)
        if attempt in {1, 2, 4, 5}:
            request = httpx.Request("POST", "http://fake.test/v1/messages")
            raise anthropic.APIConnectionError(request=request)
        if attempt == 3:
            raise ImageCompatibilityError("image input is unsupported")
        return [{"type": "text", "text": "done"}]

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(provider, "_stream_attempt", failing_attempt)
    monkeypatch.setattr(
        "openctopus_server.provider.anthropic.asyncio.sleep",
        record_sleep,
    )
    result = await provider.stream_turn(
        config=config,
        system="system",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "describe this"},
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": "aW1hZ2U=",
                        },
                    },
                ],
            }
        ],
        effort=None,
        limiter=ProviderLimiter(),
        on_delta=lambda channel, text: _noop_delta(),
    )

    assert projected_block_types == [
        ["text", "image"],
        ["text", "image"],
        ["text", "image"],
        ["text"],
        ["text"],
        ["text"],
    ]
    assert delays == [0.25, 0.5, 0.25, 0.5]
    assert result.content == [{"type": "text", "text": "done"}]
    await provider.close()


async def _noop_delta() -> None:
    return None
