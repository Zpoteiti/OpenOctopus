import asyncio
import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.chat.token_estimator import (
    TokenEstimator,
    initialize_token_estimator,
)


class _RecordingEncoding:
    def __init__(self) -> None:
        self.inputs: list[str] = []

    def encode(self, text: str, *, disallowed_special: tuple[()] = ()) -> list[int]:
        del disallowed_special
        self.inputs.append(text)
        return list(range(len(text)))


class _BoundedRecordingEncoding:
    def __init__(self) -> None:
        self.call_count = 0
        self.max_input_length = 0
        self.saw_image_data = False

    def encode(self, text: str, *, disallowed_special: tuple[()] = ()) -> list[int]:
        del disallowed_special
        self.call_count += 1
        self.max_input_length = max(self.max_input_length, len(text))
        self.saw_image_data = self.saw_image_data or "secret-image-data" in text
        return [0] if text else []


def test_request_estimate_covers_structure_and_replaces_binary_image_data() -> None:
    encoding = _RecordingEncoding()
    estimator = TokenEstimator(encoding)
    image_data = "secret-base64" * 10_000

    estimate = estimator.estimate_request(
        system="system instructions",
        messages=[
            {
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": "private reasoning"}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": image_data,
                        },
                    },
                    {"type": "image", "data": "direct-secret"},
                    {"type": "metadata", "data": "ordinary-data"},
                    {"type": "tool_result", "content": "result text"},
                ],
            },
        ],
        tools=[
            {
                "name": "example",
                "description": "tool description",
                "input_schema": {"type": "object", "properties": {"x": {"type": "string"}}},
            }
        ],
    )

    serialized = "".join(encoding.inputs)
    expected = json.dumps(
        {
            "system": "system instructions",
            "messages": [
                {
                    "role": "assistant",
                    "content": [{"type": "thinking", "thinking": "private reasoning"}],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": "image/png"},
                        },
                        {"type": "image"},
                        {"type": "metadata", "data": "ordinary-data"},
                        {"type": "tool_result", "content": "result text"},
                    ],
                },
            ],
            "tools": [
                {
                    "name": "example",
                    "description": "tool description",
                    "input_schema": {
                        "type": "object",
                        "properties": {"x": {"type": "string"}},
                    },
                }
            ],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    assert serialized == expected
    assert "system instructions" in serialized
    assert "private reasoning" in serialized
    assert "result text" in serialized
    assert "tool description" in serialized
    assert image_data not in serialized
    assert "direct-secret" not in serialized
    assert "ordinary-data" in serialized
    assert "image/png" in serialized
    assert estimate == len(expected) + 4_000


def test_large_request_is_streamed_through_bounded_encoding_inputs(monkeypatch) -> None:
    encoding = _BoundedRecordingEncoding()
    estimator = TokenEstimator(encoding)
    largest_json_string = 0
    real_dumps = json.dumps

    def bounded_dumps(value: Any, **kwargs: Any) -> str:
        nonlocal largest_json_string
        if isinstance(value, str):
            largest_json_string = max(largest_json_string, len(value))
        return real_dumps(value, **kwargs)

    monkeypatch.setattr(
        "openctopus_server.chat.token_estimator.json.dumps",
        bounded_dumps,
    )
    large_text = "中文 and English <tag>\n" * 200_000
    image_data = "secret-image-data" * 300_000

    estimate = estimator.estimate_request(
        system=large_text,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": image_data,
                        },
                    },
                    {"type": "text", "text": large_text},
                ],
            }
        ],
        tools=[{"name": "large", "description": large_text}],
    )

    assert estimate > 2_000
    assert encoding.call_count > 3
    assert encoding.max_input_length <= 64 * 1024
    assert largest_json_string <= 64 * 1024
    assert not encoding.saw_image_data


async def test_chat_runtime_offloads_synchronous_request_estimator() -> None:
    runtime = object.__new__(ChatRuntime)

    def blocking_estimator(**kwargs: Any) -> int:
        del kwargs
        time.sleep(0.25)
        return 123

    runtime._estimate_request_tokens = blocking_estimator
    started = asyncio.get_running_loop().time()
    estimation = asyncio.create_task(
        runtime._estimate_tokens(system="system", messages=[], tools=[])
    )

    await asyncio.sleep(0.02)

    assert asyncio.get_running_loop().time() - started < 0.15
    assert await estimation == 123


async def test_chat_runtime_keeps_async_request_estimator_on_event_loop() -> None:
    runtime = object.__new__(ChatRuntime)
    event_loop_thread = threading.get_ident()

    async def async_estimator(**kwargs: Any) -> int:
        del kwargs
        assert threading.get_ident() == event_loop_thread
        await asyncio.sleep(0)
        return 456

    runtime._estimate_request_tokens = async_estimator

    assert await runtime._estimate_tokens(system="system", messages=[], tools=[]) == 456


async def test_chat_runtime_repeated_cancellation_waits_for_synchronous_estimator() -> None:
    runtime = object.__new__(ChatRuntime)
    entered = threading.Event()
    release = threading.Event()

    def blocking_estimator(**kwargs: Any) -> int:
        del kwargs
        entered.set()
        assert release.wait(1)
        return 123

    runtime._estimate_request_tokens = blocking_estimator
    estimation = asyncio.create_task(
        runtime._estimate_tokens(system="system", messages=[], tools=[])
    )
    assert await asyncio.to_thread(entered.wait, 1)

    estimation.cancel()
    await asyncio.sleep(0)
    estimation.cancel()
    await asyncio.sleep(0)
    estimation.cancel()
    await asyncio.sleep(0)

    assert not estimation.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await estimation


def test_initializer_rejects_missing_or_mismatched_asset(tmp_path: Path) -> None:
    missing = tmp_path / "missing.tiktoken"
    with pytest.raises(RuntimeError, match="missing"):
        initialize_token_estimator(asset_path=missing)

    mismatched = tmp_path / "mismatch.tiktoken"
    mismatched.write_bytes(b"wrong")
    with pytest.raises(RuntimeError, match="checksum"):
        initialize_token_estimator(asset_path=mismatched)


def test_vendored_o200k_asset_initializes_without_network(monkeypatch) -> None:
    import tiktoken.load

    def reject_network(*args, **kwargs):
        del args, kwargs
        raise AssertionError("tokenizer initialization attempted network access")

    monkeypatch.setattr("urllib.request.urlopen", reject_network)
    monkeypatch.setattr(tiktoken.load, "read_file", reject_network)

    estimator = initialize_token_estimator()

    assert estimator.count_text("hello 世界") > 0
