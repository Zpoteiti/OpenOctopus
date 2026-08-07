from __future__ import annotations

import hashlib
import json
import os
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

_ASSET_FILENAME = "fb374d419588a4632f3f557e76b4b70aebbca790"
_ASSET_SHA256 = "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d"
_BINARY_IMAGE_TOKENS = 2_000
_MAX_ENCODE_CHARS = 64 * 1024
_MAX_JSON_SOURCE_CHARS = _MAX_ENCODE_CHARS // 6


class Encoding(Protocol):
    def encode(
        self,
        text: str,
        *,
        disallowed_special: tuple[()],
    ) -> list[int]: ...


class TokenEstimator:
    """Provider-independent request-size estimator using one fixed encoding."""

    def __init__(self, encoding: Encoding) -> None:
        self._encoding = encoding

    def count_text(self, text: str) -> int:
        counter = _BoundedTokenCounter(self._encoding)
        counter.write(text)
        return counter.finish()

    def estimate_request(
        self,
        *,
        system: str,
        messages: Sequence[Mapping[str, Any]],
        tools: Sequence[Mapping[str, Any]],
    ) -> int:
        counter = _BoundedTokenCounter(self._encoding)
        counter.write('{"messages":')
        image_count = _write_json(messages, counter, redact_images=True)
        counter.write(',"system":')
        _write_json(system, counter, redact_images=False)
        counter.write(',"tools":')
        _write_json(tools, counter, redact_images=False)
        counter.write("}")
        return counter.finish() + image_count * _BINARY_IMAGE_TOKENS


class _BoundedTokenCounter:
    def __init__(self, encoding: Encoding) -> None:
        self._encoding = encoding
        self._parts: list[str] = []
        self._length = 0
        self._tokens = 0

    def write(self, text: str) -> None:
        offset = 0
        while offset < len(text):
            remaining = _MAX_ENCODE_CHARS - self._length
            end = min(offset + remaining, len(text))
            self._parts.append(text[offset:end])
            self._length += end - offset
            offset = end
            if self._length == _MAX_ENCODE_CHARS:
                self._flush()

    def finish(self) -> int:
        self._flush()
        return self._tokens

    def _flush(self) -> None:
        if not self._parts:
            return
        text = "".join(self._parts)
        self._tokens += len(self._encoding.encode(text, disallowed_special=()))
        self._parts.clear()
        self._length = 0


_estimator: TokenEstimator | None = None
_initializer_lock = threading.Lock()


def initialize_token_estimator(*, asset_path: Path | None = None) -> TokenEstimator:
    """Verify the packaged o200k asset and initialize tiktoken without network I/O."""
    global _estimator

    selected_path = asset_path or _default_asset_path()
    try:
        asset = selected_path.read_bytes()
    except FileNotFoundError as exc:
        raise RuntimeError(f"Packaged o200k tokenizer asset is missing: {selected_path}") from exc
    if hashlib.sha256(asset).hexdigest() != _ASSET_SHA256:
        raise RuntimeError("Packaged o200k tokenizer asset checksum does not match")

    with _initializer_lock:
        if _estimator is not None:
            return _estimator
        os.environ["TIKTOKEN_CACHE_DIR"] = str(selected_path.parent)
        try:
            import tiktoken

            encoding = tiktoken.get_encoding("o200k_base")
        except Exception as exc:
            raise RuntimeError("Failed to initialize the packaged o200k tokenizer") from exc
        _estimator = TokenEstimator(encoding)
        return _estimator


def get_token_estimator() -> TokenEstimator:
    return _estimator or initialize_token_estimator()


def count_text_tokens(text: str) -> int:
    return get_token_estimator().count_text(text)


def estimate_request_tokens(
    *,
    system: str,
    messages: Sequence[Mapping[str, Any]],
    tools: Sequence[Mapping[str, Any]],
) -> int:
    return get_token_estimator().estimate_request(
        system=system,
        messages=messages,
        tools=tools,
    )


def _default_asset_path() -> Path:
    return Path(__file__).resolve().parents[1] / "assets" / "tiktoken" / _ASSET_FILENAME


def _write_json(
    value: Any,
    counter: _BoundedTokenCounter,
    *,
    redact_images: bool,
    omit_data_key: bool = False,
) -> int:
    if isinstance(value, Mapping):
        image = redact_images and value.get("type") == "image"
        image_count = 1 if image else 0
        counter.write("{")
        first = True
        for key, item in sorted(value.items(), key=lambda pair: str(pair[0])):
            if (image or omit_data_key) and key == "data":
                continue
            if not first:
                counter.write(",")
            first = False
            _write_json_string(str(key), counter)
            counter.write(":")
            image_count += _write_json(
                item,
                counter,
                redact_images=redact_images,
                omit_data_key=image and key == "source" and isinstance(item, Mapping),
            )
        counter.write("}")
        return image_count
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        counter.write("[")
        image_count = 0
        for index, item in enumerate(value):
            if index:
                counter.write(",")
            image_count += _write_json(item, counter, redact_images=redact_images)
        counter.write("]")
        return image_count
    if isinstance(value, str):
        _write_json_string(value, counter)
    else:
        counter.write(
            json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
    return 0


def _write_json_string(value: str, counter: _BoundedTokenCounter) -> None:
    counter.write('"')
    for offset in range(0, len(value), _MAX_JSON_SOURCE_CHARS):
        serialized = json.dumps(
            value[offset : offset + _MAX_JSON_SOURCE_CHARS],
            ensure_ascii=False,
        )
        counter.write(serialized[1:-1])
    counter.write('"')
