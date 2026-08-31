import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import anthropic
from anthropic import AsyncAnthropic

from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort

DeltaChannel = Literal["text", "thinking"]
DeltaCallback = Callable[[DeltaChannel, str], Awaitable[None]]
ToolChoice = dict[str, str]


@dataclass(frozen=True, slots=True)
class ProviderResult:
    content: list[dict[str, Any]]
    fingerprint: str


class ProviderInvocationError(Exception):
    def __init__(
        self,
        message: str,
        *,
        protocol: bool = False,
        safe_message: str | None = None,
    ) -> None:
        self.protocol = protocol
        self.safe_message = safe_message
        super().__init__(message)


class Provider(Protocol):
    async def stream_turn(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
        on_delta: DeltaCallback,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: ToolChoice | None = None,
    ) -> ProviderResult: ...

    async def close(self) -> None: ...


class AnthropicProvider:
    def __init__(
        self,
        config: ProviderConfig,
        *,
        client: AsyncAnthropic | None = None,
    ) -> None:
        self._client = client or AsyncAnthropic(
            api_key=config.api_key,
            base_url=config.endpoint,
            max_retries=0,
        )

    async def close(self) -> None:
        await self._client.close()

    async def stream_turn(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
        on_delta: DeltaCallback,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: ToolChoice | None = None,
    ) -> ProviderResult:
        await limiter.configure(config.max_concurrent_requests)
        projected_messages = messages
        stripped_images = False

        while True:
            switch_to_text_only = False
            for projection_attempt in range(3):
                produced_delta = False

                async def emit(channel: DeltaChannel, text: str) -> None:
                    nonlocal produced_delta
                    produced_delta = True
                    await on_delta(channel, text)

                try:
                    async with limiter.slot():
                        content = await self._stream_attempt(
                            config=config,
                            system=system,
                            messages=projected_messages,
                            tools=tools or [],
                            tool_choice=tool_choice,
                            effort=effort,
                            on_delta=emit,
                        )
                    return ProviderResult(
                        content=content,
                        fingerprint=provider_fingerprint(config),
                    )
                except ProviderInvocationError:
                    raise
                except Exception as exc:
                    if produced_delta:
                        raise ProviderInvocationError(
                            f"Provider stream failed after output began: {exc}"
                        ) from exc
                    if (
                        not stripped_images
                        and _contains_images(projected_messages)
                        and _is_image_compatibility_error(exc)
                    ):
                        switch_to_text_only = True
                        break
                    if projection_attempt < 2 and _is_retryable(exc):
                        await asyncio.sleep(0.25 * (2**projection_attempt))
                        continue
                    raise ProviderInvocationError(
                        "Provider request failed",
                        safe_message=_safe_provider_rejection(exc, config=config),
                    ) from exc

            if switch_to_text_only:
                # ADR-026 gives the stripped projection a fresh retry budget.
                projected_messages = _strip_images(projected_messages)
                stripped_images = True
                continue
            raise ProviderInvocationError("Provider request failed")

    async def _stream_attempt(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        tool_choice: ToolChoice | None,
        effort: Effort | None,
        on_delta: DeltaCallback,
    ) -> list[dict[str, Any]]:
        request: dict[str, Any] = {
            "model": config.model,
            "max_tokens": config.max_output_tokens,
            "system": system,
            "messages": messages,
            "cache_control": {"type": "ephemeral"},
        }
        if tools:
            request["tools"] = tools
        if tool_choice is not None:
            request["tool_choice"] = tool_choice
        request.update(_thinking_controls(effort))

        stream_method: Any = self._client.messages.stream
        async with stream_method(**request) as stream:
            async for event in stream:
                if event.type != "content_block_delta":
                    continue
                if event.delta.type == "text_delta":
                    await on_delta("text", event.delta.text)
                elif event.delta.type == "thinking_delta":
                    await on_delta("thinking", event.delta.thinking)
            final_message: Any = stream.current_message_snapshot

        content: list[dict[str, Any]] = []
        for block in final_message.content:
            block_type = block.type
            if block_type == "text":
                content.append({"type": "text", "text": block.text})
            elif block_type == "thinking":
                content.append(
                    {
                        "type": "thinking",
                        "thinking": block.thinking,
                        "signature": block.signature,
                    }
                )
            elif block_type == "redacted_thinking":
                content.append({"type": "redacted_thinking", "data": block.data})
            elif block_type == "tool_use":
                content.append(
                    {
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    }
                )
            else:
                raise ProviderInvocationError(
                    f"Provider returned unsupported content block: {block_type}",
                    protocol=True,
                )
        if not content:
            raise ProviderInvocationError(
                "Provider returned an empty assistant message",
                protocol=True,
            )
        return content


def provider_fingerprint(config: ProviderConfig) -> str:
    source = f"{config.endpoint.rstrip('/')}\0{config.model}".encode()
    return hashlib.sha256(source).hexdigest()


def _thinking_controls(effort: Effort | None) -> dict[str, Any]:
    if effort is None or effort == Effort.OFF:
        return {"thinking": {"type": "disabled"}}
    return {
        "thinking": {"type": "adaptive"},
        "output_config": {"effort": effort.value},
    }


def _contains_images(messages: list[dict[str, Any]]) -> bool:
    return any(
        block.get("type") == "image" for message in messages for block in message.get("content", [])
    )


def _strip_images(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            **message,
            "content": [
                block for block in message.get("content", []) if block.get("type") != "image"
            ],
        }
        for message in messages
    ]


def _is_retryable(exc: Exception) -> bool:
    if isinstance(
        exc,
        (
            anthropic.APIConnectionError,
            anthropic.APITimeoutError,
            anthropic.RateLimitError,
            anthropic.InternalServerError,
        ),
    ):
        return True
    status = getattr(exc, "status_code", None)
    return status == 408 or status == 429 or (isinstance(status, int) and status >= 500)


def _safe_provider_rejection(
    exc: Exception,
    *,
    config: ProviderConfig,
) -> str | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, bool) or not isinstance(status, int) or not 400 <= status < 500:
        return None
    message: object = None
    body = getattr(exc, "body", None)
    if isinstance(body, Mapping):
        error = body.get("error")
        if isinstance(error, Mapping):
            message = error.get("message")
    if message is None:
        message = getattr(exc, "message", None)
    if not isinstance(message, str):
        return None
    message = " ".join(message.split())
    if not message:
        return None
    lowered = message.casefold()
    if lowered.startswith("error code:") or any(marker in message for marker in "{}[]"):
        # The Anthropic SDK formats JSON error bodies as Python reprs in
        # ``message``. Treat that as a raw response body, not a safe upstream
        # sentence.
        return None
    forbidden = (
        "http://",
        "https://",
        "authorization",
        "bearer ",
        "api key",
        "api_key",
        config.api_key.casefold(),
        config.endpoint.casefold(),
    )
    if any(value and value in lowered for value in forbidden):
        return None
    if not any(
        marker in lowered
        for marker in ("context", "token limit", "too many tokens", "prompt is too long")
    ):
        return None
    prefix = f"Provider rejected the request (HTTP {status}): "
    return prefix + message[: 1_000 - len(prefix)]


def _is_image_compatibility_error(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    if status not in {400, 413, 415, 422}:
        return False
    detail = str(exc).lower()
    return any(
        marker in detail for marker in ("image", "vision", "media type", "payload too large")
    )
