"""OO model invocation policies and product projection over Pydantic AI models."""

import hashlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast

import httpx2
from pydantic_ai.direct import model_request_stream
from pydantic_ai.messages import (
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
    ThinkingPart,
    ThinkingPartDelta,
)
from pydantic_ai.models import Model, ModelRequestParameters
from pydantic_ai.settings import ModelSettings
from pydantic_ai.tools import ToolDefinition

from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.messages import model_messages, response_blocks
from openctopus_server.provider.models import build_model
from openctopus_server.provider.policy import PolicyModel
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
    def native_model(self, config: ProviderConfig) -> Model: ...

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


class ModelProvider:
    def __init__(self, config: ProviderConfig, *, http_client: httpx2.AsyncClient | None = None) -> None:
        self.model = build_model(config, http_client=http_client)
        self.policy = PolicyModel(self.model)

    def native_model(self, config: ProviderConfig) -> Model:
        return self.policy

    async def close(self) -> None:
        await self.model.client.close()

    async def stream_turn(
        self, *, config: ProviderConfig, system: str, messages: list[dict[str, Any]],
        effort: Effort | None, limiter: ProviderLimiter, on_delta: DeltaCallback,
        tools: list[dict[str, Any]] | None = None, tool_choice: ToolChoice | None = None,
    ) -> ProviderResult:
        await limiter.configure(config.max_concurrent_requests)
        try:
            history = model_messages(
                messages, model_name=self.model.model_name, provider_name=self.model.system,
                provider_url=self.model.base_url or "",
            )
            settings = model_settings(config, effort)
            if tool_choice is not None:
                settings["tool_choice"] = [tool_choice["name"]]
            parameters = ModelRequestParameters(function_tools=[
                ToolDefinition(name=tool["name"], description=tool.get("description"),
                               parameters_json_schema=tool["input_schema"])
                for tool in tools or []
            ])
            from pydantic_ai.messages import InstructionPart
            parameters.instruction_parts = [InstructionPart(system)]
            async with limiter.slot(), model_request_stream(
                self.policy, history, model_settings=settings, model_request_parameters=parameters,
            ) as stream:
                async for event in stream:
                    if isinstance(event, PartStartEvent):
                        if isinstance(event.part, (TextPart, ThinkingPart)) and event.part.content:
                            await on_delta("thinking" if isinstance(event.part, ThinkingPart) else "text", event.part.content)
                    elif isinstance(event, PartDeltaEvent):
                        if isinstance(event.delta, TextPartDelta) and event.delta.content_delta:
                            await on_delta("text", event.delta.content_delta)
                        elif isinstance(event.delta, ThinkingPartDelta) and event.delta.content_delta:
                            await on_delta("thinking", event.delta.content_delta)
                response = stream.get()
            try:
                content = response_blocks(response)
            except ValueError as exc:
                raise ProviderInvocationError(str(exc), protocol=True) from exc
            return ProviderResult(content, provider_fingerprint(config))
        except ProviderInvocationError:
            raise
        except Exception as exc:
            raise ProviderInvocationError(
                "Provider request failed", safe_message=safe_provider_rejection(exc, config=config),
            ) from exc


def model_settings(config: ProviderConfig, effort: Effort | None) -> ModelSettings:
    settings: dict[str, Any] = {"max_tokens": config.max_output_tokens}
    enabled = effort is not None and effort != Effort.OFF
    effort_value = effort.value if effort is not None else "off"
    if config.protocol == "anthropic":
        settings["anthropic_cache"] = True
        settings["anthropic_thinking"] = {"type": "adaptive" if enabled else "disabled"}
        if enabled:
            settings["anthropic_effort"] = effort_value
    elif config.protocol == "openrouter":
        settings["openrouter_reasoning"] = {"enabled": enabled}
        if enabled:
            settings["openrouter_reasoning"]["effort"] = "xhigh" if effort == Effort.MAX else effort_value
    else:
        settings["openai_reasoning_effort"] = (
            "xhigh" if effort == Effort.MAX else effort_value if enabled else "none"
        )
    return cast(ModelSettings, settings)


def provider_fingerprint(config: ProviderConfig) -> str:
    source = f"{config.protocol}\0{config.endpoint.rstrip('/')}\0{config.model}".encode()
    return hashlib.sha256(source).hexdigest()


def safe_provider_rejection(
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
