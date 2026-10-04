"""Enterprise endpoints backed by the SDK's native model implementations."""

from dataclasses import replace

import httpx2
from anthropic import AsyncAnthropic
from openai import AsyncOpenAI
from pydantic_ai.messages import ModelMessage, ModelResponse, ThinkingPart
from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.models.openrouter import OpenRouterModel
from pydantic_ai.profiles.openai import OpenAIModelProfile
from pydantic_ai.providers.anthropic import AnthropicProvider
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.providers.openrouter import OpenRouterProvider

from openctopus_server.provider.config import ProviderConfig


def build_model(
    config: ProviderConfig, *, http_client: httpx2.AsyncClient | None = None,
) -> AnthropicModel | OpenAIChatModel:
    """Build against the configured endpoint, with retries owned by OO's policy.

    The application owns the supplied SDK client's lifetime and closes it when
    it disposes this model. Never infer a provider from an untrusted model name.
    """
    if config.protocol == "anthropic":
        return AnthropicModel(config.model, provider=AnthropicProvider(anthropic_client=AsyncAnthropic(
            api_key=config.api_key, base_url=config.endpoint, max_retries=0, http_client=http_client,
        )))
    client = AsyncOpenAI(
        api_key=config.api_key, base_url=f"{config.endpoint.rstrip('/')}/v1", max_retries=0,
        http_client=http_client,
    )
    # Enterprise aliases need not identify the underlying chat template. The
    # SDK can merge leading instructions for templates that accept one system
    # message; their order and content remain unchanged.
    profile = OpenAIModelProfile(openai_chat_supports_multiple_system_messages=False)
    if config.protocol == "openrouter":
        return OpenRouterModel(config.model, provider=OpenRouterProvider(openai_client=client), profile=profile)
    return OpenAIChatModel(config.model, provider=OpenAIProvider(openai_client=client), profile=profile)


def compatible_history(
    messages: list[ModelMessage], *, provider_url: str, model_name: str, provider_name: str,
) -> list[ModelMessage]:
    """Keep opaque reasoning only for the endpoint/model that produced it.

    SDK provider names alone do not separate two enterprise endpoints hosting
    the same model. Copy changed responses so archived evidence stays intact.
    """
    projected: list[ModelMessage] = []
    for message in messages:
        if isinstance(message, ModelResponse) and (
            (message.provider_url or "").rstrip("/") != provider_url.rstrip("/")
            or message.model_name != model_name
            or message.provider_name != provider_name
        ):
            parts = [part for part in message.parts if not isinstance(part, ThinkingPart)]
            if parts:
                projected.append(replace(message, parts=parts))
        else:
            projected.append(message)
    return projected
