"""OO request policies around native SDK models; no provider wire parsing."""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import anthropic
import httpx2
import openai
from pydantic_ai import RunContext
from pydantic_ai.exceptions import ModelAPIError, ModelHTTPError
from pydantic_ai.messages import (
    BinaryContent,
    ImageUrl,
    ModelMessage,
    ModelRequest,
    ModelRequestPart,
    ModelResponse,
    ModelResponseStreamEvent,
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
    ThinkingPart,
    ThinkingPartDelta,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models import Model, ModelRequestParameters, StreamedResponse
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.usage import RequestUsage

from openctopus_server.provider.models import compatible_history


def _is_image(value: object) -> bool:
    return isinstance(value, ImageUrl) or isinstance(value, BinaryContent) and value.is_image


def without_images(messages: list[ModelMessage]) -> list[ModelMessage]:
    """Project prompt/tool media only; leave tool arguments and saved evidence alone."""
    projected: list[ModelMessage] = []
    for message in messages:
        if not isinstance(message, ModelRequest):
            projected.append(message)
            continue
        parts: list[ModelRequestPart] = []
        for part in message.parts:
            if isinstance(part, (UserPromptPart, ToolReturnPart)) and isinstance(part.content, list):
                parts.append(replace(part, content=[item for item in part.content if not _is_image(item)]))
            else:
                parts.append(part)
        projected.append(replace(message, parts=parts))
    return projected


def _has_images(messages: list[ModelMessage]) -> bool:
    return any(
        _is_image(item)
        for message in messages if isinstance(message, ModelRequest)
        for part in message.parts if isinstance(part, (UserPromptPart, ToolReturnPart))
        if isinstance(part.content, list)
        for item in part.content
    )


def _image_rejection(error: Exception) -> bool:
    if not isinstance(error, ModelHTTPError) or error.status_code not in {400, 413, 415, 422}:
        return False
    detail = str(error.body).lower()
    return any(word in detail for word in ("image", "vision", "media", "payload"))


def _transient(error: Exception) -> bool:
    cause = error.__cause__ if isinstance(error, ModelAPIError) else error
    return (
        isinstance(error, ModelHTTPError) and (error.status_code in {408, 429} or error.status_code >= 500)
        or isinstance(cause, (anthropic.APIConnectionError, openai.APIConnectionError, httpx2.TransportError))
    )


class _Attempts:
    def __init__(self, messages: list[ModelMessage]) -> None:
        self.messages = messages
        self.stripped = False
        self.attempt = 0

    async def retry(self, error: Exception) -> None:
        if not self.stripped and _has_images(self.messages) and _image_rejection(error):
            self.messages = without_images(self.messages)
            self.stripped = True
            self.attempt = 0
        elif self.attempt < 2 and _transient(error):
            await asyncio.sleep(0.25 * 2**self.attempt)
            self.attempt += 1
        else:
            raise error


class PolicyModel(WrapperModel):
    def _history(self, messages: list[ModelMessage]) -> list[ModelMessage]:
        return compatible_history(
            messages, provider_url=self.base_url or "", model_name=self.model_name,
            provider_name=self.system,
        )

    async def request(
        self, messages: list[ModelMessage], model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        attempts = _Attempts(self._history(messages))
        while True:
            try:
                return await self.wrapped.request(attempts.messages, model_settings, model_request_parameters)
            except Exception as error:
                await attempts.retry(error)

    @asynccontextmanager
    async def request_stream(
        self, messages: list[ModelMessage], model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters, run_context: RunContext[Any] | None = None,
    ) -> AsyncIterator[StreamedResponse]:
        stream = _PolicyStream(
            self.wrapped, self._history(messages), model_settings, model_request_parameters, run_context,
        )
        try:
            yield stream
        finally:
            await stream.close_stream()


class _PolicyStream(StreamedResponse):
    """Forward official events, allowing retries only before any visible content.

    Buffer preamble/tool-argument events until text/thinking begins or the request
    completes. A failed attempt therefore cannot leak partial tool arguments into
    the successful attempt. Provider parsing and final response assembly stay in
    the wrapped SDK stream.
    """

    def __init__(
        self, model: Model, messages: list[ModelMessage], settings: ModelSettings | None,
        parameters: ModelRequestParameters, context: RunContext[Any] | None,
    ) -> None:
        super().__init__(parameters)
        self.model = model
        self.messages = messages
        self.settings = settings
        self.context = context
        self.active: StreamedResponse | None = None
        self.started_at = datetime.now(UTC)
        self.iterator = self._get_event_iterator()

    def __aiter__(self) -> AsyncIterator[ModelResponseStreamEvent]:
        # The SDK stream already emits PartEnd/FinalResult events.
        return self.iterator

    async def _get_event_iterator(self) -> AsyncGenerator[ModelResponseStreamEvent]:
        attempts = _Attempts(self.messages)
        visible = False
        while True:
            buffered: list[ModelResponseStreamEvent] = []
            try:
                async with self.model.request_stream(
                    attempts.messages, self.settings, self.model_request_parameters, self.context,
                ) as stream:
                    self.active = stream
                    async for event in stream:
                        visible = visible or _visible(event)
                        if visible:
                            for pending in buffered:
                                yield pending
                            buffered.clear()
                            yield event
                        else:
                            buffered.append(event)
                    for pending in buffered:
                        yield pending
                    self.final_result_event = stream.final_result_event
                    self.finish_reason = stream.finish_reason
                    self.state = stream.state
                    return
            except Exception as error:
                if visible:
                    raise
                await attempts.retry(error)

    def get(self) -> ModelResponse:
        return self.active.get() if self.active else ModelResponse([], state="incomplete")

    @property
    def usage(self) -> RequestUsage:
        return self.active.usage if self.active else RequestUsage()

    @property
    def model_name(self) -> str:
        return self.model.model_name

    @property
    def provider_name(self) -> str:
        return self.model.system

    @property
    def provider_url(self) -> str | None:
        return self.model.base_url

    @property
    def timestamp(self) -> datetime:
        return self.active.timestamp if self.active else self.started_at

    async def close_stream(self) -> None:
        await self.iterator.aclose()

    async def cancel(self) -> None:
        if self.active is not None:
            await self.active.cancel()

    @property
    def cancelled(self) -> bool:
        return self.active.cancelled if self.active else False

    def time_to_first_chunk(self, request_start: float) -> float | None:
        return self.active.time_to_first_chunk(request_start) if self.active else None


def _visible(event: ModelResponseStreamEvent) -> bool:
    if isinstance(event, PartStartEvent) and isinstance(event.part, (TextPart, ThinkingPart)):
        return bool(event.part.content)
    if isinstance(event, PartDeltaEvent):
        if isinstance(event.delta, TextPartDelta):
            return bool(event.delta.content_delta)
        if isinstance(event.delta, ThinkingPartDelta):
            return bool(event.delta.content_delta)
    return False
