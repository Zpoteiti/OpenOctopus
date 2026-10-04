"""Bounded Jev Choice decisions shared by Dream and Heartbeat."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import httpx
import httpx2
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_ai.exceptions import ModelAPIError, ModelHTTPError, UnexpectedModelBehavior
from pydantic_ai.models.decision import ChoiceQuestion, DecisionRequest
from pydantic_ai.models.typesafe import TypeSafeModel
from pydantic_ai.providers.typesafe import TypeSafeProvider
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy

from openctopus_server.db.models import SystemConfig
from openctopus_server.dto.config import JevStatus, JevStatusState

JEV_MAX_REQUEST_BYTES = 512_000
JEV_MAX_RESPONSE_BYTES = 128_000
JEV_MAX_QUESTIONS = 16
JEV_MAX_CONCURRENCY = 8
JEV_TIMEOUT_SECONDS = 15.0
JEV_CONFIG_LOCK = "openoctopus:jev_config"
_CONFIG_KEYS = {"jev_endpoint", "jev_api_key", "jev_revision", "jev_status"}
JevState = str | dict[str, Any] | list[Any]


class JevChoiceQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    type: Literal["choice"] = "choice"
    instructions: str = Field(min_length=1, max_length=16_000)
    criteria: dict[Literal["run", "skip"], str]


class JevChoiceAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    type: Literal["choice"]
    choice: Literal["run", "skip"]
    probabilities: dict[Literal["run", "skip"], float]
    confidence: float = Field(ge=0, le=1)


class _Usage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)


class _Response(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    model: str = Field(min_length=1)
    answers: dict[str, JevChoiceAnswer]
    usage: _Usage


class JevError(Exception):
    """A fixed reason code; credentials and response bodies never reach callers."""

    def __init__(self, reason: JevStatusState | Literal["input_limit"]) -> None:
        self.reason = reason
        super().__init__(f"Jev decision failed: {reason}")


@dataclass(frozen=True, slots=True)
class _Config:
    endpoint: str
    api_key: str
    revision: str


def jev_config_status(rows: Mapping[str, Any]) -> JevStatus:
    if not (rows.get("jev_endpoint") and rows.get("jev_api_key")):
        return JevStatus(state="not_configured", checked_at=None)
    raw = rows.get("jev_status")
    if isinstance(raw, dict) and raw.get("revision") == rows.get("jev_revision", ""):
        try:
            return JevStatus.model_validate(
                {"state": raw.get("state"), "checked_at": raw.get("checked_at")}
            )
        except ValidationError:
            pass
    return JevStatus(state="unchecked", checked_at=None)


async def lock_jev_config(db: AsyncSession) -> None:
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": JEV_CONFIG_LOCK},
    )


class _BoundedTransport(httpx2.AsyncBaseTransport):
    """Reuse OO's HTTP pool while applying byte and strict-response limits to SDK I/O."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        payload = await request.aread()
        if len(payload) > JEV_MAX_REQUEST_BYTES:
            raise JevError("input_limit")
        async with self.client.stream(
            request.method, str(request.url), headers=request.headers.raw,
            content=payload, timeout=10.0, follow_redirects=False,
        ) as response:
            body = bytearray()
            async for chunk in response.aiter_bytes():
                if len(body) + len(chunk) > JEV_MAX_RESPONSE_BYTES:
                    raise JevError("invalid_response")
                body.extend(chunk)
            if response.status_code == 200:
                try:
                    _Response.model_validate_json(body)
                except ValidationError:
                    raise JevError("invalid_response") from None
            return httpx2.Response(response.status_code, headers=response.headers, content=bytes(body))


class JevService:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._engine = engine
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=10.0)
        self._slots = asyncio.Semaphore(JEV_MAX_CONCURRENCY)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _rows(self, db: AsyncSession) -> dict[str, Any]:
        result = await db.execute(select(SystemConfig).where(SystemConfig.key.in_(_CONFIG_KEYS)))
        return {row.key: row.value for row in result.scalars().all()}

    async def status(self) -> JevStatus:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            return jev_config_status(await self._rows(db))

    async def _config(self) -> _Config:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            rows = await self._rows(db)
        endpoint, api_key = rows.get("jev_endpoint"), rows.get("jev_api_key")
        if not (isinstance(endpoint, str) and endpoint and isinstance(api_key, str) and api_key):
            raise JevError("not_configured")
        return _Config(endpoint=endpoint, api_key=api_key, revision=rows.get("jev_revision", ""))

    async def _observe(self, config: _Config, state: JevStatusState) -> None:
        async with AsyncSession(self._engine, expire_on_commit=False) as db:
            await lock_jev_config(db)
            rows = await self._rows(db)
            if rows.get("jev_revision", "") != config.revision:
                return
            value = {
                "revision": config.revision,
                "state": state,
                "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            }
            row = await db.scalar(select(SystemConfig).where(SystemConfig.key == "jev_status"))
            if row is None:
                db.add(SystemConfig(key="jev_status", value=value))
            else:
                row.value = value
            await db.commit()

    async def evaluate(
        self,
        *,
        state: JevState,
        questions: dict[str, JevChoiceQuestion],
    ) -> dict[str, JevChoiceAnswer]:
        if not 1 <= len(questions) <= JEV_MAX_QUESTIONS or any(
            not key or len(key) > 128 or set(question.criteria) != {"run", "skip"}
            for key, question in questions.items()
        ):
            raise JevError("input_limit")
        try:
            payload = json.dumps(
                {
                    "model": "jev-latest",
                    "state": state,
                    "questions": {key: question.model_dump() for key, question in questions.items()},
                },
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (ValueError, TypeError) as exc:
            raise JevError("input_limit") from exc
        if len(payload) > JEV_MAX_REQUEST_BYTES:
            raise JevError("input_limit")
        config = await self._config()
        try:
            async with asyncio.timeout(JEV_TIMEOUT_SECONDS), self._slots:
                answers = await self._request(config, payload, questions)
        except (TimeoutError, httpx.HTTPError, ModelAPIError) as exc:
            if isinstance(exc, ModelHTTPError):
                reason: JevStatusState = "unauthorized" if exc.status_code in {401, 403} else "unavailable"
                await self._observe(config, reason)
                raise JevError(reason) from None
            await self._observe(config, "unreachable")
            raise JevError("unreachable") from None
        except UnexpectedModelBehavior:
            await self._observe(config, "invalid_response")
            raise JevError("invalid_response") from None
        except JevError as exc:
            # Local input failures occur before this request boundary.
            await self._observe(config, exc.reason)  # type: ignore[arg-type]
            raise
        await self._observe(config, "available")
        return answers

    async def _request(
        self,
        config: _Config,
        payload: bytes,
        questions: dict[str, JevChoiceQuestion],
    ) -> dict[str, JevChoiceAnswer]:
        request = json.loads(payload)
        async with AsyncTypeSafeClient(
            api_key=config.api_key, base_url=config.endpoint, retry=RetryPolicy(max_retries=0),
            transport=_BoundedTransport(self._client), timeout=10.0,
        ) as client:
            model = TypeSafeModel("jev-latest", provider=TypeSafeProvider(typesafe_client=client))
            result = await model.decide(DecisionRequest(
                state=request["state"], questions={name: ChoiceQuestion(instructions=question.instructions,
                    criteria={key: value for key, value in question.criteria.items()}) for name, question in questions.items()},
            ), {"timeout": 10.0})
        try:
            answers = {name: JevChoiceAnswer.model_validate(asdict(answer)) for name, answer in result.answers.items()}
        except ValidationError:
            raise JevError("invalid_response") from None
        if set(answers) != set(questions):
            raise JevError("invalid_response")
        for answer in answers.values():
            probabilities = answer.probabilities
            if (
                set(probabilities) != {"run", "skip"}
                or any(not 0 <= value <= 1 for value in probabilities.values())
                or not math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-6)
                or probabilities[answer.choice] < max(probabilities.values())
            ):
                raise JevError("invalid_response")
        return answers

    async def check(self) -> JevStatus:
        try:
            await self.evaluate(
                state="OpenOctopus Jev connection check. No work is pending.",
                questions={
                    "connection_check": JevChoiceQuestion(
                        instructions="Choose skip for this connection check because no work is pending.",
                        criteria={"run": "Work is pending.", "skip": "No work is pending."},
                    )
                },
            )
        except JevError:
            pass
        return await self.status()
