from __future__ import annotations

import asyncio
import copy
import json
from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.dto.config import ConfigPatch
from openctopus_server.provider import jev
from openctopus_server.provider.jev import JevChoiceQuestion, JevError, JevService
from openctopus_server.services.system_config import patch_config


def _questions() -> dict[str, JevChoiceQuestion]:
    return {"memory": JevChoiceQuestion(instructions="Should memory change?", criteria={"run": "New facts", "skip": "No new facts"})}


def _response(choice: str = "run") -> dict[str, Any]:
    return {
        "model": "jev-1.13.0",
        "answers": {
            "memory": {
                "type": "choice", "choice": choice,
                "probabilities": {"run": 0.6 if choice == "run" else 0.4, "skip": 0.4 if choice == "run" else 0.6},
                "confidence": 0.1,
            }
        },
        "usage": {"input_tokens": 100, "output_tokens": 20},
    }


async def _configure(engine: Any, *, key: str = "jev-secret") -> None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        await patch_config(db, ConfigPatch(jev_endpoint="https://jev.test", jev_api_key=key))


@pytest.mark.parametrize("choice", ["run", "skip"])
async def test_jev_wire_contract_uses_returned_choice_without_threshold(pg_engine: Any, choice: str) -> None:
    await _configure(pg_engine)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == "https://jev.test/v1/systemone"
        assert request.headers["authorization"] == "Bearer jev-secret"
        assert json.loads(request.content) == {
            "model": "jev-latest", "state": {"memory": "old", "conversations": ["new"]},
            "questions": {key: question.model_dump() for key, question in _questions().items()},
        }
        return httpx.Response(200, json=_response(choice))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = JevService(pg_engine, client=client)
        answers = await service.evaluate(state={"memory": "old", "conversations": ["new"]}, questions=_questions())
        assert answers["memory"].choice == choice
        assert answers["memory"].confidence == 0.1
        status = await service.status()
        assert status.state == "available"
        assert status.checked_at is not None


def _invalid_responses() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = [{}, {"model": "jev", "answers": {}, "usage": {"input_tokens": 0, "output_tokens": 0}}]
    for field, value in [
        ("type", "noul"), ("choice", "other"), ("probabilities", {"run": 1.0}),
        ("probabilities", {"run": 0.7, "skip": 0.7}),
        ("probabilities", {"run": True, "skip": False}),
        ("probabilities", {"run": "0.6", "skip": 0.4}),
        ("probabilities", {"run": 1.1, "skip": -0.1}),
        ("probabilities", {"run": 0.1, "skip": 0.9}),
        ("probabilities", {"run": 0.6, "skip": 0.4, "other": 0.0}),
        ("confidence", "0.5"), ("confidence", True), ("confidence", 1.1),
    ]:
        response = copy.deepcopy(_response())
        response["answers"]["memory"][field] = value
        cases.append(response)
    response = _response()
    response["answers"]["unknown"] = response["answers"].pop("memory")
    cases.append(response)
    response = _response()
    response["usage"]["input_tokens"] = True
    cases.append(response)
    return cases


@pytest.mark.parametrize("body", _invalid_responses())
async def test_jev_rejects_missing_or_malformed_typed_answers(pg_engine: Any, body: dict[str, Any]) -> None:
    await _configure(pg_engine)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body))) as client:
        service = JevService(pg_engine, client=client)
        with pytest.raises(JevError, match="invalid_response"):
            await service.evaluate(state="state", questions=_questions())
        assert (await service.status()).state == "invalid_response"


@pytest.mark.parametrize("status,expected", [(401, "unauthorized"), (403, "unauthorized"), (429, "unavailable"), (529, "unavailable"), (500, "unavailable"), (302, "unavailable")])
async def test_jev_http_failures_are_sanitized(pg_engine: Any, status: int, expected: str) -> None:
    await _configure(pg_engine)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(status, text="jev-secret raw response"))) as client:
        service = JevService(pg_engine, client=client)
        with pytest.raises(JevError) as caught:
            await service.evaluate(state="state", questions=_questions())
        assert caught.value.reason == expected
        assert "jev-secret" not in str(caught.value)
        assert "raw response" not in str(caught.value)
        assert (await service.status()).state == expected


async def test_jev_network_failure_and_recovery(pg_engine: Any) -> None:
    await _configure(pg_engine)
    failed = True

    def handler(request: httpx.Request) -> httpx.Response:
        if failed:
            raise httpx.ReadTimeout("secret provider URL/key", request=request)
        return httpx.Response(200, json=_response("skip"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = JevService(pg_engine, client=client)
        with pytest.raises(JevError, match="unreachable") as caught:
            await service.evaluate(state="state", questions=_questions())
        assert str(caught.value) == "Jev decision failed: unreachable"
        assert (await service.status()).state == "unreachable"
        failed = False
        await service.evaluate(state="state", questions=_questions())
        assert (await service.status()).state == "available"


async def test_jev_old_request_cannot_overwrite_status_after_edit(pg_engine: Any) -> None:
    await _configure(pg_engine)
    started, release = asyncio.Event(), asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        started.set()
        await release.wait()
        return httpx.Response(401, text="old key rejected")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = JevService(pg_engine, client=client)
        request_task = asyncio.create_task(service.evaluate(state="state", questions=_questions()))
        await asyncio.wait_for(started.wait(), 1)
        await _configure(pg_engine, key="new-key")
        release.set()
        with pytest.raises(JevError, match="unauthorized"):
            await request_task
        assert (await service.status()).model_dump() == {"state": "unchecked", "checked_at": None}


async def test_jev_io_bounds_and_nonfinite_numbers(pg_engine: Any) -> None:
    await _configure(pg_engine)
    bodies = [b"x" * (jev.JEV_MAX_RESPONSE_BYTES + 1), b'{"model":"jev","answers":{"memory":{"type":"choice","choice":"run","probabilities":{"run":NaN,"skip":0},"confidence":0.5}},"usage":{"input_tokens":0,"output_tokens":0}}']
    for body in bodies:
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))) as client:
            service = JevService(pg_engine, client=client)
            with pytest.raises(JevError, match="invalid_response"):
                await service.evaluate(state="state", questions=_questions())
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: pytest.fail("oversize input cannot reach HTTP"))) as client:
        service = JevService(pg_engine, client=client)
        with pytest.raises(JevError, match="input_limit"):
            await service.evaluate(state="x" * jev.JEV_MAX_REQUEST_BYTES, questions=_questions())


async def test_jev_concurrency_and_total_timeout_are_bounded(pg_engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    await _configure(pg_engine)
    active = maximum = started = 0
    release = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, maximum, started
        active += 1
        started += 1
        maximum = max(active, maximum)
        try:
            await release.wait()
            return httpx.Response(200, json=_response())
        finally:
            active -= 1

    monkeypatch.setattr(jev, "JEV_TIMEOUT_SECONDS", 0.05)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = JevService(pg_engine, client=client)
        # Exercise admission/request deadlines independently of DB pool checkout.
        # Config reads are covered separately and are outside this deadline.
        config = await service._config()

        async def configured():
            return config

        monkeypatch.setattr(service, "_config", configured)
        results = await asyncio.gather(*[
            service.evaluate(state="state", questions=_questions())
            for _ in range(jev.JEV_MAX_CONCURRENCY + 1)
        ], return_exceptions=True)
        assert maximum == jev.JEV_MAX_CONCURRENCY
        assert started <= jev.JEV_MAX_CONCURRENCY + 1
        assert all(isinstance(result, JevError) and result.reason == "unreachable" for result in results)
        assert active == 0
