from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import Message, PendingMessage, Session, SystemConfig, TurnRun
from openctopus_server.provider.anthropic import ProviderInvocationError, ProviderResult


@pytest.mark.parametrize("input_tokens", [25, 901])
async def test_memory_proposal_shares_provider_resources_and_checks_context(pg_engine, input_tokens):
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(SystemConfig(key=key, value=value) for key, value in {
            "llm_endpoint": "http://provider.test", "llm_api_key": "secret", "llm_model": "model",
            "llm_max_context_tokens": 1000, "llm_max_output_tokens": 100,
        }.items())
        await db.commit()
    result = ProviderResult(content=[{"type": "tool_use", "name": "memory_update", "input": {"action": "skip"}}], fingerprint="test")
    provider = SimpleNamespace(stream_turn=AsyncMock(return_value=result), close=AsyncMock())
    factory = Mock(return_value=provider)
    runtime = ChatRuntime(pg_engine, provider_factory=factory, request_token_estimator=lambda **kwargs: input_tokens)
    tool = {"name": "memory_update", "input_schema": {"type": "object"}}
    messages = [{"role": "user", "content": "bounded evidence"}]
    if input_tokens == 901:
        with pytest.raises(ProviderInvocationError, match="context limit"):
            await runtime.propose_memory_update(system="fixed rubric", messages=messages, tool=tool)
        factory.assert_not_called()
        provider.stream_turn.assert_not_called()
    else:
        assert await runtime.propose_memory_update(system="fixed rubric", messages=messages, tool=tool) is result
        assert await runtime.propose_memory_update(system="fixed rubric", messages=messages, tool=tool) is result
        assert factory.call_count == 1
        call = provider.stream_turn.call_args.kwargs
        assert call["limiter"] is runtime.limiter
        assert call["system"] == "fixed rubric"
        assert call["messages"] == messages
        assert call["tools"] == [tool]
        assert call["tool_choice"] == {"type": "tool", "name": "memory_update"}
        assert call["effort"] is None
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        for model in (Session, Message, PendingMessage, TurnRun):
            assert await db.scalar(select(func.count()).select_from(model)) == 0
    await runtime.close()
    if input_tokens == 25:
        provider.close.assert_awaited_once()
