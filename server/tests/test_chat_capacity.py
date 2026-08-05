import asyncio
import json
from collections import defaultdict
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any
from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import Message, PendingMessage, SystemConfig, TurnRun
from openctopus_server.provider.anthropic import (
    DeltaCallback,
    ProviderResult,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.workspace.fs import WorkspaceFS

_SESSION_COUNT = 500


class CapacityProvider:
    def __init__(self, expected_calls: int) -> None:
        self.expected_calls = expected_calls
        self.started = 0
        self.all_started = asyncio.Event()
        self.release = asyncio.Event()

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
    ) -> ProviderResult:
        del system, effort, limiter, tools
        prompt = str(messages[-1]["content"][-1]["text"])
        reply = prompt.replace("session-", "reply-", 1)
        self.started += 1
        if self.started == self.expected_calls:
            self.all_started.set()
        await self.release.wait()
        await on_delta("text", reply)
        return ProviderResult(
            content=[{"type": "text", "text": reply}],
            fingerprint=provider_fingerprint(config),
        )

    async def count_tokens(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
    ) -> int:
        del config, system, messages, tools, effort, limiter
        return 1

    async def close(self) -> None:
        return None


async def _configure_provider(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://capacity.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
            ]
        )
        await db.commit()


async def _wait_for_no_runtime_states(runtime: ChatRuntime) -> None:
    for _ in range(200):
        async with runtime._states_lock:
            if not runtime._states:
                return
        await asyncio.sleep(0.01)
    raise AssertionError("completed session states were not evicted")


async def test_500_concurrent_sessions_complete_without_cross_talk(
    user_client,
    test_app,
    pg_engine,
) -> None:
    await _configure_provider(pg_engine)
    provider = CapacityProvider(_SESSION_COUNT)
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
    )
    test_app.state.chat_runtime = runtime
    workspace_fs = WorkspaceFS(AsyncMock())
    workspace_slots_entered = 0
    all_workspace_slots_entered = asyncio.Event()
    release_workspace_slots = asyncio.Event()

    async def hold_workspace_slot(
        slot: Callable[[], AbstractAsyncContextManager[None]],
    ) -> None:
        nonlocal workspace_slots_entered
        async with slot():
            workspace_slots_entered += 1
            if workspace_slots_entered == 12:
                all_workspace_slots_entered.set()
            await release_workspace_slots.wait()

    workspace_holders = [
        asyncio.create_task(hold_workspace_slot(slot))
        for slot in (
            workspace_fs.materialization_slot,
            workspace_fs.file_operation_slot,
            workspace_fs.heavy_operation_slot,
        )
        for _ in range(4)
    ]
    await asyncio.wait_for(all_workspace_slots_entered.wait(), timeout=1)
    session_ids = [
        uuid5(NAMESPACE_URL, f"openoctopus-capacity-{index}") for index in range(_SESSION_COUNT)
    ]
    requests = [
        asyncio.create_task(
            user_client.post(
                f"/api/sessions/{session_id}/messages",
                json={
                    "content": [{"type": "text", "text": f"session-{index}"}],
                    "attachments": [],
                },
            )
        )
        for index, session_id in enumerate(session_ids)
    ]

    try:
        await asyncio.wait_for(provider.all_started.wait(), timeout=60)
        async with runtime._states_lock:
            assert len(runtime._states) == _SESSION_COUNT
    except BaseException:
        provider.release.set()
        release_workspace_slots.set()
        await asyncio.gather(*requests, return_exceptions=True)
        await asyncio.gather(*workspace_holders)
        await runtime.close()
        raise

    provider.release.set()
    release_workspace_slots.set()
    await asyncio.gather(*workspace_holders)
    responses = await asyncio.wait_for(asyncio.gather(*requests), timeout=60)
    for index, response in enumerate(responses):
        assert response.status_code == 200
        events = [json.loads(line) for line in response.text.splitlines()]
        deltas = [event for event in events if event["type"] == "token_delta"]
        assert [event["text"] for event in deltas] == [f"reply-{index}"]

    await _wait_for_no_runtime_states(runtime)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        messages = list(
            (
                await db.execute(select(Message.session_id, Message.message_kind, Message.content))
            ).all()
        )
        pending_ids = list((await db.execute(select(PendingMessage.id))).scalars().all())
        run_statuses = list((await db.execute(select(TurnRun.status))).scalars().all())

    by_session: dict[UUID, list[tuple[str, list[dict[str, Any]]]]] = defaultdict(list)
    for session_id, message_kind, content in messages:
        by_session[session_id].append((message_kind, content))
    assert len(by_session) == _SESSION_COUNT
    for index, session_id in enumerate(session_ids):
        session_messages = dict(by_session[session_id])
        assert set(session_messages) == {"human", "assistant"}
        assert session_messages["human"][-1] == {
            "type": "text",
            "text": f"session-{index}",
        }
        assert session_messages["assistant"] == [{"type": "text", "text": f"reply-{index}"}]
    assert pending_ids == []
    assert len(run_statuses) == _SESSION_COUNT
    assert set(run_statuses) == {"completed"}
    await runtime.close()
