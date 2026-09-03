import json
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import (
    DiscordConfig,
    Message,
    PendingMessage,
    Session,
    SystemConfig,
    User,
)
from openctopus_server.provider.anthropic import (
    DeltaCallback,
    ProviderResult,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.services.messages import reserve_pending_turn
from openctopus_server.tools.base import Tool, ToolContext, ToolResult
from openctopus_server.tools.registry import ToolRegistry


@dataclass(frozen=True, slots=True)
class _StreamStep:
    content: list[dict[str, Any]]


class _ScriptedProvider:
    def __init__(self, *, counts: list[int], steps: list[_StreamStep]) -> None:
        self._counts = deque(counts)
        self._steps = deque(steps)
        self.count_calls: list[dict[str, Any]] = []
        self.stream_calls: list[dict[str, Any]] = []

    def estimate_tokens(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> int:
        self.count_calls.append(
            {
                "system": system,
                "messages": messages,
                "tools": tools,
            }
        )
        return self._counts.popleft()

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
        del limiter, on_delta
        self.stream_calls.append(
            {
                "config": config,
                "system": system,
                "messages": messages,
                "effort": effort,
                "tools": tools,
            }
        )
        return ProviderResult(
            content=self._steps.popleft().content,
            fingerprint=provider_fingerprint(config),
        )

    async def close(self) -> None:
        return None


class _EchoTool(Tool):
    def name(self) -> str:
        return "echo"

    def schema(self) -> dict[str, Any]:
        return {
            "name": "echo",
            "description": "Return the supplied value.",
            "input_schema": {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": False,
            },
        }

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        del ctx
        return ToolResult(content=str(args["value"]))


async def _configure_compaction(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://fake.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
                SystemConfig(key="llm_max_output_tokens", value=1000),
                SystemConfig(key="llm_max_context_tokens", value=10_000),
                SystemConfig(key="llm_compaction_threshold_tokens", value=5000),
            ]
        )
        await db.commit()


def _install_runtime(
    test_app,
    pg_engine,
    provider: _ScriptedProvider,
    *,
    tool_registry: ToolRegistry | None = None,
    request_token_estimator=None,
) -> ChatRuntime:
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=tool_registry if tool_registry is not None else ToolRegistry(()),
        request_token_estimator=request_token_estimator or provider.estimate_tokens,
    )
    test_app.state.chat_runtime = runtime
    return runtime


class _ChannelContextEstimator:
    def __init__(self, *, base_tokens: int, tokens_per_entry: int) -> None:
        self.base_tokens = base_tokens
        self.tokens_per_entry = tokens_per_entry
        self.calls: list[list[dict[str, Any]]] = []

    def __call__(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> int:
        del system, tools
        self.calls.append(messages)
        encoded = json.dumps(messages)
        return self.base_tokens + encoded.count("CHANNEL_CONTEXT_ENTRY_") * self.tokens_per_entry


async def _configure_context_fence(
    pg_engine,
    *,
    max_context_tokens: int = 100,
    max_output_tokens: int = 20,
    compaction_threshold_tokens: int | None = None,
) -> None:
    rows = [
        SystemConfig(key="llm_endpoint", value="http://fake.test"),
        SystemConfig(key="llm_api_key", value="fake-key"),
        SystemConfig(key="llm_model", value="fake-model"),
        SystemConfig(key="llm_max_output_tokens", value=max_output_tokens),
        SystemConfig(key="llm_max_context_tokens", value=max_context_tokens),
    ]
    if compaction_threshold_tokens is not None:
        rows.append(
            SystemConfig(
                key="llm_compaction_threshold_tokens",
                value=compaction_threshold_tokens,
            )
        )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(rows)
        await db.commit()


def _channel_context(*indexes: int) -> list[dict[str, Any]]:
    return [
        {
            "source_message_id": f"context-{index}",
            "sender_id": f"sender-{index}",
            "sender_display_name": f"Sender {index}",
            "sent_at": f"2026-09-02T00:00:{index:02d}Z",
            "text": f"CHANNEL_CONTEXT_ENTRY_{index}",
            "attachment_summaries": [],
        }
        for index in indexes
    ]


def _pending_channel_message(
    *,
    session: Session,
    user: User,
    text: str,
    context_indexes: tuple[int, ...],
    binding_generation: UUID,
    received_at: datetime,
) -> PendingMessage:
    return PendingMessage(
        id=uuid4(),
        session_id=session.id,
        user_id=user.id,
        session_key=session.session_key,
        content=[{"type": "text", "text": text}],
        attachment_refs=[],
        sender_id=str(user.id),
        sender_display_name=user.name,
        sender_classification="owner",
        ingress_tool_profile="owner_full",
        source_message_id=f"source-{uuid4()}",
        channel_binding_generation=binding_generation,
        channel_context=_channel_context(*context_indexes),
        effort=None,
        received_at=received_at,
    )


def _paired_discord_config(
    *,
    user: User,
    binding_generation: UUID,
    application_id: str,
    now: datetime,
) -> DiscordConfig:
    return DiscordConfig(
        user_id=user.id,
        bot_token="secret",
        application_id=application_id,
        bot_user_id="bot-1",
        bot_display_name="Bot",
        binding_generation=binding_generation,
        owner_platform_user_id=str(user.id),
        owner_dm_chat_id="owner-dm",
        paired_at=now,
        allow_list=[],
    )


def _events(response) -> list[dict[str, Any]]:
    return [json.loads(line) for line in response.text.splitlines()]


def _text_message(
    session_id: UUID,
    *,
    kind: str,
    text: str,
    created_at: datetime,
) -> Message:
    authority = (
        {
            "sender_id": str(session_id),
            "sender_classification": "owner",
            "ingress_tool_profile": "owner_full",
        }
        if kind == "human"
        else {}
    )
    return Message(
        id=uuid4(),
        session_id=session_id,
        message_kind=kind,
        content=[{"type": "text", "text": text}],
        delivery_refs=[],
        llm_fingerprint=None,
        is_compacted=False,
        created_at=created_at,
        **authority,
    )


async def _messages(pg_engine, session_id: UUID) -> list[Message]:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        return list(
            (
                await db.execute(
                    select(Message)
                    .where(Message.session_id == session_id)
                    .order_by(Message.created_at, Message.id)
                )
            )
            .scalars()
            .all()
        )


async def test_stage_one_orders_summary_before_pending_and_replays_only_active_rows(
    user_client,
    test_app,
    pg_engine,
) -> None:
    await _configure_compaction(pg_engine)
    provider = _ScriptedProvider(
        counts=[6000, 1000],
        steps=[
            _StreamStep([{"type": "text", "text": "S1"}]),
            _StreamStep([{"type": "text", "text": "A11"}]),
        ],
    )
    runtime = _install_runtime(test_app, pg_engine, provider)
    session_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        db.add(
            Session(
                id=session_id,
                user_id=user.id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="New chat",
                created_at=now,
            )
        )
        await db.flush()
        db.add_all(
            [
                _text_message(session_id, kind="human", text="U1", created_at=now),
                _text_message(
                    session_id,
                    kind="assistant",
                    text="A1",
                    created_at=now + timedelta(microseconds=1),
                ),
            ]
        )
        await db.commit()

    response = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [{"type": "text", "text": "U11"}], "attachments": []},
    )

    assert response.status_code == 200
    assert _events(response)[-1]["status"] == "completed"
    rows = await _messages(pg_engine, session_id)
    assert [(row.content[-1]["text"], row.is_compacted) for row in rows] == [
        ("U1", True),
        ("A1", True),
        ("S1", False),
        ("U11", False),
        ("A11", False),
    ]

    summary_call, normal_call = provider.stream_calls
    assert [message["role"] for message in summary_call["messages"]] == [
        "user",
        "assistant",
        "user",
    ]
    assert [message["content"][-1]["text"] for message in summary_call["messages"]] == [
        "U1",
        "A1",
        "Write the compacted summary now.",
    ]
    assert [message["role"] for message in normal_call["messages"]] == [
        "assistant",
        "user",
    ]
    assert normal_call["messages"][0]["content"] == [{"type": "text", "text": "S1"}]
    assert normal_call["messages"][1]["content"][-1] == {
        "type": "text",
        "text": "U11",
    }
    normal_texts = [
        block.get("text")
        for message in normal_call["messages"]
        for block in message["content"]
        if block.get("type") == "text"
    ]
    assert "U1" not in normal_texts
    assert "A1" not in normal_texts

    history = (await user_client.get(f"/api/sessions/{session_id}/messages")).json()
    assert [message["is_compacted"] for message in history["messages"]] == [
        True,
        True,
        False,
        False,
        False,
    ]
    await runtime.close()


async def test_compaction_still_sends_one_final_call_when_local_estimate_remains_too_large(
    user_client,
    test_app,
    pg_engine,
) -> None:
    await _configure_compaction(pg_engine)
    provider = _ScriptedProvider(
        counts=[6000, 6000],
        steps=[
            _StreamStep([{"type": "text", "text": "S1"}]),
            _StreamStep([{"type": "text", "text": "provider accepted"}]),
        ],
    )
    runtime = _install_runtime(test_app, pg_engine, provider)
    session_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        db.add(
            Session(
                id=session_id,
                user_id=user.id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="New chat",
                created_at=now,
            )
        )
        await db.flush()
        db.add(_text_message(session_id, kind="human", text="U1", created_at=now))
        await db.commit()

    response = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [{"type": "text", "text": "U11"}], "attachments": []},
    )

    assert response.status_code == 200
    assert _events(response)[-1]["status"] == "completed"
    assert len(provider.stream_calls) == 2
    rows = await _messages(pg_engine, session_id)
    assert [row.message_kind for row in rows] == [
        "human",
        "compaction_summary",
        "human",
        "assistant",
    ]
    assert [row.is_compacted for row in rows] == [True, False, False, False]
    await runtime.close()


async def test_stage_two_preserves_latest_human_and_compacts_only_tool_tail(
    user_client,
    test_app,
    pg_engine,
) -> None:
    await _configure_compaction(pg_engine)
    provider = _ScriptedProvider(
        counts=[1000, 6000, 1000],
        steps=[
            _StreamStep(
                [
                    {
                        "type": "tool_use",
                        "id": "toolu_echo",
                        "name": "echo",
                        "input": {
                            "value": "tool output",
                            "openoctopus_device": "server",
                        },
                    }
                ]
            ),
            _StreamStep([{"type": "text", "text": "T1"}]),
            _StreamStep([{"type": "text", "text": "final"}]),
        ],
    )
    runtime = _install_runtime(
        test_app,
        pg_engine,
        provider,
        tool_registry=ToolRegistry((_EchoTool(),)),
    )
    session_id = uuid4()

    response = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [{"type": "text", "text": "U11"}], "attachments": []},
    )

    assert response.status_code == 200
    events = _events(response)
    assert len({event["turn_id"] for event in events if "turn_id" in event}) == 2
    rows = await _messages(pg_engine, session_id)
    assert [row.message_kind for row in rows] == [
        "human",
        "assistant",
        "tool_result",
        "compaction_summary",
        "assistant",
    ]
    assert [row.is_compacted for row in rows] == [False, True, True, False, False]
    assert rows[0].content[-1] == {"type": "text", "text": "U11"}
    assert rows[3].content == [{"type": "text", "text": "T1"}]

    summary_call = provider.stream_calls[1]
    assert [message["role"] for message in summary_call["messages"]] == [
        "user",
        "assistant",
        "user",
        "user",
    ]
    summary_texts = [
        block.get("text")
        for message in summary_call["messages"]
        for block in message["content"]
        if block.get("type") == "text"
    ]
    assert "U11" not in summary_texts
    assert any(
        block.get("type") == "tool_use" and block.get("id") == "toolu_echo"
        for message in summary_call["messages"]
        for block in message["content"]
    )
    assert any(
        block.get("type") == "tool_result" and block.get("tool_use_id") == "toolu_echo"
        for message in summary_call["messages"]
        for block in message["content"]
    )
    final_call = provider.stream_calls[2]
    assert [message["role"] for message in final_call["messages"]] == [
        "user",
        "assistant",
        "user",
    ]
    assert final_call["messages"][0]["content"][-1] == {
        "type": "text",
        "text": "U11",
    }
    assert final_call["messages"][1]["content"] == [{"type": "text", "text": "T1"}]
    assert final_call["messages"][2]["content"] == [
        {
            "type": "text",
            "text": "Continue the current task from the compacted state above.",
        }
    ]
    await runtime.close()


async def test_later_stage_one_absorbs_an_active_prior_summary(
    user_client,
    test_app,
    pg_engine,
) -> None:
    await _configure_compaction(pg_engine)
    provider = _ScriptedProvider(
        counts=[6000, 1000],
        steps=[
            _StreamStep([{"type": "text", "text": "S2"}]),
            _StreamStep([{"type": "text", "text": "A100"}]),
        ],
    )
    runtime = _install_runtime(test_app, pg_engine, provider)
    session_id = uuid4()
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        db.add(
            Session(
                id=session_id,
                user_id=user.id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="New chat",
                created_at=now,
            )
        )
        await db.flush()
        db.add_all(
            [
                _text_message(
                    session_id,
                    kind="compaction_summary",
                    text="S1",
                    created_at=now,
                ),
                _text_message(
                    session_id,
                    kind="human",
                    text="U11",
                    created_at=now + timedelta(microseconds=1),
                ),
                _text_message(
                    session_id,
                    kind="assistant",
                    text="A11",
                    created_at=now + timedelta(microseconds=2),
                ),
            ]
        )
        await db.commit()

    response = await user_client.post(
        f"/api/sessions/{session_id}/messages",
        json={"content": [{"type": "text", "text": "U100"}], "attachments": []},
    )

    assert response.status_code == 200
    rows = await _messages(pg_engine, session_id)
    assert [(row.content[-1]["text"], row.is_compacted) for row in rows] == [
        ("S1", True),
        ("U11", True),
        ("A11", True),
        ("S2", False),
        ("U100", False),
        ("A100", False),
    ]
    first_summary_call, normal_call = provider.stream_calls
    assert first_summary_call["messages"][0]["content"] == [{"type": "text", "text": "S1"}]
    assert first_summary_call["messages"][-1]["content"] == [
        {"type": "text", "text": "Write the compacted summary now."}
    ]
    assert normal_call["messages"][0]["content"] == [{"type": "text", "text": "S2"}]
    assert "S1" not in json.dumps(normal_call["messages"])
    await runtime.close()


async def test_fresh_external_pending_overflow_removes_context_but_keeps_trigger(
    user_client,
    test_app,
    pg_engine,
) -> None:
    del user_client
    await _configure_context_fence(pg_engine)
    estimator = _ChannelContextEstimator(base_tokens=90, tokens_per_entry=6)
    provider = _ScriptedProvider(counts=[], steps=[])
    runtime = _install_runtime(
        test_app,
        pg_engine,
        provider,
        request_token_estimator=estimator,
    )
    session_id = uuid4()
    trigger = "Keep this trigger, even with <untrusted_channel_context> lookalike text."
    now = datetime.now(UTC)
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        session = Session(
            id=session_id,
            user_id=user.id,
            session_key=f"discord:application:{session_id}",
            channel="discord",
            chat_id="dm-1",
            title="DM",
            created_at=now,
        )
        db.add_all(
            [
                session,
                _paired_discord_config(
                    user=user,
                    binding_generation=binding_generation,
                    application_id=f"app-{session_id}",
                    now=now,
                ),
            ]
        )
        await db.flush()
        pending = _pending_channel_message(
            session=session,
            user=user,
            text=trigger,
            context_indexes=(1, 2, 3),
            binding_generation=binding_generation,
            received_at=now,
        )
        db.add(pending)
        await db.commit()
        turn = await reserve_pending_turn(
            db,
            session_id=session_id,
            runner_instance_id=runtime.runner_instance_id,
        )
    assert turn is not None

    prepared = await runtime._prepare_turn(turn)

    encoded = json.dumps(prepared.messages)
    assert "CHANNEL_CONTEXT_ENTRY_1" not in encoded
    assert "CHANNEL_CONTEXT_ENTRY_2" not in encoded
    assert "CHANNEL_CONTEXT_ENTRY_3" not in encoded
    assert trigger in encoded
    assert len(estimator.calls) == 2
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        promoted = await db.get(Message, pending.id)
        assert promoted is not None
        assert promoted.channel_context == _channel_context(1, 2, 3)
    await runtime.close()


async def test_channel_context_fence_omits_globally_oldest_entries_at_exact_limit(
    user_client,
    test_app,
    pg_engine,
) -> None:
    del user_client
    await _configure_context_fence(pg_engine)
    estimator = _ChannelContextEstimator(base_tokens=70, tokens_per_entry=5)
    provider = _ScriptedProvider(counts=[], steps=[])
    runtime = _install_runtime(
        test_app,
        pg_engine,
        provider,
        request_token_estimator=estimator,
    )
    session_id = uuid4()
    now = datetime.now(UTC)
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        session = Session(
            id=session_id,
            user_id=user.id,
            session_key=f"discord:application:{session_id}",
            channel="discord",
            chat_id="dm-2",
            title="DM",
            created_at=now,
        )
        db.add_all(
            [
                session,
                _paired_discord_config(
                    user=user,
                    binding_generation=binding_generation,
                    application_id=f"app-{session_id}",
                    now=now,
                ),
            ]
        )
        await db.flush()
        earlier = _text_message(
            session_id,
            kind="human",
            text="Earlier trigger",
            created_at=now,
        )
        earlier.channel_context = _channel_context(1, 2)
        pending = _pending_channel_message(
            session=session,
            user=user,
            text="Current trigger",
            context_indexes=(3, 4),
            binding_generation=binding_generation,
            received_at=now + timedelta(microseconds=1),
        )
        db.add_all([earlier, pending])
        await db.commit()
        turn = await reserve_pending_turn(
            db,
            session_id=session_id,
            runner_instance_id=runtime.runner_instance_id,
        )
    assert turn is not None

    prepared = await runtime._prepare_turn(turn)

    encoded = json.dumps(prepared.messages)
    assert "CHANNEL_CONTEXT_ENTRY_1" not in encoded
    assert "CHANNEL_CONTEXT_ENTRY_2" not in encoded
    assert "CHANNEL_CONTEXT_ENTRY_3" in encoded
    assert "CHANNEL_CONTEXT_ENTRY_4" in encoded
    assert "Earlier trigger" in encoded
    assert "Current trigger" in encoded
    await runtime.close()


async def test_channel_context_fence_applies_to_compaction_and_final_requests(
    user_client,
    test_app,
    pg_engine,
) -> None:
    del user_client
    await _configure_context_fence(
        pg_engine,
        max_context_tokens=10_000,
        max_output_tokens=1000,
        compaction_threshold_tokens=5000,
    )
    estimator = _ChannelContextEstimator(base_tokens=8500, tokens_per_entry=400)
    provider = _ScriptedProvider(
        counts=[],
        steps=[_StreamStep([{"type": "text", "text": "Compacted summary"}])],
    )
    runtime = _install_runtime(
        test_app,
        pg_engine,
        provider,
        request_token_estimator=estimator,
    )
    session_id = uuid4()
    now = datetime.now(UTC)
    binding_generation = uuid4()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = (await db.execute(select(User).where(User.email == "user@test.com"))).scalar_one()
        session = Session(
            id=session_id,
            user_id=user.id,
            session_key=f"discord:application:{session_id}",
            channel="discord",
            chat_id="dm-3",
            title="DM",
            created_at=now,
        )
        db.add_all(
            [
                session,
                _paired_discord_config(
                    user=user,
                    binding_generation=binding_generation,
                    application_id=f"app-{session_id}",
                    now=now,
                ),
            ]
        )
        await db.flush()
        earlier = _text_message(
            session_id,
            kind="human",
            text="Earlier trigger survives summary input",
            created_at=now,
        )
        earlier.channel_context = _channel_context(1, 2, 3)
        pending = _pending_channel_message(
            session=session,
            user=user,
            text="Current trigger survives final input",
            context_indexes=(4, 5, 6),
            binding_generation=binding_generation,
            received_at=now + timedelta(microseconds=1),
        )
        db.add_all([earlier, pending])
        await db.commit()
        turn = await reserve_pending_turn(
            db,
            session_id=session_id,
            runner_instance_id=runtime.runner_instance_id,
        )
    assert turn is not None

    prepared = await runtime._prepare_turn(turn)

    assert len(provider.stream_calls) == 1
    summary_json = json.dumps(provider.stream_calls[0]["messages"])
    assert "CHANNEL_CONTEXT_ENTRY_1" not in summary_json
    assert "CHANNEL_CONTEXT_ENTRY_2" not in summary_json
    assert "CHANNEL_CONTEXT_ENTRY_3" in summary_json
    assert "Earlier trigger survives summary input" in summary_json
    final_json = json.dumps(prepared.messages)
    assert "CHANNEL_CONTEXT_ENTRY_4" not in final_json
    assert "CHANNEL_CONTEXT_ENTRY_5" not in final_json
    assert "CHANNEL_CONTEXT_ENTRY_6" in final_json
    assert "Current trigger survives final input" in final_json
    assert len(estimator.calls) <= 15
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        persisted_earlier = await db.get(Message, earlier.id)
        promoted = await db.get(Message, pending.id)
        assert persisted_earlier is not None
        assert promoted is not None
        assert persisted_earlier.channel_context == _channel_context(1, 2, 3)
        assert promoted.channel_context == _channel_context(4, 5, 6)
    await runtime.close()
