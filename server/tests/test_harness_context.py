import json
import re
from uuid import UUID, uuid4

import pytest
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    SystemPromptPart,
    TextPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.memory import memory_path
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import AgentContext, Message, SystemConfig, ToolOverflow
from openctopus_server.tools.base import Tool, ToolResult, ToolRoutingMode
from openctopus_server.tools.registry import ToolRegistry


class Provider:
    def __init__(self, model):
        self.model = model
    def native_model(self, config):
        return self.model
    async def close(self):
        pass


async def configure(engine, **values):
    values = {'llm_endpoint': 'http://test.invalid', 'llm_model': 'fixture', 'llm_api_key': 'fixture', **values}
    async with AsyncSession(engine) as db:
        for key, value in values.items():
            await db.merge(SystemConfig(key=key, value=value))
        await db.commit()


async def post(client, session, text):
    response = await client.post(f'/api/sessions/{session}/messages', json={'content': [{'type': 'text', 'text': text}], 'attachments': []})
    assert response.status_code == 200, response.text
    events = [json.loads(line) for line in response.text.splitlines()]
    assert events[-1]['type'] == 'turn_finished', events
    assert events[-1]['status'] == 'completed', json.dumps(events, ensure_ascii=False)
    return events


async def test_native_context_compacts_without_modifying_original_history(user_client, test_app, pg_engine):
    await configure(pg_engine, llm_max_context_tokens=10000, llm_compaction_threshold_tokens=5000, llm_max_output_tokens=128)
    summaries = []
    async def summarize(messages, info):
        summaries.append(messages)
        return ModelResponse([TextPart('The user is testing persistence; preserve their requests.')])
    async def stream(messages, info):
        yield 'acknowledged'
    model = FunctionModel(function=summarize, stream_function=stream)
    runtime = ChatRuntime(pg_engine, provider_factory=lambda _: Provider(model))
    test_app.state.chat_runtime = runtime
    session = uuid4()
    try:
        for index in range(7):
            await post(user_client, session, f'original-{index}: ' + 'evidence ' * 3000)
        async with AsyncSession(pg_engine) as db:
            saved = await db.get(AgentContext, session)
            history = ModelMessagesTypeAdapter.validate_python(saved.messages)
            originals = list((await db.scalars(select(Message).where(Message.session_id == session))).all())
        assert summaries, [(type(m).__name__, len(str(m.parts)), getattr(m, "usage", None)) for m in history]
        assert any(isinstance(part, SystemPromptPart) and 'Summary of previous conversation' in part.content
                   for message in history for part in message.parts)
        assert len(originals) == 14
        assert all(any(f'original-{index}:' in json.dumps(row.content) for row in originals) for index in range(7))
        assert len(history) < len(originals)
    finally:
        await runtime.close()


async def test_official_memory_tool_and_injection_share_editor_store(user_client, test_app, pg_engine):
    await configure(pg_engine)
    captured = []
    async def stream(messages, info):
        captured.append(messages)
        if not any(isinstance(part, ToolReturnPart) for message in messages for part in message.parts):
            yield {0: DeltaToolCall(name='write_memory', json_args='{"content":"Prefers concise answers."}', tool_call_id='remember-1')}
        else:
            yield 'remembered'
    runtime = ChatRuntime(pg_engine, provider_factory=lambda _: Provider(FunctionModel(stream_function=stream)))
    test_app.state.chat_runtime = runtime
    try:
        owner = (await user_client.get('/api/me')).json()
        await post(user_client, uuid4(), 'remember my preference')
        note = await runtime.memory.store.read(memory_path(UUID(owner['id'])), max_chars=1000)
        assert note.content == 'Prefers concise answers.\n'
        assert (await user_client.get('/api/memory/MEMORY.md')).json()['version'] == note.version
        assert 'Prefers concise answers.' in ModelMessagesTypeAdapter.dump_json(captured[-1]).decode()
    finally:
        await runtime.close()


class LargeResult(Tool):
    routing_mode = ToolRoutingMode.PURE_SERVER
    def name(self):
        return 'large_result'
    def schema(self):
        return {'name': self.name(), 'input_schema': {'type': 'object', 'properties': {}}}
    async def execute(self, args, ctx):
        return ToolResult('long evidence\n' * 4000 + 'EXACT-END-SENTINEL')


async def test_official_tool_spill_can_read_original_tail(user_client, test_app, pg_engine):
    await configure(pg_engine)
    async def stream(messages, info):
        returns = [part for message in messages if isinstance(message, ModelRequest)
                   for part in message.parts if isinstance(part, ToolReturnPart)]
        if not returns:
            yield {0: DeltaToolCall(name='large_result', json_args='{}', tool_call_id='large-1')}
        elif returns[-1].tool_name == 'large_result':
            value = returns[-1].model_response_str()
            assert len(value) < 5000
            handle = re.search(r'[a-f0-9]{64}', value).group()
            yield {0: DeltaToolCall(name='read_tool_result', json_args=json.dumps({'handle': handle, 'from_end': True, 'limit': 2}), tool_call_id='read-1')}
        else:
            assert 'EXACT-END-SENTINEL' in returns[-1].model_response_str()
            yield 'tail recovered'
    runtime = ChatRuntime(pg_engine, provider_factory=lambda _: Provider(FunctionModel(stream_function=stream)),
                          tool_registry=ToolRegistry([LargeResult()]))
    test_app.state.chat_runtime = runtime
    try:
        session = uuid4()
        await post(user_client, session, 'read a large result')
        async with AsyncSession(pg_engine) as db:
            stored = await db.scalar(select(ToolOverflow.data).where(ToolOverflow.session_id == session))
        assert b'EXACT-END-SENTINEL' in stored
    finally:
        await runtime.close()


@pytest.mark.parametrize('name', ['reviewer', 'Review Guide_中文', 'a' * 64, 'builtin-create-skill'])
async def test_official_skills_load_on_demand(user_client, test_app, pg_engine, name):
    from test_workspace_prompt import _PromptWorkspace

    from openctopus_server.chat.skills import personal_skill_id
    await configure(pg_engine)
    workspace = _PromptWorkspace({f'skills/{name}/SKILL.md': f'---\nname: {name}\ndescription: Review changes\n---\nREVIEW-PRIVATE-INSTRUCTIONS'.encode()})
    captured = []
    async def stream(messages, info):
        captured.append(ModelMessagesTypeAdapter.dump_json(messages).decode())
        if len(captured) == 1:
            assert 'REVIEW-PRIVATE-INSTRUCTIONS' not in captured[-1]
            tool = next(t for t in info.function_tools if t.name == 'load_capability')
            assert tool.parameters_json_schema
            yield {0: DeltaToolCall(name='load_capability', json_args=json.dumps({'id': personal_skill_id(name)}), tool_call_id='load-reviewer')}
        else:
            assert 'REVIEW-PRIVATE-INSTRUCTIONS' in captured[-1]
            yield 'skill loaded'
    runtime = ChatRuntime(pg_engine, provider_factory=lambda _: Provider(FunctionModel(stream_function=stream)), workspace_service=workspace)
    test_app.state.chat_runtime = runtime
    try:
        await post(user_client, uuid4(), 'review with the skill')
        assert len(captured) == 2
    finally:
        await runtime.close()


async def test_history_recall_cannot_read_another_session(user_client, test_app, pg_engine):
    await configure(pg_engine)
    other_session, session = uuid4(), uuid4()
    calls = 0
    async def stream(messages, info):
        nonlocal calls
        calls += 1
        if calls < 3:
            yield 'noted'
        else:
            returns = [p for m in messages for p in m.parts if isinstance(p, ToolReturnPart)]
            if not returns:
                yield {0: DeltaToolCall(name='search_conversation_history', json_args=json.dumps({'query': 'SENTINEL', 'run_id': str(other_session)}), tool_call_id='search-other')}
            elif len(returns) == 1:
                assert 'PRIVATE-OTHER-SENTINEL' not in returns[-1].model_response_str()
                assert 'No persisted history' in returns[-1].model_response_str()
                yield {0: DeltaToolCall(name='search_conversation_history', json_args='{"query":"OWN-RECALL-SENTINEL"}', tool_call_id='search-own')}
            else:
                assert 'OWN-RECALL-SENTINEL' in returns[-1].model_response_str()
                assert 'PRIVATE-OTHER-SENTINEL' not in returns[-1].model_response_str()
                yield 'recalled'
    runtime = ChatRuntime(pg_engine, provider_factory=lambda _: Provider(FunctionModel(stream_function=stream)))
    test_app.state.chat_runtime = runtime
    try:
        await post(user_client, other_session, 'PRIVATE-OTHER-SENTINEL')
        await post(user_client, session, 'OWN-RECALL-SENTINEL')
        await post(user_client, session, 'find the earlier value')
        assert calls == 5
    finally:
        await runtime.close()


async def test_request_budget_is_shared_by_children_but_separate_between_roots(pg_engine):
    from types import SimpleNamespace

    import pytest
    from pydantic_ai.exceptions import UsageLimitExceeded

    from openctopus_server.chat.delegation import reserve_request
    from openctopus_server.chat.scope import active_run
    from openctopus_server.db.models import AgentRequest, Session, User
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        owner = User(email='budget@test.invalid', name='Budget', password_hash='fixture')
        db.add(owner)
        await db.flush()
        parent = Session(user_id=owner.id, session_key=f'web:{uuid4()}', channel='web', chat_id='parent')
        child = Session(user_id=owner.id, session_key=f'agent:{uuid4()}', channel='web', chat_id='child')
        db.add_all([parent, child])
        await db.flush()
        records = [AgentRequest(id=uuid4(), root_workflow_id='root-a', session_id=parent.id if i % 2 else child.id) for i in range(200)]
        db.add_all(records)
        await db.commit()
    token = active_run.set(SimpleNamespace(runtime=SimpleNamespace(engine=pg_engine)))
    try:
        await reserve_request('root-a', child.id, records[0].id)
        with pytest.raises(UsageLimitExceeded):
            await reserve_request('root-a', child.id, uuid4())
        await reserve_request('root-b', parent.id, uuid4())
    finally:
        active_run.reset(token)


async def test_restricted_channel_run_excludes_owner_history_memory_and_tools(pg_engine):
    from datetime import UTC, datetime, timedelta

    from test_channel_ingress import _discord_config

    from openctopus_server.chat.runner import _SessionState
    from openctopus_server.chat.types import TurnStart
    from openctopus_server.db.models import Session, TurnRun, User
    await configure(pg_engine)
    owner_id, session_id, first_id, input_id, turn_id, generation = (uuid4() for _ in range(6))
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine) as db:
        owner = User(id=owner_id, email='restricted@test.invalid', name='Owner', password_hash='fixture')
        db.add(owner)
        await db.flush()
        await _discord_config(db, owner, binding_generation=generation, allow_list=['participant'])
        db.add(Session(id=session_id, user_id=owner_id, session_key='discord:restricted', channel='discord', chat_id='restricted'))
        await db.flush()
        db.add(Message(id=first_id, session_id=session_id, message_kind='human', content=[{'type':'text','text':'OWNER_PRIVATE_HISTORY'}],
                       sender_id='owner-1', sender_classification='owner', ingress_tool_profile='owner_full',
                       channel_binding_generation=generation, created_at=now-timedelta(seconds=1)))
        db.add(Message(id=input_id, session_id=session_id, message_kind='human', content=[{'type':'text','text':'PUBLIC_PARTICIPANT_INPUT'}],
                       sender_id='participant', sender_classification='allowed_non_owner', ingress_tool_profile='message_only',
                       channel_binding_generation=generation, created_at=now))
        await db.flush()
        db.add(AgentContext(session_id=session_id, through_message_id=first_id, tool_profile='owner_full',
                            messages=ModelMessagesTypeAdapter.dump_python([ModelRequest([SystemPromptPart('OWNER_PRIVATE_HISTORY')])], mode='json')))
        db.add(TurnRun(id=turn_id, session_id=session_id, runner_instance_id=uuid4(), status='running', tool_profile='message_only',
                      workflow_id=str(turn_id), input_message_ids=[str(input_id)]))
        await db.commit()
    seen = []
    async def stream(messages, info):
        serialized = ModelMessagesTypeAdapter.dump_json(messages).decode()
        assert 'PUBLIC_PARTICIPANT_INPUT' in serialized
        assert 'OWNER_PRIVATE_HISTORY' not in serialized
        assert 'OWNER_PRIVATE_MEMORY' not in serialized
        assert not info.function_tools
        seen.append(True)
        yield 'safe reply'
    runtime = ChatRuntime(pg_engine, provider_factory=lambda _: Provider(FunctionModel(stream_function=stream)), tool_registry=ToolRegistry([LargeResult()]))
    try:
        await runtime.memory.store.write(memory_path(owner_id), 'OWNER_PRIVATE_MEMORY', expected_version=None)
        await runtime._execute_chain(_SessionState(session_id), TurnStart(session_id, turn_id, (input_id,), None, 'message_only'))
        assert seen == [True]
        async with AsyncSession(pg_engine) as db:
            assert (await db.get(TurnRun, turn_id)).status == 'completed'
    finally:
        await runtime.close()
