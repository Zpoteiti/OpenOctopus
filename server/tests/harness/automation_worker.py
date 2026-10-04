"""Real DBOS wake-ups around Heartbeat publication and official Memory writes."""
import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from pydantic_ai.messages import ModelRequest, ToolReturnPart
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from openctopus_server.automations.dream import DreamService
from openctopus_server.automations.durable import bind_automations, start_schedules
from openctopus_server.automations.heartbeat import (
    HeartbeatDecision,
    HeartbeatEvaluation,
    HeartbeatPulse,
)
from openctopus_server.chat.durable import enqueue_transaction
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import Message, Session, SystemConfig, User
from openctopus_server.provider.jev import JevChoiceAnswer
from openctopus_server.provider.runtime import ProviderResult
from openctopus_server.services.heartbeat import publish_heartbeat_phase_two
from openctopus_server.tools.base import Tool, ToolResult, ToolRoutingMode
from openctopus_server.tools.registry import ToolRegistry

ROOT = Path(os.environ['HARNESS_TEST_ROOT'])
KIND = os.environ['HARNESS_TEST_CASE']


async def barrier():
    (ROOT / 'effect_committed').touch()
    while not (ROOT / 'continue').exists():
        await asyncio.sleep(0.02)


class Counter(Tool):
    routing_mode = ToolRoutingMode.PURE_SERVER
    def __init__(self, engine):
        self.engine = engine
    def name(self):
        return 'counter'
    def schema(self):
        return {'name': 'counter', 'input_schema': {'type': 'object', 'properties': {}}}
    async def execute(self, args, ctx):
        async with self.engine.begin() as conn:
            await conn.execute(text('INSERT INTO recovery_effects(owner) VALUES (:owner)'), {'owner': str(ctx.user_id)})
        await barrier()
        return ToolResult('committed')


class Provider:
    def native_model(self, config):
        async def stream(messages, info):
            if any(isinstance(p, ToolReturnPart) for m in messages if isinstance(m, ModelRequest) for p in m.parts):
                yield 'completed wake-up'
            else:
                yield {0: DeltaToolCall(name='counter', json_args='{}', tool_call_id='counter-1')}
        return FunctionModel(stream_function=stream)
    async def close(self):
        pass


class Workspace:
    async def stat(self, *args, **kwargs):
        return SimpleNamespace(size=len(b'## Active Tasks\n- Perform the due check.'))
    async def read(self, *args, **kwargs):
        return b'## Active Tasks\n- Perform the due check.'


class Gate:
    async def status(self):
        return SimpleNamespace(state='available')
    async def evaluate(self, *, state, questions):
        return {key: JevChoiceAnswer(type='choice', choice='run', probabilities={'run': 1.0, 'skip': 0.0}, confidence=1.0) for key in questions}


class Writer:
    async def propose_memory_update(self, **kwargs):
        state = json.loads(kwargs['messages'][0]['content'][0]['text'])
        return ProviderResult(content=[{'type': 'tool_use', 'name': 'propose_memory_update', 'input': {
            'edits': [], 'append': 'Durable preference.\n', 'append_source_ids': [state['conversations'][0]['id']],
        }}], fingerprint='fixture')


async def main():
    engine = create_async_engine(os.environ['HARNESS_TEST_DATABASE'])
    runtime = ChatRuntime(engine, provider_factory=lambda _: Provider(), tool_registry=ToolRegistry([Counter(engine)]))
    async def decide(**kwargs):
        return HeartbeatEvaluation(HeartbeatDecision('run', ('Perform the due check.',)), 'ready')
    async def publish(request):
        return await publish_heartbeat_phase_two(engine, runtime, request)
    heartbeat = HeartbeatPulse(engine=engine, runtime=SimpleNamespace(evaluate_heartbeat_decision=decide),
                               workspace_service=Workspace(), publish_phase_two=publish)
    dream = DreamService(engine=engine, memory=runtime.memory.store, jev=Gate(), writer=Writer())
    original_write = runtime.memory.store.write
    async def write(*args, **kwargs):
        result = await original_write(*args, **kwargs)
        await barrier()
        return result
    if KIND == 'dream':
        runtime.memory.store.write = write
    bind_automations(heartbeat, dream)
    await runtime.durable.start()
    await start_schedules()
    if os.environ.get('HARNESS_TEST_SUBMIT') == '1':
        async with engine.begin() as conn:
            await conn.execute(text('CREATE TABLE recovery_effects (owner text)'))
        async with AsyncSession(engine, expire_on_commit=False) as db:
            owner = User(email='automation@test.com', name='Owner', password_hash='fixture', timezone='UTC')
            db.add(owner)
            db.add_all([SystemConfig(key=key, value=value) for key, value in {
                'llm_endpoint': 'http://fixture.test', 'llm_model': 'fixture', 'llm_api_key': 'fixture-secret',
            }.items()])
            await db.flush()
            if KIND == 'dream':
                session = Session(user_id=owner.id, session_key=f'web:{uuid4()}', channel='web', chat_id='source')
                db.add(session)
                await db.flush()
                db.add(Message(session_id=session.id, message_kind='human', content=[{'type': 'text', 'text': 'Remember this preference.'}],
                               sender_id=str(owner.id), sender_classification='owner', ingress_tool_profile='owner_full',
                               created_at=datetime.now(UTC)-timedelta(days=1)))
            scheduled = datetime.now(UTC) + timedelta(seconds=2)
            # Concurrent pulses prove the upstream queue's per-user deduplication.
            for index in range(3):
                await enqueue_transaction(db, {'workflow_name': 'oo.automation_pulse', 'workflow_id': f'pulse-{index}',
                    'queue_name': 'oo-maintenance', 'app_version': 'oo-harness-1', 'delay_seconds': 2},
                    scheduled + timedelta(milliseconds=index), KIND)
            await db.commit()
            (ROOT / 'owner').write_text(str(owner.id))
    (ROOT / 'ready').touch()
    await asyncio.Event().wait()


asyncio.run(main())
