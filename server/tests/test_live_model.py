"""Opt-in end-to-end check against an explicitly supplied OpenAI-compatible model."""
import asyncio
import json
import os
from uuid import uuid4

import pytest
from test_harness_context import configure, post

from openctopus_server.chat.runner import ChatRuntime

pytestmark = pytest.mark.skipif(not os.environ.get('OO_TEST_MODEL_ENDPOINT'), reason='Set OO_TEST_MODEL_ENDPOINT and OO_TEST_MODEL_NAME for live model acceptance')


async def test_live_model_streams_through_harness(user_client, test_app, pg_engine):
    await configure(pg_engine, llm_protocol='openai', llm_endpoint=os.environ['OO_TEST_MODEL_ENDPOINT'],
                    llm_model=os.environ['OO_TEST_MODEL_NAME'], llm_api_key=os.environ.get('OO_TEST_MODEL_KEY', 'local'),
                    llm_max_output_tokens=128, llm_max_context_tokens=262144)
    runtime = ChatRuntime(pg_engine)
    test_app.state.chat_runtime = runtime
    try:
        events = await asyncio.wait_for(post(user_client, uuid4(), 'Reply with exactly HARNESS_READY. Do not call any tools.'), 90)
        final = [event['message'] for event in events if event['type'] == 'message_persisted' and event['message']['message_kind'] == 'assistant']
        assert 'HARNESS_READY' in json.dumps(final)
        assert any(event['type'] == 'token_delta' for event in events)
    finally:
        await runtime.close()
