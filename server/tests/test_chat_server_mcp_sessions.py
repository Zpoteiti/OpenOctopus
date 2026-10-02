"""Agent-loop ownership and fresh MCP schemas at real provider boundaries."""

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

from test_py3_agent_loop import (
    _configure_provider,
    _events,
    _post,
    _ProviderStep,
    _ScriptedProvider,
    _ScriptedTool,
    _tool_use,
    _ToolStep,
)

from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.devices.mcp_models import (
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SourceMcpTool,
)
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.mcp.catalog import build_server_persisted_catalog
from openctopus_server.mcp.models import ServerMcpEnvelope, ServerStreamableHttpMcpServerConfig
from openctopus_server.tools.base import ToolResult
from openctopus_server.tools.registry import ToolRegistry


class RecordingSessions:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.started = []
        self.finished = []
        self.prepared = []
        self.forgotten = []
        self.active = set()
        self.generation = new_uuid7()

    @asynccontextmanager
    async def run(self, *, user_id, session_id):
        key = (user_id, session_id)
        assert key not in self.active
        self.active.add(key)
        self.started.append(key)
        try:
            yield
        finally:
            self.active.remove(key)
            self.finished.append(key)

    async def prepare(self, *, user_id, session_id, envelope):
        key = (user_id, session_id)
        assert key in self.active
        self.prepared.append(key)
        if self.fail:
            raise ConfigError(ErrorCode.TOOL_MCP_BUSY, "MCP session capacity is busy")
        version = len(self.prepared)
        config = ServerStreamableHttpMcpServerConfig(
            name="search",
            transport="streamable_http",
            url="https://mcp.invalid/mcp",
            enabled_capabilities=[],
        )
        source = SourceMcpCatalog(
            version=1,
            servers=[
                SourceMcpServerCatalog(
                    name="search",
                    tools=[
                        SourceMcpTool(
                            raw_name="query",
                            description=f"Search schema version {version}",
                            input_schema={
                                "type": "object",
                                "properties": {f"query_{version}": {"type": "string"}},
                                "required": [f"query_{version}"],
                            },
                        )
                    ],
                    resources=[],
                    resource_templates=[],
                    prompts=[],
                )
            ],
        )
        catalog = build_server_persisted_catalog([config], source, entry_id_factory=new_uuid7)
        return ServerMcpEnvelope(
            version=1,
            config_revision=envelope.config_revision,
            mcp_servers=[config],
            mcp_catalog=catalog,
        ), {"search": self.generation}

    async def forget_session(self, *, user_id, session_id):
        self.forgotten.append((user_id, session_id))

    async def forget_user(self, *, user_id):
        self.forgotten.append(user_id)


def install_runtime(test_app, pg_engine, provider, manager, tool=None):
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry((tool,) if tool is not None else ()),
        server_mcp_sessions=manager,
        request_token_estimator=lambda **kwargs: 1,
    )
    test_app.state.chat_runtime = runtime
    return runtime


async def wait_for_release(manager):
    async with asyncio.timeout(5):
        while manager.active:
            await asyncio.sleep(0.01)


async def test_run_lease_spans_tools_and_each_model_step_gets_fresh_schema(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    manager = RecordingSessions()
    provider = _ScriptedProvider(
        [
            _ProviderStep(content=[_tool_use("tool-one", "one")]),
            _ProviderStep(content=[{"type": "text", "text": "done"}]),
        ]
    )
    tool = _ScriptedTool([_ToolStep(result=ToolResult(content="one"))])
    runtime = install_runtime(test_app, pg_engine, provider, manager, tool)
    session_id = uuid4()
    try:
        response = await _post(user_client, session_id, "run")
        assert response.status_code == 200
        assert all(
            event["status"] == "completed"
            for event in _events(response)
            if event["type"] == "turn_finished"
        ), response.text
        await wait_for_release(manager)
        assert len(manager.started) == len(manager.finished) == 1
        assert manager.prepared == manager.started * 2
        schemas = [
            next(tool for tool in call["tools"] if tool["name"] == "mcp_search_query")
            for call in provider.calls
        ]
        assert "version 1" in schemas[0]["description"]
        assert "version 2" in schemas[1]["description"]
        assert "query_1" in schemas[0]["input_schema"]["required"]
        assert "query_2" in schemas[1]["input_schema"]["required"]
        assert tool.calls == ["one"]

        deleted = await user_client.delete(f"/api/sessions/{session_id}")
        assert deleted.status_code == 204
        assert manager.forgotten == manager.started
    finally:
        await runtime.close()


async def test_failed_mcp_preparation_releases_run_without_calling_provider(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    manager = RecordingSessions(fail=True)
    provider = _ScriptedProvider([])
    runtime = install_runtime(test_app, pg_engine, provider, manager)
    try:
        response = await _post(user_client, uuid4(), "run")
        await wait_for_release(manager)
        assert not provider.calls
        assert manager.started == manager.finished
        assert len(manager.started) == 1
        assert "[tool_mcp_busy]" in response.text
    finally:
        await runtime.close()


async def test_shutdown_during_provider_releases_conversation_lease(
    user_client,
    test_app,
    pg_engine,
):
    await _configure_provider(pg_engine)
    manager = RecordingSessions()
    started = asyncio.Event()
    provider = _ScriptedProvider(
        [
            _ProviderStep(
                content=[{"type": "text", "text": "done"}],
                started=started,
                release=asyncio.Event(),
            )
        ]
    )
    runtime = install_runtime(test_app, pg_engine, provider, manager)
    request = asyncio.create_task(_post(user_client, uuid4(), "run"))
    try:
        await asyncio.wait_for(started.wait(), 5)
        assert manager.active
        await runtime.close()
        assert not manager.active
        assert manager.started == manager.finished
    finally:
        request.cancel()
        await asyncio.gather(request, return_exceptions=True)
        await runtime.close()


async def test_restricted_channel_profile_does_not_open_mcp(pg_engine):
    from openctopus_server.chat.types import TurnStart
    from openctopus_server.mcp.models import empty_server_mcp_envelope

    manager = RecordingSessions(fail=True)
    runtime = ChatRuntime(pg_engine, server_mcp_sessions=manager)
    turn = TurnStart(
        session_id=uuid4(),
        turn_id=uuid4(),
        message_ids=(),
        effort=None,
        tool_profile="participant_safe",
    )
    try:
        envelope = empty_server_mcp_envelope()
        result, generations = await runtime._prepare_server_mcp(turn, uuid4(), envelope)
        assert result is envelope
        assert not generations
        assert not manager.started and not manager.prepared
    finally:
        await runtime.close()
