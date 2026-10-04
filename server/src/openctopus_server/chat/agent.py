"""SDK-owned agent loop with OO's authorized inputs and product event projection."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid5

from dbos import DBOS
from dbos import error as dbos_error
from pydantic_ai import Agent, AgentRunResult, RunContext
from pydantic_ai.capabilities import AbstractCapability, WrapModelRequestHandler
from pydantic_ai.durable_exec.dbos import DBOSDurability
from pydantic_ai.exceptions import ToolFailed, UsageLimitExceeded
from pydantic_ai.messages import (
    AgentStreamEvent,
    FunctionToolResultEvent,
    ModelResponse,
    PartDeltaEvent,
    PartStartEvent,
    RetryPromptPart,
    TextPart,
    TextPartDelta,
    ThinkingPart,
    ThinkingPartDelta,
    ToolReturn,
    ToolReturnPart,
)
from pydantic_ai.models import Model, ModelRequestContext, ModelResolutionContext
from pydantic_ai.toolsets import DynamicToolset, FunctionToolset
from pydantic_ai.usage import UsageLimits
from pydantic_ai_harness.conversation_search import ConversationSearch
from pydantic_ai_harness.memory import Memory
from pydantic_ai_harness.subagents import SubAgent, SubAgents
from pydantic_ai_harness.tool_output_limits import ToolOutputLimits
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.compaction_capability import ConfiguredCompaction
from openctopus_server.chat.context import PendingSelectionChangedError
from openctopus_server.chat.durable import initial_model, prepare_turn
from openctopus_server.chat.durable_tools import DurableTools
from openctopus_server.chat.harness_stores import ConversationHistory, ToolOutputStore
from openctopus_server.chat.history import save_context
from openctopus_server.chat.scope import active_run
from openctopus_server.chat.skills import WorkspaceSkills, skill_sources
from openctopus_server.chat.types import TurnStart
from openctopus_server.provider.messages import model_messages, prompt_content, response_blocks
from openctopus_server.provider.runtime import (
    ProviderInvocationError,
    model_settings,
    provider_fingerprint,
    safe_provider_rejection,
)
from openctopus_server.services.messages import (
    finish_final_turn,
    finish_tool_batch_and_continue,
    persist_human_marker,
    persist_tool_result,
)
from openctopus_server.tools.sdk import toolset_from_schemas

if TYPE_CHECKING:
    from openctopus_server.chat.runner import (
        ChatRuntime,
        _CompletedProviderTurn,
        _PreparedTurn,
        _SessionState,
    )


class AgentStoppedError(Exception):
    """The product cancellation/failure has already been persisted and published."""


@dataclass
class AgentRun:
    runtime: ChatRuntime
    state: _SessionState
    turn: TurnStart
    prepared: _PreparedTurn | None = None
    completed: _CompletedProviderTurn | None = None
    started: bool = False
    requests: int = 0
    last_result_id: UUID | None = None
    tool_results: set[str] = field(default_factory=set)
    user_id: UUID | None = None
    model_revision: str = ""
    root_workflow_id: str = ""
    worker: bool = False
    authority_message_id: UUID | None = None
    skill_files: dict[str, bytes] = field(default_factory=dict)
    repeated_call: tuple[str, str] | None = None
    repeated_count: int = 0
    admission: AsyncExitStack = field(default_factory=AsyncExitStack)

    async def prepare(self, ctx: RunContext[str]) -> _PreparedTurn:
        if self.prepared is not None and self.completed is None:
            return self.prepared
        runtime, state = self.runtime, self.state
        if self.completed is not None:
            uses = [block for block in self.completed.assistant.content if block.get("type") == "tool_use"]
            returned = {part.tool_call_id for message in ctx.messages for part in message.parts
                        if isinstance(part, (ToolReturnPart, RetryPromptPart))}
            self.tool_results = {str(block["id"]) for block in uses if block["id"] in returned}
            if uses and uses[-1]["id"] in returned:
                self.last_result_id = uuid5(self.turn.turn_id, f"tool:{uses[-1]['id']}")
            for block in uses:
                key = (block["name"], json.dumps(block["input"], sort_keys=True, separators=(",", ":")))
                self.repeated_count = self.repeated_count + 1 if key == self.repeated_call else 1
                self.repeated_call = key
            if await runtime._cancel_requested(self.turn.session_id):
                remaining = [str(block["id"]) for block in self.completed.assistant.content
                             if block.get("type") == "tool_use" and block["id"] not in self.tool_results]
                await runtime._cancel_turn(state, self.turn, outcome_unknown_tool_ids=[], cancelled_tool_ids=remaining)
                raise AgentStoppedError
            if self.requests >= 200:
                await runtime._fail_iteration_limit(state, self.turn)
                raise AgentStoppedError
            if self.repeated_count >= 3 and self.repeated_call is not None:
                async with AsyncSession(runtime.engine, expire_on_commit=False) as db:
                    marker = await persist_human_marker(db, turn=self.turn, text_content=(
                        f"You've called `{self.repeated_call[0]}` with the same args 3 times. "
                        "Reconsider or ask the user for clarification."
                    ))
                await runtime._publish_message(state, self.turn, marker)
                self.repeated_call, self.repeated_count = None, 0
            async with AsyncSession(runtime.engine, expire_on_commit=False) as db:
                next_turn = await finish_tool_batch_and_continue(
                    db, turn=self.turn, runner_instance_id=runtime.runner_instance_id,
                )
            await runtime._publish_turn_finished(state, self.turn, status="completed", final_message_id=self.last_result_id)
            await runtime._transfer_turn_subscriber(state, self.turn.turn_id, next_turn)
            self.turn = next_turn
            self.completed = None
            self.last_result_id = None
            self.tool_results.clear()
        self.started = False
        try:
            user_id = await runtime._session_owner_id(self.turn.session_id)
            await self.admission.enter_async_context(runtime._context_slot(user_id))
            for attempt in range(2):
                try:
                    self.prepared = await prepare_turn(runtime.runner_instance_id, self.turn, self.model_revision)
                    self.turn = self.prepared.turn
                    async with state.lock:
                        state.streams.set_active_turn(self.turn, inherit_preview=True)
                    break
                except PendingSelectionChangedError:
                    if attempt == 1:
                        raise
            assert self.prepared is not None
            if self.authority_message_id is None and self.turn.message_ids:
                self.authority_message_id = self.turn.message_ids[-1]
            await runtime._claim_promoted_subscriber(state, self.turn)
            await runtime._publish_turn_started(state, self.turn)
            self.started = True
            if await runtime._cancel_requested(self.turn.session_id):
                await runtime._cancel_turn(state, self.turn, outcome_unknown_tool_ids=[], cancelled_tool_ids=[])
                raise AgentStoppedError
            return self.prepared
        except AgentStoppedError:
            raise

    async def check_authority(self) -> None:
        from openctopus_server.db.models import Message, Session, WorkflowCancellation
        from openctopus_server.services.messages import (
            inbound_authority_is_current,
            locked_channel_config,
        )
        async with AsyncSession(self.runtime.engine) as db:
            row = await db.get(Message, self.authority_message_id) if self.authority_message_id else None
            session = await db.get(Session, row.session_id) if row else None
            if row is None or session is None or session.user_id != self.user_id:
                raise ToolFailed("Conversation authority is no longer available")
            if await db.get(WorkflowCancellation, self.root_workflow_id) is not None:
                raise AgentStoppedError
            config = await locked_channel_config(db, user_id=self.user_id, channel=session.channel)
            if not inbound_authority_is_current(row, channel=session.channel, config=config):
                raise ToolFailed("The sender or channel binding is no longer authorized")

    async def model(self, ctx: RunContext[str]) -> str:
        prepared = await self.prepare(ctx)
        return "oo:" + prepared.model_id

    async def tools(self, ctx: RunContext[str]) -> FunctionToolset[str]:
        assert self.prepared is not None
        return toolset_from_schemas(self.prepared.tools, self.execute)

    async def execute(self, name: str, args: dict[str, Any], ctx: RunContext[str]) -> ToolReturn[Any]:
        assert self.completed is not None and ctx.tool_call_id is not None
        result, message_id = await self.runtime._execute_agent_tool(
            self.state, self.completed,
            {"id": ctx.tool_call_id, "name": name, "input": args},
        )
        self.tool_results.add(ctx.tool_call_id)
        self.last_result_id = message_id
        if result.is_error:
            raise ToolFailed(result.content if isinstance(result.content, str) else json.dumps(result.content))
        parts = prompt_content(result.content)
        if isinstance(parts, str):
            return ToolReturn(parts)
        return ToolReturn(
            "\n".join(part for part in parts if isinstance(part, str)),
            content=[part for part in parts if not isinstance(part, str)] or None,
        )

    async def events(self, ctx: RunContext[str], stream: AsyncIterable[AgentStreamEvent]) -> None:
        async for event in stream:
            channel, content = "text", ""
            if isinstance(event, PartStartEvent) and isinstance(event.part, (TextPart, ThinkingPart)):
                content = event.part.content
                channel = "thinking" if isinstance(event.part, ThinkingPart) else "text"
            elif isinstance(event, PartDeltaEvent):
                if isinstance(event.delta, TextPartDelta):
                    content = event.delta.content_delta
                elif isinstance(event.delta, ThinkingPartDelta):
                    content, channel = event.delta.content_delta or "", "thinking"
            elif isinstance(event, FunctionToolResultEvent) and event.tool_call_id not in self.tool_results:
                # Validation/unknown-tool failures never reach the routed callback,
                # but still need a paired product-history result.
                part = event.part
                content_value = part.model_response() if isinstance(part, RetryPromptPart) else part.model_response_str()
                async with AsyncSession(self.runtime.engine, expire_on_commit=False) as db:
                    _, message = await persist_tool_result(db, turn=self.turn, block={
                        "type": "tool_result", "tool_use_id": event.tool_call_id,
                        "content": content_value,
                        "is_error": isinstance(part, RetryPromptPart) or part.outcome == "failed",
                    })
                self.last_result_id = message.id
                self.tool_results.add(event.tool_call_id)
                await self.runtime._publish_message(self.state, self.turn, message)
            if content:
                await self.runtime._publish(self.state, self.turn.turn_id, {
                    "type": "token_delta", "turn_id": str(self.turn.turn_id), "channel": channel, "text": content,
                })

    async def run(self) -> AgentRunResult[str] | None:
        run_id = str(self.turn.turn_id)
        self.runtime._agent_runs[run_id] = self
        scope_token = active_run.set(self)
        self.root_workflow_id = self.root_workflow_id or DBOS.workflow_id or run_id
        try:
            revision = self.model_revision or await initial_model(self.runtime.runner_instance_id)
            self.model_revision = revision
            self.user_id = await self.runtime._session_owner_id(self.turn.session_id)
            if self.turn.tool_profile == "owner_full":
                self.skill_files = await skill_sources()
            agent = self.runtime.worker_agent if self.worker else self.runtime.restricted_agent if self.turn.tool_profile == "message_only" else self.runtime.agent
            result = await agent.run(
                model="oo:" + revision, deps=run_id, conversation_id=str(self.turn.session_id), run_id=run_id,
                usage_limits=UsageLimits(request_limit=50 if self.worker else 200),
            )
            assert self.completed is not None
            completed = self.completed
            runtime = self.runtime
            if runtime._channel_final_delivery is not None and not self.worker:
                try:
                    await runtime._channel_final_delivery.deliver_final(
                        turn=self.turn, assistant=completed.assistant, user_id=completed.user_id,
                        channel=completed.current_channel, chat_id=completed.current_chat_id,
                        binding_generation=completed.current_binding_generation,
                    )
                except Exception:
                    pass  # Delivery owns its terminal record; the assistant is already saved.
            async with AsyncSession(runtime.engine, expire_on_commit=False) as db:
                await finish_final_turn(db, turn=self.turn)
            await runtime._publish_turn_finished(self.state, self.turn, status="completed", final_message_id=completed.assistant.id)
            await runtime._close_turn_subscriber(self.state, self.turn.turn_id)
            return result
        except AgentStoppedError:
            from openctopus_server.chat.cancellation import reconcile_cancellation
            async with AsyncSession(self.runtime.engine, expire_on_commit=False) as db:
                cancelled = await reconcile_cancellation(db, self.turn.session_id, DBOS.workflow_id or run_id)
            if cancelled is not None:
                turn, rows, marker = cancelled
                for row in [*rows, marker]:
                    await self.runtime._publish_message(self.state, turn, row)
                await self.runtime._publish_turn_finished(self.state, turn, status="cancelled", final_message_id=marker.id)
            await self.runtime._close_turn_subscriber(self.state, self.turn.turn_id)
        except dbos_error.DBOSException:
            raise
        except UsageLimitExceeded:
            await self.runtime._fail_iteration_limit(self.state, self.turn)
        except Exception as exc:
            try:
                if self.completed is not None:
                    await self.runtime._fail_unexpected_chain(self.state)
                elif self.started:
                    error = exc if isinstance(exc, ProviderInvocationError) else ProviderInvocationError(
                        "Provider request failed",
                        safe_message=safe_provider_rejection(exc, config=self.prepared.config) if self.prepared else None,
                    )
                    await self.runtime._fail_provider(self.state, self.turn, error=error)
                else:
                    await self.runtime._fail_preflight(self.state, self.turn, exc)
            except Exception:
                await self.runtime._fail_unexpected_chain(self.state)
        finally:
            await self.admission.aclose()
            self.runtime._agent_runs.pop(run_id, None)
            active_run.reset(scope_token)
        return None


@dataclass
class ProductLifecycle(AbstractCapability[str]):
    runtime: ChatRuntime

    def scope(self, ctx: RunContext[str]) -> AgentRun:
        return self.runtime._agent_runs[ctx.deps]

    async def before_run(self, ctx: RunContext[str]) -> None:
        # Tool discovery is a replayable DBOS step. Prepare outside that step so
        # replay reconstructs process-local scope even when discovery is skipped.
        await self.scope(ctx).prepare(ctx)

    async def before_node_run(self, ctx: RunContext[str], *, node: Any) -> Any:
        if Agent.is_model_request_node(node):
            await self.scope(ctx).prepare(ctx)
        return node

    async def before_tool_execute(self, ctx: RunContext[str], *, call: Any, tool_def: Any, args: Any) -> Any:
        await self.scope(ctx).check_authority()
        return args

    async def resolve_model_id(self, ctx: ModelResolutionContext[str], *, model_id: str) -> Model | None:
        if not model_id.startswith("oo:"):
            return None
        config = self.runtime._model_configurations[model_id[3:]]
        provider = await self.runtime._provider_for(config)
        return provider.native_model(config)

    def get_instructions(self) -> Any:
        async def instructions(ctx: RunContext[str]) -> str:
            return (await self.scope(ctx).prepare(ctx)).system
        return instructions

    def get_model(self) -> Any:
        async def model(ctx: RunContext[str]) -> str:
            return await self.scope(ctx).model(ctx)
        return model

    async def wrap_model_request(
        self, ctx: RunContext[str], *, request_context: ModelRequestContext,
        handler: WrapModelRequestHandler,
    ) -> ModelResponse:
        scope = self.scope(ctx)
        prepared = await scope.prepare(ctx)
        await scope.check_authority()
        model = request_context.model
        if scope.requests == 0:
            history = list(prepared.history)
            incoming = prepared.messages
        else:
            history = list(ctx.messages)
            # SDK tool returns already belong to this run. Only newly admitted
            # human input and host notices cross the product boundary again.
            incoming = [message for message in prepared.messages if message["role"] == "user"
                        and not any(block.get("type") == "tool_result" for block in message["content"])]
        history.extend(model_messages(incoming, model_name=model.model_name,
                                      provider_name=model.system, provider_url=model.base_url or "", previous=history))
        ctx.messages[:] = history
        request_context = replace(
            request_context, messages=history, model_settings=model_settings(prepared.config, scope.turn.effort),
        )
        from openctopus_server.chat.delegation import reserve_request
        await reserve_request(scope.root_workflow_id, scope.turn.session_id, uuid5(scope.turn.turn_id, "model-request"))
        await scope.runtime.limiter.configure(prepared.config.max_concurrent_requests)
        async with scope.runtime.limiter.slot():
            response = await handler(request_context)
        await scope.admission.aclose()
        from openctopus_server.chat.runner import _CompletedProviderTurn
        assistant = await scope.runtime._persist_assistant_message(
            scope.state, scope.turn, content=response_blocks(response), fingerprint=provider_fingerprint(prepared.config),
        )
        await save_context(scope.runtime.engine, scope.turn.session_id, [*ctx.messages, response], assistant.id, scope.turn.tool_profile)
        scope.requests += 1
        scope.completed = _CompletedProviderTurn(
            scope.turn, assistant, prepared.user_id, prepared.device_targets, prepared.mcp_snapshot,
            prepared.current_channel, prepared.current_chat_id, prepared.current_binding_generation,
        )
        return response


def build_agent(runtime: ChatRuntime, *, restricted: bool = False, worker: bool = False) -> Agent[str, str]:
    async def tools(ctx: RunContext[str]) -> FunctionToolset[str]:
        return await runtime._agent_runs[ctx.deps].tools(ctx)

    async def events(ctx: RunContext[str], stream: AsyncIterable[AgentStreamEvent]) -> None:
        await runtime._agent_runs[ctx.deps].events(ctx, stream)

    capabilities: list[AbstractCapability[str]] = [ProductLifecycle(runtime), ConfiguredCompaction(runtime)]
    if not restricted:
        capabilities.extend([
            DurableTools(ConversationSearch(source=ConversationHistory(), scope="conversation")),
            DurableTools(ToolOutputLimits(store=ToolOutputStore())),
        ])
        capabilities.append(WorkspaceSkills())
        capabilities.append(DurableTools(Memory(
            store=runtime.memory.store, namespace=lambda ctx: str(runtime._agent_runs[ctx.deps].user_id),
            injection_errors="raise",
        )))
    toolsets: list[Any] = [DynamicToolset(tools, id="oo-routed-tools")]
    if not restricted and not worker:
        from openctopus_server.chat.delegation import DurableDelegate, delegate_background
        capabilities.append(SubAgents(agents=[SubAgent(DurableDelegate(runtime.worker_agent),
                                  name="worker", description="Complete an independent task with its own context")],
                                  agent_folders=None, max_depth=2))
        toolsets.append(FunctionToolset([delegate_background]))
    capabilities.append(DBOSDurability(
            event_stream_handler=events, parallel_execution_mode="sequential",
    ))
    return Agent[str, str](
        "oo:configured", name="openoctopus_worker" if worker else "openoctopus_restricted" if restricted else "openoctopus", deps_type=str,
        capabilities=capabilities,
        toolsets=toolsets, retries=3,
    )
