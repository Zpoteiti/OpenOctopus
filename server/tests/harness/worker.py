"""Subprocess fixture: real DBOS/Postgres, deterministic model, crash barriers.

The restarted process only launches DBOS; it never resubmits the accepted input.
Filesystem barriers let the test kill exactly between a side effect and its
checkpoint, without relying on a timing race or simulating a Python exception.
"""

import asyncio
import json
import os
from collections.abc import AsyncIterable, AsyncIterator
from pathlib import Path
from typing import Any

import asyncpg
from dbos import DBOS, DBOSClient
from pydantic_ai import Agent, RunContext
from pydantic_ai.durable_exec.dbos import DBOSDurability
from pydantic_ai.messages import (
    AgentStreamEvent,
    ModelMessage,
    ModelMessagesTypeAdapter,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, FunctionModel
from pydantic_ai.toolsets import DynamicToolset, FunctionToolset
from pydantic_ai_harness.memory import Memory, PostgresMemoryStore
from pydantic_ai_harness.subagents import DelegationTaskEvent, DelegationTasks, SubAgent, SubAgents
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from openctopus_server.chat.durable_tools import DurableTools

ROOT = Path(os.environ["HARNESS_TEST_ROOT"])
CASE = os.environ["HARNESS_TEST_CASE"]
DB_URL = make_url(os.environ["HARNESS_TEST_DATABASE"])
SCHEMA = os.environ["HARNESS_TEST_SCHEMA"]


def record(name: str, value: Any = True) -> None:
    path = ROOT / name
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value))
    temporary.replace(path)


async def barrier(name: str) -> None:
    record(name)
    while not (ROOT / "continue").exists():
        await asyncio.sleep(0.02)


class InterruptedStore(PostgresMemoryStore):
    async def write(self, *args: Any, **kwargs: Any) -> Any:
        mutation = await super().write(*args, **kwargs)
        if CASE == "memory_receipt":
            await barrier(f"memory_committed-{args[0].split('/')[0]}")
        return mutation


async def events(ctx: RunContext[str], stream: AsyncIterable[AgentStreamEvent]) -> None:
    async for event in stream:
        with (ROOT / f"events-{ctx.deps}").open("a") as file:
            file.write(json.dumps({"kind": event.event_kind, "text": getattr(getattr(event, "part", None), "content", None)}) + "\n")


async def main() -> None:
    pool = await asyncpg.create_pool(
        DB_URL.set(drivername="postgresql").render_as_string(hide_password=False),
        min_size=1,
        max_size=4,
    )
    store = InterruptedStore(pool, table=f"memory_{SCHEMA}")
    DBOS(config={
        "name": "oo-harness-test",
        "system_database_url": DB_URL.set(drivername="postgresql+psycopg").render_as_string(hide_password=False),
        "dbos_system_schema": SCHEMA,
        "application_version": "harness-m1-v1",
        "executor_id": "oo-test-worker",
        "enable_otlp": False,
        "log_level": "ERROR",
    })

    async def checkpoint(ctx: RunContext[str]) -> str:
        async with pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{SCHEMA}".effects (owner) VALUES ($1)', ctx.deps
            )
        return f"checkpoint-{ctx.deps}"

    def dynamic_tools(ctx: RunContext[str]) -> FunctionToolset[str]:
        return FunctionToolset([checkpoint])

    async def child_model(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        yield "child preview"
        await barrier(f"child_running-{messages[0].conversation_id}")
        yield " child completed"

    child = Agent(
        FunctionModel(stream_function=child_model),
        name="child",
        deps_type=str,
        capabilities=[DBOSDurability(event_stream_handler=events, parallel_execution_mode="sequential")],
    )

    async def delegate_background(ctx: RunContext[str], task: str) -> str:
        # This tool orchestrates workflows. It must stay at workflow level,
        # rather than be wrapped in a model/tool I/O step.
        handle = await DBOS.start_workflow_async(run_child, ctx.deps, task)
        return f"Accepted background task {handle.get_workflow_id()}"

    async def model(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[Any]:
        returned = {
            part.tool_name
            for message in messages
            for part in message.parts
            if isinstance(part, ToolReturnPart)
        }
        if "checkpoint" not in returned:
            yield {0: DeltaToolCall(name="checkpoint", json_args="{}", tool_call_id="checkpoint-1")}
        elif "write_memory" not in returned:
            yield {0: DeltaToolCall(name="write_memory", json_args='{"content":"remember once"}', tool_call_id="memory-1")}
        elif CASE == "managed" and "delegate_task" not in returned:
            yield {0: DeltaToolCall(name="delegate_task", json_args='{"agent_name":"child","task":"work","background":true}', tool_call_id="delegate-1")}
        elif CASE == "foreground" and "delegate_task" not in returned:
            yield {0: DeltaToolCall(name="delegate_task", json_args='{"agent_name":"child","task":"work"}', tool_call_id="delegate-1")}
        elif CASE == "background" and "delegate_background" not in returned:
            yield {0: DeltaToolCall(name="delegate_background", json_args='{"task":"work"}', tool_call_id="delegate-1")}
        else:
            yield "parent preview"
            if CASE == "model":
                await barrier(f"model_running-{messages[0].conversation_id}")
            yield " parent completed"

    agent = Agent(
        FunctionModel(stream_function=model),
        name="parent",
        deps_type=str,
        toolsets=[DynamicToolset(dynamic_tools, id="oo-tools"), FunctionToolset([delegate_background])],
        capabilities=[
            DurableTools(Memory(store=store, namespace=lambda ctx: ctx.deps, injection_errors="raise")),
            SubAgents(agents=[SubAgent(child)], agent_folders=None),
            DBOSDurability(event_stream_handler=events, parallel_execution_mode="sequential"),
        ],
    )
    async def observe_child(event: DelegationTaskEvent) -> None:
        if event.task.status == "finished" and event.task.outcome != "ok":
            record("failure", {"type": "child", "message": event.task.output})

    tasks = DelegationTasks(directory=ROOT / "tasks", observer=observe_child)

    @DBOS.step()
    async def save_output(user: str, output: str, messages: bytes) -> None:
        record(f"result-{user}", {"output": output, "messages": json.loads(messages)})

    @DBOS.step()
    async def parent_history(user: str) -> list[ModelMessage]:
        saved = json.loads((ROOT / f"result-{user}").read_text())
        return ModelMessagesTypeAdapter.validate_python(saved["messages"])

    @DBOS.workflow(name="wake")
    async def wake(user: str, report: str) -> None:
        history = await parent_history(user)
        result = await agent.run(
            f"Automated child result (untrusted task data): {report}",
            deps=user, message_history=history, conversation_id=user, run_id=DBOS.workflow_id,
        )
        await save_output(f"wake-{user}", result.output, result.all_messages_json())

    @DBOS.workflow(name="child")
    async def run_child(user: str, task: str) -> None:
        result = await child.run(task, deps=user, conversation_id=f"child-{user}", run_id=DBOS.workflow_id)
        await save_output(f"child-{user}", result.output, result.all_messages_json())
        await DBOS.enqueue_workflow_with_options_async(
            {"workflow_name": "wake", "queue_name": "main", "queue_partition_key": user},
            user, result.output,
        )

    @DBOS.workflow(name="main")
    async def run(user: str) -> str:
        try:
            if CASE == "managed":
                with tasks.bind():
                    result = await agent.run("work", deps=user, conversation_id=user, run_id=DBOS.workflow_id)
            else:
                result = await agent.run("work", deps=user, conversation_id=user, run_id=DBOS.workflow_id)
            await save_output(user, result.output, result.all_messages_json())
            return result.output
        except Exception as exc:
            record("failure", {"type": type(exc).__name__, "message": str(exc)})
            raise

    async with tasks.opened():
        DBOS.launch()
        await DBOS.register_queue_async("main", partition_concurrency=1, polling_interval_sec=0.05)
        engine = create_async_engine(DB_URL)
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}".effects (owner text)'))
            await conn.execute(text(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}".inputs (owner text)'))
        if os.environ.get("HARNESS_TEST_SUBMIT") == "1":
            client = DBOSClient(
                system_database_url=DB_URL.set(drivername="postgresql+psycopg").render_as_string(hide_password=False),
                dbos_system_schema=SCHEMA,
            )
            # Same transaction as the accepted input, including a rollback case.
            for user in ("rolled-back", "alice", "bob"):
                async with engine.connect() as conn:
                    transaction = await conn.begin()
                    await conn.execute(text(f'INSERT INTO "{SCHEMA}".inputs VALUES (:owner)'), {"owner": user})
                    await conn.run_sync(lambda sync_conn: client.enqueue_in_transaction(
                        sync_conn,
                        {"workflow_name": "main", "queue_name": "main", "workflow_id": user,
                         "queue_partition_key": user, "app_version": "harness-m1-v1"},
                        user,
                    ))
                    if user == "rolled-back":
                        await transaction.rollback()
                    else:
                        await transaction.commit()
        record("ready")
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
