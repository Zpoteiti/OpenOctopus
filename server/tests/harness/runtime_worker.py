"""Exercise the production ChatRuntime and ingress transaction across SIGKILL."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from pydantic_ai.messages import ModelRequest, ToolReturnPart
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from openctopus_server.chat import durable
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.chat.scope import active_run
from openctopus_server.db.models import SystemConfig, User
from openctopus_server.services.messages import accept_message
from openctopus_server.tools.base import Tool, ToolResult, ToolRoutingMode
from openctopus_server.tools.registry import ToolRegistry

ROOT = Path(os.environ["HARNESS_TEST_ROOT"])
CASE = os.environ.get("HARNESS_TEST_CASE", "model")


async def barrier(name):
    (ROOT / name).touch()
    while not (ROOT / "continue").exists():
        await asyncio.sleep(0.02)


class Counter(Tool):
    routing_mode = ToolRoutingMode.PURE_SERVER
    def __init__(self, engine):
        self.engine = engine
    def name(self):
        return "counter"
    def schema(self):
        return {"name": "counter", "input_schema": {"type": "object", "properties": {}}}
    async def execute(self, args, ctx):
        async with self.engine.begin() as conn:
            await conn.execute(text("INSERT INTO recovery_effects (owner) VALUES (:owner)"), {"owner": str(ctx.user_id)})
        if CASE in {"tool_receipt", "cron"}:
            if CASE == "cron":
                (ROOT / "accepted").write_text(json.dumps({"session": str(ctx.session_id), "turn": str(ctx.turn_id)}))
            await barrier("effect_committed")
        return ToolResult("counter committed")


class Provider:
    def native_model(self, config):
        async def stream(messages, info):
            results = [part for message in messages if isinstance(message, ModelRequest)
                       for part in message.parts if isinstance(part, ToolReturnPart)]
            if CASE.startswith("child_"):
                if active_run.get().worker:
                    if not results:
                        yield {0: DeltaToolCall(name="counter", json_args="{}", tool_call_id="child-counter")}
                    else:
                        yield "child preview"
                        await barrier("child_running")
                        yield " child completed"
                elif not results:
                    name = "delegate_task" if CASE == "child_foreground" else "delegate_background"
                    args = {"task": "independent child task"}
                    if name == "delegate_task":
                        args["agent_name"] = "worker"
                    yield {0: DeltaToolCall(name=name, json_args=json.dumps(args), tool_call_id="delegate-1")}
                else:
                    yield "completed after delegate"
            elif not results:
                yield {0: DeltaToolCall(name="counter", json_args="{}", tool_call_id="counter-1")}
            else:
                if CASE == "skills" and len(results) == 1:
                    yield {0: DeltaToolCall(name="load_capability", json_args='{"id":"builtin-create-skill"}', tool_call_id="load-skill")}
                    return
                if CASE == "skills":
                    assert any(part.tool_name == "load_capability" and part.outcome == "success" for part in results)
                    await barrier("skills_running")
                if CASE in {"model", "graceful"}:
                    yield "preview"
                    await barrier("model_running")
                if CASE == "handoff":
                    (ROOT / "final_model_ready").touch()
                    while not (ROOT / "queued").exists():
                        await asyncio.sleep(0.01)
                yield "completed"
        return FunctionModel(stream_function=stream, model_name=config.model)
    async def close(self):
        pass


async def main():
    engine = create_async_engine(os.environ["HARNESS_TEST_DATABASE"])
    runtime = ChatRuntime(engine, provider_factory=lambda config: Provider(), tool_registry=ToolRegistry([Counter(engine)]))
    if CASE == "handoff":
        original_reserve = durable.reserve_pending_turn
        attempts = 0
        async def reserve(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1 and not (ROOT / "continue").exists():
                raise RuntimeError("temporary database failure")
            reserved = await original_reserve(*args, **kwargs)
            if reserved is not None:
                await barrier("handoff_committed")
            return reserved
        durable.reserve_pending_turn = reserve
    await runtime.durable.start()
    if os.environ.get("HARNESS_TEST_SUBMIT") == "1":
        async with engine.begin() as conn:
            await conn.execute(text("CREATE TABLE recovery_effects (owner text)"))
        async with AsyncSession(engine, expire_on_commit=False) as db:
            user = User(email="restart@test.com", name="Restart", password_hash="test")
            db.add(user)
            for key, value in {"llm_endpoint": "http://offline.test", "llm_model": "fixture", "llm_api_key": "fixture-secret"}.items():
                db.add(SystemConfig(key=key, value=value))
            await db.commit()
            if CASE == "cron":
                from openctopus_server.dto.cron import CronCreateRequest
                from openctopus_server.services.cron import create_owned
                await create_owned(db, user_id=user.id, request=CronCreateRequest(
                    name="durable timer", message="work", at=(datetime.now(UTC) + timedelta(seconds=3)).isoformat()))
            else:
                accepted = await accept_message(db, user=user, session_id=uuid4(), content=[{"type": "text", "text": "work"}], effort=None, runner_instance_id=runtime.runner_instance_id)
                (ROOT / "accepted").write_text(json.dumps({"session": str(accepted.session_id), "turn": str(accepted.turn.turn_id)}))
                if CASE == "handoff":
                    while not (ROOT / "final_model_ready").exists():
                        await asyncio.sleep(0.01)
                    await accept_message(db, user=user, session_id=accepted.session_id,
                                         content=[{"type": "text", "text": "queued input"}], effort=None,
                                         runner_instance_id=runtime.runner_instance_id)
                    (ROOT / "queued").touch()
        # Transactional acceptance alone must start execution: no activate(),
        # browser, manual resume, or another message is involved.
    (ROOT / "ready").touch()
    while True:
        if (ROOT / "shutdown_live").exists():
            await runtime.close()
            await engine.dispose()
            (ROOT / "closed").touch()
            return
        if (ROOT / "stop_live").exists():
            from openctopus_server.services.messages import request_cancel
            accepted = json.loads((ROOT / "accepted").read_text())
            async with AsyncSession(engine) as db:
                owner = await db.scalar(text("SELECT user_id FROM sessions WHERE id=:id"), {"id": accepted["session"]})
                await request_cancel(db, user_id=owner, session_id=UUID(accepted["session"]))
            await runtime.durable.apply_cancellations()
            (ROOT / "cancelled_live").touch()
            await asyncio.Event().wait()
        await asyncio.sleep(0.02)


asyncio.run(main())
