"""Bridge committed stop/deletion intent to DBOS's durable cancellation."""

from uuid import UUID, uuid5

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.types import TurnStart
from openctopus_server.db.models import (
    AgentTask,
    Message,
    ToolOperation,
    TurnRun,
    WorkflowCancellation,
)


async def record_cancellation(db: AsyncSession, session_id: UUID, workflow_id: str) -> None:
    await db.execute(insert(WorkflowCancellation).values(
        workflow_id=workflow_id, session_id=session_id,
    ).on_conflict_do_nothing())


async def record_session_cancellation(db: AsyncSession, session_id: UUID) -> None:
    # Include completed parents: their background children can still be running.
    for workflow_id in (await db.scalars(select(TurnRun.workflow_id).where(
        TurnRun.session_id == session_id, TurnRun.workflow_id.is_not(None),
    ).distinct())).all():
        assert workflow_id is not None
        await record_cancellation(db, session_id, workflow_id)
    for task in (await db.scalars(select(AgentTask).where(AgentTask.parent_session_id == session_id))).all():
        await record_cancellation(db, session_id, task.root_workflow_id)
        await record_cancellation(db, task.session_id, task.workflow_id)



async def reconcile_cancellation(db: AsyncSession, session_id: UUID, workflow_id: str) -> tuple[TurnStart, list[Message], Message] | None:
    from openctopus_server.services.messages import cancel_tool_batch

    run = await db.scalar(select(TurnRun).where(TurnRun.session_id == session_id, TurnRun.status == "running", TurnRun.workflow_id == workflow_id))
    if run is None:
        return None
    assistant = await db.get(Message, uuid5(run.id, "assistant"))
    unknown: list[str] = []
    cancelled: list[str] = []
    for part in assistant.content if assistant else []:
        if part.get("type") != "tool_use":
            continue
        operation = uuid5(run.id, f"tool:{part['id']}")
        if await db.get(Message, operation) is not None:
            continue
        has_intent = await db.get(ToolOperation, operation) is not None
        # Native mutable tools have SDK receipts rather than OO dispatch rows.
        # Their outcome cannot be inferred from a missing product event.
        native_mutation = part.get("name") in {"write_memory", "delete_memory", "delegate_task", "delegate_background"}
        target = unknown if has_intent or native_mutation else cancelled
        target.append(str(part["id"]))
    turn = TurnStart(session_id, run.id, tuple(UUID(item) for item in run.input_message_ids), None,
                     "message_only" if run.tool_profile == "message_only" else "owner_full")
    rows, marker = await cancel_tool_batch(db, turn=turn, outcome_unknown_tool_ids=unknown, cancelled_tool_ids=cancelled)
    return turn, rows, marker
