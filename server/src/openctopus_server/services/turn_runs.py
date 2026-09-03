import uuid
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.chat.repair import repair_unpaired_tool_uses
from openctopus_server.db.advisory import lock_uuid_identity
from openctopus_server.db.models import Message, TurnRun

_ABANDONED_TEXT = (
    "[turn_abandoned] The previous request was closed because the "
    "OpenOctopus server restarted."
)


async def abandon_running_turns(
    engine: AsyncEngine,
    *,
    runner_instance_id: UUID,
) -> None:
    """Close promoted work from an older runner without replaying its Provider call."""
    async with AsyncSession(engine, expire_on_commit=False) as db:
        run_ids = tuple(
            (
                await db.scalars(
                    select(TurnRun.id).where(
                        TurnRun.status == "running",
                        TurnRun.runner_instance_id != runner_instance_id,
                    )
                )
            ).all()
        )
        await db.rollback()

    for run_id in run_ids:
        await _close_abandoned_turn(engine, run_id, runner_instance_id)


async def _close_abandoned_turn(
    engine: AsyncEngine,
    run_id: UUID,
    current_runner_instance_id: UUID,
) -> None:
    async with AsyncSession(engine, expire_on_commit=False) as db:
        run = await db.get(TurnRun, run_id)
        if (
            run is None
            or run.status != "running"
            or run.runner_instance_id == current_runner_instance_id
        ):
            await db.rollback()
            return
        input_ids = tuple(UUID(message_id) for message_id in run.input_message_ids)
        promoted_ids = set(
            (
                await db.scalars(
                    select(Message.id).where(
                        Message.session_id == run.session_id,
                        Message.id.in_(input_ids),
                    )
                )
            ).all()
        )
        session_id = run.session_id
        await db.rollback()

    if promoted_ids:
        if len(promoted_ids) != len(input_ids):
            raise RuntimeError("Abandoned turn input promotion is incomplete")
        async with AsyncSession(engine, expire_on_commit=False) as db:
            await repair_unpaired_tool_uses(db, session_id=session_id)

    async with AsyncSession(engine, expire_on_commit=False) as db:
        try:
            await lock_uuid_identity(db, session_id)
            run = (
                await db.execute(
                    select(TurnRun).where(TurnRun.id == run_id).with_for_update()
                )
            ).scalar_one_or_none()
            if (
                run is None
                or run.status != "running"
                or run.runner_instance_id == current_runner_instance_id
            ):
                await db.commit()
                return

            if promoted_ids:
                rows = list(
                    (
                        await db.scalars(
                            select(Message)
                            .where(Message.session_id == session_id)
                            .order_by(Message.created_at, Message.id)
                            .with_for_update()
                        )
                    ).all()
                )
                input_positions = [
                    index for index, row in enumerate(rows) if row.id in promoted_ids
                ]
                if len(input_positions) != len(promoted_ids):
                    raise RuntimeError("Abandoned turn input disappeared during closure")
                turn_rows = rows[max(input_positions) + 1 :]
                if not _has_terminal_assistant(turn_rows):
                    last_created_at = rows[-1].created_at
                    db.add(
                        Message(
                            id=uuid.uuid4(),
                            session_id=session_id,
                            message_kind="synthetic_assistant_error",
                            content=[{"type": "text", "text": _ABANDONED_TEXT}],
                            attachment_refs=[],
                            delivery_refs=[],
                            is_compacted=False,
                            created_at=max(
                                datetime.now(UTC),
                                last_created_at + timedelta(microseconds=1),
                            ),
                        )
                    )

            run.status = "abandoned"
            run.finished_at = datetime.now(UTC)
            await db.commit()
        except Exception:
            await db.rollback()
            raise


def _has_terminal_assistant(rows: list[Message]) -> bool:
    for row in rows:
        if row.message_kind == "synthetic_assistant_error":
            return True
        if row.message_kind != "assistant":
            continue
        if not any(block.get("type") == "tool_use" for block in row.content):
            return True
    return False
