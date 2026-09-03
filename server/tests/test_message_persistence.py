from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.types import TurnStart
from openctopus_server.db.models import Message, Session, TurnRun, User
from openctopus_server.services.messages import persist_tool_result


async def _running_message_turn(pg_engine) -> tuple[TurnStart, Message]:
    user_id = uuid4()
    session_id = uuid4()
    turn_id = uuid4()
    assistant = Message(
        id=uuid4(),
        session_id=session_id,
        message_kind="assistant",
        content=[
            {
                "type": "tool_use",
                "id": "toolu_message",
                "name": "message",
                "input": {"content": "Report", "media": ["report.pdf"]},
            }
        ],
        delivery_refs=[],
        is_compacted=False,
        created_at=datetime.now(UTC),
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(
            User(
                id=user_id,
                email=f"{user_id}@test.com",
                password_hash="hash",
                name="User",
            )
        )
        await db.flush()
        db.add(
            Session(
                id=session_id,
                user_id=user_id,
                session_key=f"web:{session_id}",
                channel="web",
                chat_id=str(session_id),
                title="Message delivery",
            )
        )
        await db.flush()
        db.add_all(
            [
                TurnRun(
                    id=turn_id,
                    session_id=session_id,
                    runner_instance_id=uuid4(),
                    status="running",
                    tool_profile="owner_full",
                    started_at=datetime.now(UTC),
                ),
                assistant,
            ]
        )
        await db.commit()
    return TurnStart(session_id=session_id, turn_id=turn_id, message_ids=(), effort=None), assistant


def _delivery_ref() -> dict[str, object]:
    return {
        "tool_use_id": "toolu_message",
        "type": "workspace_file",
        "openoctopus_device": "server",
        "path": "report.pdf",
        "workspace_id": str(uuid4()),
        "workspace_relative_path": "report.pdf",
        "filename": "report.pdf",
        "mime": "application/pdf",
        "size": 123,
        "online_only": False,
    }


async def test_message_refs_and_tool_result_commit_together(pg_engine) -> None:
    turn, assistant = await _running_message_turn(pg_engine)
    block = {
        "type": "tool_result",
        "tool_use_id": "toolu_message",
        "content": [{"type": "text", "text": "Delivered message with 1 attachment."}],
        "is_error": False,
    }
    delivery_ref = _delivery_ref()

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        updated_assistant, result = await persist_tool_result(
            db,
            turn=turn,
            block=block,
            assistant_message_id=assistant.id,
            delivery_refs=[delivery_ref],
        )

    assert updated_assistant is not None
    assert updated_assistant.id == assistant.id
    assert updated_assistant.delivery_refs == [delivery_ref]
    assert result.content == [block]

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        rows = list(
            (
                await db.execute(
                    select(Message)
                    .where(Message.session_id == turn.session_id)
                    .order_by(Message.created_at, Message.id)
                )
            )
            .scalars()
            .all()
        )
    assert [row.message_kind for row in rows] == ["assistant", "tool_result"]
    assert rows[0].delivery_refs == [delivery_ref]


async def test_missing_assistant_rolls_back_message_tool_result(pg_engine) -> None:
    turn, _ = await _running_message_turn(pg_engine)
    block = {
        "type": "tool_result",
        "tool_use_id": "toolu_message",
        "content": [{"type": "text", "text": "Delivered."}],
        "is_error": False,
    }

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        with pytest.raises(RuntimeError, match="assistant message"):
            await persist_tool_result(
                db,
                turn=turn,
                block=block,
                assistant_message_id=uuid4(),
                delivery_refs=[_delivery_ref()],
            )

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        rows = list(
            (await db.execute(select(Message).where(Message.session_id == turn.session_id)))
            .scalars()
            .all()
        )
    assert [row.message_kind for row in rows] == ["assistant"]


async def test_content_only_message_republishes_existing_assistant(pg_engine) -> None:
    turn, assistant = await _running_message_turn(pg_engine)
    block = {
        "type": "tool_result",
        "tool_use_id": "toolu_message",
        "content": [{"type": "text", "text": "Delivered message."}],
        "is_error": False,
    }

    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        updated_assistant, _ = await persist_tool_result(
            db,
            turn=turn,
            block=block,
            assistant_message_id=assistant.id,
            delivery_refs=[],
        )

    assert updated_assistant is not None
    assert updated_assistant.id == assistant.id
    assert updated_assistant.delivery_refs == []
