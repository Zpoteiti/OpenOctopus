import json
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import ToolContext, ToolRoutingMode
from openctopus_server.tools.cron import CronTool


async def _owner(pg_engine) -> User:
    user = User(
        id=uuid.uuid4(),
        email=f"{uuid.uuid4()}@example.com",
        password_hash="hash",
        name="Owner",
        timezone="Asia/Shanghai",
        is_admin=False,
        created_at=datetime.now(UTC),
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(user)
        await db.commit()
    return user


async def test_cron_tool_add_list_and_remove(pg_engine) -> None:
    owner = await _owner(pg_engine)
    wakes = 0

    def wake() -> None:
        nonlocal wakes
        wakes += 1

    tool = CronTool(pg_engine, wake=wake)
    context = ToolContext(user_id=owner.id, session_id=uuid.uuid4())

    added = await tool.execute(
        {
            "action": "add",
            "name": "weekday report",
            "message": "Prepare the report.",
            "cron_expr": "0 9 * * MON-FRI",
        },
        context,
    )
    assert not added.is_error
    add_payload = json.loads(str(added.content))
    job_id = add_payload["job"]["id"]
    assert add_payload["job"]["schedule"] == {
        "type": "cron",
        "cron_expr": "0 9 * * MON-FRI",
        "tz": "Asia/Shanghai",
    }

    listed = await tool.execute({"action": "list", "offset": 0}, context)
    assert not listed.is_error
    list_payload = json.loads(str(listed.content))
    assert list_payload["items"][0]["id"] == job_id
    assert "message" not in list_payload["items"][0]
    assert list_payload["next_offset"] is None

    removed = await tool.execute(
        {"action": "remove", "job_id": job_id},
        ToolContext(user_id=owner.id, session_id=uuid.UUID(job_id)),
    )
    assert not removed.is_error
    assert removed.content == "Future triggers stopped; existing history retained."
    assert wakes == 2


async def test_cron_tool_list_is_fixed_twenty_item_page(pg_engine) -> None:
    owner = await _owner(pg_engine)
    tool = CronTool(pg_engine)
    context = ToolContext(user_id=owner.id, session_id=uuid.uuid4())
    for index in range(21):
        result = await tool.execute(
            {
                "action": "add",
                "name": f"job-{index:02d}",
                "message": "x",
                "every_seconds": 60,
            },
            context,
        )
        assert not result.is_error

    first = json.loads(str((await tool.execute({"action": "list"}, context)).content))
    second = json.loads(
        str(
            (
                await tool.execute(
                    {"action": "list", "offset": first["next_offset"]}, context
                )
            ).content
        )
    )

    assert len(first["items"]) == 20
    assert first["next_offset"] == 20
    assert len(second["items"]) == 1
    assert second["next_offset"] is None


async def test_cron_tool_schema_and_error_contract(pg_engine, monkeypatch) -> None:
    owner = await _owner(pg_engine)
    tool = CronTool(pg_engine)
    context = ToolContext(user_id=owner.id, session_id=uuid.uuid4())

    assert tool.name() == "cron"
    assert tool.routing_mode is ToolRoutingMode.PURE_SERVER
    assert tool.max_output_chars() == 16_000
    assert tool.schema()["input_schema"]["properties"]["action"]["enum"] == [
        "add",
        "list",
        "remove",
    ]

    missing = await tool.execute({"action": "add", "every_seconds": 60}, context)
    assert missing.is_error
    assert missing.code is ErrorCode.TOOL_MISSING_REQUIRED_FIELD

    invalid = await tool.execute(
        {"action": "add", "message": "x", "every_seconds": 1}, context
    )
    assert invalid.is_error
    assert invalid.code is ErrorCode.TOOL_INVALID_SCHEDULE

    absent = await tool.execute(
        {"action": "remove", "job_id": str(uuid.uuid4())}, context
    )
    assert absent.is_error
    assert absent.code is ErrorCode.TOOL_CRON_JOB_NOT_FOUND

    monkeypatch.setattr(
        "openctopus_server.tools.cron.cron_service.list_owned",
        AsyncMock(side_effect=RuntimeError("secret dsn")),
    )
    failed = await tool.execute({"action": "list"}, context)
    assert failed.is_error
    assert failed.code is ErrorCode.TOOL_DB_ERROR
    assert "secret dsn" not in str(failed.content)
