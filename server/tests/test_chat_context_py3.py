from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.context import build_provider_context, project_message_rows
from openctopus_server.chat.public_projection import message_response, provider_role
from openctopus_server.chat.repair import repair_unpaired_tool_uses
from openctopus_server.db.models import Message, Session, User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.config import ProviderConfig


def _message(
    session_id: UUID,
    *,
    kind: str,
    content: list[dict[str, object]],
    created_at: datetime,
    compacted: bool = False,
    fingerprint: str | None = "current-fingerprint",
) -> Message:
    authority = (
        {
            "sender_id": str(session_id),
            "sender_classification": "owner",
            "ingress_tool_profile": "owner_full",
        }
        if kind == "human"
        else {}
    )
    return Message(
        id=uuid4(),
        session_id=session_id,
        message_kind=kind,
        content=content,
        delivery_refs=[],
        llm_fingerprint=fingerprint,
        is_compacted=compacted,
        created_at=created_at,
        **authority,
    )


async def _seed_session(db: AsyncSession) -> tuple[User, Session]:
    user = User(
        id=uuid4(),
        email=f"{uuid4()}@test.com",
        password_hash="hash",
        name="User",
        is_admin=False,
    )
    session_id = uuid4()
    session = Session(
        id=session_id,
        user_id=user.id,
        session_key=f"web:{session_id}",
        channel="web",
        chat_id=str(session_id),
        title="New chat",
    )
    db.add(user)
    await db.flush()
    db.add(session)
    await db.flush()
    return user, session


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("human", "user"),
        ("tool_result", "user"),
        ("synthetic_tool_result", "user"),
        ("assistant", "assistant"),
        ("synthetic_assistant_error", "assistant"),
        ("compaction_summary", "assistant"),
    ],
)
def test_provider_role_is_derived_from_message_kind(kind: str, expected: str) -> None:
    assert provider_role(kind) == expected


def test_provider_role_rejects_unknown_message_kind() -> None:
    with pytest.raises(ValueError, match="Unsupported message kind"):
        provider_role("system")


def test_message_dto_derives_role_and_exposes_compacted_audit_state() -> None:
    session_id = uuid4()
    session = Session(
        id=session_id,
        user_id=uuid4(),
        session_key=f"web:{session_id}",
        channel="web",
        chat_id=str(session_id),
        title="New chat",
    )
    row = _message(
        session_id,
        kind="compaction_summary",
        content=[{"type": "text", "text": "older history"}],
        created_at=datetime.now(UTC),
        compacted=True,
    )

    dto = message_response(row, session=session)

    assert dto.role == "assistant"
    assert dto.message_kind == "compaction_summary"
    assert dto.is_compacted is True
    assert dto.model_dump(mode="json")["is_compacted"] is True


async def test_provider_context_filters_compacted_rows(pg_engine) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        _, session = await _seed_session(db)
        db.add_all(
            [
                _message(
                    session.id,
                    kind="human",
                    content=[{"type": "text", "text": "compacted-old"}],
                    created_at=now,
                    compacted=True,
                    fingerprint=None,
                ),
                _message(
                    session.id,
                    kind="human",
                    content=[{"type": "text", "text": "active-new"}],
                    created_at=now + timedelta(microseconds=1),
                    fingerprint=None,
                ),
            ]
        )
        await db.commit()

        _, messages = await build_provider_context(
            db,
            session_id=session.id,
            config=ProviderConfig(
                endpoint="http://fake.test",
                api_key="key",
                model="model",
                max_output_tokens=1024,
                max_concurrent_requests=0,
                max_context_tokens=None,
            ),
        )

    assert messages == [{"role": "user", "content": [{"type": "text", "text": "active-new"}]}]


def test_adjacent_tool_results_collapse_and_strip_internal_codes() -> None:
    session_id = uuid4()
    now = datetime.now(UTC)
    rows = [
        _message(
            session_id,
            kind="assistant",
            content=[
                {"type": "text", "text": "checking"},
                {"type": "tool_use", "id": "tool-1", "name": "web_fetch", "input": {}},
                {"type": "tool_use", "id": "tool-2", "name": "web_fetch", "input": {}},
            ],
            created_at=now,
        ),
        _message(
            session_id,
            kind="tool_result",
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "content": [{"type": "text", "text": "first"}],
                    "is_error": False,
                    "code": "internal-success-code",
                }
            ],
            created_at=now + timedelta(microseconds=1),
            fingerprint=None,
        ),
        _message(
            session_id,
            kind="synthetic_tool_result",
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": "tool-2",
                    "content": [{"type": "text", "text": "not run"}],
                    "is_error": True,
                    "code": "server_restart",
                }
            ],
            created_at=now + timedelta(microseconds=2),
            fingerprint=None,
        ),
    ]

    projected = project_message_rows(rows, current_fingerprint="current-fingerprint")

    assert [message["role"] for message in projected] == ["assistant", "user"]
    assert [block["tool_use_id"] for block in projected[1]["content"]] == [
        "tool-1",
        "tool-2",
    ]
    assert all("code" not in block for block in projected[1]["content"])
    assert rows[1].content[0]["code"] == "internal-success-code"
    assert rows[2].content[0]["code"] == "server_restart"


def test_tool_result_interleaving_is_rejected_instead_of_reordered() -> None:
    session_id = uuid4()
    now = datetime.now(UTC)
    rows = [
        _message(
            session_id,
            kind="assistant",
            content=[{"type": "tool_use", "id": "tool-1", "name": "web_fetch", "input": {}}],
            created_at=now,
        ),
        _message(
            session_id,
            kind="human",
            content=[{"type": "text", "text": "interrupting message"}],
            created_at=now + timedelta(microseconds=1),
            fingerprint=None,
        ),
        _message(
            session_id,
            kind="tool_result",
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "content": [{"type": "text", "text": "late"}],
                    "is_error": False,
                }
            ],
            created_at=now + timedelta(microseconds=2),
            fingerprint=None,
        ),
    ]

    with pytest.raises(ChatError) as raised:
        project_message_rows(rows, current_fingerprint="current-fingerprint")

    assert raised.value.code == ErrorCode.PROVIDER_PROTOCOL_ERROR
    assert "message boundary splits" in raised.value.message


def test_empty_boundary_cannot_split_an_unresolved_tool_batch() -> None:
    session_id = uuid4()
    now = datetime.now(UTC)
    rows = [
        _message(
            session_id,
            kind="assistant",
            content=[{"type": "tool_use", "id": "tool-1", "name": "web_fetch", "input": {}}],
            created_at=now,
        ),
        _message(
            session_id,
            kind="human",
            content=[],
            created_at=now + timedelta(microseconds=1),
            fingerprint=None,
        ),
        _message(
            session_id,
            kind="tool_result",
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "content": [{"type": "text", "text": "late"}],
                    "is_error": False,
                }
            ],
            created_at=now + timedelta(microseconds=2),
            fingerprint=None,
        ),
    ]

    with pytest.raises(ChatError) as raised:
        project_message_rows(rows, current_fingerprint="current-fingerprint")

    assert raised.value.code == ErrorCode.PROVIDER_PROTOCOL_ERROR
    assert "message boundary splits" in raised.value.message


def test_empty_tool_result_row_is_rejected_explicitly() -> None:
    session_id = uuid4()
    now = datetime.now(UTC)
    rows = [
        _message(
            session_id,
            kind="assistant",
            content=[{"type": "tool_use", "id": "tool-1", "name": "web_fetch", "input": {}}],
            created_at=now,
        ),
        _message(
            session_id,
            kind="tool_result",
            content=[],
            created_at=now + timedelta(microseconds=1),
            fingerprint=None,
        ),
    ]

    with pytest.raises(ChatError) as raised:
        project_message_rows(rows, current_fingerprint="current-fingerprint")

    assert raised.value.code == ErrorCode.PROVIDER_PROTOCOL_ERROR
    assert "tool-result row is empty" in raised.value.message


async def test_restart_repair_preserves_real_results_and_inserts_only_missing_ones(
    pg_engine,
) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        _, session = await _seed_session(db)
        assistant = _message(
            session.id,
            kind="assistant",
            content=[
                {"type": "tool_use", "id": "tool-1", "name": "web_fetch", "input": {}},
                {"type": "tool_use", "id": "tool-2", "name": "web_fetch", "input": {}},
            ],
            created_at=now,
        )
        real_result = _message(
            session.id,
            kind="tool_result",
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "content": [{"type": "text", "text": "real result"}],
                    "is_error": False,
                }
            ],
            created_at=now + timedelta(microseconds=1),
            fingerprint=None,
        )
        db.add_all([assistant, real_result])
        await db.commit()
        real_result_id = real_result.id
        real_result_content = real_result.content

        repaired = await repair_unpaired_tool_uses(db, session_id=session.id)
        repaired_again = await repair_unpaired_tool_uses(db, session_id=session.id)
        rows = list(
            (
                await db.execute(
                    select(Message)
                    .where(Message.session_id == session.id)
                    .order_by(Message.created_at, Message.id)
                )
            )
            .scalars()
            .all()
        )

    assert len(repaired) == 1
    assert repaired_again == []
    assert [row.message_kind for row in rows] == [
        "assistant",
        "tool_result",
        "synthetic_tool_result",
    ]
    assert rows[1].id == real_result_id
    assert rows[1].content == real_result_content
    assert repaired[0].content == [
        {
            "type": "tool_result",
            "tool_use_id": "tool-2",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "[server restart: tool execution outcome is unknown because the "
                        "OpenOctopus server restarted before recording its result]"
                    ),
                }
            ],
            "is_error": True,
            "code": "tool_execution_outcome_unknown",
        }
    ]

    projected = project_message_rows(rows, current_fingerprint="current-fingerprint")
    assert [block["tool_use_id"] for block in projected[1]["content"]] == [
        "tool-1",
        "tool-2",
    ]
    assert all("code" not in block for block in projected[1]["content"])


async def test_restart_repair_timestamps_follow_the_active_tail(pg_engine) -> None:
    future_tail = datetime.now(UTC) + timedelta(days=1)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        _, session = await _seed_session(db)
        db.add(
            _message(
                session.id,
                kind="assistant",
                content=[
                    {"type": "tool_use", "id": "tool-1", "name": "web_fetch", "input": {}},
                    {"type": "tool_use", "id": "tool-2", "name": "web_fetch", "input": {}},
                ],
                created_at=future_tail,
            )
        )
        await db.commit()

        repaired = await repair_unpaired_tool_uses(db, session_id=session.id)

    assert [row.created_at for row in repaired] == [
        future_tail + timedelta(microseconds=1),
        future_tail + timedelta(microseconds=2),
    ]


async def test_restart_repair_rejects_an_empty_tool_result_row(pg_engine) -> None:
    now = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        _, session = await _seed_session(db)
        session_id = session.id
        db.add_all(
            [
                _message(
                    session.id,
                    kind="assistant",
                    content=[
                        {
                            "type": "tool_use",
                            "id": "tool-1",
                            "name": "web_fetch",
                            "input": {},
                        }
                    ],
                    created_at=now,
                ),
                _message(
                    session.id,
                    kind="tool_result",
                    content=[],
                    created_at=now + timedelta(microseconds=1),
                    fingerprint=None,
                ),
            ]
        )
        await db.commit()

        with pytest.raises(ChatError) as raised:
            await repair_unpaired_tool_uses(db, session_id=session_id)

        rows = list(
            (
                await db.execute(
                    select(Message)
                    .where(Message.session_id == session_id)
                    .order_by(Message.created_at, Message.id)
                )
            )
            .scalars()
            .all()
        )

    assert raised.value.code == ErrorCode.PROVIDER_PROTOCOL_ERROR
    assert [row.message_kind for row in rows] == ["assistant", "tool_result"]
