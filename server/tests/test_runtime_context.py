from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from openctopus_server.chat.public_projection import public_content
from openctopus_server.chat.runtime_context import (
    RuntimeContext,
    build_runtime_block,
    parse_runtime_block,
    runtime_matches_session,
)
from openctopus_server.db.models import Session


def _session(*, channel: str = "web", chat_id: str | None = None) -> Session:
    session_id = uuid4()
    return Session(
        id=session_id,
        user_id=uuid4(),
        session_key=f"{channel}:{session_id}",
        channel=channel,
        chat_id=chat_id or str(session_id),
        title="New chat",
        created_at=datetime.now(UTC),
    )


def test_runtime_codec_round_trips_the_exact_server_grammar() -> None:
    session = _session()
    sender_id = UUID("42b31c27-7a70-42b0-83f8-af37e9bd64ce")
    timestamp = "2026-08-02T14:15:16.123456+00:00"

    block = build_runtime_block(timestamp=timestamp, session=session, user_id=sender_id)

    assert block == {
        "type": "text",
        "text": (
            "<runtime>\n"
            f"time: {timestamp}\n"
            "channel: web\n"
            f"chat_id: {session.chat_id}\n"
            f"sender: partner:{sender_id}\n"
            "trust: partner\n"
            "</runtime>"
        ),
    }
    assert parse_runtime_block(block) == RuntimeContext(
        time=timestamp,
        channel="web",
        chat_id=session.chat_id,
        sender_id=sender_id,
        trust="partner",
    )
    assert runtime_matches_session(block, session=session) is True


@pytest.mark.parametrize(
    "mutation",
    [
        lambda text: f"{text}\n",
        lambda text: text.replace("trust: partner", "trust: untrusted"),
        lambda text: text.replace("channel: web", "channel: Web"),
        lambda text: text.replace(
            "42b31c27-7a70-42b0-83f8-af37e9bd64ce",
            "------------------------------------",
        ),
        lambda text: text.replace("time:", "timestamp:"),
    ],
)
def test_runtime_parser_rejects_near_matches(mutation) -> None:
    session = _session()
    block = build_runtime_block(
        timestamp="2026-08-02T14:15:16+00:00",
        session=session,
        user_id=UUID("42b31c27-7a70-42b0-83f8-af37e9bd64ce"),
    )
    malformed = {"type": "text", "text": mutation(block["text"])}

    assert parse_runtime_block(malformed) is None


def test_public_projection_strips_only_an_exact_matching_first_runtime_block() -> None:
    session = _session()
    runtime = build_runtime_block(
        timestamp="2026-08-02T14:15:16+00:00",
        session=session,
        user_id=session.user_id,
    )
    user_text = {"type": "text", "text": "hello"}

    assert public_content([runtime, user_text], session=session, human=True) == [user_text]
    assert public_content([user_text, runtime], session=session, human=True) == [
        user_text,
        runtime,
    ]
    assert public_content([runtime, user_text], session=session, human=False) == [
        runtime,
        user_text,
    ]


def test_public_projection_preserves_runtime_block_for_another_session() -> None:
    source_session = _session(chat_id="source-chat")
    current_session = _session(chat_id="current-chat")
    runtime = build_runtime_block(
        timestamp="2026-08-02T14:15:16+00:00",
        session=source_session,
        user_id=source_session.user_id,
    )

    assert runtime_matches_session(runtime, session=current_session) is False
    assert (
        public_content(
            [runtime, {"type": "text", "text": "hello"}],
            session=current_session,
            human=True,
        )[0]
        == runtime
    )
