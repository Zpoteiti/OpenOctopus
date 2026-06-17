from datetime import UTC, datetime
from uuid import uuid4

from openctopus_server.dto.error import ErrorResponse
from openctopus_server.dto.message import MessageResponse, PostMessageRequest
from openctopus_server.dto.session import SessionResponse


def test_post_message_request():
    req = PostMessageRequest(content="hello")
    assert req.content == "hello"


def test_message_response():
    msg = MessageResponse(
        id=uuid4(),
        role="user",
        message_kind="human",
        content=[{"type": "text", "text": "hi"}],
        created_at=datetime.now(UTC),
    )
    assert msg.role == "user"


def test_session_response():
    sess = SessionResponse(
        id=uuid4(),
        session_key="key",
        channel="web",
        chat_id="chat",
        title="title",
        unread=False,
        created_at=datetime.now(UTC),
    )
    assert sess.channel == "web"


def test_error_response():
    err = ErrorResponse(code="workspace_not_found", message="not found", detail={"path": "/x"})
    assert err.detail == {"path": "/x"}
