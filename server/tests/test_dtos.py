from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from openctopus_server.dto.error import ErrorResponse
from openctopus_server.dto.message import MessageResponse, PostMessageRequest
from openctopus_server.dto.session import SessionResponse


def test_post_message_request():
    req = PostMessageRequest(
        content=[{"type": "text", "text": "hello"}],
        attachments=[],
    )
    assert req.content[0].text == "hello"


def test_post_message_request_accepts_live_device_attachment_refs() -> None:
    device_id = uuid4()

    req = PostMessageRequest(
        content=[],
        attachments=[
            {
                "openoctopus_device": "laptop-cn",
                "device_id": device_id,
                "path": "documents/report.pdf",
            }
        ],
    )

    assert req.attachments[0].model_dump(mode="json", exclude_none=True) == {
        "openoctopus_device": "laptop-cn",
        "device_id": str(device_id),
        "path": "documents/report.pdf",
    }


def test_server_attachment_serialization_omits_client_identity() -> None:
    req = PostMessageRequest(
        content=[],
        attachments=[{"openoctopus_device": "server", "path": "report.pdf"}],
    )

    assert req.model_dump(mode="json")["attachments"] == [
        {"openoctopus_device": "server", "path": "report.pdf"}
    ]


@pytest.mark.parametrize(
    "attachment",
    [
        {"openoctopus_device": "laptop-cn", "path": "report.pdf"},
        {
            "openoctopus_device": "server",
            "device_id": "8fe3f335-43d3-48e6-81a9-3d72c87c2199",
            "path": "report.pdf",
        },
    ],
)
def test_post_message_request_requires_device_id_only_for_client_refs(
    attachment: dict[str, str],
) -> None:
    with pytest.raises(ValidationError):
        PostMessageRequest(content=[], attachments=[attachment])


def test_message_response():
    msg = MessageResponse(
        id=uuid4(),
        session_id=uuid4(),
        role="user",
        message_kind="human",
        content=[{"type": "text", "text": "hi"}],
        attachment_refs=[],
        delivery_refs=[],
        is_compacted=False,
        created_at=datetime.now(UTC),
    )
    assert msg.role == "user"


def test_session_response():
    sess = SessionResponse(
        id=uuid4(),
        user_id=uuid4(),
        session_key="key",
        channel="web",
        chat_id="chat",
        title="title",
        last_inbound_at=None,
        unread=False,
        cancel_requested=False,
        created_at=datetime.now(UTC),
    )
    assert sess.channel == "web"


def test_error_response():
    err = ErrorResponse(code="workspace_not_found", message="not found")
    assert err.model_dump() == {
        "code": "workspace_not_found",
        "message": "not found",
    }
