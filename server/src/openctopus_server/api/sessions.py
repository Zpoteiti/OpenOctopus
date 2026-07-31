from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.message import MessagesResponse, PostMessageRequest
from openctopus_server.services import messages

router = APIRouter(
    prefix="/api/sessions/{session_id}/messages",
    tags=["Sessions"],
)


def get_chat_runtime(request: Request) -> ChatRuntime:
    runtime = getattr(request.app.state, "chat_runtime", None)
    if runtime is None:
        runtime = ChatRuntime(get_engine())
        request.app.state.chat_runtime = runtime
    return runtime


@router.get(
    "",
    response_model=MessagesResponse,
    response_model_exclude_none=True,
)
async def get_messages(
    session_id: UUID,
    before: UUID | None = None,
    after: UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> MessagesResponse:
    return await messages.get_messages_response(
        db,
        user_id=user.id,
        session_id=session_id,
        before=before,
        after=after,
        limit=limit,
    )


@router.post("")
async def post_message(
    session_id: UUID,
    body: PostMessageRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    runtime: ChatRuntime = Depends(get_chat_runtime),
) -> StreamingResponse:
    accepted = await messages.accept_message(
        db,
        user=user,
        session_id=session_id,
        body=body,
        runner_instance_id=runtime.runner_instance_id,
    )
    runtime.schedule(accepted)
    subscriber = await runtime.register(accepted)

    async def event_stream() -> AsyncIterator[bytes]:
        try:
            async for chunk in subscriber.ndjson():
                yield chunk
        finally:
            await runtime.unregister(
                session_id=session_id,
                subscriber=subscriber,
            )

    return StreamingResponse(
        event_stream(),
        media_type="application/x-ndjson",
    )
