from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.chat.attachments import (
    expand_server_workspace_attachments,
    normalize_browser_attachment_refs,
)
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.engine import get_engine
from openctopus_server.db.models import User
from openctopus_server.db.session import get_db
from openctopus_server.dto.error import ErrorResponse
from openctopus_server.dto.message import MessagesResponse, PostMessageRequest
from openctopus_server.dto.session import SessionPatchRequest, SessionResponse
from openctopus_server.services import messages, sessions
from openctopus_server.workspace.service import WorkspaceService, get_workspace_service

collection_router = APIRouter(
    prefix="/api/sessions",
    tags=["Sessions"],
)

router = APIRouter(
    prefix="/api/sessions/{session_id}/messages",
    tags=["Sessions"],
)

control_router = APIRouter(
    prefix="/api/sessions/{session_id}",
    tags=["Sessions"],
)


def get_chat_runtime(request: Request) -> ChatRuntime:
    runtime = getattr(request.app.state, "chat_runtime", None)
    if runtime is None:
        runtime = ChatRuntime(get_engine())
        request.app.state.chat_runtime = runtime
    return runtime


@collection_router.get(
    "",
    response_model=list[SessionResponse],
)
async def list_sessions(
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[SessionResponse]:
    return await sessions.list_owned(
        db,
        user_id=user.id,
        limit=limit,
        offset=offset,
    )


@router.get(
    "",
    response_model=MessagesResponse,
)
async def get_messages(
    session_id: UUID,
    before: UUID | None = None,
    after: UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> JSONResponse:
    result = await messages.get_messages_response(
        db,
        user_id=user.id,
        session_id=session_id,
        before=before,
        after=after,
        limit=limit,
    )
    payload = result.model_dump(mode="json", exclude_none=True)
    payload["active_turn_id"] = (
        str(result.active_turn_id) if result.active_turn_id is not None else None
    )
    payload["last_message_id"] = (
        str(result.last_message_id) if result.last_message_id is not None else None
    )
    return JSONResponse(payload)


@router.post(
    "",
    response_model=None,
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "Newline-delimited best-effort agent events.",
            "content": {
                "application/x-ndjson": {
                    "schema": {"type": "string"},
                }
            },
        },
        400: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def post_message(
    session_id: UUID,
    body: PostMessageRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    runtime: ChatRuntime = Depends(get_chat_runtime),
    workspace_service: WorkspaceService = Depends(get_workspace_service),
) -> StreamingResponse:
    async with runtime.session_operation(session_id):
        if body.attachments:
            await messages.preflight_message_target(
                db,
                user_id=user.id,
                session_id=session_id,
            )
        attachment_refs = await normalize_browser_attachment_refs(
            db,
            user_id=user.id,
            attachments=body.attachments,
        )
        content = await expand_server_workspace_attachments(
            db,
            workspace_service=workspace_service,
            user_id=user.id,
            content=[
                block.model_dump(mode="json", exclude_none=True) for block in body.content
            ],
            attachments=body.attachments,
        )
        accepted = await messages.accept_message(
            db,
            user=user,
            session_id=session_id,
            content=content,
            attachment_refs=attachment_refs,
            effort=body.effort,
            runner_instance_id=runtime.runner_instance_id,
        )
        await runtime.schedule(accepted)
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


@control_router.patch(
    "",
    response_model=SessionResponse,
    responses={400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
)
async def patch_session(
    session_id: UUID,
    body: SessionPatchRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SessionResponse:
    return await sessions.patch_owned(
        db,
        user_id=user.id,
        session_id=session_id,
        patch=body,
    )


@control_router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
)
async def delete_session(
    session_id: UUID,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    runtime: ChatRuntime = Depends(get_chat_runtime),
) -> Response:
    await sessions.delete_owned(
        db,
        user_id=user.id,
        session_id=session_id,
        runtime=runtime,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@control_router.post("/cancel", status_code=202)
async def cancel_session(
    session_id: UUID,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, bool]:
    cancel_requested = await messages.request_cancel(
        db,
        user_id=user.id,
        session_id=session_id,
    )
    return {"cancel_requested": cancel_requested}
