"""Authenticated editing of the same notebook used by Harness agents."""

from fastapi import APIRouter, Depends, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field
from pydantic_ai_harness.memory import MemoryConflictError

from openctopus_server.api.sessions import get_chat_runtime
from openctopus_server.auth.dependencies import get_current_user
from openctopus_server.chat.memory import MEMORY_MAX_CHARS, memory_path
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError

router = APIRouter(prefix="/api/memory", tags=["Memory"])


class MemoryNote(BaseModel):
    path: str
    content: str
    version: str | None


class MemoryEdit(BaseModel):
    model_config = ConfigDict(extra="forbid")
    content: str = Field(max_length=MEMORY_MAX_CHARS)
    expected_version: str | None


class MemoryPage(BaseModel):
    paths: list[str]
    next_offset: int | None


def _path(user: User, path: str) -> str:
    try:
        return memory_path(user.id, path)
    except ValueError as exc:
        raise WorkspaceError(ErrorCode.WORKSPACE_INVALID_REQUEST, str(exc)) from None


@router.get("", response_model=MemoryPage)
async def list_notes(
    offset: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=100),
    user: User = Depends(get_current_user), runtime: ChatRuntime = Depends(get_chat_runtime),
) -> MemoryPage:
    prefix = f"{user.id}/main/"
    paths = await runtime.memory.store.list_paths(prefix, limit=offset + limit + 1)
    return MemoryPage(paths=[path[len(prefix):] for path in paths[offset:offset + limit]],
                      next_offset=offset + limit if len(paths) > offset + limit else None)


@router.get("/{path:path}", response_model=MemoryNote)
async def read_note(
    path: str, user: User = Depends(get_current_user), runtime: ChatRuntime = Depends(get_chat_runtime),
) -> MemoryNote:
    note = await runtime.memory.store.read(_path(user, path), max_chars=MEMORY_MAX_CHARS)
    if note is not None and note.truncated:
        raise WorkspaceError(ErrorCode.WORKSPACE_INVALID_REQUEST, "Memory is too large to edit")
    return MemoryNote(path=path, content=note.content if note else "", version=note.version if note else None)


@router.put("/{path:path}", response_model=MemoryNote)
async def write_note(
    path: str, edit: MemoryEdit, user: User = Depends(get_current_user),
    runtime: ChatRuntime = Depends(get_chat_runtime),
) -> MemoryNote:
    try:
        result = await runtime.memory.store.write(_path(user, path), edit.content, expected_version=edit.expected_version)
    except MemoryConflictError:
        raise WorkspaceError(ErrorCode.WORKSPACE_FILE_CHANGED, "Memory changed. Reload before saving your edits.") from None
    return MemoryNote(path=path, content=edit.content, version=result.version)


@router.delete("/{path:path}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_note(
    path: str, expected_version: str = Query(min_length=1), user: User = Depends(get_current_user),
    runtime: ChatRuntime = Depends(get_chat_runtime),
) -> Response:
    try:
        await runtime.memory.store.delete(_path(user, path), expected_version=expected_version)
    except MemoryConflictError:
        raise WorkspaceError(ErrorCode.WORKSPACE_FILE_CHANGED, "Memory changed. Reload before deleting it.") from None
    return Response(status_code=status.HTTP_204_NO_CONTENT)
