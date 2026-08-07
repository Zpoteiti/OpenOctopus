from collections.abc import Sequence
from typing import Any, NoReturn
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.prompt import build_system_prompt
from openctopus_server.chat.public_projection import provider_role
from openctopus_server.db.models import Message, PendingMessage, Session, User
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.provider.anthropic import provider_fingerprint
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.workspace.skills import SkillsCache

from .prompt import PromptWorkspaceService

_TOOL_RESULT_KINDS = {"tool_result", "synthetic_tool_result"}
_COMPACTION_CONTINUATION: dict[str, Any] = {
    "role": "user",
    "content": [
        {
            "type": "text",
            "text": "Continue the current task from the compacted state above.",
        }
    ],
}


async def build_provider_context(
    db: AsyncSession,
    *,
    session_id: UUID,
    config: ProviderConfig,
    include_pending: bool = False,
    add_compaction_continuation: bool = True,
    workspace_service: PromptWorkspaceService | None = None,
    skills_cache: SkillsCache | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    session = await db.get(Session, session_id)
    if session is None:
        raise ChatError(ErrorCode.NOT_FOUND, "Session not found")
    user = await db.get(User, session.user_id)
    if user is None:
        raise ChatError(ErrorCode.NOT_FOUND, "Session owner not found")

    rows = list(
        (
            await db.execute(
                select(Message)
                .where(
                    Message.session_id == session_id,
                    Message.is_compacted.is_(False),
                )
                .order_by(Message.created_at, Message.id)
            )
        )
        .scalars()
        .all()
    )
    pending_rows: list[PendingMessage] = []
    if include_pending:
        pending_rows = list(
            (
                await db.execute(
                    select(PendingMessage)
                    .where(PendingMessage.session_id == session_id)
                    .order_by(PendingMessage.received_at, PendingMessage.id)
                )
            )
            .scalars()
            .all()
        )
    projected = project_provider_messages(
        rows,
        current_fingerprint=provider_fingerprint(config),
        pending_rows=pending_rows,
        add_compaction_continuation=add_compaction_continuation,
    )

    system = await build_system_prompt(
        db,
        session=session,
        user=user,
        workspace_service=workspace_service,
        skills_cache=skills_cache,
    )
    return system, projected


def project_provider_messages(
    rows: Sequence[Message],
    *,
    current_fingerprint: str,
    pending_rows: Sequence[PendingMessage] = (),
    add_compaction_continuation: bool = True,
) -> list[dict[str, Any]]:
    projected = project_message_rows(rows, current_fingerprint=current_fingerprint)
    projected.extend(
        {"role": "user", "content": [dict(block) for block in row.content]} for row in pending_rows
    )
    if (
        add_compaction_continuation
        and not pending_rows
        and rows
        and rows[-1].message_kind == "compaction_summary"
    ):
        projected.append(
            {
                "role": _COMPACTION_CONTINUATION["role"],
                "content": [dict(block) for block in _COMPACTION_CONTINUATION["content"]],
            }
        )
    return projected


def project_message_rows(
    rows: Sequence[Message],
    *,
    current_fingerprint: str,
) -> list[dict[str, Any]]:
    projected: list[dict[str, Any]] = []
    unresolved_tool_ids: list[str] = []
    previous_was_tool_result = False

    for row in rows:
        content = [_provider_block(block) for block in row.content]
        role = provider_role(row.message_kind)
        if role == "assistant" and row.llm_fingerprint != current_fingerprint:
            content = [
                block
                for block in content
                if block.get("type") not in {"thinking", "redacted_thinking"}
            ]
        if not content:
            if row.message_kind in _TOOL_RESULT_KINDS:
                _invalid_history("tool-result row is empty")
            if unresolved_tool_ids:
                _invalid_history("message boundary splits an incomplete tool-result batch")
            previous_was_tool_result = False
            continue

        if row.message_kind in _TOOL_RESULT_KINDS:
            if not unresolved_tool_ids or any(
                block.get("type") != "tool_result" for block in content
            ):
                _invalid_history("tool-result row has no adjacent assistant tool-use batch")
            result_ids: list[str] = []
            for block in content:
                tool_id = block.get("tool_use_id")
                if not isinstance(tool_id, str):
                    _invalid_history("tool-result row contains an invalid tool_use_id")
                result_ids.append(tool_id)
            for tool_id in result_ids:
                if tool_id not in unresolved_tool_ids:
                    _invalid_history("tool-result row is duplicate or belongs to another batch")
                unresolved_tool_ids.remove(tool_id)
            if previous_was_tool_result:
                projected[-1]["content"].extend(content)
            else:
                projected.append({"role": "user", "content": content})
            previous_was_tool_result = True
            continue

        if unresolved_tool_ids:
            _invalid_history("message boundary splits an incomplete tool-result batch")

        if role == "assistant":
            tool_ids: list[str] = []
            for block in content:
                if block.get("type") != "tool_use":
                    continue
                tool_id = block.get("id")
                if not isinstance(tool_id, str) or not tool_id:
                    _invalid_history("assistant row contains an invalid tool-use id")
                tool_ids.append(tool_id)
            if len(tool_ids) != len(set(tool_ids)):
                _invalid_history("assistant row contains duplicate tool-use ids")
            unresolved_tool_ids = list(tool_ids)

        projected.append({"role": role, "content": content})
        previous_was_tool_result = False

    if unresolved_tool_ids:
        _invalid_history("assistant tool-use batch is missing persisted results")
    return projected


def _provider_block(block: dict[str, Any]) -> dict[str, Any]:
    projected = dict(block)
    if projected.get("type") == "tool_result":
        projected.pop("code", None)
    return projected


def _invalid_history(message: str) -> NoReturn:
    raise ChatError(ErrorCode.PROVIDER_PROTOCOL_ERROR, f"Invalid persisted history: {message}")
