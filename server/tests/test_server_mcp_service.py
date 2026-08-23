from __future__ import annotations

import traceback
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.exc import DBAPIError

from openctopus_server.mcp.models import empty_server_mcp_envelope
from openctopus_server.services import server_mcp


async def test_commit_ack_failure_is_reported_as_unknown_after_best_effort_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_lock(_db: object) -> None:
        return None

    monkeypatch.setattr(server_mcp, "lock_global_mcp_catalog_write", no_lock)
    monkeypatch.setattr(server_mcp, "lock_server_mcp_config_write", no_lock)
    secret = "Bearer commit-secret-sentinel"
    commit_error = DBAPIError(
        "UPDATE system_configs SET value = :payload",
        {"payload": {"headers": {"authorization": secret}}},
        RuntimeError("database acknowledgement was lost"),
        False,
    )
    db = Mock()
    db.scalar = AsyncMock(return_value=None)
    db.commit = AsyncMock(side_effect=commit_error)
    db.rollback = AsyncMock(side_effect=RuntimeError("connection is still unavailable"))
    candidate = empty_server_mcp_envelope().model_copy(update={"config_revision": 2})

    with pytest.raises(server_mcp.ServerMcpCommitOutcomeUnknownError) as captured:
        await server_mcp.commit_candidate(
            db,
            base_config_revision=1,
            candidate=candidate,
        )

    rendered = "".join(traceback.format_exception(captured.value))
    assert secret not in rendered
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert captured.value.interruption is None
    db.rollback.assert_awaited_once()
