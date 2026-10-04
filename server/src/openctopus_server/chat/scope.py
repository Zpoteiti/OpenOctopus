"""Task-local host dependencies for SDK storage protocols without a RunContext."""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openctopus_server.chat.agent import AgentRun

active_run: ContextVar[AgentRun] = ContextVar("openoctopus_agent_run")
