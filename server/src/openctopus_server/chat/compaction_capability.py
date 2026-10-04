"""Configure the upstream compactor from the captured model deployment limits."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from pydantic_ai import RunContext
from pydantic_ai_harness.compaction import SummarizingCompaction

if TYPE_CHECKING:
    from openctopus_server.chat.runner import ChatRuntime


class ConfiguredCompaction(SummarizingCompaction[str]):
    def __init__(self, runtime: ChatRuntime) -> None:
        super().__init__(max_fraction=0.8, keep_messages=8, receipts=True)
        self.runtime = runtime

    async def for_run(self, ctx: RunContext[str]) -> SummarizingCompaction[str]:
        scope = self.runtime._agent_runs[ctx.deps]
        config = self.runtime._model_configurations[scope.model_revision]
        # replace on the public base dataclass avoids carrying host dependencies
        # into the summarizer's durable operation arguments.
        capability = SummarizingCompaction[str](keep_messages=8, receipts=True, max_fraction=0.8)
        if config.compaction_threshold_tokens is not None:
            assert config.max_context_tokens is not None
            capability = replace(capability, max_fraction=None,
                                 max_tokens=config.max_context_tokens - config.compaction_threshold_tokens)
        elif config.max_context_tokens is not None:
            capability = replace(capability, context_window=config.max_context_tokens)
        return capability
