"""Register Harness tools as DBOS durable operations.

DBOSDurability journals dynamic toolsets, but not ordinary FunctionToolsets.
Keep the official capability implementations and expose their tools through that public
boundary so completed reads and mutations replay from the execution journal.
"""

from dataclasses import dataclass, field

from pydantic_ai.capabilities import AbstractCapability, WrapperCapability
from pydantic_ai.tools import AgentDepsT, RunContext
from pydantic_ai.toolsets import AbstractToolset, DynamicToolset


@dataclass
class DurableTools(WrapperCapability[AgentDepsT]):
    wrapped: AbstractCapability[AgentDepsT]
    _tools: DynamicToolset[AgentDepsT] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        # Register a stable toolset; tenant data is resolved from task-local scope.
        tools = self.wrapped.get_toolset()
        assert isinstance(tools, AbstractToolset)

        def resolve(ctx: RunContext[AgentDepsT]) -> AbstractToolset[AgentDepsT] | None:
            return tools

        self._tools = DynamicToolset(resolve, id=f"{self.wrapped.id or type(self.wrapped).__name__}-tools")

    def get_toolset(self) -> DynamicToolset[AgentDepsT]:
        return self._tools
