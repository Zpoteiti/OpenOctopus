"""Expose OO's routed tool schemas through the SDK with real argument validation."""

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from jsonschema import ValidationError
from jsonschema.validators import validator_for
from pydantic_ai import ModelRetry, RunContext
from pydantic_ai.tools import Tool
from pydantic_ai.toolsets import FunctionToolset
from referencing import Registry


def toolset_from_schemas[DepsT](
    schemas: Sequence[dict[str, Any]],
    execute: Callable[[str, dict[str, Any], RunContext[DepsT]], Awaitable[Any]],
) -> FunctionToolset[DepsT]:
    """Bind per-run routing to SDK tools; the host still checks live authority.

    Tool.from_schema deliberately skips JSON Schema validation. Its public
    args_validator hook enforces the exact schema exposed to the model, including
    device enums and nested MCP parameters. References resolve locally only.
    """
    return FunctionToolset([_tool(schema, execute) for schema in schemas])


def _tool[DepsT](
    schema: dict[str, Any],
    execute: Callable[[str, dict[str, Any], RunContext[DepsT]], Awaitable[Any]],
) -> Tool[DepsT]:
    name = str(schema["name"])
    input_schema = schema["input_schema"]
    validator = validator_for(input_schema)(input_schema, registry=Registry())

    async def validate(ctx: RunContext[DepsT], **args: Any) -> None:
        try:
            validator.validate(args)
        except ValidationError as exc:
            raise ModelRetry(f"Invalid arguments for {name}: {exc.message[:1000]}") from exc

    async def call(ctx: RunContext[DepsT], **args: Any) -> Any:
        return await execute(name, args, ctx)

    return Tool.from_schema(
        call, name=name, description=schema.get("description"), json_schema=input_schema,
        takes_ctx=True, sequential=True, args_validator=validate,
    )
