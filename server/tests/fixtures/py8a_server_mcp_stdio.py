"""Real MCP SDK stdio fixture for the opt-in Py8a Server MCP E2E."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("py8a-stdio")


@mcp.tool(name="echo", description="Echo one text value through stdio.")
def echo(text: str) -> str:
    return f"stdio:{text}"


@mcp.resource(
    "file:///openoctopus-py8a-manual.txt",
    name="manual",
    description="OpenOctopus Py8a test manual.",
    mime_type="text/plain",
)
def manual() -> str:
    return "resource:py8a-manual"


@mcp.resource(
    "https://example.test/py8a/issues/{issue_id}",
    name="issue",
    description="Read one Py8a test issue.",
    mime_type="text/plain",
)
def issue(issue_id: str) -> str:
    return f"resource:py8a-issue-{issue_id}"


@mcp.prompt(name="explain", description="Explain one Py8a topic.")
def explain(topic: str) -> str:
    return f"prompt:{topic}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
