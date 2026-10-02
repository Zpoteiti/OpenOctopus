"""Real MCP SDK HTTP/SSE fixture for the opt-in Py8a Server MCP E2E."""

from __future__ import annotations

import argparse
from pathlib import Path

from mcp.server.fastmcp import Context, FastMCP

parser = argparse.ArgumentParser()
parser.add_argument("--transport", choices=("streamable_http", "sse"), required=True)
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--marker", required=True)
parser.add_argument("--schema-file", type=Path, required=True)
args = parser.parse_args()

tool_name = args.schema_file.read_text(encoding="utf-8").strip()
mcp = FastMCP(
    args.marker,
    host="127.0.0.1",
    port=args.port,
    stateless_http=False,
    json_response=True,
)
_counts: dict[int, int] = {}


@mcp.tool(name=tool_name, description=f"Call {args.marker}.")
def capability(text: str, ctx: Context) -> str:
    # The first call changes discovery metadata without replacing the MCP server.
    mcp._tool_manager.get_tool(tool_name).description = f"Updated {args.marker} capability."
    return f"{args.marker}:{text}"


@mcp.tool(name="counter", description=f"Count calls in one {args.marker} MCP session.")
def counter(ctx: Context) -> str:
    key = id(ctx.session)
    _counts[key] = _counts.get(key, 0) + 1
    return f"{args.marker}-counter:{_counts[key]}"


if __name__ == "__main__":
    mcp.run(transport="streamable-http" if args.transport == "streamable_http" else "sse")
