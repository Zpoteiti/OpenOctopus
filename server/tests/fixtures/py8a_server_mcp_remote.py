"""Real MCP SDK HTTP/SSE fixture for the opt-in Py8a Server MCP E2E."""

from __future__ import annotations

import argparse
from pathlib import Path

from mcp.server.fastmcp import FastMCP

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
    stateless_http=args.transport == "streamable_http",
    json_response=True,
)


@mcp.tool(name=tool_name, description=f"Call {args.marker}.")
def capability(text: str) -> str:
    return f"{args.marker}:{text}"


if __name__ == "__main__":
    mcp.run(transport="streamable-http" if args.transport == "streamable_http" else "sse")
