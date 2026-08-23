from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from mcp import types
from pydantic import SecretStr

from openoctopus_client import __version__
from openoctopus_client.config import ConfigurationError, load_config
from openoctopus_client.connection import ClientRuntime
from openoctopus_client.document_convert import (
    ConversionError,
    conversion_worker_main,
    convert_path,
)
from openoctopus_client.mcp.catalog import discover_server_catalog
from openoctopus_client.mcp.models import StdioMcpServerConfig
from openoctopus_client.mcp.runtime import build_runtime_client
from openoctopus_client.mcp.transport import BoundedStdioTransport
from openoctopus_client.process import (
    ProcessBackendError,
    PtyUnavailableError,
    frozen_backend_smoke,
    validate_pty_backend,
)

_MCP_SMOKE_ENV_NAME = "MCP_FROZEN_SMOKE_SENTINEL"
_MCP_SMOKE_ENV_VALUE = "openoctopus-mcp-stdio-smoke"


async def _mcp_stdio_smoke(command: str, fixture: Path) -> None:
    client = build_runtime_client(
        StdioMcpServerConfig(
            name="frozen_smoke",
            transport="stdio",
            command=command,
            args=[str(fixture)],
            env={_MCP_SMOKE_ENV_NAME: SecretStr(_MCP_SMOKE_ENV_VALUE)},
        )
    )
    transport = cast(BoundedStdioTransport, client.transport)
    try:
        await client.__aenter__()
        catalog = await discover_server_catalog("frozen_smoke", client.session)
        if [tool.raw_name for tool in catalog.tools] != ["environment"]:
            raise RuntimeError("unexpected MCP catalog")
        response = await client.session.send_request(
            types.ClientRequest(
                root=types.CallToolRequest(
                    params=types.CallToolRequestParams(
                        name="environment",
                        arguments={
                            "keys": [
                                _MCP_SMOKE_ENV_NAME,
                                "OPENOCTOPUS_DEVICE_TOKEN",
                            ]
                        },
                    )
                )
            ),
            types.CallToolResult,
        )
        if not response.content or not isinstance(response.content[0], types.TextContent):
            raise RuntimeError("unexpected MCP result")
        values = json.loads(response.content[0].text)
        if values != {
            _MCP_SMOKE_ENV_NAME: _MCP_SMOKE_ENV_VALUE,
            "OPENOCTOPUS_DEVICE_TOKEN": None,
        }:
            raise RuntimeError("MCP child environment boundary failed")
    finally:
        await client.close()
    if (
        transport.cleanup_incomplete
        or transport.process is None
        or transport.process.returncode is None
    ):
        raise RuntimeError("MCP child cleanup did not converge")


def _configure_utf8_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            cast(Callable[..., Any], reconfigure)(encoding="utf-8", errors="strict")


def main() -> int:
    _configure_utf8_stdio()
    parser = argparse.ArgumentParser(prog="openoctopus-client")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("version")
    commands.add_parser("run")
    spike = commands.add_parser("_spike-convert", help=argparse.SUPPRESS)
    spike.add_argument("path", type=Path)
    spike.add_argument("--pages")
    commands.add_parser("_conversion-worker", help=argparse.SUPPRESS)
    commands.add_parser("_exec-backend-smoke", help=argparse.SUPPRESS)
    mcp_smoke = commands.add_parser("_mcp-stdio-smoke", help=argparse.SUPPRESS)
    mcp_smoke.add_argument("executable")
    mcp_smoke.add_argument("fixture", type=Path)
    arguments = parser.parse_args()
    if arguments.command == "version":
        print(__version__)
        return 0
    if arguments.command == "_conversion-worker":
        return conversion_worker_main()
    if arguments.command == "_exec-backend-smoke":
        try:
            payload = asyncio.run(frozen_backend_smoke())
        except ProcessBackendError:
            print(json.dumps({"code": "tool_exec_failed", "ok": False}, sort_keys=True))
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0
    if arguments.command == "_mcp-stdio-smoke":
        try:
            asyncio.run(_mcp_stdio_smoke(arguments.executable, arguments.fixture))
        except Exception:
            print(json.dumps({"code": "mcp_smoke_failed", "ok": False}, sort_keys=True))
            return 1
        print(json.dumps({"ok": True, "stdio_mcp": True}, sort_keys=True))
        return 0
    if arguments.command in {None, "run"}:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        try:
            config = load_config()
        except ConfigurationError as exc:
            print(f"configuration error: {exc}", file=sys.stderr)
            return 78
        try:
            validate_pty_backend()
        except PtyUnavailableError:
            print("backend error: PTY backend is unavailable", file=sys.stderr)
            return 78
        try:
            return asyncio.run(ClientRuntime(config).run())
        except KeyboardInterrupt:
            return 0
    try:
        text = convert_path(arguments.path, pages=arguments.pages)
    except ConversionError as exc:
        print(json.dumps({"code": exc.code, "message": exc.message, "ok": False}, sort_keys=True))
        return 1
    except Exception:
        print(
            json.dumps(
                {
                    "code": "tool_content_conversion_failed",
                    "message": "Document conversion failed",
                    "ok": False,
                },
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps({"ok": True, "text": text}, ensure_ascii=False, sort_keys=True))
    return 0
