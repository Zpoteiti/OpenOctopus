"""Internal worker and frozen-build smoke entry points of the core binary.

These modes are not user-facing commands.  The tray launches the core with
``_core-run``; helpers and smokes are launched by the core itself or by the
frozen smoke scripts.  None of them load the tray, read system credentials,
or start a second GUI instance.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from mcp import types
from pydantic import SecretStr

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
    frozen_backend_smoke,
)

_MCP_SMOKE_ENV_NAME = "MCP_FROZEN_SMOKE_SENTINEL"
_MCP_SMOKE_ENV_VALUE = "openoctopus-mcp-stdio-smoke"


def configure_utf8_stdio() -> None:
    import sys

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            cast(Callable[..., Any], reconfigure)(encoding="utf-8", errors="strict")


async def run_mcp_stdio_smoke(command: str, fixture: Path) -> None:
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


def exec_backend_smoke() -> int:
    try:
        payload = asyncio.run(frozen_backend_smoke())
    except ProcessBackendError:
        print(json.dumps({"code": "tool_exec_failed", "ok": False}, sort_keys=True))
        return 1
    print(json.dumps(payload, sort_keys=True))
    return 0


def mcp_stdio_smoke(command: str, fixture: Path) -> int:
    try:
        asyncio.run(run_mcp_stdio_smoke(command, fixture))
    except Exception:
        print(json.dumps({"code": "mcp_smoke_failed", "ok": False}, sort_keys=True))
        return 1
    print(json.dumps({"ok": True, "stdio_mcp": True}, sort_keys=True))
    return 0


def spike_convert(path: Path, pages: str | None) -> int:
    try:
        text = convert_path(path, pages=pages)
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


__all__ = [
    "configure_utf8_stdio",
    "conversion_worker_main",
    "exec_backend_smoke",
    "mcp_stdio_smoke",
    "run_mcp_stdio_smoke",
    "spike_convert",
]
