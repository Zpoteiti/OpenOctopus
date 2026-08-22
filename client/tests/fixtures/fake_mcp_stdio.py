"""Small line-delimited MCP server used by the transport integration tests."""

from __future__ import annotations

import json
import os
import sys
from typing import Any


def _result(request_id: object, result: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}) + "\n")
    sys.stdout.flush()


for raw_line in sys.stdin:
    request = json.loads(raw_line)
    method = request.get("method")
    request_id = request.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        _result(
            request_id,
            {
                "protocolVersion": request["params"]["protocolVersion"],
                "capabilities": {
                    "tools": {},
                    "resources": {},
                    "prompts": {},
                },
                "serverInfo": {"name": "openoctopus-fake", "version": "1"},
            },
        )
    elif method == "tools/list":
        _result(
            request_id,
            {
                "tools": [
                    {
                        "name": "environment",
                        "description": "Return selected child environment values.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "keys": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                }
                            },
                            "required": ["keys"],
                        },
                    }
                ]
            },
        )
    elif method == "resources/list":
        _result(request_id, {"resources": []})
    elif method == "resources/templates/list":
        _result(request_id, {"resourceTemplates": []})
    elif method == "prompts/list":
        _result(request_id, {"prompts": []})
    elif method == "tools/call":
        keys = request["params"].get("arguments", {}).get("keys", [])
        payload = {key: os.environ.get(key) for key in keys}
        _result(
            request_id,
            {
                "content": [{"type": "text", "text": json.dumps(payload, sort_keys=True)}],
                # Deliberately mismatches outputSchema. Raw send_request must not validate it.
                "structuredContent": {"unexpected": True},
            },
        )
    else:
        sys.stdout.write(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": "method not found"},
                }
            )
            + "\n"
        )
        sys.stdout.flush()
