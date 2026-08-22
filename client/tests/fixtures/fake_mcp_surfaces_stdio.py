"""Line-delimited MCP server exposing all four Py7 discovery surfaces."""

from __future__ import annotations

import json
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
    params = request.get("params", {})
    if method == "initialize":
        _result(
            request_id,
            {
                "protocolVersion": params["protocolVersion"],
                "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
                "serverInfo": {"name": "openoctopus-surfaces", "version": "1"},
            },
        )
    elif method == "tools/list":
        _result(
            request_id,
            {
                "tools": [
                    {
                        "name": "echo",
                        "description": "Echo one text value.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                    }
                ]
            },
        )
    elif method == "resources/list":
        _result(
            request_id,
            {
                "resources": [
                    {
                        "name": "manual",
                        "uri": "file:///openoctopus-manual.txt",
                        "description": "OpenOctopus test manual.",
                        "mimeType": "text/plain",
                    }
                ]
            },
        )
    elif method == "resources/templates/list":
        _result(
            request_id,
            {
                "resourceTemplates": [
                    {
                        "name": "issue",
                        "uriTemplate": "https://example.test/issues/{id}",
                        "description": "Read one test issue.",
                        "mimeType": "text/plain",
                    }
                ]
            },
        )
    elif method == "prompts/list":
        _result(
            request_id,
            {
                "prompts": [
                    {
                        "name": "explain",
                        "description": "Explain one topic.",
                        "arguments": [{"name": "topic", "required": True}],
                    }
                ]
            },
        )
    elif method == "tools/call":
        _result(
            request_id,
            {"content": [{"type": "text", "text": f"echo:{params['arguments']['text']}"}]},
        )
    elif method == "resources/read":
        uri = params["uri"]
        _result(
            request_id,
            {
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": "text/plain",
                        "text": f"resource:{uri}",
                    }
                ]
            },
        )
    elif method == "prompts/get":
        topic = params.get("arguments", {}).get("topic", "")
        _result(
            request_id,
            {
                "messages": [
                    {
                        "role": "user",
                        "content": {"type": "text", "text": f"prompt:{topic}"},
                    }
                ]
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
