"""Static OpenAPI contracts introduced by Py8a."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml


def _schemas() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[2] / "docs" / "API.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], document["components"]["schemas"])


def test_device_mcp_response_is_a_strict_transport_tagged_union() -> None:
    schemas = _schemas()
    response = schemas["DeviceMcpServerConfigView"]

    assert response["oneOf"] == [
        {"$ref": "#/components/schemas/DeviceStdioMcpServerConfigView"},
        {"$ref": "#/components/schemas/DeviceRemoteMcpServerConfigView"},
    ]
    assert response["discriminator"] == {
        "propertyName": "transport",
        "mapping": {
            "stdio": "#/components/schemas/DeviceStdioMcpServerConfigView",
            "streamable_http": "#/components/schemas/DeviceRemoteMcpServerConfigView",
            "sse": "#/components/schemas/DeviceRemoteMcpServerConfigView",
        },
    }

    stdio = schemas["DeviceStdioMcpServerConfigView"]
    remote = schemas["DeviceRemoteMcpServerConfigView"]
    assert stdio["additionalProperties"] is False
    assert set(stdio["required"]) == {
        "name",
        "transport",
        "command",
        "args",
        "cwd",
        "env",
        "enabled_capabilities",
        "effective_status",
        "shadowed_by",
    }
    assert set(stdio["properties"]) == set(stdio["required"])
    assert stdio["properties"]["transport"]["enum"] == ["stdio"]

    assert remote["additionalProperties"] is False
    assert set(remote["required"]) == {
        "name",
        "transport",
        "url",
        "headers",
        "enabled_capabilities",
        "effective_status",
        "shadowed_by",
    }
    assert set(remote["properties"]) == set(remote["required"])
    assert remote["properties"]["transport"]["enum"] == [
        "streamable_http",
        "sse",
    ]


def test_server_mcp_response_is_canonical_and_redacted() -> None:
    schemas = _schemas()
    response = schemas["ServerMcpResponse"]["properties"]["mcp_servers"]
    assert response["items"] == {
        "$ref": "#/components/schemas/ServerMcpServerConfigView"
    }

    tagged = schemas["ServerMcpServerConfigView"]
    assert tagged["oneOf"] == [
        {"$ref": "#/components/schemas/ServerStdioMcpServerConfigView"},
        {"$ref": "#/components/schemas/ServerRemoteMcpServerConfigView"},
    ]
    assert tagged["discriminator"]["mapping"] == {
        "stdio": "#/components/schemas/ServerStdioMcpServerConfigView",
        "streamable_http": "#/components/schemas/ServerRemoteMcpServerConfigView",
        "sse": "#/components/schemas/ServerRemoteMcpServerConfigView",
    }

    stdio = schemas["ServerStdioMcpServerConfigView"]
    remote = schemas["ServerRemoteMcpServerConfigView"]
    assert set(stdio["required"]) == set(stdio["properties"]) == {
        "name",
        "transport",
        "command",
        "args",
        "cwd",
        "env",
        "enabled_capabilities",
        "max_concurrent_calls",
    }
    assert stdio["properties"]["env"]["additionalProperties"]["enum"] == [
        "<redacted>"
    ]
    assert set(remote["required"]) == set(remote["properties"]) == {
        "name",
        "transport",
        "url",
        "headers",
        "enabled_capabilities",
        "max_concurrent_calls",
    }
    assert remote["properties"]["headers"]["additionalProperties"]["enum"] == [
        "<redacted>"
    ]
