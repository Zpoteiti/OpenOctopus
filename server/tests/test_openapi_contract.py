import json
from pathlib import Path
from typing import Any

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode

from openctopus_server.main import create_app

_HTTP_METHODS = {"delete", "get", "head", "options", "patch", "post", "put", "trace"}


def _static_openapi() -> dict[str, Any]:
    path = Path(__file__).parents[2] / "docs" / "API.yaml"
    source = path.read_text(encoding="utf-8")
    node = yaml.compose(source, Loader=yaml.SafeLoader)
    assert node is not None
    _assert_unique_mapping_keys(node)
    document = yaml.safe_load(source)
    assert isinstance(document, dict)
    return document


def _assert_unique_mapping_keys(node: Node) -> None:
    if isinstance(node, MappingNode):
        seen: set[str] = set()
        for key, value in node.value:
            assert isinstance(key, ScalarNode), "OpenAPI mapping keys must be scalar"
            assert key.value not in seen, f"duplicate OpenAPI mapping key: {key.value}"
            seen.add(key.value)
            _assert_unique_mapping_keys(value)
        return
    for child in getattr(node, "value", ()):
        if isinstance(child, Node):
            _assert_unique_mapping_keys(child)


def _operations(document: dict[str, Any]) -> set[tuple[str, str]]:
    return {
        (path, method)
        for path, path_item in document["paths"].items()
        for method in path_item
        if method in _HTTP_METHODS
    }


def test_static_api_inventory_matches_runtime() -> None:
    static = _static_openapi()
    runtime = create_app().openapi()

    assert _operations(static) == _operations(runtime)


def test_static_validation_statuses_match_runtime() -> None:
    static = _static_openapi()
    runtime = create_app().openapi()

    for path, method in _operations(runtime):
        static_operation = static["paths"][path][method]
        runtime_operation = runtime["paths"][path][method]
        static_statuses = {
            str(status)
            for status in static_operation.get("responses", {})
            if str(status) in {"400", "422"}
        }
        runtime_statuses = {
            str(status)
            for status in runtime_operation.get("responses", {})
            if str(status) in {"400", "422"}
        }
        assert static_statuses == runtime_statuses, f"{method.upper()} {path}"


def test_static_channels_contract_matches_runtime() -> None:
    static = _static_openapi()
    runtime = create_app().openapi()

    assert set(static["paths"]["/api/channels/{channel}"]) & _HTTP_METHODS == {
        "patch",
        "delete",
    }
    assert set(
        static["paths"]["/api/channels/{channel}/pairing"]
    ) & _HTTP_METHODS == {"post"}
    for path, method in (
        ("/api/channels", "get"),
        ("/api/channels/{channel}", "patch"),
        ("/api/channels/{channel}", "delete"),
        ("/api/channels/{channel}/pairing", "post"),
    ):
        assert set(static["paths"][path][method]["responses"]) == set(
            runtime["paths"][path][method]["responses"]
        )

    static_schemas = static["components"]["schemas"]
    runtime_schemas = runtime["components"]["schemas"]
    assert static_schemas["ExternalChannel"]["enum"] == runtime_schemas[
        "ExternalChannel"
    ]["enum"]
    assert static_schemas["ChannelState"]["enum"] == runtime_schemas[
        "ChannelState"
    ]["enum"]
    assert set(static_schemas["ChannelConfigPatchRequest"]["properties"]) == {
        "bot_token",
        "client_id",
        "client_secret",
        "allow_list",
    }
    assert all(
        static_schemas["ChannelConfigPatchRequest"]["properties"][name][
            "writeOnly"
        ]
        for name in ("bot_token", "client_id", "client_secret")
    )
    response = static_schemas["ChannelConfigResponse"]
    assert set(response["required"]) == set(response["properties"])
    assert not {
        "bot_token",
        "client_id",
        "client_secret",
    } & set(response["properties"])
    assert set(static_schemas["ChannelOwnerResponse"]["properties"]) == {
        "id",
        "dm_chat_id",
    }
    assert set(static_schemas["ChannelOwnerResponse"]["required"]) == {
        "id",
        "dm_chat_id",
    }
    assert set(static_schemas["ChannelOwnerResponse"]["properties"]) == set(
        runtime_schemas["ChannelOwnerResponse"]["properties"]
    )


def test_static_message_schemas_expose_channel_sidecars() -> None:
    schemas = _static_openapi()["components"]["schemas"]
    runtime_schemas = create_app().openapi()["components"]["schemas"]
    message = schemas["Message"]
    pending = schemas["PendingMessage"]

    assert {
        "sender",
        "source_message_id",
        "channel_context",
        "deliveries",
    } <= set(message["properties"])
    assert {"sender", "source_message_id", "channel_context"} <= set(
        pending["properties"]
    )
    assert "sender" in pending["required"]
    assert message["properties"]["deliveries"]["items"]["$ref"].endswith(
        "/ChannelDeliveryResponse"
    )
    assert set(schemas["ChannelDeliveryResponse"]["properties"]["status"]["enum"]) == {
        "prepared",
        "attempting",
        "sent",
        "partial",
        "failed",
        "unknown",
    }
    for name in (
        "MessageSenderResponse",
        "ChannelContextEntryResponse",
        "ChannelContextResponse",
        "ChannelDeliveryResponse",
    ):
        assert set(schemas[name]["properties"]) == set(
            runtime_schemas[name]["properties"]
        )


def test_device_delivery_ref_requires_preflight_frozen_size() -> None:
    schemas = _static_openapi()["components"]["schemas"]
    runtime_schemas = create_app().openapi()["components"]["schemas"]
    device_ref = schemas["MessageDeliveryRef"]["oneOf"][1]

    assert "size" in device_ref["required"]
    assert "preflight" in device_ref["properties"]["size"]["description"]
    assert "fingerprint" not in device_ref["properties"]
    assert "size" in runtime_schemas["DeviceFileDeliveryRefResponse"]["required"]
    assert "fingerprint" not in runtime_schemas["DeviceFileDeliveryRefResponse"][
        "properties"
    ]


def test_message_openapi_describes_user_input_and_ndjson_response() -> None:
    runtime = create_app().openapi()
    post = runtime["paths"]["/api/sessions/{session_id}/messages"]["post"]
    request = runtime["components"]["schemas"]["PostMessageRequest"]
    response_content = post["responses"]["200"]["content"]

    assert request["properties"]["content"]["items"]["$ref"].endswith(
        "/UserContentBlock"
    )
    assert request["properties"]["attachments"]["maxItems"] == 10
    attachment = runtime["components"]["schemas"]["MessageAttachmentRef"]
    assert attachment["anyOf"] == [
        {"$ref": "#/components/schemas/ServerMessageAttachmentRef"},
        {"$ref": "#/components/schemas/ClientMessageAttachmentRef"},
    ]
    server_attachment = runtime["components"]["schemas"]["ServerMessageAttachmentRef"]
    client_attachment = runtime["components"]["schemas"]["ClientMessageAttachmentRef"]
    assert set(server_attachment["required"]) == {"openoctopus_device", "path"}
    assert "device_id" not in server_attachment["properties"]
    assert set(client_attachment["required"]) == {
        "openoctopus_device",
        "device_id",
        "path",
    }
    assert client_attachment["properties"]["openoctopus_device"]["not"] == {
        "enum": ["server"]
    }
    assert request["anyOf"] == [
        {"properties": {"content": {"minItems": 1}}},
        {"properties": {"attachments": {"minItems": 1}}},
    ]
    assert set(response_content) == {"application/x-ndjson"}
    assert response_content["application/x-ndjson"]["schema"]["type"] == "string"
    assert "409" in post["responses"]


def test_attachment_refs_are_required_in_public_message_schemas() -> None:
    schemas = create_app().openapi()["components"]["schemas"]

    assert "attachment_refs" in schemas["MessageResponse"]["required"]
    assert "attachment_refs" in schemas["PendingMessageResponse"]["required"]


def test_device_directory_picker_openapi_accepts_immutable_device_id() -> None:
    route = create_app().openapi()["paths"]["/api/workspace/list/{path}"]["get"]

    assert any(
        parameter["name"] == "openoctopus_device_id"
        for parameter in route["parameters"]
    )


def test_messages_response_runtime_schema_is_structured() -> None:
    schema = create_app().openapi()["components"]["schemas"]["MessagesResponse"]

    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {
        "messages",
        "pending_messages",
        "status",
        "active_turn_id",
        "last_message_id",
        "pending_count",
        "has_more_before",
    }


def test_admin_config_schema_exposes_every_structured_control() -> None:
    runtime = create_app().openapi()
    schema = runtime["components"]["schemas"]["AdminConfig"]
    patch = runtime["components"]["schemas"]["ConfigPatch"]
    properties = schema["properties"]

    assert set(schema["required"]) == set(properties)
    for name in (
        "llm_endpoint",
        "llm_api_key",
        "llm_model",
        "llm_max_context_tokens",
        "llm_compaction_threshold_tokens",
        "llm_max_concurrent_requests",
    ):
        assert {branch.get("type") for branch in properties[name]["anyOf"]} >= {"null"}

    assert properties["llm_endpoint"]["examples"]
    assert properties["llm_model"]["examples"]
    assert properties["web_fetch_denylist"]["description"]
    assert not patch.get("required")
    assert all('"null"' not in json.dumps(value) for value in patch["properties"].values())


def test_static_effort_enum_keeps_off_as_a_string() -> None:
    effort = _static_openapi()["components"]["schemas"]["Effort"]
    assert "off" in effort["enum"]
    assert False not in effort["enum"]


def test_static_client_attachment_ref_excludes_server_device_name() -> None:
    attachment = _static_openapi()["components"]["schemas"]["MessageAttachmentRef"]
    client_name = attachment["oneOf"][1]["properties"]["openoctopus_device"]

    assert client_name["not"] == {"enum": ["server"]}


def test_runtime_validation_responses_match_stable_error_envelope() -> None:
    runtime = create_app().openapi()
    patch = runtime["components"]["schemas"]["SessionPatchRequest"]

    assert patch["anyOf"] == [
        {"required": ["title"]},
        {"required": ["read_through_message_id"]},
    ]
    assert all('"null"' not in json.dumps(value) for value in patch["properties"].values())
    assert "HTTPValidationError" not in runtime["components"]["schemas"]
    assert "ValidationError" not in runtime["components"]["schemas"]

    admin_responses = runtime["paths"]["/api/admin/config"]["patch"]["responses"]
    assert "422" not in admin_responses
    assert admin_responses["400"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("/ErrorResponse")

    server_mcp = runtime["paths"]["/api/admin/server-mcp"]["put"]["responses"]
    assert "400" not in server_mcp
    assert server_mcp["422"]["content"]["application/json"]["schema"][
        "$ref"
    ].endswith("/ErrorResponse")

    device_config = runtime["paths"]["/api/devices/{name}/config"]["patch"][
        "responses"
    ]
    assert {"400", "422"} <= set(device_config)

    workspace_upload = runtime["paths"]["/api/workspace/files/{path}"]["put"][
        "responses"
    ]
    workspace_patch = runtime["paths"]["/api/workspace/patch"]["post"]["responses"]
    assert {"400", "422"} <= set(workspace_upload)
    assert {"400", "422"} <= set(workspace_patch)
