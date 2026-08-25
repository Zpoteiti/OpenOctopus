import json
from pathlib import Path
from typing import Any

import yaml

from openctopus_server.main import create_app

_HTTP_METHODS = {"delete", "get", "head", "options", "patch", "post", "put", "trace"}
_DEFERRED_OPERATIONS = {
    ("/api/channels", "get"),
    ("/api/channels/{channel}", "delete"),
    ("/api/channels/{channel}", "patch"),
    ("/api/cron", "get"),
    ("/api/cron", "post"),
    ("/api/cron/{id}", "delete"),
    ("/api/cron/{id}", "get"),
    ("/api/cron/{id}", "patch"),
}


def _static_openapi() -> dict[str, Any]:
    path = Path(__file__).parents[2] / "docs" / "API.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def _operations(document: dict[str, Any]) -> set[tuple[str, str]]:
    return {
        (path, method)
        for path, path_item in document["paths"].items()
        for method in path_item
        if method in _HTTP_METHODS
    }


def test_static_api_inventory_matches_runtime_except_deferred_routes() -> None:
    static = _static_openapi()
    runtime = create_app().openapi()

    assert _operations(static) - _DEFERRED_OPERATIONS == _operations(runtime)


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


def test_message_openapi_describes_user_input_and_ndjson_response() -> None:
    runtime = create_app().openapi()
    post = runtime["paths"]["/api/sessions/{session_id}/messages"]["post"]
    request = runtime["components"]["schemas"]["PostMessageRequest"]
    response_content = post["responses"]["200"]["content"]

    assert request["properties"]["content"]["items"]["$ref"].endswith(
        "/UserContentBlock"
    )
    assert request["properties"]["attachments"]["maxItems"] == 10
    assert request["anyOf"] == [
        {"properties": {"content": {"minItems": 1}}},
        {"properties": {"attachments": {"minItems": 1}}},
    ]
    assert set(response_content) == {"application/x-ndjson"}
    assert response_content["application/x-ndjson"]["schema"]["type"] == "string"


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
