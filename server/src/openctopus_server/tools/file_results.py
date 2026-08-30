from __future__ import annotations

import json
from typing import Any

FILE_RESULT_MAX_OUTPUT_CHARS = 50_000
_FILE_RESULT_JSON_MAX_CHARS = 49_500

CLIENT_FILE_MUTATIONS = frozenset(
    {
        "write_file",
        "edit_file",
        "apply_patch",
        "delete_file",
        "delete_folder",
        "notebook_edit",
    }
)


def canonical_server_path(path: str) -> str:
    """Return a provider-reusable path in the Server Workspace namespace."""

    path = path.replace("\\", "/")
    if path == "~":
        return "~"
    if path.startswith("~/"):
        suffix = path[2:].lstrip("/")
        return "~" if not suffix else f"~/{suffix}"
    if path.startswith("/"):
        return path
    return f"~/{path}"


def file_mutation_result(
    operation: str,
    *,
    device: str,
    requested_path: str,
    canonical_path: str,
    **details: Any,
) -> str:
    payload: dict[str, Any] = {
        "ok": True,
        "operation": operation,
        "device": device,
        "requested_path": requested_path,
        "canonical_path": canonical_path,
        **details,
    }
    return _dump_bounded(payload)


def file_patch_result(
    *,
    device: str,
    dry_run: bool,
    edits: list[dict[str, Any]],
) -> str:
    """Return valid bounded JSON, omitting only trailing patch details if needed."""

    total_edits = len(edits)
    retained = list(edits)
    while True:
        payload: dict[str, Any] = {
            "ok": True,
            "operation": "apply_patch",
            "device": device,
            "dry_run": dry_run,
            "total_edits": total_edits,
            "edits": retained,
        }
        if len(retained) != total_edits:
            payload["omitted_edits"] = total_edits - len(retained)
        encoded = json.dumps(payload, ensure_ascii=False)
        if len(encoded) <= _FILE_RESULT_JSON_MAX_CHARS:
            return encoded
        if not retained:
            raise ValueError("patch summary cannot fit the structured result bound")
        retained.pop()


def file_transfer_result(
    *,
    mode: str,
    source_device: str,
    source_path: str,
    destination_device: str,
    destination_path: str,
    kind: str,
    files_transferred: int,
    bytes_transferred: int,
    sha256: str,
    warnings: tuple[str, ...],
) -> str:
    payload: dict[str, Any] = {
        "ok": True,
        "operation": "file_transfer",
        "mode": mode,
        "source": {
            "device": source_device,
            "requested_path": source_path,
            "canonical_path": (
                canonical_server_path(source_path) if source_device == "server" else source_path
            ),
        },
        "destination": {
            "device": destination_device,
            "requested_path": destination_path,
            "canonical_path": (
                canonical_server_path(destination_path)
                if destination_device == "server"
                else destination_path
            ),
        },
        "kind": kind,
        "files_transferred": files_transferred,
        "bytes_transferred": bytes_transferred,
        "sha256": sha256,
        "warnings": list(warnings),
    }
    encoded = json.dumps(payload, ensure_ascii=False)
    if len(encoded) <= _FILE_RESULT_JSON_MAX_CHARS:
        return encoded

    for endpoint in (payload["source"], payload["destination"]):
        endpoint.pop("requested_path")
        endpoint["requested_path_omitted"] = True
    encoded = json.dumps(payload, ensure_ascii=False)
    if len(encoded) > FILE_RESULT_MAX_OUTPUT_CHARS:
        raise ValueError("file transfer result exceeds the routed result bound")
    return encoded


def attach_device_to_client_file_result(
    operation: str,
    content: str,
    *,
    device: str,
) -> str:
    """Validate a Client mutation result and add trusted routing provenance."""

    if operation not in CLIENT_FILE_MUTATIONS:
        raise ValueError("operation is not a Client file mutation")
    payload = json.loads(content)
    if (
        not isinstance(payload, dict)
        or payload.get("ok") is not True
        or payload.get("operation") != operation
    ):
        raise ValueError("Client file mutation result is invalid")
    if operation == "apply_patch":
        edits = payload.get("edits")
        if not isinstance(edits, list):
            raise ValueError("Client patch result is invalid")
        for edit in edits:
            if not isinstance(edit, dict) or not _has_paths(edit):
                raise ValueError("Client patch edit result is invalid")
        if not edits and not _is_fully_omitted_patch(payload):
            raise ValueError("Client patch result is invalid")
    elif not _has_paths(payload):
        raise ValueError("Client file mutation paths are invalid")
    payload["device"] = device
    encoded = json.dumps(payload, ensure_ascii=False)
    if len(encoded) > FILE_RESULT_MAX_OUTPUT_CHARS:
        raise ValueError("Client file result exceeds the routed result bound")
    return encoded


def _has_paths(payload: dict[str, Any]) -> bool:
    return all(
        isinstance(payload.get(key), str) and bool(payload[key])
        for key in ("requested_path", "canonical_path")
    )


def _is_fully_omitted_patch(payload: dict[str, Any]) -> bool:
    total_edits = payload.get("total_edits")
    omitted_edits = payload.get("omitted_edits")
    return (
        isinstance(total_edits, int)
        and not isinstance(total_edits, bool)
        and total_edits > 0
        and omitted_edits == total_edits
    )


def _dump_bounded(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False)
    if len(encoded) > _FILE_RESULT_JSON_MAX_CHARS:
        raise ValueError("file result exceeds the structured result bound")
    return encoded
