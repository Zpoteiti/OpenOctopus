from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from openctopus_server.devices.protocol import (
    MAX_TEXT_FRAME_BYTES,
    PROTOCOL_VERSION,
    HelloAckFrame,
    encode_server_frame,
    frame_to_wire_dict,
    parse_client_frame,
    parse_server_frame,
)
from openctopus_server.devices.transfer import LATE_PROGRESS_MAX

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "protocol_v3" / "frames.json"
)
_LIMITS_PATH = _FIXTURE_PATH.with_name("limits.json")


def _fixtures() -> dict[str, list[dict[str, Any]]]:
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))


def test_shared_protocol_v3_golden_frames_round_trip() -> None:
    for case in _fixtures()["valid"]:
        payload = json.dumps(case["frame"], ensure_ascii=False, separators=(",", ":"))
        frame = (
            parse_client_frame(payload)
            if case["direction"] == "client_to_server"
            else parse_server_frame(payload)
        )
        assert frame_to_wire_dict(frame) == case["frame"]


def test_transfer_success_ack_metadata_shape_round_trips_from_client() -> None:
    case = next(
        case
        for case in _fixtures()["valid"]
        if case["direction"] == "client_to_server"
        and case["frame"].get("type") == "transfer_end"
        and case["frame"].get("id") == "0190d5a7-0000-7000-8000-000000000008"
        and case["frame"].get("created") is True
    )

    frame = parse_client_frame(json.dumps(case["frame"]))

    assert frame_to_wire_dict(frame) == case["frame"]
    assert case["frame"]["etag"] == "destination-fingerprint"


def test_shared_protocol_v3_invalid_frames_are_strictly_rejected() -> None:
    for case in _fixtures()["invalid"]:
        payload = json.dumps(case["frame"], ensure_ascii=False, separators=(",", ":"))
        parse = (
            parse_client_frame if case["direction"] == "client_to_server" else parse_server_frame
        )
        with pytest.raises((ValidationError, ValueError)):
            parse(payload)


def test_secret_bearing_config_is_safe_in_repr_but_exact_on_wire() -> None:
    case = next(case for case in _fixtures()["valid"] if case["frame"]["type"] == "hello_ack")
    frame = parse_server_frame(json.dumps(case["frame"]))

    assert isinstance(frame, HelloAckFrame)
    assert "fake-token" not in repr(frame)
    assert "fake-token" not in frame.model_dump_json()
    assert "fake-token" in json.dumps(frame_to_wire_dict(frame))
    assert json.loads(encode_server_frame(frame)) == case["frame"]


def test_both_parsers_enforce_the_12_mib_text_frame_limit_before_json() -> None:
    payload = " " * (MAX_TEXT_FRAME_BYTES + 1)

    with pytest.raises(ValueError, match="maximum size"):
        parse_client_frame(payload)
    with pytest.raises(ValueError, match="maximum size"):
        parse_server_frame(payload)


def test_protocol_version_and_late_progress_limit_are_shared_contracts() -> None:
    limits = cast(dict[str, int], json.loads(_LIMITS_PATH.read_text(encoding="utf-8")))

    assert PROTOCOL_VERSION == "3"
    assert LATE_PROGRESS_MAX == limits["late_progress_max"] == 64
