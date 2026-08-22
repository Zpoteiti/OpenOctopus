from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from openoctopus_client.protocol import (
    CONTROL_FRAME_MAX_BYTES,
    HelloAck,
    ProtocolError,
    decode_client_frame,
    decode_server_frame,
    frame_to_wire_dict,
)

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "protocol_v3" / "frames.json"
)


def _fixtures() -> dict[str, list[dict[str, Any]]]:
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))


def test_shared_protocol_v3_golden_frames_round_trip() -> None:
    for case in _fixtures()["valid"]:
        payload = json.dumps(case["frame"], ensure_ascii=False, separators=(",", ":"))
        frame = (
            decode_client_frame(payload)
            if case["direction"] == "client_to_server"
            else decode_server_frame(payload)
        )
        assert frame_to_wire_dict(frame) == case["frame"]


def test_shared_protocol_v3_invalid_frames_are_strictly_rejected() -> None:
    for case in _fixtures()["invalid"]:
        payload = json.dumps(case["frame"], ensure_ascii=False, separators=(",", ":"))
        decode = (
            decode_client_frame if case["direction"] == "client_to_server" else decode_server_frame
        )
        with pytest.raises(ProtocolError):
            decode(payload)


def test_secret_bearing_config_is_safe_in_repr_but_exact_on_wire() -> None:
    case = next(case for case in _fixtures()["valid"] if case["frame"]["type"] == "hello_ack")
    frame = decode_server_frame(json.dumps(case["frame"]))

    assert isinstance(frame, HelloAck)
    assert "fake-token" not in repr(frame)
    assert "fake-token" not in frame.model_dump_json()
    assert "fake-token" in json.dumps(frame_to_wire_dict(frame))


def test_both_decoders_enforce_the_12_mib_text_frame_limit_before_json() -> None:
    payload = " " * (CONTROL_FRAME_MAX_BYTES + 1)

    with pytest.raises(ProtocolError, match="maximum size"):
        decode_client_frame(payload)
    with pytest.raises(ProtocolError, match="maximum size"):
        decode_server_frame(payload)
