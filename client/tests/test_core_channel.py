from __future__ import annotations

import pytest

from openoctopus_client.core_channel import (
    MAX_MESSAGE_BYTES,
    ChannelError,
    CoreFailureCode,
    CoreState,
    ExitResultMessage,
    StartupConfigMessage,
    StatusEventMessage,
    StopCommandMessage,
    encode_message,
    parse_core_event,
    parse_gui_message,
)

_TOKEN = "openoctopus_dev_pipe-secret-value"


def _startup() -> StartupConfigMessage:
    return StartupConfigMessage(
        type="startup-config",
        generation=7,
        server_url="https://openoctopus.example",
        token=_TOKEN,
    )


def test_startup_round_trip_keeps_the_token_in_memory_only() -> None:
    line = encode_message(_startup())
    parsed = parse_gui_message(line + "")
    assert isinstance(parsed, StartupConfigMessage)
    assert parsed.token == _TOKEN
    assert _TOKEN not in repr(parsed)
    assert _TOKEN not in str(parsed)


def test_status_events_round_trip_and_reject_extras() -> None:
    event = StatusEventMessage(
        type="status",
        generation=1,
        state=CoreState.RECONNECTING,
        error_code=CoreFailureCode.SERVER_UNREACHABLE,
        retry_in_seconds=4.5,
        attempt=3,
    )
    parsed = parse_core_event(encode_message(event))
    assert parsed == event
    with pytest.raises(Exception):
        parse_core_event('{"type":"status","generation":1,"state":"online","token":"x"}')


def test_unknown_malformed_and_oversized_lines_are_rejected() -> None:
    with pytest.raises(ChannelError):
        parse_gui_message("not json")
    with pytest.raises(ChannelError):
        parse_gui_message('{"type":"exec","command":"rm -rf /"}')
    with pytest.raises(ChannelError):
        parse_gui_message(b"x" * (MAX_MESSAGE_BYTES + 1))
    with pytest.raises(ChannelError):
        parse_core_event('{"type":"status","generation":-1,"state":"online"}')


def test_stop_command_and_exit_result_round_trip() -> None:
    assert parse_gui_message('{"type":"stop"}') == StopCommandMessage(type="stop")
    exit_result = ExitResultMessage(
        type="exit",
        generation=2,
        return_code=0,
        cleanup_complete=True,
        reason="stopped",
    )
    assert parse_core_event(encode_message(exit_result)) == exit_result


def test_encode_bounds_are_enforced() -> None:
    oversized = _startup().model_copy(
        update={"token": _TOKEN + "a" * MAX_MESSAGE_BYTES}
    )
    with pytest.raises(ChannelError):
        encode_message(oversized)
