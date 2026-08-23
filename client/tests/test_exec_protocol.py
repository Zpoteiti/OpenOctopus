from __future__ import annotations

import json
from uuid import UUID

import pytest

from openoctopus_client.protocol import (
    DeviceConfig,
    Hello,
    ShellMetadata,
    ToolCall,
    encode_frame,
)

CALL_ID = UUID("0190d5a7-0000-7000-8000-000000000002")
CHAT_ID = UUID("00000000-0000-4000-8000-000000000003")


def test_protocol_v3_hello_advertises_shells_without_exec_capability() -> None:
    hello = Hello.new(
        client_version="0.0.1",
        operating_system="linux",
        shells=ShellMetadata(default="bash", available=["bash", "sh"]),
    )

    payload = json.loads(encode_frame(hello))

    assert payload == {
        "caps": {
            "file_transfer": ["send", "receive"],
            "http_relay": True,
            "shared_tools": True,
            "web_fetch": True,
        },
        "client_version": "0.0.1",
        "id": str(hello.id),
        "os": "linux",
        "shells": {"available": ["bash", "sh"], "default": "bash"},
        "type": "hello",
        "version": "3",
    }


def test_shell_metadata_requires_unique_nonempty_members_and_default() -> None:
    with pytest.raises(ValueError):
        ShellMetadata(default="bash", available=[])
    with pytest.raises(ValueError):
        ShellMetadata(default="bash", available=["sh"])
    with pytest.raises(ValueError):
        ShellMetadata(default="bash", available=["bash", "bash"])


def test_device_config_validates_exec_policy() -> None:
    config = DeviceConfig(
        workspace_path="/tmp/workspace",
        restrict_to_workspace=False,
        ssrf_denylist=[],
        shell_timeout_max=600,
        env_allowlist=["PATH", "HOME"],
    )
    assert config.shell_timeout_max == 600
    assert config.env_allowlist == ["PATH", "HOME"]

    with pytest.raises(ValueError):
        config.model_copy(update={"shell_timeout_max": 86401}).model_validate(
            config.model_copy(update={"shell_timeout_max": 86401}).model_dump(),
            strict=True,
        )
    with pytest.raises(ValueError):
        DeviceConfig(
            workspace_path="/tmp/workspace",
            restrict_to_workspace=False,
            ssrf_denylist=[],
            shell_timeout_max=600,
            env_allowlist=["PATH", "PATH"],
        )
    with pytest.raises(ValueError):
        DeviceConfig(
            workspace_path="/tmp/workspace",
            restrict_to_workspace=False,
            ssrf_denylist=[],
            shell_timeout_max=600,
            env_allowlist=["OPENOCTOPUS_DEVICE_TOKEN"],
        )


def test_device_config_strictly_rejects_legacy_sandbox_mode() -> None:
    with pytest.raises(ValueError):
        DeviceConfig.model_validate(
            {
                "workspace_path": "/tmp/workspace",
                "sandbox_mode": True,
                "ssrf_denylist": [],
                "shell_timeout_max": 600,
                "env_allowlist": ["PATH"],
            },
            strict=True,
        )


@pytest.mark.parametrize("name", ["exec", "write_stdin", "list_exec_sessions"])
def test_exec_tool_calls_require_hidden_chat_session_id(name: str) -> None:
    call = ToolCall(
        id=CALL_ID,
        name=name,
        args={},
        max_result_bytes=1024,
        chat_session_id=CHAT_ID,
    )
    assert call.chat_session_id == CHAT_ID

    with pytest.raises(ValueError):
        ToolCall(id=CALL_ID, name=name, args={}, max_result_bytes=1024)


def test_existing_device_calls_may_omit_chat_session_id() -> None:
    call = ToolCall(
        id=CALL_ID,
        name="read_file",
        args={"path": "notes.txt"},
        max_result_bytes=1024,
    )
    assert call.chat_session_id is None
