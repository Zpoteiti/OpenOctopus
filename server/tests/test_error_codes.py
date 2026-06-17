import json
from pathlib import Path

from openctopus_server.errors.codes import ErrorCode


def test_error_codes_match_snapshot():
    snapshot_path = Path(__file__).parent / "snapshots" / "error_codes.json"
    snapshot = json.loads(snapshot_path.read_text())
    current = {e.name: e.value for e in ErrorCode}
    assert current == snapshot


def test_all_exception_classes_exist():
    from openctopus_server.errors.exceptions import (
        AuthError,
        McpError,
        NetworkError,
        OpenOctopusError,
        ProtocolError,
        ToolError,
        WorkspaceError,
    )

    assert issubclass(WorkspaceError, OpenOctopusError)
    assert issubclass(ToolError, OpenOctopusError)
    assert issubclass(NetworkError, OpenOctopusError)
    assert issubclass(ProtocolError, OpenOctopusError)
    assert issubclass(McpError, OpenOctopusError)
    assert issubclass(AuthError, OpenOctopusError)
