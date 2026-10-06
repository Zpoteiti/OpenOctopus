from __future__ import annotations

import json
from pathlib import Path

from frozen_runtime_smoke import _runtime_smoke_payload


def test_runtime_smoke_payload_includes_pipe_and_tray_metrics(tmp_path: Path) -> None:
    payload = _runtime_smoke_payload(
        bundle=tmp_path / "openoctopus-client",
        version_seconds=0.1,
        version_peak_rss=10,
        version_peak_processes=1,
        core_pipe_seconds=0.2,
        exec_seconds=0.25,
        exec_peak_rss=25,
        exec_peak_processes=2,
        mcp_seconds=0.275,
        mcp_peak_rss=27,
        mcp_peak_processes=2,
        conversion_seconds=0.3,
        conversion_peak_rss=30,
        conversion_peak_processes=2,
        tray={"returncode": 0, "seconds": 3.0, "stderr": ""},
    )

    assert json.loads(json.dumps(payload)) == {
        "bundle_bytes": 0,
        "conversion_child": {
            "seconds": 0.3,
            "sampled_process_tree_peak_processes": 2,
            "sampled_process_tree_peak_rss_bytes": 30,
        },
        "core_pipe": {"seconds": 0.2},
        "exec_backends": {
            "seconds": 0.25,
            "sampled_process_tree_peak_processes": 2,
            "sampled_process_tree_peak_rss_bytes": 25,
        },
        "mcp_stdio": {
            "seconds": 0.275,
            "sampled_process_tree_peak_processes": 2,
            "sampled_process_tree_peak_rss_bytes": 27,
        },
        "tray_single_instance": {
            "returncode": 0,
            "seconds": 3.0,
            "stderr": "",
        },
        "version": {
            "seconds": 0.1,
            "sampled_process_tree_peak_processes": 1,
            "sampled_process_tree_peak_rss_bytes": 10,
        },
    }
