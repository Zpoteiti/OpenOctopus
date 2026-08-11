from __future__ import annotations

import json
from pathlib import Path

from frozen_runtime_smoke import _runtime_smoke_payload


def test_runtime_smoke_payload_includes_cli_and_child_process_metrics(tmp_path: Path) -> None:
    payload = _runtime_smoke_payload(
        bundle=tmp_path / "openoctopus-client",
        version_seconds=0.1,
        version_peak_rss=10,
        version_peak_processes=1,
        run_seconds=0.2,
        run_peak_rss=20,
        run_peak_processes=1,
        conversion_seconds=0.3,
        conversion_peak_rss=30,
        conversion_peak_processes=2,
    )

    assert json.loads(json.dumps(payload)) == {
        "bundle_bytes": 0,
        "conversion_child": {
            "seconds": 0.3,
            "sampled_process_tree_peak_processes": 2,
            "sampled_process_tree_peak_rss_bytes": 30,
        },
        "run_cli": {
            "seconds": 0.2,
            "sampled_process_tree_peak_processes": 1,
            "sampled_process_tree_peak_rss_bytes": 20,
        },
        "version": {
            "seconds": 0.1,
            "sampled_process_tree_peak_processes": 1,
            "sampled_process_tree_peak_rss_bytes": 10,
        },
    }
