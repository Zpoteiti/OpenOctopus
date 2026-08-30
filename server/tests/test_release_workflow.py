from pathlib import Path

import yaml


def test_server_release_images_are_verified_on_native_runners() -> None:
    workflow_path = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "release.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    job = workflow["jobs"]["server-verify"]

    assert job["runs-on"] == "${{ matrix.runner }}"
    assert job["strategy"]["matrix"]["include"] == [
        {"arch": "amd64", "runner": "ubuntu-24.04"},
        {"arch": "arm64", "runner": "ubuntu-24.04-arm"},
    ]
    assert all(
        step.get("uses") != "docker/setup-qemu-action@v3" for step in job["steps"]
    )
