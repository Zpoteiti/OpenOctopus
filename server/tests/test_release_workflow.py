import os
import subprocess
import tempfile
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


def test_server_ci_runs_when_the_release_workflow_changes() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    workflow_path = repo_root / ".github" / "workflows" / "ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    classifier = workflow["jobs"]["changes"]["steps"][1]["run"]

    with tempfile.TemporaryDirectory() as temp_dir:
        repo = Path(temp_dir)
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        subprocess.run(
            ["git", "config", "user.email", "ci@test.invalid"], cwd=repo, check=True
        )
        subprocess.run(
            ["git", "config", "user.name", "CI Test"], cwd=repo, check=True
        )
        changed_file = repo / ".github" / "workflows" / "release.yml"
        changed_file.parent.mkdir(parents=True)
        changed_file.write_text("name: Release\n", encoding="utf-8")
        subprocess.run(["git", "add", "."], cwd=repo, check=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
        base_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True
        ).strip()
        changed_file.write_text("name: Updated Release\n", encoding="utf-8")
        subprocess.run(["git", "add", "."], cwd=repo, check=True)
        subprocess.run(["git", "commit", "-qm", "change release workflow"], cwd=repo, check=True)
        head_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True
        ).strip()

        outputs = repo / "github-output"
        runner_temp = repo / "runner-temp"
        runner_temp.mkdir()
        env = {
            **os.environ,
            "BASE_SHA": base_sha,
            "HEAD_SHA": head_sha,
            "EVENT_NAME": "push",
            "GITHUB_OUTPUT": str(outputs),
            "RUNNER_TEMP": str(runner_temp),
        }
        subprocess.run(
            ["bash", "-euo", "pipefail", "-c", classifier],
            cwd=repo,
            env=env,
            check=True,
        )

        results = dict(line.split("=", 1) for line in outputs.read_text().splitlines())
        assert results["server"] == "true"
