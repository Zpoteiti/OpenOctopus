from __future__ import annotations

import os
from pathlib import Path

import pytest

from openoctopus_client.tools import paths as paths_module
from openoctopus_client.tools.common import ToolFailure
from openoctopus_client.tools.paths import WorkspacePaths


def test_relative_and_home_paths_use_the_documented_roots(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    home = tmp_path / "home"
    workspace = home / "workspace"
    workspace.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: home)
    paths = WorkspacePaths(workspace, restrict_to_workspace=True)

    assert paths.resolve("notes/file.txt") == workspace / "notes/file.txt"
    assert paths.resolve("~/workspace/notes/file.txt") == workspace / "notes/file.txt"
    assert paths.resolve(r"~\workspace/notes/file.txt") == workspace / "notes/file.txt"
    assert paths.canonical(workspace / "notes/file.txt") == "~/workspace/notes/file.txt"


def test_home_path_is_checked_after_expansion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    home = tmp_path / "home"
    workspace = home / "workspace"
    outside = home / "outside"
    workspace.mkdir(parents=True)
    outside.mkdir()
    monkeypatch.setattr(Path, "home", lambda: home)

    restricted = WorkspacePaths(workspace, restrict_to_workspace=True)
    trusted = WorkspacePaths(workspace, restrict_to_workspace=False)

    with pytest.raises(ToolFailure) as exc_info:
        restricted.resolve("~/outside/file.txt")
    assert exc_info.value.code == "tool_path_outside_workspace"
    assert trusted.resolve("~/outside/file.txt") == outside / "file.txt"


def test_home_expansion_keeps_repeated_separators_under_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    home = tmp_path / "home"
    workspace = home / "workspace"
    home.mkdir()
    workspace.mkdir()
    monkeypatch.setattr(Path, "home", lambda: home)
    paths = WorkspacePaths(workspace, restrict_to_workspace=False)

    assert paths.resolve("~//docs/file.txt") == home / "docs/file.txt"
    assert paths.resolve(r"~\\docs/file.txt") == home / "docs/file.txt"


def test_missing_path_is_normalized_before_the_workspace_check(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    outside = tmp_path / "outside"
    workspace.mkdir()
    outside.mkdir()
    paths = WorkspacePaths(workspace, restrict_to_workspace=True)

    with pytest.raises(ToolFailure) as exc_info:
        paths.resolve("missing/../../outside/file.txt")
    assert exc_info.value.code == "tool_path_outside_workspace"

    assert paths.resolve("missing/../file.txt") == workspace / "file.txt"


def test_native_absolute_path_is_checked_against_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    outside = tmp_path / "outside"
    workspace.mkdir()
    outside.mkdir()

    restricted = WorkspacePaths(workspace, restrict_to_workspace=True)
    trusted = WorkspacePaths(workspace, restrict_to_workspace=False)

    with pytest.raises(ToolFailure) as exc_info:
        restricted.resolve(str(outside / "file.txt"))
    assert exc_info.value.code == "tool_path_outside_workspace"
    assert trusted.resolve(str(outside / "file.txt")) == outside / "file.txt"


@pytest.mark.skipif(os.name == "nt", reason="POSIX path semantics")
def test_posix_double_slash_absolute_path_is_not_treated_as_windows_unc(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    outside = tmp_path / "outside"
    workspace.mkdir()
    outside.mkdir()
    paths = WorkspacePaths(workspace, restrict_to_workspace=False)
    double_slash = f"//{str(outside / 'file.txt').lstrip('/')}"

    assert paths.resolve(double_slash) == outside / "file.txt"


def test_paths_over_the_public_limit_are_rejected(tmp_path: Path) -> None:
    paths = WorkspacePaths(tmp_path, restrict_to_workspace=False)

    with pytest.raises(ToolFailure) as exc_info:
        paths.resolve("a" * 4097)
    assert exc_info.value.code == "tool_path_outside_workspace"


@pytest.mark.parametrize(
    ("value", "ambiguous"),
    [
        (r"C:\workspace\file.txt", False),
        (r"C:/workspace/file.txt", False),
        (r"\\server\share\file.txt", False),
        (r"C:workspace\file.txt", True),
        (r"\workspace\file.txt", True),
        ("/workspace/file.txt", True),
    ],
)
def test_windows_path_classifier_rejects_only_ambiguous_native_forms(
    value: str, ambiguous: bool
) -> None:
    assert paths_module._is_ambiguous_windows_path(value) is ambiguous


@pytest.mark.skipif(os.name != "nt", reason="Windows path semantics")
@pytest.mark.parametrize("value", [r"C:workspace\file.txt", r"\workspace\file.txt", "/file.txt"])
def test_windows_client_rejects_drive_and_root_relative_paths(tmp_path: Path, value: str) -> None:
    paths = WorkspacePaths(tmp_path, restrict_to_workspace=False)

    with pytest.raises(ToolFailure) as exc_info:
        paths.resolve(value)
    assert exc_info.value.code == "tool_path_outside_workspace"
