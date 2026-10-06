from __future__ import annotations

import os
from pathlib import Path

import pytest

from openoctopus_client.gui.autostart import AutostartController, AutostartError


@pytest.fixture(autouse=True)
def xdg_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    # Unit tests own only temporary files, including on Windows/macOS runners.
    monkeypatch.setattr("openoctopus_client.gui.autostart.sys.platform", "linux")


def _executable(tmp_path: Path) -> Path:
    target = tmp_path / "openoctopus-client"
    target.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    target.chmod(0o755)
    return target


def test_disabled_until_enabled(tmp_path: Path) -> None:
    controller = AutostartController(
        launch_command=[str(_executable(tmp_path))],
        autostart_directory=tmp_path / "autostart",
    )
    assert controller.is_enabled() is False
    controller.enable()
    assert controller.is_enabled() is True
    entry = (tmp_path / "autostart" / "openoctopus-client.desktop").read_text(
        encoding="utf-8"
    )
    assert "Hidden=true" not in entry
    controller.disable()
    assert controller.is_enabled() is False


def test_enable_refuses_a_missing_target(tmp_path: Path) -> None:
    controller = AutostartController(
        launch_command=[str(tmp_path / "does-not-exist")],
        autostart_directory=tmp_path / "autostart",
    )
    with pytest.raises(AutostartError):
        controller.enable()
    assert controller.is_enabled() is False


def test_a_deleted_target_disables_the_entry(tmp_path: Path) -> None:
    if os.name == "nt":
        pytest.skip("POSIX execute permissions")
    target = _executable(tmp_path)
    controller = AutostartController(
        launch_command=[str(target)],
        autostart_directory=tmp_path / "autostart",
    )
    controller.enable()
    assert controller.is_enabled() is True
    target.chmod(0o644)
    assert controller.is_enabled() is False


def test_repeated_toggles_follow_the_real_entry(tmp_path: Path) -> None:
    controller = AutostartController(
        launch_command=[str(_executable(tmp_path))],
        autostart_directory=tmp_path / "autostart",
    )
    for _ in range(3):
        controller.enable()
        controller.enable()
        assert controller.is_enabled() is True
        controller.disable()
        controller.disable()
        assert controller.is_enabled() is False
