"""Per-user login autostart, built only on the OS's own mechanisms.

Ubuntu writes a user XDG autostart ``.desktop`` entry pointing at the
installed program (with a target-existence check), Windows writes a current
user ``HKCU`` Run value with the absolute installed path, and macOS writes a
user LaunchAgent that starts the app at graphical login without a persistent
restart policy.  Startup entries never carry secrets; the tray reads its own
saved configuration after login.  The checked state always follows the real
OS entry, not a private flag.
"""

from __future__ import annotations

import importlib
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

# ``winreg`` only exists on Windows; the cast keeps static analysis quiet on
# the POSIX builders while the dispatch below still runs it natively there.
_winreg: Any = (
    cast(Any, importlib.import_module("winreg")) if sys.platform == "win32" else None
)

APP_ID = "dev.openoctopus.client"
_AUTOSTART_FILE_NAME = "openoctopus-client.desktop"
_WINDOWS_RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
_WINDOWS_VALUE_NAME = "OpenOctopus Client"


class AutostartError(RuntimeError):
    """The autostart entry could not be read or changed."""


def default_launch_command() -> list[str]:
    """The absolute command that a login session should execute."""

    if getattr(sys, "frozen", False):
        return [sys.executable]
    return [sys.executable, "-m", "openoctopus_client"]


def default_autostart_directory() -> Path:
    config_home = os.environ.get("XDG_CONFIG_HOME")
    base = Path(config_home) if config_home else Path.home() / ".config"
    return base / "autostart"


class AutostartController:
    def __init__(
        self,
        *,
        launch_command: list[str] | None = None,
        autostart_directory: Path | None = None,
    ) -> None:
        self._command = list(launch_command) if launch_command else default_launch_command()
        self._directory = autostart_directory

    # -- Public, platform-dispatching surface ----------------------------------

    def is_enabled(self) -> bool:
        if sys.platform == "win32":
            return self._windows_is_enabled()
        if sys.platform == "darwin":
            return self._macos_is_enabled()
        return self._xdg_is_enabled()

    def enable(self) -> None:
        if sys.platform == "win32":
            self._windows_enable()
            return
        if sys.platform == "darwin":
            self._macos_enable()
            return
        self._xdg_enable()

    def disable(self) -> None:
        if sys.platform == "win32":
            self._windows_disable()
            return
        if sys.platform == "darwin":
            self._macos_disable()
            return
        self._xdg_disable()

    # -- XDG autostart (Linux) ---------------------------------------------------

    def _desktop_path(self) -> Path:
        directory = (
            self._directory
            if self._directory is not None
            else default_autostart_directory()
        )
        return directory / _AUTOSTART_FILE_NAME

    def _desktop_contents(self) -> str:
        executable = " ".join(shlex.quote(part) for part in self._command)
        return (
            "[Desktop Entry]\n"
            "Type=Application\n"
            "Name=OpenOctopus Client\n"
            "Comment=OpenOctopus tray client\n"
            f"Exec={executable}\n"
            "Terminal=false\n"
            "X-GNOME-Autostart-Destination=xdg\n"
        )

    def _target_exists(self) -> bool:
        target = self._command[0]
        return os.path.isfile(target) and os.access(target, os.X_OK)

    def _xdg_is_enabled(self) -> bool:
        path = self._desktop_path()
        try:
            text = path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError, UnicodeError):
            return False
        if "Hidden=true" in text:
            return False
        if f"Exec={' '.join(shlex.quote(part) for part in self._command)}" not in text:
            return False
        return self._target_exists()

    def _xdg_enable(self) -> None:
        if not self._target_exists():
            raise AutostartError("the installed program target does not exist")
        path = self._desktop_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".desktop.tmp")
            temporary.write_text(self._desktop_contents(), encoding="utf-8")
            os.replace(temporary, path)
        except OSError as exc:
            raise AutostartError("autostart entry could not be written") from exc

    def _xdg_disable(self) -> None:
        path = self._desktop_path()
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            raise AutostartError("autostart entry could not be removed") from exc

    # -- Windows (unverified on this Linux devbox) --------------------------------

    def _windows_is_enabled(self) -> bool:
        try:
            with _winreg.OpenKey(_winreg.HKEY_CURRENT_USER, _WINDOWS_RUN_KEY) as key:
                value, _ = _winreg.QueryValueEx(key, _WINDOWS_VALUE_NAME)
        except OSError:
            return False
        return str(value).strip('"').lower() == self._command[0].lower()

    def _windows_enable(self) -> None:
        if not os.path.isfile(self._command[0]):
            raise AutostartError("the installed program target does not exist")
        try:
            with _winreg.OpenKey(
                _winreg.HKEY_CURRENT_USER, _WINDOWS_RUN_KEY, 0, _winreg.KEY_SET_VALUE
            ) as key:
                value = f'"{self._command[0]}"'
                _winreg.SetValueEx(key, _WINDOWS_VALUE_NAME, 0, _winreg.REG_SZ, value)
        except OSError as exc:
            raise AutostartError("autostart entry could not be written") from exc

    def _windows_disable(self) -> None:
        try:
            with _winreg.OpenKey(
                _winreg.HKEY_CURRENT_USER, _WINDOWS_RUN_KEY, 0, _winreg.KEY_SET_VALUE
            ) as key:
                _winreg.DeleteValue(key, _WINDOWS_VALUE_NAME)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise AutostartError("autostart entry could not be removed") from exc

    # -- macOS LaunchAgent (unverified on this Linux devbox) ------------------------

    def _launch_agent_path(self) -> Path:
        if self._directory is not None:
            return self._directory / f"{APP_ID}.plist"
        return Path.home() / "Library" / "LaunchAgents" / f"{APP_ID}.plist"

    def _macos_plist(self) -> str:
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" "
            "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n"
            "<plist version=\"1.0\">\n"
            "<dict>\n"
            f"  <key>Label</key><string>{APP_ID}</string>\n"
            "  <key>ProgramArguments</key>\n"
            "  <array>\n"
            + "".join(f"    <string>{part}</string>\n" for part in self._command)
            + "  </array>\n"
            "  <key>RunAtLoad</key><true/>\n"
            "  <key>KeepAlive</key><false/>\n"
            "</dict>\n"
            "</plist>\n"
        )

    def _macos_is_enabled(self) -> bool:
        path = self._launch_agent_path()
        try:
            text = path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError, UnicodeError):
            return False
        return f"<string>{self._command[0]}</string>" in text and self._target_exists()

    def _macos_enable(self) -> None:
        if not self._target_exists():
            raise AutostartError("the installed program target does not exist")
        path = self._launch_agent_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(self._macos_plist(), encoding="utf-8")
        except OSError as exc:
            raise AutostartError("LaunchAgent could not be written") from exc
        self._run_launchctl(["bootstrap", f"gui/{os.getuid()}", str(path)])

    def _macos_disable(self) -> None:
        self._run_launchctl(["bootout", f"gui/{os.getuid()}/{APP_ID}"])
        try:
            self._launch_agent_path().unlink(missing_ok=True)
        except OSError as exc:
            raise AutostartError("LaunchAgent could not be removed") from exc

    @staticmethod
    def _run_launchctl(arguments: list[str]) -> None:
        try:
            completed = subprocess.run(
                ["launchctl", *arguments],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise AutostartError("launchctl could not run") from exc
        if completed.returncode != 0:
            raise AutostartError("launchctl rejected the LaunchAgent change")
