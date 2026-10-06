#!/usr/bin/env python3
"""Build a per-user Windows installer (NSIS) around the PyInstaller bundle.

Usage: build_windows_installer.py <path-to-onedir-dir> <version> [output-dir]

Requires NSIS (makensis) on PATH.  The installer installs the one-folder
bundle under %LOCALAPPDATA%\\Programs\\OpenOctopusClient (no admin rights),
adds a Start Menu shortcut and an uninstaller entry. Login autostart is off
until the user enables it in the tray.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

APP = "openoctopus-client"
NAME = "OpenOctopusClient"


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    bundle = Path(argv[1]).resolve()
    version = argv[2]
    out = Path(argv[3] if len(argv) > 3 else "dist-installer").resolve()
    exe = bundle / f"{APP}.exe"
    if not exe.is_file():
        print(f"bundle is missing the executable {exe}", file=sys.stderr)
        return 2
    parts = version.split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        print("version must be MAJOR.MINOR.PATCH", file=sys.stderr)
        return 2
    out.mkdir(parents=True, exist_ok=True)
    script = _nsis_script(bundle, version, out)
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "installer.nsi"
        path.write_text(script, encoding="utf-8")
        result = subprocess.run(
            ["makensis", str(path)],  # noqa: S603 - fixed build tool
            check=False,
        )
    if result.returncode != 0:
        print("makensis failed", file=sys.stderr)
        return result.returncode
    produced = out / f"{NAME}-Setup-{version}-per-user.exe"
    if not produced.is_file():
        print(f"NSIS did not produce {produced}", file=sys.stderr)
        return 1
    print(str(produced))
    return 0


def _nsis_script(bundle: Path, version: str, out: Path) -> str:
    produced = out / f"{NAME}-Setup-{version}-per-user.exe"
    return f"""!include "MUI2.nsh"
!include "FileFunc.nsh"

Name "OpenOctopus Client {version}"
OutFile "{produced}"
InstallDir "$LOCALAPPDATA\\Programs\\{NAME}"
InstallDirRegKey HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{NAME}" \
  "InstallLocation"
RequestExecutionLevel user
Unicode True

!define MUI_ABORTWARNING
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_LANGUAGE "English"

!macro RequireClosed filename label
  IfFileExists "$INSTDIR\\${{filename}}" 0 ${{label}}_done
  ClearErrors
  FileOpen $0 "$INSTDIR\\${{filename}}" a
  IfErrors 0 ${{label}}_close
  MessageBox MB_OK|MB_ICONEXCLAMATION "Please quit OpenOctopus Client before continuing." /SD IDOK
  Abort
  ${{label}}_close:
  FileClose $0
  ${{label}}_done:
!macroend

Section "Install"
  SetShellVarContext current
  !insertmacro RequireClosed "{APP}.exe" install_gui
  !insertmacro RequireClosed "openoctopus-core.exe" install_core
  SetOutPath "$INSTDIR"
  File /r "{bundle}\\*.*"
  WriteRegStr HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{NAME}" \
    "DisplayName" "OpenOctopus Client"
  WriteRegStr HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{NAME}" \
    "DisplayVersion" "{version}"
  WriteRegStr HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{NAME}" \
    "InstallLocation" "$INSTDIR"
  WriteRegStr HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{NAME}" \
    "UninstallString" '"$INSTDIR\\Uninstall.exe"'
  WriteRegDWORD HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{NAME}" \
    "NoModify" 1
  WriteRegDWORD HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{NAME}" \
    "NoRepair" 1
  CreateDirectory "$SMPROGRAMS\\OpenOctopus Client"
  CreateShortcut "$SMPROGRAMS\\OpenOctopus Client\\OpenOctopus Client.lnk" "$INSTDIR\\{APP}.exe"
  WriteUninstaller "$INSTDIR\\Uninstall.exe"
SectionEnd

Section "Uninstall"
  SetShellVarContext current
  !insertmacro RequireClosed "{APP}.exe" uninstall_gui
  !insertmacro RequireClosed "openoctopus-core.exe" uninstall_core
  DeleteRegValue HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Run" "OpenOctopus Client"
  Delete "$SMPROGRAMS\\OpenOctopus Client\\OpenOctopus Client.lnk"
  RMDir "$SMPROGRAMS\\OpenOctopus Client"
  DeleteRegKey HKCU "Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\{NAME}"
  RMDir /r "$INSTDIR"
SectionEnd
"""


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
