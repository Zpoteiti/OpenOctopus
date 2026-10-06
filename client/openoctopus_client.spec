# ruff: noqa: F821

import sys
import tomllib

from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs, copy_metadata

is_win = sys.platform == "win32"
with open("pyproject.toml", "rb") as project_file:
    version = tomllib.load(project_file)["project"]["version"]

_WINPTY_NATIVE_FILES = frozenset(
    {
        "conpty.dll",
        "openconsole.exe",
        "winpty.dll",
        "winpty-agent.exe",
    }
)


def _assert_winpty_native_files(entries):
    names = {
        str(entry[0]).replace("\\", "/").rsplit("/", 1)[-1].casefold()
        for entry in entries
    }
    missing = sorted(_WINPTY_NATIVE_FILES - names)
    if not any(name.startswith("_winpty.") and name.endswith(".pyd") for name in names):
        missing.append("_winpty.*.pyd")
    if missing:
        raise RuntimeError(
            "pywinpty native files are missing from the frozen build: " + ", ".join(missing)
        )

datas = collect_data_files("magika", includes=["config/**", "models/**"])
datas += copy_metadata("fastmcp-slim")
# The tray reads credentials through the pinned keyring backends; their
# metadata keeps entry-point discovery working inside the frozen bundle.
datas += copy_metadata("keyring")
binaries = collect_dynamic_libs("onnxruntime")
if is_win:
    from PyInstaller.utils.hooks import collect_all

    winpty_datas, winpty_binaries, winpty_hiddenimports = collect_all("winpty")
    datas += winpty_datas
    binaries += winpty_binaries

hiddenimports = winpty_hiddenimports if is_win else ["openoctopus_client.pty_worker"]
hiddenimports += [
    "keyring.backends.SecretService",
    "keyring.backends.Windows",
    "keyring.backends.macOS",
]

a = Analysis(
    ["src/openoctopus_client/__main__.py"],
    pathex=["src"],
    binaries=binaries,
    datas=datas,
    excludes=["_pytest", "mypy", "psutil", "pytest", "ruff"],
    hiddenimports=hiddenimports,
)
if is_win:
    _assert_winpty_native_files([*a.binaries, *a.datas])
pyz = PYZ(a.pure)
# The windowed Windows bootloader has no Python standard streams. Keep a
# separate console-enabled core for the private pipe and conversion workers;
# QProcess launches it with redirected handles and no console window.
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="openoctopus-client",
    console=False,
)
core = EXE(
    pyz, a.scripts, [], exclude_binaries=True, name="openoctopus-core", console=True,
)
coll = COLLECT(exe, core, a.binaries, a.zipfiles, a.datas, name="openoctopus-client")
if sys.platform == "darwin":
    app = BUNDLE(
        coll,
        name="OpenOctopus Client.app",
        version=version,
        bundle_identifier="dev.openoctopus.client",
        info_plist={"LSUIElement": True, "NSHighResolutionCapable": True},
    )
