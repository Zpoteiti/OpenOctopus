# ruff: noqa: F821

import sys

from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs, copy_metadata

is_win = sys.platform == "win32"

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
binaries = collect_dynamic_libs("onnxruntime")
if is_win:
    from PyInstaller.utils.hooks import collect_all

    winpty_datas, winpty_binaries, winpty_hiddenimports = collect_all("winpty")
    datas += winpty_datas
    binaries += winpty_binaries

hiddenimports = winpty_hiddenimports if is_win else ["openoctopus_client.pty_worker"]

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
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="openoctopus-client",
    console=True,
)
coll = COLLECT(exe, a.binaries, a.zipfiles, a.datas, name="openoctopus-client")
