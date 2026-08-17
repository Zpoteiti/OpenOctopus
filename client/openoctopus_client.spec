import sys

from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

is_win = sys.platform == "win32"

datas = collect_data_files("magika", includes=["config/**", "models/**"])
binaries = collect_dynamic_libs("onnxruntime")
if is_win:
    from PyInstaller.utils.hooks import collect_all

    winpty_datas, winpty_binaries, winpty_hiddenimports = collect_all("winpty")
    datas += winpty_datas
    binaries += winpty_binaries
    if not winpty_binaries:
        raise RuntimeError("pywinpty native binaries are missing from the frozen build")

hiddenimports = winpty_hiddenimports if is_win else ["openoctopus_client.pty_worker"]

a = Analysis(
    ["src/openoctopus_client/__main__.py"],
    pathex=["src"],
    binaries=binaries,
    datas=datas,
    excludes=["_pytest", "mypy", "psutil", "pytest", "ruff"],
    hiddenimports=hiddenimports,
)
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
