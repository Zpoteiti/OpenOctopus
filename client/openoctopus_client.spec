from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

datas = collect_data_files("magika", includes=["config/**", "models/**"])
binaries = collect_dynamic_libs("onnxruntime")

a = Analysis(
    ["src/openoctopus_client/__main__.py"],
    pathex=["src"],
    binaries=binaries,
    datas=datas,
    excludes=["_pytest", "mypy", "psutil", "pytest", "ruff"],
    hiddenimports=[],
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
