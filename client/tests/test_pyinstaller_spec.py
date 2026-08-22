from __future__ import annotations

import runpy
import sys
import tempfile
import types
import unittest
from collections.abc import Collection
from pathlib import Path
from unittest.mock import patch

SPEC_PATH = Path(__file__).resolve().parents[1] / "openoctopus_client.spec"
WINPTY_NATIVE_FILES = frozenset(
    {
        "_winpty.cp312-win_amd64.pyd",
        "conpty.dll",
        "OpenConsole.exe",
        "winpty.dll",
        "winpty-agent.exe",
    }
)


def _run_windows_spec(native_files: Collection[str]) -> None:
    hooks = types.ModuleType("PyInstaller.utils.hooks")
    hooks.collect_data_files = lambda *args, **kwargs: []  # type: ignore[attr-defined]
    hooks.collect_dynamic_libs = lambda *args, **kwargs: []  # type: ignore[attr-defined]
    hooks.copy_metadata = lambda *args, **kwargs: []  # type: ignore[attr-defined]

    def collect_all(package: str) -> tuple[list[tuple[str, str]], list[tuple[str, str]], list[str]]:
        assert package == "winpty"
        datas = [
            (f"C:/site-packages/winpty/{name}", "winpty")
            for name in native_files
            if name.lower().endswith(".exe")
        ]
        binaries = [
            (f"C:/site-packages/winpty/{name}", "winpty")
            for name in native_files
            if name.lower().endswith(".dll")
        ]
        return datas, binaries, ["winpty._winpty"]

    hooks.collect_all = collect_all  # type: ignore[attr-defined]
    pyinstaller = types.ModuleType("PyInstaller")
    utils = types.ModuleType("PyInstaller.utils")

    class FakeAnalysis:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self.binaries = [
                (f"winpty/{name}", f"C:/site-packages/winpty/{name}", "EXTENSION")
                for name in native_files
                if name.lower().endswith(".pyd")
            ]
            self.datas = [
                (f"winpty/{name}", f"C:/site-packages/winpty/{name}", "DATA")
                for name in native_files
                if not name.lower().endswith(".pyd")
            ]
            self.pure: list[object] = []
            self.scripts: list[object] = []
            self.zipfiles: list[object] = []

    fake_globals = {
        "Analysis": FakeAnalysis,
        "PYZ": lambda *args, **kwargs: object(),
        "EXE": lambda *args, **kwargs: object(),
        "COLLECT": lambda *args, **kwargs: object(),
    }
    fake_modules = {
        "PyInstaller": pyinstaller,
        "PyInstaller.utils": utils,
        "PyInstaller.utils.hooks": hooks,
    }
    with (
        patch.dict(sys.modules, fake_modules),
        patch.object(sys, "platform", "win32"),
    ):
        runpy.run_path(str(SPEC_PATH), init_globals=fake_globals)


class PyInstallerSpecTests(unittest.TestCase):
    def test_windows_spec_rejects_each_missing_winpty_native_file(self) -> None:
        for missing in WINPTY_NATIVE_FILES:
            with self.subTest(missing=missing):
                with self.assertRaises(RuntimeError):
                    _run_windows_spec(WINPTY_NATIVE_FILES - {missing})

    def test_windows_spec_accepts_complete_winpty_native_bundle(self) -> None:
        _run_windows_spec(WINPTY_NATIVE_FILES)


class FrozenRuntimeSmokeTests(unittest.TestCase):
    @staticmethod
    def _bundle(native_files: Collection[str]) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name)
        binary = root / "openoctopus-client.exe"
        binary.touch()
        native_root = root / "_internal" / "winpty"
        native_root.mkdir(parents=True)
        for name in native_files:
            (native_root / name).touch()
        return temporary, binary

    def test_runtime_smoke_rejects_each_missing_winpty_native_file(self) -> None:
        from frozen_runtime_smoke import SmokeError, _assert_winpty_native_bundle

        for missing in WINPTY_NATIVE_FILES:
            with self.subTest(missing=missing):
                temporary, binary = self._bundle(WINPTY_NATIVE_FILES - {missing})
                with temporary, self.assertRaises(SmokeError):
                    _assert_winpty_native_bundle(binary)

    def test_runtime_smoke_accepts_complete_winpty_native_bundle(self) -> None:
        from frozen_runtime_smoke import _assert_winpty_native_bundle

        temporary, binary = self._bundle(WINPTY_NATIVE_FILES)
        with temporary:
            _assert_winpty_native_bundle(binary)


if __name__ == "__main__":
    unittest.main()
