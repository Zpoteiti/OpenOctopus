"""Qt widgets require a QPA platform; CI and headless boxes use offscreen."""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication


@pytest.fixture(scope="session")
def qapp() -> QApplication:
    from typing import cast

    instance = QApplication.instance()
    return cast(QApplication, instance) if instance is not None else QApplication([])
