from __future__ import annotations

import os
from pathlib import Path

import pytest
from PySide6.QtCore import QEventLoop, QTimer
from PySide6.QtWidgets import QApplication

from openoctopus_client.gui.single_instance import (
    SingleInstance,
    _prepare_socket_directory,
    _socket_name,
    send_activation,
)


def _drain(milliseconds: int) -> None:
    loop = QEventLoop()
    QTimer.singleShot(milliseconds, loop.quit)
    loop.exec()


@pytest.mark.parametrize("nested", [False, True])
def test_first_instance_owns_and_second_activates_it(
    qapp: QApplication, tmp_path: Path, nested: bool
) -> None:
    if nested:
        tmp_path = tmp_path / ("nested-" * 20)
    first = SingleInstance(tmp_path)
    assert first.try_become_primary() is True
    received: list[bool] = []
    first.activate_requested.connect(lambda: received.append(True))

    second = SingleInstance(tmp_path)
    assert second.try_become_primary() is False

    deadline = 30
    while not received and deadline > 0:
        _drain(100)
        deadline -= 1
    assert received == [True], "the running instance was not activated"
    first.release()


def test_stale_socket_is_reclaimed(qapp: QApplication, tmp_path: Path) -> None:
    if os.name != "nt":
        name = _socket_name(tmp_path)
        _prepare_socket_directory(name)
        Path(name).write_bytes(b"stale")
    instance = SingleInstance(tmp_path)
    assert instance.try_become_primary() is True
    instance.release()


def test_release_allows_the_next_launch(qapp: QApplication, tmp_path: Path) -> None:
    first = SingleInstance(tmp_path)
    assert first.try_become_primary() is True
    first.release()
    second = SingleInstance(tmp_path)
    assert second.try_become_primary() is True
    second.release()


def test_send_activation_without_an_instance_is_advisory(
    qapp: QApplication, tmp_path: Path
) -> None:
    assert send_activation(tmp_path) is False
