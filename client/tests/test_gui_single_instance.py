from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QEventLoop, QTimer
from PySide6.QtWidgets import QApplication

from openoctopus_client.gui.single_instance import SingleInstance, send_activation


def _drain(milliseconds: int) -> None:
    loop = QEventLoop()
    QTimer.singleShot(milliseconds, loop.quit)
    loop.exec()


def test_first_instance_owns_and_second_activates_it(
    qapp: QApplication, tmp_path: Path
) -> None:
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
    (tmp_path / "tray-instance.sock").write_bytes(b"stale")
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
