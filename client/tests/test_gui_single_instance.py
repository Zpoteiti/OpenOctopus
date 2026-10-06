from __future__ import annotations

import os
import subprocess
import sys
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

    # A Windows named-pipe server must keep processing events while its peer
    # connects and writes. Exercise real second-launch process ownership.
    second = subprocess.Popen(
        [sys.executable, "-c",
         "import sys; from pathlib import Path; "
         "from PySide6.QtCore import QCoreApplication; "
         "from openoctopus_client.gui.single_instance import SingleInstance; "
         "app=QCoreApplication([]); instance=SingleInstance(Path(sys.argv[1])); "
         "sys.exit(1 if instance.try_become_primary() else 0)", str(tmp_path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        deadline = 100
        while (not received or second.poll() is None) and deadline > 0:
            _drain(100)
            deadline -= 1
        assert second.poll() == 0, "second launch did not exit successfully"
        assert received == [True], "the running instance was not activated"
    finally:
        if second.poll() is None:
            second.kill()
        second.communicate(timeout=5)
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
