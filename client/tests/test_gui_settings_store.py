from __future__ import annotations

import os
import stat

import pytest

from openoctopus_client.gui.settings_store import (
    ClientSettings,
    SettingsError,
    SettingsStore,
)


def _store(tmp_path) -> SettingsStore:  # type: ignore[no-untyped-def]
    return SettingsStore(tmp_path / "openoctopus-client")


def test_unconfigured_until_committed(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = _store(tmp_path)
    assert store.load() is None


def test_commit_round_trip_and_permissions(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = _store(tmp_path)
    store.commit(ClientSettings(server_url="https://openoctopus.example", token_account="acc-1"))
    assert store.load() == ClientSettings(
        server_url="https://openoctopus.example", token_account="acc-1"
    )
    if os.name != "nt":
        directory_mode = stat.S_IMODE(store.path.parent.stat().st_mode)
        file_mode = stat.S_IMODE(store.path.stat().st_mode)
        assert directory_mode == 0o700
        assert file_mode == 0o600


def test_commit_is_atomic_replacement_and_replaces_previous(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = _store(tmp_path)
    store.commit(ClientSettings(server_url="https://a.example", token_account="acc-1"))
    old_inode = store.path.stat().st_ino
    store.commit(ClientSettings(server_url="https://b.example", token_account="acc-2"))
    assert store.load() == ClientSettings(
        server_url="https://b.example", token_account="acc-2"
    )
    # Replacement, not in-place truncation: a reader never sees half a file.
    assert store.path.stat().st_ino != old_inode
    assert list(store.path.parent.glob(".config-*")) == []


def test_invalid_settings_file_is_an_error_not_a_reset(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = _store(tmp_path)
    store.commit(ClientSettings(server_url="https://a.example", token_account="acc-1"))
    store.path.write_text("{ broken", encoding="utf-8")
    with pytest.raises(SettingsError):
        store.load()


def test_unknown_schema_version_is_rejected(tmp_path) -> None:  # type: ignore[no-untyped-def]
    store = _store(tmp_path)
    store.commit(ClientSettings(server_url="https://a.example", token_account=None))
    store.path.write_text('{"schema_version": 99}', encoding="utf-8")
    with pytest.raises(SettingsError):
        store.load()
