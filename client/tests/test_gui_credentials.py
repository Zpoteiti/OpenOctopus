from __future__ import annotations

import keyring.errors
import pytest

from openoctopus_client.gui.credentials import (
    CredentialError,
    CredentialStore,
    CredentialUnavailableError,
    new_token_account,
)


class RecordingBackend:
    def __init__(self) -> None:
        self.entries: dict[tuple[str, str], str] = {}
        self.fail_set = False
        self.fail_get = False

    def get_password(self, service: str, username: str) -> str | None:
        if self.fail_get:
            raise keyring.errors.InitError("Secret Service is locked")
        return self.entries.get((service, username))

    def set_password(self, service: str, username: str, password: str) -> None:
        if self.fail_set:
            raise keyring.errors.PasswordSetError("denied")
        self.entries[(service, username)] = password

    def delete_password(self, service: str, username: str) -> None:
        del self.entries[(service, username)]


def test_new_accounts_never_collide() -> None:
    assert new_token_account() != new_token_account()


def test_save_load_delete_round_trip() -> None:
    backend = RecordingBackend()
    store = CredentialStore(backend=backend)
    account = new_token_account()
    store.save(account, "openoctopus_dev_value")
    assert store.load(account) == "openoctopus_dev_value"
    assert store.delete(account) is True
    assert store.load(account) is None
    assert store.delete(account) is False


def test_locked_store_reports_unavailable_without_leaking() -> None:
    backend = RecordingBackend()
    backend.fail_get = True
    store = CredentialStore(backend=backend)
    with pytest.raises(CredentialUnavailableError):
        store.load("acc")


def test_refused_save_reports_failure() -> None:
    backend = RecordingBackend()
    backend.fail_set = True
    store = CredentialStore(backend=backend)
    with pytest.raises(CredentialError):
        store.save("acc", "openoctopus_dev_value")
