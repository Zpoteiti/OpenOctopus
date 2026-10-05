"""Device tokens live only in the OS credential store.

The GUI is the only component that touches the credential store.  Backends
are pinned explicitly per platform (Windows Credential Locker, macOS
Keychain, freedesktop Secret Service); third-party auto-discovery and
plaintext fallbacks are never enabled.  Nothing here logs the token.
"""

from __future__ import annotations

import sys
import uuid
from typing import Protocol

import keyring
import keyring.errors

SERVICE_NAME = "OpenOctopus Client"


class CredentialUnavailableError(RuntimeError):
    """The platform credential store cannot be reached or is locked."""


class CredentialDeniedError(RuntimeError):
    """The platform credential store refused access."""


class CredentialError(RuntimeError):
    """A credential operation failed."""


class CredentialBackend(Protocol):
    def get_password(self, service: str, username: str) -> str | None: ...

    def set_password(self, service: str, username: str, password: str) -> None: ...

    def delete_password(self, service: str, username: str) -> None: ...



def install_platform_backend() -> None:
    """Pin keyring to the explicit platform backend, never auto-discovered."""

    if sys.platform == "win32":
        from keyring.backends.Windows import WinVaultKeyring

        backend = WinVaultKeyring()  # type: ignore[no-untyped-call]
    elif sys.platform == "darwin":
        from keyring.backends.macOS import Keyring as MacOSKeyring

        backend = MacOSKeyring()  # type: ignore[no-untyped-call]
    else:
        from keyring.backends.SecretService import Keyring as SecretServiceKeyring

        backend = SecretServiceKeyring()  # type: ignore[no-untyped-call]
    keyring.set_keyring(backend)


def new_token_account() -> str:
    """A fresh account name so a new token never overwrites the old entry."""

    return f"device-token-{uuid.uuid4().hex}"


class CredentialStore:
    def __init__(self, backend: CredentialBackend | None = None) -> None:
        self._backend: CredentialBackend = backend if backend is not None else keyring

    def load(self, account: str) -> str | None:
        try:
            return self._backend.get_password(SERVICE_NAME, account)
        except (keyring.errors.InitError, keyring.errors.NoKeyringError) as exc:
            raise CredentialUnavailableError(str(exc)) from exc
        except Exception as exc:  # noqa: BLE001 - locked or refusing stores surface oddly
            raise CredentialUnavailableError("credential store refused a read") from exc

    def save(self, account: str, token: str) -> None:
        try:
            self._backend.set_password(SERVICE_NAME, account, token)
        except keyring.errors.InitError as exc:
            raise CredentialUnavailableError(str(exc)) from exc
        except keyring.errors.PasswordSetError as exc:
            raise CredentialError("credential could not be saved") from exc
        except Exception as exc:  # noqa: BLE001
            raise CredentialError("credential could not be saved") from exc

    def delete(self, account: str) -> bool:
        try:
            self._backend.delete_password(SERVICE_NAME, account)
            return True
        except keyring.errors.PasswordDeleteError:
            return False
        except Exception:  # noqa: BLE001
            return False
