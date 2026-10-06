"""Non-secret client settings persisted with atomic replacement.

Only the Server address and a reference (account name) into the system
credential store live here.  Device tokens never touch this file; they are
stored through :mod:`openoctopus_client.gui.credentials`.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

SCHEMA_VERSION = 1
APP_DIR_NAME = "openoctopus-client"
SETTINGS_FILE_NAME = "config.json"

_SETTINGS_BYTES_MAX = 64 * 1024


class SettingsError(RuntimeError):
    """The settings file could not be read or written."""


@dataclass(frozen=True)
class ClientSettings:
    server_url: str
    token_account: str | None


def default_settings_directory() -> Path:
    from PySide6.QtCore import QStandardPaths

    locations = QStandardPaths.standardLocations(
        QStandardPaths.StandardLocation.ConfigLocation
    )
    base = Path(locations[0]) if locations else Path.home() / ".config"
    return base / APP_DIR_NAME


class SettingsStore:
    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self._path = directory / SETTINGS_FILE_NAME

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> ClientSettings | None:
        """Return the saved settings, or ``None`` when unconfigured.

        A present-but-invalid file is reported as an error instead of being
        silently discarded, because silently losing the address would strand
        the saved token reference.
        """

        try:
            raw = self._path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise SettingsError("settings file could not be read") from exc
        if len(raw) > _SETTINGS_BYTES_MAX:
            raise SettingsError("settings file is oversized")
        try:
            payload = json.loads(raw.decode("utf-8", errors="strict"))
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise SettingsError("settings file is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise SettingsError("settings file is not an object")
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise SettingsError("settings file has an unsupported schema version")
        server_url = payload.get("server_url")
        token_account = payload.get("token_account")
        if not isinstance(server_url, str) or not server_url:
            raise SettingsError("settings file is missing the server URL")
        if token_account is not None and (
            not isinstance(token_account, str) or not token_account
        ):
            raise SettingsError("settings file has an invalid token account")
        return ClientSettings(server_url=server_url, token_account=token_account)

    def commit(self, settings: ClientSettings) -> None:
        """Atomically replace the settings file and claim ownership of it."""

        payload = json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "server_url": settings.server_url,
                "token_account": settings.token_account,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        if len(payload) > _SETTINGS_BYTES_MAX:
            raise SettingsError("settings payload is oversized")
        try:
            self._ensure_private_directory()
            descriptor, temporary = tempfile.mkstemp(
                dir=self._directory, prefix=".config-", suffix=".tmp"
            )
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self._path)
            except BaseException:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
                raise
        except OSError as exc:
            raise SettingsError("settings file could not be written") from exc

    def _ensure_private_directory(self) -> None:
        self._directory.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            os.chmod(self._directory, 0o700)
        if self._path.exists():
            os.chmod(self._path, 0o600)
