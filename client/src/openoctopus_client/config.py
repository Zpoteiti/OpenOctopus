from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit


class ConfigurationError(ValueError):
    """A required client setting is absent or unsafe."""


@dataclass(frozen=True)
class DeviceToken:
    _value: str

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "DeviceToken(<redacted>)"

    __str__ = __repr__


@dataclass(frozen=True)
class ClientConfiguration:
    server_url: str
    websocket_url: str
    token: DeviceToken
    workspace_root: Path | None = None


def validate_server_url(value: str) -> str:
    """Return the canonical HTTP(S) origin or raise ``ConfigurationError``."""

    return _canonical_server_url(value)


def configuration_from_startup(
    server_url: str,
    token: str,
    workspace_root: Path | None = None,
) -> ClientConfiguration:
    """Build the runtime configuration from the private-pipe startup message.

    The token arrives only through the GUI's stdin pipe.  It is never read
    from an environment variable or a command-line argument, and the caller
    must never place it on a child command line.
    """

    if not token.startswith("openoctopus_dev_") or len(token) == len("openoctopus_dev_"):
        raise ConfigurationError("device token is invalid")
    return ClientConfiguration(
        server_url=_canonical_server_url(server_url),
        websocket_url=_websocket_url(server_url),
        token=DeviceToken(token),
        workspace_root=workspace_root,
    )


def _parsed_server_url(value: str) -> SplitResult:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ConfigurationError("server URL must be an http(s) origin")
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError("server URL must not contain userinfo")
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise ConfigurationError("server URL must not contain a path, query, or fragment")
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ConfigurationError("server URL has an invalid port") from exc
    return parsed


def _canonical_server_url(value: str) -> str:
    parsed = _parsed_server_url(value)
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _websocket_url(value: str) -> str:
    parsed = _parsed_server_url(value)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunsplit((scheme, parsed.netloc, "/ws/device", "", ""))
