from __future__ import annotations

import os
from collections.abc import MutableMapping
from dataclasses import dataclass
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


def load_config(environment: MutableMapping[str, str] | None = None) -> ClientConfiguration:
    """Load required environment variables and consume the bearer token."""

    values = os.environ if environment is None else environment
    server_url = values.get("OPENOCTOPUS_SERVER_URL")
    token = values.get("OPENOCTOPUS_DEVICE_TOKEN")
    try:
        if server_url is None or not server_url.strip():
            raise ConfigurationError("OPENOCTOPUS_SERVER_URL is required")
        websocket_url = _websocket_url(server_url)
        if (
            token is None
            or not token.startswith("openoctopus_dev_")
            or len(token) == len("openoctopus_dev_")
        ):
            raise ConfigurationError("OPENOCTOPUS_DEVICE_TOKEN is invalid")
        return ClientConfiguration(
            server_url=_canonical_server_url(server_url),
            websocket_url=websocket_url,
            token=DeviceToken(token),
        )
    finally:
        # Even invalid startup input must not survive for future children.
        values.pop("OPENOCTOPUS_DEVICE_TOKEN", None)


def _parsed_server_url(value: str) -> SplitResult:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ConfigurationError("OPENOCTOPUS_SERVER_URL must be an http(s) origin")
    if parsed.username is not None or parsed.password is not None:
        raise ConfigurationError("OPENOCTOPUS_SERVER_URL must not contain userinfo")
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise ConfigurationError(
            "OPENOCTOPUS_SERVER_URL must not contain a path, query, or fragment"
        )
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ConfigurationError("OPENOCTOPUS_SERVER_URL has an invalid port") from exc
    return parsed


def _canonical_server_url(value: str) -> str:
    parsed = _parsed_server_url(value)
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _websocket_url(value: str) -> str:
    parsed = _parsed_server_url(value)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunsplit((scheme, parsed.netloc, "/ws/device", "", ""))
