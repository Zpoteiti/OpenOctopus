from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ToolFailure(Exception):  # noqa: N818
    code: str
    message: str


@dataclass(frozen=True)
class ToolOutput:
    content: str | list[dict[str, Any]]
    is_error: bool = False
    code: str | None = None


def fail(code: str, message: str) -> ToolOutput:
    return ToolOutput(content=f"[{code}] {message}", is_error=True, code=code)


def _required_str(args: dict[str, Any], name: str) -> str:
    value = args.get(name)
    if not isinstance(value, str):
        raise ToolFailure("tool_invalid_args", f"{name} is required and must be a string")
    return value


def _optional_str(args: dict[str, Any], name: str) -> str | None:
    value = args.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ToolFailure("tool_invalid_args", f"{name} must be a string")
    return value


def _bool_arg(args: dict[str, Any], name: str, default: bool) -> bool:
    value = args.get(name, default)
    if not isinstance(value, bool):
        raise ToolFailure("tool_invalid_args", f"{name} must be a boolean")
    return value


def _int_arg(
    args: dict[str, Any],
    name: str,
    default: int | None = None,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    value = args.get(name, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        raise ToolFailure("tool_invalid_args", f"{name} is invalid")
    return value


def _optional_int(
    args: dict[str, Any], name: str, *, minimum: int, maximum: int | None = None
) -> int | None:
    if name not in args or args[name] is None:
        return None
    return _int_arg(args, name, minimum=minimum, maximum=maximum)


def _contains_nul(value: object) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_contains_nul(key) or _contains_nul(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_nul(item) for item in value)
    return False


def _cap(value: str, maximum: int) -> str:
    return value if len(value) <= maximum else value[:maximum] + "\n... (truncated)"
