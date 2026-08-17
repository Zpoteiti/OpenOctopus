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
