from __future__ import annotations

import codecs
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from uuid import UUID

import yaml

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.identifiers import validate_display_identifier_name

_MAX_FRONTMATTER_BYTES = 16 * 1024


@dataclass(frozen=True, slots=True)
class SkillInfo:
    name: str
    description: str
    always_on: bool
    body: str
    path: str


def is_skill_manifest(path: str) -> bool:
    parts = PurePosixPath(path).parts
    return len(parts) == 3 and parts[0] == "skills" and parts[2] == "SKILL.md"


def is_under_skills(path: str) -> bool:
    parts = PurePosixPath(path).parts
    return bool(parts) and parts[0] == "skills"


def parse_skill_manifest(path: str, content: bytes) -> SkillInfo:
    name, description, always_on, body_offset = _parse_header(path, content)
    try:
        body = content[body_offset:].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _invalid_skill() from exc
    return SkillInfo(
        name=name,
        description=description,
        always_on=always_on,
        body=body,
        path=path,
    )


def validate_skill_manifest(path: str, content: bytes) -> None:
    _parse_header(path, content)
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        view = memoryview(content)
        for start in range(0, len(view), 64 * 1024):
            decoder.decode(view[start : start + 64 * 1024])
        decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise _invalid_skill() from exc


def _parse_header(path: str, content: bytes) -> tuple[str, str, bool, int]:
    if not is_skill_manifest(path) or not content.startswith(b"---\n"):
        raise _invalid_skill()
    separator = content.find(b"\n---\n", 4, _MAX_FRONTMATTER_BYTES + 5)
    if separator < 0:
        raise _invalid_skill()
    try:
        frontmatter = content[4:separator].decode("utf-8")
        parsed = yaml.load(frontmatter, Loader=yaml.BaseLoader)
    except yaml.YAMLError as exc:
        raise _invalid_skill() from exc
    except UnicodeDecodeError as exc:
        raise _invalid_skill() from exc
    if not isinstance(parsed, dict) or set(parsed) - {"name", "description", "always_on"}:
        raise _invalid_skill()
    name = parsed.get("name")
    description = parsed.get("description")
    always_on_value: Any = parsed.get("always_on", "false")
    try:
        normalized_name = validate_display_identifier_name(name) if isinstance(name, str) else None
    except ValueError as exc:
        raise _invalid_skill() from exc
    folder = PurePosixPath(path).parts[1]
    if (
        not isinstance(name, str)
        or name != normalized_name
        or name != folder
        or not isinstance(description, str)
        or not description.strip()
        or always_on_value not in {"true", "false"}
    ):
        raise _invalid_skill()
    return name, description.strip(), always_on_value == "true", separator + 5


class SkillsCache:
    def __init__(
        self,
        *,
        max_bytes: int = 64 * 1024 * 1024,
        max_user_bytes: int = 1024 * 1024,
    ) -> None:
        self._max_bytes = max_bytes
        self._max_user_bytes = max_user_bytes
        self._entries: OrderedDict[UUID, tuple[tuple[SkillInfo, ...], int]] = OrderedDict()
        self._total_bytes = 0
        self._next_generation = 0
        self._inflight: dict[UUID, int] = {}

    def get(self, user_id: UUID) -> tuple[SkillInfo, ...] | None:
        entry = self._entries.get(user_id)
        if entry is None:
            return None
        self._entries.move_to_end(user_id)
        return entry[0]

    def generation(self, user_id: UUID) -> int:
        self._next_generation += 1
        self._inflight[user_id] = self._next_generation
        return self._next_generation

    def put(
        self,
        user_id: UUID,
        skills: tuple[SkillInfo, ...],
        *,
        expected_generation: int | None = None,
    ) -> None:
        if expected_generation is not None:
            if self._inflight.get(user_id) != expected_generation:
                return
            self._inflight.pop(user_id, None)
        else:
            self._inflight.pop(user_id, None)
        weight = 512 + sum(
            256
            + 4
            * (
                len(skill.name.encode("utf-8"))
                + len(skill.description.encode("utf-8"))
                + len(skill.body.encode("utf-8"))
                + len(skill.path.encode("utf-8"))
            )
            for skill in skills
        )
        self._remove_entry(user_id)
        if weight > self._max_user_bytes or weight > self._max_bytes:
            return
        self._entries[user_id] = (skills, weight)
        self._total_bytes += weight
        while self._total_bytes > self._max_bytes:
            _, (_, removed_weight) = self._entries.popitem(last=False)
            self._total_bytes -= removed_weight

    def invalidate(self, user_id: UUID) -> None:
        self._inflight.pop(user_id, None)
        self._remove_entry(user_id)

    def abandon(self, user_id: UUID, generation: int) -> None:
        if self._inflight.get(user_id) == generation:
            self._inflight.pop(user_id, None)

    def _remove_entry(self, user_id: UUID) -> None:
        old = self._entries.pop(user_id, None)
        if old is not None:
            self._total_bytes -= old[1]


def _invalid_skill() -> WorkspaceError:
    return WorkspaceError(
        ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT,
        "SKILL.md must contain valid matching YAML frontmatter",
    )


_skills_cache = SkillsCache()


def get_skills_cache() -> SkillsCache:
    return _skills_cache
