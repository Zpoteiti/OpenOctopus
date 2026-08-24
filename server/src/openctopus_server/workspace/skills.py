from __future__ import annotations

import asyncio
import codecs
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from uuid import UUID

import yaml

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.chat.token_estimator import count_text_tokens
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.identifiers import validate_display_identifier_name

MAX_SKILL_FRONTMATTER_PREFIX_BYTES = 16 * 1024 + 9
ALWAYS_ON_MAX_BYTES = 64 * 1024
ALWAYS_ON_MAX_TOKENS = 16_000


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
    header, body_offset = _parse_header(path, content)
    try:
        body = content[body_offset:].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _invalid_skill() from exc
    _validate_always_on(header, content, body)
    return SkillInfo(
        name=header.name,
        description=header.description,
        always_on=header.always_on,
        body=body,
        path=path,
    )


def parse_skill_manifest_header(path: str, content: bytes) -> SkillInfo:
    header, _ = _parse_header(path, content)
    return header


def validate_skill_manifest(path: str, content: bytes) -> None:
    header, body_offset = _parse_header(path, content)
    if header.always_on:
        if len(content) > ALWAYS_ON_MAX_BYTES:
            raise _invalid_skill()
        try:
            body = content[body_offset:].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise _invalid_skill() from exc
        _validate_always_on(header, content, body)
        return

    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        view = memoryview(content)
        for start in range(0, len(view), 64 * 1024):
            decoder.decode(view[start : start + 64 * 1024])
        decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise _invalid_skill() from exc


async def validate_skill_manifest_stream(
    path: str,
    chunks: AsyncIterator[bytes],
) -> None:
    """Validate one SKILL.md while retaining only bounded frontmatter/body bytes."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    prefix = bytearray()
    always_on_content = bytearray()
    header: SkillInfo | None = None
    total_bytes = 0
    try:
        async for chunk in chunks:
            total_bytes += len(chunk)
            decoder.decode(chunk)

            if len(prefix) < MAX_SKILL_FRONTMATTER_PREFIX_BYTES:
                remaining = MAX_SKILL_FRONTMATTER_PREFIX_BYTES - len(prefix)
                prefix.extend(chunk[:remaining])
            if header is None and b"\n---\n" in prefix[4:]:
                header = parse_skill_manifest_header(path, bytes(prefix))
                if not header.always_on:
                    always_on_content.clear()

            if header is None or header.always_on:
                remaining = ALWAYS_ON_MAX_BYTES + 1 - len(always_on_content)
                if remaining > 0:
                    always_on_content.extend(chunk[:remaining])
            if header is not None and header.always_on and total_bytes > ALWAYS_ON_MAX_BYTES:
                raise _invalid_skill()

        decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise _invalid_skill() from exc

    if header is None:
        header = parse_skill_manifest_header(path, bytes(prefix))
    if header.always_on:
        validate_skill_manifest(path, bytes(always_on_content))


def _parse_header(path: str, content: bytes) -> tuple[SkillInfo, int]:
    if not is_skill_manifest(path) or not content.startswith(b"---\n"):
        raise _invalid_skill()
    separator = content.find(b"\n---\n", 4, MAX_SKILL_FRONTMATTER_PREFIX_BYTES)
    if separator < 0:
        raise _invalid_skill()
    try:
        frontmatter = content[4:separator].decode("utf-8")
        parsed = yaml.load(frontmatter, Loader=yaml.BaseLoader)
    except (yaml.YAMLError, RecursionError) as exc:
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
    return (
        SkillInfo(
            name=name,
            description=description.strip(),
            always_on=always_on_value == "true",
            body="",
            path=path,
        ),
        separator + 5,
    )


def _validate_always_on(header: SkillInfo, content: bytes, body: str) -> None:
    if not header.always_on:
        return
    if len(content) > ALWAYS_ON_MAX_BYTES or count_text_tokens(body) > ALWAYS_ON_MAX_TOKENS:
        raise _invalid_skill()


class SkillsCache:
    def __init__(
        self,
        *,
        max_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        self._max_bytes = max_bytes
        self._entries: OrderedDict[UUID, tuple[tuple[SkillInfo, ...], int]] = OrderedDict()
        self._total_bytes = 0
        self._generations: dict[UUID, int] = {}
        self._inflight: dict[
            UUID,
            tuple[int, asyncio.Task[tuple[SkillInfo, ...]]],
        ] = {}
        self._flight_guard = asyncio.Lock()

    def get(self, user_id: UUID) -> tuple[SkillInfo, ...] | None:
        entry = self._entries.get(user_id)
        if entry is None:
            return None
        self._entries.move_to_end(user_id)
        return entry[0]

    def put(
        self,
        user_id: UUID,
        skills: tuple[SkillInfo, ...],
        *,
        expected_generation: int | None = None,
    ) -> None:
        if (
            expected_generation is not None
            and self._generations.get(user_id, 0) != expected_generation
        ):
            return
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
        if weight > self._max_bytes:
            return
        self._entries[user_id] = (skills, weight)
        self._total_bytes += weight
        while self._total_bytes > self._max_bytes:
            _, (_, removed_weight) = self._entries.popitem(last=False)
            self._total_bytes -= removed_weight

    def invalidate(self, user_id: UUID) -> None:
        self._generations[user_id] = self._generations.get(user_id, 0) + 1
        self._remove_entry(user_id)

    async def get_or_load(
        self,
        user_id: UUID,
        loader: Callable[[], Awaitable[tuple[SkillInfo, ...]]],
    ) -> tuple[SkillInfo, ...]:
        cached = self.get(user_id)
        if cached is not None:
            return cached
        generation = self._generations.get(user_id, 0)
        owner = False
        async with self._flight_guard:
            cached = self.get(user_id)
            if cached is not None:
                return cached
            current = self._inflight.get(user_id)
            if current is None or current[0] != generation:
                task = asyncio.create_task(
                    self._load_and_publish(user_id, generation, loader),
                    name=f"skills-load-{user_id}",
                )
                self._inflight[user_id] = (generation, task)
                owner = True
            else:
                task = current[1]
        if owner:
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError as cancellation:
                task.cancel()
                try:
                    await await_future_cancellation_safe(task)
                except asyncio.CancelledError:
                    pass
                raise cancellation
        return await asyncio.shield(task)

    async def _load_and_publish(
        self,
        user_id: UUID,
        generation: int,
        loader: Callable[[], Awaitable[tuple[SkillInfo, ...]]],
    ) -> tuple[SkillInfo, ...]:
        task = asyncio.current_task()
        try:
            skills = await loader()
            self.put(user_id, skills, expected_generation=generation)
            return skills
        finally:
            async with self._flight_guard:
                current = self._inflight.get(user_id)
                if current is not None and current[1] is task:
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
