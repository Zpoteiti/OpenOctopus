from uuid import uuid4

import pytest

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.skills import (
    ALWAYS_ON_MAX_BYTES,
    ALWAYS_ON_MAX_TOKENS,
    MAX_SKILL_FRONTMATTER_PREFIX_BYTES,
    SkillInfo,
    SkillsCache,
    is_skill_manifest,
    is_under_skills,
    parse_skill_manifest,
    parse_skill_manifest_header,
    validate_skill_manifest,
)


@pytest.fixture(autouse=True)
def _cheap_skill_token_count(monkeypatch) -> None:
    monkeypatch.setattr(
        "openctopus_server.workspace.skills.count_text_tokens",
        lambda text: len(text),
    )


def test_parse_skill_manifest_requires_matching_frontmatter() -> None:
    parsed = parse_skill_manifest(
        "skills/reviewer/SKILL.md",
        b"---\nname: reviewer\ndescription: Review changes\nalways_on: true\n---\nCheck carefully.\n",
    )

    assert parsed == SkillInfo(
        name="reviewer",
        description="Review changes",
        always_on=True,
        body="Check carefully.\n",
        path="skills/reviewer/SKILL.md",
    )


@pytest.mark.parametrize(
    "content",
    [
        b"not frontmatter",
        b"---\nname: wrong\ndescription: x\n---\nbody",
        b"---\nname: reviewer\n---\nbody",
        b"---\nname: reviewer\ndescription: x\nalways_on: yes\n---\nbody",
        b"---\nname: reviewer\ndescription: x\nextra: value\n---\nbody",
    ],
)
def test_parse_skill_manifest_rejects_invalid_content(content: bytes) -> None:
    with pytest.raises(WorkspaceError) as exc_info:
        parse_skill_manifest("skills/reviewer/SKILL.md", content)

    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


@pytest.mark.parametrize("name", ["bad/name", " spaced ", "a" * 65])
def test_parse_skill_manifest_applies_identifier_validation(name: str) -> None:
    content = f"---\nname: {name}\ndescription: x\n---\nbody".encode()

    with pytest.raises(WorkspaceError) as exc_info:
        parse_skill_manifest(f"skills/{name}/SKILL.md", content)

    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


def test_parse_skill_manifest_rejects_unbounded_frontmatter() -> None:
    content = b"---\nname: reviewer\ndescription: " + b"x" * (17 * 1024) + b"\n---\nbody"

    with pytest.raises(WorkspaceError) as exc_info:
        parse_skill_manifest("skills/reviewer/SKILL.md", content)

    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


def test_frontmatter_boundary_is_shared_by_header_parser_and_validator() -> None:
    description = b"x" * (16 * 1024 - len(b"name: reviewer\ndescription: "))
    content = b"---\nname: reviewer\ndescription: " + description + b"\n---\nbody"

    assert content.index(b"\n---\n") + 5 == MAX_SKILL_FRONTMATTER_PREFIX_BYTES
    assert parse_skill_manifest_header("skills/reviewer/SKILL.md", content).name == "reviewer"
    validate_skill_manifest("skills/reviewer/SKILL.md", content)

    too_long = content.replace(description, description + b"x", 1)
    with pytest.raises(WorkspaceError) as exc_info:
        validate_skill_manifest("skills/reviewer/SKILL.md", too_long)
    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


def test_validate_always_on_rejects_byte_and_token_limits(monkeypatch) -> None:
    prefix = b"---\nname: reviewer\ndescription: x\nalways_on: true\n---\n"

    with pytest.raises(WorkspaceError) as exc_info:
        validate_skill_manifest(
            "skills/reviewer/SKILL.md",
            prefix + b"x" * (ALWAYS_ON_MAX_BYTES - len(prefix) + 1),
        )
    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT

    monkeypatch.setattr(
        "openctopus_server.workspace.skills.count_text_tokens",
        lambda text: ALWAYS_ON_MAX_TOKENS + (1 if text else 0),
    )
    with pytest.raises(WorkspaceError) as exc_info:
        validate_skill_manifest("skills/reviewer/SKILL.md", prefix + b"body")
    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


def test_validate_oversized_always_on_rejects_before_utf8_scan(monkeypatch) -> None:
    prefix = b"---\nname: reviewer\ndescription: x\nalways_on: true\n---\n"

    def fail_if_scanned(name: str):
        raise AssertionError(f"unexpected incremental decoder: {name}")

    def fail_if_counted(text: str) -> int:
        raise AssertionError(f"unexpected token count for {len(text)} characters")

    monkeypatch.setattr(
        "openctopus_server.workspace.skills.codecs.getincrementaldecoder",
        fail_if_scanned,
    )
    monkeypatch.setattr(
        "openctopus_server.workspace.skills.count_text_tokens",
        fail_if_counted,
    )

    with pytest.raises(WorkspaceError) as exc_info:
        validate_skill_manifest(
            "skills/reviewer/SKILL.md",
            prefix + b"x" * (ALWAYS_ON_MAX_BYTES - len(prefix)) + b"\xff",
        )

    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


def test_validate_conditional_streams_utf8_without_materializing_body() -> None:
    class RejectBodySlice(bytes):
        def __getitem__(self, key):
            if isinstance(key, slice) and key.stop is None:
                raise AssertionError("conditional body must not be copied")
            return super().__getitem__(key)

    content = RejectBodySlice(b"---\nname: reviewer\ndescription: x\n---\n" + "正文".encode() * 100)

    validate_skill_manifest("skills/reviewer/SKILL.md", content)


def test_validate_conditional_stream_rejects_invalid_utf8() -> None:
    content = b"---\nname: reviewer\ndescription: x\n---\nbody\xff"

    with pytest.raises(WorkspaceError) as exc_info:
        validate_skill_manifest("skills/reviewer/SKILL.md", content)

    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


def test_validate_legal_always_on_decodes_and_counts_complete_body(monkeypatch) -> None:
    counted: list[str] = []
    prefix = b"---\nname: reviewer\ndescription: x\nalways_on: true\n---\n"

    def count(text: str) -> int:
        counted.append(text)
        return len(text)

    monkeypatch.setattr("openctopus_server.workspace.skills.count_text_tokens", count)

    validate_skill_manifest("skills/reviewer/SKILL.md", prefix + "完整正文".encode())

    assert counted == ["完整正文"]


def test_validate_deeply_nested_yaml_is_normalized() -> None:
    nested = b"[" * 500 + b"]" * 500
    content = b"---\nname: reviewer\ndescription: " + nested + b"\n---\nbody"

    with pytest.raises(WorkspaceError) as exc_info:
        validate_skill_manifest("skills/reviewer/SKILL.md", content)

    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


def test_skill_path_predicates_are_deliberately_narrow() -> None:
    assert is_skill_manifest("skills/reviewer/SKILL.md") is True
    assert is_skill_manifest("skills/nested/reviewer/SKILL.md") is False
    assert is_skill_manifest("/shared@12345678/skills/reviewer/SKILL.md") is False
    assert is_under_skills("skills/reviewer/notes.md") is True
    assert is_under_skills("skills") is True
    assert is_under_skills("skillset/a.md") is False


def test_skills_cache_is_weight_bounded_and_invalidatable() -> None:
    cache = SkillsCache(max_bytes=1000)
    first_user = uuid4()
    second_user = uuid4()
    first = (SkillInfo("a", "d", False, "1234", "skills/a/SKILL.md"),)
    second = (SkillInfo("b", "d", False, "5678", "skills/b/SKILL.md"),)

    cache.put(first_user, first)
    cache.put(second_user, second)

    assert cache.get(first_user) is None
    assert cache.get(second_user) == second
    cache.invalidate(second_user)
    assert cache.get(second_user) is None


def test_skills_cache_counts_metadata_for_skills_without_cached_bodies() -> None:
    cache = SkillsCache(max_bytes=1000)
    first_user = uuid4()
    second_user = uuid4()

    cache.put(first_user, (SkillInfo("a", "d", False, "", "skills/a/SKILL.md"),))
    cache.put(second_user, (SkillInfo("b", "d", False, "", "skills/b/SKILL.md"),))

    assert cache.get(first_user) is None
    assert cache.get(second_user) is not None


async def test_skills_cache_loads_different_users_independently() -> None:
    import asyncio

    cache = SkillsCache()
    first_user = uuid4()
    second_user = uuid4()
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def load_first() -> tuple[SkillInfo, ...]:
        first_started.set()
        await release_first.wait()
        return ()

    first = asyncio.create_task(cache.get_or_load(first_user, load_first))
    await first_started.wait()
    second = await cache.get_or_load(second_user, _empty_skills)
    release_first.set()

    assert second == ()
    assert await first == ()


def test_skills_cache_accepts_one_maximum_legal_snapshot() -> None:
    cache = SkillsCache()
    user_id = uuid4()
    body = "x" * 64_000
    skills = tuple(
        SkillInfo(
            f"skill-{index:03}",
            "description",
            True,
            body,
            f"skills/skill-{index:03}/SKILL.md",
        )
        for index in range(200)
    )

    cache.put(user_id, skills)

    assert cache.get(user_id) == skills


async def test_skills_cache_single_flights_same_user_loads() -> None:
    import asyncio

    cache = SkillsCache()
    user_id = uuid4()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    expected = (SkillInfo("a", "d", False, "", "skills/a/SKILL.md"),)

    async def load() -> tuple[SkillInfo, ...]:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return expected

    first = asyncio.create_task(cache.get_or_load(user_id, load))
    await started.wait()
    second = asyncio.create_task(cache.get_or_load(user_id, load))
    await asyncio.sleep(0)
    release.set()

    assert await asyncio.gather(first, second) == [expected, expected]
    assert calls == 1


async def test_skills_cache_invalidation_during_load_does_not_publish_stale_result() -> None:
    import asyncio

    cache = SkillsCache()
    user_id = uuid4()
    started = asyncio.Event()
    release = asyncio.Event()
    stale = (SkillInfo("old", "d", False, "", "skills/old/SKILL.md"),)

    async def load() -> tuple[SkillInfo, ...]:
        started.set()
        await release.wait()
        return stale

    task = asyncio.create_task(cache.get_or_load(user_id, load))
    await started.wait()
    cache.invalidate(user_id)
    release.set()

    assert await task == stale
    assert cache.get(user_id) is None


async def test_skills_cache_owner_cancellation_wakes_waiters_and_clears_flight() -> None:
    import asyncio

    cache = SkillsCache()
    user_id = uuid4()
    started = asyncio.Event()

    async def load() -> tuple[SkillInfo, ...]:
        started.set()
        await asyncio.Event().wait()
        return ()

    owner = asyncio.create_task(cache.get_or_load(user_id, load))
    await started.wait()
    waiter = asyncio.create_task(cache.get_or_load(user_id, load))
    await asyncio.sleep(0)
    await cache._flight_guard.acquire()
    owner.cancel()
    await asyncio.sleep(0)
    owner.cancel()
    await asyncio.sleep(0)

    try:
        assert not owner.done()
    finally:
        cache._flight_guard.release()

    with pytest.raises(asyncio.CancelledError):
        await owner
    with pytest.raises(asyncio.CancelledError):
        await waiter

    assert await cache.get_or_load(user_id, _empty_skills) == ()


async def _empty_skills() -> tuple[SkillInfo, ...]:
    return ()
