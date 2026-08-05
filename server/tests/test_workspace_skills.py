from uuid import uuid4

import pytest

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.skills import (
    SkillInfo,
    SkillsCache,
    is_skill_manifest,
    is_under_skills,
    parse_skill_manifest,
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


def test_skill_path_predicates_are_deliberately_narrow() -> None:
    assert is_skill_manifest("skills/reviewer/SKILL.md") is True
    assert is_skill_manifest("skills/nested/reviewer/SKILL.md") is False
    assert is_skill_manifest("/shared@12345678/skills/reviewer/SKILL.md") is False
    assert is_under_skills("skills/reviewer/notes.md") is True
    assert is_under_skills("skills") is True
    assert is_under_skills("skillset/a.md") is False


def test_skills_cache_is_weight_bounded_and_invalidatable() -> None:
    cache = SkillsCache(max_bytes=1000, max_user_bytes=1000)
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
    cache = SkillsCache(max_bytes=1000, max_user_bytes=1000)
    first_user = uuid4()
    second_user = uuid4()

    cache.put(first_user, (SkillInfo("a", "d", False, "", "skills/a/SKILL.md"),))
    cache.put(second_user, (SkillInfo("b", "d", False, "", "skills/b/SKILL.md"),))

    assert cache.get(first_user) is None
    assert cache.get(second_user) is not None


def test_skills_cache_does_not_restore_an_entry_invalidated_during_load() -> None:
    cache = SkillsCache()
    user_id = uuid4()
    generation = cache.generation(user_id)

    cache.invalidate(user_id)
    cache.put(
        user_id,
        (SkillInfo("old", "stale", False, "", "skills/old/SKILL.md"),),
        expected_generation=generation,
    )

    assert cache.get(user_id) is None


def test_skills_cache_accepts_concurrent_loads_for_different_users() -> None:
    cache = SkillsCache()
    first_user = uuid4()
    second_user = uuid4()
    first_generation = cache.generation(first_user)
    second_generation = cache.generation(second_user)

    cache.put(first_user, (), expected_generation=first_generation)
    cache.put(second_user, (), expected_generation=second_generation)

    assert cache.get(first_user) == ()
    assert cache.get(second_user) == ()
