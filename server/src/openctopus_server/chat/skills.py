"""Load workspace skill sources through the official deferred Skills capability."""

from __future__ import annotations

import hashlib
import posixpath
from dataclasses import dataclass

import yaml
from dbos import DBOS
from pydantic_ai import RunContext
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.messages import WorkspaceRef
from pydantic_ai.workspaces import FileEntry, WorkspaceReadOnlyError
from pydantic_ai_harness.skills import Skills

from openctopus_server.chat.prompt import _load_skills
from openctopus_server.chat.scope import active_run
from openctopus_server.workspace.builtin_skills import get_builtin_skill_library
from openctopus_server.workspace.skills import parse_skill_manifest


def personal_skill_id(name: str) -> str:
    """Map OO display names to distinct, bounded Agent Skills identifiers."""
    slug = "".join(character if character.isalnum() else "-" for character in name.lower())
    slug = "-".join(filter(None, slug.split("-")))[:32].rstrip("-") or "skill"
    digest = hashlib.sha256(name.encode()).hexdigest()[:16]
    return f"personal-{slug}-{digest}"


@DBOS.step(name="oo.skill_sources")
async def skill_sources() -> dict[str, bytes]:
    scope = active_run.get()
    assert scope.user_id is not None
    library = get_builtin_skill_library()
    definitions = [(skill, library.read(skill.path.removeprefix('/builtin/'), max_bytes=256 * 1024))
                   for skill in library.skills]
    service = scope.runtime.workspace_service
    if service is not None:
        personal = await _load_skills(service, user_id=scope.user_id, cache=scope.runtime.skills_cache)
        for skill in personal:
            if not skill.always_on:
                data = await service.read_personal_for_prompt(user_id=scope.user_id, path=skill.path, length=256 * 1024 + 1)
                if len(data) > 256 * 1024:
                    raise ValueError("Skill exceeds the 256 KiB loading limit")
                definitions.append((skill, data))
    result = {}
    for skill, data in definitions:
        # OO keeps the explicit source namespace, while Harness skill IDs use
        # the Agent Skills name grammar. Always-on product instructions remain
        # in the host prompt; conditional instructions are owned by Skills.
        name = f"builtin-{skill.name}" if skill.origin == "builtin" else personal_skill_id(skill.name)
        parsed = parse_skill_manifest(f"skills/{skill.name}/SKILL.md", data)
        header = yaml.safe_dump({"name": name, "description": f"{skill.name}: {skill.description}"}, allow_unicode=True)
        body = f"Source: {skill.path}\nResolve resource paths against that skill directory.\n\n{parsed.body}"
        result[f"/skills/{name}/SKILL.md"] = f"---\n{header}---\n{body}".encode()
    return result


@dataclass
class SkillSnapshot:
    files: dict[str, bytes]

    @property
    def ref(self) -> WorkspaceRef:
        digest = hashlib.sha256(b"".join(path.encode() + data for path, data in sorted(self.files.items()))).hexdigest()
        return WorkspaceRef(provider="openoctopus-skills", id=digest)

    async def working_dir(self) -> str:
        return "/"

    async def read_bytes(self, path: str) -> bytes:
        try:
            return self.files[path]
        except KeyError:
            raise FileNotFoundError(path) from None

    async def stat(self, path: str) -> FileEntry:
        data = self.files.get(path)
        directory = path in {"/", "/skills"} or any(key.startswith(path.rstrip('/') + '/') for key in self.files)
        if data is None and not directory:
            raise FileNotFoundError(path)
        return FileEntry(name=posixpath.basename(path), path=path, is_dir=directory, size=None if directory else len(data or b""))

    async def list_dir(self, path: str) -> list[FileEntry]:
        prefix = path.rstrip('/') + '/'
        children = {prefix + key[len(prefix):].split('/')[0] for key in self.files if key.startswith(prefix)}
        return [await self.stat(child) for child in sorted(children)]

    async def exists(self, path: str) -> bool:
        try:
            await self.stat(path)
        except FileNotFoundError:
            return False
        return True

    async def write_bytes(self, path: str, data: bytes) -> None:
        raise WorkspaceReadOnlyError("Skill snapshots are read-only")

    async def make_dir(self, path: str) -> None:
        raise WorkspaceReadOnlyError("Skill snapshots are read-only")

    async def remove(self, path: str) -> None:
        raise WorkspaceReadOnlyError("Skill snapshots are read-only")


class WorkspaceSkills(Skills[str]):
    def __init__(self) -> None:
        super().__init__("/skills", workspace=SkillSnapshot({}))

    async def for_run(self, ctx: RunContext[str]) -> AbstractCapability[str]:
        capability = Skills[str]("/skills", workspace=SkillSnapshot(active_run.get().skill_files))
        return await capability.for_run(ctx)
