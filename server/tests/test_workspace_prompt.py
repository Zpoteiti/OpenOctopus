from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat.prompt import _load_skills, build_system_prompt
from openctopus_server.db.models import Session, User, Workspace, WorkspaceMember
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import DirectoryEntry, DirectoryPage
from openctopus_server.workspace.skills import SkillsCache


class _PromptWorkspace:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.read_paths: list[str] = []

    async def read(
        self,
        db: AsyncSession,
        *,
        user_id: object,
        path: str,
        offset: int = 0,
        length: int = 0,
    ) -> bytes:
        del db, user_id, offset
        self.read_paths.append(path)
        if path not in self.files:
            raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "missing")
        data = self.files[path]
        return data[:length] if length else data

    async def list_dir_page(
        self,
        db: AsyncSession,
        *,
        user_id: object,
        path: str,
        limit: int,
        offset: int = 0,
        include_noise_directories: bool = False,
    ) -> DirectoryPage:
        del db, user_id, include_noise_directories
        if path != "skills":
            raise AssertionError(path)
        names = sorted(
            {file_path.split("/")[1] for file_path in self.files if file_path.startswith("skills/")}
        )
        if not names:
            raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "missing")
        entries = [
            DirectoryEntry(path=f"skills/{name}", is_directory=True, size=None) for name in names
        ]
        items = tuple(entries[offset : offset + limit])
        has_more = len(entries) > offset + limit
        return DirectoryPage(
            items=items,
            next_offset=offset + limit if has_more else None,
            truncated=False,
        )


async def test_prompt_loads_workspace_identity_memory_skills_and_shared_refs(pg_engine) -> None:
    user = User(
        id=uuid4(),
        email=f"{uuid4()}@test.com",
        password_hash="hash",
        name="Alice",
        is_admin=False,
    )
    session = Session(
        id=uuid4(),
        user_id=user.id,
        session_key=f"web:{uuid4()}",
        channel="web",
        chat_id="chat-1",
        title="New chat",
    )
    shared = Workspace(
        id=uuid4(),
        name="team",
        suffix="abcdef12",
        quota_bytes=1000,
        created_by=user.id,
        created_at=datetime.now(UTC),
    )
    workspace = _PromptWorkspace(
        {
            "SOUL.md": b"Be precise.",
            "MEMORY.md": b"Alice likes concise answers.",
            "skills/reviewer/SKILL.md": (
                b"---\nname: reviewer\ndescription: Review changes\nalways_on: true\n---\n"
                b"Inspect tests first."
            ),
            "skills/research/SKILL.md": (
                b"---\nname: research\ndescription: Find primary sources\n---\nSearch carefully."
            ),
        }
    )
    skills_cache = SkillsCache()
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(user)
        await db.flush()
        db.add_all([session, shared])
        await db.flush()
        db.add(WorkspaceMember(workspace_id=shared.id, user_id=user.id))
        await db.commit()

        prompt = await build_system_prompt(
            db,
            session=session,
            user=user,
            workspace_service=workspace,
            skills_cache=skills_cache,
        )

    assert "## SOUL\n\nBe precise." in prompt
    assert "## MEMORY\n\nAlice likes concise answers." in prompt
    assert "### reviewer (always-on)\n\nInspect tests first." in prompt
    assert "research — Find primary sources" in prompt
    assert "skills/research/SKILL.md" in prompt
    assert "/team@abcdef12/" in prompt
    assert "Relative server paths mean the personal workspace" in prompt
    assert "Use `message` to deliver files" in prompt
    cached_skills = skills_cache.get(user.id)
    assert cached_skills is not None
    assert next(skill for skill in cached_skills if skill.name == "research").body == ""


async def test_prompt_caps_optional_files_with_a_read_file_marker(pg_engine) -> None:
    user = User(
        id=uuid4(),
        email=f"{uuid4()}@test.com",
        password_hash="hash",
        name="Alice",
        is_admin=False,
    )
    session = Session(
        id=uuid4(),
        user_id=user.id,
        session_key=f"web:{uuid4()}",
        channel="web",
        chat_id="chat-2",
        title="New chat",
    )
    workspace = _PromptWorkspace({"SOUL.md": b"x" * 40_000})
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(user)
        await db.flush()
        db.add(session)
        await db.commit()
        prompt = await build_system_prompt(
            db,
            session=session,
            user=user,
            workspace_service=workspace,
            skills_cache=SkillsCache(),
        )

    soul = prompt.split("## MEMORY", 1)[0]
    assert soul.count("x") == 32_000
    assert "truncated; use read_file for SOUL.md" in soul


async def test_prompt_tolerates_a_bounded_read_ending_inside_utf8_codepoint(pg_engine) -> None:
    user = User(
        id=uuid4(),
        email=f"{uuid4()}@test.com",
        password_hash="hash",
        name="Alice",
        is_admin=False,
    )
    session = Session(
        id=uuid4(),
        user_id=user.id,
        session_key=f"web:{uuid4()}",
        channel="web",
        chat_id="chat-utf8",
        title="New chat",
    )
    workspace = _PromptWorkspace({"SOUL.md": "😀".encode() * 32_001})
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(user)
        await db.flush()
        db.add(session)
        await db.commit()

        prompt = await build_system_prompt(
            db,
            session=session,
            user=user,
            workspace_service=workspace,
            skills_cache=SkillsCache(),
        )

    soul = prompt.split("## MEMORY", 1)[0]
    assert soul.count("😀") == 32_000
    assert "truncated; use read_file for SOUL.md" in soul


async def test_skill_discovery_pages_past_non_manifest_directories() -> None:
    files = {f"skills/{index:04}/notes.txt": b"not a manifest" for index in range(1000)}
    files["skills/zzzz/SKILL.md"] = b"---\nname: zzzz\ndescription: Found on page two\n---\nbody"
    workspace = _PromptWorkspace(files)

    skills = await _load_skills(
        workspace,
        AsyncMock(spec=AsyncSession),
        user_id=uuid4(),
        cache=SkillsCache(),
    )

    assert [(skill.name, skill.description) for skill in skills] == [("zzzz", "Found on page two")]
