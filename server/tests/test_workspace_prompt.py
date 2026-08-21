import asyncio
import threading
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import openctopus_server.chat.prompt as prompt_module
from openctopus_server.chat.prompt import _load_skills, _parse_skill_header, build_system_prompt
from openctopus_server.db.models import Device, Session, User, Workspace, WorkspaceMember
from openctopus_server.devices.protocol import ShellMetadata
from openctopus_server.devices.registry import DeviceRegistry
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import WorkspaceError
from openctopus_server.workspace.fs import DirectoryEntry, DirectoryPage
from openctopus_server.workspace.skills import SkillsCache


@pytest.fixture(autouse=True)
def _cheap_skill_token_count(monkeypatch) -> None:
    monkeypatch.setattr(
        "openctopus_server.workspace.skills.count_text_tokens",
        lambda text: len(text),
    )


class _PromptWorkspace:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.read_paths: list[str] = []
        self.read_requests: list[tuple[str, int]] = []
        self.list_requests: list[tuple[int, int]] = []

    async def read_personal_for_prompt(
        self,
        *,
        user_id: object,
        path: str,
        offset: int = 0,
        length: int = 0,
    ) -> bytes:
        del user_id, offset
        self.read_paths.append(path)
        self.read_requests.append((path, length))
        if path not in self.files:
            raise WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "missing")
        data = self.files[path]
        return data[:length] if length else data

    async def list_personal_for_prompt(
        self,
        *,
        user_id: object,
        path: str,
        limit: int,
        offset: int = 0,
        include_noise_directories: bool = False,
        scan_limit: int = 10_000,
    ) -> DirectoryPage:
        del user_id, include_noise_directories
        self.list_requests.append((limit, scan_limit))
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


class _PromptTransport:
    async def send_text(self, payload: str) -> None:
        del payload

    async def send_binary(self, payload: bytes) -> None:
        del payload

    async def close(self, code: int, reason: str) -> None:
        del code, reason


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


async def test_prompt_describes_exec_only_for_trusted_devices(pg_engine) -> None:
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
        chat_id="chat-exec",
        title="New chat",
    )
    sandbox_device = Device(
        user_id=user.id,
        name="sandbox-laptop",
        token_hash=b"s" * 32,
        token_hint="sandbox-token",
        workspace_path="/sandbox/workspace",
        sandbox_mode=True,
        shell_timeout_max=600,
    )
    trusted_device = Device(
        user_id=user.id,
        name="trusted-laptop",
        token_hash=b"t" * 32,
        token_hint="trusted-token",
        workspace_path="/trusted/workspace",
        sandbox_mode=False,
        shell_timeout_max=900,
    )
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add(user)
        await db.flush()
        db.add_all([session, sandbox_device, trusted_device])
        await db.commit()

        prompt = await build_system_prompt(db, session=session, user=user)

    assert "server — OpenOctopus server tool target; exec: unavailable" in prompt
    assert (
        "sandbox-laptop — workspace_root: /sandbox/workspace; sandbox_mode: true; exec: unavailable"
    ) in prompt
    sandbox_line = next(
        line for line in prompt.splitlines() if line.startswith("- sandbox-laptop ")
    )
    assert "exec: available" not in sandbox_line
    assert "shell_timeout_max" not in sandbox_line
    assert (
        "trusted-laptop — workspace_root: /trusted/workspace; sandbox_mode: false; "
        "exec: available; shell_timeout_max: 900 seconds"
    ) in prompt
    assert (
        "Exec on trusted devices defaults to pipes; use tty=true for line-oriented "
        "interaction. It runs with host OS privileges and is not an OS sandbox."
    ) in prompt
    assert "Prefer file tools for ordinary file reads and writes." in prompt
    assert (
        "For long-running commands, yield and then use list_exec_sessions or write_stdin "
        "to poll."
    ) in prompt
    assert (
        "After tool_execution_outcome_unknown, inspect the session or external state and "
        "do not replay the command."
    ) in prompt
    assert (
        "Never request or enter passwords, 2FA codes, or passphrases; ask the user to take over."
    ) in prompt
    assert "env_allowlist" not in prompt
    assert str(trusted_device.id) not in prompt


async def test_prompt_describes_only_live_trusted_device_shell_metadata(pg_engine) -> None:
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
        chat_id="chat-live-shells",
        title="New chat",
    )
    trusted_device = Device(
        user_id=user.id,
        name="trusted-laptop",
        token_hash=b"t" * 32,
        token_hint="trusted-token",
        workspace_path="/trusted/workspace",
        sandbox_mode=False,
        shell_timeout_max=900,
    )
    registry = DeviceRegistry()
    old_handle = None
    new_handle = None
    try:
        async with AsyncSession(pg_engine, expire_on_commit=False) as db:
            db.add(user)
            await db.flush()
            db.add_all([session, trusted_device])
            await db.commit()

            old_handle = await registry.register(
                device_id=trusted_device.id,
                user_id=user.id,
                device_name=trusted_device.name,
                transport=_PromptTransport(),
                operating_system="linux",
                shells=ShellMetadata(default="bash", available=["bash", "sh"]),
            )
            prompt = await build_system_prompt(
                db,
                session=session,
                user=user,
                device_registry=registry,
            )

        assert (
            "trusted-laptop — workspace_root: /trusted/workspace; sandbox_mode: false; "
            "exec: available; shell_timeout_max: 900 seconds; os: linux; "
            "default_shell: bash; available_shells: bash, sh"
        ) in prompt
        assert str(trusted_device.id) not in prompt
        assert "trusted-token" not in prompt

        new_handle = await registry.register(
            device_id=trusted_device.id,
            user_id=user.id,
            device_name=trusted_device.name,
            transport=_PromptTransport(),
            operating_system="darwin",
            shells=ShellMetadata(default="zsh", available=["zsh", "bash", "sh"]),
        )
        prompt = await build_system_prompt(
            db,
            session=session,
            user=user,
            device_registry=registry,
        )
        assert "; os: darwin; default_shell: zsh; available_shells: zsh, bash, sh" in prompt
        assert "; os: linux; default_shell: bash; available_shells: bash, sh" not in prompt

        assert new_handle is not None
        assert await registry.unregister(new_handle) is True
        prompt = await build_system_prompt(
            db,
            session=session,
            user=user,
            device_registry=registry,
        )
        assert "default_shell:" not in prompt
    finally:
        if new_handle is not None:
            await registry.unregister(new_handle)
        elif old_handle is not None:
            await registry.unregister(old_handle)
        await registry.close()


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


async def test_skill_discovery_stops_after_first_200_candidates() -> None:
    files = {f"skills/{index:04}/notes.txt": b"not a manifest" for index in range(1000)}
    files["skills/zzzz/SKILL.md"] = b"---\nname: zzzz\ndescription: Found on page two\n---\nbody"
    workspace = _PromptWorkspace(files)

    skills = await _load_skills(
        workspace,
        user_id=uuid4(),
        cache=SkillsCache(),
    )

    assert skills == ()
    assert workspace.list_requests == [(1000, 1000)]
    assert workspace.read_paths == [f"skills/{index:04}/SKILL.md" for index in range(200)]


async def test_conditional_skill_only_reads_frontmatter_but_always_on_gets_bounded_full_read() -> (
    None
):
    from openctopus_server.workspace.skills import (
        ALWAYS_ON_MAX_BYTES,
        MAX_SKILL_FRONTMATTER_PREFIX_BYTES,
    )

    workspace = _PromptWorkspace(
        {
            "skills/conditional/SKILL.md": (
                b"---\nname: conditional\ndescription: Load on demand\n---\n" + b"\xff" * 100_000
            ),
            "skills/eager/SKILL.md": (
                b"---\nname: eager\ndescription: Always active\nalways_on: true\n---\nfull body"
            ),
        }
    )

    skills = await _load_skills(
        workspace,
        user_id=uuid4(),
        cache=SkillsCache(),
    )

    assert [(skill.name, skill.body) for skill in skills] == [
        ("conditional", ""),
        ("eager", "full body"),
    ]
    assert workspace.read_requests == [
        ("skills/conditional/SKILL.md", MAX_SKILL_FRONTMATTER_PREFIX_BYTES + 1),
        ("skills/eager/SKILL.md", MAX_SKILL_FRONTMATTER_PREFIX_BYTES + 1),
        ("skills/eager/SKILL.md", ALWAYS_ON_MAX_BYTES + 1),
    ]


async def test_malformed_examined_manifest_fails_the_complete_snapshot() -> None:
    workspace = _PromptWorkspace(
        {
            "skills/good/SKILL.md": b"---\nname: good\ndescription: Valid\n---\nbody",
            "skills/zbad/SKILL.md": b"not frontmatter",
        }
    )

    with pytest.raises(WorkspaceError) as exc_info:
        await _load_skills(
            workspace,
            user_id=uuid4(),
            cache=SkillsCache(),
        )

    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


async def test_deeply_nested_skill_yaml_is_normalized_during_prompt_load() -> None:
    nested = b"[" * 500 + b"]" * 500
    workspace = _PromptWorkspace(
        {
            "skills/reviewer/SKILL.md": (
                b"---\nname: reviewer\ndescription: " + nested + b"\n---\nbody"
            )
        }
    )

    with pytest.raises(WorkspaceError) as exc_info:
        await _load_skills(
            workspace,
            user_id=uuid4(),
            cache=SkillsCache(),
        )

    assert exc_info.value.code is ErrorCode.WORKSPACE_INVALID_SKILL_FORMAT


async def test_cancelled_header_parse_holds_slot_until_worker_exits(monkeypatch) -> None:
    started = threading.Event()
    release = threading.Event()
    calls: list[str] = []

    def parse(path: str, content: bytes):
        del content
        calls.append(path)
        started.set()
        assert release.wait(timeout=2)
        return prompt_module.SkillInfo("reviewer", "review", False, "", path)

    monkeypatch.setattr(prompt_module, "_SKILL_PARSE_SLOTS", asyncio.Semaphore(1))
    monkeypatch.setattr(prompt_module, "parse_skill_manifest_header", parse)

    first = asyncio.create_task(_parse_skill_header("skills/reviewer/SKILL.md", b"first"))
    assert await asyncio.to_thread(started.wait, 1)
    first.cancel()
    second = asyncio.create_task(_parse_skill_header("skills/reviewer/SKILL.md", b"second"))
    try:
        await asyncio.sleep(0.05)
        assert calls == ["skills/reviewer/SKILL.md"]
    finally:
        release.set()

    with pytest.raises(asyncio.CancelledError):
        await first
    assert (await second).name == "reviewer"


async def test_prompt_renders_all_always_on_bodies_without_aggregate_downgrade() -> None:
    from openctopus_server.chat.prompt import _render_skills
    from openctopus_server.workspace.skills import SkillInfo

    body = "x" * 64_001
    rendered = _render_skills(
        (
            SkillInfo("one", "first", True, body, "skills/one/SKILL.md"),
            SkillInfo("two", "second", True, body, "skills/two/SKILL.md"),
        )
    )

    assert "### one (always-on)\n\n" + body in rendered
    assert "### two (always-on)\n\n" + body in rendered
    assert "### Conditional skills" not in rendered
