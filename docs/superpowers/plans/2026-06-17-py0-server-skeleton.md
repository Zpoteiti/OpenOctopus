# Py0 Server Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the minimal FastAPI + SQLAlchemy server skeleton defined in `docs/superpowers/specs/2026-06-17-py0-server-skeleton-design.md`: connect to PostgreSQL, create all 11 tables on startup, expose `/health`, and pass lint/type-check/pytest on every push.

**Architecture:** A single Python package `openoctopus_server` under `server/src/`. Config is loaded lazily via `pydantic-settings` with `OPENOCTOPUS_` prefix. Database uses async SQLAlchemy 2.0 with `asyncpg`; startup creates `pgcrypto` then `create_all()`. Models, API routes, DTOs, errors, and tools live in focused sub-packages. Tests run against a real PostgreSQL database created per session.

**Tech Stack:** Python 3.12+, FastAPI, Uvicorn, SQLAlchemy 2.0 (async), asyncpg, pydantic 2.9+, pydantic-settings 2.6+, pytest, pytest-asyncio, httpx, ruff, mypy (strict).

---

## File map

| File | Responsibility |
|---|---|
| `server/pyproject.toml` | Package metadata, dependencies, tool configs |
| `server/.env` | Dev env vars (not committed) |
| `server/src/openoctopus_server/__init__.py` | Package init |
| `server/src/openoctopus_server/config.py` | `Settings` + `get_settings()` |
| `server/src/openoctopus_server/db/base.py` | `DeclarativeBase` |
| `server/src/openoctopus_server/db/engine.py` | Async engine + session factory |
| `server/src/openoctopus_server/db/models.py` | 11 SQLAlchemy models |
| `server/src/openoctopus_server/errors/codes.py` | `ErrorCode` StrEnum |
| `server/src/openoctopus_server/errors/exceptions.py` | Exception hierarchy |
| `server/src/openoctopus_server/tools/truncate.py` | `truncate_head()` |
| `server/src/openoctopus_server/provider/wire_types.py` | Anthropic content block models |
| `server/src/openoctopus_server/dto/session.py` | Session DTOs |
| `server/src/openoctopus_server/dto/message.py` | Message DTOs |
| `server/src/openoctopus_server/dto/error.py` | Error DTO |
| `server/src/openoctopus_server/api/router.py` | APIRouter assembly |
| `server/src/openoctopus_server/api/health.py` | `GET /health` |
| `server/src/openctopus_server/main.py` | FastAPI app + startup/shutdown |
| `server/tests/conftest.py` | PG fixture + async client |
| `server/tests/snapshots/error_codes.json` | Canonical ErrorCode snapshot |
| `server/tests/test_config.py` | `extra="forbid"` guard |
| `server/tests/test_truncate.py` | Truncation unit tests |
| `server/tests/test_wire_types.py` | Content block round-trip tests |
| `server/tests/test_error_codes.py` | Enum uniqueness + snapshot |
| `server/tests/test_schema_bootstrap.py` | Table/column count vs SCHEMA.md |
| `server/tests/test_health.py` | `/health` 200/503 |
| `.github/workflows/py0.yml` | CI gate |

---

### Task 1: Bootstrap project and dependencies

**Files:**
- Create: `server/pyproject.toml`
- Create: `server/.env` (from example; gitignored later)
- Create: `server/src/openoctopus_server/__init__.py`
- Test: `server/tests/test_config.py` (will be filled in Task 2)

- [ ] **Step 1: Write the project metadata file**

```toml
# server/pyproject.toml
[project]
name = "openoctopus-server"
version = "0.0.1"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.34",
    "sqlalchemy[asyncio]>=2.0",
    "asyncpg>=0.30",
    "pydantic>=2.9",
    "pydantic-settings>=2.6",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "httpx>=0.28",
    "ruff>=0.8",
    "mypy>=1.13",
]

[tool.ruff]
target-version = "py312"
line-length = 100
lint.select = ["E", "F", "I", "N", "W", "UP"]
lint.ignore = ["E501"]

[tool.mypy]
strict = true
python_version = "3.12"

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- [ ] **Step 2: Create package init and dev env file**

```python
# server/src/openoctopus_server/__init__.py
"""OpenOctopus Python server."""
```

```bash
# server/.env
OPENOCTOPUS_DATABASE_URL=postgresql+asyncpg://openoctopus:octopus@localhost:5432/openoctopus
OPENOCTOPUS_DATABASE_POOL_SIZE=5
OPENOCTOPUS_DATABASE_MAX_OVERFLOW=10
OPENOCTOPUS_DATABASE_POOL_TIMEOUT=30
OPENOCTOPUS_DATABASE_POOL_PRE_PING=true
OPENOCTOPUS_HOST=127.0.0.1
OPENOCTOPUS_PORT=8080
OPENOCTOPUS_JWT_SECRET=change-me-in-production
OPENOCTOPUS_COOKIE_SECURE=false
OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT=localhost:9000
OPENOCTOPUS_OBJECT_STORAGE_BUCKET=openoctopus
OPENOCTOPUS_OBJECT_STORAGE_REGION=us-east-1
OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY=minioadmin
OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY=minioadmin
```

- [ ] **Step 3: Verify install works**

Run:
```bash
cd server
pip install -e ".[dev]"
python -c "import openoctopus_server; print('ok')"
```

Expected: prints `ok` with no import errors.

- [ ] **Step 4: Commit**

```bash
git add server/pyproject.toml server/src/openoctopus_server/__init__.py server/.env
git commit -m "chore: bootstrap Py0 server package and dependencies"
```

---

### Task 2: Config module with env_prefix and forbid

**Files:**
- Create: `server/src/openoctopus_server/config.py`
- Create: `server/tests/test_config.py`

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_config.py
import os

import pytest
from pydantic import ValidationError

from openoctopus_server.config import Settings, get_settings


def test_settings_rejects_typo_env_var(monkeypatch):
    # Provide all required vars
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_SIZE", "5")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_MAX_OVERFLOW", "10")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_TIMEOUT", "30")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_PRE_PING", "true")
    monkeypatch.setenv("OPENOCTOPUS_HOST", "127.0.0.1")
    monkeypatch.setenv("OPENOCTOPUS_PORT", "8080")
    monkeypatch.setenv("OPENOCTOPUS_JWT_SECRET", "secret")
    monkeypatch.setenv("OPENOCTOPUS_COOKIE_SECURE", "false")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY", "key")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY", "secret")
    # Typo
    monkeypatch.setenv("OPENOCTOPUS_HTST", "127.0.0.1")
    with pytest.raises(ValidationError):
        get_settings()


def test_settings_loads_with_openoctopus_prefix(monkeypatch):
    monkeypatch.setenv("OPENOCTOPUS_HOST", "0.0.0.0")
    monkeypatch.setenv("OPENOCTOPUS_PORT", "9000")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_URL", "postgresql+asyncpg://u:p@localhost/db")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_SIZE", "5")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_MAX_OVERFLOW", "10")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_TIMEOUT", "30")
    monkeypatch.setenv("OPENOCTOPUS_DATABASE_POOL_PRE_PING", "true")
    monkeypatch.setenv("OPENOCTOPUS_JWT_SECRET", "secret")
    monkeypatch.setenv("OPENOCTOPUS_COOKIE_SECURE", "false")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_BUCKET", "bucket")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_REGION", "us-east-1")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY", "key")
    monkeypatch.setenv("OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY", "secret")
    settings = Settings()
    assert settings.host == "0.0.0.0"
    assert settings.port == 9000
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_config.py -v
```

Expected: `ImportError` for `openoctopus_server.config`.

- [ ] **Step 3: Implement config module**

```python
# server/src/openctopus_server/config.py
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        env_prefix="OPENOCTOPUS_",
    )

    # PostgreSQL (Py0) — all required, no defaults
    database_url: str
    database_pool_size: int
    database_max_overflow: int
    database_pool_timeout: int
    database_pool_pre_ping: bool

    # Server — required
    host: str
    port: int

    # Auth (Py1 — read, Py0 placeholder)
    jwt_secret: str
    cookie_secure: bool

    # Object Storage (Py4 — read, Py0 placeholder)
    object_storage_endpoint: str
    object_storage_bucket: str
    object_storage_region: str
    object_storage_access_key: str
    object_storage_secret_key: str


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd server
pytest tests/test_config.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/config.py server/tests/test_config.py
git commit -m "feat: pydantic-settings config with OPENOCTOPUS_ prefix and forbid guard"
```

---

### Task 3: Database base, engine, and session factory

**Files:**
- Create: `server/src/openctopus_server/db/base.py`
- Create: `server/src/openctopus_server/db/engine.py`
- Create: `server/src/openctopus_server/db/__init__.py`
- Modify: `server/tests/conftest.py` (created here, extended later)

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_engine.py
import pytest
from sqlalchemy import text

from openctopus_server.db.engine import get_engine


@pytest.mark.asyncio
async def test_engine_can_select_one():
    engine = get_engine()
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT 1"))
        assert result.scalar_one() == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_engine.py -v
```

Expected: `ImportError` for `openctopus_server.db.engine`.

- [ ] **Step 3: Implement base and engine**

```python
# server/src/openctopus_server/db/base.py
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
```

```python
# server/src/openctopus_server/db/engine.py
from sqlalchemy.ext.asyncio import create_async_engine

from openctopus_server.config import get_settings


def get_engine():
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout,
        pool_pre_ping=settings.database_pool_pre_ping,
    )
```

```python
# server/src/openctopus_server/db/__init__.py
"""Database package."""
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd server
OPENOCTOPUS_DATABASE_URL=postgresql+asyncpg://openctopus:octopus@localhost:5432/openctopus \
OPENOCTOPUS_DATABASE_POOL_SIZE=5 \
OPENOCTOPUS_DATABASE_MAX_OVERFLOW=10 \
OPENOCTOPUS_DATABASE_POOL_TIMEOUT=30 \
OPENOCTOPUS_DATABASE_POOL_PRE_PING=true \
OPENOCTOPUS_HOST=127.0.0.1 \
OPENOCTOPUS_PORT=8080 \
OPENOCTOPUS_JWT_SECRET=secret \
OPENOCTOPUS_COOKIE_SECURE=false \
OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT=localhost:9000 \
OPENOCTOPUS_OBJECT_STORAGE_BUCKET=bucket \
OPENOCTOPUS_OBJECT_STORAGE_REGION=us-east-1 \
OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY=key \
OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY=secret \
pytest tests/test_engine.py -v
```

Expected: 1 passed (requires PostgreSQL running).

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/db/ server/tests/test_engine.py
git commit -m "feat: async SQLAlchemy base and engine factory"
```

---

### Task 4: Error codes enum and snapshot

**Files:**
- Create: `server/src/openctopus_server/errors/codes.py`
- Create: `server/src/openctopus_server/errors/__init__.py`
- Create: `server/tests/snapshots/error_codes.json`
- Create: `server/tests/test_error_codes.py`

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_error_codes.py
import json
from pathlib import Path

from openctopus_server.errors.codes import ErrorCode


def test_error_codes_match_snapshot():
    snapshot_path = Path(__file__).parent / "snapshots" / "error_codes.json"
    snapshot = json.loads(snapshot_path.read_text())
    current = {e.name: e.value for e in ErrorCode}
    assert current == snapshot


def test_all_exception_classes_exist():
    from openctopus_server.errors.exceptions import (
        AuthError,
        McpError,
        NetworkError,
        OpenOctopusError,
        ProtocolError,
        ToolError,
        WorkspaceError,
    )

    assert issubclass(WorkspaceError, OpenOctopusError)
    assert issubclass(ToolError, OpenOctopusError)
    assert issubclass(NetworkError, OpenOctopusError)
    assert issubclass(ProtocolError, OpenOctopusError)
    assert issubclass(McpError, OpenOctopusError)
    assert issubclass(AuthError, OpenOctopusError)
```

Create `server/tests/snapshots/error_codes.json` with the expected content (will be generated after codes.py exists, or hand-written from spec). For now, include an empty object `{}` so the test fails meaningfully.

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_error_codes.py -v
```

Expected: `ImportError` for `openctopus_server.errors.codes`.

- [ ] **Step 3: Implement error codes and exceptions**

```python
# server/src/openctopus_server/errors/codes.py
from enum import StrEnum


class ErrorCode(StrEnum):
    # Workspace
    WORKSPACE_NOT_FOUND = "workspace_not_found"
    WORKSPACE_PERMISSION_DENIED = "workspace_permission_denied"
    WORKSPACE_SYMLINK_ESCAPE = "workspace_symlink_escape"
    WORKSPACE_SOFT_LOCKED = "workspace_soft_locked"
    WORKSPACE_UPLOAD_TOO_LARGE = "workspace_upload_too_large"
    WORKSPACE_INVALID_SKILL_FORMAT = "workspace_invalid_skill_format"
    WORKSPACE_BLOCKED_PATH = "workspace_blocked_path"
    # Tool
    TOOL_AMBIGUOUS_EDIT = "tool_ambiguous_edit"
    TOOL_NO_MATCH = "tool_no_match"
    TOOL_IS_DIRECTORY = "tool_is_directory"
    TOOL_IS_FILE = "tool_is_file"
    TOOL_NOT_A_DIRECTORY = "tool_not_a_directory"
    TOOL_INVALID_NOTEBOOK = "tool_invalid_notebook"
    TOOL_CELL_INDEX_OUT_OF_RANGE = "tool_cell_index_out_of_range"
    TOOL_INVALID_ARGS = "tool_invalid_args"
    TOOL_INVALID_REGEX = "tool_invalid_regex"
    TOOL_INVALID_GLOB = "tool_invalid_glob"
    TOOL_EXEC_TIMEOUT = "tool_exec_timeout"
    TOOL_COMMAND_DENIED = "tool_command_denied"
    TOOL_ENV_NOT_ALLOWED = "tool_env_not_allowed"
    TOOL_CWD_OUTSIDE_WORKSPACE = "tool_cwd_outside_workspace"
    TOOL_PATH_OUTSIDE_WORKSPACE = "tool_path_outside_workspace"
    TOOL_DEVICE_UNREACHABLE = "tool_device_unreachable"
    TOOL_CHANNEL_NOT_CONFIGURED = "tool_channel_not_configured"
    TOOL_UNSUPPORTED_MEDIA = "tool_unsupported_media"
    TOOL_DELIVERY_FAILED = "tool_delivery_failed"
    TOOL_INVALID_SCHEDULE = "tool_invalid_schedule"
    TOOL_MISSING_REQUIRED_FIELD = "tool_missing_required_field"
    TOOL_DB_ERROR = "tool_db_error"
    TOOL_CRON_JOB_NOT_FOUND = "tool_cron_job_not_found"
    TOOL_MCP_UNAVAILABLE = "tool_mcp_unavailable"
    # Network
    NETWORK_SSRF_BLOCKED = "network_ssrf_blocked"
    NETWORK_DNS_FAILED = "network_dns_failed"
    NETWORK_TIMEOUT = "network_timeout"
    NETWORK_HTTP_ERROR = "network_http_error"
    # Protocol
    PROTOCOL_MALFORMED_FRAME = "protocol_malformed_frame"
    PROTOCOL_UNKNOWN_TYPE = "protocol_unknown_type"
    PROTOCOL_VERSION_MISMATCH = "protocol_version_mismatch"
    PROTOCOL_TRANSFER_UNKNOWN_ID = "protocol_transfer_unknown_id"
    # MCP
    MCP_WITHIN_SERVER_COLLISION = "mcp_within_server_collision"
    MCP_SCHEMA_COLLISION = "mcp_schema_collision"
    MCP_SPAWN_FAILED = "mcp_spawn_failed"
    # Auth
    AUTH_UNAUTHORIZED = "auth_unauthorized"
    AUTH_LAST_ADMIN_REQUIRED = "auth_last_admin_required"
    # System
    SERVER_RESTART = "server_restart"
    USER_CANCELLED = "user_cancelled"
```

```python
# server/src/openctopus_server/errors/exceptions.py
from openctopus_server.errors.codes import ErrorCode


class OpenOctopusError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class WorkspaceError(OpenOctopusError):
    pass


class ToolError(OpenOctopusError):
    pass


class NetworkError(OpenOctopusError):
    pass


class ProtocolError(OpenOctopusError):
    pass


class McpError(OpenOctopusError):
    pass


class AuthError(OpenOctopusError):
    pass
```

```python
# server/src/openctopus_server/errors/__init__.py
"""Errors package."""
```

- [ ] **Step 4: Generate snapshot and run tests**

Run:
```bash
cd server
python -c "
import json
from openctopus_server.errors.codes import ErrorCode
print(json.dumps({e.name: e.value for e in ErrorCode}, indent=2, sort_keys=True))
" > tests/snapshots/error_codes.json
pytest tests/test_error_codes.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/errors/ server/tests/test_error_codes.py server/tests/snapshots/error_codes.json
git commit -m "feat: ErrorCode enum, exception hierarchy, and snapshot test"
```

---

### Task 5: Truncation helper

**Files:**
- Create: `server/src/openctopus_server/tools/truncate.py`
- Create: `server/src/openctopus_server/tools/__init__.py`
- Create: `server/tests/test_truncate.py`

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_truncate.py
from openctopus_server.tools.truncate import TRUNCATION_MARKER, truncate_head


def test_truncate_head_noop_when_under_limit():
    assert truncate_head("hello", 10) == "hello"


def test_truncate_head_truncates_with_marker():
    text = "x" * 20000
    result = truncate_head(text, 100)
    assert len(result) == 100 + len(TRUNCATION_MARKER)
    assert result.endswith(TRUNCATION_MARKER)
    assert result.startswith("x" * 100)
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_truncate.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement truncate helper**

```python
# server/src/openctopus_server/tools/truncate.py
DEFAULT_MAX_TOOL_RESULT_CHARS: int = 16_000
TRUNCATION_MARKER: str = "\n... (truncated)"


def truncate_head(text: str, max_chars: int = DEFAULT_MAX_TOOL_RESULT_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + TRUNCATION_MARKER
```

```python
# server/src/openctopus_server/tools/__init__.py
"""Tools package."""
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd server
pytest tests/test_truncate.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/tools/ server/tests/test_truncate.py
git commit -m "feat: truncate_head helper with head-only truncation"
```

---

### Task 6: Anthropic wire types

**Files:**
- Create: `server/src/openctopus_server/provider/wire_types.py`
- Create: `server/src/openctopus_server/provider/__init__.py`
- Create: `server/tests/test_wire_types.py`

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_wire_types.py
import json

import pydantic
import pytest

from openctopus_server.provider.wire_types import (
    ContentBlock,
    Effort,
    ImageBlock,
    TextBlock,
    ToolResultBlock,
)


def test_effort_enum_values():
    assert Effort.LOW == "low"
    assert Effort.MAX == "max"


def test_text_block_round_trip():
    block = TextBlock(text="hello")
    data = json.loads(block.model_dump_json())
    parsed = pydantic.TypeAdapter(ContentBlock).validate_python(data)
    assert isinstance(parsed, TextBlock)
    assert parsed.text == "hello"


def test_image_block_round_trip():
    block = ImageBlock(
        source={"type": "base64", "media_type": "image/png", "data": "abc123"}
    )
    data = json.loads(block.model_dump_json())
    parsed = pydantic.TypeAdapter(ContentBlock).validate_python(data)
    assert isinstance(parsed, ImageBlock)
    assert parsed.source.data == "abc123"


def test_tool_result_block_accepts_string_or_list():
    block_str = ToolResultBlock(tool_use_id="1", content="plain text")
    assert block_str.content == "plain text"
    block_list = ToolResultBlock(tool_use_id="1", content=[{"type": "text", "text": "hi"}])
    assert block_list.content[0].text == "hi"


def test_content_block_discriminator_rejects_unknown_type():
    with pytest.raises(pydantic.ValidationError):
        pydantic.TypeAdapter(ContentBlock).validate_python({"type": "unknown"})
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_wire_types.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement wire types**

```python
# server/src/openctopus_server/provider/wire_types.py
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field


class Effort(StrEnum):
    OFF = "off"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    XHIGH = "xhigh"
    MAX = "max"


class Base64ImageSource(BaseModel):
    type: Literal["base64"] = "base64"
    media_type: Literal["image/jpeg", "image/png", "image/gif", "image/webp"]
    data: str


class TextBlock(BaseModel):
    type: Literal["text"] = "text"
    text: str


class ImageBlock(BaseModel):
    type: Literal["image"] = "image"
    source: Base64ImageSource


class ToolUseBlock(BaseModel):
    type: Literal["tool_use"] = "tool_use"
    id: str
    name: str
    input: dict[str, Any]


class ToolResultBlock(BaseModel):
    type: Literal["tool_result"] = "tool_result"
    tool_use_id: str
    content: str | list[Any]
    is_error: bool = False


class ThinkingBlock(BaseModel):
    type: Literal["thinking"] = "thinking"
    thinking: str
    signature: str


class RedactedThinkingBlock(BaseModel):
    type: Literal["redacted_thinking"] = "redacted_thinking"
    data: str


ContentBlock = Annotated[
    TextBlock | ImageBlock | ToolUseBlock | ToolResultBlock | ThinkingBlock | RedactedThinkingBlock,
    Field(discriminator="type"),
]
```

```python
# server/src/openctopus_server/provider/__init__.py
"""Provider package."""
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd server
pytest tests/test_wire_types.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/provider/ server/tests/test_wire_types.py
git commit -m "feat: Anthropic content block wire types and discriminated union"
```

---

### Task 7: DTOs

**Files:**
- Create: `server/src/openctopus_server/dto/session.py`
- Create: `server/src/openctopus_server/dto/message.py`
- Create: `server/src/openctopus_server/dto/error.py`
- Create: `server/src/openctopus_server/dto/__init__.py`
- Create: `server/tests/test_dtos.py`

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_dtos.py
from datetime import datetime, timezone
from uuid import uuid4

from openctopus_server.dto.error import ErrorResponse
from openctopus_server.dto.message import MessageResponse, PostMessageRequest
from openctopus_server.dto.session import SessionResponse


def test_post_message_request():
    req = PostMessageRequest(content="hello")
    assert req.content == "hello"


def test_message_response():
    msg = MessageResponse(
        id=uuid4(),
        role="user",
        message_kind="human",
        content=[{"type": "text", "text": "hi"}],
        created_at=datetime.now(timezone.utc),
    )
    assert msg.role == "user"


def test_session_response():
    sess = SessionResponse(
        id=uuid4(),
        session_key="key",
        channel="web",
        chat_id="chat",
        title="title",
        unread=False,
        created_at=datetime.now(timezone.utc),
    )
    assert sess.channel == "web"


def test_error_response():
    err = ErrorResponse(code="workspace_not_found", message="not found", detail={"path": "/x"})
    assert err.detail == {"path": "/x"}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_dtos.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement DTOs**

```python
# server/src/openctopus_server/dto/session.py
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class SessionResponse(BaseModel):
    id: UUID
    session_key: str
    channel: str
    chat_id: str
    title: str | None
    unread: bool
    created_at: datetime
```

```python
# server/src/openctopus_server/dto/message.py
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from ..provider.wire_types import ContentBlock


class PostMessageRequest(BaseModel):
    content: str


class MessageResponse(BaseModel):
    id: UUID
    role: str
    message_kind: str
    content: list[ContentBlock]
    created_at: datetime
```

```python
# server/src/openctopus_server/dto/error.py
from typing import Any

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    code: str
    message: str
    detail: dict[str, Any] | None = None
```

```python
# server/src/openctopus_server/dto/__init__.py
"""DTO package."""
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd server
pytest tests/test_dtos.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/dto/ server/tests/test_dtos.py
git commit -m "feat: session, message, and error DTOs"
```

---

### Task 8: SQLAlchemy models — system_config, users, discord_configs, telegram_configs

**Files:**
- Modify: `server/src/openctopus_server/db/models.py`
- Modify: `server/tests/conftest.py`
- Test: `server/tests/test_schema_bootstrap.py` (created here, finalized later)

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_schema_bootstrap.py
import pytest
from sqlalchemy import inspect

from openctopus_server.db.base import Base
from openctopus_server.db.engine import get_engine


@pytest.mark.asyncio
async def test_tables_exist(pg_engine):
    async with pg_engine.connect() as conn:
        tables = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names())
        expected = {
            "system_config",
            "users",
            "discord_configs",
            "telegram_configs",
            "sessions",
            "messages",
            "pending_messages",
            "devices",
            "workspaces",
            "workspace_members",
            "cron_jobs",
        }
        assert expected.issubset(set(tables))
```

`pg_engine` fixture will be created in Task 22; for now, this test will fail with `FixtureLookupError`.

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py -v
```

Expected: `FixtureLookupError: 'pg_engine'`.

- [ ] **Step 3: Implement first four models**

```python
# server/src/openctopus_server/db/models.py
import uuid

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func, text

from openctopus_server.db.base import Base


class SystemConfig(Base):
    __tablename__ = "system_config"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="FALSE")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class DiscordConfig(Base):
    __tablename__ = "discord_configs"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    bot_token: Mapped[str] = mapped_column(Text, nullable=False)
    partner_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    allow_list: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class TelegramConfig(Base):
    __tablename__ = "telegram_configs"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    bot_token: Mapped[str] = mapped_column(Text, nullable=False)
    partner_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    allow_list: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
```

Note: add `from datetime import datetime` at the top of models.py.

- [ ] **Step 4: Run test to verify it still fails (fixture missing)**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py -v
```

Expected: still fails because `pg_engine` fixture does not exist yet.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/db/models.py server/tests/test_schema_bootstrap.py
git commit -m "feat(models): system_config, users, discord_configs, telegram_configs"
```

---

### Task 9: SQLAlchemy models — sessions and messages

**Files:**
- Modify: `server/src/openctopus_server/db/models.py`

- [ ] **Step 1: Write the failing test**

Extend `server/tests/test_schema_bootstrap.py`:

```python
EXPECTED_COLUMNS = {
    "sessions": 10,
    "messages": 10,
}


@pytest.mark.asyncio
async def test_column_counts(pg_engine):
    async with pg_engine.connect() as conn:
        for table, expected in EXPECTED_COLUMNS.items():
            cols = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_columns(table)
            )
            assert len(cols) == expected, f"{table} expected {expected} columns, got {len(cols)}"
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py::test_column_counts -v
```

Expected: `FixtureLookupError`.

- [ ] **Step 3: Implement sessions and messages models**

Append to `server/src/openctopus_server/db/models.py`:

```python
class Session(Base):
    __tablename__ = "sessions"
    __table_args__ = (
        UniqueConstraint("user_id", "session_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    session_key: Mapped[str] = mapped_column(Text, nullable=False)
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="'New chat'"
    )
    last_inbound_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    last_read_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    cancel_requested: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="FALSE"
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        CheckConstraint("role IN ('user', 'assistant')"),
        CheckConstraint(
            "message_kind IN ('human', 'assistant', 'tool_result', "
            "'synthetic_tool_result', 'synthetic_assistant_error', 'compaction_summary')"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False)
    message_kind: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[dict] = mapped_column(JSONB, nullable=False)
    delivery_refs: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    llm_fingerprint: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_compaction_summary: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="FALSE"
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
```

- [ ] **Step 4: Run test to verify it still fails (fixture missing)**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py -v
```

Expected: still fails because `pg_engine` fixture does not exist yet.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/db/models.py server/tests/test_schema_bootstrap.py
git commit -m "feat(models): sessions and messages"
```

---

### Task 10: SQLAlchemy models — pending_messages and devices

**Files:**
- Modify: `server/src/openctopus_server/db/models.py`

- [ ] **Step 1: Write the failing test**

Extend `EXPECTED_COLUMNS` in `server/tests/test_schema_bootstrap.py`:

```python
EXPECTED_COLUMNS = {
    "sessions": 10,
    "messages": 10,
    "pending_messages": 8,
    "devices": 13,
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py::test_column_counts -v
```

Expected: AssertionError for `pending_messages` / `devices` columns or fixture error.

- [ ] **Step 3: Implement pending_messages and devices models**

Append to `server/src/openctopus_server/db/models.py`:

```python
class PendingMessage(Base):
    __tablename__ = "pending_messages"
    __table_args__ = (
        CheckConstraint(
            "effort IS NULL OR effort IN ('off', 'low', 'medium', 'high', 'xhigh', 'max')"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    session_key: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[dict] = mapped_column(JSONB, nullable=False)
    effort: Mapped[str | None] = mapped_column(Text, nullable=True)
    received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class Device(Base):
    __tablename__ = "devices"
    __table_args__ = (
        CheckConstraint(
            "name ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND name <> 'server'"
        ),
        UniqueConstraint("user_id", "name"),
    )

    token: Mapped[str] = mapped_column(Text, primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    workspace_path: Mapped[str] = mapped_column(Text, nullable=False)
    sandbox_mode: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="TRUE"
    )
    shell_timeout_max: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="600",
    )
    ssrf_denylist: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text(
            "'[\"127.0.0.0/8\",\"::1/128\",\"10.0.0.0/8\","
            "\"172.16.0.0/12\",\"192.168.0.0/16\",\"100.64.0.0/10\","
            "\"169.254.0.0/16\",\"169.254.169.254/32\",\"fc00::/7\",\"fe80::/10\"]'::jsonb"
        ),
    )
    env_allowlist: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[\"PATH\",\"HOME\",\"LANG\",\"TERM\"]'::jsonb"),
    )
    command_denylist: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text(
            "'[\"shutdown\",\"reboot\",\"halt\",\"poweroff\","
            "\"mkfs\",\"dd\",\"mount\",\"umount\",\"systemctl\",\"service\"]'::jsonb"
        ),
    )
    mcp_servers: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
```

- [ ] **Step 4: Run test to verify it still fails (fixture missing)**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py -v
```

Expected: still fails because `pg_engine` fixture does not exist yet.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/db/models.py server/tests/test_schema_bootstrap.py
git commit -m "feat(models): pending_messages and devices"
```

---

### Task 11: SQLAlchemy models — workspaces, workspace_members, cron_jobs + indexes

**Files:**
- Modify: `server/src/openctopus_server/db/models.py`

- [ ] **Step 1: Write the failing test**

Extend `EXPECTED_COLUMNS` in `server/tests/test_schema_bootstrap.py`:

```python
EXPECTED_COLUMNS = {
    "sessions": 10,
    "messages": 10,
    "pending_messages": 8,
    "devices": 13,
    "workspaces": 5,
    "workspace_members": 3,
    "cron_jobs": 11,
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py::test_column_counts -v
```

Expected: AssertionError or fixture error.

- [ ] **Step 3: Implement remaining models and indexes**

Append to `server/src/openctopus_server/db/models.py`:

```python
class Workspace(Base):
    __tablename__ = "workspaces"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    quota_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class WorkspaceMember(Base):
    __tablename__ = "workspace_members"

    workspace_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    joined_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CronJob(Base):
    __tablename__ = "cron_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("sessions.id"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    schedule: Mapped[str] = mapped_column(Text, nullable=False)
    tz: Mapped[str | None] = mapped_column(Text, nullable=True)
    one_shot: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="FALSE"
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    last_fired_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    next_fire_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


# Indexes from SCHEMA.md §Indexes summary
Index("idx_sessions_user_id", Session.user_id)
Index(
    "idx_sessions_user_session_key",
    Session.user_id,
    Session.session_key,
    unique=True,
)
Index("idx_messages_session_created", Message.session_id, Message.created_at)
Index(
    "idx_pending_messages_session_received",
    PendingMessage.session_id,
    PendingMessage.received_at,
    PendingMessage.id,
)
Index(
    "idx_pending_messages_session_key_received",
    PendingMessage.session_key,
    PendingMessage.received_at,
    PendingMessage.id,
)
Index("idx_devices_user_id", Device.user_id)
Index("idx_workspace_members_user", WorkspaceMember.user_id)
Index("idx_cron_jobs_user_id", CronJob.user_id)
Index(
    "idx_cron_jobs_next_fire",
    CronJob.next_fire_at,
    postgresql_where=text("next_fire_at IS NOT NULL"),
)
```

- [ ] **Step 4: Run test to verify it still fails (fixture missing)**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py -v
```

Expected: still fails because `pg_engine` fixture does not exist yet.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/db/models.py server/tests/test_schema_bootstrap.py
git commit -m "feat(models): workspaces, workspace_members, cron_jobs, and indexes"
```

---

### Task 12: Add CHECK constraint for shell_timeout_max

**Files:**
- Modify: `server/src/openctopus_server/db/models.py`

- [ ] **Step 1: Write the failing test**

Extend `server/tests/test_schema_bootstrap.py`:

```python
@pytest.mark.asyncio
async def test_shell_timeout_max_check(pg_engine):
    from sqlalchemy import text

    async with pg_engine.begin() as conn:
        with pytest.raises(Exception):
            await conn.execute(
                text("INSERT INTO devices (token, user_id, name, workspace_path, shell_timeout_max) VALUES ('t1', gen_random_uuid(), 'dev', '/path', -1)")
            )
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py::test_shell_timeout_max_check -v
```

Expected: FixtureLookupError or the negative insert succeeds (if fixture existed).

- [ ] **Step 3: Add CHECK constraint to Device model**

Update `Device.__table_args__` in `server/src/openctopus_server/db/models.py`:

```python
__table_args__ = (
    CheckConstraint(
        "name ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND name <> 'server'"
    ),
    CheckConstraint("shell_timeout_max >= 0"),
    UniqueConstraint("user_id", "name"),
)
```

- [ ] **Step 4: Run test to verify it still fails (fixture missing)**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py -v
```

Expected: still fails because `pg_engine` fixture does not exist yet.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/db/models.py server/tests/test_schema_bootstrap.py
git commit -m "fix(models): add shell_timeout_max >= 0 CHECK"
```

---

### Task 13: Health endpoint

**Files:**
- Create: `server/src/openctopus_server/api/__init__.py`
- Create: `server/src/openctopus_server/api/router.py`
- Create: `server/src/openctopus_server/api/health.py`
- Create: `server/tests/test_health.py`

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_health.py
import pytest


@pytest.mark.asyncio
async def test_health_returns_ok(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "db": "connected"}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_health.py -v
```

Expected: `FixtureLookupError` for `async_client`.

- [ ] **Step 3: Implement health endpoint and router**

```python
# server/src/openctopus_server/api/__init__.py
"""API package."""
```

```python
# server/src/openctopus_server/api/health.py
import asyncio

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from openctopus_server.db.engine import get_engine

router = APIRouter()


@router.get("/health")
async def health(engine: AsyncEngine = Depends(get_engine)):
    try:
        async with asyncio.wait_for(
            _check_db(engine),
            timeout=2.0,
        ):
            return {"status": "ok", "db": "connected"}
    except asyncio.TimeoutError:
        return {"status": "error", "db": "disconnected"}, 503


async def _check_db(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
```

Wait, FastAPI `Depends` with async engine factory needs care. `get_engine()` returns an engine synchronously, so it's fine. But the dependency injection should be `engine = Depends(get_engine)`. However, `get_engine` calls `get_settings()` which needs env vars. That's okay if env vars are set.

Actually, `Depends(get_engine)` works because `get_engine()` returns an `AsyncEngine` immediately.

But the timeout wrapper is wrong. `asyncio.wait_for` takes a coroutine, not an async context manager. Let me fix:

```python
@router.get("/health")
async def health(engine: AsyncEngine = Depends(get_engine)):
    try:
        await asyncio.wait_for(_check_db(engine), timeout=2.0)
    except (asyncio.TimeoutError, Exception):
        return {"status": "error", "db": "disconnected"}, 503
    return {"status": "ok", "db": "connected"}


async def _check_db(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
```

But catching bare `Exception` is bad. Better:

```python
from sqlalchemy.exc import DBAPIError, TimeoutError as SATimeoutError

@router.get("/health")
async def health(engine: AsyncEngine = Depends(get_engine)):
    try:
        await asyncio.wait_for(_check_db(engine), timeout=2.0)
    except (asyncio.TimeoutError, DBAPIError, SATimeoutError):
        return {"status": "error", "db": "disconnected"}, 503
    return {"status": "ok", "db": "connected"}
```

```python
# server/src/openctopus_server/api/router.py
from fastapi import APIRouter

from openctopus_server.api import health

router = APIRouter()
router.include_router(health.router)
```

- [ ] **Step 4: Run test to verify it still fails (fixture missing)**

Run:
```bash
cd server
pytest tests/test_health.py -v
```

Expected: still fails because `async_client` fixture does not exist yet.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/api/ server/tests/test_health.py
git commit -m "feat: /health endpoint with DB check and timeout"
```

---

### Task 14: FastAPI main.py with startup/shutdown

**Files:**
- Create: `server/src/openctopus_server/main.py`
- Modify: `server/tests/test_health.py`

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_main.py
import pytest


@pytest.mark.asyncio
async def test_app_has_health_route(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_main.py -v
```

Expected: `FixtureLookupError` for `async_client`.

- [ ] **Step 3: Implement main.py**

```python
# server/src/openctopus_server/main.py
import asyncio
import sys

from fastapi import FastAPI
from sqlalchemy import text

from openctopus_server.api.router import router as api_router
from openctopus_server.config import get_settings
from openctopus_server.db.base import Base
from openctopus_server.db.engine import get_engine


def create_app() -> FastAPI:
    app = FastAPI(title="OpenOctopus")
    app.include_router(api_router)

    @app.on_event("startup")
    async def startup() -> None:
        try:
            settings = get_settings()
        except Exception as exc:
            print(f"Config validation failed: {exc}", file=sys.stderr)
            sys.exit(1)

        engine = get_engine()
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
                await conn.run_sync(Base.metadata.create_all)
        except Exception as exc:
            print(f"Database bootstrap failed: {exc}", file=sys.stderr)
            sys.exit(1)

    @app.on_event("shutdown")
    async def shutdown() -> None:
        engine = get_engine()
        await engine.dispose()

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(app, host=settings.host, port=settings.port)
```

- [ ] **Step 4: Run test to verify it still fails (fixture missing)**

Run:
```bash
cd server
pytest tests/test_main.py -v
```

Expected: still fails because `async_client` fixture does not exist yet.

- [ ] **Step 5: Commit**

```bash
git add server/src/openctopus_server/main.py server/tests/test_main.py
git commit -m "feat: FastAPI app with startup DB bootstrap and /health route"
```

---

### Task 15: Test fixtures (conftest.py)

**Files:**
- Create: `server/tests/conftest.py`
- Modify: all test files to use fixtures

- [ ] **Step 1: Write the failing test**

`test_schema_bootstrap.py` already references `pg_engine`. Run it to confirm fixture is missing.

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py -v
```

Expected: `FixtureLookupError: 'pg_engine'`.

- [ ] **Step 2: Implement conftest.py**

```python
# server/tests/conftest.py
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from openctopus_server.config import Settings, get_settings
from openctopus_server.db.base import Base
from openctopus_server.main import create_app


@pytest.fixture(scope="session")
def admin_database_url():
    settings = get_settings()
    # Replace path with /postgres for admin connection
    url = settings.database_url.rsplit("/", 1)[0] + "/postgres"
    return url


@pytest_asyncio.fixture(scope="session")
async def pg_engine(admin_database_url):
    settings = get_settings()
    test_db_name = f"oo_test_{uuid.uuid4().hex[:8]}"

    admin_engine = create_async_engine(
        admin_database_url,
        isolation_level="AUTOCOMMIT",
    )
    async with admin_engine.begin() as conn:
        await conn.execute(text(f'CREATE DATABASE "{test_db_name}"'))

    test_url = settings.database_url.rsplit("/", 1)[0] + f"/{test_db_name}"
    engine = create_async_engine(test_url)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    await engine.dispose()
    async with admin_engine.begin() as conn:
        await conn.execute(text(f'DROP DATABASE "{test_db_name}"'))
    await admin_engine.dispose()


@pytest_asyncio.fixture
async def db_session(pg_engine):
    from sqlalchemy.ext.asyncio import AsyncSession

    async with AsyncSession(pg_engine) as session:
        yield session


@pytest_asyncio.fixture
async def async_client(pg_engine):
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
```

- [ ] **Step 3: Run tests to verify fixtures work**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py tests/test_health.py -v
```

Expected: all pass (requires PostgreSQL running).

- [ ] **Step 4: Commit**

```bash
git add server/tests/conftest.py
git commit -m "test: session-scoped PostgreSQL fixture and async client"
```

---

### Task 16: Finalize schema bootstrap tests

**Files:**
- Modify: `server/tests/test_schema_bootstrap.py`

- [ ] **Step 1: Write comprehensive schema test**

```python
# server/tests/test_schema_bootstrap.py
import pytest
from sqlalchemy import inspect

EXPECTED_COLUMNS = {
    "system_config": 3,
    "users": 6,
    "discord_configs": 5,
    "telegram_configs": 5,
    "sessions": 10,
    "messages": 10,
    "pending_messages": 8,
    "devices": 13,
    "workspaces": 5,
    "workspace_members": 3,
    "cron_jobs": 11,
}


@pytest.mark.asyncio
async def test_all_tables_exist(pg_engine):
    async with pg_engine.connect() as conn:
        tables = set(await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names()))
    expected = set(EXPECTED_COLUMNS)
    assert expected.issubset(tables)


@pytest.mark.asyncio
async def test_column_counts(pg_engine):
    async with pg_engine.connect() as conn:
        for table, expected in EXPECTED_COLUMNS.items():
            cols = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_columns(table)
            )
            assert len(cols) == expected, f"{table}: expected {expected}, got {len(cols)}"


@pytest.mark.asyncio
async def test_indexes_exist(pg_engine):
    async with pg_engine.connect() as conn:
        indexes = await conn.run_sync(
            lambda sync_conn: {
                (idx["name"], idx["table_name"])
                for idx in inspect(sync_conn).get_indexes()
            }
        )
    expected = {
        ("users_email_key", "users"),
        ("idx_sessions_user_id", "sessions"),
        ("idx_sessions_user_session_key", "sessions"),
        ("idx_messages_session_created", "messages"),
        ("idx_pending_messages_session_received", "pending_messages"),
        ("idx_pending_messages_session_key_received", "pending_messages"),
        ("idx_devices_user_id", "devices"),
        ("devices_user_id_name_key", "devices"),
        ("idx_workspace_members_user", "workspace_members"),
        ("idx_cron_jobs_user_id", "cron_jobs"),
        ("idx_cron_jobs_next_fire", "cron_jobs"),
    }
    assert expected.issubset(indexes)


@pytest.mark.asyncio
async def test_shell_timeout_max_check(pg_engine):
    from sqlalchemy import text

    async with pg_engine.begin() as conn:
        with pytest.raises(Exception):
            await conn.execute(
                text(
                    "INSERT INTO devices (token, user_id, name, workspace_path, shell_timeout_max) "
                    "VALUES ('t1', gen_random_uuid(), 'dev', '/path', -1)"
                )
            )
```

- [ ] **Step 2: Run test to verify it passes**

Run:
```bash
cd server
pytest tests/test_schema_bootstrap.py -v
```

Expected: 4 passed.

- [ ] **Step 3: Commit**

```bash
git add server/tests/test_schema_bootstrap.py
git commit -m "test: schema bootstrap coverage for tables, columns, indexes, and CHECK"
```

---

### Task 17: Health endpoint integration test with 503 path

**Files:**
- Modify: `server/tests/test_health.py`

- [ ] **Step 1: Write the failing test**

```python
# server/tests/test_health.py
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_health_returns_ok(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "db": "connected"}


@pytest.mark.asyncio
async def test_health_returns_503_when_db_check_times_out(async_client):
    with patch(
        "openctopus_server.api.health._check_db",
        new_callable=AsyncMock,
        side_effect=TimeoutError,
    ):
        response = await async_client.get("/health")
    assert response.status_code == 503
    assert response.json()["db"] == "disconnected"
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd server
pytest tests/test_health.py -v
```

Expected: second test fails because `_check_db` is not imported/patched correctly or health returns wrong status.

- [ ] **Step 3: Adjust health endpoint if needed**

If the test fails because the health endpoint doesn't catch `TimeoutError`, update `server/src/openctopus_server/api/health.py` to catch `asyncio.TimeoutError` explicitly. The current implementation already catches it.

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd server
pytest tests/test_health.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add server/tests/test_health.py server/src/openctopus_server/api/health.py
git commit -m "test: /health 200 and 503 timeout path"
```

---

### Task 18: Remove temporary test_engine.py

**Files:**
- Delete: `server/tests/test_engine.py`

- [ ] **Step 1: Remove the file**

```bash
rm server/tests/test_engine.py
```

- [ ] **Step 2: Verify tests still pass**

Run:
```bash
cd server
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git rm server/tests/test_engine.py
git commit -m "chore: remove temporary engine smoke test"
```

---

### Task 19: CI workflow

**Files:**
- Create: `.github/workflows/py0.yml`

- [ ] **Step 1: Create the workflow file**

```yaml
# .github/workflows/py0.yml
name: py0
on: [push, pull_request]
jobs:
  check:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:18.4
        env:
          POSTGRES_USER: openoctopus
          POSTGRES_PASSWORD: octopus
          POSTGRES_DB: openoctopus
        ports: [5432:5432]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: cd server && pip install -e ".[dev]"
      - run: cd server && ruff check src/
      - run: cd server && mypy src/
      - run: cd server && pytest tests/ -v
        env:
          OPENOCTOPUS_DATABASE_URL: postgresql+asyncpg://openctopus:octopus@localhost:5432/openctopus
          OPENOCTOPUS_DATABASE_POOL_SIZE: 5
          OPENOCTOPUS_DATABASE_MAX_OVERFLOW: 10
          OPENOCTOPUS_DATABASE_POOL_TIMEOUT: 30
          OPENOCTOPUS_DATABASE_POOL_PRE_PING: true
          OPENOCTOPUS_HOST: 127.0.0.1
          OPENOCTOPUS_PORT: 8080
          OPENOCTOPUS_JWT_SECRET: ci-secret
          OPENOCTOPUS_COOKIE_SECURE: false
          OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT: localhost:9000
          OPENOCTOPUS_OBJECT_STORAGE_BUCKET: openoctopus
          OPENOCTOPUS_OBJECT_STORAGE_REGION: us-east-1
          OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY: minioadmin
          OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY: minioadmin
```

- [ ] **Step 2: Validate YAML syntax**

Run:
```bash
python -c "import yaml; yaml.safe_load(open('.github/workflows/py0.yml'))"
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/py0.yml
git commit -m "ci: py0 GitHub Actions workflow"
```

---

### Task 20: Lint and type-check

**Files:**
- Modify: any files needed to satisfy ruff/mypy

- [ ] **Step 1: Run ruff**

Run:
```bash
cd server
ruff check src/ tests/
```

Expected: no errors. If errors, fix them.

- [ ] **Step 2: Run mypy**

Run:
```bash
cd server
mypy src/ tests/
```

Expected: no errors. Common issues:
- Missing `from datetime import datetime` in models.py
- Type of `ContentBlock` in DTOs
- `Depends(get_engine)` type annotations

- [ ] **Step 3: Commit fixes**

```bash
git add -A
git commit -m "style: ruff and mypy compliance"
```

---

### Task 21: Full test suite run

**Files:**
- None

- [ ] **Step 1: Run all tests**

Start PostgreSQL locally if not running:
```bash
docker run --rm --name oo-pg \
  -e POSTGRES_USER=openctopus \
  -e POSTGRES_PASSWORD=octopus \
  -e POSTGRES_DB=openctopus \
  -p 5432:5432 \
  postgres:18.4
```

In another terminal:
```bash
cd server
pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 2: Run local server smoke test**

```bash
cd server
python -m openctopus_server.main &
curl http://127.0.0.1:8080/health
```

Expected: `{"status":"ok","db":"connected"}`.

- [ ] **Step 3: Commit any final fixes**

```bash
git add -A
git commit -m "test: full Py0 test suite green"
```

---

## Self-review

### Spec coverage

| Spec requirement | Implementing task |
|---|---|
| `pyproject.toml` with deps/tool config | Task 1 |
| `.env` with dev defaults | Task 1 |
| Config module `OPENOCTOPUS_` prefix, `extra="forbid"`, lazy `get_settings()` | Task 2 |
| `db/base.py` DeclarativeBase | Task 3 |
| `db/engine.py` async engine with pool settings | Task 3 |
| 11 SQLAlchemy models matching SCHEMA.md columns/defaults/CHECK/indexes | Tasks 8-12 |
| `pgcrypto` creation + `create_all()` on startup | Task 14 |
| `/health` with DB check and timeout | Tasks 13, 17 |
| `ErrorCode` enum + exception hierarchy + snapshot | Task 4 |
| `truncate_head()` | Task 5 |
| Anthropic wire types | Task 6 |
| DTOs | Task 7 |
| Tests: config typo, truncate, wire types, error codes, schema, health | Tasks 2, 5, 6, 4, 16, 17 |
| PG-only fixture with per-session DB | Task 15 |
| CI gate | Task 19 |

### Placeholder scan

- No `TBD`, `TODO`, or "implement later" in this plan.
- All code steps include actual code.
- Exact file paths are used throughout.

### Type consistency

- `Settings` fields match env vars via `OPENOCTOPUS_` prefix.
- `get_engine()` returns `AsyncEngine` consistently.
- `Base` is imported from `db/base.py` in `models.py` and `main.py`.
- `ContentBlock` is used as `list[ContentBlock]` in `MessageResponse`.

### Known gaps / risks

1. **mypy strict**: `Depends(get_engine)` may need an explicit return type annotation on `get_engine()` or a cast. Adjust in Task 20.
2. **pytest-asyncio fixture scoping**: `pg_engine` is session-scoped and async; ensure `pytest_asyncio` is configured with `asyncio_mode = "auto"` (done in pyproject.toml).
3. **Health 503 test**: the patch path assumes `_check_db` is importable from `openctopus_server.api.health`. If implementation moves it, update the patch path.

---

## Execution handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-17-py0-server-skeleton.md`.**

Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using `executing-plans`, batch execution with checkpoints.

Which approach?
