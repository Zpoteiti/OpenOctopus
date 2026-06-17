# Py0 — Server Skeleton Design

**Status:** design-approved
**Milestone:** Py0
**Date:** 2026-06-17
**Depends on:** Py-Setup (complete)

## Purpose

Produce a minimal but working FastAPI server skeleton: connect to PostgreSQL, apply the full 11-table schema on an empty database, and serve `/health`. No auth, no chat, no agent loop. The output is a single `pyproject.toml` + `src/openoctopus_server/` tree that passes lint, type-check, and test gate on every push.

## What's in / out

### In scope

| Deliverable | Detail |
|---|---|
| FastAPI app | `main.py` creates app, mounts routes, registers startup event |
| `/health` endpoint | Returns `{"status":"ok","db":"connected"}` after verifying DB reachable |
| SQLAlchemy models | 11 declarative models matching SCHEMA.md (see table below) |
| DB bootstrap | On startup: if DB is empty → `Base.metadata.create_all()` → 11 tables + indexes |
| Config module | `pydantic-settings` with `.env` file, fails fast on missing required vars |
| DTOs | `session.py`, `message.py`, `error.py` — Pydantic models for API shapes |
| `ErrorCode` enum | Full StrEnum per DECISIONS.md (40-ish values), living in `errors/codes.py` |
| Exception hierarchy | `OpenOctopusError` → `WorkspaceError`, `ToolError`, `NetworkError`, `ProtocolError`, `McpError`, `AuthError` |
| Truncation helper | `tools/truncate.py` — `truncate_head(text, max_chars=16000)` pure function |
| Anthropic wire types | 6 content block Pydantic models + `Effort` enum (see below) |
| `.env` file | All required env vars filled with dev defaults |
| CI gate | `ruff`, `mypy (strict)`, `pytest` — all green |

### Out of scope (later milestones)

- Auth, login, JWT — Py1
- `POST/GET messages` — Py2
- Agent loop, tool registry, merge — Py3
- Workspace files, MinIO — Py4a/Py4
- Client, channels, cron — Py5+

## Project structure

```
openoctopus/
  server/
    .env                          # dev credentials (see §Config)
    pyproject.toml
    tests/
      conftest.py                 # async SQLite fixture for model tests + async client for API tests
      test_health.py              # /health 200
      test_schema_bootstrap.py    # create_all on empty DB, diff against SCHEMA.md
      test_wire_types.py          # content block serialize/deserialize round-trip
      test_truncate.py            # truncate_head edge cases
      test_error_codes.py         # uniqueness check
    src/
      openoctopus_server/
        __init__.py
        main.py                   # FastAPI app, startup event, shutdown
        config.py                 # Settings via pydantic-settings, load .env
        api/
          __init__.py
          router.py               # APIRouter
          health.py               # GET /health
        db/
          __init__.py
          engine.py               # create_async_engine, session factory
          models.py               # 11 SQLAlchemy declarative models
        dto/
          __init__.py
          session.py              # SessionResponse, SessionListResponse
          message.py              # MessageResponse, PostMessageRequest
          error.py                # ErrorResponse
        errors/
          __init__.py
          codes.py                # ErrorCode StrEnum
          exceptions.py           # OpenOctopusError hierarchy
        provider/
          __init__.py
          wire_types.py           # ContentBlock types, Effort enum
        tools/
          __init__.py
          truncate.py             # truncate_head()
  client/                         # (future)
  docs/                           # (existing)
```

## Config module

```python
# server/src/openoctopus_server/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # PostgreSQL (Py0)
    database_url: str  # postgresql+asyncpg://...

    # Server
    host: str          # 127.0.0.1
    port: int          # 8080

    # Auth (Py1 — read, not used in Py0)
    jwt_secret: str
    cookie_secure: bool = False

    # Object Storage (Py4 — read, not used in Py0)
    object_storage_endpoint: str
    object_storage_bucket: str
    object_storage_region: str
    object_storage_access_key: str
    object_storage_secret_key: str
```

- `Settings()` reads from `.env` file at startup.
- All fields are required. Missing any → `ValidationError` → startup fails.
- `SettingsConfigDict(env_file=".env")` auto-loads the `.env` in the working directory.
- CI/container deployments set env vars directly (no `.env` file).

## DB models (11 tables from SCHEMA.md)

All models live in `db/models.py` as SQLAlchemy 2.0 declarative classes with `Mapped[]` column types.

| Table | Key columns | Notes |
|---|---|---|
| `system_config` | `key TEXT PK`, `value JSONB`, `updated_at` | No seed rows |
| `users` | `id UUID PK`, `email`, `password_hash`, `name`, `is_admin`, `created_at` | No seed rows |
| `discord_configs` | `user_id UUID PK FK→users` | No seed rows |
| `telegram_configs` | `user_id UUID PK FK→users` | No seed rows |
| `sessions` | `id UUID PK`, `user_id FK`, `session_key UNIQUE`, `channel`, `chat_id`, `title`, `last_read_at`, `cancel_requested`, `created_at` | No seed rows |
| `messages` | `id UUID PK`, `session_id FK`, `role`, `message_kind`, `content JSONB`, `delivery_refs JSONB`, `is_compaction_summary`, `created_at` | No seed rows |
| `pending_messages` | `id UUID PK`, `session_id FK`, `user_id FK`, `session_key`, `content JSONB`, `effort`, `received_at` | No seed rows |
| `devices` | `token TEXT PK`, `user_id FK`, `name UNIQUE per user`, `workspace_path`, `sandbox_mode`, `shell_timeout_max`, `ssrf_denylist JSONB`, `env_allowlist JSONB`, `command_denylist JSONB`, `mcp_servers JSONB`, `created_at` | No seed rows |
| `workspaces` | `id UUID PK`, `name`, `quota_bytes`, `created_by FK→users`, `created_at` | No seed rows |
| `workspace_members` | `workspace_id FK + user_id FK` composite PK | No seed rows |
| `cron_jobs` | `id UUID PK`, `user_id FK`, `session_id FK`, `name`, `message`, `schedule JSONB`, `next_fire_at`, `last_fired_at`, `created_at` | No seed rows |

Constraints modeled:
- `users.email UNIQUE`
- `sessions.session_key UNIQUE`
- `devices UNIQUE(user_id, name)`, `devices.name CHECK (is slug)`
- `messages CHECK (role IN ('user','assistant'))`, `messages CHECK (message_kind IN (...))`
- `pending_messages CHECK (effort IN (...))`
- FK `ON DELETE CASCADE` on all user-referencing FKs (except `workspaces.created_by`)
- All indexes from SCHEMA.md

Implementation notes:
- Models are `mapped_column()` with `nullable=False` where SCHEMA.md says `NOT NULL`
- JSONB columns use `sqlalchemy.dialects.postgresql.JSONB` type
- `CHECK` constraints use SQLAlchemy `CheckConstraint` where the ORM can't express them natively
- No relationships defined in Py0 (none needed until queries exist)

## Bootstrap logic

On startup event:

```python
async def on_startup():
    async with engine.begin() as conn:
        # Check if this is a fresh DB (no tables exist)
        await conn.run_sync(Base.metadata.create_all)
```

`create_all()` is no-op if tables exist. On fresh DB, creates all 11 tables + indexes. No Alembic in Py0 (deferred per ADR-069).

Health check:

```python
@router.get("/health")
async def health():
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected"}
    except Exception:
        return JSONResponse(
            {"status": "error", "db": "disconnected"},
            status_code=503,
        )
```

## Anthropic wire types

6 content block types + discriminated union for the `messages.content` / `pending_messages.content` JSONB columns.

```python
# provider/wire_types.py

from enum import StrEnum
from pydantic import BaseModel, Field
from typing import Annotated, Literal

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
    data: str  # base64-encoded bytes

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
    input: dict

class ToolResultBlock(BaseModel):
    type: Literal["tool_result"] = "tool_result"
    tool_use_id: str
    content: str | list  # string or array of text/image blocks
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

- `ContentBlock` is the annotated discriminated union. Pass it as the type for `messages.content` JSONB columns.
- `ToolResultBlock.content` is `str | list` — Python-main allows both raw string and safe block array (per ADR-095 normalisation).

## Error codes + exceptions

```python
# errors/codes.py
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
# errors/exceptions.py
class OpenOctopusError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        self.code = code
        self.message = message
        super().__init__(message)

class WorkspaceError(OpenOctopusError): ...
class ToolError(OpenOctopusError): ...
class NetworkError(OpenOctopusError): ...
class ProtocolError(OpenOctopusError): ...
class McpError(OpenOctopusError): ...
class AuthError(OpenOctopusError): ...
```

## Truncation helper

```python
# tools/truncate.py

DEFAULT_MAX_TOOL_RESULT_CHARS: int = 16_000
TRUNCATION_MARKER: str = "\n... (truncated)"

def truncate_head(text: str, max_chars: int = DEFAULT_MAX_TOOL_RESULT_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + TRUNCATION_MARKER
```

Pure function. Head-only per ADR-076. Character count, not token count.

## DTOs (API shapes)

`dto/session.py`:
```python
class SessionResponse(BaseModel):
    id: UUID
    session_key: str
    channel: str
    chat_id: str
    title: str | None
    unread: bool
    created_at: datetime
```

`dto/message.py`:
```python
class PostMessageRequest(BaseModel):
    content: str

class MessageResponse(BaseModel):
    id: UUID
    role: str
    message_kind: str
    content: list  # ContentBlock array parsed from JSONB
    created_at: datetime
```

`dto/error.py`:
```python
class ErrorResponse(BaseModel):
    code: str       # ErrorCode value
    message: str
    detail: dict | None = None
```

Py0 only defines the shapes. Routes that use them land in Py1+.

## Dependencies

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
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
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

## Testing

### Test infrastructure

- `conftest.py` provides an async fixture that creates a fresh SQLite in-memory DB with `create_all()`, yields an async session, and drops after each test.
- API tests use `httpx.AsyncClient` against the FastAPI app (TestClient pattern with `httpx.ASGITransport`).

### Test suite

| Test file | What it verifies |
|---|---|
| `test_health.py` | `GET /health` → 200, body has `status: "ok"` |
| `test_schema_bootstrap.py` | `create_all()` produces correct table names and column counts matching SCHEMA.md |
| `test_wire_types.py` | Each content block type serializes to JSON and back; discriminated union works |
| `test_truncate.py` | `truncate_head("hello", 10)` → no-op; `truncate_head("x" * 20000, 100)` → truncated with marker |
| `test_error_codes.py` | Every ErrorCode value is unique; every exception class exists |

### Schema bootstrap test

The schema test runs `create_all()` on an empty SQLite, then introspects with `inspect(engine).get_table_names()` and `get_columns()`. It verifies:
- 11 table names match SCHEMA.md
- Column counts per table match
- This is a lightweight diff — it catches accidental model drift but doesn't validate exact DDL types (SQLite vs PG differences)

For full PG type validation, the CI pipeline runs the same test against a real PostgreSQL container.

### CI gate (GitHub Actions)

```yaml
name: py0
on: [push, pull_request]
jobs:
  check:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:18
        env:
          POSTGRES_USER: openoctopus
          POSTGRES_PASSWORD: octopus
          POSTGRES_DB: openoctopus
        ports: [5432:5432]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install -e "server/[dev]"
      - run: ruff check server/
      - run: mypy server/
      - run: pytest server/tests/ -v
        env:
          DATABASE_URL: postgresql+asyncpg://openoctopus:octopus@localhost:5432/openoctopus
```

## Dev setup

```bash
# Terminal 1: PostgreSQL
docker run --rm --name oo-pg \
  -e POSTGRES_USER=openoctopus \
  -e POSTGRES_PASSWORD=octopus \
  -e POSTGRES_DB=openoctopus \
  -p 5432:5432 \
  postgres:18

# Terminal 2: MinIO (optional in Py0, required by Py4)
docker run --rm --name oo-minio \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -p 9000:9000 -p 9001:9001 \
  minio/minio server /data --console-address ":9001"

# Terminal 3: Server
cd server
cp .env.example .env   # or create from scratch
pip install -e ".[dev]"
python -m openoctopus_server.main
# → http://127.0.0.1:8080/health
```

## Open questions (deferred)

- Exact Pydantic version floor — `>=2.0` is broad; will tighten at implementation time
- `ContentBlock` JSONB serialization in SQLAlchemy — need a `TypeDecorator` to convert Pydantic models ↔ JSON. Implementation detail, not design concern
- `pytest-asyncio` mode — `"auto"` may need `"strict"` if fixture scoping gets complex
