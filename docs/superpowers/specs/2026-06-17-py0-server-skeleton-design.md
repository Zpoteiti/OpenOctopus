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
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="forbid")

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
```

- `extra="forbid"` — rejects unknown env vars, catches typos.
- All fields are required (no defaults). Missing any → `ValidationError` → `sys.exit(1)`.
- `SettingsConfigDict(env_file=".env")` auto-loads `.env` in working directory.
- CI/container deployments set env vars directly (no `.env` file).

### `.env` file (dev)

```bash
# PostgreSQL — required (no defaults)
DATABASE_URL=postgresql+asyncpg://openoctopus:octopus@localhost:5432/openoctopus
DATABASE_POOL_SIZE=5
DATABASE_MAX_OVERFLOW=10
DATABASE_POOL_TIMEOUT=30
DATABASE_POOL_PRE_PING=true

# Server — required
OPENOCTOPUS_HOST=127.0.0.1
OPENOCTOPUS_PORT=8080

# Auth (Py1 — read, Py0 placeholder)
OPENOCTOPUS_JWT_SECRET=change-me-in-production
OPENOCTOPUS_COOKIE_SECURE=false

# Object Storage - MinIO (Py4 — read, Py0 placeholder)
OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT=localhost:9000
OPENOCTOPUS_OBJECT_STORAGE_BUCKET=openoctopus
OPENOCTOPUS_OBJECT_STORAGE_REGION=us-east-1
OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY=minioadmin
OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY=minioadmin
```

## DB models (11 tables from SCHEMA.md)

All models live in `db/models.py` as SQLAlchemy 2.0 declarative classes with `Mapped[]` column types. Every column from SCHEMA.md is declared — Py0 creates the tables but reads/writes none. Fields unused in Py0 carry `# Py0 placeholder` comments.

| Table | Columns | Notes |
|---|---|---|
| `system_config` | `key TEXT PK`, `value JSONB NOT NULL`, `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | No seed rows. Py0 placeholder. |
| `users` | `id UUID PK DEFAULT gen_random_uuid()`, `email TEXT NOT NULL UNIQUE`, `password_hash TEXT NOT NULL`, `name TEXT NOT NULL`, `is_admin BOOLEAN NOT NULL DEFAULT FALSE`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | No seed rows. Py0 placeholder. |
| `discord_configs` | `user_id UUID PK FK→users ON DELETE CASCADE`, `bot_token TEXT NOT NULL`, `partner_chat_id TEXT NOT NULL`, `allow_list JSONB NOT NULL DEFAULT '[]'`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | No seed rows. Py0 placeholder. |
| `telegram_configs` | `user_id UUID PK FK→users ON DELETE CASCADE`, `bot_token TEXT NOT NULL`, `partner_chat_id TEXT NOT NULL`, `allow_list JSONB NOT NULL DEFAULT '[]'`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | No seed rows. Py0 placeholder. |
| `sessions` | `id UUID PK DEFAULT gen_random_uuid()`, `user_id UUID NOT NULL FK→users ON DELETE CASCADE`, `session_key TEXT NOT NULL`, `channel TEXT NOT NULL`, `chat_id TEXT NOT NULL`, `title TEXT NOT NULL DEFAULT 'New chat'`, `last_inbound_at TIMESTAMPTZ`, `last_read_at TIMESTAMPTZ`, `cancel_requested BOOLEAN NOT NULL DEFAULT FALSE`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | UNIQUE(user_id, session_key). Py0 placeholder. |
| `messages` | `id UUID PK DEFAULT gen_random_uuid()`, `session_id UUID NOT NULL FK→sessions ON DELETE CASCADE`, `role TEXT NOT NULL CHECK (IN ('user','assistant'))`, `message_kind TEXT NOT NULL CHECK (IN ('human','assistant','tool_result','synthetic_tool_result','synthetic_assistant_error','compaction_summary'))`, `content JSONB NOT NULL`, `delivery_refs JSONB NOT NULL DEFAULT '[]'`, `llm_fingerprint TEXT`, `is_compaction_summary BOOLEAN NOT NULL DEFAULT FALSE`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Py0 placeholder. |
| `pending_messages` | `id UUID PK DEFAULT gen_random_uuid()`, `session_id UUID NOT NULL FK→sessions ON DELETE CASCADE`, `user_id UUID NOT NULL FK→users ON DELETE CASCADE`, `session_key TEXT NOT NULL`, `content JSONB NOT NULL`, `effort TEXT CHECK (effort IS NULL OR effort IN ('off','low','medium','high','xhigh','max'))`, `received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Py0 placeholder. |
| `devices` | `token TEXT PK`, `user_id UUID NOT NULL FK→users ON DELETE CASCADE`, `name TEXT NOT NULL CHECK (slug regex) CHECK (name <> 'server')`, `workspace_path TEXT NOT NULL`, `sandbox_mode BOOLEAN NOT NULL DEFAULT TRUE`, `shell_timeout_max INTEGER NOT NULL DEFAULT 600 CHECK (>=0)`, `ssrf_denylist JSONB NOT NULL DEFAULT [...]`, `env_allowlist JSONB NOT NULL DEFAULT [...]`, `command_denylist JSONB NOT NULL DEFAULT [...]`, `mcp_servers JSONB NOT NULL DEFAULT '{}'`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`, UNIQUE(user_id, name) | Py0 placeholder. |
| `workspaces` | `id UUID PK DEFAULT gen_random_uuid()`, `name TEXT NOT NULL`, `quota_bytes BIGINT NOT NULL`, `created_by UUID FK→users ON DELETE SET NULL` (exception to ADR-058), `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Py0 placeholder. |
| `workspace_members` | `workspace_id UUID NOT NULL FK→workspaces ON DELETE CASCADE`, `user_id UUID NOT NULL FK→users ON DELETE CASCADE`, `joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`, PRIMARY KEY(workspace_id, user_id) | Py0 placeholder. |
| `cron_jobs` | `id UUID PK DEFAULT gen_random_uuid()`, `user_id UUID NOT NULL FK→users ON DELETE CASCADE`, `session_id UUID NOT NULL FK→sessions` (RESTRICT — 有 cron job 时不能删 session), `name TEXT NOT NULL`, `schedule TEXT NOT NULL`, `tz TEXT`, `one_shot BOOLEAN NOT NULL DEFAULT FALSE`, `message TEXT NOT NULL`, `last_fired_at TIMESTAMPTZ`, `next_fire_at TIMESTAMPTZ NOT NULL`, `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Py0 placeholder. |

All indexes from SCHEMA.md §Indexes summary are created via `Index()` in model metadata.

Implementation notes:
- `mapped_column()` with `nullable=False` where SCHEMA.md says `NOT NULL`
- JSONB columns use `sqlalchemy.dialects.postgresql.JSONB`
- `CHECK` constraints use SQLAlchemy `CheckConstraint` where the ORM can't express them natively
- No relationships defined in Py0 (none needed until queries exist — Py1+)
- `gen_random_uuid()` default requires `server_default=text("gen_random_uuid()")`

## Startup sequence

1. Load config from `.env` + os.environ via `pydantic-settings`. Any missing required field → `ValidationError` → `sys.exit(1)`.
2. Create async engine with pool settings from config.
3. Try `async with engine.connect() as conn: await conn.execute(text("SELECT 1"))`. If this fails → log error, `sys.exit(1)`. No retry. Config is wrong; admin must fix.
4. `await conn.run_sync(Base.metadata.create_all)` — idempotent, no-op if tables exist. On fresh DB creates all 11 tables + indexes.
5. Start uvicorn, listen on `{host}:{port}`.

`/health` runs the same `SELECT 1` check on every call — returns 200 if connected, 503 if the connection died (pool exhausted, network lost, etc.).

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

All tests run against a real PostgreSQL database. Local dev starts PG via Docker; CI uses a service container. Tests that need a DB use a session-scoped fixture that creates a fresh DB (`template openoctopus_test` → `CREATE DATABASE ... TEMPLATE`) per test run, runs `create_all()`, and drops at the end. No SQLite — the production backend is PostgreSQL and only PG exercises JSONB, CHECK constraints, and PG-specific index types correctly.

API tests use `httpx.AsyncClient` against the FastAPI app (TestClient pattern with `httpx.ASGITransport`).

### Test suite

| Test file | What it verifies |
|---|---|
| `test_health.py` | `GET /health` → 200, body has `status: "ok"` |
| `test_schema_bootstrap.py` | `create_all()` on empty PG → 11 tables present; table names and column counts match SCHEMA.md §1–§11 |
| `test_wire_types.py` | Each content block type serializes to JSON and back; discriminated union works |
| `test_truncate.py` | `truncate_head("hello", 10)` → no-op; `truncate_head("x" * 20000, 100)` → truncated with marker |
| `test_error_codes.py` | Every ErrorCode value is unique; every exception class exists |

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
          DATABASE_POOL_SIZE: 5
          DATABASE_MAX_OVERFLOW: 10
          DATABASE_POOL_TIMEOUT: 30
          DATABASE_POOL_PRE_PING: true
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
# Create .env with all required vars (see .env section above)
pip install -e ".[dev]"

# Start with env vars (or use .env)
DATABASE_URL=postgresql+asyncpg://openoctopus:octopus@localhost:5432/openoctopus \
DATABASE_POOL_SIZE=5 \
DATABASE_MAX_OVERFLOW=10 \
DATABASE_POOL_TIMEOUT=30 \
DATABASE_POOL_PRE_PING=true \
OPENOCTOPUS_HOST=127.0.0.1 \
OPENOCTOPUS_PORT=8080 \
OPENOCTOPUS_JWT_SECRET=dev-secret \
OPENOCTOPUS_COOKIE_SECURE=false \
OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT=localhost:9000 \
OPENOCTOPUS_OBJECT_STORAGE_BUCKET=openoctopus \
OPENOCTOPUS_OBJECT_STORAGE_REGION=us-east-1 \
OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY=minioadmin \
OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY=minioadmin \
python -m openoctopus_server.main
# → http://127.0.0.1:8080/health
```

## Open questions (deferred)

- Exact Pydantic version floor — `>=2.0` is broad; will tighten at implementation time
- `ContentBlock` JSONB serialization in SQLAlchemy — need a `TypeDecorator` to convert Pydantic models ↔ JSON. Implementation detail, not design concern
- `pytest-asyncio` mode — `"auto"` may need `"strict"` if fixture scoping gets complex
