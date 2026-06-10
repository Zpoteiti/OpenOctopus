# Plexus M1a Server Foundation Sub-Spec

**Status:** Verified; see the M1 living tracker for current status/evidence
**Parent:** [Plexus M1 Living Design Spec](2026-05-12-plexus-m1-living-design.md)
**Branch:** `rebuild-m1`
**Authors:** brainstormed in collaborative session 2026-05-12
**Supersedes:** none

---

## 1. Goal

M1a creates the first real `plexus-server` slice: a server crate that can start,
connect to PostgreSQL, initialize an empty database from the canonical schema,
run real authentication, and persist selected REST/admin writes.

The success proof is deliberately narrow: an empty database becomes usable on
server startup, users can register and log in with real credentials, admin-only
config routes enforce real auth, authenticated user profile writes persist, and
every accepted config write lands in `system_config` and reads back.

M1a does not implement the agent loop, LLM calls, chat streaming, workspace file
operations, device WebSocket, channel adapters, MCP, cron, or heartbeat.

---

## 2. Non-Goals

M1a does not include:

- LLM provider calls or `/models` validation.
- Independent mock OpenAI-compatible provider service.
- REST message ingress or SSE streams.
- Workspace file APIs or server-side file tools.
- Device token lifecycle or device WebSocket.
- Discord or Telegram adapters.
- MCP runtime management.
- Cron scheduler or heartbeat ticker.
- Production migration framework.
- Test-only auth bypasses, in-memory DBs, or stubbed persistence.

These are later sub-milestones. M1a should reject not-yet-supported writes
explicitly rather than silently accepting invalid or partially implemented
behavior.

---

## 3. Implementation Shape

Add a `plexus-server` crate to the workspace:

```text
plexus-server/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── lib.rs
│   ├── app.rs
│   ├── config.rs
│   ├── error.rs
│   ├── auth/
│   │   ├── mod.rs
│   │   ├── jwt.rs
│   │   └── password.rs
│   ├── db/
│   │   ├── mod.rs
│   │   ├── schema.sql
│   │   ├── users.rs
│   │   └── system_config.rs
│   └── routes/
│       ├── mod.rs
│       ├── auth.rs
│       ├── me.rs
│       └── admin.rs
└── tests/
    └── api_m1a.rs
```

The exact file split can change during implementation if a simpler layout
emerges, but the ownership boundaries should remain:

- `app` builds the Axum router and shared state.
- `config` loads process configuration.
- `auth` owns password hashing, JWT issue/verify, and auth extractors.
- `db` owns SQL bootstrap and typed query helpers.
- `routes` owns request/response DTOs and calls into `auth`/`db`.
- `error` adapts centralized Plexus errors to HTTP responses.

---

## 4. Dependencies

M1a should use conventional Rust server crates:

- `axum` for HTTP routing.
- `tokio` from the workspace.
- `sqlx` with Postgres, runtime Tokio, UUID, chrono/time, JSON support.
- `argon2` for password hashing.
- `jsonwebtoken` for JWT issue/verify.
- `tower-http` only if needed for tracing/cookies later; avoid speculative middleware.
- `serde`, `serde_json`, `uuid`, `thiserror`, `secrecy`, and `plexus-common`.

Use real PostgreSQL in tests. Do not use SQLite as a proxy for Postgres because
the schema uses Postgres-specific UUID, JSONB, and index behavior.

---

## 5. Process Configuration

M1a needs only the process-level settings required to start the server and issue
auth tokens:

| Setting | Source | Purpose |
|---|---|---|
| `DATABASE_URL` | env | PostgreSQL connection string |
| `PLEXUS_WORKSPACE_ROOT` | env | Root directory for server-side personal/shared workspaces |
| `PLEXUS_BIND` | env, default `127.0.0.1:8080` | HTTP bind address |
| `JWT_SECRET` | env | JWT signing secret |
| `ADMIN_TOKEN` | env, optional | Allows a registering user to become admin |
| `PLEXUS_COOKIE_SECURE` | env, default false in dev | Controls Secure flag on auth cookie |

LLM provider settings are intentionally not env vars. They live in
`system_config` and become active in M1b once provider validation exists.

Startup should ensure `PLEXUS_WORKSPACE_ROOT` exists and fail fast if it cannot
be created or accessed.

---

## 6. Database Bootstrap

`plexus-server/src/db/schema.sql` is the canonical SQL byte source for M1. It
must contain all `CREATE TABLE IF NOT EXISTS` and index statements described in
`docs/SCHEMA.md`, plus any required PostgreSQL prelude such as extension
creation needed by the UUID defaults.

On startup:

1. Connect to PostgreSQL.
2. Run `sqlx::raw_sql(include_str!("schema.sql"))`.
3. Continue startup only if schema application succeeds.

There is no migration framework in M1a. Rebuild-era schema changes require a
dev DB reset. If `schema.sql` and `docs/SCHEMA.md` diverge, implementation must
update the docs or SQL before the sub-milestone is complete.

M1a should also seed default `system_config` rows only where defaults are needed
for routes implemented in M1a. Defaults can be inserted with idempotent
`INSERT ... ON CONFLICT DO NOTHING`.

---

## 7. Auth Scope

M1a implements real auth, not a test bypass.

Endpoints:

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/me`
- `PATCH /api/me`

Behavior:

- Registration validates required fields, hashes the password, creates a user,
  returns a JWT, and sets the `plexus_session` HttpOnly cookie.
- Registration creates the user's personal workspace directory at
  `{PLEXUS_WORKSPACE_ROOT}/{user_id}/`. If the DB insert or directory creation
  fails, the request fails and does not report a successful registration.
- If `admin_token` matches `ADMIN_TOKEN`, the new user gets `is_admin=true`.
- Duplicate email returns `409 Conflict`.
- Login verifies the password hash, returns a JWT, and sets the same cookie.
- Logout clears the cookie and returns `204`. It is idempotent and does not
  require authentication.
- `GET /api/me` accepts either the cookie JWT or Bearer JWT.
- `PATCH /api/me` updates `name`, `email`, and/or `password`; password updates
  are re-hashed, and duplicate email returns `409 Conflict`.
- Auth failures use centralized `plexus-common` auth error codes.

Password hashing:

- Use Argon2id with a per-password random salt.
- Never return `password_hash` in API responses.
- Tests should assert stored password text is not equal to the cleartext input.

JWT:

- Include user id, admin flag, issued-at, and expiry claims.
- Use a bounded expiry suitable for browser sessions.
- Verify signature and expiry on every authenticated route.
- Admin routes verify JWT identity, then reload the current database user row
  and require `users.is_admin=true`. The DB row is authoritative; the JWT admin
  claim is informational and not the final authorization source.

---

## 8. Admin Config Scope

M1a implements:

- `GET /api/admin/config`
- `PATCH /api/admin/config`

Both routes require authenticated admin users. Non-admin users receive
`403 Forbidden`; unauthenticated requests receive `401 Unauthorized`. Admin
authorization uses the freshly loaded database user row.

Accepted M1a keys:

- `quota_bytes`
- `shared_workspace_quota_bytes`
- `llm_max_context_tokens`
- `llm_compaction_threshold_tokens`
- `llm_max_concurrent_requests`

These keys can be persisted without external service validation. The server
should validate basic JSON types and obvious bounds before writing.

Deferred provider identity keys:

- `llm_endpoint`
- `llm_api_key`
- `llm_model`

These keys must not be accepted in M1a because the docs require validating
`GET {llm_endpoint}/models` before DB write. M1a returns `400 Bad Request` for a
patch containing any of these keys and leaves existing config unchanged. M1b
adds the real provider validation and then enables those writes.

Opaque deployment-local keys are not accepted in M1a. The docs allow deployments
to carry opaque keys, but the first implementation slice should not open that
surface until the admin config validation story is mature.

Patch behavior:

- Apply supported keys atomically in a transaction.
- If any key is unsupported or invalid, reject the whole patch.
- Return the full current config object after a successful patch.

---

## 9. Error Handling

`plexus-common` remains the source of stable wire error codes. M1a must not
create a parallel error-code system.

`plexus-server` may define an internal HTTP adapter, such as `ApiError`, only to
implement Axum `IntoResponse`. That adapter maps typed errors to:

```json
{ "code": "unauthorized", "message": "authentication required" }
```

Rules:

- Use `plexus_common::ErrorCode` for response codes.
- Use the `Code` trait for common typed errors.
- Add new stable codes to `plexus-common` only when truly needed.
- Server-only infrastructure errors can map to an existing common code where
  semantically correct, such as `io_error` or `invalid_args`.
- Do not leak secrets in error messages, logs, or debug output.

HTTP status mapping for M1a:

| Case | Status | Code |
|---|---|---|
| Missing auth | `401` | `unauthorized` |
| Bad or expired token | `401` | `token_invalid` or `token_expired` |
| Non-admin on admin route | `403` | `forbidden` |
| Malformed JSON or invalid field | `400` | `invalid_args` |
| Duplicate email | `409` | `invalid_args` unless a more specific common code is added |
| Unexpected DB failure | `500` | `io_error` |

---

## 10. Tests

M1a uses real PostgreSQL for automated tests. The implementation plan should
choose the exact test harness, but the sub-milestone must include these checks.

### 10.1 Bootstrap Tests

- Start from an empty test database.
- Run server DB bootstrap.
- Assert all documented tables exist:
  `system_config`, `users`, `discord_configs`, `telegram_configs`, `sessions`,
  `messages`, `devices`, `workspaces`, `workspace_members`, `cron_jobs`.
- Run bootstrap twice and assert the second run succeeds.

### 10.2 Auth API Tests

- Register a normal user.
- Register an admin user using `ADMIN_TOKEN`.
- Duplicate email returns `409`.
- Stored password hash is not the cleartext password.
- Registration creates the personal workspace directory under
  `PLEXUS_WORKSPACE_ROOT`.
- Login succeeds with the right password and fails with the wrong password.
- Auth response includes a JWT and sets `plexus_session` as HttpOnly.
- `GET /api/me` succeeds with Bearer JWT.
- `GET /api/me` succeeds with cookie JWT.
- `PATCH /api/me` persists name/email changes.
- `PATCH /api/me` persists password changes as a new hash and allows login with
  the new password.
- Logout clears the cookie.

### 10.3 Admin Config Tests

- Non-admin cannot read admin config.
- Admin can read admin config.
- Admin can patch supported keys.
- Successful patch writes rows to `system_config`.
- Successful patch returns the current config object.
- Unsupported keys reject the whole patch.
- `llm_endpoint`, `llm_api_key`, and `llm_model` reject in M1a and leave
  existing config unchanged.
- Invalid value types reject the whole patch.

### 10.4 Error Shape Tests

- Auth errors serialize as `{ code, message }`.
- Validation errors serialize as `{ code, message }`.
- No response includes `password_hash`, `JWT_SECRET`, `ADMIN_TOKEN`, or
  `llm_api_key`.

---

## 11. Exit Criteria

M1a is complete when:

- `plexus-server` exists in the Cargo workspace.
- The server can start against an empty PostgreSQL database.
- Startup creates all canonical tables and indexes.
- Startup ensures the server workspace root exists.
- Real registration, login, logout, `GET /api/me`, and `PATCH /api/me` work.
- Registration creates the user's personal workspace directory.
- Admin config routes enforce real admin auth.
- Every accepted `PATCH /api/admin/config` write persists to `system_config`.
- LLM provider identity keys are explicitly rejected until M1b validation.
- Automated M1a tests pass against real PostgreSQL.
- Relevant docs remain consistent with implementation.
- The M1 living tracker is updated from `Designing` to the next appropriate
  status.

---

## 12. Handoff to Implementation Plan

After this sub-spec is approved, the implementation plan should break M1a into
small test-first steps:

1. Add server crate and dependency skeleton.
2. Add schema SQL and DB bootstrap tests.
3. Implement DB bootstrap.
4. Add auth API tests.
5. Implement password hashing, JWT, auth routes, and auth extractors.
6. Add admin config persistence tests.
7. Implement admin config routes and transaction behavior.
8. Run verification, update docs/tracker, and commit implementation.
