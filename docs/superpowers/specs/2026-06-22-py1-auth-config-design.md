# Py1 — Auth + Config Design

**Milestone:** Py1 (depends on Py0)
**Status:** proposed

Per the milestone map in `docs/DECISIONS.md`, Py1 is "Auth + config": registration/login + JWT + cookie/bearer + admin `system_config` + admin user management. Exit criteria: auth + admin config tests pass; an admin can configure a (fake) LLM provider ahead of Py2; no chat yet.

This spec builds on the Py0 skeleton (`server/src/openoctopus_server/`) and reuses its patterns: `api/router.py` sub-router assembly, `errors/exceptions.py` `OpenOctopusError`, `db/engine.py` `get_engine()`, plain-pydantic DTOs, and the `pg_engine`/`async_client` test fixtures.

## Scope

**In scope:**
- `POST /api/auth/register`, `POST /api/auth/login`, `POST /api/auth/logout`
- `GET /api/me`, `PATCH /api/me`, `DELETE /api/me`
- `GET /api/admin/config`, `PATCH /api/admin/config`
- `GET /api/admin/users`, `DELETE /api/admin/users/{id}`
- JWT issue/verify (HS256, `OPENOCTOPUS_JWT_SECRET`, 30-day exp, claims `{sub, exp}`)
- Cookie (`openoctopus_session`) + bearer delivery; `OPENOCTOPUS_COOKIE_SECURE` controls `Secure`
- argon2id password hashing
- `ADMIN_TOKEN` gating at register → `is_admin`
- Last-admin protection on user deletion
- `system_config` admin-editable keys with LLM identity validation (`GET {llm_endpoint}/models`) and `llm_api_key` redaction

**Out of scope (later milestones):**
- Personal workspace creation on register / `workspace_fs` / quota enforcement — Py4
- `POST/GET messages` / sessions / chat — Py2
- Agent loop, tool registry, merge — Py3
- Devices, channels, cron — Py5+
- `/api/admin/server-mcp` — Py8
- Refresh tokens / token revocation list; email verification / password reset; login rate limiting; frontend

**Decisions confirmed during brainstorming:**
- Register creates **only** the `users` row in Py1. Personal workspace creation is deferred to Py4.
- No refresh-token mechanism is planned anywhere in the docs; Py1 uses a single long-lived JWT (30-day exp).
- Password hashing: argon2-cffi (Argon2id).
- Auth architecture: Approach A — idiomatic FastAPI dependency chain with minimal DB hits.

## Architecture & module layout

New files build on the existing package; no existing Py0 file is restructured beyond small additions.

```
server/src/openctopus_server/
  db/
    session.py            NEW: get_db() -> AsyncSession  (the one DB dependency)
  errors/
    codes.py             +AUTH_EMAIL_TAKEN, AUTH_INVALID_CREDENTIALS, AUTH_FORBIDDEN,
                         USER_NOT_FOUND, CONFIG_UNKNOWN_KEY, CONFIG_VALIDATION_FAILED
    exceptions.py        +ConfigError(OpenOctopusError)
    http.py              NEW: ERROR_STATUS map + openoctopus_error_handler
  auth/                  NEW package
    jwt.py               create_jwt / verify_jwt
    password.py          hash_password / verify_password  (argon2-cffi)
    cookies.py           set_auth_cookie / clear_auth_cookie
    dependencies.py      get_current_user_id, get_current_user, require_admin
  services/              NEW package  (NO fastapi imports)
    users.py             user CRUD + count_admins + assert_not_last_admin
    system_config.py     get_config_view / patch_config / validate_llm_identity
  api/
    router.py            wire auth/me/admin routers
    health.py            unchanged
    auth.py              register/login/logout
    me.py                GET/PATCH/DELETE /api/me
    admin/
      __init__.py
      config.py          GET/PATCH /api/admin/config
      users.py           GET /api/admin/users, DELETE /api/admin/users/{id}
  dto/
    user.py              UserResponse, AdminUserResponse
  config.py             +admin_token: str | None = None
  main.py               register OpenOctopusError handler in create_app()
```

### Decoupling / orthogonality

- `services/` knows nothing about FastAPI. Functions take an `AsyncSession` and raise `OpenOctopusError`. They are pure and unit-testable with a `db_session` fixture.
- `errors/http.py` is the **only** place `ErrorCode` ↔ HTTP status is coupled. Domain errors stay HTTP-agnostic (the same `ErrorCode` set is reused for `tool_result` error codes per ADR-031). Routes raise semantic errors; the handler returns `{code, message}`.
- Routes are thin adapters: parse body → call service → build DTO → set/clear cookie.

### DRY

- `delete_user` + `assert_not_last_admin` are shared by `DELETE /api/me` and `DELETE /api/admin/users/{id}`.
- `update_user` is shared by `PATCH /api/me`.
- `get_config_view` is shared by `GET /api/admin/config` and the `PATCH` response.
- `validate_llm_identity` is injected with an httpx client so tests use `MockTransport` without monkeypatching.

### Junior-dev "add a feature" loop (one place per step)

1. `errors/codes.py` + `errors/http.py` status line — if a new error is needed.
2. `services/<area>.py` — one function taking a session, raising semantic errors.
3. `api/<area>.py` — one route, `Depends(get_db)` + auth dep, call service, return DTO.
4. `api/router.py` — one `include_router` line.
5. A test.

## Config additions (`config.py`)

- `admin_token: str | None = None` — optional. When unset, an `admin_token` in the register body is ignored (the user is created as a regular user). When set, a register request whose `admin_token` equals it creates `is_admin=true`.
- Existing `jwt_secret` and `cookie_secure` are reused.

## JWT & cookie (`auth/jwt.py`, `auth/cookies.py`)

- `create_jwt(user_id: UUID) -> str`: HS256 with `OPENOCTOPUS_JWT_SECRET`; payload `{"sub": str(user_id), "exp": now_utc + 30 days}`. No `is_admin` claim — the DB row is authoritative for admin status (ADR-004).
- `verify_jwt(token: str) -> UUID`: decode + verify; raise `AuthError(AUTH_UNAUTHORIZED)` on invalid/expired tokens.
- Cookie name `openoctopus_session`; `HttpOnly`, `SameSite=Strict`, `Path=/`, `Secure = OPENOCTOPUS_COOKIE_SECURE`.
- `set_auth_cookie(response, jwt)` / `clear_auth_cookie(response)`.
- Bearer fallback: `get_current_user_id` reads `Authorization: Bearer <jwt>` when the cookie is absent.

## Password hashing (`auth/password.py`)

argon2-cffi `PasswordHasher` (Argon2id). `hash_password(pw) -> str`; `verify_password(pw, hash) -> bool` (returns `False` on mismatch, never raises through the public API). `password_hash` is never included in any response DTO.

## Error handling (`errors/http.py`)

A single exception handler registered in `create_app()` via `app.add_exception_handler(OpenOctopusError, openoctopus_error_handler)`.

```
ERROR_STATUS = {
  AUTH_UNAUTHORIZED: 401, AUTH_INVALID_CREDENTIALS: 401, AUTH_FORBIDDEN: 403,
  AUTH_EMAIL_TAKEN: 409, AUTH_LAST_ADMIN_REQUIRED: 409,
  CONFIG_UNKNOWN_KEY: 400, CONFIG_VALIDATION_FAILED: 400, USER_NOT_FOUND: 404,
}
# unmapped → 500 (treated as a server bug)
handler → JSONResponse(status, {"code": exc.code.value, "message": exc.message})
```

Pydantic body-validation errors keep FastAPI's default 422 shape; business rules raise `OpenOctopusError` so they flow through the single handler with a stable `{code, message}` body.

New `ErrorCode` entries: `AUTH_EMAIL_TAKEN`, `AUTH_INVALID_CREDENTIALS`, `AUTH_FORBIDDEN`, `USER_NOT_FOUND`, `CONFIG_UNKNOWN_KEY`, `CONFIG_VALIDATION_FAILED`. New exception subclass `ConfigError(OpenOctopusError)`. The `tests/snapshots/error_codes.json` snapshot is regenerated.

## Auth dependencies (`auth/dependencies.py`)

- `get_current_user_id(request: Request) -> UUID` — read cookie **or** bearer; verify JWT; missing/invalid → `AuthError(AUTH_UNAUTHORIZED)`. No DB hit.
- `get_current_user(user_id = Depends(get_current_user_id), db = Depends(get_db)) -> User` — load the `users` row; missing → `AUTH_UNAUTHORIZED` (token points to a deleted user).
- `require_admin(user = Depends(get_current_user)) -> User` — `not user.is_admin` → `AUTH_FORBIDDEN`.

## DB sessions (`db/session.py`)

```
async def get_db() -> AsyncIterator[AsyncSession]:
    async with AsyncSession(get_engine(), expire_on_commit=False) as session:
        yield session
```

Services receive the session as an argument; they commit their own transactions. Routes do not manage transactions.

## DTOs (`dto/user.py`)

- `UserResponse {id: UUID, email: str, name: str, is_admin: bool, created_at: datetime}` — never `password_hash`.
- `AdminUserResponse = UserResponse + {quota_bytes: int | None, bytes_used: int | None, locked: bool | None}` — all three `null` in Py1 (`workspace_fs` is Py4).

## Endpoint contracts

### Auth (`api/auth.py`)

`POST /api/auth/register` — body `{email: EmailStr, password: str (min 8), name: str, admin_token?: str}`.
→ `users.create_user(...)` (email unique check → `AUTH_EMAIL_TAKEN`; `admin_token` matches `OPENOCTOPUS_ADMIN_TOKEN` when set → `is_admin=true`) → `create_jwt` → `set_auth_cookie` → `201 {jwt, user: UserResponse}`.

`POST /api/auth/login` — body `{email: EmailStr, password: str}`.
→ `get_user_by_email` + `verify_password`; mismatch → `AUTH_INVALID_CREDENTIALS` → `200 {jwt, user}` + cookie.

`POST /api/auth/logout` — `204`, `clear_auth_cookie`. Public (no auth).

### Me (`api/me.py`) — all require `get_current_user`

- `GET /api/me` → `200 UserResponse`.
- `PATCH /api/me` — body `{name?: str, email?: EmailStr, password?: str (min 8)}` → `users.update_user` (email taken → `AUTH_EMAIL_TAKEN`) → `200 UserResponse`.
- `DELETE /api/me` → `assert_not_last_admin` (last admin → `AUTH_LAST_ADMIN_REQUIRED`) then `delete_user` → `204`. Cascades via DB FKs.

### Admin config (`api/admin/config.py`) — both `require_admin`

- `GET /api/admin/config` → `200` effective config (see below).
- `PATCH /api/admin/config` — body is a JSON object of allowed keys → `200` redacted effective config.

### Admin users (`api/admin/users.py`) — both `require_admin`

- `GET /api/admin/users` — query `limit`/`offset` → `200 [AdminUserResponse]` (quota fields `null`).
- `DELETE /api/admin/users/{id}` — load or `USER_NOT_FOUND` → `assert_not_last_admin` → `delete_user` → `204`. `409 auth_last_admin_required` if the target is the last admin.

## Services

### `services/users.py` (takes `AsyncSession`; raises `OpenOctopusError`)

- `create_user(db, email, password, name, *, admin_token=None) -> User`
- `get_user_by_id(db, id) -> User | None`
- `get_user_by_email(db, email) -> User | None`
- `list_users(db, limit, offset) -> list[User]`
- `update_user(db, user, *, name=None, email=None, password=None) -> User`
- `delete_user(db, user) -> None`
- `count_admins(db) -> int`
- `assert_not_last_admin(db, user) -> None` — raises `AUTH_LAST_ADMIN_REQUIRED` if `user.is_admin` and `count_admins(db) == 1`. Shared by both delete endpoints.

### `services/system_config.py`

Allowed config keys (per `SCHEMA.md` admin-editable table):
`quota_bytes`, `shared_workspace_quota_bytes`, `llm_endpoint`, `llm_api_key`, `llm_model`, `llm_max_context_tokens`, `llm_compaction_threshold_tokens`, `llm_max_concurrent_requests`.

- `get_config_view(db) -> dict` — loads rows; returns effective config: `quota_bytes`/`shared_workspace_quota_bytes` default `524288000` (500 MiB) when missing; LLM keys included only when configured; `llm_api_key` redacted as `"<redacted>"`.
- `patch_config(db, payload: dict) -> dict`:
  1. Unknown key → `CONFIG_UNKNOWN_KEY`.
  2. If any LLM identity key (`llm_endpoint`/`llm_api_key`/`llm_model`) is present, gather the effective triple (patch value or currently-stored), require all three present, then `validate_llm_identity(endpoint, api_key, model)`. Failure → `CONFIG_VALIDATION_FAILED`.
  3. Reject the literal `"<redacted>"` as `llm_api_key` → `CONFIG_VALIDATION_FAILED`.
  4. Upsert rows, commit, return `get_config_view(db)`.
- `validate_llm_identity(endpoint, api_key, model, *, client: httpx.AsyncClient | None = None)` — `GET {endpoint}/models` with `Authorization: Bearer {api_key}`; per ADR-101, the response must be HTTP 200 **and** include the configured `llm_model` in the models list. Non-200 or model absent → `CONFIG_VALIDATION_FAILED`. The optional `client` enables test injection via `MockTransport`. Real provider credentials are only needed for live smoke testing, not automated tests (ADR-101).

`server_mcp` is not accepted (Py8); `object_storage_*` is not accepted (env config).

## Dependencies

Added to `server/pyproject.toml` runtime deps: `argon2-cffi`, `pyjwt`, `email-validator`, and `httpx` (promoted from dev-only to runtime, for `validate_llm_identity`).

## Testing strategy

- Reuse the Py0 `pg_engine` + `async_client` fixtures. Add a `db_session` fixture (`AsyncSession` on `pg_engine`) for service-level tests.
- Auth fixtures: a `register(...)` helper returning `{jwt, user}`; `auth_client` (regular user, cookie set); `admin_client` (admin via `OPENOCTOPUS_ADMIN_TOKEN`, cookie set).
- Fake LLM via DI: `validate_llm_identity(..., client=...)` with an httpx `MockTransport` returning 200 + a models list containing the configured `llm_model` (and failure variants: non-200, model absent).
- Tests:
  - auth: register `201` + cookie; duplicate email `409 auth_email_taken`; `admin_token` promotes to `is_admin`; login `200` + cookie; login wrong password `401 auth_invalid_credentials`; logout `204` clears cookie.
  - me: GET `200` / `401` without token; PATCH name/email/password; PATCH email taken `409`; DELETE `204`; DELETE last admin `409 auth_last_admin_required`.
  - admin config: GET `200` effective defaults (no rows); PATCH LLM triple with fake validation (model present) → `200` redacted; PATCH unknown key `400 config_unknown_key`; PATCH LLM with failing `/models` (non-200) → `400 config_validation_failed`; PATCH LLM with model absent from `/models` response → `400 config_validation_failed`; non-admin GET/PATCH → `403 auth_forbidden`.
  - admin users: GET list `200` (paginated); DELETE `204`; DELETE missing `404 user_not_found`; DELETE last admin `409`; non-admin → `403`.
  - error shape: every error response is `{code, message}`.
  - jwt: `verify_jwt` rejects tampered/expired tokens (unit).
  - password: hash/verify round-trip; verify rejects wrong password (unit).
  - `error_codes.json` snapshot regenerated to include the new codes.
- CI: the existing `.github/workflows/py-server.yml` already runs ruff, mypy (strict), and pytest against a PostgreSQL service; no workflow change required.

## Open questions / risks

- **422 vs 400 for body validation:** FastAPI's default 422 is kept for malformed shapes. If we later want a uniform `{code, message}` 400 for validation, add a `RequestValidationError` handler then (not needed for Py1 exit criteria).
- **`GET /api/admin/users` quota fields:** returned as `null` in Py1; Py4 will populate them via `workspace_fs`. The DTO is forward-compatible.
- **No token revocation:** a deleted user's JWT is only invalidated when `get_current_user` fails to load the row. Acceptable for the alpha; a revocation list is a later concern.
