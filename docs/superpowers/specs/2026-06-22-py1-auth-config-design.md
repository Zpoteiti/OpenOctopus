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
                         USER_NOT_FOUND, CONFIG_VALIDATION_FAILED
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
    config.py            ConfigPatch (request body), AdminConfig (response)
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

- `admin_token: str | None = None` (env var `OPENOCTOPUS_ADMIN_TOKEN`) — optional. When unset, an `admin_token` in the register body is ignored (the user is created as a regular user). When set, a register request whose `admin_token` equals it creates `is_admin=true`.
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
  CONFIG_VALIDATION_FAILED: 400, USER_NOT_FOUND: 404,
}
# unmapped → 500 (treated as a server bug)
handler → JSONResponse(status, {"code": exc.code.value, "message": exc.message})
```

Body-validation errors (malformed shapes, unknown keys, value-constraint violations) keep FastAPI's default 422 shape via Pydantic models with `extra="forbid"` and field constraints. Business rules (e.g. LLM `/models` check failure, email taken, last admin) raise `OpenOctopusError` so they flow through the single handler with a stable `{code, message}` body.

New `ErrorCode` entries: `AUTH_EMAIL_TAKEN`, `AUTH_INVALID_CREDENTIALS`, `AUTH_FORBIDDEN`, `USER_NOT_FOUND`, `CONFIG_VALIDATION_FAILED`. New exception subclass `ConfigError(OpenOctopusError)`. The `tests/snapshots/error_codes.json` snapshot is regenerated.

Note: `CONFIG_UNKNOWN_KEY` was considered but dropped — unknown keys are caught at the Pydantic body layer (`extra="forbid"` → 422), so they never reach the service layer. `CONFIG_VALIDATION_FAILED` covers the only remaining config business-rule failure (LLM `/models` check).

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

  **Deviation from API.yaml `AdminUser` schema:** the OpenAPI spec marks `quota_bytes`, `bytes_used`, and `locked` as required and non-nullable. In Py1 these fields are `null` because `workspace_fs` does not exist yet (Py4). The DTO uses `| None` to make this explicit. When Py4 lands, the types tighten to non-nullable and the fields are populated via `workspace_fs`. This is a forward-compatible deviation: existing callers that check `field is null` keep working.

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

- `GET /api/admin/config` → `200 AdminConfig` (effective config, see below).
- `PATCH /api/admin/config` — body is a `ConfigPatch` Pydantic model (`extra="forbid"`); keys present are updated, keys absent are untouched → `200 AdminConfig` (redacted effective config).

### `dto/config.py` — Pydantic models for config

`ConfigPatch` (request body for PATCH; `extra="forbid"`):
```python
class ConfigPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    quota_bytes: int | None = Field(default=None, ge=1)
    shared_workspace_quota_bytes: int | None = Field(default=None, ge=1)
    llm_endpoint: str | None = Field(default=None, min_length=1)
    llm_api_key: str | None = Field(default=None, min_length=1)
    llm_model: str | None = Field(default=None, min_length=1)
    llm_max_context_tokens: int | None = Field(default=None, ge=1)
    llm_compaction_threshold_tokens: int | None = Field(default=None, ge=4001)
    llm_max_concurrent_requests: int | None = Field(default=None, ge=0, le=1_000_000)
```
Constraints match API.yaml: `quota_bytes`/`shared_workspace_quota_bytes` ≥ 1; LLM identity strings minLength 1; `llm_max_context_tokens` ≥ 1; `llm_compaction_threshold_tokens` ≥ 4001 (because `max_output_tokens = threshold − 4000` per ADR-028, must be ≥ 1); `llm_max_concurrent_requests` 0–1000000. `null` values mean "key not in this PATCH" (untouched); sending an explicit `null` is indistinguishable from omitting the key, which is the correct partial-update semantic. Unknown keys → 422 via `extra="forbid"`.

`AdminConfig` (response for GET and PATCH):
```python
class AdminConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    quota_bytes: int                          # always present, defaults to 524288000
    shared_workspace_quota_bytes: int         # always present, defaults to 524288000
    llm_endpoint: str | None = None           # omitted (None) when unconfigured
    llm_api_key: str | None = None            # "<redacted>" when configured, None when unconfigured
    llm_model: str | None = None
    llm_max_context_tokens: int | None = None
    llm_compaction_threshold_tokens: int | None = None
    llm_max_concurrent_requests: int | None = None
```
Matches API.yaml `AdminConfig` schema: `required: [quota_bytes, shared_workspace_quota_bytes]`; unconfigured LLM keys omitted; configured `llm_api_key` returned as `"<redacted>"`.

### Admin users (`api/admin/users.py`) — both `require_admin`

- `GET /api/admin/users` — query params `limit` (default 50, min 1, max 200) and `offset` (default 0, min 0) per the API.yaml `Limit`/`Offset` parameters → `200 [AdminUserResponse]` (quota fields `null`).
- `DELETE /api/admin/users/{id}` — load or `USER_NOT_FOUND` → `assert_not_last_admin` → `delete_user` → `204`. `409 auth_last_admin_required` if the target is the last admin. An admin may delete themselves unless they are the last admin (API.yaml:1778).

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

- `get_config_view(db) -> AdminConfig` — loads rows; returns effective config: `quota_bytes`/`shared_workspace_quota_bytes` default `524288000` (500 MiB) when missing (always present); LLM keys included only when configured (omitted = `None`); `llm_api_key` redacted as `"<redacted>"` when configured. Does not surface unknown/opaque keys or `server_mcp`/`object_storage_*`.

- `patch_config(db, payload: ConfigPatch) -> AdminConfig`:
  1. Pydantic validation already enforced value constraints and rejected unknown keys at the route layer (422).
  2. Reject the literal `"<redacted>"` as `llm_api_key` → `CONFIG_VALIDATION_FAILED`.
  3. If any LLM identity key (`llm_endpoint`/`llm_api_key`/`llm_model`) is present in the patch, gather the effective triple (patch value **or** currently-stored), require all three present, then `validate_llm_identity(endpoint, api_key, model)`. Failure → `CONFIG_VALIDATION_FAILED`. First-time setup requires all three in the same PATCH; later PATCHes may reuse stored values by omitting unchanged identity keys.
  4. Upsert rows for non-None fields, commit, return `get_config_view(db)`.

- `validate_llm_identity(endpoint, api_key, model, *, client: httpx.AsyncClient | None = None)` — `GET {endpoint}/models` with `Authorization: Bearer {api_key}`; per ADR-101, the response must be HTTP 200 **and** include the configured `llm_model` in the models list. The expected response shape is OpenAI-compatible: `{"object": "list", "data": [{"id": "<model_name>", "object": "model"}, ...]}`. The check looks for `llm_model` in `data[].id`. Non-200 or model absent → `CONFIG_VALIDATION_FAILED`. The optional `client` enables test injection via `MockTransport`. Real provider credentials are only needed for live smoke testing, not automated tests (ADR-101).

- `llm_max_concurrent_requests` runtime behavior: Py1 only stores/retrieves the value. The actual semaphore is Py2 (provider runtime). Per ADR-101, if the key is missing at startup, the runtime limiter treats it as `0` (unlimited); no default row is persisted. Py1's `get_config_view` returns `None` for this key when no row exists.

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
  - admin config: GET `200` effective defaults (quota keys present, LLM keys omitted); PATCH LLM triple with fake validation (model present) → `200` redacted; PATCH unknown key → `422`; PATCH with invalid value (e.g. `quota_bytes=0`, `llm_compaction_threshold_tokens=100`) → `422`; PATCH LLM with failing `/models` (non-200) → `400 config_validation_failed`; PATCH LLM with model absent from `/models` response → `400 config_validation_failed`; PATCH `llm_api_key="<redacted>"` → `400 config_validation_failed`; non-admin GET/PATCH → `403 auth_forbidden`.
  - admin users: GET list `200` (paginated); DELETE `204`; DELETE missing `404 user_not_found`; DELETE last admin `409`; non-admin → `403`.
  - error shape: every error response is `{code, message}`.
  - jwt: `verify_jwt` rejects tampered/expired tokens (unit).
  - password: hash/verify round-trip; verify rejects wrong password (unit).
  - `error_codes.json` snapshot regenerated to include the new codes.
- CI: the existing `.github/workflows/py-server.yml` already runs ruff, mypy (strict), and pytest against a PostgreSQL service; no workflow change required.

## Open questions / risks

- **422 for body validation:** FastAPI's default 422 is used for malformed shapes, unknown keys (`extra="forbid"`), and value-constraint violations (Pydantic field constraints). Business rules (LLM `/models` check, email taken, last admin) raise `OpenOctopusError` → `{code, message}`. This split is clean for Py1; if we later want a uniform `{code, message}` 400 for validation, add a `RequestValidationError` handler then.
- **`GET /api/admin/users` quota fields:** returned as `null` in Py1 (deviation from API.yaml `AdminUser` which marks them required/non-nullable); Py4 will populate them via `workspace_fs`. The DTO is forward-compatible.
- **No token revocation:** a deleted user's JWT is only invalidated when `get_current_user` fails to load the row. Acceptable for the alpha; a revocation list is a later concern.
- **`llm_max_concurrent_requests` stored but inert in Py1:** the semaphore itself is Py2 (provider runtime). Py1 only stores/retrieves the value; no runtime enforcement.
