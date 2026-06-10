# Plexus M1c Browser Chat Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first usable browser chat path: UUID-addressed web sessions, persisted message history, SSE replay/live delivery, minimal SOUL/MEMORY prompting, non-streaming OpenAI-compatible provider responses, image-strip fallback, and safe persisted provider diagnostics.

**Architecture:** M1c adds a narrow chat spine beside the existing M1a/M1b server foundation. Persistence lives in focused `db::sessions` and `db::messages` helpers; browser HTTP lives in `routes::sessions`; prompt/content/SSE/worker behavior lives under a new `chat` module. The response worker is intentionally not a full ReAct loop: it serializes one provider-backed assistant response per web session, uses existing admin LLM config, emits persisted rows through SSE, and leaves tools/cancel/workspace attachments to later M1 slices.

**Tech Stack:** Rust 2024, Axum, Tokio, SQLx/Postgres, Reqwest with rustls, `async-stream` for SSE streams, `serde_json` content blocks, existing `OpenAiRuntime`, real PostgreSQL integration tests.

---

## Scope Check

M1c is one coherent subsystem because session lifecycle, message persistence, SSE, and the one-shot response worker must integrate to prove the browser chat path. The plan keeps the boundary explicit:

- In scope: web sessions, titles, message content blocks, inline base64 images, runtime block persistence, minimal system prompt, provider call, safe synthetic assistant failures, SSE replay/live.
- Out of scope: frontend UI, cancel, tools, MCP, devices, Discord/Telegram adapters, `.attachments/`, external image URL fetch, multipart upload, workspace REST, cron, heartbeat, compaction, provider streaming.

Docs for `API.yaml`, `DECISIONS.md`, and `SCHEMA.md` were aligned before this plan. M1c implementation still needs to update `docs/SYSTEM_PROMPT.md` and milestone status/evidence after code lands.

---

## File Structure

Create in Plexus:

- `plexus-server/src/db/sessions.rs` - typed DB helpers for session create/list/read/rename/delete and ownership checks.
- `plexus-server/src/db/messages.rs` - typed DB helpers for message insert/history/replay/latest-user queries.
- `plexus-server/src/chat/mod.rs` - module exports and small runtime glue.
- `plexus-server/src/chat/content.rs` - OpenAI-compatible content block types and request normalization.
- `plexus-server/src/chat/prompt.rs` - M1c minimal system prompt builder with optional `SOUL.md` and `MEMORY.md`.
- `plexus-server/src/chat/sse.rs` - in-process per-session broadcast broker and SSE event helpers.
- `plexus-server/src/chat/worker.rs` - per-session serialized one-shot response worker.
- `plexus-server/src/routes/sessions.rs` - browser session, message, history, and SSE routes.
- `plexus-server/tests/m1c_sessions.rs` - session lifecycle and schema tests.
- `plexus-server/tests/m1c_messages.rs` - message content normalization, auth, and persistence tests.
- `plexus-server/tests/m1c_sse.rs` - SSE replay/live tests.
- `plexus-server/tests/m1c_worker.rs` - provider-backed worker, image fallback, synthetic error, and serialization tests.

Modify in Plexus:

- `Cargo.toml` - add workspace dependency `async-stream`.
- `plexus-server/Cargo.toml` - depend on `async-stream`.
- `plexus-server/src/app.rs` - store shared `ChatRuntime`.
- `plexus-server/src/db/mod.rs` - export `sessions` and `messages`.
- `plexus-server/src/db/schema.sql` - add `sessions.title`, remove global `session_key` uniqueness, add `(user_id, session_key)` unique index.
- `plexus-server/src/db/system_config.rs` - add helper to load stored LLM config for runtime calls.
- `plexus-server/src/lib.rs` - export `chat`.
- `plexus-server/src/openai.rs` - replace string-only chat content with content arrays and add image-strip fallback.
- `plexus-server/src/routes/mod.rs` - mount session routes.
- `plexus-server/tests/support/fake_openai.rs` - accept content arrays and add failure modes needed by M1c tests.
- `docs/SYSTEM_PROMPT.md` - document that M1c persists runtime blocks on user rows.
- `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md` - update M1c status/evidence after implementation.
- `docs/reference/superpowers/specs/2026-05-14-plexus-m1c-browser-chat-path-design.md` - update status/evidence after implementation and manual smoke.

---

## Task 1: Schema, Dependencies, and Bootstrap Coverage

**Files:**
- Modify: `Cargo.toml`
- Modify: `plexus-server/Cargo.toml`
- Modify: `plexus-server/src/db/schema.sql`
- Modify: `plexus-server/tests/m1a_bootstrap.rs`

- [ ] **Step 1: Write failing schema assertions**

Extend `plexus-server/tests/m1a_bootstrap.rs` with assertions that prove the M1c session schema exists:

```rust
#[tokio::test]
async fn bootstrap_applies_m1c_session_shape() {
    let app = support::TestApp::spawn().await;

    let title: Option<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns
         WHERE table_name = 'sessions' AND column_name = 'title'",
    )
    .fetch_optional(&app.pool)
    .await
    .unwrap();
    assert_eq!(title.unwrap().0, "title");

    let old_constraint: Option<(String,)> = sqlx::query_as(
        "SELECT constraint_name FROM information_schema.table_constraints
         WHERE table_name = 'sessions' AND constraint_name = 'sessions_session_key_key'",
    )
    .fetch_optional(&app.pool)
    .await
    .unwrap();
    assert!(old_constraint.is_none());

    let index: Option<(String,)> = sqlx::query_as(
        "SELECT indexname FROM pg_indexes
         WHERE tablename = 'sessions' AND indexname = 'idx_sessions_user_session_key'",
    )
    .fetch_optional(&app.pool)
    .await
    .unwrap();
    assert_eq!(index.unwrap().0, "idx_sessions_user_session_key");
}
```

- [ ] **Step 2: Run the failing bootstrap test**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1a_bootstrap bootstrap_applies_m1c_session_shape
```

Expected: FAIL because `sessions.title` and `idx_sessions_user_session_key` do not exist yet.

- [ ] **Step 3: Add the SSE helper dependency**

In root `Cargo.toml`, add:

```toml
async-stream = "0.3"
```

In `plexus-server/Cargo.toml`, add:

```toml
async-stream.workspace = true
```

- [ ] **Step 4: Update schema SQL**

Replace the `sessions` table block in `plexus-server/src/db/schema.sql` with:

```sql
CREATE TABLE IF NOT EXISTS sessions (
    id                UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_key       TEXT         NOT NULL,
    channel           TEXT         NOT NULL,
    chat_id           TEXT         NOT NULL,
    title             TEXT         NOT NULL DEFAULT 'New chat',
    last_inbound_at   TIMESTAMPTZ,
    cancel_requested  BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT 'New chat';

ALTER TABLE sessions
    DROP CONSTRAINT IF EXISTS sessions_session_key_key;

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_user_session_key
    ON sessions(user_id, session_key);
```

Rationale: clean rebuild DBs get the canonical create statement; existing local M1a/M1b dev DBs also converge without a separate migration framework.

- [ ] **Step 5: Run the bootstrap test again**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1a_bootstrap bootstrap_applies_m1c_session_shape
```

Expected: PASS.

- [ ] **Step 6: Commit the schema slice**

```bash
git add Cargo.toml plexus-server/Cargo.toml plexus-server/src/db/schema.sql plexus-server/tests/m1a_bootstrap.rs
git commit -m "feat: add M1c session schema"
```

---

## Task 2: Session DB Helpers and Session Lifecycle Routes

**Files:**
- Create: `plexus-server/src/db/sessions.rs`
- Create: `plexus-server/src/routes/sessions.rs`
- Create: `plexus-server/tests/m1c_sessions.rs`
- Modify: `plexus-server/src/db/mod.rs`
- Modify: `plexus-server/src/routes/mod.rs`

- [ ] **Step 1: Write failing session lifecycle tests**

Create `plexus-server/tests/m1c_sessions.rs` with tests covering create/list/read/rename/delete:

```rust
mod support;

use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use support::TestApp;
use tower::ServiceExt;
use uuid::Uuid;

async fn json_request(
    app: axum::Router,
    method: Method,
    path: &str,
    body: Value,
    auth: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = auth {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

async fn register(app: axum::Router, email: &str) -> String {
    let (status, body) = json_request(
        app,
        Method::POST,
        "/api/auth/register",
        json!({
            "email": email,
            "password": "correct horse battery staple",
            "name": "Alice"
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    body["jwt"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn create_web_session_defaults_title_and_sets_web_key() {
    let app = TestApp::spawn().await;
    let token = register(app.router.clone(), "alice@example.com").await;

    let (status, body) =
        json_request(app.router.clone(), Method::POST, "/api/sessions", json!({}), Some(&token)).await;

    assert_eq!(status, StatusCode::CREATED);
    let id = body["id"].as_str().unwrap();
    Uuid::parse_str(id).unwrap();
    assert_eq!(body["title"], "New chat");
    assert_eq!(body["channel"], "web");
    assert_eq!(body["chat_id"], id);
    assert_eq!(body["session_key"], format!("web:{id}"));
}

#[tokio::test]
async fn create_list_read_rename_and_delete_session() {
    let app = TestApp::spawn().await;
    let token = register(app.router.clone(), "alice@example.com").await;

    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        "/api/sessions",
        json!({"title": "  Journey to Japan  "}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["title"], "Journey to Japan");
    let id = body["id"].as_str().unwrap();

    let (status, list) =
        json_request(app.router.clone(), Method::GET, "/api/sessions", Value::Null, Some(&token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);

    let (status, body) = json_request(
        app.router.clone(),
        Method::PATCH,
        &format!("/api/sessions/{id}"),
        json!({"title": "Japan itinerary"}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["title"], "Japan itinerary");

    let (status, _) = json_request(
        app.router.clone(),
        Method::PATCH,
        &format!("/api/sessions/{id}"),
        json!({"title": "   "}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!("/api/sessions/{id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let (status, _) = json_request(
        app.router.clone(),
        Method::GET,
        &format!("/api/sessions/{id}"),
        Value::Null,
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn session_ownership_returns_404_for_other_users() {
    let app = TestApp::spawn().await;
    let alice = register(app.router.clone(), "alice@example.com").await;
    let bob = register(app.router.clone(), "bob@example.com").await;

    let (status, body) =
        json_request(app.router.clone(), Method::POST, "/api/sessions", json!({}), Some(&alice)).await;
    assert_eq!(status, StatusCode::CREATED);
    let id = body["id"].as_str().unwrap();

    let (status, _) = json_request(
        app.router.clone(),
        Method::GET,
        &format!("/api/sessions/{id}"),
        Value::Null,
        Some(&bob),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_sessions
```

Expected: FAIL because session routes and DB helpers are not implemented.

- [ ] **Step 3: Add session DB helper module**

Create `plexus-server/src/db/sessions.rs`:

```rust
use serde::Serialize;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

pub const WEB_CHANNEL: &str = "web";
pub const DEFAULT_TITLE: &str = "New chat";
pub const MAX_TITLE_CHARS: usize = 120;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Session {
    pub id: Uuid,
    pub user_id: Uuid,
    pub session_key: String,
    pub channel: String,
    pub chat_id: String,
    pub title: String,
    pub last_inbound_at: Option<OffsetDateTime>,
    pub cancel_requested: bool,
    pub created_at: OffsetDateTime,
}

pub fn normalize_create_title(input: Option<&str>) -> Result<String, String> {
    let title = input.unwrap_or("").trim();
    let title = if title.is_empty() { DEFAULT_TITLE } else { title };
    validate_title(title)?;
    Ok(title.to_string())
}

pub fn normalize_rename_title(input: &str) -> Result<String, String> {
    let title = input.trim();
    if title.is_empty() {
        return Err("title must not be empty".to_string());
    }
    validate_title(title)?;
    Ok(title.to_string())
}

fn validate_title(title: &str) -> Result<(), String> {
    if title.chars().count() > MAX_TITLE_CHARS {
        return Err(format!("title must be at most {MAX_TITLE_CHARS} characters"));
    }
    Ok(())
}

pub async fn create_web_session(
    pool: &PgPool,
    user_id: Uuid,
    title: &str,
) -> Result<Session, sqlx::Error> {
    let id = Uuid::now_v7();
    let chat_id = id.to_string();
    let session_key = format!("{WEB_CHANNEL}:{chat_id}");
    sqlx::query_as::<_, Session>(
        r#"
        INSERT INTO sessions (id, user_id, session_key, channel, chat_id, title)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, user_id, session_key, channel, chat_id, title,
                  last_inbound_at, cancel_requested, created_at
        "#,
    )
    .bind(id)
    .bind(user_id)
    .bind(session_key)
    .bind(WEB_CHANNEL)
    .bind(chat_id)
    .bind(title)
    .fetch_one(pool)
    .await
}

pub async fn list_for_user(
    pool: &PgPool,
    user_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<Session>, sqlx::Error> {
    sqlx::query_as::<_, Session>(
        r#"
        SELECT id, user_id, session_key, channel, chat_id, title,
               last_inbound_at, cancel_requested, created_at
        FROM sessions
        WHERE user_id = $1
        ORDER BY last_inbound_at DESC NULLS LAST, created_at DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(user_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
}

pub async fn find_owned(
    pool: &PgPool,
    user_id: Uuid,
    session_id: Uuid,
) -> Result<Option<Session>, sqlx::Error> {
    sqlx::query_as::<_, Session>(
        r#"
        SELECT id, user_id, session_key, channel, chat_id, title,
               last_inbound_at, cancel_requested, created_at
        FROM sessions
        WHERE id = $1 AND user_id = $2
        "#,
    )
    .bind(session_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await
}

pub async fn rename_owned(
    pool: &PgPool,
    user_id: Uuid,
    session_id: Uuid,
    title: &str,
) -> Result<Option<Session>, sqlx::Error> {
    sqlx::query_as::<_, Session>(
        r#"
        UPDATE sessions
        SET title = $3
        WHERE id = $1 AND user_id = $2
        RETURNING id, user_id, session_key, channel, chat_id, title,
                  last_inbound_at, cancel_requested, created_at
        "#,
    )
    .bind(session_id)
    .bind(user_id)
    .bind(title)
    .fetch_optional(pool)
    .await
}

pub async fn delete_owned(
    pool: &PgPool,
    user_id: Uuid,
    session_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM sessions WHERE id = $1 AND user_id = $2")
        .bind(session_id)
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn touch_last_inbound(pool: &PgPool, session_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE sessions SET last_inbound_at = NOW() WHERE id = $1")
        .bind(session_id)
        .execute(pool)
        .await?;
    Ok(())
}
```

Update `plexus-server/src/db/mod.rs`:

```rust
pub mod messages;
pub mod sessions;
pub mod system_config;
pub mod users;
```

- [ ] **Step 4: Add session routes**

Create `plexus-server/src/routes/sessions.rs`:

```rust
use crate::{
    auth::AuthUser,
    db::sessions,
    error::ApiError,
};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use plexus_common::ErrorCode;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct SessionListQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Deserialize)]
pub struct CreateSessionRequest {
    title: Option<String>,
}

#[derive(Deserialize)]
pub struct RenameSessionRequest {
    title: String,
}

pub async fn list_sessions(
    auth: AuthUser,
    State(state): State<crate::app::AppState>,
    Query(query): Query<SessionListQuery>,
) -> Result<Json<Vec<sessions::Session>>, ApiError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let offset = query.offset.unwrap_or(0).max(0);
    let rows = sessions::list_for_user(state.pool(), auth.user.id, limit, offset)
        .await
        .map_err(ApiError::from_sqlx)?;
    Ok(Json(rows))
}

pub async fn create_session(
    auth: AuthUser,
    State(state): State<crate::app::AppState>,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<sessions::Session>), ApiError> {
    let title = sessions::normalize_create_title(req.title.as_deref())
        .map_err(ApiError::invalid_args)?;
    let session = sessions::create_web_session(state.pool(), auth.user.id, &title)
        .await
        .map_err(ApiError::from_sqlx)?;
    Ok((StatusCode::CREATED, Json(session)))
}

pub async fn get_session(
    auth: AuthUser,
    State(state): State<crate::app::AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<Json<sessions::Session>, ApiError> {
    let session = owned_session_or_404(state.pool(), auth.user.id, session_id).await?;
    Ok(Json(session))
}

pub async fn rename_session(
    auth: AuthUser,
    State(state): State<crate::app::AppState>,
    Path(session_id): Path<Uuid>,
    Json(req): Json<RenameSessionRequest>,
) -> Result<Json<sessions::Session>, ApiError> {
    let title = sessions::normalize_rename_title(&req.title).map_err(ApiError::invalid_args)?;
    let session = sessions::rename_owned(state.pool(), auth.user.id, session_id, &title)
        .await
        .map_err(ApiError::from_sqlx)?
        .ok_or_else(not_found)?;
    Ok(Json(session))
}

pub async fn delete_session(
    auth: AuthUser,
    State(state): State<crate::app::AppState>,
    Path(session_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = sessions::delete_owned(state.pool(), auth.user.id, session_id)
        .await
        .map_err(ApiError::from_sqlx)?;
    if !deleted {
        return Err(not_found());
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn owned_session_or_404(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    session_id: Uuid,
) -> Result<sessions::Session, ApiError> {
    sessions::find_owned(pool, user_id, session_id)
        .await
        .map_err(ApiError::from_sqlx)?
        .ok_or_else(not_found)
}

fn not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, ErrorCode::NotFound, "session not found")
}
```

Mount routes in `plexus-server/src/routes/mod.rs`:

```rust
pub mod sessions;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/logout", post(auth::logout))
        .route("/api/me", get(me::get_me).patch(me::patch_me))
        .route(
            "/api/admin/config",
            get(admin::get_config).patch(admin::patch_config),
        )
        .route(
            "/api/sessions",
            get(sessions::list_sessions).post(sessions::create_session),
        )
        .route(
            "/api/sessions/{id}",
            get(sessions::get_session)
                .patch(sessions::rename_session)
                .delete(sessions::delete_session),
        )
}
```

- [ ] **Step 5: Run session lifecycle tests**

Run:

```bash
rtk cargo fmt --all
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_sessions
```

Expected: PASS.

- [ ] **Step 6: Commit the session lifecycle slice**

```bash
git add plexus-server/src/db/mod.rs plexus-server/src/db/sessions.rs plexus-server/src/routes/mod.rs plexus-server/src/routes/sessions.rs plexus-server/tests/m1c_sessions.rs
git commit -m "feat: add browser session lifecycle"
```

---

## Task 3: Content Blocks, Message Persistence, and Browser Message POST

**Files:**
- Create: `plexus-server/src/chat/mod.rs`
- Create: `plexus-server/src/chat/content.rs`
- Create: `plexus-server/src/chat/sse.rs`
- Create: `plexus-server/src/db/messages.rs`
- Create: `plexus-server/tests/m1c_messages.rs`
- Modify: `plexus-server/src/app.rs`
- Modify: `plexus-server/src/lib.rs`
- Modify: `plexus-server/src/db/mod.rs`
- Modify: `plexus-server/src/routes/mod.rs`
- Modify: `plexus-server/src/routes/sessions.rs`

- [ ] **Step 1: Write failing message route tests**

Create `plexus-server/tests/m1c_messages.rs` with tests for content normalization, image validation, auth, and web-only writes:

```rust
mod support;

use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use support::TestApp;
use tower::ServiceExt;

async fn json_request(
    app: axum::Router,
    method: Method,
    path: &str,
    body: Value,
    auth: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = auth {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, json)
}

async fn register_and_create_session(app: &TestApp) -> (String, String) {
    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        "/api/auth/register",
        json!({
            "email": "alice@example.com",
            "password": "correct horse battery staple",
            "name": "Alice"
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token = body["jwt"].as_str().unwrap().to_string();

    let (status, body) =
        json_request(app.router.clone(), Method::POST, "/api/sessions", json!({}), Some(&token)).await;
    assert_eq!(status, StatusCode::CREATED);
    (token, body["id"].as_str().unwrap().to_string())
}

#[tokio::test]
async fn post_text_message_persists_runtime_and_content_blocks() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;

    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({"content": [{"type": "text", "text": "hello"}]}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let message_id = body["message_id"].as_str().unwrap();

    let stored: (serde_json::Value,) =
        sqlx::query_as("SELECT content FROM messages WHERE id = $1")
            .bind(uuid::Uuid::parse_str(message_id).unwrap())
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let blocks = stored.0.as_array().unwrap();
    assert!(blocks[0]["text"].as_str().unwrap().contains("<runtime>"));
    assert_eq!(blocks[1], json!({"type": "text", "text": "hello"}));
}

#[tokio::test]
async fn post_empty_forms_are_accepted() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;
    for body in [json!({}), json!({"content": ""}), json!({"content": []})] {
        let (status, _) = json_request(
            app.router.clone(),
            Method::POST,
            &format!("/api/sessions/{session_id}/messages"),
            body,
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }
}

#[tokio::test]
async fn inline_base64_image_is_accepted_but_external_url_is_rejected() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;

    let (status, _) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({"content": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8="}}]}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, _) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({"content": [{"type": "image_url", "image_url": {"url": "https://example.com/cat.png"}}]}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_messages
```

Expected: FAIL because message routes and content blocks do not exist yet.

- [ ] **Step 3: Add content block normalization**

Create `plexus-server/src/chat/content.rs`:

```rust
use crate::error::ApiError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text { text: String },
    ImageUrl { image_url: ImageUrlBlock },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImageUrlBlock {
    pub url: String,
}

impl ContentBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn is_image(&self) -> bool {
        matches!(self, Self::ImageUrl { .. })
    }
}

pub fn normalize_user_content(raw: Option<Value>) -> Result<Vec<ContentBlock>, ApiError> {
    match raw {
        None => Ok(Vec::new()),
        Some(Value::String(text)) if text.is_empty() => Ok(Vec::new()),
        Some(Value::String(text)) => Ok(vec![ContentBlock::text(text)]),
        Some(Value::Array(values)) => values.into_iter().map(parse_block).collect(),
        Some(Value::Null) => Err(ApiError::invalid_args("content must not be null")),
        Some(_) => Err(ApiError::invalid_args("content must be a string or array")),
    }
}

pub fn strip_images(blocks: &[ContentBlock]) -> Vec<ContentBlock> {
    blocks
        .iter()
        .filter(|block| !block.is_image())
        .cloned()
        .collect()
}

pub fn contains_image(blocks: &[ContentBlock]) -> bool {
    blocks.iter().any(ContentBlock::is_image)
}

fn parse_block(value: Value) -> Result<ContentBlock, ApiError> {
    let block: ContentBlock = serde_json::from_value(value)
        .map_err(|_| ApiError::invalid_args("content block is malformed"))?;
    validate_block(&block)?;
    Ok(block)
}

fn validate_block(block: &ContentBlock) -> Result<(), ApiError> {
    if let ContentBlock::ImageUrl { image_url } = block {
        validate_data_image_url(&image_url.url)?;
    }
    Ok(())
}

fn validate_data_image_url(url: &str) -> Result<(), ApiError> {
    let Some(rest) = url.strip_prefix("data:image/") else {
        return Err(ApiError::invalid_args(
            "M1c only accepts inline data:image/...;base64 image URLs",
        ));
    };
    let Some((mime_tail, data)) = rest.split_once(";base64,") else {
        return Err(ApiError::invalid_args(
            "image_url.url must be a base64 data URL",
        ));
    };
    if mime_tail.is_empty()
        || !mime_tail
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
        || data.is_empty()
        || !data
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '/' | '='))
    {
        return Err(ApiError::invalid_args(
            "image_url.url must be a valid data:image/...;base64 URL",
        ));
    }
    Ok(())
}
```

Create `plexus-server/src/chat/mod.rs`:

```rust
pub mod content;
pub mod prompt;
pub mod sse;
pub mod worker;
```

Update `plexus-server/src/lib.rs`:

```rust
pub mod chat;
```

- [ ] **Step 4: Add message DB helpers**

Create `plexus-server/src/db/messages.rs`:

```rust
use crate::chat::content::ContentBlock;
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Message {
    pub id: Uuid,
    pub session_id: Uuid,
    pub role: String,
    pub content: Value,
    pub is_compaction_summary: bool,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LatestUserMessage {
    pub id: Uuid,
    pub created_at: OffsetDateTime,
}

pub async fn insert_message(
    pool: &PgPool,
    session_id: Uuid,
    role: &str,
    content: Vec<ContentBlock>,
) -> Result<Message, sqlx::Error> {
    let content = serde_json::to_value(content).expect("content blocks serialize");
    sqlx::query_as::<_, Message>(
        r#"
        INSERT INTO messages (session_id, role, content)
        VALUES ($1, $2, $3)
        RETURNING id, session_id, role, content, is_compaction_summary, created_at
        "#,
    )
    .bind(session_id)
    .bind(role)
    .bind(content)
    .fetch_one(pool)
    .await
}

pub async fn list_before(
    pool: &PgPool,
    session_id: Uuid,
    before: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Message>, sqlx::Error> {
    if let Some(before) = before {
        sqlx::query_as::<_, Message>(
            r#"
            SELECT m.id, m.session_id, m.role, m.content, m.is_compaction_summary, m.created_at
            FROM messages m
            JOIN messages anchor ON anchor.id = $2 AND anchor.session_id = $1
            WHERE m.session_id = $1 AND m.created_at < anchor.created_at
            ORDER BY m.created_at DESC
            LIMIT $3
            "#,
        )
        .bind(session_id)
        .bind(before)
        .bind(limit)
        .fetch_all(pool)
        .await
    } else {
        sqlx::query_as::<_, Message>(
            r#"
            SELECT id, session_id, role, content, is_compaction_summary, created_at
            FROM messages
            WHERE session_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(session_id)
        .bind(limit)
        .fetch_all(pool)
        .await
    }
}

pub async fn replay_recent(
    pool: &PgPool,
    session_id: Uuid,
    limit: i64,
) -> Result<Vec<Message>, sqlx::Error> {
    let mut rows = sqlx::query_as::<_, Message>(
        r#"
        SELECT id, session_id, role, content, is_compaction_summary, created_at
        FROM messages
        WHERE session_id = $1
        ORDER BY created_at DESC
        LIMIT $2
        "#,
    )
    .bind(session_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.reverse();
    Ok(rows)
}

pub async fn replay_after(
    pool: &PgPool,
    session_id: Uuid,
    after: Uuid,
) -> Result<Vec<Message>, sqlx::Error> {
    sqlx::query_as::<_, Message>(
        r#"
        SELECT m.id, m.session_id, m.role, m.content, m.is_compaction_summary, m.created_at
        FROM messages m
        JOIN messages anchor ON anchor.id = $2 AND anchor.session_id = $1
        WHERE m.session_id = $1 AND m.created_at > anchor.created_at
        ORDER BY m.created_at ASC
        "#,
    )
    .bind(session_id)
    .bind(after)
    .fetch_all(pool)
    .await
}

pub async fn latest_user_message(
    pool: &PgPool,
    session_id: Uuid,
) -> Result<Option<LatestUserMessage>, sqlx::Error> {
    sqlx::query_as::<_, LatestUserMessage>(
        r#"
        SELECT id, created_at
        FROM messages
        WHERE session_id = $1 AND role = 'user'
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await
}

pub async fn history_chronological(
    pool: &PgPool,
    session_id: Uuid,
) -> Result<Vec<Message>, sqlx::Error> {
    sqlx::query_as::<_, Message>(
        r#"
        SELECT id, session_id, role, content, is_compaction_summary, created_at
        FROM messages
        WHERE session_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(session_id)
    .fetch_all(pool)
    .await
}
```

Update `plexus-server/src/db/mod.rs` to export `messages`.

- [ ] **Step 5: Add chat runtime and broadcaster shell**

Create `plexus-server/src/chat/sse.rs`:

```rust
use crate::db::messages::Message;
use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct SseBroker {
    inner: Arc<Mutex<HashMap<Uuid, broadcast::Sender<Message>>>>,
}

impl SseBroker {
    pub async fn subscribe(&self, session_id: Uuid) -> broadcast::Receiver<Message> {
        self.sender(session_id).await.subscribe()
    }

    pub async fn broadcast(&self, message: Message) {
        let sender = self.sender(message.session_id).await;
        let _ = sender.send(message);
    }

    async fn sender(&self, session_id: Uuid) -> broadcast::Sender<Message> {
        let mut inner = self.inner.lock().await;
        inner
            .entry(session_id)
            .or_insert_with(|| broadcast::channel(256).0)
            .clone()
    }
}
```

Update `plexus-server/src/chat/mod.rs`:

```rust
use std::{
    collections::HashSet,
    sync::Arc,
};
use tokio::sync::Mutex;
use uuid::Uuid;

pub mod content;
pub mod prompt;
pub mod sse;
pub mod worker;

#[derive(Clone, Default)]
pub struct ChatRuntime {
    broker: sse::SseBroker,
    active_workers: Arc<Mutex<HashSet<Uuid>>>,
}

impl ChatRuntime {
    pub fn broker(&self) -> &sse::SseBroker {
        &self.broker
    }

    pub async fn try_start_worker(&self, session_id: Uuid) -> bool {
        self.active_workers.lock().await.insert(session_id)
    }

    pub async fn finish_worker(&self, session_id: Uuid) {
        self.active_workers.lock().await.remove(&session_id);
    }
}
```

Update `plexus-server/src/app.rs`:

```rust
use crate::{chat::ChatRuntime, config::ServerConfig, openai::OpenAiRuntime, routes};

pub struct AppStateInner {
    pub pool: PgPool,
    pub config: ServerConfig,
    pub openai: OpenAiRuntime,
    pub chat: ChatRuntime,
    pub admin_config_lock: Mutex<()>,
}

pub fn chat(&self) -> &ChatRuntime {
    &self.inner.chat
}
```

Initialize `chat: ChatRuntime::default()` in `AppState::new_with_openai_runtime`.

- [ ] **Step 6: Add message POST and history routes**

Extend `plexus-server/src/routes/sessions.rs` with:

```rust
use crate::{
    chat::content::{ContentBlock, normalize_user_content},
    db::messages,
};
use serde_json::{Map, Value, json};

#[derive(Deserialize)]
pub struct MessageHistoryQuery {
    before: Option<Uuid>,
    limit: Option<i64>,
}

pub async fn list_messages(
    auth: AuthUser,
    State(state): State<crate::app::AppState>,
    Path(session_id): Path<Uuid>,
    Query(query): Query<MessageHistoryQuery>,
) -> Result<Json<Vec<messages::Message>>, ApiError> {
    let _session = owned_session_or_404(state.pool(), auth.user.id, session_id).await?;
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let rows = messages::list_before(state.pool(), session_id, query.before, limit)
        .await
        .map_err(ApiError::from_sqlx)?;
    Ok(Json(rows))
}

pub async fn post_message(
    auth: AuthUser,
    State(state): State<crate::app::AppState>,
    Path(session_id): Path<Uuid>,
    Json(body): Json<Map<String, Value>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let session = owned_session_or_404(state.pool(), auth.user.id, session_id).await?;
    if session.channel != sessions::WEB_CHANNEL {
        return Err(ApiError::invalid_args(
            "browser REST can only write to web sessions",
        ));
    }

    let mut content = vec![runtime_block(&session)];
    content.extend(normalize_user_content(body.get("content").cloned())?);

    let message = messages::insert_message(state.pool(), session.id, "user", content)
        .await
        .map_err(ApiError::from_sqlx)?;
    sessions::touch_last_inbound(state.pool(), session.id)
        .await
        .map_err(ApiError::from_sqlx)?;
    state.chat().broker().broadcast(message.clone()).await;

    Ok((StatusCode::ACCEPTED, Json(json!({ "message_id": message.id }))))
}

fn runtime_block(session: &sessions::Session) -> ContentBlock {
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    ContentBlock::text(format!(
        "<runtime>\ntime: {now}\nchannel: {}\nchat_id: {}\n</runtime>",
        session.channel, session.chat_id
    ))
}
```

Mount the routes:

```rust
.route(
    "/api/sessions/{id}/messages",
    get(sessions::list_messages).post(sessions::post_message),
)
```

- [ ] **Step 7: Run message tests**

Run:

```bash
rtk cargo fmt --all
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_messages
```

Expected: PASS.

- [ ] **Step 8: Commit the message persistence slice**

```bash
git add plexus-server/src/app.rs plexus-server/src/chat plexus-server/src/db/mod.rs plexus-server/src/db/messages.rs plexus-server/src/lib.rs plexus-server/src/routes/mod.rs plexus-server/src/routes/sessions.rs plexus-server/tests/m1c_messages.rs
git commit -m "feat: persist browser chat messages"
```

---

## Task 4: SSE Replay and Live Message Delivery

**Files:**
- Create: `plexus-server/tests/m1c_sse.rs`
- Modify: `plexus-server/src/routes/sessions.rs`
- Modify: `plexus-server/src/chat/sse.rs`

- [ ] **Step 1: Write failing SSE tests**

Create `plexus-server/tests/m1c_sse.rs`. The tests should connect to the router with `GET /api/sessions/{id}/stream`, collect SSE chunks from the response body, and assert replay and `history_end`.

Use these helpers to read enough SSE text without waiting for the infinite stream to close:

```rust
async fn read_sse_until(
    response: axum::response::Response,
    expected: &str,
    timeout: std::time::Duration,
) -> String {
    let mut body = response.into_body();
    let deadline = std::time::Instant::now() + timeout;
    let mut out = String::new();
    while std::time::Instant::now() < deadline {
        let Some(frame) = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            body.frame(),
        )
        .await
        .ok()
        .flatten()
        else {
            continue;
        };
        let frame = frame.unwrap();
        if let Some(bytes) = frame.data_ref() {
            out.push_str(std::str::from_utf8(bytes).unwrap());
            if out.contains(expected) {
                return out;
            }
        }
    }
    panic!("timed out waiting for SSE text {expected:?}; got {out}");
}
```

Add two tests:

```rust
#[tokio::test]
async fn sse_replays_messages_then_history_end() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_create_and_post(&app, "hello").await;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/sessions/{session_id}/stream?replay_limit=50"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = read_sse_until(
        response,
        "event: history_end",
        std::time::Duration::from_secs(1),
    )
    .await;
    assert!(text.contains("event: message"));
    assert!(text.contains("event: history_end"));
    assert!(text.contains("hello"));
}
```

For the live test, open the stream first, then post a message, and assert the stream emits `history_end` followed by the new `message`.

- [ ] **Step 2: Run SSE tests to verify they fail**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_sse
```

Expected: FAIL because the stream route is not implemented.

- [ ] **Step 3: Add SSE event construction**

Extend `plexus-server/src/chat/sse.rs`:

```rust
use axum::response::sse::Event;
use std::convert::Infallible;

pub fn message_event(message: &Message) -> Result<Event, Infallible> {
    Ok(Event::default()
        .event("message")
        .id(message.id.to_string())
        .json_data(message)
        .expect("message serializes for SSE"))
}

pub fn history_end_event() -> Result<Event, Infallible> {
    Ok(Event::default().event("history_end").data("{}"))
}
```

- [ ] **Step 4: Implement the stream route**

Add to `plexus-server/src/routes/sessions.rs`:

```rust
use axum::response::sse::{KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use std::convert::Infallible;

#[derive(Deserialize)]
pub struct StreamQuery {
    replay_limit: Option<i64>,
}

pub async fn stream_session(
    auth: AuthUser,
    State(state): State<crate::app::AppState>,
    Path(session_id): Path<Uuid>,
    Query(query): Query<StreamQuery>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let _session = owned_session_or_404(state.pool(), auth.user.id, session_id).await?;
    let mut receiver = state.chat().broker().subscribe(session_id).await;
    let replay_limit = query.replay_limit.unwrap_or(50).clamp(0, 200);
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok());

    let replay = if let Some(last_event_id) = last_event_id {
        messages::replay_after(state.pool(), session_id, last_event_id)
            .await
            .map_err(ApiError::from_sqlx)?
    } else if replay_limit == 0 {
        Vec::new()
    } else {
        messages::replay_recent(state.pool(), session_id, replay_limit)
            .await
            .map_err(ApiError::from_sqlx)?
    };

    let stream = async_stream::stream! {
        for message in replay {
            yield crate::chat::sse::message_event(&message);
        }
        yield crate::chat::sse::history_end_event();
        loop {
            match receiver.recv().await {
                Ok(message) => yield crate::chat::sse::message_event(&message),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()).into_response())
}
```

Mount:

```rust
.route("/api/sessions/{id}/stream", get(sessions::stream_session))
```

- [ ] **Step 5: Run SSE tests**

Run:

```bash
rtk cargo fmt --all
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_sse
```

Expected: PASS.

- [ ] **Step 6: Commit the SSE slice**

```bash
git add plexus-server/src/chat/sse.rs plexus-server/src/routes/mod.rs plexus-server/src/routes/sessions.rs plexus-server/tests/m1c_sse.rs
git commit -m "feat: add chat SSE replay and live events"
```

---

## Task 5: OpenAI Content Arrays, Image Fallback, and Fake Provider Modes

**Files:**
- Modify: `plexus-server/src/openai.rs`
- Modify: `plexus-server/tests/support/fake_openai.rs`
- Modify: `plexus-server/tests/m1b_openai_client.rs`

- [ ] **Step 1: Update OpenAI client tests first**

In `plexus-server/tests/m1b_openai_client.rs`, change string-content request helpers to content arrays:

```rust
use plexus_server::chat::content::ContentBlock;

fn user_message(text: &str) -> ChatMessage {
    ChatMessage {
        role: ChatRole::User,
        content: vec![ContentBlock::text(text)],
    }
}
```

Add M1c-specific tests:

```rust
#[tokio::test]
async fn chat_completion_sends_content_arrays() {
    let fake = FakeOpenAi::valid().await;
    let response = OpenAiClient::new()
        .chat_completion(
            &config(&fake),
            ChatCompletionRequest {
                messages: vec![user_message("hello")],
                max_tokens: None,
                temperature: None,
            },
        )
        .await
        .expect("chat response");
    assert_eq!(response.content, "hi");
}

#[tokio::test]
async fn image_payload_error_retries_with_images_stripped() {
    let fake = FakeOpenAi::image_unsupported_then_valid().await;
    let response = OpenAiClient::new()
        .chat_completion(
            &config(&fake),
            ChatCompletionRequest {
                messages: vec![ChatMessage {
                    role: ChatRole::User,
                    content: vec![
                        ContentBlock::text("what is this"),
                        ContentBlock::ImageUrl {
                            image_url: plexus_server::chat::content::ImageUrlBlock {
                                url: "data:image/png;base64,aGVsbG8=".to_string(),
                            },
                        },
                    ],
                }],
                max_tokens: None,
                temperature: None,
            },
        )
        .await
        .expect("stripped retry response");
    assert_eq!(response.content, "image stripped fallback");
    assert_eq!(fake.chat_call_count(), 2);
}

#[tokio::test]
async fn auth_error_does_not_retry_or_strip_images() {
    let fake = FakeOpenAi::valid().await;
    let mut cfg = config(&fake);
    cfg.api_key = plexus_common::LlmApiKey::new("wrong-key".to_string());
    let err = OpenAiClient::new()
        .chat_completion(
            &cfg,
            ChatCompletionRequest {
                messages: vec![user_message("hello")],
                max_tokens: None,
                temperature: None,
            },
        )
        .await
        .expect_err("auth failure");
    assert_eq!(err.status, axum::http::StatusCode::BAD_GATEWAY);
    assert!(err.message.contains("HTTP 401"));
}
```

- [ ] **Step 2: Run OpenAI tests to verify they fail**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1b_openai_client
```

Expected: FAIL because `ChatMessage.content` is still a string and fake provider lacks the new modes.

- [ ] **Step 3: Change OpenAI request types to content arrays**

Modify `plexus-server/src/openai.rs`:

```rust
use crate::{chat::content::{contains_image, strip_images, ContentBlock}, error::ApiError};

#[derive(Clone, Debug, Serialize)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: Vec<ContentBlock>,
}
```

Keep `ChatCompletionRequest` unchanged except that its `messages` now contain content arrays.

- [ ] **Step 4: Implement retry paths with image stripping**

Replace `OpenAiClient::chat_completion` with a two-path implementation:

```rust
pub async fn chat_completion(
    &self,
    cfg: &OpenAiConfig,
    request: ChatCompletionRequest,
) -> Result<ChatCompletionResponse, ApiError> {
    let has_images = request
        .messages
        .iter()
        .any(|message| contains_image(&message.content));

    match self.chat_completion_attempts(cfg, request.clone()).await {
        Ok(response) => Ok(response),
        Err(err) if has_images && err.message.contains("image-compatible retry") => {
            let stripped = ChatCompletionRequest {
                messages: request
                    .messages
                    .into_iter()
                    .map(|message| ChatMessage {
                        role: message.role,
                        content: strip_images(&message.content),
                    })
                    .collect(),
                max_tokens: request.max_tokens,
                temperature: request.temperature,
            };
            self.chat_completion_attempts(cfg, stripped).await
        }
        Err(err) => Err(err),
    }
}
```

Implement `chat_completion_attempts` by moving the existing retry loop into a helper. When a non-success status is returned, classify it:

```rust
fn is_auth_or_config_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN
}

fn is_image_compatibility_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 400 | 413 | 415 | 422)
}

fn provider_status_error(status: reqwest::StatusCode) -> ApiError {
    if is_image_compatibility_status(status) {
        provider_http_error(format!(
            "LLM chat request returned HTTP {status}; image-compatible retry"
        ))
    } else {
        provider_http_error(format!("LLM chat request returned HTTP {status}"))
    }
}
```

Do not include raw provider response bodies in `ApiError.message`. If response body text is read for classification, only use it internally to decide whether to set the `"image-compatible retry"` marker.

- [ ] **Step 5: Update fake provider**

Extend `FakeMode` in `plexus-server/tests/support/fake_openai.rs`:

```rust
enum FakeMode {
    Valid,
    MissingModel,
    MalformedModels,
    ImageUnsupportedThenValid,
    AlwaysUnavailable,
}
```

Add an atomic chat call counter to `FakeState` and expose:

```rust
pub fn chat_call_count(&self) -> usize {
    self.chat_calls.load(Ordering::SeqCst)
}
```

In `chat`, parse the last user content as either a string or an array:

```rust
fn content_to_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| {
                if block.get("type").and_then(Value::as_str) == Some("text") {
                    block.get("text").and_then(Value::as_str)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
        _ => String::new(),
    }
}
```

For `ImageUnsupportedThenValid`, return `415` when any `image_url` block is present; return `"image stripped fallback"` when the stripped retry arrives.

- [ ] **Step 6: Run OpenAI tests**

Run:

```bash
rtk cargo fmt --all
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1b_openai_client
```

Expected: PASS.

- [ ] **Step 7: Commit the provider content-array slice**

```bash
git add plexus-server/src/openai.rs plexus-server/tests/support/fake_openai.rs plexus-server/tests/m1b_openai_client.rs
git commit -m "feat: send OpenAI content arrays"
```

---

## Task 6: Minimal Prompt Builder and Stored LLM Config Loader

**Files:**
- Create: `plexus-server/src/chat/prompt.rs`
- Modify: `plexus-server/src/db/system_config.rs`
- Create: `plexus-server/tests/m1c_worker.rs`

- [ ] **Step 1: Write prompt/config tests**

Start `plexus-server/tests/m1c_worker.rs` with prompt-only tests:

```rust
mod support;

use plexus_server::{
    chat::prompt,
    db::{sessions, system_config, users},
};
use serde_json::json;
use support::TestApp;

#[tokio::test]
async fn prompt_reads_optional_soul_and_memory() {
    let app = TestApp::spawn().await;
    let user = users::create_user(&app.pool, "alice@example.com", "hash", "Alice", false)
        .await
        .unwrap();
    let user_dir = app.workspace_root.path().join(user.id.to_string());
    tokio::fs::create_dir_all(&user_dir).await.unwrap();
    tokio::fs::write(user_dir.join("SOUL.md"), "Be concise.").await.unwrap();
    tokio::fs::write(user_dir.join("MEMORY.md"), "Alice likes trains.").await.unwrap();
    let session = sessions::create_web_session(&app.pool, user.id, "New chat")
        .await
        .unwrap();

    let text = prompt::build_system_prompt(app.workspace_root.path(), &user, &session)
        .await
        .unwrap();
    assert!(text.contains("## SOUL"));
    assert!(text.contains("Be concise."));
    assert!(text.contains("## MEMORY"));
    assert!(text.contains("Alice likes trains."));
    assert!(text.contains("M1c has no tools available"));
}

#[tokio::test]
async fn stored_llm_config_requires_identity_values() {
    let app = TestApp::spawn().await;
    let err = system_config::current_llm_config(&app.pool)
        .await
        .expect_err("missing config should reject");
    assert_eq!(err.code, plexus_common::ErrorCode::InvalidArgs);

    let mut values = std::collections::BTreeMap::new();
    values.insert("llm_endpoint".to_string(), json!("http://127.0.0.1:1234/v1"));
    values.insert("llm_api_key".to_string(), json!("test-key"));
    values.insert("llm_model".to_string(), json!("test-model"));
    let mut tx = app.pool.begin().await.unwrap();
    system_config::set_many(&mut tx, &values).await.unwrap();
    tx.commit().await.unwrap();
    let cfg = system_config::current_llm_config(&app.pool)
        .await
        .expect("stored config");
    assert_eq!(cfg.model, "test-model");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_worker prompt
```

Expected: FAIL because `chat::prompt` and `current_llm_config` do not exist.

- [ ] **Step 3: Implement prompt builder**

Create `plexus-server/src/chat/prompt.rs`:

```rust
use crate::{db::{sessions::Session, users::User}, error::ApiError};
use std::path::{Path, PathBuf};

pub async fn build_system_prompt(
    workspace_root: &Path,
    user: &User,
    session: &Session,
) -> Result<String, ApiError> {
    let user_root = workspace_root.join(user.id.to_string());
    let soul = read_optional(user_root.join("SOUL.md")).await?;
    let memory = read_optional(user_root.join("MEMORY.md")).await?;

    Ok(format!(
        "## SOUL\n\n{soul}\n\n---\n\n\
         ## MEMORY\n\n{memory}\n\n---\n\n\
         ## Identity\n\n\
         You are Plexus, partnered with one human: {name} (account `{id}`).\n\
         Input typed directly by {name} in this browser chat is authoritative.\n\n---\n\n\
         ## Channels\n\n\
         Current channel: web\n\
         Current chat_id: {chat_id}\n\
         Direct replies go to this browser session.\n\n---\n\n\
         ## Operating Notes\n\n\
         M1c has no tools available. Answer in plain text. Do not claim access to files, devices, MCP, workspace tools, cron, Discord, Telegram, or message tools.",
        soul = soul,
        memory = memory,
        name = user.name,
        id = user.id,
        chat_id = session.chat_id,
    ))
}

async fn read_optional(path: PathBuf) -> Result<String, ApiError> {
    match tokio::fs::read_to_string(path).await {
        Ok(text) => Ok(text),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(err) => Err(ApiError::invalid_args(format!(
            "failed to read prompt file: {err}"
        ))),
    }
}
```

- [ ] **Step 4: Implement stored LLM config helper**

Add to `plexus-server/src/db/system_config.rs`:

```rust
pub async fn current_llm_config(pool: &PgPool) -> Result<crate::openai::OpenAiConfig, ApiError> {
    let current = get_all(pool).await.map_err(ApiError::from_sqlx)?;
    merged_llm_config(&current, &BTreeMap::new())
}
```

- [ ] **Step 5: Run prompt/config tests**

Run:

```bash
rtk cargo fmt --all
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_worker prompt stored_llm_config
```

Expected: PASS.

- [ ] **Step 6: Commit prompt/config slice**

```bash
git add plexus-server/src/chat/prompt.rs plexus-server/src/db/system_config.rs plexus-server/tests/m1c_worker.rs
git commit -m "feat: build minimal M1c prompt"
```

---

## Task 7: Response Worker, Serialized Per-Session Execution, and Synthetic Failures

**Files:**
- Create/modify: `plexus-server/src/chat/worker.rs`
- Modify: `plexus-server/src/routes/sessions.rs`
- Modify: `plexus-server/tests/m1c_worker.rs`
- Modify: `plexus-server/tests/support/fake_openai.rs`

- [ ] **Step 1: Add failing worker tests**

Extend `plexus-server/tests/m1c_worker.rs` with:

- `post_message_calls_fake_provider_and_persists_assistant`
- `missing_llm_config_persists_synthetic_assistant_message`
- `provider_failure_message_is_secret_free`
- `concurrent_posts_to_one_session_do_not_create_parallel_fake_provider_calls`
- `image_compatibility_failure_retries_stripped_and_persists_assistant`

Use a helper that registers an admin, configures fake LLM through `/api/admin/config`, creates a session, posts a message, then polls PostgreSQL until an assistant row appears:

```rust
async fn wait_for_assistant(app: &TestApp, session_id: uuid::Uuid) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if let Some((content,)) = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT content FROM messages
             WHERE session_id = $1 AND role = 'assistant'
             ORDER BY created_at DESC
             LIMIT 1",
        )
        .bind(session_id)
        .fetch_optional(&app.pool)
        .await
        .unwrap()
        {
            return content;
        }
        assert!(std::time::Instant::now() < deadline, "assistant message timed out");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
```

- [ ] **Step 2: Run worker tests to verify they fail**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_worker
```

Expected: FAIL because no worker is spawned.

- [ ] **Step 3: Implement worker payload assembly**

Create `plexus-server/src/chat/worker.rs`:

```rust
use crate::{
    chat::{content::ContentBlock, prompt},
    db::{messages, sessions, system_config},
    openai::{ChatCompletionRequest, ChatMessage, ChatRole},
};
use uuid::Uuid;

pub fn spawn_response_worker(state: crate::app::AppState, session_id: Uuid) {
    tokio::spawn(async move {
        if !state.chat().try_start_worker(session_id).await {
            return;
        }
        let result = run_worker_loop(state.clone(), session_id).await;
        state.chat().finish_worker(session_id).await;
        if let Err(err) = result {
            let content = vec![ContentBlock::text(synthetic_error_text(&err.message))];
            if let Ok(message) = messages::insert_message(state.pool(), session_id, "assistant", content).await {
                state.chat().broker().broadcast(message).await;
            }
        }
    });
}

async fn run_worker_loop(
    state: crate::app::AppState,
    session_id: Uuid,
) -> Result<(), crate::error::ApiError> {
    let mut last_answered_user_id = None;
    loop {
        let Some(latest) = messages::latest_user_message(state.pool(), session_id)
            .await
            .map_err(crate::error::ApiError::from_sqlx)?
        else {
            return Ok(());
        };
        if Some(latest.id) == last_answered_user_id {
            return Ok(());
        }

        respond_once(state.clone(), session_id).await?;
        last_answered_user_id = Some(latest.id);
    }
}

async fn respond_once(
    state: crate::app::AppState,
    session_id: Uuid,
) -> Result<(), crate::error::ApiError> {
    let session = sessions::find_by_id(state.pool(), session_id)
        .await
        .map_err(crate::error::ApiError::from_sqlx)?
        .ok_or_else(|| crate::error::ApiError::invalid_args("session disappeared"))?;
    let user = crate::db::users::find_by_id(state.pool(), session.user_id)
        .await
        .map_err(crate::error::ApiError::from_sqlx)?
        .ok_or_else(|| crate::error::ApiError::invalid_args("session user disappeared"))?;
    let cfg = system_config::current_llm_config(state.pool()).await?;
    let system_prompt = prompt::build_system_prompt(&state.config().workspace_root, &user, &session).await?;
    let history = messages::history_chronological(state.pool(), session_id)
        .await
        .map_err(crate::error::ApiError::from_sqlx)?;

    let mut chat_messages = vec![ChatMessage {
        role: ChatRole::System,
        content: vec![ContentBlock::text(system_prompt)],
    }];
    for row in history {
        chat_messages.push(ChatMessage {
            role: match row.role.as_str() {
                "user" => ChatRole::User,
                "assistant" => ChatRole::Assistant,
                _ => continue,
            },
            content: serde_json::from_value(row.content)
                .map_err(|_| crate::error::ApiError::invalid_args("stored message content was malformed"))?,
        });
    }

    let response = state
        .openai()
        .chat_completion(
            &cfg,
            ChatCompletionRequest {
                messages: chat_messages,
                max_tokens: None,
                temperature: None,
            },
        )
        .await;

    let assistant_text = match response {
        Ok(response) => response.content,
        Err(err) => synthetic_error_text(&err.message),
    };
    let message = messages::insert_message(
        state.pool(),
        session_id,
        "assistant",
        vec![ContentBlock::text(assistant_text)],
    )
    .await
    .map_err(crate::error::ApiError::from_sqlx)?;
    state.chat().broker().broadcast(message).await;
    Ok(())
}

fn synthetic_error_text(message: &str) -> String {
    let safe = message
        .replace("Bearer ", "")
        .replace("plexus-mock-key", "[redacted]");
    format!("[Plexus could not complete the LLM request: {safe}. Try again later.]")
}
```

Add `sessions::find_by_id(pool, session_id)` that fetches by id without an auth user:

```rust
pub async fn find_by_id(pool: &PgPool, session_id: Uuid) -> Result<Option<Session>, sqlx::Error> {
    sqlx::query_as::<_, Session>(
        r#"
        SELECT id, user_id, session_key, channel, chat_id, title,
               last_inbound_at, cancel_requested, created_at
        FROM sessions
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await
}
```

- [ ] **Step 4: Spawn worker from message POST**

At the end of `post_message`, after broadcast:

```rust
crate::chat::worker::spawn_response_worker(state.clone(), session.id);
```

The route still returns `202` immediately.

- [ ] **Step 5: Run worker tests**

Run:

```bash
rtk cargo fmt --all
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_worker
```

Expected: PASS.

- [ ] **Step 6: Commit worker slice**

```bash
git add plexus-server/src/chat/worker.rs plexus-server/src/db/sessions.rs plexus-server/src/routes/sessions.rs plexus-server/tests/m1c_worker.rs plexus-server/tests/support/fake_openai.rs
git commit -m "feat: add M1c response worker"
```

---

## Task 8: End-to-End SSE Worker Integration and Ownership Hardening

**Files:**
- Modify: `plexus-server/tests/m1c_sse.rs`
- Modify: `plexus-server/tests/m1c_messages.rs`
- Modify: `plexus-server/tests/m1c_worker.rs`
- Modify if tests expose gaps: `plexus-server/src/routes/sessions.rs`, `plexus-server/src/chat/worker.rs`, `plexus-server/src/db/messages.rs`

- [ ] **Step 1: Add integration tests**

Add tests that exercise the full M1c behavior:

```rust
#[tokio::test]
async fn sse_live_stream_receives_user_and_assistant_messages() {
    // Configure fake LLM, create a session, open stream, post "hello".
    // Assert SSE output includes history_end, the user message, and assistant "hi".
}

#[tokio::test]
async fn last_event_id_replays_only_newer_messages() {
    // Post two messages, open stream with Last-Event-ID set to the first message id.
    // Assert replay includes the second message and excludes the first.
}

#[tokio::test]
async fn browser_post_to_non_web_owned_session_is_bad_request() {
    // Insert a direct DB row with channel='discord' owned by the same user.
    // POST /api/sessions/{id}/messages returns 400 invalid_args.
}
```

Write the bodies using the helpers already present in `m1c_*` tests. Do not add public routes that create non-web sessions.

- [ ] **Step 2: Run integration tests**

Run:

```bash
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_sse
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_messages
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test -p plexus-server --test m1c_worker
```

Expected: PASS after small fixes surfaced by the tests.

- [ ] **Step 3: Commit hardening tests and fixes**

```bash
git add plexus-server/tests/m1c_sse.rs plexus-server/tests/m1c_messages.rs plexus-server/tests/m1c_worker.rs plexus-server/src/routes/sessions.rs plexus-server/src/chat/worker.rs plexus-server/src/db/messages.rs
git commit -m "test: cover M1c chat integration"
```

---

## Task 9: Documentation, Status, and Verification

**Files:**
- Modify: `docs/SYSTEM_PROMPT.md`
- Modify: `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`
- Modify: `docs/reference/superpowers/specs/2026-05-14-plexus-m1c-browser-chat-path-design.md`
- Verify only: `docs/API.yaml`, `docs/SCHEMA.md`, `docs/DECISIONS.md`

- [ ] **Step 1: Update SYSTEM_PROMPT runtime persistence wording**

In `docs/SYSTEM_PROMPT.md`, replace the current statement that older user messages do not carry runtime blocks with M1c-aligned wording:

```markdown
**M1c note:** ADR-094 now wins for browser chat. The `<runtime>` block is prepended at ingress and persisted in the user message row. Older user messages therefore replay with the runtime block they had when inserted. Future prompt-builder work may hide these blocks from frontend display, but provider history remains faithful to persisted DB rows.
```

- [ ] **Step 2: Update milestone status before live smoke**

In `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`, keep `M1c` out of `Verified` until manual smoke. After automated tests pass, set the status text to:

```text
Automated checks passed; awaiting live smoke
```

In `docs/reference/superpowers/specs/2026-05-14-plexus-m1c-browser-chat-path-design.md`, add an implementation evidence section with the exact commands run.

- [ ] **Step 3: Run final automated checks**

Run:

```bash
rtk cargo fmt --all -- --check
rtk cargo clippy --workspace --all-targets -- -D warnings
rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test --workspace --all-targets
rtk conda run -n Plexus python -c "import yaml, pathlib; yaml.safe_load(pathlib.Path('docs/API.yaml').read_text()); print('API.yaml ok')"
git diff --check
```

Expected: all pass.

- [ ] **Step 4: Commit docs and verification status**

```bash
git add docs/SYSTEM_PROMPT.md docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md docs/reference/superpowers/specs/2026-05-14-plexus-m1c-browser-chat-path-design.md
git commit -m "docs: record M1c automated verification"
```

- [ ] **Step 5: Manual live smoke gate**

Do not mark M1c `Verified` until the user completes this path:

```bash
cargo run -p plexus-server
```

Then manually:

1. register or log in as admin;
2. `PATCH /api/admin/config` with real provider endpoint, key, and model;
3. create a web session with `POST /api/sessions`;
4. open `GET /api/sessions/{id}/stream`;
5. post text with `POST /api/sessions/{id}/messages`;
6. observe `history_end`, user `message`, and assistant `message`;
7. post inline base64 image content and confirm either VLM response or safe image-strip fallback behavior;
8. reconnect with `Last-Event-ID` and confirm missed persisted messages replay.

After the user confirms live smoke, update M1c and living design statuses to `Verified`, commit, push, and sync NotebookLM.

---

## Self-Review Checklist

- Spec coverage:
  - UUID session routes and `web:{id}` creation: Tasks 1-2.
  - Editable title rules: Task 2.
  - Content blocks, empty messages, inline images: Task 3.
  - Data-URL-only image support and no `.attachments/`: Task 3.
  - SSE replay/live and `history_end`: Task 4.
  - OpenAI content arrays and image-strip fallback: Task 5.
  - Optional `SOUL.md` and `MEMORY.md`: Task 6.
  - Serialized response worker and M1b semaphore reuse: Task 7.
  - Synthetic assistant failures: Task 7.
  - Ownership and non-web write rejection: Task 8.
  - Docs and live smoke gate: Task 9.
- Placeholder scan:
  - The plan avoids deferred implementation placeholders inside M1c scope.
  - Future M1 work is named only in non-goals or docs/status steps.
- Type consistency:
  - `ContentBlock` is shared by DB message persistence, OpenAI request building, and tests.
  - `sessions.id` is the route UUID; `session_key` is never user-editable.
  - Message API returns `message_id`; SSE `id:` uses the same DB message UUID.

---

## Execution Handoff

Plan complete and saved to `docs/reference/superpowers/plans/2026-05-14-plexus-m1c-browser-chat-path.md`. Two execution options:

**1. Subagent-Driven (recommended)** - dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** - execute tasks in this session using executing-plans, batch execution with checkpoints.

Decide which approach before implementation starts.
