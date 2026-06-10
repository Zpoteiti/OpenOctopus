# Plexus M1a Server Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first real `plexus-server` slice: Axum server crate, PostgreSQL bootstrap, real auth, and admin config persistence.

**Architecture:** `plexus-server` is an application crate beside `plexus-common`. It owns HTTP routing, DB access, auth, process config, and REST serialization while reusing centralized wire error codes from `plexus-common`. M1a stays narrow: every accepted REST write persists to PostgreSQL, and LLM provider identity keys are rejected until M1b validation exists.

**Tech Stack:** Rust 2024, Axum, Tokio, SQLx/Postgres, Argon2id, JSON Web Tokens, real PostgreSQL integration tests.

**Post-implementation alignment notes:** The verified implementation keeps
logout unauthenticated/idempotent, maps duplicate profile-email updates to
`409 Conflict`, uses the current database `users.is_admin` row as the
authoritative admin source, and treats `llm_max_concurrent_requests=0` as
unlimited. The final route tests extend the initial failing snippets below with
wrong-password login, cookie JWT auth, profile patch name/email/password cases,
admin config read success, and invalid-value rejection.

---

## File Structure

Create:

- `plexus-server/Cargo.toml` — server crate dependencies.
- `plexus-server/src/lib.rs` — module exports for tests.
- `plexus-server/src/main.rs` — process entrypoint.
- `plexus-server/src/app.rs` — Axum router and shared state.
- `plexus-server/src/config.rs` — process env config.
- `plexus-server/src/error.rs` — HTTP error adapter over `plexus-common` error codes.
- `plexus-server/src/auth/mod.rs` — auth module wiring.
- `plexus-server/src/auth/jwt.rs` — JWT claims, issue, verify.
- `plexus-server/src/auth/password.rs` — Argon2id hash and verify.
- `plexus-server/src/db/mod.rs` — pool creation and bootstrap entrypoint.
- `plexus-server/src/db/schema.sql` — canonical SQL schema.
- `plexus-server/src/db/users.rs` — user queries.
- `plexus-server/src/db/system_config.rs` — system config queries.
- `plexus-server/src/routes/mod.rs` — route wiring.
- `plexus-server/src/routes/auth.rs` — register/login/logout.
- `plexus-server/src/routes/me.rs` — `GET/PATCH /api/me`.
- `plexus-server/src/routes/admin.rs` — `GET/PATCH /api/admin/config`.
- `plexus-server/tests/support/mod.rs` — real Postgres test DB and router helper.
- `plexus-server/tests/m1a_bootstrap.rs` — DB bootstrap tests.
- `plexus-server/tests/m1a_auth.rs` — auth API tests.
- `plexus-server/tests/m1a_admin_config.rs` — admin config tests.

Modify:

- `Cargo.toml` — add `plexus-server` workspace member and server dependencies.
- `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md` — mark M1a implementation progress when execution starts/completes.

---

## Task 1: Add Server Crate Skeleton

**Files:**
- Modify: `Cargo.toml`
- Create: `plexus-server/Cargo.toml`
- Create: `plexus-server/src/lib.rs`
- Create: `plexus-server/src/main.rs`
- Create: `plexus-server/src/app.rs`
- Create: `plexus-server/src/config.rs`
- Create: `plexus-server/src/error.rs`
- Create: `plexus-server/src/auth/mod.rs`
- Create: `plexus-server/src/auth/jwt.rs`
- Create: `plexus-server/src/auth/password.rs`
- Create: `plexus-server/src/db/mod.rs`
- Create: `plexus-server/src/db/users.rs`
- Create: `plexus-server/src/db/system_config.rs`
- Create: `plexus-server/src/routes/mod.rs`
- Create: `plexus-server/src/routes/auth.rs`
- Create: `plexus-server/src/routes/me.rs`
- Create: `plexus-server/src/routes/admin.rs`

- [ ] **Step 1: Add `plexus-server` to the workspace**

Edit root `Cargo.toml`:

```toml
[workspace]
members = ["plexus-common", "plexus-server"]
resolver = "2"
```

Add workspace dependencies:

```toml
axum = "0.8"
cookie = "0.18"
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio-rustls", "postgres", "uuid", "time", "json"] }
argon2 = "0.5"
jsonwebtoken = "9"
rand_core = { version = "0.6", features = ["getrandom"] }
time = { version = "0.3", features = ["serde"] }
tower = { version = "0.5", features = ["util"] }
http-body-util = "0.1"
url = "2"
tempfile = "3"
```

- [ ] **Step 2: Create `plexus-server/Cargo.toml`**

```toml
[package]
name = "plexus-server"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
authors.workspace = true
repository.workspace = true

[dependencies]
plexus-common = { path = "../plexus-common" }
axum.workspace = true
cookie.workspace = true
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
uuid.workspace = true
secrecy.workspace = true
sqlx.workspace = true
argon2.workspace = true
jsonwebtoken.workspace = true
rand_core.workspace = true
time.workspace = true

[dev-dependencies]
tower.workspace = true
http-body-util.workspace = true
url.workspace = true
tempfile.workspace = true
```

- [ ] **Step 3: Add module skeleton**

`plexus-server/src/lib.rs`:

```rust
pub mod app;
pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod routes;
```

`plexus-server/src/main.rs`:

```rust
use plexus_server::{app, config, db};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::ServerConfig::from_env()?;
    tokio::fs::create_dir_all(&cfg.workspace_root).await?;
    let pool = db::connect(&cfg.database_url).await?;
    db::bootstrap(&pool).await?;

    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    axum::serve(listener, app::router(app::AppState::new(pool, cfg))).await?;
    Ok(())
}
```

`plexus-server/src/app.rs`:

```rust
use crate::{config::ServerConfig, routes};
use axum::Router;
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

pub struct AppStateInner {
    pub pool: PgPool,
    pub config: ServerConfig,
}

impl AppState {
    pub fn new(pool: PgPool, config: ServerConfig) -> Self {
        Self {
            inner: Arc::new(AppStateInner { pool, config }),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    pub fn config(&self) -> &ServerConfig {
        &self.inner.config
    }
}

pub fn router(state: AppState) -> Router {
    routes::router().with_state(state)
}
```

- [ ] **Step 4: Add placeholder module files that compile**

Each module file initially contains only enough to compile:

```rust
// plexus-server/src/auth/mod.rs
pub mod jwt;
pub mod password;
```

```rust
// plexus-server/src/auth/jwt.rs
```

```rust
// plexus-server/src/auth/password.rs
```

```rust
// plexus-server/src/db/users.rs
```

```rust
// plexus-server/src/db/system_config.rs
```

```rust
// plexus-server/src/routes/auth.rs
```

```rust
// plexus-server/src/routes/me.rs
```

```rust
// plexus-server/src/routes/admin.rs
```

`plexus-server/src/routes/mod.rs`:

```rust
use crate::app::AppState;
use axum::Router;

pub mod admin;
pub mod auth;
pub mod me;

pub fn router() -> Router<AppState> {
    Router::new()
}
```

- [ ] **Step 5: Add minimal config and DB functions**

`plexus-server/src/config.rs`:

```rust
use std::{env, net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub database_url: String,
    pub workspace_root: PathBuf,
    pub bind: SocketAddr,
    pub jwt_secret: String,
    pub admin_token: Option<String>,
    pub cookie_secure: bool,
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, env::VarError> {
        let database_url = env::var("DATABASE_URL")?;
        let workspace_root = PathBuf::from(env::var("PLEXUS_WORKSPACE_ROOT")?);
        let bind = env::var("PLEXUS_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()
            .expect("PLEXUS_BIND must be host:port");
        let jwt_secret = env::var("JWT_SECRET")?;
        let admin_token = env::var("ADMIN_TOKEN").ok();
        let cookie_secure = env::var("PLEXUS_COOKIE_SECURE")
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);

        Ok(Self {
            database_url,
            workspace_root,
            bind,
            jwt_secret,
            admin_token,
            cookie_secure,
        })
    }
}
```

`plexus-server/src/db/mod.rs`:

```rust
use sqlx::{postgres::PgPoolOptions, PgPool};

pub mod system_config;
pub mod users;

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
}

pub async fn bootstrap(pool: &PgPool) -> Result<(), sqlx::Error> {
    let _ = pool;
    Ok(())
}
```

`plexus-server/src/error.rs`:

```rust
use axum::{http::StatusCode, response::IntoResponse, Json};
use plexus_common::ErrorCode;
use serde::Serialize;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Serialize)]
struct ErrorBody {
    code: ErrorCode,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(ErrorBody {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}
```

- [ ] **Step 6: Run compile check**

Run:

```bash
cargo check -p plexus-server
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml plexus-server
git commit -m "feat(server): add M1a server crate skeleton"
```

---

## Task 2: Add Real Postgres Test Harness

**Files:**
- Create: `plexus-server/tests/support/mod.rs`
- Modify: `plexus-server/Cargo.toml`

- [ ] **Step 1: Create test support module**

`plexus-server/tests/support/mod.rs`:

```rust
use plexus_server::{app, config::ServerConfig, db};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::{env, path::PathBuf};
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

pub struct TestApp {
    pub router: axum::Router,
    pub pool: PgPool,
    pub db_name: String,
    pub admin_url: String,
    pub workspace_root: TempDir,
}

impl TestApp {
    pub async fn spawn() -> Self {
        let admin_url = env::var("PLEXUS_TEST_DATABASE_URL")
            .or_else(|_| env::var("DATABASE_URL"))
            .expect("PLEXUS_TEST_DATABASE_URL or DATABASE_URL must point to a Postgres database");

        let db_name = format!("plexus_test_{}", Uuid::now_v7().simple());
        create_database(&admin_url, &db_name).await;
        let database_url = database_url_for_db(&admin_url, &db_name);
        let pool = db::connect(&database_url).await.expect("connect test db");
        let workspace_root = tempfile::tempdir().expect("temp workspace root");

        let cfg = ServerConfig {
            database_url,
            workspace_root: workspace_root.path().to_path_buf(),
            bind: "127.0.0.1:0".parse().unwrap(),
            jwt_secret: "test-jwt-secret-with-enough-entropy".to_string(),
            admin_token: Some("test-admin-token".to_string()),
            cookie_secure: false,
        };

        tokio::fs::create_dir_all(&cfg.workspace_root)
            .await
            .expect("create workspace root");
        db::bootstrap(&pool).await.expect("bootstrap test db");
        let router = app::router(app::AppState::new(pool.clone(), cfg));

        Self {
            router,
            pool,
            db_name,
            admin_url,
            workspace_root,
        }
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        let admin_url = self.admin_url.clone();
        let db_name = self.db_name.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("drop runtime");
            rt.block_on(async move {
                drop_database(&admin_url, &db_name).await;
            });
        })
        .join()
        .expect("drop database thread");
    }
}

async fn create_database(admin_url: &str, db_name: &str) {
    let mut conn = PgConnection::connect(admin_url)
        .await
        .expect("connect admin database");
    let sql = format!(r#"CREATE DATABASE "{}""#, db_name);
    conn.execute(sql.as_str()).await.expect("create test database");
}

async fn drop_database(admin_url: &str, db_name: &str) {
    let mut conn = PgConnection::connect(admin_url)
        .await
        .expect("connect admin database for cleanup");
    let terminate = format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
        db_name
    );
    let _ = conn.execute(terminate.as_str()).await;
    let sql = format!(r#"DROP DATABASE IF EXISTS "{}""#, db_name);
    let _ = conn.execute(sql.as_str()).await;
}

fn database_url_for_db(admin_url: &str, db_name: &str) -> String {
    let mut url = Url::parse(admin_url).expect("valid postgres URL");
    url.set_path(&format!("/{}", db_name));
    url.to_string()
}

#[allow(dead_code)]
pub fn workspace_path(root: &TempDir, user_id: Uuid) -> PathBuf {
    root.path().join(user_id.to_string())
}
```

- [ ] **Step 2: Run tests to verify harness compiles after later router code**

Do not run yet if Task 1 is the only implemented task; the harness references future routes. Run after Task 3:

```bash
cargo test -p plexus-server m1a_bootstrap -- --nocapture
```

- [ ] **Step 3: Commit after Task 3 passes**

Commit this harness together with DB bootstrap in Task 3.

---

## Task 3: Implement Canonical Schema Bootstrap

**Files:**
- Create: `plexus-server/src/db/schema.sql`
- Modify: `plexus-server/src/db/mod.rs`
- Create: `plexus-server/tests/m1a_bootstrap.rs`
- Uses: `plexus-server/tests/support/mod.rs`

- [ ] **Step 1: Write failing bootstrap tests**

`plexus-server/tests/m1a_bootstrap.rs`:

```rust
mod support;

use support::TestApp;

#[tokio::test]
async fn bootstrap_creates_all_m1a_tables() {
    let app = TestApp::spawn().await;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
    )
    .fetch_all(&app.pool)
    .await
    .unwrap();
    let names: Vec<String> = rows.into_iter().map(|row| row.0).collect();

    for expected in [
        "cron_jobs",
        "devices",
        "discord_configs",
        "messages",
        "sessions",
        "system_config",
        "telegram_configs",
        "users",
        "workspace_members",
        "workspaces",
    ] {
        assert!(names.contains(&expected.to_string()), "missing table {expected}");
    }
}

#[tokio::test]
async fn bootstrap_is_idempotent() {
    let app = TestApp::spawn().await;
    plexus_server::db::bootstrap(&app.pool).await.unwrap();
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p plexus-server m1a_bootstrap -- --nocapture
```

Expected: FAIL because `db::bootstrap` does not create tables yet.

- [ ] **Step 3: Add canonical schema SQL**

`plexus-server/src/db/schema.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS system_config (
    key         TEXT        PRIMARY KEY,
    value       JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    email          TEXT         NOT NULL UNIQUE,
    password_hash  TEXT         NOT NULL,
    name           TEXT         NOT NULL,
    is_admin       BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS discord_configs (
    user_id          UUID         PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    bot_token        TEXT         NOT NULL,
    partner_chat_id  TEXT         NOT NULL,
    allow_list       JSONB        NOT NULL DEFAULT '[]'::jsonb,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS telegram_configs (
    user_id          UUID         PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    bot_token        TEXT         NOT NULL,
    partner_chat_id  TEXT         NOT NULL,
    allow_list       JSONB        NOT NULL DEFAULT '[]'::jsonb,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

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

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_user_session_key ON sessions(user_id, session_key);

CREATE TABLE IF NOT EXISTS messages (
    id                       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id               UUID         NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    role                     TEXT         NOT NULL CHECK (role IN ('user', 'assistant', 'tool')),
    content                  JSONB        NOT NULL,
    is_compaction_summary    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_session_created
    ON messages(session_id, created_at);

CREATE TABLE IF NOT EXISTS devices (
    token              TEXT         PRIMARY KEY,
    user_id            UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name               TEXT         NOT NULL CHECK (lower(name) <> 'server'),
    workspace_path     TEXT         NOT NULL,
    fs_policy          TEXT         NOT NULL CHECK (fs_policy IN ('sandbox', 'unrestricted'))
                                    DEFAULT 'sandbox',
    shell_timeout_max  INTEGER      NOT NULL DEFAULT 300,
    ssrf_whitelist     JSONB        NOT NULL DEFAULT '[]'::jsonb,
    mcp_servers        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_devices_user_id ON devices(user_id);

CREATE TABLE IF NOT EXISTS workspaces (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT         NOT NULL,
    quota_bytes  BIGINT       NOT NULL,
    created_by   UUID         REFERENCES users(id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workspace_members (
    workspace_id  UUID         NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    user_id       UUID         NOT NULL REFERENCES users(id)      ON DELETE CASCADE,
    joined_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workspace_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_members_user ON workspace_members(user_id);

CREATE TABLE IF NOT EXISTS cron_jobs (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT         NOT NULL,
    schedule        TEXT         NOT NULL,
    tz              TEXT,
    one_shot        BOOLEAN      NOT NULL DEFAULT FALSE,
    description     TEXT         NOT NULL,
    channel         TEXT         NOT NULL,
    chat_id         TEXT         NOT NULL,
    deliver         BOOLEAN      NOT NULL DEFAULT TRUE,
    last_fired_at   TIMESTAMPTZ,
    next_fire_at    TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cron_jobs_user_id    ON cron_jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_next_fire  ON cron_jobs(next_fire_at)
    WHERE next_fire_at IS NOT NULL;
```

- [ ] **Step 4: Implement bootstrap**

`plexus-server/src/db/mod.rs`:

```rust
use sqlx::{postgres::PgPoolOptions, PgPool};

pub mod system_config;
pub mod users;

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
}

pub async fn bootstrap(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(include_str!("schema.sql")).execute(pool).await?;
    system_config::seed_defaults(pool).await?;
    Ok(())
}
```

`plexus-server/src/db/system_config.rs`:

```rust
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::BTreeMap;

pub const SUPPORTED_M1A_KEYS: &[&str] = &[
    "quota_bytes",
    "shared_workspace_quota_bytes",
    "llm_max_context_tokens",
    "llm_compaction_threshold_tokens",
    "llm_max_concurrent_requests",
];

pub const DEFERRED_LLM_IDENTITY_KEYS: &[&str] =
    &["llm_endpoint", "llm_api_key", "llm_model"];

pub async fn seed_defaults(pool: &PgPool) -> Result<(), sqlx::Error> {
    let defaults = [
        ("quota_bytes", json!(5_i64 * 1024 * 1024 * 1024)),
        ("shared_workspace_quota_bytes", json!(25_i64 * 1024 * 1024 * 1024)),
        ("llm_max_context_tokens", json!(128000)),
        ("llm_compaction_threshold_tokens", json!(16000)),
        ("llm_max_concurrent_requests", json!(0)),
    ];

    for (key, value) in defaults {
        sqlx::query(
            "INSERT INTO system_config (key, value) VALUES ($1, $2)
             ON CONFLICT (key) DO NOTHING",
        )
        .bind(key)
        .bind(value)
        .execute(pool)
        .await?;
    }

    Ok(())
}

pub async fn get_all(pool: &PgPool) -> Result<BTreeMap<String, Value>, sqlx::Error> {
    let rows: Vec<(String, Value)> =
        sqlx::query_as("SELECT key, value FROM system_config ORDER BY key")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().collect())
}

pub async fn set_many(
    tx: &mut Transaction<'_, Postgres>,
    values: &BTreeMap<String, Value>,
) -> Result<(), sqlx::Error> {
    for (key, value) in values {
        sqlx::query(
            "INSERT INTO system_config (key, value, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
        )
        .bind(key)
        .bind(value)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}
```

- [ ] **Step 5: Run bootstrap tests**

Run:

```bash
cargo test -p plexus-server m1a_bootstrap -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml plexus-server
git commit -m "feat(server): bootstrap canonical postgres schema"
```

---

## Task 4: Add Password and JWT Core

**Files:**
- Modify: `plexus-server/src/auth/password.rs`
- Modify: `plexus-server/src/auth/jwt.rs`

- [ ] **Step 1: Add password hashing tests**

Append to `plexus-server/src/auth/password.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_not_cleartext_and_verifies() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert_ne!(hash, "correct horse battery staple");
        assert!(verify_password("correct horse battery staple", &hash).unwrap());
        assert!(!verify_password("wrong", &hash).unwrap());
    }
}
```

- [ ] **Step 2: Implement password hashing**

`plexus-server/src/auth/password.rs`:

```rust
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use rand_core::OsRng;

pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)?
        .to_string())
}

pub fn verify_password(
    password: &str,
    password_hash: &str,
) -> Result<bool, argon2::password_hash::Error> {
    let parsed = PasswordHash::new(password_hash)?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_not_cleartext_and_verifies() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert_ne!(hash, "correct horse battery staple");
        assert!(verify_password("correct horse battery staple", &hash).unwrap());
        assert!(!verify_password("wrong", &hash).unwrap());
    }
}
```

- [ ] **Step 3: Add JWT tests**

Append to `plexus-server/src/auth/jwt.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn issued_token_verifies() {
        let user_id = Uuid::now_v7();
        let token = issue_token("secret", user_id, true).unwrap();
        let claims = verify_token("secret", &token).unwrap();
        assert_eq!(claims.sub, user_id);
        assert!(claims.is_admin);
    }
}
```

- [ ] **Step 4: Implement JWT issue/verify**

`plexus-server/src/auth/jwt.rs`:

```rust
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

const JWT_TTL_SECONDS: i64 = 60 * 60 * 24 * 7;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: Uuid,
    pub is_admin: bool,
    pub iat: i64,
    pub exp: i64,
}

pub fn issue_token(
    secret: &str,
    user_id: Uuid,
    is_admin: bool,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let claims = Claims {
        sub: user_id,
        is_admin,
        iat: now,
        exp: now + JWT_TTL_SECONDS,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

pub fn verify_token(secret: &str, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )?;
    Ok(data.claims)
}

pub fn session_cookie(token: &str, secure: bool) -> String {
    let mut cookie = format!(
        "plexus_session={}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
        token, JWT_TTL_SECONDS
    );
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

pub fn clear_session_cookie(secure: bool) -> String {
    let mut cookie = "plexus_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0".to_string();
    if secure {
        cookie.push_str("; Secure");
    }
    cookie
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issued_token_verifies() {
        let user_id = Uuid::now_v7();
        let token = issue_token("secret", user_id, true).unwrap();
        let claims = verify_token("secret", &token).unwrap();
        assert_eq!(claims.sub, user_id);
        assert!(claims.is_admin);
    }
}
```

- [ ] **Step 5: Run auth unit tests**

Run:

```bash
cargo test -p plexus-server auth:: -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-server/src/auth
git commit -m "feat(server): add password hashing and jwt auth core"
```

---

## Task 5: Implement User Queries

**Files:**
- Modify: `plexus-server/src/db/users.rs`
- Create: `plexus-server/tests/m1a_auth.rs`

- [ ] **Step 1: Add failing repository-level test through auth API file**

Create `plexus-server/tests/m1a_auth.rs` with a DB-level test first:

```rust
mod support;

use plexus_server::db::users;
use support::TestApp;

#[tokio::test]
async fn create_user_persists_without_returning_password_hash() {
    let app = TestApp::spawn().await;
    let user = users::create_user(
        &app.pool,
        "alice@example.com",
        "hash-value",
        "Alice",
        false,
    )
    .await
    .unwrap();

    assert_eq!(user.email, "alice@example.com");
    assert_eq!(user.name, "Alice");
    assert!(!user.is_admin);

    let stored_hash: (String,) = sqlx::query_as("SELECT password_hash FROM users WHERE id = $1")
        .bind(user.id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(stored_hash.0, "hash-value");
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p plexus-server create_user_persists_without_returning_password_hash -- --nocapture
```

Expected: FAIL because `db::users::create_user` does not exist.

- [ ] **Step 3: Implement user queries**

`plexus-server/src/db/users.rs`:

```rust
use serde::Serialize;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub name: String,
    pub is_admin: bool,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UserWithPassword {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub name: String,
    pub is_admin: bool,
    pub created_at: OffsetDateTime,
}

pub async fn create_user(
    pool: &PgPool,
    email: &str,
    password_hash: &str,
    name: &str,
    is_admin: bool,
) -> Result<User, sqlx::Error> {
    sqlx::query_as::<_, User>(
        r#"
        INSERT INTO users (email, password_hash, name, is_admin)
        VALUES ($1, $2, $3, $4)
        RETURNING id, email, name, is_admin, created_at
        "#
    )
    .bind(email)
    .bind(password_hash)
    .bind(name)
    .bind(is_admin)
    .fetch_one(pool)
    .await
}

pub async fn find_by_email(
    pool: &PgPool,
    email: &str,
) -> Result<Option<UserWithPassword>, sqlx::Error> {
    sqlx::query_as::<_, UserWithPassword>(
        r#"
        SELECT id, email, password_hash, name, is_admin, created_at
        FROM users
        WHERE email = $1
        "#
    )
    .bind(email)
    .fetch_optional(pool)
    .await
}

pub async fn find_by_id(pool: &PgPool, id: Uuid) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, User>(
        r#"
        SELECT id, email, name, is_admin, created_at
        FROM users
        WHERE id = $1
        "#
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn update_profile(
    pool: &PgPool,
    id: Uuid,
    email: Option<&str>,
    name: Option<&str>,
    password_hash: Option<&str>,
) -> Result<User, sqlx::Error> {
    sqlx::query_as::<_, User>(
        r#"
        UPDATE users
        SET
            email = COALESCE($2, email),
            name = COALESCE($3, name),
            password_hash = COALESCE($4, password_hash)
        WHERE id = $1
        RETURNING id, email, name, is_admin, created_at
        "#
    )
    .bind(id)
    .bind(email)
    .bind(name)
    .bind(password_hash)
    .fetch_one(pool)
    .await
}

pub async fn delete_by_id(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}
```

- [ ] **Step 4: Run test**

Run:

```bash
cargo test -p plexus-server create_user_persists_without_returning_password_hash -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plexus-server/src/db/users.rs plexus-server/tests/m1a_auth.rs
git commit -m "feat(server): add user persistence queries"
```

---

## Task 6: Implement Auth Routes and Extractors

**Files:**
- Modify: `plexus-server/src/auth/mod.rs`
- Modify: `plexus-server/src/routes/mod.rs`
- Modify: `plexus-server/src/routes/auth.rs`
- Modify: `plexus-server/src/routes/me.rs`
- Modify: `plexus-server/src/error.rs`
- Modify: `plexus-server/tests/m1a_auth.rs`

- [ ] **Step 1: Add API test helpers**

Append to `plexus-server/tests/m1a_auth.rs`:

```rust
use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

async fn json_request(
    app: &mut axum::Router,
    method: Method,
    path: &str,
    body: Value,
    auth: Option<&str>,
) -> (StatusCode, axum::http::HeaderMap, Value) {
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
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, headers, json)
}
```

- [ ] **Step 2: Add failing auth route tests**

Append to `plexus-server/tests/m1a_auth.rs`:

```rust
#[tokio::test]
async fn register_login_me_and_logout_work_with_real_auth() {
    let app = TestApp::spawn().await;
    let mut router = app.router.clone();

    let (status, headers, body) = json_request(
        &mut router,
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
    assert!(headers
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .contains("HttpOnly"));
    let jwt = body["jwt"].as_str().unwrap().to_string();
    let user_id = uuid::Uuid::parse_str(body["user"]["id"].as_str().unwrap()).unwrap();
    assert_eq!(body["user"]["email"], "alice@example.com");
    assert!(support::workspace_path(&app.workspace_root, user_id).exists());

    let stored_hash: (String,) = sqlx::query_as("SELECT password_hash FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_ne!(stored_hash.0, "correct horse battery staple");

    let (status, _, body) = json_request(
        &mut router,
        Method::POST,
        "/api/auth/login",
        json!({
            "email": "alice@example.com",
            "password": "correct horse battery staple"
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["jwt"].as_str().unwrap().len() > 20);

    let (status, _, body) =
        json_request(&mut router, Method::GET, "/api/me", Value::Null, Some(&jwt)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["email"], "alice@example.com");

    let response = router
        .oneshot(Request::builder().method(Method::POST).uri("/api/auth/logout").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(response
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .contains("Max-Age=0"));
}

#[tokio::test]
async fn admin_token_creates_admin_user() {
    let app = TestApp::spawn().await;
    let mut router = app.router.clone();
    let (status, _, body) = json_request(
        &mut router,
        Method::POST,
        "/api/auth/register",
        json!({
            "email": "admin@example.com",
            "password": "correct horse battery staple",
            "name": "Admin",
            "admin_token": "test-admin-token"
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["user"]["is_admin"], true);
}

#[tokio::test]
async fn duplicate_email_returns_conflict() {
    let app = TestApp::spawn().await;
    let mut router = app.router.clone();
    for expected in [StatusCode::CREATED, StatusCode::CONFLICT] {
        let (status, _, _) = json_request(
            &mut router,
            Method::POST,
            "/api/auth/register",
            json!({
                "email": "dupe@example.com",
                "password": "correct horse battery staple",
                "name": "Dupe"
            }),
            None,
        )
        .await;
        assert_eq!(status, expected);
    }
}
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```bash
cargo test -p plexus-server m1a_auth -- --nocapture
```

Expected: FAIL because routes are not mounted.

- [ ] **Step 4: Implement auth extractor**

`plexus-server/src/auth/mod.rs`:

```rust
pub mod jwt;
pub mod password;

use crate::{app::AppState, db::users, error::ApiError};
use axum::{
    extract::FromRequestParts,
    http::{header, request::Parts, StatusCode},
};
use cookie::Cookie;
use plexus_common::ErrorCode;

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user: users::User,
}

#[derive(Debug, Clone)]
pub struct AdminUser {
    pub user: users::User,
}

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let token = bearer_token(parts).or_else(|| cookie_token(parts)).ok_or_else(|| {
            ApiError::new(StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized, "authentication required")
        })?;
        let claims = jwt::verify_token(&state.config().jwt_secret, &token).map_err(|_| {
            ApiError::new(StatusCode::UNAUTHORIZED, ErrorCode::TokenInvalid, "token is invalid or expired")
        })?;
        let user = users::find_by_id(state.pool(), claims.sub)
            .await
            .map_err(ApiError::from_sqlx)?
            .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, ErrorCode::TokenInvalid, "token user no longer exists"))?;
        Ok(Self { user })
    }
}

impl FromRequestParts<AppState> for AdminUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let auth = AuthUser::from_request_parts(parts, state).await?;
        if !auth.user.is_admin {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                ErrorCode::Forbidden,
                "authenticated but lacks permission",
            ));
        }
        Ok(Self { user: auth.user })
    }
}

fn bearer_token(parts: &Parts) -> Option<String> {
    let value = parts.headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    value.strip_prefix("Bearer ").map(ToOwned::to_owned)
}

fn cookie_token(parts: &Parts) -> Option<String> {
    let header = parts.headers.get(header::COOKIE)?.to_str().ok()?;
    for raw in header.split(';') {
        let cookie = Cookie::parse(raw.trim().to_string()).ok()?;
        if cookie.name() == "plexus_session" {
            return Some(cookie.value().to_string());
        }
    }
    None
}
```

- [ ] **Step 5: Extend `ApiError`**

Add to `plexus-server/src/error.rs`:

```rust
impl ApiError {
    pub fn from_sqlx(err: sqlx::Error) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::IoError,
            format!("database error: {}", err),
        )
    }

    pub fn invalid_args(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidArgs, message)
    }
}
```

- [ ] **Step 6: Implement auth and me routes**

`plexus-server/src/routes/mod.rs`:

```rust
use crate::app::AppState;
use axum::{routing::{get, patch, post}, Router};

pub mod admin;
pub mod auth;
pub mod me;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/logout", post(auth::logout))
        .route("/api/me", get(me::get_me).patch(me::patch_me))
}
```

`plexus-server/src/routes/auth.rs`:

```rust
use crate::{
    app::AppState,
    auth::{jwt, password},
    db::users,
    error::ApiError,
};
use axum::{extract::State, http::{header, HeaderMap, StatusCode}, Json};
use plexus_common::ErrorCode;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct RegisterRequest {
    email: String,
    password: String,
    name: String,
    admin_token: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    jwt: String,
    user: users::User,
}

pub async fn register(
    State(state): State<AppState>,
    Json(req): Json<RegisterRequest>,
) -> Result<(StatusCode, HeaderMap, Json<AuthResponse>), ApiError> {
    validate_register(&req)?;
    let hash = password::hash_password(&req.password)
        .map_err(|_| ApiError::invalid_args("password could not be hashed"))?;
    let is_admin = state
        .config()
        .admin_token
        .as_deref()
        .is_some_and(|token| req.admin_token.as_deref() == Some(token));
    let user = users::create_user(state.pool(), &req.email, &hash, &req.name, is_admin)
        .await
        .map_err(map_create_user_error)?;
    let dir = state.config().workspace_root.join(user.id.to_string());
    if let Err(err) = tokio::fs::create_dir_all(&dir).await {
        let _ = users::delete_by_id(state.pool(), user.id).await;
        return Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::IoError,
            format!("workspace creation failed: {err}"),
        ));
    }
    let token = jwt::issue_token(&state.config().jwt_secret, user.id, user.is_admin)
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::IoError, "token issue failed"))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        jwt::session_cookie(&token, state.config().cookie_secure).parse().unwrap(),
    );
    Ok((StatusCode::CREATED, headers, Json(AuthResponse { jwt: token, user })))
}

pub async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<(HeaderMap, Json<AuthResponse>), ApiError> {
    let found = users::find_by_email(state.pool(), &req.email)
        .await
        .map_err(ApiError::from_sqlx)?
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized, "invalid email or password"))?;
    let ok = password::verify_password(&req.password, &found.password_hash)
        .map_err(|_| ApiError::new(StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized, "invalid email or password"))?;
    if !ok {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized, "invalid email or password"));
    }
    let user = users::User {
        id: found.id,
        email: found.email,
        name: found.name,
        is_admin: found.is_admin,
        created_at: found.created_at,
    };
    let token = jwt::issue_token(&state.config().jwt_secret, user.id, user.is_admin)
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::IoError, "token issue failed"))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        jwt::session_cookie(&token, state.config().cookie_secure).parse().unwrap(),
    );
    Ok((headers, Json(AuthResponse { jwt: token, user })))
}

pub async fn logout(State(state): State<AppState>) -> (StatusCode, HeaderMap) {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        jwt::clear_session_cookie(state.config().cookie_secure).parse().unwrap(),
    );
    (StatusCode::NO_CONTENT, headers)
}

fn validate_register(req: &RegisterRequest) -> Result<(), ApiError> {
    if req.email.trim().is_empty() || !req.email.contains('@') {
        return Err(ApiError::invalid_args("email must be valid"));
    }
    if req.password.len() < 8 {
        return Err(ApiError::invalid_args("password must be at least 8 characters"));
    }
    if req.name.trim().is_empty() {
        return Err(ApiError::invalid_args("name is required"));
    }
    Ok(())
}

fn map_create_user_error(err: sqlx::Error) -> ApiError {
    if let sqlx::Error::Database(db_err) = &err {
        if db_err.is_unique_violation() {
            return ApiError::new(StatusCode::CONFLICT, ErrorCode::InvalidArgs, "email already in use");
        }
    }
    ApiError::from_sqlx(err)
}
```

`plexus-server/src/routes/me.rs`:

```rust
use crate::{auth::{password, AuthUser}, db::users, error::ApiError};
use axum::Json;
use serde::Deserialize;

pub async fn get_me(auth: AuthUser) -> Json<users::User> {
    Json(auth.user)
}

#[derive(Deserialize)]
pub struct PatchMeRequest {
    name: Option<String>,
    email: Option<String>,
    password: Option<String>,
}

pub async fn patch_me(
    auth: AuthUser,
    axum::extract::State(state): axum::extract::State<crate::app::AppState>,
    Json(req): Json<PatchMeRequest>,
) -> Result<Json<users::User>, ApiError> {
    let password_hash = match req.password.as_deref() {
        Some(password) if password.len() < 8 => {
            return Err(ApiError::invalid_args("password must be at least 8 characters"));
        }
        Some(password) => Some(password::hash_password(password).map_err(|_| {
            ApiError::invalid_args("password could not be hashed")
        })?),
        None => None,
    };
    let user = users::update_profile(
        state.pool(),
        auth.user.id,
        req.email.as_deref(),
        req.name.as_deref(),
        password_hash.as_deref(),
    )
    .await
    .map_err(map_update_user_error)?;
    Ok(Json(user))
}

fn map_update_user_error(err: sqlx::Error) -> ApiError {
    if let sqlx::Error::Database(db_err) = &err {
        if db_err.is_unique_violation() {
            return ApiError::new(
                axum::http::StatusCode::CONFLICT,
                plexus_common::ErrorCode::InvalidArgs,
                "email already in use",
            );
        }
    }
    ApiError::from_sqlx(err)
}
```

- [ ] **Step 7: Run auth tests**

Run:

```bash
cargo test -p plexus-server m1a_auth -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add plexus-server/src plexus-server/tests/m1a_auth.rs
git commit -m "feat(server): implement real auth routes"
```

---

## Task 7: Implement Admin Config Routes

**Files:**
- Modify: `plexus-server/src/routes/mod.rs`
- Modify: `plexus-server/src/routes/admin.rs`
- Modify: `plexus-server/src/db/system_config.rs`
- Create: `plexus-server/tests/m1a_admin_config.rs`

- [ ] **Step 1: Add admin config tests**

`plexus-server/tests/m1a_admin_config.rs`:

```rust
mod support;

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use support::TestApp;
use tower::ServiceExt;

async fn request(
    app: &mut axum::Router,
    method: Method,
    path: &str,
    body: Value,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
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

async fn register(app: &mut axum::Router, email: &str, admin: bool) -> String {
    let mut body = json!({
        "email": email,
        "password": "correct horse battery staple",
        "name": email
    });
    if admin {
        body["admin_token"] = json!("test-admin-token");
    }
    let (status, json) = request(app, Method::POST, "/api/auth/register", body, None).await;
    assert_eq!(status, StatusCode::CREATED);
    json["jwt"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn admin_config_requires_admin() {
    let app = TestApp::spawn().await;
    let mut router = app.router.clone();
    let user_token = register(&mut router, "user@example.com", false).await;

    let (status, _) = request(&mut router, Method::GET, "/api/admin/config", Value::Null, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = request(
        &mut router,
        Method::GET,
        "/api/admin/config",
        Value::Null,
        Some(&user_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn admin_can_patch_supported_config_keys() {
    let app = TestApp::spawn().await;
    let mut router = app.router.clone();
    let admin_token = register(&mut router, "admin@example.com", true).await;

    let (status, body) = request(
        &mut router,
        Method::PATCH,
        "/api/admin/config",
        json!({
            "quota_bytes": 12345,
            "shared_workspace_quota_bytes": 67890,
            "llm_max_context_tokens": 128000,
            "llm_compaction_threshold_tokens": 16000,
            "llm_max_concurrent_requests": 32
        }),
        Some(&admin_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["quota_bytes"], 12345);

    let stored: (serde_json::Value,) =
        sqlx::query_as("SELECT value FROM system_config WHERE key = 'quota_bytes'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(stored.0, json!(12345));
}

#[tokio::test]
async fn unsupported_or_deferred_keys_reject_atomically() {
    let app = TestApp::spawn().await;
    let mut router = app.router.clone();
    let admin_token = register(&mut router, "admin@example.com", true).await;

    let (status, _) = request(
        &mut router,
        Method::PATCH,
        "/api/admin/config",
        json!({ "quota_bytes": 999, "llm_model": "gpt-test" }),
        Some(&admin_token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let stored: (serde_json::Value,) =
        sqlx::query_as("SELECT value FROM system_config WHERE key = 'quota_bytes'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_ne!(stored.0, json!(999));
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -p plexus-server m1a_admin_config -- --nocapture
```

Expected: FAIL because admin routes are not implemented.

- [ ] **Step 3: Implement config validation**

Append to `plexus-server/src/db/system_config.rs`:

```rust
use crate::error::ApiError;
use axum::http::StatusCode;
use plexus_common::ErrorCode;

pub fn validate_patch(input: BTreeMap<String, Value>) -> Result<BTreeMap<String, Value>, ApiError> {
    let mut out = BTreeMap::new();
    for (key, value) in input {
        if DEFERRED_LLM_IDENTITY_KEYS.contains(&key.as_str()) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidArgs,
                format!("{key} requires M1b provider validation before DB write"),
            ));
        }
        if !SUPPORTED_M1A_KEYS.contains(&key.as_str()) {
            return Err(ApiError::invalid_args(format!("unsupported config key: {key}")));
        }
        validate_value(&key, &value)?;
        out.insert(key, value);
    }
    Ok(out)
}

fn validate_value(key: &str, value: &Value) -> Result<(), ApiError> {
    match key {
        "quota_bytes" | "shared_workspace_quota_bytes" | "llm_max_context_tokens" => {
            positive_i64(key, value).map(|_| ())
        }
        "llm_compaction_threshold_tokens" => {
            let value = positive_i64(key, value)?;
            if value <= 4000 {
                return Err(ApiError::invalid_args(
                    "llm_compaction_threshold_tokens must be greater than 4000",
                ));
            }
            Ok(())
        }
        "llm_max_concurrent_requests" => {
            non_negative_i64(key, value).map(|_| ())
        }
        _ => Err(ApiError::invalid_args(format!("unsupported config key: {key}"))),
    }
}

fn non_negative_i64(key: &str, value: &Value) -> Result<i64, ApiError> {
    let n = value
        .as_i64()
        .ok_or_else(|| ApiError::invalid_args(format!("{key} must be an integer")))?;
    if n < 0 {
        return Err(ApiError::invalid_args(format!("{key} must be zero or positive")));
    }
    Ok(n)
}

fn positive_i64(key: &str, value: &Value) -> Result<i64, ApiError> {
    let n = value
        .as_i64()
        .ok_or_else(|| ApiError::invalid_args(format!("{key} must be an integer")))?;
    if n <= 0 {
        return Err(ApiError::invalid_args(format!("{key} must be positive")));
    }
    Ok(n)
}
```

- [ ] **Step 4: Implement admin routes**

`plexus-server/src/routes/admin.rs`:

```rust
use crate::{app::AppState, auth::AdminUser, db::system_config, error::ApiError};
use axum::{extract::State, Json};
use serde_json::Value;
use std::collections::BTreeMap;

pub async fn get_config(
    _admin: AdminUser,
    State(state): State<AppState>,
) -> Result<Json<BTreeMap<String, Value>>, ApiError> {
    let values = system_config::get_all(state.pool())
        .await
        .map_err(ApiError::from_sqlx)?;
    Ok(Json(values))
}

pub async fn patch_config(
    _admin: AdminUser,
    State(state): State<AppState>,
    Json(input): Json<BTreeMap<String, Value>>,
) -> Result<Json<BTreeMap<String, Value>>, ApiError> {
    let values = system_config::validate_patch(input)?;
    let mut tx = state.pool().begin().await.map_err(ApiError::from_sqlx)?;
    system_config::set_many(&mut tx, &values)
        .await
        .map_err(ApiError::from_sqlx)?;
    tx.commit().await.map_err(ApiError::from_sqlx)?;
    let current = system_config::get_all(state.pool())
        .await
        .map_err(ApiError::from_sqlx)?;
    Ok(Json(current))
}
```

Update `plexus-server/src/routes/mod.rs`:

```rust
use crate::app::AppState;
use axum::{
    routing::{get, patch, post},
    Router,
};

pub mod admin;
pub mod auth;
pub mod me;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/logout", post(auth::logout))
        .route("/api/me", get(me::get_me).patch(me::patch_me))
        .route("/api/admin/config", get(admin::get_config).patch(admin::patch_config))
}
```

- [ ] **Step 5: Run admin config tests**

Run:

```bash
cargo test -p plexus-server m1a_admin_config -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-server/src plexus-server/tests/m1a_admin_config.rs
git commit -m "feat(server): persist admin config keys"
```

---

## Task 8: Add Error Shape and Secret-Leak Coverage

**Files:**
- Modify: `plexus-server/tests/m1a_auth.rs`
- Modify: `plexus-server/tests/m1a_admin_config.rs`
- Modify if needed: `plexus-server/src/error.rs`

- [ ] **Step 1: Add error shape test**

Append to `plexus-server/tests/m1a_auth.rs`:

```rust
#[tokio::test]
async fn auth_error_shape_uses_common_code_and_message() {
    let app = TestApp::spawn().await;
    let mut router = app.router.clone();
    let (status, _, body) =
        json_request(&mut router, Method::GET, "/api/me", Value::Null, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");
    assert!(body["message"].as_str().unwrap().contains("authentication"));
}
```

- [ ] **Step 2: Add secret response test**

Append to `plexus-server/tests/m1a_admin_config.rs`:

```rust
#[tokio::test]
async fn responses_do_not_leak_known_secrets() {
    let app = TestApp::spawn().await;
    let mut router = app.router.clone();
    let admin_token = register(&mut router, "admin@example.com", true).await;

    let (_, body) = request(
        &mut router,
        Method::GET,
        "/api/admin/config",
        Value::Null,
        Some(&admin_token),
    )
    .await;
    let text = body.to_string();
    assert!(!text.contains("test-admin-token"));
    assert!(!text.contains("test-jwt-secret-with-enough-entropy"));
    assert!(!text.contains("password_hash"));
    assert!(!text.contains("llm_api_key"));
}
```

- [ ] **Step 3: Run tests**

Run:

```bash
cargo test -p plexus-server m1a_auth m1a_admin_config -- --nocapture
```

Expected: PASS. If the command filters incorrectly, run the two test binaries separately:

```bash
cargo test -p plexus-server --test m1a_auth -- --nocapture
cargo test -p plexus-server --test m1a_admin_config -- --nocapture
```

- [ ] **Step 4: Commit**

```bash
git add plexus-server/tests plexus-server/src/error.rs
git commit -m "test(server): cover M1a error responses"
```

---

## Task 9: Final M1a Verification and Tracker Update

**Files:**
- Modify: `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`
- Modify if needed: `docs/SCHEMA.md`
- Modify if needed: `docs/API.yaml`
- Modify if needed: `docs/DECISIONS.md`

- [ ] **Step 1: Run complete M1a server tests**

Run:

```bash
cargo test -p plexus-server -- --nocapture
```

Expected: PASS.

- [ ] **Step 2: Run workspace checks**

Run:

```bash
cargo test --workspace
```

Expected: PASS.

- [ ] **Step 3: Run formatting and lint-level checks**

Run:

```bash
cargo fmt --all -- --check
cargo check --workspace
```

Expected: both PASS.

- [ ] **Step 4: Confirm schema docs match SQL**

Run:

```bash
rg -n "CREATE TABLE IF NOT EXISTS|CREATE INDEX IF NOT EXISTS|server_mcp|llm_max_concurrent_requests|CHECK \\(lower\\(name\\) <> 'server'\\)" docs/SCHEMA.md plexus-server/src/db/schema.sql
```

Expected: the same tables and important constraints appear in both docs and SQL. If not, update the stale side before finishing.

- [ ] **Step 5: Update living tracker to `Verified`**

In `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`, update:

```markdown
| Overall M1 state | M1a verified; M1b planning next |
| Current focus | Write the `M1b` LLM provider foundation sub-spec |
| Next implementation slice | `M1b` LLM provider foundation |
```

And update the milestone row:

```markdown
| `M1a` | Verified | Server crate, startup, DB bootstrap, canonical schema application, real auth, basic REST/admin persistence, test harness | M0 | Verified by `cargo test -p plexus-server` and `cargo test --workspace` |
```

- [ ] **Step 6: Commit tracker/docs updates**

```bash
git add docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md docs/SCHEMA.md docs/API.yaml docs/DECISIONS.md
git commit -m "docs: mark M1a verified"
```

Only include docs that actually changed.

---

## Notes for Execution

- Do not add fake auth or a test-only bypass.
- Do not accept `llm_endpoint`, `llm_api_key`, or `llm_model` until M1b adds provider validation.
- `PLEXUS_BIND` is the single Axum listener for REST, SSE, and future device WebSocket. Binding `0.0.0.0` is allowed; operators are responsible for firewall, TLS, and reverse proxy setup.
- `PLEXUS_COOKIE_SECURE=false` is for local HTTP development. Production HTTPS deployments should set it true.
- Use runtime SQLx queries if compile-time `query!` macros create friction before SQLx offline metadata exists.
- Keep commits small and stop after any unexpected test failure to debug from evidence.
