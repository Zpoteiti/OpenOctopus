# Plexus M1e Device Connectivity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the server-side device lifecycle and WebSocket connectivity foundation for M1e.

**Architecture:** Device rows are persisted in the existing `devices` table. REST routes manage device lifecycle through user JWT auth, while `/ws/device` authenticates with a device token from the `Authorization` header only, completes `hello` / `hello_ack`, and stores online state in an in-memory connection registry. The registry keeps a send handle for live `config_update`, duplicate replacement, revoke/delete closes, and later M1f `tool_call` routing.

**Tech Stack:** Rust 2024, Axum 0.8 REST + WebSocket, Tokio, SQLx/Postgres, `plexus-common::protocol::WsFrame`, `tokio-tungstenite` for integration tests.

---

## File Structure

- `Cargo.toml` - enable Axum WebSocket support at the workspace dependency level and add websocket test dependencies.
- `plexus-server/Cargo.toml` - add `futures-util`; add `tokio-tungstenite` as a dev dependency.
- `plexus-server/src/app.rs` - store `DeviceRuntime` in `AppState` and expose `devices()`.
- `plexus-server/src/db/mod.rs` - export `devices`.
- `plexus-server/src/db/devices.rs` - device row model, slug normalization, token generation/hinting, config validation, CRUD helpers.
- `plexus-server/src/devices/mod.rs` - runtime exports and heartbeat constants.
- `plexus-server/src/devices/registry.rs` - in-memory online registry with stale-cleanup protection and close commands.
- `plexus-server/src/devices/ws.rs` - `/ws/device` upgrade, header-only token auth, handshake, write loop, read loop, server-driven heartbeat.
- `plexus-server/src/routes/mod.rs` - mount device REST routes and WebSocket route.
- `plexus-server/src/routes/devices.rs` - request/response structs and lifecycle handlers.
- `plexus-server/src/lib.rs` - export the new `devices` module.
- `plexus-server/tests/support/mod.rs` - add a real TCP test-server helper for WebSocket tests.
- `plexus-server/tests/support/device_client.rs` - in-process fake WebSocket device client.
- `plexus-server/tests/m1e_devices_rest.rs` - REST lifecycle tests.
- `plexus-server/tests/m1e_device_ws.rs` - WebSocket handshake, heartbeat, duplicate, config-update, regenerate/delete tests.
- `docs/SCHEMA.md` - update device API/schema notes after implementation.

Boundaries:

- `db::devices` owns persistence and pure validation/helper logic.
- `routes::devices` owns browser JWT auth, HTTP request validation, response shaping, and calls into the runtime for live updates/closes.
- `devices::registry` owns online state and per-connection commands.
- `devices::ws` owns WebSocket state transitions and close codes.

---

### Task 1: Dependencies and Runtime Skeleton

**Files:**
- Modify: `Cargo.toml`
- Modify: `plexus-server/Cargo.toml`
- Modify: `plexus-server/src/lib.rs`
- Modify: `plexus-server/src/app.rs`
- Create: `plexus-server/src/devices/mod.rs`
- Create: `plexus-server/src/devices/registry.rs`

- [ ] **Step 1: Write the failing compile test target**

Create the new module files with the initial public API used by the following tasks.

`plexus-server/src/devices/mod.rs`:

```rust
pub mod registry;
pub mod ws;

pub use registry::{CloseReason, ConnHandle, DeviceRuntime};

pub const HEARTBEAT_INTERVAL_SECS: u64 = 30;
pub const HEARTBEAT_MISSED_LIMIT: u8 = 2;
```

`plexus-server/src/devices/registry.rs`:

```rust
use plexus_common::protocol::WsFrame;
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    Replaced,
    Unauthorized,
    HeartbeatTimeout,
}

#[derive(Clone)]
pub struct ConnHandle {
    pub token: String,
    pub user_id: Uuid,
    pub device_name: String,
    pub connected_at: OffsetDateTime,
    pub last_seen: OffsetDateTime,
    pub tx: mpsc::Sender<WsFrame>,
}

#[derive(Clone, Default)]
pub struct DeviceRuntime {
    inner: Arc<Mutex<HashMap<String, ConnHandle>>>,
}

impl DeviceRuntime {
    pub fn new() -> Self {
        Self::default()
    }
}
```

Modify `plexus-server/src/lib.rs` to export the module:

```rust
pub mod app;
pub mod auth;
pub mod chat;
pub mod config;
pub mod db;
pub mod devices;
pub mod error;
pub mod openai;
pub mod routes;
pub mod tools;
pub mod workspace;
```

- [ ] **Step 2: Run compile to verify dependency gaps**

Run:

```bash
cargo check -p plexus-server
```

Expected: FAIL until `devices::ws` exists and dependency features are added.

- [ ] **Step 3: Add WebSocket dependencies**

Modify `Cargo.toml` workspace dependencies:

```toml
tokio = { version = "1", features = ["fs", "process", "macros", "rt-multi-thread", "time", "sync", "test-util"] }
axum = { version = "0.8", features = ["ws"] }
futures-util = "0.3"
tokio-tungstenite = { version = "0.29", default-features = false, features = ["connect"] }
```

Modify `plexus-server/Cargo.toml`:

```toml
[dependencies]
futures-util.workspace = true

[dev-dependencies]
tower.workspace = true
http-body-util.workspace = true
url.workspace = true
tempfile.workspace = true
tokio-tungstenite.workspace = true
```

- [ ] **Step 4: Wire runtime into AppState**

Modify `plexus-server/src/app.rs` imports:

```rust
use crate::{
    chat::ChatRuntime, config::ServerConfig, devices::DeviceRuntime, openai::OpenAiRuntime,
    routes, workspace::WorkspaceFs,
};
```

Add the field to `AppStateInner`:

```rust
pub devices: DeviceRuntime,
```

Initialize it in `new_with_openai_runtime`:

```rust
let devices = DeviceRuntime::new();
Self {
    inner: Arc::new(AppStateInner {
        pool,
        config,
        openai,
        chat: ChatRuntime::default(),
        workspace_fs,
        devices,
        admin_config_lock: Mutex::new(()),
    }),
}
```

Add accessor:

```rust
pub fn devices(&self) -> &DeviceRuntime {
    &self.inner.devices
}
```

- [ ] **Step 5: Add temporary ws module**

Create `plexus-server/src/devices/ws.rs`:

```rust
use crate::{app::AppState, error::ApiError};
use axum::response::Response;

pub async fn device_ws(_state: AppState) -> Result<Response, ApiError> {
    Err(ApiError::invalid_args("device websocket not implemented"))
}
```

Task 5 replaces this temporary handler with the real WebSocket state machine.

- [ ] **Step 6: Verify compile passes**

Run:

```bash
cargo check -p plexus-server
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml plexus-server/Cargo.toml plexus-server/src/app.rs plexus-server/src/lib.rs plexus-server/src/devices
git commit -m "feat: add device runtime skeleton"
```

---

### Task 2: Device DB Helpers, Slug Names, and Token Hints

**Files:**
- Create: `plexus-server/src/db/devices.rs`
- Modify: `plexus-server/src/db/mod.rs`
- Modify: `plexus-server/src/db/schema.sql`

- [ ] **Step 1: Write failing helper tests**

Create `plexus-server/src/db/devices.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_name_normalizes_to_slug() {
        assert_eq!(normalize_device_name("MacBook Pro").unwrap(), "macbook-pro");
        assert_eq!(normalize_device_name("John's iPhone 6S").unwrap(), "johns-iphone-6s");
        assert_eq!(normalize_device_name("lab_pc_01").unwrap(), "lab-pc-01");
        assert_eq!(normalize_device_name("lab--machine").unwrap(), "lab-machine");
    }

    #[test]
    fn device_name_rejects_reserved_empty_and_non_ascii() {
        assert!(normalize_device_name("server").is_err());
        assert!(normalize_device_name("Server").is_err());
        assert!(normalize_device_name("  ---  ").is_err());
        assert!(normalize_device_name("办公室电脑").is_err());
        assert!(normalize_device_name("bad/name").is_err());
    }

    #[test]
    fn token_hint_keeps_prefix_and_last_four_only() {
        assert_eq!(token_hint("plexus_dev_abcdefghijklmnopqrstuvwxyz"), "plexus_dev_...wxyz");
    }

    #[test]
    fn generated_token_has_device_prefix_and_entropy() {
        let token = generate_device_token();
        assert!(token.starts_with(plexus_common::consts::DEVICE_TOKEN_PREFIX));
        assert!(token.len() > plexus_common::consts::DEVICE_TOKEN_PREFIX.len() + 32);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p plexus-server db::devices::tests -- --nocapture
```

Expected: FAIL because `db::devices` is not exported and helpers do not exist.

- [ ] **Step 3: Export the DB module**

Modify `plexus-server/src/db/mod.rs`:

```rust
pub mod devices;
pub mod messages;
pub mod pending_messages;
pub mod sessions;
pub mod system_config;
pub mod users;
```

- [ ] **Step 4: Implement helper types and pure functions**

Replace `plexus-server/src/db/devices.rs` with:

```rust
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use plexus_common::consts::DEVICE_TOKEN_PREFIX;
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

pub const DEFAULT_WORKSPACE_PATH: &str = "~/plexus/workspace";
pub const DEFAULT_FS_POLICY: &str = "sandbox";
pub const DEFAULT_SHELL_TIMEOUT_MAX: i32 = 300;
pub const MAX_DEVICE_NAME_CHARS: usize = 64;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DeviceRow {
    pub token: String,
    pub user_id: Uuid,
    pub name: String,
    pub workspace_path: String,
    pub fs_policy: String,
    pub shell_timeout_max: i32,
    pub ssrf_whitelist: Value,
    pub mcp_servers: Value,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct NewDevice {
    pub name: String,
    pub workspace_path: String,
    pub fs_policy: String,
    pub shell_timeout_max: i32,
    pub ssrf_whitelist: Value,
    pub mcp_servers: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DevicePatch {
    pub name: Option<String>,
    pub workspace_path: Option<String>,
    pub fs_policy: Option<String>,
    pub shell_timeout_max: Option<i32>,
    pub ssrf_whitelist: Option<Value>,
    pub mcp_servers: Option<Value>,
}

pub fn generate_device_token() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    format!("{}{}", DEVICE_TOKEN_PREFIX, URL_SAFE_NO_PAD.encode(bytes))
}

pub fn token_hint(token: &str) -> String {
    let suffix = token.get(token.len().saturating_sub(4)..).unwrap_or(token);
    format!("{}...{}", DEVICE_TOKEN_PREFIX, suffix)
}

pub fn normalize_device_name(raw: &str) -> Result<String, String> {
    let mut out = String::new();
    let mut last_was_sep = false;
    for ch in raw.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if ch.is_ascii_whitespace() || ch == '_' || ch == '-' {
            if !out.is_empty() && !last_was_sep {
                out.push('-');
                last_was_sep = true;
            }
        } else if ch == '\'' {
            continue;
        } else {
            return Err("device name may contain only ASCII letters, digits, spaces, underscores, apostrophes, and hyphens".to_string());
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        return Err("device name must not be empty".to_string());
    }
    if out == "server" {
        return Err("device name 'server' is reserved".to_string());
    }
    if out.chars().count() > MAX_DEVICE_NAME_CHARS {
        return Err(format!("device name must be at most {MAX_DEVICE_NAME_CHARS} characters"));
    }
    Ok(out)
}

pub fn validate_fs_policy(value: &str) -> Result<String, String> {
    match value {
        "sandbox" | "unrestricted" => Ok(value.to_string()),
        _ => Err("fs_policy must be 'sandbox' or 'unrestricted'".to_string()),
    }
}

pub fn validate_shell_timeout(value: i32) -> Result<i32, String> {
    if (1..=3600).contains(&value) {
        Ok(value)
    } else {
        Err("shell_timeout_max must be between 1 and 3600".to_string())
    }
}

pub fn default_new_device(raw_name: &str) -> Result<NewDevice, String> {
    Ok(NewDevice {
        name: normalize_device_name(raw_name)?,
        workspace_path: DEFAULT_WORKSPACE_PATH.to_string(),
        fs_policy: DEFAULT_FS_POLICY.to_string(),
        shell_timeout_max: DEFAULT_SHELL_TIMEOUT_MAX,
        ssrf_whitelist: json!([]),
        mcp_servers: json!({}),
    })
}
```

- [ ] **Step 5: Add CRUD helpers**

Append to `db/devices.rs`:

```rust
pub async fn create(pool: &PgPool, user_id: Uuid, new: NewDevice) -> Result<DeviceRow, sqlx::Error> {
    let token = generate_device_token();
    sqlx::query_as::<_, DeviceRow>(
        r#"
        INSERT INTO devices (token, user_id, name, workspace_path, fs_policy,
                             shell_timeout_max, ssrf_whitelist, mcp_servers)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING token, user_id, name, workspace_path, fs_policy,
                  shell_timeout_max, ssrf_whitelist, mcp_servers, created_at
        "#,
    )
    .bind(token)
    .bind(user_id)
    .bind(new.name)
    .bind(new.workspace_path)
    .bind(new.fs_policy)
    .bind(new.shell_timeout_max)
    .bind(new.ssrf_whitelist)
    .bind(new.mcp_servers)
    .fetch_one(pool)
    .await
}

pub async fn list_by_user(pool: &PgPool, user_id: Uuid) -> Result<Vec<DeviceRow>, sqlx::Error> {
    sqlx::query_as::<_, DeviceRow>(
        r#"
        SELECT token, user_id, name, workspace_path, fs_policy,
               shell_timeout_max, ssrf_whitelist, mcp_servers, created_at
        FROM devices
        WHERE user_id = $1
        ORDER BY name ASC
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
}

pub async fn find_by_user_and_name(
    pool: &PgPool,
    user_id: Uuid,
    name: &str,
) -> Result<Option<DeviceRow>, sqlx::Error> {
    sqlx::query_as::<_, DeviceRow>(
        r#"
        SELECT token, user_id, name, workspace_path, fs_policy,
               shell_timeout_max, ssrf_whitelist, mcp_servers, created_at
        FROM devices
        WHERE user_id = $1 AND name = $2
        "#,
    )
    .bind(user_id)
    .bind(name)
    .fetch_optional(pool)
    .await
}

pub async fn find_by_token(pool: &PgPool, token: &str) -> Result<Option<DeviceRow>, sqlx::Error> {
    sqlx::query_as::<_, DeviceRow>(
        r#"
        SELECT token, user_id, name, workspace_path, fs_policy,
               shell_timeout_max, ssrf_whitelist, mcp_servers, created_at
        FROM devices
        WHERE token = $1
        "#,
    )
    .bind(token)
    .fetch_optional(pool)
    .await
}

pub async fn patch(
    pool: &PgPool,
    user_id: Uuid,
    current_name: &str,
    patch: DevicePatch,
) -> Result<Option<DeviceRow>, sqlx::Error> {
    let current = match find_by_user_and_name(pool, user_id, current_name).await? {
        Some(row) => row,
        None => return Ok(None),
    };
    let next_name = patch.name.unwrap_or(current.name);
    let next_workspace_path = patch.workspace_path.unwrap_or(current.workspace_path);
    let next_fs_policy = patch.fs_policy.unwrap_or(current.fs_policy);
    let next_shell_timeout = patch.shell_timeout_max.unwrap_or(current.shell_timeout_max);
    let next_ssrf_whitelist = patch.ssrf_whitelist.unwrap_or(current.ssrf_whitelist);
    let next_mcp_servers = patch.mcp_servers.unwrap_or(current.mcp_servers);

    let row = sqlx::query_as::<_, DeviceRow>(
        r#"
        UPDATE devices
        SET name = $3,
            workspace_path = $4,
            fs_policy = $5,
            shell_timeout_max = $6,
            ssrf_whitelist = $7,
            mcp_servers = $8
        WHERE user_id = $1 AND token = $2
        RETURNING token, user_id, name, workspace_path, fs_policy,
                  shell_timeout_max, ssrf_whitelist, mcp_servers, created_at
        "#,
    )
    .bind(user_id)
    .bind(current.token)
    .bind(next_name)
    .bind(next_workspace_path)
    .bind(next_fs_policy)
    .bind(next_shell_timeout)
    .bind(next_ssrf_whitelist)
    .bind(next_mcp_servers)
    .fetch_one(pool)
    .await?;
    Ok(Some(row))
}

pub async fn regenerate_token(
    pool: &PgPool,
    user_id: Uuid,
    name: &str,
) -> Result<Option<(String, DeviceRow)>, sqlx::Error> {
    let Some(current) = find_by_user_and_name(pool, user_id, name).await? else {
        return Ok(None);
    };
    let old_token = current.token;
    let new_token = generate_device_token();
    let row = sqlx::query_as::<_, DeviceRow>(
        r#"
        UPDATE devices
        SET token = $3
        WHERE user_id = $1 AND token = $2
        RETURNING token, user_id, name, workspace_path, fs_policy,
                  shell_timeout_max, ssrf_whitelist, mcp_servers, created_at
        "#,
    )
    .bind(user_id)
    .bind(old_token.clone())
    .bind(new_token)
    .fetch_one(pool)
    .await?;
    Ok(Some((old_token, row)))
}

pub async fn delete_by_user_and_name(
    pool: &PgPool,
    user_id: Uuid,
    name: &str,
) -> Result<Option<DeviceRow>, sqlx::Error> {
    sqlx::query_as::<_, DeviceRow>(
        r#"
        DELETE FROM devices
        WHERE user_id = $1 AND name = $2
        RETURNING token, user_id, name, workspace_path, fs_policy,
                  shell_timeout_max, ssrf_whitelist, mcp_servers, created_at
        "#,
    )
    .bind(user_id)
    .bind(name)
    .fetch_optional(pool)
    .await
}
```

- [ ] **Step 6: Strengthen canonical schema for slug names**

Modify the `devices.name` check in `plexus-server/src/db/schema.sql`:

```sql
name TEXT NOT NULL CHECK (name ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND name <> 'server'),
```

- [ ] **Step 7: Run helper tests**

Run:

```bash
cargo test -p plexus-server db::devices::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add plexus-server/src/db/mod.rs plexus-server/src/db/devices.rs plexus-server/src/db/schema.sql
git commit -m "feat: add device persistence helpers"
```

---

### Task 3: Device REST API

**Files:**
- Create: `plexus-server/src/routes/devices.rs`
- Modify: `plexus-server/src/routes/mod.rs`
- Test: `plexus-server/tests/m1e_devices_rest.rs`

- [ ] **Step 1: Write failing REST lifecycle tests**

Create `plexus-server/tests/m1e_devices_rest.rs`:

```rust
mod support;

use axum::http::{Method, StatusCode};
use serde_json::json;
use support::TestApp;

#[tokio::test]
async fn device_lifecycle_returns_token_once_and_hint_afterwards() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;

    let (status, created) = support::json_request(
        app.router.clone(),
        Method::POST,
        "/api/devices",
        json!({"name": "MacBook Pro"}),
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token = created["token"].as_str().unwrap();
    assert!(token.starts_with("plexus_dev_"));
    assert_eq!(created["device"]["name"], "macbook-pro");
    assert_eq!(created["device"]["workspace_path"], "~/plexus/workspace");
    assert_eq!(created["device"]["fs_policy"], "sandbox");
    assert_eq!(created["device"]["shell_timeout_max"], 300);
    assert_eq!(created["device"]["ssrf_whitelist"], json!([]));
    assert_eq!(created["device"]["mcp_servers"], json!({}));

    let (status, list) = support::json_request(
        app.router.clone(),
        Method::GET,
        "/api/devices",
        json!({}),
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert!(list[0].get("token").is_none());
    assert_eq!(list[0]["token_hint"], format!("plexus_dev_...{}", &token[token.len() - 4..]));
    assert_eq!(list[0]["online"], false);
}

#[tokio::test]
async fn create_rejects_bad_names_and_duplicate_same_user() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;

    for name in ["server", "办公室电脑", "bad/name"] {
        let (status, body) = support::json_request(
            app.router.clone(),
            Method::POST,
            "/api/devices",
            json!({"name": name}),
            Some(&jwt),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "invalid_args");
    }

    let (status, _) = support::json_request(
        app.router.clone(),
        Method::POST,
        "/api/devices",
        json!({"name": "Lab PC 01"}),
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = support::json_request(
        app.router.clone(),
        Method::POST,
        "/api/devices",
        json!({"name": "lab-pc-01"}),
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "invalid_args");

    let (other_jwt, _) = support::register_user(&app, "bob@example.com").await;
    let (status, _) = support::json_request(
        app.router.clone(),
        Method::POST,
        "/api/devices",
        json!({"name": "lab-pc-01"}),
        Some(&other_jwt),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn patch_can_rename_and_update_config_without_changing_token() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;

    let (status, created) = support::json_request(
        app.router.clone(),
        Method::POST,
        "/api/devices",
        json!({"name": "old laptop"}),
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let token = created["token"].as_str().unwrap().to_string();

    let (status, patched) = support::json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/devices/old-laptop/config",
        json!({
            "name": "New Laptop",
            "workspace_path": "/tmp/plexus-testing-path",
            "fs_policy": "unrestricted",
            "shell_timeout_max": 120,
            "ssrf_whitelist": ["10.0.0.5:8080"],
            "mcp_servers": {"minimax": {"command": ["npx", "minimax"]}}
        }),
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(patched.get("token").is_none());
    assert_eq!(patched["name"], "new-laptop");
    assert_eq!(patched["workspace_path"], "/tmp/plexus-testing-path");
    assert_eq!(patched["fs_policy"], "unrestricted");
    assert_eq!(patched["shell_timeout_max"], 120);
    assert_eq!(patched["ssrf_whitelist"], json!(["10.0.0.5:8080"]));

    let row: (String,) = sqlx::query_as("SELECT token FROM devices WHERE name = 'new-laptop'")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(row.0, token);
}

#[tokio::test]
async fn regenerate_preserves_config_and_delete_removes_device() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;

    let (status, created) = support::json_request(
        app.router.clone(),
        Method::POST,
        "/api/devices",
        json!({"name": "devbox", "workspace_path": "/tmp/plexus-testing-path"}),
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let old_token = created["token"].as_str().unwrap().to_string();

    let (status, regenerated) = support::json_request(
        app.router.clone(),
        Method::POST,
        "/api/devices/devbox/regenerate-token",
        json!({}),
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let new_token = regenerated["token"].as_str().unwrap();
    assert_ne!(new_token, old_token);
    assert_eq!(regenerated["device"]["workspace_path"], "/tmp/plexus-testing-path");

    let (status, _) = support::empty_request(
        app.router.clone(),
        Method::DELETE,
        "/api/devices/devbox",
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM devices")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(count.0, 0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p plexus-server --test m1e_devices_rest -- --nocapture
```

Expected: FAIL with 404 responses because device routes are not mounted.

- [ ] **Step 3: Implement REST route module**

Create `plexus-server/src/routes/devices.rs`:

```rust
use crate::{
    app::AppState,
    auth::AuthUser,
    db::devices::{self, DevicePatch, DeviceRow, NewDevice},
    error::ApiError,
};
use axum::{Json, extract::{Path, State}, http::StatusCode};
use plexus_common::ErrorCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
pub struct CreateDeviceRequest {
    name: String,
    workspace_path: Option<String>,
    fs_policy: Option<String>,
    shell_timeout_max: Option<i32>,
    ssrf_whitelist: Option<Value>,
    mcp_servers: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct PatchDeviceRequest {
    name: Option<String>,
    workspace_path: Option<String>,
    fs_policy: Option<String>,
    shell_timeout_max: Option<i32>,
    ssrf_whitelist: Option<Value>,
    mcp_servers: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct DeviceResponse {
    name: String,
    workspace_path: String,
    fs_policy: String,
    shell_timeout_max: i32,
    ssrf_whitelist: Value,
    mcp_servers: Value,
    created_at: time::OffsetDateTime,
    online: bool,
    token_hint: String,
}

#[derive(Debug, Serialize)]
pub struct DeviceWithTokenResponse {
    token: String,
    device: DeviceResponse,
}

pub async fn create_device(
    AuthUser { user }: AuthUser,
    State(state): State<AppState>,
    Json(req): Json<CreateDeviceRequest>,
) -> Result<(StatusCode, Json<DeviceWithTokenResponse>), ApiError> {
    let new = request_to_new_device(req)?;
    let row = devices::create(state.pool(), user.id, new)
        .await
        .map_err(map_write_error)?;
    let token = row.token.clone();
    let device = response_for(&row, state.devices().is_online(&row.token).await);
    Ok((StatusCode::CREATED, Json(DeviceWithTokenResponse { token, device })))
}

pub async fn list_devices(
    AuthUser { user }: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<DeviceResponse>>, ApiError> {
    let rows = devices::list_by_user(state.pool(), user.id)
        .await
        .map_err(ApiError::from_sqlx)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let online = state.devices().is_online(&row.token).await;
        out.push(response_for(&row, online));
    }
    Ok(Json(out))
}

pub async fn patch_device(
    AuthUser { user }: AuthUser,
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<PatchDeviceRequest>,
) -> Result<Json<DeviceResponse>, ApiError> {
    let patch = request_to_patch(req)?;
    let row = devices::patch(state.pool(), user.id, &name, patch)
        .await
        .map_err(map_write_error)?
        .ok_or_else(not_found)?;
    let online = state.devices().is_online(&row.token).await;
    if online {
        state.devices().send_config_update(&row).await;
    }
    Ok(Json(response_for(&row, online)))
}

pub async fn regenerate_token(
    AuthUser { user }: AuthUser,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<DeviceWithTokenResponse>, ApiError> {
    let (old_token, row) = devices::regenerate_token(state.pool(), user.id, &name)
        .await
        .map_err(ApiError::from_sqlx)?
        .ok_or_else(not_found)?;
    state.devices().close(&old_token, crate::devices::CloseReason::Unauthorized).await;
    let token = row.token.clone();
    let device = response_for(&row, false);
    Ok(Json(DeviceWithTokenResponse { token, device }))
}

pub async fn delete_device(
    AuthUser { user }: AuthUser,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let row = devices::delete_by_user_and_name(state.pool(), user.id, &name)
        .await
        .map_err(ApiError::from_sqlx)?
        .ok_or_else(not_found)?;
    state.devices().close(&row.token, crate::devices::CloseReason::Unauthorized).await;
    Ok(StatusCode::NO_CONTENT)
}

fn request_to_new_device(req: CreateDeviceRequest) -> Result<NewDevice, ApiError> {
    let mut new = devices::default_new_device(&req.name).map_err(ApiError::invalid_args)?;
    if let Some(path) = req.workspace_path {
        if path.trim().is_empty() {
            return Err(ApiError::invalid_args("workspace_path must not be empty"));
        }
        new.workspace_path = path;
    }
    if let Some(policy) = req.fs_policy {
        new.fs_policy = devices::validate_fs_policy(&policy).map_err(ApiError::invalid_args)?;
    }
    if let Some(timeout) = req.shell_timeout_max {
        new.shell_timeout_max = devices::validate_shell_timeout(timeout).map_err(ApiError::invalid_args)?;
    }
    if let Some(value) = req.ssrf_whitelist {
        if !value.is_array() {
            return Err(ApiError::invalid_args("ssrf_whitelist must be an array"));
        }
        new.ssrf_whitelist = value;
    }
    if let Some(value) = req.mcp_servers {
        if !value.is_object() {
            return Err(ApiError::invalid_args("mcp_servers must be an object"));
        }
        new.mcp_servers = value;
    }
    Ok(new)
}

fn request_to_patch(req: PatchDeviceRequest) -> Result<DevicePatch, ApiError> {
    Ok(DevicePatch {
        name: req.name.map(|name| devices::normalize_device_name(&name)).transpose().map_err(ApiError::invalid_args)?,
        workspace_path: req.workspace_path,
        fs_policy: req.fs_policy.map(|policy| devices::validate_fs_policy(&policy)).transpose().map_err(ApiError::invalid_args)?,
        shell_timeout_max: req.shell_timeout_max.map(devices::validate_shell_timeout).transpose().map_err(ApiError::invalid_args)?,
        ssrf_whitelist: req.ssrf_whitelist,
        mcp_servers: req.mcp_servers,
    })
}

fn response_for(row: &DeviceRow, online: bool) -> DeviceResponse {
    DeviceResponse {
        name: row.name.clone(),
        workspace_path: row.workspace_path.clone(),
        fs_policy: row.fs_policy.clone(),
        shell_timeout_max: row.shell_timeout_max,
        ssrf_whitelist: row.ssrf_whitelist.clone(),
        mcp_servers: row.mcp_servers.clone(),
        created_at: row.created_at,
        online,
        token_hint: devices::token_hint(&row.token),
    }
}

fn not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, ErrorCode::InvalidArgs, "device not found")
}

fn map_write_error(err: sqlx::Error) -> ApiError {
    if let sqlx::Error::Database(db_err) = &err
        && db_err.is_unique_violation()
    {
        return ApiError::new(StatusCode::CONFLICT, ErrorCode::InvalidArgs, "device name already exists");
    }
    ApiError::from_sqlx(err)
}
```

- [ ] **Step 4: Mount routes**

Modify `plexus-server/src/routes/mod.rs` imports:

```rust
use axum::{
    Router,
    routing::{delete, get, post},
};

pub mod devices;
```

Add route mounts:

```rust
.route("/api/devices", get(devices::list_devices).post(devices::create_device))
.route(
    "/api/devices/{name}/config",
    axum::routing::patch(devices::patch_device),
)
.route(
    "/api/devices/{name}/regenerate-token",
    post(devices::regenerate_token),
)
.route("/api/devices/{name}", delete(devices::delete_device))
```

- [ ] **Step 5: Add temporary runtime stubs used by REST**

Extend `DeviceRuntime` in `devices/registry.rs` with temporary REST-facing methods. Task 4 replaces their internals with the real registry while keeping these method names stable:

```rust
impl DeviceRuntime {
    pub async fn is_online(&self, token: &str) -> bool {
        self.inner.lock().await.contains_key(token)
    }

    pub async fn send_config_update(&self, _row: &crate::db::devices::DeviceRow) {}

    pub async fn close(&self, _token: &str, _reason: CloseReason) {}
}
```

- [ ] **Step 6: Run REST tests**

Run:

```bash
cargo test -p plexus-server --test m1e_devices_rest -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add plexus-server/src/routes/mod.rs plexus-server/src/routes/devices.rs plexus-server/src/devices/registry.rs plexus-server/tests/m1e_devices_rest.rs
git commit -m "feat: add device lifecycle REST API"
```

---

### Task 4: Connection Registry Semantics

**Files:**
- Modify: `plexus-server/src/devices/registry.rs`

- [ ] **Step 1: Add failing registry tests**

Append tests to `registry.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use plexus_common::protocol::{PingFrame, WsFrame};

    fn handle(token: &str, name: &str) -> (ConnHandle, mpsc::Receiver<WsFrame>) {
        let (tx, rx) = mpsc::channel(8);
        let now = OffsetDateTime::now_utc();
        (
            ConnHandle {
                token: token.to_string(),
                user_id: Uuid::now_v7(),
                device_name: name.to_string(),
                connected_at: now,
                last_seen: now,
                tx,
            },
            rx,
        )
    }

    #[tokio::test]
    async fn replace_returns_old_handle_and_keeps_new_online() {
        let runtime = DeviceRuntime::new();
        let (old, _old_rx) = handle("t", "old");
        let (old_generation, old_replaced) = runtime.register(old).await;
        assert!(old_replaced.is_none());
        let (new, _new_rx) = handle("t", "new");
        let (new_generation, new_replaced) = runtime.register(new.clone()).await;
        assert!(new_replaced.is_some());
        assert!(new_generation > old_generation);
        assert!(runtime.is_online("t").await);
        assert_eq!(runtime.get("t").await.unwrap().device_name, "new");
    }

    #[tokio::test]
    async fn stale_cleanup_does_not_remove_replacement() {
        let runtime = DeviceRuntime::new();
        let (old, _old_rx) = handle("t", "old");
        let (old_generation, old_replaced) = runtime.register(old).await;
        assert!(old_replaced.is_none());
        let (new, _new_rx) = handle("t", "new");
        let (new_generation, new_replaced) = runtime.register(new).await;
        assert!(new_replaced.is_some());
        runtime.unregister_if_current("t", old_generation).await;
        assert_eq!(runtime.generation("t").await, Some(new_generation));
    }

    #[tokio::test]
    async fn send_frame_removes_stale_closed_channel() {
        let runtime = DeviceRuntime::new();
        let (h, rx) = handle("t", "devbox");
        drop(rx);
        runtime.register(h).await;
        let ok = runtime.send("t", WsFrame::Ping(PingFrame { id: Uuid::now_v7() })).await;
        assert!(!ok);
        assert!(!runtime.is_online("t").await);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p plexus-server devices::registry::tests -- --nocapture
```

Expected: FAIL because registry methods do not exist.

- [ ] **Step 3: Implement registry with generation guard**

Replace `registry.rs` internals with:

```rust
use crate::db::devices::DeviceRow;
use plexus_common::protocol::{ConfigUpdateFrame, WsFrame};
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    Replaced,
    Unauthorized,
    HeartbeatTimeout,
}

#[derive(Clone)]
pub struct ConnHandle {
    pub token: String,
    pub user_id: Uuid,
    pub device_name: String,
    pub connected_at: OffsetDateTime,
    pub last_seen: OffsetDateTime,
    pub tx: mpsc::Sender<WsFrame>,
}

#[derive(Clone)]
struct RegistryEntry {
    generation: u64,
    handle: ConnHandle,
}

#[derive(Clone, Default)]
pub struct DeviceRuntime {
    inner: Arc<Mutex<RegistryState>>,
}

#[derive(Default)]
struct RegistryState {
    next_generation: u64,
    by_token: HashMap<String, RegistryEntry>,
}

impl DeviceRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, handle: ConnHandle) -> (u64, Option<ConnHandle>) {
        let mut state = self.inner.lock().await;
        state.next_generation += 1;
        let generation = state.next_generation;
        let old = state.by_token.insert(
            handle.token.clone(),
            RegistryEntry { generation, handle },
        );
        (generation, old.map(|entry| entry.handle))
    }

    pub async fn generation(&self, token: &str) -> Option<u64> {
        self.inner.lock().await.by_token.get(token).map(|entry| entry.generation)
    }

    pub async fn get(&self, token: &str) -> Option<ConnHandle> {
        self.inner.lock().await.by_token.get(token).map(|entry| entry.handle.clone())
    }

    pub async fn is_online(&self, token: &str) -> bool {
        self.inner.lock().await.by_token.contains_key(token)
    }

    pub async fn unregister_if_current(&self, token: &str, generation: u64) {
        let mut state = self.inner.lock().await;
        if state.by_token.get(token).is_some_and(|entry| entry.generation == generation) {
            state.by_token.remove(token);
        }
    }

    pub async fn send(&self, token: &str, frame: WsFrame) -> bool {
        let handle = self.get(token).await;
        let Some(handle) = handle else { return false; };
        if handle.tx.send(frame).await.is_ok() {
            return true;
        }
        self.remove_stale_sender(token, &handle.tx).await;
        false
    }

    pub async fn send_config_update(&self, row: &DeviceRow) {
        let config = crate::devices::ws::device_config_from_row(row);
        let frame = WsFrame::ConfigUpdate(ConfigUpdateFrame { id: Uuid::now_v7(), config });
        let _ = self.send(&row.token, frame).await;
    }

    pub async fn close(&self, token: &str, reason: CloseReason) {
        let Some(handle) = self.get(token).await else { return; };
        let frame = crate::devices::ws::close_command_frame(reason);
        let _ = handle.tx.send(frame).await;
    }

    async fn remove_stale_sender(&self, token: &str, tx: &mpsc::Sender<WsFrame>) {
        let mut state = self.inner.lock().await;
        if state.by_token.get(token).is_some_and(|entry| entry.handle.tx.same_channel(tx)) {
            state.by_token.remove(token);
        }
    }
}
```

- [ ] **Step 4: Run registry tests**

Run:

```bash
cargo test -p plexus-server devices::registry::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plexus-server/src/devices/registry.rs
git commit -m "feat: add device connection registry"
```

---

### Task 5: WebSocket Handshake and Server-Driven Heartbeat

**Files:**
- Modify: `plexus-server/src/devices/ws.rs`
- Modify: `plexus-server/src/routes/mod.rs`
- Modify: `plexus-server/tests/support/mod.rs`
- Create: `plexus-server/tests/support/device_client.rs`
- Create: `plexus-server/tests/m1e_device_ws.rs`

- [ ] **Step 1: Add real-server support for WebSocket tests**

Append to `plexus-server/tests/support/mod.rs`:

```rust
use tokio::net::TcpListener;

impl TestApp {
    pub async fn spawn_server(&self) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = self.router.clone();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("ws://{}", addr)
    }
}

pub mod device_client;
```

- [ ] **Step 2: Create fake device client helper**

Create `plexus-server/tests/support/device_client.rs`:

```rust
use futures_util::{SinkExt, StreamExt};
use plexus_common::protocol::{HelloCaps, HelloFrame, PongFrame, WsFrame};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::{Message, client::IntoClientRequest}};
use uuid::Uuid;

pub struct DeviceClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl DeviceClient {
    pub async fn connect(base: &str, token: Option<&str>) -> Self {
        Self::connect_path(base, "/ws/device", token).await
    }

    pub async fn connect_path(base: &str, path: &str, token: Option<&str>) -> Self {
        let mut req = format!("{base}{path}").into_client_request().unwrap();
        if let Some(token) = token {
            req.headers_mut().insert(
                "Authorization",
                format!("Bearer {token}").parse().unwrap(),
            );
        }
        let (ws, _) = connect_async(req).await.unwrap();
        Self { ws }
    }

    pub async fn send_hello(&mut self, version: &str) -> Uuid {
        let id = Uuid::now_v7();
        self.send(WsFrame::Hello(HelloFrame {
            id,
            version: version.to_string(),
            client_version: "test-client".to_string(),
            os: "linux".to_string(),
            caps: HelloCaps { sandbox: "none".to_string(), exec: false, fs: "rw".to_string() },
        })).await;
        id
    }

    pub async fn send(&mut self, frame: WsFrame) {
        let text = serde_json::to_string(&frame).unwrap();
        self.ws.send(Message::Text(text.into())).await.unwrap();
    }

    pub async fn recv_frame(&mut self) -> WsFrame {
        loop {
            match self.ws.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(payload) => self.ws.send(Message::Pong(payload)).await.unwrap(),
                other => panic!("unexpected websocket message: {other:?}"),
            }
        }
    }

    pub async fn recv_close_code(&mut self) -> u16 {
        loop {
            match self.ws.next().await.unwrap().unwrap() {
                Message::Close(Some(frame)) => return frame.code.into(),
                Message::Close(None) => return 1005,
                Message::Text(_) => continue,
                other => panic!("unexpected websocket message: {other:?}"),
            }
        }
    }

    pub async fn reply_pong(&mut self, id: Uuid) {
        self.send(WsFrame::Pong(PongFrame { id })).await;
    }
}
```

- [ ] **Step 3: Write failing WS tests**

Create `plexus-server/tests/m1e_device_ws.rs`:

```rust
mod support;

use axum::http::{Method, StatusCode};
use plexus_common::{protocol::WsFrame, version::PROTOCOL_VERSION};
use serde_json::json;
use support::{TestApp, device_client::DeviceClient};

async fn create_device(app: &TestApp, jwt: &str) -> String {
    let (status, body) = support::json_request(
        app.router.clone(),
        Method::POST,
        "/api/devices",
        json!({"name": "devbox"}),
        Some(jwt),
    ).await;
    assert_eq!(status, StatusCode::CREATED);
    body["token"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn valid_hello_receives_hello_ack_and_device_is_online() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;
    let token = create_device(&app, &jwt).await;
    let base = app.spawn_server().await;

    let mut client = DeviceClient::connect(&base, Some(&token)).await;
    let hello_id = client.send_hello(PROTOCOL_VERSION).await;
    let frame = client.recv_frame().await;
    match frame {
        WsFrame::HelloAck(ack) => {
            assert_eq!(ack.id, hello_id);
            assert_eq!(ack.device_name, "devbox");
            assert_eq!(ack.config.workspace_path, "~/plexus/workspace");
        }
        other => panic!("expected hello_ack, got {other:?}"),
    }

    let (status, list) = support::json_request(app.router.clone(), Method::GET, "/api/devices", json!({}), Some(&jwt)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list[0]["online"], true);
}

#[tokio::test]
async fn missing_or_query_token_is_rejected() {
    let app = TestApp::spawn().await;
    let base = app.spawn_server().await;

    let mut no_header = DeviceClient::connect(&base, None).await;
    assert_eq!(no_header.recv_close_code().await, 4401);

    let mut query_token = DeviceClient::connect_path(&base, "/ws/device?token=not-accepted", None).await;
    assert_eq!(query_token.recv_close_code().await, 4401);
}

#[tokio::test]
async fn protocol_mismatch_closes_4409() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;
    let token = create_device(&app, &jwt).await;
    let base = app.spawn_server().await;

    let mut client = DeviceClient::connect(&base, Some(&token)).await;
    client.send_hello("999").await;
    assert_eq!(client.recv_close_code().await, 4409);
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run:

```bash
cargo test -p plexus-server --test m1e_device_ws -- --nocapture
```

Expected: FAIL because `/ws/device` is not mounted and the temporary `devices::ws` handler returns bad request.

- [ ] **Step 5: Mount WebSocket route**

Modify `routes/mod.rs` imports:

```rust
use axum::{
    Router,
    routing::{delete, get, post},
};
```

Add route:

```rust
.route("/ws/device", get(crate::devices::ws::device_ws))
```

- [ ] **Step 6: Implement WebSocket handshake**

Replace `devices/ws.rs` with a real handler. Keep heartbeat loop minimal in this task; timeout behavior lands in the next task.

```rust
use crate::{app::AppState, db::{devices, devices::DeviceRow}};
use axum::{
    extract::{State, ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade}},
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use plexus_common::{ErrorCode, protocol::{DeviceConfig, ErrorFrame, FsPolicy, HelloAckFrame, PingFrame, WsFrame}, version::PROTOCOL_VERSION};
use std::borrow::Cow;
use tokio::sync::mpsc;
use uuid::Uuid;

pub async fn device_ws(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let token = bearer_token(&headers);
    ws.on_upgrade(move |socket| async move {
        run_socket(state, socket, token).await;
    })
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    value.strip_prefix("Bearer ").map(ToOwned::to_owned)
}

async fn run_socket(state: AppState, mut socket: WebSocket, token: Option<String>) {
    let Some(token) = token else {
        close(&mut socket, 4401, r#"{"code":"unauthorized"}"#).await;
        return;
    };
    let row = match devices::find_by_token(state.pool(), &token).await {
        Ok(Some(row)) => row,
        _ => {
            close(&mut socket, 4401, r#"{"code":"unauthorized"}"#).await;
            return;
        }
    };
    let Some(Ok(Message::Text(text))) = socket.next().await else {
        close(&mut socket, 1002, "expected hello").await;
        return;
    };
    let Ok(WsFrame::Hello(hello)) = serde_json::from_str::<WsFrame>(&text) else {
        close(&mut socket, 1002, "expected hello").await;
        return;
    };
    if hello.version != PROTOCOL_VERSION {
        close(&mut socket, 4409, r#"{"code":"version_unsupported"}"#).await;
        return;
    }

    let ack = WsFrame::HelloAck(HelloAckFrame {
        id: hello.id,
        device_name: row.name.clone(),
        user_id: row.user_id,
        config: device_config_from_row(&row),
    });
    let text = serde_json::to_string(&ack).unwrap();
    if socket.send(Message::Text(text.into())).await.is_err() {
        return;
    }

    let (tx, mut rx) = mpsc::channel::<WsFrame>(32);
    let now = time::OffsetDateTime::now_utc();
    let handle = crate::devices::ConnHandle {
        token: row.token.clone(),
        user_id: row.user_id,
        device_name: row.name.clone(),
        connected_at: now,
        last_seen: now,
        tx,
    };
    let (generation, old) = state.devices().register(handle).await;
    if let Some(old) = old {
        let _ = old.tx.send(close_command_frame(crate::devices::CloseReason::Replaced)).await;
    }

    let (mut sender, mut receiver) = socket.split();
    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            let text = serde_json::to_string(&frame).unwrap();
            if sender.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(Ok(message)) = receiver.next().await {
        match message {
            Message::Text(text) => match serde_json::from_str::<WsFrame>(&text) {
                Ok(WsFrame::Pong(_)) => {}
                Ok(WsFrame::Error(_)) => {}
                _ => {}
            },
            Message::Close(_) => break,
            _ => {}
        }
    }
    writer.abort();
    state.devices().unregister_if_current(&row.token, generation).await;
}

pub fn device_config_from_row(row: &DeviceRow) -> DeviceConfig {
    DeviceConfig {
        workspace_path: row.workspace_path.clone(),
        fs_policy: if row.fs_policy == "unrestricted" { FsPolicy::Unrestricted } else { FsPolicy::Sandbox },
        shell_timeout_max: row.shell_timeout_max as u32,
        ssrf_whitelist: serde_json::from_value(row.ssrf_whitelist.clone()).unwrap_or_default(),
        mcp_servers: serde_json::from_value(row.mcp_servers.clone()).unwrap_or_default(),
    }
}

pub fn close_command_frame(reason: crate::devices::CloseReason) -> WsFrame {
    let (code, message) = match reason {
        crate::devices::CloseReason::Replaced => (ErrorCode::Ok, "connection replaced"),
        crate::devices::CloseReason::Unauthorized => (ErrorCode::Unauthorized, "unauthorized"),
        crate::devices::CloseReason::HeartbeatTimeout => (ErrorCode::DeviceUnreachable, "heartbeat timeout"),
    };
    WsFrame::Error(ErrorFrame { id: None, code, message: message.to_string() })
}

async fn close(socket: &mut WebSocket, code: u16, reason: &'static str) {
    let _ = socket.send(Message::Close(Some(CloseFrame {
        code,
        reason: Cow::Borrowed(reason),
    }))).await;
}
```

- [ ] **Step 7: Run handshake tests**

Run:

```bash
cargo test -p plexus-server --test m1e_device_ws -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add plexus-server/src/devices/ws.rs plexus-server/src/routes/mod.rs plexus-server/tests/support/mod.rs plexus-server/tests/support/device_client.rs plexus-server/tests/m1e_device_ws.rs
git commit -m "feat: add device websocket handshake"
```

---

### Task 6: Heartbeat, Duplicate Replacement, Config Update, and Revocation Closes

**Files:**
- Modify: `plexus-server/src/devices/ws.rs`
- Modify: `plexus-server/src/devices/registry.rs`
- Modify: `plexus-server/tests/m1e_device_ws.rs`

- [ ] **Step 1: Add failing tests for runtime behavior**

Append to `m1e_device_ws.rs`:

```rust
#[tokio::test]
async fn duplicate_connection_replaces_old_connection() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;
    let token = create_device(&app, &jwt).await;
    let base = app.spawn_server().await;

    let mut first = DeviceClient::connect(&base, Some(&token)).await;
    first.send_hello(PROTOCOL_VERSION).await;
    assert!(matches!(first.recv_frame().await, WsFrame::HelloAck(_)));

    let mut second = DeviceClient::connect(&base, Some(&token)).await;
    second.send_hello(PROTOCOL_VERSION).await;
    assert!(matches!(second.recv_frame().await, WsFrame::HelloAck(_)));

    assert_eq!(first.recv_close_code().await, 1000);

    let (status, list) = support::json_request(app.router.clone(), Method::GET, "/api/devices", json!({}), Some(&jwt)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list[0]["online"], true);
}

#[tokio::test]
async fn patch_sends_live_config_update() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;
    let token = create_device(&app, &jwt).await;
    let base = app.spawn_server().await;

    let mut client = DeviceClient::connect(&base, Some(&token)).await;
    client.send_hello(PROTOCOL_VERSION).await;
    assert!(matches!(client.recv_frame().await, WsFrame::HelloAck(_)));

    let (status, _) = support::json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/devices/devbox/config",
        json!({"workspace_path": "/tmp/plexus-testing-path"}),
        Some(&jwt),
    ).await;
    assert_eq!(status, StatusCode::OK);

    match client.recv_frame().await {
        WsFrame::ConfigUpdate(update) => assert_eq!(update.config.workspace_path, "/tmp/plexus-testing-path"),
        other => panic!("expected config_update, got {other:?}"),
    }
}

#[tokio::test]
async fn regenerate_closes_active_old_token_connection_4401() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;
    let token = create_device(&app, &jwt).await;
    let base = app.spawn_server().await;

    let mut client = DeviceClient::connect(&base, Some(&token)).await;
    client.send_hello(PROTOCOL_VERSION).await;
    assert!(matches!(client.recv_frame().await, WsFrame::HelloAck(_)));

    let (status, _) = support::json_request(app.router.clone(), Method::POST, "/api/devices/devbox/regenerate-token", json!({}), Some(&jwt)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(client.recv_close_code().await, 4401);
}

#[tokio::test]
async fn delete_closes_active_connection_4401() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;
    let token = create_device(&app, &jwt).await;
    let base = app.spawn_server().await;

    let mut client = DeviceClient::connect(&base, Some(&token)).await;
    client.send_hello(PROTOCOL_VERSION).await;
    assert!(matches!(client.recv_frame().await, WsFrame::HelloAck(_)));

    let (status, _) = support::empty_request(
        app.router.clone(),
        Method::DELETE,
        "/api/devices/devbox",
        Some(&jwt),
    ).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(client.recv_close_code().await, 4401);
}

#[tokio::test(start_paused = true)]
async fn server_driven_heartbeat_pings_and_missed_pongs_close_4408() {
    let app = TestApp::spawn().await;
    let (jwt, _) = support::register_user(&app, "alice@example.com").await;
    let token = create_device(&app, &jwt).await;
    let base = app.spawn_server().await;

    let mut client = DeviceClient::connect(&base, Some(&token)).await;
    client.send_hello(PROTOCOL_VERSION).await;
    assert!(matches!(client.recv_frame().await, WsFrame::HelloAck(_)));

    let first_ping = match client.recv_frame().await {
        WsFrame::Ping(ping) => ping,
        other => panic!("expected ping, got {other:?}"),
    };
    client.reply_pong(first_ping.id).await;
    let (status, list) = support::json_request(app.router.clone(), Method::GET, "/api/devices", json!({}), Some(&jwt)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list[0]["online"], true);

    tokio::time::advance(std::time::Duration::from_secs(30)).await;
    let second_ping = match client.recv_frame().await {
        WsFrame::Ping(ping) => ping,
        other => panic!("expected ping, got {other:?}"),
    };
    assert_ne!(second_ping.id, first_ping.id);

    tokio::time::advance(std::time::Duration::from_secs(60)).await;
    assert_eq!(client.recv_close_code().await, 4408);
    let (status, list) = support::json_request(app.router.clone(), Method::GET, "/api/devices", json!({}), Some(&jwt)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list[0]["online"], false);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -p plexus-server --test m1e_device_ws -- --nocapture
```

Expected: FAIL until registry close commands are actual WebSocket close messages and live config updates flow through writer.

- [ ] **Step 3: Replace close command pseudo-frame with registry commands**

Change registry channel type from `mpsc::Sender<WsFrame>` to a local command enum:

```rust
#[derive(Debug, Clone)]
pub enum DeviceCommand {
    Frame(WsFrame),
    Close(CloseReason),
}
```

Update `ConnHandle.tx`:

```rust
pub tx: mpsc::Sender<DeviceCommand>,
```

Update `send` and `send_config_update` to send `DeviceCommand::Frame(frame)`. Update `close` to send `DeviceCommand::Close(reason)`.

- [ ] **Step 4: Update WS writer to handle close commands**

In `ws.rs`, change the channel type and writer loop:

```rust
let (tx, mut rx) = mpsc::channel::<crate::devices::registry::DeviceCommand>(32);

let writer = tokio::spawn(async move {
    while let Some(command) = rx.recv().await {
        match command {
            crate::devices::registry::DeviceCommand::Frame(frame) => {
                let text = serde_json::to_string(&frame).unwrap();
                if sender.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            crate::devices::registry::DeviceCommand::Close(reason) => {
                let (code, body) = close_payload(reason);
                let _ = sender.send(Message::Close(Some(CloseFrame {
                    code,
                    reason: Cow::Owned(body),
                }))).await;
                break;
            }
        }
    }
});
```

Add helper:

```rust
fn close_payload(reason: crate::devices::CloseReason) -> (u16, String) {
    match reason {
        crate::devices::CloseReason::Replaced => (1000, String::new()),
        crate::devices::CloseReason::Unauthorized => (4401, r#"{"code":"unauthorized"}"#.to_string()),
        crate::devices::CloseReason::HeartbeatTimeout => (4408, String::new()),
    }
}
```

- [ ] **Step 5: Add server-driven heartbeat**

Inside `run_socket`, create a heartbeat interval and track missed pongs:

```rust
let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(crate::devices::HEARTBEAT_INTERVAL_SECS));
let mut awaiting_pong: Option<Uuid> = None;
let mut missed: u8 = 0;

loop {
    tokio::select! {
        _ = heartbeat.tick() => {
            if awaiting_pong.is_some() {
                missed += 1;
                if missed >= crate::devices::HEARTBEAT_MISSED_LIMIT {
                    state.devices().close(&row.token, crate::devices::CloseReason::HeartbeatTimeout).await;
                    break;
                }
            }
            let id = Uuid::now_v7();
            awaiting_pong = Some(id);
            let _ = state.devices().send(&row.token, WsFrame::Ping(PingFrame { id })).await;
        }
        msg = receiver.next() => {
            let Some(Ok(message)) = msg else { break; };
            match message {
                Message::Text(text) => match serde_json::from_str::<WsFrame>(&text) {
                    Ok(WsFrame::Pong(pong)) if Some(pong.id) == awaiting_pong => {
                        awaiting_pong = None;
                        missed = 0;
                    }
                    Ok(WsFrame::Error(_)) => {}
                    _ => {}
                },
                Message::Close(_) => break,
                _ => {}
            }
        }
    }
}
```

Keep the stale cleanup after the loop.

- [ ] **Step 6: Run WebSocket runtime tests**

Run:

```bash
cargo test -p plexus-server --test m1e_device_ws -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add plexus-server/src/devices/registry.rs plexus-server/src/devices/ws.rs plexus-server/tests/m1e_device_ws.rs
git commit -m "feat: complete device websocket runtime"
```

---

### Task 7: Schema Docs and Final Verification

**Files:**
- Modify: `docs/SCHEMA.md`
- Modify: `docs/reference/superpowers/specs/2026-05-21-plexus-m1e-device-connectivity-design.md` only if implementation discovers a necessary correction.

- [ ] **Step 1: Update schema docs**

In `docs/SCHEMA.md` section `8. devices`, add these bullets after the SQL block:

```markdown
- `token` is the plaintext device credential and primary key. It is returned only from `POST /api/devices` and `POST /api/devices/{name}/regenerate-token`.
- `GET /api/devices` returns full device details plus `token_hint`; it never returns the plaintext token.
- `name` is stored as a canonical `a-z0-9-` slug. The value `server` is reserved.
- Online state is derived from the in-memory device connection registry; the table has no `online` or `last_seen_at` columns.
- `workspace_path` defaults to the literal `~/plexus/workspace`; the server stores explicit overrides verbatim.
```

- [ ] **Step 2: Run focused checks**

Run:

```bash
cargo test -p plexus-server db::devices::tests -- --nocapture
cargo test -p plexus-server --test m1e_devices_rest -- --nocapture
cargo test -p plexus-server --test m1e_device_ws -- --nocapture
```

Expected: all PASS.

- [ ] **Step 3: Run package tests**

Run:

```bash
cargo test -p plexus-server
```

Expected: all PASS.

- [ ] **Step 4: Run formatting and diff checks**

Run:

```bash
cargo fmt --all -- --check
git diff --check
git status --short
```

Expected:

```text
cargo fmt: no output and exit 0
git diff --check: no output and exit 0
git status --short: only expected M1e implementation/doc files before final commit
```

- [ ] **Step 5: Commit docs and verification updates**

```bash
git add docs/SCHEMA.md
git commit -m "docs: update schema for M1e devices"
```

Commit `docs/SCHEMA.md`. Amend this commit to include the M1e spec only when implementation uncovered a real contract correction.

---

## Self-Review

Spec coverage:

- REST lifecycle: Tasks 2 and 3 implement create, list, patch, regenerate, delete.
- Header-only WS auth: Task 5 tests and implements bearer header extraction and rejects query-token fallback.
- Slug names, `server` reservation, same-user conflicts, and same-name cross-user allowance: Task 2 helper tests and Task 3 REST tests cover them.
- Plaintext token returned once and `token_hint` later: Task 3 covers it.
- Config defaults and explicit `workspace_path` override: Task 2 helpers and Task 3 tests cover it.
- Rename via PATCH: Task 3 covers it.
- Live `config_update`: Task 6 covers it.
- Online state in memory: Tasks 1, 4, 5, and 6 cover it.
- Duplicate replacement and stale cleanup: Tasks 4 and 6 cover it.
- Server-driven heartbeat: Task 6 covers it.
- Regenerate/delete close active sockets with 4401: Tasks 3 and 6 cover it.

Placeholder scan:

- No task uses TBD/TODO/fill-in language.
- Code snippets name concrete files and functions.
- Commands include expected outcomes.

Type consistency:

- REST code uses `db::devices::DeviceRow`, `NewDevice`, and `DevicePatch` from Task 2.
- Runtime code uses `DeviceRuntime`, `ConnHandle`, `CloseReason`, and `DeviceCommand` consistently after Task 6 updates the channel type.
- WebSocket code uses `plexus_common::protocol::WsFrame` and does not define duplicate frame types.
