# Plexus M1d Workspace Files and Attachments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement M1d server workspace files, strict message attachment ingestion, quota-aware workspace operations, server file tools, and tool schema merge v0.

**Architecture:** M1d adds a server-side `workspace_fs` service as the only file-operation and quota choke point. REST routes and server file tools call that service; browser message ingress validates strict content/attachment arrays and expands attachment refs into path markers plus generated image blocks. The tool registry implements only merge v0: inject required `plexus_device` with enum `["server"]` for shared file tools, while dynamic device discovery remains an M1f delta.

**Tech Stack:** Rust 2024, Axum, Tokio fs, SQLx/Postgres, `serde_json`, `base64`, `sha2`, `regex`, existing `plexus-common` content blocks and errors, real PostgreSQL integration tests.

---

## Scope Check

The approved M1d spec covers several surfaces, but they are one coherent server milestone because all of them depend on the same `workspace_fs` and strict message contract:

- server workspace file service;
- workspace REST routes;
- strict browser message ingress;
- attachment expansion from workspace refs;
- server-side shared file tools;
- tool schema merge v0;
- docs alignment.

The plan keeps device routing out of scope. Any non-server `plexus_device` value fails clearly in M1d.

---

## File Structure

Create in Plexus:

- `plexus-server/src/workspace/mod.rs` - module exports for server workspace service.
- `plexus-server/src/workspace/fs.rs` - `WorkspaceFs`, path resolution, read/write/edit/delete/list/glob/grep, quota checks, image byte helpers.
- `plexus-server/src/routes/workspace.rs` - REST handlers for workspace quota and file APIs.
- `plexus-server/src/chat/attachments.rs` - attachment request parsing, server-device validation, image detection, dedupe, path marker assembly.
- `plexus-server/src/tools/mod.rs` - server tool module exports.
- `plexus-server/src/tools/registry.rs` - merge v0 schema injection and file-tool dispatch entry points.
- `plexus-server/src/tools/file_ops.rs` - server implementations of shared file tools over `WorkspaceFs`.
- `plexus-server/tests/m1d_workspace_fs.rs` - unit-style integration tests for `WorkspaceFs`.
- `plexus-server/tests/m1d_workspace_rest.rs` - REST route tests.
- `plexus-server/tests/m1d_message_contract.rs` - strict message request and attachment expansion tests.
- `plexus-server/tests/m1d_tools.rs` - registry injection and server file tool tests.

Modify in Plexus:

- `Cargo.toml` - add workspace dependencies `base64`, `sha2`, and `regex`.
- `plexus-server/Cargo.toml` - depend on `base64`, `sha2`, `regex`, and
  `globset`.
- `plexus-common/src/errors/mod.rs` - add `QuotaNotConfigured` wire code if missing quota is detected.
- `plexus-common/src/errors/workspace.rs` - add `WorkspaceError::QuotaNotConfigured`.
- `plexus-server/src/app.rs` - store `WorkspaceFs`.
- `plexus-server/src/lib.rs` - export `workspace` and `tools`.
- `plexus-server/src/routes/mod.rs` - mount workspace routes.
- `plexus-server/src/routes/sessions.rs` - parse strict message shape and call attachment assembler.
- `plexus-server/src/chat/content.rs` - remove string shorthand and expose base64 decode/hash helpers.
- `plexus-server/tests/support/mod.rs` - add raw/body request helpers for binary file REST tests.
- `plexus-server/tests/m1c_messages.rs` - update legacy M1c expectations that M1d intentionally changes.
- `docs/API.yaml`, `docs/TOOLS.md`, `docs/DECISIONS.md`, `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`, `docs/reference/superpowers/specs/2026-05-18-plexus-m1d-workspace-files-attachments-design.md` - align docs and status after implementation.

---

## Task 1: Workspace FS Foundation and Quota

**Files:**
- Modify: `Cargo.toml`
- Modify: `plexus-server/Cargo.toml`
- Modify: `plexus-common/src/errors/mod.rs`
- Modify: `plexus-common/src/errors/workspace.rs`
- Create: `plexus-server/src/workspace/mod.rs`
- Create: `plexus-server/src/workspace/fs.rs`
- Modify: `plexus-server/src/lib.rs`
- Modify: `plexus-server/src/app.rs`
- Create: `plexus-server/tests/m1d_workspace_fs.rs`

- [ ] **Step 1: Write failing workspace service tests**

Create `plexus-server/tests/m1d_workspace_fs.rs`:

```rust
mod support;

use plexus_common::WorkspaceError;
use plexus_server::workspace::WorkspaceFs;
use serde_json::json;
use support::TestApp;

async fn set_quota(app: &TestApp, quota: i64) {
    sqlx::query(
        "INSERT INTO system_config (key, value, updated_at)
         VALUES ('quota_bytes', $1, NOW())
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(json!(quota))
    .execute(&app.pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn write_read_and_delete_file_under_user_workspace() {
    let app = TestApp::spawn().await;
    let (_, user_id) = support::register_user(&app, "alice@example.com").await;
    set_quota(&app, 10_000).await;

    let fs = WorkspaceFs::new(app.workspace_root.path().to_path_buf(), app.pool.clone());
    fs.write_file(user_id, "notes/hello.txt", b"hello".to_vec())
        .await
        .unwrap();

    let bytes = fs.read_file(user_id, "notes/hello.txt").await.unwrap();
    assert_eq!(bytes, b"hello");

    fs.delete_file(user_id, "notes/hello.txt").await.unwrap();
    let err = fs.read_file(user_id, "notes/hello.txt").await.unwrap_err();
    assert!(matches!(err, WorkspaceError::NotFound(_)));
}

#[tokio::test]
async fn path_traversal_is_rejected() {
    let app = TestApp::spawn().await;
    let (_, user_id) = support::register_user(&app, "alice@example.com").await;
    set_quota(&app, 10_000).await;

    let fs = WorkspaceFs::new(app.workspace_root.path().to_path_buf(), app.pool.clone());
    let err = fs.write_file(user_id, "../escape.txt", b"no".to_vec()).await.unwrap_err();
    assert!(matches!(err, WorkspaceError::PathOutsideWorkspace(_)));
}

#[tokio::test]
async fn missing_quota_blocks_writes() {
    let app = TestApp::spawn().await;
    let (_, user_id) = support::register_user(&app, "alice@example.com").await;

    let fs = WorkspaceFs::new(app.workspace_root.path().to_path_buf(), app.pool.clone());
    let err = fs.write_file(user_id, "a.txt", b"a".to_vec()).await.unwrap_err();
    assert!(matches!(err, WorkspaceError::QuotaNotConfigured));
}

#[tokio::test]
async fn upload_too_large_uses_single_op_cap() {
    let app = TestApp::spawn().await;
    let (_, user_id) = support::register_user(&app, "alice@example.com").await;
    set_quota(&app, 100).await;

    let fs = WorkspaceFs::new(app.workspace_root.path().to_path_buf(), app.pool.clone());
    let err = fs.write_file(user_id, "large.bin", vec![b'x'; 81]).await.unwrap_err();
    assert!(matches!(err, WorkspaceError::UploadTooLarge { .. }));
}

#[tokio::test]
async fn soft_lock_blocks_writes_but_allows_delete() {
    let app = TestApp::spawn().await;
    let (_, user_id) = support::register_user(&app, "alice@example.com").await;
    set_quota(&app, 1_000).await;

    let fs = WorkspaceFs::new(app.workspace_root.path().to_path_buf(), app.pool.clone());
    fs.write_file(user_id, "old.bin", vec![b'x'; 700]).await.unwrap();

    set_quota(&app, 100).await;
    let err = fs.write_file(user_id, "new.bin", b"no".to_vec()).await.unwrap_err();
    assert!(matches!(err, WorkspaceError::SoftLocked));

    fs.delete_file(user_id, "old.bin").await.unwrap();
    let quota = fs.quota(user_id).await.unwrap();
    assert!(!quota.locked);
}
```

- [ ] **Step 2: Run the failing workspace tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_workspace_fs
```

Expected: FAIL because `WorkspaceFs`, `QuotaNotConfigured`, and `support::register_user` do not exist yet.

- [ ] **Step 3: Add dependencies**

In root `Cargo.toml` workspace dependencies, add:

```toml
base64 = "0.22"
sha2 = "0.10"
regex = "1"
```

In `plexus-server/Cargo.toml`, add:

```toml
base64.workspace = true
sha2.workspace = true
regex.workspace = true
globset.workspace = true
```

- [ ] **Step 4: Add quota-not-configured error**

In `plexus-common/src/errors/mod.rs`, add `QuotaNotConfigured` under the workspace group:

```rust
    QuotaNotConfigured,
```

In `plexus-common/src/errors/workspace.rs`, add the variant and code mapping:

```rust
    #[error("workspace quota_bytes is not configured")]
    QuotaNotConfigured,
```

```rust
            WorkspaceError::QuotaNotConfigured => ErrorCode::QuotaNotConfigured,
```

Extend the existing tests:

```rust
#[test]
fn quota_not_configured_maps() {
    assert_eq!(
        WorkspaceError::QuotaNotConfigured.code(),
        ErrorCode::QuotaNotConfigured
    );
}
```

- [ ] **Step 5: Add shared test helper**

In `plexus-server/tests/support/mod.rs`, add:

```rust
pub async fn register_user(app: &TestApp, email: &str) -> (String, Uuid) {
    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        "/api/auth/register",
        serde_json::json!({
            "email": email,
            "password": "correct horse battery staple",
            "name": "Alice"
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    (
        body["jwt"].as_str().unwrap().to_string(),
        Uuid::parse_str(body["user"]["id"].as_str().unwrap()).unwrap(),
    )
}
```

- [ ] **Step 6: Create workspace module exports**

Create `plexus-server/src/workspace/mod.rs`:

```rust
pub mod fs;

pub use fs::{DirEntry, QuotaState, WorkspaceFs};
```

In `plexus-server/src/lib.rs`, add:

```rust
pub mod workspace;
```

- [ ] **Step 7: Implement `WorkspaceFs` foundation**

Create `plexus-server/src/workspace/fs.rs` with this public shape:

```rust
use plexus_common::WorkspaceError;
use sqlx::PgPool;
use std::path::{Component, Path, PathBuf};
use tokio::fs;
use uuid::Uuid;

#[derive(Clone)]
pub struct WorkspaceFs {
    root: PathBuf,
    pool: PgPool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct QuotaState {
    pub quota_bytes: u64,
    pub bytes_used: u64,
    pub locked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DirEntry {
    pub name: String,
    pub path: String,
    pub kind: String,
    pub size: u64,
}

impl WorkspaceFs {
    pub fn new(root: PathBuf, pool: PgPool) -> Self {
        Self { root, pool }
    }

    pub async fn quota(&self, user_id: Uuid) -> Result<QuotaState, WorkspaceError> {
        let quota_bytes = self.quota_bytes().await?;
        let workspace = self.personal_root(user_id);
        fs::create_dir_all(&workspace).await?;
        let bytes_used = dir_size(&workspace).await?;
        Ok(QuotaState {
            quota_bytes,
            bytes_used,
            locked: bytes_used > quota_bytes,
        })
    }

    pub async fn read_file(&self, user_id: Uuid, path: &str) -> Result<Vec<u8>, WorkspaceError> {
        let full = self.resolve_existing(user_id, path).await?;
        let meta = fs::metadata(&full).await?;
        if meta.is_dir() {
            return Err(WorkspaceError::PathOutsideWorkspace(full));
        }
        Ok(fs::read(full).await?)
    }

    pub async fn write_file(
        &self,
        user_id: Uuid,
        path: &str,
        bytes: Vec<u8>,
    ) -> Result<(), WorkspaceError> {
        self.ensure_can_add(user_id, bytes.len() as u64).await?;
        let full = self.resolve_for_write(user_id, path).await?;
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(full, bytes).await?;
        Ok(())
    }

    pub async fn delete_file(&self, user_id: Uuid, path: &str) -> Result<(), WorkspaceError> {
        let full = self.resolve_existing(user_id, path).await?;
        let meta = fs::metadata(&full).await?;
        if meta.is_dir() {
            return Err(WorkspaceError::PathOutsideWorkspace(full));
        }
        fs::remove_file(full).await?;
        Ok(())
    }

    fn personal_root(&self, user_id: Uuid) -> PathBuf {
        self.root.join(user_id.to_string())
    }
}
```

Complete the private helpers in the same file:

```rust
impl WorkspaceFs {
    async fn quota_bytes(&self) -> Result<u64, WorkspaceError> {
        let value: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT value FROM system_config WHERE key = 'quota_bytes'")
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| WorkspaceError::IoError(std::io::Error::other(err)))?;
        value
            .and_then(|v| v.as_i64())
            .filter(|v| *v > 0)
            .map(|v| v as u64)
            .ok_or(WorkspaceError::QuotaNotConfigured)
    }

    async fn ensure_can_add(&self, user_id: Uuid, added_bytes: u64) -> Result<(), WorkspaceError> {
        let quota = self.quota(user_id).await?;
        if quota.locked {
            return Err(WorkspaceError::SoftLocked);
        }
        if added_bytes > quota.quota_bytes.saturating_mul(80) / 100 {
            return Err(WorkspaceError::UploadTooLarge {
                actual_bytes: added_bytes,
                quota_bytes: quota.quota_bytes,
            });
        }
        Ok(())
    }

    async fn resolve_existing(&self, user_id: Uuid, path: &str) -> Result<PathBuf, WorkspaceError> {
        let root = self.personal_root(user_id);
        fs::create_dir_all(&root).await?;
        plexus_common::tools::path::resolve_in_workspace(&root, path)
    }

    async fn resolve_for_write(&self, user_id: Uuid, path: &str) -> Result<PathBuf, WorkspaceError> {
        if path.is_empty() || Path::new(path).components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(WorkspaceError::PathOutsideWorkspace(PathBuf::from(path)));
        }
        let root = self.personal_root(user_id);
        fs::create_dir_all(&root).await?;
        let canonical_root = root.canonicalize().map_err(|_| WorkspaceError::NotFound(root.clone()))?;
        let candidate = if Path::new(path).is_absolute() {
            PathBuf::from(path)
        } else {
            canonical_root.join(path)
        };
        if !candidate.starts_with(&canonical_root) {
            return Err(WorkspaceError::PathOutsideWorkspace(candidate));
        }
        Ok(candidate)
    }
}

async fn dir_size(root: &Path) -> Result<u64, WorkspaceError> {
    let mut total = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            if meta.is_dir() {
                stack.push(entry.path());
            } else if meta.is_file() {
                total += meta.len();
            }
        }
    }
    Ok(total)
}
```

- [ ] **Step 8: Store `WorkspaceFs` in app state**

In `plexus-server/src/app.rs`, import and store the service:

```rust
use crate::{
    chat::ChatRuntime, config::ServerConfig, openai::OpenAiRuntime, routes,
    workspace::WorkspaceFs,
};
```

Add to `AppStateInner`:

```rust
pub workspace_fs: WorkspaceFs,
```

Initialize in `new_with_openai_runtime`:

```rust
let workspace_fs = WorkspaceFs::new(config.workspace_root.clone(), pool.clone());
```

Expose:

```rust
pub fn workspace_fs(&self) -> &WorkspaceFs {
    &self.inner.workspace_fs
}
```

- [ ] **Step 9: Run workspace foundation tests**

Run:

```bash
rtk cargo test -p plexus-common workspace
rtk cargo test -p plexus-server --test m1d_workspace_fs
```

Expected: PASS.

- [ ] **Step 10: Commit workspace foundation**

```bash
git add Cargo.toml plexus-server/Cargo.toml plexus-common/src/errors/mod.rs plexus-common/src/errors/workspace.rs plexus-server/src/workspace plexus-server/src/lib.rs plexus-server/src/app.rs plexus-server/tests/support/mod.rs plexus-server/tests/m1d_workspace_fs.rs
git commit -m "feat: add workspace fs foundation"
```

---

## Task 2: Workspace REST Routes

**Files:**
- Create: `plexus-server/src/routes/workspace.rs`
- Modify: `plexus-server/src/routes/mod.rs`
- Modify: `plexus-server/src/error.rs`
- Modify: `plexus-server/tests/support/mod.rs`
- Create: `plexus-server/tests/m1d_workspace_rest.rs`

- [ ] **Step 1: Add raw request helpers**

In `plexus-server/tests/support/mod.rs`, add:

```rust
pub async fn bytes_request(
    app: axum::Router,
    method: Method,
    path: &str,
    bytes: impl Into<Vec<u8>>,
    content_type: &str,
    auth: Option<&str>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, content_type);
    if let Some(token) = auth {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = app
        .oneshot(builder.body(Body::from(bytes.into())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, headers, bytes)
}

pub async fn empty_request(
    app: axum::Router,
    method: Method,
    path: &str,
    auth: Option<&str>,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(token) = auth {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = app.oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, bytes)
}
```

- [ ] **Step 2: Write failing REST tests**

Create `plexus-server/tests/m1d_workspace_rest.rs`:

```rust
mod support;

use axum::http::{Method, StatusCode};
use serde_json::json;
use support::{TestApp, bytes_request, empty_request, json_request, register_user};

async fn set_quota(app: &TestApp, quota: i64) {
    sqlx::query(
        "INSERT INTO system_config (key, value, updated_at)
         VALUES ('quota_bytes', $1, NOW())
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(json!(quota))
    .execute(&app.pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn file_routes_require_explicit_server_device() {
    let app = TestApp::spawn().await;
    let (token, _) = register_user(&app, "alice@example.com").await;

    let (status, _) = empty_request(
        app.router.clone(),
        Method::GET,
        "/api/workspace/files/notes.txt",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = empty_request(
        app.router.clone(),
        Method::GET,
        "/api/workspace/files/notes.txt?plexus_device=devbox",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn put_get_delete_file_round_trip() {
    let app = TestApp::spawn().await;
    let (token, _) = register_user(&app, "alice@example.com").await;
    set_quota(&app, 10_000).await;

    let (status, _, _) = bytes_request(
        app.router.clone(),
        Method::PUT,
        "/api/workspace/files/.attachments/uploads/abc/cat.txt?plexus_device=server",
        b"cat",
        "application/octet-stream",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _, body) = bytes_request(
        app.router.clone(),
        Method::GET,
        "/api/workspace/files/.attachments/uploads/abc/cat.txt?plexus_device=server",
        Vec::new(),
        "application/octet-stream",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"cat");

    let (status, _) = empty_request(
        app.router.clone(),
        Method::DELETE,
        "/api/workspace/files/.attachments/uploads/abc/cat.txt?plexus_device=server",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn quota_route_reports_server_workspace_usage() {
    let app = TestApp::spawn().await;
    let (token, _) = register_user(&app, "alice@example.com").await;
    set_quota(&app, 10_000).await;

    let (status, body) = json_request(
        app.router.clone(),
        Method::GET,
        "/api/workspace/quota",
        json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["quota_bytes"], json!(10_000));
    assert_eq!(body["locked"], json!(false));
}
```

- [ ] **Step 3: Run failing REST tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_workspace_rest
```

Expected: FAIL because workspace routes are not mounted.

- [ ] **Step 4: Map workspace errors to API responses**

In `plexus-server/src/error.rs`, add:

```rust
impl From<plexus_common::WorkspaceError> for ApiError {
    fn from(err: plexus_common::WorkspaceError) -> Self {
        use axum::http::StatusCode;
        use plexus_common::{Code, WorkspaceError};
        let status = match &err {
            WorkspaceError::NotFound(_) => StatusCode::NOT_FOUND,
            WorkspaceError::PathOutsideWorkspace(_) => StatusCode::FORBIDDEN,
            WorkspaceError::SoftLocked | WorkspaceError::UploadTooLarge { .. } => {
                StatusCode::CONFLICT
            }
            WorkspaceError::QuotaNotConfigured => StatusCode::BAD_REQUEST,
            WorkspaceError::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError::new(status, err.code(), err.to_string())
    }
}
```

- [ ] **Step 5: Implement route module**

Create `plexus-server/src/routes/workspace.rs`:

```rust
use crate::{app::AppState, auth::AuthUser, error::ApiError};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct DeviceQuery {
    plexus_device: Option<String>,
}

fn require_server(query: &DeviceQuery) -> Result<(), ApiError> {
    match query.plexus_device.as_deref() {
        Some("server") => Ok(()),
        Some(_) => Err(ApiError::invalid_args("M1d only supports plexus_device=server")),
        None => Err(ApiError::invalid_args("plexus_device is required")),
    }
}

pub async fn quota(
    auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<crate::workspace::QuotaState>, ApiError> {
    Ok(Json(state.workspace_fs().quota(auth.user.id).await?))
}

pub async fn get_file(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<DeviceQuery>,
) -> Result<impl IntoResponse, ApiError> {
    require_server(&query)?;
    let bytes = state.workspace_fs().read_file(auth.user.id, &path).await?;
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());
    Ok((headers, bytes))
}

pub async fn put_file(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<DeviceQuery>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    require_server(&query)?;
    state.workspace_fs().write_file(auth.user.id, &path, body.to_vec()).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_file(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<DeviceQuery>,
) -> Result<StatusCode, ApiError> {
    require_server(&query)?;
    state.workspace_fs().delete_file(auth.user.id, &path).await?;
    Ok(StatusCode::NO_CONTENT)
}
```

- [ ] **Step 6: Mount routes**

In `plexus-server/src/routes/mod.rs`, import `delete`, `put`, and module:

```rust
use axum::{
    Router,
    routing::{delete, get, post, put},
};

pub mod workspace;
```

Add routes:

```rust
.route("/api/workspace/quota", get(workspace::quota))
.route(
    "/api/workspace/files/{*path}",
    get(workspace::get_file)
        .put(workspace::put_file)
        .delete(workspace::delete_file),
)
```

- [ ] **Step 7: Run REST tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_workspace_rest
```

Expected: PASS.

- [ ] **Step 8: Commit REST routes**

```bash
git add plexus-server/src/routes/workspace.rs plexus-server/src/routes/mod.rs plexus-server/src/error.rs plexus-server/tests/support/mod.rs plexus-server/tests/m1d_workspace_rest.rs
git commit -m "feat: add workspace file REST routes"
```

---

## Task 3: Strict Message Request Contract

**Files:**
- Modify: `plexus-server/src/chat/content.rs`
- Modify: `plexus-server/src/routes/sessions.rs`
- Modify: `plexus-server/tests/m1c_messages.rs`
- Create: `plexus-server/tests/m1d_message_contract.rs`

- [ ] **Step 1: Write strict request tests**

Create `plexus-server/tests/m1d_message_contract.rs` with these tests first:

```rust
mod support;

use axum::http::{Method, StatusCode};
use serde_json::json;
use support::{TestApp, json_request};

async fn register_and_create_session(app: &TestApp) -> (String, String) {
    let (token, _) = support::register_user(app, "alice@example.com").await;
    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        "/api/sessions",
        json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    (token, body["id"].as_str().unwrap().to_string())
}

#[tokio::test]
async fn message_requires_content_and_attachments_arrays() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;

    for body in [
        json!({"content": []}),
        json!({"attachments": []}),
        json!({"content": "hello", "attachments": []}),
        json!({"content": [], "attachments": [], "extra": true}),
    ] {
        let (status, _) = json_request(
            app.router.clone(),
            Method::POST,
            &format!("/api/sessions/{session_id}/messages"),
            body,
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn rejects_message_when_both_arrays_are_empty() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;

    let (status, _) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({"reasoning_effort": null, "content": [], "attachments": []}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn accepts_text_and_direct_inline_image_with_empty_attachments() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;

    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({
            "reasoning_effort": null,
            "content": [
                {"type": "text", "text": "describe"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8="}}
            ],
            "attachments": []
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body["message_id"].as_str().is_some());
}

#[tokio::test]
async fn rejects_external_direct_image_url() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;

    let (status, _) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({
            "content": [
                {"type": "image_url", "image_url": {"url": "https://example.com/cat.png"}}
            ],
            "attachments": []
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 2: Run failing message contract tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_message_contract
```

Expected: FAIL because string shorthand, missing arrays, and empty messages are still accepted.

- [ ] **Step 3: Replace content normalization with strict parsing**

In `plexus-server/src/chat/content.rs`, replace `normalize_user_content` with:

```rust
use crate::error::ApiError;
use base64::{Engine as _, engine::general_purpose::STANDARD};
pub use plexus_common::{ContentBlock, ImageUrlBlock};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn parse_content_array(raw: &Value) -> Result<Vec<ContentBlock>, ApiError> {
    let Value::Array(values) = raw else {
        return Err(ApiError::invalid_args("content must be an array"));
    };
    values.iter().cloned().map(parse_block).collect()
}

fn parse_block(value: Value) -> Result<ContentBlock, ApiError> {
    let block: ContentBlock = serde_json::from_value(value)
        .map_err(|_| ApiError::invalid_args("content block is malformed"))?;
    if let ContentBlock::ImageUrl { image_url } = &block {
        decode_data_image_url(&image_url.url)?;
    }
    Ok(block)
}

pub fn decode_data_image_url(url: &str) -> Result<(String, Vec<u8>), ApiError> {
    let Some(rest) = url.strip_prefix("data:image/") else {
        return Err(ApiError::invalid_args(
            "image_url.url must be an inline data:image/...;base64 URL",
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
    {
        return Err(ApiError::invalid_args(
            "image_url.url must be a valid data:image/...;base64 URL",
        ));
    }
    let bytes = STANDARD
        .decode(data)
        .map_err(|_| ApiError::invalid_args("image_url.url base64 is invalid"))?;
    Ok((format!("image/{mime_tail}"), bytes))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
```

- [ ] **Step 4: Parse strict top-level message body**

In `plexus-server/src/routes/sessions.rs`, change the import:

```rust
chat::content::{ContentBlock, parse_content_array},
```

Add a helper:

```rust
fn required_array<'a>(body: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ApiError> {
    body.get(key)
        .ok_or_else(|| ApiError::invalid_args(format!("{key} is required")))
        .and_then(|value| {
            if value.is_array() {
                Ok(value)
            } else {
                Err(ApiError::invalid_args(format!("{key} must be an array")))
            }
        })
}

fn reject_unknown_message_fields(body: &Map<String, Value>) -> Result<(), ApiError> {
    for key in body.keys() {
        if !matches!(key.as_str(), "reasoning_effort" | "content" | "attachments") {
            return Err(ApiError::invalid_args(format!("unsupported message field: {key}")));
        }
    }
    Ok(())
}
```

In `post_message`, before persistence:

```rust
reject_unknown_message_fields(&body)?;
let content_value = required_array(&body, "content")?;
let attachments_value = required_array(&body, "attachments")?;
let user_content = parse_content_array(content_value)?;
let attachments_len = attachments_value.as_array().unwrap().len();
if user_content.is_empty() && attachments_len == 0 {
    return Err(ApiError::invalid_args("content and attachments cannot both be empty"));
}

let mut content = vec![runtime_block(&session)];
content.extend(user_content);
```

This step only validates that `attachments` is an array. Task 4 parses and expands attachment refs.

- [ ] **Step 5: Update legacy M1c tests**

In `plexus-server/tests/m1c_messages.rs`, replace `post_empty_forms_are_accepted` with:

```rust
#[tokio::test]
async fn m1d_rejects_legacy_empty_and_string_forms() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;
    for body in [
        json!({"reasoning_effort": "none"}),
        json!({"content": "", "attachments": []}),
        json!({"content": [], "attachments": []}),
    ] {
        let (status, _) = json_request(
            app.router.clone(),
            Method::POST,
            &format!("/api/sessions/{session_id}/messages"),
            body,
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
```

Update all remaining successful message posts in M1c tests to include:

```json
"attachments": []
```

- [ ] **Step 6: Run message tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_message_contract
rtk cargo test -p plexus-server --test m1c_messages
```

Expected: PASS.

- [ ] **Step 7: Commit strict message contract**

```bash
git add plexus-server/src/chat/content.rs plexus-server/src/routes/sessions.rs plexus-server/tests/m1c_messages.rs plexus-server/tests/m1d_message_contract.rs
git commit -m "feat: enforce strict M1d message shape"
```

---

## Task 4: Attachment Expansion and Image Dedupe

**Files:**
- Create: `plexus-server/src/chat/attachments.rs`
- Modify: `plexus-server/src/chat/mod.rs`
- Modify: `plexus-server/src/routes/sessions.rs`
- Modify: `plexus-server/tests/m1d_message_contract.rs`

- [ ] **Step 1: Add attachment expansion tests**

Append to `plexus-server/tests/m1d_message_contract.rs`:

```rust
async fn set_quota(app: &TestApp, quota: i64) {
    sqlx::query(
        "INSERT INTO system_config (key, value, updated_at)
         VALUES ('quota_bytes', $1, NOW())
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(json!(quota))
    .execute(&app.pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_attachment_device_or_file_rejects_whole_message() {
    let app = TestApp::spawn().await;
    let (token, session_id) = register_and_create_session(&app).await;
    set_quota(&app, 10_000).await;

    for body in [
        json!({
            "content": [],
            "attachments": [{"path": ".attachments/uploads/a/cat.png"}]
        }),
        json!({
            "content": [],
            "attachments": [{"plexus_device": "devbox", "path": ".attachments/uploads/a/cat.png"}]
        }),
    ] {
        let (status, _) = json_request(
            app.router.clone(),
            Method::POST,
            &format!("/api/sessions/{session_id}/messages"),
            body,
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    let (status, _) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({
            "content": [],
            "attachments": [{"plexus_device": "server", "path": ".attachments/uploads/a/missing.png"}]
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn non_image_attachment_adds_marker_only_before_user_content() {
    let app = TestApp::spawn().await;
    let (token, user_id) = support::register_user(&app, "alice@example.com").await;
    let (status, session) = json_request(
        app.router.clone(),
        Method::POST,
        "/api/sessions",
        json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let session_id = session["id"].as_str().unwrap();
    set_quota(&app, 10_000).await;
    app.state
        .workspace_fs()
        .write_file(
            user_id,
            ".attachments/uploads/a/readme.txt",
            b"plain text".to_vec(),
        )
        .await
        .unwrap();

    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({
            "content": [{"type": "text", "text": "read this"}],
            "attachments": [{"plexus_device": "server", "path": ".attachments/uploads/a/readme.txt"}]
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let message_id = uuid::Uuid::parse_str(body["message_id"].as_str().unwrap()).unwrap();
    let stored: (serde_json::Value,) =
        sqlx::query_as("SELECT content FROM messages WHERE id = $1")
            .bind(message_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let blocks = stored.0.as_array().unwrap();
    assert!(blocks[1]["text"].as_str().unwrap().contains(".attachments/uploads/a/readme.txt"));
    assert_eq!(blocks[2], json!({"type": "text", "text": "read this"}));
    assert_eq!(blocks.iter().filter(|b| b["type"] == "image_url").count(), 0);
}

#[tokio::test]
async fn image_attachment_adds_marker_then_generated_image_before_user_content() {
    let app = TestApp::spawn().await;
    let (token, user_id) = support::register_user(&app, "alice@example.com").await;
    let (status, session) = json_request(
        app.router.clone(),
        Method::POST,
        "/api/sessions",
        json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let session_id = session["id"].as_str().unwrap();
    set_quota(&app, 10_000).await;
    app.state
        .workspace_fs()
        .write_file(user_id, ".attachments/uploads/a/cat.png", png_bytes())
        .await
        .unwrap();

    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({
            "content": [{"type": "text", "text": "describe"}],
            "attachments": [{"plexus_device": "server", "path": ".attachments/uploads/a/cat.png"}]
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let message_id = uuid::Uuid::parse_str(body["message_id"].as_str().unwrap()).unwrap();
    let stored: (serde_json::Value,) =
        sqlx::query_as("SELECT content FROM messages WHERE id = $1")
            .bind(message_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let blocks = stored.0.as_array().unwrap();
    assert!(blocks[1]["text"].as_str().unwrap().contains("cat.png"));
    assert_eq!(blocks[2]["type"], "image_url");
    assert_eq!(blocks[3], json!({"type": "text", "text": "describe"}));
}

#[tokio::test]
async fn duplicate_direct_image_gets_marker_inserted_before_existing_image() {
    let app = TestApp::spawn().await;
    let (token, user_id) = support::register_user(&app, "alice@example.com").await;
    let (status, session) = json_request(
        app.router.clone(),
        Method::POST,
        "/api/sessions",
        json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let session_id = session["id"].as_str().unwrap();
    set_quota(&app, 10_000).await;
    app.state
        .workspace_fs()
        .write_file(user_id, ".attachments/uploads/a/cat.png", png_bytes())
        .await
        .unwrap();

    let data_url = format!("data:image/png;base64,{}", base64::engine::general_purpose::STANDARD.encode(png_bytes()));
    let (status, body) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({
            "content": [
                {"type": "text", "text": "describe"},
                {"type": "image_url", "image_url": {"url": data_url}}
            ],
            "attachments": [{"plexus_device": "server", "path": ".attachments/uploads/a/cat.png"}]
        }),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let message_id = uuid::Uuid::parse_str(body["message_id"].as_str().unwrap()).unwrap();
    let stored: (serde_json::Value,) =
        sqlx::query_as("SELECT content FROM messages WHERE id = $1")
            .bind(message_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    let blocks = stored.0.as_array().unwrap();
    assert_eq!(blocks[1], json!({"type": "text", "text": "describe"}));
    assert!(blocks[2]["text"].as_str().unwrap().contains("cat.png"));
    assert_eq!(blocks[3]["type"], "image_url");
    assert_eq!(blocks.iter().filter(|b| b["type"] == "image_url").count(), 1);
}

fn png_bytes() -> Vec<u8> {
    vec![
        0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n',
        0, 0, 0, 0, b'I', b'E', b'N', b'D',
    ]
}
```

Add `use base64::Engine as _;` at the top of this test file.

- [ ] **Step 2: Run failing attachment tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_message_contract
```

Expected: FAIL because attachment parsing and expansion are not implemented.

- [ ] **Step 3: Create attachment assembler**

Create `plexus-server/src/chat/attachments.rs`:

```rust
use crate::{
    chat::content::{decode_data_image_url, sha256_hex},
    error::ApiError,
    workspace::WorkspaceFs,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use plexus_common::{ContentBlock, ImageUrlBlock};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentRef {
    pub plexus_device: String,
    pub path: String,
}

pub async fn assemble_user_content(
    workspace: &WorkspaceFs,
    user_id: Uuid,
    user_content: Vec<ContentBlock>,
    raw_attachments: &Value,
) -> Result<Vec<ContentBlock>, ApiError> {
    let attachments = parse_attachments(raw_attachments)?;
    let direct_hashes = direct_image_hashes(&user_content)?;
    let mut prefix = Vec::new();
    let mut markers_before_direct: BTreeMap<usize, Vec<ContentBlock>> = BTreeMap::new();

    for attachment in attachments {
        if attachment.plexus_device != "server" {
            return Err(ApiError::invalid_args("M1d only supports plexus_device=server"));
        }
        let bytes = workspace.read_file(user_id, &attachment.path).await?;
        let marker = ContentBlock::text(format!(
            "User uploaded file to device='server', path='{}'",
            attachment.path
        ));
        if let Some(mime) = sniff_image_mime(&bytes) {
            let hash = sha256_hex(&bytes);
            if let Some(index) = direct_hashes.iter().find_map(|(idx, h)| (*h == hash).then_some(*idx)) {
                markers_before_direct.entry(index).or_default().push(marker);
            } else {
                prefix.push(marker);
                prefix.push(ContentBlock::ImageUrl {
                    image_url: ImageUrlBlock {
                        url: format!("data:{mime};base64,{}", STANDARD.encode(&bytes)),
                    },
                });
            }
        } else {
            prefix.push(marker);
        }
    }

    let mut out = prefix;
    for (index, block) in user_content.into_iter().enumerate() {
        if let Some(markers) = markers_before_direct.remove(&index) {
            out.extend(markers);
        }
        out.push(block);
    }
    Ok(out)
}

fn parse_attachments(value: &Value) -> Result<Vec<AttachmentRef>, ApiError> {
    let Value::Array(items) = value else {
        return Err(ApiError::invalid_args("attachments must be an array"));
    };
    items
        .iter()
        .cloned()
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|_| ApiError::invalid_args("attachment block is malformed"))
        })
        .collect()
}

fn direct_image_hashes(blocks: &[ContentBlock]) -> Result<Vec<(usize, String)>, ApiError> {
    let mut hashes = Vec::new();
    for (index, block) in blocks.iter().enumerate() {
        if let ContentBlock::ImageUrl { image_url } = block {
            let (_, bytes) = decode_data_image_url(&image_url.url)?;
            hashes.push((index, sha256_hex(&bytes)));
        }
    }
    Ok(hashes)
}

fn sniff_image_mime(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n']) {
        Some("image/png")
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        Some("image/webp")
    } else {
        None
    }
}
```

- [ ] **Step 4: Export attachment module**

In `plexus-server/src/chat/mod.rs`, add:

```rust
pub mod attachments;
```

- [ ] **Step 5: Use assembler in message route**

In `plexus-server/src/routes/sessions.rs`, import:

```rust
chat::attachments::assemble_user_content,
```

Replace the simple content extension with:

```rust
let user_content = parse_content_array(content_value)?;
let attachments_len = attachments_value.as_array().unwrap().len();
if user_content.is_empty() && attachments_len == 0 {
    return Err(ApiError::invalid_args("content and attachments cannot both be empty"));
}
let assembled = assemble_user_content(
    state.workspace_fs(),
    auth.user.id,
    user_content,
    attachments_value,
)
.await?;

let mut content = vec![runtime_block(&session)];
content.extend(assembled);
```

- [ ] **Step 6: Run attachment tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_message_contract
```

Expected: PASS.

- [ ] **Step 7: Commit attachment expansion**

```bash
git add plexus-server/src/chat/attachments.rs plexus-server/src/chat/mod.rs plexus-server/src/routes/sessions.rs plexus-server/tests/support/mod.rs plexus-server/tests/m1d_message_contract.rs
git commit -m "feat: expand workspace attachments in messages"
```

---

## Task 5: Workspace Edit/List/Glob/Grep Operations and REST

**Files:**
- Modify: `plexus-server/src/workspace/fs.rs`
- Modify: `plexus-server/src/routes/workspace.rs`
- Modify: `plexus-server/src/routes/mod.rs`
- Create or extend: `plexus-server/tests/m1d_workspace_rest.rs`

- [ ] **Step 1: Add failing REST coverage for list, edit, folder delete, glob, and grep**

Append to `plexus-server/tests/m1d_workspace_rest.rs`:

```rust
#[tokio::test]
async fn edit_list_glob_grep_and_folder_delete_work() {
    let app = TestApp::spawn().await;
    let (token, _) = register_user(&app, "alice@example.com").await;
    set_quota(&app, 10_000).await;

    let (status, _, _) = bytes_request(
        app.router.clone(),
        Method::PUT,
        "/api/workspace/files/docs/a.txt?plexus_device=server",
        b"hello world",
        "application/octet-stream",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = json_request(
        app.router.clone(),
        Method::PATCH,
        "/api/workspace/files/docs/a.txt?plexus_device=server",
        json!({"old_text": "world", "new_text": "plexus", "replace_all": false}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["replacements"], json!(1));

    let (status, body) = json_request(
        app.router.clone(),
        Method::GET,
        "/api/workspace/list/docs?plexus_device=server",
        json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap()[0]["name"], json!("a.txt"));

    let (status, body) = json_request(
        app.router.clone(),
        Method::GET,
        "/api/workspace/glob?plexus_device=server&pattern=docs/*.txt",
        json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap()[0], json!("docs/a.txt"));

    let (status, body) = json_request(
        app.router.clone(),
        Method::GET,
        "/api/workspace/grep?plexus_device=server&pattern=plexus&path=docs",
        json!({}),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.to_string().contains("hello plexus"));

    let (status, _) = empty_request(
        app.router.clone(),
        Method::DELETE,
        "/api/workspace/folders/docs?plexus_device=server",
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_workspace_rest edit_list_glob_grep_and_folder_delete_work
```

Expected: FAIL because routes and methods are missing.

- [ ] **Step 3: Extend `WorkspaceFs`**

Add these public methods to `plexus-server/src/workspace/fs.rs`:

```rust
pub async fn edit_file(
    &self,
    user_id: Uuid,
    path: &str,
    old_text: &str,
    new_text: &str,
    replace_all: bool,
) -> Result<usize, WorkspaceError> {
    let bytes = self.read_file(user_id, path).await?;
    let text = String::from_utf8(bytes)
        .map_err(|err| WorkspaceError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, err)))?;
    let replacements = if replace_all {
        text.matches(old_text).count()
    } else if text.contains(old_text) {
        1
    } else {
        0
    };
    if replacements == 0 {
        return Err(WorkspaceError::NotFound(PathBuf::from(path)));
    }
    let edited = if replace_all {
        text.replace(old_text, new_text)
    } else {
        text.replacen(old_text, new_text, 1)
    };
    self.write_file(user_id, path, edited.into_bytes()).await?;
    Ok(replacements)
}

pub async fn delete_folder(&self, user_id: Uuid, path: &str) -> Result<(), WorkspaceError> {
    let full = self.resolve_existing(user_id, path).await?;
    fs::remove_dir_all(full).await?;
    Ok(())
}

pub async fn list_dir(&self, user_id: Uuid, path: &str) -> Result<Vec<DirEntry>, WorkspaceError> {
    let full = self.resolve_existing(user_id, path).await?;
    let root = self.personal_root(user_id).canonicalize().map_err(|_| WorkspaceError::NotFound(self.personal_root(user_id)))?;
    let mut out = Vec::new();
    let mut entries = fs::read_dir(&full).await?;
    while let Some(entry) = entries.next_entry().await? {
        let meta = entry.metadata().await?;
        let path = entry.path();
        let rel = path.strip_prefix(&root).unwrap_or(&path).to_string_lossy().replace('\\', "/");
        out.push(DirEntry {
            name: entry.file_name().to_string_lossy().to_string(),
            path: rel,
            kind: if meta.is_dir() { "directory" } else { "file" }.to_string(),
            size: meta.len(),
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}
```

Add simple recursive `glob` and `grep` methods:

```rust
pub async fn glob(&self, user_id: Uuid, pattern: &str) -> Result<Vec<String>, WorkspaceError> {
    let root = self.personal_root(user_id);
    fs::create_dir_all(&root).await?;
    let matcher = globset::Glob::new(pattern)
        .map_err(|err| WorkspaceError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidInput, err)))?
        .compile_matcher();
    let mut files = Vec::new();
    collect_files(&root, &root, &mut files).await?;
    let mut out: Vec<String> = files
        .into_iter()
        .filter(|path| matcher.is_match(path))
        .collect();
    out.sort();
    Ok(out)
}

pub async fn grep(
    &self,
    user_id: Uuid,
    pattern: &str,
    path: Option<&str>,
) -> Result<Vec<String>, WorkspaceError> {
    let regex = regex::Regex::new(pattern)
        .map_err(|err| WorkspaceError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidInput, err)))?;
    let root = self.personal_root(user_id);
    let search_root = match path {
        Some(path) => self.resolve_existing(user_id, path).await?,
        None => root.clone(),
    };
    let mut files = Vec::new();
    collect_files(&root, &search_root, &mut files).await?;
    let mut out = Vec::new();
    for rel in files {
        let full = root.join(&rel);
        let Ok(text) = fs::read_to_string(&full).await else {
            continue;
        };
        for (index, line) in text.lines().enumerate() {
            if regex.is_match(line) {
                out.push(format!("{}:{}:{}", rel, index + 1, line));
            }
        }
    }
    Ok(out)
}
```

Add the helper:

```rust
async fn collect_files(root: &Path, start: &Path, out: &mut Vec<String>) -> Result<(), WorkspaceError> {
    let mut stack = vec![start.to_path_buf()];
    while let Some(dir_or_file) = stack.pop() {
        let meta = fs::metadata(&dir_or_file).await?;
        if meta.is_file() {
            let rel = dir_or_file
                .strip_prefix(root)
                .unwrap_or(&dir_or_file)
                .to_string_lossy()
                .replace('\\', "/");
            out.push(rel);
        } else if meta.is_dir() {
            let mut entries = fs::read_dir(&dir_or_file).await?;
            while let Some(entry) = entries.next_entry().await? {
                stack.push(entry.path());
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Add route handlers**

In `plexus-server/src/routes/workspace.rs`, add request/query structs:

```rust
#[derive(Deserialize)]
pub struct EditRequest {
    old_text: String,
    new_text: String,
    #[serde(default)]
    replace_all: bool,
}

#[derive(Deserialize)]
pub struct GlobQuery {
    plexus_device: Option<String>,
    pattern: String,
}

#[derive(Deserialize)]
pub struct GrepQuery {
    plexus_device: Option<String>,
    pattern: String,
    path: Option<String>,
}
```

Add handlers:

```rust
pub async fn patch_file(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<DeviceQuery>,
    Json(req): Json<EditRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_server(&query)?;
    let replacements = state
        .workspace_fs()
        .edit_file(auth.user.id, &path, &req.old_text, &req.new_text, req.replace_all)
        .await?;
    Ok(Json(serde_json::json!({ "replacements": replacements })))
}

pub async fn delete_folder(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<DeviceQuery>,
) -> Result<StatusCode, ApiError> {
    require_server(&query)?;
    state.workspace_fs().delete_folder(auth.user.id, &path).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_dir(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<DeviceQuery>,
) -> Result<Json<Vec<crate::workspace::DirEntry>>, ApiError> {
    require_server(&query)?;
    Ok(Json(state.workspace_fs().list_dir(auth.user.id, &path).await?))
}

pub async fn glob(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(query): Query<GlobQuery>,
) -> Result<Json<Vec<String>>, ApiError> {
    require_server(&DeviceQuery { plexus_device: query.plexus_device })?;
    Ok(Json(state.workspace_fs().glob(auth.user.id, &query.pattern).await?))
}

pub async fn grep(
    auth: AuthUser,
    State(state): State<AppState>,
    Query(query): Query<GrepQuery>,
) -> Result<Json<Vec<String>>, ApiError> {
    require_server(&DeviceQuery { plexus_device: query.plexus_device })?;
    Ok(Json(
        state
            .workspace_fs()
            .grep(auth.user.id, &query.pattern, query.path.as_deref())
            .await?,
    ))
}
```

- [ ] **Step 5: Mount new routes**

In `plexus-server/src/routes/mod.rs`, update workspace routes:

```rust
.route(
    "/api/workspace/files/{*path}",
    get(workspace::get_file)
        .put(workspace::put_file)
        .patch(workspace::patch_file)
        .delete(workspace::delete_file),
)
.route(
    "/api/workspace/folders/{*path}",
    delete(workspace::delete_folder),
)
.route("/api/workspace/list/{*path}", get(workspace::list_dir))
.route("/api/workspace/glob", get(workspace::glob))
.route("/api/workspace/grep", get(workspace::grep))
```

- [ ] **Step 6: Run REST tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_workspace_rest
```

Expected: PASS.

- [ ] **Step 7: Commit full REST operations**

```bash
git add plexus-server/src/workspace/fs.rs plexus-server/src/routes/workspace.rs plexus-server/src/routes/mod.rs plexus-server/tests/m1d_workspace_rest.rs
git commit -m "feat: complete workspace file operations"
```

---

## Task 6: Tool Registry Merge V0

**Files:**
- Create: `plexus-server/src/tools/mod.rs`
- Create: `plexus-server/src/tools/registry.rs`
- Modify: `plexus-server/src/lib.rs`
- Create: `plexus-server/tests/m1d_tools.rs`

- [ ] **Step 1: Write failing registry tests**

Create `plexus-server/tests/m1d_tools.rs`:

```rust
use plexus_common::tools::schemas::{READ_FILE_SCHEMA, WRITE_FILE_SCHEMA};
use plexus_server::tools::registry::merged_file_tool_schemas;

#[test]
fn shared_file_source_schemas_do_not_contain_plexus_device() {
    for schema in [&*READ_FILE_SCHEMA, &*WRITE_FILE_SCHEMA] {
        let props = schema["input_schema"]["properties"].as_object().unwrap();
        assert!(!props.contains_key("plexus_device"));
    }
}

#[test]
fn merge_v0_injects_required_server_device() {
    let schemas = merged_file_tool_schemas();
    let read_file = schemas
        .iter()
        .find(|schema| schema["name"] == "read_file")
        .unwrap();
    let input = &read_file["input_schema"];
    assert_eq!(
        input["properties"]["plexus_device"]["enum"],
        serde_json::json!(["server"])
    );
    assert!(input["required"].as_array().unwrap().contains(&serde_json::json!("plexus_device")));
}
```

- [ ] **Step 2: Run failing registry tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_tools merge_v0_injects_required_server_device
```

Expected: FAIL because `plexus_server::tools` does not exist.

- [ ] **Step 3: Add tools module**

Create `plexus-server/src/tools/mod.rs`:

```rust
pub mod registry;
```

In `plexus-server/src/lib.rs`, add:

```rust
pub mod tools;
```

- [ ] **Step 4: Implement merge v0**

Create `plexus-server/src/tools/registry.rs`:

```rust
use plexus_common::tools::schemas::{
    DELETE_FILE_SCHEMA, DELETE_FOLDER_SCHEMA, EDIT_FILE_SCHEMA, GLOB_SCHEMA, GREP_SCHEMA,
    LIST_DIR_SCHEMA, READ_FILE_SCHEMA, WRITE_FILE_SCHEMA,
};
use serde_json::{Value, json};

pub const SERVER_DEVICE: &str = "server";

pub fn merged_file_tool_schemas() -> Vec<Value> {
    [
        &*READ_FILE_SCHEMA,
        &*WRITE_FILE_SCHEMA,
        &*EDIT_FILE_SCHEMA,
        &*DELETE_FILE_SCHEMA,
        &*DELETE_FOLDER_SCHEMA,
        &*LIST_DIR_SCHEMA,
        &*GLOB_SCHEMA,
        &*GREP_SCHEMA,
    ]
    .into_iter()
    .map(inject_server_device)
    .collect()
}

fn inject_server_device(schema: &Value) -> Value {
    let mut schema = schema.clone();
    let input = schema
        .get_mut("input_schema")
        .and_then(Value::as_object_mut)
        .expect("tool schema has object input_schema");
    input
        .entry("properties")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .unwrap()
        .insert(
            "plexus_device".to_string(),
            json!({
                "type": "string",
                "enum": ["server"],
                "description": "Which install site to execute on.",
                "x-plexus-device": true
            }),
        );
    let required = input.entry("required").or_insert_with(|| json!([]));
    let required = required.as_array_mut().unwrap();
    if !required.iter().any(|value| value == "plexus_device") {
        required.push(json!("plexus_device"));
    }
    schema
}
```

- [ ] **Step 5: Run registry tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_tools
```

Expected: PASS.

- [ ] **Step 6: Commit merge v0**

```bash
git add plexus-server/src/tools plexus-server/src/lib.rs plexus-server/tests/m1d_tools.rs
git commit -m "feat: add file tool schema merge v0"
```

---

## Task 7: Server File Tool Operations

**Files:**
- Create: `plexus-server/src/tools/file_ops.rs`
- Modify: `plexus-server/src/tools/mod.rs`
- Modify: `plexus-server/src/tools/registry.rs`
- Modify: `plexus-server/tests/m1d_tools.rs`

- [ ] **Step 1: Add failing file tool tests**

Append to `plexus-server/tests/m1d_tools.rs`:

```rust
mod support;

use plexus_common::{Code, ErrorCode};
use plexus_server::tools::registry::FileToolRegistry;
use serde_json::json;

async fn set_quota(app: &support::TestApp, quota: i64) {
    sqlx::query(
        "INSERT INTO system_config (key, value, updated_at)
         VALUES ('quota_bytes', $1, NOW())
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(json!(quota))
    .execute(&app.pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn file_tool_registry_rejects_missing_or_non_server_device() {
    let app = support::TestApp::spawn().await;
    let (_, user_id) = support::register_user(&app, "alice@example.com").await;
    let registry = FileToolRegistry::new(app.state.workspace_fs().clone());

    let err = registry
        .call(user_id, "read_file", json!({"path": "a.txt"}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgs);

    let err = registry
        .call(user_id, "read_file", json!({"plexus_device": "devbox", "path": "a.txt"}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::InvalidArgs);
}

#[tokio::test]
async fn write_read_edit_list_and_delete_file_tools_use_workspace_fs() {
    let app = support::TestApp::spawn().await;
    let (_, user_id) = support::register_user(&app, "alice@example.com").await;
    set_quota(&app, 10_000).await;
    let registry = FileToolRegistry::new(app.state.workspace_fs().clone());

    registry
        .call(
            user_id,
            "write_file",
            json!({"plexus_device": "server", "path": "docs/a.txt", "content": "hello world"}),
        )
        .await
        .unwrap();

    let read = registry
        .call(
            user_id,
            "read_file",
            json!({"plexus_device": "server", "path": "docs/a.txt"}),
        )
        .await
        .unwrap();
    assert!(read.contains("hello world"));

    registry
        .call(
            user_id,
            "edit_file",
            json!({
                "plexus_device": "server",
                "path": "docs/a.txt",
                "old_text": "world",
                "new_text": "plexus"
            }),
        )
        .await
        .unwrap();

    let list = registry
        .call(user_id, "list_dir", json!({"plexus_device": "server", "path": "docs"}))
        .await
        .unwrap();
    assert!(list.contains("a.txt"));

    registry
        .call(user_id, "delete_file", json!({"plexus_device": "server", "path": "docs/a.txt"}))
        .await
        .unwrap();
}
```

- [ ] **Step 2: Run failing tool operation tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_tools write_read_edit_list_and_delete_file_tools_use_workspace_fs
```

Expected: FAIL because `FileToolRegistry` and tool execution are missing.

- [ ] **Step 3: Add file operation dispatcher**

Create `plexus-server/src/tools/file_ops.rs`:

```rust
use crate::workspace::WorkspaceFs;
use plexus_common::{Code, ToolError, WorkspaceError};
use serde_json::{Value, json};
use uuid::Uuid;

pub async fn call_file_tool(
    fs: &WorkspaceFs,
    user_id: Uuid,
    name: &str,
    args: Value,
) -> Result<String, ToolError> {
    let device = args
        .get("plexus_device")
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidArgs("plexus_device is required".to_string()))?;
    if device != "server" {
        return Err(ToolError::InvalidArgs(
            "M1d only supports plexus_device=server".to_string(),
        ));
    }
    match name {
        "read_file" => {
            let path = string_arg(&args, "path")?;
            let bytes = fs.read_file(user_id, path).await.map_err(workspace_to_tool)?;
            Ok(String::from_utf8_lossy(&bytes).to_string())
        }
        "write_file" => {
            let path = string_arg(&args, "path")?;
            let content = string_arg(&args, "content")?;
            fs.write_file(user_id, path, content.as_bytes().to_vec())
                .await
                .map_err(workspace_to_tool)?;
            Ok("written".to_string())
        }
        "edit_file" => {
            let path = string_arg(&args, "path")?;
            let old_text = string_arg(&args, "old_text")?;
            let new_text = string_arg(&args, "new_text")?;
            let replace_all = args.get("replace_all").and_then(Value::as_bool).unwrap_or(false);
            let replacements = fs
                .edit_file(user_id, path, old_text, new_text, replace_all)
                .await
                .map_err(workspace_to_tool)?;
            Ok(json!({ "replacements": replacements }).to_string())
        }
        "delete_file" => {
            let path = string_arg(&args, "path")?;
            fs.delete_file(user_id, path).await.map_err(workspace_to_tool)?;
            Ok("deleted".to_string())
        }
        "delete_folder" => {
            let path = string_arg(&args, "path")?;
            fs.delete_folder(user_id, path).await.map_err(workspace_to_tool)?;
            Ok("deleted".to_string())
        }
        "list_dir" => {
            let path = string_arg(&args, "path")?;
            let entries = fs.list_dir(user_id, path).await.map_err(workspace_to_tool)?;
            serde_json::to_string(&entries).map_err(|err| ToolError::InvalidArgs(err.to_string()))
        }
        "glob" => {
            let pattern = string_arg(&args, "pattern")?;
            let matches = fs.glob(user_id, pattern).await.map_err(workspace_to_tool)?;
            serde_json::to_string(&matches).map_err(|err| ToolError::InvalidArgs(err.to_string()))
        }
        "grep" => {
            let pattern = string_arg(&args, "pattern")?;
            let path = args.get("path").and_then(Value::as_str);
            let matches = fs.grep(user_id, pattern, path).await.map_err(workspace_to_tool)?;
            serde_json::to_string(&matches).map_err(|err| ToolError::InvalidArgs(err.to_string()))
        }
        _ => Err(ToolError::InvalidArgs(format!("unknown file tool: {name}"))),
    }
}

fn string_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidArgs(format!("{key} is required")))
}

fn workspace_to_tool(err: WorkspaceError) -> ToolError {
    ToolError::InvalidArgs(format!("{:?}: {}", err.code(), err))
}
```

- [ ] **Step 4: Wire registry to file ops**

In `plexus-server/src/tools/mod.rs`, add:

```rust
pub mod file_ops;
pub mod registry;
```

In `plexus-server/src/tools/registry.rs`, add:

```rust
use crate::{tools::file_ops, workspace::WorkspaceFs};
use plexus_common::ToolError;
use uuid::Uuid;

#[derive(Clone)]
pub struct FileToolRegistry {
    fs: WorkspaceFs,
}

impl FileToolRegistry {
    pub fn new(fs: WorkspaceFs) -> Self {
        Self { fs }
    }

    pub async fn call(&self, user_id: Uuid, name: &str, args: Value) -> Result<String, ToolError> {
        file_ops::call_file_tool(&self.fs, user_id, name, args).await
    }
}
```

- [ ] **Step 5: Run tool tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_tools
```

Expected: PASS.

- [ ] **Step 6: Commit server file tools**

```bash
git add plexus-server/src/tools plexus-server/tests/m1d_tools.rs
git commit -m "feat: add server file tool operations"
```

---

## Task 8: Docs Alignment

**Files:**
- Modify: `docs/API.yaml`
- Modify: `docs/TOOLS.md`
- Modify: `docs/DECISIONS.md`
- Modify: `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`
- Modify: `docs/reference/superpowers/specs/2026-05-18-plexus-m1d-workspace-files-attachments-design.md`

- [ ] **Step 1: Update API docs for strict message shape**

In `docs/API.yaml`, update `POST /api/sessions/{id}/messages` request schema so it requires `content` and `attachments`:

```yaml
required: [content, attachments]
properties:
  reasoning_effort:
    allOf:
      - $ref: '#/components/schemas/ReasoningEffort'
    nullable: true
  content:
    type: array
    items: { $ref: '#/components/schemas/ContentBlock' }
  attachments:
    type: array
    items: { $ref: '#/components/schemas/MessageAttachmentRef' }
additionalProperties: false
```

Add component schema:

```yaml
MessageAttachmentRef:
  type: object
  required: [plexus_device, path]
  additionalProperties: false
  properties:
    plexus_device:
      type: string
      enum: [server]
      description: Explicit file source target. M1d accepts only `server`; M1f expands this enum dynamically.
    path:
      type: string
```

- [ ] **Step 2: Update REST device parameter docs**

In `docs/API.yaml`, change `components.parameters.PlexusDevice` to required with no default:

```yaml
PlexusDevice:
  name: plexus_device
  in: query
  required: true
  schema:
    type: string
    enum: [server]
  description: Explicit target install site. M1d requires `server`; later milestones build this enum dynamically.
```

Remove text saying REST defaults to `server`.

- [ ] **Step 3: Update tools docs**

In `docs/TOOLS.md`, add an M1d note near the schema merge section:

```markdown
**M1d implementation note:** M1d implements merge v0 only. Shared file tool source schemas remain device-free, and the server registry injects required `plexus_device` with enum `["server"]`. Automatic install-site detection, client advertisements, intrinsic-device enum extension with real devices, schema collision handling across install sites, and non-server dispatch are M1f work.
```

- [ ] **Step 4: Update decisions docs**

In `docs/DECISIONS.md`, update ADR-044 and ADR-080 wording:

```markdown
**M1d browser correction:** Browser-uploaded files are first written through `/api/workspace/files/{path}?plexus_device=server`; message send references that existing path. The message API does not move or copy files into `.attachments/{msg_id}`. Channel adapters that receive raw bytes may still choose `.attachments/...` placement when they implement their own byte-ingress path.
```

```markdown
**M1d browser correction:** ADR-080 applies to ingress paths that write attachment bytes while receiving a message. M1d browser message writes only reference existing workspace files, so invalid or unreadable attachment refs reject the whole message before persistence.
```

- [ ] **Step 5: Update milestone specs**

In `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`, align M1d/M1f scope with the approved spec: M1d server-only explicit device contract, M1f automatic device detection and routing.

In `docs/reference/superpowers/specs/2026-05-18-plexus-m1d-workspace-files-attachments-design.md`, change status:

```markdown
**Status:** Implemented
```

Only do this after code and tests pass.

- [ ] **Step 6: Run docs sanity scan**

Run:

```bash
rtk rg -n "default `server`|Strings are accepted as shorthand|external HTTP\\(S\\) URL ingestion|\\.attachments/\\{msg_id\\}" docs/API.yaml docs/TOOLS.md docs/DECISIONS.md docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md docs/reference/superpowers/specs/2026-05-18-plexus-m1d-workspace-files-attachments-design.md
```

Expected: no matches that describe M1d browser behavior as current.

- [ ] **Step 7: Commit docs**

```bash
git add docs/API.yaml docs/TOOLS.md docs/DECISIONS.md docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md docs/reference/superpowers/specs/2026-05-18-plexus-m1d-workspace-files-attachments-design.md
git commit -m "docs: align M1d workspace contract"
```

---

## Task 9: Full Verification

**Files:**
- No source files expected unless verification exposes a defect.

- [ ] **Step 1: Run focused M1d tests**

Run:

```bash
rtk cargo test -p plexus-server --test m1d_workspace_fs
rtk cargo test -p plexus-server --test m1d_workspace_rest
rtk cargo test -p plexus-server --test m1d_message_contract
rtk cargo test -p plexus-server --test m1d_tools
```

Expected: all PASS.

- [ ] **Step 2: Run regression tests for touched areas**

Run:

```bash
rtk cargo test -p plexus-common
rtk cargo test -p plexus-server --test m1a_bootstrap
rtk cargo test -p plexus-server --test m1a_admin_config
rtk cargo test -p plexus-server --test m1b_openai_client
rtk cargo test -p plexus-server --test m1c_messages
rtk cargo test -p plexus-server --test m1c_sse
rtk cargo test -p plexus-server --test m1c_worker
```

Expected: all PASS.

- [ ] **Step 3: Run formatting and lint checks**

Run:

```bash
rtk cargo fmt --check
rtk cargo clippy -p plexus-common -p plexus-server --all-targets -- -D warnings
```

Expected: both PASS.

- [ ] **Step 4: Inspect final status**

Run:

```bash
rtk git status --short
rtk git log --oneline -8
```

Expected: working tree clean after the task commits; recent commits correspond to the plan tasks.

- [ ] **Step 5: Manual smoke checklist**

With the server configured and running:

```bash
rtk cargo run -p plexus-server
```

Use the existing auth/session flow to:

- register or login;
- set `quota_bytes`;
- create a session;
- upload an image with `PUT /api/workspace/files/.attachments/uploads/manual/cat.png?plexus_device=server`;
- post a message with `content: []` and an attachment ref to that image;
- confirm the persisted user message contains marker then `image_url`;
- post the same image as direct base64 plus attachment ref;
- confirm only one `image_url` exists and the marker sits immediately before it;
- upload a text file and send it as an attachment-only message;
- confirm only the marker is persisted for the text file.

- [ ] **Step 6: Final implementation commit if verification fixed defects**

If verification required fixes, commit them:

```bash
git add <changed-files>
git commit -m "fix: finish M1d verification"
```

If no fixes were needed, do not create an empty commit.
