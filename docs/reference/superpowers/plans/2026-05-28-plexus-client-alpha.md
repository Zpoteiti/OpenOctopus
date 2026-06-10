# Plexus Client Alpha Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a real env-driven `plexus-client` daemon that connects to `plexus-server`, executes client-routed tools, and proves the distributed-agent loop with file tools, one-shot exec, web_fetch, and server/client file transfer.

**Architecture:** Add a new `plexus-client` crate to the workspace. Keep client concerns split into CLI/env loading, WebSocket lifecycle, in-memory device config, FIFO worker, local tool executors, transfer slots, and shutdown coordination. Reuse `plexus-common` protocol, tool schemas, path policy, transfer header, error codes, and result block types; update server transfer plumbing only where Client Alpha needs server/client binary streaming.

**Tech Stack:** Rust 2024, Tokio, tokio-tungstenite, Axum WebSocket tests, serde/serde_json, reqwest, regex, zip, base64, sha2, uuid v7, tempfile, existing `plexus-common` and `plexus-server` helpers.

---

## File Structure

Create the client crate:

- `plexus-client/Cargo.toml` — crate dependencies and binary target.
- `plexus-client/src/main.rs` — CLI entrypoint.
- `plexus-client/src/lib.rs` — public testable API.
- `plexus-client/src/cli.rs` — subcommand and `--log-level` parsing.
- `plexus-client/src/config.rs` — env loading, secret handling, URL derivation.
- `plexus-client/src/logging.rs` — tracing subscriber setup.
- `plexus-client/src/runtime.rs` — top-level client runtime orchestration.
- `plexus-client/src/ws.rs` — WebSocket connect, frame read/write loop, reconnect decisions.
- `plexus-client/src/worker.rs` — per-device FIFO worker and result emission.
- `plexus-client/src/tools/mod.rs` — local tool registry and shared result helpers.
- `plexus-client/src/tools/fs.rs` — shared file tools against local disk.
- `plexus-client/src/tools/exec.rs` — one-shot exec runner.
- `plexus-client/src/tools/web_fetch.rs` — client web_fetch with SSRF policy.
- `plexus-client/src/transfer.rs` — client-side transfer slots and binary chunks.
- `plexus-client/tests/config_cli.rs` — CLI/env/unit tests.
- `plexus-client/tests/ws_lifecycle.rs` — local WebSocket lifecycle tests.
- `plexus-client/tests/worker.rs` — FIFO and tool_result tests.
- `plexus-client/tests/file_tools.rs` — client file tool behavior.
- `plexus-client/tests/exec.rs` — one-shot exec behavior.
- `plexus-client/tests/web_fetch.rs` — web_fetch policy tests.
- `plexus-client/tests/transfer.rs` — client transfer slot tests.

Modify shared/server files:

- `Cargo.toml` — add `plexus-client` workspace member and workspace deps needed by the client.
- `plexus-common/src/errors/mod.rs` — add `AlreadyExists` and `Sha256Mismatch` for Client Alpha error codes.
- `plexus-common/src/tools/mod.rs` — export `file_content`.
- `plexus-common/src/tools/file_content.rs` — shared image/PDF/office/text detection and result formatting.
- `plexus-server/src/tools/file_ops.rs` — reuse common file content helper.
- `plexus-server/src/devices/registry.rs` — add binary frame command and transfer result waiters.
- `plexus-server/src/devices/ws.rs` — send/receive binary transfer frames.
- `plexus-server/src/tools/file_transfer.rs` — implement `server -> client` and `client -> server`.
- `plexus-server/tests/support/device_client.rs` — binary frame helpers for integration tests.
- `plexus-server/tests/m1f_file_transfer.rs` — server/client transfer tests.
- `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md` — update Client Alpha status after verification.

Do not add a local client config directory. Do not add `logout`.

---

### Task 1: Scaffold `plexus-client` Crate and CLI Config

**Files:**
- Modify: `Cargo.toml`
- Create: `plexus-client/Cargo.toml`
- Create: `plexus-client/src/main.rs`
- Create: `plexus-client/src/lib.rs`
- Create: `plexus-client/src/cli.rs`
- Create: `plexus-client/src/config.rs`
- Create: `plexus-client/src/logging.rs`
- Test: `plexus-client/tests/config_cli.rs`

- [ ] **Step 1: Add failing CLI/env tests**

Create `plexus-client/tests/config_cli.rs`:

```rust
use plexus_client::{cli::Cli, config::StartupConfig};
use secrecy::ExposeSecret;

#[test]
fn cli_defaults_to_run() {
    let cli = Cli::parse_from(["plexus-client"]);
    assert!(cli.command.is_run());
}

#[test]
fn cli_parses_version() {
    let cli = Cli::parse_from(["plexus-client", "version"]);
    assert!(cli.command.is_version());
}

#[test]
fn cli_rejects_logout_subcommand() {
    let err = Cli::try_parse_from(["plexus-client", "logout"]).unwrap_err();
    assert!(err.to_string().contains("unrecognized subcommand"));
}

#[test]
fn startup_config_requires_server_url() {
    let err = StartupConfig::from_pairs([("PLEXUS_DEVICE_TOKEN", "plexus_dev_test")]).unwrap_err();
    assert!(err.to_string().contains("PLEXUS_SERVER_URL"));
}

#[test]
fn startup_config_requires_device_token() {
    let err = StartupConfig::from_pairs([("PLEXUS_SERVER_URL", "http://localhost:8080")]).unwrap_err();
    assert!(err.to_string().contains("PLEXUS_DEVICE_TOKEN"));
}

#[test]
fn startup_config_derives_ws_url_from_http_base() {
    let config = StartupConfig::from_pairs([
        ("PLEXUS_SERVER_URL", "http://localhost:8080"),
        ("PLEXUS_DEVICE_TOKEN", "plexus_dev_test"),
    ])
    .unwrap();
    assert_eq!(config.ws_url.as_str(), "ws://localhost:8080/ws/device");
    assert_eq!(config.device_token.expose_secret(), "plexus_dev_test");
}

#[test]
fn startup_config_rejects_path_prefixed_server_url() {
    let err = StartupConfig::from_pairs([
        ("PLEXUS_SERVER_URL", "https://example.com/plexus"),
        ("PLEXUS_DEVICE_TOKEN", "plexus_dev_test"),
    ])
    .unwrap_err();
    assert!(err.to_string().contains("path component"));
}
```

- [ ] **Step 2: Run tests to verify crate is missing**

Run:

```bash
cargo test -p plexus-client --test config_cli
```

Expected: FAIL with `package ID specification 'plexus-client' did not match any packages`.

- [ ] **Step 3: Add workspace member and client dependencies**

Modify root `Cargo.toml`:

```toml
[workspace]
members = ["plexus-common", "plexus-server", "plexus-client"]
resolver = "2"
```

Add workspace dependencies if absent:

```toml
clap = { version = "4", features = ["derive"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "time"] }
url = "2"
```

Create `plexus-client/Cargo.toml`:

```toml
[package]
name = "plexus-client"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
authors.workspace = true
repository.workspace = true

[dependencies]
plexus-common = { path = "../plexus-common" }
tokio.workspace = true
tokio-tungstenite.workspace = true
futures-util.workspace = true
serde.workspace = true
serde_json.workspace = true
secrecy.workspace = true
thiserror.workspace = true
uuid.workspace = true
url.workspace = true
clap.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

- [ ] **Step 4: Add CLI/config implementation**

Create `plexus-client/src/lib.rs`:

```rust
pub mod cli;
pub mod config;
pub mod logging;
pub mod runtime;
pub mod tools;
pub mod transfer;
pub mod worker;
pub mod ws;
```

Create `plexus-client/src/cli.rs`:

```rust
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "plexus-client")]
pub struct Cli {
    #[arg(long)]
    pub log_level: Option<String>,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
pub enum Command {
    Run,
    Version,
}

impl Cli {
    pub fn parse_from<I, T>(itr: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        <Self as Parser>::parse_from(itr)
    }

    pub fn try_parse_from<I, T>(itr: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        <Self as Parser>::try_parse_from(itr)
    }

    pub fn effective_command(&self) -> Command {
        self.command.unwrap_or(Command::Run)
    }
}

impl Command {
    pub fn is_run(self) -> bool {
        matches!(self, Self::Run)
    }

    pub fn is_version(self) -> bool {
        matches!(self, Self::Version)
    }
}
```

Create `plexus-client/src/config.rs`:

```rust
use secrecy::SecretString;
use std::{collections::HashMap, env};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required env var {0}")]
    Missing(&'static str),
    #[error("PLEXUS_SERVER_URL is invalid: {0}")]
    InvalidUrl(String),
    #[error("PLEXUS_SERVER_URL must not include a path component in v1")]
    PathComponent,
    #[error("PLEXUS_SERVER_URL must use http or https")]
    UnsupportedScheme,
}

#[derive(Clone)]
pub struct StartupConfig {
    pub server_url: Url,
    pub ws_url: Url,
    pub device_token: SecretString,
}

impl StartupConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_pairs(env::vars())
    }

    pub fn from_pairs<I, K, V>(pairs: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let map: HashMap<String, String> = pairs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let server = map
            .get("PLEXUS_SERVER_URL")
            .filter(|value| !value.is_empty())
            .ok_or(ConfigError::Missing("PLEXUS_SERVER_URL"))?;
        let token = map
            .get("PLEXUS_DEVICE_TOKEN")
            .filter(|value| !value.is_empty())
            .ok_or(ConfigError::Missing("PLEXUS_DEVICE_TOKEN"))?;
        let server_url = Url::parse(server).map_err(|err| ConfigError::InvalidUrl(err.to_string()))?;
        if server_url.path() != "/" {
            return Err(ConfigError::PathComponent);
        }
        let mut ws_url = server_url.clone();
        let ws_scheme = match server_url.scheme() {
            "http" => "ws",
            "https" => "wss",
            _ => return Err(ConfigError::UnsupportedScheme),
        };
        ws_url.set_scheme(ws_scheme).map_err(|_| ConfigError::UnsupportedScheme)?;
        ws_url.set_path("/ws/device");
        Ok(Self {
            server_url,
            ws_url,
            device_token: SecretString::from(token.clone()),
        })
    }
}
```

Create `plexus-client/src/logging.rs`:

```rust
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init(log_level: Option<&str>) {
    let fallback = log_level.unwrap_or("info");
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(fallback))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_ansi(false).with_target(false))
        .init();
}
```

Create placeholder modules that compile:

```rust
// plexus-client/src/runtime.rs
use crate::config::StartupConfig;

pub async fn run(_config: StartupConfig) -> anyhow::Result<()> {
    Ok(())
}
```

```rust
// plexus-client/src/tools/mod.rs
pub mod exec;
pub mod fs;
pub mod web_fetch;
```

```rust
// plexus-client/src/tools/exec.rs
```

```rust
// plexus-client/src/tools/fs.rs
```

```rust
// plexus-client/src/tools/web_fetch.rs
```

```rust
// plexus-client/src/transfer.rs
```

```rust
// plexus-client/src/worker.rs
```

```rust
// plexus-client/src/ws.rs
```

Add `anyhow = "1"` to workspace/client only if this placeholder uses it. Prefer replacing with a crate-local error before Task 2 if avoiding a new dependency is simpler.

Create `plexus-client/src/main.rs`:

```rust
use clap::Parser;
use plexus_client::{cli::{Cli, Command}, config::StartupConfig};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    plexus_client::logging::init(cli.log_level.as_deref());
    match cli.effective_command() {
        Command::Version => {
            println!(
                "plexus-client v{} (protocol v{})",
                env!("CARGO_PKG_VERSION"),
                plexus_common::version::PROTOCOL_VERSION
            );
        }
        Command::Run => {
            let config = match StartupConfig::from_env() {
                Ok(config) => config,
                Err(err) => {
                    eprintln!("{err}");
                    std::process::exit(2);
                }
            };
            if let Err(err) = plexus_client::runtime::run(config).await {
                eprintln!("{err}");
                std::process::exit(1);
            }
        }
    }
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p plexus-client --test config_cli
cargo check -p plexus-client
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml plexus-client
git commit -m "feat: scaffold plexus client"
```

---

### Task 2: WebSocket Lifecycle, Handshake, and Reconnect Decisions

**Files:**
- Modify: `plexus-client/Cargo.toml`
- Modify: `plexus-client/src/ws.rs`
- Modify: `plexus-client/src/runtime.rs`
- Test: `plexus-client/tests/ws_lifecycle.rs`

- [ ] **Step 1: Write failing lifecycle tests**

Create `plexus-client/tests/ws_lifecycle.rs`:

```rust
use futures_util::{SinkExt, StreamExt};
use plexus_client::{config::StartupConfig, ws::{build_hello, close_decision, CloseDecision}};
use plexus_common::protocol::WsFrame;
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[test]
fn hello_contains_protocol_version_and_caps() {
    let hello = build_hello("0.1.0");
    assert_eq!(hello.version, plexus_common::version::PROTOCOL_VERSION);
    assert!(hello.caps.exec);
    assert_eq!(hello.caps.fs, "rw");
}

#[test]
fn close_4401_exits_without_retry() {
    assert_eq!(close_decision(4401), CloseDecision::ExitAuth);
}

#[test]
fn close_4409_exits_version_mismatch() {
    assert_eq!(close_decision(4409), CloseDecision::ExitVersionMismatch);
}

#[test]
fn ordinary_close_retries() {
    assert_eq!(close_decision(1001), CloseDecision::Retry);
}

#[tokio::test]
async fn client_sends_hello_and_replies_to_ping() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let Message::Text(text) = ws.next().await.unwrap().unwrap() else {
            panic!("expected hello text");
        };
        let frame: WsFrame = serde_json::from_str(&text).unwrap();
        let WsFrame::Hello(hello) = frame else {
            panic!("expected hello");
        };
        ws.send(Message::Text(serde_json::to_string(&WsFrame::Ping(
            plexus_common::protocol::PingFrame { id: hello.id },
        )).unwrap().into())).await.unwrap();
        let Message::Text(text) = ws.next().await.unwrap().unwrap() else {
            panic!("expected pong text");
        };
        let frame: WsFrame = serde_json::from_str(&text).unwrap();
        match frame {
            WsFrame::Pong(pong) => assert_eq!(pong.id, hello.id),
            other => panic!("expected pong, got {other:?}"),
        }
    });

    let config = StartupConfig::from_pairs([
        ("PLEXUS_SERVER_URL", format!("http://{addr}")),
        ("PLEXUS_DEVICE_TOKEN", "plexus_dev_test".to_string()),
    ]).unwrap();
    plexus_client::ws::connect_once_for_test(config).await.unwrap();
    server.await.unwrap();
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p plexus-client --test ws_lifecycle
```

Expected: FAIL with unresolved `build_hello`, `CloseDecision`, and `connect_once_for_test`.

- [ ] **Step 3: Implement frame helpers and one-shot test connection**

Add dependencies to `plexus-client/Cargo.toml` if missing:

```toml
futures-util.workspace = true
tokio-tungstenite.workspace = true
```

Implement `plexus-client/src/ws.rs`:

```rust
use crate::config::StartupConfig;
use futures_util::{SinkExt, StreamExt};
use secrecy::ExposeSecret;
use thiserror::Error;
use tokio_tungstenite::{connect_async, tungstenite::{client::IntoClientRequest, http::header, Message}};
use uuid::Uuid;

use plexus_common::protocol::{HelloCaps, HelloFrame, PongFrame, WsFrame};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseDecision {
    Retry,
    ExitAuth,
    ExitVersionMismatch,
}

#[derive(Debug, Error)]
pub enum WsError {
    #[error("websocket request failed: {0}")]
    Request(String),
    #[error("websocket transport failed: {0}")]
    Transport(String),
}

pub fn close_decision(code: u16) -> CloseDecision {
    match code {
        4401 => CloseDecision::ExitAuth,
        4409 => CloseDecision::ExitVersionMismatch,
        _ => CloseDecision::Retry,
    }
}

pub fn build_hello(client_version: &str) -> HelloFrame {
    HelloFrame {
        id: Uuid::now_v7(),
        version: plexus_common::version::PROTOCOL_VERSION.to_string(),
        client_version: client_version.to_string(),
        os: std::env::consts::OS.to_string(),
        caps: HelloCaps {
            sandbox: "none".to_string(),
            exec: true,
            fs: "rw".to_string(),
        },
    }
}

pub async fn connect_once_for_test(config: StartupConfig) -> Result<(), WsError> {
    let mut request = config
        .ws_url
        .as_str()
        .into_client_request()
        .map_err(|err| WsError::Request(err.to_string()))?;
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {}", config.device_token.expose_secret())
            .parse()
            .map_err(|err| WsError::Request(err.to_string()))?,
    );
    let (mut ws, _) = connect_async(request)
        .await
        .map_err(|err| WsError::Transport(err.to_string()))?;
    ws.send(Message::Text(
        serde_json::to_string(&WsFrame::Hello(build_hello(env!("CARGO_PKG_VERSION"))))
            .unwrap()
            .into(),
    ))
    .await
    .map_err(|err| WsError::Transport(err.to_string()))?;
    if let Some(message) = ws.next().await {
        match message.map_err(|err| WsError::Transport(err.to_string()))? {
            Message::Text(text) => {
                if let Ok(WsFrame::Ping(ping)) = serde_json::from_str::<WsFrame>(&text) {
                    ws.send(Message::Text(
                        serde_json::to_string(&WsFrame::Pong(PongFrame { id: ping.id }))
                            .unwrap()
                            .into(),
                    ))
                    .await
                    .map_err(|err| WsError::Transport(err.to_string()))?;
                }
            }
            Message::Close(Some(frame)) => {
                let _ = close_decision(frame.code.into());
            }
            _ => {}
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Add runtime loop skeleton**

Modify `plexus-client/src/runtime.rs`:

```rust
use crate::{config::StartupConfig, ws};

pub async fn run(config: StartupConfig) -> Result<(), ws::WsError> {
    loop {
        match ws::connect_once_for_test(config.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                tracing::warn!(error = %err, "device websocket connect failed");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
```

This loop is deliberately primitive. Task 3 replaces it with the real frame router and capped jitter backoff.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p plexus-client --test ws_lifecycle
cargo check -p plexus-client
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-client/Cargo.toml plexus-client/src/ws.rs plexus-client/src/runtime.rs plexus-client/tests/ws_lifecycle.rs
git commit -m "feat: add client websocket handshake"
```

---

### Task 3: Device Config Store, FIFO Worker, and Tool Result Emission

**Files:**
- Modify: `plexus-client/src/runtime.rs`
- Modify: `plexus-client/src/ws.rs`
- Modify: `plexus-client/src/worker.rs`
- Modify: `plexus-client/src/tools/mod.rs`
- Test: `plexus-client/tests/worker.rs`

- [ ] **Step 1: Write failing worker tests**

Create `plexus-client/tests/worker.rs`:

```rust
use plexus_client::{
    tools::{LocalToolResult, ToolRegistry},
    worker::{ToolJob, Worker},
};
use plexus_common::{DeviceToolResultContent, ErrorCode, protocol::DeviceConfig};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use uuid::Uuid;

fn config() -> DeviceConfig {
    DeviceConfig {
        workspace_path: ".".to_string(),
        fs_policy: plexus_common::protocol::FsPolicy::Sandbox,
        shell_timeout_max: 60,
        ssrf_whitelist: vec![],
        mcp_servers: Default::default(),
    }
}

#[tokio::test]
async fn worker_executes_jobs_fifo() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut registry = ToolRegistry::empty();
    let seen_clone = Arc::clone(&seen);
    registry.insert_test_tool("echo", move |args| {
        let seen = Arc::clone(&seen_clone);
        async move {
            let label = args["label"].as_str().unwrap().to_string();
            seen.lock().unwrap().push(label.clone());
            Ok(LocalToolResult::text(label))
        }
    });

    let (result_tx, mut result_rx) = mpsc::channel(4);
    let worker = Worker::new(registry, config(), result_tx);
    worker.enqueue(ToolJob { id: Uuid::now_v7(), name: "echo".into(), args: json!({"label": "first"}) }).await.unwrap();
    worker.enqueue(ToolJob { id: Uuid::now_v7(), name: "echo".into(), args: json!({"label": "second"}) }).await.unwrap();
    worker.drain_for_test().await;

    let first = result_rx.recv().await.unwrap();
    let second = result_rx.recv().await.unwrap();
    assert_eq!(first.content, DeviceToolResultContent::Text("first".into()));
    assert_eq!(second.content, DeviceToolResultContent::Text("second".into()));
    assert_eq!(&*seen.lock().unwrap(), &["first".to_string(), "second".to_string()]);
}

#[tokio::test]
async fn unknown_tool_returns_invalid_args_result() {
    let (result_tx, mut result_rx) = mpsc::channel(1);
    let worker = Worker::new(ToolRegistry::empty(), config(), result_tx);
    worker.enqueue(ToolJob { id: Uuid::now_v7(), name: "missing".into(), args: json!({}) }).await.unwrap();
    worker.drain_for_test().await;
    let result = result_rx.recv().await.unwrap();
    assert!(result.is_error);
    assert_eq!(result.code, Some(ErrorCode::InvalidArgs));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p plexus-client --test worker
```

Expected: FAIL with unresolved `Worker`, `ToolRegistry`, and `LocalToolResult`.

- [ ] **Step 3: Implement local tool result and registry**

Modify `plexus-client/src/tools/mod.rs`:

```rust
pub mod exec;
pub mod fs;
pub mod web_fetch;

use plexus_common::{DeviceToolResultContent, ErrorCode};
use serde_json::Value;
use std::{future::Future, pin::Pin, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LocalToolError {
    #[error("invalid args: {0}")]
    InvalidArgs(String),
    #[error("tool failed: {0}")]
    Failed(String),
    #[error("tool timed out")]
    Timeout,
}

impl LocalToolError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::InvalidArgs(_) => ErrorCode::InvalidArgs,
            Self::Failed(_) => ErrorCode::IoError,
            Self::Timeout => ErrorCode::ExecTimeout,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalToolResult {
    pub content: DeviceToolResultContent,
}

impl LocalToolResult {
    pub fn text(text: impl Into<String>) -> Self {
        Self { content: DeviceToolResultContent::Text(text.into()) }
    }
}

type ToolFuture = Pin<Box<dyn Future<Output = Result<LocalToolResult, LocalToolError>> + Send>>;
type ToolFn = Arc<dyn Fn(Value) -> ToolFuture + Send + Sync>;

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: std::collections::HashMap<String, ToolFn>,
}

impl ToolRegistry {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn insert_test_tool<F, Fut>(&mut self, name: &str, f: F)
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<LocalToolResult, LocalToolError>> + Send + 'static,
    {
        self.tools.insert(name.to_string(), Arc::new(move |args| Box::pin(f(args))));
    }

    pub async fn call(&self, name: &str, args: Value) -> Result<LocalToolResult, LocalToolError> {
        let Some(tool) = self.tools.get(name) else {
            return Err(LocalToolError::InvalidArgs(format!("unknown tool: {name}")));
        };
        tool(args).await
    }
}
```

- [ ] **Step 4: Implement FIFO worker**

Modify `plexus-client/src/worker.rs`:

```rust
use crate::tools::ToolRegistry;
use plexus_common::{DeviceToolResultContent, protocol::{DeviceConfig, ToolResultFrame}};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ToolJob {
    pub id: Uuid,
    pub name: String,
    pub args: Value,
}

pub struct Worker {
    tx: mpsc::Sender<ToolJob>,
    rx: Mutex<mpsc::Receiver<ToolJob>>,
    registry: ToolRegistry,
    config: DeviceConfig,
    result_tx: mpsc::Sender<ToolResultFrame>,
}

impl Worker {
    pub fn new(registry: ToolRegistry, config: DeviceConfig, result_tx: mpsc::Sender<ToolResultFrame>) -> Self {
        let (tx, rx) = mpsc::channel(64);
        Self { tx, rx: Mutex::new(rx), registry, config, result_tx }
    }

    pub async fn enqueue(&self, job: ToolJob) -> Result<(), mpsc::error::SendError<ToolJob>> {
        self.tx.send(job).await
    }

    pub async fn drain_for_test(&self) {
        loop {
            let job = {
                let mut rx = self.rx.lock().await;
                rx.try_recv().ok()
            };
            let Some(job) = job else { break };
            self.execute_one(job).await;
        }
    }

    async fn execute_one(&self, job: ToolJob) {
        let result = match self.registry.call(&job.name, job.args).await {
            Ok(output) => ToolResultFrame {
                id: job.id,
                content: output.content,
                is_error: false,
                code: None,
            },
            Err(err) => ToolResultFrame {
                id: job.id,
                content: DeviceToolResultContent::Text(err.to_string()),
                is_error: true,
                code: Some(err.code()),
            },
        };
        let _ = self.result_tx.send(result).await;
    }
}
```

The `config` field is intentionally retained even before tools consume it. Later tasks pass config snapshots into concrete executors.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p plexus-client --test worker
cargo check -p plexus-client
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-client/src/tools/mod.rs plexus-client/src/worker.rs plexus-client/tests/worker.rs
git commit -m "feat: add client tool worker"
```

---

### Task 4: Local Shared File Tools

**Files:**
- Create: `plexus-common/src/tools/file_content.rs`
- Modify: `plexus-common/src/tools/mod.rs`
- Modify: `plexus-server/src/tools/file_ops.rs`
- Modify: `plexus-client/src/tools/fs.rs`
- Modify: `plexus-client/src/tools/mod.rs`
- Test: `plexus-client/tests/file_tools.rs`
- Test: existing `plexus-server/tests/m1d_tools.rs`

- [ ] **Step 1: Write failing client file tool tests**

Create `plexus-client/tests/file_tools.rs`:

```rust
use plexus_client::tools::fs::{FileToolContext, call_file_tool};
use plexus_common::{DeviceToolResultContent, ErrorCode, protocol::FsPolicy};
use serde_json::json;
use tempfile::TempDir;

fn ctx(root: &TempDir, policy: FsPolicy) -> FileToolContext {
    FileToolContext {
        workspace_path: root.path().to_path_buf(),
        fs_policy: policy,
    }
}

#[tokio::test]
async fn write_read_list_and_delete_file_in_workspace() {
    let root = TempDir::new().unwrap();
    let ctx = ctx(&root, FsPolicy::Sandbox);
    call_file_tool(&ctx, "write_file", json!({"path": "a/hello.txt", "content": "hello"})).await.unwrap();
    let read = call_file_tool(&ctx, "read_file", json!({"path": "a/hello.txt"})).await.unwrap();
    assert_eq!(read.content, DeviceToolResultContent::Text("hello".into()));
    let list = call_file_tool(&ctx, "list_dir", json!({"path": "a"})).await.unwrap();
    assert!(format!("{:?}", list.content).contains("hello.txt"));
    call_file_tool(&ctx, "delete_file", json!({"path": "a/hello.txt"})).await.unwrap();
    assert!(!root.path().join("a/hello.txt").exists());
}

#[tokio::test]
async fn sandbox_rejects_path_escape() {
    let root = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let ctx = ctx(&root, FsPolicy::Sandbox);
    let err = call_file_tool(&ctx, "read_file", json!({"path": outside.path().join("x.txt")})).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::PathOutsideWorkspace);
}

#[tokio::test]
async fn unrestricted_allows_absolute_path_outside_workspace() {
    let root = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let target = outside.path().join("x.txt");
    tokio::fs::write(&target, "outside").await.unwrap();
    let ctx = ctx(&root, FsPolicy::Unrestricted);
    let read = call_file_tool(&ctx, "read_file", json!({"path": target})).await.unwrap();
    assert_eq!(read.content, DeviceToolResultContent::Text("outside".into()));
}

#[tokio::test]
async fn edit_file_replaces_text() {
    let root = TempDir::new().unwrap();
    let ctx = ctx(&root, FsPolicy::Sandbox);
    call_file_tool(&ctx, "write_file", json!({"path": "note.txt", "content": "hello world"})).await.unwrap();
    call_file_tool(&ctx, "edit_file", json!({"path": "note.txt", "old_text": "world", "new_text": "plexus"})).await.unwrap();
    let read = call_file_tool(&ctx, "read_file", json!({"path": "note.txt"})).await.unwrap();
    assert_eq!(read.content, DeviceToolResultContent::Text("hello plexus".into()));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p plexus-client --test file_tools
```

Expected: FAIL with unresolved `FileToolContext` and `call_file_tool`.

- [ ] **Step 3: Move read-file content formatting to common**

Create `plexus-common/src/tools/file_content.rs` by moving the reusable portions of `plexus-server/src/tools/file_ops.rs`:

```rust
use crate::{ToolError, ToolResultContentBlock};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use regex::Regex;
use serde_json::Value;
use std::io::{Cursor, Read};

#[derive(Debug, Clone, PartialEq)]
pub enum FileContentOutput {
    Text(String),
    Blocks(Vec<ToolResultContentBlock>),
}

pub fn read_file_output(path: &str, bytes: Vec<u8>) -> Result<FileContentOutput, ToolError> {
    if let Some(mime) = detect_image_mime(&bytes) {
        return Ok(FileContentOutput::Blocks(vec![
            ToolResultContentBlock::text(format!("Successfully read file: {path}")),
            ToolResultContentBlock::image_base64(mime, STANDARD.encode(&bytes)),
        ]));
    }
    if is_pdf(&bytes) {
        return extract_pdf_text(&bytes).map(|text| FileContentOutput::Blocks(vec![
            ToolResultContentBlock::text(format!("Successfully read file: {path}")),
            ToolResultContentBlock::text(text),
        ]));
    }
    if is_zip_document(path, &bytes) {
        return extract_office_text(path, &bytes).map(|text| FileContentOutput::Blocks(vec![
            ToolResultContentBlock::text(format!("Successfully read file: {path}")),
            ToolResultContentBlock::text(text),
        ]));
    }
    String::from_utf8(bytes)
        .map(FileContentOutput::Text)
        .map_err(|_| ToolError::InvalidArgs("unsupported binary file".to_string()))
}

pub fn string_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidArgs(format!("{key} is required")))
}
```

Also move `detect_image_mime`, `is_pdf`, `is_zip_document`, `extract_pdf_text`,
`extract_office_text`, `office_xml_entry_matches`, `xml_to_text`, and
`decode_xml_entities` unchanged from server `file_ops.rs` into this module.

Modify `plexus-common/src/tools/mod.rs`:

```rust
pub mod file_content;
```

Modify `plexus-server/src/tools/file_ops.rs` to call `plexus_common::tools::file_content::read_file_output` and convert `FileContentOutput` into `ToolOutput`.

- [ ] **Step 4: Implement client file tools**

Implement `plexus-client/src/tools/fs.rs`:

```rust
use crate::tools::{LocalToolError, LocalToolResult};
use plexus_common::{
    DeviceToolResultContent, ErrorCode, ToolError, ToolResultContentBlock,
    protocol::FsPolicy,
    tools::{file_content::{self, FileContentOutput}, path::resolve_in_workspace},
};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct FileToolContext {
    pub workspace_path: PathBuf,
    pub fs_policy: FsPolicy,
}

impl LocalToolError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::InvalidArgs(message) if message.contains("PathOutsideWorkspace") => ErrorCode::PathOutsideWorkspace,
            Self::InvalidArgs(_) => ErrorCode::InvalidArgs,
            Self::Failed(_) => ErrorCode::IoError,
            Self::Timeout => ErrorCode::ExecTimeout,
        }
    }
}

pub async fn call_file_tool(ctx: &FileToolContext, name: &str, args: Value) -> Result<LocalToolResult, LocalToolError> {
    match name {
        "read_file" => {
            let path = file_content::string_arg(&args, "path").map_err(tool_to_local)?;
            let resolved = resolve_path(ctx, path)?;
            let bytes = tokio::fs::read(&resolved).await.map_err(io_to_local)?;
            file_output_to_local(file_content::read_file_output(path, bytes).map_err(tool_to_local)?)
        }
        "write_file" => {
            let path = file_content::string_arg(&args, "path").map_err(tool_to_local)?;
            let content = file_content::string_arg(&args, "content").map_err(tool_to_local)?;
            let resolved = resolve_path_for_write(ctx, path).await?;
            if let Some(parent) = resolved.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(io_to_local)?;
            }
            tokio::fs::write(resolved, content).await.map_err(io_to_local)?;
            Ok(LocalToolResult::text("written"))
        }
        "edit_file" => edit_file(ctx, args).await,
        "delete_file" => {
            let path = file_content::string_arg(&args, "path").map_err(tool_to_local)?;
            let resolved = resolve_path(ctx, path)?;
            tokio::fs::remove_file(resolved).await.map_err(io_to_local)?;
            Ok(LocalToolResult::text("deleted"))
        }
        "delete_folder" => {
            let path = file_content::string_arg(&args, "path").map_err(tool_to_local)?;
            let resolved = resolve_path(ctx, path)?;
            tokio::fs::remove_dir_all(resolved).await.map_err(io_to_local)?;
            Ok(LocalToolResult::text("deleted"))
        }
        "list_dir" => list_dir(ctx, args).await,
        "glob" => glob_files(ctx, args).await,
        "grep" => grep_files(ctx, args).await,
        "notebook_edit" => notebook_edit(ctx, args).await,
        other => Err(LocalToolError::InvalidArgs(format!("unknown file tool: {other}"))),
    }
}

fn resolve_path(ctx: &FileToolContext, path: &str) -> Result<PathBuf, LocalToolError> {
    match ctx.fs_policy {
        FsPolicy::Sandbox => resolve_in_workspace(&ctx.workspace_path, path)
            .map_err(|err| LocalToolError::InvalidArgs(format!("{:?}: {err}", err.code()))),
        FsPolicy::Unrestricted => {
            let raw = Path::new(path);
            Ok(if raw.is_absolute() { raw.to_path_buf() } else { ctx.workspace_path.join(raw) })
        }
    }
}
```

Fill the private helpers in the same file:

- `resolve_path_for_write` creates missing parents in unrestricted mode and uses `resolve_in_workspace` after creating parents in sandbox mode.
- `edit_file` reads UTF-8, rejects empty `old_text`, replaces first or all matches, writes the result, and returns `{"replacements": n}`.
- `list_dir` returns JSON array of `{ "name", "path", "is_dir", "size" }`.
- `glob_files` uses `globset` or a small recursive walk plus `glob::Pattern`; add `glob = "0.3"` if that is simpler than `globset`.
- `grep_files` uses `regex` and returns JSON matches with path/line.
- `notebook_edit` parses `.ipynb` as `serde_json::Value`, edits `cells[cell_index].source`, and writes pretty JSON.

Keep each helper under roughly 80 lines. If any helper grows larger, split it into a private submodule under `plexus-client/src/tools/fs/`.

- [ ] **Step 5: Wire file tools into the registry**

Modify `ToolRegistry` construction in `plexus-client/src/tools/mod.rs`:

```rust
impl ToolRegistry {
    pub fn alpha(config: plexus_common::protocol::DeviceConfig) -> Self {
        let mut registry = Self::empty();
        let fs_ctx = fs::FileToolContext {
            workspace_path: std::path::PathBuf::from(config.workspace_path.clone()),
            fs_policy: config.fs_policy,
        };
        for name in [
            "read_file",
            "write_file",
            "edit_file",
            "delete_file",
            "delete_folder",
            "list_dir",
            "glob",
            "grep",
            "notebook_edit",
        ] {
            let fs_ctx = fs_ctx.clone();
            registry.insert_test_tool(name, move |args| {
                let fs_ctx = fs_ctx.clone();
                async move { fs::call_file_tool(&fs_ctx, name, args).await }
            });
        }
        registry
    }
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p plexus-client --test file_tools
cargo test -p plexus-server --test m1d_tools
cargo test -p plexus-common tools::path
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add plexus-common/src/tools plexus-server/src/tools/file_ops.rs plexus-client/src/tools plexus-client/tests/file_tools.rs Cargo.toml plexus-client/Cargo.toml
git commit -m "feat: add client file tools"
```

---

### Task 5: One-Shot Exec Runner

**Files:**
- Modify: `plexus-client/src/tools/exec.rs`
- Modify: `plexus-client/src/tools/mod.rs`
- Test: `plexus-client/tests/exec.rs`

- [ ] **Step 1: Write failing exec tests**

Create `plexus-client/tests/exec.rs`:

```rust
use plexus_client::tools::exec::{ExecConfig, run_exec};
use plexus_common::{DeviceToolResultContent, ErrorCode, protocol::FsPolicy};
use serde_json::json;
use tempfile::TempDir;

fn config(root: &TempDir, policy: FsPolicy) -> ExecConfig {
    ExecConfig {
        workspace_path: root.path().to_path_buf(),
        fs_policy: policy,
        shell_timeout_max: 2,
    }
}

#[tokio::test]
async fn exec_returns_stdout_stderr_and_exit_code() {
    let root = TempDir::new().unwrap();
    let result = run_exec(&config(&root, FsPolicy::Sandbox), json!({
        "command": "printf hello",
        "timeout": 1
    })).await.unwrap();
    let DeviceToolResultContent::Text(text) = result.content else { panic!("expected text") };
    assert!(text.contains("\"exit_code\":0"));
    assert!(text.contains("hello"));
}

#[tokio::test]
async fn exec_rejects_sandbox_cwd_outside_workspace() {
    let root = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let err = run_exec(&config(&root, FsPolicy::Sandbox), json!({
        "command": "pwd",
        "working_dir": outside.path()
    })).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::CwdOutsideWorkspace);
}

#[tokio::test]
async fn exec_timeout_returns_exec_timeout() {
    let root = TempDir::new().unwrap();
    let err = run_exec(&config(&root, FsPolicy::Sandbox), json!({
        "command": "sleep 5",
        "timeout": 1
    })).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::ExecTimeout);
}

#[tokio::test]
async fn exec_does_not_forward_device_token() {
    let root = TempDir::new().unwrap();
    std::env::set_var("PLEXUS_DEVICE_TOKEN", "plexus_dev_secret_should_not_print");
    let result = run_exec(&config(&root, FsPolicy::Sandbox), json!({
        "command": "env",
        "timeout": 1
    })).await.unwrap();
    let DeviceToolResultContent::Text(text) = result.content else { panic!("expected text") };
    assert!(!text.contains("plexus_dev_secret_should_not_print"));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p plexus-client --test exec
```

Expected: FAIL with unresolved `ExecConfig` and `run_exec`.

- [ ] **Step 3: Implement one-shot exec**

Implement `plexus-client/src/tools/exec.rs`:

```rust
use crate::tools::{LocalToolError, LocalToolResult};
use plexus_common::{ErrorCode, protocol::FsPolicy, tools::path::resolve_in_workspace};
use serde_json::{json, Value};
use std::{collections::HashMap, path::{Path, PathBuf}, process::Stdio, time::Duration};
use tokio::process::Command;

#[derive(Clone, Debug)]
pub struct ExecConfig {
    pub workspace_path: PathBuf,
    pub fs_policy: FsPolicy,
    pub shell_timeout_max: u32,
}

pub async fn run_exec(config: &ExecConfig, args: Value) -> Result<LocalToolResult, LocalToolError> {
    let command = args
        .get("command")
        .and_then(Value::as_str)
        .ok_or_else(|| LocalToolError::InvalidArgs("command is required".to_string()))?;
    let requested_timeout = args.get("timeout").and_then(Value::as_u64).unwrap_or(60) as u32;
    let timeout_secs = requested_timeout.min(config.shell_timeout_max.max(1));
    let cwd = resolve_cwd(config, args.get("working_dir").and_then(Value::as_str))?;
    let mut child = shell_command(command);
    child
        .current_dir(cwd)
        .env_clear()
        .envs(stripped_env())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = child.spawn().map_err(|err| LocalToolError::Failed(err.to_string()))?;
    let output = match tokio::time::timeout(Duration::from_secs(timeout_secs as u64), child.wait_with_output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => return Err(LocalToolError::Failed(err.to_string())),
        Err(_) => return Err(LocalToolError::Timeout),
    };
    let body = json!({
        "exit_code": output.status.code().unwrap_or(-1),
        "stdout": String::from_utf8_lossy(&output.stdout),
        "stderr": String::from_utf8_lossy(&output.stderr),
    });
    Ok(LocalToolResult::text(body.to_string()))
}

fn resolve_cwd(config: &ExecConfig, requested: Option<&str>) -> Result<PathBuf, LocalToolError> {
    let raw = requested.unwrap_or_else(|| config.workspace_path.to_str().unwrap_or("."));
    match config.fs_policy {
        FsPolicy::Sandbox => resolve_in_workspace(&config.workspace_path, raw)
            .map_err(|err| LocalToolError::InvalidArgs(format!("{:?}: {err}", ErrorCode::CwdOutsideWorkspace))),
        FsPolicy::Unrestricted => {
            let path = Path::new(raw);
            Ok(if path.is_absolute() { path.to_path_buf() } else { config.workspace_path.join(path) })
        }
    }
}

fn shell_command(command: &str) -> Command {
    #[cfg(windows)]
    {
        let mut cmd = Command::new(std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".to_string()));
        cmd.arg("/c").arg(command);
        cmd
    }
    #[cfg(not(windows))]
    {
        let mut cmd = Command::new(std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string()));
        cmd.arg("-c").arg(command);
        cmd
    }
}

fn stripped_env() -> HashMap<String, String> {
    let mut env = HashMap::new();
    for key in ["PATH", "HOME", "LANG", "TERM"] {
        if let Ok(value) = std::env::var(key) {
            env.insert(key.to_string(), value);
        }
    }
    #[cfg(windows)]
    for key in ["SYSTEMROOT", "COMSPEC", "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "TEMP", "TMP", "PATHEXT", "APPDATA", "LOCALAPPDATA", "ProgramData", "ProgramFiles", "ProgramFiles(x86)", "ProgramW6432"] {
        if let Ok(value) = std::env::var(key) {
            env.insert(key.to_string(), value);
        }
    }
    env
}
```

Adjust `LocalToolError::code()` so `LocalToolError::Timeout` maps to `ExecTimeout` and the cwd error maps to `CwdOutsideWorkspace` by introducing a dedicated variant if string matching is not sufficient:

```rust
CwdOutsideWorkspace(String),
```

- [ ] **Step 4: Wire exec into the registry**

Modify `ToolRegistry::alpha`:

```rust
let exec_config = exec::ExecConfig {
    workspace_path: std::path::PathBuf::from(config.workspace_path.clone()),
    fs_policy: config.fs_policy,
    shell_timeout_max: config.shell_timeout_max,
};
registry.insert_test_tool("exec", move |args| {
    let exec_config = exec_config.clone();
    async move { exec::run_exec(&exec_config, args).await }
});
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p plexus-client --test exec
cargo test -p plexus-client --test worker
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-client/src/tools plexus-client/tests/exec.rs
git commit -m "feat: add one shot client exec"
```

---

### Task 6: Client `web_fetch`

**Files:**
- Modify: `plexus-client/src/tools/web_fetch.rs`
- Modify: `plexus-client/src/tools/mod.rs`
- Test: `plexus-client/tests/web_fetch.rs`

- [ ] **Step 1: Write failing web_fetch tests**

Create `plexus-client/tests/web_fetch.rs`:

```rust
use plexus_client::tools::web_fetch::{WebFetchConfig, web_fetch};
use plexus_common::ErrorCode;
use serde_json::json;

#[tokio::test]
async fn localhost_is_blocked_without_whitelist() {
    let err = web_fetch(&WebFetchConfig { ssrf_whitelist: vec![] }, json!({
        "url": "http://127.0.0.1:12345"
    })).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::PrivateAddressBlocked);
}

#[tokio::test]
async fn localhost_allowed_when_whitelisted() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await.unwrap();
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello").await.unwrap();
    });
    let result = web_fetch(&WebFetchConfig { ssrf_whitelist: vec![addr.to_string()] }, json!({
        "url": format!("http://{addr}"),
        "maxChars": 100
    })).await.unwrap();
    assert!(format!("{:?}", result.content).contains("hello"));
    server.await.unwrap();
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p plexus-client --test web_fetch
```

Expected: FAIL with unresolved `web_fetch`.

- [ ] **Step 3: Implement SSRF policy and fetch**

Implement `plexus-client/src/tools/web_fetch.rs`:

```rust
use crate::tools::{LocalToolError, LocalToolResult};
use plexus_common::ErrorCode;
use serde_json::Value;
use std::net::{IpAddr, ToSocketAddrs};

#[derive(Clone, Debug)]
pub struct WebFetchConfig {
    pub ssrf_whitelist: Vec<String>,
}

pub async fn web_fetch(config: &WebFetchConfig, args: Value) -> Result<LocalToolResult, LocalToolError> {
    let url = args
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| LocalToolError::InvalidArgs("url is required".to_string()))?;
    let max_chars = args.get("maxChars").and_then(Value::as_u64).unwrap_or(50_000) as usize;
    let parsed = reqwest::Url::parse(url).map_err(|err| LocalToolError::InvalidArgs(err.to_string()))?;
    enforce_ssrf_policy(config, &parsed)?;
    let body = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|err| LocalToolError::Failed(err.to_string()))?
        .get(parsed)
        .send()
        .await
        .map_err(|err| LocalToolError::Failed(err.to_string()))?
        .text()
        .await
        .map_err(|err| LocalToolError::Failed(err.to_string()))?;
    Ok(LocalToolResult::text(body.chars().take(max_chars).collect::<String>()))
}

fn enforce_ssrf_policy(config: &WebFetchConfig, url: &reqwest::Url) -> Result<(), LocalToolError> {
    let host = url.host_str().ok_or_else(|| LocalToolError::InvalidArgs("url host is required".to_string()))?;
    let port = url.port_or_known_default().unwrap_or(80);
    let host_port = format!("{host}:{port}");
    if config.ssrf_whitelist.iter().any(|entry| entry == host || entry == &host_port) {
        return Ok(());
    }
    for addr in (host, port).to_socket_addrs().map_err(|err| LocalToolError::Failed(err.to_string()))? {
        if private_ip(addr.ip()) {
            return Err(LocalToolError::NetworkBlocked(ErrorCode::PrivateAddressBlocked, host_port));
        }
    }
    Ok(())
}

fn private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.octets()[0] == 100,
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local(),
    }
}
```

Add `NetworkBlocked(ErrorCode, String)` to `LocalToolError` and map it to its embedded `ErrorCode`.

- [ ] **Step 4: Wire web_fetch into registry**

Modify `ToolRegistry::alpha`:

```rust
let web_config = web_fetch::WebFetchConfig {
    ssrf_whitelist: config.ssrf_whitelist.clone(),
};
registry.insert_test_tool("web_fetch", move |args| {
    let web_config = web_config.clone();
    async move { web_fetch::web_fetch(&web_config, args).await }
});
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p plexus-client --test web_fetch
cargo test -p plexus-client
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-client/src/tools plexus-client/tests/web_fetch.rs
git commit -m "feat: add client web fetch"
```

---

### Task 7: Real Client Runtime Frame Router

**Files:**
- Modify: `plexus-client/src/runtime.rs`
- Modify: `plexus-client/src/ws.rs`
- Modify: `plexus-client/src/worker.rs`
- Test: `plexus-client/tests/ws_lifecycle.rs`

- [ ] **Step 1: Add failing runtime integration test**

Append to `plexus-client/tests/ws_lifecycle.rs`:

```rust
#[tokio::test]
async fn runtime_processes_tool_call_and_returns_tool_result() {
    use plexus_common::protocol::{DeviceConfig, FsPolicy, HelloAckFrame, ToolCallFrame};
    use tempfile::TempDir;
    let workspace = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let Message::Text(text) = ws.next().await.unwrap().unwrap() else { panic!("hello") };
        let WsFrame::Hello(hello) = serde_json::from_str(&text).unwrap() else { panic!("hello frame") };
        ws.send(Message::Text(serde_json::to_string(&WsFrame::HelloAck(HelloAckFrame {
            id: hello.id,
            device_name: "devbox".into(),
            user_id: uuid::Uuid::now_v7(),
            config: DeviceConfig {
                workspace_path: workspace.path().to_string_lossy().to_string(),
                fs_policy: FsPolicy::Sandbox,
                shell_timeout_max: 60,
                ssrf_whitelist: vec![],
                mcp_servers: Default::default(),
            },
        })).unwrap().into())).await.unwrap();
        let call_id = uuid::Uuid::now_v7();
        ws.send(Message::Text(serde_json::to_string(&WsFrame::ToolCall(ToolCallFrame {
            id: call_id,
            name: "write_file".into(),
            args: serde_json::json!({"path": "hello.txt", "content": "hello"}),
        })).unwrap().into())).await.unwrap();
        let Message::Text(text) = ws.next().await.unwrap().unwrap() else { panic!("tool_result") };
        let WsFrame::ToolResult(result) = serde_json::from_str(&text).unwrap() else { panic!("tool_result frame") };
        assert_eq!(result.id, call_id);
        assert!(!result.is_error);
    });
    let config = StartupConfig::from_pairs([
        ("PLEXUS_SERVER_URL", format!("http://{addr}")),
        ("PLEXUS_DEVICE_TOKEN", "plexus_dev_test".to_string()),
    ]).unwrap();
    plexus_client::runtime::run_once_for_test(config).await.unwrap();
    server.await.unwrap();
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test -p plexus-client --test ws_lifecycle runtime_processes_tool_call_and_returns_tool_result -- --nocapture
```

Expected: FAIL with unresolved `run_once_for_test` or missing frame routing.

- [ ] **Step 3: Implement run-once frame router**

Modify `runtime.rs` to:

- connect with `ws::connect`;
- send `hello`;
- wait for `hello_ack`;
- create `ToolRegistry::alpha(ack.config.clone())`;
- create a `Worker`;
- route `ToolCall` to `Worker::enqueue`;
- route worker result frames back to the server;
- reply to `Ping`;
- apply `ConfigUpdate` by replacing the registry for new jobs.

Use this public test hook:

```rust
pub async fn run_once_for_test(config: StartupConfig) -> Result<(), ws::WsError> {
    run_until_disconnect(config, true).await
}
```

Keep the production `run` as:

```rust
pub async fn run(config: StartupConfig) -> Result<(), ws::WsError> {
    run_with_reconnect(config).await
}
```

- [ ] **Step 4: Implement capped reconnect backoff**

Add a small backoff helper:

```rust
#[derive(Debug, Clone)]
pub struct Backoff {
    next_secs: u64,
}

impl Backoff {
    pub fn new() -> Self { Self { next_secs: 1 } }
    pub fn reset(&mut self) { self.next_secs = 1; }
    pub fn next(&mut self) -> std::time::Duration {
        let secs = self.next_secs;
        self.next_secs = (self.next_secs * 2).min(30);
        std::time::Duration::from_secs(secs)
    }
}
```

Do not add jitter until the deterministic tests are in place. Add jitter in a later hardening pass or inject a deterministic RNG for tests.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p plexus-client --test ws_lifecycle
cargo test -p plexus-client --test worker
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-client/src/runtime.rs plexus-client/src/ws.rs plexus-client/src/worker.rs plexus-client/tests/ws_lifecycle.rs
git commit -m "feat: route client websocket tool calls"
```

---

### Task 8: Client-Side Transfer Slots

**Files:**
- Modify: `plexus-client/src/transfer.rs`
- Modify: `plexus-client/src/runtime.rs`
- Modify: `plexus-client/src/ws.rs`
- Test: `plexus-client/tests/transfer.rs`

- [ ] **Step 1: Write failing transfer tests**

Create `plexus-client/tests/transfer.rs`:

```rust
use plexus_client::transfer::{TransferManager, TransferRoot};
use plexus_common::protocol::{TransferBeginFrame, TransferDirection};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn server_to_client_writes_chunks_and_verifies_sha() {
    let root = TempDir::new().unwrap();
    let mut manager = TransferManager::new(TransferRoot::new(root.path().to_path_buf()));
    let id = Uuid::now_v7();
    manager.begin(TransferBeginFrame {
        id,
        direction: TransferDirection::ServerToClient,
        src_device: "server".into(),
        src_path: "a.txt".into(),
        dst_device: "devbox".into(),
        dst_path: "copied/a.txt".into(),
        total_bytes: 5,
        sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
        mime: None,
    }).await.unwrap();
    manager.chunk(id, b"hello").await.unwrap();
    manager.finish(id).await.unwrap();
    assert_eq!(tokio::fs::read_to_string(root.path().join("copied/a.txt")).await.unwrap(), "hello");
}

#[tokio::test]
async fn sha_mismatch_removes_partial_file() {
    let root = TempDir::new().unwrap();
    let mut manager = TransferManager::new(TransferRoot::new(root.path().to_path_buf()));
    let id = Uuid::now_v7();
    manager.begin(TransferBeginFrame {
        id,
        direction: TransferDirection::ServerToClient,
        src_device: "server".into(),
        src_path: "a.txt".into(),
        dst_device: "devbox".into(),
        dst_path: "bad.txt".into(),
        total_bytes: 5,
        sha256: "0000000000000000000000000000000000000000000000000000000000000000".into(),
        mime: None,
    }).await.unwrap();
    manager.chunk(id, b"hello").await.unwrap();
    assert!(manager.finish(id).await.is_err());
    assert!(!root.path().join("bad.txt").exists());
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p plexus-client --test transfer
```

Expected: FAIL with unresolved `TransferManager`.

- [ ] **Step 3: Implement transfer manager**

Implement `plexus-client/src/transfer.rs`:

```rust
use plexus_common::{ErrorCode, protocol::{TransferBeginFrame, TransferDirection}, tools::path::resolve_in_workspace};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::PathBuf};
use thiserror::Error;
use tokio::{fs::File, io::AsyncWriteExt};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("invalid transfer args: {0}")]
    InvalidArgs(String),
    #[error("transfer target already exists: {0}")]
    AlreadyExists(String),
    #[error("sha256 mismatch")]
    Sha256Mismatch,
    #[error("io error: {0}")]
    Io(String),
}

impl TransferError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::AlreadyExists(_) => ErrorCode::InvalidArgs,
            Self::Sha256Mismatch => ErrorCode::InvalidArgs,
            Self::InvalidArgs(_) => ErrorCode::InvalidArgs,
            Self::Io(_) => ErrorCode::IoError,
        }
    }
}

#[derive(Clone)]
pub struct TransferRoot {
    workspace_path: PathBuf,
}

impl TransferRoot {
    pub fn new(workspace_path: PathBuf) -> Self {
        Self { workspace_path }
    }
}

struct IncomingSlot {
    frame: TransferBeginFrame,
    path: PathBuf,
    file: File,
    hasher: Sha256,
    bytes: u64,
}

pub struct TransferManager {
    root: TransferRoot,
    incoming: HashMap<Uuid, IncomingSlot>,
}

impl TransferManager {
    pub fn new(root: TransferRoot) -> Self {
        Self { root, incoming: HashMap::new() }
    }

    pub async fn begin(&mut self, frame: TransferBeginFrame) -> Result<(), TransferError> {
        if frame.direction != TransferDirection::ServerToClient {
            return Err(TransferError::InvalidArgs("client receiver only handles server_to_client in begin".into()));
        }
        let path = resolve_in_workspace(&self.root.workspace_path, &frame.dst_path)
            .map_err(|err| TransferError::InvalidArgs(err.to_string()))?;
        if tokio::fs::try_exists(&path).await.map_err(|err| TransferError::Io(err.to_string()))? {
            return Err(TransferError::AlreadyExists(frame.dst_path));
        }
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|err| TransferError::Io(err.to_string()))?;
        }
        let file = File::create(&path).await.map_err(|err| TransferError::Io(err.to_string()))?;
        self.incoming.insert(frame.id, IncomingSlot { frame, path, file, hasher: Sha256::new(), bytes: 0 });
        Ok(())
    }

    pub async fn chunk(&mut self, id: Uuid, bytes: &[u8]) -> Result<(), TransferError> {
        let slot = self.incoming.get_mut(&id).ok_or_else(|| TransferError::InvalidArgs("unknown transfer id".into()))?;
        slot.file.write_all(bytes).await.map_err(|err| TransferError::Io(err.to_string()))?;
        slot.hasher.update(bytes);
        slot.bytes += bytes.len() as u64;
        Ok(())
    }

    pub async fn finish(&mut self, id: Uuid) -> Result<(), TransferError> {
        let mut slot = self.incoming.remove(&id).ok_or_else(|| TransferError::InvalidArgs("unknown transfer id".into()))?;
        slot.file.flush().await.map_err(|err| TransferError::Io(err.to_string()))?;
        drop(slot.file);
        let actual = format!("{:x}", slot.hasher.finalize());
        if slot.bytes != slot.frame.total_bytes || actual != slot.frame.sha256 {
            let _ = tokio::fs::remove_file(&slot.path).await;
            return Err(TransferError::Sha256Mismatch);
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Route transfer frames in runtime**

Modify `runtime.rs` so:

- `WsFrame::TransferBegin` calls `transfer_manager.begin(frame)`;
- binary messages call `plexus_common::protocol::transfer::parse_chunk` then `transfer_manager.chunk(id, bytes)`;
- `WsFrame::TransferEnd { ok: true }` calls `finish(id)` and sends a `TransferEnd { ok: true, sha256 }` acknowledgement;
- errors send `TransferEnd { ok: false, error }`.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -p plexus-client --test transfer
cargo test -p plexus-client --test ws_lifecycle
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-client/src/transfer.rs plexus-client/src/runtime.rs plexus-client/src/ws.rs plexus-client/tests/transfer.rs
git commit -m "feat: add client transfer slots"
```

---

### Task 9: Server Binary Transfer Plumbing

**Files:**
- Modify: `plexus-server/src/devices/registry.rs`
- Modify: `plexus-server/src/devices/ws.rs`
- Modify: `plexus-server/tests/support/device_client.rs`
- Test: `plexus-server/tests/m1f_file_transfer.rs`

- [ ] **Step 1: Add failing server WS binary test**

Append to `plexus-server/tests/m1f_file_transfer.rs`:

```rust
#[tokio::test]
async fn server_can_send_binary_frame_to_connected_device() {
    let app = TestApp::spawn().await;
    let (jwt, _) = register_user(&app, "alice-binary-send@example.com").await;
    let token = support::create_device(&app, &jwt, "devbox").await;
    let base = app.spawn_server().await;
    let mut device = support::device_client::DeviceClient::connect(&base, Some(&token)).await;
    device.send_hello(plexus_common::version::PROTOCOL_VERSION).await;
    device.recv_hello_ack().await;

    let id = uuid::Uuid::now_v7();
    assert!(app.state.devices().send_binary(&token, plexus_common::protocol::transfer::pack_chunk(id, b"hello")).await);
    let (actual_id, chunk) = device.recv_binary_chunk().await;
    assert_eq!(actual_id, id);
    assert_eq!(chunk, b"hello");
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test -p plexus-server --test m1f_file_transfer server_can_send_binary_frame_to_connected_device -- --nocapture
```

Expected: FAIL with unresolved `send_binary` and `recv_binary_chunk`.

- [ ] **Step 3: Extend registry commands**

Modify `DeviceCommand` in `plexus-server/src/devices/registry.rs`:

```rust
#[derive(Debug, Clone)]
pub enum DeviceCommand {
    Frame(WsFrame),
    Binary(Vec<u8>),
    Close(CloseReason),
}
```

Add:

```rust
pub async fn send_binary(&self, token: &str, bytes: Vec<u8>) -> bool {
    let handle = self.get(token).await;
    let Some(handle) = handle else {
        return false;
    };
    if handle.tx.send(DeviceCommand::Binary(bytes)).await.is_ok() {
        return true;
    }
    self.remove_stale_sender(token, &handle.tx).await;
    false
}
```

- [ ] **Step 4: Send binary frames from `devices/ws.rs`**

Modify the writer match:

```rust
crate::devices::registry::DeviceCommand::Binary(bytes) => {
    if sender.send(Message::Binary(bytes.into())).await.is_err() {
        break;
    }
}
```

- [ ] **Step 5: Add test helper**

Modify `plexus-server/tests/support/device_client.rs`:

```rust
pub async fn recv_binary_chunk(&mut self) -> (Uuid, Vec<u8>) {
    loop {
        match self.next_message().await {
            Message::Binary(bytes) => {
                let (id, chunk) = plexus_common::protocol::transfer::parse_chunk(&bytes).unwrap();
                return (id, chunk.to_vec());
            }
            Message::Text(text) => {
                let frame: WsFrame = serde_json::from_str(&text).unwrap();
                if let WsFrame::Ping(ping) = frame {
                    self.reply_pong(ping.id).await;
                }
            }
            other => panic!("unexpected websocket message: {other:?}"),
        }
    }
}
```

- [ ] **Step 6: Run test**

Run:

```bash
cargo test -p plexus-server --test m1f_file_transfer server_can_send_binary_frame_to_connected_device -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add plexus-server/src/devices/registry.rs plexus-server/src/devices/ws.rs plexus-server/tests/support/device_client.rs plexus-server/tests/m1f_file_transfer.rs
git commit -m "feat: support device binary frames"
```

---

### Task 10: Server `server -> client` File Transfer

**Files:**
- Modify: `plexus-server/src/tools/file_transfer.rs`
- Modify: `plexus-server/tests/m1f_file_transfer.rs`

- [ ] **Step 1: Write failing server-to-client transfer test**

Append to `plexus-server/tests/m1f_file_transfer.rs`:

```rust
#[tokio::test]
async fn server_to_client_transfer_streams_file_to_device() {
    let app = TestApp::spawn().await;
    let (jwt, user_id) = register_user(&app, "alice-server-client-transfer@example.com").await;
    set_quota(&app, 1_000_000).await;
    support::write_workspace_file(&app, user_id, "src/a.txt", "hello").await;
    let token = support::create_device(&app, &jwt, "devbox").await;
    let base = app.spawn_server().await;
    let mut device = support::device_client::DeviceClient::connect(&base, Some(&token)).await;
    device.send_hello(plexus_common::version::PROTOCOL_VERSION).await;
    device.recv_hello_ack().await;

    let transfer = tokio::spawn({
        let state = app.state.clone();
        async move {
            plexus_server::tools::file_transfer::execute(
                &state,
                user_id,
                json!({
                    "plexus_src_device": "server",
                    "src_path": "src/a.txt",
                    "plexus_dst_device": "devbox",
                    "dst_path": "dst/a.txt",
                    "mode": "copy"
                }),
            )
            .await
            .unwrap()
        }
    });

    let begin = device.recv_transfer_begin().await;
    assert_eq!(begin.direction, plexus_common::protocol::TransferDirection::ServerToClient);
    assert_eq!(begin.dst_path, "dst/a.txt");
    let (_id, chunk) = device.recv_binary_chunk().await;
    assert_eq!(chunk, b"hello");
    device.send_transfer_end_ok(begin.id, Some(begin.sha256.clone())).await;

    let output = transfer.await.unwrap();
    let plexus_server::tools::output::ToolOutput::Text(text) = output else { panic!("text") };
    assert!(text.contains("copied"));
}
```

- [ ] **Step 2: Add helper methods to fake device client**

Add to `plexus-server/tests/support/device_client.rs`:

```rust
pub async fn recv_transfer_begin(&mut self) -> plexus_common::protocol::TransferBeginFrame {
    loop {
        match self.recv_frame().await {
            WsFrame::TransferBegin(frame) => return frame,
            WsFrame::Ping(ping) => self.reply_pong(ping.id).await,
            other => panic!("expected transfer_begin, got {other:?}"),
        }
    }
}

pub async fn send_transfer_end_ok(&mut self, id: Uuid, sha256: Option<String>) {
    self.send(WsFrame::TransferEnd(plexus_common::protocol::TransferEndFrame {
        id,
        ok: true,
        error: None,
        sha256,
    })).await;
}
```

- [ ] **Step 3: Run test to verify failure**

Run:

```bash
cargo test -p plexus-server --test m1f_file_transfer server_to_client_transfer_streams_file_to_device -- --nocapture
```

Expected: FAIL with current `device file_transfer streaming is not implemented in M1f`.

- [ ] **Step 4: Add transfer waiter to registry**

Add pending transfer map to `RegistryState`:

```rust
type PendingTransferKey = (String, u64, Uuid);
type PendingTransferSender = oneshot::Sender<Result<plexus_common::protocol::TransferEndFrame, DeviceCallError>>;
pending_transfers: HashMap<PendingTransferKey, PendingTransferSender>,
```

Add methods:

```rust
pub async fn begin_transfer_wait(
    &self,
    token: &str,
    generation: u64,
    id: Uuid,
) -> oneshot::Receiver<Result<plexus_common::protocol::TransferEndFrame, DeviceCallError>> {
    let (tx, rx) = oneshot::channel();
    self.inner.lock().await.pending_transfers.insert((token.to_string(), generation, id), tx);
    rx
}

pub async fn complete_transfer_end(
    &self,
    token: &str,
    generation: u64,
    frame: plexus_common::protocol::TransferEndFrame,
) -> bool {
    let sender = self.inner.lock().await.pending_transfers.remove(&(token.to_string(), generation, frame.id));
    sender.is_some_and(|sender| sender.send(Ok(frame)).is_ok())
}
```

Ensure unregister/revoke fails pending transfer senders with `DeviceUnreachable`, matching pending tool calls.

- [ ] **Step 5: Route `TransferEnd` in server WS**

Modify `devices/ws.rs` text-frame match:

```rust
Ok(WsFrame::TransferEnd(frame)) => {
    if !state.devices().complete_transfer_end(&row.token, generation, frame).await
        && !send_error(&state, &row.token, generation, ErrorCode::TransferUnknownId, "unknown transfer id").await
    {
        break;
    }
}
```

Modify binary-message handling so unknown binary transfer chunks are not rejected after Task 11 adds client->server. For this task, keep receiving binary as malformed unless it belongs to a registered inbound transfer.

- [ ] **Step 6: Implement `server_to_client`**

In `plexus-server/src/tools/file_transfer.rs`, add:

```rust
async fn server_to_client(
    state: &AppState,
    user_id: Uuid,
    src_path: &str,
    dst_device: &str,
    dst_path: &str,
    mode: &str,
) -> Result<ToolOutput, ToolError> {
    let row = devices::find_by_user_and_name(state.pool(), user_id, dst_device)
        .await
        .map_err(|err| ToolError::InvalidArgs(err.to_string()))?
        .ok_or_else(|| ToolError::InvalidArgs(format!("unknown device: {dst_device}")))?;
    let generation = state.devices().generation(&row.token).await.ok_or_else(|| ToolError::DeviceUnreachable { device: row.name.clone() })?;
    let bytes = state.workspace_fs().read_file(user_id, src_path).await.map_err(workspace_to_tool)?;
    let sha256 = sha256_hex(&bytes);
    let id = Uuid::now_v7();
    let rx = state.devices().begin_transfer_wait(&row.token, generation, id).await;
    let begin = plexus_common::protocol::TransferBeginFrame {
        id,
        direction: plexus_common::protocol::TransferDirection::ServerToClient,
        src_device: SERVER_DEVICE.to_string(),
        src_path: src_path.to_string(),
        dst_device: row.name.clone(),
        dst_path: dst_path.to_string(),
        total_bytes: bytes.len() as u64,
        sha256: sha256.clone(),
        mime: None,
    };
    if !state.devices().send_if_current(&row.token, generation, plexus_common::protocol::WsFrame::TransferBegin(begin)).await {
        return Err(ToolError::DeviceUnreachable { device: row.name });
    }
    for chunk in bytes.chunks(64 * 1024) {
        if !state.devices().send_binary(&row.token, plexus_common::protocol::transfer::pack_chunk(id, chunk)).await {
            return Err(ToolError::DeviceUnreachable { device: row.name });
        }
    }
    let end = rx.await.map_err(|_| ToolError::DeviceUnreachable { device: row.name.clone() })?
        .map_err(|_| ToolError::DeviceUnreachable { device: row.name.clone() })?;
    if !end.ok {
        return Err(ToolError::InvalidArgs(end.error.unwrap_or_else(|| "transfer failed".to_string())));
    }
    if mode == "move" {
        state.workspace_fs().delete_file(user_id, src_path).await.map_err(workspace_to_tool)?;
    }
    let verb = if mode == "move" { "moved" } else { "copied" };
    Ok(ToolOutput::Text(format!("{verb} {src_path:?} from server to {dst_path:?} on {dst_device}")))
}
```

Add `sha256_hex` helper using `sha2::{Digest, Sha256}`.

- [ ] **Step 7: Dispatch transfer branch**

In `execute`, branch:

```rust
if src_device == SERVER_DEVICE && dst_device != SERVER_DEVICE {
    return server_to_client(state, user_id, src_path, dst_device, dst_path, mode).await;
}
```

- [ ] **Step 8: Run tests**

Run:

```bash
cargo test -p plexus-server --test m1f_file_transfer server_to_client_transfer_streams_file_to_device -- --nocapture
cargo test -p plexus-server --test m1f_file_transfer
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add plexus-server/src/devices plexus-server/src/tools/file_transfer.rs plexus-server/tests
git commit -m "feat: add server to client transfer"
```

---

### Task 11: Server `client -> server` File Transfer

**Files:**
- Modify: `plexus-server/src/devices/registry.rs`
- Modify: `plexus-server/src/devices/ws.rs`
- Modify: `plexus-server/src/tools/file_transfer.rs`
- Modify: `plexus-server/tests/m1f_file_transfer.rs`

- [ ] **Step 1: Write failing client-to-server transfer test**

Append to `plexus-server/tests/m1f_file_transfer.rs`:

```rust
#[tokio::test]
async fn client_to_server_transfer_streams_file_from_device() {
    let app = TestApp::spawn().await;
    let (jwt, user_id) = register_user(&app, "alice-client-server-transfer@example.com").await;
    set_quota(&app, 1_000_000).await;
    let token = support::create_device(&app, &jwt, "devbox").await;
    let base = app.spawn_server().await;
    let mut device = support::device_client::DeviceClient::connect(&base, Some(&token)).await;
    device.send_hello(plexus_common::version::PROTOCOL_VERSION).await;
    device.recv_hello_ack().await;

    let transfer = tokio::spawn({
        let state = app.state.clone();
        async move {
            plexus_server::tools::file_transfer::execute(
                &state,
                user_id,
                json!({
                    "plexus_src_device": "devbox",
                    "src_path": "src/a.txt",
                    "plexus_dst_device": "server",
                    "dst_path": "dst/a.txt",
                    "mode": "copy"
                }),
            )
            .await
            .unwrap()
        }
    });

    let begin = device.recv_transfer_begin().await;
    assert_eq!(begin.direction, plexus_common::protocol::TransferDirection::ClientToServer);
    device.send_binary(plexus_common::protocol::transfer::pack_chunk(begin.id, b"hello")).await;
    device.send_transfer_end_ok(begin.id, Some(begin.sha256.clone())).await;

    let output = transfer.await.unwrap();
    let plexus_server::tools::output::ToolOutput::Text(text) = output else { panic!("text") };
    assert!(text.contains("copied"));
    assert_eq!(support::read_workspace_file(&app, user_id, "dst/a.txt").await, "hello");
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test -p plexus-server --test m1f_file_transfer client_to_server_transfer_streams_file_from_device -- --nocapture
```

Expected: FAIL with current file_transfer not requesting inbound bytes.

- [ ] **Step 3: Add server inbound transfer slots**

In `DeviceRuntime`, add methods to register an inbound server-side writer:

```rust
pub async fn begin_inbound_transfer(
    &self,
    token: &str,
    generation: u64,
    id: Uuid,
    slot: InboundTransferSlot,
) -> bool
```

Keep the actual slot type in `plexus-server/src/tools/file_transfer.rs` if possible, not in registry. Registry should only route binary chunks by `id` to a channel:

```rust
type PendingBinarySender = mpsc::Sender<Vec<u8>>;
pending_binary: HashMap<PendingTransferKey, PendingBinarySender>,
```

Add:

```rust
pub async fn register_binary_receiver(
    &self,
    token: &str,
    generation: u64,
    id: Uuid,
    tx: mpsc::Sender<Vec<u8>>,
) { ... }

pub async fn complete_binary_chunk(
    &self,
    token: &str,
    generation: u64,
    id: Uuid,
    chunk: Vec<u8>,
) -> bool { ... }
```

- [ ] **Step 4: Route binary chunks in server WS**

Modify `devices/ws.rs` binary handling:

```rust
Message::Binary(bytes) => {
    let Ok((id, chunk)) = plexus_common::protocol::transfer::parse_chunk(&bytes) else {
        let _ = send_error(&state, &row.token, generation, ErrorCode::MalformedFrame, "malformed binary transfer frame").await;
        continue;
    };
    if !state.devices().complete_binary_chunk(&row.token, generation, id, chunk.to_vec()).await
        && !send_error(&state, &row.token, generation, ErrorCode::TransferUnknownId, "unknown transfer id").await
    {
        break;
    }
}
```

- [ ] **Step 5: Implement `client_to_server`**

In `plexus-server/src/tools/file_transfer.rs`, implement:

- reject existing destination before sending `TransferBegin`;
- create a temp partial path inside the user's server workspace;
- register a binary receiver channel for the transfer id;
- send `TransferBegin` with `ClientToServer`;
- consume chunks until `TransferEnd` arrives;
- write chunks through workspace-safe temp file handling;
- verify sha256 and bytes;
- write final bytes through `workspace_fs.write_file`;
- delete source on client for `move` by dispatching a client `delete_file` tool after a successful copy.

For Alpha, use an in-memory Vec only for tests smaller than 1 MiB if integrating with `workspace_fs.write_file` requires whole bytes. Add a comment and a follow-up note in the task implementation commit if `workspace_fs` lacks a streaming write API.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p plexus-server --test m1f_file_transfer client_to_server_transfer_streams_file_from_device -- --nocapture
cargo test -p plexus-server --test m1f_file_transfer
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add plexus-server/src/devices plexus-server/src/tools/file_transfer.rs plexus-server/tests/m1f_file_transfer.rs
git commit -m "feat: add client to server transfer"
```

---

### Task 12: End-to-End Client Alpha Smoke Test

**Files:**
- Create: `plexus-server/tests/client_alpha_e2e.rs`
- Modify: `plexus-server/tests/support/mod.rs` if helpers are missing.

- [ ] **Step 1: Write failing e2e test**

Create `plexus-server/tests/client_alpha_e2e.rs`:

```rust
mod support;

use axum::http::{Method, StatusCode};
use serde_json::json;
use support::{TestApp, fake_anthropic::FakeAnthropic, json_request, register_user};

#[tokio::test]
async fn real_client_alpha_loop_executes_file_tool_and_exec() {
    let fake = FakeAnthropic::tool_batch_then_text(
        vec![
            ("toolu_write", "write_file", json!({"plexus_device": "devbox", "path": "hello.txt", "content": "hello"})),
            ("toolu_exec", "exec", json!({"plexus_device": "devbox", "command": "printf alpha", "timeout": 2})),
        ],
        "client alpha ok",
    ).await;
    let app = TestApp::spawn_with_anthropic(fake.config()).await;
    let (jwt, _) = register_user(&app, "alice-client-alpha-e2e@example.com").await;
    let token = support::create_device(&app, &jwt, "devbox").await;
    let base = app.spawn_server().await;

    let client = tokio::spawn(async move {
        let config = plexus_client::config::StartupConfig::from_pairs([
            ("PLEXUS_SERVER_URL", base),
            ("PLEXUS_DEVICE_TOKEN", token),
        ]).unwrap();
        plexus_client::runtime::run_once_for_test(config).await.unwrap();
    });

    let session_id = support::create_web_session(&app, &jwt, "client alpha").await;
    let (status, _) = json_request(
        app.router.clone(),
        Method::POST,
        &format!("/api/sessions/{session_id}/messages"),
        json!({"effort": null, "content": [{"type": "text", "text": "use client"}], "attachments": []}),
        Some(&jwt),
    ).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    support::wait_for_assistant_text(&app, &session_id, "client alpha ok").await;
    client.abort();
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test -p plexus-server --test client_alpha_e2e -- --nocapture
```

Expected: FAIL until the server test target can depend on `plexus-client` and the runtime exits predictably in test mode.

- [ ] **Step 3: Add dev-dependency from server tests to client**

Modify `plexus-server/Cargo.toml`:

```toml
[dev-dependencies]
plexus-client = { path = "../plexus-client" }
```

If Cargo rejects this because `plexus-client` is bin-only, ensure `plexus-client/src/lib.rs` exports `config` and `runtime`.

- [ ] **Step 4: Make `run_once_for_test` exit after idle**

In `plexus-client/src/runtime.rs`, make `run_once_for_test` return after:

- server closes the socket;
- or a test-only idle timeout after processing all currently queued results.

Use a small timeout only in the test helper, not production `run`.

- [ ] **Step 5: Run e2e**

Run:

```bash
cargo test -p plexus-server --test client_alpha_e2e -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add plexus-server/Cargo.toml plexus-server/tests/client_alpha_e2e.rs plexus-client/src/runtime.rs
git commit -m "test: add client alpha e2e"
```

---

### Task 13: Docs Sync and Final Verification

**Files:**
- Modify docs only if implementation changed contracts:
  - `docs/PROTOCOL.md`
  - `docs/TOOLS.md`
  - `docs/DECISIONS.md`
  - `docs/reference/superpowers/specs/2026-05-28-plexus-client-alpha-design.md`
  - `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`

- [ ] **Step 1: Run focused client checks**

Run:

```bash
cargo test -p plexus-client
cargo check -p plexus-client
```

Expected: PASS.

- [ ] **Step 2: Run focused server/client checks**

Run:

```bash
cargo test -p plexus-server --test m1f_device_execution -- --nocapture
cargo test -p plexus-server --test m1f_file_transfer -- --nocapture
cargo test -p plexus-server --test client_alpha_e2e -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Run common protocol checks**

Run:

```bash
cargo test -p plexus-common
```

Expected: PASS.

- [ ] **Step 4: Run workspace compile and formatting**

Run:

```bash
cargo fmt --all -- --check
cargo check --workspace
```

Expected: PASS.

- [ ] **Step 5: Run docs diff checks**

Run:

```bash
git diff --check
! git diff -- docs/reference/superpowers/specs/2026-05-28-plexus-client-alpha-design.md docs/DECISIONS.md docs/TOOLS.md docs/PROTOCOL.md | rg -n '^\+.*(TB[D]|TO[D]O|FIXM[E]|~/.config/plexus)'
! git diff -- plexus-client Cargo.toml | rg -n '^\+.*(logout|client config dir(ectory)?|~/.config/plexus)'
```

Expected:

- `git diff --check` exits 0.
- Both negated `rg` commands exit 0 with no output, proving the current diff did not add placeholder markers, a client config directory, or a client logout path.

- [ ] **Step 6: Update M1 living design status**

If all checks and live smoke pass, update `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`:

```markdown
| `Client Alpha` | Verified | Minimal real `plexus-client`: env-only startup, device-token connection, `hello`/`hello_ack`, heartbeat/reconnect, server-pushed config, FIFO worker queue, shared file tools, `web_fetch`, one-shot `exec`, and server/client file transfer | `M1e`, `M1f` | Verified on 2026-05-28: real client e2e proved file tools, one-shot exec, and server/client file transfer through the normal agent loop |
```

If live smoke is not run yet, use `Implemented` instead of `Verified` and list the missing smoke in the final handoff.

- [ ] **Step 7: Commit docs/status**

```bash
git add docs plexus-client plexus-common plexus-server Cargo.toml Cargo.lock
git commit -m "docs: mark client alpha implementation status"
```

---

## Manual Curl/API Smoke

Run this after automated checks pass.

1. Start clean local stack.

```bash
docker compose down -v
docker compose up -d postgres
cargo run -p plexus-server
```

2. Register admin and configure LLM using the existing API smoke pattern from earlier M1 milestones.

3. Create a device.

```bash
curl -sS -X POST "$BASE/api/devices" \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -d '{"name":"devbox","workspace_path":"~/plexus/workspace","fs_policy":"sandbox","shell_timeout_max":60}'
```

4. Start client.

```bash
PLEXUS_SERVER_URL="$BASE" \
PLEXUS_DEVICE_TOKEN="$PLEXUS_DEVICE_TOKEN" \
RUST_LOG=info \
cargo run -p plexus-client -- run
```

5. Through `POST /api/sessions/{id}/messages`, ask the agent to:

- list files on `devbox`;
- write and read `hello.txt` on `devbox`;
- run `printf alpha` on `devbox` through `exec`;
- copy a server workspace file to `devbox`;
- copy a `devbox` file back to the server workspace.

6. Kill the client during a slow tool call and verify the transcript contains `device_unreachable`.

---

## Self-Review Checklist

- Spec coverage:
  - Env-only startup: Task 1.
  - WS lifecycle: Tasks 2 and 7.
  - FIFO worker: Task 3.
  - Shared file tools: Task 4.
  - One-shot exec: Task 5.
  - web_fetch: Task 6.
  - server/client file transfer: Tasks 8 through 11.
  - e2e acceptance: Task 12 and Manual Curl/API Smoke.
  - docs/status: Task 13.
- Deferred work remains deferred:
  - No local config dir.
  - No `logout`.
  - No long-running exec session.
  - No `client -> client` transfer bridge.
  - No MCP implementation.
  - No service installer.
- Type consistency:
  - WS frame types come from `plexus_common::protocol`.
  - Error codes come from `plexus_common::ErrorCode`.
  - Device config comes from `plexus_common::protocol::DeviceConfig`.
  - Binary chunks use `plexus_common::protocol::transfer`.
