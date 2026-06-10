# Plexus M0 — `plexus-common` Foundation Spec

**Status:** approved for implementation
**Branch:** `rebuild-m0`
**Authors:** brainstormed in collaborative session 2026-04-28
**Supersedes:** none — first implementation milestone of the rebuild

---

## 1. Goals

M0 builds the **`plexus-common` crate** — the shared foundation that both `plexus-server` (M1) and `plexus-client` (M2) depend on. The goal is to land **as much shared logic as possible in M0** so that common's public API surface can be **frozen** once M1 begins. M1 and M2 are then strictly additive in their own crates: they consume `plexus-common`, never modify it (except for genuine bug fixes).

End-of-M0 deliverable: a library crate that compiles cleanly under `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` (per ADR-102), passes ~140 unit tests + 3 integration tests, and is `cargo add`-ready for the M1 server crate.

There is **no demoable runtime at end of M0.** This is library code; the deliverable is `cargo test` passing and the API surface being intentional.

---

## 2. Decomposition Context

The Plexus rebuild splits into four milestones:

| Milestone | Crate | Demoable cut |
|---|---|---|
| **M0** (this spec) | `plexus-common` | None — library code; `cargo test` passes |
| M1 | `plexus-server` | User registers via web UI → chats with the agent → agent calls LLM and uses server-side tools (`read_file` / `write_file` / `edit_file` / `list_dir` / `glob` / `grep` / `web_fetch` / `cron`) on the user's personal workspace |
| M2 | `plexus-client` | User connects a client device → agent dispatches `exec` and per-device MCP tools to the device |
| M3 | frontend | Polished UX; chat UI, Settings → Devices download links, per-user SSE event handling |

Server-before-client is deliberate: server is a standalone working product, client is an extension. Building client first would mean writing a fake-server stub to test against — which is just M2's work in M1's clothes.

Frozen-common-after-M0 means M1 and M2 do NOT add to common except as genuine bug fixes. If during M1/M2 we discover something obviously belongs in common, that's an M0 underscope to flag and revisit, not a casual addition.

---

## 3. Module Layout

```
plexus-common/
├── Cargo.toml
└── src/
    ├── lib.rs              # top-level re-exports; thin facade
    ├── consts.rs           # reserved string prefixes + schema markers
    ├── version.rs          # PROTOCOL_VERSION + crate_version!() helper
    ├── secrets.rs          # secrecy newtypes (DeviceToken, JwtSecret, LlmApiKey)
    │
    ├── errors/             # ADR-046 — one file per typed error
    │   ├── mod.rs          # ErrorCode enum + Code trait (fn code(&self) -> ErrorCode)
    │   ├── workspace.rs    # WorkspaceError (NotFound, SoftLocked, UploadTooLarge, PathOutsideWorkspace, ...)
    │   ├── tool.rs         # ToolError (ExecTimeout, CommandDenied, EnvNotAllowed, McpUnavailable, ...)
    │   ├── auth.rs         # AuthError (TokenInvalid, ...)
    │   ├── protocol.rs     # ProtocolError (MalformedFrame, UnknownType, VersionMismatch, ...)
    │   ├── mcp.rs          # McpError (SchemaCollision, WithinServerCollision, SpawnFailed, ...)
    │   └── network.rs      # NetworkError (SsrfBlocked, DNSFailed, Timeout, HttpError)
    │
    ├── protocol/           # PROTOCOL.md — wire protocol structs
    │   ├── mod.rs
    │   ├── frames.rs       # all WS text frames as a serde-tagged enum WsFrame
    │   ├── transfer.rs     # binary frame header layout (16-byte UUID + chunk bytes)
    │   └── types.rs        # frame inner shapes — DeviceConfig, McpServerConfig, McpSchemas
    │
    ├── tools/              # TOOLS.md — shared tool infrastructure
    │   ├── mod.rs          # Tool trait + ToolResult + ToolContext
    │   ├── result.rs       # wrap_result() per ADR-095
    │   ├── path.rs         # resolve_in_workspace() per ADR-073/105
    │   ├── format.rs       # line-numbered output ("LINE_NUM|content"), head-only truncation, char clipping
    │   ├── schemas.rs      # all hardcoded tool schemas as LazyLock<serde_json::Value>
    │   └── validate.rs     # JSON-schema validation for incoming tool_call args
    │
    └── mcp/                # ADR-047/048/049/099/100/105 — shared MCP infrastructure
        ├── mod.rs
        ├── naming.rs       # mcp_<server>_<tool|resource_<n>|prompt_<n>> name builders + parsers
        ├── wrap.rs         # URI template parser → schema properties; prompt-output stringify
        ├── filter.rs       # enabled-glob matcher
        ├── session.rs      # McpSession wrapper around rmcp::ServiceExt + TokioChildProcess
        └── lifecycle.rs    # spawn_mcp() / teardown_mcp()
```

### Layout decisions

- **All hardcoded tool schemas live in `tools/schemas.rs`** as `LazyLock<serde_json::Value>` (Choice 1B), regardless of which crate dispatches them. Server's merger imports from here; client's dispatcher imports from here. Common owns the schema definitions as the single source of truth (per ADR-038), but does not implement any tool — implementations live in their owning crate.
- **Errors get one file per type** (ADR-046). The trait `Code` (`fn code(&self) -> ErrorCode`) lives in `errors/mod.rs`; every error type implements it for uniform wire-level mapping.
- **MCP infrastructure is its own top-level module**, parallel to `tools/`. MCP wrapping has enough cohesive surface area (naming, URI templates, prompt stringify, glob filtering, lifecycle, session) to justify a sibling rather than a nested subdirectory of `tools/`.
- **`protocol/types.rs` separates frame *contents* from frame *envelopes*.** `DeviceConfig` shows up in both `HelloAckFrame` and `ConfigUpdateFrame`; defining it once in `types.rs` and embedding by reference keeps `frames.rs` readable.
- **`secrets.rs` is its own file**, not folded into a misc utility module. One place to audit what the codebase considers a secret.

---

## 4. Key API Surface Choices

### 4.1 Forced choices (no real tradeoff)

- **Async runtime: `tokio`.** The `Tool` trait is `async fn execute(&self, args, ctx) -> ToolResult`. All IO-bound primitives (filesystem, subprocess spawn, rmcp call) are async.
- **Serde for ser/de.** WS frames are an internally-tagged enum:
  ```rust
  #[derive(Serialize, Deserialize)]
  #[serde(tag = "type")]
  pub enum WsFrame {
      #[serde(rename = "hello")]        Hello(HelloFrame),
      #[serde(rename = "tool_call")]    ToolCall(ToolCallFrame),
      #[serde(rename = "register_mcp")] RegisterMcp(RegisterMcpFrame),
      // ...
  }
  ```
  Both server and client serialize/deserialize the same enum.
- **`thiserror` for error derives.** Library code; no `anyhow`. Each error type uses `#[derive(Debug, thiserror::Error)]` for `Display` + `Error`. The `Code` trait is added separately.
- **`uuid::v7` for IDs.** Frame correlation, message ids, transfer slots. Time-sortable.
- **`jsonschema` crate for tool-arg validation.** Most popular; supports JSON Schema draft 2019/2020.
- **`globset` for the `enabled` filter** (ADR-100). Compiled glob matchers; same crate Cargo uses.
- **Rust MSRV: 1.90+.** Required for stable `LazyLock` and other features the crate uses.

### 4.2 Tool schema representation — Choice 1 / Option B (locked)

Tool schemas are exposed as `LazyLock<serde_json::Value>` parsed once at startup:

```rust
use std::sync::LazyLock;

pub static READ_FILE_SCHEMA: LazyLock<serde_json::Value> = LazyLock::new(|| {
    serde_json::json!({
        "name": "read_file",
        "description": "...",
        "input_schema": { ... }
    })
});
```

**Rationale:** the `serde_json::json!` macro provides compile-time JSON syntax checking — typos surface at `cargo check` time, not at runtime. Schemas are parsed exactly once per process, accessed via `&*READ_FILE_SCHEMA`. Zero runtime startup cost (`LazyLock` is one atomic check per access).

### 4.3 MCP session wrapping — Choice 2 / Option B (locked)

Common provides a thin wrapper around `rmcp` that hides the underlying types:

```rust
pub struct McpSession {
    inner: rmcp::RunningService<RoleClient, ()>,  // private
}

impl McpSession {
    pub async fn list_tools(&self) -> Result<Vec<ToolDef>, McpError>;
    pub async fn list_resources(&self) -> Result<Vec<ResourceDef>, McpError>;
    pub async fn list_prompts(&self) -> Result<Vec<PromptDef>, McpError>;
    pub async fn call_tool(&self, name: &str, args: serde_json::Value) -> Result<String, McpError>;
    pub async fn read_resource(&self, uri: &str) -> Result<String, McpError>;
    pub async fn get_prompt(&self, name: &str, args: serde_json::Value) -> Result<String, McpError>;
}
```

`get_prompt` returns the joined-string form per ADR-048's prompt-output convention (matches nanobot `mcp.py:408–421`).

**Rationale:** rmcp version drift only affects common; server and client are insulated. Easy to mock for tests. Common owns exactly the API surface used (~6 methods).

### 4.4 Crate dependencies (Cargo.toml sketch)

```toml
[package]
name = "plexus-common"
version = "0.1.0"
edition = "2024"
rust-version = "1.90"

[dependencies]
tokio = { version = "1", features = ["fs", "process", "macros", "rt-multi-thread", "time"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
uuid = { version = "1", features = ["v7", "serde"] }
secrecy = { version = "0.10", features = ["serde"] }
jsonschema = "0.30"
globset = "0.4"
rmcp = { version = "=1.5.0", features = ["client", "transport-child-process"] }  # pinned to 1.5.0; bumps only between milestones, never mid-implementation
zeroize = "1"   # transitive via secrecy but worth pinning

[dev-dependencies]
proptest = "1"
pretty_assertions = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "test-util"] }
```

No `anyhow` (library, not application). No HTTP client (server has reqwest, client has tokio-tungstenite). No DB driver (server-only). Total: ~10 direct dependencies, all common-Rust-ecosystem stuff that compiles cleanly under musl per ADR-102.

---

## 5. Testing Strategy

Common is pure-Rust library code — no DB, no HTTP, no LLM calls — so test coverage should be heavy.

### 5.1 Unit tests (in-file `#[cfg(test)] mod tests`)

| Module | Test focus | Approx count |
|---|---|---|
| `protocol/frames.rs` | ser/de roundtrip for every `WsFrame` variant | ~25 |
| `protocol/transfer.rs` | Binary header layout: pack/unpack, edge cases | ~5 |
| `errors/*.rs` | Variant → unique `ErrorCode`; sensible `Display` strings | ~7 |
| `tools/path.rs` | `resolve_in_workspace()`: relative, absolute-inside, absolute-outside, symlink escape, `..` traversal, empty path, root-missing | ~15 |
| `tools/result.rs` | `wrap_result(empty/plain/already-wrapped)` | ~5 |
| `tools/format.rs` | Line numbering, head-only truncation, multi-byte char boundaries | ~10 |
| `tools/schemas.rs` | Every `LazyLock<Value>` parses; every schema validates against meta-schema | ~15 |
| `tools/validate.rs` | Sample valid args validate; invalid args reject | ~14 |
| `mcp/naming.rs` | Wrap-name round-trips; collision detection | ~10 |
| `mcp/wrap.rs` | URI template parsing + substitution; prompt-output stringify | ~12 |
| `mcp/filter.rs` | `enabled` glob: literals, wildcards, mixed lists, default-allow | ~8 |
| `secrets.rs` | `Debug`/`Display` redact; `ExposeSecret` reveals; `Drop` zeroizes | ~6 |

Total: **~140 unit tests**, all running in `cargo test --workspace -p plexus-common`, no external dependencies.

### 5.2 Integration tests (`plexus-common/tests/`)

- **`tests/mcp_lifecycle.rs`** — `spawn_mcp()` spawns a tiny test-only stdio subprocess (a Rust binary in `tests/fixtures/fake-mcp/` that responds to `list_tools` etc., ~100 LoC), call all six `McpSession` methods, then `teardown_mcp()`. Verifies the rmcp wrapper end-to-end without requiring real MCP servers.
- **`tests/end_to_end_schema_pipeline.rs`** — Tool schema → JSON-schema-validate sample args → wrap result → ser/de through `tool_result` frame → deserialize. Catches breaks at module boundaries.
- **`tests/secret_no_leak.rs`** — Construct structs with secret fields, format via `{:?}`, assert no token-shaped string (`plexus_dev_*`, JWT-looking) appears. Implements the redaction guardrail from ADR-104's logging section.

### 5.3 Property tests (`proptest`, used selectively)

- `protocol/frames.rs` — generate arbitrary `WsFrame` instances; assert ser/de roundtrip identity.
- `tools/path.rs` — generate arbitrary path strings; assert `resolve_in_workspace` either succeeds-with-path-inside-root or rejects with `PathOutsideWorkspace`.

Two invocations across the crate. Not a heavy investment.

### 5.4 What we're NOT testing in M0

- Real MCP server tests (no `npx @modelcontextprotocol/server-*` in the test environment).
- Actual file IO performance.
- Mutation testing or fuzz testing.
- Coverage threshold gates (aim for ~85% but don't fail CI on missed lines).

### 5.5 CI

- `cargo test --workspace -p plexus-common`
- `cargo clippy --workspace -p plexus-common -- -D warnings`
- `cargo fmt --check`
- `cargo build --target x86_64-unknown-linux-musl -p plexus-common`
- `cargo build --target aarch64-unknown-linux-musl -p plexus-common`

GitHub Actions, single `.github/workflows/ci.yml`, ~50 lines. Linux-only matrix in M0; macOS/Windows targets land with M2 client.

---

## 6. Out of Scope (Explicit)

### 6.1 Server-only — lands in M1
- DB layer, schema.sql, sqlx access patterns
- HTTP handlers (axum routes), API.yaml endpoint impls
- JWT issuance + verification
- Channel adapters (Discord/Telegram)
- Agent loop / orchestrator (ADR-031, ADR-036)
- LLM client (OpenAI chat completions per ADR-101)
- Compaction (`tiktoken-rs` + two-stage logic per ADR-028)
- System prompt builder (ADR-022/023)
- `workspace_fs` (quota enforcement, skills validation, write ordering)
- Cron scheduler + heartbeat ticker (ADR-053/054)
- Per-user SSE event broadcaster (ADR-106)
- Tool schema merger (ADR-071)
- DB row types (`User`, `Session`, `Message`, `ContentBlock`, `CronJob`, `RuntimeBlock`)

### 6.2 Client-only — lands in M2
- WS connection state machine + worker queue (ADR-105)
- bwrap sandbox wrapper (Linux, ADR-073)
- `exec` subprocess wrapper
- File-transfer slot manager
- Reconnect-with-backoff loop
- CLI parsing (`clap`)
- Logging setup (`tracing-subscriber` config)
- Config dir handling (`~/.config/plexus/`)
- `register_mcp` snapshot builder (ADR-105 worker side)
- Auto-mkdir workspace on hello_ack

### 6.3 Frontend — lands in M3
- React/Vite app; embedded via `rust-embed` in server binary
- Chat UI, Devices tab, Settings pages

### 6.4 Non-features in v1 entirely (per existing ADRs)
- Multi-server multiplexing in client (ADR-103)
- Subagents / agent-spawning (ADR-062)
- Dream module (ADR-055/063)
- Whisper/ASR (ADR-064)
- Real migrations framework (ADR-069)
- Horizontal-scale coordination (ADR-061/070)

---

## 7. Acceptance Criteria

### 7.1 Code
- [ ] All 16 items implemented in their respective modules.
- [ ] Public API surface (`lib.rs` re-exports + module-level `pub`) reviewed and intentional.
- [ ] Zero `.unwrap()` / `.expect()` in non-test code. Every fallible path returns a typed error.
- [ ] Zero `panic!()` outside `#[cfg(test)]` and `unreachable!()`.
- [ ] No `unsafe` code.

### 7.2 Tests
- [ ] ~140 unit tests passing.
- [ ] 3 integration tests passing.
- [ ] `proptest` cases for frame ser/de + path validation pass at default 256 iterations.
- [ ] Fake-mcp subprocess fixture compiles and runs on Linux x86_64.
- [ ] All doctests pass (`cargo test --doc`).

### 7.3 Build
- [ ] `cargo build --target x86_64-unknown-linux-musl -p plexus-common` succeeds.
- [ ] `cargo build --target aarch64-unknown-linux-musl -p plexus-common` succeeds.
- [ ] `cargo clippy --workspace -p plexus-common -- -D warnings` clean.
- [ ] `cargo fmt --check` clean.
- [ ] `cargo doc --no-deps -p plexus-common` builds without warnings.

### 7.4 Dependencies
- [ ] `Cargo.toml` matches the §4.4 sketch.
- [ ] `Cargo.lock` committed.
- [ ] All deps build cleanly under musl.

### 7.5 Documentation
- [ ] `plexus-common/README.md` — 1-page overview.
- [ ] Doc comments on every public type, trait, and function. Important behaviors include `# Examples`.

### 7.6 CI
- [ ] `.github/workflows/ci.yml` runs the full check matrix on push.
- [ ] CI green on `rebuild-m0` HEAD.

### 7.7 Versioning
- [ ] Workspace `version = "0.1.0"` per ADR-107.
- [ ] No git tag at end of M0 (we tag at M1 when there's a runnable artifact).

### 7.8 Handoff to M1
- [ ] M1 server crate can `cargo add plexus-common = { path = "../plexus-common" }` and import every needed type without compile errors.
- [ ] M1 doesn't need to add new public APIs to common in its first week. If it does, that's a sign M0 was under-scoped — flag and revisit.

---

## 8. Cross-References to Architecture ADRs

This spec implements the following ADRs in `docs/DECISIONS.md`:

| ADR | Topic | Relevant module(s) |
|---|---|---|
| ADR-001 | Three-crate workspace | (workspace structure) |
| ADR-038 | File tool schemas in plexus-common | `tools/schemas.rs` |
| ADR-046 | All typed errors in plexus-common | `errors/` |
| ADR-047 | Shared MCP client + three surfaces | `mcp/session.rs`, `mcp/lifecycle.rs` |
| ADR-048 | MCP wrapping naming + prompt output | `mcp/naming.rs`, `mcp/wrap.rs` |
| ADR-049 | MCP collision rejection types | `errors/mcp.rs` (error variants) |
| ADR-073 | File-tool jail (in-process Rust path validation) | `tools/path.rs` |
| ADR-077 | Tool trait | `tools/mod.rs` |
| ADR-095 | Untrusted tool result wrap | `tools/result.rs` |
| ADR-096 | WS protocol headlines | `protocol/frames.rs` |
| ADR-099 | URI template expansion | `mcp/wrap.rs` |
| ADR-100 | `enabled` filter uniform | `mcp/filter.rs` |
| ADR-101 | LLM provider strategy | `secrets.rs` (`LlmApiKey` newtype) |
| ADR-102 | Distribution targets | (build config, CI) |
| ADR-104 | Logging + `secrecy` | `secrets.rs` |
| ADR-105 | MCP subprocess lifecycle | `mcp/lifecycle.rs` |
| ADR-106 | Per-user SSE events | (server-only, but `UserEvent*` types may live in `protocol/types.rs` if both sides need them — to be confirmed during implementation) |
| ADR-107 | Versioning policy | `version.rs` |

---

## 9. Risk Registry

Risks specific to M0 worth tracking. All five accepted; mitigations applied at the points below.

| Risk | Status | Mitigation |
|---|---|---|
| **rmcp API churn during M0** — rmcp is young; breaking changes between releases possible | accepted | **Pinned to `rmcp = "=1.5.0"`** (exact-version constraint) in `Cargo.toml`. Update only between milestones, never mid-implementation. |
| **JSON Schema validation crate (`jsonschema`) compile times** — heavy deps possible | accepted | Compile times accepted as-is. No fallback alternative explored in M0; revisit only if it becomes a measured pain point. |
| **Cross-platform fake-mcp fixture** — stdio subprocess on Windows has CRLF and async-IO quirks | accepted | Fixture is Linux-only in M0 per the testing strategy; Windows verification deferred to M2 when the client crate adds Windows code paths. |
| **`secrecy` crate's `serde` integration** — `SecretString` via serde requires care to avoid leaking through serialization | accepted | Audit every `Serialize` impl that includes a secret field; either skip the field or use a manually-redacting impl. Tests in `tests/secret_no_leak.rs` cover the common cases. |
| **Public API surface drift during implementation** — easy to add small public items piecemeal | accepted | Plan 3 closing pass explicitly walks `lib.rs` re-exports + every `pub` in submodules and asks "should this be public?" before signoff. |

---

## 10. Implementation Order

M0 is split into **three sequential plans**, each independently committable. The plans share this one spec; each plan turns its scope into a granular task list via `superpowers:writing-plans`.

| Plan | Scope | Modules | Estimated effort |
|---|---|---|---|
| **Plan 1 — Foundation + Protocol** | Workspace skeleton, foundation types, wire format | `consts.rs`, `version.rs`, `secrets.rs`, `errors/`, `protocol/` (all 3 files), CI YAML | ~1 week |
| **Plan 2 — Tools** | All shared tool infrastructure | `tools/` (all 6 files), `tests/end_to_end_schema_pipeline.rs`, `tests/secret_no_leak.rs` | ~1 week |
| **Plan 3 — MCP + Polish** | rmcp wrapper + lifecycle + final polish | `mcp/` (all 5 files), `tests/fixtures/fake-mcp/`, `tests/mcp_lifecycle.rs`, `README.md`, doc-comment pass, public-API audit | ~1-2 weeks |

Each plan's exit criterion: subset of §7 acceptance criteria checked, CI green for the modules in scope. End of Plan 3 = M0 done = full §7 satisfied.

### Suggested sequence within each plan

Below is the granular ordering across all three plans, used as the input scaffold when each plan is expanded into a `writing-plans` task list:

1. **Workspace skeleton** — `Cargo.toml`, `plexus-common/Cargo.toml`, basic `lib.rs`, CI YAML stub. Compile-check only.
2. **`consts.rs` + `version.rs`** — trivial; gets the simplest content in.
3. **`secrets.rs`** — establishes the secret-handling pattern early. Drives `tests/secret_no_leak.rs`.
4. **`errors/`** — all 6 error types + `ErrorCode` + `Code` trait. Foundation for everything fallible downstream.
5. **`protocol/types.rs`** — `DeviceConfig`, `McpServerConfig`, `McpSchemas`. Pure data structures, no logic.
6. **`protocol/frames.rs`** — full `WsFrame` enum + variants. ser/de roundtrip tests catch issues immediately.
7. **`protocol/transfer.rs`** — binary header layout. Small.
8. **`tools/result.rs`, `tools/path.rs`, `tools/format.rs`** — pure helpers, easy to test thoroughly.
9. **`tools/schemas.rs`** — all hardcoded schemas. Mostly mechanical translation from `docs/TOOLS.md`.
10. **`tools/mod.rs`** (Tool trait) + **`tools/validate.rs`** — depends on schemas being defined.
11. **`mcp/naming.rs`, `mcp/filter.rs`, `mcp/wrap.rs`** — pure logic, no rmcp dependency.
12. **`mcp/session.rs`** — rmcp wrapper. First module to introduce real rmcp usage.
13. **`mcp/lifecycle.rs`** — `spawn_mcp` / `teardown_mcp`. Depends on `session.rs`.
14. **Integration tests** — `tests/mcp_lifecycle.rs` (with fake-mcp fixture), `tests/end_to_end_schema_pipeline.rs`, `tests/secret_no_leak.rs`.
15. **Documentation pass** — `README.md`, doctests, rustdoc cleanup.
16. **CI green-up** — clippy, fmt, all targets build.

Total estimate: **~3-4 weeks of focused work** at one engineer.
