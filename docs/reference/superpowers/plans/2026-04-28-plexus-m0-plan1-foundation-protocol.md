# Plexus M0 — Plan 1: Foundation + Protocol — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the workspace skeleton, foundational types (consts, version, secrets), all six typed error enums, and the WebSocket protocol structs (frame inner types, binary transfer header, full `WsFrame` enum) for the `plexus-common` crate.

**Architecture:** Library-only Rust crate. Pure data types, ser/de via `serde`, errors via `thiserror`, secret redaction via the `secrecy` crate. No async, no IO, no rmcp dependency yet — those land in Plans 2 and 3. This plan establishes the foundation that everything in M0/M1/M2 builds on.

**Tech Stack:** Rust 1.90+, edition 2024. Crates: `serde` + `serde_json`, `thiserror`, `uuid` (v7), `secrecy`, `zeroize`, `proptest` + `pretty_assertions` (test-only).

**Spec:** [docs/reference/superpowers/specs/2026-04-28-plexus-m0-design.md](../specs/2026-04-28-plexus-m0-design.md)

**Branch:** `rebuild-m0` (already checked out per the brainstorming step that created this plan).

---

## File map

Files this plan creates:

| Path | Responsibility |
|---|---|
| `Cargo.toml` (workspace root) | Workspace config, shared deps, MSRV |
| `.github/workflows/ci.yml` | CI: test + clippy + fmt + musl builds (× 2 archs) |
| `.gitignore` (extension) | Ignore `target/` |
| `plexus-common/Cargo.toml` | Crate metadata + per-crate deps via workspace inheritance |
| `plexus-common/src/lib.rs` | Top-level re-exports; thin facade |
| `plexus-common/src/consts.rs` | Reserved string prefixes + schema markers (ADR-095, ADR-007, ADR-071) |
| `plexus-common/src/version.rs` | `PROTOCOL_VERSION` + `crate_version!()` macro (ADR-107) |
| `plexus-common/src/secrets.rs` | `DeviceToken`, `JwtSecret`, `LlmApiKey`, `McpEnvSecret` newtypes (ADR-104) |
| `plexus-common/src/errors/mod.rs` | `ErrorCode` enum + `Code` trait (ADR-046) |
| `plexus-common/src/errors/workspace.rs` | `WorkspaceError` |
| `plexus-common/src/errors/tool.rs` | `ToolError` |
| `plexus-common/src/errors/auth.rs` | `AuthError` |
| `plexus-common/src/errors/protocol.rs` | `ProtocolError` |
| `plexus-common/src/errors/mcp.rs` | `McpError` |
| `plexus-common/src/errors/network.rs` | `NetworkError` |
| `plexus-common/src/protocol/mod.rs` | Module facade |
| `plexus-common/src/protocol/types.rs` | `DeviceConfig`, `McpServerConfig`, `McpSchemas` and inner shapes |
| `plexus-common/src/protocol/transfer.rs` | Binary frame header (16-byte UUID + payload) |
| `plexus-common/src/protocol/frames.rs` | All `WsFrame` variants (Hello, ToolCall, ToolResult, RegisterMcp, etc.) |

Total: ~18 files, ~1700 LoC of code, ~600 LoC of tests.

---

## Conventions

- **Tests live in the same file as the code**, in a `#[cfg(test)] mod tests { ... }` block at the bottom. Integration tests in `tests/` come in Plan 2.
- **Run all tests** via: `cargo test --workspace -p plexus-common`. Run a single test via: `cargo test --workspace -p plexus-common <test_name>`.
- **Commit after every passing task**, not in batches. Frequent commits = small reverts when something goes sideways.
- **Cargo working dir is the repository root** (`/home/yucheng/Documents/GitHub/Plexus`) for all commands in this plan.
- Code in this plan has minimal comments — only added where the WHY is non-obvious. Don't add narrative comments per the project's `CLAUDE.md` guidelines.

---

### Task 1: Workspace skeleton

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `plexus-common/Cargo.toml`
- Create: `plexus-common/src/lib.rs`
- Modify: `.gitignore`

- [ ] **Step 1: Update `.gitignore`**

Read the current `.gitignore` first (it exists). Add the lines below if not already present:

```
# Rust build artifacts
/target
**/*.rs.bk
Cargo.lock.bak
```

Note: `Cargo.lock` is NOT ignored — for binary crates and workspaces with binary outputs, Cargo.lock should be committed. This crate is a library but the workspace will have binaries (server, client) later, so we commit Cargo.lock.

- [ ] **Step 2: Create the workspace root Cargo.toml**

Create `Cargo.toml` at the repo root:

```toml
[workspace]
members = ["plexus-common"]
resolver = "2"

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.90"
license = "Apache-2.0"
authors = ["Plexus Authors"]
repository = "https://github.com/yucheng/plexus"

[workspace.dependencies]
# Async runtime — used in Plan 2 (Tool trait) and beyond
tokio = { version = "1", features = ["fs", "process", "macros", "rt-multi-thread", "time"] }

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Errors
thiserror = "2"

# IDs
uuid = { version = "1", features = ["v7", "serde"] }

# Secrets
secrecy = { version = "0.10", features = ["serde"] }
zeroize = "1"

# JSON Schema validation — used in Plan 2
jsonschema = "0.30"

# Glob matching for `enabled` filter — used in Plan 3
globset = "0.4"

# MCP client — pinned to exact version per spec §9 risk registry
rmcp = { version = "=1.5.0", features = ["client", "transport-child-process"] }

# Test-only
proptest = "1"
pretty_assertions = "1"

[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
strip = "symbols"
```

- [ ] **Step 3: Create `plexus-common/Cargo.toml`**

Create the directory and file:

```bash
mkdir -p plexus-common/src
```

Create `plexus-common/Cargo.toml`:

```toml
[package]
name = "plexus-common"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
authors.workspace = true
repository.workspace = true
description = "Shared types, errors, protocol, and tool infrastructure for the Plexus rebuild"

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
secrecy = { workspace = true }
zeroize = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
pretty_assertions = { workspace = true }
```

Note we don't add `tokio`, `jsonschema`, `globset`, or `rmcp` here yet — they're for Plans 2/3. YAGNI.

- [ ] **Step 4: Create the empty `lib.rs`**

Create `plexus-common/src/lib.rs`:

```rust
//! Shared types, errors, protocol, and tool infrastructure for Plexus.
//!
//! See `docs/reference/superpowers/specs/2026-04-28-plexus-m0-design.md` for the full
//! design and `docs/DECISIONS.md` for cross-cutting architecture decisions.
```

- [ ] **Step 5: Verify cargo build**

Run: `cargo build --workspace`

Expected: succeeds with output like:
```
   Compiling plexus-common v0.1.0 (...)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in <X>s
```

If you see "missing field `name` in `[package]`" or similar errors, double-check the Cargo.toml files match exactly.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml plexus-common/Cargo.toml plexus-common/src/lib.rs .gitignore
git commit -m "chore: workspace skeleton + plexus-common crate stub"
```

---

### Task 2: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create the workflow directory**

```bash
mkdir -p .github/workflows
```

- [ ] **Step 2: Create the CI YAML**

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main, rebuild, rebuild-m0, rebuild-m1, rebuild-m2, rebuild-m3]
  pull_request:
    branches: [main, rebuild]

env:
  CARGO_TERM_COLOR: always
  RUSTFLAGS: "-D warnings"

jobs:
  fmt:
    name: cargo fmt
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt
      - run: cargo fmt --all --check

  clippy:
    name: cargo clippy
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy
      - uses: Swatinem/rust-cache@v2
      - run: cargo clippy --workspace --all-targets -- -D warnings

  test:
    name: cargo test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --workspace --all-targets

  build-musl:
    name: cargo build (musl)
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64-unknown-linux-musl, aarch64-unknown-linux-musl]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}
      - name: Install musl-tools
        run: sudo apt-get update && sudo apt-get install -y musl-tools
      - name: Install cross compiler (aarch64)
        if: matrix.target == 'aarch64-unknown-linux-musl'
        run: sudo apt-get install -y gcc-aarch64-linux-gnu
      - uses: Swatinem/rust-cache@v2
      - run: cargo build --workspace --target ${{ matrix.target }}
        env:
          CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER: aarch64-linux-gnu-gcc
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add GitHub Actions workflow (fmt + clippy + test + musl builds)"
```

CI won't actually run anything until pushed, but locally we can dry-run the same commands in later tasks.

---

### Task 3: `consts.rs` — reserved string prefixes

Implements the literal string constants from ADR-007, ADR-071, ADR-095. Used by the result-wrap helper (Plan 2) and the schema merger (server-only, M1).

**Files:**
- Create: `plexus-common/src/consts.rs`
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Write a test for the constants (TDD-light: lock the literal strings)**

Add this to `plexus-common/src/lib.rs`:

```rust
//! Shared types, errors, protocol, and tool infrastructure for Plexus.
//!
//! See `docs/reference/superpowers/specs/2026-04-28-plexus-m0-design.md` for the full
//! design and `docs/DECISIONS.md` for cross-cutting architecture decisions.

pub mod consts;
```

Create `plexus-common/src/consts.rs`:

```rust
//! Reserved string constants used across the protocol and tool layers.
//!
//! These are wire-level conventions — changing any of them is a breaking
//! protocol change.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untrusted_message_prefix_format() {
        assert_eq!(
            UNTRUSTED_MESSAGE_PREFIX_TEMPLATE,
            "[untrusted message from {}]: "
        );
    }

    #[test]
    fn untrusted_tool_result_prefix() {
        assert_eq!(UNTRUSTED_TOOL_RESULT_PREFIX, "[untrusted tool result]: ");
    }

    #[test]
    fn plexus_field_prefix() {
        assert_eq!(PLEXUS_FIELD_PREFIX, "plexus_");
    }

    #[test]
    fn plexus_device_marker() {
        assert_eq!(PLEXUS_DEVICE_MARKER, "x-plexus-device");
    }

    #[test]
    fn device_token_prefix() {
        assert_eq!(DEVICE_TOKEN_PREFIX, "plexus_dev_");
    }
}
```

- [ ] **Step 2: Run the test — should fail (consts not defined)**

Run: `cargo test --workspace -p plexus-common consts::`

Expected: COMPILE FAILURE — `cannot find value 'UNTRUSTED_MESSAGE_PREFIX_TEMPLATE'`. That's the failing test.

- [ ] **Step 3: Add the constants above the `#[cfg(test)]` block**

Edit `plexus-common/src/consts.rs` to add the constants:

```rust
//! Reserved string constants used across the protocol and tool layers.
//!
//! These are wire-level conventions — changing any of them is a breaking
//! protocol change.

/// Template for the untrusted-message wrap (ADR-007).
///
/// When a channel adapter receives a message from a non-partner, it prepends
/// this string (with the sender's display name substituted) to the content.
/// The `{}` is the sender name — adapters call `format!()` with it.
pub const UNTRUSTED_MESSAGE_PREFIX_TEMPLATE: &str = "[untrusted message from {}]: ";

/// Prefix wrapped onto every tool result before the LLM sees it (ADR-095).
pub const UNTRUSTED_TOOL_RESULT_PREFIX: &str = "[untrusted tool result]: ";

/// Reserved prefix for fields the merger injects into tool schemas (ADR-071).
///
/// MCP authors must not use this prefix on their own schema fields.
pub const PLEXUS_FIELD_PREFIX: &str = "plexus_";

/// Marker the merger sets on routing-only schema fields (ADR-071).
///
/// Used to distinguish merge-time-injected fields from intrinsic-device
/// fields when the schema is later modified.
pub const PLEXUS_DEVICE_MARKER: &str = "x-plexus-device";

/// Prefix for device tokens (ADR-091, ADR-097).
///
/// The full token format is `plexus_dev_<random base64 hash>`.
pub const DEVICE_TOKEN_PREFIX: &str = "plexus_dev_";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untrusted_message_prefix_format() {
        assert_eq!(
            UNTRUSTED_MESSAGE_PREFIX_TEMPLATE,
            "[untrusted message from {}]: "
        );
    }

    #[test]
    fn untrusted_tool_result_prefix() {
        assert_eq!(UNTRUSTED_TOOL_RESULT_PREFIX, "[untrusted tool result]: ");
    }

    #[test]
    fn plexus_field_prefix() {
        assert_eq!(PLEXUS_FIELD_PREFIX, "plexus_");
    }

    #[test]
    fn plexus_device_marker() {
        assert_eq!(PLEXUS_DEVICE_MARKER, "x-plexus-device");
    }

    #[test]
    fn device_token_prefix() {
        assert_eq!(DEVICE_TOKEN_PREFIX, "plexus_dev_");
    }
}
```

- [ ] **Step 4: Run the test — should pass**

Run: `cargo test --workspace -p plexus-common consts::`

Expected: 5 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/consts.rs plexus-common/src/lib.rs
git commit -m "feat(common): add reserved string constants module"
```

---

### Task 4: `version.rs` — protocol + crate version

**Files:**
- Create: `plexus-common/src/version.rs`
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Write the test**

Create `plexus-common/src/version.rs`:

```rust
//! Version constants. See ADR-107.
//!
//! The protocol version is the WS wire-protocol version. The binary release
//! version comes from `Cargo.toml`'s `version` field at compile time.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_is_v1() {
        assert_eq!(PROTOCOL_VERSION, "1");
    }

    #[test]
    fn crate_version_is_zero_one() {
        assert_eq!(crate_version(), "0.1.0");
    }
}
```

Add to `plexus-common/src/lib.rs`:

```rust
pub mod version;
```

- [ ] **Step 2: Run the test — should fail**

Run: `cargo test --workspace -p plexus-common version::`

Expected: compile failure (`PROTOCOL_VERSION` and `crate_version` undefined).

- [ ] **Step 3: Implement**

Edit `plexus-common/src/version.rs`:

```rust
//! Version constants. See ADR-107.
//!
//! The protocol version is the WS wire-protocol version. The binary release
//! version comes from `Cargo.toml`'s `version` field at compile time.

/// The current Plexus WebSocket protocol version (ADR-107).
///
/// Bumps only when the wire format changes in a breaking way. Most binary
/// releases do NOT bump this — see ADR-107 for the rules.
pub const PROTOCOL_VERSION: &str = "1";

/// Returns the running crate's version (e.g. `"0.1.0"`) as set in `Cargo.toml`.
///
/// Read at compile time via `env!("CARGO_PKG_VERSION")` — no runtime cost.
pub fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_version_is_v1() {
        assert_eq!(PROTOCOL_VERSION, "1");
    }

    #[test]
    fn crate_version_is_zero_one() {
        assert_eq!(crate_version(), "0.1.0");
    }
}
```

- [ ] **Step 4: Run the test — should pass**

Run: `cargo test --workspace -p plexus-common version::`

Expected: 2 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/version.rs plexus-common/src/lib.rs
git commit -m "feat(common): add version constants (PROTOCOL_VERSION + crate_version)"
```

---

### Task 5: `secrets.rs` — redacting newtypes

Implements four newtypes around `secrecy::SecretString`. Each type's `Debug`/`Display` impls redact (via `SecretString`), and the `secrecy::ExposeSecret` trait gives controlled access to the inner value.

**Files:**
- Create: `plexus-common/src/secrets.rs`
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Write the test for redaction**

Create `plexus-common/src/secrets.rs`:

```rust
//! Newtypes for secret values, wrapping `secrecy::SecretString`.
//!
//! All four types' `Debug`/`Display` impls redact. Exposing the inner value
//! requires explicit `secrecy::ExposeSecret`. See ADR-104.

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn device_token_debug_redacts() {
        let token = DeviceToken::new("plexus_dev_abc123secret".into());
        let dbg = format!("{:?}", token);
        assert!(!dbg.contains("abc123secret"), "Debug leaked secret: {}", dbg);
    }

    #[test]
    fn device_token_display_redacts() {
        let token = DeviceToken::new("plexus_dev_abc123secret".into());
        let disp = format!("{}", token);
        assert!(!disp.contains("abc123secret"), "Display leaked secret: {}", disp);
    }

    #[test]
    fn device_token_expose_secret_returns_value() {
        let token = DeviceToken::new("plexus_dev_abc123secret".into());
        assert_eq!(token.expose_secret(), "plexus_dev_abc123secret");
    }

    #[test]
    fn jwt_secret_redacts() {
        let secret = JwtSecret::new("my-jwt-secret".into());
        let dbg = format!("{:?}", secret);
        assert!(!dbg.contains("jwt-secret"), "Debug leaked: {}", dbg);
    }

    #[test]
    fn llm_api_key_redacts() {
        let key = LlmApiKey::new("sk-proj-actualsecretkey".into());
        let dbg = format!("{:?}", key);
        assert!(!dbg.contains("actualsecretkey"), "Debug leaked: {}", dbg);
    }

    #[test]
    fn mcp_env_secret_redacts() {
        let env = McpEnvSecret::new("GOOGLE_API_KEY=actualkey".into());
        let dbg = format!("{:?}", env);
        assert!(!dbg.contains("actualkey"), "Debug leaked: {}", dbg);
    }
}
```

Add to `plexus-common/src/lib.rs`:

```rust
pub mod secrets;
```

- [ ] **Step 2: Run the test — should fail (types undefined)**

Run: `cargo test --workspace -p plexus-common secrets::`

Expected: compile failure (`DeviceToken`, etc. undefined).

- [ ] **Step 3: Implement the four newtypes**

Edit `plexus-common/src/secrets.rs` — add above the test block:

```rust
//! Newtypes for secret values, wrapping `secrecy::SecretString`.
//!
//! All four types' `Debug`/`Display` impls redact. Exposing the inner value
//! requires explicit `secrecy::ExposeSecret`. See ADR-104.

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! secret_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(SecretString);

        impl $name {
            /// Construct from a plain string. The string is moved into the
            /// secret and zeroized on drop.
            pub fn new(value: String) -> Self {
                Self(SecretString::new(value.into()))
            }

            /// Access the inner string. Use sparingly — every call is an
            /// audit point for secret leakage.
            pub fn expose_secret(&self) -> &str {
                self.0.expose_secret()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_tuple(stringify!($name)).field(&"<redacted>").finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("<redacted>")
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self::new(s)
            }
        }
    };
}

secret_newtype!(
    DeviceToken,
    "Bearer token issued at device registration (ADR-091, ADR-097).\n\nFormat: `plexus_dev_<base64>`. The full literal value of `PLEXUS_DEVICE_TOKEN` env var on the client side."
);

secret_newtype!(
    JwtSecret,
    "Server-side JWT signing secret. Loaded from env at server startup."
);

secret_newtype!(
    LlmApiKey,
    "API key for the OpenAI-compatible LLM endpoint (ADR-101). Stored in `system_config.llm_api_key`."
);

secret_newtype!(
    McpEnvSecret,
    "Secret value from an MCP server's `env` config (ADR-050). MCPs typically need API keys here (e.g. `GOOGLE_API_KEY`)."
);
```

- [ ] **Step 4: Run the test — should pass**

Run: `cargo test --workspace -p plexus-common secrets::`

Expected: 6 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/secrets.rs plexus-common/src/lib.rs
git commit -m "feat(common): add redacting secret newtypes (DeviceToken, JwtSecret, LlmApiKey, McpEnvSecret)"
```

---

### Task 6: `errors/mod.rs` — `ErrorCode` enum + `Code` trait

The wire-stable error enum that every typed error maps to. Used by API responses and tool_result error contents.

**Files:**
- Create: `plexus-common/src/errors/mod.rs`
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Write the test**

Create the directory:

```bash
mkdir -p plexus-common/src/errors
```

Create `plexus-common/src/errors/mod.rs`:

```rust
//! Typed error enums + the wire-stable `ErrorCode`. See ADR-046.
//!
//! Every error type in this module implements the `Code` trait so any error
//! can be rendered to the wire via its stable `ErrorCode`.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_serialize_matches_lowercase_snake() {
        assert_eq!(
            serde_json::to_string(&ErrorCode::TokenInvalid).unwrap(),
            "\"token_invalid\""
        );
    }

    #[test]
    fn error_code_deserialize_from_lowercase_snake() {
        let parsed: ErrorCode = serde_json::from_str("\"path_outside_workspace\"").unwrap();
        assert_eq!(parsed, ErrorCode::PathOutsideWorkspace);
    }
}
```

Add to `plexus-common/src/lib.rs`:

```rust
pub mod errors;
```

- [ ] **Step 2: Run the test — should fail**

Run: `cargo test --workspace -p plexus-common errors::`

Expected: compile failure (`ErrorCode` undefined).

- [ ] **Step 3: Implement `ErrorCode` + `Code` trait**

Edit `plexus-common/src/errors/mod.rs`:

```rust
//! Typed error enums + the wire-stable `ErrorCode`. See ADR-046.
//!
//! Every error type in this module implements the `Code` trait so any error
//! can be rendered to the wire via its stable `ErrorCode`.

use serde::{Deserialize, Serialize};

pub mod auth;
pub mod mcp;
pub mod network;
pub mod protocol;
pub mod tool;
pub mod workspace;

pub use auth::AuthError;
pub use mcp::McpError;
pub use network::NetworkError;
pub use protocol::ProtocolError;
pub use tool::ToolError;
pub use workspace::WorkspaceError;

/// Stable wire-level error code. Serialized as `snake_case` strings.
///
/// New variants are additive; never repurpose an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    // Workspace
    NotFound,
    SoftLocked,
    UploadTooLarge,
    PathOutsideWorkspace,
    IoError,

    // Tool
    ExecTimeout,
    SandboxFailure,
    McpUnavailable,
    McpRestarting,
    CwdOutsideWorkspace,
    InvalidArgs,
    DeviceUnreachable,
    ClientShuttingDown,

    // Auth
    TokenInvalid,
    TokenExpired,
    Unauthorized,
    Forbidden,

    // Protocol
    MalformedFrame,
    UnknownType,
    VersionMismatch,
    TransferUnknownId,

    // MCP
    SchemaCollision,
    WithinServerCollision,
    SpawnFailed,

    // Network
    PrivateAddressBlocked,
    WhitelistMiss,
    DnsFailed,
    Timeout,
    HttpError,
}

/// Implemented by every typed error in this crate.
///
/// Maps the error variant to its wire-stable `ErrorCode`.
pub trait Code {
    fn code(&self) -> ErrorCode;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_serialize_matches_lowercase_snake() {
        assert_eq!(
            serde_json::to_string(&ErrorCode::TokenInvalid).unwrap(),
            "\"token_invalid\""
        );
    }

    #[test]
    fn error_code_deserialize_from_lowercase_snake() {
        let parsed: ErrorCode = serde_json::from_str("\"path_outside_workspace\"").unwrap();
        assert_eq!(parsed, ErrorCode::PathOutsideWorkspace);
    }

    #[test]
    fn error_code_roundtrip_all_variants() {
        let variants = [
            ErrorCode::NotFound, ErrorCode::SoftLocked, ErrorCode::UploadTooLarge,
            ErrorCode::PathOutsideWorkspace, ErrorCode::IoError,
            ErrorCode::ExecTimeout, ErrorCode::SandboxFailure, ErrorCode::McpUnavailable,
            ErrorCode::McpRestarting, ErrorCode::CwdOutsideWorkspace, ErrorCode::InvalidArgs,
            ErrorCode::DeviceUnreachable, ErrorCode::ClientShuttingDown,
            ErrorCode::TokenInvalid, ErrorCode::TokenExpired, ErrorCode::Unauthorized,
            ErrorCode::Forbidden,
            ErrorCode::MalformedFrame, ErrorCode::UnknownType, ErrorCode::VersionMismatch,
            ErrorCode::TransferUnknownId,
            ErrorCode::SchemaCollision, ErrorCode::WithinServerCollision, ErrorCode::SpawnFailed,
            ErrorCode::PrivateAddressBlocked, ErrorCode::WhitelistMiss, ErrorCode::DnsFailed,
            ErrorCode::Timeout, ErrorCode::HttpError,
        ];
        for variant in variants {
            let json = serde_json::to_string(&variant).unwrap();
            let back: ErrorCode = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back, "roundtrip failed for {:?}", variant);
        }
    }
}
```

This file references the six error submodules — they don't exist yet, so the build will fail until Tasks 7-12 land. Create stub submodule files now so `mod auth;` etc. compile:

```bash
touch plexus-common/src/errors/auth.rs plexus-common/src/errors/mcp.rs plexus-common/src/errors/network.rs plexus-common/src/errors/protocol.rs plexus-common/src/errors/tool.rs plexus-common/src/errors/workspace.rs
```

Each stub file just needs to declare its empty error type so `pub use` in `mod.rs` resolves. We'll fill them in across Tasks 7-12. For now, add this minimal stub to each of the 6 files (so the build still works between Tasks 6 and 7):

`plexus-common/src/errors/auth.rs`:
```rust
//! `AuthError` — see Task 9.
//!
//! Stub; full impl in next task.
use thiserror::Error;
#[derive(Debug, Error)]
pub enum AuthError {}
```

`plexus-common/src/errors/mcp.rs`:
```rust
//! `McpError` — see Task 11. Stub.
use thiserror::Error;
#[derive(Debug, Error)]
pub enum McpError {}
```

`plexus-common/src/errors/network.rs`:
```rust
//! `NetworkError` — see Task 12. Stub.
use thiserror::Error;
#[derive(Debug, Error)]
pub enum NetworkError {}
```

`plexus-common/src/errors/protocol.rs`:
```rust
//! `ProtocolError` — see Task 10. Stub.
use thiserror::Error;
#[derive(Debug, Error)]
pub enum ProtocolError {}
```

`plexus-common/src/errors/tool.rs`:
```rust
//! `ToolError` — see Task 8. Stub.
use thiserror::Error;
#[derive(Debug, Error)]
pub enum ToolError {}
```

`plexus-common/src/errors/workspace.rs`:
```rust
//! `WorkspaceError` — see Task 7. Stub.
use thiserror::Error;
#[derive(Debug, Error)]
pub enum WorkspaceError {}
```

The empty enum stubs need `thiserror` — add it to `plexus-common/Cargo.toml` `[dependencies]` if not already there. (Should already be there from Task 1, Step 3.)

- [ ] **Step 4: Run the test — should pass**

Run: `cargo test --workspace -p plexus-common errors::`

Expected: 3 tests passed (the 2 we wrote + roundtrip-all-variants we added during impl).

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/errors plexus-common/src/lib.rs
git commit -m "feat(common): add ErrorCode enum + Code trait + 6 error type stubs"
```

---

### Task 7: `errors/workspace.rs` — `WorkspaceError`

**Files:**
- Modify: `plexus-common/src/errors/workspace.rs`

- [ ] **Step 1: Write the test**

Replace the contents of `plexus-common/src/errors/workspace.rs` with:

```rust
//! Errors raised by `workspace_fs` (server) and the file-tool jail (both
//! crates). See ADR-046, ADR-073.

use crate::errors::{Code, ErrorCode};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("path not found: {0}")]
    NotFound(PathBuf),

    #[error("workspace is over quota; only deletes are allowed until usage drops")]
    SoftLocked,

    #[error("upload size {actual_bytes} exceeds 80% of quota ({quota_bytes} bytes)")]
    UploadTooLarge {
        actual_bytes: u64,
        quota_bytes: u64,
    },

    #[error("path {0} resolves outside the workspace root")]
    PathOutsideWorkspace(PathBuf),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

impl Code for WorkspaceError {
    fn code(&self) -> ErrorCode {
        match self {
            WorkspaceError::NotFound(_) => ErrorCode::NotFound,
            WorkspaceError::SoftLocked => ErrorCode::SoftLocked,
            WorkspaceError::UploadTooLarge { .. } => ErrorCode::UploadTooLarge,
            WorkspaceError::PathOutsideWorkspace(_) => ErrorCode::PathOutsideWorkspace,
            WorkspaceError::IoError(_) => ErrorCode::IoError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_maps_to_not_found_code() {
        let e = WorkspaceError::NotFound(PathBuf::from("/some/path"));
        assert_eq!(e.code(), ErrorCode::NotFound);
    }

    #[test]
    fn soft_locked_maps() {
        assert_eq!(WorkspaceError::SoftLocked.code(), ErrorCode::SoftLocked);
    }

    #[test]
    fn upload_too_large_maps() {
        let e = WorkspaceError::UploadTooLarge {
            actual_bytes: 1000,
            quota_bytes: 800,
        };
        assert_eq!(e.code(), ErrorCode::UploadTooLarge);
    }

    #[test]
    fn path_outside_workspace_maps() {
        let e = WorkspaceError::PathOutsideWorkspace(PathBuf::from("/etc/passwd"));
        assert_eq!(e.code(), ErrorCode::PathOutsideWorkspace);
    }

    #[test]
    fn io_error_maps() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "nope");
        let e: WorkspaceError = io_err.into();
        assert_eq!(e.code(), ErrorCode::IoError);
    }

    #[test]
    fn display_includes_path_for_not_found() {
        let e = WorkspaceError::NotFound(PathBuf::from("/foo/bar"));
        assert_eq!(format!("{}", e), "path not found: /foo/bar");
    }
}
```

- [ ] **Step 2: Run the test — should pass (no failing test cycle here; we're filling in a stub)**

Run: `cargo test --workspace -p plexus-common workspace::`

Expected: 6 tests passed.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/src/errors/workspace.rs
git commit -m "feat(common): WorkspaceError variants + Code impl"
```

---

### Task 8: `errors/tool.rs` — `ToolError`

**Files:**
- Modify: `plexus-common/src/errors/tool.rs`

- [ ] **Step 1: Implement and test**

Replace the contents of `plexus-common/src/errors/tool.rs` with:

```rust
//! Errors raised during tool dispatch and execution. See ADR-031, ADR-046,
//! ADR-073, ADR-105.

use crate::errors::{Code, ErrorCode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("tool execution timed out after {seconds}s")]
    ExecTimeout { seconds: u32 },

    #[error("sandbox setup failed: {0}")]
    SandboxFailure(String),

    #[error("MCP server '{server}' is not running. Last error: {last_error}")]
    McpUnavailable {
        server: String,
        last_error: String,
    },

    #[error("MCP server '{server}' is restarting; try again in a moment")]
    McpRestarting { server: String },

    #[error("working directory {0} resolves outside the workspace")]
    CwdOutsideWorkspace(String),

    #[error("invalid args: {0}")]
    InvalidArgs(String),

    #[error("device '{device}' is unreachable")]
    DeviceUnreachable { device: String },

    #[error("client process is shutting down")]
    ClientShuttingDown,
}

impl Code for ToolError {
    fn code(&self) -> ErrorCode {
        match self {
            ToolError::ExecTimeout { .. } => ErrorCode::ExecTimeout,
            ToolError::SandboxFailure(_) => ErrorCode::SandboxFailure,
            ToolError::McpUnavailable { .. } => ErrorCode::McpUnavailable,
            ToolError::McpRestarting { .. } => ErrorCode::McpRestarting,
            ToolError::CwdOutsideWorkspace(_) => ErrorCode::CwdOutsideWorkspace,
            ToolError::InvalidArgs(_) => ErrorCode::InvalidArgs,
            ToolError::DeviceUnreachable { .. } => ErrorCode::DeviceUnreachable,
            ToolError::ClientShuttingDown => ErrorCode::ClientShuttingDown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_timeout_maps() {
        assert_eq!(
            ToolError::ExecTimeout { seconds: 60 }.code(),
            ErrorCode::ExecTimeout
        );
    }

    #[test]
    fn mcp_unavailable_maps_and_displays() {
        let e = ToolError::McpUnavailable {
            server: "google".into(),
            last_error: "GOOGLE_API_KEY env var not set".into(),
        };
        assert_eq!(e.code(), ErrorCode::McpUnavailable);
        assert!(format!("{}", e).contains("google"));
        assert!(format!("{}", e).contains("GOOGLE_API_KEY"));
    }

    #[test]
    fn device_unreachable_maps() {
        let e = ToolError::DeviceUnreachable {
            device: "mac-mini".into(),
        };
        assert_eq!(e.code(), ErrorCode::DeviceUnreachable);
    }

    #[test]
    fn client_shutting_down_maps() {
        assert_eq!(
            ToolError::ClientShuttingDown.code(),
            ErrorCode::ClientShuttingDown
        );
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --workspace -p plexus-common tool::`

Expected: 4 tests passed.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/src/errors/tool.rs
git commit -m "feat(common): ToolError variants + Code impl"
```

---

### Task 9: `errors/auth.rs` — `AuthError`

**Files:**
- Modify: `plexus-common/src/errors/auth.rs`

- [ ] **Step 1: Implement and test**

Replace the contents of `plexus-common/src/errors/auth.rs`:

```rust
//! Authentication errors. Used at REST and WS handshake boundaries.

use crate::errors::{Code, ErrorCode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("token is invalid or revoked")]
    TokenInvalid,

    #[error("token has expired")]
    TokenExpired,

    #[error("authentication required")]
    Unauthorized,

    #[error("authenticated but lacks permission")]
    Forbidden,
}

impl Code for AuthError {
    fn code(&self) -> ErrorCode {
        match self {
            AuthError::TokenInvalid => ErrorCode::TokenInvalid,
            AuthError::TokenExpired => ErrorCode::TokenExpired,
            AuthError::Unauthorized => ErrorCode::Unauthorized,
            AuthError::Forbidden => ErrorCode::Forbidden,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_variants_map() {
        assert_eq!(AuthError::TokenInvalid.code(), ErrorCode::TokenInvalid);
        assert_eq!(AuthError::TokenExpired.code(), ErrorCode::TokenExpired);
        assert_eq!(AuthError::Unauthorized.code(), ErrorCode::Unauthorized);
        assert_eq!(AuthError::Forbidden.code(), ErrorCode::Forbidden);
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --workspace -p plexus-common auth::`

Expected: 1 test passed.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/src/errors/auth.rs
git commit -m "feat(common): AuthError variants + Code impl"
```

---

### Task 10: `errors/protocol.rs` — `ProtocolError`

**Files:**
- Modify: `plexus-common/src/errors/protocol.rs`

- [ ] **Step 1: Implement and test**

Replace `plexus-common/src/errors/protocol.rs`:

```rust
//! Wire-protocol errors raised by the WS frame layer (PROTOCOL.md §5.1).

use crate::errors::{Code, ErrorCode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("malformed frame: {0}")]
    MalformedFrame(String),

    #[error("unknown frame type: {0}")]
    UnknownType(String),

    #[error("protocol version mismatch: server requires {required}, client sent {client_sent}")]
    VersionMismatch {
        required: String,
        client_sent: String,
    },

    #[error("transfer slot {0} is not active")]
    TransferUnknownId(String),
}

impl Code for ProtocolError {
    fn code(&self) -> ErrorCode {
        match self {
            ProtocolError::MalformedFrame(_) => ErrorCode::MalformedFrame,
            ProtocolError::UnknownType(_) => ErrorCode::UnknownType,
            ProtocolError::VersionMismatch { .. } => ErrorCode::VersionMismatch,
            ProtocolError::TransferUnknownId(_) => ErrorCode::TransferUnknownId,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_mismatch_displays_both_versions() {
        let e = ProtocolError::VersionMismatch {
            required: "2".into(),
            client_sent: "1".into(),
        };
        let disp = format!("{}", e);
        assert!(disp.contains("2"));
        assert!(disp.contains("1"));
    }

    #[test]
    fn all_variants_map() {
        assert_eq!(
            ProtocolError::MalformedFrame("oops".into()).code(),
            ErrorCode::MalformedFrame
        );
        assert_eq!(
            ProtocolError::UnknownType("zzz".into()).code(),
            ErrorCode::UnknownType
        );
        assert_eq!(
            ProtocolError::VersionMismatch {
                required: "2".into(),
                client_sent: "1".into(),
            }
            .code(),
            ErrorCode::VersionMismatch
        );
        assert_eq!(
            ProtocolError::TransferUnknownId("uuid".into()).code(),
            ErrorCode::TransferUnknownId
        );
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --workspace -p plexus-common protocol::`

Expected: 2 tests passed.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/src/errors/protocol.rs
git commit -m "feat(common): ProtocolError variants + Code impl"
```

---

### Task 11: `errors/mcp.rs` — `McpError`

**Files:**
- Modify: `plexus-common/src/errors/mcp.rs`

- [ ] **Step 1: Implement and test**

Replace `plexus-common/src/errors/mcp.rs`:

```rust
//! MCP-specific errors. See ADR-047, ADR-049, ADR-105.

use crate::errors::{Code, ErrorCode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum McpError {
    #[error("schema for '{wrapped_name}' differs across install sites")]
    SchemaCollision { wrapped_name: String },

    #[error("MCP server '{server}' advertises duplicate name: '{wrapped_name}'")]
    WithinServerCollision {
        server: String,
        wrapped_name: String,
    },

    #[error("MCP server '{server}' failed to spawn: {detail}")]
    SpawnFailed { server: String, detail: String },
}

impl Code for McpError {
    fn code(&self) -> ErrorCode {
        match self {
            McpError::SchemaCollision { .. } => ErrorCode::SchemaCollision,
            McpError::WithinServerCollision { .. } => ErrorCode::WithinServerCollision,
            McpError::SpawnFailed { .. } => ErrorCode::SpawnFailed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_collision_maps() {
        let e = McpError::SchemaCollision {
            wrapped_name: "mcp_google_search".into(),
        };
        assert_eq!(e.code(), ErrorCode::SchemaCollision);
    }

    #[test]
    fn within_server_collision_maps() {
        let e = McpError::WithinServerCollision {
            server: "google".into(),
            wrapped_name: "mcp_google_search".into(),
        };
        assert_eq!(e.code(), ErrorCode::WithinServerCollision);
    }

    #[test]
    fn spawn_failed_displays_server_and_detail() {
        let e = McpError::SpawnFailed {
            server: "google".into(),
            detail: "GOOGLE_API_KEY env var not set".into(),
        };
        let disp = format!("{}", e);
        assert!(disp.contains("google"));
        assert!(disp.contains("GOOGLE_API_KEY"));
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --workspace -p plexus-common mcp::`

Expected: 3 tests passed.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/src/errors/mcp.rs
git commit -m "feat(common): McpError variants + Code impl"
```

---

### Task 12: `errors/network.rs` — `NetworkError`

**Files:**
- Modify: `plexus-common/src/errors/network.rs`

- [ ] **Step 1: Implement and test**

Replace `plexus-common/src/errors/network.rs`:

```rust
//! Network-layer errors. Raised by `web_fetch` and MCP transports.
//! See ADR-052.

use crate::errors::{Code, ErrorCode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("blocked: target IP {0} is in the private-address block-list")]
    PrivateAddressBlocked(String),

    #[error("blocked: target {0} is not in the device's ssrf_whitelist")]
    WhitelistMiss(String),

    #[error("DNS resolution failed for '{0}'")]
    DnsFailed(String),

    #[error("network operation timed out after {seconds}s")]
    Timeout { seconds: u32 },

    #[error("HTTP error: status {status}, body {body}")]
    HttpError { status: u16, body: String },
}

impl Code for NetworkError {
    fn code(&self) -> ErrorCode {
        match self {
            NetworkError::PrivateAddressBlocked(_) => ErrorCode::PrivateAddressBlocked,
            NetworkError::WhitelistMiss(_) => ErrorCode::WhitelistMiss,
            NetworkError::DnsFailed(_) => ErrorCode::DnsFailed,
            NetworkError::Timeout { .. } => ErrorCode::Timeout,
            NetworkError::HttpError { .. } => ErrorCode::HttpError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_address_maps() {
        let e = NetworkError::PrivateAddressBlocked("10.0.0.1".into());
        assert_eq!(e.code(), ErrorCode::PrivateAddressBlocked);
    }

    #[test]
    fn http_error_displays_status_and_body() {
        let e = NetworkError::HttpError {
            status: 404,
            body: "not found".into(),
        };
        let disp = format!("{}", e);
        assert!(disp.contains("404"));
        assert!(disp.contains("not found"));
    }

    #[test]
    fn all_variants_map() {
        assert_eq!(
            NetworkError::WhitelistMiss("foo".into()).code(),
            ErrorCode::WhitelistMiss
        );
        assert_eq!(
            NetworkError::DnsFailed("foo".into()).code(),
            ErrorCode::DnsFailed
        );
        assert_eq!(
            NetworkError::Timeout { seconds: 30 }.code(),
            ErrorCode::Timeout
        );
        assert_eq!(
            NetworkError::HttpError {
                status: 500,
                body: "x".into()
            }
            .code(),
            ErrorCode::HttpError
        );
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --workspace -p plexus-common network::`

Expected: 3 tests passed.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/src/errors/network.rs
git commit -m "feat(common): NetworkError variants + Code impl"
```

---

### Task 13: `protocol/types.rs` — frame inner types

Defines the data shapes that frames carry: `DeviceConfig` (sent in hello_ack and config_update), `McpServerConfig`, `McpSchemas` and its inner `ToolDef`, `ResourceDef`, `PromptDef`, `PromptArgument`.

**Files:**
- Create: `plexus-common/src/protocol/mod.rs`
- Create: `plexus-common/src/protocol/types.rs`
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Create the module facade**

```bash
mkdir -p plexus-common/src/protocol
```

Create `plexus-common/src/protocol/mod.rs`:

```rust
//! WebSocket protocol structs. See `docs/PROTOCOL.md`.

pub mod frames;
pub mod transfer;
pub mod types;

pub use frames::WsFrame;
pub use types::{DeviceConfig, McpServerConfig, McpSchemas, PromptArgument, PromptDef, ResourceDef, ToolDef};
```

We'll reference `frames` and `transfer` in this re-export — they don't exist yet, but Tasks 14/15 fill them. Stub them with empty files for now:

```bash
touch plexus-common/src/protocol/frames.rs plexus-common/src/protocol/transfer.rs
```

The empty files won't compile yet because `mod.rs` references types from them. Add minimal stubs:

`plexus-common/src/protocol/frames.rs`:
```rust
//! WS frame types — see Task 15. Stub.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WsFrame {}
```

`plexus-common/src/protocol/transfer.rs`:
```rust
//! Binary transfer frame layout — see Task 14. Stub (empty).
```

Add to `plexus-common/src/lib.rs`:

```rust
pub mod protocol;
```

- [ ] **Step 2: Write the test**

Create `plexus-common/src/protocol/types.rs`:

```rust
//! Frame inner types — the data shapes carried by frames in `frames.rs`.

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn device_config_roundtrip() {
        let cfg = DeviceConfig {
            workspace_path: "/home/alice/.plexus".into(),
            fs_policy: FsPolicy::Sandbox,
            shell_timeout_max: 300,
            ssrf_whitelist: vec!["10.180.20.30:8080".into()],
            mcp_servers: serde_json::json!({}),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: DeviceConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg.workspace_path, back.workspace_path);
        assert_eq!(cfg.fs_policy, back.fs_policy);
        assert_eq!(cfg.shell_timeout_max, back.shell_timeout_max);
        assert_eq!(cfg.ssrf_whitelist, back.ssrf_whitelist);
    }

    #[test]
    fn fs_policy_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&FsPolicy::Sandbox).unwrap(),
            "\"sandbox\""
        );
        assert_eq!(
            serde_json::to_string(&FsPolicy::Unrestricted).unwrap(),
            "\"unrestricted\""
        );
    }

    #[test]
    fn mcp_server_config_roundtrip() {
        let cfg = McpServerConfig {
            command: vec!["npx".into(), "@plexus/mcp-google".into()],
            env: serde_json::json!({"GOOGLE_API_KEY": "redacted"}),
            description: Some("Google search".into()),
            enabled: Some(vec!["mcp_google_*".into()]),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: McpServerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg.command, back.command);
        assert_eq!(cfg.description, back.description);
        assert_eq!(cfg.enabled, back.enabled);
    }

    #[test]
    fn mcp_schemas_roundtrip() {
        let s = McpSchemas {
            server_name: "minimax".into(),
            tools: vec![ToolDef {
                name: "web_search".into(),
                input_schema: serde_json::json!({"type": "object"}),
                description: Some("Search the web".into()),
            }],
            resources: vec![ResourceDef {
                name: "page".into(),
                uri: "minimax://page/{page_id}".into(),
                description: None,
                mime_type: None,
            }],
            prompts: vec![PromptDef {
                name: "code_review".into(),
                arguments: vec![PromptArgument {
                    name: "language".into(),
                    description: None,
                    required: true,
                }],
                description: None,
            }],
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: McpSchemas = serde_json::from_str(&json).unwrap();
        assert_eq!(s.server_name, back.server_name);
        assert_eq!(s.tools.len(), back.tools.len());
        assert_eq!(s.resources.len(), back.resources.len());
        assert_eq!(s.prompts.len(), back.prompts.len());
    }

    #[test]
    fn empty_mcp_schemas_serializes_with_empty_arrays() {
        let s = McpSchemas {
            server_name: "empty".into(),
            tools: vec![],
            resources: vec![],
            prompts: vec![],
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"tools\":[]"));
        assert!(json.contains("\"resources\":[]"));
        assert!(json.contains("\"prompts\":[]"));
    }
}
```

- [ ] **Step 3: Run the test — should fail**

Run: `cargo test --workspace -p plexus-common protocol::types::`

Expected: compile failure (`DeviceConfig`, `FsPolicy`, etc. undefined).

- [ ] **Step 4: Implement**

Edit `plexus-common/src/protocol/types.rs` — add above the test block:

```rust
//! Frame inner types — the data shapes carried by frames in `frames.rs`.

use serde::{Deserialize, Serialize};

/// Filesystem policy controlling both the file-tool jail and the subprocess
/// jail (ADR-073).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FsPolicy {
    /// Default — file tools enforce the workspace boundary; on Linux the
    /// subprocess jail (bwrap) also fires.
    Sandbox,
    /// Both jails lifted. Requires typed-name confirmation per ADR-051.
    Unrestricted,
}

/// Device configuration sent in `hello_ack` and `config_update` (ADR-050).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceConfig {
    /// Absolute workspace root path on the device.
    pub workspace_path: String,

    pub fs_policy: FsPolicy,

    /// Maximum `exec` timeout the agent can request, in seconds.
    pub shell_timeout_max: u32,

    /// Per-device SSRF whitelist for `web_fetch` (ADR-052). `host` or
    /// `host:port` strings.
    #[serde(default)]
    pub ssrf_whitelist: Vec<String>,

    /// MCP server configurations as a JSON object keyed by server name.
    /// Each value matches `McpServerConfig`.
    pub mcp_servers: serde_json::Value,
}

/// Per-MCP-server configuration (ADR-050, ADR-100).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerConfig {
    /// Argv to spawn the subprocess (e.g. `["npx", "@plexus/mcp-google"]`).
    pub command: Vec<String>,

    /// Environment variables for the subprocess. Values may include secrets.
    #[serde(default = "empty_object")]
    pub env: serde_json::Value,

    #[serde(default)]
    pub description: Option<String>,

    /// Optional allow-list of post-wrap entry names (ADR-100). Glob patterns.
    /// When `None`, every advertised capability registers.
    #[serde(default)]
    pub enabled: Option<Vec<String>>,
}

fn empty_object() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}

/// All capabilities advertised by one MCP server (ADR-047, ADR-048).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpSchemas {
    pub server_name: String,

    #[serde(default)]
    pub tools: Vec<ToolDef>,

    #[serde(default)]
    pub resources: Vec<ResourceDef>,

    #[serde(default)]
    pub prompts: Vec<PromptDef>,
}

/// One tool advertised by an MCP server (raw shape from `list_tools`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,

    pub input_schema: serde_json::Value,

    #[serde(default)]
    pub description: Option<String>,
}

/// One resource advertised by an MCP server (raw shape from `list_resources`).
///
/// `uri` may be a static URI or a URI template with `{var}` placeholders
/// (ADR-099). The wrap step (Plan 3) converts the template into schema
/// properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceDef {
    pub name: String,

    pub uri: String,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default, rename = "mimeType")]
    pub mime_type: Option<String>,
}

/// One prompt advertised by an MCP server (raw shape from `list_prompts`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptDef {
    pub name: String,

    #[serde(default)]
    pub arguments: Vec<PromptArgument>,

    #[serde(default)]
    pub description: Option<String>,
}

/// One argument of an MCP prompt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptArgument {
    pub name: String,

    #[serde(default)]
    pub description: Option<String>,

    #[serde(default)]
    pub required: bool,
}
```

- [ ] **Step 5: Run the test — should pass**

Run: `cargo test --workspace -p plexus-common protocol::types::`

Expected: 5 tests passed.

- [ ] **Step 6: Commit**

```bash
git add plexus-common/src/protocol plexus-common/src/lib.rs
git commit -m "feat(common): add protocol::types — DeviceConfig, McpServerConfig, McpSchemas + inner types"
```

---

### Task 14: `protocol/transfer.rs` — binary frame header

Per PROTOCOL.md §4.3 the binary frame layout is `[16 bytes UUID transfer_id][chunk bytes...]`.

**Files:**
- Modify: `plexus-common/src/protocol/transfer.rs`

- [ ] **Step 1: Add `uuid` to plexus-common deps**

Edit `plexus-common/Cargo.toml`. The `[dependencies]` section already has uuid via workspace inheritance (we added it in Task 1). No change needed if it's already there. If not, add:

```toml
[dependencies]
# ... existing deps ...
uuid = { workspace = true }
```

- [ ] **Step 2: Write the test**

Replace `plexus-common/src/protocol/transfer.rs`:

```rust
//! Binary frame header layout for file-transfer chunks.
//!
//! Per PROTOCOL.md §4.3: every binary WebSocket frame's payload starts with
//! a 16-byte UUID v7 (`transfer_id`), followed by the chunk's bytes.

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn header_size_is_16() {
        assert_eq!(HEADER_SIZE, 16);
    }

    #[test]
    fn pack_then_parse_roundtrip() {
        let id = Uuid::now_v7();
        let chunk = b"hello world";
        let packed = pack_chunk(id, chunk);
        let (parsed_id, parsed_chunk) = parse_chunk(&packed).expect("parses");
        assert_eq!(parsed_id, id);
        assert_eq!(parsed_chunk, chunk);
    }

    #[test]
    fn pack_empty_chunk() {
        let id = Uuid::now_v7();
        let packed = pack_chunk(id, &[]);
        assert_eq!(packed.len(), HEADER_SIZE);
        let (parsed_id, parsed_chunk) = parse_chunk(&packed).expect("parses");
        assert_eq!(parsed_id, id);
        assert_eq!(parsed_chunk, b"");
    }

    #[test]
    fn parse_too_short_returns_none() {
        let short = vec![0u8; 8];
        assert!(parse_chunk(&short).is_err());
    }

    #[test]
    fn parse_exactly_header_size_returns_empty_chunk() {
        let id = Uuid::now_v7();
        let header_only = id.as_bytes().to_vec();
        let (parsed_id, parsed_chunk) = parse_chunk(&header_only).expect("parses");
        assert_eq!(parsed_id, id);
        assert_eq!(parsed_chunk, b"");
    }
}
```

- [ ] **Step 3: Run the test — should fail**

Run: `cargo test --workspace -p plexus-common protocol::transfer::`

Expected: compile failure (`HEADER_SIZE`, `pack_chunk`, `parse_chunk` undefined).

- [ ] **Step 4: Implement**

Edit `plexus-common/src/protocol/transfer.rs` — add above the test block:

```rust
//! Binary frame header layout for file-transfer chunks.
//!
//! Per PROTOCOL.md §4.3: every binary WebSocket frame's payload starts with
//! a 16-byte UUID v7 (`transfer_id`), followed by the chunk's bytes.

use crate::errors::ProtocolError;
use uuid::Uuid;

/// Size of the binary frame header in bytes (the UUID).
pub const HEADER_SIZE: usize = 16;

/// Pack a transfer chunk for a binary WS frame.
///
/// Returns a buffer containing the 16-byte transfer_id followed by `chunk`.
/// Allocates one Vec; caller can use the result directly as the WS payload.
pub fn pack_chunk(transfer_id: Uuid, chunk: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_SIZE + chunk.len());
    out.extend_from_slice(transfer_id.as_bytes());
    out.extend_from_slice(chunk);
    out
}

/// Parse a binary WS frame payload into `(transfer_id, chunk_bytes)`.
///
/// `chunk_bytes` borrows from the input; no allocation. Returns
/// `ProtocolError::MalformedFrame` if the input is shorter than `HEADER_SIZE`.
pub fn parse_chunk(payload: &[u8]) -> Result<(Uuid, &[u8]), ProtocolError> {
    if payload.len() < HEADER_SIZE {
        return Err(ProtocolError::MalformedFrame(format!(
            "binary frame payload is {} bytes; expected at least {} (header)",
            payload.len(),
            HEADER_SIZE
        )));
    }
    let mut id_bytes = [0u8; HEADER_SIZE];
    id_bytes.copy_from_slice(&payload[..HEADER_SIZE]);
    let id = Uuid::from_bytes(id_bytes);
    Ok((id, &payload[HEADER_SIZE..]))
}
```

- [ ] **Step 5: Run the test — should pass**

Run: `cargo test --workspace -p plexus-common protocol::transfer::`

Expected: 5 tests passed.

- [ ] **Step 6: Commit**

```bash
git add plexus-common/src/protocol/transfer.rs
git commit -m "feat(common): add protocol::transfer — binary frame header (pack/parse)"
```

---

### Task 15: `protocol/frames.rs` — full `WsFrame` enum

Implements every text-frame variant from PROTOCOL.md §2 as an internally-tagged serde enum.

**Files:**
- Modify: `plexus-common/src/protocol/frames.rs`

- [ ] **Step 1: Write the test**

Replace `plexus-common/src/protocol/frames.rs` with:

```rust
//! WebSocket text frames. PROTOCOL.md §2.
//!
//! All frames serialize via serde with `#[serde(tag = "type")]` —
//! `{"type": "<name>", ...fields}` on the wire.

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    fn id() -> Uuid {
        Uuid::now_v7()
    }

    #[test]
    fn hello_roundtrip() {
        let frame = WsFrame::Hello(HelloFrame {
            id: id(),
            version: "1".into(),
            client_version: "0.1.0".into(),
            os: "linux".into(),
            caps: HelloCaps {
                sandbox: "bwrap".into(),
                exec: true,
                fs: "rw".into(),
            },
        });
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("\"type\":\"hello\""));
        let back: WsFrame = serde_json::from_str(&json).unwrap();
        assert_matches_hello(&frame, &back);
    }

    #[test]
    fn hello_ack_roundtrip() {
        let frame = WsFrame::HelloAck(HelloAckFrame {
            id: id(),
            device_name: "mac-mini".into(),
            user_id: Uuid::now_v7(),
            config: crate::protocol::types::DeviceConfig {
                workspace_path: "/home/alice/.plexus".into(),
                fs_policy: crate::protocol::types::FsPolicy::Sandbox,
                shell_timeout_max: 300,
                ssrf_whitelist: vec![],
                mcp_servers: serde_json::json!({}),
            },
        });
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("\"type\":\"hello_ack\""));
        let _back: WsFrame = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn tool_call_roundtrip() {
        let frame = WsFrame::ToolCall(ToolCallFrame {
            id: id(),
            name: "exec".into(),
            args: serde_json::json!({"command": "git status", "timeout": 60}),
        });
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("\"type\":\"tool_call\""));
        let back: WsFrame = serde_json::from_str(&json).unwrap();
        if let WsFrame::ToolCall(tc) = back {
            assert_eq!(tc.name, "exec");
        } else {
            panic!("expected ToolCall variant");
        }
    }

    #[test]
    fn tool_result_roundtrip_success() {
        let req_id = id();
        let frame = WsFrame::ToolResult(ToolResultFrame {
            id: req_id,
            content: "ok".into(),
            is_error: false,
            code: None,
        });
        let json = serde_json::to_string(&frame).unwrap();
        let back: WsFrame = serde_json::from_str(&json).unwrap();
        if let WsFrame::ToolResult(r) = back {
            assert_eq!(r.id, req_id);
            assert_eq!(r.content, "ok");
            assert!(!r.is_error);
        } else {
            panic!("expected ToolResult variant");
        }
    }

    #[test]
    fn tool_result_roundtrip_error_with_code() {
        let frame = WsFrame::ToolResult(ToolResultFrame {
            id: id(),
            content: "MCP server 'google' is not running".into(),
            is_error: true,
            code: Some("mcp_unavailable".into()),
        });
        let json = serde_json::to_string(&frame).unwrap();
        let back: WsFrame = serde_json::from_str(&json).unwrap();
        if let WsFrame::ToolResult(r) = back {
            assert!(r.is_error);
            assert_eq!(r.code.as_deref(), Some("mcp_unavailable"));
        } else {
            panic!();
        }
    }

    #[test]
    fn register_mcp_roundtrip_with_failures() {
        let frame = WsFrame::RegisterMcp(RegisterMcpFrame {
            id: id(),
            mcp_servers: vec![],
            spawn_failures: vec![SpawnFailure {
                server_name: "google".into(),
                error: "subprocess exited code 1; stderr: GOOGLE_API_KEY env var not set".into(),
                failed_at: "2026-04-27T12:00:00Z".into(),
            }],
        });
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("\"type\":\"register_mcp\""));
        let back: WsFrame = serde_json::from_str(&json).unwrap();
        if let WsFrame::RegisterMcp(r) = back {
            assert_eq!(r.spawn_failures.len(), 1);
            assert_eq!(r.spawn_failures[0].server_name, "google");
        } else {
            panic!();
        }
    }

    #[test]
    fn config_update_roundtrip() {
        let frame = WsFrame::ConfigUpdate(ConfigUpdateFrame {
            id: id(),
            config: crate::protocol::types::DeviceConfig {
                workspace_path: "/home/alice/.plexus".into(),
                fs_policy: crate::protocol::types::FsPolicy::Unrestricted,
                shell_timeout_max: 600,
                ssrf_whitelist: vec!["10.180.20.30:8080".into()],
                mcp_servers: serde_json::json!({}),
            },
        });
        let json = serde_json::to_string(&frame).unwrap();
        let _back: WsFrame = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn transfer_begin_roundtrip() {
        let frame = WsFrame::TransferBegin(TransferBeginFrame {
            id: id(),
            direction: TransferDirection::ClientToServer,
            src_device: "mac-mini".into(),
            src_path: "/home/alice/.plexus/.attachments/photo.jpg".into(),
            dst_device: "server".into(),
            dst_path: "/alice-uuid/.attachments/photo.jpg".into(),
            total_bytes: 2_457_600,
            sha256: "5e884898da280471".into(),
            mime: Some("image/jpeg".into()),
        });
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("\"type\":\"transfer_begin\""));
        let _back: WsFrame = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn transfer_progress_roundtrip() {
        let frame = WsFrame::TransferProgress(TransferProgressFrame {
            id: id(),
            bytes_sent: 1_048_576,
        });
        let json = serde_json::to_string(&frame).unwrap();
        let _back: WsFrame = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn transfer_end_success_roundtrip() {
        let frame = WsFrame::TransferEnd(TransferEndFrame {
            id: id(),
            ok: true,
            error: None,
            sha256: Some("5e884898da280471".into()),
        });
        let json = serde_json::to_string(&frame).unwrap();
        let _back: WsFrame = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn transfer_end_failure_roundtrip() {
        let frame = WsFrame::TransferEnd(TransferEndFrame {
            id: id(),
            ok: false,
            error: Some("sha256_mismatch".into()),
            sha256: None,
        });
        let json = serde_json::to_string(&frame).unwrap();
        let back: WsFrame = serde_json::from_str(&json).unwrap();
        if let WsFrame::TransferEnd(e) = back {
            assert!(!e.ok);
            assert_eq!(e.error.as_deref(), Some("sha256_mismatch"));
        } else {
            panic!();
        }
    }

    #[test]
    fn ping_pong_roundtrip() {
        let p = WsFrame::Ping(PingFrame { id: id() });
        let pong = WsFrame::Pong(PongFrame { id: id() });
        let _: WsFrame = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        let _: WsFrame = serde_json::from_str(&serde_json::to_string(&pong).unwrap()).unwrap();
    }

    #[test]
    fn error_frame_roundtrip() {
        let frame = WsFrame::Error(ErrorFrame {
            id: Some(id()),
            code: "malformed_frame".into(),
            message: "expected field 'name'".into(),
        });
        let json = serde_json::to_string(&frame).unwrap();
        let _back: WsFrame = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn unknown_type_fails_deserialize() {
        let json = r#"{"type": "totally_unknown", "id": "abc"}"#;
        let result: Result<WsFrame, _> = serde_json::from_str(json);
        assert!(result.is_err(), "should fail on unknown frame type");
    }

    fn assert_matches_hello(a: &WsFrame, b: &WsFrame) {
        if let (WsFrame::Hello(a), WsFrame::Hello(b)) = (a, b) {
            assert_eq!(a.id, b.id);
            assert_eq!(a.version, b.version);
            assert_eq!(a.client_version, b.client_version);
            assert_eq!(a.os, b.os);
        } else {
            panic!("not Hello variants");
        }
    }
}

/// Property-based ser/de roundtrip — generates arbitrary plausible frames
/// and asserts roundtrip identity.
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn arb_uuid() -> impl Strategy<Value = Uuid> {
        any::<[u8; 16]>().prop_map(Uuid::from_bytes)
    }

    fn arb_string() -> impl Strategy<Value = String> {
        // ASCII only — keeps test output readable; serde handles unicode fine
        // and we don't need to torture-test that here.
        "[a-zA-Z0-9 _.,/:-]{0,40}".prop_map(String::from)
    }

    proptest! {
        #[test]
        fn ping_roundtrip(uuid in arb_uuid()) {
            let frame = WsFrame::Ping(PingFrame { id: uuid });
            let json = serde_json::to_string(&frame).unwrap();
            let back: WsFrame = serde_json::from_str(&json).unwrap();
            if let WsFrame::Ping(p) = back {
                prop_assert_eq!(p.id, uuid);
            } else {
                prop_assert!(false, "expected Ping variant");
            }
        }

        #[test]
        fn tool_call_roundtrip(
            uuid in arb_uuid(),
            name in arb_string(),
        ) {
            let frame = WsFrame::ToolCall(ToolCallFrame {
                id: uuid,
                name: name.clone(),
                args: serde_json::json!({}),
            });
            let json = serde_json::to_string(&frame).unwrap();
            let back: WsFrame = serde_json::from_str(&json).unwrap();
            if let WsFrame::ToolCall(t) = back {
                prop_assert_eq!(t.id, uuid);
                prop_assert_eq!(t.name, name);
            } else {
                prop_assert!(false);
            }
        }
    }
}
```

- [ ] **Step 2: Run the tests — should fail (frames undefined)**

Run: `cargo test --workspace -p plexus-common protocol::frames::`

Expected: compile failure (`WsFrame` variants undefined; `HelloFrame`, etc. undefined).

- [ ] **Step 3: Implement all the frame types**

Edit `plexus-common/src/protocol/frames.rs` — add above the test blocks:

```rust
//! WebSocket text frames. PROTOCOL.md §2.
//!
//! All frames serialize via serde with `#[serde(tag = "type")]` —
//! `{"type": "<name>", ...fields}` on the wire.

use crate::protocol::types::{DeviceConfig, McpSchemas};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// All WebSocket text frames, internally tagged by `type`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsFrame {
    Hello(HelloFrame),
    HelloAck(HelloAckFrame),
    ToolCall(ToolCallFrame),
    ToolResult(ToolResultFrame),
    RegisterMcp(RegisterMcpFrame),
    ConfigUpdate(ConfigUpdateFrame),
    TransferBegin(TransferBeginFrame),
    TransferProgress(TransferProgressFrame),
    TransferEnd(TransferEndFrame),
    Ping(PingFrame),
    Pong(PongFrame),
    Error(ErrorFrame),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloFrame {
    pub id: Uuid,
    pub version: String,
    pub client_version: String,
    pub os: String,
    pub caps: HelloCaps,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloCaps {
    pub sandbox: String,
    pub exec: bool,
    pub fs: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAckFrame {
    pub id: Uuid,
    pub device_name: String,
    pub user_id: Uuid,
    pub config: DeviceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallFrame {
    pub id: Uuid,
    pub name: String,
    pub args: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResultFrame {
    pub id: Uuid,
    pub content: String,
    #[serde(default)]
    pub is_error: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterMcpFrame {
    pub id: Uuid,
    #[serde(default)]
    pub mcp_servers: Vec<McpSchemas>,
    #[serde(default)]
    pub spawn_failures: Vec<SpawnFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnFailure {
    pub server_name: String,
    pub error: String,
    pub failed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigUpdateFrame {
    pub id: Uuid,
    pub config: DeviceConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    ClientToServer,
    ServerToClient,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferBeginFrame {
    pub id: Uuid,
    pub direction: TransferDirection,
    pub src_device: String,
    pub src_path: String,
    pub dst_device: String,
    pub dst_path: String,
    pub total_bytes: u64,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferProgressFrame {
    pub id: Uuid,
    pub bytes_sent: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferEndFrame {
    pub id: Uuid,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingFrame {
    pub id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongFrame {
    pub id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorFrame {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
    pub code: String,
    pub message: String,
}
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common protocol::frames::`

Expected: 14 unit tests + 2 proptest cases (×256 iterations each) all pass.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/protocol/frames.rs
git commit -m "feat(common): add protocol::frames — WsFrame enum + all 12 frame variants"
```

---

### Task 16: Final verification + lib.rs re-exports

Confirm Plan 1 acceptance: every test passes, clippy clean, fmt clean, both musl targets build, public API surface intentional.

**Files:**
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Audit `lib.rs` re-exports**

The `lib.rs` should expose the major types at the top level for ergonomic consumption. Replace `plexus-common/src/lib.rs`:

```rust
//! Shared types, errors, protocol, and tool infrastructure for Plexus.
//!
//! See `docs/reference/superpowers/specs/2026-04-28-plexus-m0-design.md` for the full
//! design and `docs/DECISIONS.md` for cross-cutting architecture decisions.
//!
//! # Plan 1 surface (Foundation + Protocol)
//!
//! - [`consts`] — wire-level reserved string constants.
//! - [`version`] — `PROTOCOL_VERSION` + `crate_version()`.
//! - [`secrets`] — redacting newtypes for tokens / API keys.
//! - [`errors`] — typed error enums + `ErrorCode` + `Code` trait.
//! - [`protocol`] — WS frame types + binary transfer header.
//!
//! Plans 2 (`tools`) and 3 (`mcp`) extend the public surface.

pub mod consts;
pub mod errors;
pub mod protocol;
pub mod secrets;
pub mod version;

// Top-level re-exports for ergonomic access.
pub use errors::{
    AuthError, Code, ErrorCode, McpError, NetworkError, ProtocolError, ToolError, WorkspaceError,
};
pub use protocol::{
    DeviceConfig, FsPolicy, McpSchemas, McpServerConfig, PromptArgument, PromptDef, ResourceDef,
    ToolDef, WsFrame,
};
pub use secrets::{DeviceToken, JwtSecret, LlmApiKey, McpEnvSecret};
pub use version::{crate_version, PROTOCOL_VERSION};
```

The `protocol` module's `mod.rs` already re-exports `FsPolicy` from `types`. Ensure the `pub use` line in `protocol/mod.rs` includes `FsPolicy`:

Edit `plexus-common/src/protocol/mod.rs` to:

```rust
//! WebSocket protocol structs. See `docs/PROTOCOL.md`.

pub mod frames;
pub mod transfer;
pub mod types;

pub use frames::WsFrame;
pub use types::{
    DeviceConfig, FsPolicy, McpSchemas, McpServerConfig, PromptArgument, PromptDef, ResourceDef,
    ToolDef,
};
```

- [ ] **Step 2: Run the full test suite**

Run: `cargo test --workspace`

Expected: all tests across all 16 tasks pass. Roughly:
- consts: 5 tests
- version: 2 tests
- secrets: 6 tests
- errors (mod + 6 types): ~25 tests
- protocol::types: 5 tests
- protocol::transfer: 5 tests
- protocol::frames: 14 unit + 2 proptest
- = **~64 tests passing** (Plan 2 will bring the total toward ~140)

- [ ] **Step 3: Run clippy clean**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: no warnings. Fix any that appear.

Common likely warnings to fix:
- `clippy::large_enum_variant` on `WsFrame` — allowed if all variants are similar size. Add `#[allow(clippy::large_enum_variant)]` on the enum if needed.
- `clippy::module_name_repetitions` — disable globally if it fires for our naming convention (e.g. `protocol::ProtocolError`). Add `#![allow(clippy::module_name_repetitions)]` to `lib.rs` if it does.

- [ ] **Step 4: Run fmt check**

Run: `cargo fmt --all --check`

Expected: no diff. If there's a diff, run `cargo fmt --all` and re-run check.

- [ ] **Step 5: Build for both musl targets**

Install musl targets if not already installed:

```bash
rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
```

Then:

```bash
cargo build --workspace --target x86_64-unknown-linux-musl
cargo build --workspace --target aarch64-unknown-linux-musl
```

Expected: both succeed. If `aarch64-unknown-linux-musl` fails with linker errors, it's because the local box doesn't have a cross-linker. CI handles this; locally it's optional in M0 (Plan 3 acceptance criteria fully require both).

- [ ] **Step 6: Run cargo doc clean**

Run: `cargo doc --no-deps -p plexus-common`

Expected: builds without warnings. If there are warnings about missing docs or broken links, fix them.

- [ ] **Step 7: Commit the lib.rs update**

```bash
git add plexus-common/src/lib.rs plexus-common/src/protocol/mod.rs
git commit -m "feat(common): finalize Plan 1 lib.rs re-exports + public API surface"
```

- [ ] **Step 8: Push the branch**

```bash
git push origin rebuild-m0
```

CI runs on push. Watch the GitHub Actions run go green for fmt, clippy, test, and both musl builds. If any fail, address before declaring Plan 1 done.

---

## Plan 1 acceptance criteria (subset of spec §7)

Before declaring Plan 1 done, confirm:

- [ ] All ~64 unit/proptest tests passing (`cargo test --workspace -p plexus-common`).
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo fmt --all --check` clean.
- [ ] `cargo build --workspace --target x86_64-unknown-linux-musl` succeeds (CI verifies aarch64).
- [ ] `cargo doc --no-deps -p plexus-common` builds without warnings.
- [ ] `Cargo.lock` committed.
- [ ] CI green on `rebuild-m0` HEAD.
- [ ] Public API surface in `lib.rs` reviewed and intentional — no accidentally `pub` items.
- [ ] No `unwrap()` / `expect()` / `panic!()` outside `#[cfg(test)]`.
- [ ] No `unsafe` code.

When all boxes checked, Plan 1 is done. Proceed to Plan 2 (Tools) via `superpowers:writing-plans` again with that scope.

---

## Post-Plan Adjustments

The plan is historical: code blocks above describe what tasks attempted, not what the final code looks like. Where the implementation diverged from this plan, this section records the deltas, the rationale, and the commit SHA.

### Workspace dep version bumps (commit `9e95b00`)

After Task 1 landed, the code-quality reviewer flagged three stale dep pins inherited from this plan as written. All three were bumped before any consumer existed:

- `thiserror = "1"` → `"2"`
- `secrecy = "0.8"` → `"0.10"` (breaking API change in `SecretString` — see secrets.rs delta below)
- `jsonschema = "0.18"` → `"0.30"` (breaking `Validator` API; affects Plan 2)

Why now: with zero consumers, bumping was free. After Task 6 added six `#[derive(Error)]` sites, thiserror migration would have been a 6-file diff. After Plan 2's tool-arg validation, jsonschema migration would have been a Validator-API rewrite.

### Task 5 — `secrets.rs` serde adapter (commit `b02499f`)

The plan specified `#[derive(Serialize, Deserialize)] #[serde(transparent)]` over `SecretString`. **This does not compile under `secrecy = "0.10"`.** `SecretString` is `SecretBox<str>` — `secrecy` 0.10 removed the blanket `Serialize`/`Deserialize` impls for unsized types.

The implementation hand-rolls `Serialize`/`Deserialize` impls inside the `secret_newtype!` macro, going through the plain `String` representation:

```rust
impl Serialize for $name {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.expose_secret().serialize(s)
    }
}

impl<'de> Deserialize<'de> for $name {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d).map(Self::new)
    }
}
```

Wire format is identical to `#[serde(transparent)]` over a string. Redaction property (Debug/Display) is unaffected.

### M0-freeze polish (commit `2d64543`)

After the final Plan 1 code review, six adjustments landed in a single "API surface polish before M0 freeze" commit. Each is at the plan/spec design level — no ADR drift:

1. **`ToolResultFrame.code: Option<String>` → `Option<ErrorCode>`** and **`ErrorFrame.code: String` → `ErrorCode`** (Task 15). Wire format unchanged (snake_case strings via `#[serde(rename_all)]`), but compile-time typo protection at every M1/M2 callsite. The plan defined `ErrorCode` in Task 6 specifically as the wire-stable code type, then didn't use it in the frame definitions — pure plan-level oversight.
2. **`DeviceConfig.mcp_servers: serde_json::Value` → `HashMap<String, McpServerConfig>`** (Task 13). Same JSONB wire format, eliminates `from_value::<HashMap<...>>` boilerplate at every consumer.
3. **`PartialEq` derives on all `protocol/types.rs` and `protocol/frames.rs` structs** (`Eq` where no `serde_json::Value` member). ADR-105's worker queue needs `cfg != prev_cfg` to detect changes.
4. **`#[non_exhaustive]` on `WsFrame` and `ErrorCode`** (Task 6 + Task 15). PROTOCOL.md §6 commits to additive evolution; this enforces it at the type level.
5. **Re-export inner frame structs and transfer helpers at `protocol::*`** (Task 16 → `protocol/mod.rs`). M1/M2 will write `WsFrame::Hello(HelloFrame { … })` constructors hundreds of times; saves the import friction.
6. **Two new malformed-frame regression tests** (Task 15 → `frames.rs::tests::missing_required_field_fails`, `extra_unknown_fields_are_tolerated`). Locks in PROTOCOL.md §6's forward-compat guarantee.
7. **Delete `McpEnvSecret` newtype + lib.rs re-export** (Task 5). Per the user's `feedback_speculative_scaffolding.md` rule — no consumer in M0; re-add when MCP env wiring lands in Plan 3.

Final test count after polish: 63 passing (61 + 2 new malformed tests + 1 new HashMap test - 1 deleted McpEnvSecret test).

### SHA table

| Commit | What |
|---|---|
| `be57860` | Task 1 — workspace skeleton |
| `9e95b00` | Workspace dep bumps (thiserror, secrecy, jsonschema) |
| `6989bf7` | Task 2 — CI workflow |
| `c34fe4b` | Task 3 — consts.rs |
| `1e9f8ac` | Task 4 — version.rs |
| `b02499f` | Task 5 — secrets.rs (with secrecy 0.10 serde adapter) |
| `2791bb0` | Task 6 — errors/mod.rs + 6 stubs |
| `157b814` | Task 7 — WorkspaceError |
| `fd953a3` | Task 8 — ToolError |
| `2250ebb` | Task 9 — AuthError |
| `eceffe2` | Task 10 — ProtocolError |
| `316fa62` | Task 11 — McpError |
| `7bfd277` | Task 12 — NetworkError |
| `4816eb9` | Task 13 — protocol/types.rs |
| `0cad90d` | Task 14 — protocol/transfer.rs |
| `3527599` | Task 15 — protocol/frames.rs |
| `52ad7ec` | Task 16 — finalize lib.rs re-exports |
| `2d64543` | M0-freeze polish (this footer's deltas applied) |
