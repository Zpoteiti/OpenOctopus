# Plexus M0 — Plan 3: MCP + Polish — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the entire `mcp/` module of `plexus-common` (5 files), the `fake-mcp` test-fixture binary, the `tests/mcp_lifecycle.rs` integration test, and final polish (README, doc-comment pass, public-API audit). End of this plan = M0 complete.

**Architecture:** Pure-Rust library code that wraps `rmcp` 1.5.0's client side. Naming follows ADR-048's typed-infix convention (`mcp_<server>_<tool>` / `_resource_<name>` / `_prompt_<name>`). URI templates per ADR-099 surface as input_schema properties. Lifecycle helpers (`spawn_mcp`/`teardown_mcp`) per ADR-105 use rmcp's `TokioChildProcess` transport with a 30-second startup timeout. The `fake-mcp` fixture is a separate binary in this crate that uses rmcp's server side; integration tests find it via `env!("CARGO_BIN_EXE_fake-mcp")`.

**Tech Stack:** Rust 1.90+, edition 2024. New deps consumed in this plan: `globset` 0.4 (already in workspace deps, just consume), `rmcp` 1.5.0 with extra `server` + `transport-io` features (for fake-mcp).

**rmcp API uncertainty:** rmcp 1.5.0's exact API surface (handler trait names, transport setup) may differ from what this plan sketches. The implementer SHOULD reference https://docs.rs/rmcp/1.5.0/ when in doubt. The wrappers we expose (`McpSession`'s six methods, `spawn_mcp`/`teardown_mcp` signatures) MUST stay stable regardless of how rmcp evolves; only the bodies adapt.

**Spec:** [docs/reference/superpowers/specs/2026-04-28-plexus-m0-design.md](../specs/2026-04-28-plexus-m0-design.md)

**Branch:** `rebuild-m0` (Plans 1 + 2 already shipped; common has consts/version/secrets/errors/protocol/tools/).

---

## File map

| Path | Responsibility |
|---|---|
| `plexus-common/Cargo.toml` | Add globset consumption + rmcp server/transport-io features + [[bin]] entry for fake-mcp |
| `plexus-common/src/mcp/mod.rs` | Module facade re-exporting submodule items |
| `plexus-common/src/mcp/naming.rs` | `wrap_tool_name`/`wrap_resource_name`/`wrap_prompt_name` builders + `parse_wrapped_name` parser; `McpSurface` enum |
| `plexus-common/src/mcp/filter.rs` | `EnabledFilter` wrapping `globset::GlobSet` per ADR-100 |
| `plexus-common/src/mcp/wrap.rs` | URI template parser per ADR-099 + substitutor |
| `plexus-common/src/mcp/session.rs` | `McpSession` wrapper hiding `rmcp::RunningService`; six async methods (list_tools/resources/prompts, call_tool, read_resource, get_prompt) |
| `plexus-common/src/mcp/lifecycle.rs` | `spawn_mcp` + `teardown_mcp` helpers per ADR-105 |
| `plexus-common/src/lib.rs` | Add `pub mod mcp;` + final mcp/* re-exports |
| `plexus-common/tests/fixtures/fake-mcp/main.rs` | Minimal MCP server binary (1 tool, 1 resource, 1 prompt) using rmcp's server side |
| `plexus-common/tests/mcp_lifecycle.rs` | Integration test exercising spawn_mcp → list/call all surfaces → teardown |
| `plexus-common/README.md` | 1-page crate overview |

Total: ~7 new src files, 1 test fixture binary, 1 integration test, 1 README. Approx ~1200 LoC code + ~400 LoC tests + 100 LoC fixture + 80 LoC README.

---

## Conventions

- **Tests live in same file** for unit tests (`#[cfg(test)] mod tests`); cross-module/cross-crate tests in `tests/`.
- **Run all tests** via: `cargo test --workspace -p plexus-common`. Run a single module via: `cargo test --workspace -p plexus-common mcp::naming::`.
- **Cargo working dir is `/home/yucheng/Documents/GitHub/Plexus`** for all commands.
- **Commit after every passing task.** Frequent commits = small reverts.
- **Doc comments referencing ADRs are fine.** WHAT-comments are not. Per the project's `CLAUDE.md`.

---

### Task 1: Cargo.toml deps + fake-mcp [[bin]] entry

**Files:**
- Modify: `plexus-common/Cargo.toml`

- [ ] **Step 1: Add globset to dependencies and rmcp with extra features**

Replace the `[dependencies]` block of `plexus-common/Cargo.toml` (the existing block is from Plans 1 and 2):

```toml
[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
secrecy = { workspace = true }
zeroize = { workspace = true }
tokio = { workspace = true }
jsonschema = { workspace = true }
async-trait = "0.1"
globset = { workspace = true }
rmcp = { workspace = true, features = ["server", "transport-io"] }
```

Note: `rmcp = { workspace = true, features = ["server", "transport-io"] }` ADDS those features to the workspace-declared `["client", "transport-child-process"]` (Cargo merges feature sets across workspace + crate). The library-side code only uses `client` + `transport-child-process`; `server` + `transport-io` exist for the fake-mcp binary fixture (Task 8). Unused features are cfg-gated within rmcp; binary-size impact is minimal.

- [ ] **Step 2: Add the [[bin]] entry for fake-mcp**

Append to the END of `plexus-common/Cargo.toml`:

```toml
[[bin]]
name = "fake-mcp"
path = "tests/fixtures/fake-mcp/main.rs"
test = false
doc = false
```

- [ ] **Step 3: Verify build still works**

Run: `cargo build --workspace -p plexus-common`

Expected: builds cleanly with no warnings. The `[[bin]]` referencing `tests/fixtures/fake-mcp/main.rs` will FAIL because that file doesn't exist yet — that's expected, fix follows.

If the build fails with `error: couldn't read tests/fixtures/fake-mcp/main.rs`, create a placeholder so Cargo can resolve:

```bash
mkdir -p plexus-common/tests/fixtures/fake-mcp
cat > plexus-common/tests/fixtures/fake-mcp/main.rs <<'EOF'
//! Placeholder — full impl in Task 8.
fn main() {}
EOF
```

Then re-run `cargo build --workspace -p plexus-common`. Expected: succeeds.

- [ ] **Step 4: Commit**

```bash
git add plexus-common/Cargo.toml plexus-common/tests/fixtures/fake-mcp/main.rs
git commit -m "chore(common): add globset + rmcp server/transport-io features + fake-mcp [[bin]] entry"
```

---

### Task 2: mcp/ module scaffold

**Files:**
- Create: `plexus-common/src/mcp/mod.rs`
- Create: `plexus-common/src/mcp/naming.rs` (stub)
- Create: `plexus-common/src/mcp/filter.rs` (stub)
- Create: `plexus-common/src/mcp/wrap.rs` (stub)
- Create: `plexus-common/src/mcp/session.rs` (stub)
- Create: `plexus-common/src/mcp/lifecycle.rs` (stub)
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Create the mcp directory**

```bash
mkdir -p plexus-common/src/mcp
```

- [ ] **Step 2: Create the module facade**

Create `plexus-common/src/mcp/mod.rs`:

```rust
//! Shared MCP infrastructure. See ADR-047, ADR-048, ADR-049, ADR-099, ADR-100, ADR-105.
//!
//! - [`naming`] — typed-infix wrapped names (mcp_<server>_<tool> etc.).
//! - [`filter`] — `enabled` glob filter (ADR-100).
//! - [`wrap`] — URI template parsing (ADR-099) and substitution.
//! - [`session`] — `McpSession` wrapper hiding `rmcp::RunningService`.
//! - [`lifecycle`] — `spawn_mcp` / `teardown_mcp` helpers (ADR-105).

pub mod filter;
pub mod lifecycle;
pub mod naming;
pub mod session;
pub mod wrap;
```

- [ ] **Step 3: Create the 5 stub files**

`plexus-common/src/mcp/naming.rs`:
```rust
//! Stub — full impl in Task 3.
```

`plexus-common/src/mcp/filter.rs`:
```rust
//! Stub — full impl in Task 4.
```

`plexus-common/src/mcp/wrap.rs`:
```rust
//! Stub — full impl in Task 5.
```

`plexus-common/src/mcp/session.rs`:
```rust
//! Stub — full impl in Task 6.
```

`plexus-common/src/mcp/lifecycle.rs`:
```rust
//! Stub — full impl in Task 7.
```

- [ ] **Step 4: Add `pub mod mcp;` to lib.rs**

Read `plexus-common/src/lib.rs` first. The existing module block (after Plans 1 + 2) reads:
```rust
pub mod consts;
pub mod errors;
pub mod protocol;
pub mod secrets;
pub mod tools;
pub mod version;
```

Add `pub mod mcp;` between `errors` and `protocol` (alphabetical order). The new block should read:
```rust
pub mod consts;
pub mod errors;
pub mod mcp;
pub mod protocol;
pub mod secrets;
pub mod tools;
pub mod version;
```

- [ ] **Step 5: Verify build**

Run: `cargo build --workspace -p plexus-common`

Expected: succeeds, no warnings.

- [ ] **Step 6: Commit**

```bash
git add plexus-common/src/mcp plexus-common/src/lib.rs
git commit -m "feat(common): add mcp/ module scaffold (5 stubs)"
```

---

### Task 3: mcp/naming.rs — typed-infix name builders + parser

**Files:**
- Modify: `plexus-common/src/mcp/naming.rs`

- [ ] **Step 1: Write the failing tests**

Replace `plexus-common/src/mcp/naming.rs` with:

```rust
//! Wrapped-name builders + parser per ADR-048 (typed-infix convention).
//!
//! Wrapped name formats:
//! - tool: `mcp_<server>_<tool_name>`
//! - resource: `mcp_<server>_resource_<resource_name>`
//! - prompt: `mcp_<server>_prompt_<prompt_name>`
//!
//! The typed infixes (`_resource_` / `_prompt_`) make cross-surface name
//! collisions impossible. Tools stay unprefixed.
//!
//! **Naming constraint:** server names MUST NOT contain underscores OR the
//! literal substrings `_resource_` / `_prompt_`. Validated at MCP install time
//! by ADR-049's collision check; this module's parser assumes the constraint.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_tool_name_basic() {
        assert_eq!(wrap_tool_name("google", "search"), "mcp_google_search");
    }

    #[test]
    fn wrap_resource_name_basic() {
        assert_eq!(
            wrap_resource_name("notion", "page"),
            "mcp_notion_resource_page"
        );
    }

    #[test]
    fn wrap_prompt_name_basic() {
        assert_eq!(
            wrap_prompt_name("helper", "code_review"),
            "mcp_helper_prompt_code_review"
        );
    }

    #[test]
    fn parse_tool_round_trip() {
        let wrapped = wrap_tool_name("google", "search");
        let parsed = parse_wrapped_name(&wrapped).unwrap();
        assert_eq!(parsed.server, "google");
        assert_eq!(parsed.surface, McpSurface::Tool);
        assert_eq!(parsed.raw_name, "search");
    }

    #[test]
    fn parse_resource_round_trip() {
        let wrapped = wrap_resource_name("notion", "page");
        let parsed = parse_wrapped_name(&wrapped).unwrap();
        assert_eq!(parsed.server, "notion");
        assert_eq!(parsed.surface, McpSurface::Resource);
        assert_eq!(parsed.raw_name, "page");
    }

    #[test]
    fn parse_prompt_round_trip() {
        let wrapped = wrap_prompt_name("helper", "code_review");
        let parsed = parse_wrapped_name(&wrapped).unwrap();
        assert_eq!(parsed.server, "helper");
        assert_eq!(parsed.surface, McpSurface::Prompt);
        assert_eq!(parsed.raw_name, "code_review");
    }

    #[test]
    fn parse_resource_with_underscore_raw_name() {
        // raw_name with underscore — legal for resource because the infix
        // unambiguously bounds the server segment.
        let wrapped = wrap_resource_name("notion", "page_content");
        let parsed = parse_wrapped_name(&wrapped).unwrap();
        assert_eq!(parsed.server, "notion");
        assert_eq!(parsed.raw_name, "page_content");
    }

    #[test]
    fn parse_no_mcp_prefix_returns_none() {
        assert!(parse_wrapped_name("google_search").is_none());
        assert!(parse_wrapped_name("read_file").is_none());
    }

    #[test]
    fn parse_just_prefix_returns_none() {
        assert!(parse_wrapped_name("mcp_").is_none());
        assert!(parse_wrapped_name("mcp_google").is_none());
    }

    #[test]
    fn parse_empty_string_returns_none() {
        assert!(parse_wrapped_name("").is_none());
    }

    #[test]
    fn parse_resource_with_empty_raw_name_returns_none() {
        // "mcp_notion_resource_" has empty raw name
        assert!(parse_wrapped_name("mcp_notion_resource_").is_none());
    }
}
```

- [ ] **Step 2: Run — should fail**

Run: `cargo test --workspace -p plexus-common mcp::naming::`

Expected: compile failure (`wrap_tool_name`, `McpSurface`, etc. undefined).

- [ ] **Step 3: Implement**

Add above the test block:

```rust
//! Wrapped-name builders + parser per ADR-048 (typed-infix convention).
//!
//! Wrapped name formats:
//! - tool: `mcp_<server>_<tool_name>`
//! - resource: `mcp_<server>_resource_<resource_name>`
//! - prompt: `mcp_<server>_prompt_<prompt_name>`
//!
//! The typed infixes (`_resource_` / `_prompt_`) make cross-surface name
//! collisions impossible. Tools stay unprefixed.
//!
//! **Naming constraint:** server names MUST NOT contain underscores OR the
//! literal substrings `_resource_` / `_prompt_`. Validated at MCP install time
//! by ADR-049's collision check; this module's parser assumes the constraint.

const MCP_PREFIX: &str = "mcp_";
const RESOURCE_INFIX: &str = "_resource_";
const PROMPT_INFIX: &str = "_prompt_";

/// MCP capability surface a wrapped name represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSurface {
    Tool,
    Resource,
    Prompt,
}

/// Components of a parsed wrapped name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrappedName {
    pub server: String,
    pub surface: McpSurface,
    pub raw_name: String,
}

/// Build the wrapped name for a tool.
pub fn wrap_tool_name(server: &str, tool: &str) -> String {
    format!("{MCP_PREFIX}{server}_{tool}")
}

/// Build the wrapped name for a resource.
pub fn wrap_resource_name(server: &str, resource: &str) -> String {
    format!("{MCP_PREFIX}{server}{RESOURCE_INFIX}{resource}")
}

/// Build the wrapped name for a prompt.
pub fn wrap_prompt_name(server: &str, prompt: &str) -> String {
    format!("{MCP_PREFIX}{server}{PROMPT_INFIX}{prompt}")
}

/// Parse a wrapped name into its components.
///
/// Returns `None` if the input doesn't match any of the three formats
/// (e.g. missing `mcp_` prefix, empty server, empty raw name).
///
/// Resource / prompt parsing uses the typed infix as an unambiguous
/// boundary; tool parsing splits on the FIRST underscore after `mcp_`,
/// which assumes server names contain no underscores (ADR-048 constraint).
pub fn parse_wrapped_name(wrapped: &str) -> Option<WrappedName> {
    let after_prefix = wrapped.strip_prefix(MCP_PREFIX)?;
    if after_prefix.is_empty() {
        return None;
    }

    if let Some(idx) = after_prefix.find(RESOURCE_INFIX) {
        let server = &after_prefix[..idx];
        let raw = &after_prefix[idx + RESOURCE_INFIX.len()..];
        if !server.is_empty() && !raw.is_empty() {
            return Some(WrappedName {
                server: server.to_string(),
                surface: McpSurface::Resource,
                raw_name: raw.to_string(),
            });
        }
        return None;
    }

    if let Some(idx) = after_prefix.find(PROMPT_INFIX) {
        let server = &after_prefix[..idx];
        let raw = &after_prefix[idx + PROMPT_INFIX.len()..];
        if !server.is_empty() && !raw.is_empty() {
            return Some(WrappedName {
                server: server.to_string(),
                surface: McpSurface::Prompt,
                raw_name: raw.to_string(),
            });
        }
        return None;
    }

    let (server, raw) = after_prefix.split_once('_')?;
    if server.is_empty() || raw.is_empty() {
        return None;
    }
    Some(WrappedName {
        server: server.to_string(),
        surface: McpSurface::Tool,
        raw_name: raw.to_string(),
    })
}
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common mcp::naming::`

Expected: 11 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/mcp/naming.rs
git commit -m "feat(common): add mcp::naming — typed-infix wrappers + parser (ADR-048)"
```

---

### Task 4: mcp/filter.rs — `EnabledFilter` (ADR-100)

**Files:**
- Modify: `plexus-common/src/mcp/filter.rs`

- [ ] **Step 1: Write the failing tests**

Replace `plexus-common/src/mcp/filter.rs` with:

```rust
//! `EnabledFilter` — `enabled` glob filter per ADR-100.
//!
//! Each MCP server config carries an optional `enabled: [<glob>...]` field.
//! When present, only entries whose post-wrap name matches at least one
//! glob register. When absent, every advertised capability registers
//! (default-allow). Empty list = nothing matches = nothing registers.
//!
//! Globs are compiled via the `globset` crate — same matcher Cargo uses.

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(patterns: Option<&[&str]>) -> EnabledFilter {
        let owned: Option<Vec<String>> =
            patterns.map(|ps| ps.iter().map(|s| s.to_string()).collect());
        EnabledFilter::from_patterns(owned.as_deref()).expect("compile ok")
    }

    #[test]
    fn none_patterns_default_allows_everything() {
        let f = filter(None);
        assert!(f.accepts("mcp_google_search"));
        assert!(f.accepts("mcp_notion_resource_page"));
        assert!(f.accepts("anything_at_all"));
    }

    #[test]
    fn empty_patterns_deny_everything() {
        let f = filter(Some(&[]));
        assert!(!f.accepts("mcp_google_search"));
        assert!(!f.accepts("mcp_notion_resource_page"));
    }

    #[test]
    fn server_wildcard_matches_all_entries_of_server() {
        let f = filter(Some(&["mcp_notion_*"]));
        assert!(f.accepts("mcp_notion_search"));
        assert!(f.accepts("mcp_notion_resource_page"));
        assert!(f.accepts("mcp_notion_prompt_review"));
        assert!(!f.accepts("mcp_google_search"));
    }

    #[test]
    fn surface_wildcard_matches_resources_only() {
        let f = filter(Some(&["mcp_*_resource_*"]));
        assert!(f.accepts("mcp_notion_resource_page"));
        assert!(f.accepts("mcp_google_resource_index"));
        assert!(!f.accepts("mcp_notion_search"));
        assert!(!f.accepts("mcp_helper_prompt_review"));
    }

    #[test]
    fn multiple_patterns_union() {
        let f = filter(Some(&[
            "mcp_notion_search",
            "mcp_notion_resource_*",
        ]));
        assert!(f.accepts("mcp_notion_search"));
        assert!(f.accepts("mcp_notion_resource_page"));
        assert!(!f.accepts("mcp_notion_other_tool"));
        assert!(!f.accepts("mcp_google_search"));
    }

    #[test]
    fn literal_pattern_exact_match() {
        let f = filter(Some(&["mcp_google_search"]));
        assert!(f.accepts("mcp_google_search"));
        assert!(!f.accepts("mcp_google_search_v2"));
        assert!(!f.accepts("mcp_google_searchx"));
    }

    #[test]
    fn invalid_glob_returns_error() {
        // Unbalanced bracket — globset rejects.
        let result = EnabledFilter::from_patterns(Some(&["mcp_[invalid".to_string()]));
        assert!(result.is_err(), "expected glob compile error");
    }

    #[test]
    fn empty_pattern_string_rejected() {
        // globset rejects empty strings as patterns.
        let result = EnabledFilter::from_patterns(Some(&[String::new()]));
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: Run — should fail**

Run: `cargo test --workspace -p plexus-common mcp::filter::`

Expected: compile failure.

- [ ] **Step 3: Implement**

Add above the test block:

```rust
//! `EnabledFilter` — `enabled` glob filter per ADR-100.
//!
//! Each MCP server config carries an optional `enabled: [<glob>...]` field.
//! When present, only entries whose post-wrap name matches at least one
//! glob register. When absent, every advertised capability registers
//! (default-allow). Empty list = nothing matches = nothing registers.
//!
//! Globs are compiled via the `globset` crate — same matcher Cargo uses.

use globset::{Glob, GlobSet, GlobSetBuilder};

/// Compiled filter for the `enabled` field of a McpServerConfig.
///
/// `None` inner = default-allow (no filter set in config).
/// `Some(set)` = only post-wrap names matching the set are accepted.
pub struct EnabledFilter(Option<GlobSet>);

impl EnabledFilter {
    /// Compile a filter from optional glob patterns.
    ///
    /// `None` produces a default-allow filter that accepts every name.
    /// `Some(empty)` produces a deny-all filter (no patterns to match).
    /// `Some(non_empty)` compiles each pattern via `globset::Glob::new`;
    /// any invalid pattern returns `Err(message)`.
    pub fn from_patterns(patterns: Option<&[String]>) -> Result<Self, String> {
        let Some(pats) = patterns else {
            return Ok(Self(None));
        };
        let mut builder = GlobSetBuilder::new();
        for pat in pats {
            let glob = Glob::new(pat)
                .map_err(|e| format!("invalid glob '{pat}': {e}"))?;
            builder.add(glob);
        }
        let set = builder
            .build()
            .map_err(|e| format!("glob set build: {e}"))?;
        Ok(Self(Some(set)))
    }

    /// Test whether `wrapped_name` is accepted by this filter.
    pub fn accepts(&self, wrapped_name: &str) -> bool {
        match &self.0 {
            None => true,
            Some(set) => set.is_match(wrapped_name),
        }
    }
}
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common mcp::filter::`

Expected: 8 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/mcp/filter.rs
git commit -m "feat(common): add mcp::filter::EnabledFilter (ADR-100)"
```

---

### Task 5: mcp/wrap.rs — URI template parser + substitutor (ADR-099)

**Files:**
- Modify: `plexus-common/src/mcp/wrap.rs`

- [ ] **Step 1: Write the failing tests**

Replace `plexus-common/src/mcp/wrap.rs` with:

```rust
//! URI template parsing per ADR-099 — surfaces `{var}` placeholders as
//! `input_schema` properties + substitutes at call time.
//!
//! Simple `{var}` syntax only (regex `\{(\w+)\}`). RFC 6570 features
//! (operators, query strings, fragments) are NOT supported; if a real
//! MCP needs them we revisit.

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_static_uri_no_placeholders() {
        let placeholders = parse_uri_placeholders("notion://workspace/index");
        assert!(placeholders.is_empty());
    }

    #[test]
    fn parse_single_placeholder() {
        let placeholders = parse_uri_placeholders("notion://page/{page_id}");
        assert_eq!(placeholders, vec!["page_id"]);
    }

    #[test]
    fn parse_multiple_placeholders() {
        let placeholders = parse_uri_placeholders("api://{org}/{project}/{file}");
        assert_eq!(placeholders, vec!["org", "project", "file"]);
    }

    #[test]
    fn parse_placeholder_with_underscore() {
        let placeholders = parse_uri_placeholders("api://{user_id}/profile");
        assert_eq!(placeholders, vec!["user_id"]);
    }

    #[test]
    fn build_input_schema_static() {
        let schema = build_resource_input_schema("notion://workspace/index");
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"].as_object().unwrap().is_empty());
        assert!(schema["required"].as_array().unwrap().is_empty());
    }

    #[test]
    fn build_input_schema_with_placeholder() {
        let schema = build_resource_input_schema("notion://page/{page_id}");
        assert_eq!(schema["type"], "object");
        let properties = schema["properties"].as_object().unwrap();
        assert!(properties.contains_key("page_id"));
        assert_eq!(properties["page_id"]["type"], "string");
        let required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(required, vec!["page_id"]);
    }

    #[test]
    fn substitute_static_uri_unchanged() {
        let result = substitute_uri("notion://workspace/index", &json!({})).unwrap();
        assert_eq!(result, "notion://workspace/index");
    }

    #[test]
    fn substitute_single_placeholder() {
        let result =
            substitute_uri("notion://page/{page_id}", &json!({"page_id": "abc123"})).unwrap();
        assert_eq!(result, "notion://page/abc123");
    }

    #[test]
    fn substitute_multiple_placeholders() {
        let result = substitute_uri(
            "api://{org}/{project}",
            &json!({"org": "plexus", "project": "rebuild"}),
        )
        .unwrap();
        assert_eq!(result, "api://plexus/rebuild");
    }

    #[test]
    fn substitute_missing_placeholder_returns_error() {
        let result = substitute_uri("notion://page/{page_id}", &json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn substitute_extra_args_ignored() {
        // Args have an extra key that's not in the URI; substitution succeeds.
        let result = substitute_uri(
            "notion://page/{page_id}",
            &json!({"page_id": "x", "extra": "ignored"}),
        )
        .unwrap();
        assert_eq!(result, "notion://page/x");
    }

    #[test]
    fn substitute_non_string_arg_uses_string_repr() {
        // Numeric arg — substituted as its JSON-string form (without quotes).
        let result = substitute_uri("api://item/{id}", &json!({"id": 42})).unwrap();
        assert_eq!(result, "api://item/42");
    }
}
```

- [ ] **Step 2: Run — should fail**

Run: `cargo test --workspace -p plexus-common mcp::wrap::`

Expected: compile failure.

- [ ] **Step 3: Implement**

Add above the test block:

```rust
//! URI template parsing per ADR-099 — surfaces `{var}` placeholders as
//! `input_schema` properties + substitutes at call time.
//!
//! Simple `{var}` syntax only (regex `\{(\w+)\}`). RFC 6570 features
//! (operators, query strings, fragments) are NOT supported; if a real
//! MCP needs them we revisit.

use crate::errors::McpError;
use serde_json::{Value, json};

/// Extract the placeholder variable names from a URI template.
///
/// Order matches occurrence order in the template; duplicates preserved.
pub fn parse_uri_placeholders(uri: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = uri.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'{' {
            i += 1;
            continue;
        }
        let start = i + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end] != b'}' {
            // Per ADR-099, placeholder names match `\w+` — ASCII letters, digits, underscore.
            let c = bytes[end];
            if !(c.is_ascii_alphanumeric() || c == b'_') {
                break;
            }
            end += 1;
        }
        if end < bytes.len() && bytes[end] == b'}' && end > start {
            // Valid placeholder: between start and end.
            // SAFETY: ASCII slice from valid UTF-8 input is valid UTF-8.
            out.push(uri[start..end].to_string());
            i = end + 1;
        } else {
            i += 1;
        }
    }
    out
}

/// Build the `input_schema` body for a resource URI template.
///
/// Static URIs (no placeholders) produce `{type:object, properties:{}, required:[]}`.
/// Templates produce one required `string` property per placeholder.
pub fn build_resource_input_schema(uri: &str) -> Value {
    let placeholders = parse_uri_placeholders(uri);
    let mut properties = serde_json::Map::new();
    for name in &placeholders {
        properties.insert(
            name.clone(),
            json!({
                "type": "string",
                "description": format!("URI template variable: {name}")
            }),
        );
    }
    json!({
        "type": "object",
        "properties": properties,
        "required": placeholders,
    })
}

/// Substitute placeholder values into the URI template using `args`.
///
/// Each `{name}` is replaced with the string form of `args[name]`. Numeric
/// values are rendered without quotes; strings without quotes; booleans as
/// "true"/"false". Returns `McpError::SpawnFailed`-style error if any
/// placeholder has no corresponding key in `args`.
pub fn substitute_uri(uri: &str, args: &Value) -> Result<String, McpError> {
    let placeholders = parse_uri_placeholders(uri);
    let mut out = uri.to_string();
    for name in placeholders {
        let value = args.get(&name).ok_or_else(|| McpError::SpawnFailed {
            server: "uri-substitute".to_string(),
            detail: format!("missing placeholder '{name}' in args"),
        })?;
        let replacement = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".to_string(),
            other => other.to_string(),
        };
        out = out.replace(&format!("{{{name}}}"), &replacement);
    }
    Ok(out)
}
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common mcp::wrap::`

Expected: 12 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/mcp/wrap.rs
git commit -m "feat(common): add mcp::wrap — URI template parser + substitutor (ADR-099)"
```

---

### Task 6: mcp/session.rs — `McpSession` wrapper hiding rmcp

This task wraps `rmcp` 1.5.0's client-side `RunningService` behind the six methods Plexus consumers need. Tests are deferred to Task 9 (integration via fake-mcp).

**rmcp API caveat:** the precise API of `rmcp` 1.5.0 may differ from the sketch below. The implementer should consult https://docs.rs/rmcp/1.5.0/ and adjust the bodies as needed — but keep the SIX wrapper-method signatures stable. Common patterns that may need adjustment:
- The exact name of the running-service type (e.g. `RunningService`, `ClientService`, etc.)
- Whether `list_tools` etc. take a parameter struct or are bare methods
- How the response types unwrap to text content

**Files:**
- Modify: `plexus-common/src/mcp/session.rs`

- [ ] **Step 1: Implement (no unit tests — integration test in Task 9)**

Replace `plexus-common/src/mcp/session.rs` with:

```rust
//! `McpSession` — thin wrapper around `rmcp::RunningService` that exposes
//! exactly the six methods Plexus consumers need, returning crate-typed
//! errors (`McpError`) instead of leaking rmcp's error type.
//!
//! See ADR-047 for the wrapping rationale. Tests live in
//! `tests/mcp_lifecycle.rs` (Task 9) — they spawn the `fake-mcp` fixture
//! to exercise the full client/server protocol.

use crate::errors::McpError;
use crate::protocol::{PromptArgument, PromptDef, ResourceDef, ToolDef};
use serde_json::Value;

/// MCP client session. Hides the underlying rmcp running service.
///
/// Every method returns `McpError` on failure — the inner rmcp error
/// types do not leak through.
pub struct McpSession {
    inner: rmcp::service::RunningService<rmcp::RoleClient, ()>,
}

impl McpSession {
    /// Construct from a started rmcp service. Used by `lifecycle::spawn_mcp`.
    pub(crate) fn from_running(inner: rmcp::service::RunningService<rmcp::RoleClient, ()>) -> Self {
        Self { inner }
    }

    /// List the tools advertised by the MCP server.
    pub async fn list_tools(&self) -> Result<Vec<ToolDef>, McpError> {
        let response = self
            .inner
            .list_tools(Default::default())
            .await
            .map_err(|e| McpError::SpawnFailed {
                server: "session".to_string(),
                detail: format!("list_tools: {e}"),
            })?;
        Ok(response
            .tools
            .into_iter()
            .map(|t| ToolDef {
                name: t.name.to_string(),
                input_schema: serde_json::to_value(&t.input_schema).unwrap_or(Value::Null),
                description: t.description.map(|s| s.to_string()),
            })
            .collect())
    }

    /// List the resources advertised by the MCP server.
    pub async fn list_resources(&self) -> Result<Vec<ResourceDef>, McpError> {
        let response = self
            .inner
            .list_resources(Default::default())
            .await
            .map_err(|e| McpError::SpawnFailed {
                server: "session".to_string(),
                detail: format!("list_resources: {e}"),
            })?;
        Ok(response
            .resources
            .into_iter()
            .map(|r| ResourceDef {
                name: r.name,
                uri: r.uri,
                description: r.description,
                mime_type: r.mime_type,
            })
            .collect())
    }

    /// List the prompts advertised by the MCP server.
    pub async fn list_prompts(&self) -> Result<Vec<PromptDef>, McpError> {
        let response = self
            .inner
            .list_prompts(Default::default())
            .await
            .map_err(|e| McpError::SpawnFailed {
                server: "session".to_string(),
                detail: format!("list_prompts: {e}"),
            })?;
        Ok(response
            .prompts
            .into_iter()
            .map(|p| PromptDef {
                name: p.name,
                arguments: p
                    .arguments
                    .unwrap_or_default()
                    .into_iter()
                    .map(|a| PromptArgument {
                        name: a.name,
                        description: a.description,
                        required: a.required.unwrap_or(false),
                    })
                    .collect(),
                description: p.description,
            })
            .collect())
    }

    /// Call a tool. Returns the tool result content concatenated as text.
    pub async fn call_tool(&self, name: &str, args: Value) -> Result<String, McpError> {
        let arguments = args.as_object().cloned();
        let response = self
            .inner
            .call_tool(rmcp::model::CallToolRequestParam {
                name: name.to_string().into(),
                arguments,
            })
            .await
            .map_err(|e| McpError::SpawnFailed {
                server: "session".to_string(),
                detail: format!("call_tool {name}: {e}"),
            })?;
        Ok(content_blocks_to_string(response.content))
    }

    /// Read a resource. Returns its text content.
    pub async fn read_resource(&self, uri: &str) -> Result<String, McpError> {
        let response = self
            .inner
            .read_resource(rmcp::model::ReadResourceRequestParam {
                uri: uri.to_string(),
            })
            .await
            .map_err(|e| McpError::SpawnFailed {
                server: "session".to_string(),
                detail: format!("read_resource {uri}: {e}"),
            })?;
        Ok(resource_contents_to_string(response.contents))
    }

    /// Get a prompt with arguments. Returns the rendered prompt messages
    /// joined with `"\n"` per ADR-048's prompt-output stringify convention.
    /// Empty result → `"(no output)"`.
    pub async fn get_prompt(&self, name: &str, args: Value) -> Result<String, McpError> {
        let arguments = args.as_object().cloned();
        let response = self
            .inner
            .get_prompt(rmcp::model::GetPromptRequestParam {
                name: name.to_string(),
                arguments,
            })
            .await
            .map_err(|e| McpError::SpawnFailed {
                server: "session".to_string(),
                detail: format!("get_prompt {name}: {e}"),
            })?;
        Ok(prompt_messages_to_string(response.messages))
    }

    /// Cancel and tear down the session. Used by `lifecycle::teardown_mcp`.
    pub(crate) async fn cancel(self) {
        let _ = self.inner.cancel().await;
    }
}

/// Concatenate rmcp content blocks (text/image/resource) to a single string.
/// Non-text blocks are stringified via Display.
fn content_blocks_to_string(blocks: Vec<rmcp::model::Content>) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(blocks.len());
    for block in blocks {
        match block.raw {
            rmcp::model::RawContent::Text(t) => parts.push(t.text),
            other => parts.push(format!("{other:?}")),
        }
    }
    parts.join("\n")
}

/// Concatenate read-resource contents to a single string.
fn resource_contents_to_string(contents: Vec<rmcp::model::ResourceContents>) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(contents.len());
    for content in contents {
        match content {
            rmcp::model::ResourceContents::TextResourceContents { text, .. } => parts.push(text),
            rmcp::model::ResourceContents::BlobResourceContents { blob, .. } => parts.push(blob),
        }
    }
    parts.join("\n")
}

/// Stringify a list of PromptMessage per ADR-048 (`get_prompt` output).
///
/// Iterates messages, extracts text content from each, joins with `"\n"`.
/// Non-text content stringified via Debug. Empty list → `"(no output)"`.
fn prompt_messages_to_string(messages: Vec<rmcp::model::PromptMessage>) -> String {
    if messages.is_empty() {
        return "(no output)".to_string();
    }
    let mut parts: Vec<String> = Vec::with_capacity(messages.len());
    for msg in messages {
        match msg.content {
            rmcp::model::PromptMessageContent::Text { text } => parts.push(text),
            other => parts.push(format!("{other:?}")),
        }
    }
    parts.join("\n")
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build --workspace -p plexus-common`

Expected: succeeds.

If the rmcp 1.5.0 API differs from the sketch:
- Look for compile errors mentioning `rmcp::model::*` or `rmcp::service::*` types.
- Reference https://docs.rs/rmcp/1.5.0/ for the actual type/method names.
- Adjust the body of each method while keeping the public method signature stable.
- Common adaptations: `RunningService` → `ClientService`; `list_tools(Default::default())` → `list_tools()` if no params; field names on response structs.

If you make adaptations, document them in the commit message.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/src/mcp/session.rs
git commit -m "feat(common): add mcp::session::McpSession wrapping rmcp client"
```

---

### Task 7: mcp/lifecycle.rs — `spawn_mcp` + `teardown_mcp`

**Files:**
- Modify: `plexus-common/src/mcp/lifecycle.rs`

- [ ] **Step 1: Implement (no unit tests — integration test in Task 9)**

Replace `plexus-common/src/mcp/lifecycle.rs` with:

```rust
//! MCP lifecycle helpers per ADR-105.
//!
//! `spawn_mcp` boots the rmcp subprocess, performs the client handshake,
//! lists tools/resources/prompts, and returns an `McpSession` plus the
//! collected `McpSchemas`. Bounded by a 30-second startup timeout.
//!
//! `teardown_mcp` cancels the running service cleanly.
//!
//! Tests live in `tests/mcp_lifecycle.rs` (Task 9) using the `fake-mcp`
//! fixture binary to exercise the full client/server flow.

use crate::errors::McpError;
use crate::mcp::session::McpSession;
use crate::protocol::{McpSchemas, McpServerConfig};
use std::time::Duration;
use tokio::process::Command;

/// Maximum startup time per ADR-105.
const SPAWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Spawn an MCP server subprocess, perform the rmcp handshake, list its
/// tools / resources / prompts, and return the session + schemas.
///
/// Bounded by [`SPAWN_TIMEOUT`] (30 seconds). On timeout or any other
/// failure during startup, returns `McpError::SpawnFailed` with detail.
pub async fn spawn_mcp(
    config: &McpServerConfig,
) -> Result<(McpSession, McpSchemas), McpError> {
    if config.command.is_empty() {
        return Err(McpError::SpawnFailed {
            server: "spawn".to_string(),
            detail: "empty command argv".to_string(),
        });
    }

    let server_label = config
        .command
        .first()
        .cloned()
        .unwrap_or_else(|| "<unknown>".to_string());

    tokio::time::timeout(SPAWN_TIMEOUT, spawn_inner(config))
        .await
        .map_err(|_| McpError::SpawnFailed {
            server: server_label.clone(),
            detail: format!("startup timeout after {}s", SPAWN_TIMEOUT.as_secs()),
        })?
}

async fn spawn_inner(
    config: &McpServerConfig,
) -> Result<(McpSession, McpSchemas), McpError> {
    let server_label = config
        .command
        .first()
        .cloned()
        .unwrap_or_else(|| "<unknown>".to_string());

    let mut command = Command::new(&config.command[0]);
    if config.command.len() > 1 {
        command.args(&config.command[1..]);
    }
    for (k, v) in &config.env {
        command.env(k, v);
    }

    let transport = rmcp::transport::TokioChildProcess::new(command).map_err(|e| {
        McpError::SpawnFailed {
            server: server_label.clone(),
            detail: format!("subprocess transport: {e}"),
        }
    })?;

    let running = rmcp::ServiceExt::serve(rmcp::RoleClient, transport)
        .await
        .map_err(|e| McpError::SpawnFailed {
            server: server_label.clone(),
            detail: format!("rmcp handshake: {e}"),
        })?;

    let session = McpSession::from_running(running);

    let tools = session.list_tools().await?;
    let resources = session.list_resources().await?;
    let prompts = session.list_prompts().await?;

    let schemas = McpSchemas {
        server_name: server_label,
        tools,
        resources,
        prompts,
    };

    Ok((session, schemas))
}

/// Cancel the running session and reap the subprocess.
pub async fn teardown_mcp(session: McpSession) {
    session.cancel().await;
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build --workspace -p plexus-common`

Expected: succeeds.

If the rmcp 1.5.0 API names differ (e.g. `TokioChildProcess` lives at a different path, or `ServiceExt::serve` has different signature), adapt the bodies. Keep `spawn_mcp` and `teardown_mcp` signatures stable.

Common pitfalls:
- The `rmcp::transport::TokioChildProcess::new` may take `&mut Command` or own the `Command` outright; adapt.
- `rmcp::ServiceExt::serve` may need explicit type parameters.
- `RoleClient` might be at `rmcp::service::RoleClient` or `rmcp::RoleClient`.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/src/mcp/lifecycle.rs
git commit -m "feat(common): add mcp::lifecycle::{spawn_mcp,teardown_mcp} (ADR-105)"
```

---

### Task 8: fake-mcp fixture binary

A minimal MCP server binary that supports the integration test. Implements 1 tool ("echo"), 1 static resource ("fake://fixed"), 1 prompt ("greet").

**Files:**
- Modify: `plexus-common/tests/fixtures/fake-mcp/main.rs` (currently a `fn main() {}` stub from Task 1)

**rmcp server-side API caveat:** Same as Task 6 — the exact `ServerHandler` trait method names may differ in 1.5.0. Reference https://docs.rs/rmcp/1.5.0/ for the canonical shape. The fixture's BEHAVIOR (1 tool/1 resource/1 prompt as listed in Task 9 test) is what must stay stable.

- [ ] **Step 1: Implement the fixture**

Replace `plexus-common/tests/fixtures/fake-mcp/main.rs` with:

```rust
//! Test fixture: minimal MCP server speaking on stdio.
//!
//! Used by `tests/mcp_lifecycle.rs` to exercise plexus-common's MCP
//! client wrapper end-to-end without depending on real MCP servers.
//!
//! Capabilities exposed:
//! - Tool `echo` — returns the received args as text.
//! - Resource `fake://fixed` — returns "fixed-resource-content".
//! - Prompt `greet` — returns a single user message "hello from greet".

use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    Annotated, CallToolRequestParam, CallToolResult, Content, GetPromptRequestParam,
    GetPromptResult, ListPromptsResult, ListResourcesResult, ListToolsResult, PaginatedRequestParam,
    Prompt, PromptMessage, PromptMessageContent, PromptMessageRole, RawContent,
    RawTextResourceContents, ReadResourceRequestParam, ReadResourceResult, Resource,
    ResourceContents, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServiceExt};
use serde_json::json;
use std::sync::Arc;

#[derive(Default, Clone)]
struct FakeMcp;

#[rmcp::async_trait]
impl ServerHandler for FakeMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
            ..Default::default()
        }
    }

    async fn list_tools(
        &self,
        _params: PaginatedRequestParam,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::Error> {
        Ok(ListToolsResult {
            tools: vec![Tool {
                name: "echo".into(),
                description: Some("Echo args back as text".into()),
                input_schema: Arc::new(
                    json!({
                        "type": "object",
                        "properties": {
                            "x": { "type": "integer" }
                        }
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ),
                annotations: None,
            }],
            next_cursor: None,
        })
    }

    async fn call_tool(
        &self,
        params: CallToolRequestParam,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::Error> {
        let args_text = serde_json::to_string(&params.arguments).unwrap_or_default();
        Ok(CallToolResult {
            content: vec![Annotated {
                raw: RawContent::Text(rmcp::model::RawTextContent {
                    text: format!("echoed: {args_text}"),
                }),
                annotations: None,
            }],
            is_error: Some(false),
        })
    }

    async fn list_resources(
        &self,
        _params: PaginatedRequestParam,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, rmcp::Error> {
        Ok(ListResourcesResult {
            resources: vec![Resource {
                name: "fixed".into(),
                uri: "fake://fixed".into(),
                description: None,
                mime_type: Some("text/plain".into()),
                annotations: None,
                size: None,
            }],
            next_cursor: None,
        })
    }

    async fn read_resource(
        &self,
        params: ReadResourceRequestParam,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, rmcp::Error> {
        Ok(ReadResourceResult {
            contents: vec![ResourceContents::TextResourceContents(RawTextResourceContents {
                uri: params.uri,
                mime_type: Some("text/plain".into()),
                text: "fixed-resource-content".into(),
            })],
        })
    }

    async fn list_prompts(
        &self,
        _params: PaginatedRequestParam,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, rmcp::Error> {
        Ok(ListPromptsResult {
            prompts: vec![Prompt {
                name: "greet".into(),
                description: Some("Returns a one-line user message".into()),
                arguments: None,
            }],
            next_cursor: None,
        })
    }

    async fn get_prompt(
        &self,
        _params: GetPromptRequestParam,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, rmcp::Error> {
        Ok(GetPromptResult {
            description: None,
            messages: vec![PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::Text {
                    text: "hello from greet".into(),
                },
            }],
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = (tokio::io::stdin(), tokio::io::stdout());
    let service = FakeMcp.serve(transport).await?;
    service.waiting().await?;
    Ok(())
}
```

- [ ] **Step 2: Build the fixture**

Run: `cargo build --workspace -p plexus-common --bin fake-mcp`

Expected: succeeds.

If the rmcp 1.5.0 server-side API differs — and the sketch above WILL likely need adjustment, since the API surface is sizable — debug by:
1. Reading the actual `ServerHandler` trait at https://docs.rs/rmcp/1.5.0/.
2. Adjusting field names on `Tool` / `Resource` / `Prompt` / response structs.
3. The macro for async-trait on rmcp may be `rmcp::async_trait` or `async_trait::async_trait` — both work; pick whichever rmcp re-exports.
4. The `ServiceExt::serve` setup at the bottom: rmcp likely has a helper for stdio transport like `rmcp::transport::Stdio`. If `(stdin(), stdout())` doesn't work as a transport directly, use rmcp's stdio helper.

Behavior to preserve:
- 1 tool named `echo`, accepting an object with optional `x: integer`.
- 1 resource named `fixed` at URI `fake://fixed`, returning text "fixed-resource-content".
- 1 prompt named `greet`, no arguments, returns a single user message "hello from greet".

If you make significant adaptations, document them in the commit message.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/tests/fixtures/fake-mcp/main.rs
git commit -m "test(common): add fake-mcp fixture binary (1 tool, 1 resource, 1 prompt)"
```

---

### Task 9: tests/mcp_lifecycle.rs — integration test using fake-mcp

**Files:**
- Create: `plexus-common/tests/mcp_lifecycle.rs`

- [ ] **Step 1: Create the integration test**

Create `plexus-common/tests/mcp_lifecycle.rs`:

```rust
//! Integration test: spawn fake-mcp via lifecycle::spawn_mcp, exercise
//! all six McpSession methods (list_tools/resources/prompts +
//! call_tool/read_resource/get_prompt), then teardown.
//!
//! The fake-mcp fixture is a separate binary in this crate (declared as
//! [[bin]] in Cargo.toml). Cargo sets `CARGO_BIN_EXE_fake-mcp` to its
//! built path when running integration tests.

use plexus_common::mcp::lifecycle::{spawn_mcp, teardown_mcp};
use plexus_common::protocol::McpServerConfig;
use serde_json::json;
use std::collections::HashMap;

fn fake_mcp_config() -> McpServerConfig {
    McpServerConfig {
        command: vec![env!("CARGO_BIN_EXE_fake-mcp").to_string()],
        env: HashMap::new(),
        description: None,
        enabled: None,
    }
}

#[tokio::test]
async fn spawn_then_list_then_teardown() {
    let config = fake_mcp_config();
    let (session, schemas) = spawn_mcp(&config).await.expect("spawn");
    assert_eq!(schemas.tools.len(), 1, "fake-mcp advertises 1 tool");
    assert_eq!(schemas.tools[0].name, "echo");
    assert_eq!(schemas.resources.len(), 1, "fake-mcp advertises 1 resource");
    assert_eq!(schemas.resources[0].uri, "fake://fixed");
    assert_eq!(schemas.prompts.len(), 1, "fake-mcp advertises 1 prompt");
    assert_eq!(schemas.prompts[0].name, "greet");
    teardown_mcp(session).await;
}

#[tokio::test]
async fn call_tool_returns_echoed_args() {
    let config = fake_mcp_config();
    let (session, _) = spawn_mcp(&config).await.expect("spawn");
    let result = session
        .call_tool("echo", json!({"x": 42}))
        .await
        .expect("call_tool");
    assert!(
        result.contains("42"),
        "echo result should contain the arg, got: {result}"
    );
    assert!(
        result.starts_with("echoed:"),
        "echo result should be tagged, got: {result}"
    );
    teardown_mcp(session).await;
}

#[tokio::test]
async fn read_resource_returns_text() {
    let config = fake_mcp_config();
    let (session, _) = spawn_mcp(&config).await.expect("spawn");
    let result = session
        .read_resource("fake://fixed")
        .await
        .expect("read_resource");
    assert_eq!(result, "fixed-resource-content");
    teardown_mcp(session).await;
}

#[tokio::test]
async fn get_prompt_returns_joined_messages() {
    let config = fake_mcp_config();
    let (session, _) = spawn_mcp(&config).await.expect("spawn");
    let result = session
        .get_prompt("greet", json!({}))
        .await
        .expect("get_prompt");
    assert_eq!(result, "hello from greet");
    teardown_mcp(session).await;
}

#[tokio::test]
async fn spawn_with_invalid_command_fails() {
    let config = McpServerConfig {
        command: vec!["/this/binary/does/not/exist".to_string()],
        env: HashMap::new(),
        description: None,
        enabled: None,
    };
    let result = spawn_mcp(&config).await;
    assert!(result.is_err(), "expected spawn to fail for nonexistent binary");
}

#[tokio::test]
async fn spawn_with_empty_command_fails() {
    let config = McpServerConfig {
        command: vec![],
        env: HashMap::new(),
        description: None,
        enabled: None,
    };
    let result = spawn_mcp(&config).await;
    assert!(result.is_err(), "expected spawn to fail for empty command");
}
```

- [ ] **Step 2: Run the integration test**

Run: `cargo test --workspace -p plexus-common --test mcp_lifecycle`

Expected: 6 tests pass.

If tests fail because of fixture / wrapper API mismatches:
- The fixture's behavior is fixed by Task 8 (1 tool, 1 resource, 1 prompt with specific names/output).
- If the wrapper extracts tool/resource/prompt names differently, fix the wrapper in `session.rs` (Task 6).
- The fixture can be debugged manually: `cargo run --bin fake-mcp` then send JSON-RPC manually via stdin (advanced — usually not needed; trust rmcp's protocol implementation).

- [ ] **Step 3: Commit**

```bash
git add plexus-common/tests/mcp_lifecycle.rs
git commit -m "test(common): integration — spawn fake-mcp + exercise all 6 session methods"
```

---

### Task 10: plexus-common/README.md

**Files:**
- Create: `plexus-common/README.md`

- [ ] **Step 1: Write the README**

Create `plexus-common/README.md`:

```markdown
# plexus-common

Shared types, errors, protocol, and tool infrastructure for the Plexus rebuild.

`plexus-common` is the foundation crate of the Plexus workspace. It is consumed
by `plexus-server` (M1) and `plexus-client` (M2) and contains everything that
both sides need to agree on — wire formats, error codes, tool schemas, MCP
wrapping, secret handling.

This crate has **no async runtime of its own** and **no IO**. It declares the
shapes; the runtime crates implement against them.

## Modules

- **`consts`** — wire-level reserved string constants (`[untrusted message from <name>]:`,
  the `plexus_` field prefix, etc.).
- **`version`** — `PROTOCOL_VERSION` and a `crate_version()` helper.
- **`secrets`** — redacting newtypes (`DeviceToken`, `JwtSecret`, `LlmApiKey`)
  wrapping `secrecy::SecretString`. Their `Debug`/`Display` impls always
  return `"<redacted>"`.
- **`errors`** — six typed error enums (`Workspace`, `Tool`, `Auth`, `Protocol`,
  `Mcp`, `Network`) all implementing `Code → ErrorCode` for stable wire-level
  mapping.
- **`protocol`** — WebSocket frame structs (`WsFrame` enum + 12 variants),
  binary-transfer header layout, frame inner types (`DeviceConfig`,
  `McpServerConfig`, `McpSchemas`, etc.).
- **`tools`** — the `Tool` trait, file-tool jail (`resolve_in_workspace`),
  result wrap helper (`wrap_result`), output formatters
  (`with_line_numbers`, `truncate_head`), all 14 first-class tool schemas
  as `LazyLock<serde_json::Value>` (and matching `LazyLock<Validator>` for
  cached JSON Schema validation), plus `validate_args` / `validate_with`.
- **`mcp`** — typed-infix wrapped names (`mcp_<server>_<tool>` / `_resource_<n>` /
  `_prompt_<n>`), URI template parser, `enabled` glob filter, `McpSession`
  wrapping `rmcp` 1.5.0's client, and the `spawn_mcp` / `teardown_mcp`
  lifecycle helpers.

## Design references

- Spec: [`docs/reference/superpowers/specs/2026-04-28-plexus-m0-design.md`](../docs/reference/superpowers/specs/2026-04-28-plexus-m0-design.md)
- Architecture decisions: [`docs/DECISIONS.md`](../docs/DECISIONS.md)
- WebSocket protocol: [`docs/PROTOCOL.md`](../docs/PROTOCOL.md)
- Tool catalog: [`docs/TOOLS.md`](../docs/TOOLS.md)

## Status

M0 deliverable. The public API surface is **frozen** here — additions during M1
or M2 should be exceptional and flagged as M0 underscoping. If a real new
need emerges, it goes through an ADR before landing in common.

## License

Apache 2.0.
```

- [ ] **Step 2: Verify it renders cleanly**

Run: `cargo doc --no-deps -p plexus-common`

(The README is not auto-rendered by rustdoc unless you point `[package.readme]` at it; for now, just verify the markdown syntax is reasonable. No test command — visual check only.)

- [ ] **Step 3: Commit**

```bash
git add plexus-common/README.md
git commit -m "docs(common): add 1-page crate README"
```

---

### Task 11: Final verification + lib.rs re-exports

**Files:**
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Update lib.rs with mcp/* re-exports**

Read the existing `plexus-common/src/lib.rs` first. After Plan 2 it has Plan 1 + Plan 2 re-exports. Add the Plan 3 mcp/* re-exports.

Replace `plexus-common/src/lib.rs` with:

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
//! # Plan 2 surface (Tools)
//!
//! - [`tools`] — Tool trait + path validation + result wrap + format helpers
//!   + 14 hardcoded tool schemas + JSON Schema arg validation.
//!
//! # Plan 3 surface (MCP)
//!
//! - [`mcp`] — typed-infix wrapped names + `enabled` glob filter + URI
//!   template parsing + `McpSession` wrapping rmcp + `spawn_mcp` /
//!   `teardown_mcp` lifecycle.

pub mod consts;
pub mod errors;
pub mod mcp;
pub mod protocol;
pub mod secrets;
pub mod tools;
pub mod version;

// Top-level re-exports for ergonomic access.
pub use errors::{
    AuthError, Code, ErrorCode, McpError, NetworkError, ProtocolError, ToolError, WorkspaceError,
};
pub use mcp::filter::EnabledFilter;
pub use mcp::lifecycle::{spawn_mcp, teardown_mcp};
pub use mcp::naming::{
    McpSurface, WrappedName, parse_wrapped_name, wrap_prompt_name, wrap_resource_name,
    wrap_tool_name,
};
pub use mcp::session::McpSession;
pub use protocol::{
    ConfigUpdateFrame, DeviceConfig, ErrorFrame, FsPolicy, HEADER_SIZE, HelloAckFrame, HelloCaps,
    HelloFrame, McpSchemas, McpServerConfig, PingFrame, PongFrame, PromptArgument, PromptDef,
    RegisterMcpFrame, ResourceDef, SpawnFailure, ToolCallFrame, ToolDef, ToolResultFrame,
    TransferBeginFrame, TransferDirection, TransferEndFrame, TransferProgressFrame, WsFrame,
    pack_chunk, parse_chunk,
};
pub use secrets::{DeviceToken, JwtSecret, LlmApiKey};
pub use tools::Tool;
pub use tools::result::wrap_result;
pub use tools::validate::{validate_args, validate_with};
pub use version::{PROTOCOL_VERSION, crate_version};
```

If `cargo fmt` reformats the order, accept the reformatted version.

- [ ] **Step 2: Run the full test suite**

Run: `cargo test --workspace`

Expected: all tests pass. Roughly:
- Plan 1 + 2 inherited: ~125
- Plan 3 unit tests added: ~11 (naming) + ~8 (filter) + ~12 (wrap) = ~31
- Plan 3 integration tests: 6 (mcp_lifecycle)
- **Total: ~160-165 tests**

- [ ] **Step 3: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: clean. Fix any warnings inline.

- [ ] **Step 4: Run fmt check**

Run: `cargo fmt --all --check`

Expected: clean. If diff, run `cargo fmt --all` and re-check.

- [ ] **Step 5: Build for both musl targets**

```bash
cargo build --workspace --target x86_64-unknown-linux-musl
cargo build --workspace --target aarch64-unknown-linux-musl
```

Expected: both succeed.

- [ ] **Step 6: cargo doc clean**

```bash
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p plexus-common
```

Expected: builds without warnings. Per the spec acceptance criteria, rustdoc warnings should be zero. Fix any inline.

- [ ] **Step 7: Public API audit**

Look at `plexus-common/src/lib.rs` and walk every `pub use` line. For each: should this be at top level, or should it stay nested? Goal: ergonomic for M1/M2 dispatchers.

The current set:
- Top-level: error types, frame structs, secrets, `Tool`, `wrap_result`, `validate_args`/`validate_with`, version constants, mcp filter/lifecycle/naming/session items.
- Stays nested (not re-exported at top level): tool schemas (`tools::schemas::READ_FILE_SCHEMA` is the canonical path), path validation (`tools::path::resolve_in_workspace`), format helpers (`tools::format::with_line_numbers`), wrap helpers (`mcp::wrap::*`), constants (`consts::*`).

Confirm this matches your intent. If anything should move, edit `lib.rs` and re-run tests.

- [ ] **Step 8: Acceptance criteria checklist**

Confirm each:
- [ ] All ~160 tests pass.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo fmt --all --check` clean.
- [ ] `cargo build --target x86_64-unknown-linux-musl -p plexus-common` succeeds.
- [ ] `cargo build --target aarch64-unknown-linux-musl -p plexus-common` succeeds.
- [ ] `cargo doc --no-deps -p plexus-common` clean (zero rustdoc warnings).
- [ ] `Cargo.lock` committed.
- [ ] No `unwrap()`/`expect()` in non-test code (allowed in `parse_chunk`, `tools::path`, and `schemas::compile` where they document unreachable invariants on hardcoded data).
- [ ] No `unsafe` code.
- [ ] All 14 tool schemas reachable via `plexus_common::tools::schemas::*`.
- [ ] `Tool` trait reachable from `plexus_common::Tool`.
- [ ] Six `McpSession` methods reachable via `plexus_common::McpSession`.
- [ ] `spawn_mcp`/`teardown_mcp` reachable at top level.
- [ ] README explains the crate in 1 page.
- [ ] Public API surface in `lib.rs` reviewed and intentional — no accidentally `pub` items.

- [ ] **Step 9: Commit lib.rs**

```bash
git add plexus-common/src/lib.rs
git commit -m "feat(common): finalize Plan 3 lib.rs re-exports — M0 complete"
```

- [ ] **Step 10: Push**

```bash
git push origin rebuild-m0
```

CI runs on push. Verify all gates green.

---

## Plan 3 acceptance criteria

End of Plan 3 = end of M0. The full M0 acceptance criteria from the spec §7 must be met:

- [ ] All 16 items from spec §3 module layout implemented across Plans 1, 2, 3.
- [ ] Public API surface frozen — additions during M1/M2 require explicit ADR.
- [ ] Zero `.unwrap()`/`.expect()`/`.panic!()` in non-test code beyond the 3 documented unreachables.
- [ ] Zero `unsafe` code.
- [ ] ~160 tests passing across the full suite (unit + 3 integration tests).
- [ ] All CI gates green: clippy, fmt, test, build × 2 musl targets.
- [ ] Cargo.lock committed.
- [ ] `plexus-common/README.md` written.
- [ ] CI green on `rebuild-m0` HEAD.

When all boxes checked, M0 is done. Next steps:
1. Open a PR from `rebuild-m0` to `rebuild` (or whatever the integration branch is) for human review.
2. After merge, kick off **M1 (`plexus-server`)** as a fresh planning cycle: spec → plans → execution.

---

## Post-Plan Adjustments

Reserved for the implementation pass to record any deltas from this plan, the rationale, and SHAs (per `feedback_docs_alignment.md`). Empty until execution begins.
