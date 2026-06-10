# Plexus M0 — Plan 2: Tools — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the entire `tools/` module of `plexus-common` — shared tool infrastructure that both `plexus-server` (M1) and `plexus-client` (M2) build on. Six submodules (path validation, result wrapping, output formatting, tool schemas, Tool trait, JSON-Schema validation) plus 2 integration tests.

**Architecture:** Pure-Rust library code, no IO or async logic of its own. The `Tool` trait declares the contract; concrete tool impls land in M1/M2. Schemas are `LazyLock<serde_json::Value>` — parsed once per process, accessed via `&*SCHEMA_NAME`. Path validation is the OS-agnostic file-tool jail (ADR-073/105) — the only thing keeping file tools in their lane on macOS/Windows where bwrap doesn't exist.

**Tech Stack:** Rust 1.90+, edition 2024. New deps for this plan: `tokio` (workspace dep, for async Tool trait), `jsonschema` 0.30 (workspace dep, for arg validation), `async-trait` 0.1 (Tool trait async fn with Send bounds), `tempfile` 3 (dev-dep, for path-validation tests).

**Spec:** [docs/reference/superpowers/specs/2026-04-28-plexus-m0-design.md](../specs/2026-04-28-plexus-m0-design.md) (§3 module layout row 2; §5 testing strategy)

**Branch:** `rebuild-m0` (Plan 1 already shipped; common has consts/version/secrets/errors/protocol/).

---

## File map

| Path | Responsibility |
|---|---|
| `plexus-common/Cargo.toml` | Add tokio, jsonschema, async-trait deps; tempfile + tokio test feature for dev |
| `plexus-common/src/tools/mod.rs` | Tool trait (ADR-077); module facade |
| `plexus-common/src/tools/result.rs` | `wrap_result()` per ADR-095 |
| `plexus-common/src/tools/path.rs` | `resolve_in_workspace()` per ADR-073/105 — file-tool jail |
| `plexus-common/src/tools/format.rs` | `prepend_line_numbers()`, `truncate_head()` — output helpers |
| `plexus-common/src/tools/schemas.rs` | All 14 hardcoded tool schemas as `LazyLock<Value>` |
| `plexus-common/src/tools/validate.rs` | `validate_args()` — JSON Schema arg validation |
| `plexus-common/src/lib.rs` | Add `pub mod tools;` + final re-exports |
| `plexus-common/tests/end_to_end_schema_pipeline.rs` | Integration test: schema → validate → wrap → frame ser/de |
| `plexus-common/tests/secret_no_leak.rs` | Integration test: secret newtypes never leak through formatting |

Total: ~7 files in `src/tools/`, 2 integration tests, ~2500 LoC code + ~700 LoC tests.

---

## Conventions

- **Tests live in same file** for unit tests (`#[cfg(test)] mod tests`); cross-module/cross-crate tests in `tests/`.
- **Run all tests** via: `cargo test --workspace -p plexus-common`. Run a single test via: `cargo test --workspace -p plexus-common <test_name>`.
- **Cargo working dir is `/home/yucheng/Documents/GitHub/Plexus`** for all commands.
- **Commit after every passing task.** Frequent commits = small reverts.
- **Code minimal-comment per project CLAUDE.md.** Doc comments referencing ADRs are fine; narrating-the-change comments are not.

---

### Task 1: Add tokio + jsonschema + async-trait deps

**Files:**
- Modify: `plexus-common/Cargo.toml`

- [ ] **Step 1: Add dependencies**

Replace the `[dependencies]` and `[dev-dependencies]` blocks of `plexus-common/Cargo.toml`:

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

[dev-dependencies]
proptest = { workspace = true }
pretty_assertions = { workspace = true }
tempfile = "3"
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

Note: `tokio` already in workspace deps from Plan 1 (used by `process` later). The crate-level `[dependencies]` line just declares it consumed. The `dev-dependencies` add the test runtime + `tempfile` for path-validation tests.

- [ ] **Step 2: Verify build still works**

Run: `cargo build --workspace -p plexus-common`

Expected: succeeds. The new deps compile but aren't used yet — no errors.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/Cargo.toml
git commit -m "chore(common): add tokio + jsonschema + async-trait deps for Plan 2"
```

---

### Task 2: tools/ module scaffold

**Files:**
- Create: `plexus-common/src/tools/mod.rs`
- Create: `plexus-common/src/tools/format.rs` (stub)
- Create: `plexus-common/src/tools/path.rs` (stub)
- Create: `plexus-common/src/tools/result.rs` (stub)
- Create: `plexus-common/src/tools/schemas.rs` (stub)
- Create: `plexus-common/src/tools/validate.rs` (stub)
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Create tools directory**

```bash
mkdir -p plexus-common/src/tools
```

- [ ] **Step 2: Create the module facade**

Create `plexus-common/src/tools/mod.rs`:

```rust
//! Shared tool infrastructure. See ADR-038, ADR-077, ADR-095.
//!
//! - [`result`] — wrap_result() for the [untrusted tool result]: prefix.
//! - [`path`] — resolve_in_workspace() for the file-tool jail.
//! - [`format`] — line-numbered output and head-only truncation helpers.
//! - [`schemas`] — hardcoded JSON schemas for the 14 first-class tools.
//! - [`validate`] — JSON Schema validation for tool_call args.
//!
//! The [`Tool`] trait itself lands in this module in Task 8.

pub mod format;
pub mod path;
pub mod result;
pub mod schemas;
pub mod validate;
```

- [ ] **Step 3: Create the 5 stub files**

Create each stub file with a doc comment placeholder (Tasks 3-9 fill them):

`plexus-common/src/tools/result.rs`:
```rust
//! Stub — full impl in Task 3.
```

`plexus-common/src/tools/path.rs`:
```rust
//! Stub — full impl in Task 4.
```

`plexus-common/src/tools/format.rs`:
```rust
//! Stub — full impl in Task 5.
```

`plexus-common/src/tools/schemas.rs`:
```rust
//! Stub — full impl in Tasks 6-7.
```

`plexus-common/src/tools/validate.rs`:
```rust
//! Stub — full impl in Task 9.
```

- [ ] **Step 4: Wire in lib.rs**

Edit `plexus-common/src/lib.rs` — add `pub mod tools;` to the module list (alphabetical after `secrets`, before `version`):

The existing module declarations should now read:
```rust
pub mod consts;
pub mod errors;
pub mod protocol;
pub mod secrets;
pub mod tools;
pub mod version;
```

- [ ] **Step 5: Verify build**

Run: `cargo build --workspace -p plexus-common`

Expected: succeeds.

- [ ] **Step 6: Commit**

```bash
git add plexus-common/src/tools plexus-common/src/lib.rs
git commit -m "feat(common): add tools/ module scaffold (5 stubs)"
```

---

### Task 3: tools/result.rs — wrap_result helper

**Files:**
- Modify: `plexus-common/src/tools/result.rs`

- [ ] **Step 1: Write the failing tests**

Replace `plexus-common/src/tools/result.rs` with:

```rust
//! Tool result wrapping per ADR-095.
//!
//! Every tool's result content is prefixed with `[untrusted tool result]: `
//! at construction time, before reaching the LLM. Uniform across all tools.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::UNTRUSTED_TOOL_RESULT_PREFIX;

    #[test]
    fn wrap_empty_string() {
        assert_eq!(wrap_result(""), UNTRUSTED_TOOL_RESULT_PREFIX);
    }

    #[test]
    fn wrap_plain_text() {
        let result = wrap_result("hello");
        assert!(result.starts_with(UNTRUSTED_TOOL_RESULT_PREFIX));
        assert!(result.ends_with("hello"));
        assert_eq!(result.len(), UNTRUSTED_TOOL_RESULT_PREFIX.len() + 5);
    }

    #[test]
    fn wrap_already_wrapped_double_wraps() {
        // Double-wrapping is intentional behavior per ADR-095:
        // every call to wrap_result prepends the prefix unconditionally.
        let once = wrap_result("hello");
        let twice = wrap_result(&once);
        let triple_check = format!("{UNTRUSTED_TOOL_RESULT_PREFIX}{once}");
        assert_eq!(twice, triple_check);
    }

    #[test]
    fn wrap_multiline_text() {
        let raw = "line 1\nline 2\nline 3";
        let wrapped = wrap_result(raw);
        assert_eq!(
            wrapped,
            format!("{UNTRUSTED_TOOL_RESULT_PREFIX}line 1\nline 2\nline 3")
        );
    }

    #[test]
    fn wrap_preserves_unicode() {
        let raw = "héllo 世界 🦀";
        let wrapped = wrap_result(raw);
        assert!(wrapped.contains("héllo 世界 🦀"));
    }
}
```

- [ ] **Step 2: Run the tests — should fail**

Run: `cargo test --workspace -p plexus-common tools::result::`

Expected: compile failure (`wrap_result` undefined).

- [ ] **Step 3: Implement**

Add above the test block in `plexus-common/src/tools/result.rs`:

```rust
//! Tool result wrapping per ADR-095.
//!
//! Every tool's result content is prefixed with `[untrusted tool result]: `
//! at construction time, before reaching the LLM. Uniform across all tools.

use crate::consts::UNTRUSTED_TOOL_RESULT_PREFIX;

/// Wrap a raw tool result with the untrusted-tool-result prefix.
///
/// The prefix is the structural signal to the LLM that the content
/// should not be followed as instructions (ADR-095). Every tool's
/// dispatcher should call this before emitting the `tool_result` block.
pub fn wrap_result(raw: &str) -> String {
    let mut out = String::with_capacity(UNTRUSTED_TOOL_RESULT_PREFIX.len() + raw.len());
    out.push_str(UNTRUSTED_TOOL_RESULT_PREFIX);
    out.push_str(raw);
    out
}
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common tools::result::`

Expected: 5 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/tools/result.rs
git commit -m "feat(common): add tools::result::wrap_result helper (ADR-095)"
```

---

### Task 4: tools/path.rs — resolve_in_workspace (file-tool jail)

The OS-agnostic file-tool jail per ADR-073/105. Validates that any path the agent supplies stays inside `workspace_root` after canonicalization. Symlinks that point outside are caught because `canonicalize()` follows them.

**Files:**
- Modify: `plexus-common/src/tools/path.rs`

- [ ] **Step 1: Write the failing tests**

Replace `plexus-common/src/tools/path.rs` with:

```rust
//! File-tool jail — `resolve_in_workspace` per ADR-073, ADR-105.
//!
//! Every shared file tool (read_file/write_file/edit_file/...) calls this
//! helper before any disk operation. Pure Rust path validation, no OS
//! primitive — works identically on Linux/macOS/Windows.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::WorkspaceError;
    use std::fs;
    use tempfile::TempDir;

    fn workspace() -> TempDir {
        TempDir::new().expect("create tempdir")
    }

    #[test]
    fn relative_path_resolves_under_root() {
        let ws = workspace();
        let resolved = resolve_in_workspace(ws.path(), "MEMORY.md").unwrap();
        // Compare canonicalized — TempDir on macOS may use /private/var symlink.
        assert!(resolved.starts_with(ws.path().canonicalize().unwrap()));
        assert!(resolved.ends_with("MEMORY.md"));
    }

    #[test]
    fn relative_subdir_resolves_under_root() {
        let ws = workspace();
        fs::create_dir(ws.path().join("subdir")).unwrap();
        let resolved = resolve_in_workspace(ws.path(), "subdir/file.txt").unwrap();
        assert!(resolved.ends_with("subdir/file.txt"));
    }

    #[test]
    fn absolute_path_inside_workspace_accepted() {
        let ws = workspace();
        let canonical = ws.path().canonicalize().unwrap();
        let inside = canonical.join("file.txt");
        let resolved = resolve_in_workspace(ws.path(), inside.to_str().unwrap()).unwrap();
        assert_eq!(resolved, inside);
    }

    #[test]
    fn absolute_path_outside_workspace_rejected() {
        let ws = workspace();
        let result = resolve_in_workspace(ws.path(), "/etc/passwd");
        assert!(matches!(
            result,
            Err(WorkspaceError::PathOutsideWorkspace(_))
        ));
    }

    #[test]
    fn dot_dot_traversal_rejected() {
        let ws = workspace();
        // ../etc/passwd resolves to ws.parent/etc/passwd which is outside ws.
        let result = resolve_in_workspace(ws.path(), "../etc/passwd");
        assert!(matches!(
            result,
            Err(WorkspaceError::PathOutsideWorkspace(_))
        ));
    }

    #[test]
    fn dot_dot_that_stays_inside_accepted() {
        let ws = workspace();
        fs::create_dir(ws.path().join("a")).unwrap();
        fs::create_dir(ws.path().join("b")).unwrap();
        // a/../b resolves to b which is inside.
        let resolved = resolve_in_workspace(ws.path(), "a/../b").unwrap();
        assert!(resolved.ends_with("b"));
    }

    #[test]
    fn empty_path_rejected() {
        let ws = workspace();
        let result = resolve_in_workspace(ws.path(), "");
        assert!(matches!(
            result,
            Err(WorkspaceError::PathOutsideWorkspace(_))
        ));
    }

    #[test]
    fn workspace_root_does_not_exist_rejected() {
        let result = resolve_in_workspace(
            std::path::Path::new("/nonexistent/totally/fake/dir"),
            "file.txt",
        );
        assert!(matches!(result, Err(WorkspaceError::NotFound(_))));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_inside_pointing_outside_rejected() {
        use std::os::unix::fs::symlink;
        let ws = workspace();
        // Create /tmp/<rand> so we have something outside the workspace
        // to point a symlink at. We use the temp_dir() parent so the path
        // exists and is genuinely outside ws.
        let outside_target = std::env::temp_dir();
        let link = ws.path().join("escape");
        symlink(&outside_target, &link).unwrap();
        let result = resolve_in_workspace(ws.path(), "escape");
        assert!(matches!(
            result,
            Err(WorkspaceError::PathOutsideWorkspace(_))
        ));
    }

    #[test]
    fn nonexistent_file_under_workspace_accepted() {
        let ws = workspace();
        // For write_file we need to allow new file paths. Validation
        // checks the parent dir's canonicalization, not the file's.
        let resolved = resolve_in_workspace(ws.path(), "new_file_to_create.txt").unwrap();
        assert!(resolved.ends_with("new_file_to_create.txt"));
    }

    #[test]
    fn nonexistent_file_in_nonexistent_subdir_rejected() {
        let ws = workspace();
        // Parent must exist for validation. Don't auto-mkdir nested paths.
        let result = resolve_in_workspace(ws.path(), "no/such/dir/file.txt");
        assert!(matches!(result, Err(WorkspaceError::NotFound(_))));
    }

    #[test]
    fn workspace_root_itself_accepted() {
        let ws = workspace();
        let canonical = ws.path().canonicalize().unwrap();
        let resolved =
            resolve_in_workspace(ws.path(), canonical.to_str().unwrap()).unwrap();
        assert_eq!(resolved, canonical);
    }

    #[test]
    fn trailing_slash_handled() {
        let ws = workspace();
        fs::create_dir(ws.path().join("subdir")).unwrap();
        let resolved = resolve_in_workspace(ws.path(), "subdir/").unwrap();
        assert!(resolved.ends_with("subdir"));
    }

    #[test]
    fn deep_nested_path_under_workspace_accepted() {
        let ws = workspace();
        fs::create_dir_all(ws.path().join("a/b/c")).unwrap();
        let resolved = resolve_in_workspace(ws.path(), "a/b/c/file.txt").unwrap();
        let canonical = ws.path().canonicalize().unwrap();
        assert!(resolved.starts_with(&canonical));
    }

    #[test]
    fn absolute_path_to_workspace_parent_rejected() {
        let ws = workspace();
        let parent = ws.path().parent().unwrap();
        let result = resolve_in_workspace(ws.path(), parent.to_str().unwrap());
        assert!(matches!(
            result,
            Err(WorkspaceError::PathOutsideWorkspace(_))
        ));
    }
}
```

- [ ] **Step 2: Run the tests — should fail**

Run: `cargo test --workspace -p plexus-common tools::path::`

Expected: compile failure (`resolve_in_workspace` undefined).

- [ ] **Step 3: Implement**

Add above the test block:

```rust
//! File-tool jail — `resolve_in_workspace` per ADR-073, ADR-105.
//!
//! Every shared file tool (read_file/write_file/edit_file/...) calls this
//! helper before any disk operation. Pure Rust path validation, no OS
//! primitive — works identically on Linux/macOS/Windows.

use crate::errors::WorkspaceError;
use std::path::{Path, PathBuf};

/// Resolve `path` (relative or absolute) against `workspace_root` and verify
/// the result stays inside `workspace_root` after canonicalization.
///
/// - Relative paths are joined onto `workspace_root`.
/// - Absolute paths are accepted as-is for validation.
/// - The path itself need NOT exist (so write_file can create new files).
///   The path's parent directory MUST exist — if it doesn't, returns
///   `WorkspaceError::NotFound(parent)`.
/// - Symlinks anywhere in the path are followed via `canonicalize()`. A
///   symlink that points outside the workspace fails the boundary check.
/// - The workspace root itself MUST exist; missing root returns
///   `WorkspaceError::NotFound(root)`.
///
/// Returns the canonicalized absolute path on success.
pub fn resolve_in_workspace(
    workspace_root: &Path,
    path: &str,
) -> Result<PathBuf, WorkspaceError> {
    if path.is_empty() {
        return Err(WorkspaceError::PathOutsideWorkspace(PathBuf::from(path)));
    }

    let canonical_root = workspace_root
        .canonicalize()
        .map_err(|_| WorkspaceError::NotFound(workspace_root.to_path_buf()))?;

    let candidate = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        canonical_root.join(path)
    };

    let resolved = match candidate.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            // Path doesn't exist (e.g. write to a new file).
            // Canonicalize the parent and re-attach the basename;
            // this still catches symlink escapes via the parent.
            let parent = candidate
                .parent()
                .ok_or_else(|| WorkspaceError::PathOutsideWorkspace(candidate.clone()))?;
            let canonical_parent = parent
                .canonicalize()
                .map_err(|_| WorkspaceError::NotFound(parent.to_path_buf()))?;
            let basename = candidate
                .file_name()
                .ok_or_else(|| WorkspaceError::PathOutsideWorkspace(candidate.clone()))?;
            canonical_parent.join(basename)
        }
    };

    if !resolved.starts_with(&canonical_root) {
        return Err(WorkspaceError::PathOutsideWorkspace(resolved));
    }

    Ok(resolved)
}
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common tools::path::`

Expected: 14 tests passed (15 if running on Unix — the symlink test is `#[cfg(unix)]`).

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/tools/path.rs
git commit -m "feat(common): add tools::path::resolve_in_workspace (ADR-073/105)"
```

---

### Task 5: tools/format.rs — line numbering + truncation

**Files:**
- Modify: `plexus-common/src/tools/format.rs`

- [ ] **Step 1: Write the failing tests**

Replace `plexus-common/src/tools/format.rs` with:

```rust
//! Output formatting helpers for file tools.
//!
//! - [`prepend_line_numbers`] — render `LINE_NUM|content` for read_file output.
//! - [`truncate_head`] — head-only character clipping with marker.
//! Both are pure UTF-8 safe.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepend_line_numbers_empty() {
        assert_eq!(prepend_line_numbers(""), "");
    }

    #[test]
    fn prepend_line_numbers_single_line_no_trailing_newline() {
        assert_eq!(prepend_line_numbers("hello"), "1|hello");
    }

    #[test]
    fn prepend_line_numbers_single_line_with_trailing_newline() {
        assert_eq!(prepend_line_numbers("hello\n"), "1|hello\n");
    }

    #[test]
    fn prepend_line_numbers_multiple_lines() {
        let input = "alpha\nbeta\ngamma";
        let expected = "1|alpha\n2|beta\n3|gamma";
        assert_eq!(prepend_line_numbers(input), expected);
    }

    #[test]
    fn prepend_line_numbers_blank_lines_are_numbered() {
        let input = "a\n\nb";
        let expected = "1|a\n2|\n3|b";
        assert_eq!(prepend_line_numbers(input), expected);
    }

    #[test]
    fn truncate_head_short_input_unchanged() {
        let input = "short text";
        assert_eq!(truncate_head(input, 100), "short text");
    }

    #[test]
    fn truncate_head_exact_length_unchanged() {
        let input = "exact-len";
        assert_eq!(truncate_head(input, 9), "exact-len");
    }

    #[test]
    fn truncate_head_long_input_clipped_with_marker() {
        let input = "a".repeat(200);
        let result = truncate_head(&input, 50);
        assert!(result.starts_with(&"a".repeat(50)));
        assert!(result.ends_with("\n... (truncated)"));
    }

    #[test]
    fn truncate_head_does_not_split_multibyte_codepoint() {
        // "héllo" — 'é' is 2 bytes (U+00E9 = 0xC3 0xA9).
        // If max=2 chars (in our byte-based truncation), we must not split mid-codepoint.
        let input = "héllo";
        let result = truncate_head(input, 2);
        // Either "h" (truncate before 'é') or "hé" (truncate after 'é'),
        // never mid-codepoint. Both end with the marker.
        assert!(
            result == "h\n... (truncated)" || result == "hé\n... (truncated)",
            "unexpected truncation: {:?}",
            result
        );
    }

    #[test]
    fn truncate_head_zero_max_produces_only_marker() {
        let input = "anything";
        let result = truncate_head(input, 0);
        assert_eq!(result, "\n... (truncated)");
    }

    #[test]
    fn truncate_head_empty_input_unchanged() {
        assert_eq!(truncate_head("", 100), "");
    }
}
```

- [ ] **Step 2: Run the tests — should fail**

Run: `cargo test --workspace -p plexus-common tools::format::`

Expected: compile failure (`prepend_line_numbers`, `truncate_head` undefined).

- [ ] **Step 3: Implement**

Add above the test block:

```rust
//! Output formatting helpers for file tools.
//!
//! - [`prepend_line_numbers`] — render `LINE_NUM|content` for read_file output.
//! - [`truncate_head`] — head-only character clipping with marker.
//! Both are pure UTF-8 safe.

const TRUNCATION_MARKER: &str = "\n... (truncated)";

/// Render `text` with each line prefixed by its 1-indexed line number,
/// using `LINE_NUM|` as the separator (matches nanobot's read_file output).
///
/// Trailing newlines are preserved. Blank lines are numbered.
pub fn prepend_line_numbers(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    let has_trailing_newline = text.ends_with('\n');
    let body = if has_trailing_newline {
        &text[..text.len() - 1]
    } else {
        text
    };
    let mut out = String::with_capacity(text.len() + 16);
    for (i, line) in body.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("{n}|{line}", n = i + 1));
    }
    if has_trailing_newline {
        out.push('\n');
    }
    out
}

/// Truncate `text` to at most `max_chars` Unicode characters, appending
/// the truncation marker if cut. UTF-8-safe: never splits a codepoint.
///
/// `max_chars` counts Unicode scalar values (Rust `char`s), not bytes.
pub fn truncate_head(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max_chars).collect();
    out.push_str(TRUNCATION_MARKER);
    out
}
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common tools::format::`

Expected: 11 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/tools/format.rs
git commit -m "feat(common): add tools::format — line numbering + head-only truncation"
```

---

### Task 6: tools/schemas.rs — 10 shared tool schemas

The first half of the schemas: 9 file tools + web_fetch (all the schemas marked "shared" in `docs/TOOLS.md`).

**Files:**
- Modify: `plexus-common/src/tools/schemas.rs`

- [ ] **Step 1: Write a meta-validation test that asserts every schema is valid JSON**

Replace `plexus-common/src/tools/schemas.rs` with the test stub first:

```rust
//! Hardcoded JSON schemas for the 14 first-class tools (ADR-038).
//!
//! Each schema is a `LazyLock<serde_json::Value>` parsed exactly once
//! per process via the `serde_json::json!` macro. Compile-time JSON
//! syntax check; zero runtime startup cost.

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: every schema must have name + description + input_schema fields.
    fn assert_well_formed(schema: &serde_json::Value, expected_name: &str) {
        assert_eq!(
            schema.get("name").and_then(|v| v.as_str()),
            Some(expected_name),
            "schema name mismatch"
        );
        assert!(
            schema.get("description").and_then(|v| v.as_str()).is_some(),
            "schema missing description"
        );
        let input_schema = schema
            .get("input_schema")
            .expect("schema missing input_schema");
        assert_eq!(input_schema.get("type").and_then(|v| v.as_str()), Some("object"));
    }

    #[test]
    fn read_file_schema_well_formed() {
        assert_well_formed(&READ_FILE_SCHEMA, "read_file");
    }

    #[test]
    fn write_file_schema_well_formed() {
        assert_well_formed(&WRITE_FILE_SCHEMA, "write_file");
    }

    #[test]
    fn edit_file_schema_well_formed() {
        assert_well_formed(&EDIT_FILE_SCHEMA, "edit_file");
    }

    #[test]
    fn delete_file_schema_well_formed() {
        assert_well_formed(&DELETE_FILE_SCHEMA, "delete_file");
    }

    #[test]
    fn delete_folder_schema_well_formed() {
        assert_well_formed(&DELETE_FOLDER_SCHEMA, "delete_folder");
    }

    #[test]
    fn list_dir_schema_well_formed() {
        assert_well_formed(&LIST_DIR_SCHEMA, "list_dir");
    }

    #[test]
    fn glob_schema_well_formed() {
        assert_well_formed(&GLOB_SCHEMA, "glob");
    }

    #[test]
    fn grep_schema_well_formed() {
        assert_well_formed(&GREP_SCHEMA, "grep");
    }

    #[test]
    fn notebook_edit_schema_well_formed() {
        assert_well_formed(&NOTEBOOK_EDIT_SCHEMA, "notebook_edit");
    }

    #[test]
    fn web_fetch_schema_well_formed() {
        assert_well_formed(&WEB_FETCH_SCHEMA, "web_fetch");
    }

    #[test]
    fn read_file_required_includes_path() {
        let required = READ_FILE_SCHEMA["input_schema"]["required"]
            .as_array()
            .unwrap();
        assert!(required.iter().any(|v| v.as_str() == Some("path")));
    }

    #[test]
    fn edit_file_required_includes_old_and_new_text() {
        let required = EDIT_FILE_SCHEMA["input_schema"]["required"]
            .as_array()
            .unwrap();
        let names: Vec<&str> = required.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"path"));
        assert!(names.contains(&"old_text"));
        assert!(names.contains(&"new_text"));
    }
}
```

- [ ] **Step 2: Run — should fail (constants undefined)**

Run: `cargo test --workspace -p plexus-common tools::schemas::`

Expected: compile failure.

- [ ] **Step 3: Implement the 10 schemas**

Add this above the test block (the schemas match `docs/TOOLS.md` verbatim):

```rust
//! Hardcoded JSON schemas for the 14 first-class tools (ADR-038).
//!
//! Each schema is a `LazyLock<serde_json::Value>` parsed exactly once
//! per process via the `serde_json::json!` macro. Compile-time JSON
//! syntax check; zero runtime startup cost.

use serde_json::{Value, json};
use std::sync::LazyLock;

pub static READ_FILE_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "read_file",
        "description": "Read a file (text, image, or document). Text output format: LINE_NUM|CONTENT. Images return visual content for analysis. Supports PDF, DOCX, XLSX, PPTX documents. Use offset and limit for large text files. Reads exceeding ~128K chars are truncated.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "The file path to read" },
                "offset": {
                    "type": "integer",
                    "description": "Line number to start reading from (1-indexed, default 1)",
                    "minimum": 1
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of lines to read (default 2000)",
                    "minimum": 1
                },
                "pages": {
                    "type": "string",
                    "description": "Page range for PDF files, e.g. '1-5' (default: all, max 20 pages)"
                }
            },
            "required": ["path"]
        }
    })
});

pub static WRITE_FILE_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "write_file",
        "description": "Write content to a file. Creates the file if it does not exist; overwrites if it does. Implicit mkdir -p on the parent directory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "The file path to write" },
                "content": { "type": "string", "description": "Bytes to write" }
            },
            "required": ["path", "content"]
        }
    })
});

pub static EDIT_FILE_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "edit_file",
        "description": "Replace text in a file. Three-level fuzzy match: exact, whitespace-insensitive, line-based. Set replace_all=true to replace every occurrence; default false replaces the first match only.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "old_text": { "type": "string", "description": "Text to find" },
                "new_text": { "type": "string", "description": "Replacement text" },
                "replace_all": {
                    "type": "boolean",
                    "description": "Replace every occurrence (default false)",
                    "default": false
                }
            },
            "required": ["path", "old_text", "new_text"]
        }
    })
});

pub static DELETE_FILE_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "delete_file",
        "description": "Remove a single file. Always allowed (releases quota).",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" }
            },
            "required": ["path"]
        }
    })
});

pub static DELETE_FOLDER_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "delete_folder",
        "description": "Recursively remove a folder and all its contents.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" }
            },
            "required": ["path"]
        }
    })
});

pub static LIST_DIR_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "list_dir",
        "description": "List entries in a directory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "recursive": { "type": "boolean", "default": false },
                "max_entries": { "type": "integer", "minimum": 1, "default": 1000 }
            },
            "required": ["path"]
        }
    })
});

pub static GLOB_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "glob",
        "description": "Find files matching a glob pattern (e.g. '**/*.rs'). Returns sorted list of matching paths.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "Glob pattern" },
                "path": { "type": "string", "description": "Root directory (default: workspace root)" },
                "max_results": { "type": "integer", "minimum": 1, "default": 1000 },
                "head_limit": { "type": "integer", "minimum": 1 },
                "offset": { "type": "integer", "minimum": 0, "default": 0 },
                "entry_type": {
                    "type": "string",
                    "enum": ["file", "directory", "any"],
                    "default": "file"
                }
            },
            "required": ["pattern"]
        }
    })
});

pub static GREP_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "grep",
        "description": "Search file contents for a regex pattern. Multiple output modes (content, files_with_matches, count). Supports context lines, file-type filtering, head limit, offset for pagination.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "Regular expression to search for" },
                "path": { "type": "string", "description": "Directory or file to search (default: workspace root)" },
                "output_mode": {
                    "type": "string",
                    "enum": ["content", "files_with_matches", "count"],
                    "default": "content"
                },
                "fixed_strings": { "type": "boolean", "default": false },
                "case_insensitive": { "type": "boolean", "default": false },
                "multiline": { "type": "boolean", "default": false },
                "type": { "type": "string", "description": "File-type filter (e.g. 'rust', 'python')" },
                "context_before": { "type": "integer", "minimum": 0 },
                "context_after": { "type": "integer", "minimum": 0 },
                "context": { "type": "integer", "minimum": 0, "description": "Lines of context both before and after each match" },
                "head_limit": { "type": "integer", "minimum": 1 },
                "offset": { "type": "integer", "minimum": 0, "default": 0 },
                "show_line_numbers": { "type": "boolean", "default": true }
            },
            "required": ["pattern"]
        }
    })
});

pub static NOTEBOOK_EDIT_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "notebook_edit",
        "description": "Edit a Jupyter notebook (.ipynb) cell. Three modes: replace cell at index, insert new cell after index, or delete cell at index.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "cell_index": { "type": "integer", "minimum": 0 },
                "new_source": { "type": "string", "description": "New cell source (required for replace and insert)" },
                "cell_type": {
                    "type": "string",
                    "enum": ["code", "markdown"],
                    "description": "Cell type for insert mode (default 'code')"
                },
                "edit_mode": {
                    "type": "string",
                    "enum": ["replace", "insert", "delete"],
                    "default": "replace"
                }
            },
            "required": ["path", "cell_index"]
        }
    })
});

pub static WEB_FETCH_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "web_fetch",
        "description": "Fetch a URL and extract readable content (HTML → markdown/text). Output is capped at maxChars (default 50 000). Works for most web pages and docs; may fail on login-walled or JS-heavy sites.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "URL to fetch" },
                "extractMode": {
                    "type": "string",
                    "enum": ["markdown", "text"],
                    "default": "markdown"
                },
                "maxChars": { "type": "integer", "minimum": 100 }
            },
            "required": ["url"]
        }
    })
});
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common tools::schemas::`

Expected: 12 tests passed (10 well-formed + 2 specific-required-fields).

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/tools/schemas.rs
git commit -m "feat(common): add 10 shared tool schemas (file tools + web_fetch)"
```

---

### Task 7: tools/schemas.rs — 4 non-shared schemas (message, file_transfer, cron, exec)

The remaining 4 schemas: 3 server-only + 1 client-only.

**Files:**
- Modify: `plexus-common/src/tools/schemas.rs` (append)

- [ ] **Step 1: Write tests for the 4 new schemas + a meta-validation pass**

Append to the `mod tests` block in `plexus-common/src/tools/schemas.rs` (immediately before its closing `}`):

```rust
    #[test]
    fn message_schema_well_formed() {
        assert_well_formed(&MESSAGE_SCHEMA, "message");
    }

    #[test]
    fn file_transfer_schema_well_formed() {
        assert_well_formed(&FILE_TRANSFER_SCHEMA, "file_transfer");
    }

    #[test]
    fn cron_schema_well_formed() {
        assert_well_formed(&CRON_SCHEMA, "cron");
    }

    #[test]
    fn exec_schema_well_formed() {
        assert_well_formed(&EXEC_SCHEMA, "exec");
    }

    #[test]
    fn cron_action_enum_has_three_values() {
        let action = &CRON_SCHEMA["input_schema"]["properties"]["action"];
        let values = action["enum"].as_array().unwrap();
        let names: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();
        assert_eq!(names, vec!["add", "list", "remove"]);
    }

    #[test]
    fn exec_command_required() {
        let required = EXEC_SCHEMA["input_schema"]["required"].as_array().unwrap();
        assert!(required.iter().any(|v| v.as_str() == Some("command")));
    }

    #[test]
    fn all_14_schemas_are_distinct_names() {
        let names: Vec<&str> = [
            &*READ_FILE_SCHEMA,
            &*WRITE_FILE_SCHEMA,
            &*EDIT_FILE_SCHEMA,
            &*DELETE_FILE_SCHEMA,
            &*DELETE_FOLDER_SCHEMA,
            &*LIST_DIR_SCHEMA,
            &*GLOB_SCHEMA,
            &*GREP_SCHEMA,
            &*NOTEBOOK_EDIT_SCHEMA,
            &*WEB_FETCH_SCHEMA,
            &*MESSAGE_SCHEMA,
            &*FILE_TRANSFER_SCHEMA,
            &*CRON_SCHEMA,
            &*EXEC_SCHEMA,
        ]
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect();
        let unique: std::collections::HashSet<_> = names.iter().copied().collect();
        assert_eq!(unique.len(), 14, "duplicate name in schemas: {:?}", names);
    }
```

- [ ] **Step 2: Run — should fail**

Run: `cargo test --workspace -p plexus-common tools::schemas::`

Expected: compile failure (the 4 new constants undefined).

- [ ] **Step 3: Implement the 4 schemas**

Append the 4 new schemas to the top portion of `plexus-common/src/tools/schemas.rs` (after `WEB_FETCH_SCHEMA`, before the test module):

```rust
pub static MESSAGE_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "message",
        "description": "Send a message (text, media, or interactive buttons) to a chat. If channel and chat_id are omitted, delivers to the current session's channel + chat_id (the default reply path). If specified, delivers cross-channel.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": { "type": "string", "description": "Message text" },
                "channel": {
                    "type": "string",
                    "description": "Target channel (e.g. 'discord', 'telegram'). Optional — defaults to the current session's channel."
                },
                "chat_id": {
                    "type": "string",
                    "description": "Target chat identifier on that channel. Required if channel is set."
                },
                "media": {
                    "type": "array",
                    "description": "Workspace paths to media files to attach. Server-side workspace_fs path relative to user's workspace.",
                    "items": { "type": "string" }
                },
                "buttons": {
                    "type": "array",
                    "description": "Inline keyboard buttons (e.g. ['Yes', 'No']). When pressed, the label is sent back as a normal user message.",
                    "items": { "type": "string" }
                }
            },
            "required": ["content"]
        }
    })
});

pub static FILE_TRANSFER_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "file_transfer",
        "description": "Copy or move a file or folder between devices (server ↔ device, device ↔ device). Same-device move is an atomic rename. Folders transfer recursively.",
        "input_schema": {
            "type": "object",
            "properties": {
                "src_path": { "type": "string" },
                "dst_path": { "type": "string" },
                "mode": {
                    "type": "string",
                    "enum": ["copy", "move"],
                    "default": "copy"
                }
            },
            "required": ["src_path", "dst_path"]
        }
    })
});

pub static CRON_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "cron",
        "description": "Manage scheduled agent invocations (add, list, remove). Triggered job runs in a dedicated session that inherits the current session's channel + chat_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["add", "list", "remove"]
                },
                "message": {
                    "type": "string",
                    "description": "REQUIRED when action='add'. Instruction for the agent to execute when the job triggers (e.g., 'Send a reminder to WeChat: xxx' or 'Check system status and report'). Not used for action='list' or action='remove'."
                },
                "every_seconds": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "For recurring jobs: interval in seconds. One of every_seconds, cron_expr, or at must be provided when action='add'."
                },
                "cron_expr": {
                    "type": "string",
                    "description": "For recurring jobs: standard cron expression (5 fields)."
                },
                "at": {
                    "type": "string",
                    "description": "ISO datetime for one-time execution (e.g. '2026-02-12T10:30:00'). Naive values use the tool's default timezone."
                },
                "tz": {
                    "type": "string",
                    "description": "Timezone (e.g. 'America/Los_Angeles'). Default: UTC."
                },
                "deliver": {
                    "type": "boolean",
                    "description": "Whether to deliver the execution result to the user channel (default true)",
                    "default": true
                },
                "job_id": {
                    "type": "string",
                    "description": "REQUIRED when action='remove'. The id returned by add."
                }
            },
            "required": ["action"]
        }
    })
});

pub static EXEC_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "name": "exec",
        "description": "Execute a shell command and return its output. Prefer read_file/write_file/edit_file over cat/echo/sed, and grep/glob over shell find/grep. Use -y or --yes flags to avoid interactive prompts. Output is truncated at 10 000 chars; timeout defaults to 60s.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": { "type": "string", "description": "The shell command to execute" },
                "working_dir": { "type": "string", "description": "Optional working directory for the command" },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in seconds. Increase for long-running commands like compilation or installation (default 60, max 600).",
                    "minimum": 1,
                    "maximum": 600
                }
            },
            "required": ["command"]
        }
    })
});
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common tools::schemas::`

Expected: 19 tests passed (the 12 from Task 6 + 4 well-formed + 2 specific + 1 distinct-names).

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/tools/schemas.rs
git commit -m "feat(common): add 4 non-shared tool schemas (message, file_transfer, cron, exec)"
```

---

### Task 8: tools/mod.rs — Tool trait

The `Tool` trait per ADR-077. Async, with default `max_output_chars` of 16,000.

**Files:**
- Modify: `plexus-common/src/tools/mod.rs`

- [ ] **Step 1: Write the failing test**

Replace `plexus-common/src/tools/mod.rs` with the test stub first:

```rust
//! Shared tool infrastructure. See ADR-038, ADR-077, ADR-095.
//!
//! - [`result`] — wrap_result() for the [untrusted tool result]: prefix.
//! - [`path`] — resolve_in_workspace() for the file-tool jail.
//! - [`format`] — line-numbered output and head-only truncation helpers.
//! - [`schemas`] — hardcoded JSON schemas for the 14 first-class tools.
//! - [`validate`] — JSON Schema validation for tool_call args.

pub mod format;
pub mod path;
pub mod result;
pub mod schemas;
pub mod validate;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::ToolError;
    use serde_json::{Value, json};

    /// Minimal Tool impl for trait-shape testing.
    struct EchoTool {
        schema: Value,
    }

    #[async_trait::async_trait]
    impl Tool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }

        fn schema(&self) -> &Value {
            &self.schema
        }

        async fn execute(&self, args: Value) -> Result<String, ToolError> {
            Ok(args.to_string())
        }
    }

    #[tokio::test]
    async fn tool_trait_dispatches_execute() {
        let tool = EchoTool {
            schema: json!({"name": "echo", "description": "test", "input_schema": {}}),
        };
        assert_eq!(tool.name(), "echo");
        assert_eq!(tool.schema()["name"], "echo");
        let result = tool.execute(json!({"x": 1})).await.unwrap();
        assert!(result.contains("\"x\""));
    }

    #[tokio::test]
    async fn tool_default_max_output_chars_is_16k() {
        let tool = EchoTool {
            schema: json!({"name": "echo", "description": "test", "input_schema": {}}),
        };
        assert_eq!(tool.max_output_chars(), 16_000);
    }
}
```

- [ ] **Step 2: Run — should fail**

Run: `cargo test --workspace -p plexus-common tools::tests::`

Expected: compile failure (`Tool` trait undefined).

- [ ] **Step 3: Implement the Tool trait**

Add the trait above the `mod tests` block in `plexus-common/src/tools/mod.rs`:

```rust
use crate::errors::ToolError;
use serde_json::Value;

/// The Tool trait — every tool the agent can dispatch implements this.
///
/// Per ADR-077:
/// - `name()`: the wrapped tool name (e.g. "read_file" or "mcp_google_search").
/// - `schema()`: JSON Schema describing accepted args (matches one of the
///   constants in [`schemas`] for built-in tools).
/// - `max_output_chars()`: result-content cap before truncation. Defaults
///   to 16,000 (ADR-076); per-tool override via custom impl.
/// - `execute()`: dispatch the tool with parsed args, returning the raw
///   result string. The dispatcher wraps the result with [`result::wrap_result`]
///   before emitting the `tool_result` block.
///
/// Implementors hold their own context as struct fields (e.g. a server
/// `ReadFileTool` would hold `Arc<WorkspaceFs>`).
#[async_trait::async_trait]
pub trait Tool: Send + Sync {
    /// The wrapped tool name as the agent sees it.
    fn name(&self) -> &str;

    /// JSON Schema for accepted args.
    fn schema(&self) -> &Value;

    /// Maximum characters in the raw result before head-only truncation
    /// (ADR-076). Default 16,000. Override for tools with larger outputs
    /// (e.g. `read_file` overrides to 128,000).
    fn max_output_chars(&self) -> usize {
        16_000
    }

    /// Dispatch with the agent-supplied args. Returns the raw result string.
    /// The dispatcher wraps it via [`result::wrap_result`] before sending.
    async fn execute(&self, args: Value) -> Result<String, ToolError>;
}
```

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common tools::tests::`

Expected: 2 tests passed.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/tools/mod.rs
git commit -m "feat(common): add tools::Tool trait (ADR-077)"
```

---

### Task 9: tools/validate.rs — JSON Schema arg validation

Use `jsonschema` 0.30 to validate incoming tool_call args against the registered schema. Returns `ToolError::InvalidArgs` on failure.

**Files:**
- Modify: `plexus-common/src/tools/validate.rs`

- [ ] **Step 1: Write the failing tests**

Replace `plexus-common/src/tools/validate.rs` with:

```rust
//! JSON Schema validation for incoming `tool_call` args.
//!
//! Uses `jsonschema` 0.30. Failures return `ToolError::InvalidArgs` with
//! a human-readable message that includes every validation error found.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{Code, ErrorCode, ToolError};
    use serde_json::json;

    fn echo_schema() -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "x": { "type": "integer" },
                "name": { "type": "string" }
            },
            "required": ["x"]
        })
    }

    #[test]
    fn valid_args_pass() {
        let result = validate_args(&echo_schema(), &json!({"x": 42}));
        assert!(result.is_ok(), "expected ok, got {:?}", result);
    }

    #[test]
    fn valid_args_with_optional_pass() {
        let result = validate_args(&echo_schema(), &json!({"x": 42, "name": "alice"}));
        assert!(result.is_ok());
    }

    #[test]
    fn missing_required_field_rejected() {
        let result = validate_args(&echo_schema(), &json!({"name": "alice"}));
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::InvalidArgs);
        assert!(matches!(err, ToolError::InvalidArgs(_)));
    }

    #[test]
    fn wrong_type_rejected() {
        let result = validate_args(&echo_schema(), &json!({"x": "not an int"}));
        assert!(matches!(result, Err(ToolError::InvalidArgs(_))));
    }

    #[test]
    fn empty_schema_accepts_anything() {
        let schema = json!({});
        assert!(validate_args(&schema, &json!({})).is_ok());
        assert!(validate_args(&schema, &json!({"anything": [1, 2, 3]})).is_ok());
    }

    #[test]
    fn error_message_lists_all_violations() {
        let schema = json!({
            "type": "object",
            "properties": {
                "a": { "type": "integer" },
                "b": { "type": "string" }
            },
            "required": ["a", "b"]
        });
        // Missing both required fields
        let result = validate_args(&schema, &json!({}));
        let err = result.unwrap_err();
        match err {
            ToolError::InvalidArgs(msg) => {
                assert!(msg.contains("a") || msg.contains("b"));
            }
            _ => panic!("expected InvalidArgs"),
        }
    }
}
```

- [ ] **Step 2: Run — should fail**

Run: `cargo test --workspace -p plexus-common tools::validate::`

Expected: compile failure (`validate_args` undefined).

- [ ] **Step 3: Implement**

Add above the test block:

```rust
//! JSON Schema validation for incoming `tool_call` args.
//!
//! Uses `jsonschema` 0.30. Failures return `ToolError::InvalidArgs` with
//! a human-readable message that includes every validation error found.

use crate::errors::ToolError;
use serde_json::Value;

/// Validate `args` against `schema`. On failure, returns `ToolError::InvalidArgs`
/// with all validation errors joined by `; `.
pub fn validate_args(schema: &Value, args: &Value) -> Result<(), ToolError> {
    let validator = jsonschema::validator_for(schema)
        .map_err(|e| ToolError::InvalidArgs(format!("invalid schema: {e}")))?;

    let errors: Vec<String> = validator.iter_errors(args).map(|e| e.to_string()).collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ToolError::InvalidArgs(errors.join("; ")))
    }
}
```

Note on the `jsonschema` 0.30 API: `validator_for(schema)` is the entry point; it returns a `Validator` that has `iter_errors(instance)` returning an iterator of `ValidationError`. We collect the errors into strings and join.

- [ ] **Step 4: Run the tests — should pass**

Run: `cargo test --workspace -p plexus-common tools::validate::`

Expected: 6 tests passed.

If `jsonschema::validator_for` doesn't exist in 0.30 (API may have shifted), debug by reading https://docs.rs/jsonschema/0.30/ — likely alternatives: `jsonschema::Validator::new(&schema)` or `jsonschema::draft202012::new(&schema)`. Pick whichever entry point compiles and passes the tests; the wrapper API stays the same.

- [ ] **Step 5: Commit**

```bash
git add plexus-common/src/tools/validate.rs
git commit -m "feat(common): add tools::validate::validate_args (jsonschema 0.30)"
```

---

### Task 10: Integration test — end_to_end_schema_pipeline.rs

Stitches together: schema → validate → wrap_result → frame ser/de. Lives in `plexus-common/tests/`, exercises only the public API.

**Files:**
- Create: `plexus-common/tests/end_to_end_schema_pipeline.rs`

- [ ] **Step 1: Create the test file**

Create `plexus-common/tests/end_to_end_schema_pipeline.rs`:

```rust
//! Integration test stitching schemas → validation → result wrap → frame ser/de.
//!
//! Exercises only the public API of `plexus-common`. Catches breaks at
//! module boundaries that unit tests would miss.

use plexus_common::consts::UNTRUSTED_TOOL_RESULT_PREFIX;
use plexus_common::errors::ToolError;
use plexus_common::protocol::{ToolResultFrame, WsFrame};
use plexus_common::tools::result::wrap_result;
use plexus_common::tools::schemas::READ_FILE_SCHEMA;
use plexus_common::tools::validate::validate_args;
use serde_json::json;
use uuid::Uuid;

#[test]
fn read_file_schema_validates_and_round_trips_through_frame() {
    // 1. Validate args against the schema (schema is a tool-level wrapper;
    //    validate_args expects the inner input_schema for the actual JSON
    //    Schema validation step).
    let input_schema = &READ_FILE_SCHEMA["input_schema"];
    let valid_args = json!({"path": "MEMORY.md"});
    validate_args(input_schema, &valid_args).expect("valid args should pass");

    // 2. Reject invalid args.
    let invalid_args = json!({"offset": -5}); // missing required "path", and offset < 1
    let err = validate_args(input_schema, &invalid_args).unwrap_err();
    assert!(matches!(err, ToolError::InvalidArgs(_)));

    // 3. Build a tool_result with wrapped content.
    let raw = "1|hello\n2|world";
    let wrapped = wrap_result(raw);
    assert!(wrapped.starts_with(UNTRUSTED_TOOL_RESULT_PREFIX));

    // 4. Pack into a ToolResultFrame and roundtrip via JSON.
    let frame = WsFrame::ToolResult(ToolResultFrame {
        id: Uuid::now_v7(),
        content: wrapped.clone(),
        is_error: false,
        code: None,
    });
    let json_str = serde_json::to_string(&frame).unwrap();
    assert!(json_str.contains("\"type\":\"tool_result\""));

    let back: WsFrame = serde_json::from_str(&json_str).unwrap();
    if let WsFrame::ToolResult(tr) = back {
        assert_eq!(tr.content, wrapped);
        assert!(!tr.is_error);
    } else {
        panic!("expected ToolResult variant after roundtrip");
    }
}

#[test]
fn error_path_carries_typed_code() {
    use plexus_common::errors::ErrorCode;

    let frame = WsFrame::ToolResult(ToolResultFrame {
        id: Uuid::now_v7(),
        content: wrap_result("operation timed out"),
        is_error: true,
        code: Some(ErrorCode::ExecTimeout),
    });
    let json_str = serde_json::to_string(&frame).unwrap();
    // Wire format is snake_case string per ADR-046
    assert!(json_str.contains("\"code\":\"exec_timeout\""));
    let back: WsFrame = serde_json::from_str(&json_str).unwrap();
    if let WsFrame::ToolResult(tr) = back {
        assert!(tr.is_error);
        assert_eq!(tr.code, Some(ErrorCode::ExecTimeout));
    } else {
        panic!();
    }
}
```

- [ ] **Step 2: Run — expect pass**

Run: `cargo test --workspace -p plexus-common --test end_to_end_schema_pipeline`

Expected: 2 tests pass.

If the build fails because some import isn't re-exported, add the missing re-export to `plexus-common/src/lib.rs` first. The Task 12 final pass also handles re-exports — at this point if `plexus_common::tools::result::wrap_result` etc. don't resolve, you may need to first run Task 12's lib.rs update.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/tests/end_to_end_schema_pipeline.rs
git commit -m "test(common): integration — schema → validate → wrap → frame roundtrip"
```

---

### Task 11: Integration test — secret_no_leak.rs

Asserts that no token-shaped string ever appears in the formatted output of any struct holding secret newtypes. Implements the redaction guardrail from ADR-104.

**Files:**
- Create: `plexus-common/tests/secret_no_leak.rs`

- [ ] **Step 1: Create the test**

Create `plexus-common/tests/secret_no_leak.rs`:

```rust
//! Integration test for ADR-104's "never log secrets" guarantee.
//!
//! Constructs structs holding secret newtypes (DeviceToken/JwtSecret/LlmApiKey)
//! and asserts that `format!("{:?}", x)` and `format!("{}", x)` never
//! reveal the inner value, even when the secret is nested inside another struct.

use plexus_common::secrets::{DeviceToken, JwtSecret, LlmApiKey};
use secrecy::ExposeSecret;

const SECRET_LITERAL: &str = "this-is-a-secret-value-that-must-not-leak";
const TOKEN_LITERAL: &str = "plexus_dev_actualsecrettoken12345";

#[test]
fn device_token_debug_does_not_leak() {
    let t = DeviceToken::new(TOKEN_LITERAL.into());
    let dbg = format!("{:?}", t);
    assert!(
        !dbg.contains("actualsecrettoken"),
        "Debug leaked: {}",
        dbg
    );
}

#[test]
fn device_token_display_does_not_leak() {
    let t = DeviceToken::new(TOKEN_LITERAL.into());
    let disp = format!("{}", t);
    assert!(
        !disp.contains("actualsecrettoken"),
        "Display leaked: {}",
        disp
    );
}

#[test]
fn jwt_secret_does_not_leak_through_debug() {
    let j = JwtSecret::new(SECRET_LITERAL.into());
    let dbg = format!("{:?}", j);
    assert!(!dbg.contains("must-not-leak"), "Debug leaked: {}", dbg);
}

#[test]
fn llm_api_key_does_not_leak_through_debug() {
    let k = LlmApiKey::new(SECRET_LITERAL.into());
    let dbg = format!("{:?}", k);
    assert!(!dbg.contains("must-not-leak"), "Debug leaked: {}", dbg);
}

#[test]
fn secret_inside_struct_is_redacted() {
    #[derive(Debug)]
    struct DeviceConfig {
        name: String,
        token: DeviceToken,
    }
    let cfg = DeviceConfig {
        name: "mac-mini".into(),
        token: DeviceToken::new(TOKEN_LITERAL.into()),
    };
    let dbg = format!("{:?}", cfg);
    assert!(
        !dbg.contains("actualsecrettoken"),
        "Debug leaked through containing struct: {}",
        dbg
    );
    assert!(dbg.contains("mac-mini"), "non-secret field should still print");
}

#[test]
fn expose_secret_returns_inner() {
    // Sanity check: the secret IS recoverable via the explicit API.
    let t = DeviceToken::new(TOKEN_LITERAL.into());
    assert_eq!(t.expose_secret(), TOKEN_LITERAL);
}

#[test]
fn cloning_secret_preserves_redaction() {
    let t = DeviceToken::new(TOKEN_LITERAL.into());
    let cloned = t.clone();
    let dbg_orig = format!("{:?}", t);
    let dbg_clone = format!("{:?}", cloned);
    assert!(!dbg_orig.contains("actualsecrettoken"));
    assert!(!dbg_clone.contains("actualsecrettoken"));
    assert_eq!(cloned.expose_secret(), TOKEN_LITERAL);
}
```

- [ ] **Step 2: Run — expect pass**

Run: `cargo test --workspace -p plexus-common --test secret_no_leak`

Expected: 7 tests pass.

- [ ] **Step 3: Commit**

```bash
git add plexus-common/tests/secret_no_leak.rs
git commit -m "test(common): integration — secrets never leak through formatting (ADR-104)"
```

---

### Task 12: Final verification + lib.rs re-exports

Add public-API re-exports for everything in `tools/`. Run the full check matrix.

**Files:**
- Modify: `plexus-common/src/lib.rs`

- [ ] **Step 1: Update lib.rs with tools/* re-exports**

Replace `plexus-common/src/lib.rs`:

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
//! Plan 3 (`mcp`) extends the public surface.

pub mod consts;
pub mod errors;
pub mod protocol;
pub mod secrets;
pub mod tools;
pub mod version;

// Top-level re-exports for ergonomic access.
pub use errors::{
    AuthError, Code, ErrorCode, McpError, NetworkError, ProtocolError, ToolError, WorkspaceError,
};
pub use protocol::{
    ConfigUpdateFrame, DeviceConfig, ErrorFrame, FsPolicy, HelloAckFrame, HelloCaps, HelloFrame,
    HEADER_SIZE, McpSchemas, McpServerConfig, PingFrame, PongFrame, PromptArgument, PromptDef,
    RegisterMcpFrame, ResourceDef, SpawnFailure, ToolCallFrame, ToolDef, ToolResultFrame,
    TransferBeginFrame, TransferDirection, TransferEndFrame, TransferProgressFrame, WsFrame,
    pack_chunk, parse_chunk,
};
pub use secrets::{DeviceToken, JwtSecret, LlmApiKey};
pub use tools::Tool;
pub use version::{PROTOCOL_VERSION, crate_version};
```

- [ ] **Step 2: Run the full test suite**

Run: `cargo test --workspace`

Expected: all tests pass. Roughly:
- Plan 1 inherited: 58 (current after simplify pass)
- Tools added: ~5 (result) + ~14 (path) + ~11 (format) + ~19 (schemas) + 2 (Tool trait) + 6 (validate) = ~57
- Integration tests: 2 (end_to_end_schema_pipeline) + 7 (secret_no_leak) = 9
- **Total: ~120-125 tests**

If anything fails, debug. Common issues:
- Missing `pub use` in `lib.rs` — check the import paths used by the integration tests against `lib.rs`.
- `validate.rs` API drift — the `jsonschema` 0.30 entry-point function name might differ; see Task 9 Step 4 fallback notes.

- [ ] **Step 3: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: clean. Fix any warnings inline.

- [ ] **Step 4: Run fmt check**

Run: `cargo fmt --all --check`

Expected: clean. If not, run `cargo fmt --all` and re-check.

- [ ] **Step 5: Build for both musl targets**

```bash
cargo build --workspace --target x86_64-unknown-linux-musl
cargo build --workspace --target aarch64-unknown-linux-musl
```

Expected: both succeed (CI also verifies).

- [ ] **Step 6: cargo doc clean**

```bash
cargo doc --no-deps -p plexus-common
```

Expected: no warnings.

- [ ] **Step 7: Commit lib.rs**

```bash
git add plexus-common/src/lib.rs
git commit -m "feat(common): finalize Plan 2 lib.rs re-exports + Tool trait surface"
```

- [ ] **Step 8: Push**

```bash
git push origin rebuild-m0
```

CI runs on push. Verify all gates green.

---

## Plan 2 acceptance criteria

Before declaring Plan 2 done, confirm:

- [ ] All ~120 tests pass (`cargo test --workspace`).
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [ ] `cargo fmt --all --check` clean.
- [ ] Both musl targets build.
- [ ] `cargo doc --no-deps -p plexus-common` clean.
- [ ] All 14 tool schemas present and valid JSON.
- [ ] `Tool` trait + `validate_args` + `wrap_result` + `resolve_in_workspace` + format helpers all reachable from `plexus_common::*` top level.
- [ ] CI green on `rebuild-m0` HEAD.
- [ ] No `unwrap()`/`expect()` in non-test code (`expect()` is allowed in `parse_chunk` and `path::resolve_in_workspace` where it's documenting an unreachable invariant).
- [ ] No `unsafe` code.

When all boxes checked, Plan 2 is done. Proceed to Plan 3 (MCP + Polish) via `superpowers:writing-plans` again with that scope.

---

## Post-Plan Adjustments

Reserved for the implementation pass to record any deltas from this plan, the rationale, and SHAs (per `feedback_docs_alignment.md`). Empty until execution begins.
