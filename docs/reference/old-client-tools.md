# plexus-client Tool Reference

Built-in tools registered in `LOCAL_TOOL_REGISTRY` (see `executor.rs`). All tools are policy-aware -- the active `FsPolicy` (sent by server) controls what paths are accessible.

Global execution timeout and per-tool timeouts are configured per-device via the web UI (Settings > Devices).

---

## shell

Execute a shell command on the device. Runs via `sh -c` on Linux/macOS, `cmd /C` on Windows.

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `command` | string | yes | -- | The shell command to execute |
| `timeout_sec` | integer | no | per-device config | Per-command timeout. Default and max are configured per-device via the web UI |
| `working_dir` | string | no | workspace root | Must be within workspace. Validated via `sanitize_path()` |

**Behavior:**
- Environment is **always** isolated via `env_clear()` + `min_env()`, even in Unrestricted mode. Only `PATH` (inherited from system), `HOME`, `LANG`, `TERM` are passed through.
- In `Sandbox` policy + Linux + bwrap available: command is wrapped in a bubblewrap sandbox (see SECURITY.md).
- In `Sandbox` policy: command passes through `guardrails::check_shell_command()` (deny patterns + SSRF check). In `Unrestricted` mode: guardrails are skipped.
- Absolute paths in the command are extracted and validated against workspace. Path traversal (`../`) is blocked.
- Output is dual-end truncated at 10,000 chars (first 5,000 + last 5,000).
- On timeout, the child process is killed.

**Example:**
```json
{
  "command": "find . -name '*.rs' | head -20",
  "timeout_sec": 30,
  "working_dir": "/home/user/.plexus/workspace/myproject"
}
```

**Error cases:**
- `InvalidParams` -- missing `command`
- `Blocked` -- denied by guardrails (dangerous pattern, SSRF, path traversal, path outside scope)
- `ExecutionFailed` -- non-zero exit code, spawn failure
- `Timeout` -- command exceeded `timeout_sec`

---

## read_file

Read a file's contents with line numbers. Supports pagination for large files. Detects images and returns metadata instead of content.

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `path` | string | yes | -- | File path (absolute or relative to workspace) |
| `offset` | integer | no | 1 | Start line number (1-indexed) |
| `limit` | integer | no | 2000 | Max lines to return |

**Behavior:**
- Output format: `{line_number}| {content}` per line.
- Images (PNG, JPEG, GIF, WebP) detected by magic bytes -- returns `[Image: path, sizeKB]`.
- Binary files (non-UTF-8, non-image) return an error.
- Total output capped at 128,000 characters; lines beyond the cap are dropped.
- Pagination hint appended when more lines exist: `(Showing lines X-Y of Z. Use offset=N to continue.)`.
- Filesystem operations have a configurable timeout (set per-device via web UI).

**Example:**
```json
{
  "path": "src/main.rs",
  "offset": 100,
  "limit": 50
}
```

**Error cases:**
- `InvalidParams` -- missing `path`, offset beyond EOF
- `NotFound` -- file doesn't exist
- `ExecutionFailed` -- permission denied, binary file, read failure
- `Blocked` -- path outside policy scope

---

## write_file

Write content to a file. Creates parent directories automatically.

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `path` | string | yes | -- | File path (absolute or relative to workspace) |
| `content` | string | yes | -- | Content to write (overwrites entire file) |

**Behavior:**
- Parent directories are created with `create_dir_all` if they don't exist.
- Writes are atomic (via `tokio::fs::write`).
- Timeout configurable per-device via web UI.

**Example:**
```json
{
  "path": "output/results.txt",
  "content": "hello world\n"
}
```

**Error cases:**
- `InvalidParams` -- missing `path` or `content`
- `Blocked` -- path outside workspace (Sandbox mode)
- `ExecutionFailed` -- permission denied, directory creation failure

---

## edit_file

Surgical string replacement in a file. Finds `old_string` and replaces with `new_string`.

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `file_path` | string | yes | -- | Path to the file to edit |
| `old_string` | string | yes | -- | Exact string to find (must appear exactly once) |
| `new_string` | string | yes | -- | Replacement string |

**Behavior:**
- Reads the file, counts occurrences of `old_string`, replaces if exactly 1 match.
- Fails fast on 0 matches or >1 matches -- no partial edits.
- `old_string` must not be empty.
- Uses write-path policy validation (same rules as `write_file`).
- Timeout configurable per-device via web UI.

**Example:**
```json
{
  "file_path": "src/config.rs",
  "old_string": "const MAX_RETRIES: u32 = 3;",
  "new_string": "const MAX_RETRIES: u32 = 5;"
}
```

**Error cases:**
- `InvalidParams` -- missing params, empty `old_string`, 0 matches, >1 matches
- `NotFound` -- file doesn't exist
- `Blocked` -- path outside policy scope
- `ExecutionFailed` -- permission denied, write failure

---

## list_dir

List directory contents. Supports recursive listing with auto-ignored noise directories.

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `path` | string | yes | -- | Directory path to list |
| `recursive` | boolean | no | false | Recursively list all entries |
| `max_entries` | integer | no | 200 | Max entries to return (min 1) |

**Behavior:**
- Non-recursive: entries prefixed with `[DIR]` or `[FILE]`.
- Recursive: directories suffixed with `/`, files shown as relative paths from root.
- Results are sorted alphabetically.
- Auto-ignored directories: `.git`, `node_modules`, `__pycache__`, `.venv`, `venv`, `dist`, `build`, `.tox`, `.mypy_cache`, `.pytest_cache`, `.ruff_cache`, `.coverage`, `htmlcov`.
- Truncation message appended when entries exceed `max_entries`.
- Timeout configurable per-device via web UI.

**Example:**
```json
{
  "path": ".",
  "recursive": true,
  "max_entries": 50
}
```

**Error cases:**
- `InvalidParams` -- missing `path`
- `NotFound` -- directory doesn't exist
- `Blocked` -- path outside policy scope
- `ExecutionFailed` -- permission denied

---

## glob

Find files by pattern. Uses standard glob syntax.

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `pattern` | string | yes | -- | Glob pattern (e.g., `**/*.rs`, `src/**/*.test.ts`, `*.json`) |
| `path` | string | no | workspace root | Directory to search in |

**Behavior:**
- Matches files within the workspace (or specified path) using glob patterns.
- Returns matching file paths sorted by modification time (newest first).
- Auto-ignores noise directories (`.git`, `node_modules`, `__pycache__`, etc.).
- Uses read-path policy validation.
- Timeout configurable per-device via web UI.

**Example:**
```json
{
  "pattern": "**/*.rs",
  "path": "src"
}
```

**Error cases:**
- `InvalidParams` -- missing `pattern`
- `Blocked` -- path outside policy scope

---

## grep

Search file contents with regex. Returns matching lines with context.

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `pattern` | string | yes | -- | Regex pattern to search for |
| `path` | string | no | workspace root | File or directory to search in |
| `include` | string | no | all files | Glob filter for files to search (e.g., `*.rs`, `*.py`) |
| `context` | integer | no | 0 | Number of context lines before and after each match |

**Behavior:**
- Searches file contents within the workspace using regex.
- Returns matching lines with file paths and line numbers.
- Supports file type filtering via `include` parameter.
- Auto-ignores binary files and noise directories.
- Uses read-path policy validation.
- Timeout configurable per-device via web UI.

**Example:**
```json
{
  "pattern": "async fn main",
  "path": "src",
  "include": "*.rs",
  "context": 2
}
```

**Error cases:**
- `InvalidParams` -- missing `pattern`, invalid regex
- `Blocked` -- path outside policy scope

---

## MCP Tools

Tools from MCP servers are dynamically discovered and registered with prefixed names: `mcp_{server_name}_{tool_name}`. Parameters and behavior are defined by the MCP server. Execution timeout defaults to 30s (configurable per-server via `tool_timeout`).

## Error Exit Codes

All tools map errors to standardized exit codes:

| Exit Code | Constant | ToolError variant |
|---|---|---|
| 0 | `EXIT_CODE_SUCCESS` | (success) |
| 1 | `EXIT_CODE_ERROR` | `NotFound`, `InvalidParams`, `ExecutionFailed` |
| -1 | `EXIT_CODE_TIMEOUT` | `Timeout` |
| -2 | `EXIT_CODE_CANCELLED` | `Blocked` (guardrails) |
