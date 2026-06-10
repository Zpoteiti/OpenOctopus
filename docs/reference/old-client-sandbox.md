# plexus-client Security Model

The client is the execution boundary -- it runs agent-generated commands on real machines. Security is enforced in layers, each independent of the others.

## FsPolicy Tiers

The server controls all client security settings — workspace path, filesystem policy, MCP servers, and shell timeout. All are configured per-device through the web UI (Settings > Devices), sent in `LoginSuccess` on connect, and kept in sync via `HeartbeatAck`.

### Sandbox (default)

```rust
FsPolicy::Sandbox
```

- Filesystem tools (`read_file`, `write_file`, `edit_file`, `list_dir`, `stat`) are restricted to the workspace directory (configured per-device via web UI, sent by server on connect).
- Shell commands: absolute paths are extracted and validated against workspace. Path traversal (`../`) is blocked. Guardrail checks (dangerous patterns + SSRF) are active.
- If bwrap is available on Linux, shell commands run inside a bubblewrap namespace (see below).
- Symlink resolution: paths are canonicalized before comparison, so symlinks that escape the workspace are caught.
- For writes to new files, the parent directory is canonicalized to catch symlink-based escapes.

### Unrestricted

```rust
FsPolicy::Unrestricted
```

- All filesystem paths are allowed for read and write. No workspace restriction.
- Shell guardrails are **skipped** (`check_shell_command` is not called).
- Shell `guard_command_policy` returns `Ok(())` immediately.
- Environment isolation **still applies** -- env vars are still stripped (see below).
- Use case: trusted device where the agent needs full system access (e.g., admin's own machine).

## Shell Dangerous Pattern Deny List

In Sandbox mode, every shell command passes through `guardrails::check_shell_command()` before execution. The deny list (`DENY_PATTERNS` in `guardrails.rs`) uses precompiled regexes:

| Pattern | What it catches |
|---|---|
| `\brm\s+-[rf]{1,2}\b` | `rm -rf`, `rm -r`, `rm -f` |
| `\bdel\s+/[fq]\b` | Windows `del /f`, `del /q` |
| `\bformat\s+[a-z]:` | Drive formatting |
| `\bdd\s+if=\b` | Direct device read via `dd` |
| `:()\s*\{.*?\};:` | Fork bombs |
| `\b(shutdown\|reboot\|poweroff\|init\s+0\|init\s+6)\b` | System shutdown/reboot |
| `>\s*/dev/sd[a-z]` | Direct disk writes |
| `\b(mkfifo\|mknod)\s+/dev/` | Device file creation |

Match = immediate `ToolError::Blocked` (exit code -2). No execution.

## SSRF Protection

After deny-pattern checks, `guardrails::contains_internal_url()` extracts all URLs matching `https?://[^\s'"]+` from the command and validates each:

1. If the host is an IP address, it's checked directly against blocked ranges.
2. If the host is a domain, async DNS resolution is performed (`tokio::net::lookup_host`). All resolved IPs are checked.
3. If DNS resolution fails, the URL is **conservatively rejected**.

Blocked IP ranges (by default):

| Range | Description |
|---|---|
| `0.0.0.0/8` | Current network |
| `10.0.0.0/8` | Private (RFC 1918) |
| `100.64.0.0/10` | Shared address space (CGN) |
| `127.0.0.0/8` | Loopback |
| `169.254.0.0/16` | Link-local (cloud metadata!) |
| `172.16.0.0/12` | Private (RFC 1918) |
| `192.168.0.0/16` | Private (RFC 1918) |
| `::1/128` | IPv6 loopback |
| `fc00::/7` | IPv6 unique local |
| `fe80::/10` | IPv6 link-local |

**Per-device SSRF whitelist:** Users can whitelist specific CIDR ranges per device via the web UI (Settings > Devices). For example, whitelisting `10.180.0.0/16` allows the agent on that device to access internal services on that subnet. This is separate from the server-side `web_fetch` whitelist (which is per-user). Responsibility for whitelisted ranges is on the user.

## Environment Variable Isolation

`env::min_env()` (in `env.rs`) defines the only env vars passed to spawned processes:

```
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HOME=(inherited)
LANG=en_US.UTF-8
TERM=xterm-256color
```

Key points:
- `PATH` is **inherited** from the parent process so custom tools (like conda or node) can be found. Secrets in the host environment (`AWS_SECRET_ACCESS_KEY`, `PLEXUS_AUTH_TOKEN`, `DATABASE_URL`, etc.) are never visible to agent-executed commands because the environment is explicitly cleared first.
- Windows uses `C:\Windows\system32;C:\Windows;C:\Windows\System32\Wbem` as safe PATH.

## Bubblewrap Sandbox

When `FsPolicy::Sandbox` is active and `bwrap` is installed (Linux only), shell commands are wrapped in a namespace via `sandbox::wrap_command()`.

Availability is checked once at startup via `LazyLock`:
```rust
static BWRAP_AVAILABLE: LazyLock<bool> = LazyLock::new(|| {
    Command::new("bwrap").arg("--version").status().map(|s| s.success()).unwrap_or(false)
});
```

### Mount Layout

| Mount | Type | Purpose |
|---|---|---|
| `/usr` | `--ro-bind` | System binaries and libraries (required) |
| `/bin`, `/lib`, `/lib64` | `--ro-bind-try` | Additional system dirs (skipped if missing) |
| `/etc/alternatives` | `--ro-bind-try` | Debian alternatives |
| `/etc/ssl/certs` | `--ro-bind-try` | TLS certificates (for HTTPS) |
| `/etc/resolv.conf` | `--ro-bind-try` | DNS resolution |
| `/etc/ld.so.cache` | `--ro-bind-try` | Dynamic linker cache |
| `/proc` | `--proc` | Minimal procfs |
| `/dev` | `--dev` | Minimal devfs |
| `/tmp` | `--tmpfs` | Fresh tmpfs (isolated from host /tmp) |
| Workspace parent (e.g., `~`) | `--tmpfs` | **Masks home directory** -- hides `~/.ssh/`, `~/.plexus/config`, etc. |
| Workspace (e.g., `~/.plexus/workspace`) | `--dir` + `--bind` | Read-write, mounted on top of the parent tmpfs mask |

### Flags

- `--new-session` -- new session ID (prevents signal injection from outside).
- `--die-with-parent` -- sandbox process dies if the client process exits.
- Arguments are shell-escaped via a simple escaper that single-quotes anything with special characters.

### Degradation

If bwrap is not installed, the `is_available()` check returns false and commands execute directly (with guardrails and env isolation still active). No error, no warning at runtime -- bwrap is optional hardening.

## Path Traversal Protection

Two layers:

1. **Shell commands** (`guard_command_policy` in `shell.rs`): if the command string contains `../` or `..\`, it's blocked immediately. Absolute paths are extracted token-by-token and checked against workspace + allowed paths. Exceptions: `/dev/null` and `/tmp/plexus*` are always allowed.

2. **Filesystem tools** (`sanitize_path_with_policy` in `env.rs`): paths are resolved via `canonicalize()` (follows symlinks) before comparing against workspace. Relative paths are joined to workspace root first. For write operations on new files (where `canonicalize()` would fail), the parent directory is canonicalized instead.

## Server-Controlled Policy

The client never decides its own security policy. The flow:

1. Client connects and authenticates with `SubmitToken`.
2. Server responds with `LoginSuccess { fs_policy, mcp_servers, ... }`.
3. Every 15s heartbeat, server responds with `HeartbeatAck { fs_policy, mcp_servers }`.
4. If the policy changed (compared by `PartialEq` on `FsPolicy`), the client updates its `Arc<RwLock<FsPolicy>>`.
5. All tool calls read-lock this value before execution.

This means an admin can change a device's policy via the server API and it takes effect within 15 seconds, without reconnecting the client.
