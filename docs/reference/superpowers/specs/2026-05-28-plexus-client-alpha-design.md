# Plexus Client Alpha Design Spec

**Status:** verified on 2026-06-04
**Branch:** `client-alpha`
**Authors:** brainstormed in collaborative session 2026-05-28
**Depends on:** `M1e`, `M1f`

---

## 1. Purpose

Client Alpha proves Plexus's core distributed-agent loop with a real
`plexus-client`: the server thinks, a user-owned device executes, and the
agent can observe the result through the normal tool loop.

This is not the production client milestone. It deliberately avoids MCP,
service installers, long-running shell sessions, full subprocess sandboxing,
and frontend work. The Alpha target is a small but real command-line daemon
that connects to `plexus-server`, receives server-pushed device config,
executes a focused set of tools, and is sufficient for curl/API e2e
acceptance.

## 2. Goals

Client Alpha must deliver:

- A real `plexus-client` binary with `run` and `version` CLI behavior.
- Startup through environment variables only.
- Device WebSocket lifecycle: bearer-token connect, `hello`, `hello_ack`,
  `ping`/`pong`, reconnect, `config_update`, and close-code handling.
- A per-device FIFO worker that executes `tool_call` frames one at a time.
- Shared file tools against the client workspace.
- Shared `web_fetch` with client-side SSRF policy.
- Client-only one-shot `exec`.
- File transfer for `server -> client` and `client -> server`.
- Clear failure paths that return normal `tool_result(is_error=true, code=...)`
  data to the server agent.
- Enough tests and curl/API smoke coverage to prove the distributed loop.

## 3. Non-Goals and Deferred Work

These items are intentionally out of Client Alpha. They must stay visible so
later LLM-led work does not forget or accidentally re-scope them.

| Deferred item | Later track | Reason it is not in Alpha |
|---|---|---|
| Long-running exec sessions: `yield_time_ms`, `exec_session_id`, poll, `write_stdin`, terminate, list sessions | Client Hardening / Alpha+1 | Requires remote process ownership, output buffering, idle cleanup, shutdown behavior, and session authorization. |
| Persistent PTY / interactive shell | Client Hardening / Alpha+1 | Same lifecycle complexity as long-running exec, plus terminal semantics. |
| `client -> client` file-transfer bridge | Client Hardening / Alpha+1 | Requires server bridge coordination, two client legs, backpressure, and two-sided error propagation. |
| Client MCP subprocess management and `register_mcp` | MCP milestone | Depends on the real client runtime but should be designed after the basic client loop is proven. |
| Admin shared-service MCP runtime | MCP milestone | Server-side shared-service MCP follows Client Alpha. |
| Cron scheduler and autonomous heartbeat sessions | Cron/Heartbeat milestone | Autonomous flows should run after MCP and the client loop are stable. |
| Frontend | Frontend milestone | UI should consume stable REST/SSE/WS semantics after Hardening Lite. |
| Discord, Telegram, Slack, Feishu, and other channels | Channels and later expansion | Channels are ingress adapters over existing sessions; they should not define the distributed execution contract. |
| Production installers and service units | Client Hardening | Alpha documents manual foreground/background execution only. |
| Strong subprocess sandboxing and cross-platform sandbox polish | Client Hardening | Alpha enforces file-tool path policy and env stripping; Linux bwrap and platform-specific hardening come later. |
| Local config directory, setup wizard, login/logout | Later only if needed | Alpha client is pure env + server-pushed config. |
| Server-side sandboxed code execution without a connected client | Later server expansion | Needs a separate security design. |

## 4. Startup and Distribution

Client Alpha uses a single-binary, environment-driven startup contract.

Required env vars for `run`:

| Var | Example | Purpose |
|---|---|---|
| `PLEXUS_SERVER_URL` | `http://localhost:8080` | Base server URL. Client derives `/ws/device` by switching `http(s)` to `ws(s)`. |
| `PLEXUS_DEVICE_TOKEN` | `plexus_dev_...` | Device bearer token minted by the server. |

Optional logging inputs:

| Input | Purpose |
|---|---|
| `RUST_LOG` | Standard tracing filter override. |
| `--log-level=<level>` | Convenience default when `RUST_LOG` is absent. |

CLI surface:

```text
plexus-client run       # default if no subcommand is supplied
plexus-client version   # print binary version and protocol version
```

There is no Alpha `logout`, setup wizard, local config file, package-manager
installer, or service installer.

Linux foreground example:

```bash
PLEXUS_SERVER_URL=http://localhost:8080 \
PLEXUS_DEVICE_TOKEN=plexus_dev_xxx \
RUST_LOG=info \
./plexus-client run
```

Linux background example for manual smoke testing:

```bash
nohup env \
  PLEXUS_SERVER_URL=http://localhost:8080 \
  PLEXUS_DEVICE_TOKEN=plexus_dev_xxx \
  RUST_LOG=info \
  ./plexus-client run > plexus-client.log 2>&1 &
```

Users may also run the process in `screen`, `tmux`, systemd, launchd, or a
Windows service wrapper, but Client Alpha does not generate those service files.
The binary itself must behave like a daemon: log to stderr, reconnect forever on
retryable failures, and shut down cleanly on SIGTERM/SIGINT.

Configuration ownership:

- `plexus-server` uses env + DB.
- `plexus-client` uses env + server-pushed config.
- The client does not persist device config locally.
- Revoking a device is a server-side action through the existing device APIs.

## 5. Connection Lifecycle

The client connects to:

```text
GET /ws/device
Authorization: Bearer <PLEXUS_DEVICE_TOKEN>
```

After the WebSocket upgrade, the client sends `hello` first:

```jsonc
{
  "type": "hello",
  "id": "0190d5a7-...",
  "version": "1",
  "client_version": "0.X.Y",
  "os": "linux",
  "caps": {
    "exec": true,
    "fs": "rw",
    "policy": ["workspace_path", "ssrf_denylist", "env_allowlist", "command_denylist"]
  }
}
```

The server replies with `hello_ack`, including:

- `device_name`
- `user_id` for logging
- `config.workspace_path`
- `config.sandbox_mode`
- `config.shell_timeout_max`
- `config.ssrf_denylist`
- `config.env_allowlist`
- `config.command_denylist`
- `config.mcp_servers` as opaque future config, ignored by Alpha

Reconnect behavior follows `docs/PROTOCOL.md`:

- Retry initial connect and post-disconnect connect forever with exponential
  backoff capped at 30 seconds plus jitter.
- Reply to server `ping` with `pong`.
- Exit on `4401` unauthorized/token revoked.
- Exit with code 78 on `4409` protocol mismatch.
- Do not resume in-flight tool calls after reconnect.

`config_update` hot-reloads config for future tool calls. In-flight calls keep
the config snapshot they started with.

## 6. Runtime Components

Client Alpha should keep implementation boundaries small and explicit:

| Component | Responsibility |
|---|---|
| CLI/config loader | Read env vars, parse logging option, validate startup inputs. |
| WS connection loop | Connect, handshake, reconnect, route frames, handle close codes. |
| Config store | Hold the current server-pushed `DeviceConfig` snapshot in memory. |
| Worker queue | FIFO queue of `tool_call` jobs for this device connection. |
| Tool registry | Map tool names to local executors. |
| File tool executor | Run shared file tools against `workspace_path` and `sandbox_mode`. |
| Web fetch executor | Apply client SSRF policy and fetch content. |
| Exec runner | Run one-shot shell commands with env stripping and timeout. |
| Transfer manager | Manage server/client transfer slots and binary chunks. |
| Shutdown coordinator | Cancel in-flight work and send best-effort failure frames. |

The worker queue serializes tool execution per connected device. Control frames
such as `ping`, `pong`, close handling, and transfer binary frame plumbing must
not wait behind a long tool call in a way that breaks heartbeat behavior.

## 7. Tool Scope

Client Alpha implements these tool surfaces:

| Tool | Alpha | Notes |
|---|---:|---|
| `read_file` | yes | Supports text and safe image blocks. |
| `write_file` | yes | Creates parent directories. |
| `edit_file` | yes | Uses shared matcher behavior. |
| `delete_file` | yes | Single file only. |
| `delete_folder` | yes | Recursive directory delete. |
| `list_dir` | yes | Directory listing. |
| `glob` | yes | Workspace-aware glob search. |
| `grep` | yes | Workspace-aware text search. |
| `notebook_edit` | yes | Jupyter notebook cell edits. |
| `web_fetch` | yes | Default private-address block plus device whitelist. |
| `exec` | yes | One-shot only. |
| `file_transfer` | partial | `server -> client`, `client -> server`, existing `server -> server`; no client bridge. |
| MCP tools/resources/prompts | no | MCP milestone. |
| `cron` | no | Cron/Heartbeat milestone. |
| `message` | no client implementation | Server-owned. |

The client returns raw result text or safe text/image blocks. The server remains
responsible for validation, persistence, and the ADR-095 untrusted-tool-result
wrap before provider replay.

## 8. Filesystem Policy

`workspace_path` arrives from `hello_ack` / `config_update`. The client expands
`~` locally, creates the directory if missing, and accepts an existing directory
as-is. Plexus does not own the workspace; user files remain user files.

`sandbox_mode=true`:

- File tools must resolve paths through the shared workspace path policy.
- Relative paths resolve inside `workspace_path`.
- Absolute paths are accepted only when they resolve inside `workspace_path`.
- Symlink escape is rejected.

`sandbox_mode=false`:

- File tools may access absolute paths outside `workspace_path`.
- Relative paths still resolve from `workspace_path`.
- The mode is intentionally powerful and should remain a server-side device
  config choice.

Alpha does not maintain a Plexus app config directory on the client, so there
is no config-directory overlap check in this milestone.

## 9. One-Shot Exec

Alpha `exec` accepts:

```jsonc
{
  "command": "git status",
  "working_dir": "/home/alice/project",
  "timeout": 60
}
```

Behavior:

- Default timeout is 60 seconds.
- Effective timeout is capped by `device.shell_timeout_max` when configured.
- `working_dir` defaults to `workspace_path`.
- In `sandbox_mode=true`, `working_dir` must be inside `workspace_path`.
- In `sandbox_mode=false`, `working_dir` may be anywhere the OS permits.
- The command runs in a platform-appropriate shell.
- stdin is not interactive.
- stdout and stderr are captured separately.
- Timeout kills the process and returns `exec_timeout`.
- Non-zero exit code is returned in the result. It is not automatically a
  protocol failure; the agent sees the exit code and output.

Environment:

- Alpha strips the host environment before spawning.
- Preserve only a small allowlist needed for ordinary commands, such as `PATH`,
  `HOME`, `LANG`, `TERM`, and platform-required Windows variables.
- Do not pass `PLEXUS_DEVICE_TOKEN` to child processes.

Extension boundary for future long-running exec:

- Keep the one-shot runner behind an `ExecRunner`-style boundary rather than
  embedding it directly in WS frame handling.
- Future long-running exec uses a hybrid ownership model:
  - server owns logical `exec_session_id`, session/user/device authorization,
    and metadata;
  - client owns the real OS process handle, stdin/PTY handle, and output buffer.
- Reconnect does not resume old processes in the planned model; disconnect
  remains `device_unreachable`.

## 10. File Transfer

Client Alpha implements actual binary transfer for:

- `server -> client`
- `client -> server`
- existing `server -> server`

The Alpha transfer implementation follows the existing `docs/PROTOCOL.md`
slot model, with one Client Alpha upload-request specialization:

- `server -> client`: server emits `TransferBegin(ServerToClient)`, streams
  binary chunks tagged with the 16-byte slot id, emits `TransferEnd` with the
  digest, and waits for the client acknowledgement.
- `client -> server`: server emits `TransferBegin(ClientToServer)` as an upload
  request naming the client `src_path` and server `dst_path`; the client streams
  binary chunks, emits `TransferEnd(ok=true, sha256=<actual digest>)`, and the
  server verifies, writes atomically, and sends the final acknowledgement.

Required behavior:

- Client-side transfer receive/send streams chunks to/from disk. Alpha server
  workspace legs still use the current whole-file `WorkspaceFs` read/write
  APIs: `server -> client` buffers the server source file before streaming, and
  `client -> server` buffers inbound bytes up to the workspace upload cap before
  an atomic write. End-to-end streaming server workspace reads/writes are
  deferred to the client-hardening/large-file slice.
- Reject destination if it already exists unless the existing tool contract
  explicitly allows overwrite.
- Delete partial client destination files on failed or cancelled transfer.
  Server destinations are written atomically after verification, so failures do
  not leave or delete an existing final destination path.
- Surface disconnects as `device_unreachable`.
- Surface sha mismatch as `sha256_mismatch`.

`client -> client` transfer is explicitly deferred. It will use the server as a
pure bridge with the same slot id on both legs, but Alpha does not implement the
two-leg bridge.

## 11. Failure Semantics

Tool failures are data for the agent. The client returns
`tool_result(is_error=true, code=...)` for recoverable tool failures instead of
breaking the loop.

Required Alpha error codes include:

| Code | Meaning |
|---|---|
| `device_unreachable` | Server synthesizes when WS closes before result. |
| `client_shutting_down` | Client is shutting down and cancels in-flight work. |
| `exec_timeout` | One-shot exec exceeded effective timeout. |
| `command_denied` | Device command denylist rejected the command before spawn. |
| `cwd_outside_workspace` | Sandbox-mode exec cwd outside workspace. |
| `path_outside_workspace` | Sandbox-mode file path outside workspace. |
| `permission_denied` | OS denied read/write/exec access. |
| `not_found` | Source path missing. |
| `already_exists` | Destination path already exists. |
| `sha256_mismatch` | Transfer digest verification failed. |
| `invalid_args` | Malformed tool arguments. |

On SIGTERM/SIGINT:

1. Stop accepting new work.
2. Best-effort send `client_shutting_down` for in-flight tool calls.
3. Best-effort send failed `transfer_end` for in-flight transfer slots.
4. Kill in-flight one-shot exec.
5. Close WS and exit zero.

## 12. Testing

Automated tests should cover the client without real external services where
possible:

- CLI env validation.
- URL to WS endpoint derivation.
- `hello` frame construction.
- Reconnect decision matrix for retryable failures, `4401`, and `4409`.
- `ping` -> `pong`.
- `config_update` updates future tool config.
- FIFO execution order.
- Shared file tool path policy in sandbox and trusted mode.
- `web_fetch` SSRF denylist behavior with a local fake server/DNS
  strategy where practical.
- One-shot exec success, timeout, non-zero exit, cwd policy, and env stripping.
- Transfer begin/chunk/end success and sha mismatch cleanup.
- Shutdown emits best-effort cancellation frames.

Server/client integration tests should cover:

- A real client connects to a test server and appears online.
- Server dispatches a client-routed file tool and receives a result.
- Server dispatches `exec` and receives stdout/stderr/exit code.
- Server-to-client and client-to-server transfer complete and verify sha through
  real `plexus-client` runtime coverage.
- Disconnect mid-call produces server-synthesized `device_unreachable`.

Manual curl/API smoke:

1. Start Postgres and `plexus-server`.
2. Register admin by API.
3. Configure a fake or real Anthropic-compatible LLM.
4. Create a device and capture `PLEXUS_DEVICE_TOKEN`.
5. Start `plexus-client` with env vars.
6. Use REST chat APIs to ask the agent to:
   - list the client workspace;
   - write and read a file on the client;
   - run a one-shot command;
   - copy a file server -> client;
   - copy a file client -> server.
7. Kill the client during a tool call and verify the agent observes
   `device_unreachable`.

## 13. Docs to Keep in Sync

Implementation must keep these docs aligned:

- `docs/API.yaml` if any device/client-facing REST response changes.
- `docs/PROTOCOL.md` for frame behavior, transfer scope, or close-code changes.
- `docs/TOOLS.md` for tool schemas, result shapes, error codes, and transfer
  semantics.
- `docs/SCHEMA.md` if any persistence changes land.
- `docs/DECISIONS.md` for ADR-level startup, distribution, sandbox, and roadmap
  decisions.
- `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md` for status
  and milestone map changes.

## 14. Exit Criteria

Client Alpha is complete when:

- `plexus-client` builds as part of the workspace.
- Focused client tests pass.
- Focused server/client integration tests pass.
- Relevant server/common tests still pass.
- A scripted API e2e proves a real client can execute file tools, one-shot exec,
  and server/client file transfers through the normal agent loop.
- Docs above are updated.
- Deferred work remains explicitly listed in this spec or a follow-up roadmap.
