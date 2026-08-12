# Py5 Python Client and Device Files Design

**Status:** accepted (2026-08-10)
**Milestone:** Py5 Client Alpha
**Depends on:** implemented Py4 workspace/files/message slice and the completed
Python Client/PyInstaller feasibility spike
**Supersedes for Py5:** ADR-001's Go-or-Rust client choice, ADR-102's
single-static-binary packaging assumption, the unused future config fields in
ADR-050, and ADR-087's folder-transfer implementation timing

## Outcome

Py5 adds an independent Python client which connects one user-owned device to
one OpenOctopus server. A server Agent can use the existing shared file tools
and `web_fetch` against that paired device. The browser can use the existing
Workspace Files REST shapes against the same device. A regular file can be
copied in either direction between the server workspace and the device, and a
web `device_file` reference can be downloaded by relaying bytes from an online
device without persisting them in RustFS.

This is a server-and-client vertical slice. The current server has a reserved
`devices` model and forward-looking documentation, but no device routes,
WebSocket registry, client dispatcher, or relay. The current uncommitted
`client/` tree is only the document-conversion and PyInstaller feasibility
spike. Both sides must land together and pass a real end-to-end flow.

The implementation remains intentionally smaller than the final protocol in
`PROTOCOL.md`: Py5 has no shell, MCP, background process sessions, client to
client bridge, recursive folder transfer, frontend, installer, service manager,
or OS security sandbox.

## Decisions made by this design

1. **The client is Python 3.12.** It is an independent `client/` project and
   imports no server package. The shared contract is the wire protocol, tool
   schemas, error codes, and contract fixtures—not shared runtime code.
2. **The distributable is a PyInstaller one-folder bundle.** A compressed
   archive contains the launcher and its private runtime files. It is not
   described as a static or single-file binary.
3. **Linux x86-64 is the Py5 local merge gate.** Windows x86-64 and macOS
   x86-64/arm64 remain product targets, and all new code must avoid knowingly
   platform-specific assumptions, but native frozen verification is deferred.
   Linux Docker can make the Linux build reproducible; it cannot validate native
   Windows or macOS artifacts.
4. **Py5 device config contains only fields Py5 executes:** `workspace_path`,
   `sandbox_mode`, and `ssrf_denylist`. `shell_timeout_max`, `env_allowlist`,
   `command_denylist`, and `mcp_servers` return with the milestones that
   implement shell and MCP. They are not stored or sent speculatively now.
5. **Device bearer tokens are stored as hashes, not plaintext credentials.** A
   token is returned only on creation or regeneration. The server persists a
   SHA-256 digest and a non-secret display hint. There is no production data or
   migration compatibility requirement on this development branch.
6. **Cross-device transfer is single-regular-file only in Py5.** Recursive
   folder copy/move is deferred. This does not remove the local `delete_folder`
   tool, which operates on one device under its normal path policy.
7. **Client-to-server upload requests use a distinct `transfer_request`
   control frame.** `transfer_begin` always comes from the byte sender after it
   has opened and described the source. This removes the current documented
   ambiguity where `transfer_begin` is sometimes a declaration and sometimes a
   request.

These decisions require an appended ADR; accepted historical ADR numbers are
not rewritten or reused.

This accepted design is the target Py5 contract. The first
implementation slice updates `PROTOCOL.md`, `API.yaml`, `SCHEMA.md`, `TOOLS.md`,
and contract fixtures before server and client runtime work proceeds. Until
then, those files intentionally describe an older forward-looking design and
must not be mixed independently with this proposal.

## Scope

### Included

- An independent `openoctopus-client` Python package with `run` and `version`.
- Required `OPENOCTOPUS_SERVER_URL` and `OPENOCTOPUS_DEVICE_TOKEN` startup
  variables, with the token removed from the inherited process environment
  immediately after validation.
- Device create/list/update/delete/token-regeneration REST APIs.
- `/ws/device` bearer authentication, handshake, heartbeat, reconnect,
  duplicate-connection replacement, config push, and graceful shutdown.
- In-memory online-device registry with generation-scoped sends and pending
  operations.
- Device-aware tool schema construction and dispatch for all eleven shared
  tools: `read_file`, `write_file`, `edit_file`, `apply_patch`, `delete_file`,
  `delete_folder`, `list_dir`, `find_files`, `grep`, `notebook_edit`, and
  `web_fetch`.
- Device routing for the existing Workspace Files REST surface, preserving its
  server-side request and response shapes. `notebook_edit` remains Agent-only.
- Client-local MarkItDown conversion for PDF, DOCX, XLSX, PPTX, and downloaded
  HTML, reusing the bounded conversion spike.
- Single-file `server -> client` and `client -> server` `file_transfer`, with
  copy and move semantics, incremental SHA-256 verification, bounded buffering,
  no overwrite, and partial cleanup.
- Browser upload to and download from a paired device through Workspace Files
  REST, including web `device_file` download relay.
- Web `message` delivery refs for online-only device files; the refs remain
  provider-hidden and contain no file bytes.
- Linux source-mode and frozen-bundle tests, a real server/client E2E, and a
  500-connection/session capacity run.

### Deferred

- `exec`, `write_stdin`, `list_exec_sessions`, persistent shell state, and
  reconnectable processes (Py6).
- Client or server MCP, `register_mcp`, `config_validate`, MCP config, resources,
  and prompts (Py7/Py8).
- Client-to-client transfer/bridge.
- Recursive folder upload, download, copy, or move. Agents transfer individual
  files in Py5.
- HTTP Range, transfer resume, resumable partials, transfer-content
  deduplication, and compression.
- VLM, OCR, audio, video, Azure, YouTube, archive recursion, and remote-document
  conversion.
- A frontend. Py5 supplies consistent APIs for a later frontend.
- PyPI publication, auto-update, systemd/launchd/Windows-service installers,
  code signing, and macOS notarization.
- Native Windows/macOS frozen validation. These targets must be tested before a
  public multi-platform release but do not block this Linux development slice.
- An OS-level filesystem/network sandbox. `sandbox_mode` in Py5 is an
  application policy, not a hostile-process security boundary.

## Non-negotiable invariants

1. A device row belongs to exactly one user; every REST route, WS operation,
   tool dispatch, delivery ref, and relay checks that ownership.
2. Device tokens appear only in the initial REST response and the client's
   initial process memory. They are never logged, persisted by the client,
   included in URLs, forwarded to conversion children, or inherited by future
   tool subprocesses.
3. Paired-but-offline devices remain in Agent tool enums. Dispatch fails once
   with `tool_device_unreachable`; the server does not retry a device tool.
4. One stale/replaced socket generation can never send through, unregister, or
   close the newer socket for the same device.
5. No database connection is held while waiting for a client, queue admission,
   WebSocket I/O, RustFS I/O, an HTTP response consumer, or conversion work.
6. Control frames and heartbeats cannot wait behind a whole file. One serialized
   WS writer multiplexes byte-and-count-bounded critical, normal-control, and
   64 KiB binary-frame queues.
7. Transfer and relay implementations never collect a whole file in server
   memory. Every queue and slot count is bounded; slow receivers backpressure
   senders.
8. A failed, cancelled, disconnected, timed-out, or checksum-mismatched transfer
   never exposes a partial destination and releases all permits, queues, path
   reservations, temporary objects/files, futures, and tasks.
9. Device files relayed to a browser are never written to RustFS. Durable server
   storage requires an explicit `file_transfer` to `server`.
10. The client sends raw tool content. Only the server adds the untrusted-result
    warning, truncates/normalizes output, persists it, and projects it to the
    Provider.
11. Source tool schemas stay device-free for routing-only shared tools. The
    server injects the required `openoctopus_device` enum without changing any
    other schema field.
12. Py5 remains a single-ASGI-worker deployment. The online registry, pending
    calls, and admissions are process-local and are not advertised as safe for
    multi-worker routing.
13. Cross-user admission is bounded and keyed: one user's slow transfers or
    queued calls cannot consume every global slot. Transfer waiters are selected
    round-robin across non-empty user queues. A busy device/user receives a
    stable busy result rather than being mislabeled as unreachable.
14. The existing server workspace implementation remains behind
    `WorkspaceService`; device relay does not create a second durable server file
    store.

## 1. Project and packaging boundary

`client/` is a standalone package:

```text
client/
  pyproject.toml
  openoctopus_client.spec
  src/openoctopus_client/
    __main__.py
    cli.py
    config.py
    runtime.py
    protocol.py
    connection.py
    tools/
    transfer.py
    document_convert.py
  tests/
```

The list is an ownership map, not a requirement to create empty modules. Code
stays combined while a module has only one caller or one responsibility.

Runtime dependencies are pinned deliberately and kept smaller than the server
environment. They include a WebSocket client, Pydantic validation, HTTPX,
MarkItDown 0.1.7 with the PDF/DOCX/XLSX/PPTX extras, and pypdf. Dev/build tools
remain optional extras. The PyInstaller spec excludes pytest, Ruff, mypy, and
other development-only packages.

The completed Linux spike establishes feasibility, not the final product:

| Measurement | Spike result |
|---|---:|
| Logical one-folder bundle | about 308 MB |
| Compressed bundle | about 111 MB |
| `version` startup median | about 0.123 s |
| Document conversion | about 0.67–0.83 s on the fixture corpus |
| Sampled process-tree peak RSS | about 221 MB maximum |
| Linux conversion address-space cap | 2 GiB |

MarkItDown's public import path currently brings Magika/ONNX runtime data into
the frozen graph even though Py5 invokes only five converters. Py5 accepts that
bundle cost instead of depending on unsupported private imports. The metrics are
recorded as evidence, not release SLOs.

The merge artifact is built natively on Linux x86-64. A Linux container based on
the oldest supported glibc environment may be added for repeatable Linux builds.
Docker is not used as evidence for native Windows or macOS behavior.

## 2. Device persistence and REST API

### 2.1 Minimal row

Because this is a development database with no migration requirement, the
reserved model is corrected before it becomes executable:

```text
devices
  id              UUID primary key
  user_id         UUID foreign key users(id), cascade delete
  name            canonical per-user routing slug
  token_hash      32-byte SHA-256 digest, unique
  token_hint      non-secret first/last display fragment
  workspace_path  non-empty string, default "~/openoctopus/workspace"
  sandbox_mode    boolean, default true
  ssrf_denylist   JSON array of CIDR/host/host:port entries
  created_at
  unique(user_id, name)
```

Tokens contain at least 256 random bits after the `openoctopus_dev_` prefix.
SHA-256 is sufficient here because the source credential has high entropy; this
is not password hashing. Authentication hashes the supplied bearer value and
looks up the digest. A token hint is display-only and cannot authenticate.

Online state remains memory-only. There is no `online` or `last_seen_at` column.

### 2.2 Routes

All routes use the existing authenticated-user dependency and standard JSON
error envelope:

| Route | Behavior |
|---|---|
| `GET /api/devices` | List this user's devices; includes computed `online`, never token/hash |
| `POST /api/devices` | Canonicalize name, create row, return plaintext token once |
| `PATCH /api/devices/{name}/config` | Partial update/rename; push committed config to an online client |
| `POST /api/devices/{name}/regenerate-token` | Rotate digest, return new token once, revoke old socket |
| `DELETE /api/devices/{name}` | Delete row, remove registry entry, close socket, fail pending work |

`POST` and `PATCH` accept only `name`, `workspace_path`, `sandbox_mode`, and
`ssrf_denylist` as appropriate. Unknown fields are rejected rather than silently
stored for later milestones. Empty PATCH is a no-op and sends no config frame.

Name canonicalization and ownership rules remain those already documented:
NFC normalize, trim, ASCII lowercase, collapse whitespace to hyphens, require
`^[a-z0-9]+(-[a-z0-9]+)*$`, maximum 64 characters, and reject `server`.

Create/delete/rename/rotation invalidate the user's tool-schema cache. A failed
post-commit config push returns the committed row with `online=false`; it does
not roll back valid persisted config merely because the device disconnected.

## 3. Connection lifecycle

### 3.1 Startup

```text
openoctopus-client run       # default when no subcommand is supplied
openoctopus-client version
```

`run` requires:

- `OPENOCTOPUS_SERVER_URL`: `http://` or `https://`, no path/query/fragment.
- `OPENOCTOPUS_DEVICE_TOKEN`: a non-empty `openoctopus_dev_...` bearer token.

After validation, the client copies the token into a redacting secret wrapper
and removes it from `os.environ` before starting threads, multiprocessing, HTTP,
or WebSocket tasks. The server URL is converted from `http(s)` to `ws(s)` and
`/ws/device` is appended. The client creates no config/token/log file.

Logs are single-line stderr logs. Lifecycle changes are INFO; per-call detail is
DEBUG. URLs are logged without userinfo/query, and headers, tokens, delivery
refs, file content, and MCP-shaped future config are never logged.

### 3.2 Handshake

The client sends `hello` first with a fresh UUID v7:

```json
{
  "type": "hello",
  "id": "<uuid-v7>",
  "version": "1",
  "client_version": "<package version>",
  "os": "linux|darwin|windows",
  "caps": {
    "shared_tools": true,
    "web_fetch": true,
    "file_transfer": ["send", "receive"],
    "http_relay": true,
    "exec": false,
    "mcp": false
  }
}
```

The server validates the token before reading `hello`, validates the frame and
protocol version, then re-queries the token digest before registering the
socket. A delete/rotation tombstone closes the narrow stale-row registration
window.

`hello_ack` echoes the ID and sends only active Py5 config:

```json
{
  "type": "hello_ack",
  "id": "<hello id>",
  "device_name": "alice-laptop",
  "config": {
    "workspace_path": "~/openoctopus/workspace",
    "sandbox_mode": true,
    "ssrf_denylist": ["127.0.0.0/8", "::1/128"]
  }
}
```

The client expands only a leading `~`, creates the workspace directory if
missing, verifies it is a directory, and installs an immutable config snapshot.
`config_update` has the same `device_name` and `config` shape. Work already in
flight uses its captured snapshot; later work uses the new one.

If a client cannot prepare an updated workspace path, it sends a sanitized
protocol error and closes instead of continuing indefinitely under stale policy.
The persisted row remains the desired configuration, the server reports the
device offline, and the user can correct it through another PATCH. Reconnect
retries the authoritative persisted config; there is no hidden local fallback.

### 3.3 Registry and duplicate connections

The server registry is keyed by immutable device ID, not by mutable name or
plaintext token. Each accepted socket receives a monotonically increasing
generation. Registering a replacement atomically publishes the new generation,
then closes the old socket with retryable code `4000` and reason
`connection_replaced`.

Every send, heartbeat, completion, and unregister operation includes the
generation. Work from the old reader becomes a no-op when its generation is no
longer current. Replacement fails the old generation's pending calls and slots;
it never transfers them to the new connection.

The registry owns:

- the current socket generation and one serialized writer;
- last-pong state;
- bounded pending tool-call futures keyed by call ID;
- bounded transfer/relay slots keyed by slot ID;
- close/revoke tombstones with a short TTL;
- shutdown cancellation.

It does not import FastAPI route code or hold database sessions.

Each server-side generation has the same single-writer discipline as the client:
critical control, normal control, and per-transfer bulk queues are bounded by
count and bytes. Both endpoints disable WebSocket compression, accept at most
one queued inbound message beyond the reader, cap text frames at 12 MiB, and cap
binary frames at 16 UUID bytes plus 64 KiB payload. Oversized frames close the
generation with a protocol error before they enter application queues.

### 3.4 Heartbeat and reconnect

The server sends the first ping 30 seconds after `hello_ack`, then every 30
seconds. Two missed replies close with `4408`; all pending work becomes
`tool_device_unreachable`.

The client reconnects forever after retryable DNS/TCP/TLS failures, HTTP 429 or
5xx upgrade responses, and unexpected `1000`, `1001`, `1013`, `4000`, or `4408` WS closure, using
1, 2, 4, 8, 16, then 30 seconds with ±20% jitter. `Retry-After` is honored up to
the same 30-second cap. Each successful connection resets the backoff. HTTP
401/403 and WS `4401` are permanent authentication failures and exit non-zero.
Other HTTP 4xx upgrade responses are permanent URL/protocol configuration
errors and exit 78. WS `4409` prints sanitized upgrade details and exits 78.
Malformed response bodies or close-reason JSON are never interpolated into logs
without length/control-character cleanup.

SIGINT/SIGTERM stops admission, cancels the active tool and all transfer slots,
best-effort sends terminal errors, closes with `1001`, reaps conversion children,
and exits. Shutdown has a short fixed grace; it never drains unbounded work.

## 4. Client runtime concurrency

One connection generation owns four cooperating tasks:

1. **Reader:** validates and routes incoming JSON/binary frames. It never runs a
   tool or performs blocking file I/O.
2. **Writer:** the only task that calls WebSocket `send`. A 16-frame critical
   lane is reserved for `pong`, transfer ready/end, protocol errors, and close;
   a normal lane holds at most 64 frames/32 MiB; each active transfer bulk lane
   holds four 64 KiB chunks. Critical frames are checked before normal and bulk
   work; normal and active bulk slots are serviced round-robin. No frame is
   silently dropped. Critical overflow or inability to enqueue a completed tool
   result closes the broken generation so the server fails pending work rather
   than hanging it.
3. **Tool worker:** one bounded FIFO, one active tool per device. This preserves
   deterministic local mutations without blocking heartbeats or transfer bytes.
4. **Transfer manager:** at most two active local slots. It coordinates path
   reservations with tool operations but does not run through the tool FIFO.

The client tool queue holds at most 64 waiting calls and at most 32 MiB of
encoded retained arguments/results. Queue overflow returns `tool_device_busy`;
it does not disconnect and is not reported as unreachable. The WS library also
sets a 12 MiB maximum control-frame size, enough for one bounded 8 MiB image/file
tool payload after JSON/base64 overhead. These are fixed client safety bounds in
Py5 rather than more user-facing config.

Server-side device work uses required global/per-user bounded admission.
Initial deployment settings are:

| Required environment variable | Example | Validation |
|---|---:|---|
| `OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX` | `4096` | integer `64..65536` |
| `OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX_PER_USER` | `64` | integer `1..1024`, less than global |
| `OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX` | `268435456` | integer `16777216..1073741824` |
| `OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX_PER_USER` | `33554432` | integer `1048576..268435456`, less than global |
| `OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY` | `32` | integer `2..256` |
| `OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY_PER_USER` | `2` | integer `1..32`, less than global |
| `OPENOCTOPUS_DEVICE_TRANSFER_QUEUE_TIMEOUT_SECONDS` | `5` | number `0.1..60` |
| `OPENOCTOPUS_DEVICE_TRANSFER_IDLE_TIMEOUT_SECONDS` | `30` | number `1..600` |

Count and encoded-byte permits are acquired before a call frame/future is
retained and released on every completion/cancellation/disconnect. A frame that
exceeds its per-frame bound is rejected even if aggregate capacity remains.
The permit weight is the encoded call plus the tool-specific maximum result
frame advertised in `tool_call.max_result_bytes`; the client must reject output
that would exceed that credit. This reserves response memory before dispatch
instead of discovering a large image result after capacity is exhausted.
Transfer permits cover the whole slot, including HTTP response streaming.
Bounded queues use four 64 KiB chunks per active slot, giving an approximate
server payload-buffer ceiling of `transfer_concurrency * 256 KiB`, excluding
protocol/task overhead.

Pending-call limits are immediate retention limits: capacity exhaustion returns
busy instead of building another unbounded wait queue. Transfer admission has
one FIFO per user and a round-robin arbiter across users; it never relies on
`asyncio.Semaphore` waiter ordering as its fairness contract. Per-user limits
still bound active work after admission. A finite global service can reject a
new caller when genuinely full; the guarantee is isolation from a single noisy
user, not infinite capacity.

Capacity settings do not promise 500-session support by themselves. Py5 only
makes that claim after the acceptance load run completes without leaked tasks,
cross-user starvation, protocol corruption, or unbounded memory growth.

## 5. Tool schemas and dispatch

### 5.1 Schema construction

At each provider iteration, the server loads the user's paired device names in
a short database query before building tool schemas. It then closes the
transaction and builds schemas synchronously from the snapshot.

- Shared tools: inject `openoctopus_device` with `server` plus every paired
  device name, online or offline.
- `message` and `file_transfer`: extend their marked intrinsic device enums with
  the same paired names.
- No client-only tools are advertised in Py5.
- Device online/offline transitions do not change the enum and therefore need
  no schema invalidation; pairing, rename, and delete do.

The captured device-name snapshot is advisory. Dispatch always re-resolves
`(user_id, device_name)` and checks the live generation, so rename/delete/offline
races fail safely.

### 5.2 Dispatch

For a non-server shared-tool target, the registry:

1. removes the routing-only `openoctopus_device` field;
2. resolves the owned paired device;
3. acquires pending-call admission without holding a DB session;
4. sends `tool_call{id,name,args}` to the captured generation;
5. waits for `tool_result`, disconnect, cancellation, or the tool's documented
   deadline;
6. validates string or safe text/image blocks and returns raw content to the
   existing server normalization path.

Unknown IDs, duplicate results, unsafe block types, invalid base64, malformed
frames, and results from stale generations are protocol errors and never become
another call's result. Client tool failures are normal `tool_result` frames,
not `error` frames.

## 6. Local filesystem policy and races

### 6.1 Path resolution

Relative paths resolve under the expanded device workspace. With
`sandbox_mode=true`, absolute paths must also stay under that root. Resolution:

1. rejects NUL and platform-invalid forms;
2. normalizes path syntax without accepting drive-relative Windows paths;
3. checks each existing component for symlink/reparse traversal;
4. resolves the closest existing parent and verifies containment;
5. opens only regular files for file reads/transfers and directories for
   directory operations;
6. repeats the relevant identity/containment check immediately before commit.

With `sandbox_mode=false`, absolute paths outside the workspace are allowed, but
special files such as devices, sockets, and FIFOs remain rejected for bounded
read/transfer operations.

This blocks ordinary path escape but is not an OS sandbox. An external process
can race path components between checks, and a local editor does not honor
OpenOctopus locks. Py7 must add an OS-specific hard boundary without changing
the tool schemas.

### 6.2 Mutation coordination

The client owns cancellation-safe keyed locks by canonical path. Multi-path
operations sort keys before acquiring them. Tool edits, REST edits, transfers,
and relays use the same coordinator.

Ordinary writes use a same-directory temporary regular file, flush/fsync, then
an atomic replace because overwrite is their documented behavior. A
create-without-overwrite transfer commits the completed temporary file with an
atomic no-replace primitive—on ordinary local filesystems this is a same-device
hard-link/create operation which fails if the destination exists, followed by
unlinking the temporary name. A platform/filesystem without a proven atomic
no-replace operation returns a conflict/unsupported error; it never falls back
to `os.replace`. Temporary names are random, hidden, and removed on all failures.

Local move uses the platform-native exclusive no-replace rename primitive:
Linux `renameat2(RENAME_NOREPLACE)`, macOS `renameatx_np(RENAME_EXCL)`, and a
handle-based no-replace rename on Windows. It is zero-copy and same-volume only.
Cross-volume moves and platforms/filesystems without that primitive fail
explicitly without changing either source or destination; moves never use a
hard-link or implicit copy fallback.

For an external user/editor race, the client compares the source identity and a
content/metadata fingerprint immediately before commit. A detected change
returns `workspace_file_changed`. There is still an unavoidable final
check-to-rename race with non-cooperating local processes on portable filesystems;
Py5 documents this instead of claiming a cross-process lock it does not own.

`apply_patch` preserves the existing tool's validation and result semantics.
It is coordinated across its paths but is not falsely advertised as a durable
filesystem transaction across unrelated external writers.

## 7. Shared tool implementation

The client independently implements the exact source arguments and result shapes
from `TOOLS.md`. Server-owned JSON fixtures assert that client argument models
accept every canonical example and reject routing fields inside source args.

- Text reads retain line numbers, offset/limit behavior, UTF-8 diagnostics, and
  output caps.
- Image reads return only allowed base64 image blocks.
- PDF/DOCX/XLSX/PPTX reads use the conversion worker described below.
- Text writes/edits/patches preserve the server-side matching semantics.
- Directory listing/find/grep enforce their existing page, scan, noise-directory,
  regex, glob, and output bounds.
- `notebook_edit` is dispatchable by the Agent but has no Workspace Files REST
  endpoint.
- `web_fetch` keeps URL validation, redirect limits, DNS re-resolution, actual
  connect-IP checks, response byte limits, charset handling, timeout, and
  output normalization. In sandbox mode, each resolved/connect IP is checked
  against `ssrf_denylist`; trusted mode still honors explicitly stored entries.

No tool implementation receives the device token or raw connection object.

### 7.1 MarkItDown boundary

The spike becomes an internal `read_file`/`web_fetch` component; the private
conversion-worker mode remains test-only and is not documented as a user
command. The spike's `multiprocessing` launch is replaced by an explicit
same-executable subprocess so the parent can supply a minimal environment
rather than inheriting every user secret.

- Input is at most 8 MiB and output at most 128,000 characters.
- PDF defaults to pages 1–20; an explicit inclusive range is limited to 20
  pages and reports total/continuation information.
- OOXML ZIP preflight rejects traversal, excessive members, expanded bytes,
  oversized entries, and suspicious compression ratios.
- A fresh spawn child handles one conversion with a 20-second wall deadline.
- Linux applies 2 GiB `RLIMIT_AS` and CPU limits before MarkItDown imports.
- Windows/macOS, when later validated, retain byte/output/deadline/concurrency
  limits but must be documented as lacking this Linux `resource` boundary.
- The helper subprocess receives either already-downloaded bounded HTML bytes or
  one already-resolved local document path plus trusted format metadata. For a
  local path it performs the regular-file, no-follow, and 8 MiB bounded read
  before invoking MarkItDown on bytes; the deadline therefore covers a slow
  open/read as well as parsing. MarkItDown itself never receives a URL/path.
- The helper is launched with an explicit platform-minimal environment
  (`SYSTEMROOT`/`WINDIR` where required, locale, and temporary-directory
  variables only). It inherits no arbitrary parent variables, proxy settings,
  cloud keys, or device token. Tests seed fake secrets and inspect the worker's
  environment contract.
- VLM, plugins, default converter discovery, OCR, audio/video, Azure, YouTube,
  and archive recursion remain disabled.

Conversion failure returns stable tool codes and sanitized messages. Tracebacks,
local environment values, and parser internals do not cross the protocol.

## 8. Workspace Files REST routing

Every existing Workspace Files route continues to require
`openoctopus_device`. `server` follows the Py4 path unchanged. A paired name is
resolved to the authenticated user's device and routed over its live socket.

The device-backed route keeps the same public shape as the server-backed route:

| REST operation | Device action |
|---|---|
| GET file | Relay regular-file bytes to a backpressured HTTP response |
| PUT file | Stream request bytes into an atomic device destination |
| PATCH file | Dispatch `edit_file` with conditional fingerprint |
| POST patch set | Dispatch `apply_patch` with its existing multi-file result shape |
| DELETE file/folder | Dispatch matching local mutation |
| list/find/grep | Dispatch matching bounded local query |
| transfer | Invoke server-owned `file_transfer` orchestration |

Device ETags are opaque version fingerprints derived from stable stat identity,
size, and nanosecond modification time; transfer SHA-256 is a separate integrity
field. This avoids reading a multi-gigabyte file twice merely to start a GET.
`If-Match` and `If-None-Match` are evaluated on the device while its path
lock/reservation is held and checked again before commit. A successful mutation
returns the same DTO fields as the server path. The documented external-writer
race boundary still applies on filesystems whose metadata can be changed
without changing the fingerprint.

The server checks browser JWT ownership before contacting the device. The
client re-applies path policy; the server never treats a user-supplied path as
authorized merely because it was inside an owned-device URL.

HTTP disconnect cancels the slot. Slow body receive or response send beyond the
configured idle timeout cancels both sides. The HTTP/WS bridge has bounded
queues and does not retain the whole request/response.

## 9. Transfer protocol

### 9.1 Control frames

Py5 clarifies the slot state machine:

```text
requester -> sender:   transfer_request     # only when requester needs sender bytes
sender    -> receiver: transfer_begin       # source opened; metadata declared
receiver  -> sender:   transfer_ready       # destination/consumer reserved
sender    -> receiver: binary chunks        # UUID bytes + <=64 KiB payload
sender    -> receiver: transfer_end         # ack=false; digest and byte count
receiver  -> sender:   transfer_end         # ack=true; verified result
```

`transfer_request` contains:

```json
{
  "type": "transfer_request",
  "id": "<uuid-v7>",
  "purpose": "file_transfer|workspace_upload|http_relay",
  "src_path": "reports/a.pdf",
  "dst_path": "archive/a.pdf"
}
```

`dst_path` is present only when the requester is also the destination. An HTTP
relay has no server destination.

`transfer_begin` contains direction, purpose, source/destination device/path as
applicable, `total_bytes`, optional MIME, and optional precomputed SHA-256.
`total_bytes` is a non-negative integer for a regular-file source and may be
`null` only for a chunked browser upload whose HTTP request did not provide a
length. The receiver answers with `transfer_ready{id}` only after it has
reserved the destination or HTTP consumer. It may instead reject with
`transfer_end(ok=false, code=...)`. A sender does not enqueue bytes until
`transfer_ready` arrives.

`transfer_end` has an `ack` boolean. The first terminal frame has `ack=false`;
the peer cleans up and sends exactly one `ack=true` terminal frame. A successful
sender end always carries `bytes_sent` and its digest; the receiver compares
both and returns the verified digest. Either side cancels/fails from any
non-terminal state with `ack=false, ok=false, code=<stable code>`; the peer
cleans up and acknowledges that failure. An `ack=true` frame is never answered.
Control-frame DTOs reject unknown/oversized fields, invalid UUIDs/digests,
negative sizes, and direction/purpose-inconsistent paths.

Binary layout remains:

```text
16 bytes UUID slot ID | 0..65536 bytes payload
```

The state machine is:

```text
REQUESTED? -> BEGUN -> READY -> STREAMING -> SENDER_ENDED -> COMMITTED
     \          \        \          \             \
      +----------+--------+----------+---------------> ABORTED
```

The receiver owns destination/consumer cleanup; the sender owns source/request
cleanup. Each side releases its slot and admission after terminal acknowledgement
or connection/idle timeout. Terminal slot IDs remain as short-lived tombstones:
an identical late terminal ACK is ignored idempotently, while a conflicting
late frame is a protocol error. For a known failed tombstone recording the
locally emitted `transfer_end(ack=false, ok=false)` (or its matching failure
acknowledgement), the peer may discard only bounded binary chunks already in
flight for that failed slot. Unknown, normally completed, and expired slots
remain errors. Disconnect aborts both local roles without waiting for an ACK.

An unknown/closed slot (other than that narrow known-failure exception),
oversized frame, bytes before `transfer_ready`, extra bytes beyond declared
length, begin/end in an invalid state, or cross-generation slot is a protocol
error. It never writes those bytes to another slot.

For browser `PUT ...?openoctopus_device=<client>`, the server acquires the
per-user/global transfer admission before reading the HTTP body, then becomes the
byte sender and sends `transfer_begin(purpose="workspace_upload", dst_path=...)`
after authorization. The slot covers body receive, WS streaming, verification,
commit, and cleanup. Capacity exhaustion returns 429 before consuming the body.
The client reserves a temporary destination and answers `transfer_ready`; only
then does the server map each request-body chunk into the bounded binary queue.
Unknown HTTP length is represented by `total_bytes=null`. Browser disconnect or
upload idle timeout sends a failing non-ACK terminal frame, closes the request
reader, and removes the client temporary file.

### 9.2 Server to client

The server authorizes and opens a RustFS stream, then sends `transfer_begin`.
The client reserves a non-existing destination, acknowledges, streams chunks to
a same-directory temporary file, verifies byte count/digest, fsyncs, and exposes
the destination atomically. Only then does it acknowledge success.

For `mode=move`, the server deletes the RustFS source only after the client
acknowledges the destination. Delete failure returns success plus a warning and
leaves both copies; it never deletes the verified destination.

### 9.3 Client to server

The server authorizes the destination and sends `transfer_request`. The client
opens/fstats a regular source under its captured config and responds with
metadata in `transfer_begin`.

The server streams the bytes through a bounded adapter into a temporary RustFS
object/multipart upload while incrementally hashing and enforcing the current
workspace quota/single-operation bound. After the sender end is verified, the
existing workspace mutation coordinator rechecks quota and destination absence,
promotes the temporary object while holding the existing WorkspaceService
destination mutation lock, then acknowledges success. Every supported OO write
to the private bucket uses that coordinator; direct external bucket writers and
multi-worker servers are outside the supported topology. Where RustFS exposes a
conditional destination-create primitive the adapter uses it as defense in
depth. Temporary objects are outside user-visible paths and are deleted on every
failure/startup recovery.

For `mode=move`, the server asks the client to delete the source only after the
RustFS destination is committed. Source deletion failure becomes a warning.

An existing `server -> server` regular-file copy/move remains local to
`WorkspaceService`. When source and destination are the same paired client, the
server dispatches one coordinated local copy/move call without sending bytes
through the server. Only two *different* client devices require the deferred
client-to-client bridge.

### 9.4 File-only rule

Every `file_transfer` source must be one regular file. A directory source fails
before any slot or destination is opened. `workspace_upload` is the only
non-file-source exception: its source is the authenticated bounded HTTP request
stream, it has no `src_path`/fstat semantics, and it still produces exactly one
regular destination file. There is no tar/ZIP packaging, implicit traversal,
recursive copy, or multi-file transaction in Py5.

## 10. Web `device_file` delivery and relay

For `message(channel="web", media=[...],
openoctopus_device="<paired>")`, the server validates device ownership and path
shape but does not read the device. It atomically stores a provider-hidden
sidecar ref on the existing assistant tool-use row:

```json
{
  "tool_use_id": "...",
  "type": "device_file",
  "device_id": "0190d5a7-...",
  "openoctopus_device": "alice-laptop",
  "path": "reports/a.pdf",
  "filename": "a.pdf",
  "mime": "application/pdf",
  "online_only": true
}
```

`device_id` is the immutable owned-device identity captured when the ref is
created; `openoctopus_device` is the captured name. `size` is optional; sending
a message never opens a relay merely to discover metadata. The ref is returned
by message-history APIs but never appears in `messages.content` or a Provider
request.

When a browser later calls the normal Workspace Files GET with the ref's device
and path, the server:

1. authenticates the browser, resolves the captured immutable `device_id`, and
   proves it still belongs to that user and still has the captured
   `openoctopus_device` name;
2. acquires per-user/global transfer admission;
3. sends `transfer_request(purpose="http_relay")`;
4. waits for validated metadata before starting HTTP headers;
5. forwards bounded chunks directly into the response;
6. cancels the device slot on browser disconnect, idle timeout, or checksum
   failure.

Relay never resolves by name alone. Renaming or deleting the captured device,
or pairing a different device later under the same name, makes the old ref fail
closed; it cannot silently retarget the download.

The response uses `Content-Length` when known, a sanitized RFC-compatible
`Content-Disposition`, `application/octet-stream` unless the client supplies a
safe MIME hint, and `X-Content-Type-Options: nosniff`. Py5 does not support
Range. Offline click returns `tool_device_unreachable`; missing/policy-blocked
paths retain their stable error codes.

## 11. Errors and caller behavior

The implementation adds only the stable distinctions callers need:

| Condition | Tool/protocol code | REST behavior |
|---|---|---|
| paired device offline/disconnected | `tool_device_unreachable` | 409 |
| fair queue or device queue full | `tool_device_busy` | 429 + `Retry-After` |
| transfer made no progress | `workspace_transfer_timeout` | 408 if headers not sent; otherwise close stream |
| checksum/length mismatch | `workspace_transfer_integrity_failed` | 502 |
| target already exists/precondition changed | existing conflict/file-changed code | 409 |
| invalid client path/policy | existing path/policy tool code | mapped existing status |
| client shutting down | `tool_device_unreachable` at server boundary | 409 |
| malformed/unknown frame | `protocol_*` error | not a REST response by itself |

Specific local failures may be visible to the Agent as sanitized tool results,
so the Agent can explain or retry. Protocol internals, exception reprs, local
absolute paths outside a user-supplied path, database details, credentials, and
tracebacks are not returned.

Once HTTP body bytes have started, a later relay failure cannot change the HTTP
status. The stream closes and server logs record a sanitized code; the frontend
must treat an incomplete `Content-Length` as failed.

## 12. Implementation slices and TDD order

Each slice begins with failing tests and ends with its focused tests, Ruff, and
strict mypy passing. Do not build all protocol code before the first real flow.

### Slice A — accept the spike and client foundation

- Promote the current package/CLI/conversion spike into tracked source.
- Add config secret handling and remove the token from inherited environment.
- Preserve conversion regression/frozen smoke tests.
- Record the Linux bundle metrics and prune generated build artifacts.

**Proof:** source tests + Linux frozen conversion smoke.

### Slice B — minimal device connection

- Correct the device model/bootstrap schema and add minimal device REST.
- Add protocol DTOs, registry, WS authentication, hello/ack, heartbeat,
  generations, reconnect, config update, and shutdown.
- Implement client `run` with no tools beyond protocol handling.

**Proof:** create device → launch real source client → online list → config
update → reconnect → rotate/delete → permanent 4401 exit.

### Slice C — one read-only vertical tool

- Add async paired-device schema discovery and dispatch.
- Implement client `list_dir`, then `read_file`, including document conversion.
- Route the corresponding REST reads.

**Proof:** a real ChatRuntime turn reads a file through the real client; offline
target becomes one normalized `tool_device_unreachable` result.

### Slice D — remaining shared tools and local races

- Implement write/edit/patch/delete/delete-folder/find/grep/notebook/web-fetch.
- Add path policy, keyed mutation coordination, atomic writes, SSRF checks, and
  stable errors.

**Proof:** schema contract fixtures and local/REST/Agent behavior parity,
including external-change conflict tests.

### Slice E — transfer and browser relay

- Add transfer-request state machine, fair admission, single writer
  multiplexing, RustFS streaming adapter, device temp files, and cleanup.
- Implement `file_transfer`, device Workspace Files upload/download, message
  device refs, and delayed HTTP relay.

**Proof:** both directions, copy/move, slow peers, disconnects, mismatches,
destination races, quota rejection, browser cancellation, and no RustFS write
for HTTP relay.

### Slice F — capacity, packaging, and documentation

- Run complete server/client tests, Ruff, strict mypy, schema/OpenAPI checks,
  live PostgreSQL/RustFS tests, frozen Linux E2E, and peak-memory measurement.
- Update API/SCHEMA/TOOLS/PROTOCOL/DECISIONS and deployment docs to the executable
  Py5 subset.
- Keep native Windows/macOS jobs documented but non-blocking until they are run
  on native hosted runners.

## 13. Required tests

### Unit and contract

- Token entropy/hash/hint/redaction, ownership, canonical names, rotation, and
  concurrent delete/handshake races.
- Every frame model, UUID/digest/size limit, unknown field/type, safe result
  block, and binary header vector.
- Connection backoff/jitter, close-code decisions, delayed first ping, missed
  pong, duplicate replacement, stale generation unregister/send, and shutdown.
- FIFO ordering, control priority, queue full, cancellation, timeout, result ID
  mismatch, and late result after reconnect.
- Linux/macOS/Windows path fixtures: traversal, drive-relative/UNC forms,
  case/Unicode, symlink/reparse markers, special files, atomic cleanup, and
  sorted multi-path locking.
- Shared tool schema parity and behavior fixtures, including Chinese text,
  tables, images, notebooks, malformed/large documents, and HTML.
- SSRF redirect/rebinding/connect-IP cases against the device denylist.
- Transfer state machine, interleaved slots, backpressure, unknown slot,
  oversized chunk, length/digest mismatch, ENOSPC/quota, no-overwrite, move
  warning, timeout, cancellation, and partial cleanup.

### Integration

- Real PostgreSQL and RustFS device lifecycle tests.
- Real FastAPI WebSocket + source client, with no in-process fake transport.
- ChatRuntime provider stub selects every shared device tool and observes the
  persisted normalized result.
- Browser Workspace Files routes have matching DTOs for `server` and device.
- Server↔client regular-file copy/move with byte-for-byte digest checks.
- Browser upload to device and slow browser download from device.
- `message` stores `device_file` only in `delivery_refs`; relay touches neither
  `messages.content` nor object storage.
- Cross-user token, device name, delivery ref, slot ID, and path attacks fail.

### Capacity and memory

One repeatable source-mode harness opens 500 authenticated device connections
across at least 100 users, while 500 independent sessions dispatch bounded
read-only calls. It verifies:

- no cross-user result or slot delivery;
- per-device FIFO and cross-device concurrency;
- a deliberately slow user cannot exhaust other users' admission;
- ping/pong remains live under tool and transfer load;
- all calls complete or return the documented busy/unreachable result;
- registry/future/limiter/task counts return to baseline after disconnect;
- parent RSS reaches a plateau rather than growing per completed call.

The harness records wall time, p50/p95 dispatch latency, peak RSS, open file
descriptors, task count, and queue high-water marks. The first run establishes a
baseline; no unsupported latency SLO is invented in the spec. A separate
single real frozen client E2E proves packaging. Spawning 500 frozen processes is
not a meaningful server-capacity test.

## 14. Documentation changes on acceptance

- Append a new ADR recording Python/PyInstaller, active Py5 config, token
  hashing, file-only transfer timing, and native-test deferral.
- Update ADR-001/050/087/102/104/120 with forward references rather than
  rewriting history.
- Reduce `PROTOCOL.md`'s executable Py5 catalog to active frames and add
  `transfer_request`; keep shell/MCP sections explicitly future.
- Update `SCHEMA.md` to the hashed-token minimal device row.
- Update `API.yaml` device DTOs, REST errors, device Workspace Files routes, and
  the `workspace_file | device_file` delivery-ref union.
- Keep `TOOLS.md` schemas canonical and mark recursive transfer, exec, and MCP
  with their actual later milestones.
- Add `client/README.md` with startup, trust boundary, Linux packaging, and the
  unverified-native-platform statement.
- Add all new required server environment variables to `.env.example` and
  deployment capacity guidance.

## 15. Exit criteria

Py5 is complete only when all of the following are true:

1. A device can be created, connected, renamed/reconfigured, rotated, deleted,
   and observed online/offline through the documented REST/WS contracts.
2. A real Agent turn reads and writes a paired client's local workspace through
   unified tool names; an offline device yields one visible
   `tool_device_unreachable` result without retry.
3. All eleven shared tools and device-backed Workspace Files REST operations
   pass their schema/behavior contract tests.
4. One regular file transfers server→client and client→server in copy and move
   modes without overwrite, corruption, whole-file server buffering, or partial
   visibility.
5. A web `device_file` ref downloads through a bounded live relay and never
   appears in Provider context or RustFS.
6. Disconnect, replacement, cancellation, slow-peer, checksum, quota, external
   edit, and cross-user race tests pass without leaked resources.
7. The 500-connection/session harness completes and publishes its capacity and
   peak-memory report.
8. The Linux x86-64 PyInstaller bundle passes black-box startup, conversion, WS,
   file-tool, and transfer smoke tests.
9. Complete server and client pytest suites, Ruff, strict mypy, OpenAPI/schema
   checks, live PostgreSQL/RustFS tests, and `git diff --check` pass.
10. Windows/macOS support is described honestly as unverified until native jobs
    run; neither Docker nor Linux-only CI is cited as evidence for it.
