# Python-Main ADR Audit

**Status:** in progress
**Branch:** `python-main`
**Last updated:** 2026-06-10

This file tracks which Rust-era ADRs remain binding for the hand-written Python
rewrite. Old ADRs are inputs, not automatic requirements. Each ADR should be
classified before implementation depends on it.

## Status Values

- `Keep` - Semantics carry forward unchanged.
- `Translate` - Semantics carry forward, but Rust-specific mechanics change.
- `Supersede` - Replaced by a newer Python-main decision.
- `Archive-only` - Historical context only; not a Python-main requirement.
- `Rewrite-needed` - Product direction is still relevant, but the contract needs
  a new ADR/spec before implementation.

## Current Accepted Python-Main Decisions

- `Py-Prep` is the current docs-only ADR audit and cleanup track. It removes
  stale Rust-era cognitive burden before Python production code starts.
- `Py0` is common-only. It builds `openoctopus_common` shared DTOs, base types,
  error codes, API/protocol/tool/provider contracts, path/workspace refs, and
  documented DB/storage choices. It does not include a FastAPI app, server
  runner, or client runtime.
- Server milestones use hand-written Python: a first-party async agent loop,
  a OpenOctopus-owned Anthropic Messages adapter, Pydantic contracts, and explicit
  OpenOctopus-owned persistence/protocol behavior.
- Server milestones should use mainstream Python infrastructure SDKs where
  they fit the contract: FastAPI for HTTP/streaming responses, SQLAlchemy for
  database access, Pydantic for DTOs/contracts, and the Anthropic Python SDK
  for provider transport. SDKs are adapters; OpenOctopus still owns protocol,
  transcript, tool, workspace, and error semantics.
- LangChain is not a production dependency. The existing LangChain script is a
  live-smoke/reference check only.
- LangGraph is not a production dependency. It remains a future option only if
  graph-level orchestration becomes a real product need and a later ADR defines
  how graph checkpoints relate to the OpenOctopus transcript.
- Anthropic Messages remains the only provider wire format unless a future ADR
  changes ADR-101.
- The Python server alpha runs one ASGI worker and uses asyncio for
  concurrency. Redis is not a server-alpha dependency, and Postgres is not used
  as a general cross-worker command bus. `POST messages` may stream
  best-effort live preview events for the current HTTP connection; `GET
  messages` is the PostgreSQL-backed canonical history/status surface. The
  server alpha does not guarantee per-turn live stream replay across
  disconnects or process restarts.
- Pending browser messages drain as a batch at the next safe boundary. If
  multiple browser POSTs arrive while a session is running, every accepted
  message remains durable in `pending_messages`, but only the newest queued POST
  response is kept as the live subscriber for the upcoming batch. Older queued
  responses receive `stream_replaced` and close.
- `GET /api/sessions/{id}/messages` returns a DB-only snapshot with canonical
  `messages` and durable `pending_messages` separated. Pending rows keep the
  same UUID they will use after safe-boundary drain, allowing frontend
  reconciliation without treating pending input as provider-visible history.
- LLM provider concurrency is protected by the admin-configured
  `llm_max_concurrent_requests` in-process semaphore. Blocking file IO, hashes,
  recursive grep/find_files, and transfer work must cross a background/thread
  boundary so the single event loop stays responsive.
- Workspace Files REST mirrors the shared file tools: every route requires an
  explicit `openoctopus_device`, there is no server default, server relative paths
  resolve to the user's personal workspace, server absolute `/name@suffix/...`
  paths address shared workspaces, paired device names route over `/ws/device`,
  and offline paired targets fail at dispatch with `device_unreachable`.
- Web `message(media=...)` delivery may create online-only device file refs:
  the message stores device+path metadata, and the browser downloads later
  through the Workspace Files `GET` relay. Third-party channels do not receive
  OpenOctopus download links by default; they stream device/server bytes directly
  into the platform's native file upload API.
- Channel configuration uses generic routes over platform-specific payloads:
  `GET /api/channels`, `PATCH /api/channels/{channel}`, and
  `DELETE /api/channels/{channel}`. There is no per-channel detail GET and no
  `enabled` flag; config existence means enabled. Platform fields keep their
  native names, and secret reads return `bot_token_hint` only.
- Python-main removes `GET /api/me/events`. Account-level UI changes are
  observed through ordinary authoritative reads. Online client MCP config
  failures return from `PATCH /api/devices/{name}/config`; offline MCP config
  failures are pruned on reconnect and become visible through device config
  reads.
- Python-main Sessions API is message-driven: there is no `POST /api/sessions`,
  no `GET /api/sessions/{id}`, and no `GET /api/sessions/{id}/stream`.
  Frontends generate a UUID and `POST /api/sessions/{id}/messages` creates the
  web session if it is missing.
- `GET /api/sessions` returns a derived `unread` boolean but no live run
  status or message preview. Read state is persisted as `sessions.last_read_at`
  and advanced explicitly through `PATCH /api/sessions/{id}` with
  `read_through_message_id`. The target message must already be a
  user-visible canonical message, and the marker only moves forward.
- `PATCH /api/sessions/{id}` may update user-owned UI metadata for any owned
  session, including non-web channel sessions. This includes `title` and
  `last_read_at`; it does not make non-web sessions browser-message-writable.
- `DELETE /api/sessions/{id}` is a hard stop for any owned session: terminate
  in-memory runner/streams, delete the session row, and rely on cascade cleanup
  for `messages` and `pending_messages`. It does not insert stop markers.
- `POST /api/sessions/{id}/cancel` is a safe-boundary session-control operation
  for any owned session. It is a no-op if no runner is active; otherwise it sets
  `cancel_requested`, waits for the current external action to finish, writes
  synthetic `user_cancelled` results for unstarted tools, writes the stop
  marker, emits a cancelled turn finish event, clears the flag, and exits.
- Python-main device permissions are per-device only. `devices.sandbox_mode` is
  the coarse persisted switch; sessions cannot temporarily escalate. Client
  filesystem policy, client `web_fetch` SSRF policy, exec cwd policy, env
  inheritance, and command denial all read the target device row.
- Device policy uses denylist/allowlist asymmetrically: SSRF and commands are
  denylist fields (`ssrf_denylist`, `command_denylist`) so users can remove a
  blocking default rule; env stays an allowlist (`env_allowlist`) because secret
  env names are not enumerable.
- Device identity keeps the existing token-as-PK design: `devices.token` is the
  plaintext credential and primary key, REST/tool routing uses `(user_id, name)`,
  and token regeneration updates the PK in place while preserving the row/config.
  This remains valid only while `devices` has no inbound foreign keys.
- Device online state is in memory only. `GET /api/devices` computes `online`
  from the WebSocket registry; offline-but-paired devices remain in
  `openoctopus_device` enums and fail at dispatch with `device_unreachable`.

## Accepted Theme 1: Project Shape, Ingress, Sessions, and Outbound Surface

This theme covers the project/package shape and the early message-bus/channel
contracts from ADR-001 through ADR-020. Python-main preserves the single
normalized-ingress philosophy, but browser chat streaming is no longer inherited
from the page-lifetime SSE/no-token-streaming design.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-001 | Three-crate workspace | `Supersede` | Replace Rust crates with one Python project containing `openoctopus_common`, `openoctopus_server`, and `openoctopus_client` packages. |
| ADR-002 | Frontend embedded in server binary; Vite + proxy | `Rewrite-needed` | Rust binary embedding does not carry forward. Frontend/dev-server/packaging policy needs a Python/Docker-era decision later. |
| ADR-003 | Browser uses REST + SSE; devices use WebSocket | `Rewrite-needed` | Device WebSocket carries forward. Browser chat uses streaming `POST messages` for the current turn and `GET messages` polling for canonical history/status; the old per-session chat SSE stream is not inherited. Browser web sessions are created implicitly by `POST messages` when the client-generated UUID is missing. |
| ADR-004 | Auth: cookie for browser, bearer for programmatic | `Translate` | Keep the browser cookie/programmatic bearer split, implemented with the Python web stack. |
| ADR-005 | Single `InboundMessage` shape; no `EventKind` | `Translate` | Keep one normalized ingress shape, implemented as Pydantic DTOs and durable session input handling. |
| ADR-006 | `session_key` = override or `{channel}:{chat_id}` | `Translate` | Keep deterministic session routing, implemented with Python DTOs/DB helpers. Browser sessions still use a stable public UUID path and an internal web session key unless a later browser API ADR changes it. |
| ADR-007 | No `is_partner` field; wrap baked into content at adapter | `Translate` | Keep adapter-authored trust wrapping for channel ingress. Exact wrapper text should be rechecked when channel adapters are rescoped. |
| ADR-008 | No `sender_id` on `InboundMessage` | `Translate` | Keep the thin ingress shape. Sender identity can remain adapter metadata unless a later moderation/subagent ADR adds a persisted field. |
| ADR-009 | `user_id` stamped at ingress | `Translate` | Keep early account ownership stamping across REST, channel, cron, and heartbeat ingress. |
| ADR-010 | Autonomous flows = user-message injection into dedicated sessions | `Translate` | Keep cron/heartbeat as normal session ingress, not separate agent-loop branches. Python cron/heartbeat mechanics will be designed later. |
| ADR-011 | Per-session async lock + pending queue for mid-turn follow-ups | `Rewrite-needed` | Preserve per-session serial semantics and durable pending messages, but replace Rust/Tokio locking with Python asyncio reservations inside the single server-alpha ASGI worker. Postgres stores durable state; Redis and cross-worker routing are deferred. |
| ADR-012 | Three external ingress sources + two internal synthesizers | `Translate` | Keep the external/internal ingress taxonomy as product direction. Concrete channel adapters remain deferred; early server milestones start with REST and test synthesizers. |
| ADR-013 | Fire-and-forget ingress; HTTP caller does not wait on agent | `Rewrite-needed` | Browser/API turn start changes from immediate 202 to streaming POST. The runner remains detached from the HTTP request lifetime; reconnect uses `GET messages` polling and intentionally does not recover missed token deltas. |
| ADR-014 | Crash recovery is passive — JIT repair at iteration start | `Translate` | Keep transcript-is-state recovery: repair unpaired `tool_use` blocks from persisted history before the next provider call. |
| ADR-015 | Two outbound variants: Hint + Final | `Rewrite-needed` | Replace or map this to best-effort POST-stream preview events plus channel-adapter aggregation. Durable final messages remain; transient deltas/hints are not replay guarantees. |
| ADR-016 | No token-level streaming | `Supersede` | Superseded by the Python-main decision to support best-effort token-level live preview for browser/API consumers while persisting only complete messages. |
| ADR-017 | Hints are mechanical, not LLM-narrated | `Translate` | Keep mechanical tool/progress events, expressed as `TurnEvent` records rather than Rust `Outbound::Hint`. |
| ADR-018 | Interim LLM narration alongside `tool_use` | `Rewrite-needed` | Token-level live preview changes what can be surfaced live. Persistence keeps full assistant messages only; partial live tokens are discarded on disconnect gaps/restart rather than becoming transcript state. |
| ADR-019 | Per-channel hint rendering contract | `Rewrite-needed` | Channel adapters should aggregate `TurnEvent`s according to channel capability. Browser no longer inherits the old session SSE hint-only shape. |
| ADR-020 | Direct replies route to current session; `message` tool defaults current session | `Translate` | Keep the product behavior for direct replies and the `message` tool default target. Python channel adapters and tool schemas will translate the mechanics later. |

## Accepted Theme 2: Agent Loop, Provider Projection, and Transcript State

This theme covers ADR-021 through ADR-037. Python-main keeps the
transcript-is-state model and the hand-written ReAct loop. Rust-specific
runtime details, non-streaming assumptions, and old `Outbound` event names do
not carry forward unchanged.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-021 | Single while-loop, terminate when LLM returns no `tool_use` blocks | `Translate` | Keep the hand-written loop shape as an async Python `AgentRunner`; integrate token-level transient `TurnEvent`s without adding LangGraph state. |
| ADR-022 | `context::build_context` is a pure function | `Translate` | Keep the pure-input context builder principle. Replace Rust structs with Pydantic/domain DTOs and keep DB/AppState access outside the builder. |
| ADR-023 | Single system prompt shape, no `PromptMode` | `Translate` | Keep one prompt shape for user, cron, heartbeat, and later channel turns. Python prompt assembly gets its own tests. |
| ADR-024 | Skills: always-on full body; conditional name + description | `Translate` | Keep progressive skill disclosure. Python file layout and skill loading mechanics need implementation-specific tests. |
| ADR-025 | `tiktoken-rs` for accurate token counts | `Translate` | Preserve accurate token accounting, replacing `tiktoken-rs` with an approved Python tokenizer strategy. Exact model/tokenizer mapping must be revalidated. |
| ADR-026 | Vision retry lives in the provider layer | `Translate` | Keep stateless provider-layer image stripping/retry in the OpenOctopus-owned Anthropic SDK adapter. Do not introduce session-level `vision_stripped` state. |
| ADR-027 | Path-text markers accompany every chat attachment | `Translate` | Keep path markers plus inline image blocks for image attachments. Python workspace/attachment expansion mechanics need a Python spec. |
| ADR-028 | Two-stage compaction | `Translate` | Keep the two-stage concept and DB summary rows, but Python compaction/token counting is not needed before server agent milestones unless context tests require it. |
| ADR-029 | Serial tool dispatch; DB is mid-turn source of truth | `Translate` | Keep serial execution, immediate `tool_result` persistence, and provider-only JIT collapsing. This is a server-agent core invariant. |
| ADR-030 | One hint per `tool_use` at dispatch time, no end-hint | `Rewrite-needed` | Replace old hint-only wording with structured `TurnEvent` tool progress. Channel adapters can still choose whether to render start/end events. |
| ADR-031 | Tool failures propagate as `tool_result` error content | `Translate` | Keep stable tool-result errors and no automatic server-side retry. Python tools/devices must normalize failures into the same block shape. |
| ADR-032 | Persist immediately on every state transition | `Translate` | Keep immediate Postgres inserts for user, assistant, tool result, synthetic error, and compaction rows. Token deltas remain transient and are not persisted as messages. |
| ADR-033 | `publish_final` when: no more tool calls, hard cap, or fatal error | `Translate` | Keep termination conditions, but map `publish_final` to persisted assistant messages plus `assistant_message_finished`/`turn_finished` events and channel aggregation. |
| ADR-034 | Mid-turn inbound queues; drains at iteration boundary | `Translate` | Keep durable pending rows and safe-boundary drains after a tool batch is fully addressed. Multi-worker reservation details belong with ADR-011's rewrite. |
| ADR-035 | User stop button: cancel flag + persisted user message | `Translate` | Keep safe-boundary cancel semantics and synthetic `user_cancelled` tool results. Python API/event names can change with the browser streaming rewrite. |
| ADR-036 | Hard cap 200 iterations + trap-in-loop detection | `Translate` | Keep runaway bounds and repeated-tool trap detection. Python hashing/canonicalization of args needs deterministic tests. |
| ADR-037 | Graceful shutdown observes cancellation token at iteration boundaries | `Translate` | Keep graceful boundary checks, implemented with `asyncio` cancellation/events and DB-consistent exit behavior. |

## Accepted Theme 3: Tools, Workspace, and Error Model

This theme covers ADR-038 through ADR-046. Python-main keeps the product
contract that tools are schema-first, device-routed where appropriate, and
workspace-confined. The mechanics move to Python packages and mainstream SDKs:
tool contracts live in `openoctopus_common`, server execution uses FastAPI and
SQLAlchemy boundaries, and provider transport may use the Anthropic SDK without
giving that SDK ownership of OpenOctopus transcript semantics.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-038 | Shared tool schemas live in `openoctopus_common` | `Translate` | Keep shared schemas and validators in `openoctopus_common`, likely under `openoctopus_common.tools`. Shared server/client tools import the same Pydantic/schema definitions instead of duplicating JSON schema by hand. |
| ADR-039 | Client-only tools live in `openoctopus_client` | `Translate` | Keep `exec` and other host-local capabilities client-owned. The server learns client-only schemas from device handshake/registry data and routes calls; it must not statically depend on client executors. |
| ADR-040 | Server-only tools live in `openoctopus_server` | `Rewrite-needed` | The old server-only ownership matrix is stale. `message`, `cron`, and `file_transfer` remain server-orchestrated, but `web_fetch` must exist on both server and client. A new Python tool-ownership matrix should define shared/server/client/intrinsic-device tools explicitly before implementation depends on this ADR. |
| ADR-041 | `openoctopus_device` routes file tool calls (injected at merge) | `Translate` | Keep nanobot-shaped file tool source schemas in `openoctopus_common` and add OpenOctopus multi-device routing only at schema merge. Py0 should fixture-test against nanobot's current file tool schema set, including `read_file`, `write_file`, `edit_file`, `apply_patch`, `list_dir`, `find_files`, and `grep`. `openoctopus_device` is the reserved routing field injected or extended at schema merge. The enum is based on paired devices, not only online devices; paired-but-offline targets stay visible and fail at dispatch with `device_unreachable`. |
| ADR-042 | `edit_file` uses nanobot-derived fallback matcher | `Translate` | Keep exact, line-trimmed, and smart-quote-normalized matching plus the create-file shortcut. Mirror nanobot's current selector/guard args (`occurrence`, `line_hint`, `expected_replacements`) in `openoctopus_common` and test server/client behavior through the same matcher. |
| ADR-043 | Tool path policy — relative paths resolve to personal workspace; shared workspaces use `name@suffix` absolute form | `Translate` | Keep the path semantics, but re-spec the Python implementation around `pathlib` and nanobot-style workspace guards: resolve relative paths against the intended workspace, enforce `relative_to` containment after resolution, handle symlink escapes, and cover Windows/macOS edge cases in tests if those targets are supported. |
| ADR-044 | Workspace is the canonical file store; no parallel file cache | `Translate` | Keep one durable workspace-backed file model. Server attachments still live under reserved workspace paths, images may also be durable in DB content blocks, and client devices do not gain a separate `.attachments` cache. Device-origin outbound delivery is not automatically durable: web uses online-only device refs, while third-party channels upload to the platform. |
| ADR-045 | `workspace_fs` is the single write path server-side | `Translate` | Keep a single Python workspace service for server-side MinIO object access, quota accounting, path safety, temporary staging, and skills-cache invalidation. Heavy object IO, temp-file IO, hashing, recursive find_files/grep, and copy work should not run directly on the FastAPI/agent event loop; use an explicit thread/background boundary. Py4 must also design object-client pooling, workspace IO backpressure, same-path mutation races, quota races, temp cleanup, and stable S3/MinIO error normalization inside this service before enabling Workspace Files at scale. |
| ADR-046 | All typed errors live in `openoctopus_common/src/errors/` | `Translate` | Replace the Rust module with `openoctopus_common.errors`: stable `ErrorCode` values, typed domain exceptions, and framework-specific HTTP/tool mappings at the edge. FastAPI, SQLAlchemy, httpx, and Anthropic SDK exceptions must be normalized before crossing OpenOctopus wire/tool boundaries. |

## Accepted API Theme: Workspaces and Workspace Files

`docs/API.yaml` is updated for Python-main to treat Workspaces and Workspace
Files as server-alpha surface rather than Rust milestone notes.

- Workspace management routes carry forward: personal + shared workspaces,
  `name@suffix` addressing, allow-list membership, quota state, and rename
  semantics remain binding.
- Python-main server workspaces are MinIO-backed. `workspace_fs` owns all
  object-store access and exposes virtual workspace paths to APIs/tools; server
  disk is only temporary staging/materialization and must be cleaned after use.
- Py-Prep intentionally surfaces but does not solve the Py4 implementation
  complexity of MinIO connection pools, workspace IO concurrency limits,
  same-path write/delete races, quota races, object-index/counter choices,
  temporary staging cleanup, and object-store error normalization. These stay
  inside `workspace_fs` and should not leak into tool/API schemas.
- Quota state is returned on `Workspace` objects from `GET /api/workspaces` and
  `GET /api/workspaces/{workspace_ref}`. The older personal-only
  `GET /api/workspace/quota` route is removed to avoid a second quota surface.
- Workspace file routes require `openoctopus_device` on every operation. Missing
  target is a request error, not an implicit server operation.
- `openoctopus_device=server` uses the Python server workspace service. Relative
  paths resolve to the authenticated user's personal workspace; absolute
  `/name@suffix/...` paths address shared workspaces.
- Paired device names route over the device WebSocket. Paired-but-offline
  devices remain visible and fail with `device_unreachable` when called.
- Workspace Files `GET` against a paired device is also the browser download
  primitive for online-only web `message` delivery refs. The server relays
  device bytes to the HTTP response with bounded buffering and does not stage
  them into MinIO.
- `find_files` and `grep` REST parameters mirror nanobot's richer tool schemas
  closely enough for frontend use: explicit target, path/query/glob/type
  filters, result caps, offset, directory inclusion, sort mode, and grep
  mode/filter options.
- `file_transfer` REST keeps the tool's intrinsic `openoctopus_src_device` and
  `openoctopus_dst_device` fields, rejects destination overwrite, and treats
  cross-device moves as copy-then-delete after destination verification.
- Python implementation note: object reads/writes, temp-file staging, hashes,
  recursive walks, grep, find_files, and transfers must cross an explicit
  background/thread boundary so FastAPI request handlers and the agent loop do
  not block on heavy file IO.

## Accepted API Theme: Devices

`docs/API.yaml`, `docs/SCHEMA.md`, `docs/PROTOCOL.md`, and `docs/DECISIONS.md`
are updated for Python-main to treat Devices as a durable per-user install-site
registry plus a WebSocket reachability layer.

- Device rows are keyed by `devices.token`, which is also the plaintext bearer
  credential used by `WS /ws/device`. The token is returned only from
  `POST /api/devices` and `POST /api/devices/{name}/regenerate-token`.
  Ordinary device reads return `token_hint`, never the full token.
- REST and tool routing use canonical device names, not tokens. Device names are
  canonical slugs, unique per user, normalized on create/rename and path lookup,
  and `server` is reserved for the built-in server install site.
- Token-as-PK is retained because no persistent table has an inbound FK to
  `devices`. If a future milestone adds durable device references, ADR-091 must
  be revisited before adding the FK; likely outcome is introducing immutable
  `devices.id UUID` and demoting `token` to a unique credential.
- `GET /api/devices` lists every paired device for the user and computes
  `online` from the in-memory WebSocket registry. There are no `online` or
  `last_seen_at` columns in Postgres.
- Paired-but-offline devices remain visible in tool schemas. Calls to them fail
  at dispatch with `device_unreachable`; the enum is based on paired topology,
  not momentary connectivity.
- `POST /api/devices` creates a device row, canonicalizes the name, fills default
  config, returns the token exactly once, and stores desired MCP config even if
  the device is not online yet.
- `PATCH /api/devices/{name}/config` is a partial top-level update. Omitted
  fields remain unchanged; `ssrf_denylist`, `env_allowlist`,
  `command_denylist`, and `mcp_servers` are whole-field replacements when
  present. Empty PATCH is a no-op.
- Online MCP config edits are validation-first: server sends `config_validate`,
  the client spawn/introspects without activating the candidate, and only after
  successful validation does the server commit the DB row and send
  `config_update`. Offline MCP config edits store desired config and are
  validated on next reconnect.
- `sandbox_mode` is the only coarse privilege switch. `true` means client file
  tools and Workspace Files routes stay under `workspace_path`, client
  `web_fetch` applies `ssrf_denylist`, and `exec.workdir` must stay inside the
  workspace. `false` means a trusted device may use paths outside
  `workspace_path` and internal/private network access is allowed unless the
  user keeps explicit deny entries.
- `env_allowlist` remains an allowlist for `exec` and client MCP subprocess
  inheritance. `OPENOCTOPUS_DEVICE_TOKEN` must never be forwarded to agent-run
  subprocesses.
- `command_denylist` applies before client `exec` spawn in both sandbox and
  trusted modes. It is a product guardrail, not a hard subprocess sandbox.
- `DELETE /api/devices/{name}` is the state-3 complete wipe: delete the row,
  invalidate the token, close any live WS with 4401, fail in-flight tool calls as
  `device_unreachable`, remove the device from future tool enums, and rely on the
  absence of inbound FKs for single-row cleanup.
- `POST /api/devices/{name}/regenerate-token` preserves the device row, name,
  workspace path, policy fields, shell timeout cap, and MCP config. It replaces
  `devices.token`, returns the new token once, closes the old live connection
  with 4401, and leaves the device offline-but-paired until the client reconnects
  with the new token.

## Accepted API Theme: Channels

`docs/API.yaml`, `docs/SCHEMA.md`, and `docs/DECISIONS.md` are updated for
Python-main to treat channel configs as adapter-owned payloads behind a small
generic API.

- `GET /api/channels` returns every supported channel in one array. Unconfigured
  entries are included as `configured=false, config=null`; there is no separate
  `GET /api/channels/{channel}`.
- `PATCH /api/channels/{channel}` creates or partially updates the user's config
  for a supported channel. Omitted fields are unchanged. `allow_list`, when
  present, is a whole-array replacement.
- `DELETE /api/channels/{channel}` deletes the config row. Config existence is
  the enablement state, so there is no `enabled` flag.
- Platform config payloads keep platform field names instead of being squeezed
  into a generic map. Discord and Telegram currently share
  `bot_token`/`bot_token_hint`, `partner_chat_id`, and `allow_list`; future
  Feishu/Weixin schemas may differ.
- Secrets are write-only. Reads return `bot_token_hint`; callers omit
  `bot_token` to keep the existing secret, and `bot_token: "<redacted>"` is
  rejected.
- `bot_token` updates validate against the platform before persistence.
  `partner_chat_id` updates validate by sending a pairing/success message to
  that target before persistence. `allow_list` entries get basic schema/length
  validation only and are classified by the adapter at receive time.
- Py10 hot reload is a later runtime contract: after successful DB write/delete,
  ChannelManager starts, reloads, or stops the affected user's adapter. Py-Prep
  only fixes the storage/API boundary.
