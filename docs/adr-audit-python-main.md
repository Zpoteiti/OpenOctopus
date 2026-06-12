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
| ADR-003 | Browser REST; devices use WebSocket | `Supersede` | Device WebSocket carries forward. ADR-121 supersedes the old browser per-session SSE stream with streaming `POST messages` for the current turn and `GET messages` polling for canonical history/status. Browser web sessions are created implicitly by `POST messages` when the client-generated UUID is missing. |
| ADR-004 | Auth: cookie for browser, bearer for programmatic | `Translate` | Keep the browser cookie/programmatic bearer split, implemented with the Python web stack. |
| ADR-005 | Single `InboundMessage` shape; no `EventKind` | `Translate` | Keep one normalized ingress shape, implemented as Pydantic DTOs and durable session input handling. |
| ADR-006 | `session_key` = override or `{channel}:{chat_id}` | `Translate` | Keep deterministic session routing, implemented with Python DTOs/DB helpers. Browser sessions still use a stable public UUID path and an internal web session key unless a later browser API ADR changes it. |
| ADR-007 | No `is_partner` field; wrap baked into content at adapter | `Translate` | Keep adapter-authored trust wrapping for channel ingress. Exact wrapper text should be rechecked when channel adapters are rescoped. |
| ADR-008 | No `sender_id` on `InboundMessage` | `Translate` | Keep the thin ingress shape. Sender identity can remain adapter metadata unless a later moderation/subagent ADR adds a persisted field. |
| ADR-009 | `user_id` stamped at ingress | `Translate` | Keep early account ownership stamping across REST, channel, cron, and heartbeat ingress. |
| ADR-010 | Autonomous flows = user-message injection into dedicated sessions | `Translate` | Keep cron/heartbeat as normal session ingress, not separate agent-loop branches. Python cron/heartbeat mechanics are detailed by ADR-053, ADR-054, ADR-112, and ADR-113. |
| ADR-011 | Per-session async lock + pending queue for mid-turn follow-ups | `Translate` | Preserve per-session serial semantics and durable pending messages, but replace Rust/Tokio locking with Python asyncio reservations inside the single server-alpha ASGI worker. Postgres stores durable state; Redis and cross-worker routing are deferred. |
| ADR-012 | Three external ingress sources + two internal synthesizers | `Translate` | Keep the external/internal ingress taxonomy as product direction. Concrete channel adapters remain deferred; early server milestones start with REST and test synthesizers. |
| ADR-013 | Fire-and-forget ingress; HTTP caller does not wait on agent | `Supersede` | ADR-121 supersedes immediate 202 + SSE with streaming POST. The runner remains detached from the HTTP request lifetime; reconnect uses `GET messages` polling and intentionally does not recover missed token deltas. |
| ADR-014 | Crash recovery is passive — JIT repair at iteration start | `Translate` | Keep transcript-is-state recovery: repair unpaired `tool_use` blocks from persisted history before the next provider call. |
| ADR-015 | Two outbound variants: Hint + Final | `Supersede` | Replace the Rust `Outbound` enum with durable persisted messages plus best-effort POST-stream preview events and channel-adapter aggregation. Durable final messages remain; transient deltas/progress are not replay guarantees. |
| ADR-016 | No token-level streaming | `Supersede` | Superseded by the Python-main decision to support best-effort token-level live preview for browser/API consumers while persisting only complete messages. |
| ADR-017 | Hints are mechanical, not LLM-narrated | `Translate` | Keep mechanical tool/progress events, expressed as transient progress records rather than provider-authored narration. |
| ADR-018 | Interim LLM narration alongside `tool_use` | `Supersede` | ADR-121 allows connected browser POST streams to preview interim tokens transiently. Persistence keeps complete assistant messages only; partial live tokens are discarded on disconnect gaps/restart rather than becoming transcript state. |
| ADR-019 | Per-channel hint rendering contract | `Supersede` | Channel adapters aggregate/drop transient progress according to channel capability. Browser no longer inherits the old session SSE hint-only shape; it uses `PostMessageStreamEvent` on active POST responses. |
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
| ADR-030 | One hint per `tool_use` at dispatch time, no end-hint | `Supersede` | Replace old hint-only wording with structured `tool_progress` events on active POST streams. Channel adapters can still choose whether to render start/end events. |
| ADR-031 | Tool failures propagate as `tool_result` error content | `Translate` | Keep stable tool-result errors and no automatic server-side retry. Python tools/devices must normalize failures into the same block shape. |
| ADR-032 | Persist immediately on every state transition | `Translate` | Keep immediate Postgres inserts for user, assistant, tool result, synthetic error, and compaction rows. Token deltas remain transient and are not persisted as messages. |
| ADR-033 | `publish_final` when: no more tool calls, hard cap, or fatal error | `Translate` | Keep termination conditions, but map `publish_final` to persisted assistant messages plus `assistant_message_finished`/`turn_finished` events and channel aggregation. |
| ADR-034 | Mid-turn inbound queues; drains at iteration boundary | `Translate` | Keep durable pending rows and safe-boundary drains after a tool batch is fully addressed. Multi-worker reservation details belong with ADR-011's rewrite. |
| ADR-035 | User stop button: cancel flag + persisted user message | `Translate` | Keep safe-boundary cancel semantics and synthetic `user_cancelled` tool results. Python API/event names can change with the browser streaming rewrite. |
| ADR-036 | Hard cap 200 iterations + trap-in-loop detection | `Translate` | Keep runaway bounds and repeated-tool trap detection. Python hashing/canonicalization of args needs deterministic tests. |
| ADR-037 | Graceful shutdown observes cancellation token at iteration boundaries | `Translate` | Keep graceful boundary checks, implemented with `asyncio` cancellation/events and DB-consistent exit behavior. |

## Accepted API Theme: Browser Sessions and Streaming

`docs/API.yaml`, `docs/SCHEMA.md`, `docs/DECISIONS.md`, and
`docs/PROTOCOL.md` are updated for Python-main to treat browser chat as
canonical message polling plus best-effort current-turn POST streaming.

- `POST /api/sessions/{id}/messages` creates missing web sessions from the
  client-generated UUID, durably accepts the user message, and may keep the HTTP
  response open as a newline-delimited `PostMessageStreamEvent` stream.
- The POST stream is a live subscriber only. Disconnecting it does not cancel the
  runner, roll back accepted input, or make token deltas replayable.
- `GET /api/sessions/{id}/messages` is the recovery and canonical history
  surface. It returns persisted complete messages, durable pending messages, and
  run status, but never in-flight token deltas.
- Mid-turn browser follow-ups are durable `pending_messages`. The newest queued
  POST response may become the live subscriber for the next drained batch; older
  queued responses close with `stream_replaced` after their own input is durable.
- Browser progress events use `PostMessageStreamEvent` (`token_delta`,
  `tool_progress`, `message_persisted`, `turn_finished`, `stream_replaced`, and
  keepalive). Channel adapters may aggregate or drop equivalent transient
  progress; durable final messages remain the portable channel contract.
- The Python server alpha intentionally does not use Redis or a durable token log
  for live replay. Postgres remains the durable source of truth for sessions,
  messages, pending rows, tool results, and recovery.

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
| ADR-040 | Python-main tool ownership matrix | `Supersede` | The old server-owned tool matrix is replaced by the explicit `docs/TOOLS.md` inventory: server-orchestrated tools are `message`, `cron`, and `file_transfer`; client-only is `exec`; shared tools include `web_fetch`; MCP-wrapped entries run wherever installed. |
| ADR-041 | `openoctopus_device` routes file tool calls (injected at merge) | `Translate` | Keep nanobot-shaped file tool source schemas in `openoctopus_common` and add OpenOctopus multi-device routing only at schema merge. Py0 should fixture-test against nanobot's current file tool schema set, including `read_file`, `write_file`, `edit_file`, `apply_patch`, `list_dir`, `find_files`, and `grep`. `openoctopus_device` is the reserved routing field injected or extended at schema merge. The enum is based on paired devices, not only online devices; paired-but-offline targets stay visible and fail at dispatch with `device_unreachable`. |
| ADR-042 | `edit_file` uses nanobot-derived fallback matcher | `Translate` | Keep exact, line-trimmed, and smart-quote-normalized matching plus the create-file shortcut. Mirror nanobot's current selector/guard args (`occurrence`, `line_hint`, `expected_replacements`) in `openoctopus_common` and test server/client behavior through the same matcher. |
| ADR-043 | Tool path policy — relative paths resolve to personal workspace; shared workspaces use `name@suffix` absolute form | `Translate` | Keep the path semantics, but re-spec the Python implementation around `pathlib` and nanobot-style workspace guards: resolve relative paths against the intended workspace, enforce `relative_to` containment after resolution, handle symlink escapes, and cover Windows/macOS edge cases in tests if those targets are supported. |
| ADR-044 | Workspace is the canonical file store; no parallel file cache | `Translate` | Keep one durable workspace-backed file model. Server attachments still live under reserved workspace paths, images may also be durable in DB content blocks, and client devices do not gain a separate `.attachments` cache. Device-origin outbound delivery is not automatically durable: web uses online-only device refs, while third-party channels upload to the platform. |
| ADR-045 | `workspace_fs` is the single write path server-side | `Translate` | Keep a single Python workspace service for server-side MinIO object access, quota accounting, path safety, temporary staging, and skills-cache invalidation. Heavy object IO, temp-file IO, hashing, recursive find_files/grep, and copy work should not run directly on the FastAPI/agent event loop; use an explicit thread/background boundary. Py4 must also design object-client pooling, workspace IO backpressure, same-path mutation races, quota races, temp cleanup, and stable S3/MinIO error normalization inside this service before enabling Workspace Files at scale. |
| ADR-046 | All typed errors live in `openoctopus_common/src/errors/` | `Translate` | Replace the Rust module with `openoctopus_common.errors`: stable `ErrorCode` values, typed domain exceptions, and framework-specific HTTP/tool mappings at the edge. FastAPI, SQLAlchemy, httpx, and Anthropic SDK exceptions must be normalized before crossing OpenOctopus wire/tool boundaries. |

The Python-main tool ownership matrix is now explicit in `docs/TOOLS.md`:
shared schemas live in `openoctopus_common`, shared implementations run on server
and client install sites, `message`/`cron`/`file_transfer` are
server-orchestrated, `exec` is client-only, and MCP-wrapped entries are dynamic
per install site. `web_fetch` is part of the shared tool set.

## Accepted Theme 4: MCP, Device Config, and Web Fetch

This theme covers ADR-047 through ADR-052. Python-main keeps MCP as a flat
tool-registry surface, device config as the persistent policy boundary, and
`web_fetch` as a shared server/client tool. Rust-specific crate/module details
translate to Python package boundaries.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-047 | Shared MCP client in `openoctopus_common` | `Translate` | Keep one shared MCP session/wrapper layer conceptually in `openoctopus_common`, with server/admin and client/device runtimes importing it through Python package boundaries. All three MCP surfaces (`tools`, `resources`, `prompts`) register into the per-user tool registry. |
| ADR-048 | MCP wrapping — tools, resources, prompts as tool-registry entries | `Translate` | Keep the wrapped naming convention (`mcp_<server>_<tool>`, `_resource_`, `_prompt_`), prompt output stringification, resource URI-template argument expansion, and merge-time `openoctopus_device` injection. Python implementations replace Rust structs/rmcp mechanics but preserve the agent-visible schema contract. |
| ADR-049 | MCP collision rejection — server orchestrates DB cleanup + corrective config_update | `Translate` | Keep rejection for within-server duplicate wrapped names, cross-install schema drift, and spawn/introspection failures. Online device edits validate before DB commit; offline desired config is pruned on reconnect and surfaced through ordinary device reads. Admin server-MCP validation is synchronous on the admin HTTP request. |
| ADR-050 | Device config is first-class + editable | `Translate` | Keep the device row as the config source for `workspace_path`, `sandbox_mode`, `shell_timeout_max`, SSRF/env/command policy, and `mcp_servers`. PATCH is partial top-level, policy arrays/maps are whole-field replacements, MCP edits use validation-first `config_validate` when online, and accepted changes push authoritative `config_update`. |
| ADR-051 | Device policy is persistent; no session-level privilege escalation | `Translate` | Keep per-device policy as the only privilege boundary. Browser, channel, cron, and heartbeat sessions cannot temporarily escalate; users change the durable device config when a workflow needs broader access. |
| ADR-052 | `web_fetch` is shared; server hard-blocks private addresses, clients use per-device denylist policy | `Translate` | Keep `web_fetch` as a shared server/client tool. Server install site always applies the hard private/reserved-address block; client install sites apply the target device's `ssrf_denylist` according to `sandbox_mode`. This remains structured tool policy, not an OS-level network sandbox. |

## Accepted Theme 5: Cron, Heartbeat, and Dream Deferral

This theme covers ADR-053 through ADR-055. Python-main keeps autonomous flows as
ordinary user-message injection into dedicated sessions, while keeping Dream out
of the near-term server/client milestones.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-053 | Cron: per-job dedicated isolated session | `Translate` | Keep cron as a durable trigger that injects the stored `message` into a dedicated `session_key="cron:{job_id}"` session. Python-main details are in the `/api/cron` and `cron` tool contracts plus ADR-112 ticker mechanics. Cron does not inherit the creator chat's history or delivery target. |
| ADR-054 | Heartbeat: 2-phase, only Phase 2 goes through the bus | `Translate` | Keep the two-phase shape: a lightweight Phase 1 decision over `HEARTBEAT.md`, then normal session ingress only when Phase 2 runs. Python-main fanout/read-only session mechanics are detailed by ADR-113; users cannot post directly into heartbeat sessions. |
| ADR-055 | Dream deferred for v1 | `Keep` | Keep Dream out of the Python server alpha and near-term milestones. No `last_dream_at`, Dream prompts, Dream cron kind, or special `ToolAllowlist` surface is required. Future memory consolidation needs its own ADR and must not reintroduce `EventKind`/`PromptMode` branches. |

## Accepted Theme 6: Rate Limits and Persistence Baseline

This theme covers ADR-056 through ADR-060. Most persistence shape decisions carry
forward, but the Rust-specific database bootstrap mechanism does not.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-056 | No rate limiting in v1 | `Translate` | Keep no product-level per-user rate-limit buckets in the bus. Provider 429s remain provider-layer errors/retries, and `llm_max_concurrent_requests` is a backend protection semaphore rather than a user-facing quota system. |
| ADR-057 | Canonical `schema.sql` loaded via `include_str!` | `Rewrite-needed` | The Rust `schema.sql` + `sqlx::include_str!` bootstrap does not carry forward. `docs/SCHEMA.md` remains the schema-shape contract, but Python-main needs a persistence/bootstrap decision before implementation chooses SQLAlchemy metadata, raw SQL bootstrap, Alembic, or another migration path. |
| ADR-058 | Every user-referencing FK has `ON DELETE CASCADE` inline | `Translate` | Keep account deletion as one database-level cascade for user-owned rows, with the documented Python-main exception that shared workspaces survive creator deletion through `workspaces.created_by ON DELETE SET NULL`. |
| ADR-059 | Messages store provider-shape content blocks as JSONB; images inline as base64 | `Translate` | Keep `messages.content` as Anthropic Messages-shaped JSONB: text, image, tool_use/tool_result, thinking, and redacted_thinking blocks. Images remain inline base64 for durable replay; workspace copies are separate agent-accessible files. |
| ADR-060 | No `users.soul`, `users.memory_text`, or user-level SSRF policy | `Translate` | Keep SOUL.md and MEMORY.md as personal workspace files, not user columns. Keep server-side SSRF policy hardcoded for server `web_fetch`; only per-device client policy is editable. |

## Accepted Theme 7: Core Tool Runtime, Schema Merge, Timeouts, and Result Caps

This theme covers ADR-071, ADR-075, ADR-076, and ADR-077. Python-main
preserves the product contract that tools merge by canonical schema, timeouts
are per-tool, result caps are character-based with per-tool override, and
each tool implements a common contract. Rust-specific mechanics translate to
Python package boundaries and Python protocol/ABC concepts.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-071 | Tools with the same name + schema are merged; `openoctopus_device` enum lists install sites | `Translate` | Keep schema merge, inject/extend `openoctopus_device`, paired-but-offline visibility. Device MCP capabilities are maintained by `register_mcp` (not synchronous device query per loop). Python-main requires stable canonicalization: normalize JSON key order, whitespace, and OpenAI-compatibility transforms. |
| ADR-075 | Tool timeouts are decentralized; agent may override where the schema advertises | `Translate` | Keep per-tool timeout ownership. Python-main adds event-loop safety: blocking tool work (file IO, hashing, recursive find_files/grep, transfer staging) must cross an explicit background/thread boundary. |
| ADR-076 | Tool result cap: 16k chars global default + per-tool override; head-only truncation | `Translate` | Keep 16,000-char global default, per-tool override, head-only truncation. Truncation helper lives in `openoctopus_common`. Python-main uses Python character counting. |
| ADR-077 | `Tool` trait pattern with default methods | `Translate` | Rust `Tool` trait translates to Python protocol/ABC concept with `name()`, `schema()`, `max_output_chars()`, `execute()`. Exact Python shape (ABC, Protocol, duck typing) chosen at Py0. Cross-cutting concerns added via default methods/mixins. |

## Accepted Theme 8: Workspace, Quota, Skills, and File Tool Semantics

This theme covers ADR-078 through ADR-088. Python-main preserves the product
contract that `workspace_fs` owns quota, skills live in personal workspace,
attachments degrade gracefully under quota lock, and file operations use
simple semantics. Rust-specific mechanics translate to Python boundaries.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-078 | Quota: one global value + workspace_fs-owned usage | `Translate` | Keep `workspace_fs` as quota/usage authority. Python-main effective defaults: personal quota 500 MiB, shared workspace quota ceiling 500 MiB. Shared usage counts only against shared quota, not personal. Shared workspace members have equal permissions (no RBAC). |
| ADR-079 | No schema-level quota counter in v1 | `Translate` | Keep no public `users.bytes_used` column. `workspace_fs` computes/caches usage internally. |
| ADR-080 | Byte-ingress attachments degrade gracefully under quota lock | `Translate` | Python-main uses best-effort per-attachment for channel byte ingress into personal server workspace. Successful attachments kept; failed ones skipped with note. No rollback. Browser refs excluded; shared workspace not default inbound target. |
| ADR-081 | No server-side `.attachments/` sweeper | `Keep` | No background cleanup, no TTL, no auto-deletion. Users clean up via UI or agent tools. Users who want automatic retention use agent + cron. |
| ADR-082 | SKILL.md format + write-time validation | `Translate` | Keep YAML frontmatter, folder-name match, write-time validation. Skills live in personal workspace `skills/*/SKILL.md`. |
| ADR-083 | Skill discovery scans exactly one level deep | `Translate` | Keep `skills/*/SKILL.md` one-level scan. Supporting files at any depth. Skills only load from personal workspace. |
| ADR-084 | Skill install paths: user browser + agent `file_transfer` | `Translate` | Keep two install paths (browser workspace write, agent `file_transfer` from paired client). No `install_skill` server tool. Skills install to personal workspace. |
| ADR-085 | Skills cache mirrors `tools_registry` | `Translate` | Keep per-user skills cache, invalidated on write/delete under `skills/`. Skills only from personal workspace. |
| ADR-086 | `delete_folder` shared tool (recursive, no flag) | `Translate` | Keep recursive delete, no flag. Allowed on locked workspaces (personal and shared) to enable quota recovery. |
| ADR-087 | `file_transfer` unified with `mode`; folder semantics are recursive | `Translate` | Python-main includes client->client bridging. All four directions active. Destination exists rejects (no overwrite flag). Partial cleanup: server-orchestrated, destination-executed, best-effort. |
| ADR-088 | `write_file` implicitly creates parent directories | `Translate` | Keep `mkdir -p` semantics on parent directory. |

## Accepted Theme 9: MCP As Dynamic Tool Surface

This theme covers ADR-099, ADR-100, ADR-105, and ADR-114. Python-main
preserves MCP as a flat tool-registry surface with wrapped names, resource
URI templates, tools-only enabled filter, and two tenancy scopes.

| ADR | Title | Python-main status | Python-main note |
|---|---|---|---|
| ADR-099 | MCP resource templates — URI placeholders are surfaced as schema properties | `Translate` | Keep URI template `{var}` -> schema property conversion. Static URIs remain zero-arg. |
| ADR-100 | MCP `enabled_tools` filter — tools only, simple string list | `Supersede` | Python-main renames `enabled` to `enabled_tools`, uses exact tool name list (not glob), applies to tools only (resources/prompts always registered). Config validation responses include `mcp_discovered` so users can see available capabilities before filtering. |
| ADR-105 | MCP subprocess lifecycle on openoctopus_client | `Translate` | Keep lifecycle model, worker queue, register_mcp as capability cache/update path. MCP subprocesses survive WS reconnect. |
| ADR-114 | Python-main MCP tenancy: admin shared-service + device only | `Translate` | Keep two tenancy scopes. Py8 one shared runtime/client per server MCP, bounded FIFO queue, no pool/per-user/session runtime. |

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

## Accepted API Theme: Cron

`docs/API.yaml`, `docs/SCHEMA.md`, `docs/DECISIONS.md`, and `docs/TOOLS.md`
are updated for Python-main to treat cron as a durable trigger that injects a
normal user message into an isolated cron session.

- `/api/cron` is the Py9 REST surface for listing, creating, updating, and
  deleting jobs. It shares the same schedule validation contract as the agent
  `cron` tool.
- `POST /api/cron` and `cron(action="add")` require `message` plus exactly one
  schedule form: `every_seconds`, `cron_expr`, or `at`. `name` and `tz` remain
  optional. `PATCH /api/cron/{id}` can update the label, message, and schedule.
- There is no cron `enabled` flag, no pause/resume endpoint, and no delivery
  switch. Jobs that should stop firing are deleted.
- `cron_jobs` stores `session_id` for the dedicated cron session and the
  scheduler-injected `message`. It does not store `channel`, `chat_id`,
  `deliver`, or `description`.
- Each job session uses `session_key = "cron:{job_id}"`. It does not inherit the
  creating chat's history or delivery target, and browser REST cannot write user
  messages into it via `POST /api/sessions/{id}/messages`.
- Cron result delivery is normal agent behavior: if a scheduled task should
  notify a user through Telegram, Discord, web, or another channel, that intent
  belongs in the scheduled message and the agent sends it with the `message`
  tool.

## Accepted API Theme: Admin

`docs/API.yaml`, `docs/SCHEMA.md`, `docs/DECISIONS.md`, and `docs/TOOLS.md`
are updated for Python-main to keep Admin API narrow: runtime product config,
basic user management, and a Py8-reserved server MCP surface.

- `GET /api/admin/config` returns only OpenOctopus-recognized LLM/quota keys.
  Unknown `system_config` keys are ignored in this API view, and
  `PATCH /api/admin/config` rejects unknown keys with `400 Bad Request`.
- Admin config keeps LLM and quota policy in the database. Object storage is
  deployment infrastructure config, supplied through environment / deployment
  secrets before server startup, and is not editable through the admin API.
- Missing quota rows use an effective 500 MiB default (`524288000`) for both
  personal workspace quota and shared-workspace quota ceiling. PATCHed quota
  values must be positive integers; `null` / empty values are invalid.
- `llm_endpoint`, `llm_api_key`, and `llm_model` remain admin API settings.
  First setup requires all three; later patches may omit unchanged values.
  Identity changes validate `/models` before any DB write. Configured
  `llm_api_key` is returned as `"<redacted>"`.
- `GET /api/admin/users` returns flat user identity fields plus personal server
  workspace `quota_bytes`, `bytes_used`, and `locked` derived through
  `workspace_fs`. There is no single-user admin GET route.
- `DELETE /api/admin/users/{id}` protects the last admin and returns
  `409 last_admin_required` when deletion would remove it.
- `/api/admin/server-mcp` remains a Py8 later route. Each configured server MCP
  gets one shared runtime/client and a bounded FIFO queue. There is no pool,
  per-user runtime, or session-scoped runtime in the Py8 contract. MCP `env` is
  returned as stored because admin is the trust boundary for that route.
