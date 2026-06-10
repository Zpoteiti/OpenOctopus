# Plexus M1 Living Design Spec

**Status:** living design; Python rewrite baseline
**Branch:** `python-main`
**Authors:** brainstormed in collaborative session 2026-05-12
**Supersedes:** none
**Last updated:** 2026-06-08

---

## 1. Purpose

M1 was the Rust server/client rebuild track. It reached a verified Client Alpha
state, then was archived because the team chose to restart implementation in
Python on `python-main` for long-term maintainability. The archived Rust branch
remains a reference implementation; this document now tracks the retained
contract and the Python rewrite baseline.

This document is a living tracker, not a full implementation plan. It records
the current M1 milestone map, dependency order, cross-cutting constraints, and
status. Each sub-milestone gets its own design sub-spec and implementation plan
before coding starts.

The milestone labels below are retained as historical contract checkpoints. New
Python work should use them as behavior/spec references, not as Rust
implementation sequencing.

---

## 2. Current Snapshot

| Field | Value |
|---|---|
| Overall M1 state | Rust M1a through M1f plus Client Alpha verified, then archived on `archive/rust-client-alpha-2026-06-05` |
| Current focus | Python rewrite ADR audit and milestone cleanup on `python-main` |
| Next implementation slice | `Py0` common-only package and contract-first tests |
| Frontend scope | Later Python track after core server/client contracts are stable |
| Client scope | Rust Client Alpha verified as reference; Python client/server runtime to be rebuilt from docs |
| Channels | Discord, Telegram, Slack, Feishu, and similar adapters are deferred to the later channels track |
| LLM credentials | Automated tests use fake Anthropic-compatible providers; real keys only for live smoke |

---

## 3. M1 Goals

Historically, M1 was the server milestone. For `python-main`, these goals are
retained as product requirements for the rewrite rather than Rust crate
requirements.

In scope:

- Python project with `plexus_common`, `plexus_server`, and `plexus_client`
  packages.
- PostgreSQL-backed persistence using the canonical schema.
- Startup DB initialization for an empty database with `CREATE TABLE IF NOT EXISTS`.
- Authentication, admin configuration APIs, and user-facing REST APIs from `docs/API.yaml`.
- Streaming `POST messages` for current-turn preview plus DB-backed
  `GET messages` recovery for browser/API sessions.
- Anthropic-compatible LLM provider layer, including admin validation and optional concurrency limiting.
- Server-side workspace/file APIs and server-side shared tools behind
  `workspace_fs`; MinIO-compatible object storage is the persistent server
  workspace layer and stays hidden behind that abstraction.
- Device token lifecycle and server-side device WebSocket protocol.
- Routing file and tool operations by `plexus_device`, with `server` as the built-in install-site name.
- Admin shared-service MCP and device MCP support.
- Cron scheduler and heartbeat delivery.
- Focused automated tests and live smoke paths.

Out of scope:

- M3 frontend implementation.
- Production-grade client hardening beyond Client Alpha.
- Discord, Telegram, Slack, Feishu, and other channel adapters.
- User-scoped server MCP.
- Session-scoped MCP.
- Server-side sandboxed code execution for users without a connected client.
- A production migration framework in M1.
- Additional LLM wire protocols beyond Anthropic Messages.
- Background job systems beyond the documented cron and heartbeat design.

---

## 4. Global Constraints

These constraints apply to every M1 sub-spec and implementation slice.

### 4.1 Contract Sources

- `docs/API.yaml` is the REST/streaming/admin contract.
- `docs/TOOLS.md` is the tool behavior contract.
- `docs/PROTOCOL.md` is the device WebSocket contract.
- `docs/SCHEMA.md` is the persistence contract.
- `docs/DECISIONS.md` is the ADR source.

Docs are rebuild specs. If implementation discovers a necessary design change,
update the relevant docs and this living tracker before treating the work as
complete.

### 4.2 Test Method

M1 uses API-first and e2e-first tests where practical. For persistence features,
tests must prove that REST writes actually land in PostgreSQL and can be read
back through the API or DB.

External services are not required for automated tests:

- LLM tests use a fake Anthropic-compatible HTTP service after M1f. Older M1b-M1d tests used the OpenAI-compatible bootstrap fake.
- Channel adapters use local adapter-level tests until live smoke when that
  later track begins.
- Device WebSocket tests may use an in-process test client.
- MCP tests use fake/local MCP servers where practical.

Real LLM credentials and real channel tokens are only used for live smoke after
the relevant implementation slice is complete.

### 4.3 Persistence

M1 does not introduce a migration framework. On server startup, if the database
has no usable tables, the server applies the canonical schema with
`CREATE TABLE IF NOT EXISTS`.

Schema drift is handled during development by updating `docs/SCHEMA.md` and the
canonical schema together. A migration framework can be revisited after M1 if
the project needs upgrade-in-place production releases.

### 4.4 Device Names and Routing

The literal device name `server` is reserved for the built-in install-site name.
Users cannot create or rename a device to `server`.

File REST APIs and file tools require an explicit `plexus_device`. M1d accepts
only `server`, the built-in server workspace install site; missing values and
non-server values fail validation. Client-device file operations are future
M1f work: routing will be based on automatically detected install sites, will
dispatch over device WebSocket, and will obey the target device's
`workspace_path` and `sandbox_mode`. Offline target devices return
`device_unreachable`.

Workspace transfer keeps explicit source/destination device fields:
`plexus_src_device` and `plexus_dst_device`.

### 4.5 LLM Provider

All LLM requests go through the shared provider layer. Bootstrap does not seed a
concurrency config row. If `llm_max_concurrent_requests` is missing at startup,
only the runtime limiter treats it as `0` (unlimited); admins may configure a
provider-level semaphore for weaker providers or installations without a
gateway.

When an admin changes LLM endpoint, API key, or model, the server validates the
configuration before writing it to the database:

- `GET {llm_endpoint}/models`
- reject unreachable endpoints
- reject unauthorized responses
- reject malformed model responses
- reject when configured `llm_model` is absent

### 4.6 MCP Tenancy

M1 supports only two MCP scopes:

- Admin shared-service MCP: configured by admins, shared credentials, available
  through `plexus_device="server"`, intended for stateless or low-state service
  tools.
- Device MCP: configured on a user device row, runs on that device, available
  through `plexus_device="<device-name>"`.

M1 explicitly excludes user-scoped server MCP and session-scoped MCP.

Admin shared-service MCP uses one runtime per configured MCP server with a
bounded per-MCP queue. There is no `pool_size` field in the M1 config contract.

### 4.7 Cron and Heartbeat

Cron writes can enter only through the agent tool path or the user REST API.
Both paths must share one write helper that validates the job and notifies the
ticker.

Cron delivery goes to the session that created the cron job. Heartbeat delivery
goes to a dedicated read-only heartbeat session for the user.

Cron drift should be rejected at write time where possible. The scheduler cap
scan covers all users and therefore uses a conservative interval rather than a
per-user fast loop.

---

## 5. Sub-Milestone Map

This is the initial M1 sequence plus the post-M1f roadmap re-cut. It is expected
to evolve as implementation dependencies become clearer. The first map already
applied one dependency-driven re-cut: the LLM provider foundation came before
browser chat because the chat path needed a provider contract. The 2026-05-27
re-cut moves a real client alpha ahead of channel adapters and frontend work so
the distributed-agent value loop is exercised before polishing ingress and UI.

| ID | Status | Scope | Depends on | Exit criteria |
|---|---|---|---|---|
| `M1a` | Verified | Server crate, startup, DB bootstrap, canonical schema application, real auth, basic REST/admin persistence, test harness | M0 | Verified by `cargo test -p plexus-server`, `cargo test --workspace`, `cargo fmt --all -- --check`, and `cargo check --workspace` |
| `M1b` | Verified | OpenAI-compatible LLM foundation, admin config validation, external FastAPI mock service, hermetic fake-provider test strategy, concurrency semaphore | `M1a` | Verified on 2026-05-13 from branch `rebuild-m1-M1b`: invalid provider config is rejected before DB write, valid fake provider completes non-streaming external call mechanics, sibling mock tests pass |
| `M1c` | Verified | Browser chat path: UUID-addressed web sessions, editable titles, REST message ingress, session storage, SSE history replay/live stream, minimal SOUL/MEMORY prompt, inline content-block images, fake LLM-backed response loop, and required manual live smoke | `M1a`, `M1b` | Verified on 2026-05-14: automated checks passed and a real MiniMax provider smoke validated admin LLM config, SSE user/assistant flow, persisted history, and replay |
| `M1d` | Verified | Server workspace/file REST APIs, strict message attachment refs, server-side workspace FS, quota enforcement, server-side shared file tools, merge-v0 `plexus_device=server` schemas | `M1a`, `M1c` | Verified on 2026-05-18: full workspace/all-targets tests passed against local PostgreSQL; focused M1d tests cover REST, message attachments, workspace FS, and tool schemas/execution |
| `M1e` | Verified | Device token lifecycle, device naming rules, device WebSocket handshake/control frames | `M1a` | Verified on 2026-05-25: device rows, token lifecycle, WS handshake, config push, heartbeat, and online registry behavior passed focused and full server tests |
| `M1f` | Verified | Anthropic Messages provider pivot, full tool execution loop, device-routed file/tool execution over WS, offline handling, multimodal `read_file`, cancel/restart repair, transfer plumbing | `M1d`, `M1e` | Verified on 2026-05-27: Anthropic Messages path, agent loop, device-routed execution, remote attachment handling, restart/cancel repair, provider fallback, and file transfer tests passed and merged to `rebuild-m1` |
| `Client Alpha` | Verified | Minimal real `plexus-client`: env-only startup, device-token connection, `hello`/`hello_ack`, heartbeat/reconnect, server-pushed config, FIFO worker queue, shared file tools, `web_fetch`, one-shot `exec`, and server/client file transfer | `M1e`, `M1f` | Verified on 2026-06-04: real-client API e2e covers file tools, one-shot exec, and bidirectional file transfer through the normal agent loop; focused client/common/server tests, workspace check, fmt, diff hygiene, and clippy passed |
| `Py-Prep` | In progress | Review old ADRs/docs, remove stale Rust-era cognitive load, split oversized decisions by theme, and lock the Python rewrite sequence | Retained docs, archived Rust Client Alpha | ADR audit documents the Python-main status of old decisions; docs no longer describe server runtime work as `Py0` |
| `Py0` | Planned | Common-only Python package: `plexus_common` DTOs, base types, error codes, API/protocol/tool/provider contracts, path/workspace refs, schema helpers, and documented DB/storage choices | `Py-Prep` | Contract-first tests import `plexus_common`, validate representative API/protocol/tool schemas, and do not require a running FastAPI app |
| `Py1` | Planned | Server foundation: FastAPI app, SQLAlchemy/PostgreSQL, auth, registration/login, JWT/cookie/bearer auth, bootstrap, config/admin routes, and one-ASGI-worker runtime shell | `Py0` | Auth/admin/config tests pass against Postgres; the app starts without agent-loop, client, or workspace execution semantics |
| `Py2` | Planned | Single-turn Anthropic Messages chat: `POST/GET messages`, Postgres transcript integration, Anthropic SDK adapter, admin-configured LLM semaphore, and fake-provider tests | `Py1` | A browser/API message can create a session, call a fake Anthropic-compatible provider once, persist the complete assistant response, and recover through `GET messages` |
| `Py3` | Planned | Hand-written agent loop and server tools: tool-use iterations, JIT tool-result collapsing, best-effort token preview, cancel/restart repair, `web_fetch`, `message`, and account/context helpers | `Py2` | Fake-provider agent-loop tests cover tool calls, token preview events, pending-message drain, cancellation, and restart repair without LangChain/LangGraph |
| `Py4` | Planned | Workspace files: `workspace_fs`, file REST APIs/tools, quota, grep/glob/list/read/write/edit/delete, transfer basics, and MinIO-compatible object storage as the persistent server file layer | `Py3` | File API/tool tests pass through `workspace_fs`; APIs/tools never access MinIO directly, and server disk is used only for temporary staging/materialization |
| `Py5` | Planned | Client Alpha: Python client WebSocket runtime, device token connect/reconnect, config push, shared tools, `web_fetch`, and agent access to client files | `Py4` | Real Python client e2e proves server agent can read/write/list/fetch through a paired client and returns `device_unreachable` for paired offline targets |
| `Py6` | Planned | Better client shell: persistent shell sessions, reconnect behavior, diagnostics, and stronger execution ergonomics | `Py5` | Shell tests cover session continuity, reconnect, timeout/cancel behavior, and no event-loop starvation |
| `Py7` | Planned | Client sandbox and client-side MCP | `Py6` | Client-side file/subprocess jail behavior and fake MCP registration/execution pass on supported platforms |
| `Py8` | Planned | Server sandbox and server-side MCP | `Py7` | Server-side sandbox/MCP has an explicit security design; fake admin shared-service MCP exposes tools through `server` with collision and failure behavior covered |
| `Py9` | Planned | Cron/heartbeat autonomous message injection | `Py3` | Cron fires into creator sessions; heartbeat posts to read-only heartbeat sessions; both reuse the normal session/agent path |
| `Py10` | Planned | Channels: Discord, Telegram, Feishu, Slack-like adapters, and channel-level event aggregation | `Py3`, `Py9` | Channel adapters are thin ingress/delivery layers over sessions/messages; token deltas/tool events are aggregated per channel capability |
| `Py11` | Planned | Memory/Dream-style consolidation | `Py3`, `Py4` | Memory consolidation has its own ADR before implementation and does not reintroduce old `EventKind`/`PromptMode` branches |
| `Py12+` | Deferred | Frontend, packaging, hardening, scale-out/Redis if proven necessary, extra channels, deeper MCP, and later server expansion | Core server/client loop | Later tracks do not change the established provider transcript, tool, workspace, or session contracts without new ADRs |

---

## 6. Sub-Spec Contract

Each M1 sub-spec must include:

- Goal and non-goals.
- API endpoints, tool contracts, or protocol frames touched.
- Data model changes and persistence behavior.
- Runtime components and ownership boundaries.
- Error handling and error codes.
- Test plan with automated tests first.
- Live smoke needs, if any.
- Exit criteria.
- Docs that must be updated after implementation.

Each sub-spec should be small enough to implement and verify in one focused
branch segment.

---

## 7. Status Rules

Allowed sub-milestone statuses:

- `Planned`: not designed in detail yet.
- `Designing`: sub-spec in progress.
- `Approved`: sub-spec approved; implementation plan next.
- `Implementing`: code in progress.
- `Implemented`: code and docs are complete enough for focused compile checks, but final runtime verification or smoke is still pending.
- `Verified`: automated checks and relevant smoke checks passed.
- `Blocked`: waiting on user input, credentials, dependency, or design decision.
- `Deferred`: intentionally moved out of the current sequence.

After each sub-milestone, update this document with:

- final status
- important deviations from the original plan
- test evidence
- follow-up work
- docs sync status

---

## 8. Current M1 Status

`M1a` is verified. The server crate exists, empty PostgreSQL databases bootstrap
from the canonical schema, real auth works through REST, and the M1a admin
config subset persists to `system_config`.

`M1b` is verified. Its scope was the LLM identity validation and external-call
foundation only. M1f supersedes the bootstrap OpenAI-compatible provider wire
with Anthropic Messages, but keeps the "one provider wire format, no provider
trait" principle.

External OpenAI, Gemini, local-model, or other non-Anthropic-native services
must be reached through an external Anthropic-compatible gateway if needed.
Automated Rust tests use a hermetic in-process fake provider. The sibling
`../Plexus-mock-llm` FastAPI service is only for local/manual smoke testing.

M1a verification evidence:

- `cargo test -p plexus-server -- --nocapture`
- `cargo test --workspace`
- `cargo fmt --all -- --check`
- `cargo check --workspace`
- schema/docs consistency search for canonical tables, indexes, `server_mcp`,
  `llm_max_concurrent_requests`, and reserved `server` device-name constraint

M1b verification evidence from 2026-05-13 on branch `rebuild-m1-M1b`:

- `rtk git status --short`
- `rtk cargo fmt --all -- --check`
- `rtk cargo clippy --workspace --all-targets -- -D warnings`
- `rtk bash scripts/reset-postgres18-and-test.sh`
- `rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test --workspace --all-targets`
- `rtk conda run -n Plexus pytest -q` in `../Plexus-mock-llm`
- `rtk git diff --check`
- `docs/API.yaml` validated with `ruamel.yaml`

PostgreSQL 18 verification used container `plexus` from `pgvector/pgvector:pg18`.
After both PostgreSQL-backed test runs, only the persistent `plexus` database
matched `plexus%`, and the persistent `plexus.public` schema contained no
tables. No test tables or rows landed in the persistent database.

M1c automated verification evidence from 2026-05-14 on branch
`rebuild-m1-M1c`:

- `rtk cargo fmt --all -- --check`
- `rtk cargo clippy --workspace --all-targets -- -D warnings`
- `rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test --workspace --all-targets`
- `rtk conda run -n Plexus python -c "import yaml, pathlib; yaml.safe_load(pathlib.Path('docs/API.yaml').read_text()); print('API.yaml ok')"`
- `git diff --check`

M1c live-smoke verification from 2026-05-14 used a temporary Plexus server,
isolated PostgreSQL database, and MiniMax OpenAI-compatible provider. That
provider wire was the M1b-M1d bootstrap shape and is superseded by M1f. The smoke
validated admin `PATCH /api/admin/config` provider validation, browser session
creation, `GET /api/sessions/{id}/stream` history cut-over, text message POST,
live SSE user and assistant messages, persisted history containing both rows,
SSE replay of persisted rows, and persisted assistant `reasoning_content`
(removed by the later M1f Anthropic Messages pivot).
Review hardening on 2026-05-14 additionally covered browser `web:` namespace
writability, SSE replay/live de-duplication, SSE lag reconnect behavior, and
serialized worker wake/progress races. Follow-up hardening on 2026-05-15 moved
mid-response browser posts into durable `pending_messages`, drained them at
safe boundaries, and added startup recovery for queued rows.

M1d verification status from 2026-05-18 on branch `rebuild-m1-M1d`:

- `workspace_fs` now owns server workspace path resolution, quota checks,
  serialized mutating operations, read/write/edit/delete/delete_folder/list/glob
  and grep.
- Workspace REST file routes require explicit `plexus_device=server`; there is
  no REST default in M1d.
- Browser message writes used the strict M1d `{ reasoning_effort, content,
  attachments }` shape, with required `content` and `attachments` arrays and no
  string shorthand. M1f replaces `reasoning_effort` with `effort`.
- Browser attachment refs point at existing workspace files and are validated
  before persistence. Image refs expand into marker + base64 image blocks, with
  decoded-byte dedupe against direct inline image blocks.
- Shared file tool source schemas remain device-free. The server registry
  injects required `plexus_device` enum `["server"]` and dispatches server file
  tools through `workspace_fs`.
- Verification used the existing local PostgreSQL test container `plexus` and
  passed:
  - `rtk cargo fmt --check`
  - `rtk cargo clippy -p plexus-common -p plexus-server --all-targets -- -D warnings`
  - `rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test --workspace --all-targets`
  - `rtk conda run -n Plexus python -c "import yaml, pathlib; yaml.safe_load(pathlib.Path('docs/API.yaml').read_text()); print('API.yaml ok')"`
  - targeted stale M1d wording scan over `docs/API.yaml`, `docs/TOOLS.md`,
    `docs/DECISIONS.md`, and the M1/M1d specs
  - `rtk git diff --check`
