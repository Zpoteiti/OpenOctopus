# OpenOctopus — Database Schema

The PostgreSQL schema contract for `openoctopus_server`. During Py-Prep this doc
is the canonical reference for table, column, index, constraint, and storage
semantics. Python bootstrap uses SQLAlchemy declarative models/metadata with
`create_all()`; Alembic or equivalent migration framework is deferred until
production launch after frontend completion (ADR-057, ADR-069).

**Thirteen tables.** Account deletion fences affected workspaces, then commits
the user deletion and durable object-cleanup intents together. RustFS purge is
idempotent and may finish after that logical deletion. Every user-referencing
FK has `ON DELETE CASCADE` defined inline (ADR-058), with one explicit exception
in `workspaces.created_by` (`ON DELETE SET NULL`, see ADR-108) so a workspace
persists for its remaining members when the creator's account is removed.

This doc is the canonical reference for the schema's *shape*. When Python
implementation SQL or ORM metadata exists, it must be kept in sync with this
contract; if they disagree, update the implementation or this spec deliberately
instead of treating drift as incidental.

---

## 1. `system_config` — global key-value store

```sql
CREATE TABLE IF NOT EXISTS system_config (
    key         TEXT        PRIMARY KEY,
    value       JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Python-main admin-editable keys (`PATCH /api/admin/config` accepts only these keys;
unsupported keys return `400 Bad Request`):

| Key | Type | ADR | Purpose |
|---|---|---|---|
| `quota_bytes` | int | ADR-046 | Per-user workspace quota. Missing means the effective default is 500 MiB (`524288000`). |
| `shared_workspace_quota_bytes` | int | ADR-108 | Quota ceiling that any single shared workspace may request at create or rename time. Missing means the effective default is 500 MiB (`524288000`). |
| `llm_endpoint` | string | ADR-101 | Unversioned base URL of an Anthropic-compatible Messages API; do not include `/v1`. |
| `llm_api_key` | string | ADR-101 | Bearer credential for outbound LLM calls; redacted in admin API responses. |
| `llm_model` | string | ADR-101 | Model name passed in the Anthropic Messages request body. |
| `llm_max_context_tokens` | int | ADR-101 | LLM context window in tokens (e.g. `128000` for gpt-4o). Counted with the configured-model Python tokenizer strategy (ADR-025, ADR-101). |
| `llm_max_output_tokens` | int | ADR-101, ADR-125 | Maximum output tokens passed to Anthropic Messages. Missing means the effective default is 16384. Admin-editable; changes apply to the next provider turn, not an already-running request. |
| `llm_compaction_threshold_tokens` | int | ADR-028, ADR-101, ADR-126 | Py3 compaction headroom trigger. Missing disables compaction; when configured, `llm_max_context_tokens` is required and the value must be `4001 <= threshold < max_context_tokens`. |
| `llm_max_concurrent_requests` | int | ADR-101 | Optional in-process semaphore for outbound LLM calls. A configured `0` means unlimited and creates no semaphore. A positive integer caps concurrent in-flight LLM calls; negative values and values above the server maximum are invalid. If missing at server startup, only the runtime limiter treats it as `0`; no row is persisted. |

Reserved/future known keys (not PATCH-editable until their milestone):

| Key | Type | ADR | Purpose |
|---|---|---|---|
| `server_mcp` | array of `McpServerConfig` | ADR-114 | Admin-configured shared-service MCPs exposed as install site `server`; shared credentials, one runtime per MCP, bounded queue. |

Bootstrap does not seed `system_config` rows. Fresh `GET /api/admin/config`
therefore returns the effective quota defaults and
`llm_max_output_tokens=16384`, while omitting the unconfigured LLM identity and
context/compaction/concurrency keys until an admin writes values. Deployments
may carry additional opaque keys inserted outside the admin API; OpenOctopus
ignores them in the admin config view. `PATCH /api/admin/config` rejects keys
outside the admin-editable table above, including `server_mcp` and
`object_storage_*`.

Python-main accepts `llm_endpoint`, `llm_api_key`, and `llm_model` only after provider
validation succeeds. First setup must provide all three identity values; later
PATCHes may reuse stored values by omitting unchanged identity keys. Validation
uses `GET {llm_endpoint}/v1/models` before any DB write, so failed identity
changes do not persist paired config updates. `llm_api_key` is stored in
`system_config` for outbound calls but redacted as `"<redacted>"` in admin
config read and patch responses. Sending the literal redaction marker as a new
key is rejected. `server_mcp` is documented for the Py8 MCP scope and is not
accepted by the admin-config endpoint. Object storage is deployment
infrastructure config supplied through environment / deployment secrets, not
`system_config`.

`llm_max_output_tokens` is validated as an integer in `1..1_000_000`. When
`llm_max_context_tokens` is configured, a merged config update must also keep
`llm_max_output_tokens <= llm_max_context_tokens`. The output budget includes
thinking and visible output and does not expand the context window.

---

## 2. `users` — OpenOctopus accounts

```sql
CREATE TABLE IF NOT EXISTS users (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    email          TEXT         NOT NULL UNIQUE,
    password_hash  TEXT         NOT NULL,
    name           TEXT         NOT NULL,
    is_admin       BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

- `password_hash` — argon2 (or bcrypt — implementer's choice within reason). Never returned by any API.
- `is_admin` — true for any user who registered with the `OPENOCTOPUS_ADMIN_TOKEN`. Admin APIs protect the last remaining admin from deletion.
- **No `soul`, `memory_text`, or user-level SSRF policy columns** — workspace-file-only per ADR-060.
- **No inline channel fields** — Discord/Telegram live in their own tables (ADR-090).
- **No `bytes_used` column** — workspace usage is computed on demand by `workspace_fs` summing paged RustFS object metadata under the workspace prefix.

---

## 3. `discord_configs` — per-user Discord bot integration

```sql
CREATE TABLE IF NOT EXISTS discord_configs (
    user_id          UUID         PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    bot_token        TEXT         NOT NULL,
    partner_chat_id  TEXT         NOT NULL,
    allow_list       JSONB        NOT NULL DEFAULT '[]'::jsonb,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

- `user_id` is both PK and FK — at most one Discord config per user.
- `bot_token` is the Discord bot's secret. API never returns it; `GET /api/channels` returns only `bot_token_hint`, computed from the first/last visible characters by the shared secret-redaction helper. The hint is display-only and is never accepted for authentication, lookup, or update.
- `partner_chat_id` is the partner human's Discord user ID. Messages from this ID are *not* wrapped (`[untrusted message from <name>]:`); messages from anyone else are (ADR-007).
- `allow_list` — JSONB array of heterogeneous Discord identifiers the partner has authorized to also reach the bot. Each entry is one of:
    - **User ID** (e.g. `"123456789012345678"`) — the named user is allowed to DM the bot or @-mention it in any channel.
    - **Channel ID** — every member of that channel can @-mention the bot in that channel.
    - **Guild ID** — every member of that guild can @-mention the bot in any of its channels.
  Inbound message is allowed if its sender_id matches a User ID entry **OR** its message-context (channel, guild) matches a Channel/Guild ID entry. Allowed messages still get the `[untrusted message from <name>]:` wrap (ADR-007); only the partner is unwrapped. Agent treats allow-list senders as non-partner allowed users (see ADR-074 trust model). Format is positional — entries are stored verbatim as Discord-snowflake-shaped strings; the adapter classifies (user/channel/guild) by API lookup at receive time, not by string form.

---

## 4. `telegram_configs` — per-user Telegram bot integration

```sql
CREATE TABLE IF NOT EXISTS telegram_configs (
    user_id          UUID         PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    bot_token        TEXT         NOT NULL,
    partner_chat_id  TEXT         NOT NULL,
    allow_list       JSONB        NOT NULL DEFAULT '[]'::jsonb,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

Symmetric to `discord_configs`: at most one Telegram config per user, and config
existence means enabled. `GET /api/channels` returns `bot_token_hint`, never the
raw `bot_token`. `allow_list` follows the same heterogeneous-identifier rule
(Telegram terminology):
- **User ID** — the named user can DM the bot.
- **Chat ID** of a group — every member of that group can @-mention the bot in the group.
- **Channel ID** — broadcast-channel admins can post; allowed bot interactions follow Telegram's bot-in-channel API rules.

Match logic identical to Discord — sender_id ∪ chat-context-id checked against the list; allowed messages get the untrusted wrap.

Adding a future channel = adding a `<channel>_configs` table; no `users` migration (ADR-090).

---

## 5. `sessions` — chat sessions per channel-conversation

```sql
CREATE TABLE IF NOT EXISTS sessions (
    id                UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_key       TEXT         NOT NULL,
    channel           TEXT         NOT NULL,
    chat_id           TEXT         NOT NULL,
    title             TEXT         NOT NULL DEFAULT 'New chat',
    last_inbound_at   TIMESTAMPTZ,
    last_read_at      TIMESTAMPTZ,
    cancel_requested  BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_user_session_key ON sessions(user_id, session_key);
```

- `session_key` is the composite identity from ADR-006 — `{channel}:{chat_id}` for external channels, an override (`cron:{job_id}`, `heartbeat:{user_id}`, `web:{id}`) for internal/web sessions. It is unique per user, not globally unique.
- `id` is the internal UUID used as FK target by `messages.session_id` and the browser REST path identifier. Most internal code uses `id`; channel adapters may look up by `(user_id, session_key)`.
- Browser web session rows are created implicitly by `POST /api/sessions/{id}/messages` when the client-generated UUID does not yet exist. Python-main has no separate `POST /api/sessions` create route.
- `title` is the human-facing mutable session name. It defaults to `"New chat"` and never affects `id`, `chat_id`, or `session_key`.
- `last_inbound_at` — bumped on every new InboundMessage; powers session-list ordering in the UI.
- `last_read_at` — browser inbox read marker. Updated by `PATCH /api/sessions/{id}` with `read_through_message_id`; the update sets the marker to the greater of the current value and the target canonical message's `created_at`. `GET /api/sessions` derives `unread` by checking for user-visible messages newer than this timestamp. `GET /api/sessions/{id}/messages` does not mutate this marker, so prefetching and polling do not accidentally mark a session as read.
- `cancel_requested` — set true by `POST /api/sessions/{id}/cancel` only when a runner is active (ADR-035), observed at the next safe boundary, then cleared. Cancel on an idle session is a no-op and must not leave this flag true. If an in-flight provider request returns a final response with no tools, normal completion wins and clears the flag without a stop marker (ADR-129).
- `DELETE /api/sessions/{id}` removes session rows after terminating any in-memory runner/streams. `ON DELETE CASCADE` removes that session's `turn_runs`, `messages`, and `pending_messages`; channel configuration rows are not tied to session deletion. Active cron sessions are rejected by the FK from `cron_jobs.session_id`; delete the cron job through `/api/cron/{id}` so the job row and its dedicated history stay consistent. Completed one-shot cron sessions with no remaining `cron_jobs` row can be deleted as normal history.

---

## 6. `messages` — persisted transcript rows

```sql
CREATE TABLE IF NOT EXISTS messages (
    id                       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id               UUID         NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    message_kind             TEXT         NOT NULL CHECK (message_kind IN (
                                 'human',
                                 'assistant',
                                 'tool_result',
                                 'synthetic_tool_result',
                                 'synthetic_assistant_error',
                                 'compaction_summary'
                             )),
    content                  JSONB        NOT NULL,
    delivery_refs            JSONB        NOT NULL DEFAULT '[]'::jsonb,
    llm_fingerprint          TEXT,
    is_compacted             BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_session_created
    ON messages(session_id, created_at);
```

- `content` — JSONB array of Anthropic Messages content blocks (ADR-059, ADR-101, ADR-117). Block shapes mirror what the LLM will receive after provider-layer projection, except optional OpenOctopus `tool_result.code` metadata is retained for storage/public diagnostics and stripped from the strict provider request. Supported persisted block types are `text`, `image`, `tool_use`, `tool_result`, `thinking`, and `redacted_thinking`. **Images** are stored as Anthropic `image` blocks with base64 data inline. **Tool results** store `content` as a safe block array; real tool output starts with the server-generated untrusted-result warning block, while server-authored synthetic tool results use the same array shape for diagnostic text. **Non-image files** (PDFs, CSVs, audio, ...) live only in workspaces; the DB carries path-text markers and the agent reaches bytes via `read_file`. Remote attachment runtime failures are also persisted as server-authored text marker blocks so the user message is not lost.
- `delivery_refs` — JSONB array of user-visible file delivery references for channel adapters, ignored by provider replay. The web `message(media=...)` tool uses this sidecar for file chips/download links so `messages.content` can stay Anthropic-compatible. Each ref links to its generating `tool_use_id`. Server workspace refs are durable and retain the virtual path plus immutable workspace ID/workspace-relative path so a frontend can recover from a shared-workspace rename. Device refs are online-only pointers to immutable `(device_id, captured device name, path)` identity; a later relay must reject rename/delete/name-reuse drift instead of reading from another device. Device refs do not cause a device read or RustFS write when the message is sent.
- `llm_fingerprint` — nullable model/provider fingerprint for assistant rows that contain opaque thinking state. Provider replay may use raw `thinking` / `redacted_thinking` blocks only when this matches the current compatible model segment.
- `message_kind` — stored OpenOctopus semantic discriminator: `human` for external/user-marker rows, `assistant` for normal provider responses, `tool_result` for real server/device tool results, `synthetic_tool_result` for restart/cancel/unreachable repair rows, `synthetic_assistant_error` for exhausted provider failures, or `compaction_summary` for provider-compatible summary rows. Provider projection derives `role='user'` for `human`, `tool_result`, and `synthetic_tool_result`; it derives `role='assistant'` for the other three kinds. This avoids JSONB inspection and prevents invalid stored role/kind pairs (ADR-089, ADR-126).
- `is_compacted` — provider/compaction context membership. `FALSE` means the row participates in normal provider replay and may be input to a later compaction. `TRUE` means the row remains available for canonical history/audit but is excluded from both. A `compaction_summary` begins `FALSE` and may later become `TRUE` when absorbed into a newer summary.
- The `idx_messages_session_created` index powers the `GET /api/sessions/{id}/messages` cursor scan.
- Runtime block (ADR-094) is prepended into the user-row's `content` JSONB at
  ingress time; not a separate column. Public history projection inspects only
  the first text block, validates the exact anchored server grammar and matching
  session `channel`/`chat_id`, and omits that block from the DTO. Stored content
  and provider replay remain unchanged.

---

## 7. `pending_messages` — durable inbound waiting for safe boundary

```sql
CREATE TABLE IF NOT EXISTS pending_messages (
    id                UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id        UUID         NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    user_id           UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_key       TEXT         NOT NULL,
    content           JSONB        NOT NULL,
    effort            TEXT         CHECK (
                          effort IS NULL
                          OR effort IN ('off', 'low', 'medium', 'high', 'xhigh', 'max')
                      ),
    received_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pending_messages_session_received
    ON pending_messages(session_id, received_at, id);
CREATE INDEX IF NOT EXISTS idx_pending_messages_session_key_received
    ON pending_messages(session_key, received_at, id);
```

- `pending_messages` stores inbound user messages that are durable but not yet provider-visible. Py2 uses it for messages arriving while a worker is active. Beginning with Py3, every inbound user message is staged here first so Stage 1 compaction can place its replacement summary before the new user batch (ADR-126). Before preflight, a continuation with no captured input fixes the current ordered pending IDs as its boundary; token counting and promotion consume only that prefix, while later arrivals remain pending for the following provider call. Browser HTTP stream ownership is not stored here: queued subscribers remain bound in memory by message ID until a boundary is captured, then the newest subscriber within that captured batch becomes its live preview owner. Older subscribers from that batch can close with `stream_replaced` while their rows remain durable; subscribers for later arrivals remain queued. A delayed registration attaches only when its message ID still belongs to the active preview batch; otherwise the transient stream closes and recovers through the canonical GET surface. `effort` is nullable; `NULL` and `off` send `thinking.type=disabled`; non-off values send `thinking.type=adaptive` plus Anthropic `output_config.effort`.
- `session_key` is stored alongside `session_id` so channel/session routing can recover pending work without recomputing the key.
- At the safe boundary, the worker captures the current rows for the session
  and drains that fixed prefix in one DB transaction: select them in
  `(received_at, id)` order, insert them
  into `messages` with the same `id` and `message_kind='human'`, delete the
  selected pending rows, commit, then the resulting user messages become
  visible to canonical history and the latest live POST preview stream.
  Messages accepted after capture remain pending for the following call.
  Canonical `messages.created_at` values are assigned at drain time in that
  same order; copying the earlier `received_at` would incorrectly place queued
  input before the assistant response whose safe boundary it waited for. In
  tool-less Py2, the boundary follows persistence of the current complete
  assistant response and terminal run state. From Py3 onward, the current
  assistant tool batch must also be fully addressed (ADR-034, ADR-125).
  When an idle Py3 runner finds no Stage 1 work, it performs this promotion
  immediately. When Stage 1 triggers, summary generation runs while the new
  user batch remains pending; the commit marks all selected active source rows
  `is_compacted=TRUE`, inserts the new active summary, and only then promotes
  the pending rows. Canonical active order is `S1, U11` without backdating.
  `GET /api/sessions/{id}/messages` returns these rows separately as
  `pending_messages` until they drain; its public projection removes a valid
  server-generated first runtime block without mutating the row. The stable
  `id` lets the frontend reconcile the pending item with the eventual canonical
  message. When neither a session run nor boundary compaction is in flight,
  this table should normally be empty.
- A process restart drops live token previews and in-memory stream subscribers.
  Startup reconciles only stale `turn_runs` lifecycle rows; it does not drain or
  reorder `pending_messages`. The next inbound POST/channel activity rebuilds
  from PostgreSQL and drains durable pending rows at the next safe boundary. If
  there is no running turn, that new inbound is first inserted as pending and
  drained with the older rows in `(received_at, id)` order before the recovered
  provider turn starts; it must not leapfrog the surviving queue.

---

## 8. `turn_runs` — durable agent provider-call/tool-batch lifecycle

```sql
CREATE TABLE IF NOT EXISTS turn_runs (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id          UUID         NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    runner_instance_id  UUID         NOT NULL,
    status              TEXT         NOT NULL CHECK (
                                      status IN (
                                          'running',
                                          'completed',
                                          'failed',
                                          'abandoned',
                                          'cancelled'
                                      )
                                  ),
    started_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_turn_runs_one_running_per_session
    ON turn_runs(session_id)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_turn_runs_session_started
    ON turn_runs(session_id, started_at DESC, id DESC);
```

- `id` is the `turn_id` emitted by `turn_started`, `token_delta`,
  `message_persisted`, and `turn_finished`.
- A runner inserts `status='running'` before each normal agent-loop provider
  call and `turn_started`. The same row covers persistence of that call's
  complete assistant response and, when it contains `tool_use`, the complete
  following serial tool-result batch. It reaches `completed` only after that
  batch is fully addressed; exhausted provider failure updates it to `failed`;
  safe-boundary cancellation updates it to `cancelled`. The next provider call
  in the same ReAct chain creates another row and public `turn_id`, so one
  external user request can produce several run rows (ADR-126).
- The partial unique index is the durable backstop for one active
  provider-call/tool-batch run per session. The in-memory reservation remains
  the fast scheduler path.
- The server process creates one boot-scoped `runner_instance_id`. During
  startup, before accepting traffic, it performs one indexed reconciliation
  update that marks leftover `status='running'` rows from the prior process as
  `abandoned` and sets `finished_at`. Partial tokens were never durable; pending
  user rows remain available for the next normal recovery/drain.
- `GET /api/sessions/{id}/messages` derives its public status from the latest
  run: `running`/`failed`/`abandoned` map directly; no run, `completed`, or
  `cancelled` maps to `idle`. `active_turn_id` is present only for `running`.
- Run rows are lifecycle/audit state, not provider messages. They are never
  projected into Anthropic history.

---

## 9. `devices` — per-user client devices

```sql
CREATE TABLE IF NOT EXISTS devices (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id            UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name               TEXT         NOT NULL CHECK (char_length(name) <= 64 AND name ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND name <> 'server'),
    workspace_path     TEXT         NOT NULL,
    sandbox_mode       BOOLEAN      NOT NULL DEFAULT TRUE,
    shell_timeout_max  INTEGER      NOT NULL DEFAULT 600 CHECK (shell_timeout_max BETWEEN 0 AND 86400),
    ssrf_denylist      JSONB        NOT NULL DEFAULT
        '["0.0.0.0/8","127.0.0.0/8","224.0.0.0/4","240.0.0.0/4","::/128","::1/128","10.0.0.0/8","172.16.0.0/12","192.168.0.0/16","100.64.0.0/10","169.254.0.0/16","169.254.169.254/32","fc00::/7","fe80::/10","ff00::/8"]'::jsonb,
    env_allowlist      JSONB        NOT NULL DEFAULT
        '["PATH","HOME","LANG","TERM","SystemRoot","ComSpec","PATHEXT","TEMP","TMP","USERPROFILE"]'::jsonb,
    token_hash         BYTEA        NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    token_hint         TEXT         NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_devices_user_id ON devices(user_id);
```

- `id` is the immutable internal device identity. `token_hash` is the unique credential lookup key (ADR-131), stored as the 32-byte SHA-256 digest of the bearer token; the server never stores plaintext. `token_hint` is the non-secret prefix/suffix hint generated at issuance; REST returns the plaintext token only from create/regenerate responses.
- `name` is the REST/tool-routing canonical slug. It is UNIQUE per user, so the URL `PATCH /api/devices/laptop/config` resolves to `(user_id, "laptop")` without ever touching the token. Raw create/rename input is canonicalized server-side: NFC normalize, trim, ASCII-lowercase, convert whitespace runs to a single hyphen, then require `^[a-z0-9]+(-[a-z0-9]+)*$`. Stored names are at most 64 characters and use only lowercase ASCII letters, digits, and hyphens. The literal name `server` is reserved for OpenOctopus's built-in server install site and is rejected for user devices after canonicalization.
- `sandbox_mode` — the per-device privilege switch. `true` is the default restricted profile: client file tools and Workspace Files REST routes must stay inside `workspace_path`, and client `web_fetch` applies `ssrf_denylist`. `false` is the trusted-device profile: client file tools may address paths outside `workspace_path`, and internal/private network access is allowed unless the user keeps explicit deny entries. This is a persisted device property; sessions cannot temporarily override it.
- `ssrf_denylist` — JSONB array of CIDRs, hosts, or `host:port` entries rejected by client-site `web_fetch`. Default sandbox devices are seeded with private/reserved ranges plus common metadata-service addresses; trusted devices (`sandbox_mode=false`) created without an explicit value store `[]`. Users remove entries to permit an internal target rather than guessing a whitelist entry. Server-site `web_fetch` keeps its hardcoded block-list and ignores this column (ADR-052).
- Py6 persists `shell_timeout_max` and `env_allowlist` for client-only exec.
  `shell_timeout_max` is the per-device hard process lifetime cap; zero permits
  only explicitly bounded `timeout=0` calls. `env_allowlist` is an exact-name
  parent-environment allowlist and every `OPENOCTOPUS_*` name is rejected.
  Py6 has no `command_denylist` or MCP device columns.
- **Online state is in-memory only** — no `online` / `last_seen_at` columns; the connected-WS registry keyed by immutable device ID is the source of truth. The `Device` API response computes `online` on demand. Three device states per ADR-110: state-1 (online, in-registry), state-2 (offline-but-paired, row exists, not in registry — listed in `openoctopus_device` enum so the agent can still attempt and fail loudly), state-3 (deleted — row gone, in-memory entry gone, live WS force-closed, tool registry invalidated; complete wipe with no soft-delete tombstone).
- **No inbound FKs reference `devices`** from other tables. This keeps token regeneration and device deletion local to one row. State-3 transition is a single-row DELETE; cascades from `users.id` are the only path that takes multiple device rows out at once (account deletion). If a future milestone adds durable tables that reference devices, revisit ADR-091 before adding those FKs.
- `workspace_path` default is the literal string `~/openoctopus/workspace` on every OS (ADR-111) when omitted from `POST /api/devices`. Explicit overrides and PATCH updates must be non-empty strings and are stored verbatim. The server does not expand `~` or check client disk existence; the client expands `~` against its own home dir and creates/reports the path at startup or config-update time.

---

## 10. `workspaces` — shared workspace registry (ADR-108)

```sql
CREATE TABLE IF NOT EXISTS workspaces (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT         NOT NULL,
    suffix       TEXT         NOT NULL,
    quota_bytes  BIGINT       NOT NULL,
    created_by   UUID         REFERENCES users(id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

- `id` — UUID primary key. Drives the RustFS object prefix `workspaces/{id}/`.
- `name` — display label. **Not unique.** Two unrelated teams may both create a workspace called "Xmas gift". The validator in ADR-109 enforces character rules (no `/`, `\`, `@`, `:`, control chars, etc.), NFC-normalizes, and length-caps at 64 chars.
- `suffix` — persisted 8+ hex-character addressing suffix. It starts with the
  shortest collision-free prefix of `id` allowed by ADR-108 and remains stable
  across renames and membership changes.
- `quota_bytes` — capped at `system_config.shared_workspace_quota_bytes` at create and rename time.
- Quota state for both personal and shared workspaces is exposed through `Workspace` API responses (`quota_bytes`, `bytes_used`, `locked`) from `GET /api/workspaces` and `GET /api/workspaces/{workspace_ref}`; there is no separate personal-only quota route.
- `created_by` — author. **Exception to ADR-058**: uses `ON DELETE SET NULL`, not `CASCADE`. Removing the creator's user account does not delete a workspace that still has other members; `created_by` becomes NULL and the membership rows survive.
- Last-member-leaves auto-deletion is enforced in application code under a
  PostgreSQL workspace-row lock. The workspace row and a durable cleanup intent
  commit together; RustFS prefix deletion then runs idempotently.

---

## 11. `workspace_members` — shared workspace allow-list (ADR-108)

```sql
CREATE TABLE IF NOT EXISTS workspace_members (
    workspace_id  UUID         NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    user_id       UUID         NOT NULL REFERENCES users(id)      ON DELETE CASCADE,
    joined_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workspace_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_members_user ON workspace_members(user_id);
```

- Composite PK is the natural identity (a user is in a workspace at most once).
- Two cascades: deleting a workspace removes all its members; deleting a user removes them from every workspace they joined.
- `idx_workspace_members_user` powers the per-user "list my workspaces" query that runs at every `build_context` to render the system prompt's Workspaces section.

---

## 12. `workspace_deletions` — durable RustFS cleanup intents

```sql
CREATE TABLE IF NOT EXISTS workspace_deletions (
    kind        TEXT        NOT NULL CHECK (kind IN ('personal', 'shared')),
    target_id   UUID        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, target_id)
);
```

- The composite key identifies `users/{target_id}/` or
  `workspaces/{target_id}/`; there is intentionally no FK because the metadata
  row is deleted in the same transaction that creates this cleanup intent.
- Lifecycle order is: retire the in-process target, commit metadata deletion
  plus this row, purge the RustFS prefix, then delete this row.
- Post-commit purge failure does not undo or misreport the logical deletion.
  The running process retries pending rows periodically, and startup recovery
  drains them after the RustFS capability probe.

---

## 13. `cron_jobs` — scheduled agent invocations

```sql
CREATE TABLE IF NOT EXISTS cron_jobs (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id      UUID         NOT NULL REFERENCES sessions(id),
    name            TEXT         NOT NULL,
    schedule        TEXT         NOT NULL,
    tz              TEXT,
    one_shot        BOOLEAN      NOT NULL DEFAULT FALSE,
    message         TEXT         NOT NULL,
    last_fired_at   TIMESTAMPTZ,
    next_fire_at    TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cron_jobs_user_id    ON cron_jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_next_fire  ON cron_jobs(next_fire_at)
    WHERE next_fire_at IS NOT NULL;
```

- `schedule` — normalized cron expression (server parses agent-supplied `every_seconds` / `cron_expr` / `at` into a single canonical form at insert time).
- `name` — short user-facing label. The cron tool defaults it from the first 30 characters of the message; REST callers may provide it explicitly.
- `session_id` — dedicated cron session created by the shared cron write helper. The session uses `channel='cron'`, `chat_id=<job_id>`, and `session_key='cron:<job_id>'`.
- `tz` — optional IANA timezone used when parsing cron expressions or naive one-shot timestamps.
- `one_shot` — true when the agent created the job from a `cron(action="add", at=...)` call (one-time future trigger). Once fired, the row is deleted and the dedicated cron session remains as normal session history.
- `message` — the agent-facing instruction the scheduler injects into the cron session as a synthesized user message when the job fires.
- `next_fire_at` — denormalized for the scheduler index. Recomputed each time the job fires.
- Cron writes must validate the schedule before insert/update: exactly one timing form, positive intervals, known timezone, valid cron expression, and a future `next_fire_at`. Past one-shots and unrunnable schedules are rejected rather than stored.
- **No `kind` column** — heartbeat is a tick loop, not a cron row, and Dream is deferred (ADR-055, ADR-092).

---

## Constraints summary

- Every user-referencing FK has `ON DELETE CASCADE` (ADR-058). Account deletion
  is an application transaction because it also records durable RustFS cleanup
  intents. **Sole FK exception:** `workspaces.created_by` uses `ON DELETE SET
  NULL` per ADR-108 so a workspace persists for its remaining members.
- No surrogate "is_active" / "deleted_at" columns — deletes are hard, undo lives in admin's backup strategy.
- No migration framework before production launch (ADR-069). Schema changes
  during the development rebuild require resetting the development database;
  real-user deployments add a versioned migration framework later.

---

## Indexes summary

| Index | Table | Purpose |
|---|---|---|
| `users_email_key` | users (UNIQUE on `email`) | Login lookup. |
| `idx_sessions_user_id` | sessions | List user's sessions. |
| `idx_sessions_user_session_key` | sessions (UNIQUE on `(user_id, session_key)`) | Per-user channel-message → session lookup. |
| `idx_messages_session_created` | messages (`session_id, created_at`) | History replay + cursor scan. |
| `idx_pending_messages_session_received` | pending_messages (`session_id, received_at, id`) | Safe-boundary drain order. |
| `idx_pending_messages_session_key_received` | pending_messages (`session_key, received_at, id`) | Recovery and channel-key lookup for queued inbound. |
| `idx_turn_runs_one_running_per_session` | turn_runs (partial UNIQUE on `session_id WHERE status='running'`) | Durable one-active provider-call/tool-batch invariant. |
| `idx_turn_runs_session_started` | turn_runs (`session_id, started_at DESC, id DESC`) | Latest run status for GET recovery. |
| `idx_devices_user_id` | devices | List user's devices. |
| `devices_user_id_name_key` | devices (UNIQUE on `(user_id, name)`) | URL resolution `/api/devices/{name}`. |
| `workspace_members_pkey` | workspace_members (PK on `(workspace_id, user_id)`) | Membership lookup at workspace-fs entry. |
| `idx_workspace_members_user` | workspace_members | Per-user "list my workspaces" for system-prompt rebuild. |
| `idx_cron_jobs_user_id` | cron_jobs | List user's cron jobs. |
| `idx_cron_jobs_next_fire` | cron_jobs (`next_fire_at`) | Scheduler poll. |

---

## Extensions

- `uuid-ossp` or `pgcrypto` for `gen_random_uuid()` — `pgcrypto` is built-in on most PostgreSQL distributions and is the default choice.
- No other extensions in v1.
