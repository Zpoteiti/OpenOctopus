# OpenOctopus — Database Schema

The PostgreSQL schema contract for `openoctopus_server`. During Py-Prep this doc
is the canonical reference for table, column, index, constraint, and storage
semantics. Python bootstrap uses SQLAlchemy declarative models/metadata with
`create_all()`; Alembic or equivalent migration framework is deferred until
production launch after frontend completion (ADR-057, ADR-069).

**Ten tables.** Account deletion is a single `DELETE FROM users WHERE id = $1`; every user-referencing FK has `ON DELETE CASCADE` defined inline (ADR-058) — with one explicit exception in `workspaces.created_by` (`ON DELETE SET NULL`, see ADR-108) so a workspace persists for its remaining members when the creator's account is removed.

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
| `llm_endpoint` | string | ADR-101 | Anthropic-compatible Messages API base URL. |
| `llm_api_key` | string | ADR-101 | Bearer credential for outbound LLM calls; redacted in admin API responses. |
| `llm_model` | string | ADR-101 | Model name passed in the Anthropic Messages request body. |
| `llm_max_context_tokens` | int | ADR-101 | LLM context window in tokens (e.g. `128000` for gpt-4o). Counted with tiktoken-rs (ADR-025). |
| `llm_compaction_threshold_tokens` | int | ADR-028, ADR-101 | Accepted/reserved config for later compaction decisions. Missing means not configured; future compaction code must handle that explicitly. Python-main may store the value before compaction is implemented. |
| `llm_max_concurrent_requests` | int | ADR-101 | Optional in-process semaphore for outbound LLM calls. A configured `0` means unlimited and creates no semaphore. A positive integer caps concurrent in-flight LLM calls; negative values and values above the server maximum are invalid. If missing at server startup, only the runtime limiter treats it as `0`; no row is persisted. |

Reserved/future known keys (not PATCH-editable until their milestone):

| Key | Type | ADR | Purpose |
|---|---|---|---|
| `server_mcp` | array of `McpServerConfig` | ADR-114 | Admin-configured shared-service MCPs exposed as install site `server`; shared credentials, one runtime per MCP, bounded queue. |

Bootstrap does not seed `system_config` rows. Fresh `GET /api/admin/config`
therefore returns the effective quota defaults and omits unconfigured LLM keys
until an admin writes values. Deployments may carry additional opaque keys
inserted outside the admin API; OpenOctopus ignores them in the admin config
view. `PATCH /api/admin/config` rejects keys outside the admin-editable table
above, including `server_mcp` and `object_storage_*`.

Python-main accepts `llm_endpoint`, `llm_api_key`, and `llm_model` only after provider
validation succeeds. First setup must provide all three identity values; later
PATCHes may reuse stored values by omitting unchanged identity keys. Validation
uses `GET {llm_endpoint}/models` before any DB write, so failed identity
changes do not persist paired config updates. `llm_api_key` is stored in
`system_config` for outbound calls but redacted as `"<redacted>"` in admin
config read and patch responses. Sending the literal redaction marker as a new
key is rejected. `server_mcp` is documented for the Py8 MCP scope and is not
accepted by the admin-config endpoint. Object storage is deployment
infrastructure config supplied through environment / deployment secrets, not
`system_config`.

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
- `is_admin` — true for any user who registered with the `ADMIN_TOKEN`. Admin APIs protect the last remaining admin from deletion.
- **No `soul`, `memory_text`, or user-level SSRF policy columns** — workspace-file-only per ADR-060.
- **No inline channel fields** — Discord/Telegram live in their own tables (ADR-090).
- **No `bytes_used` column** — workspace usage is computed on demand by `workspace_fs` summing MinIO object sizes under the workspace prefix (or maintained via a denormalized counter/index hidden inside `workspace_fs`; not part of the API contract).

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
- `cancel_requested` — set true by `POST /api/sessions/{id}/cancel` only when a runner is active (ADR-035), observed at the next safe boundary, then cleared. Cancel on an idle session is a no-op and must not leave this flag true.
- `DELETE /api/sessions/{id}` removes session rows after terminating any in-memory runner/streams. `ON DELETE CASCADE` removes that session's `messages` and `pending_messages`; channel configuration rows are not tied to session deletion. Active cron sessions are rejected by the FK from `cron_jobs.session_id`; delete the cron job through `/api/cron/{id}` so the job row and its dedicated history stay consistent. Completed one-shot cron sessions with no remaining `cron_jobs` row can be deleted as normal history.

---

## 6. `messages` — every assistant/user/tool turn

```sql
CREATE TABLE IF NOT EXISTS messages (
    id                       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id               UUID         NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    role                     TEXT         NOT NULL CHECK (role IN ('user', 'assistant')),
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
    is_compaction_summary    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_session_created
    ON messages(session_id, created_at);
```

- `content` — JSONB array of Anthropic Messages content blocks (ADR-059, ADR-101, ADR-117). Block shapes mirror what the LLM will receive after provider-layer projection. Supported persisted block types are `text`, `image`, `tool_use`, `tool_result`, `thinking`, and `redacted_thinking`. **Images** are stored as Anthropic `image` blocks with base64 data inline. **Tool results** store `content` as a safe block array; real tool output starts with the server-generated untrusted-result warning block, while server-authored synthetic tool results use the same array shape for diagnostic text. **Non-image files** (PDFs, CSVs, audio, ...) live only in workspaces; the DB carries path-text markers and the agent reaches bytes via `read_file`. Remote attachment runtime failures are also persisted as server-authored text marker blocks so the user message is not lost.
- `delivery_refs` — JSONB array of user-visible file delivery references for channel adapters, ignored by provider replay. Web `message(media=...)` uses this sidecar for file chips/download links so `messages.content` can stay Anthropic-compatible. Server workspace refs are durable and point at `openoctopus_device="server"` paths. Device refs are online-only pointers to `(device name, path)`; the browser resolves them later through the Workspace Files `GET` route and may receive `device_unreachable`, `not_found`, or policy errors at click time. Third-party channel native uploads do not need a OpenOctopus download ref unless a later UI wants to render platform receipts.
- `llm_fingerprint` — nullable model/provider fingerprint for assistant rows that contain opaque thinking state. Provider replay may use raw `thinking` / `redacted_thinking` blocks only when this matches the current compatible model segment.
- `role` — strictly `user` or `assistant` (ADR-089). Tool results are `role='user'` rows containing `tool_result` blocks.
- `message_kind` — OpenOctopus semantic discriminator exposed through SSE/history: `human` for external/user-marker rows, `assistant` for normal provider responses, `tool_result` for real server/device tool results, `synthetic_tool_result` for restart/cancel/unreachable repair rows, `synthetic_assistant_error` for exhausted provider failures, or `compaction_summary` for provider-compatible summary rows. It avoids JSONB inspection for latest-human detection, JIT collapsing, frontend rendering, and audit.
- `is_compaction_summary` — retained as the fast compaction marker. Compaction summary rows also use `message_kind='compaction_summary'`.
- The `idx_messages_session_created` index powers the `GET /api/sessions/{id}/messages` cursor scan.
- Runtime block (ADR-094) is prepended into the user-row's `content` JSONB at ingress time; not a separate column.

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

- `pending_messages` stores inbound user messages that arrive while a session worker is active. These rows are durable but not provider-visible history yet. Browser HTTP stream ownership is not stored here: an older queued POST can close with `stream_replaced` while its row remains durable, and the newest queued POST is only an in-memory live preview subscriber. `effort` is nullable; `NULL` and `off` send `thinking.type=disabled`; non-off values send `thinking.type=adaptive` plus Anthropic `output_config.effort`.
- `session_key` is stored alongside `session_id` so channel/session routing can recover pending work without recomputing the key.
- At the safe boundary after the current assistant tool batch is fully addressed (ADR-034), the worker drains all rows for the session in one DB transaction: select pending rows in `(received_at, id)` order, insert them into `messages` with the same `id` and `message_kind='human'`, delete the selected pending rows, commit, then the resulting user messages become visible to canonical history and the latest live POST preview stream. `GET /api/sessions/{id}/messages` returns these rows separately as `pending_messages` until they drain; the stable `id` lets the frontend reconcile the pending item with the eventual canonical message. When no session is in flight, this table should normally be empty.
- The Python server alpha uses passive recovery instead of startup scans. A process restart drops live token previews and in-memory stream subscribers; the next inbound POST/channel activity rebuilds from Postgres and drains durable pending rows at the next safe boundary.

---

## 8. `devices` — per-user client devices

```sql
CREATE TABLE IF NOT EXISTS devices (
    token              TEXT         PRIMARY KEY,
    user_id            UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name               TEXT         NOT NULL CHECK (name ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND name <> 'server'),
    workspace_path     TEXT         NOT NULL,
    sandbox_mode       BOOLEAN      NOT NULL DEFAULT TRUE,
    shell_timeout_max  INTEGER      NOT NULL DEFAULT 600 CHECK (shell_timeout_max >= 0),
    ssrf_denylist      JSONB        NOT NULL DEFAULT
        '["127.0.0.0/8","::1/128","10.0.0.0/8","172.16.0.0/12","192.168.0.0/16","100.64.0.0/10","169.254.0.0/16","169.254.169.254/32","fc00::/7","fe80::/10"]'::jsonb,
    env_allowlist      JSONB        NOT NULL DEFAULT '["PATH","HOME","LANG","TERM"]'::jsonb,
    command_denylist   JSONB        NOT NULL DEFAULT
        '["shutdown","reboot","halt","poweroff","mkfs","dd","mount","umount","systemctl","service"]'::jsonb,
    mcp_servers        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_devices_user_id ON devices(user_id);
```

- `token` is the PK *and* the credential (ADR-091). Stored plaintext — it IS the credential. WS handshake `Authorization: Bearer <token>` is matched directly against this column. REST returns the plaintext token only from `POST /api/devices` and `POST /api/devices/{name}/regenerate-token`; `GET /api/devices` returns full device details plus `token_hint`, never the plaintext token. `token_hint` is computed as the first 16 characters of the token, `...`, and the final 6 characters.
- `name` is the REST/tool-routing canonical slug. It is UNIQUE per user, so the URL `PATCH /api/devices/laptop/config` resolves to `(user_id, "laptop")` without ever touching the token. Raw create/rename input is canonicalized server-side: NFC normalize, trim, ASCII-lowercase, convert whitespace runs to a single hyphen, then require `^[a-z0-9]+(-[a-z0-9]+)*$`. Stored names are at most 64 characters and use only lowercase ASCII letters, digits, and hyphens. The literal name `server` is reserved for OpenOctopus's built-in server install site and is rejected for user devices after canonicalization.
- `sandbox_mode` — the per-device privilege switch. `true` is the default restricted profile: client file tools and Workspace Files REST routes must stay inside `workspace_path`, and client `web_fetch` applies `ssrf_denylist`. `false` is the trusted-device profile: client file tools may address paths outside `workspace_path`, and internal/private network access is allowed unless the user keeps explicit deny entries. This is a persisted device property; sessions cannot temporarily override it.
- `ssrf_denylist` — JSONB array of CIDRs, hosts, or `host:port` entries rejected by client-site `web_fetch`. Default sandbox devices are seeded with private/reserved ranges plus common metadata-service addresses; trusted devices (`sandbox_mode=false`) created without an explicit value store `[]`. Users remove entries to permit an internal target rather than guessing a whitelist entry. Server-site `web_fetch` keeps its hardcoded block-list and ignores this column (ADR-052).
- `env_allowlist` — JSONB array of parent-process environment variable names allowed into `exec` and client MCP subprocesses. Defaults to `PATH`, `HOME`, `LANG`, and `TERM`. This intentionally stays an allowlist because secret env names are not enumerable.
- `command_denylist` — JSONB array of command-name deny entries for client `exec`. It is enforced before spawning and applies in both sandbox and trusted mode. The initial default blocks obvious host-management and destructive commands; users delete entries per device when they intentionally want that device to run them.
- `shell_timeout_max` — per-device cap for client `exec` hard timeouts, in seconds. Positive values bound positive `exec.timeout` requests. `0` means no cap and permits no-hard-timeout exec sessions on this device (e.g. long-running services started with `timeout=0` and `yield_time_ms`). Default `600` keeps ordinary devices nanobot-aligned while allowing a device owner to opt into unlimited sessions explicitly.
- `mcp_servers` — JSONB map of `<server_name>: McpServerConfig` (see API.yaml), stored as the full unredacted config including `env` secrets. REST responses redact every `mcp_servers.*.env.*` value as `"<redacted>"`; writes reject that marker so clients cannot accidentally persist a redacted response over real secrets. Device WebSocket `hello_ack` and `config_update` use the stored unredacted DB value. Config changes are pushed to the live device via `config_update` frame (ADR-050, PROTOCOL.md §3.7).
- **Online state is in-memory only** — no `online` / `last_seen_at` columns; the connected-WS registry keyed by device token is the source of truth. The `Device` API response computes `online` on demand. Three device states per ADR-110: state-1 (online, in-registry), state-2 (offline-but-paired, row exists, not in registry — listed in `openoctopus_device` enum so the agent can still attempt and fail loudly), state-3 (deleted — row gone, in-memory entry gone, live WS force-closed, tool registry invalidated; complete wipe with no soft-delete tombstone).
- **No inbound FKs reference `devices`** from other tables. This is the schema precondition that makes `devices.token` as primary key acceptable while still allowing token regeneration by updating the PK in place. State-3 transition is a single-row DELETE; cascades from `users.id` are the only path that takes multiple device rows out at once (account deletion). If a future milestone adds durable tables that reference devices, revisit ADR-091 before adding those FKs.
- `workspace_path` default is the literal string `~/openoctopus/workspace` on every OS (ADR-111) when omitted from `POST /api/devices`. Explicit overrides and PATCH updates must be non-empty strings and are stored verbatim. The server does not expand `~` or check client disk existence; the client expands `~` against its own home dir and creates/reports the path at startup or config-update time.

---

## 9. `workspaces` — shared workspace registry (ADR-108)

```sql
CREATE TABLE IF NOT EXISTS workspaces (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT         NOT NULL,
    quota_bytes  BIGINT       NOT NULL,
    created_by   UUID         REFERENCES users(id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

- `id` — UUID primary key. Drives the MinIO object prefix `workspaces/{id}/` and the public-facing `name@suffix` addressing form (ADR-108, ADR-123) where `suffix` is the first 8 hex chars of `id` (auto-extended on collision per ADR-108).
- `name` — display label. **Not unique.** Two unrelated teams may both create a workspace called "Xmas gift". The validator in ADR-109 enforces character rules (no `/`, `\`, `@`, `:`, control chars, etc.), NFC-normalizes, and length-caps at 64 chars.
- `quota_bytes` — capped at `system_config.shared_workspace_quota_bytes` at create and rename time.
- Quota state for both personal and shared workspaces is exposed through `Workspace` API responses (`quota_bytes`, `bytes_used`, `locked`) from `GET /api/workspaces` and `GET /api/workspaces/{workspace_ref}`; there is no separate personal-only quota route.
- `created_by` — author. **Exception to ADR-058**: uses `ON DELETE SET NULL`, not `CASCADE`. Removing the creator's user account does not delete a workspace that still has other members; `created_by` becomes NULL and the membership rows survive.
- Last-member-leaves auto-deletion (per `DELETE FROM workspace_members WHERE workspace_id = $1`) is enforced in application code (`workspace_fs`), not SQL — when no `workspace_members` rows remain for a `workspaces.id`, the row is deleted and the corresponding MinIO object prefix is deleted.

---

## 10. `workspace_members` — shared workspace allow-list (ADR-108)

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

## 11. `cron_jobs` — scheduled agent invocations

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

- Every user-referencing FK has `ON DELETE CASCADE` (ADR-058) → account deletion is one statement. **Sole exception:** `workspaces.created_by` uses `ON DELETE SET NULL` per ADR-108 so a workspace persists for its remaining members when its creator's account is removed.
- No surrogate "is_active" / "deleted_at" columns — deletes are hard, undo lives in admin's backup strategy.
- No migration framework in v1 (ADR-069). Schema changes during rebuild require dev-DB reset (`scripts/reset-db.sh`). Real-user deployments add `sqlx::migrate!` later.

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
