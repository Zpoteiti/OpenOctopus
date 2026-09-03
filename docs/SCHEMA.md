# OpenOctopus — Database Schema

The PostgreSQL schema contract for `openoctopus_server`. During Py-Prep this doc
is the canonical reference for table, column, index, constraint, and storage
semantics. Python bootstrap uses SQLAlchemy declarative models/metadata with
`create_all()`; Alembic or equivalent migration framework is deferred until
production launch after frontend completion (ADR-057, ADR-069).

**Sixteen tables.** Account deletion fences affected workspaces, then commits
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
| `web_fetch_denylist` | array of strings | ADR-133 | Effective Server `web_fetch` denylist. Missing uses the private/reserved/metadata default; explicit `[]` allows all otherwise-valid HTTP(S) targets. PATCH validates and canonicalizes the complete list before writing, and later fetches read the current value without a restart. |

Dedicated admin-managed keys (not editable through `PATCH /api/admin/config`):

| Key | Type | ADR | Purpose |
|---|---|---|---|
| `server_mcp` | `ServerMcpEnvelope` object | ADR-114 | Authoritative Py8a admin shared-service MCP config, monotonic revision, and complete last-good catalog. Managed only through `GET/PUT /api/admin/server-mcp`. |

Bootstrap does not seed `system_config` rows. Fresh `GET /api/admin/config`
therefore returns the effective quota defaults, `llm_max_output_tokens=16384`,
and the effective canonical `web_fetch_denylist`, while omitting the unconfigured
LLM identity and context/compaction/concurrency keys until an admin writes values. Deployments
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
key is rejected. `server_mcp` is not accepted by the generic admin-config
endpoint. Object storage is deployment
infrastructure config supplied through environment / deployment secrets, not
`system_config`.

The Provider identity and API key are deployment-wide administrator
configuration. Channel owners do not supply a separate LLM credential: all
Web, Discord, DingTalk, Cron, and Heartbeat turns use the administrator's
Provider account, so the administrator bears their Provider usage and cost.

`system_config.server_mcp` stores one strict JSONB envelope:

```json
{
  "version": 1,
  "config_revision": 7,
  "mcp_servers": [],
  "mcp_catalog": {
    "version": 1,
    "digest": "<lowercase sha256>",
    "servers": []
  }
}
```

The config, revision, and complete last-good four-surface catalog change in one
row and one transaction. A missing row reads as the canonical empty envelope at
revision 1; the first effective PUT writes revision 2. Effective no-ops neither
write nor advance `updated_at`; deleting the final config writes an empty
envelope with the next revision rather than deleting the row. Strict parsing,
catalog/config correspondence, bounded canonical JSON, and the catalog digest
are authoritative invariants; corruption is a startup failure, while an MCP
endpoint outage is only degraded runtime state.

Server MCP stdio `env` and remote `headers` are intentionally reversible
plaintext inside this PostgreSQL value. REST projections preserve their keys
but replace every value with `"<redacted>"`; catalogs, Provider schemas,
runtime diagnostics, errors, and logs contain no secret values. Each Server MCP
config also stores its effective `max_concurrent_calls` (`1..32`; stdio default
1, Streamable HTTP/SSE default 8). Runtime generations, counters, retry state,
and sanitized errors are process-local and never stored in the envelope.

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
    timezone       TEXT         NOT NULL DEFAULT 'UTC'
                                 CHECK (char_length(timezone) BETWEEN 1 AND 64),
    is_admin       BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

- `password_hash` — argon2 (or bcrypt — implementer's choice within reason). Never returned by any API.
- `timezone` — canonical IANA zone name used as the default when a Cron request
  omits `tz` and to render Heartbeat Phase 1 local time. The application
  validates it with the pinned `zoneinfo`/`tzdata` database; the SQL length
  check is only a storage invariant. Existing jobs keep their stored effective
  timezone when this profile value changes.
- `is_admin` — true for any user who registered with the `OPENOCTOPUS_ADMIN_TOKEN`. Admin APIs protect the last remaining admin from deletion.
- **No `soul`, `memory_text`, or user-level SSRF policy columns** — workspace-file-only per ADR-060.
- **No inline channel fields** — Discord and DingTalk live in their own tables
  (ADR-090, ADR-136).
- **No `bytes_used` column** — workspace usage is computed on demand by `workspace_fs` summing paged RustFS object metadata under the workspace prefix.

---

## 3. `discord_configs` — per-user Discord bot integration

```sql
CREATE TABLE IF NOT EXISTS discord_configs (
    user_id                 UUID         PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    bot_token               TEXT         NOT NULL,
    application_id          TEXT         NOT NULL UNIQUE,
    bot_user_id             TEXT         NOT NULL,
    bot_display_name        TEXT,
    bot_avatar_url          TEXT,
    binding_generation      UUID         NOT NULL,
    revision                BIGINT       NOT NULL DEFAULT 1 CHECK (revision >= 1),
    owner_platform_user_id  TEXT,
    owner_dm_chat_id        TEXT,
    paired_at               TIMESTAMPTZ,
    allow_list              JSONB        NOT NULL DEFAULT '[]'::jsonb
                                          CHECK (jsonb_typeof(allow_list) = 'array'),
    pairing_code_hash       BYTEA        CHECK (
                                          pairing_code_hash IS NULL
                                          OR octet_length(pairing_code_hash) = 32
                                      ),
    pairing_expires_at      TIMESTAMPTZ,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CHECK (
        (owner_platform_user_id IS NULL AND owner_dm_chat_id IS NULL AND paired_at IS NULL)
        OR (owner_platform_user_id IS NOT NULL AND owner_dm_chat_id IS NOT NULL
            AND paired_at IS NOT NULL)
    ),
    CHECK ((pairing_code_hash IS NULL) = (pairing_expires_at IS NULL))
);
```

- `user_id` is both PK and FK — at most one Discord config per user.
- `bot_token` is a write-only secret. The API returns only the non-secret
  `credential_hint="Configured"`; omitting the secret on update keeps it.
- Credential validation resolves and stores the unique Discord application and
  Bot identity before commit. Replacing the application creates a new
  `binding_generation`, clears owner pairing, and fences callbacks and
  deliveries from the old Bot. `revision` fences concurrent config updates.
- The owner is never hand-entered. A human sends the hash-only, ten-minute
  one-time code in a Bot direct message; successful confirmation atomically
  fills `owner_platform_user_id`, `owner_dm_chat_id`, and `paired_at`.
- `allow_list` is a whole-array replacement containing only exact platform
  **user IDs** manually entered by the owner (at most 256). It never contains a
  guild, channel, group, or role ID. Matching senders receive the persisted
  `allowed_non_owner` / `message_only` authority profile.

---

## 4. `dingtalk_configs` — per-user DingTalk bot integration

```sql
CREATE TABLE IF NOT EXISTS dingtalk_configs (
    user_id                 UUID         PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    client_id               TEXT         NOT NULL UNIQUE,
    client_secret           TEXT         NOT NULL,
    bot_user_id             TEXT         NOT NULL,
    bot_display_name        TEXT,
    bot_avatar_url          TEXT,
    binding_generation      UUID         NOT NULL,
    revision                BIGINT       NOT NULL DEFAULT 1 CHECK (revision >= 1),
    owner_platform_user_id  TEXT,
    owner_dm_chat_id        TEXT,
    paired_at               TIMESTAMPTZ,
    allow_list              JSONB        NOT NULL DEFAULT '[]'::jsonb
                                          CHECK (jsonb_typeof(allow_list) = 'array'),
    pairing_code_hash       BYTEA        CHECK (
                                          pairing_code_hash IS NULL
                                          OR octet_length(pairing_code_hash) = 32
                                      ),
    pairing_expires_at      TIMESTAMPTZ,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CHECK (
        (owner_platform_user_id IS NULL AND owner_dm_chat_id IS NULL AND paired_at IS NULL)
        OR (owner_platform_user_id IS NOT NULL AND owner_dm_chat_id IS NOT NULL
            AND paired_at IS NOT NULL)
    ),
    CHECK ((pairing_code_hash IS NULL) = (pairing_expires_at IS NULL))
);
```

The pairing, exact-user-ID allow list, generation, and revision contracts match
Discord. `client_secret` is write-only. DingTalk's configured `client_id` is
also the active-message `robotCode` and the unique stored Bot identity. The
token and Stream handshakes do not expose a platform Bot name, avatar, or
chatbot user ID, so validation stores `bot_user_id=client_id` and leaves
`bot_display_name` / `bot_avatar_url` null. The API and UI preserve that unknown
state instead of inventing platform profile data.

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

- `session_key` is the composite route identity: `{channel}:{chat_id}` for
  channel adapters and the deterministic `web:{id}`, `cron:{job_id}`, or
  `heartbeat:{user_id}` routes for current Web/internal constructors. It is
  unique per user, not globally unique.
- `id` is the internal UUID used as FK target by `messages.session_id` and the browser REST path identifier. Most internal code uses `id`; channel adapters may look up by `(user_id, session_key)`.
- Browser web session rows are created implicitly by `POST /api/sessions/{id}/messages` when the client-generated UUID does not yet exist. Python-main has no separate `POST /api/sessions` create route.
- Discord and DingTalk sessions are created only by accepted adapter ingress.
  Their history is visible in the browser, including sender, source context, and
  delivery state, but has no browser message composer. Message POST remains
  restricted to Web sessions; the owner may still rename/read-mark, cancel a
  running Turn, or delete an external Session and its history.
- `title` is the human-facing mutable session name. It defaults to `"New chat"` and never affects `id`, `chat_id`, or `session_key`.
- `last_inbound_at` — bumped on every new InboundMessage; powers session-list ordering in the UI.
- `last_read_at` — browser inbox read marker. Updated by `PATCH /api/sessions/{id}` with `read_through_message_id`; the update sets the marker to the greater of the current value and the target canonical message's `created_at`. `GET /api/sessions` derives `unread` by checking for user-visible messages newer than this timestamp. `GET /api/sessions/{id}/messages` does not mutate this marker, so prefetching and polling do not accidentally mark a session as read.
- `cancel_requested` — set true by `POST /api/sessions/{id}/cancel` only when a runner is active (ADR-035), observed at the next safe boundary, then cleared. Cancel on an idle session is a no-op and must not leave this flag true. If an in-flight provider request returns a final response with no tools, normal completion wins and clears the flag without a stop marker (ADR-129).
- `DELETE /api/sessions/{id}` removes session rows after terminating any
  in-memory runner/streams. `ON DELETE CASCADE` removes that session's
  `turn_runs`, `messages`, `pending_messages`, channel deliveries, and their
  actions. Channel receipts retain their source idempotency key with
  `session_id=NULL`; channel configuration and Cron job rows are not tied to
  Session deletion. A later accepted Cron fire or
  Heartbeat Phase 2 recreates its route with the same stable UUID and empty
  history. Deleting a Cron job is a separate operation that stops future
  triggers while preserving any existing Session/history.

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
    attachment_refs          JSONB        NOT NULL DEFAULT '[]'::jsonb,
    delivery_refs            JSONB        NOT NULL DEFAULT '[]'::jsonb,
    sender_id                TEXT,
    sender_display_name      TEXT,
    sender_classification    TEXT         CHECK (
                                 sender_classification IS NULL OR
                                 sender_classification IN ('owner', 'allowed_non_owner', 'internal')
                             ),
    ingress_tool_profile     TEXT         CHECK (
                                 ingress_tool_profile IS NULL OR
                                 ingress_tool_profile IN ('owner_full', 'message_only')
                             ),
    source_message_id        TEXT,
    channel_binding_generation UUID,
    channel_context          JSONB        NOT NULL DEFAULT '[]'::jsonb
                                          CHECK (jsonb_typeof(channel_context) = 'array'),
    llm_fingerprint          TEXT,
    is_compacted             BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CHECK (
        message_kind <> 'human' OR
        (sender_id IS NOT NULL AND sender_classification IS NOT NULL
         AND ingress_tool_profile IS NOT NULL)
    ),
    CHECK (
        message_kind = 'human' OR
        (sender_id IS NULL AND sender_display_name IS NULL
         AND sender_classification IS NULL AND ingress_tool_profile IS NULL
         AND source_message_id IS NULL AND channel_binding_generation IS NULL
         AND channel_context = '[]'::jsonb)
    ),
    CHECK (
        sender_classification IS NULL OR
        (sender_classification = 'owner' AND ingress_tool_profile = 'owner_full') OR
        (sender_classification = 'allowed_non_owner' AND ingress_tool_profile = 'message_only') OR
        sender_classification = 'internal'
    )
);

CREATE INDEX IF NOT EXISTS idx_messages_session_created
    ON messages(session_id, created_at);
```

- `content` — JSONB array of Anthropic Messages content blocks (ADR-059, ADR-101, ADR-117). Block shapes mirror what the LLM will receive after provider-layer projection, except optional OpenOctopus `tool_result.code` metadata is retained for storage/public diagnostics and stripped from the strict provider request. Supported persisted block types are `text`, `image`, `tool_use`, `tool_result`, `thinking`, and `redacted_thinking`. **Images** are stored as Anthropic `image` blocks with base64 data inline. **Tool results** store `content` as a safe block array; real tool output starts with the server-generated untrusted-result warning block, while server-authored synthetic tool results use the same array shape for diagnostic text. **Non-image files** (PDFs, CSVs, audio, ...) stay at their referenced Workspace or Client location; the DB carries provider-visible path-text markers and the agent reaches bytes via `read_file`. A later Client read failure is persisted as the normal tool-result error; message acceptance does not invent a remote-read failure marker.
- `attachment_refs` — provider-hidden JSONB sidecar for normalized browser attachment identities (ADR-135). Server refs retain the virtual Workspace path. Client refs retain `(device name, immutable device UUID, path)` without reading or copying Client bytes during message acceptance. Active human rows plus the captured pending prefix provide a conservative name-level UUID fence for device tool routing, Device MCP authority, and System-prompt Device metadata; conflicting UUIDs for one name disable that target while leaving its name in fixed built-in Provider schemas. A compacted source row keeps its historical sidecar, but the generated summary receives `[]` and does not preserve an actionable attachment capability.
- `delivery_refs` — JSONB array of user-visible file delivery references for channel adapters, ignored by provider replay. The web `message(media=...)` tool uses this sidecar for file chips/download links so `messages.content` can stay Anthropic-compatible. Each ref links to its generating `tool_use_id`. Server workspace refs are durable and retain the virtual path plus immutable workspace ID/workspace-relative path so a frontend can recover from a shared-workspace rename. Device refs are online-only pointers to immutable `(device_id, captured device name, path)` identity; a later relay must reject rename/delete/name-reuse drift instead of reading from another device. Device refs do not cause a device read or RustFS write when the message is sent.
- Human rows persist their authoritative sender identity, classification, and
  ingress tool profile. Owners and internal synthesizers use `owner_full`;
  exact-ID allow-listed non-owners use `message_only`. That association is
  data, not runtime text, and survives queueing, restart, compaction, and
  Provider retries. Non-human rows must leave every authority field empty.
- `source_message_id` plus `channel_binding_generation` retain the external
  event and Bot-binding identity. `channel_context` is a bounded, structured
  read-only background snapshot; Provider projection wraps it as untrusted
  context, and it cannot grant tools or change the persisted profile.
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
    attachment_refs   JSONB        NOT NULL DEFAULT '[]'::jsonb,
    sender_id         TEXT         NOT NULL,
    sender_display_name TEXT,
    sender_classification TEXT      NOT NULL CHECK (
                                      sender_classification IN
                                      ('owner', 'allowed_non_owner', 'internal')
                                  ),
    ingress_tool_profile TEXT       NOT NULL CHECK (
                                      ingress_tool_profile IN
                                      ('owner_full', 'message_only')
                                  ),
    source_message_id TEXT,
    channel_binding_generation UUID,
    channel_context   JSONB        NOT NULL DEFAULT '[]'::jsonb
                                   CHECK (jsonb_typeof(channel_context) = 'array'),
    effort            TEXT         CHECK (
                          effort IS NULL
                          OR effort IN ('off', 'low', 'medium', 'high', 'xhigh', 'max')
                      ),
    received_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CHECK (
        (sender_classification IN ('owner', 'internal')
         AND ingress_tool_profile = 'owner_full') OR
        (sender_classification = 'allowed_non_owner'
         AND ingress_tool_profile = 'message_only')
    )
);

CREATE INDEX IF NOT EXISTS idx_pending_messages_session_received
    ON pending_messages(session_id, received_at, id);
CREATE INDEX IF NOT EXISTS idx_pending_messages_session_key_received
    ON pending_messages(session_key, received_at, id);
```

- `pending_messages` stores inbound user messages that are durable but not yet provider-visible. Every row carries the same sender/profile/source/binding/context authority fields that will be copied into its canonical human row. Before preflight, a continuation captures only the oldest consecutive prefix with one `ingress_tool_profile`; a later row with a different profile stays pending for a separate Turn. This prevents an owner row and an allow-listed non-owner row from sharing one Provider/tool authority boundary. Browser HTTP stream ownership remains process-local and keyed to captured message IDs. `effort` is nullable; `NULL` and `off` send `thinking.type=disabled`; non-off values send `thinking.type=adaptive` plus Anthropic `output_config.effort`.
- `attachment_refs` is copied unchanged into the promoted human `messages` row. It is returned in pending/history APIs but excluded from Provider messages; its separately persisted path marker is sent to the Provider and omitted from public `content` projections.
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
    tool_profile        TEXT         NOT NULL CHECK (
                                      tool_profile IN ('owner_full', 'message_only')
                                  ),
    input_message_ids   JSONB        NOT NULL DEFAULT '[]'::jsonb
                                      CHECK (jsonb_typeof(input_message_ids) = 'array'),
    failed_delivery_targets JSONB    NOT NULL DEFAULT '[]'::jsonb
                                      CHECK (jsonb_typeof(failed_delivery_targets) = 'array'),
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
- `tool_profile` and the ordered `input_message_ids` freeze the authority of
  the captured Pending prefix for the whole ReAct chain. The Provider sees the
  full owner tool catalog for `owner_full` and only the restricted text
  `message` projection for `message_only`; dispatch repeats the same gate before
  any side effect. `failed_delivery_targets` prevents another platform issue to
  the same target in that Turn after a failed or unknown delivery.
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

## 9. `channel_message_receipts` — external event idempotency

```sql
CREATE TABLE IF NOT EXISTS channel_message_receipts (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id          UUID         REFERENCES sessions(id) ON DELETE SET NULL,
    channel             TEXT         NOT NULL CHECK (channel IN ('discord', 'dingtalk')),
    binding_generation  UUID         NOT NULL,
    chat_id             TEXT         NOT NULL,
    source_message_id   TEXT         NOT NULL,
    disposition         TEXT         NOT NULL CHECK (
                                     disposition IN (
                                         'context', 'context_omitted', 'trigger',
                                         'attachment_rejected'
                                     )
                                 ),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_channel_receipt_source UNIQUE (
        user_id, channel, binding_generation, chat_id, source_message_id
    )
);
```

The unique source identity makes repeated Gateway/Stream callbacks
idempotent across process restarts and distinguishes events received through a
replacement Bot binding. Trigger and selected/omitted history rows commit with
the Pending handoff. A non-owner attachment-only rejection also records one
receipt before its one policy delivery. Session deletion preserves the receipt
with `session_id=NULL`, so the same platform event cannot recreate a deleted
conversation by redelivery.

---

## 10. `channel_deliveries` — durable logical delivery outcomes

```sql
CREATE TABLE IF NOT EXISTS channel_deliveries (
    id                    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id               UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id            UUID         REFERENCES sessions(id) ON DELETE CASCADE,
    turn_id               UUID         REFERENCES turn_runs(id) ON DELETE SET NULL,
    assistant_message_id  UUID         REFERENCES messages(id) ON DELETE SET NULL,
    tool_use_id           TEXT,
    delivery_key          TEXT         NOT NULL,
    origin                TEXT         NOT NULL CHECK (
                                       origin IN (
                                           'final', 'message_tool', 'policy_notice',
                                           'pairing_confirmation'
                                       )
                                   ),
    channel               TEXT         NOT NULL CHECK (channel IN ('discord', 'dingtalk')),
    chat_id               TEXT         NOT NULL,
    binding_generation    UUID         NOT NULL,
    status                TEXT         NOT NULL CHECK (
                                       status IN (
                                           'prepared', 'attempting', 'sent', 'partial',
                                           'failed', 'unknown'
                                       )
                                   ),
    total_actions         INTEGER      NOT NULL CHECK (
                                       total_actions >= 0 AND total_actions <= 32
                                   ),
    visible_sent_actions  INTEGER      NOT NULL DEFAULT 0 CHECK (
                                       visible_sent_actions >= 0
                                       AND visible_sent_actions <= total_actions
                                   ),
    last_error_code       TEXT,
    last_error_message    TEXT,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    started_at            TIMESTAMPTZ,
    finished_at           TIMESTAMPTZ,
    CONSTRAINT uq_channel_delivery_key UNIQUE (user_id, delivery_key)
);

CREATE INDEX IF NOT EXISTS idx_channel_deliveries_status
    ON channel_deliveries(status);
```

`delivery_key` is the stable logical idempotency key for a complete final,
`message` tool result, policy notice, or pairing confirmation. The Router first
persists `prepared`, commits each action as `attempting` before platform issue,
then records the terminal aggregate. `partial` means at least one visible action
was sent before a later action failed or became unknown. A process restart
closes leftover `prepared` as `failed` and leftover `attempting` as `unknown`;
neither state is replayed. Failed/unknown targets are fenced for the rest of the
same Turn, and only a new inbound user Turn may make another delivery attempt.

---

## 11. `channel_delivery_actions` — ordered platform issue facts

```sql
CREATE TABLE IF NOT EXISTS channel_delivery_actions (
    id                   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id          UUID         NOT NULL REFERENCES channel_deliveries(id)
                                      ON DELETE CASCADE,
    action_index         INTEGER      NOT NULL CHECK (
                                      action_index >= 0 AND action_index < 32
                                  ),
    action_kind          TEXT         NOT NULL CHECK (
                                      action_kind IN (
                                          'text_message', 'file_upload', 'file_message'
                                      )
                                  ),
    visible              BOOLEAN      NOT NULL,
    status               TEXT         NOT NULL CHECK (
                                      status IN (
                                          'prepared', 'attempting', 'sent', 'failed',
                                          'unknown', 'skipped'
                                      )
                                  ),
    platform_message_id  TEXT,
    last_error_code      TEXT,
    last_error_message   TEXT,
    started_at           TIMESTAMPTZ,
    finished_at          TIMESTAMPTZ,
    CONSTRAINT uq_channel_delivery_action_index UNIQUE (delivery_id, action_index)
);
```

Adapters receive the complete logical reply and create at most 32 ordered
actions using their platform text/file limits. Actions execute serially; the
first `failed` or `unknown` action makes every remaining action `skipped`.
There is no delivery worker or automatic retry loop. The browser projects the
logical and per-action states into external read-only history.

---

## 12. `devices` — per-user client devices

```sql
CREATE TABLE IF NOT EXISTS devices (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id            UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name               TEXT         NOT NULL CHECK (char_length(name) <= 64 AND name ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND name <> 'server'),
    workspace_path     TEXT         NOT NULL,
    restrict_to_workspace BOOLEAN   NOT NULL DEFAULT TRUE,
    shell_timeout_max  INTEGER      NOT NULL DEFAULT 600 CHECK (shell_timeout_max BETWEEN 0 AND 86400),
    ssrf_denylist      JSONB        NOT NULL DEFAULT
        '["0.0.0.0/8","10.0.0.0/8","100.64.0.0/10","127.0.0.0/8","169.254.0.0/16","172.16.0.0/12","192.0.0.0/29","192.0.0.8/32","192.0.0.11/32","192.0.0.12/30","192.0.0.16/28","192.0.0.32/27","192.0.0.64/26","192.0.0.128/25","192.0.2.0/24","192.88.99.2/32","192.168.0.0/16","198.18.0.0/15","198.51.100.0/24","203.0.113.0/24","224.0.0.0/4","240.0.0.0/4","::/128","::1/128","::ffff:0.0.0.0/96","64:ff9b:1::/48","100::/64","100:0:0:1::/64","2001::/32","2001:1::/128","2001:1::4/126","2001:1::8/125","2001:1::10/124","2001:1::20/123","2001:1::40/122","2001:1::80/121","2001:1::100/120","2001:1::200/119","2001:1::400/118","2001:1::800/117","2001:1::1000/116","2001:1::2000/115","2001:1::4000/114","2001:1::8000/113","2001:1::1:0/112","2001:1::2:0/111","2001:1::4:0/110","2001:1::8:0/109","2001:1::10:0/108","2001:1::20:0/107","2001:1::40:0/106","2001:1::80:0/105","2001:1::100:0/104","2001:1::200:0/103","2001:1::400:0/102","2001:1::800:0/101","2001:1::1000:0/100","2001:1::2000:0/99","2001:1::4000:0/98","2001:1::8000:0/97","2001:1::1:0:0/96","2001:1::2:0:0/95","2001:1::4:0:0/94","2001:1::8:0:0/93","2001:1::10:0:0/92","2001:1::20:0:0/91","2001:1::40:0:0/90","2001:1::80:0:0/89","2001:1::100:0:0/88","2001:1::200:0:0/87","2001:1::400:0:0/86","2001:1::800:0:0/85","2001:1::1000:0:0/84","2001:1::2000:0:0/83","2001:1::4000:0:0/82","2001:1::8000:0:0/81","2001:1:0:0:1::/80","2001:1:0:0:2::/79","2001:1:0:0:4::/78","2001:1:0:0:8::/77","2001:1:0:0:10::/76","2001:1:0:0:20::/75","2001:1:0:0:40::/74","2001:1:0:0:80::/73","2001:1:0:0:100::/72","2001:1:0:0:200::/71","2001:1:0:0:400::/70","2001:1:0:0:800::/69","2001:1:0:0:1000::/68","2001:1:0:0:2000::/67","2001:1:0:0:4000::/66","2001:1:0:0:8000::/65","2001:1:0:1::/64","2001:1:0:2::/63","2001:1:0:4::/62","2001:1:0:8::/61","2001:1:0:10::/60","2001:1:0:20::/59","2001:1:0:40::/58","2001:1:0:80::/57","2001:1:0:100::/56","2001:1:0:200::/55","2001:1:0:400::/54","2001:1:0:800::/53","2001:1:0:1000::/52","2001:1:0:2000::/51","2001:1:0:4000::/50","2001:1:0:8000::/49","2001:1:1::/48","2001:1:2::/47","2001:1:4::/46","2001:1:8::/45","2001:1:10::/44","2001:1:20::/43","2001:1:40::/42","2001:1:80::/41","2001:1:100::/40","2001:1:200::/39","2001:1:400::/38","2001:1:800::/37","2001:1:1000::/36","2001:1:2000::/35","2001:1:4000::/34","2001:1:8000::/33","2001:2::/32","2001:4::/40","2001:4:100::/44","2001:4:110::/47","2001:4:113::/48","2001:4:114::/46","2001:4:118::/45","2001:4:120::/43","2001:4:140::/42","2001:4:180::/41","2001:4:200::/39","2001:4:400::/38","2001:4:800::/37","2001:4:1000::/36","2001:4:2000::/35","2001:4:4000::/34","2001:4:8000::/33","2001:5::/32","2001:6::/31","2001:8::/29","2001:10::/28","2001:40::/26","2001:80::/25","2001:100::/24","2001:db8::/32","2002::/16","3fff::/20","5f00::/16","fc00::/7","fe80::/10","ff00::/8"]'::jsonb,
    env_allowlist      JSONB        NOT NULL DEFAULT
        '["PATH","HOME","LANG","TERM","SystemRoot","ComSpec","PATHEXT","TEMP","TMP","USERPROFILE"]'::jsonb,
    mcp_servers        JSONB        NOT NULL DEFAULT '[]'::jsonb,
    mcp_catalog        JSONB        NOT NULL DEFAULT
        '{"version":1,"digest":"d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf","servers":[]}'::jsonb,
    config_revision    BIGINT       NOT NULL DEFAULT 1 CHECK (config_revision >= 1),
    token_hash         BYTEA        NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    token_hint         TEXT         NOT NULL,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_devices_user_id ON devices(user_id);
```

- `id` is the immutable internal device identity. `token_hash` is the unique credential lookup key (ADR-131), stored as the 32-byte SHA-256 digest of the bearer token; the server never stores plaintext. `token_hint` is the non-secret prefix/suffix hint generated at issuance; REST returns the plaintext token only from create/regenerate responses.
- `name` is the REST/tool-routing canonical slug. It is UNIQUE per user, so the URL `PATCH /api/devices/laptop/config` resolves to `(user_id, "laptop")` without ever touching the token. Raw create/rename input is canonicalized server-side: NFC normalize, trim, ASCII-lowercase, convert whitespace runs to a single hyphen, then require `^[a-z0-9]+(-[a-z0-9]+)*$`. Stored names are at most 64 characters and use only lowercase ASCII letters, digits, and hyphens. The literal name `server` is reserved for OpenOctopus's built-in server install site and is rejected for user devices after canonicalization.
- `restrict_to_workspace` — when true, OpenOctopus-resolved Client file paths and exec/PTY initial `working_dir` must stay inside `workspace_path`; when false, absolute paths outside it are allowed. Relative paths still resolve from the workspace in both modes. This is application path policy, not an OS sandbox: shell commands and stdio MCP retain the Device user's filesystem and network access.
- `ssrf_denylist` — JSONB array of CIDRs, hosts, or `host:port` entries rejected by client-site `web_fetch`, independent of `restrict_to_workspace`. Omitted create input uses the private/reserved/metadata default; explicit `[]` permits internal targets. Server-site `web_fetch` reads its separate admin `system_config.web_fetch_denylist`.
- `shell_timeout_max` and `env_allowlist` configure client-only exec.
  `shell_timeout_max` is the per-device hard process lifetime cap; zero permits
  only explicitly bounded `timeout=0` calls. `env_allowlist` is an exact-name
  parent-environment allowlist and every `OPENOCTOPUS_*` name is rejected.
  There is no command denylist or exec network filter.
- `mcp_servers` — authoritative Py7 Device MCP configuration for at most 16
  uniquely named stdio, Streamable HTTP, or SSE servers. Stdio `env` and remote
  `headers` are deliberately stored as reversible plaintext in PostgreSQL;
  REST responses preserve the keys but replace every value with
  `"<redacted>"`.
- `mcp_catalog` — the complete bounded last-good discovery snapshot for tools,
  static resources, resource templates, and prompts. It contains disabled
  entries and immutable UUIDv7 route identities, but no MCP secret values.
  Provider schemas are built from this durable catalog even while a Device is
  offline; dispatch then returns `tool_device_unreachable`.
- `config_revision` — monotonically increases for every effective Device
  config or name change. MCP config and its matching last-good catalog commit
  atomically at the same revision.
- **Online state is in-memory only** — no `online` / `last_seen_at` columns; the connected-WS registry keyed by immutable device ID is the source of truth. The `Device` API response computes `online` on demand. Three device states per ADR-110: state-1 (online, in-registry), state-2 (offline-but-paired, row exists, not in registry — listed in `openoctopus_device` enum so the agent can still attempt and fail loudly), state-3 (deleted — row gone, in-memory entry gone, live WS force-closed, tool registry invalidated; complete wipe with no soft-delete tombstone).
- **No inbound FKs reference `devices`** from other tables. This keeps token regeneration and device deletion local to one row. State-3 transition is a single-row DELETE; cascades from `users.id` are the only path that takes multiple device rows out at once (account deletion). If a future milestone adds durable tables that reference devices, revisit ADR-091 before adding those FKs.
- `workspace_path` default is the literal string `~/openoctopus/workspace` on every OS (ADR-111) when omitted from `POST /api/devices`. Explicit overrides and PATCH updates must be non-empty strings and are stored verbatim. The server does not expand `~` or check client disk existence; the client expands `~` against its own home dir and creates/reports the path at startup or config-update time.

---

## 13. `workspaces` — shared workspace registry (ADR-108)

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

## 14. `workspace_members` — shared workspace allow-list (ADR-108)

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

## 15. `workspace_deletions` — durable RustFS cleanup intents

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

## 16. `cron_jobs` — scheduled agent invocations

```sql
CREATE TABLE IF NOT EXISTS cron_jobs (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT         NOT NULL,
    schedule_kind   TEXT         NOT NULL
                                 CHECK (schedule_kind IN ('every', 'cron', 'at')),
    schedule_value  TEXT         NOT NULL,
    timezone        TEXT,
    message         TEXT         NOT NULL,
    last_fired_at   TIMESTAMPTZ,
    next_fire_at    TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CHECK (
        (schedule_kind = 'every' AND timezone IS NULL)
        OR (schedule_kind IN ('cron', 'at') AND timezone IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_cron_jobs_user_id ON cron_jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_next_fire
    ON cron_jobs(next_fire_at, id);
```

- `name` — short user-facing label. The cron tool defaults it from the first 30 characters of the message; REST callers may provide it explicitly.
- `schedule_kind` — one of `every`, `cron`, or `at`; it also determines
  whether an accepted fire advances the row or deletes it as a one-shot.
- `schedule_value` — canonical decimal seconds, canonical five-field Cron
  expression, or UTC RFC3339 instant respectively.
- `timezone` — `NULL` for `every`; otherwise the effective validated IANA zone
  used to interpret/project `cron` and `at` schedules. Aware one-shots without
  an explicit zone store `UTC`.
- `message` — the agent-facing instruction the scheduler injects into the cron session as a synthesized user message when the job fires.
- `next_fire_at` — authoritative UTC instant for the scheduler index. The
  shared write service computes it before commit; recurring fires and busy
  skips advance it to a strictly future boundary without duration drift.
- Job UUID is also the stable Cron Session UUID, but there is deliberately no
  Session FK. The Session uses `channel='cron'`, `chat_id=<job UUID>`, and
  `session_key='cron:<job UUID>'`, and is created only on an accepted fire.
- Creating/updating validates exactly one timing form, `every_seconds` in
  `60..31536000`, a standard five-field Cron expression, timezone/DST rules,
  and a future one-shot. Invalid schedules never enter the table.
- An accepted fire updates/deletes this row and writes its synthetic
  PendingMessage/TurnRun in one transaction. Deleting a job preserves any
  Session/history; deleting the Session preserves the job and permits later
  JIT recreation with the same UUID.
- **No `kind` column** — heartbeat is a tick loop, not a cron row, and Dream is deferred (ADR-055, ADR-092).

---

## Constraints summary

- Every user-referencing FK has `ON DELETE CASCADE` (ADR-058). Account deletion
  is an application transaction because it also records durable RustFS cleanup
  intents. **Sole user-FK exception:** `workspaces.created_by` uses `ON DELETE
  SET NULL` per ADR-108 so a workspace persists for its remaining members.
- Channel receipts use `sessions.id ON DELETE SET NULL` to preserve external
  event idempotency after history deletion. Delivery `turn_id` and
  `assistant_message_id` also use `SET NULL` so their terminal transport facts
  survive narrower transcript cleanup; a delivery tied directly to a deleted
  Session and all of its action rows cascade.
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
| `uq_channel_receipt_source` | channel_message_receipts (UNIQUE on source identity) | Deduplicate one external event within one Bot binding. |
| `uq_channel_delivery_key` | channel_deliveries (UNIQUE on `(user_id, delivery_key)`) | Durable logical-delivery at-most-once identity. |
| `idx_channel_deliveries_status` | channel_deliveries (`status`) | Startup reconciliation of incomplete delivery rows. |
| `uq_channel_delivery_action_index` | channel_delivery_actions (UNIQUE on `(delivery_id, action_index)`) | One ordered outcome row per planned action. |
| `idx_devices_user_id` | devices | List user's devices. |
| `devices_user_id_name_key` | devices (UNIQUE on `(user_id, name)`) | URL resolution `/api/devices/{name}`. |
| `workspace_members_pkey` | workspace_members (PK on `(workspace_id, user_id)`) | Membership lookup at workspace-fs entry. |
| `idx_workspace_members_user` | workspace_members | Per-user "list my workspaces" for system-prompt rebuild. |
| `idx_cron_jobs_user_id` | cron_jobs | List user's cron jobs. |
| `idx_cron_jobs_next_fire` | cron_jobs (`next_fire_at, id`) | Stable scheduler due scan. |

---

## Extensions

- `uuid-ossp` or `pgcrypto` for `gen_random_uuid()` — `pgcrypto` is built-in on most PostgreSQL distributions and is the default choice.
- No other extensions in v1.
