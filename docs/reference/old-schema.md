# plexus-server Database Schema

PostgreSQL. All tables created by `db::init_db` on startup (idempotent `CREATE TABLE IF NOT EXISTS`).

---

## users

Core user accounts.

| Column | Type | Description |
|---|---|---|
| `user_id` | `TEXT PRIMARY KEY` | UUID v4 |
| `email` | `TEXT UNIQUE NOT NULL` | Login identifier |
| `password_hash` | `TEXT NOT NULL DEFAULT ''` | bcrypt hash |
| `is_admin` | `BOOLEAN DEFAULT FALSE` | Admin flag |
| `display_name` | `TEXT` | Optional display name (nullable). Added via migration. |
| `timezone` | `TEXT NOT NULL DEFAULT 'UTC'` | User's local timezone (IANA name). Used by heartbeat phase 1 for time-of-day gating. Added via migration (Plan A). |
| `last_dream_at` | `TIMESTAMPTZ` (nullable) | Timestamp of the most recent dream pass for this user. NULL means never dreamed. Advanced before Phase 1 runs to prevent refire during LLM latency. See ADR-35. Added via migration (Plan D). |
| `last_heartbeat_at` | `TIMESTAMPTZ` (nullable) | Timestamp of the most recent heartbeat tick for this user. NULL means the user has never fired; the tick loop treats this as "due immediately". Advanced *before* Phase 1 runs to prevent refire during LLM latency. See ADR-36. Added via migration (Plan E). |
| `created_at` | `TIMESTAMPTZ DEFAULT NOW()` | |

**Read by:** `auth/mod.rs` (login), `context.rs` (display_name, timezone), `api.rs` (profile)
**Written by:** `auth/mod.rs` (register), `api.rs` (update display_name), `dream.rs` / `heartbeat.rs` (advance cursors)

---

## device_tokens

Device authentication and per-device configuration.

| Column | Type | Description |
|---|---|---|
| `token` | `TEXT PRIMARY KEY` | `plexus_dev_` + 32 hex chars |
| `user_id` | `TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE` | Owner. Cascades on user deletion (AD-1). |
| `device_name` | `TEXT NOT NULL` | Human-readable name |
| `fs_policy` | `JSONB NOT NULL DEFAULT '{"mode":"sandbox"}'` | Filesystem access policy |
| `mcp_config` | `JSONB NOT NULL DEFAULT '[]'` | MCP server entries for this device |
| `workspace_path` | `TEXT NOT NULL DEFAULT '~/.plexus/workspace'` | Client-side root workspace path sent to client on connect |
| `shell_timeout` | `BIGINT NOT NULL DEFAULT 60` | Shell command timeout in seconds sent to client on connect |
| `ssrf_whitelist` | `JSONB NOT NULL DEFAULT '[]'` | Per-device SSRF allowlist (array of URL prefixes) |
| `created_at` | `TIMESTAMPTZ DEFAULT NOW()` | |

**Indexes:**
- `idx_device_tokens_user_device` -- `UNIQUE (user_id, device_name)` -- one token per device name per user

**Read by:** `ws.rs` (token verification, policy/MCP on login and heartbeat), `api.rs` (list devices), `auth/device.rs` (CRUD)
**Written by:** `auth/device.rs` (create/delete token, update policy/MCP)

---

## sessions

Conversation sessions. One per chat context (web UI tab, Discord channel, cron job).

| Column | Type | Description |
|---|---|---|
| `session_id` | `TEXT PRIMARY KEY` | UUID or `cron:{job_id}` |
| `user_id` | `TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE` | Owner. Cascades on user deletion (AD-1). |
| `created_at` | `TIMESTAMPTZ DEFAULT NOW()` | |

**Read by:** `auth/mod.rs` (list sessions), `db/sessions.rs` (get messages, ownership check)
**Written by:** `agent_loop.rs` (ensure_session on first message), `auth/mod.rs` (delete)

---

## messages

All messages in all sessions (user, assistant, tool results).

| Column | Type | Description |
|---|---|---|
| `message_id` | `TEXT PRIMARY KEY` | UUID v4 |
| `session_id` | `TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE` | Parent session. Cascades when session is deleted (AD-1). |
| `role` | `TEXT NOT NULL` | `user`, `assistant`, or `tool` |
| `content` | `TEXT NOT NULL` | Message text |
| `tool_call_id` | `TEXT` | Links tool result to tool call |
| `tool_name` | `TEXT` | Tool name (assistant role with tool calls) |
| `tool_arguments` | `TEXT` | JSON string of tool arguments |
| `compressed` | `BOOLEAN DEFAULT FALSE` | Marked true after context compression |
| `created_at` | `TIMESTAMPTZ DEFAULT NOW()` | |

**Indexes:**
- `idx_messages_session` -- `(session_id, created_at)` -- efficient session history retrieval

**Read by:** `db/messages.rs` (session history reconstruction, session messages API)
**Written by:** `agent_loop.rs` (save_message for user/assistant/tool), `memory.rs` (mark_messages_compressed)

History reconstruction (`get_session_history`): merges consecutive assistant rows with `tool_name` into a single assistant message with a `tool_calls` array. Excludes rows where `compressed = TRUE`.

---

## discord_configs

Per-user Discord bot configuration.

| Column | Type | Description |
|---|---|---|
| `user_id` | `TEXT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE` | One config per user. Cascades on user deletion (AD-1). |
| `bot_token` | `TEXT NOT NULL` | Discord bot token |
| `bot_user_id` | `TEXT` | Bot's own Discord user ID (set after first connection) |
| `partner_discord_id` | `TEXT` | Discord user ID of the designated partner (for security boundaries) |
| `enabled` | `BOOLEAN DEFAULT TRUE` | |
| `allowed_users` | `TEXT[] DEFAULT '{}'` | Discord user IDs allowed to interact |
| `created_at` | `TIMESTAMPTZ DEFAULT NOW()` | |
| `updated_at` | `TIMESTAMPTZ DEFAULT NOW()` | |

**Read by:** `channels/discord/` (get all enabled configs, get by user_id)
**Written by:** `auth/discord_config.rs` (upsert/delete), `channels/discord/` (update bot_user_id)

---

## system_config

Key-value store for server-wide settings.

| Column | Type | Description |
|---|---|---|
| `key` | `TEXT PRIMARY KEY` | Config key |
| `value` | `TEXT NOT NULL` | Config value (JSON or plain text) |
| `updated_at` | `TIMESTAMPTZ DEFAULT NOW()` | |

**Known keys** (seeded by `seed_defaults_if_missing` unless noted):

- `default_soul` -- text: server-wide default system prompt (SOUL.md template). Seeded from `templates/workspace/SOUL.md`.
- `default_memory` -- text: default MEMORY.md template seeded into each user's workspace on first registration. Seeded from `templates/workspace/MEMORY.md`.
- `default_heartbeat` -- text: default HEARTBEAT.md template seeded into each user's workspace on first registration. Seeded from `templates/workspace/HEARTBEAT.md`.
- `heartbeat_phase1_prompt` -- text: system prompt injected for heartbeat Phase 1 LLM evaluation. Seeded from `templates/prompts/heartbeat_phase1.md`.
- `heartbeat_interval_seconds` -- integer as string, default `1800` (30 min). Controls how often the heartbeat tick loop checks each user.
- `dream_enabled` -- boolean as string, default `"true"`. Global kill switch for the dream subsystem (Plan D).
- `workspace_quota_bytes` -- integer as string, default `5368709120` (5 GiB). Per-user workspace quota enforced by the workspace API.
- `dream_phase1_prompt` -- text: optional admin override for dream Phase 1 LLM prompt. Not seeded; consumed at boot from DB if present.
- `dream_phase2_prompt` -- text: optional admin override for dream Phase 2 LLM prompt. Not seeded; consumed at boot from DB if present.
- `llm_config` -- JSON: `{ api_base, model, api_key, context_window }`. Admin-set via API.
- `server_mcp_config` -- JSON: array of MCP server entries. Admin-set via API.
- `rate_limit_per_min` -- integer as string, `0` = unlimited. Consumed by `bus::check_rate_limit`.

**Read by:** `main.rs` (startup load), `bus.rs` (rate limit), `dream.rs` (dream_enabled), `heartbeat.rs` (heartbeat_interval_seconds), `context.rs` (default soul cache), `auth/admin.rs`, `workspace/registration.rs`
**Written by:** `auth/admin.rs` (LLM config, rate limit, server MCP), `api.rs` (default soul), `seed_defaults_if_missing` (first-boot defaults)

---

## cron_jobs

Scheduled tasks that inject prompts into the agent loop.

| Column | Type | Description |
|---|---|---|
| `job_id` | `TEXT PRIMARY KEY` | First 8 chars of UUID v4 |
| `user_id` | `TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE` | Owner. Cascades on user deletion (AD-1). |
| `name` | `TEXT NOT NULL` | Human-readable label |
| `kind` | `TEXT NOT NULL DEFAULT 'user' CHECK (kind IN ('user', 'system'))` | Job kind. `system` jobs (e.g. dream) are visible to the user but not user-deletable. Added by Plan A/C. |
| `enabled` | `BOOLEAN DEFAULT TRUE` | |
| `cron_expr` | `TEXT` | Standard 5-field crontab expression (converted to 7-field internally) |
| `every_seconds` | `INTEGER` | Interval-based scheduling (alternative to cron_expr) |
| `timezone` | `TEXT DEFAULT 'UTC'` | |
| `message` | `TEXT NOT NULL` | Prompt injected when job fires |
| `channel` | `TEXT NOT NULL` | Target channel (e.g., `webui`, `discord`) |
| `chat_id` | `TEXT NOT NULL` | Target chat/conversation ID |
| `delete_after_run` | `BOOLEAN DEFAULT FALSE` | One-shot: delete after first execution |
| `deliver` | `BOOLEAN DEFAULT TRUE` | Whether to deliver the result to the target channel after execution |
| `next_run_at` | `TIMESTAMPTZ` | Next scheduled fire time |
| `last_run_at` | `TIMESTAMPTZ` | |
| `claimed_at` | `TIMESTAMPTZ` | Set atomically when a worker claims the job; prevents double-fire under concurrent ticks. Added via migration. |
| `last_status` | `TEXT` | Result of the most recent execution (e.g. `ok`, error string). Added via migration. |
| `run_count` | `INTEGER DEFAULT 0` | |
| `created_at` | `TIMESTAMPTZ DEFAULT NOW()` | |

**Indexes:**
- `cron_jobs_system_name_uq` -- `UNIQUE (user_id, name) WHERE kind = 'system'` -- partial unique index preventing TOCTOU duplicate system jobs (Plan C, I-1 fix)

**Read by:** `cron.rs` (get_due_cron_jobs every 10s), `auth/cron_api.rs` (list)
**Written by:** `auth/cron_api.rs` (create/update/delete), `db/cron.rs` (update after run), `server_tools/cron.rs`

---

## telegram_configs

Per-user Telegram bot configuration.

| Column | Type | Description |
|---|---|---|
| `user_id` | `TEXT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE` | One config per user. Cascades on user deletion (AD-1). |
| `bot_token` | `TEXT NOT NULL` | Telegram bot token |
| `partner_telegram_id` | `TEXT` | Telegram user ID of the designated partner (nullable) |
| `enabled` | `BOOLEAN DEFAULT TRUE` | |
| `allowed_users` | `TEXT[] DEFAULT '{}'` | Telegram user IDs allowed to interact |
| `group_policy` | `TEXT NOT NULL DEFAULT 'mention'` | How the bot responds in group chats (`mention` = only when @-mentioned) |
| `created_at` | `TIMESTAMPTZ DEFAULT NOW()` | |
| `updated_at` | `TIMESTAMPTZ DEFAULT NOW()` | |

**Read by:** `channels/telegram/` (get all enabled configs, get by user_id)
**Written by:** `auth/telegram_api.rs` (upsert/delete)

---

## Migrations

All migrations are idempotent (`ADD COLUMN IF NOT EXISTS`, `CREATE UNIQUE INDEX IF NOT EXISTS`, `DROP COLUMN IF EXISTS`, `DROP TABLE IF EXISTS`). Applied in order by `create_tables` at every startup:

- `ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT`
- `ALTER TABLE users ADD COLUMN IF NOT EXISTS timezone TEXT NOT NULL DEFAULT 'UTC'`
- `ALTER TABLE users ADD COLUMN IF NOT EXISTS last_dream_at TIMESTAMPTZ`
- `ALTER TABLE users ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ`
- `ALTER TABLE cron_jobs ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ`
- `ALTER TABLE cron_jobs ADD COLUMN IF NOT EXISTS last_status TEXT`
- `ALTER TABLE cron_jobs ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'user'`
- `CREATE UNIQUE INDEX IF NOT EXISTS cron_jobs_system_name_uq ON cron_jobs (user_id, name) WHERE kind = 'system'`
- `ALTER TABLE users DROP COLUMN IF EXISTS memory_text` (A-17: dropped; skills live on disk)
- `ALTER TABLE users DROP COLUMN IF EXISTS soul` (A-17: dropped; soul lives at `{workspace}/{user_id}/SOUL.md`)
- `DROP TABLE IF EXISTS skills` (A-17: skills table replaced by filesystem layout `{workspace}/{user_id}/skills/`)
- FK cascade migration: `device_tokens`, `sessions`, `messages`, `discord_configs`, `cron_jobs`, `telegram_configs` all have their `user_id` / `session_id` FKs re-created with `ON DELETE CASCADE` (AD-1, account deletion)
