# plexus-server API Reference

All endpoints return JSON. Errors use a standard `{ "error": { "code": "...", "message": "..." } }` envelope.

Auth header for protected routes: `Authorization: Bearer <jwt_token>`

## Auth

### POST /api/auth/register

Create a new user account. Pass `admin_token` to create an admin user.

```json
// Request
{ "email": "user@example.com", "password": "hunter2", "admin_token": "optional-admin-token" }

// Response 200
{ "token": "eyJ...", "user_id": "uuid", "is_admin": false }
```

**Errors:** `409 Conflict` if email already registered.

### POST /api/auth/login

```json
// Request
{ "email": "user@example.com", "password": "hunter2" }

// Response 200
{ "token": "eyJ...", "user_id": "uuid", "is_admin": false }
```

**Errors:** `401 Unauthorized` on bad credentials.

---

## Devices

### POST /api/device-tokens  (auth required)

Create a device token for connecting a plexus-client.

```json
// Request
{ "device_name": "my-laptop" }

// Response 200
{ "token": "plexus_dev_a1b2c3...", "device_name": "my-laptop" }
```

**Errors:** `409 Conflict` if device name already exists for this user.

### GET /api/device-tokens  (auth required)

List all device tokens for the current user.

```json
// Response 200
[
  { "token": "plexus_dev_...", "device_name": "my-laptop", "created_at": "2025-01-01T00:00:00Z" }
]
```

### DELETE /api/device-tokens/{token}  (auth required)

Hard-delete a device token. Gone forever.

```json
// Response 200
{ "message": "Token deleted" }
```

**Errors:** `404 Not Found` if token does not exist or does not belong to user.

### GET /api/devices  (auth required)

List all registered devices with live status (online/offline, tool count).

```json
// Response 200
[
  {
    "device_name": "my-laptop",
    "status": "online",
    "last_seen_secs_ago": 5,
    "tools_count": 12,
    "fs_policy": { "mode": "sandbox" }
  }
]
```

### GET /api/devices/{device_name}/policy  (auth required)

Get filesystem policy for a device.

```json
// Response 200
{ "device_name": "my-laptop", "fs_policy": { "mode": "sandbox" } }
```

### PATCH /api/devices/{device_name}/policy  (auth required)

Update filesystem policy. Change takes effect on the next heartbeat cycle.

```json
// Request
{ "fs_policy": { "mode": "unrestricted" } }

// Response 200
{ "device_name": "my-laptop", "fs_policy": { "mode": "unrestricted" } }
```

### GET /api/devices/{device_name}/mcp  (auth required)

Get MCP server config for a device.

```json
// Response 200
{ "device_name": "my-laptop", "mcp_servers": [] }
```

### PUT /api/devices/{device_name}/mcp  (auth required)

Update MCP server config for a device. Pushed on next heartbeat.

```json
// Request
{ "mcp_servers": [{ "name": "my-mcp", "command": "npx", "args": ["-y", "my-mcp-server"] }] }

// Response 200
{ "device_name": "my-laptop", "mcp_servers": [...] }
```

---

## Sessions

### GET /api/sessions  (auth required)

List all sessions for the current user.

```json
// Response 200
[
  { "session_id": "uuid", "created_at": "2025-01-01T00:00:00Z" }
]
```

### DELETE /api/sessions/{session_id}  (auth required)

Delete a session and all its messages.

```json
// Response 200
{ "message": "Session deleted" }
```

**Errors:** `404 Not Found` if session does not exist or does not belong to user.

### GET /api/sessions/{session_id}/messages  (auth required)

Paginated message history for a session.

**Query params:** `limit` (default 50, max 500), `offset` (default 0).

```json
// Response 200
[
  {
    "message_id": "uuid",
    "role": "user",
    "content": "hello",
    "tool_call_id": null,
    "tool_name": null,
    "tool_arguments": null,
    "created_at": "2025-01-01T00:00:00Z"
  }
]
```

---

## User

### GET /api/user/profile  (auth required)

```json
// Response 200
{ "user_id": "uuid", "email": "user@example.com", "is_admin": false, "created_at": "..." }
```

### GET /api/user/soul  (auth required)

Get the user's custom system prompt (soul).

```json
// Response 200
{ "soul": "You are a helpful assistant..." }
```

### PATCH /api/user/soul  (auth required)

```json
// Request
{ "soul": "You are a sarcastic pirate..." }

// Response 200
{ "message": "Soul updated" }
```

### GET /api/user/memory  (auth required)

Get persistent memory text (4K char cap).

```json
// Response 200
{ "memory": "User prefers dark mode. Project deadline is Friday." }
```

### PATCH /api/user/memory  (auth required)

```json
// Request
{ "memory": "Updated memory text" }

// Response 200
{ "message": "Memory updated" }
```

**Errors:** `422 Validation Failed` if memory exceeds 4096 characters.

---

## Admin

All admin endpoints require `is_admin: true` in JWT claims.

### GET /api/admin/default-soul  (admin)

Get the server-wide default soul.

```json
// Response 200
{ "default_soul": "You are PLEXUS..." }
```

### PUT /api/admin/default-soul  (admin)

```json
// Request
{ "soul": "You are PLEXUS, a distributed AI agent..." }

// Response 200
{ "message": "Default soul updated" }
```

### GET /api/admin/rate-limit  (admin)

```json
// Response 200
{ "rate_limit_per_min": 30 }
```

A value of `0` means unlimited.

### PUT /api/admin/rate-limit  (admin)

```json
// Request
{ "rate_limit_per_min": 30 }

// Response 200
{ "message": "Rate limit updated", "rate_limit_per_min": 30 }
```

---

## Skills

Skills are per-user isolated — each user has their own skill set. Skills can be installed via the web UI, the API, or by the agent itself (via the `install_skill` server tool).

### GET /api/skills  (auth required)

List current user's skills (metadata only).

```json
// Response 200
{ "skills": [{ "skill_id": "...", "name": "my-skill", "description": "Does cool stuff", "always_on": false, "skill_path": "..." }] }
```

### POST /api/skills  (auth required)

Create or update a skill by uploading its SKILL.md content.

```json
// Request
{ "name": "my-skill", "content": "---\nname: My Skill\ndescription: Does cool stuff\n---\n\nInstructions here..." }

// Response 200
{ "skill_id": "uuid", "name": "My Skill", "description": "Does cool stuff", "always_on": false }
```

**Errors:** `422 Validation Failed` if name contains invalid characters.

### POST /api/skills/install  (auth required)

Install a skill from a GitHub repo (fetches `SKILL.md` from the repo root).

```json
// Request
{ "repo": "owner/repo", "branch": "main" }

// Response 200
{ "skill_id": "uuid", "name": "repo-skill", "description": "...", "always_on": false, "source": "owner/repo" }
```

### DELETE /api/skills/{name}  (auth required)

Delete a skill and its files.

```json
// Response 200
{ "message": "Skill deleted" }
```

---

## Cron

### GET /api/cron-jobs  (auth required)

```json
// Response 200
{ "cron_jobs": [{
  "job_id": "abc12345",
  "name": "daily-report",
  "enabled": true,
  "cron_expr": "0 9 * * *",
  "every_seconds": null,
  "timezone": "UTC",
  "message": "Generate the daily report",
  "channel": "webui",
  "chat_id": "",
  "delete_after_run": false,
  "next_run_at": "...",
  "last_run_at": null,
  "run_count": 0
}] }
```

### POST /api/cron-jobs  (auth required)

Schedule options are mutually exclusive: `cron_expr`, `every_seconds`, or `at` (RFC 3339 one-shot).

```json
// Request
{
  "message": "Check server health",
  "cron_expr": "*/30 * * * *",
  "channel": "webui",
  "name": "health-check",
  "timezone": "UTC",
  "delete_after_run": false
}

// Response 201
{ "job_id": "abc12345" }
```

### PATCH /api/cron-jobs/{job_id}  (auth required)

Enable/disable or change the message.

```json
// Request
{ "enabled": false, "message": "Updated instruction" }

// Response 200
{ "message": "Cron job updated" }
```

### DELETE /api/cron-jobs/{job_id}  (auth required)

```
// Response 204 No Content  (empty body)

// Response 404 Not Found  — job does not exist, or belongs to a different user
// Response 403 Forbidden  — job has kind='system' (managed by the server; not user-deletable)
```

System-kind cron jobs (e.g. Plan D's per-user "dream" job) cannot be deleted through this endpoint or via the `cron` server tool; both paths return the same refusal shape. Ownership mismatch returns 404 rather than 403 to avoid leaking the existence of other users' job IDs.

---

## Files

### POST /api/files  (auth required)

Multipart file upload. Field name must be `file`. Max 25MB.

```
Content-Type: multipart/form-data
```

```json
// Response 200
{ "file_id": "uuid", "file_name": "report.pdf" }
```

### GET /api/files/{file_id}  (auth required)

Download a file. Returns binary with appropriate `Content-Type` and `Content-Disposition: attachment` headers. Serves from user uploads or shared media directory.

**Errors:** `404 Not Found`, `422 Validation Failed` on path traversal attempt.

---

## Server MCP (admin)

### GET /api/server-mcp  (admin)

Get server-side MCP server configuration.

```json
// Response 200
{ "mcp_servers": [{ "name": "...", "command": "...", "args": [...] }] }
```

### PUT /api/server-mcp  (admin)

Update and reinitialize server MCP servers.

```json
// Request
{ "mcp_servers": [{ "name": "web-search", "command": "npx", "args": ["-y", "mcp-web-search"] }] }

// Response 200
{ "mcp_servers": [...] }
```

---

## LLM Config (admin)

### GET /api/llm-config  (admin)

Returns current LLM config with masked API key.

```json
// Response 200 (configured)
{ "model": "gpt-4o", "api_key": "sk-abc1...wxyz", "api_base": "https://api.openai.com/v1", "context_window": 204800 }

// Response 200 (not configured)
{ "status": "not_configured", "message": "LLM provider has not been configured yet..." }
```

### PUT /api/llm-config  (admin)

Update LLM provider config at runtime. Persisted to DB.

```json
// Request
{ "api_base": "https://api.openai.com/v1", "model": "gpt-4o", "api_key": "sk-...", "context_window": 128000 }

// Response 200
{ "message": "LLM config updated" }
```

Only `api_base` is required. `model`, `api_key`, and `context_window` are optional (preserves existing values if omitted). Default context window: 204800.

---

## Discord Config

### POST /api/discord-config  (auth required)

Create or update Discord bot config for the current user.

```json
// Request
{ "bot_token": "MTIz...", "allowed_users": ["discord_user_id_1"], "owner_discord_id": "123456789" }

// Response 200
{ "user_id": "uuid", "enabled": true, "allowed_users": ["discord_user_id_1"], "owner_discord_id": "123456789" }
```

### GET /api/discord-config  (auth required)

```json
// Response 200
{ "user_id": "uuid", "bot_user_id": "bot_id", "enabled": true, "allowed_users": ["..."] }
```

### DELETE /api/discord-config  (auth required)

```json
// Response 200
{ "message": "Discord config deleted" }
```

---

## Telegram Config

### POST /api/telegram-config  (auth required)

Create or update Telegram bot config for the current user. Starts the bot immediately on success.

```json
// Request
{ "bot_token": "123456:ABC...", "partner_telegram_id": "987654321", "allowed_users": ["user_id_1"], "group_policy": "mention" }

// Response 200
{ "user_id": "uuid", "enabled": true, "partner_telegram_id": "987654321", "allowed_users": ["user_id_1"], "group_policy": "mention" }
```

`group_policy` defaults to `"mention"` if omitted. Options: `"mention"` (respond only when @mentioned in groups), `"all"` (respond to all messages in groups).

### GET /api/telegram-config  (auth required)

```json
// Response 200
{ "user_id": "uuid", "enabled": true, "partner_telegram_id": "987654321", "allowed_users": ["..."], "group_policy": "mention" }
```

**Errors:** `404 Not Found` if Telegram not configured.

### DELETE /api/telegram-config  (auth required)

Stops the bot and removes config.

```json
// Response 200
{ "message": "Telegram config deleted" }
```

---

## WebSocket

### GET /ws

Client device WebSocket connection. Not JWT-authenticated -- uses device token handshake.

**Handshake flow:**
1. Server sends `RequireLogin { message }`
2. Client sends `SubmitToken { token, protocol_version: "1.0" }`
3. Server verifies token, sends `LoginSuccess { user_id, device_name, fs_policy, mcp_servers }` or `LoginFailed { reason }`

**Message loop:**
- Client sends `Heartbeat { hash, status }` -- server replies `HeartbeatAck { fs_policy, mcp_servers }`
- Client sends `RegisterTools { schemas }` -- registers tool schemas
- Server sends `ExecuteToolRequest { request_id, tool_name, arguments }` -- client executes and replies with `ToolExecutionResult`
- File transfer: `FileUploadResponse`, `FileDownloadResponse`

Heartbeat interval: 15s (`HEARTBEAT_INTERVAL_SEC` constant). Timeout: 4 missed heartbeats = 60s (built-in constant, not configurable).

---

## Workspace API

All endpoints require a valid user JWT. Paths in the `path` query param are relative to `{WORKSPACE_ROOT}/{user_id}/`. Traversal attempts return 403 Forbidden. Quota overflows return 413 Payload Too Large (or 422 ValidationFailed, per current ApiError mapping).

### GET /api/workspace/quota

Returns the user's current workspace usage and total quota.

**Response:** `{ "used_bytes": number, "total_bytes": number }` (200 OK)

### GET /api/workspace/tree

Returns a flat list of the user's workspace entries. Directories first, then alphabetical.

**Response:** `[{ "path": string, "is_dir": bool, "size_bytes": number, "modified_at": "RFC3339" }]` (200 OK)

### GET /api/workspace/file?path={path}

Streams the file's bytes. Content-Type sniffed from extension.

**Response:** raw bytes (200 OK), or 403/404 on error.

### PUT /api/workspace/file?path={path}

Body is raw bytes. Parent dirs are created as needed. Quota-checked.

**Response:** 204 No Content on success; 403/422/500 on failure.

### DELETE /api/workspace/file?path={path}&recursive={bool}

Deletes the file (or directory if `recursive=true`).

**Response:** 204 No Content; 422 if directory without recursive; 403 on traversal.

### POST /api/workspace/upload (multipart/form-data)

Accepts one or more files under any form-field name. Each is saved at `uploads/{date}-{hash}-{filename}`.

**Response:** `[{ "path": string, "size_bytes": number }]` — failed files appear with `path="ERROR:{filename}"` + `size_bytes: 0`.

### GET /api/workspace/skills

Returns the user's skills as parsed frontmatter. Reuses the server's SkillsCache.

**Response:** `[{ "name": string, "description": string, "always_on": bool }]`.
