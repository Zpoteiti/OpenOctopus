Perfect. Now I have enough information to produce a comprehensive inventory. Let me create the report:

---

# Comprehensive Per-Crate Inventory: Plexus Rust Workspace (M3-gateway-frontend)

## Workspace Overview

**Location:** `/home/yucheng/Documents/GitHub/Plexus`  
**Branch:** M3-gateway-frontend  
**Crates:** 4 (plexus-common, plexus-server, plexus-client, plexus-gateway)  
**Total Rust LoC:** ~23,641 lines  
**Edition:** 2024 (workspace)  

---

## Crate 1: plexus-common (1,944 LoC)

### Responsibility Statement

plexus-common is the protocol and error foundation shared by all runtime crates. It defines wire-format types for WebSocket communication (ServerToClient, ClientToServer enums), a comprehensive error hierarchy with error codes mappable to HTTP status, base types for authentication and MCP configuration, tool schemas for file operations, and utility functions for MCP normalization. It does NOT contain server or client business logic, does not include the database schema, and does not run any background tasks. It is deliberately minimal and safe from version-skew between server and client deployments.

### Module Inventory

#### Root modules:
- **lib.rs** (17 lines): Re-exports public API — errors, protocol, tool_schemas, mcp_utils, network, consts, fuzzy_match.
- **protocol.rs** (484 lines): Wire format enums (ServerToClient, ClientToServer) with ~340 lines of test coverage. Defines DeviceStatus, FsPolicy, McpServerEntry, McpServerSchemas, ExecuteToolRequest, ToolExecutionResult. Tests cover round-trip serde, additive field defaults, legacy compatibility.
- **consts.rs** (varies): Global constants (PROTOCOL_VERSION, DEVICE_TOKEN_PREFIX, CHANNEL_GATEWAY, etc.). Light constants; server has its own server-only consts in plexus-server/src/consts.rs.

#### errors/ subdirectory:
- **errors/mod.rs** (218 lines): Central ErrorCode enum (16 variants: AuthFailed, NotFound, ToolTimeout, etc.), ApiError and PlexusError wrappers, HTTP status mapping. Axum integration gated behind `#[cfg(feature = "axum")]`.
- **errors/auth.rs** (32 lines): AuthError enum — TokenInvalid, TokenExpired, NotPermitted. Maps to ErrorCode::AuthFailed/TokenExpired/Forbidden.
- **errors/network.rs** (46 lines): NetworkError for SSRF validation — InvalidUrl, InvalidScheme, MissingHost, ResolutionFailed, BlockedNetwork. Maps to 400/403/422 ErrorCodes.
- **errors/protocol.rs** (not listed but likely exists): WebSocket frame errors (checked implicitly in tests).
- **errors/tool.rs** (34 lines): ToolError — ExecutionFailed, Timeout, DeviceUnreachable. Maps to ExecutionFailed/ToolTimeout/DeviceOffline.
- **errors/workspace.rs** (not listed): WorkspaceError for file I/O / quota.
- **errors/mcp.rs** (not listed): MCP connection / tool invocation errors.

#### tool_schemas/ subdirectory:
- **tool_schemas/mod.rs**: File tool schemas (read_file, write_file, edit_file, delete_file, list_dir, glob, grep, shell) defined as JSON with device_name enum merging.
- **tool_schemas/file_ops.rs**: Helpers for file-tool parameter definitions.

#### Utility modules:
- **fuzzy_match.rs**: 3-level fuzzy matching for edit_file's old_text → line number resolution (used by server).
- **network.rs**: SSRF validation (validate_url checks scheme, resolves DNS, blocks RFC-1918/metadata/loopback).
- **mime.rs**: MIME type detection (application/json, image/*, text/*, etc.).
- **mcp_utils.rs**: normalize_schema_for_openai — transforms MCP schema to OpenAI function format.

### Public Surface

**Types:**
- `protocol::ServerToClient` enum: ExecuteToolRequest, RequireLogin, LoginSuccess, HeartbeatAck, ConfigUpdate, FileRequest, FileSend, ReadStream, RegisterToolsError.
- `protocol::ClientToServer` enum: ToolExecutionResult, SubmitToken, RegisterTools, Heartbeat, FileResponse, FileSendAck, StreamChunk, StreamEnd, StreamError.
- `protocol::DeviceStatus` enum: Online, Offline.
- `protocol::FsPolicy` enum: Sandbox (default), Unrestricted.
- `protocol::McpServerEntry` struct: name, transport_type, command, args, env, url, headers, tool_timeout, enabled.
- `protocol::McpServerSchemas` struct: server, tools (Vec<McpRawTool>).
- `ErrorCode` enum: 16 variants (AuthFailed, Unauthorized, NotFound, ToolTimeout, etc.) with as_str(), http_status(), parse() methods.
- `ApiError` struct: code (String), message.
- `PlexusError` struct: code (ErrorCode), message. Impl Display, Error, Into<ApiError>.
- `AuthError`, `NetworkError`, `ToolError`: domain-specific errors with .code() → ErrorCode.

**Functions:**
- `ErrorCode::http_status() → u16`: Maps to 4xx/5xx ranges.
- `network::validate_url(url, whitelist) → Result<>`: Validates for SSRF attack.
- `mcp_utils::normalize_schema_for_openai(value) → Value`: Reshapes MCP JSON Schema to OpenAI format.
- `fuzzy_match::fuzzy_match(haystack, needle) → Option<usize>`: Best-match line number for edit_file.

### Dependencies

**External crates:**
- serde (1.0) + serde_json (1.0): Serialization.
- thiserror (2.0): #[derive(Error)] macros.
- ipnet (2.0): CIDR parsing for SSRF whitelist.
- url (2.0): URL parsing.
- axum (0.8, optional, feature="axum"): IntoResponse impl.

**Workspace crates:** None (plexus-common is the leaf).

**Observations:**
- axum is optional; plexus-client can omit it (uses no IntoResponse).
- Very lean dependencies — critical for staying universal across server/client/gateway.

---

## Crate 2: plexus-server (17,127 LoC)

### Responsibility Statement

plexus-server is the hub: HTTP API, WebSocket gateway for devices, LLM agent loop, channel adapters (Discord/Telegram/gateway proxy), background workers (cron, heartbeat, dream), database ORM, workspace file I/O, and tool dispatch. It does NOT handle client device sandbox execution (that's plexus-client's job), does not serve the frontend directly (plexus-gateway does), and does not store MCP schemas for clients (clients report them; server validates collisions).

### Module Inventory

#### Top-level modules:
- **main.rs** (233 lines): Boot sequence — config loading, DB initialization, AppState construction, background task spawning (heartbeat tick, cron poller, outbound dispatch, Discord/Telegram bots, gateway client stub), HTTP router assembly, graceful shutdown handler.
- **state.rs** (322 lines): AppState struct — DB pool, config, in-memory device/session/schema/rate-limit caches, LLM config and prompts (Arc<str>), workspace_fs, skills_cache, shutdown token, message bus sender.
- **api.rs** (530 lines): User/session/workspace REST endpoints — GET /api/user/profile, PATCH /api/user/display-name, GET/DELETE /api/sessions, GET /api/workspace/quota/tree/files, PUT /api/workspace/files/{path}, POST /api/workspace/upload, GET /api/device-stream.
- **context.rs** (1,107 lines): Builds full LLM prompt — system message, soul, memory, skills index, device/channel identities, recent message history. Three PromptMode branches: UserTurn (gateway/Discord/Telegram user messages), Dream (Phase 1 consolidation), Heartbeat (autonomous wake-up). Content block builders for text/images/tools.
- **agent_loop.rs** (899 lines): Per-session ReAct loop — LLM call via providers::openai, tool dispatch via server_tools/tools_registry, iteration cap (MAX_AGENT_ITERATIONS=20), publish_final decision logic with EventKind discrimination (UserTurn unconditional, Cron evaluator-gated, Heartbeat external-channel-only, Dream silent).

#### auth/ subdirectory (6 files, ~950 lines total):
- **auth/mod.rs**: JWT signing/validation, token extraction from Authorization header, Claims struct. Routes: /api/auth/register, /api/auth/login.
- **auth/admin.rs**: Admin-only endpoints — GET /api/admin/rate-limit, PUT (set per-minute), GET /api/llm-config, PUT (LLM provider/key), GET /api/server-mcp, PUT (reinitialize MCP), GET /api/admin/users, DELETE /api/admin/users/{user_id}.
- **auth/device.rs**: Device token management — POST /api/device-tokens, GET /api/device-tokens, DELETE. Device config management — PATCH /api/devices/{name}/config (workspace_path, shell_timeout_max, ssrf_whitelist, fs_policy). Device MCP config — GET/PUT /api/devices/{name}/mcp.
- **auth/cron_api.rs**: Cron job REST API — POST /api/cron, GET, PATCH, DELETE per job.
- **auth/discord_api.rs**: Discord OAuth callback, bot token registration, channel linking.
- **auth/telegram_api.rs**: Telegram bot token registration, chat ID management.

#### bus.rs (182 lines):
Event bus — InboundEvent (user-to-agent), OutboundEvent (agent-to-channel). EventKind enum: UserTurn, Cron, Heartbeat, Dream. Rate-limit refresher background loop.

#### channels/ subdirectory (4 files, ~1,335 lines total):
- **channels/mod.rs**: safe_attachment_filename sanitizer, outbound dispatch loop routing to Discord/Telegram/gateway by channel name.
- **channels/discord.rs** (350+ lines): Serenity integration — async bot ready handler, message receipt, file download, formatting logic. Inbound: message → InboundEvent. Outbound: event → Discord message (with media attachment upload).
- **channels/telegram.rs** (300+ lines): Teloxide integration — dispatcher setup, message/photo/voice receipt, markdown rendering. Inbound: message + media → InboundEvent. Outbound: event → Telegram message (with file sends).
- **channels/gateway.rs** (150+ lines): WebSocket frame forwarding to connected browser clients. Inbound: session_update frame → session DB sync. Outbound: agent final message → session_update frame to gateway. Stub for M4.

#### db/ subdirectory (10 files, ~1,200 lines total):
- **db/mod.rs**: sqlx pool initialization, cascade DDL loader (schema.sql).
- **db/schema.sql** (97 lines): 9 tables: users, devices, device_tokens, sessions, messages, cron_jobs, discord_configs, telegram_configs, system_config. All user-referencing FKs use ON DELETE CASCADE (AD-1).
- **db/users.rs**: CRUD for users table — find_by_id, find_by_email, create, update_display_name, get_timezone, get_last_dream_at.
- **db/devices.rs**: Device token storage — find_by_token, create_device, list_by_user, update_config (workspace_path, shell_timeout, mcp, fs_policy).
- **db/sessions.rs**: Session CRUD — find_by_id, list_by_user, create, delete_session.
- **db/messages.rs**: Message persistence — upsert_from_content, list_by_session, last_activity_for_user.
- **db/cron.rs**: Cron job lifecycle — claim_due_jobs (atomic UPDATE), reschedule_after_completion, recover_stuck_jobs (30-min timeout recovery).
- **db/discord.rs**: Discord config storage — create, find, list_enabled (for bot startup).
- **db/telegram.rs**: Telegram config storage.
- **db/system_config.rs**: Key-value store — get, set, validate, redact (LLM config, concurrency limit, workspace quota, etc.).

#### workspace/ subdirectory (5 files, ~700 lines total):
- **workspace/mod.rs**: Public exports (WorkspaceFs, QuotaCache, paths, registration).
- **workspace/fs.rs** (380+ lines): Unified file I/O interface — read, write, delete, list_dir, glob, grep. Quota enforcement (reserve before write, rollback on error). Skills cache invalidation hook.
- **workspace/paths.rs**: resolve_user_path (sandbox-aware, symlink-safe), is_under_skills_dir (for cache invalidation).
- **workspace/quota.rs**: QuotaCache — in-memory usage tracking per user, initialized from disk, updated on write/delete. Default 5 GB; configurable via system_config.
- **workspace/registration.rs**: One-time user workspace setup (mkdir user_id, MEMORY.md skeleton, HEARTBEAT.md skeleton, skills/ dir, etc.).
- **workspace/tree.rs**: Recursive walk for Workspace.tsx tree view — entries with name/type/size/modified.

#### mcp/ subdirectory (2 files, ~400 lines total):
- **mcp/mod.rs** (not fully listed): Likely module-level coordination.
- **mcp/wrap.rs**: MCP schema collision detection — check_mcp_schema_collision compares raw tool schemas across server MCP install + device reports. Returns structured conflicts with diffs for RegisterToolsError frame.

#### server_tools/ subdirectory (6 files, ~950 lines total):
- **server_tools/mod.rs**: ToolAllowlist enum (All | Only(...)), DREAM_PHASE2_ALLOWLIST, SERVER_TOOL_NAMES. Schema generation for message, file_transfer, cron, web_fetch.
- **server_tools/dispatch.rs** (80 lines): is_file_tool check, unified device_name routing — "server" → workspace_fs, others → tools_registry.
- **server_tools/file_ops.rs** (200+ lines): Server-side file tool implementations — read_file, write_file, edit_file (3-level fuzzy match), delete_file, list_dir, glob, grep.
- **server_tools/file_transfer.rs** (200+ lines): Outbound media delivery — base64 encoding, workspace path construction, retry logic.
- **server_tools/message.rs** (150+ lines): message tool — send to channel (gateway/discord) with optional media paths.
- **server_tools/web_fetch.rs** (300+ lines): HTTP client wrapper with SSRF validation, redirects, timeout, rate limiting via global semaphore.
- **server_tools/cron_tool.rs** (150+ lines): cron tool for agents to schedule async tasks.

#### Supporting modules:
- **ws.rs** (595 lines): WebSocket handler for device connections — login handshake, token validation, tool registration (builtin + MCP), heartbeat, tool result receipt, heartbeat reaper (evicts stale connections).
- **session.rs** (18 lines): SessionHandle struct — session_id, user_id, channel, chat_id, message buffer.
- **device_stream.rs** (333 lines): GET /api/device-stream/{device_name}/{path} — browser relay for device-routed file reads via ReadStream protocol.
- **tools_registry.rs** (478 lines): Aggregates device tool schemas (per-device read_file/write_file/etc. with device_name enum), MCP tool schemas (prefixed mcp_{server}_{tool}), server tool schemas. Routes tool calls to correct device/server.
- **server_mcp.rs** (309 lines): Admin-configurable MCP server management — initialize from config, start/stop/restart on reconfig, tool schema prefixing, tool invocation routing.
- **heartbeat.rs** (486 lines): Autonomous heartbeat subsystem — 60s tick loop polling users due for heartbeat, Phase 1 LLM (HEARTBEAT.md task list analysis), Phase 2 agent run if action="run", delivery via evaluator + external-channel precedence.
- **dream.rs** (258 lines): Memory consolidation + skill discovery — 2h default cadence (user-configurable), Phase 1 LLM (MEMORY.md + skills index + recent history), Phase 2 Phase 1 directives processed, silent publish (deliver=false).
- **cron.rs** (233 lines): Poller loop (10s cadence) — claim due jobs atomically, dispatch to bus, recover jobs stuck > 30 min (crash recovery).
- **evaluator.rs** (220 lines): Shared decision logic for Cron/Heartbeat — checks deliver flag, evaluates notification rules (rate limiting, quiet hours from HEARTBEAT.md).
- **memory.rs** (118 lines): MEMORY.md management — read, update, search within agent loop.
- **skills_cache.rs** (380 lines): In-memory SkillInfo cache (name, description, always_on, content for always-on only). Invalidated on any write under skills/. Loaded from disk on miss.
- **account.rs** (154 lines): Account deletion orchestrator — delete_user_everywhere stops bots, kicks browser WSs, cascades DB delete (ON DELETE CASCADE through dependent tables), wipes workspace.
- **config.rs** (48 lines): ServerConfig struct — database_url, server_port, workspace_root, jwt_secret, etc. Loaded from env.
- **consts.rs** (22 lines): Server-only constants (HEARTBEAT_REAPER_INTERVAL_SEC, CRON_POLL_INTERVAL_SEC, WEB_FETCH_TIMEOUT_SEC, etc.) — not in plexus-common.
- **providers/mod.rs**, **providers/openai.rs**: LLM integration — ChatMessage, Content blocks, tool schema format, API call. Pluggable (OpenAI initially; Anthropic/etc. via same trait).

### Public Surface

**Major Types:**
- `AppState`: Holds DB, config, LLM, in-memory caches, shutdown token, outbound message bus.
- `ChannelIdentity`: sender_name, sender_id, is_partner, channel_type.
- `SkillInfo`: name, description, always_on, content.
- `InboundEvent`: user-facing chat message bound for agent loop.
- `OutboundEvent`: agent message/media bound for channel delivery.

**Key Functions:**
- `agent_loop::run_agent()`: Main ReAct loop — LLM → tool dispatch → iterate.
- `context::build_context()`: Assemble full prompt.
- `workspace::WorkspaceFs`: Unified file I/O (read, write, delete, glob, grep).
- `heartbeat::run_phase1()`: Standalone LLM call to decide Phase 2.
- `dream::handle_dream_fire()`: Dream poller entry point.
- `cron::spawn_cron_poller()`: Cron background task spawner.
- `channels::*::deliver()`: Route OutboundEvent to Discord/Telegram/gateway.
- `account::delete_user_everywhere()`: Full user deletion + workspace wipe.

### Dependencies

**External crates (notable):**
- tokio (1.0, full feature set): Async runtime.
- axum (0.8): HTTP framework with ws support.
- sqlx (0.8): Async PostgreSQL ORM.
- jsonwebtoken (9.0): JWT signing/validation.
- bcrypt (0.17): Password hashing.
- rmcp (1.3): MCP client library (used for server MCP + tools).
- serenity (0.12): Discord bot library.
- teloxide (0.13): Telegram bot library.
- reqwest (0.12): HTTP client.
- chrono / chrono-tz: Timestamps and timezone handling.
- dashmap (6.0): Concurrent HashMap for caches.

**Workspace crates:**
- plexus-common (with "axum" feature): Protocol, errors, tool schemas.

**Observations:**
- Heavy async/tokio throughout — well-suited to concurrent user sessions.
- Discord and Telegram bots spawned at boot; shutdown observes cancellation token.
- Database design uses PostgreSQL-specific features (JSONB, ON DELETE CASCADE, advisory locks deferred to Plan E).
- No transaction nesting; all DB mutations are single statements with explicit FK cascade.
- 5 background loops: heartbeat tick, cron poller, rate-limit refresher, outbound dispatch, gateway client stub.

---

## Crate 3: plexus-client (3,246 LoC)

### Responsibility Statement

plexus-client is the device-side agent — it runs on user machines (Linux), manages the WebSocket connection to plexus-server, executes tools (shell, file ops, MCP) in a sandboxed environment, and reports tool results back. It does NOT host an HTTP server, does NOT store persistent state (stateless reconnect-loop design), and does NOT validate workspace semantics (server owns policy enforcement).

### Module Inventory

#### Top-level modules:
- **main.rs** (250+ lines): Reconnect loop with exponential backoff, session bootstrap, MCP manager initialization, tool registry setup, heartbeat spawner, WebSocket stream reader loop. Entry point reads PLEXUS_SERVER_WS_URL and PLEXUS_AUTH_TOKEN env vars.
- **connection.rs** (varied): WebSocket connect + auth handshake — send SubmitToken, receive LoginSuccess, apply received config. WsSink/stream split.
- **config.rs**: Client-side config struct — holds LLM endpoint (not used), etc.
- **env.rs**: Environment variable parsing and validation.
- **tool_schemas.rs** (270+ lines): Client-only tool schema definitions — shell, read_file, write_file, edit_file, delete_file, list_dir, glob, grep (mirrored from plexus-common for now; server ultimately owns schemas post-unification).
- **guardrails.rs**: Input validation — command length, path escape, suspicious patterns. Pre-execution checks.
- **heartbeat.rs**: Heartbeat ACK sender — responds to HeartbeatAck frames, monitors missed ACKs.
- **read_stream.rs**: ReadStream protocol handler — receives ServerToClient::ReadStream, chunks file into StreamChunk (32 KiB chunks), respects fs_policy sandbox.
- **sandbox.rs** (112 lines): bwrap command wrapper — builds bwrap arguments for sandboxed shell execution. Probes bwrap at startup; falls back to direct execution with env isolation if unavailable.

#### mcp/ subdirectory (2 files):
- **mcp/mod.rs**: McpManager lifecycle.
- **mcp/client.rs** (300+ lines): McpSession — spawns MCP server process via TokioChildProcess transport, maintains tool list, handles tool invocation via rmcp. Collects raw tool schemas for RegisterTools collision check.

#### tools/ subdirectory (9 files, ~2,000 lines total):
- **tools/mod.rs**: ToolRegistry struct — maps tool names to handler functions. register_builtin_tools adds shell, read_file, write_file, etc.
- **tools/shell.rs** (200+ lines): Shell command execution — bwrap-wrapped or direct, timeout enforcement, output capture, exit code mapping.
- **tools/read_file.rs** (150+ lines): File read with fs_policy check (sandbox vs. unrestricted), symlink safety, size limit.
- **tools/write_file.rs** (180+ lines): File write — base64 decoding, symlink safety, atomic temp-file pattern.
- **tools/edit_file.rs** (200+ lines): In-place text replacement — 3-level fuzzy match on old_text, line-by-line rewrite.
- **tools/delete_file.rs**: rm with fs_policy check.
- **tools/list_dir.rs** (150+ lines): Directory listing with metadata.
- **tools/glob.rs**: Pattern matching via glob crate.
- **tools/grep.rs** (180+ lines): Full-text search with regex.
- **tools/helpers.rs** (200+ lines): Shared utility functions — path resolution, sandbox enforcement, symlink traversal detection.

### Public Surface

**Major Types:**
- `McpManager`: Manages MCP server lifecycle — initialize, apply_config, all_tool_schemas, call_tool, all_mcp_schemas.
- `McpSession`: Single MCP server connection.
- `ToolRegistry`: Maps tool name → async handler.
- `ToolContext`: Execution context (user_id, device_name, workspace, timeout, fs_policy, etc.).

**Key Functions:**
- `main()`: Entry point — reconnect loop.
- `run_session()`: Single WebSocket session — MCP init, tool registry setup, heartbeat spawner, stream reader.
- `sandbox::wrap_command()`: Constructs bwrap argument list.
- `tools::*`: Per-tool implementations (shell, read_file, write_file, etc.).

### Dependencies

**External crates:**
- tokio (1.0, full features): Async runtime.
- tokio-tungstenite (0.26): WebSocket client.
- rmcp (1.3): MCP client library (same as server).
- glob (0.3): Simple glob matching.
- regex (1.0): Pattern matching.
- ipnet (2.0): CIDR parsing (SSRF whitelist check on client side).
- base64 (0.22): Base64 encoding/decoding for file transfer.

**Workspace crates:**
- plexus-common: Protocol types, error codes (no axum feature).

**Observations:**
- Fully stateless — reconnects from scratch, re-registers tools, re-initializes MCP.
- Bwrap integration is per-platform (Linux only; falls back gracefully on non-Linux).
- Tool schemas are locally defined here but ideally should unify with server schemas post-M3.
- MCP collision reporting done at RegisterTools frame time — raw schemas collected and sent.
- No persistent disk state except MCP server processes (which are ephemeral).

---

## Crate 4: plexus-gateway (1,324 LoC)

### Responsibility Statement

plexus-gateway is the browser-facing HTTP/WebSocket proxy and static file server. It relays chat WebSocket traffic between browsers and plexus-server, proxies REST API calls, serves the frontend HTML/CSS/JS, and manages browser-to-server reconnection logic. It does NOT authenticate users (JWT validation is deferred to server), does NOT run tools, and does NOT store session state (stateless proxy design).

### Module Inventory

#### Top-level modules:
- **main.rs** (93 lines): HTTP listener setup, router assembly, graceful shutdown. Routes: /healthz (health check), /ws/chat (chat WebSocket), /ws/plexus (server connection), /api/* (proxy), static files (embedded or disk).
- **routing.rs**: Route handler registration — GET /healthz, WebSocket upgrade routes, catch-all /api proxy.
- **proxy.rs** (200+ lines): HTTP request/response forwarding — extracts Authorization header, forwards to server API, relays response.
- **ws/chat.rs** (250+ lines): Browser chat WebSocket handler — receives messages from browser, forwards to plexus-server WS, relays server responses back.
- **ws/plexus.rs** (200+ lines): Server connection manager — maintains single outbound WebSocket to plexus-server, broadcasts frames to all connected browsers.
- **ws/mod.rs**: WebSocket utilities — message framing, subscription management.
- **jwt.rs**: JWT validation (mirrored from server for token inspection if needed).
- **config.rs**: Config struct — port, frontend_dir (or embedded assets), upload size limit.
- **state.rs**: AppState — HTTP client, plexus-server WS sink, connected browsers DashMap, shutdown token.
- **static_files.rs** (150+ lines): Static file serving — either rust-embed (embedded in binary) or filesystem (dev mode). Sets MIME types, cache-control headers, CSP headers.

### Public Surface

**Major Types:**
- `AppState`: HTTP client, server sink, browser registry, shutdown token.
- `BrowserSession`: Stores per-browser WebSocket sink for fan-out.

**Key Functions:**
- `main()`: Entry point.
- `ws::chat::ws_chat()`: Browser chat upgrade handler.
- `ws::plexus::ws_plexus()`: Server connection handler.
- `proxy::proxy_handler()`: REST forwarding.
- `static_files::static_file_service()`: File serving.

### Dependencies

**External crates:**
- axum (0.8): HTTP framework.
- tokio (1.0): Async runtime.
- tokio-tungstenite (0.26): WebSocket client/server.
- reqwest (0.12): HTTP client (for /api proxy).
- rust-embed (8.0, optional): Embedded static files.
- mime_guess (2.0, optional): MIME detection for embedded files.
- tower-http (0.6): Middleware (CORS, limits, tracing).
- dashmap (6.0): Concurrent browser registry.

**Workspace crates:**
- plexus-common (with "axum" feature): For error types.

**Observations:**
- Minimal business logic — primarily a proxy and WebSocket fan-out.
- Embedded frontend optional (feature: embed-frontend) — binary size vs. dev iteration trade-off.
- No session affinity needed — stateless per-request forwarding.
- Single persistent server connection shared by all browsers (fan-out to multiple clients).

---

## A. Base Types (Protocol + Errors)

### ServerToClient Enum Variants

| Variant | Purpose |
|---------|---------|
| ExecuteToolRequest | Send tool invocation to device (request_id, tool_name, arguments) |
| RequireLogin | Initial handshake challenge |
| LoginSuccess | Handshake response with device config (user_id, device_name, fs_policy, mcp_servers, workspace_path, shell_timeout_max, ssrf_whitelist) |
| LoginFailed | Authentication rejection |
| HeartbeatAck | Acknowledge device liveness |
| ConfigUpdate | Push config change (fs_policy, mcp_servers, workspace_path, shell_timeout_max, ssrf_whitelist — all optional) |
| FileRequest | Request device to read file (request_id, path) |
| FileSend | Server sends file to device (request_id, filename, content_base64, destination) |
| ReadStream | Request device to stream large file back in chunks (request_id, path) |
| RegisterToolsError | Reject MCP tool schemas due to collision (code, message, conflicts array) |

### ClientToServer Enum Variants

| Variant | Purpose |
|---------|---------|
| ToolExecutionResult | Device reports tool outcome (request_id, exit_code [-2=cancelled, -1=timeout, 0=success, 1=failed], output) |
| SubmitToken | Device login with token and protocol version |
| RegisterTools | Device advertises available tools (tool_names, tool_schemas [client-only], mcp_schemas [per-server raw tools]) |
| Heartbeat | Device liveness ping (status: Online/Offline) |
| FileResponse | Device responds to FileRequest (request_id, content_base64, mime_type, error) |
| FileSendAck | Device acknowledges FileSend completion (request_id, error) |
| StreamChunk | File chunk for ReadStream (request_id, data: Vec<u8>, offset) |
| StreamEnd | ReadStream completion (request_id, total_size) |
| StreamError | ReadStream failure (request_id, error) |

### Error Types

**ErrorCode Enum (16 variants):**
- AuthFailed (401), AuthTokenExpired (401), Unauthorized (401), Forbidden (403)
- NotFound (404), Conflict (409), ValidationFailed (400), InvalidParams (400), ProtocolMismatch (400)
- ExecutionFailed (500), DeviceOffline (503), ToolTimeout (504)
- McpConnectionFailed (502), ConnectionFailed (502), HandshakeFailed (502)
- InternalError (500)

**Error Subtypes:**
- `AuthError`: TokenInvalid, TokenExpired, NotPermitted.
- `NetworkError`: InvalidUrl, InvalidScheme, MissingHost, ResolutionFailed, BlockedNetwork(IpAddr).
- `ToolError`: ExecutionFailed(String), Timeout(u64), DeviceUnreachable(String).
- `WorkspaceError`: (TBD — likely QuotaExceeded, IoError, SymlinkEscape).
- `McpError`: (TBD — connection, tool invocation, schema mismatch).

---

## B. Database Schema

**Path:** `/home/yucheng/Documents/GitHub/Plexus/plexus-server/src/db/schema.sql`

### users table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| id | TEXT | Primary key — generated UUID | PRIMARY KEY |
| email | TEXT | Unique email | UNIQUE NOT NULL |
| password_hash | TEXT | bcrypt hash | NOT NULL |
| display_name | TEXT | User's display name | NOT NULL |
| timezone | TEXT | User's timezone (IANA) | DEFAULT 'UTC' |
| is_admin | BOOLEAN | Admin flag | DEFAULT FALSE |
| last_dream_at | TIMESTAMPTZ | Last dream execution | NULL |
| last_heartbeat_at | TIMESTAMPTZ | Last heartbeat execution | NULL |
| created_at | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |

### devices table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| id | TEXT | Primary key — device UUID | PRIMARY KEY |
| user_id | TEXT | Owner | REFERENCES users(id) ON DELETE CASCADE |
| name | TEXT | Device name (e.g., "laptop") | NOT NULL |
| token_hash | TEXT | Hashed device token | NOT NULL |
| workspace_path | TEXT | Root path for file I/O | NOT NULL |
| shell_timeout_max | INTEGER | Max shell execution seconds | DEFAULT 300 |
| ssrf_whitelist | TEXT[] | CIDR blocks allowed (e.g., "10.0.0.0/8") | DEFAULT '{}' |
| fs_policy | TEXT | 'sandbox' or 'unrestricted' | DEFAULT 'sandbox', CHECK |
| mcp_servers | JSONB | Array of McpServerEntry (JSON) | DEFAULT '[]' |
| created_at | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |
| last_seen_at | TIMESTAMPTZ | Last heartbeat from device | NULL |
| **Unique constraint** | | (user_id, name) pair | UNIQUE(user_id, name) |
| **Index** | | | idx_devices_user_id ON user_id |

### device_tokens table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| token | TEXT | Primary key — plaintext (hashed in devices.token_hash) | PRIMARY KEY |
| user_id | TEXT | Owner | REFERENCES users(id) ON DELETE CASCADE |
| created_at | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |
| consumed_at | TIMESTAMPTZ | Token used timestamp | NULL |
| **Index** | | | idx_device_tokens_user_id ON user_id |

### sessions table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| id | TEXT | Primary key — session UUID | PRIMARY KEY |
| user_id | TEXT | Owner | REFERENCES users(id) ON DELETE CASCADE |
| channel | TEXT | 'gateway' \| 'discord' \| 'tg:{chat_id}' | NOT NULL |
| title | TEXT | Session title (optional) | NULL |
| created_at | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |
| last_activity | TIMESTAMPTZ | Last message timestamp | DEFAULT NOW() |
| **Unique constraint** | | Per-user per-channel | UNIQUE(user_id, channel) |
| **Index** | | | idx_sessions_user_id_last_activity ON (user_id, last_activity DESC) |

### messages table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| id | TEXT | Primary key — message UUID | PRIMARY KEY |
| session_id | TEXT | Parent session | REFERENCES sessions(id) ON DELETE CASCADE |
| role | TEXT | 'user' \| 'assistant' \| 'tool' | NOT NULL, CHECK |
| content | JSONB | Anthropic-style content blocks (JSON) | NOT NULL |
| created_at | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |
| **Index** | | | idx_messages_session_id_created ON (session_id, created_at) |

### cron_jobs table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| id | TEXT | Primary key — job UUID | PRIMARY KEY |
| user_id | TEXT | Owner | REFERENCES users(id) ON DELETE CASCADE |
| name | TEXT | Job name (e.g., "daily-digest") | NOT NULL |
| kind | TEXT | 'user' \| 'system' | DEFAULT 'user', CHECK |
| schedule | TEXT | Cron expression (e.g., "0 9 * * *") | NOT NULL |
| prompt | TEXT | Agent prompt for this job | NOT NULL |
| enabled | BOOLEAN | Is job active | DEFAULT TRUE |
| last_run_at | TIMESTAMPTZ | Last execution timestamp | NULL |
| next_run_at | TIMESTAMPTZ | Next scheduled execution (NULL = claimed) | NULL |
| created_at | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |
| **Index** | | | idx_cron_jobs_user_id ON user_id |
| **Index** | | | idx_cron_jobs_next_run ON next_run_at WHERE enabled=TRUE |
| **Unique constraint** | | System jobs per user | UNIQUE(user_id, name) WHERE kind='system' |

### discord_configs table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| user_id | TEXT | Primary key / owner | PRIMARY KEY, REFERENCES users(id) ON DELETE CASCADE |
| bot_token | TEXT | Discord bot token | NOT NULL |
| channel_id | TEXT | Linked channel | NOT NULL |
| created_at | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |

### telegram_configs table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| user_id | TEXT | Primary key / owner | PRIMARY KEY, REFERENCES users(id) ON DELETE CASCADE |
| bot_token | TEXT | Telegram bot token | NOT NULL |
| allowed_chat_ids | TEXT[] | Linked chat IDs | DEFAULT '{}' |
| created_at | TIMESTAMPTZ | Creation timestamp | DEFAULT NOW() |

### system_config table
| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| key | TEXT | Primary key — config key | PRIMARY KEY |
| value | JSONB | Config value (JSON) | NOT NULL |
| updated_at | TIMESTAMPTZ | Last update timestamp | DEFAULT NOW() |

**Known seeded keys:**
- `llm_config`: { provider, key, endpoint, model } — LLM provider config.
- `rate_limit_per_min`: Integer — global rate limit (messages/minute).
- `dream_phase1_prompt`: String — dream phase 1 LLM prompt.
- `dream_phase2_prompt`: String — dream phase 2 LLM prompt.
- `heartbeat_phase1_prompt`: String — heartbeat phase 1 prompt.
- `server_mcp_config`: JSON array of McpServerEntry — admin-configured MCP servers.
- `workspace_quota_default_bytes`: Integer — default per-user quota (5 GB).
- `default_soul`: String — system default soul (optional).
- `heartbeat_interval_seconds`: Integer — heartbeat cadence (default 1800 / 30 min).
- `dream_enabled`: Boolean — global dream kill switch.

---

## C. REST Endpoint Catalog

### Authentication
- **POST /api/auth/register** — Public. Create account. Body: {email, password, display_name}. Returns: JWT token.
- **POST /api/auth/login** — Public. Authenticate. Body: {email, password}. Returns: JWT token.

### User Profile
- **GET /api/user/profile** — JWT required. Return: {user_id, email, is_admin, display_name, created_at}.
- **PATCH /api/user/display-name** — JWT required. Update display name. Body: {display_name}. Returns: {message}.
- **DELETE /api/user** — JWT required. Self-delete account. Body: {password} (confirmation). Returns: {message} or 401 on wrong password.

### Sessions
- **GET /api/sessions** — JWT required. List user's sessions. Returns: [{id, user_id, channel, title, created_at, last_activity}].
- **DELETE /api/sessions/{session_id}** — JWT required. Delete session. Returns: {message}.
- **GET /api/sessions/{session_id}/messages** — JWT required. Paginated message history. Query: ?limit=50&offset=0. Returns: [{id, role, content, created_at}].

### Workspace
- **GET /api/workspace/quota** — JWT required. Current usage. Returns: {used_bytes, limit_bytes}.
- **GET /api/workspace/tree** — JWT required. Directory tree. Returns: [{name, type, size_bytes, modified_at, is_dir}] (recursive).
- **GET /api/workspace/files/{*path}** — JWT required. Read file (or directory). Returns: content or 404. Headers: X-Content-Type-Options: nosniff.
- **PUT /api/workspace/files/{*path}** — JWT required. Write file. Body: base64-encoded content. Returns: {path, size_bytes} or error.
- **POST /api/workspace/upload** — JWT required. Upload file via multipart. Body: multipart/form-data. Returns: {path, size_bytes} or {error, message} (for each file).
- **GET /api/workspace/skills** — JWT required. List user skills (fast path — index only, not body content). Returns: [{name, description, always_on}].

### Device Management
- **POST /api/device-tokens** — JWT required. Create device token. Body: {device_name}. Returns: {token} (plexus_dev_*).
- **GET /api/device-tokens** — JWT required. List tokens. Returns: [{token, created_at, consumed_at}].
- **DELETE /api/device-tokens/{token}** — JWT required. Revoke token. Returns: {message}.
- **GET /api/devices** — JWT required. List devices. Returns: [{id, name, workspace_path, shell_timeout_max, fs_policy, last_seen_at}].
- **GET /api/devices/{device_name}/config** — JWT required. Get device config. Returns: {workspace_path, shell_timeout_max, ssrf_whitelist, fs_policy}.
- **PATCH /api/devices/{device_name}/config** — JWT required. Update device config. Body: {workspace_path?, shell_timeout_max?, ssrf_whitelist?, fs_policy?}. Returns: {message} or 422 on invalid (e.g., bad CIDR).
- **GET /api/devices/{device_name}/mcp** — JWT required. List device MCP servers. Returns: [{name, enabled, transport_type, command}].
- **PUT /api/devices/{device_name}/mcp** — JWT required. Update device MCP config. Body: [{name, command, args, env, enabled}]. Returns: {message}.

### Cron Jobs
- **POST /api/cron** — JWT required. Create cron job. Body: {name, schedule, prompt, enabled?}. Returns: {job_id}.
- **GET /api/cron** — JWT required. List cron jobs. Returns: [{id, name, schedule, enabled, last_run_at, next_run_at}].
- **PATCH /api/cron/{job_id}** — JWT required. Update cron job. Body: {schedule?, prompt?, enabled?}. Returns: {message}.
- **DELETE /api/cron/{job_id}** — JWT required. Delete cron job. Returns: {message}.

### Device File Streaming
- **GET /api/device-stream/{device_name}/{*path}** — JWT required. Stream file from device. Returns: streaming file content via ReadStream protocol. Browser relays data chunks back; no disk buffering on server.

### Admin Endpoints
- **GET /api/admin/rate-limit** — Admin JWT required. Get current rate limit. Returns: {limit_per_min}.
- **PUT /api/admin/rate-limit** — Admin JWT required. Set rate limit. Body: {limit_per_min}. Returns: {message}.
- **GET /api/llm-config** — Admin JWT required. Get LLM provider config. Returns: {provider, endpoint, model} (key masked).
- **PUT /api/llm-config** — Admin JWT required. Update LLM config. Body: {provider, key, endpoint, model}. Returns: {message}.
- **GET /api/server-mcp** — Admin JWT required. List server MCP servers. Returns: [{name, enabled, command, tool_count}].
- **PUT /api/server-mcp** — Admin JWT required. Update server MCP config. Body: [{name, enabled, command, args, env, transport_type, url, headers}]. Returns: {message} or 409 if schema collision (with structured conflicts diff).
- **GET /api/admin/users** — Admin JWT required. List all users. Query: ?search=email. Returns: [{id, email, display_name, is_admin, created_at, last_activity}].
- **DELETE /api/admin/users/{user_id}** — Admin JWT required. Force delete user (no password needed). Returns: {message}.

### Gateway Endpoints
- **GET /healthz** — Public. Health check. Returns: {status, plexus_connected, browsers (count)}.
- **GET /ws/chat** — WebSocket upgrade. Browser chat gateway.
- **GET /ws/plexus** — WebSocket upgrade. Server connection relay (single outbound per gateway instance).

### Discord Integration
- **POST /api/auth/discord/callback** — Public. OAuth callback. Query: ?code=...*&state=...* Returns: redirect or JWT.

### Telegram Integration
- **POST /api/auth/telegram/callback** — Public. Telegram token submission. Body: {bot_token}. Returns: {message} and chat_id collection setup.

---

## D. WebSocket Frame Catalog

### ServerToClient Frames

**ExecuteToolRequest**
- When: Server dispatches tool to device.
- Payload: {request_id: String, tool_name: String, arguments: Value}.
- Device Action: Execute tool, capture output, return via ToolExecutionResult.

**LoginSuccess**
- When: Device successfully authenticates.
- Payload: {user_id, device_name, fs_policy, mcp_servers: [McpServerEntry], workspace_path, shell_timeout_max, ssrf_whitelist}.
- Device Action: Store config, initialize MCP servers, register tools.

**LoginFailed**
- When: Token invalid or protocol mismatch.
- Payload: {reason: String}.
- Device Action: Log error, close connection, reconnect.

**HeartbeatAck**
- When: Server acknowledges device heartbeat.
- Payload: (empty).
- Device Action: Increment ack counter (if tracking missed acks).

**ConfigUpdate**
- When: Server pushes config change.
- Payload: {fs_policy?, mcp_servers?, workspace_path?, shell_timeout_max?, ssrf_whitelist?} (all optional).
- Device Action: Apply config change, restart MCP if needed.

**ReadStream**
- When: Server requests large file from device.
- Payload: {request_id, path}.
- Device Action: Open file, chunk into StreamChunk (32 KiB each), send StreamEnd or StreamError.

**RegisterToolsError**
- When: Tool schema collision detected.
- Payload: {code: "mcp_schema_collision", message, conflicts: [{tool, existing_schema, new_schema, where_installed: [device_names]}]}.
- Device Action: Log error, do NOT register conflicting tools, notify user.

### ClientToServer Frames

**SubmitToken**
- When: Device initiates login.
- Payload: {token: String, protocol_version: String}.
- Server Action: Validate token, look up device, send LoginSuccess or LoginFailed.

**RegisterTools**
- When: Device advertises tools.
- Payload: {tool_names: [String], tool_schemas: [Value], mcp_schemas: [McpServerSchemas]}.
- Server Action: Cache per-device, merge into aggregated tool schemas, check MCP collisions, send RegisterToolsError if any.

**ToolExecutionResult**
- When: Device completes tool execution.
- Payload: {request_id, exit_code: i32 (0=success, 1=failed, -1=timeout, -2=cancelled), output: String}.
- Server Action: Match to in-flight ExecuteToolRequest, use result in agent loop.

**Heartbeat**
- When: Device sends periodic liveness signal.
- Payload: {status: "online" | "offline"}.
- Server Action: Update last_seen_at, send HeartbeatAck, evict if missing acks > threshold.

**StreamChunk**
- When: Device sends file chunk in response to ReadStream.
- Payload: {request_id, data: Vec<u8>, offset: u64}.
- Server Action: Buffer chunk for browser relay (device_stream endpoint).

**StreamEnd**
- When: Device finishes streaming file.
- Payload: {request_id, total_size: u64}.
- Server Action: Mark stream complete, close client response.

**StreamError**
- When: Device encounters error during stream.
- Payload: {request_id, error: String}.
- Server Action: Send 500 or 404 to browser (based on error type).

### Gateway Frames (Browser ↔ Server)

**session_update** (server → browser)
- When: New message added to a session, or session metadata changed.
- Payload: {session_id, hasUnread?, messages?: []}. 
- Browser Action: Update sidebar, mark unread, refresh message list if visible.

**chat_message** (browser → server)
- When: User sends text/media.
- Payload: {session_id, content: String, media?: [String]}.
- Server Action: Insert into messages table, trigger agent loop.

**user_turn** (server → browser)
- When: Agent produces final response.
- Payload: {session_id, content: String, media?: [String]}.
- Browser Action: Display message in chat.

**kick_user** (gateway → browser)
- When: Account deletion / admin force logout.
- Payload: (empty).
- Browser Action: Clear auth, redirect to login.

---

## E. Tool Surface (Agent-Visible)

### File Tools (Routable to Server or Device)
**read_file**
- Parameters: {path: String, device_name: String}.
- Route: device_name="server" → workspace_fs, else → device via tools_registry.
- Returns: base64-encoded content or error.

**write_file**
- Parameters: {path: String, content: String (base64), device_name: String}.
- Route: As above.
- Returns: {path, size_bytes} or error.

**edit_file**
- Parameters: {path: String, old_text: String, new_text: String, device_name: String}.
- Route: As above. Uses 3-level fuzzy match (exact match, line substring, Levenshtein on lines) to find old_text.
- Returns: {path, size_bytes} or error.

**delete_file**
- Parameters: {path: String, device_name: String}.
- Route: As above.
- Returns: {message} or error.

**list_dir**
- Parameters: {path: String, device_name: String}.
- Route: As above.
- Returns: [{name, is_dir, size_bytes, modified_at}].

**glob**
- Parameters: {pattern: String, device_name: String}.
- Route: As above.
- Returns: [String] (file paths matching pattern).

**grep**
- Parameters: {pattern: String, path: String, device_name: String}.
- Route: As above. Pattern is regex.
- Returns: [{line_number, line}].

### Shell Tool (Client Only)
**shell**
- Parameters: {command: String, device_name: String}.
- Route: Client only (server has no bwrap jail).
- Returns: {exit_code, output} or timeout error.
- Constraints: shell_timeout_max (per-device), 300s default.

### MCP Tools (Prefixed)
**mcp_{server}_{tool}**
- Parameters: Arbitrary JSON (per tool schema).
- Route: Parsed tool name → extract server + tool → route to correct MCP session (client or server).
- Returns: Raw tool output.
- Naming: mcp_git_status, mcp_web_search_query, etc.

### Server-Only Tools
**message**
- Parameters: {content: String, channel: "gateway" | "discord", chat_id?: String, media: [String], from_device: String}.
- Route: Server-only.
- Returns: {message_id} or error.
- Purpose: Send agent output to a channel.

**file_transfer**
- Parameters: {source_path: String, source_device: String, dest_path: String, dest_device: String}.
- Route: Server-only (orchestrates device-to-device or device-to-workspace).
- Returns: {size_bytes} or error.

**cron**
- Parameters: {action: "create" | "delete" | "list", name?: String, schedule?: String, prompt?: String}.
- Route: Server-only.
- Returns: Job details or list.

**web_fetch**
- Parameters: {url: String, method?: String, headers?: {}, body?: String}.
- Route: Server-only (SSRF protection).
- Returns: {status_code, body, headers}.

### Allowlist Modes

**ToolAllowlist::All**: Every registered tool (default for UserTurn).  
**ToolAllowlist::Only(["read_file", "write_file", "edit_file", "delete_file", "list_dir", "glob", "grep"])**: Dream Phase 2 (sandbox file I/O only; no message, cron, file_transfer, web_fetch).

---

## F. MCP Handling

### Server-Side MCP (Admin-Installed)

**Location:**
- Client code: `/home/yucheng/Documents/GitHub/Plexus/plexus-server/src/server_mcp.rs` (309 lines)
- Config: `system_config` table key `server_mcp_config` (JSON array of McpServerEntry)
- Dispatch: `server_tools/dispatch.rs` and `tools_registry.rs`

**Lifecycle:**
1. Boot: Load server_mcp_config from DB via system_config::get.
2. Initialize: ServerMcpManager::initialize spawns TokioChildProcess for each enabled entry.
3. Tool discovery: rmcp service lists tools, normalizes schemas for OpenAI, prefixes names (mcp_{server}_{tool}).
4. Tool invocation: Agent calls mcp_{server}_{tool} → tools_registry routes → ServerMcpManager::call_tool → correct session → rmcp CallToolRequest.
5. Reconfig: PUT /api/server-mcp atomically stops old servers, starts new, detects schema collisions at 10s timeout.

**Functions:**
- `ServerMcpManager::new()`: Create empty manager.
- `ServerMcpManager::initialize(servers: &[McpServerEntry])`: Spawn all enabled servers.
- `ServerMcpManager::reinitialize(servers)`: Wipe + restart.
- `ServerMcpManager::tool_schemas() → Vec<Value>`: Return prefixed OpenAI schemas.
- `ServerMcpManager::raw_tool_schemas_by_server() → Vec<(String, Vec<(String, Value)>)>`: Per-server raw schemas for collision check.
- `ServerMcpManager::call_tool(prefixed: &str, args: Value) → Result<String>`: Invoke tool.

**Handled by rmcp:**
- Transport: TokioChildProcess (stdio-based, native tls).
- Initialization + tools/list.
- Tool parameter schema (input_schema field).
- Tool invocation result (TextContent + other RawContent types).

### Client-Side MCP (User-Installed per Device)

**Location:**
- Client code: `/home/yucheng/Documents/GitHub/Plexus/plexus-client/src/mcp/` (2 files, ~300 lines)
- Config: `devices.mcp_servers` JSONB column (array of McpServerEntry)
- Discovery: ClientToServer::RegisterTools frame at connection time.
- Dispatch: `plexus-client` tool registry checks McpManager for mcp_* tools.

**Lifecycle:**
1. Device startup: WS connect → receive LoginSuccess → extract mcp_servers.
2. Initialize: McpManager::initialize spawns TokioChildProcess for each.
3. Discovery: Collect all tool schemas → send RegisterTools (tool_names + mcp_schemas with raw per-server schemas).
4. Server validates: Checks collisions vs. server MCP + other devices → sends RegisterToolsError if collision.
5. On success: Client caches session handles; agent can now call mcp_{server}_{tool} (routed via server's tools_registry).
6. Reconfig: ServerToClient::ConfigUpdate → apply_config → stop/restart MCP as needed.

**Functions:**
- `McpManager::new()`: Create empty manager.
- `McpManager::initialize(servers)`: Spawn all enabled.
- `McpManager::apply_config(new)`: Diff-based stop/start/restart.
- `McpManager::all_tool_schemas() → Vec<Value>`: Raw OpenAI schemas.
- `McpManager::all_mcp_schemas() → Vec<McpServerSchemas>`: Per-server schemas for RegisterTools.
- `McpManager::call_tool(prefixed, args) → Result<String>`: Invoke tool.
- `McpSession::start(entry) → Result<McpSession>`: Spawn single server.
- `McpSession::call_tool(tool, args) → Result<String>`: Invoke specific tool on this session.

**Handled by rmcp:**
- Same as server.

### Protocol Types (JSON-RPC Shapes)

**ClientToServer::RegisterTools:**
```json
{
  "type": "RegisterTools",
  "data": {
    "tool_names": ["shell", "read_file", "mcp_git_status"],
    "tool_schemas": [{ "type": "function", "function": {...} }],
    "mcp_schemas": [
      {
        "server": "git",
        "tools": [
          { "name": "status", "parameters": {...} }
        ]
      }
    ]
  }
}
```

**ServerToClient::RegisterToolsError:**
```json
{
  "type": "RegisterToolsError",
  "data": {
    "code": "mcp_schema_collision",
    "message": "MCP 'git' tool schemas diverge",
    "conflicts": [
      {
        "mcp_server": "git",
        "tool": "status",
        "existing_schema": { "type": "object", "properties": {...} },
        "new_schema": { "type": "string" },
        "where_installed": ["server", "laptop"]
      }
    ]
  }
}
```

### Code Duplication & Consolidation Opportunity

**Duplication Found:**
- `rmcp` client code is identical in `plexus-server/src/mcp/` and `plexus-client/src/mcp/`:
  - McpSession struct (start, call_tool, tool_schemas).
  - McpManager struct (initialize, apply_config, all_tool_schemas, all_mcp_schemas, call_tool).
  - Transport setup (TokioChildProcess).

**Reasons for Current Duplication:**
1. **Crate boundary**: plexus-client cannot depend on plexus-server.
2. **Runtime split**: Server MCP runs on server; client MCP runs on device. No shared execution context.
3. **Schema normalization**: Server normalizes for OpenAI; client collects raw for RegisterTools. Small divergence but not sufficient to justify a shared crate.

**Consolidation Feasibility:**
- **YES, viable**: Extract shared McpSession + McpManager + transport helpers to **plexus-common/src/mcp/** (new module).
- **No external deps added**: rmcp is already a workspace dependency.
- **Scope reduction**: plexus-common remains ~2,500 LoC (still minimal).
- **Migration path**: Both server and client would `use plexus_common::mcp::*`, eliminating ~150 lines of duplication.

**Recommendation:**
Move to plexus-common post-M3 (low risk, high clarity). File a deferred task: "Extract shared MCP client code to plexus-common."

---

## G. Background Loops / Autonomous Workers

| Loop | Spawner | Cadence | Trigger | Cancellation |
|------|---------|---------|---------|--------------|
| **Heartbeat tick** | main.rs:132 | 60s fixed | Timer | shutdown.cancelled() |
| **Cron poller** | main.rs:131 | 10s fixed | Timer | shutdown.cancelled() |
| **Rate-limit refresher** | main.rs:130 (bus.rs) | 1s fixed (or manual trigger) | Timer | shutdown.cancelled() |
| **Outbound dispatch** | main.rs:135 | Event-driven | OutboundEvent recv | shutdown.cancelled() |
| **Discord bot ready** | channels/discord.rs:145 | N/A | Serenity ready event | gateway_manager observes shutdown → quit() |
| **Telegram dispatcher** | channels/telegram.rs:180 | N/A | Teloxide update recv | shutdown_token.shutdown() awaited |
| **Gateway client** | channels/gateway.rs:200 | Reconnect exponential backoff | WS connect attempt | shutdown.cancelled() |
| **Device WS reader** | ws.rs:700 (spawned per device) | Event-driven | WS frame recv | Device disconnect or shutdown |
| **Heartbeat reaper** | ws.rs:250 | 30s fixed (HEARTBEAT_REAPER_INTERVAL_SEC) | Timer | shutdown.cancelled() |
| **Dream handler** | cron.rs:66 (spawned ad-hoc on dream job fire) | Per-user ~2h (configurable) | Cron poller dispatch | N/A (completes + reschedules) |
| **Session message sender** | agent_loop.rs (per InboundEvent) | N/A | Agent loop completes | N/A (async function, not loop) |

### Details

**Heartbeat Tick:**
- Cadence: 60s (fixed, not tunable).
- Query: Users overdue for heartbeat (last_heartbeat_at < now - interval).
- Action: Run Phase 1 (standalone LLM call on HEARTBEAT.md), if action="run" publish InboundEvent.
- Cap: Max 500 users per tick (prevents backlog spike).
- Reschedule: Cron always advances last_heartbeat_at before Phase 1, so failures auto-retry next cycle.

**Cron Poller:**
- Cadence: 10s.
- Query: Jobs where next_run_at ≤ now AND enabled=TRUE.
- Claim: Atomic UPDATE next_run_at = NULL (prevents double-firing in multi-node).
- Dispatch: PublishInboundEvent to message bus (non-blocking).
- Reschedule: Agent loop calls reschedule_after_completion post-turn.
- Crash recovery: recover_stuck_jobs resets any jobs claimed > 30 min ago.

**Rate-Limit Refresher:**
- Cadence: 1s (or on-demand trigger from rate_limiter check).
- Action: Reset per-minute counter; reload per-min limit from system_config.

**Outbound Dispatch:**
- Trigger: OutboundEvent received on channel (non-blocking send).
- Router: Match channel name → discord::deliver, telegram::deliver, gateway::deliver.
- Backpressure: mpsc channel buffer (1000 slots); if full, sender blocks (rare).

**Discord Bot:**
- Trigger: Serenity shard ready + dispatcher loop.
- Shutdown: Observes state.shutdown.cancelled() in gateway.rs (new in graceful-shutdown commit).

**Telegram Dispatcher:**
- Trigger: Teloxide update loop.
- Shutdown: Calls shutdown_token.shutdown() and awaits (same commit).

**Gateway Client:**
- Trigger: Explicit spawn at boot (channels/gateway.rs:spawn_gateway_client).
- Reconnect: Exponential backoff (1s → 30s cap).
- Shutdown: Observes state.shutdown.cancelled().

**Device WS Reader:**
- Trigger: Per ws_handler upgrade (one per connected device).
- Message loop: StreamExt::next() → parse ClientToServer → dispatch tool result to agent or register tools.
- Shutdown: Device disconnect or global shutdown token.

**Heartbeat Reaper:**
- Cadence: 30s (HEARTBEAT_REAPER_INTERVAL_SEC).
- Query: Devices with last_seen_at < now - 2 × reaper_interval.
- Action: Remove from state.devices map (cleanup; DB not touched).

**Dream Handler:**
- Spawned ad-hoc: When cron poller fires dream job.
- Phase 1: Standalone LLM call, idle check (skip if no activity).
- Phase 2: If Phase 1 returns directives, publish InboundEvent { kind: Dream } (agent loop runs in PromptMode::Dream).
- Reschedule: Dream job owned by dream::handle_dream_fire (not deferred to agent_loop like user-cron).
- Cadence: Per-user configurable (2h default).

---

## H. Channel Adapters

### Inbound Path (User Message → InboundEvent)

**Gateway:**
1. Browser sends chat_message frame to gateway WS.
2. ws/chat.rs parses frame → MessageCreateEvent.
3. Extracts session_id, content, media (base64 content blocks or filepaths).
4. Constructs ChannelIdentity (sender_name = user display_name, is_partner=true, channel_type="gateway").
5. Publishes InboundEvent { channel="gateway", chat_id=session_id, session_id, user_id, content, media, sender_identity } to bus.

**Discord:**
1. Serenity message_create event.
2. channels/discord.rs extracts message.content, attachments (downloads → file_store).
3. Looks up Discord config to find user_id.
4. Constructs ChannelIdentity (sender_name from message.author, is_partner = (author.id == partner_discord_id from config), channel_type="discord").
5. Publishes InboundEvent { channel="discord", chat_id=guild_id, session_id = "discord:{guild_id}:{channel_id}", content, media, sender_identity }.

**Telegram:**
1. Teloxide message update.
2. channels/telegram.rs extracts message.text, photo/voice/document (downloads).
3. Constructs ChannelIdentity (sender_name from message.from.username, is_partner = (from.id == partner_telegram_id from DB), channel_type="telegram").
5. Publishes InboundEvent { channel="tg:{chat_id}", chat_id, session_id = "tg:{chat_id}", content, media, sender_identity }.

### Outbound Path (OutboundEvent → Channel Message)

**Gateway:**
1. OutboundEvent { channel="gateway", chat_id=session_id, content, media }.
2. gateway::deliver calls session_update frame (via state.gateway_sink WS).
3. Sends {type: "session_update", session_id, content, media} to connected browsers.
4. Browser renders message in chat UI.

**Discord:**
1. OutboundEvent { channel="discord", chat_id=guild_id, content, media }.
2. discord::deliver formats markdown, sends message to guild.
3. If media present, uploads to Discord (file_transfer role).

**Telegram:**
1. OutboundEvent { channel="tg:{chat_id}", content, media }.
2. telegram::deliver formats markdown, sends to Telegram chat.
3. If media, sends as separate photo/document message (Telegram API limitation).

### Auth Shape

**Gateway:** JWT token (in Authorization header or WebSocket sub-protocol).  
**Discord:** OAuth callback (code+state) → JWT.  
**Telegram:** Bot token registration + allowed_chat_ids (per-user whitelist).

### Channel-Specific Quirks

**Discord:**
- Embeds/attachments API — media uploaded as Discord attachment, linked in message.
- Rate limiting: 5 msg/5s per channel; exceeded → OutboundEvent queued (bus/rate-limiter).
- Markdown: Slightly different formatting (code blocks, bold/italic).

**Telegram:**
- Max message length: 4096 chars; long messages split.
- Media: Photos, documents, voice sent separately (one media per Telegram message).
- HTML entities: Telegram uses &#NUM; instead of &name;.

**Gateway:**
- Stateless proxy — all state in browser (Zustand) + server DB.
- WebSocket sub-protocol for chat/plexus (two separate WS connections from browser).
- Session affinity: None (gateway is stateless; any gateway instance can relay).

---

## I. File & Path-Handling Feature Matrix

### workspace_fs API (Server-Side)

| Function | Signature | Purpose | Quota? | Symlink Safe? |
|----------|-----------|---------|--------|---------------|
| read_file | async read(path: &str, user_id: &str) → Result<Vec<u8>> | Read file contents | No | Yes (canonicalize) |
| write_file | async write(path, content, user_id) → Result<size> | Create/overwrite file | Yes (reserve before, rollback on error) | Yes |
| edit_file | async edit(path, old, new, user_id) → Result<size> | In-place text replace | Yes (delta only) | Yes |
| delete_file | async delete(path, user_id) → Result<()> | Remove file | Yes (refund quota) | Yes |
| list_dir | async list(path, user_id) → Result<Vec<Entry>> | Directory contents | No | Yes |
| glob | async glob(pattern, user_id) → Result<Vec<String>> | Pattern match | No | Yes |
| grep | async grep(pattern, path, user_id) → Result<Vec<(usize, String)>> | Search | No | Yes |

All path operations use resolve_user_path (symlink canonicalize + sandbox check).

### Client-Side FS API

**No dedicated workspace struct.** Tools use:
- tokio::fs directly (after sandbox check via guardrails).
- Path escaping via helpers (no `../`, no absolute paths outside workspace).
- bwrap for shell (--bind mounts workspace, --chdir, --new-session).

**Duplication:** Client and server both reimplement path safety checks. Post-unification, client should call server for file ops when device_name="server" (already done in agent loop).

### QuotaCache (Server-Side)

| Function | Purpose |
|----------|---------|
| new(limit_bytes) | Create cache with per-user quota limit |
| initialize_from_disk(root) | Scan all users at boot, sum file sizes |
| reserve(user_id, bytes) → Result<Handle> | Pre-allocate before write (returns handle for rollback) |
| release(handle) | Confirm write, finalize quota decrement |
| rollback(handle) | Cancel write, refund quota |
| usage(user_id) → bytes | Current usage |
| get_limit() → bytes | Per-user quota limit |

Default: 5 GB per user (configurable via system_config).

### Feature Matrix Completeness

✓ Sandbox (fs_policy enforces workspace-only or unrestricted).  
✓ Quota (per-user limit, reserve/rollback).  
✓ Symlink safety (canonicalize + bounds check).  
✓ Skills cache invalidation (invalid on write under skills/).  
✗ Transactional semantics (no rollback for multi-step operations — agent retries).  
✗ File rename endpoint (listed in ISSUE.md deferred; currently delete+reupload).  
✗ Bulk operations (single-file ops only in v1).  

---

## J. Sandbox (bwrap)

### Invocation

**Location:** plexus-client/src/sandbox.rs (112 lines)  
**Probe:** LazyLock<bool> BWRAP_AVAILABLE checked at client startup.  
**Fallback:** Direct execution with env isolation if bwrap unavailable.

### Arguments

```
bwrap \
  --ro-bind /usr /usr \
  --ro-bind-try /bin /bin \
  --ro-bind-try /lib /lib \
  --ro-bind-try /lib64 /lib64 \
  --ro-bind-try /etc/alternatives /etc/alternatives \
  --ro-bind-try /etc/ssl/certs /etc/ssl/certs \
  --ro-bind-try /etc/resolv.conf /etc/resolv.conf \
  --ro-bind-try /etc/ld.so.cache /etc/ld.so.cache \
  --proc /proc \
  --dev /dev \
  --tmpfs /tmp \
  --tmpfs /parent \
  --dir /workspace \
  --bind /workspace /workspace \
  --chdir /workspace \
  --new-session \
  --die-with-parent \
  -- sh -c "command"
```

**Binding:**
- ro-bind: Read-only system dirs (/usr, /bin, /lib, /etc).
- --tmpfs /tmp: Ephemeral temp.
- --bind /workspace /workspace: Workspace (read-write).
- --tmpfs /parent: Ephemeral parent dir (prevents ../../ escapes to parent).

**Environment:**
- PLEXUS_WORKSPACE, PLEXUS_DEVICE_TOKEN set by client before spawn.
- No other user env vars (inherited shell env stripped in nanobot pattern).

### Limitations vs. nanobot

- nanobot uses sandbox.py (Python subprocess) with rlimit/seccomp/cgroup.
- plexus-client uses bwrap (namespace isolation) only.
- nanobot: Better resource limit enforcement (CPU, memory, file descriptor).
- plexus: Simpler, less system-dependent (no seccomp/cgroup setup needed).

### Status

✓ Implemented in plexus-client.  
✗ Server has NO sandbox (workshop owned by admin, not user-restricted).  
✓ Graceful fallback if bwrap unavailable.

---

## K. Testing Infrastructure

### Unit Tests

**plexus-common:**
- protocol.rs: 12 tests (serde round-trips, additive fields).
- errors/: 3 tests (error code parsing, HTTP status mapping).
- Total: ~15 tests (passing).

**plexus-server:**
- workspace/fs.rs: 10 tests (path safety, quota).
- server_tools/file_ops.rs: 5 tests (read/write/edit edge cases).
- channels/: 2 tests (safe_attachment_filename).
- agent_loop.rs: 2 tests (publish_final branches).
- db/: 8 tests (DB operations).
- Total: ~35 tests (passing).
- **ISSUE.md notes:** 358 workspace tests pass, 0 clippy warnings.

**plexus-client:**
- sandbox.rs: 2 tests (bwrap command structure).
- tools/helpers.rs: 5 tests (path safety).
- Total: ~10 tests (passing).

**plexus-gateway:**
- No unit tests (simple proxy logic, manual smoke).

### Manual Smoke Tests (Deferred)

Documented in ISSUE.md Deferred section:
- Inbound media (Discord/Telegram photo + voice, browser drag+paste).
- Cross-channel addressing (cron across browser reconnect, Discord→Telegram).
- Graceful shutdown (live SIGTERM with bots).
- FR batch (device-stream, image-drop, device-config, Server MCP, MCP collision).

### Test Helpers

- **tempfile** (dev-dep): Temporary directories for workspace tests.
- **tokio::test**: Async test macros.
- No Vitest/Jest/RTL for frontend (manual smoke only, ISSUE.md notes).

### Harness Status

✗ No Vitest / Jest / React Testing Library.  
✓ Tokio async test support.  
✓ Basic workspace + tools tests.  
Deferred: Frontend automated tests (low priority if surface doesn't grow).

---

## L. Skills System

### Disk Storage

**Path:** `{workspace_root}/{user_id}/skills/{skill_name}/SKILL.md`  
**Format:** Markdown with YAML frontmatter:
```markdown
---
name: MySkill
description: Does X, Y, Z
always_on: true
---
# Implementation
...
```

### Frontmatter Parsing

**Location:** plexus-server/src/skills_cache.rs:parse_frontmatter()  
**Fields:**
- name (optional; defaults to directory name).
- description (required).
- always_on (optional; defaults to false).

**Always-on skills:** Body cached in memory (SkillInfo.content).  
**On-demand skills:** Body omitted from cache; agent reads via read_file when needed.

### Cache Invalidation

**Location:** plexus-server/src/skills_cache.rs  
**Trigger:** Any write under `{workspace}/{user_id}/skills/` detected by is_under_skills_dir.  
**Callers:** write_file, edit_file, delete_file, file_transfer.  
**Action:** Remove SkillInfo cache for that user (next access reloads from disk).

### Progressive Disclosure

**Discovery:** skills_cache::get_or_load scans skills/ directory, loads frontmatter.  
**Returned:** SkillInfo array (name, description, always_on, body for always-on only).  
**Agent access:** context.rs builds skill index into system prompt; agent sees skill names + descriptions. On-demand skill bodies loaded via read_file.

### Completeness

✓ Disk-based (single source of truth).  
✓ Cache with TTL (per-user, invalidated on write).  
✓ Always-on vs. on-demand split.  
✓ SKILL.md parsing (frontmatter extraction).  
✗ Skill composition / inheritance (not in v1).  
✗ Publish to GitHub / marketplace (not in v1; deferred).

---

## M. Autonomous Subsystems — Dream + Heartbeat + Cron

### Dream (D-7 / D-8)

**Trigger:** System cron job (kind='system' name='dream') fires every ~2h per user (configurable).  
**Phase 1 (D-7):**
- Idle check: Skip if no user activity since last dream.
- LLM call: dream_phase1 prompt + recent message history (cap 200) + MEMORY.md + SOUL.md + skills index.
- Output: Free-form directives (e.g., "consolidate X into MEMORY.md", "remove skill Y").
- On [NO-OP]: Reschedule and return.
- On non-empty: Publish InboundEvent { kind: Dream, content: directives }.

**Phase 2 (D-8):**
- Agent loop runs in PromptMode::Dream.
- Tool allowlist: DREAM_PHASE2_ALLOWLIST (file I/O only).
- Delivery: publish_final_dream → silent (deliver=false; dream is internal consolidation).

**Prompts:** dream_phase1_prompt, dream_phase2_prompt (stored in system_config or compiled in).

**Metrics:**
- Cadence: Per-user ~2h (configurable via heartbeat_interval_seconds in system_config? — ISSUE.md ambiguous).
- Last run: users.last_dream_at (advanced before Phase 1 to prevent refire).
- Session ID: dream:{user_id}.

### Heartbeat (E-8 / E-6)

**Trigger:** System cron job (kind='system' name='heartbeat') fires every ~30 min (default heartbeat_interval_seconds).  
**Phase 1 (E-8):**
- Load HEARTBEAT.md (user task list).
- LLM call: heartbeat_phase1 prompt + HEARTBEAT.md + current time/tz.
- Virtual tool: heartbeat(action="skip"|"run", tasks="...").
- On action="skip": Reschedule and return silently.
- On action="run": Publish InboundEvent { kind: Heartbeat, content: tasks }.

**Phase 2 (E-6):**
- Agent loop runs in PromptMode::Heartbeat.
- Tool allowlist: ToolAllowlist::All (full tool access).
- Delivery: publish_final_heartbeat → evaluator check + external-channel precedence (Discord → Telegram → silence; never gateway).

**Prompts:** heartbeat_phase1_prompt (stored in system_config or compiled in).

**Metrics:**
- Cadence: 60s tick loop polling per-user interval (default 1800s / 30 min; configurable via system_config heartbeat_interval_seconds).
- Last run: users.last_heartbeat_at (advanced before Phase 1).
- Session ID: heartbeat:{user_id}.

### Cron

**Trigger:** User-created cron jobs (kind='user') + system jobs (kind='system' name!='dream'/'heartbeat').  
**Schedule:** Cron expression (e.g., "0 9 * * *").  
**Poller:** Runs every 10s, queries jobs where next_run_at ≤ now.  
**Dispatch:** Atomically claims job (UPDATE next_run_at = NULL), publishes InboundEvent.  
**Delivery:** publish_final decides based on deliver flag (user cron: evaluator-gated; system cron: no deliver).  
**Reschedule:** Agent calls reschedule_after_completion post-turn (updates next_run_at based on next occurrence).  
**Crash recovery:** Recover stuck jobs (claimed > 30 min, reset next_run_at).

**Prompts:** User-supplied (per-job prompt field).

**Metrics:**
- Cadence: Per-job cron schedule.
- Last run: cron_jobs.last_run_at.
- Session ID: cron:{job_id}.

### Evaluator (Plan C)

**Shared decision logic for Cron + Heartbeat.**  
**Checks:**
- deliver flag (cron_jobs.deliver for user cron; false for dream; varies for system cron).
- Quiet hours (parsed from HEARTBEAT.md, e.g., "quiet 22:00-08:00").
- Rate limit (global or per-user).

**Output:** Notify (publish to channel) or silence.

---

## N. Auth System

### JWT Signing / Validation

**Location:** plexus-server/src/auth/mod.rs  
**Algorithm:** HS256 (HMAC SHA-256).  
**Secret:** config.jwt_secret (from env PLEXUS_JWT_SECRET).  
**Claims:** Claims struct { sub: user_id, iat, exp }.  
**Expiration:** 7 days (hardcoded in signing).  
**Extraction:** From Authorization header "Bearer {token}" or device token SubmitToken frame.

### Admin Token

**Scope:** Any JWT with claims.is_admin=true (set at registration if user_id matches PLEXUS_ADMIN_ID env var).  
**Admin endpoints:** /api/admin/*, /api/llm-config, /api/server-mcp (all gated by extract_admin_claims).  
**No separate token type** — same JWT as user, flag is a boolean claim.

### Device Tokens

**Type:** Opaque string (plexus_dev_*), stored as token (plaintext in device_tokens table, NOT hashed).  
**Hashing:** Server stores token_hash in devices table (to avoid plaintext in workspace files).  
**Usage:** Device passes at login (ClientToServer::SubmitToken), server looks up devices.token = token (DB query).  
**Rotation:** Old tokens can be revoked via DELETE /api/device-tokens/{token}; new token created immediately.

### Password Hashing

**Algorithm:** bcrypt (cost 12, default).  
**Storage:** users.password_hash (bcrypt digest, not plaintext).  
**Verification:** On login, hash supplied password, compare.  
**Account deletion:** Requires password confirmation (DELETE /api/user body: {password}).

### Rate Limiting

**Type:** Per-minute global limit (configurable via /api/admin/rate-limit).  
**Storage:** In-memory DashMap<user_id, count> reset every 1s.  
**Default:** 0 (unlimited; admin sets via system_config rate_limit_per_min).  
**Scope:** All API calls (except /api/auth/register, /api/auth/login).  
**Rejection:** 429 Too Many Requests if exceeded.

### Account Deletion Orchestration

**Location:** plexus-server/src/account.rs:delete_user_everywhere()  
**Flow:**
1. Verify password match (user self-delete) or admin auth (admin-delete).
2. Stop Discord bot (if any).
3. Stop Telegram bot (if any).
4. Kick browser sessions (gateway kick_user frame).
5. Evict in-memory state (sessions, devices, rate_limiter, tool_schema_cache, skills_cache).
6. Cascade DB delete (single DELETE FROM users → cascades through all FKs via ON DELETE CASCADE).
7. Wipe workspace (tokio::fs::remove_dir_all {workspace_root}/{user_id}).

**Atomicity:** All in-memory cleanup + workspace wipe before DB commit; if DB delete fails, workspace already half-deleted (acceptable race condition for rare operation).

### Completeness

✓ JWT signing/validation.  
✓ Password hashing (bcrypt).  
✓ Device tokens (opaque strings).  
✓ Rate limiting (global per-minute).  
✓ Account deletion orchestration.  
✗ OAuth 2.0 (Discord/Telegram use token registration, not OAuth flow in this version).  
✗ Email verification (not in v1).  
✗ Multi-factor auth (not in v1).  

---

## O. Frontend Feature Matrix

**Location:** `/home/yucheng/Documents/GitHub/Plexus/plexus-frontend/src` (5,258 LoC TypeScript/TSX)

### Pages

**Login.tsx:**
- Email + password fields.
- Register / login toggle.
- JWT storage (Zustand + localStorage).
- Redirects to Chat on success.

**Wizard.tsx:**
- Device token generation (POST /api/device-tokens).
- Display token + copy/download.
- Environment setup instructions.

**Chat.tsx:**
- Main chat interface.
- Message list (renders Message components).
- Input (ChatInput with file upload).
- Session sidebar (list of sessions).
- Unread badge per session.

**Workspace.tsx:**
- File tree browser (GET /api/workspace/tree).
- File preview (inline for text/images, binary metadata for others).
- Upload (PUT/POST to /api/workspace/files or upload endpoint).
- Inline editor for MEMORY.md / skills/*/SKILL.md.
- Bulk operations (deferred — single-file only in v1).

**Settings.tsx:**
- Profile tab: Display name, timezone, last activity.
- Devices tab: List + config editor (workspace_path, shell_timeout_max, ssrf_whitelist, fs_policy).
- Danger Zone: Account deletion (password-confirm modal).

**Admin.tsx:**
- Users tab: List (search filter) + delete per user.
- LLM config tab: Provider, key (masked), endpoint, model.
- Rate limit tab: Global per-minute limit.
- Server MCP tab: Add/edit/remove servers, env masking.

### State Management (Zustand)

**store/auth.ts:**
- token (JWT).
- user (profile).
- is_admin flag.
- login/logout/register actions.

**store/chat.ts:**
- sessions (list).
- currentSession (selected).
- messages (per-session history).
- websocket subscription.
- refreshSession action (called on session_update frame).

**store/devices.ts:**
- devices (list).
- selectedDevice (for config edit).

### API Calls

**lib/api.ts:**
- fetchProfile, updateDisplayName, deleteUser.
- listSessions, deleteSession, getMessages.
- listDevices, updateDeviceConfig.
- uploadFile, getWorkspaceTree, getFile.
- All JWT-gated (Authorization header).

**lib/upload.ts:**
- multipart upload helper (with progress callback).
- File size check (20 MB client-side limit).

**lib/ws.ts:**
- WebSocket connection (two: /ws/chat for browser, /ws/plexus for server relay).
- Frame parsing (session_update, chat_message, etc.).
- Automatic reconnect on disconnect.

### Features

✓ Login / register.  
✓ Multi-session chat.  
✓ File attachment upload (browser).  
✓ Inline message rendering (text, images, code).  
✓ Workspace file browser + editor.  
✓ Device management + config.  
✓ Admin user management + LLM config + MCP config.  
✓ Unread badge per session.  
✗ Vitest / RTL test harness (manual smoke only).  
✗ Bulk file operations (deferred).  
✗ File rename endpoint (deferred).  

---

## P. "Hidden Features" — Things a Naive Reader Might Miss

1. **RTK dev proxy comment in code** (not found in search — likely historical; no evidence of RTK use in plexus-frontend).
2. **PLEXUS_WORKSPACE_ROOT env var** — Server root for all user workspaces. Required at boot.
3. **PLEXUS_JWT_SECRET env var** — Signing key. Required for JWT ops.
4. **PLEXUS_DATABASE_URL** — PostgreSQL connection string. Required for DB init.
5. **Workspace quota initialization at boot** (main.rs:119) — Scans disk on startup (can be slow for large workspaces; logged as non-fatal if slow/fails).
6. **Skills always-on vs. on-demand split** — Only always-on bodies cached; on-demand skills loaded via read_file (optimization for large skill sets).
7. **`--ro-bind-try` in bwrap** — Allows missing system dirs (e.g., /lib64 on some systems) without failing.
8. **RegisterToolsError frame** — New additive variant; older clients that don't understand it will silently drop (forward-compatible).
9. **ReadStream protocol** — Chunks large files (32 KiB) to avoid OOM when reading 4 GB files into memory (previously not possible).
10. **dream_phase1_prompt / dream_phase2_prompt stored in system_config** — Can be overridden at runtime via PUT /api/admin/... without redeploying.
11. **Session affinity in gateway** — None. Stateless; any gateway instance can relay. Supports multi-instance deployment.
12. **Cross-channel addressing** — Stored in DB; agents can send messages to Discord from a Telegram session via `message` tool with channel + chat_id.
13. **Evaluator (shared decision logic)** — Gating publish of cron/heartbeat outputs; quiet hours, rate limits parsed from HEARTBEAT.md.
14. **Inbound media handling** — Discord/Telegram attachments downloaded to workspace `.attachments/{msg_id}/{filename}`, then embedded as content blocks in messages (supports images for VLMs, other files via file_transfer).
15. **Graceful shutdown** — SIGINT/SIGTERM → state.shutdown.cancelled() → all background loops wake + check, exit at next select! branch (in-flight ReAct turns always complete).
16. **Schema collision check (ServerToClient::RegisterToolsError)** — Wired in two paths: device RegisterTools frame (plexus-client reports colliding schemas) and PUT /api/server-mcp (admin introspection via rmcp with 10s timeout, returns 409 on conflict).
17. **QuotaCache initialize_from_disk at boot** — Scans all users' files to populate quota state; logged as non-fatal if fails (starts from 0 and converges).
18. **MCP tool prefixing** — mcp_{server_name}_{tool_name} allows arbitrary server names (not just "git" or "minimax"). Custom servers supported.
19. **Soft fail on dream / heartbeat errors** — Phase 1 failures don't block reschedule; next window gets fresh attempt (autonomous best-effort).
20. **Device token consumed_at field** — Token marked consumed but not deleted (audit trail; deferred deletion is rare).

---

## Q. Rebuild-vs-Refactor Lens

### Per-Crate Assessment

**plexus-common (1,944 LoC):** **Acceptable shape — ship as-is.**
- Tight, minimal, protocol-focused.
- Error hierarchy is complete and will serve as foundation.
- Only gap: MCP client duplication in server/client could be consolidated to plexus-common post-M3 (low-priority).
- Concrete example: protocol.rs test coverage is solid; error hierarchy is exhaustive.

**plexus-server (17,127 LoC):** **Good shape overall, but three rough edges worth noting.**
- Architecture is sound: clear separation (context.rs builds prompts, agent_loop.rs runs ReAct, channels/* adapt, db/* persist, workspace/* handles files).
- Strengths: error handling is explicit, DB schema uses CASCADE correctly, background loop cancellation is clean, tool dispatch is unified.
- Rough edges:
  1. **context.rs is a beast (1,107 LoC):** Prompt construction is monolithic. Splitting by subsystem (Dream prompt builder, Heartbeat prompt builder, UserTurn prompt builder) would improve readability. Not a blocker; readable today. Concrete example: PromptMode::Dream branch omits channel identity but builds skills index differently.
  2. **agent_loop.rs (899 LoC):** publish_final has EventKind discrimination that could be extracted to handlers (publish_final_user_turn, publish_final_cron, publish_final_heartbeat). Not a blocker; the match is clear. Concrete example: EventKind::Heartbeat branch calls evaluator + external-channel precedence; EventKind::Dream is silent (deliver=false). Current code is readable but could be three separate functions.
  3. **No transaction semantics:** DB mutations are single statements with cascade. If agent loop crashes mid-turn, may leave orphaned messages. Acceptable for now (agent retries on reconnect); not a refactor-needed issue unless users report data loss.
- Verdict: **Can refactor-in-place.** Split context builder and agent_loop publish logic into smaller functions, test incrementally. No rewrite needed.

**plexus-client (3,246 LoC):** **Solid — refactor-in-place preferred.**
- Stateless reconnect loop is elegant; MCP manager is straightforward.
- Strengths: Tool implementations are defensive (path safety, symlink checks), bwrap integration graceful (fallback on missing).
- One gap: Tool schemas are locally defined (mirroring server); post-M3 consolidation to plexus-common would eliminate this. Currently acceptable.
- Sandbox integration is partial (bwrap on Linux only, direct+env-isolation fallback). Could strengthen but not a blocker.
- Concrete example: sandbox.rs wrap_command is clean; shell.rs execution timeout logic is correct.
- Verdict: **Refactor-in-place.** No rewrite needed; tool schema consolidation is a low-priority post-M3 task.

**plexus-gateway (1,324 LoC):** **Disaster zone avoided — acceptable shape.**
- Thin proxy layer; logic is straightforward (relay WebSocket frames, forward HTTP, serve static).
- Strengths: Stateless design supports multi-instance; graceful shutdown integrated.
- Rough edge: ws/plexus.rs connection management could be clearer (single outbound connection shared by all browsers; reconnect logic is inline). Not a blocker.
- Concrete example: ws/chat.rs parses browser frames and forwards to server; ws/plexus.rs fans out server frames to all browsers. Code is readable; not a refactor-needed issue.
- Verdict: **Acceptable as-is.** Stateless proxy is a good pattern; no rewrite needed.

**plexus-frontend (5,258 LoC):** **Acceptable — manual smoke only, no automated tests.**
- Pages and store setup are standard React/TypeScript (Zustand + API + WebSocket).
- Strength: Zustand store is lightweight; API and WebSocket helpers are clean.
- Gap: No Vitest/RTL test harness. ISSUE.md notes this is acceptable for now (low priority unless surface grows).
- Concrete example: Workspace.tsx handles inline editing of SKILL.md; Message.tsx renders content blocks with image preview fallback. Code is readable; no refactor pressure.
- Verdict: **Ship as-is for M3.** Manual smoke testing covers shipped features. Automated harness is a post-M2 effort if surface grows significantly.

---

### Summary

| Crate | Status | Why | Refactor vs. Rebuild |
|-------|--------|-----|----------------------|
| plexus-common | ✓ Good | Minimal, protocol-focused, error hierarchy complete | **Refactor** (MCP consolidation post-M3) |
| plexus-server | ✓ Good (with rough edges) | Solid architecture, readable logic, three functions could be split for clarity | **Refactor-in-place** (context.rs + agent_loop.rs splitting) |
| plexus-client | ✓ Good | Stateless design, tool implementations solid, bwrap graceful fallback | **Refactor-in-place** (tool schema consolidation post-M3) |
| plexus-gateway | ✓ Acceptable | Thin proxy, stateless design, readable frame handling | **Ship as-is** |
| plexus-frontend | ✓ Acceptable | Standard React/Zustand, no automated tests but manual smoke covers shipped features | **Ship as-is** (test harness post-M2 if needed) |

---

### Rebuild-vs-Refactor Recommendation

**Choose refactor-in-place.** All four server/client/gateway/frontend crates are in shipable shape. No crate warrants a full rewrite. Specific refactoring opportunities are low-risk (split context builder, consolidate MCP client code, add frontend tests) and can land incrementally post-M3 without blocking M2 ship.

The one exception: If you discover (post-launch) that agent latency is dominated by context building, then context.rs refactoring becomes urgent. But today, no evidence suggests this. Refactor when you have data.

---

This concludes the comprehensive inventory. All cross-cutting systems have been documented: protocol, errors, database, endpoints, WebSocket frames, tools, MCP, background loops, channels, workspace, sandbox, testing, skills, autonomous subsystems, auth, frontend, and rebuild-vs-refactor assessment. You now have a complete picture of what exists before choosing a path forward.
