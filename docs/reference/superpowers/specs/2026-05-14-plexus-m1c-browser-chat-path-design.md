# Plexus M1c Browser Chat Path Sub-Spec

**Status:** Verified
**Parent:** [Plexus M1 Living Design Spec](2026-05-12-plexus-m1-living-design.md)
**Branch:** `rebuild-m1-M1c`
**Base:** `rebuild-m1`
**Authors:** brainstormed in collaborative session 2026-05-14
**Supersedes:** none

---

## 1. Goal

M1c adds the first usable browser chat path on top of the M1a server
foundation and the M1b OpenAI-compatible provider layer.

The success proof is intentionally focused:

- a user can create a browser chat session;
- the session has a stable UUID identity, generated web `session_key`, and
  editable human title;
- the user can post text, empty, image-only, or mixed text/image content;
- messages persist to PostgreSQL and replay through Server-Sent Events;
- the server builds a minimal prompt using optional `SOUL.md` and `MEMORY.md`;
- Plexus calls the configured OpenAI-compatible provider and persists the
  assistant response;
- provider failures produce a safe persisted assistant diagnostic;
- automated fake-provider tests pass;
- the milestone is not marked verified until the user completes a manual live
  smoke with a real LLM key.

M1c is the chat spine, not the full agent. It should prove REST ingress,
history persistence, SSE replay/live delivery, prompt construction, provider
calling, and live-provider smoke without pulling in tools, devices, workspaces,
MCP, cron, or frontend code.

---

## 2. Non-Goals

M1c does not include:

- React/frontend implementation;
- the full ReAct agent loop;
- tool calls, tool dispatch, or tool-call repair implementation;
- `POST /api/sessions/{id}/cancel`;
- workspace/file REST APIs;
- `.attachments/` writes or quota accounting for uploaded images;
- multipart upload;
- external `http(s)` image URL ingestion;
- image resizing or transcoding;
- skills loading;
- workspace tree rendering in the system prompt;
- device status or device WebSocket support;
- server MCP or device MCP;
- Discord or Telegram adapters;
- cron or heartbeat;
- context compaction;
- streaming chat completions;
- native Anthropic, Gemini, or other non-OpenAI protocols;
- a provider abstraction framework.

M1c may update docs and ADR wording that conflicts with the chosen browser chat
shape. Those updates are part of the milestone, not incidental cleanup.

---

## 3. Contract Corrections

Earlier docs contained one important tension: `docs/API.yaml` used
`/api/sessions/{id}`, while ADR-098 discussed browser writes to
`/api/sessions/{key}/messages`. M1c resolves the tension in favor of UUID
browser routes, and ADR-098 now records that correction.

M1c resolves this as follows:

- public browser REST routes address sessions by `sessions.id` UUID;
- `session_key` remains an internal/channel routing identity;
- browser sessions derive `session_key = web:{id}`;
- browser sessions use `channel = "web"` and `chat_id = id::text`;
- the browser cannot set or rename `session_key`;
- the user-facing mutable name is `sessions.title`;
- browser message writes are allowed only when the target session belongs to
  the authenticated user and `channel = "web"`;
- later non-web sessions may be read by UUID if owned by the user, but browser
  REST must not be able to write into Discord, Telegram, cron, heartbeat, or any
  future non-web namespace.

Natural session keys still matter for later ingress adapters:

```text
discord:dm:{discord_user_id}
discord:guild:{guild_id}:channel:{channel_id}
telegram:{chat_id}
cron:{job_id}
heartbeat:{user_id}
```

Those keys are routing identities used to find or create a `sessions` row. They
are not the physical primary key for messages.

M1c should update `docs/API.yaml`, `docs/SCHEMA.md`, and `docs/DECISIONS.md`
to reflect this split.

---

## 4. Data Model

M1c keeps `sessions.id UUID PRIMARY KEY` as the relational identity. Message
rows continue to reference `messages.session_id -> sessions.id`.

M1c adds:

```sql
title TEXT NOT NULL DEFAULT 'New chat'
```

to `sessions`.

M1c should change `session_key` uniqueness from global uniqueness to per-user
uniqueness:

```sql
UNIQUE (user_id, session_key)
```

This avoids cross-user collisions for natural channel keys. For example, two
Plexus users may each have a Discord DM session with the same external Discord
user id, or later both may have a session called `web:{their-own-id}`.

### 4.1 Browser Session Creation

`POST /api/sessions` creates a browser session row.

For each browser session:

```text
id          = server-generated UUID
channel     = "web"
chat_id     = id as a string
session_key = "web:{id}"
title       = normalized request title or "New chat"
```

The implementation may generate the UUID in Rust and insert all fields in one
statement. That avoids an insert-then-update just to compute `session_key`.

### 4.2 Title Rules

Titles are human-facing only. They never affect `id`, `session_key`,
`channel`, `chat_id`, or message routing.

Rules:

- create with missing, empty, or whitespace-only `title` stores `"New chat"`;
- create with a non-empty title stores the trimmed title;
- rename requires a present, non-empty title after trimming;
- title maximum is 120 Unicode scalar values after trimming;
- titles are not unique.

---

## 5. API Surface

All M1c session routes require the existing authenticated user.

### 5.1 Create Session

```text
POST /api/sessions
```

Request:

```json
{
  "title": "Journey to Japan"
}
```

`title` is optional.

Response: `201 Created` with the `Session` object.

### 5.2 List Sessions

```text
GET /api/sessions?limit=50&offset=0
```

Returns sessions owned by the authenticated user. Sort order should be:

1. `last_inbound_at DESC NULLS LAST`
2. `created_at DESC`

### 5.3 Read, Rename, Delete

```text
GET    /api/sessions/{id}
PATCH  /api/sessions/{id}
DELETE /api/sessions/{id}
```

`{id}` is `sessions.id` UUID.

`PATCH` only renames the title:

```json
{
  "title": "Japan itinerary"
}
```

`DELETE` removes the session. Existing `ON DELETE CASCADE` behavior removes its
messages.

### 5.4 Post Message

```text
POST /api/sessions/{id}/messages
```

The target session must:

- exist;
- belong to the authenticated user;
- have `channel = "web"`;
- have `session_key` starting with `web:`.

If the session does not exist or belongs to another user, return `404` to avoid
leaking existence. If the session is owned by the caller but is not a web
session or uses a non-`web:` internal namespace key, return `400 invalid_args`;
non-web sessions are readable later, but not writable through browser REST.

The route persists the user message, broadcasts it to active SSE subscribers,
starts the M1c response worker if one is not already active for this session,
and returns `202 Accepted`.

The request body may omit `reasoning_effort` or set it to `null`; in that case
Plexus sends no reasoning controls to the provider. If present, it must be one
of `none`, `minimal`, `low`, `medium`, `high`, or `xhigh`. The frontend may
render `none` as "off", but the wire value remains `none`.

### 5.5 Message History

```text
GET /api/sessions/{id}/messages?before={message_id}&limit=50
```

Returns older persisted messages for scrollback. The canonical "open the chat"
path remains the SSE stream because it replays recent history and then switches
to live events without a race.

### 5.6 SSE Stream

```text
GET /api/sessions/{id}/stream?replay_limit=50
```

On connect:

1. authenticate and verify session ownership;
2. replay recent persisted messages as `event: message` in chronological order;
3. emit one `event: history_end`;
4. keep the connection open for live `message` events.

Each `message` event uses the message UUID as the SSE `id:` field. If
`Last-Event-ID` is present, replay messages after that message id instead of
using the normal replay window. `replay_limit=0` skips normal replay when
`Last-Event-ID` is absent.

Because the server subscribes before replay, any queued live event whose id was
already emitted in replay is skipped. If the live receiver lags and drops
messages, the stream closes so the browser reconnects and uses
`Last-Event-ID` replay instead of continuing with a permanent gap.

M1c does not implement `hint`, `session_update`, `kick`, or cancel events.

---

## 6. Message Content

M1c stores `messages.content` as a JSONB array of OpenAI-compatible content
blocks. The normalization rules below describe the user-supplied content before
the runtime block is prepended. Persisted user rows contain the runtime block
followed by the normalized user content.

The POST body accepts:

```json
{ "content": "hello" }
```

or:

```json
{
  "content": [
    { "type": "text", "text": "What is this?" },
    {
      "type": "image_url",
      "image_url": {
        "url": "data:image/png;base64,iVBOR..."
      }
    }
  ]
}
```

Normalization:

- omitted `content` becomes `[]`;
- `""` becomes `[]`;
- a string becomes one text block unless it is empty;
- an array preserves valid blocks in order;
- text blocks may contain empty strings;
- image-only messages are valid;
- fully empty messages are valid.

`null` is invalid. Unknown block types are invalid in M1c.

Image rules:

- M1c accepts only syntactically valid `data:image/...;base64,...` URLs in
  `image_url.url`;
- external `http(s)` image URLs are rejected in M1c;
- multipart upload is not supported;
- Plexus does not write `.attachments/` copies in M1c;
- Plexus does not inject ADR-027 path-text markers in M1c because there is no
  workspace file path yet.

This is a temporary M1c tradeoff. M1d owns external image fetch, workspace
attachment writes, quota checks, future resizing, and real ADR-027 path-text
markers.

---

## 7. Runtime Block

M1c follows ADR-094: a runtime block is constructed once at user-message
ingress and persisted as part of that user row.

For web chat, the runtime block is a text block prepended before user-supplied
content:

```text
<runtime>
time: 2026-05-14 12:34:56 +08:00
channel: web
chat_id: {session.id}
</runtime>
```

The block is immutable after insert. It is included in the content sent to the
LLM and replayed through SSE as part of the persisted message. Later frontend
work may hide runtime blocks from the visual chat transcript, but M1c keeps the
database and API surfaces faithful.

M1c should update `docs/SYSTEM_PROMPT.md` where it still implies older user
messages do not carry persisted runtime blocks.

---

## 8. Prompt Construction

M1c adds a minimal context builder for the one-shot browser response worker. It
is not the full future `context::build_context`, but it should be shaped so the
future builder can replace it cleanly.

The M1c static system prompt includes:

1. `SOUL`
2. `MEMORY`
3. `Identity`
4. `Channels`
5. `Operating Notes`

### 8.1 SOUL

Load from:

```text
{PLEXUS_WORKSPACE_ROOT}/{user_id}/SOUL.md
```

If missing, render an empty `## SOUL` section. Missing files are non-fatal.

### 8.2 MEMORY

Load from:

```text
{PLEXUS_WORKSPACE_ROOT}/{user_id}/MEMORY.md
```

If missing, render an empty `## MEMORY` section. Missing files are non-fatal.

M1c does not edit `MEMORY.md` and does not implement Dream or compaction.

### 8.3 Identity

Include the authenticated user's name and account id. State that direct browser
input from this user is authoritative.

### 8.4 Channels

Include only the current web channel/session. Discord, Telegram, cron, and
heartbeat are not implemented in M1c.

### 8.5 Operating Notes

State that M1c has no tools available and should answer in plain text. Do not
claim file, device, MCP, workspace, cron, or message tools exist.

### 8.6 Deferred Prompt Sections

M1c does not include:

- skills;
- workspace trees;
- shared workspace membership;
- device status;
- tool schemas;
- channel adapter format notes beyond web;
- compaction summaries.

---

## 9. Provider Wire Format

M1c upgrades the internal provider request type from string-only chat content to
content block arrays.

All outbound chat completion messages use content arrays, including text-only
messages:

```json
{
  "role": "user",
  "content": [
    { "type": "text", "text": "hello" }
  ]
}
```

Image messages use OpenAI-compatible image blocks:

```json
{
  "role": "user",
  "content": [
    { "type": "text", "text": "What is this?" },
    {
      "type": "image_url",
      "image_url": {
        "url": "data:image/png;base64,..."
      }
    }
  ]
}
```

Assistant responses are persisted as one text block containing the normalized
visible answer. The provider layer also normalizes native
`choices[0].message.reasoning_content` and leading `<think>...</think>` blocks
into durable `messages.reasoning_content`. Assistant history is replayed with
that stored reasoning value, or with `reasoning_content: ""` when no reasoning
was stored for the assistant row.

`stream` remains `false`. If the browser omits `reasoning_effort` or sends
`null`, the provider request omits both `reasoning_effort` and
`chat_template_kwargs`. If the browser sends an explicit reasoning value, the
provider request includes `reasoning_effort` and
`chat_template_kwargs.enable_thinking`; the latter is `false` only when
`reasoning_effort = "none"`.

---

## 10. Provider Retry and Image Fallback

M1c keeps provider calls inside `plexus-server/src/openai.rs`.

Retry policy:

1. Send the full message list, including image blocks.
2. If the request succeeds, return the assistant content.
3. Auth/config failures fail fast:
   - `401`
   - `403`
   - model/config errors that are clearly not transient
4. Transient failures retry the same payload with backoff:
   - request timeout
   - connection failure
   - `408`
   - `429`
   - `500`
   - `502`
   - `503`
   - `504`
   - `529`
5. Image compatibility or payload errors retry once immediately with
   `image_url` blocks stripped when the original request contained images:
   - `400`
   - `413`
   - `415`
   - `422`
   - provider error text clearly mentions unsupported image, vision, or content
     block input
6. The stripped request then gets its own transient retry path.
7. If all attempts fail, the caller receives a safe provider failure summary.

When images are stripped, M1c removes only `image_url` blocks and keeps all text
blocks. If stripping leaves only the runtime block or even no user-authored
content, the stripped request is still valid. Plexus should not invent a fake
workspace path or fake attachment marker.

Backoff values should be short and deterministic in tests. Production defaults
may be `1s`, `2s`, `4s`; tests may inject shorter delays or use helper hooks so
the suite remains fast.

Error handling must never expose `llm_api_key`, raw authorization headers, full
provider response bodies that may contain secrets, or stack traces.

---

## 11. Response Worker and Concurrency

M1c has no tool loop, but it still needs per-session serialization.

Behavior:

- `POST /api/sessions/{id}/messages` durably accepts the user message and
  returns `202` without waiting for the LLM.
- Each session has at most one active M1c response worker.
- If no worker is active, the user message is inserted directly into
  provider-visible `messages`, broadcast over SSE, and a worker starts.
- If a user posts while a response is running, the new message is inserted into
  durable `pending_messages` with `session_id`, `user_id`, `session_key`,
  content, optional reasoning effort, and receive time. It is not included in
  provider history until a safe boundary.
- The M1c safe boundary is immediately after an assistant response or synthetic
  failure message is persisted. At that point the worker drains pending rows for
  the session in receive order, inserts them into `messages` using the same ids,
  deletes the drained pending rows, broadcasts the visible user rows, and runs
  another provider pass.
- This preserves logical order for concurrent posts: `U1`, `A1`, `U2`, `U3`,
  rather than physical receive order `U1`, `U2`, `U3`, `A1`.
- Server startup scans `pending_messages` and visible transcript tails. It
  starts recovery workers for queued rows and for sessions whose latest visible
  message is an unanswered user row. Visible-tail recovery uses unspecified
  reasoning controls because direct `messages` rows do not store the original
  per-turn setting in M1c.
- Cross-session workers may run concurrently, subject to the M1b provider
  semaphore.

The response worker reads the current stored LLM identity config before each
provider call. Missing or invalid stored config produces a persisted synthetic
assistant diagnostic instead of panicking.

M1c should reuse the M1b provider runtime so `llm_max_concurrent_requests`
applies to browser chat calls.

---

## 12. Failure Messages

If the provider call fails after all retries and fallback attempts, M1c persists
a synthetic assistant message and emits it through SSE like any other assistant
message.

Example:

```text
[Plexus could not complete the LLM request: provider returned HTTP 529. Try again later.]
```

Rules:

- role is `assistant`;
- content is a single text block;
- message is clearly marked as Plexus-generated;
- text is English-only in M1c;
- diagnostics are safe and concise;
- no secrets, raw API keys, authorization headers, stack traces, or large raw
  provider bodies;
- the synthetic row remains in history so the next LLM call can understand that
  the previous attempt failed.

M1c does not emit a separate SSE `error` event for provider failure when it can
persist this assistant diagnostic. Connection-level SSE errors may still happen
normally if the HTTP stream itself fails.

---

## 13. SSE Delivery

M1c should use an in-process broadcaster keyed by session UUID.

Persistence remains the source of truth:

- live broadcasts are an optimization for connected clients;
- stream replay reads from PostgreSQL;
- if a client disconnects, reconnect uses replay/`Last-Event-ID`.

On message insert, the server broadcasts the persisted `Message` object after
the insert succeeds. If broadcast fails because there are no subscribers, the
DB row is still authoritative.

M1c does not need durable event rows separate from `messages`. The message id is
the SSE event id for `message` events.

---

## 14. Tests

Automated tests must not require real credentials.

Minimum automated coverage:

- create web session with omitted title -> `"New chat"`;
- create web session with title -> trimmed title;
- rename title;
- reject empty rename title;
- session rows use `channel = "web"`, `chat_id = id`, and
  `session_key = web:{id}`;
- `UNIQUE (user_id, session_key)` behavior does not block another user from
  owning the same natural key shape where practical to test;
- POST text message persists a user row and returns `202`;
- POST omitted content, empty string, and empty array are accepted;
- POST base64 `data:image/...` block is accepted and persisted;
- external `http(s)` image URL is rejected in M1c;
- SSE replay emits persisted messages then `history_end`;
- SSE live stream receives user and assistant messages;
- response worker calls the hermetic fake provider and persists the assistant
  response;
- concurrent posts to one session do not create parallel response workers;
- provider synthetic failure message is persisted and secret-free;
- image compatibility failure triggers stripped retry;
- transient failures retry with backoff and eventually succeed/fail as
  expected;
- missing LLM config produces a persisted synthetic assistant diagnostic;
- auth/ownership checks prevent reading or writing another user's session;
- browser POST to a non-web session is rejected if a non-web fixture exists.

Focused checks:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- PostgreSQL-backed `cargo test --workspace --all-targets`
- `docs/API.yaml` parse/reference validation if tooling is available
- `git diff --check`

---

## 15. Automated Implementation Evidence

Automated verification from 2026-05-14 on branch `rebuild-m1-M1c`:

- `rtk cargo fmt --all -- --check`
- `rtk cargo clippy --workspace --all-targets -- -D warnings`
- `rtk env PLEXUS_TEST_DATABASE_URL=postgres://plexus:plexus@127.0.0.1:5432/plexus cargo test --workspace --all-targets`
- `rtk conda run -n Plexus python -c "import yaml, pathlib; yaml.safe_load(pathlib.Path('docs/API.yaml').read_text()); print('API.yaml ok')"`
- `git diff --check`

Result: automated checks passed.

---

## 16. Manual Live Smoke

M1c live smoke passed on 2026-05-14 with a real OpenAI-compatible MiniMax
provider.

Expected smoke path:

1. start the database;
2. run `cargo run -p plexus-server`;
3. register or log in as an admin;
4. configure real LLM values through `PATCH /api/admin/config`;
5. confirm provider validation passes;
6. create a web session with `POST /api/sessions`;
7. open `GET /api/sessions/{id}/stream`;
8. post a text message with `POST /api/sessions/{id}/messages`;
9. observe SSE `history_end`, user `message`, and assistant `message`;
10. post an inline base64 image message to a VLM provider and observe a real
    assistant response, or deliberately use a non-VLM provider and confirm
    image-strip fallback produces a safe response or diagnostic;
11. confirm persisted history replays after reconnect.

`scripts/m1c-smoke.py` automates steps 3-11 against an already-running Plexus
server and database. It loads provider credentials from local ignored env files,
including `scripts/.env`, but deliberately does not start services, reset
databases, or manage Docker containers.

Actual smoke evidence:

- temporary Plexus server on an isolated PostgreSQL database;
- admin registration and `GET /api/me` succeeded;
- `PATCH /api/admin/config` validated the MiniMax endpoint/model through
  Plexus provider validation;
- `POST /api/sessions` created a `web:{id}` browser session;
- `GET /api/sessions/{id}/stream?replay_limit=0` emitted `history_end`;
- `POST /api/sessions/{id}/messages` returned `202`;
- live SSE emitted the persisted user message and a real assistant message;
- assistant `reasoning_content` was present;
- `GET /api/sessions/{id}/messages` returned persisted user and assistant rows;
- reconnect replay emitted both persisted rows before `history_end`.

---

## 17. Documentation Updates

M1c implementation must update:

- `docs/API.yaml`
  - session routes use `{id}` UUID;
  - add/clarify `POST /api/sessions`;
  - add/clarify `PATCH /api/sessions/{id}` for title rename;
  - clarify browser message writes require `channel = "web"`;
  - document content-block input and data-URL-only image support in M1c;
  - keep cancel marked target/non-M1c.
- `docs/SCHEMA.md`
  - add `sessions.title`;
  - define web `chat_id = id` and `session_key = web:{id}`;
  - change session key uniqueness to `(user_id, session_key)`.
- `docs/DECISIONS.md`
  - revise ADR-098 or add a follow-up note that browser REST writes are
    UUID-addressed and web-only;
  - keep natural session keys for adapter lookup.
- `docs/SYSTEM_PROMPT.md`
  - resolve the runtime block persistence conflict in favor of ADR-094.
- `docs/reference/superpowers/specs/2026-05-12-plexus-m1-living-design.md`
  - update M1c status and evidence as the milestone progresses.

---

## 18. Exit Criteria

M1c can move from draft/design to implementation only after this spec is
reviewed and approved.

M1c implementation is complete when:

- session lifecycle APIs work for browser sessions;
- message POST persists user rows and starts serialized response work;
- SSE replay/live behavior works;
- optional `SOUL.md` and `MEMORY.md` are included in the prompt;
- text, empty, image-only, and mixed messages persist as content blocks;
- provider calls use content arrays and `stream=false`;
- image-strip fallback works;
- provider failure persists a safe assistant diagnostic;
- automated PostgreSQL-backed tests pass;
- docs are synchronized;
- the user completes the real-provider manual live smoke.

Only after the manual smoke passes should M1c be marked `Verified`.
