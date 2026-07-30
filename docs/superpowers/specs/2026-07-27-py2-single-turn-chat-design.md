# Py2 — Streaming Single-Provider-Turn Chat Design

**Milestone:** Py2 (depends on Py1)
**Status:** approved on 2026-07-30; production implementation is in review

## Summary

Py2 builds the browser chat lifecycle that Py3's tool loop will reuse. It
implements:

- `POST /api/sessions/{id}/messages`;
- `GET /api/sessions/{id}/messages`;
- real Anthropic Messages streaming;
- transient text/thinking token preview over NDJSON;
- complete-message persistence in PostgreSQL;
- detached per-session runners;
- durable same-session overlap through `pending_messages`;
- persisted provider-turn lifecycle through `turn_runs`;
- provider-wide concurrency control;
- admin-configured `llm_max_output_tokens`, with effective default `16384`.

Py2 performs exactly one Anthropic provider call per provider-visible turn. A
pending batch may cause the detached runner to start another turn after the
first one reaches its safe boundary, but Py2 has no ReAct/tool iteration inside
a turn.

The central durability rule is:

> Live deltas are a best-effort preview. Complete messages, pending inputs, and
> turn lifecycle are durable. GET reconstructs chat state from PostgreSQL
> without replaying missed deltas.

## Resolved review decisions

| Topic | Accepted Py2 decision |
|---|---|
| Streaming | Use real provider streaming and emit text/thinking `token_delta` events. No `tool_progress` in Py2. |
| Output budget | `llm_max_output_tokens` is an admin-editable `system_config` key. Missing means effective `16384`; do not seed a row. |
| Same-session overlap | Persist the second and later inputs in `pending_messages`; do not return `409 session_busy`. |
| Public runtime metadata | Keep the server-generated runtime block in DB/provider history, but remove it structurally from public canonical and pending DTOs. |
| Prompt layout | Establish the final stable section order now; populate only content available in Py2. Treat it as a cacheable configuration snapshot, not immutable text. |
| GET shape | Return the full target `MessagesResponse`, including pending queue and persisted run status. |
| Images/files | Accept text, direct inline base64 images, and `effort`. Require `attachments=[]`; no MinIO/workspace attachments yet. |
| Mid-stream failure | Retry only before the first emitted delta. After a delta, persist a synthetic assistant error and fail the turn without persisting partial output. |

There are no unresolved architecture questions in this draft. Final maintainer
approval of the document remains the implementation gate.

## Goals

- Prove a complete persisted provider turn using an Anthropic-compatible fake
  provider.
- Establish the token-streaming wire contract Py3 will extend.
- Preserve accepted user input across overlap, disconnect, and process restart.
- Make one active provider turn per session enforceable both in memory and in
  PostgreSQL.
- Make GET sufficient for frontend recovery after a broken POST stream.
- Keep provider calls asynchronous and bounded by the shared configured
  semaphore.
- Keep provider history in Anthropic-native content-block form.
- Build the system/runtime prompt boundary without requiring Py4 workspace or
  Py10 channel implementations.

## Non-goals

- Tool definitions, tool execution, ReAct iteration, or `tool_progress`.
- Cancel/restart UI behavior.
- Compaction or context-window enforcement.
- Workspace/device attachment refs, MinIO, or server workspace files.
- Device WebSocket dispatch, client execution, MCP, cron, heartbeat, or channel
  adapters.
- Durable token logs or replay of missed deltas.
- Redis, multiple ASGI workers, or multi-node coordination.
- Frontend implementation.

## Canonical contract

Implementation must reconcile these documents rather than treating this spec as
an isolated source:

- `docs/DECISIONS.md`: ADR-011, ADR-013, ADR-026, ADR-032, ADR-034,
  ADR-094, ADR-098, ADR-101, ADR-117, ADR-121, ADR-122, and ADR-125.
- `docs/API.yaml`: HTTP request, response, DTO, and stream-event shapes.
- `docs/SCHEMA.md`: `messages`, `pending_messages`, `turn_runs`, and
  `system_config`.
- `docs/SYSTEM_PROMPT.md`: system snapshot, runtime block, public projection,
  and out-of-band execution context.

If implementation reveals a real contradiction, update the canonical document
deliberately before changing behavior.

## Architecture

### Suggested module layout

```text
server/src/openoctopus_server/
  api/
    sessions.py                 POST/GET route handlers
  chat/
    context.py                  provider-history projection
    prompt.py                   system snapshot + runtime block
    public_projection.py        safe public content sanitization
    runner.py                   detached session runner
    runner_registry.py          process-local reservations/subscribers
    stream.py                   NDJSON event encoding/subscription
  dto/
    messages.py                 request/response/event DTOs
  providers/
    anthropic.py                Anthropic SDK adapter + stream assembly
    config.py                   raw provider config snapshot
    limiter.py                  shared resizable concurrency gate
  services/
    messages.py                 persistence and cursor reads
    pending_messages.py         atomic pending drain
    turn_runs.py                durable run lifecycle/reconciliation
```

Names may change during implementation, but the boundaries must remain:

- routes validate/authenticate and translate HTTP;
- services own transactions;
- the runner owns turn sequencing;
- the provider adapter owns Anthropic wire behavior and retry;
- public projection never mutates stored/provider content;
- live subscriber state is process-local and disposable.

### Application-owned runtime

The FastAPI application owns:

- one `AsyncAnthropic`-compatible adapter/client lifecycle;
- one provider-wide concurrency limiter;
- one process-local session runner registry;
- one boot-scoped `runner_instance_id`;
- process-local subscriber queues for active and queued POST streams.

These resources are created during lifespan startup and closed during
lifespan shutdown. Routes do not construct SDK clients or semaphores per
request.

### One-worker deployment

Py2 runs exactly one ASGI worker. Async tasks provide cross-session
concurrency. Redis and cross-worker stream routing are absent.

PostgreSQL remains authoritative for messages, pending input, and run state.
The runner registry improves scheduling and fan-out but cannot be required for
recovery correctness.

## Data model

### `messages`

Py2 persists:

- inbound canonical rows as `role="user"`, `message_kind="human"`;
- completed provider responses as `role="assistant"`,
  `message_kind="assistant"`;
- exhausted/terminal provider failures as `role="assistant"`,
  `message_kind="synthetic_assistant_error"`.

Every row contains a JSON array of Anthropic-compatible content blocks. Direct
inline image bytes remain base64 inside JSONB for Py2. No object storage is
introduced.

### `pending_messages`

Inputs accepted while a session has an active `turn_runs.status="running"` row
are inserted here. Pending rows:

- are durable;
- contain their server-generated runtime first block;
- are returned separately by GET;
- are not provider-visible until drained;
- preserve their UUID when moved into `messages`.

At the Py2 safe boundary, the runner atomically:

1. selects every pending row for the session in `(received_at, id)` order;
2. inserts matching canonical human rows with the same IDs;
3. deletes the selected pending rows;
4. commits;
5. starts the next provider turn from the resulting canonical history.

The Py2 safe boundary is after the current complete assistant response and
terminal `turn_runs` state have been committed. Py3 extends the definition to
require complete tool-result pairing.

### `turn_runs`

Each provider call has one durable run row:

- `id` is the public `turn_id`;
- `runner_instance_id` identifies the server boot;
- `status` is `running`, `completed`, `failed`, `abandoned`, or `cancelled`;
- `started_at` and `finished_at` provide lifecycle timestamps.

A partial unique index on `session_id WHERE status='running'` is the database
backstop for one active turn per session.

Before accepting traffic at startup, Py2 marks leftover `running` rows
`abandoned` and sets `finished_at`. Token previews were never durable, so they
are discarded. Pending rows are not reordered or automatically drained during
startup; the next normal session activity wakes recovery.

## Prompt and provider context

### System configuration snapshot

Py2 uses the final stable section order from `docs/SYSTEM_PROMPT.md`:

1. SOUL
2. MEMORY
3. Identity
4. Channels
5. Skills
6. Workspaces
7. Devices
8. Operating Notes

Only available Py2 material is populated. Missing future workspace, skill,
device, or channel data renders an empty/minimal section rather than a
milestone-specific prompt shape.

The snapshot is cacheable while its source configuration is unchanged. It may
change legitimately between turns when SOUL, MEMORY, channel configuration,
skills, workspace membership, or stable device capabilities change. Volatile
connectivity, quota usage, current run state, and locks do not belong in it.

### Runtime block

At ingress, before either canonical or pending persistence, the server prepends
a first text block using the deterministic Py2 form:

```text
<runtime>
time: <server timestamp with timezone>
channel: web
chat_id: <session UUID>
sender: partner:<authenticated user UUID>
trust: partner
</runtime>
```

The block is server-authored. Client-supplied text that resembles `<runtime>`
is ordinary user content and must not gain authority.

The provider sees the stored block during the current turn and future replay.
The public API does not.

### Public runtime projection

For canonical human messages and pending messages only:

1. inspect the first content block only;
2. require `type="text"`;
3. parse the exact anchored server runtime grammar;
4. require parsed `channel` and `chat_id` to match the owning session;
5. omit that one block from the response DTO;
6. leave all other blocks unchanged.

Do not concatenate text blocks or use a broad regex that could remove
user-authored content. Assistant and tool-result rows are not candidates.
Stored JSONB and provider projection remain unchanged.

## POST `/api/sessions/{id}/messages`

### Request

```json
{
  "effort": "high",
  "content": [
    {"type": "text", "text": "What is in this image?"},
    {
      "type": "image",
      "source": {
        "type": "base64",
        "media_type": "image/png",
        "data": "..."
      }
    }
  ],
  "attachments": []
}
```

Py2 rules:

- `{id}` is a UUID and identifies `sessions.id`.
- `content` is required and must contain at least one accepted block.
- Browser input accepts only `text` and direct inline base64 `image`.
- Empty text blocks, malformed base64, unsupported image media types, and
  unsupported block types are rejected before persistence.
- `attachments` is required for the stable request shape and must equal `[]`.
- `effort` may be omitted/null or one of `off`, `low`, `medium`, `high`,
  `xhigh`, or `max`.
- `null`, omitted, and `off` disable thinking. Other values request adaptive
  thinking and pass the matching output effort.

### Session creation and authorization

The browser generates the session UUID before its first send.

If the session does not exist, POST atomically creates:

```text
id          = path UUID
user_id     = authenticated user
channel     = "web"
chat_id     = path UUID as text
session_key = "web:" + path UUID
title       = "New chat"
```

If it exists, it must belong to the caller and be browser-writable
(`channel="web"` and `session_key` begins `web:`). Cross-user and disallowed
session access returns `404` without existence disclosure.

GET never creates a session.

### Atomic accept decision

The service serializes the accept decision for one session with a short
database transaction/session-row lock:

- if no turn is running but durable pending rows survived a prior
  completed/failed/abandoned run, insert the new input as pending, atomically
  drain the old rows plus the new row in `(received_at, id)` order, and create
  the new running turn from that full batch;
- if no turn is running and no pending rows exist, insert the canonical human
  row and a new `turn_runs(status="running")` row;
- if a turn is running, insert a `pending_messages` row;
- commit before emitting `message_accepted`.

Provider latency is never inside this transaction. The partial unique run index
handles races that bypass the process-local fast path.

The recovery case reports the new POST as `disposition="started"` and lists
every drained ID in `turn_started.message_ids`. It must not insert the newer
input directly ahead of older pending rows.

### Detached runner

After the durable accept:

- the route subscribes its response queue;
- the application schedules/wakes the session runner;
- the route streams queue events;
- disconnecting cancels only the subscription, not the runner.

The runner owns provider execution and persistence. It must not inherit the
request cancellation scope.

### NDJSON events

Every line is one JSON object followed by `\n`. The first event for a committed
POST is always:

```json
{"type":"message_accepted","message_id":"...","disposition":"started","created_session":false}
```

For a newly started turn, the normal order is:

1. `message_accepted(disposition="started")`
2. `turn_started(turn_id, message_ids)`
3. zero or more `token_delta(channel="text"|"thinking")`
4. `message_persisted(turn_id, message=<complete public assistant DTO>)`
5. `turn_finished(turn_id, status="completed", final_message_id)`

Provider deltas may be coalesced into small transport chunks, but the adapter
must forward them incrementally without waiting for the complete answer.
Deltas are never inserted into PostgreSQL.

For input accepted during an active turn:

1. emit `message_accepted(disposition="queued")`;
2. optionally emit `keepalive` while waiting;
3. either:
   - emit `stream_replaced` and close when a newer queued POST becomes the live
     subscriber; or
   - remain the newest subscriber and receive `turn_started` plus preview
     events for the entire drained batch.

All queued inputs remain durable regardless of which response owns preview.
`turn_started.message_ids` lists the drained human IDs in provider order.

Py2 never emits `tool_progress`. The schema reserves that event for Py3.

### Pre-stream errors

Validation, auth, provider-not-configured, and transaction failures that occur
before the streaming response opens use the normal JSON error contract.
Failures after `message_accepted` are represented by stream lifecycle events
and durable state; the already-committed user input is not rolled back.

## GET `/api/sessions/{id}/messages`

GET returns the full `MessagesResponse`:

```json
{
  "messages": [],
  "pending_messages": [],
  "status": "idle",
  "active_turn_id": null,
  "last_message_id": null,
  "pending_count": 0,
  "has_more_before": false
}
```

Rules:

- `messages` is canonical persisted history in chronological order for the
  requested cursor window.
- `pending_messages` is the complete session queue in `(received_at, id)`
  order and is not counted against `limit`.
- `status` is derived from the latest persisted run:
  - latest `running` -> `running`;
  - latest `failed` -> `failed`;
  - latest `abandoned` -> `abandoned`;
  - no run, `completed`, or `cancelled` -> `idle`.
- `active_turn_id` is non-null only for `running`.
- `last_message_id` is the newest canonical message in the whole session,
  independent of the cursor window.
- `pending_count` equals the durable pending queue size.
- `has_more_before` reports older canonical rows before the returned window.
- in-flight deltas are never returned.
- public message and pending content use the runtime projection above.

`before` and `after` are mutually exclusive. Without either cursor, return the
latest `limit` canonical rows but order the returned array chronologically.
Cursor comparison uses stable `(created_at, id)` ordering even though the
public cursor value is a message UUID.

### Recovery scenario

1. Turn A is streaming.
2. The user sends turn B; B is committed to `pending_messages`.
3. The browser connection drops.
4. GET reports A's canonical messages, B in `pending_messages`,
   `status="running"`, and A's `active_turn_id`.
5. The detached runner completes A, drains B, and starts a new run.
6. Later GETs show B as a canonical human message and eventually the complete
   assistant answer. Missed token deltas are intentionally absent.

## Provider adapter

### Configuration

The adapter reads raw values from `system_config`, never the redacted admin DTO:

- `llm_endpoint`;
- `llm_api_key`;
- `llm_model`;
- `llm_max_output_tokens` (effective default `16384`);
- `llm_max_concurrent_requests` (missing/zero means unlimited);
- context/compaction values reserved for later enforcement.

Missing provider identity fails before user-message persistence with
`503 provider_not_configured`.

Configuration is snapshotted when a new turn starts. Admin changes affect the
next provider turn, not one already running.

`llm_max_output_tokens` must be in `1..1_000_000`. When a context limit is
configured, output must not exceed it. The output budget includes thinking and
visible text; it does not add to or enlarge the input context window.

### SDK use

Use the official asynchronous Anthropic SDK where it preserves the accepted
wire contract. Disable SDK-owned automatic retries so OpenOctopus owns the
visibility boundary:

```python
AsyncAnthropic(
    api_key=config.api_key,
    base_url=config.endpoint,
    max_retries=0,
)
```

The request supplies:

- configured model;
- effective `max_tokens`;
- the cacheable system snapshot;
- chronological persisted provider messages;
- no tools;
- disabled/adaptive thinking fields derived from `effort`.

The SDK has no conversation memory; PostgreSQL is the source of truth.

### Stream assembly

The adapter:

- maps provider text and thinking stream events to `token_delta`;
- accumulates the original provider block order and metadata;
- validates the final complete response;
- returns one assistant content array for persistence.

Allowed Py2 assistant blocks are `text`, `thinking`, and
`redacted_thinking`. A provider `tool_use` is a protocol failure because Py2
sent no tools. Unknown unsafe block types fail closed and are not persisted
raw.

Normal public assistant projection retains visible thinking text but strips
thinking signatures and raw redacted-thinking data as required by API.yaml.
Provider persistence retains the full compatible blocks.

### Retry boundary and failures

OpenOctopus owns a maximum of three attempts (first attempt plus two retries)
with bounded exponential backoff for network errors, timeouts, HTTP 408/429,
and provider 5xx responses.

Retries are permitted only while the current attempt has produced no
text/thinking delta:

- before the first delta, a transient retry may begin a fresh stream;
- after the first delta, no transparent retry is allowed.

For an image/payload compatibility rejection under ADR-026, and only before a
live delta:

1. retain the full-fidelity persisted input;
2. remove only image blocks from the provider projection;
3. retry the text remainder under the same bounded policy;
4. store no `vision_stripped` session state.

On exhausted pre-delta failure or any post-delta failure:

1. discard the incomplete assistant accumulator from durable state;
2. persist one `synthetic_assistant_error` row;
3. update `turn_runs.status="failed"` and `finished_at`;
4. emit `message_persisted` for the synthetic row if a subscriber remains;
5. emit `turn_finished(status="failed")`;
6. proceed to the Py2 safe-boundary pending drain.

This can leave the transcript with a durable human row followed by a synthetic
error, but never with a partial assistant answer.

### Provider fingerprint

Assistant rows containing opaque thinking state store:

```text
sha256(normalized_endpoint + NUL + model)
```

The API key is excluded. On provider replay:

- a matching fingerprint allows full compatible thinking replay;
- a different/missing fingerprint removes `thinking` and
  `redacted_thinking` blocks while retaining visible text;
- if no blocks remain, omit that assistant row from provider projection.

Projection never rewrites the database row.

## Concurrency and pending subscribers

### Provider-wide limiter

All actual Anthropic HTTP attempts acquire the shared provider limiter.
Configured `0` or missing means unlimited. Positive values cap concurrent
attempts across sessions. Backoff sleep does not hold a permit.

An admin change resizes/replaces the limiter for subsequent attempts without
interrupting permits already held by active requests.

### Per-session runner registry

The registry tracks:

- whether a runner task exists;
- the active-turn subscriber;
- at most one newest queued subscriber;
- runner wake-up state.

When a newer queued POST arrives:

- the older queued subscriber receives `stream_replaced`;
- the older message remains in `pending_messages`;
- the new response becomes the preview candidate for the next drained batch.

The registry is not a durable queue and is intentionally empty after restart.

### Race handling

Correctness tests must cover two concurrent POSTs that both observe an
apparently idle session. The short database lock plus partial unique
`turn_runs` index must resolve them into:

- one canonical starting human message and one running turn;
- one durable pending human message;
- no duplicate provider calls for the same starting state.

## Error mapping

| Code | HTTP/stream behavior | Meaning |
|---|---|---|
| `provider_not_configured` | HTTP 503 before acceptance | Required LLM identity is missing. |
| `provider_unavailable` | synthetic error + failed turn after acceptance | Retryable failures exhausted or stream failed after a delta. |
| `provider_protocol_error` | synthetic error + failed turn after acceptance | Malformed/unsupported provider stream, including unexpected `tool_use`. |
| `invalid_message_content` | HTTP 400 before acceptance | Unsupported content block, malformed image, empty content, or non-empty attachments. |
| `not_found` | HTTP 404 | Session absent/not owned/not browser-writable without existence disclosure. |

There is no Py2 `session_busy` error.

## Testing

All automated provider tests use a local fake Anthropic-compatible HTTP server;
CI needs no real provider credentials.

### API and persistence

- implicit web-session creation uses the path UUID and expected key fields;
- ownership and browser-write allow-list return non-disclosing 404s;
- text, inline base64 image, and every effort value validate;
- unsupported blocks, malformed images, empty content, and non-empty
  attachments reject before persistence;
- `message_accepted` is emitted only after the row commits;
- complete assistant blocks persist once;
- no token delta is stored as a message;
- public GET strips only a valid server-generated first runtime block;
- runtime-looking user text in any other position survives unchanged;
- provider replay retains runtime metadata and full-fidelity inline images.

### Streaming

- fake provider chunks produce incremental text/thinking `token_delta` events;
- final block order matches the provider response;
- normal event order is accepted -> started -> deltas -> persisted -> finished;
- Py2 never emits `tool_progress`;
- disconnecting the POST does not cancel the runner;
- GET eventually observes the complete assistant row after disconnect.

### Pending overlap

- a second same-session POST persists to `pending_messages`, not 409;
- pending rows drain in `(received_at, id)` order and preserve IDs;
- multiple queued POSTs use latest-wins subscriber replacement while preserving
  every message;
- the drained batch produces one `turn_started.message_ids` list in order;
- cross-session turns may run concurrently subject to the provider limiter;
- DB race tests prove only one `running` row per session.

### Recovery and run state

- a run row exists before `turn_started`;
- completion/failure updates terminal state and timestamp;
- GET status mapping matches the latest run;
- startup marks leftover running rows abandoned;
- restart does not reorder or delete pending rows;
- the first post-restart inbound drains older pending rows before the new input
  and starts one correctly ordered batch;
- partial token preview is never recovered as canonical content.

### Retry and failure

- pre-delta transient failures retry at most twice;
- the limiter is released during backoff;
- image compatibility fallback strips only provider projection;
- a failure after the first delta makes no second provider attempt;
- post-delta failure persists only a synthetic error, never partial assistant
  content;
- unexpected provider `tool_use` fails the turn without creating an unpaired
  tool request.

### Admin config

- fresh GET returns effective `llm_max_output_tokens=16384` without a row;
- PATCH accepts `1..1_000_000`;
- output greater than configured context rejects atomically;
- a config change affects the next turn and not an active one;
- `max_tokens=16384` reaches the fake provider when unconfigured.

### Quality gates

From `server/`:

```bash
uv run ruff check .
uv run mypy src
uv run pytest
```

## Acceptance criteria

Py2 is complete when:

1. The canonical docs and implementation agree.
2. A browser POST can create a session and durably accept text or inline
   images.
3. A fake Anthropic provider streams text/thinking deltas through NDJSON.
4. The complete assistant response, not partial deltas, is persisted.
5. Disconnecting the subscriber does not stop the turn.
6. Same-session overlap is durable, latest-wins for preview, and drains at the
   safe boundary.
7. GET fully describes canonical messages, pending input, and durable run
   state while hiding runtime blocks from public content.
8. Provider retry behavior respects the first-delta boundary.
9. The admin API exposes and applies the effective 16K output budget.
10. The server runs with one ASGI worker and all quality gates pass.

## Deferred handoff

### Py3

- ReAct/tool loop;
- web_fetch and message tools;
- `tool_progress`;
- tool-result pairing and JIT collapse;
- cancel/restart;
- Py3's extended safe-boundary rule.

### Py4/Py5

- MinIO-backed workspace files;
- workspace/device attachment refs;
- server/device file tools and client dispatch.

### Later milestones

- cron/heartbeat;
- Discord/Telegram/other channel adapters;
- multi-worker/cross-node coordination;
- durable stream replay, only if product evidence requires it.
