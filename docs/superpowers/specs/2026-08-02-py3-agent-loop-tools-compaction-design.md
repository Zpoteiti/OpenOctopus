# Py3 Agent Loop, `web_fetch`, and Compaction Design

**Status:** approved for implementation
**Milestone:** Py3
**Depends on:** Py2 durable browser chat (`b20ded7`)
**Canonical decisions:** ADR-014, ADR-021, ADR-028–036, ADR-095, ADR-121,
ADR-125–129

## Outcome

Py3 turns the Py2 single-call chat worker into a serial ReAct loop. One
authenticated browser message may cause several provider calls. Each call gets
its own durable `turn_id`, may execute a serial `web_fetch` batch, and persists
every assistant/tool transition before continuing. Long histories are reduced
through the two-stage compaction workflow without removing audit rows.

## Scope

### Included

- Stage every inbound human message in `pending_messages`, including idle
  ingress, then promote at the Stage 1 boundary.
- Store semantic `message_kind` plus `is_compacted`; derive provider/public
  roles and remove the old stored `role` and `is_compaction_summary` columns.
- Run a hand-written serial ReAct loop with one `turn_runs` row per normal
  provider call and following tool batch.
- Accept and persist provider `tool_use` blocks.
- Register only `web_fetch`, merge its routing schema, and execute the server
  target.
- Normalize every real tool result with the untrusted-result warning.
- Persist real and synthetic tool results immediately and collapse adjacent
  tool-result rows only in Anthropic provider projection.
- Emit transient `tool_progress`, and keep the POST stream open across all
  `turn_id` values in one ReAct chain.
- JIT-repair tool uses left unpaired by a restart.
- Safe-boundary cancellation, including normal-completion-wins for a completed
  final provider response.
- Two-stage token-triggered compaction with rolling summary absorption.
- One authoritative runtime-block builder/parser and a small server-only
  `ToolContext`.
- Hard cap of 200 provider iterations and consecutive identical-call warning.

### Deferred

- The executable `message` tool and its delivery persistence helper (Py4).
- Workspace files, MinIO, attachments, media, buttons, and delivery routing
  (Py4+).
- Client/device WebSocket execution. Py3 advertises the currently executable
  `web_fetch` site only; paired-client dispatch lands with the client milestone.
- MCP tools, channel adapters, cron, heartbeat, and multi-worker coordination.
- Durable token/progress event logs or resumable POST streams.
- A generic runtime-context framework or speculative runtime fields.

## Exit Criteria

- A fake provider can return `tool_use`; the server executes `web_fetch`,
  persists a normalized result, calls the provider again, and completes.
- The chain exposes a different `turn_id` per provider call and does not close
  the POST stream after an intermediate tool batch.
- Parallel tool uses execute in model order, with durable result rows and
  started/finished progress for each.
- Restart repair and all cancellation boundaries leave every persisted
  `tool_use` paired.
- Provider context is valid Anthropic history built from active rows only.
- Stage 1 produces active order `summary, pending humans`; Stage 2 preserves
  the latest human batch; a later Stage 1 absorbs prior summaries.
- The complete server test suite, Ruff, and strict MyPy pass against disposable
  PostgreSQL without a real provider key.

## Durable Model

### `messages`

The final Py3 table has eight columns:

| Column | Meaning |
|---|---|
| `id` | Durable message identity. |
| `session_id` | Owning transcript. |
| `message_kind` | `human`, `assistant`, `tool_result`, `synthetic_tool_result`, `synthetic_assistant_error`, or `compaction_summary`. |
| `content` | Anthropic-compatible block array. |
| `delivery_refs` | Provider-hidden sidecar; remains `[]` in Py3. |
| `llm_fingerprint` | Provider/model identity for thinking replay. |
| `is_compacted` | `false` means active for provider replay and future compaction input. |
| `created_at` | Canonical transcript order, with UUID as tie-breaker. |

Role mapping is fixed:

- `user`: `human`, `tool_result`, `synthetic_tool_result`
- `assistant`: `assistant`, `synthetic_assistant_error`,
  `compaction_summary`

Public DTOs expose the derived `role` and `is_compacted`. They do not expose a
second summary boolean.

### `pending_messages`

Every inbound human row is inserted here first. If no run is active, ingress
also reserves a running `turn_runs` row and wakes the detached runner. If a run
is active, the row waits for the current assistant tool batch to become fully
paired. Promotion preserves the pending UUID and assigns canonical
`created_at` values in `(received_at, id)` order.

### `turn_runs`

A row means one normal agent-loop provider request plus the complete tool batch
returned by that request. Internal token-count/summary calls are outside this
public lifecycle.

For a chain `U1 -> provider A1(tool) -> result -> provider A2(final)`:

1. `T1`: promote `U1`, call provider, persist `A1`, execute/persist its tools,
   finish `T1=completed`.
2. `T2`: call provider with no newly promoted IDs, persist `A2`, finish
   `T2=completed`, close the live POST stream.

Only one `running` row exists per session. Creating the continuation row and
finishing the prior row happen at the tool-batch boundary.

## Runtime Context

`chat/runtime_context.py` owns the exact runtime grammar, construction, and
parsing. Ingress calls it once and stores the resulting first text block.
Public projection removes that block only after exact parsing and a matching
session channel/chat ID.

Py3 fields remain:

- ingress time
- channel
- chat ID
- partner sender UUID
- trust

`ToolContext` is separate and never built from model-visible text. It carries
authoritative `user_id` and `session_id` to the tool handler. Later services are
added only when a registered tool needs them.

## Tool Surface

### Registry and schema merge

The Py3 registry has one canonical source schema: `web_fetch`. The merge helper
deep-copies a source schema, injects the marked `openoctopus_device` property,
and appends it to `required`. The current enum contains only `server` because
Py3 has no client execution transport. The separate enum-extension helper is
implemented and unit-tested against intrinsic device fields for later tools,
but it does not make those tools executable.

The provider receives the merged schema on every normal call. The dispatcher
rejects unknown tools and non-server targets as ordinary error results rather
than breaking the loop.

### `web_fetch`

- Validate `http` or `https`, `extractMode`, and `maxChars` through the source
  schema model.
- Resolve the hostname and reject loopback, link-local, private, multicast,
  unspecified, reserved, and carrier-grade NAT addresses.
- Pin the validated resolved address for the connection so a second DNS answer
  cannot redirect the request to a forbidden target.
- Follow redirects only after validating every target.
- Use 10-second connect and 30-second total limits.
- Request identity encoding, reject compressed responses, and raw-stream no
  more than 5 MB before decoding.
- Extract readable text for HTML, return decoded text for textual content, and
  cap output at `maxChars` (default 50,000).
- Treat response bodies as untrusted data. Normalization prepends the exact
  server warning before persistence/provider replay.

Tool errors become `tool_result(is_error=true)` with a stable `code` and
server-authored diagnostic content. They are not retried by the dispatcher.

## Provider Projection

Load only `messages.is_compacted=false`, ordered by `(created_at, id)`, then:

1. Derive role from `message_kind`.
2. Strip incompatible thinking blocks when the stored provider fingerprint
   differs.
3. Collapse only adjacent tool-result rows into one `user` message.
4. Validate that every assistant `tool_use` has exactly one later matching
   result before crossing a human/assistant boundary.

Projection never mutates persisted rows or crosses an interleaving boundary to
repair invalid order.

## Agent Loop

For each normal provider iteration:

1. Observe shutdown and perform JIT repair of unpaired historical tools.
2. Under the session lock, capture current pending IDs and latest effort into an
   empty continuation turn. A non-empty capture never expands; later arrivals
   remain pending for the next provider-call boundary.
3. At an inbound boundary, evaluate Stage 1 before promoting that captured
   prefix. Promote it directly when no Stage 1 summary is needed, or atomically
   with its summary when needed.
4. Build active provider context and merged tool schemas.
5. Evaluate Stage 2 for mid-turn assistant/tool growth; rebuild context after a
   compaction commit.
6. Emit `turn_started`, call the provider, and stream transient deltas.
7. Persist the complete assistant row before any dispatch.
8. If there are no tools, clear a late cancel flag, complete the turn, emit the
   persisted message and terminal event, and close the chain stream.
9. If cancellation is set, persist synthetic cancellation results for every
   returned tool, add the stop marker, clear the flag, and finish cancelled.
10. Otherwise execute tools serially. Emit `tool_started`; persist the real or
    error result; emit `tool_finished`.
11. After each completed tool, check cancellation. The completed result stays;
    remaining tools receive synthetic cancellation results.
12. Once the batch is fully paired, atomically finish this run and create the
    continuation run. Capture and promote the pending-human prefix at that safe
    boundary before the next provider call.

Provider/tool failure rules:

- Provider failure after retry persists `synthetic_assistant_error` and ends
  the chain failed.
- Tool failure is data for the next provider call and does not end the chain.
- Empty/unsupported provider output is a provider protocol failure.
- After 200 provider calls, persist a synthetic assistant error and stop.
- Three consecutive identical `(tool name, canonical args)` calls add the
  documented human warning before the next provider request.

## Cancellation and Restart

- Idle cancel is a no-op and leaves `cancel_requested=false`.
- Cancel during a final provider response: persist it, clear the flag, complete
  normally, no stop marker.
- Cancel during a provider response containing tools: persist the assistant,
  synthesize all tool results, add stop marker, finish cancelled.
- Cancel during a tool: let it finish, synthesize only remaining results, add
  stop marker, finish cancelled.
- Restart: before the next provider call, find missing results in the active
  tail and persist `server_restart` synthetic rows. Existing real results are
  retained.

## Compaction

Compaction is disabled unless both context-window and threshold config are
present. Trigger when:

`max_context_tokens - full_request_token_count < compaction_threshold_tokens`

The count covers the system snapshot, merged tools, active provider history,
and the captured pending-human prefix when testing Stage 1. The provider adapter
owns counting for the configured model and mirrors the normal request controls,
limiter, retries, and image fallback; tests inject deterministic counts. Summary
generation uses no tools, disabled thinking, and
`max_output_tokens = compaction_threshold_tokens - 4000`. Configuration is
invalid unless
`4001 <= compaction_threshold_tokens < max_context_tokens`.

### Stage 1

At a user boundary, select every active canonical row before the pending batch.
If triggered and the selection is non-empty:

1. Generate a summary outside a DB transaction.
2. Re-lock the session and selected rows.
3. Verify the source rows are still active and the pending boundary is still
   present.
4. In one commit, mark sources compacted, insert the active summary, promote
   the captured ordered pending prefix, and delete those pending rows. Messages
   accepted while summary generation was running remain pending for the next
   boundary.

Resulting active order is `S1, U11`. On a stale selection, discard the summary
and reevaluate. No database transaction spans a provider request. Summary calls
end with a synthetic user instruction to produce the summary.

### Stage 2

Before a continuing provider call, preserve the latest active external human
boundary, identified by its exact server-generated runtime first block, and
select every active row after it. Server-authored `human` markers such as the
repeated-tool warning remain part of this compactable tail. If no
runtime-bearing human exists, fall back to the latest active human for legacy
compatibility. If triggered and the selection is non-empty, summarize and
atomically replace those selected rows.
Resulting active order is `..., U11, S2`. A later Stage 1 may select and absorb
both earlier summary rows. If `S2` is the final active row, normal provider
projection appends a non-persisted user instruction to continue the preserved
task. After either stage commits, recount the exact rebuilt normal request once;
if it still crosses the trigger, fail before issuing that normal request.

## Verification

### Unit tests

- runtime codec exact match/mismatch and public stripping
- message-kind role mapping and active-row filtering
- valid/invalid tool-result collapse and JIT repair
- schema injection/extension without source mutation
- tool-result normalization and truncation
- `web_fetch` validation, SSRF cases, redirects, extraction, caps, and errors
- deterministic Stage 1/Stage 2 selection and stale-commit rejection

### PostgreSQL-backed/API tests

- idle ingress stages then promotes with stable UUID
- multi-call ReAct stream, durable rows, and distinct turn IDs
- serial multiple tools and transient progress order
- tool failure followed by provider recovery
- pending follow-up drains only after complete tool pairing
- cancellation at provider-final, pre-dispatch, and mid-batch boundaries
- restart repair with partial prior results
- both compaction stages and later summary absorption
- history returns compacted audit rows but provider receives active rows only

### Quality gates

Run from `server/` against disposable PostgreSQL:

```bash
.venv/bin/pytest -q
.venv/bin/ruff check src tests
.venv/bin/mypy src
```
