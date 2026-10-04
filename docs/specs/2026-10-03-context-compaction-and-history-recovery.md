# Context compaction and conversation history recovery

**Status:** implementation direction superseded by the
[Pydantic AI Harness runtime design](2026-10-03-pydantic-ai-harness-runtime.md).
Implementation has not started. The new design carries forward the recovery and
isolation requirements while replacing the proposed custom runtime algorithms.
**Date:** 2026-10-03
**Inspected OO revision:** `d5e97c0a7a05`
**Reference Codex revision:** `ca466061d64f`

## Outcome and scope

An agent continues a long task with a compact checkpoint, the current user
request and recent complete tool exchanges. It can recover older messages and
saved tool results through a read-only, conversation-scoped `history` tool.
Oversized results are preserved within explicit storage limits before OO makes
the shorter excerpt used in the prompt.

This specification includes completion validation, compaction selection and
projection, checkpoint provenance, bounded result storage, history retrieval,
permissions, lifecycle handling and regression tests. The server decides when
to compact and when to commit; the LLM produces the checkpoint text.

Dream, `MEMORY.md`, cross-conversation search, agent-authored persistent notes,
new context-reset tools, semantic search, relevance classifiers and a new
frontend history browser are outside this change. Compaction has no Jev
dependency. Existing provider support and tool execution semantics remain the
baseline. Development schema changes can be direct; no data migration or
backward-compatibility layer is required for this devbox.

## Current behavior and the changes

Current code is authoritative where historical design documents differ.

| Area | Current implementation | Proposed behavior |
| --- | --- | --- |
| History | Original `messages` remain; `is_compacted` selects provider replay. | Retain this mechanism and add checkpoint provenance and bounded result payloads. |
| Inbound boundary | Stage 1 replaces all active history with a summary before promoting the captured pending messages. | Preserve a recent complete segment as well as incoming messages; summarize older material. |
| Continuing run | Stage 2 replaces the activity after the latest external human boundary. | Preserve the current request and newest complete exchanges; summarize older eligible activity. |
| Summary acceptance | Nonempty text is sufficient; `ProviderResult` omits the completion reason. | Require an explicitly completed, structurally valid checkpoint before any history replacement. |
| Result handling | Several tool paths shorten results before persistence; server MCP also rejects text above its 16,000-character mapping credit. | Separate prompt excerpt size from accepted-result byte limits; archive eligible results before excerpting. |
| Recovery | Saved rows are available to application history, but there is no agent history tool. | Agents can list, search and read authorized saved content in this conversation. |
| Counting | Packaged `o200k_base` estimates the request locally. | Keep this estimator and provider-authoritative context rejection. |

Implementation anchors:

- [Selection and commits](../../server/src/openctopus_server/chat/compaction.py)
- [Context projection](../../server/src/openctopus_server/chat/context.py)
- [Preparation and summary generation](../../server/src/openctopus_server/chat/runner.py)
- [Provider result and streaming](../../server/src/openctopus_server/provider/anthropic.py)
- [Tool dispatch and normalization](../../server/src/openctopus_server/tools/registry.py)
- [Server MCP result mapping](../../server/src/openctopus_server/mcp/result.py)
- [Current token estimator](../../server/src/openctopus_server/chat/token_estimator.py)

## Required invariants

1. Compaction changes prompt visibility, never the contents of original saved
   messages. A new checkpoint is a new message.
2. Current user input is retained verbatim. Tool calls and their complete result
   batches remain paired. In-flight tool batches are never compacted.
3. A failed, cancelled, stale, empty or output-limited summary does not change
   active history or promote pending messages through a compaction commit.
4. Retrieval reads stored evidence. It does not execute the original tool,
   reconnect an MCP, contact an external service or recreate an expired process.
5. Every retrieval is scoped by trusted `ToolContext`, current ownership and
   the active turn's tool profile. A guessed ID grants no access.
6. The model can distinguish a prompt excerpt, a completely retained result,
   a source-limited result and a result whose omitted content was not retained.
7. Storage, search and model responses have finite bounds. No limit is described
   as a judgment that the omitted content was unimportant.

## Stored history and prompt projection

Keep `messages.content` and `is_compacted`. Add typed checkpoint metadata on
`compaction_summary` rows containing the exact selected source message IDs,
the protected user-boundary IDs and the provider fingerprint used to generate
the checkpoint. Previous checkpoints can be sources of the next checkpoint;
their provenance remains available without duplicating the underlying text.
The server records these IDs; the model does not choose the authoritative list.

There is at most one active checkpoint per conversation after a successful
compaction. Its sources can be noncontiguous because a protected current request
can sit between older material and recent tool exchanges.

Canonical history remains ordered by `(created_at, id)`. Provider projection
places the active checkpoint before the retained raw messages, which otherwise
keep canonical order. Do not backdate the checkpoint or rewrite source
timestamps to achieve this. Pending input follows retained history. The normal
current system prompt and current tools are built through their existing paths.

The checkpoint is assistant/context material, not a new system or developer
instruction. Recorded tool content, quoted external messages and old assistant
claims retain their source identity and trust level. A checkpoint does not grant
an attachment, device or tool capability that is no longer authorized.

## Compaction workflow

### Budget policy

The following are proposed implementation defaults, for review. They do not
require additional administrator settings in this change.

Let `W` be configured context capacity, `O` the configured normal output limit,
`H` the configured compaction headroom and `E` the estimated complete next input
including system, tools, retained messages and captured pending input.

- Compaction stays disabled unless both `W` and `H` are configured.
- Safety allowance `M = max(1024, ceil(0.02 * W))`.
- Trigger when `W - E < max(H, O + M)`.
- Checkpoint output allowance `S = min(4096, O, max(1, floor(0.05 * W)))`.
  This replaces the coupling between summary length and `H - 4000`.
- Preferred retained recent-history allowance `R = min(8192, floor(0.10 * W))`.
- Aim for rebuilt input at or below `min(floor(0.65 * W), W - O - M)`.
  This is a planning target, not a provider-independent hard context gate.
- With the removed `H - 4000` dependency, validate `1 <= H < W`; keep the
  existing admin field and explain that it is remaining-context headroom.

Current system instructions, always-on skills, pending input and the protected
user boundary are not silently cut to meet these targets. If the fixed content
already makes the target impossible, retain it. Local estimates guide selection;
the provider still decides whether the final request fits, as in the current
provider contract. A provider context rejection becomes the existing durable
error. There is no unbounded compact/retry loop.

### Selection at a safe boundary

1. Finish or repair the current tool batch through the existing lifecycle before
   selecting sources. Snapshot active row IDs, provider configuration and the
   captured pending prefix.
2. Protect the current external user boundary. At an inbound boundary, this is
   the captured pending batch. During continuation, retain the human inputs
   belonging to the latest accepted batch, identified from the most recent
   `TurnRun.input_message_ids` whose inputs were promoted into this Session's
   `messages`. Continuation runs with empty input IDs and abandoned/unpromoted
   batches do not reset it. Server-generated internal markers do not reset it.
3. Group each assistant tool-use message with all its corresponding result
   messages. Treat this group as indivisible. Plain conversational messages are
   also eligible for the recent segment.
4. Retain the newest contiguous sequence of complete groups within `R`.
   Always retain the newest complete tool exchange of an unfinished run, even
   if that exchange exceeds `R` after normal result excerpting. Keep the
   protected user boundary independently of this recent segment.
5. Select the remaining older active material, plus the previous checkpoint,
   for summarization. Every newly compacted original row must be represented in
   the summary input. Use the same bounded result excerpts visible to the model;
   include their stored-result references rather than loading full payloads.
6. Skip compaction if there are no newly eligible original rows. Repeatedly
   summarizing the same checkpoint alone is not useful progress.

Retain the two existing scheduling opportunities (incoming messages and
mid-run growth), but share this selection policy. Selection is mechanical;
there is no classifier deciding whether a tool result is important.

### Checkpoint generation and validation

Make one non-tool LLM request using the existing shared provider limiter, with
thinking disabled. Give the summarizer the previous checkpoint and selected
source material labeled with server-generated message IDs. Always include the
protected current human batch as clearly labeled continuation context so the
summarizer understands the task behind the tool results. Count it in the
summary request's input budget. The retained recent segment may also be supplied
as context. These retained inputs are not sources to be marked compacted; if the
summary request cannot fit, fail with the original history unchanged.

Require these Markdown sections, each containing text or an explicit `None`:

- Goal and constraints
- Progress and decisions
- Evidence and unresolved results
- Next steps

The prompt must distinguish completed actions, proposed actions, failed actions
and unknown execution outcomes. Preserve important error text, identifiers,
paths and uncertainty, and cite message IDs for supporting evidence. Summaries
must not convert retrieved tool text into instructions. Markdown avoids a
dependency on provider-specific JSON-schema generation.

Extend `ProviderResult` to carry the provider's completion reason and verified
stream completion. For checkpoint generation accept only a terminal
`message_stop` with `stop_reason=end_turn`, nonempty text and the required
sections. Reject EOF without completion, `max_tokens`, refusal, tool-use output,
unknown/missing completion reason and malformed output. This check does not
change the interpretation of ordinary successful tool-use responses.

Validate every cited ID against the prepared same-session source/context IDs
and provenance of prior checkpoints. A model-invented ID fails validation. The
server appends the checkpoint's own ID and a bounded instruction describing
how to recover source details; it does not dump all provenance IDs into every
normal prompt. Syntax and completion checks cannot prove semantic accuracy;
original-history recovery is still necessary.

Build the candidate next prompt before committing. Accept a checkpoint only if
it reduces the local estimated input size. Hitting the preferred target is not
required when protected content prevents it. At most one summarization attempt
is made per preparation boundary; an invalid or failed attempt ends that run
through the normal error path with originals still active. If the summary
request itself exceeds the provider limit, preserve history and report failure;
this version does not silently drop summary input or add recursive chunking.

### Commit and continuation

Generate the checkpoint outside the database transaction. Then acquire the
existing conversation lock and verify the selected rows, previous checkpoint,
protected boundary, active run, cancellation state and captured pending prefix.
A configuration change that invalidates the prepared request requires a fresh
preparation. A new pending boundary supersedes mid-run compaction.

In one transaction: mark exactly the selected rows compacted, insert the
checkpoint and provenance, and promote exactly the captured pending prefix
when applicable. Newer inbound messages stay pending for their normal boundary.
On stale selection, discard the draft and return to normal boundary selection.
Retain cancellation-safe cleanup and the existing ownership of a running turn.

Rebuild the next normal request from committed state and current tools. Retained
messages appear once; checkpoints do not contain executable tool-use blocks.
If the estimate still exceeds the preferred target, make the normal provider
request once. Do not compact again without new eligible source material.

## Oversized result preservation

### What is preserved

Preserve the complete **accepted, normalized result of this particular call**
before OO's prompt-only shortening. This is not a promise to retain an entire
file when the call requested one page, all future stdout from a running process,
bytes an upstream tool omitted, or content rejected by transport/media checks.
Source limits and partial-result markers remain visible after storage/retrieval.

Apply this at the shared result persistence boundary for built-ins, client
tools and server/client MCP. Move server-side prompt truncation out of upstream
normalization into excerpt projection. For server MCP, the text excerpt credit
must no longer reject otherwise valid results below the accepted byte ceiling.
Keep atomic rejection of malformed content, unsupported media and oversized
transport payloads; failed MCP results retain their existing disclosure policy.

Client MCP already bounds returned frames by byte credit. Archive accepted
frames before `_bounded_mcp_content` loses text. Client command/file pagination,
output selection and transport credits remain acquisition limits. Audit each
truncation point: a presentation cap moves after capture; an intentional
acquisition/resource limit remains and must be reported as such. In particular,
`web_fetch.maxChars` is the caller's requested result cap: history preserves that
selected result, not downloaded bytes beyond the requested selection. Subsequent
prompt-only shortening can archive the selected result. The fetch body's 5 MB
read ceiling and bounded HTML conversion must report source-limited output, or
unknown source completeness when an exact limit event cannot be determined.
Neither may be advertised as the complete remote document. No background replay
or new client spool protocol is introduced.

### Storage and proposed limits

Use PostgreSQL for this bounded data alongside the existing conversation. This
allows the result message and recoverable payload to commit together and lets
session deletion cascade without introducing object-store cleanup machinery.

Add `tool_result_payloads`, keyed by result `message_id` with a cascading foreign
key. Store immutable normalized content and its canonical encoded byte count.
Store a payload only when the ordinary message contains an excerpt. Small
results remain inline without a second full copy. Payloads are excluded from
ordinary history lists, prompt construction and Dream input.

| Limit | Proposed value / rule |
| --- | --- |
| Full mapped result | At most 12 MiB of canonical UTF-8 JSON; an existing smaller source/transport limit still applies. |
| Extra payload storage per conversation | 64 MiB. |
| Extra payload storage per user | 256 MiB across owned conversations. |
| Prompt excerpt | Existing per-tool character allowance, with a 128 KiB UTF-8 ceiling for excerpt text. |
| History response | At most 16,000 content characters and 128 KiB total serialized response, including metadata. |

These are fixed initial constants, not additional UI controls. Count logical
encoded bytes before PostgreSQL compression. Check archive quotas atomically
under a per-user archive-quota lock, with a consistent conversation-then-user
lock order. Two conversations must not both consume the same remaining quota.
Existing execution/materialization admission and transport limits continue to
bound in-flight work; archiving adds no unbounded queue or background copies.

Keep source order and typed blocks. For oversized text, show beginning and end
within the combined excerpt allowance, with an explicit omission marker.
Preserve call identity, success/error state, stable error code and existing
execution metadata outside the excerpt. Do not splice structured JSON into a
fake valid object: a partial serialization is labeled as an excerpt. Supported
media follows existing validation and byte limits; this version's history tool
returns media descriptors, not base64 or a new media-download capability.

Each result records enough metadata to expose:

- whether content is inline, archived, or omitted without an archive;
- the stable message reference and known original/retained size;
- whether the source reported partial output or its completeness is unknown;
- the reason omitted content is unavailable, such as a source limit or quota.

Allocate the result message ID before producing the reference, then persist
message, excerpt metadata, optional payload and existing delivery side effects
in the same transaction. Never publish a recoverable reference before commit.

When archive limits are exceeded, persist the bounded excerpt and an explicit
`not retained: storage limit` marker. Keep the actual tool outcome: successful
execution is not relabeled as failed execution. Do not evict older archived
results silently and do not rerun the tool. A database failure follows the
existing persistence/unknown-outcome handling; it cannot produce a dangling
successful archive reference. Cancellation after an externally issued tool
retains the existing no-blind-replay guarantees.

Payloads live as long as their messages and are removed by cascade on normal
conversation/user deletion. Deleting a conversation releases archive quota.
There is no separate expiry timer. Storage limits bound only the new extra
payloads; this change does not introduce a global transcript-retention policy.

## Agent history tool

Register one `PURE_SERVER` tool named `history`, with `action` selecting `list`,
`search` or `read`. It accepts no user ID, session ID, device, filesystem path or
URL. Reject unknown arguments and invalid action-specific combinations.

| Action | Inputs | Behavior |
| --- | --- | --- |
| `list` | Optional cursor, optional `compacted_only`, limit (default 10, maximum 20). | List saved messages with ID, kind, timestamp, speaker, short preview and result availability. |
| `search` | Required literal query, optional cursor, optional `compacted_only`, limit (default 10, maximum 20). | Search visible text/arguments and retained textual results; return matching excerpts and exact read coordinates. |
| `read` | Required message ID; optional text-block index, zero-based character offset, limit (default 8,000, maximum 16,000). | Read the saved original or archived result in bounded slices, with continuation coordinates. |

Query length is 1–256 characters; search is case-insensitive literal matching,
not regular expressions, SQL wildcards or embeddings. It can find text omitted
from a prompt excerpt. Results use deterministic newest-first message order;
reads return content in original block/character order. List/search freeze an
upper message boundary on the first page so their own tool-result messages do
not cause pagination drift.

Bound each search call to 200 examined messages and 1 MiB of searchable UTF-8
text, with a cursor that can resume inside a large result. Preserve enough
overlap at slice boundaries to find a query spanning two slices without
duplicate matches. Return a continuation cursor even when a bounded scan finds
no match. Use keyset pagination and bounded database reads; never materialize
the conversation or all archived results in application memory. Cursor scope,
query and offsets are validated on every call; a cursor is not authorization.

Search/read operate on an explicit safe projection: human and assistant text,
tool names/arguments, outcomes, retained text and checkpoint text. Exclude
thinking/redacted-thinking blocks, signatures, hidden runtime-control blocks
and binary payloads. Do not join provider/device configuration or credentials
into the result. Ordinary source text may itself contain sensitive information;
this projection is not a semantic secret detector. Preserve speaker
classification and tool identity as metadata. A text excerpt never claims that
every binary or upstream byte is recoverable.

Every call checks session ownership using `ToolContext.user_id/session_id` and
requires `owner_full` at dispatch and execution. `message_only` turns neither
see the schema nor bypass the gate by inventing a call. Channel runs must still
pass their current binding-generation authorization. Scope is the same Session,
including its compacted messages, never other conversations owned by the same
user. An inaccessible or missing ID returns the same generic not-found result.

Recovered content is an ordinary untrusted tool result. It does not reinstate
old instructions or grant current tool/attachment permissions. References are
for exact saved evidence, not a request to repeat an external operation.

History responses are persisted as normal bounded tool results. They are not
archived again as new full payloads: enforce the history tool's response bound
at construction, and give next-read coordinates when more content exists.
This prevents retrieval from creating recursive copies of a large result.

## Example and user-facing behavior

A diagnostic tool returns 80,000 characters with an important error in the
middle. OO saves the accepted result and presents a bounded excerpt with a
message reference. After several more steps, compaction creates a checkpoint
and keeps the latest request and latest complete diagnostic exchange.

If the checkpoint lacks the exact error, the agent searches the earlier result,
then reads the matching range using its message ID. The returned text is from
the original execution, even if the MCP connection has closed or the device is
offline. No diagnostic command is rerun to recover old output.

The current chat view continues to show canonical messages and existing
compaction summaries. Excerpt text says that more was saved and includes its
reference, or explains that omitted content is unavailable. A summary failure
uses the normal visible error path. This change adds no archive editor,
settings screen or dedicated browsing/download API; a later UI can reuse the
same permission-checked service.

## Failure and concurrency contract

| Situation | Required result |
| --- | --- |
| Summary hits output limit or stream ends early | Reject checkpoint; original active rows and pending boundary remain intact. |
| New user input arrives during mid-run summarization | Discard stale draft; handle the pending boundary. |
| Cancellation or restart during summary generation | No partial checkpoint commit; normal turn recovery can retry later. |
| Transcript/configuration changes before commit | Revalidate and prepare again; never mark an unselected row compacted. |
| Summary is valid but does not reduce estimated size | Keep originals and report compaction failure for this attempt. |
| Protected content prevents reaching target size | Retain it; perform one normal provider request after a successful reduction, or skip compaction if nothing is eligible. |
| Result exceeds archive capacity | Preserve excerpt, original outcome and explicit unavailability metadata. |
| Archived payload write fails | Roll back its result transaction; no usable reference is published and no external action is blindly replayed. |
| Session is deleted during read/storage | Respect deletion and normal lifecycle locks; create no orphan payloads. |
| Old device/MCP is offline | Read retained evidence normally; retrieval makes no device/MCP calls. |

## Acceptance criteria

Use mocked provider streams and existing disposable PostgreSQL fixtures; live
model quality is assessed separately and is not implied by deterministic tests.

1. Both compaction boundaries preserve pending/current human input and complete
   recent exchanges. No tool-use/result pair is split. A protected batch with
   several human messages survives continuation and restart.
2. Provider projection contains exactly one active checkpoint before retained
   history; canonical timestamps/order are unchanged. Repeated compaction
   preserves source provenance and avoids duplicate replay.
3. Terminal `end_turn` with valid checkpoint text succeeds. Nonempty
   `max_tokens`, missing terminal events/reasons, refusal, tool-use output,
   missing sections, invented references and no-size-reduction fail without
   changing active/pending rows. Mid-run summary input always includes the
   protected human batch as non-compacted context.
4. Late pending input, stale selection, config replacement, cancellation and
   deletion during generation/persistence preserve the existing run ownership
   and atomicity guarantees.
5. A middle-of-output sentinel omitted from an oversized prompt excerpt can be
   searched and read exactly after compaction and server restart. Test built-in,
   client MCP and server MCP paths. Neither recovery call invokes the original
   tool or reconnects its transport.
6. Inputs at and just above byte/character/storage boundaries behave as
   specified, including multibyte text, multiple MCP blocks and structured JSON.
   Source-limited output never advertises unavailable bytes as retained. Verify
   explicit `web_fetch.maxChars`, the fetch byte ceiling and HTML conversion
   limits independently from prompt excerpting.
7. The result message and payload commit together. Concurrent conversations
   respect the per-user quota; deletion releases it. Interrupted transactions
   leave no dangling references or orphan rows.
8. Guessed IDs/cursors from another user or conversation, `message_only` runs
   and stale channel authorization cannot retrieve content or infer whether a
   foreign message exists. Thinking, signatures and binary payloads are absent.
9. Search handles matches beyond excerpts and across scan boundaries; pagination
   remains deterministic while new messages arrive. Response, scan and materialized
   memory sizes remain bounded, including calls returning zero matches.
10. A history read can itself enter a later checkpoint without recursively
    archiving or duplicating its source payload. Dream still reads its existing
    original human/assistant text inputs and excludes checkpoint/tool payloads.
11. Missing compaction configuration keeps compaction disabled. Local estimates
    never become a replacement for the provider's final context decision, and
    irreducible oversized instructions/user input are not silently truncated.

During implementation run focused provider, compaction, transcript projection,
tool-result/MCP, permission and lifecycle tests; then the required backend
suite, Ruff and MyPy. Run client checks if acquisition/result handling changes
there. Update frontend tests only for affected contracts or rendered markers.

## Implementation sequence after approval

1. Expose provider completion state and add checkpoint validation regression
   tests; preserve normal tool-use and partial-output failure behavior.
2. Implement bounded result retention/excerpt metadata and atomic lifecycle;
   move relevant prompt caps after capture and test each result path.
3. Add permission-scoped history projection, paging and the `history` tool.
4. Implement shared retention selection, checkpoint provenance and explicit
   provider ordering; exercise repeated compaction and interrupted runs.
5. Update `SCHEMA.md`, `TOOLS.md`, `SYSTEM_PROMPT.md`, affected API/protocol
   contracts and current decisions. Mark superseded portions of the original
   Py3 spec accurately; retain the local-estimator/provider-rejection contract.
6. Run integration checks and a mocked end-to-end recovery scenario before
   proposing the implementation for merge.

## Review decisions

Scope is agreed: include oversized-result preservation and same-conversation
recovery. The fixed proposed budgets, archive quotas, one-attempt failure policy
and text/structured-content retrieval boundary above are explicit choices for
this review, not measurements of optimal model behavior. The implementation
starts only after the user has reviewed this draft.
