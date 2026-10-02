# Built-in skills and Jev-gated Dream

Status: implemented; live Jev acceptance remains pending credentials. Mock tests
verify the API contract and failure handling, not model judgment, latency or cost.

## Shared built-in skills

Six ordinary `SKILL.md` files ship inside the server package: creating skills,
connecting MCP, connecting clients, connecting external channels, creating Cron
jobs, and editing Heartbeat. Startup validates and indexes the one shared release
library. It does not generate files or copy them into individual workspaces.

Agents discover conditional skill metadata in their normal system prompt and use
`read_file` at `/builtin/skills/<name>/SKILL.md`. Users open the Built-in skills
location in Workspace. The backend reserves this namespace and rejects writes,
edits, patches, deletion, uploads and transfers involving it, including aliases
and traversal attempts. Read and download work normally. Personal skills have a
separate origin and cannot shadow built-ins. Updates arrive through a new server
release; administrators also cannot edit this library through workspace APIs.

## Jev service

Administrators save an unversioned Jev endpoint and API key in Settings. The key
is redacted on reads. Saving configuration resets its observed availability;
reading or saving settings does not make an inference request. The explicit
Check connection action sends one small fixed decision request. Normal Dream
and Heartbeat requests also update observed availability. A late response from
an older configuration cannot overwrite the current configuration's status.

The official contract is `POST <endpoint>/v1/systemone`, Bearer authentication,
model `jev-latest`, and `state` plus named `questions` of type `choice`. Each
question supplies instructions and the `run`/`skip` criteria. Responses contain
`model`, `answers` and `usage`; each answer includes the selected choice,
probabilities and confidence. Selection follows the validated choice. Prompt
criteria request `skip` for uncertain evidence; no additional confidence
threshold is invented. The service accepts a resolved model name different from
the request alias.

Requests are capped at 512,000 UTF-8 bytes, responses at 128,000 bytes, and
concurrency at eight with a 15-second deadline including admission. Missing
answers, invalid choices or malformed responses are failures. The normal LLM is
not a substitute for Jev. The admin page displays **Dream is not available**
with a reason when configuration is missing or an observed request fails; a
successful check or request clears it. Unchecked configuration is shown as such,
not as a confirmed healthy connection. Heartbeat has the same dependency.

Official references: [API](https://docs.typesafe.ai/api) and
[TypeSafe introduction](https://typesafe.ai/blog/introducing-system-one-models-and-jev).
No live/paid Jev request was made during development.

## Controlled Dream workflow

At midnight in the user's IANA timezone, the server makes the completed local
day eligible. A one-minute lifecycle ticker also catches older unprocessed input
after downtime and continues unfinished batches. It processes up to four users
at once and one batch per user per pass. Cron timing is unchanged. DST uses local
calendar midnights rather than adding a fixed 24 hours.

Input is saved human and assistant **text** across every owned Session: web,
Cron, Heartbeat, Discord and DingTalk. Each excerpt retains its message ID,
Session, channel, timestamp and speaker classification. Assistant statements,
automation instructions and other participants' messages are not automatically
user facts. Raw tool payloads, thinking, binary attachments and compaction
summaries are excluded; original messages remain eligible even after compaction.
Running or queued Sessions wait until settled. Current-day messages wait until
the next midnight.

When Jev is unconfigured, new decisions wait without creating run history,
reading memory or advancing source progress. Prepared writes and restores still
recover; configured endpoints that become unreachable retain the hourly retry.

1. Read a bounded batch and the current `MEMORY.md` plus its ETag.
2. Ask Jev whether there is new durable evidence or a supported correction that
   is not already in memory. Empty input makes no Jev or LLM request.
3. On `run`, ask the configured normal LLM for one constrained proposal: exact
   replacements and appended text, each citing source message IDs. It has no
   executable tools and has a 120-second deadline including provider admission.
   The application validates references, unique replacement
   matches, response structure and the resulting size.
4. Persist the proposal before conditionally writing **only `MEMORY.md`**. An ETag
   change prevents a manual edit from being overwritten.
5. Atomically record completion and per-message text offsets after a successful
   update, unchanged proposal or valid Jev skip. Failed decisions or explicit
   write conflicts keep input unprocessed and retry after an hour. A prepared
   write with an uncertain storage/database outcome stays pending for the next
   recovery pass. A crash after the file write can
   finish its saved proposal without another model request or duplicate write.

A batch contains at most 16 message excerpts and 64,000 characters, with at most
16,000 characters from a message. Large messages continue from their durable
offset in a later batch; there is no silent truncation. Memory is capped at
64,000 UTF-8 bytes. Oversized memory fails explicitly and retains input. A source
message deleted before completion does not acquire a new progress row.

Dream proposes durable preferences, project decisions, commitments and supported
corrections. Its instructions exclude secrets, duplicates, speculation and
transient activity. This is a constrained model judgment, not a guarantee of
semantic accuracy. Dream does not edit `SOUL.md`, create skills or create new
chat messages. Its own audit records are never input to Dream.

## History and undo

Automations shows Dream availability and owned run history, with before/after
memory for actual updates. Undo restores the prior contents only while the
current memory still matches that update's ETag. Later manual or automatic edits
cause a conflict rather than being overwritten. Undo itself is recoverable after
interruption and does not rewind input progress, so the same conversation is not
immediately relearned. Completed records retain offsets/provenance and actual
memory changes, rather than a second copy of conversation text. Deleting a user
cascades their Dream records; deleting a source message cascades its progress.

## Heartbeat and Cron

Heartbeat Phase 1 uses the same mandatory Jev service. Deterministic parsing
assigns a decision to each active task, preserves task order/text, and passes
only selected tasks to the existing Phase 2 agent. Jev sees task instructions and
time, not remote machine state. Phase 2 performs any actual status checks and
uses the normal tools. A failed Jev request skips that pulse; it does not create
queued catch-up work. Existing busy checks and phase-two publication remain.

Cron remains deterministic: when an eligible schedule fires, the normal agent
runs. Jev does not veto scheduled jobs. Dream subsequently learns from both
Cron and Heartbeat conversation history.

## Validation boundary

Development uses the documented Jev request/response contract through HTTP
mocks, including unavailable and malformed responses. Server workflow tests
exercise persistence, source isolation, midnight/DST eligibility, chunk
continuation, conflicts, recovery and undo. Browser tests cover the settings,
read-only library and Dream history controls. Live Jev authentication, model
quality, cost and latency still require the user's endpoint/key. Live DingTalk
acceptance remains deferred because no account is available.
