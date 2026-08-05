# Py4 Workspace Files and Initial Message Tool Design

**Status:** accepted
**Milestone:** Py4
**Depends on:** implemented Py4a RustFS workspace foundation
**Canonical decisions:** ADR-021–024, ADR-038–046, ADR-071, ADR-075–088,
ADR-095, ADR-108–109, ADR-121–124, ADR-127

## Outcome

Py4 turns the Py4a storage foundation into a usable server workspace. Authenticated
users receive consistent workspace-management and file REST APIs, and the agent
receives the server implementations of the shared file tools. The current web
session also gains the first `message` tool implementation for delivering files
that already exist in the server workspace.

Every byte operation continues through `WorkspaceService`; REST handlers and tools
never call RustFS directly. Py4 remains single-worker, server-workspace-only, and
memory-bounded. It does not implement a frontend, paired-device routing, file
transfer, or third-party channels.

## Scope

### Included

- Shared-workspace create, list, get, rename, quota, and membership REST APIs.
- Server workspace file download, upload, edit, patch, delete, directory listing,
  find, and grep REST APIs.
- Consistent collection envelopes and pagination fields across the Py4 REST API.
- Opaque ETags and optional conditional requests for single-file mutations.
- Server implementations and schemas for `read_file`, `write_file`, `edit_file`,
  `apply_patch`, `delete_file`, `delete_folder`, `list_dir`, `find_files`, `grep`,
  and `notebook_edit`.
- Workspace-backed `SOUL.md`, `MEMORY.md`, skills, and shared-workspace prompt
  sections.
- Initial current-web/server-workspace `message` tool.
- Server-generated, provider-hidden `delivery_refs` attached to the existing
  assistant message containing the matching `message` tool call.
- TDD coverage for authorization, limits, concurrency, provider replay, and 500
  independent sessions.

### Deferred

- Frontend code.
- Paired-device file tools and Workspace Files relay; these begin with Py5.
- `file_transfer`; it depends on the client transport in Py5.
- Third-party `message` delivery, explicit `channel`/`chat_id`, and buttons; these
  begin with the channel milestone.
- A REST endpoint for `notebook_edit`. The operation is agent-only in Py4.
- Other S3 providers, distributed workers, local staging/cache, archives, and
  native-path tools.
- Server execution of workspace content. Workspace files remain inert data.

## REST Conventions

Py4 standardizes its workspace REST surface for a future frontend without
rewriting unrelated auth, admin, or chat APIs.

- JWT auth and `{code, message}` errors are identical on every route.
- Every file route requires `openoctopus_device`. Py4 accepts only `server`; the
  parameter remains explicit so Py5 can add paired-device names without a second
  file API.
- Single-resource JSON reads return the resource directly. Collection reads
  return `{items, limit, offset, next_offset, truncated}`. `next_offset` is
  `null` when a one-item look-ahead proves the page is complete. `truncated=true`
  means a hard internal scan ceiling prevented a complete result and there is no
  next page. Total counts are omitted so a UI page does not force a full RustFS
  scan.
- REST collection queries consistently use `limit` and `offset`, even where the
  nanobot-shaped agent tool uses `max_entries` or `head_limit`. Tool schemas and
  REST DTOs share behavior, not necessarily parameter names.
- `limit` defaults to 200 and is capped at 1,000. `offset` defaults to zero and is
  capped at 10,000 for object-tree searches. Workspace/member DB collections use
  the same response envelope and may use a smaller route default.
- JSON bodies set `additionalProperties: false`. Empty PATCH bodies are rejected.
- File upload/download are the only raw-byte operations. All other requests and
  responses are JSON.
- Slow request bodies and streaming responses do not pin a PostgreSQL connection.
  Authorization is rechecked at the mutation boundary; an already-open download
  may finish after membership revocation, while every later request is denied.
- Successful creates return 201, JSON mutations return 200, and successful
  deletes return 204.

The collection envelope is deliberately uniform:

```json
{
  "items": [],
  "limit": 200,
  "offset": 0,
  "next_offset": null,
  "truncated": false
}
```

## Workspace Management API

The management routes are:

| Method and path | Result |
|---|---|
| `GET /api/workspaces` | Page containing the personal workspace and accessible shared workspaces |
| `POST /api/workspaces` | Create a shared workspace and add the caller |
| `GET /api/workspaces/{workspace_ref}` | One authorized shared workspace |
| `PATCH /api/workspaces/{workspace_ref}` | Rename and/or change quota |
| `GET /api/workspaces/{workspace_ref}/members` | Page of members |
| `POST /api/workspaces/{workspace_ref}/members` | Add one user by UUID or email |
| `DELETE /api/workspaces/{workspace_ref}/members/{user_id}` | Remove one member; last member deletes the workspace |

The personal workspace is virtual: its ID is the authenticated user's ID and it
has no `workspaces` row or membership row. Shared workspace responses include the
persisted suffix and ready-to-use `name@suffix` ref. Every response computes live
`bytes_used` through `WorkspaceService` and sets `locked = bytes_used > quota_bytes`.
List/get first capture an authorized immutable workspace snapshot, end the DB
transaction, and then calculate usage with at most four concurrent prefix scans.
This keeps a large quota response from pinning a database connection or consuming
the entire RustFS pool; membership mutations affect subsequent requests.

Shared-workspace names use ADR-109 normalization. Creation generates the UUID in
application code, begins with its first eight hex characters, and extends the
persisted suffix until it does not collide in the caller's accessible set. Member
addition performs the same prospective-user collision check while holding the
workspace row and rejects a collision with `workspace_ref_conflict`; it never
changes the stable suffix. Renames validate only the new name; the suffix and RustFS prefix do
not change. Quota creation/updates take the existing system-config advisory lock
and must not exceed `shared_workspace_quota_bytes`.

Because accessible-set suffix uniqueness cannot be expressed as one SQL unique
constraint, creation and member addition take transaction-scoped advisory locks for
every affected user ID in sorted order before checking and writing membership. This
serializes two concurrent workspaces being added to the same user without creating a
global workspace lock.

All members retain equal permissions. Mutations lock the workspace row. Existing
Py4a last-member deletion and cleanup-outbox behavior remains the only deletion
path; Py4 does not add a separate workspace DELETE route.

## Workspace File API

The Py4 server routes are:

| Method and path | Result |
|---|---|
| `GET /api/workspace/files/{path}` | Backpressured raw download |
| `PUT /api/workspace/files/{path}` | Raw create or full replacement |
| `PATCH /api/workspace/files/{path}` | Text edit |
| `DELETE /api/workspace/files/{path}` | Delete one file |
| `DELETE /api/workspace/folders/{path}` | Recursively delete one prefix |
| `POST /api/workspace/patch` | Structured multi-file patch or dry run |
| `GET /api/workspace/list/{path}` | Directory-entry page |
| `GET /api/workspace/find-files` | Matching-path page |
| `GET /api/workspace/grep` | Content-search result page |

Paths use one wildcard FastAPI parameter so raw slashes and URL-encoded slashes
resolve identically after decoding once. The route passes the decoded virtual path
to `WorkspaceService`; it never constructs an object key.

### Download

`GET` opens a bounded `WorkspaceService` stream. It returns
`application/octet-stream`, `Content-Length`, a quoted opaque `ETag`,
`Content-Disposition: attachment`, and `X-Content-Type-Options: nosniff`. The
download is not subject to the 8 MiB edit/materialization limit.

Authorization first produces a private, short-lived download ticket containing the
resolved immutable target and relative path. The route ends that DB transaction
before waiting for RustFS capacity or yielding response bytes. Opening the object
may therefore return not-found if lifecycle deletion wins after authorization; an
object already opened before membership removal is allowed to finish. The ticket is
internal to `WorkspaceService` and cannot be constructed from an API request.

The internal stream reads at most 64 KiB per executor handoff and holds one object
operation/pool slot until the response body is closed. The response generator closes
and releases the RustFS body in `finally` on completion, disconnect, cancellation,
or error. It does not stage bytes on disk, collect the complete object in RAM, or
hold a database connection for the client-download lifetime.

### Upload and single-file concurrency

`PUT` accepts `application/octet-stream`. A short authorization/quota preflight
computes the collection ceiling and ends its DB transaction. `WorkspaceService`
then acquires one of the four materialization slots before reading the request body,
rejects an oversized `Content-Length` immediately, and still counts streamed chunks
so a missing or false header cannot bypass the limit. It collects no more than
`min(64 MiB, floor(0.8 * workspace quota)) + 1` bytes before rejecting. The normal
write opens a fresh transaction, re-resolves authorization/quota, and performs the
authoritative locked `WorkspaceFS.write`. Revocation or a quota reduction during
upload therefore rejects the write without pinning a DB connection while the client
sends its body.

Successful `PUT` and `PATCH` responses use one mutation shape:

```json
{
  "path": "notes/today.md",
  "size": 123,
  "etag": "opaque-revision",
  "created": false,
  "replacements": 1
}
```

`replacements` is omitted for full writes. The response also returns the quoted
`ETag` header so the frontend does not need another read before its next save.

Single-file `PUT`, `PATCH`, and `DELETE` accept an optional `If-Match` containing
exactly one strong ETag. A mismatch returns 409 `workspace_file_changed`. Missing
`If-Match` explicitly means unconditional last-writer-wins behavior. `PUT` also
accepts `If-None-Match: *` for race-safe creation; it cannot be combined with
`If-Match`. Weak validators, lists of validators, and other `If-None-Match` values
return 400. Agent tool schemas do not gain ETag fields.

Folder deletion has no single object revision. Structured patching operates on the
latest locked contents and uses exact old-text matching; a frontend that needs a
strict single-file revision guard uses the file `PATCH` route.

### Edit, patch, and validation

`PATCH /files/{path}` uses the same pure three-level matcher as `edit_file` and
returns the number of replacements. Empty `old_text` creates only when the target
does not exist. `apply_patch` accepts at most 20 edits, resolves every path first,
locks affected workspaces in sorted immutable-ID order, validates all matches,
sizes, quotas, and `SKILL.md` results before its first write, then uploads in request
order. RustFS cannot provide a multi-object transaction; a storage failure after a
write returns a stable error whose sanitized message states how many edits committed
rather than claiming rollback succeeded. `dry_run=true` never writes.

Every write path validates `skills/*/SKILL.md` and invalidates the per-user skills
cache after a successful mutation under `skills/`. Deletes always remain available
while over quota.

### Listing, find, and grep

All scans consume RustFS metadata pages of at most 1,000 objects and run under a
four-operation heavy-work semaphore. They retain only the requested page plus one
look-ahead, except `sort=modified`, which uses a bounded top-k heap. A scan examines
at most 10,000 candidate objects per call. Reaching that boundary returns
`truncated=true` and `next_offset=null`; a later milestone may replace this safety
ceiling with an opaque object-store cursor if observed workloads require it.

Non-recursive `list` uses the S3 delimiter/common-prefix listing mode so one large
child directory cannot consume the 10,000-object scan ceiling before its sibling is
seen. Recursive `list` synthesizes directories from ordinary object prefixes.
`find-files` supports fragment, glob, type, directory inclusion, and path/modified
sorting. Both skip the fixed noise-directory list. `grep` also
honors discovered `.gitignore` rules, skips binary objects and objects over 2 MiB,
and streams candidates line-by-line instead of materializing a tree. Regex matching
uses a timeout-capable regex engine with bounded pattern and line sizes; invalid or
timed-out expressions return `tool_invalid_regex` instead of occupying the event
loop indefinitely.

REST collection results stay structured. Agent tools render the same domain results
into their documented LLM-friendly strings and apply the existing 16,000-character
result cap.

## Agent File Tools

Py4 registers the ten server file tools listed in Scope. Their source schemas remain
nanobot-shaped and device-free. Registry merge injects required
`openoctopus_device` with `enum: ["server"]`; Py5 later extends that enum.

The registry distinguishes three routing modes with one small tool attribute:

- shared/routing-only tools: inject and remove `openoctopus_device`, validate the
  selected site, and place it in `ToolContext`;
- intrinsic-device server tools such as `message`: keep their marked device field
  in the tool arguments and extend its enum;
- pure server tools: no device field.

This replaces the Py3 assumption that every registered tool is routing-only without
building a general plugin framework.

Each server file tool opens an `AsyncSession`, calls `WorkspaceService`, and returns
ordinary normalized tool results. Tool exceptions remain `is_error=true` results;
they never break the ReAct loop.

Specific behavior remains as documented in `TOOLS.md`:

- `read_file` line-paginates text with a 128,000-character cap, stream-scans large
  text, recognizes images, and materializes supported PDF/Office documents only
  within the 8 MiB bound. Its unchanged-read cache is a bounded process LRU keyed by
  session, virtual path, ETag, offset, and limit; `force=true` bypasses it.
- `write_file` is an intentional unconditional replacement.
- `edit_file` and `apply_patch` transform the latest locked content and surface
  ambiguous/no-match conflicts.
- `delete_file` rejects prefixes; `delete_folder` rejects files.
- `list_dir`, `find_files`, and `grep` use the bounded scan implementation shared
  with REST but keep their documented tool argument names and output formatting.
- `notebook_edit` validates `.ipynb` JSON and cell shape, changes one cell, and
  writes through `WorkspaceService`. It has no REST route.

Document parsing, YAML frontmatter, gitignore matching, and timeout-bounded regular
expressions use focused libraries that accept in-memory bytes. They run behind the
heavy-work semaphore and never receive an internal object key or native server path.

Py4 adds only the direct dependencies required by the documented tool contract:
`PyYAML` for `SKILL.md`, `pathspec` for gitignore rules, `regex` for match timeouts,
and `pypdf`, `python-docx`, `openpyxl`, and `python-pptx` for supported documents.
Document parsing runs in at most two disposable subprocesses so a parser timeout or
CPU-heavy document can be terminated without wedging the async worker or RustFS
executor. Inputs remain capped at 8 MiB and extracted output remains capped by the
`read_file` result limit.

## Workspace-Backed Prompt Inputs

At each agent-loop preflight, the server loads a bounded snapshot before building
the pure prompt:

- missing personal `SOUL.md` means the built-in default identity;
- missing personal `MEMORY.md` means an empty memory section;
- `skills/*/SKILL.md` is loaded through the lazy per-user skills cache;
- accessible shared workspaces are listed using their exact `/name@suffix/` path
  form, never UUID-only labels;
- the operating notes explain that relative server paths mean the personal
  workspace and that file delivery uses `message`, not `read_file`.

Always-on skills inline their complete bounded body. Conditional skills include only
name, description, and the `skills/{name}/SKILL.md` read path. Writes/deletes under
`skills/` invalidate the cache after commit; an already-prepared provider iteration
may keep its old snapshot, matching the existing one-turn stale-read tolerance.

Prompt loading uses explicit caps: 32,000 characters for `SOUL.md`, 128,000 for
`MEMORY.md`, 64,000 per `SKILL.md`, 200 discovered skills, and 128,000 aggregate
always-on skill characters. Oversized optional files are included up to the cap with
a marker telling the agent to use `read_file`; always-on bodies beyond the aggregate
cap fall back to the same name/description/path entry as conditional skills. The
skills cache is a weighted LRU capped at 64 MiB process-wide and 1 MiB per user, so
thousands of historical users cannot grow process memory monotonically.

Prompt reads use `WorkspaceService`, tolerate only `workspace_not_found` as an empty
optional file, and fail preflight on storage or authorization errors. The prompt no
longer says workspace memory, skills, or file access are unavailable in Py3.

## Initial `message` Tool

Py4 exposes only this provider-visible input:

```json
{
  "content": "Here is the report.",
  "openoctopus_device": "server",
  "media": ["reports/report.pdf"]
}
```

`content` is required. `media` is optional and capped at ten paths. The Py4 schema
does not expose `channel`, `chat_id`, buttons, or paired-device names. The tool is
valid only for the current web session.

For every media path, the tool calls `WorkspaceService.stat`, verifies that it is a
file accessible to the session owner, and produces filename, size, and a conservative
MIME hint without reading the file bytes. Validation is all-or-nothing: any invalid
path returns an error and produces no delivery refs.

`delivery_refs` are never tool inputs and are never projected to the LLM provider.
On success the tool returns internal side-effect metadata with its normal tool
result. The runner atomically:

1. locks the already-persisted assistant row containing this `message` tool use;
2. appends server-generated refs to that row's `delivery_refs`;
3. inserts the matching provider-visible tool-result row in the same transaction;
4. republishes the updated assistant snapshot, then the tool-result snapshot, to the
   best-effort POST stream.

Each ref includes the generating `tool_use_id`, type `workspace_file`,
`openoctopus_device="server"`, virtual path, filename, MIME/size hints, and
`online_only=false`. Server refs also include the immutable personal/shared
workspace ID and workspace-relative path. The `tool_use_id` lets a future frontend
associate attachments with the correct content when one assistant response contains
more than one `message` call. The immutable workspace fields let it recover a shared
file link after a workspace rename by fetching the current workspace ref; deletion or
file movement still invalidates the ref normally. Live `message_persisted` events are
idempotent snapshots keyed by message ID, so the updated assistant row is an upsert
rather than a second message.

Provider replay reads only `messages.content`; it ignores `delivery_refs`. The
provider therefore sees exactly the original assistant tool-use row followed by its
matching tool-result batch. Py4 creates no extra assistant row, delivery table, or
provider content block.

A future frontend renders a successful `message` tool call from its existing
`tool_use.input.content`, selects refs with the same `tool_use_id`, and downloads them
through the authenticated Workspace Files GET route. A failed tool result means the
call is not rendered as delivered.

## Implementation Slices and TDD Order

Py4 stays on `py4-impl` but lands in three reviewable commits.

### Slice 1: workspace REST and core file transport

1. Write DTO/route contract tests for collection envelopes, auth, explicit server
   target, workspace CRUD/membership, raw streaming, upload limits, and ETags.
2. Implement workspace services and API routes.
3. Add streaming download and bounded REST body collection to the Py4a service.
4. Verify unit/API tests plus live RustFS upload/download and disconnect cleanup.

### Slice 2: transformations, search, prompt, and file tools

1. Write pure matcher, patch, notebook, skill, find, grep, and schema snapshots.
2. Extend `WorkspaceService` with bounded scan and multi-file operations.
3. Implement/register the ten server file tools.
4. Activate workspace-backed prompt inputs and skills-cache invalidation.
5. Verify REST/tool parity and agent-loop end-to-end tests.

### Slice 3: current-web `message`

1. Write failing tests proving refs are server-generated and absent from provider
   requests.
2. Implement the restricted schema, validation, and internal tool-result side effect.
3. Atomically attach refs to the existing assistant row and persist the matching tool
   result.
4. Verify multiple message calls, failure atomicity, compaction/provider replay, live
   upsert events, and canonical GET recovery.

Each slice gets its own commit only after its focused tests, the full server suite,
Ruff lint/format, and strict MyPy pass. No implementation begins until this proposed
spec is accepted.

## Verification and Exit Criteria

- Every Py4 route is authenticated and every file route requires
  `openoctopus_device=server`.
- Collection APIs share the documented envelope, `limit`, `offset`,
  `next_offset`, and `truncated` behavior; contract snapshots prevent drift.
- Workspace/member races preserve suffix uniqueness, authorization, quota ceilings,
  and last-member cleanup, including concurrent member additions to one user.
- Download cancellation releases response bodies, connections, semaphores, and
  executor work without blocking the event loop; 500 waiting/slow downloads do not
  pin PostgreSQL connections.
- Upload tests prove early `Content-Length` rejection, streamed overrun rejection,
  four-body materialization bound, quota authority, and no local staging.
- Conditional mutation tests cover matching, stale, malformed, absent, and
  `If-None-Match: *` behavior under concurrent user/agent writes.
- Tool tests cover schema snapshots, routing, timeouts, error normalization, output
  caps, images/documents, matcher ambiguity, patch partial failure, notebook errors,
  ignored directories, regex timeout, and skills invalidation.
- Prompt tests prove bounded SOUL/MEMORY/skill loading and exact `name@suffix`
  workspace paths.
- Message tests prove delivery refs attach to the existing assistant row, correlate
  by `tool_use_id`, never appear in provider payloads, and never split tool-use/result
  adjacency.
- A capacity regression runs 500 independent sessions while workspace operations
  saturate their configured bounds and proves chat progress, isolation, and event-loop
  responsiveness.
- Real RustFS tests cover streaming GET, PUT, ETag conflicts, recursive scans,
  shared-workspace isolation, deletes, and message-file stat.
- The full server suite, Ruff lint and format, strict MyPy, and diff checks pass.
