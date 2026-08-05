# Py4a RustFS Workspace Service Design

**Status:** implemented and verified
**Milestone:** Py4a
**Depends on:** Py3 agent loop and bounded single-worker runtime
**Canonical decisions:** ADR-043–046, ADR-075–080, ADR-108–109, ADR-122–123

## Outcome

Py4a fixes the implementation boundary for Python-main server workspaces before
the Py4 REST routes and agent tools are written. Durable bytes live in one
RustFS bucket. `WorkspaceService` is the authorized virtual-path façade; its
internal `WorkspaceFS` layer is the only component that knows object keys, uses
`minio-py`, enforces quotas, or coordinates concurrent mutations.

This is deliberately not a general object-storage framework. RustFS is the only
supported deployment in Py4. The service uses RustFS's S3-compatible API so a
future storage replacement does not leak into tools or REST handlers, but Py4
adds no provider interface, adapter registry, capability negotiation, or
provider-specific branches.

## Scope

### Included

- One process-wide `minio.Minio` client connected to RustFS.
- Required object-storage deployment configuration and startup validation.
- Startup read/write/delete capability probing and runtime health reporting.
- Personal keys under `users/{user_id}/...` and shared keys under
  `workspaces/{workspace_id}/...`.
- Authorized virtual-path resolution before any object call, using a persisted
  shared-workspace suffix that remains stable across renames.
- Bounded object I/O and bounded in-memory file transformation.
- One mutation lock per resolved workspace, including quota serialization.
- Opaque object revisions for stale REST-write detection.
- On-demand quota usage derived from RustFS object metadata.
- Stable normalization of RustFS/MinIO failures.
- Durable, retryable prefix cleanup after account or last-member deletion.
- The storage primitives consumed by Py4 file REST routes and server file
  tools.

### Deferred

- Other S3 implementations and a pluggable storage abstraction.
- Distributed OpenOctopus workers or cross-process locking.
- Client-device files and `file_transfer` transport.
- Frontend workspace browser/editor implementation.
- Durable local workspace directories or a server-side file cache.
- Local temporary-file staging, multipart composition, archives, and tools that
  require a native filesystem path.
- The initial `message` tool and `delivery_refs` implementation; those remain
  part of Py4 but are designed after `workspace_fs` exists.

## Service Boundary

The workspace package owns the process-wide RustFS client. `WorkspaceService`
accepts an authenticated user plus virtual path and retains the PostgreSQL
authorization transaction through the file operation. `WorkspacePathResolver`
produces an opaque personal/shared `WorkspaceTarget`; the internal
`WorkspaceFS` maps that target to object keys and exposes stat, bounded read,
write, edit, bounded list, delete-file, delete-prefix, purge, and usage. There
is no abstract `ObjectStore` protocol in Py4.

REST handlers and tools supply the authenticated user and a virtual workspace
path to `WorkspaceService`. The service and internal filesystem then:

1. normalizes the path and rejects blocked forms;
2. resolves personal or strict `/name@suffix/...` shared access through
   PostgreSQL;
3. checks membership for a shared workspace;
4. maps the immutable workspace identity to an internal RustFS object key;
5. performs the operation, quota accounting, and error normalization.

No API accepts a bucket name, object prefix, RustFS credential, ETag supplied as
an object key, or local server path. Target-based `WorkspaceFS` operations are
internal, except for trusted retire/purge lifecycle code. Direct object calls
elsewhere in the server are a test failure. The configured bucket is exclusive
to OpenOctopus; direct admin or third-party writes under its managed prefixes
are unsupported because they bypass authorization, locks, and quota checks.

## RustFS Client and Backpressure

Server startup constructs one `minio.Minio` client and one underlying
`urllib3.PoolManager`. They live until application shutdown; clients are not
created per request.

OpenOctopus requires every documented server environment variable to be
present; there are no implicit code defaults. Py4 adds the required
`OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS`. It must be an integer from 1
through 256. The example environment recommends 32, but the maintainer must
explicitly retain or change that value before startup. The same value controls
both the HTTP pool and the object-operation semaphore so the two capacities
cannot disagree. It is per OpenOctopus process, not per user or session.

Py4 also makes the existing `OPENOCTOPUS_ADMIN_TOKEN` required and rejects an
empty value. This preserves the project-wide explicit-configuration rule and
prevents a deployment from starting without a way to create an administrator.
All other previously documented variables remain required. Adding a required
variable is therefore a deployment-breaking change that must update
`.env.example` and release notes in the same change.

The resulting client limits are:

- connection pool: `OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS`, blocking when
  full;
- object-operation semaphore: the same number of active RustFS calls;
- materialization semaphore: 4 active request bodies or full-file transforms;
- connect timeout: 5 seconds;
- socket read timeout: 30 seconds of inactivity;
- at most two SDK retries for retryable connection and 5xx failures, with no
  OpenOctopus retry loop around the SDK.

`minio-py` is synchronous. Every blocking object call and response-body read
runs in a dedicated executor sized to the same configured connection limit
while the asyncio event loop remains free. An operation acquires the
OpenOctopus semaphore before the HTTP pool, so pool exhaustion is bounded and
cannot create an unbounded set of waiting worker threads. Tool-level timeouts
from ADR-075 remain the outer deadline. A streaming response holds its
object-operation slot until its RustFS body is closed.

Cancelled workspace mutations retain their semaphore, materialization, and
workspace-lock scopes until the synchronous worker exits; otherwise an older
cancelled PUT could finish after a newer write. The non-mutating health probe
is the one exception: cancellation returns immediately, while its detached
worker remains tracked and retains the object-operation slot until it exits.
Shutdown waits for tracked workers before closing the dedicated executor and
clearing the HTTP pool.

Py4a byte-returning reads require an upper bound and request one look-ahead byte
to report truncation. Metadata scans consume pages of at most 1,000 objects;
usage retains only a counter, and write checks retain only target/conflict
metadata. Recursive deletion processes one page at a time. `list_dir` refuses
results above 1,000 direct entries until the later REST slice defines a cursor
contract. The future REST `GET` path will return a backpressured RustFS stream
instead of using this materializing read primitive.

The existing REST upload ceiling remains 64 MiB. A `PUT` acquires a
materialization slot before collecting the body, validates its exact byte size,
then sends it to RustFS. Agent `write_file` content is already materialized by
JSON/tool parsing and enters the same bounded write path. This simple Py4 path
caps raw simultaneous upload bodies at 256 MiB; streaming upload bridges can be
added only if the 64 MiB product limit later changes.

## Startup and Health Checks

RustFS is required infrastructure once Py4 workspace routes are enabled. The
server does not start in a degraded disk-backed mode and does not create a
missing bucket automatically.

After configuration validation and client construction, startup performs a
capability probe with a 60-second outer cancellation threshold against the
configured bucket. This is not a hard wall-clock guarantee: safe cancellation
waits for an in-flight synchronous mutation and cleanup to finish.

1. verify that the bucket exists;
2. write random probe bytes to the fixed internal
   `_openoctopus/startup-probe` key, replacing any probe left by a killed prior
3. stat and read it, verifying the exact bytes;
4. delete it;
5. attempt deletion again in `finally` if any earlier step fails after the
   object may have been created.

The internal prefix is outside all personal/shared workspace prefixes and never
counts toward quota or appears in file APIs. A connectivity, credentials,
bucket, read, write, or delete failure logs its normalized category and stops
startup. Credentials, probe bytes, and internal keys are not included in the
public error text. The normal-failure cleanup is best effort; the fixed probe
key prevents probe objects from accumulating if the server is killed before
`finally` runs. Multiple OpenOctopus server processes sharing a bucket remain
unsupported in Py4.

After the capability probe, startup drains durable workspace-deletion intents.
Failure to complete that recovery stops startup with a separate recovery error;
it is not reported as a probe failure.

`GET /health` checks PostgreSQL and RustFS concurrently, each with the existing
two-second health deadline. The RustFS side uses a non-mutating bucket-existence
request through the shared client and object-operation semaphore; it does not
write a probe on every health poll. A healthy response is:

```json
{"status":"ok","db":"connected","object_storage":"connected"}
```

If either dependency fails or times out, the route returns HTTP 503 with
`status="error"` and reports each dependency independently as `connected` or
`disconnected`. Startup proves full permissions; the recurring health check is
intentionally only a lightweight reachability/readiness signal.

## Working Data and the 8 MiB Edit Limit

RustFS always holds the canonical bytes. Py4 creates no local temporary files
and keeps no downloaded-file cache.

`edit_file` and `notebook_edit` may materialize a target only when both its
current and resulting object sizes are at most 8 MiB. `apply_patch` applies the
same per-file limit and additionally caps the aggregate source bytes and
aggregate resulting bytes at 8 MiB each across the call. Metadata is checked
before download and received lengths are validated again. The operation keeps
the bytes only for its request, transforms them in memory, uploads complete
replacement objects, and drops all references on completion or failure. A
larger target returns `workspace_file_too_large_to_edit`; it remains available
for ordinary download and, after the client milestone, transfer to a client.

`read_file` reads only enough content to satisfy its offset/limit and 128,000
character result cap. `grep` lists matching candidate keys and scans their
response streams line by line with a bounded chunk and partial-line buffer;
it does not download a workspace tree or invoke `rg` against staged files.
`find_files` and `list_dir` use object metadata only. Raw REST downloads remain
streaming regardless of the edit limit.

Because Py4 does not stage local files, there is no temporary directory,
request cleanup sweeper, or crash-recovery disk scan. Writes use known lengths
within the 64 MiB REST cap and do not introduce an OpenOctopus-managed multipart
workflow. If a later milestone adds staging or explicit multipart uploads, that
milestone must add `finally` cleanup plus startup recovery before enabling the
new path.

## Mutation, Revision, and Folder Races

One async mutation lock is keyed by immutable personal/shared workspace ID. It
is held across the complete read-check-transform-write/delete sequence and the
quota decision. Locks for different workspaces do not contend. Idle keyed-lock
entries are removed when they have neither a holder nor waiters.

This workspace-level lock is intentionally broader than a path lock. It gives
simple, deterministic behavior for:

- write/write, edit/write, edit/edit, and delete/write on one file;
- folder-delete versus any write below that prefix;
- `apply_patch` touching several files;
- quota checks racing with writes or deletes.

Object reads that do not mutate are not locked. RustFS returns a complete old or
new object rather than partial write contents. A read may legitimately finish
with the previous value while a replacement is committing.

Membership removal also locks the shared-workspace PostgreSQL row before
deciding whether the last member has left. Account deletion records cleanup for
the personal prefix and any last-member shared prefixes in the same transaction
as metadata deletion. Shared workspaces with remaining members survive creator
deletion with `created_by=NULL`. A retired-target tombstone rejects
already-authorized writes waiting behind lifecycle deletion; it is removed when
rollback is proven or cleanup completes.

Logical deletion and RustFS cleanup are joined by the
`workspace_deletions(kind, target_id)` outbox. The target is fenced first; the
metadata deletion and outbox row commit together; prefix purge and outbox
removal happen afterward. A purge failure cannot reactivate a committed target
or turn logical deletion into an HTTP failure. Cleanup retries every 60 seconds
in the running process and again at startup. Commit ambiguity keeps the target
fenced unless a fresh transaction proves that metadata and cleanup intent both
rolled back.

A lock cannot cover the human think-time between a REST `GET` and a later save.
Therefore file reads expose the RustFS ETag as an opaque `ETag` response header.
REST replacement, edit, and delete accept `If-Match`; when it is present,
`WorkspaceFS` compares it with current metadata while holding the workspace
lock and returns `workspace_file_changed` on mismatch. The frontend milestone
must send this header for editor saves. Upload clients may omit it only when
they intentionally request unconditional full replacement.

Agent operations do not add revision fields to the established tool schemas.
`edit_file` and `apply_patch` transform the latest content under the lock, and
their old-text matching rejects a conflicting same-region change. `write_file`
is explicitly an unconditional whole-file replacement, so two accepted
`write_file` calls are serialized and the later call wins. This is intentional
tool behavior, not an atomicity failure; agents should use `apply_patch` for
changes based on content they previously read.

An `apply_patch` call resolves all paths before acquiring locks. If it spans
multiple workspaces, locks are acquired in sorted workspace-ID order to avoid
deadlock. All inputs, matches, size limits, quotas, and SKILL.md results are
validated before its first upload. RustFS has no multi-object transaction, so
an infrastructure failure during the upload phase can leave an explicitly
reported partial patch; Py4 does not build a false transactional layer with
temporary object keys and fallible rollback.

## Quota Race Resolution

The simplest correct Py4 implementation remains on-demand accounting. While
holding every affected workspace mutation lock, `WorkspaceFS` pages through
that workspace prefix and sums object sizes without collecting the complete
listing. It retains replacement-target metadata so
the projected usage is:

`current usage - replaced/deleted bytes + committed new bytes`

The service applies the soft-lock rule, the 80-percent single-operation cap,
and the workspace quota before writing. A delete is always allowed and the
next on-demand calculation observes the released space. Concurrent writes to
one workspace cannot both pass against the same old total because the lock
serializes their calculations and commits.

Personal-path resolution also takes a transaction-scoped shared advisory lock
while reading the global personal quota. `PATCH /api/admin/config` takes the
matching exclusive lock when changing `quota_bytes`, so a write cannot continue
under an older larger quota after the update commits. This also coordinates the
missing-row 500 MiB default, which cannot be protected by a row lock.

Py4 adds no PostgreSQL byte counter or persistent usage index. If a RustFS
write/delete fails partway through a prefix operation, the next list is the
authority, so there is no counter to repair. On-demand listing may become slow
for workspaces containing very many objects; observed performance, not a
speculative cache, will justify a later index.

## Error Normalization

RustFS/MinIO exception messages are logged with operation and request context
but credentials and internal object keys are never returned. API and tool
edges receive only stable codes:

| RustFS/SDK condition | OpenOctopus result |
|---|---|
| Missing object/prefix or inaccessible workspace/member | `workspace_not_found` |
| Invalid/blocked virtual path | existing path-policy code |
| Quota lock or size cap | existing quota code |
| Target exceeds 8 MiB edit limit | `workspace_file_too_large_to_edit` |
| Directory exceeds the bounded listing result | `workspace_directory_too_large` |
| Stale `If-Match` revision | `workspace_file_changed` |
| Missing bucket, bad RustFS credentials, or access denied by RustFS | `workspace_storage_unavailable` |
| Connect/read timeout, connection failure, or exhausted operation deadline | `workspace_storage_unavailable` |
| Retry-exhausted RustFS 5xx | `workspace_storage_unavailable` |
| Malformed/unexpected S3 response | `workspace_storage_error` |

`workspace_file_changed` maps to HTTP 409. The edit-size error maps to HTTP 413.
Storage availability/error codes map to HTTP 503 and become ordinary error tool
results rather than crashing the agent loop. Bad credentials and a missing
bucket also make the workspace component unhealthy; startup does not silently
fall back to disk.

## Verification and Exit Criteria

- Unit tests prove path authorization occurs before object-key construction and
  no public response leaks an internal key.
- A fake client proves no more than the configured object-connection limit and
  four materializations execute concurrently while the event loop remains
  responsive.
- Race tests cover write/write, edit/write, folder-delete/write, stale
  `If-Match`, multi-agent personal-workspace edits, and user/agent shared-file
  edits.
- Quota tests start concurrent writes at the same barrier and prove only the
  writes fitting the serialized projected total commit.
- Failure tests cover every normalization row and release locks, semaphores,
  response bodies, and pool connections.
- Configuration tests prove every documented variable is required, admin token
  cannot be empty, and object-storage connection limits outside 1–256 fail
  startup.
- Startup tests prove the RustFS probe checks bucket existence, write, stat,
  read, byte equality, and delete; each failure prevents startup and invokes
  best-effort cleanup.
- Health tests cover independent database and RustFS success, failure, and
  timeout states, including the combined HTTP 503 response.
- A real disposable RustFS test exercises write, ranged/bounded read, list,
  overwrite, delete, shared-prefix isolation, and ETag retrieval.
- A capacity test starts 500 independent personal-workspace operations, proves
  isolation and bounded object concurrency, and completes without blocking chat
  session progress.
- The verified implementation passes the full server suite, including the two
  live RustFS tests, plus Ruff, formatting, strict MyPy, and diff checks. No
  frontend implementation is required for Py4a acceptance.
