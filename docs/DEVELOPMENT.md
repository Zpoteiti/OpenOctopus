# Implementation ownership

This guide maps the main execution paths to their owning modules. Public HTTP,
tool, and device protocol contracts remain in [API.yaml](API.yaml),
[TOOLS.md](TOOLS.md), and [PROTOCOL.md](PROTOCOL.md).

## Server chat

[`chat/runner.py`](../server/src/openctopus_server/chat/runner.py) owns session
scheduling, durable turn transitions, Provider calls, and tool execution.
[`chat/session_streams.py`](../server/src/openctopus_server/chat/session_streams.py)
owns live preview subscribers, pending-message selection, and stream handoff.

The runtime holds the **same session lock** around database-dependent registration
and subscriber changes. `SessionStreams` methods are synchronous and run under
that lock. A turn claims only subscribers belonging to its captured message IDs;
messages arriving after that boundary remain queued. Persisted history remains
authoritative when a preview closes, is replaced, or disconnects.

## Built-in skills and memory automation

[`workspace/builtin_skills.py`](../server/src/openctopus_server/workspace/builtin_skills.py)
validates and indexes the packaged library; workspace authorization enforces its
reserved read-only namespace. Packaged files are under `assets/builtin_skills/`.

[`provider/jev.py`](../server/src/openctopus_server/provider/jev.py) owns bounded
HTTP decisions and configuration-revision-fenced availability. Heartbeat Phase 1
selects existing parsed tasks through this service. Phase 2 uses the normal agent.

[`automations/dream.py`](../server/src/openctopus_server/automations/dream.py)
owns midnight eligibility, bounded text batches, durable progress, prepared
memory changes and undo. `ChatRuntime.propose_memory_update` shares the normal
provider and limiter for a single constrained proposal; it never dispatches
executable tools. New database tables bootstrap with the development schema.
Test with mock Jev transport until credentials are available; do not substitute a
runtime fake or imply live model acceptance. See the
[workflow contract](specs/2026-09-29-builtin-skills-and-dream-direction.md).

## Server device transfers

| Module | Responsibility |
|---|---|
| [`devices/transfer.py`](../server/src/openctopus_server/devices/transfer.py) | Transfer entry points, direct Server/Client transfers, generation fencing, and coordinated shutdown |
| [`devices/transfer_bridge.py`](../server/src/openctopus_server/devices/transfer_bridge.py) | Client-to-Client relay transitions, endpoint acknowledgements, and relay cleanup |
| [`devices/transfer_slots.py`](../server/src/openctopus_server/devices/transfer_slots.py) | Shared slot namespace, lock, tombstone capacity, expiration, and eviction |
| [`devices/transfer_admission.py`](../server/src/openctopus_server/devices/transfer_admission.py) | Fair per-user admission and operation-lease ownership |
| [`devices/transfer_types.py`](../server/src/openctopus_server/devices/transfer_types.py) | Transfer states, endpoint records, result types, and terminal-result helpers |
| [`devices/transfer_io.py`](../server/src/openctopus_server/devices/transfer_io.py) | Fenced control-frame writes and transport-error normalization |

Direct transfers and relays share one `TransferSlots` instance and one admission
controller. A relay occupies one logical transfer slot and reserves two endpoint
tombstones. Moving relay logic must preserve the shared collision checks,
provisional tombstone pinning, and the second tombstone lookup after endpoint
lookup. The second lookup handles cleanup that completes between those awaits.

Cancellation after an irreversible commit retains the committed result. Cleanup
continues to own its tasks and admission until they finish; splitting modules
does not introduce a second admission acquisition for directory children.

## Client tools

[`tools/dispatcher.py`](../client/src/openoctopus_client/tools/dispatcher.py)
validates arguments, selects the implementation, applies time limits, and maps
errors to tool results. Implementations live in:

- [`tools/file_tools.py`](../client/src/openoctopus_client/tools/file_tools.py):
  local file tools and workspace REST operations using the same path policy and
  path locks.
- [`tools/web_fetch.py`](../client/src/openoctopus_client/tools/web_fetch.py):
  bounded downloads, DNS pinning, redirect validation, and HTML conversion.
- [`tools/local_transfer.py`](../client/src/openoctopus_client/tools/local_transfer.py):
  local copy/move, platform-specific exclusive commits, and transfer cleanup.
- [`tools/blocking.py`](../client/src/openoctopus_client/tools/blocking.py):
  worker-thread tracking and cancellation/drain ownership shared by file and
  local-transfer operations.

The runtime supplies the admission controller, drain registry, and path locks.
Cancelling a coroutine does not stop its worker thread. Mutation locks remain
held until the worker finishes; abandoned transfer work hands its resources to
the runtime drain registry. The runtime still waits for work tracked by each
dispatcher.

## MCP catalogs

Server and Device catalogs inside the Server use the strict resource-template
parser in
[`devices/mcp_catalog.py`](../server/src/openctopus_server/devices/mcp_catalog.py).
The Server MCP catalog imports that parser alongside the existing shared catalog
validation helpers. Client MCP remains an independent implementation, aligned
through contract tests as specified in [ADR-047](DECISIONS.md#adr-047--mcp-clients-live-at-their-execution-site).

Admins configure Server MCP services once for all users. Candidate validation
uses a temporary client and closes it after discovery. During each tool-enabled
Agent iteration, the conversation-owned runtime connects and refreshes the
catalog before Provider schemas are built; the saved administrator allowlist is
applied to that fresh catalog. The client remains available to the same
conversation across the iteration and is retained for up to 10 idle minutes.
Idle clients may be evicted under configured process-wide client and stdio
limits (`OPENOCTOPUS_SERVER_MCP_MAX_CLIENTS=256`,
`OPENOCTOPUS_SERVER_MCP_MAX_STDIO_CLIENTS=32`, and
`OPENOCTOPUS_SERVER_MCP_MAX_STARTING=8` by default); active clients are never
evicted. Runtime ownership is in memory, so
restarts and idle eviction may end a connection and require a later iteration
to reconnect. It is not durable state. User-specific OAuth credentials are not
implemented; current remote headers and stdio environment credentials remain
administrator-owned and shared.

## Browser chat

- [`ChatPage.tsx`](../frontend/src/chat/ChatPage.tsx) coordinates the active
  conversation, composer, uploads, and live stream.
- [`useRecoveredHistory.ts`](../frontend/src/chat/useRecoveredHistory.ts) owns
  persisted-history polling, recovery cursors, and disposal of stale requests.
- [`Transcript.tsx`](../frontend/src/chat/Transcript.tsx) renders messages,
  tool activity, sender identity, and channel delivery/context details.
- [`attachments.ts`](../frontend/src/chat/attachments.ts) contains draft types,
  attachment identity/display helpers, and clipboard-image extraction.

Navigation and asynchronous callbacks retain their session/generation checks.
UI extraction must not allow a late stream, upload, rename, or delete result to
modify a newly selected conversation.

## Verification

Run the lint, type-check, unit, and browser commands in the
[development guide](../README.md#development-and-verification). Server tests need
PostgreSQL; real storage and device checks also need RustFS and the source Client
installed in the test environment.

The Server uses SQLAlchemy 2.0. Its dependency range stays below 2.1, which changes
the type signatures of query and result objects. Update those annotations and
verify the database paths together when upgrading SQLAlchemy.

The Server CI enables these additional checks when running `pytest`:

```bash
OO_RUN_CAPACITY_HARNESS=1 OO_RUN_NETWORK_CAPACITY_HARNESS=1 \
RUN_RUSTFS_INTEGRATION=1 PY5_REAL_E2E=1 PY6_REAL_E2E=1 \
PY7_REAL_E2E=1 PY8A_REAL_E2E=1 PY8C_REAL_E2E=1 pytest -q
```

Preserve the existing cancellation, late-frame, shared-admission, and
cross-platform move tests when changing these boundaries. Native Client CI and
frozen-bundle smoke tests exercise Linux, macOS, and Windows behavior.
