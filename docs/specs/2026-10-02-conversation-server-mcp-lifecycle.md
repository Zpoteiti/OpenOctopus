# Conversation-owned Server MCP lifecycle

Status: accepted. Implements [ADR-139](../DECISIONS.md#adr-139--conversation-owned-server-mcp-clients).

## Ownership and discovery

Administrators install MCP configurations for all users. Installation uses a
temporary connection to validate and save a catalog preview, then closes that
connection. Server startup loads configuration without opening MCP connections.

Each `(user, conversation, MCP server)` owns its FastMCP client and transport.
Before every tool-enabled model request, the agent prepares its configured MCP
connections and discovers their current tools, resources, templates, and prompts.
Later model requests refresh the catalog on the same connections. A changed
description, schema, or capability list is visible in the next request.
Restricted channel participants do not acquire these connections.

The admin configuration revision, conversation runtime generation, and private
catalog identify each issued tool route. A stale or foreign route fails before
execution. Upstream changes do not rewrite admin configuration. Explicit selected
capabilities that disappear are unavailable until rediscovered; their saved
selection remains intact.

## Idle retention and resource limits

An agent run holds a lease through all model and tool iterations. Completion,
failure, and cancellation release it. After the last run in a conversation ends,
its connections remain available for 600 seconds. A new run cancels expiry and
reuses them. Session/account deletion and process shutdown retire connections.

The process counts opening, active, idle, closing, and temporary validation clients
against these admission limits:

| Setting | Default | Purpose |
| --- | ---: | --- |
| `OPENOCTOPUS_SERVER_MCP_MAX_CLIENTS` | 256 | All live MCP clients |
| `OPENOCTOPUS_SERVER_MCP_MAX_STDIO_CLIENTS` | 32 | Owned stdio clients/process trees |
| `OPENOCTOPUS_SERVER_MCP_MAX_STARTING` | 8 | Concurrent connection starts |

When client capacity is needed, the oldest idle eligible connection is closed
first. Active conversations are protected. If capacity remains unavailable, the
request receives a bounded busy error. A resource is counted until cleanup is
confirmed; stdio teardown includes owned descendant processes.

Tool invocations have separate immediate admission limits: 32 active/draining
calls per process, four per user, and the configured per-client concurrency.
Saturation returns `tool_mcp_busy` without a shared call queue.

Retention is best effort. Eviction, remote disconnects, configuration changes,
and server restarts can end temporary browser/REPL state before ten minutes.
Persistent state requires support from the MCP server itself. Admin-configured
credentials remain shared; separate clients do not grant separate upstream
accounts, filesystem permissions, or OS isolation.

## Failure behavior and visibility

If a configured MCP cannot connect or refresh its catalog, the tool-enabled model
request fails with an MCP availability message. Capacity exhaustion produces an
MCP busy message. The server health endpoint remains independent of MCP uptime.
Already-issued tool calls are never automatically replayed after uncertainty.

The admin page shows aggregate active, idle, closing, and active-call counts plus
bounded diagnostic errors. It does not expose users, conversation identifiers,
arguments, or results. Its saved catalog is an installation preview.

## Verification

Tests cover ownership across users/conversations, reuse and idle expiry, fresh
schemas at model boundaries, stale-route fencing, LRU admission, cancellation,
deletion, configuration changes, and real HTTP/stdio cleanup. The capacity harness
measures private connections and explicit busy outcomes at a configured cap;
its 500-user burst does not establish support for 500 simultaneous live sessions.
