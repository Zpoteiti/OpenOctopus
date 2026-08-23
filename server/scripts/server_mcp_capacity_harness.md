# Server MCP 500-user capacity evidence

Run the merge gate from `server/`:

```bash
conda run --no-capture-output -n oo python \
  scripts/server_mcp_capacity_harness.py \
  --users 500 --runtime-concurrency 8 \
  | tee /tmp/openoctopus-server-mcp-capacity-500.json
```

The harness submits 500 unique users to the production
`ServerMcpCoordinator` and one default-concurrency runtime. The first eight
calls are issued, 32 remain in the bounded queue, and the rest receive
immediate busy admission. The deterministic scheduler clock then expires the
queued calls at the production five-second deadline without sleeping for five
wall-clock seconds.

Issued calls use one real shared FastMCP client/session over Streamable HTTP to
a loopback MCP server exposing an equivalent `search` tool. The loopback server
runs under Uvicorn, and the report samples its live TCP protocol objects for the
HTTP connection high-water. It also records real process RSS, file descriptor
and asyncio task high-water values. Pending-future high-water means accepted
scheduler tickets whose issue future is still unresolved; queue and reservation
counters come directly from the production scheduler.

The same run separately fills the fixed global 32-permit boundary and submits
five calls from one user across runtimes to prove the fixed per-user limit of
four. Cleanup must leave zero scheduler reservations, waiters and HTTP
connections, with task and descriptor counts returned to their baseline.

Ordinary Server CI runs the 20-user smoke in
`tests/test_server_mcp_capacity_harness.py`. The 500-user merge gate is the
`Server MCP Capacity 500 Gate` workflow, triggered manually or by applying the
existing `capacity-500` label to a pull request. That label also starts the
separate Device capacity gate. Retain both JSON artifacts with the merge
evidence.

This is a scheduler/shared-session capacity gate. Its MCP endpoint is a local,
deterministic search wrapper rather than public SearXNG, and it does not run a
Provider/Agent turn. The separate Py8a real smoke covers those product paths.
