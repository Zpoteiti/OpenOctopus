# Server MCP private-client capacity evidence

Run the optional 500-user burst gate from `server/`:

```bash
conda run --no-capture-output -n oo python \
  scripts/server_mcp_capacity_harness.py \
  --users 500 --max-clients 8 \
  | tee /tmp/openoctopus-server-mcp-capacity-500.json
```

The harness opens eight private Streamable HTTP MCP clients for eight users and holds a real `search` call open on each. It then tries 492 more users while those eight conversations are active. All 492 must receive immediate `tool_mcp_busy` capacity errors; there is no waiting queue. The report counts MCP session IDs returned by the loopback server, active and idle supervisor sessions, actual HTTP calls and connections, process RSS, file descriptors, and asyncio tasks. It checks that a later call in one conversation reuses its client and that an idle client is evicted when a new user arrives.

After shutdown, all MCP session IDs must have received a close request, and the supervisor must have no private sessions or open HTTP connections. The ordinary CI smoke uses 20 users with a two-client cap. The 500-user workflow runs manually or when a pull request has the `capacity-500` label.

This gate measures a **500-user burst against an eight-client cap**, not 500 simultaneous live clients. Its endpoint is a deterministic local search fixture, and it does not make a Provider/Agent call. The opt-in Py8a acceptance test covers real chat turns with HTTP and stdio MCP fixtures.
