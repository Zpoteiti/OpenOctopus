# Real device-network capacity evidence

Run this only when the local PostgreSQL database configured by `server/.env`
is available:

```bash
cd server
conda run --no-capture-output -n oo python scripts/device_network_capacity_harness.py \
  --connections 500 --users 100 --sessions 500 --dispatch-concurrency 64 \
  | tee /tmp/openoctopus-device-capacity-500.json
```

The script starts a loopback Uvicorn listener and opens 500 real `/ws/device`
WebSockets. Each lightweight peer completes the Protocol v3 hello and
config-applied acknowledgement. It inserts uniquely prefixed `users` and
`devices` rows, so each Bearer token is authenticated by the production PostgreSQL
`devices.token_hash` lookup. The source side is 500 lightweight protocol peer
tasks in one process—not 500 PyInstaller processes, and not provider/Agent
turns. The JSON report states those limits explicitly.

The script also runs one bounded server-to-client transfer per connection and a
burst of same-owner, distinct-client bridges while heartbeats and unrelated
Device tool calls remain active. Each bridge consumes one Server admission
permit and two endpoint indexes. The burst reaches the effective global limit,
reaches the per-user limit of two, fills the bounded fair wait queue, and records
stable busy results for excess work. One owner's destinations delay readiness
and Server-side writes to model a slow network sink, so the relay queue applies
backpressure while another owner still completes first. Each lightweight peer
enforces the Client's two-slot transfer limit.

The JSON records bridge wall time and p50/p95 latency, warnings/errors, peak RSS,
FDs, tasks, active/waiting permits, endpoint/tombstone counts, and the four-chunk
relay queue high-water. It verifies active indexes, pinned tombstones, reserved
credits, tasks, local slots, and admission counters return to zero after bridge
cleanup; finalized protocol tombstones remain bounded for their normal TTL. The
script also creates one bounded offline MCP catalog per connected Device,
projects each owner's Provider schemas/routes, and records catalog high-water
metrics. The lightweight peers do not start MCP runtimes, so
`mcp_runtime_high_water` is zero; the native/frozen Client E2E is the real-runtime
gate. The harness deletes only the exact rows it created in `finally`. It never
truncates existing tables.
The ordinary `device_capacity_harness.py` remains the faster in-memory registry probe; its
`network_exercised` value stays `false`.

The ordinary CI runs the eight-connection opt-in smoke. The 500-connection merge
gate is the `Device Capacity 500 Gate` workflow, started manually either with
`workflow_dispatch` or by applying the `capacity-500` label to a PR. Its JSON
artifact must be retained with the PR evidence before merge.

The real PostgreSQL smoke test is opt-in:

```bash
cd server
OO_RUN_NETWORK_CAPACITY_HARNESS=1 \
  conda run --no-capture-output -n oo pytest -q \
  tests/test_device_network_capacity_harness.py
```
