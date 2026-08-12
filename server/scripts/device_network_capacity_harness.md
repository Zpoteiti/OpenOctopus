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
WebSockets. It inserts uniquely prefixed `users` and `devices` rows, so each
Bearer token is authenticated by the production PostgreSQL
`devices.token_hash` lookup. The source side is 500 lightweight protocol peer
tasks in one process—not 500 PyInstaller processes, and not provider/Agent
turns. The JSON report states those limits explicitly.

The script also runs one bounded server-to-client transfer per connection while
heartbeats remain active. It raises the `RLIMIT_NOFILE` soft limit only up to the
hard limit when needed, records RSS/fd/task/queue/transfer high-water metrics,
measures actual online connections before shutdown and after cleanup, and deletes
only the exact rows it created in `finally`. It never truncates existing tables. The ordinary
`device_capacity_harness.py` remains the faster in-memory registry probe; its
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
