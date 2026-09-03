# Py10 ChannelManager capacity evidence

The harness drives the production `ChannelManager` with detached Discord and
DingTalk config metadata and lightweight in-memory adapters. It opens neither
PostgreSQL nor a platform connection, so the JSON says
`database_exercised=false` and `platform_network_exercised=false`.

## 500-adapter merge gate

Run from `server/` and retain the JSON with the merge evidence:

```bash
conda run --no-capture-output -n oo python \
  scripts/channel_capacity_harness.py \
  --adapters 500 \
  --duration-seconds 600 \
  --sample-interval-seconds 1 \
  --heartbeat-interval-seconds 30 \
  | tee /tmp/openoctopus-channel-capacity-500.json
```

The `Channel Capacity` workflow runs a short ordinary-CI smoke. Apply the
`channel-capacity-500` pull-request label before merge to run the ten-minute
500-adapter gate and upload its JSON artifact. A successful merge-gate report
has top-level `ok=true` and every value under `checks` set to `true`.

## 1000-adapter recorded run

The 1000-adapter profile is additional recorded evidence, not the required
merge gate. Start it with the workflow's `1000` manual option or run:

```bash
conda run --no-capture-output -n oo python \
  scripts/channel_capacity_harness.py \
  --adapters 1000 \
  --duration-seconds 600 \
  --sample-interval-seconds 1 \
  --heartbeat-interval-seconds 30 \
  | tee /tmp/openoctopus-channel-capacity-1000.json
```

Record the commit, machine/runner type, Python version, and unmodified JSON
alongside the result so later runs are comparable.

## What is verified

The config source splits configs evenly across Discord and DingTalk. The
manager starts each platform in production-sized pages and must never exceed
its production startup limit of 32 concurrent adapter starts. The harness then
closes every current adapter in one concentrated burst. It records both the
manager's full-jitter delays and observed replacement start times, and rejects
a tight reconnect herd.

After reconnect, every config must have exactly one live adapter whose runtime
generation matches `ChannelManager.status()`. An old adapter and its replacement
emit the same handoff callback; generation fencing must reject the stale copy,
leaving every accepted callback source ID unique. Shutdown must leave the
manager with zero background tasks and no harness-created asyncio task alive.

The report also records:

- startup and reconnect wall time;
- current/peak/after-shutdown RSS, file descriptors, and asyncio tasks;
- RSS and FD deltas from baseline;
- event-loop lag mean, p95, and maximum at the requested sample interval;
- configured metadata-adapter heartbeat interval and observed mean, p95, and
  maximum interval.

RSS and FD values are machine-dependent observations. They are intentionally
not absolute pass/fail thresholds. The heartbeat is a deterministic metadata
adapter probe of manager task scheduling; it is not a Discord or DingTalk SDK
heartbeat. Real platform connection behavior remains part of the credentialed
Py10 end-to-end run.

## Recorded local acceptance — 2026-09-02

Both profiles ran against the same final `ChannelManager` and harness sources
on Linux 7.0.0 x86_64, Python 3.12.13, an Intel Core i5-14600KF (20 logical
CPUs), and 61 GiB RAM. The working tree was based on
`1b97145807d2f16b1b2a7e71cd78c5789505264a`.

- `ChannelManager` SHA-256: `3e733e0898d4243003e5d27866ef540183d82c8c9f131176980fabf6e9788094`
- harness SHA-256: `93c3b0c83f1439c88906a9155e4143d06d1d3dced0965e20ebf99d0f5b6dd720`
- [500-adapter raw JSON](capacity-results/2026-09-02-500.json), SHA-256
  `acb7ee302d3d4d0095b4d74795795854306f247c7806d7e8662bb1cd3bcd8743`
- [1000-adapter raw JSON](capacity-results/2026-09-02-1000.json), SHA-256
  `319ccdbf304c5fa5a2b80487866fd206d4e1016796f5b38bda9e01b7019e78ad`

| Metric | 500 merge gate | 1000 recorded run |
|---|---:|---:|
| Result / wall time | pass / 601.145 s | pass / 601.415 s |
| Startup / max concurrent starts | 0.062 s / 32 | 0.138 s / 32 |
| Reconnect time / observed spread | 1.010 s / 0.999 s | 1.027 s / 1.004 s |
| Accepted callbacks / duplicates | 1,000 / 0 | 2,000 / 0 |
| Stale callbacks rejected | 500 | 1,000 |
| Event-loop lag mean / p95 / max | 1.198 / 1.948 / 2.434 ms | 1.147 / 1.874 / 2.865 ms |
| Heartbeat mean / p95 / max | 30.000634 / 30.001064 / 30.049755 s | 30.000618 / 30.001072 / 30.002285 s |
| RSS peak/after delta | 10,027,008 / 10,027,008 B | 18,882,560 / 18,882,560 B |
| FD peak/after delta | 0 / 0 | 0 / 0 |
| Tasks after shutdown / manager tasks | 1 baseline task / 0 | 1 baseline task / 0 |

Every boolean check in both raw reports is `true`. These are metadata-adapter
capacity results; PostgreSQL and platform networks remain explicitly marked as
not exercised.
