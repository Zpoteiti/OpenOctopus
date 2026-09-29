---
name: manage-cron
description: Create, inspect, update, or remove exact scheduled Agent jobs using Cron
always_on: false
---
# Manage Cron jobs

Use Cron for a requested one-time or recurring task at a definite schedule. Establish the task, timing, and timezone; use the account timezone when no other timezone is requested. Conditional checks whose timing depends on state belong in Heartbeat.

Inspect existing jobs with the `cron` tool's `action="list"` before creating duplicates. Follow `next_offset` using the `offset` argument. The available Agent tool supports `add`, `list`, and `remove`.

For `action="add"`, provide a clear `name`, task `message`, and exactly one schedule:

- `every_seconds`: interval of at least 60 seconds, at most 31,536,000 seconds.
- `cron_expr`: standard five-field expression; use `tz` for its IANA timezone.
- `at`: future RFC 3339 instant, or local date-time with `tz`, for a one-time task.

The message describes what the future Agent turn should accomplish, the relevant sources, and when to notify. Check the returned job's schedule, timezone, and `next_fire_at`; successful creation does not mean its task has already run. Cron creates/reuses its own conversation history when it fires. Runtime tool permissions remain authoritative.

To edit a job, use **Automations → Cron** or an authorized authenticated `PATCH /api/cron/{job_id}` call. The Agent `cron` tool has no update action. The UI/API also provide owned-job list/create/read/delete at `/api/cron`. Read before editing and preserve fields the user did not ask to change. When only the Agent tool is available, explain that editing needs the UI rather than silently removing and recreating the job.

To stop future triggers at the user's request, call `cron(action="remove", job_id="<actual ID>")`. Removal retains existing conversation history; deleting a conversation is a separate action. Report only the schedule or action actually confirmed by the tool/API response.
