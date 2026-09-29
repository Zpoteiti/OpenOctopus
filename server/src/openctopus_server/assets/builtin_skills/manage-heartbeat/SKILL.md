---
name: manage-heartbeat
description: Edit personal HEARTBEAT.md for conditional checks evaluated by Heartbeat
always_on: false
---
# Manage Heartbeat tasks

Use Heartbeat for conditional checks that should be considered during periodic pulses. Use Cron for exact-time work. Heartbeat reads the personal Server file `HEARTBEAT.md`; users can edit it in **Automations → Heartbeat** or through ordinary personal Workspace file tools.

Read the current file first and preserve unrelated content with precise text edits. UI/API saves can use the earlier ETag in `If-Match`; ordinary Agent file tools do not expose that header. Create an exact `## Active Tasks` heading when absent and place the requested checks beneath it. For example:

```markdown
# Heartbeat

## Active Tasks
- During working hours in my account timezone, check the specified inbox for an interview invitation. Notify me only when a new invitation needs action.
```

The first meaningful `## Active Tasks` section is considered up to the next level-one or level-two heading. Use top-level bullet or numbered entries for separate tasks; continuation lines remain part of that task. Plain paragraphs are also accepted. Completed `[x]` entries are ignored. HTML comments and headings inside fenced code do not activate tasks. Keep at most eight tasks, 500 characters per task, and 2,000 characters in total; oversized task sets wait until the file is shortened. Missing or empty active tasks produce no work. Read the saved file to verify the intended check is active. To remove a task, delete its active text while preserving others; to stop all checks, empty the active section.

State concrete conditions, sources, and notification expectations. A decision pulse requires the administrator's Jev configuration and sees the Heartbeat document, current time, and the account timezone; it has no live tools or external state. Jev selects `run` or `skip` for each parsed task. Selected original task bodies run in file order in a normal Agent turn where authorized tools can perform the actual checks. Missing Jev configuration, failed decisions, busy sessions, and admission can defer execution, so a saved task is not a guarantee of an immediate run. Administrators can configure and explicitly check Jev in **Admin settings**.

Account timezone is configured in **Account**. Heartbeat has no dedicated Agent scheduling tool, and writing the file does not change the Server's pulse interval. Record success only after the file write/readback; report actual runs from their Heartbeat conversation history.
