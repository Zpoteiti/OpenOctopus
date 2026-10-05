---
name: pair-client
description: Pair a user-owned computer with the Server and verify the Client connection
always_on: false
---
# Pair an OpenOctopus Client

Guide the user through **Devices → Add device**. Creating a device returns a one-time `openoctopus_dev_...` token; the user should copy it directly into the Client's connection settings dialog. Do not paste a real token into conversation, source files, or a skill.

On that computer, install the native tray Client for its operating system (a `.deb` on Linux x64, the per-user `.exe` installer on Windows x64, or the `.dmg` on macOS). The tray is the only launch path: open its menu, choose **连接设置** (connection settings), enter the Server URL and the issued token, and click **保存并连接**. The Client stores the token in the operating system credential store; it is never kept in a configuration file, environment variable, or log. Running from source requires Python 3.12 and the repository's Client installation instructions.

Verify **Devices** reports the device online (the tray menu shows 状态：在线) and that the tray's status stays stable for a minute. A registered entry alone does not establish a live connection. Use an available file tool against the exact advertised device name for a small read/list check. Device tools require ownership and a live connection; Server tools cannot execute host commands.

The paired computer's effective Workspace is the Client's own root, `~/.openoctopus/workspace`; the tray passes it to its core and it takes precedence over the Server-side `workspace_path` field, which is why the browser Workspace path does not point at a folder the user can inspect on that machine. The path-restriction policy, `ssrf_denylist`, timeouts, and environment allow-list still come from the Server. Client exec and MCP run with the host user's privileges; the Workspace restriction is not an operating-system sandbox. Explain that boundary before the user chooses the policy.

If the token was lost, use **Regenerate Token**, then paste the new value into the tray's connection settings and save; rotation invalidates the old token and disconnects the Client. Deleting the device also revokes access. Account/device setup uses the browser UI or authenticated `/api/devices` routes. This guide adds no device-management tool to the Agent; prepare instructions for the user when configuration tools are unavailable.
