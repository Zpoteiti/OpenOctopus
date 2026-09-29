---
name: pair-client
description: Pair a user-owned computer with the Server and verify the Client connection
always_on: false
---
# Pair an OpenOctopus Client

Guide the user through **Devices → Add device**. Choose the device name, Workspace path, and path restriction policy. Creating a device returns a one-time `openoctopus_dev_...` token; the user should copy it directly into the Client process environment.

On that computer, use the release bundle for its operating system and architecture. Configure `OPENOCTOPUS_SERVER_URL` with the actual Server URL and `OPENOCTOPUS_DEVICE_TOKEN` with the issued token, then run the bundle's `openoctopus-client run` executable. Do not paste a real token into conversation, source files, or a skill. The Client consumes the token from its environment at startup rather than a Client configuration file. Source operation requires Python 3.12 and the repository's Client installation instructions.

Verify **Devices** reports the device online and the configured Workspace matches the intended folder. A registered entry alone does not establish a live connection. Use an available file tool against the exact advertised device name for a small read/list check. Device tools require ownership and a live connection; Server tools cannot execute host commands.

The Workspace restriction guards OpenOctopus-resolved file paths and initial command working directories. Client exec and MCP run with the host user's privileges; this setting is not an operating-system sandbox. Explain that boundary before the user chooses the policy.

If the token was lost, use **Regenerate Token** and restart the Client with the new value. Rotation invalidates the old token and disconnects the Client; deleting the device also revokes access. Account/device setup uses the browser UI or authenticated `/api/devices` routes. This guide adds no device-management tool to the Agent; prepare instructions for the user when configuration tools are unavailable.
