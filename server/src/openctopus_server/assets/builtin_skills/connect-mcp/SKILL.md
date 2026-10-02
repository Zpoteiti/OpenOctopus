---
name: connect-mcp
description: Configure Device MCP or administrator-installed Server MCP services with private conversation connections
always_on: false
---
# Connect an MCP service

First establish where the service should execute. Device MCP runs on a paired Client for its owner. Server MCP is installed by an administrator and available to all users; each conversation has its own Server connection, retained for up to 10 idle minutes. Admin-provided credentials remain shared, so personal OAuth credentials are not supported there. Server names reserve the same name across Device MCP.

For personal services, guide the user to **Devices → Manage Device MCP**. Inspect the existing configuration, choose the supported transport (`stdio`, `streamable_http`, or `sse`), and fill the actual executable/arguments or service URL. Stdio uses explicit environment variables and optional working directory; remote services use configured headers. Do not invent an executable, endpoint, credential, or capability.

Adding or changing Device MCP requires the Client online and a real initialize/discovery check. Review discovered capabilities and select what should be enabled. Save the complete configuration list: `PATCH /api/devices/{name}` takes `base_config_revision` and a whole-field `mcp_servers` replacement. Reload after a revision conflict; pure removal can be saved offline.

For shared services, guide an administrator to **Admin → Shared MCP**. The authenticated admin API is `GET`/`PUT /api/admin/server-mcp`; replacement uses `base_config_revision` and the complete `mcp_servers` list. New or changed configurations must pass real initialize/discovery before saving. Validation uses a temporary connection that closes after discovery. The displayed discovery catalog is an installation preview; each conversation refreshes available capabilities before every tool-enabled model step and applies the administrator's saved capability allowlist. Shared MCP reserves names and can suppress a same-named Device service.

Use only capabilities present in the current Agent tool catalog, with their advertised schemas and routing. A saved installation preview alone does not prove that a conversation can currently use a capability. This skill grants no configuration tool or administrator authority. When no authorized configuration tool is available, prepare the settings and let the user save them in the UI. The user should enter secrets directly into the configuration fields; do not include them in chat or personal skill files.
