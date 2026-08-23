# OpenOctopus Python client

The Py7 client is a standalone Python 3.12 package for one user-owned device.
It connects to the server over `/ws/device` and runs shared file tools,
`web_fetch`, fixed `exec` tools, and user-configured MCP runtimes locally.

## Install

From this directory, create a virtual environment and install the package with
development/build extras:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev,build]'
```

Set the two required startup variables. The token is consumed from the process
environment during startup and is not written to a client config file:

```bash
export OPENOCTOPUS_SERVER_URL='https://openoctopus.example'
export OPENOCTOPUS_DEVICE_TOKEN='openoctopus_dev_...'
```

`OPENOCTOPUS_SERVER_URL` must be an `http://` or `https://` origin without a
path, query, or fragment. The client derives `/ws/device`; use `wss` through
HTTPS. The token is the one-time value returned by device creation or token
regeneration.

## Run and version

```bash
python -m openoctopus_client version
python -m openoctopus_client run
```

With no subcommand, `run` is the default. Before the process has completed one
Protocol v3 `hello`/config-applied acknowledgement, an unreachable Server is a
startup failure and the process exits. After one successful ready transition,
ordinary disconnects retry with bounded exponential backoff. Authentication,
replacement, and protocol configuration failures exit.

## Local policy and conversion

The server sends `workspace_path`, `restrict_to_workspace`, `ssrf_denylist`,
`shell_timeout_max`, and `env_allowlist` during the WebSocket handshake. A
leading `~` expands against the local user's home.
When `restrict_to_workspace=true`, structured file paths and an exec/PTY
process's initial working directory must stay under the workspace root. When it
is false, absolute paths outside the workspace are allowed, but special files
and symlink/reparse escapes remain blocked for bounded operations. Shell
commands themselves are unrestricted in both modes. `ssrf_denylist` applies
independently to client `web_fetch`. This is application policy, not an OS
security sandbox. Exec and PTY are available on every paired Device; commands
use closed stdin pipes by default and `tty=true`
selects a line-oriented POSIX PTY or Windows ConPTY for REPLs and simple
prompts. Full-screen TUI and secret input are not supported.

## Device MCP

Device MCP configuration is owned by the Server and changed through
`GET/PATCH /api/devices/{name}/config`; the Client has no local MCP config
file. Py7 supports explicit `stdio`, `streamable_http`, and legacy `sse`
transports through FastMCP 3.4.7. A new or modified MCP is saved only after the
online Client completes a real initialize and bounded discovery of tools,
static resources, resource templates, and prompts. Pure deletion may be saved
while the Device is offline.

`env` and HTTP header values are sent only in private Device config frames and
must use WSS when non-empty. The Client removes every `OPENOCTOPUS_*` variable
from stdio MCP children. `restrict_to_workspace` and `ssrf_denylist` do not
confine MCP or exec networking; installing an MCP trusts it with the Device
user's host and network access. MCP runtimes survive an ordinary Server
WebSocket reconnect, but calls that may have been sent are never replayed.

PDF, DOCX, XLSX, PPTX, and downloaded HTML conversion runs locally in a helper
process. Inputs are limited to 8 MiB, output to 128,000 characters, and PDF
requests to at most 20 pages. Each conversion has a 20-second deadline;
Linux applies a 2 GiB address-space limit and CPU limit. The helper receives no
device token or arbitrary parent environment variables. It reopens the
canonical path without following links where the OS supports it and verifies
the file identity captured before the worker started.

`SIGINT` and `SIGTERM` cancel active work. Process backends use a fixed
two-second graceful-termination window before force-kill, while the complete
client shutdown has a 15-second watchdog. Children are reaped whenever the OS
backend permits it; the watchdog prevents the standalone client from hanging
indefinitely.

## Frozen bundle

Build the platform-native PyInstaller one-folder bundle on the target OS:

```bash
python -m PyInstaller --noconfirm --clean openoctopus_client.spec
./dist/openoctopus-client/openoctopus-client version
```

The output directory is `dist/openoctopus-client/`; distribute the directory
as a compressed archive rather than a single static binary. Linux cannot
validate macOS `forkpty` or Windows ConPTY/DLL packaging, so run
`tests/frozen_runtime_smoke.py` on each native artifact.

## Tests

```bash
python -m pytest -q
python -m ruff check .
python -m mypy --strict src tests
```
