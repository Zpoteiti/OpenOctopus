# OpenOctopus Python client

The Py6 client is a standalone Python 3.12 package for one user-owned device.
It connects to the server over `/ws/device` and runs shared file tools,
`web_fetch`, and the trusted-device `exec` tools locally.

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

With no subcommand, `run` is the default. Transient connection failures retry
with bounded exponential backoff. Authentication and protocol configuration
failures exit instead of retrying forever.

## Local policy and conversion

The server sends `workspace_path`, `sandbox_mode`, `ssrf_denylist`,
`shell_timeout_max`, and `env_allowlist` during the WebSocket handshake. A
leading `~` expands against the local user's home.
In sandbox mode, file paths must stay under the workspace root; trusted mode
allows paths outside it, but special files and symlink/reparse escapes remain
blocked for bounded operations. `ssrf_denylist` applies to the client
`web_fetch` path. This is application policy, not an OS security sandbox.
Shell tools are exposed only when `sandbox_mode=false`. Commands use closed
stdin pipes by default; `tty=true` selects a line-oriented POSIX PTY or Windows
ConPTY for REPLs and simple prompts. Full-screen TUI and secret input are not
supported.

PDF, DOCX, XLSX, PPTX, and downloaded HTML conversion runs locally in a helper
process. Inputs are limited to 8 MiB, output to 128,000 characters, and PDF
requests to at most 20 pages. Each conversion has a 20-second deadline;
Linux applies a 2 GiB address-space limit and CPU limit. The helper receives no
device token or arbitrary parent environment variables.

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
