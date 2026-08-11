# OpenOctopus Python client

The Py5 client is a standalone Python 3.12 package for one user-owned device.
It connects to the server over `/ws/device` and runs the shared file tools and
`web_fetch` locally.

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

The server sends `workspace_path`, `sandbox_mode`, and `ssrf_denylist` during
the WebSocket handshake. A leading `~` expands against the local user's home.
In sandbox mode, file paths must stay under the workspace root; trusted mode
allows paths outside it, but special files and symlink/reparse escapes remain
blocked for bounded operations. `ssrf_denylist` applies to the client
`web_fetch` path. This is application policy, not an OS security sandbox.

PDF, DOCX, XLSX, PPTX, and downloaded HTML conversion runs locally in a helper
process. Inputs are limited to 8 MiB, output to 128,000 characters, and PDF
requests to at most 20 pages. Each conversion has a 20-second deadline;
Linux applies a 2 GiB address-space limit and CPU limit. The helper receives no
device token or arbitrary parent environment variables.

`SIGINT` and `SIGTERM` cancel active work and give cleanup a fixed two-second
grace. Conversion children are terminated, force-killed if necessary, and
reaped. If an operating-system filesystem call cannot be cancelled within that
grace, the standalone client hard-exits instead of hanging indefinitely.

## Linux bundle

Py5's merge gate is a Linux x86-64 PyInstaller one-folder bundle:

```bash
python -m PyInstaller --noconfirm --clean openoctopus_client.spec
./dist/openoctopus-client/openoctopus-client version
```

The output directory is `dist/openoctopus-client/`; distribute the directory
as a compressed archive rather than a single static binary. Native Windows and
macOS frozen CI/verification are deferred and are not validated by a Linux
build.

## Tests

```bash
python -m pytest -q
python -m ruff check .
python -m mypy --strict src tests
```
