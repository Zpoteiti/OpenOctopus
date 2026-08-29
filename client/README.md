# OpenOctopus Client

The OpenOctopus Client pairs one user-owned computer with an OpenOctopus
Server over Protocol v3. It runs local file tools, transfers, `web_fetch`,
pipe/PTY command sessions, and Device MCP services on behalf of that user's
agent.

The Client supports Linux x64, macOS arm64/x64, and Windows x64. It requires
Python 3.12 when run from source.

> The Client is an alpha/demo release. Commands and MCP services run with the
> permissions of the operating-system user that starts it. The Workspace
> restriction is a path guard, not an operating-system sandbox.

All source installation, build, and test commands below run from the
repository's `client/` directory.

## Pair the computer

In the OpenOctopus browser UI:

1. Open **Devices** and create a device.
2. Choose its Workspace path and policy.
3. Copy the `openoctopus_dev_...` token. It is shown only once; losing it
   requires token regeneration.

The token is consumed from the Client process environment at startup and is
not written to a Client configuration file.

## Run a release bundle

Download the archive for the target computer from
[GitHub Releases](https://github.com/Zpoteiti/OpenOctopus/releases):

- `openoctopus-client-<version>-linux-x64.tar.gz`
- `openoctopus-client-<version>-macos-arm64.tar.gz`
- `openoctopus-client-<version>-macos-x64.tar.gz`
- `openoctopus-client-<version>-windows-x64.zip`

Linux or macOS:

```bash
VERSION='v0.0.1' # replace with the release tag you downloaded
PLATFORM='linux' # or macos
ARCH='x64'       # or arm64 on macOS
tar -xzf "openoctopus-client-${VERSION}-${PLATFORM}-${ARCH}.tar.gz"
export OPENOCTOPUS_SERVER_URL='https://openoctopus.example'
export OPENOCTOPUS_DEVICE_TOKEN='openoctopus_dev_...'
./openoctopus-client/openoctopus-client version
./openoctopus-client/openoctopus-client run
```

Windows PowerShell:

```powershell
$Version = 'v0.0.1' # replace with the release tag you downloaded
Expand-Archive ".\openoctopus-client-$Version-windows-x64.zip" -DestinationPath .
$env:OPENOCTOPUS_SERVER_URL = 'https://openoctopus.example'
$env:OPENOCTOPUS_DEVICE_TOKEN = 'openoctopus_dev_...'
.\openoctopus-client\openoctopus-client.exe version
.\openoctopus-client\openoctopus-client.exe run
```

These are unsigned, platform-native one-folder bundles. Keep the complete
`openoctopus-client` directory together. They are not installers, services, or
single static binaries.

## Run from source

Linux or macOS:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
export OPENOCTOPUS_SERVER_URL='https://openoctopus.example'
export OPENOCTOPUS_DEVICE_TOKEN='openoctopus_dev_...'
python -m openoctopus_client run
```

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
$env:OPENOCTOPUS_SERVER_URL = 'https://openoctopus.example'
$env:OPENOCTOPUS_DEVICE_TOKEN = 'openoctopus_dev_...'
python -m openoctopus_client run
```

With no subcommand, `run` is the default.

`OPENOCTOPUS_SERVER_URL` must be an `http://` or `https://` origin without a
path, query, fragment, or credentials. The Client derives `/ws/device` and uses
WSS for an HTTPS Server.

## Connection lifecycle

Before the first successful Protocol v3 hello/config acknowledgement, an
unreachable Server is a startup failure. After the Client has reached ready
once, ordinary network disconnects retry with bounded exponential backoff.
Authentication, connection replacement, protocol mismatch, and invalid Server
configuration are permanent failures and stop the process.

Ordinary Server disconnects do not stop running exec sessions or MCP runtimes.
Calls whose outcome became ambiguous are not replayed automatically. Client
shutdown, device deletion, or token rotation stops Client-owned child work.

## Workspace and command policy

The Server owns and sends these settings during the device handshake:

- `workspace_path`
- `restrict_to_workspace`
- `ssrf_denylist`
- `shell_timeout_max`
- `env_allowlist`

A leading `~` expands once against the Client operating-system user's home.
Relative paths resolve under `workspace_path`. Native absolute paths use POSIX
syntax on Linux/macOS and drive or UNC syntax on Windows.

When `restrict_to_workspace=true`, structured file paths and an exec/PTY
process's initial working directory must stay within the Workspace. The Client
also rejects symlink/reparse escapes for bounded file operations. When it is
false, native absolute paths outside the Workspace are allowed, while the
same no-follow file checks remain.

This policy does not inspect or constrain shell commands. Exec and PTY are
available on every paired device and use closed stdin pipes by default.
`tty=true` selects a line-oriented POSIX PTY or Windows ConPTY for REPLs and
simple prompts. Full-screen TUI applications and reliable secret/password
input are not supported.

The Client `web_fetch` denylist is independent of the Server denylist. It does
not constrain networking performed by exec or MCP.

## Device MCP

Device MCP configuration is stored on the Server and managed through the
device page or `GET/PATCH /api/devices/{name}/config`; the Client has no local
MCP configuration file. Supported transports are `stdio`, `streamable_http`,
and legacy `sse` through FastMCP 3.4.7.

Adding or changing an MCP service requires the Client to be online. The Client
performs real initialize and bounded discovery of tools, static resources,
resource templates, and prompts before the Server saves the configuration.
Pure deletion can be saved while the device is offline.

MCP environment and HTTP-header values are transported only in private device
configuration frames and require WSS when non-empty. The Client removes every
`OPENOCTOPUS_*` variable from `stdio` MCP child environments. Installing an MCP
trusts it with the Client user's host and network access. Remote MCP headers
additionally require an HTTPS MCP endpoint.

## File conversion

PDF, DOCX, XLSX, PPTX, and downloaded HTML conversion runs in an isolated
helper process. Inputs are limited to 8 MiB, converted output to 128,000
characters, PDF requests to 20 pages, and each conversion to 20 seconds. Linux
also applies a 2 GiB address-space limit and a CPU limit.

The helper receives neither the device token nor arbitrary parent environment
variables. OCR, audio/video, archive recursion, and direct remote PDF/Office
conversion are outside the current support boundary; downloaded HTML is
supported.

## Build a native bundle

Build on each target operating system; cross-building cannot validate POSIX
PTY, Windows ConPTY/DLL packaging, or native runtime behavior.

```bash
python -m pip install -e '.[build]'
python -m PyInstaller --noconfirm --clean openoctopus_client.spec
./dist/openoctopus-client/openoctopus-client version
```

On Windows, the executable is
`dist\openoctopus-client\openoctopus-client.exe`. Distribute the complete
`dist/openoctopus-client/` directory as an archive.

## Development and verification

```bash
python -m pip install -e '.[dev,build]'
python -m ruff check .
python -m mypy --strict src tests
python -m pytest -q
python -m PyInstaller --noconfirm --clean openoctopus_client.spec
```

Run both frozen smoke tests against every native bundle:

```bash
export OO_CLIENT_BIN="$PWD/dist/openoctopus-client/openoctopus-client"
export OO_DOCUMENT_CORPUS="$PWD/../server/tests/fixtures/documents"
python tests/frozen_smoke.py
python tests/frozen_runtime_smoke.py
```

Use the `.exe` path for `OO_CLIENT_BIN` on Windows. CI runs source tests, strict
type checking, frozen smoke tests, and release-shaped packaging natively on
Linux x64, macOS arm64/x64, and Windows x64.
