# OpenOctopus Client

The OpenOctopus Client pairs one user-owned computer with an OpenOctopus
Server over Protocol v3. It runs local file tools, transfers, `web_fetch`,
pipe/PTY command sessions, and Device MCP services on behalf of that user's
agent.

The Client ships as a tray application. It owns the Server address and the
device token; chat, Workspace browsing, and device management live in the
Server's web UI, not in the Client. There is no user-facing CLI anymore: the
program takes no arguments and starts the tray.

The Client supports Linux x64, macOS arm64/x64, and Windows x64. Running from
source requires Python 3.12.

> The Client is an alpha/demo release. Commands and MCP services run with the
> permissions of the operating-system user that starts it. The Workspace
> restriction is a path guard, not an operating-system sandbox.

All source installation, build, and test commands below run from the
repository's `client/` directory.

## Install

Download the installer for the target computer from
[GitHub Releases](https://github.com/Zpoteiti/OpenOctopus/releases):

| Platform | Artifact | Notes |
| --- | --- | --- |
| Linux x64 | `openoctopus-client_<version>_amd64.deb` | Installs to `/opt/OpenOctopus` with a `/usr/bin/openoctopus-client` launcher and a desktop entry |
| Windows x64 | `OpenOctopusClient-Setup-<version>-per-user.exe` | Per-user installer (no admin rights); installs under `%LOCALAPPDATA%\Programs\OpenOctopusClient` |
| macOS | `OpenOctopusClient-<version>-<arch>.dmg` | Drag the `OpenOctopus Client.app` (LSUIElement; lives in the menu bar) into Applications |

Installers are unsigned. Launch the tray from the desktop entry, Start Menu,
or `openoctopus-client`.

## Pair the computer

1. In the OpenOctopus browser UI, open **Devices** and create a device. Copy
   the `openoctopus_dev_...` token; it is shown only once, and losing it
   requires token regeneration.
2. Start the tray. Open its menu and choose **连接设置** (connection
   settings), enter the Server address and the device token, and click
   **保存并连接**.

The tray starts the execution core as a separate process only after the
settings are saved, and the status line in the tray menu then reports
未配置 / 连接中 / 在线 / 重连中 / 已停止 / 需要处理. The Server address must
be an `http://` or `https://` origin without a path, query, fragment, or
credentials; the Client derives `/ws/device` and uses WSS for an HTTPS
Server.

The device token is stored in the operating system's credential store
(Windows Credential Manager, macOS Keychain, and Secret Service/libsecret on
Linux). Only the Server address and a reference into that store are kept in
the settings file (`config.json` under the per-user config directory). The
token never appears on a command line, in an environment variable, in a log,
or in any event the core produces; it crosses the private pipe to the core
in memory only.

The Server supplies the Workspace path and device policies configured in the
browser. Changing that configuration takes effect through the existing device
configuration handshake.

Closing the settings window only hides it; the tray keeps running with the
saved configuration. **打开网页** opens the Server web UI in the default
browser. **停止客户端** stops the core; **登录后自动启动** toggles autostart
(the desktop autostart entry, the `HKCU` Run key, or the macOS login item,
respectively). Autostart is off by default and changes apply at the next login. Starting a second instance focuses the running tray instead of
launching a second one.

## Run from source

```bash
python3.12 -m venv .venv
. .venv/bin/activate           # Windows: .venv\Scripts\activate
python -m pip install -e .
python -m openoctopus_client   # starts the same tray
```

On Linux the tray needs a system tray (StatusNotifierItem/AppIndicator) or it
shows the settings window and stops background connections. Retry tray detection
or quit from that window. A desktop session is required.

## Connection lifecycle

An unreachable Server retries with bounded exponential backoff, including on
first launch. Online is reported only after the Protocol v3 hello/config
acknowledgement. Authentication rejection, connection replacement, and invalid
Server configuration stop the core and leave the error visible in the tray.
A confirmed clean stop permits a fresh start. An abnormal exit or unconfirmed
cleanup requires attention and blocks replacement of the core in that session.

Ordinary Server disconnects do not stop running exec sessions or MCP runtimes.
Calls whose outcome became ambiguous are not replayed automatically. Stopping
the core, device deletion, or token rotation stops Client-owned child work.
If the tray itself exits, closing the pipe ends the core's ownership and the
core performs its full stop flow before exiting.

## Workspace and command policy

The Server owns and sends these settings during the device handshake:

- `workspace_path`
- `restrict_to_workspace`
- `ssrf_denylist`
- `shell_timeout_max`
- `env_allowlist`

A leading `~` expands once against the Client operating-system user's home.
Relative paths resolve under the Workspace root. Native absolute paths use
POSIX syntax on Linux/macOS and drive or UNC syntax on Windows.

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

## Program modes

The installed `openoctopus-client` starts the tray. Its sibling
`openoctopus-core` provides private modes for the tray, helpers, and CI; both
executables share the packaged dependencies:

```text
openoctopus-client                 # the tray (the only user-facing mode)
_core-run                          # the execution core fed by the private pipe
_conversion-worker                 # one isolated document conversion
_pty-worker CONTROL_FD EVENTS_FD   # one pipe/PTY session worker
_exec-backend-smoke                # frozen pipe/PTY backend smoke
_mcp-stdio-smoke EXECUTABLE FIXTURE
_spike-convert PATH [--pages RANGE]
_version                           # print the bundled client version
```

Internal modes are not a public interface; the tray launches them, and tests
may launch `_core-run` directly by writing the startup configuration
(`{"type": "startup-config", ...}`) as one JSON line on stdin.

## Build installers

Build on each target operating system; cross-building cannot validate POSIX
PTY, Windows ConPTY/DLL packaging, or native runtime behavior.

```bash
python -m pip install -e '.[build]'
python -m PyInstaller --noconfirm --clean openoctopus_client.spec
```

Then wrap the one-folder bundle:

```bash
# Linux x64 -> openoctopus-client_<version>_amd64.deb in dist-deb/
packaging/build_deb.sh dist/openoctopus-client 0.0.1

# Windows x64 -> per-user installer in dist-installer/ (requires NSIS)
python packaging/build_windows_installer.py dist/openoctopus-client 0.0.1

# macOS -> OpenOctopusClient-<version>-<arch>.dmg in dist-dmg/ (requires hdiutil)
packaging/build_dmg.sh "dist/OpenOctopus Client.app" 0.0.1
```

Installers are unsigned, per-user/platform-native, and not code-reviewed by
any OS vendor.

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
export OO_CLIENT_BIN="$PWD/dist/openoctopus-client/openoctopus-core"
export OO_DOCUMENT_CORPUS="$PWD/../server/tests/fixtures/documents"
python tests/frozen_smoke.py
python tests/frozen_runtime_smoke.py
```

Use the `.exe` path for `OO_CLIENT_BIN` on Windows. The Server-side real E2E
suites (`PY5_REAL_E2E=1 PY6_REAL_E2E=1 PY7_REAL_E2E=1 PY8C_REAL_E2E=1` in
`server/`) accept the same `OO_CLIENT_BIN` and drive the frozen core through
the stdin startup-configuration contract. CI runs source tests, strict type
checking, frozen smoke tests, and installer packaging natively on Linux x64,
macOS arm64/x64, and Windows x64.
