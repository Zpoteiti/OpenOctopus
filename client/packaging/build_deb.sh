#!/usr/bin/env bash
# Assemble a Debian package from a built PyInstaller one-folder bundle.
#
# Usage: build_deb.sh <path-to-onedir-bundle> <version> [output-dir]
#
# The tray binary is installed under /opt and exposed through
# /usr/bin/openoctopus-client; the autostart entry ships as a standard
# desktop file that the tray copies into ~/.config/autostart on demand.
set -euo pipefail

BUNDLE=${1:?usage: build_deb.sh <onedir-bundle> <version> [output-dir]}
VERSION=${2:?missing version (for example 0.0.1)}
OUT=${3:-dist-deb}
APP=openoctopus-client

if [ ! -x "$BUNDLE/$APP" ]; then
  echo "bundle is missing the executable $BUNDLE/$APP" >&2
  exit 2
fi
if ! printf '%s' "$VERSION" | grep -Eq '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'; then
  echo "version must be MAJOR.MINOR.PATCH without epoch or revision" >&2
  exit 2
fi

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

install -d "$STAGE/opt/OpenOctopus" \
  "$STAGE/usr/bin" \
  "$STAGE/usr/share/applications" \
  "$STAGE/usr/share/icons" \
  "$STAGE/DEBIAN"

cp -a "$BUNDLE" "$STAGE/opt/OpenOctopus/$APP"

cat > "$STAGE/usr/bin/$APP" <<'LAUNCHER'
#!/bin/sh
exec /opt/OpenOctopus/openoctopus-client/openoctopus-client "$@"
LAUNCHER
chmod 755 "$STAGE/usr/bin/$APP"

cat > "$STAGE/usr/share/applications/$APP.desktop" <<'DESKTOP'
[Desktop Entry]
Type=Application
Version=1.0
Name=OpenOctopus Client
Comment=OpenOctopus device tray client
Exec=openoctopus-client
Icon=openoctopus-client
Terminal=false
Categories=Utility;TrayIcon;
StartupNotify=false
X-GNOME-Autostart-enabled=false
DESKTOP

ICON_SRC="${OO_CLIENT_ICON:-$(cd "$(dirname "$0")" && pwd)/openoctopus-client.svg}"
if [ -f "$ICON_SRC" ]; then
  install -d "$STAGE/usr/share/icons/hicolor/scalable/apps"
  install -m 644 "$ICON_SRC" "$STAGE/usr/share/icons/hicolor/scalable/apps/$APP.svg"
fi

cat > "$STAGE/DEBIAN/control" <<CONTROL
Package: $APP
Version: $VERSION
Section: utils
Priority: optional
Architecture: amd64
Maintainer: OpenOctopus Contributors <openoctopus@example.invalid>
Depends: libxcb-cursor0, libxcb-xinerama0, libxcb-icccm4, libxcb-image0, libxcb-keysyms1, libxcb-render-util0, libxcb-shape0, libxkbcommon-x11-0, libdbus-1-3, libglib2.0-0, libegl1, libgl1, libxkbcommon0, zlib1g
Recommends: xdg-utils, gnome-keyring
Description: OpenOctopus device tray client
 Tray application that owns the OpenOctopus Server address and device
 token settings, supervises the private execution core, and opens the
 web workspace in the default browser.
CONTROL

mkdir -p "$OUT"
dpkg-deb --root-owner-group --build "$STAGE" "$OUT/${APP}_${VERSION}_amd64.deb"
dpkg-deb --info "$OUT/${APP}_${VERSION}_amd64.deb" >/dev/null
