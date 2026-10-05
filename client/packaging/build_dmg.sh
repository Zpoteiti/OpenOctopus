#!/usr/bin/env bash
# Build a macOS application DMG from the PyInstaller one-folder bundle.
#
# Usage: build_dmg.sh <path-to-onedir-bundle> <version> [output-dir]
#
# The tray is wrapped in a minimal .app bundle (LSUIElement: no Dock icon,
# it lives in the system status bar), autostart uses the macOS LaunchServices
# entry the tray manages itself, and the DMG carries just the app bundle.
set -euo pipefail

BUNDLE=${1:?usage: build_dmg.sh <onedir-bundle> <version> [output-dir]}
VERSION=${2:?missing version (for example 0.0.1)}
OUT=${3:-dist-dmg}
APP=openoctopus-client

if [ ! -x "$BUNDLE/$APP" ]; then
  echo "bundle is missing the executable $BUNDLE/$APP" >&2
  exit 2
fi

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
APP_DIR="$STAGE/OpenOctopus Client.app"
install -d "$APP_DIR/Contents/MacOS" "$APP_DIR/Contents/Resources"

cat > "$APP_DIR/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key><string>$APP</string>
  <key>CFBundleIdentifier</key><string>dev.openoctopus.client</string>
  <key>CFBundleName</key><string>OpenOctopus Client</string>
  <key>CFBundleShortVersionString</key><string>$VERSION</string>
  <key>CFBundleVersion</key><string>$VERSION</string>
  <key>LSUIElement</key><true/>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST

mv "$BUNDLE" "$APP_DIR/Contents/MacOS/$APP"

DMG="$OUT/OpenOctopusClient-$VERSION.dmg"
mkdir -p "$OUT"
hdiutil create -volname "OpenOctopus Client" -srcfolder "$STAGE" -ov -format UDZO "$DMG"
