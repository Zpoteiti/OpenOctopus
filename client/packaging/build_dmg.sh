#!/usr/bin/env bash
# Package the PyInstaller .app, preserving its signed framework layout.
# Usage: build_dmg.sh <app-bundle> <version> [output-dir]
set -euo pipefail
BUNDLE=${1:?missing app bundle}
VERSION=${2:?missing version}
OUT=${3:-dist-dmg}
APP=openoctopus-client
if [ ! -f "$BUNDLE/Contents/MacOS/$APP" ] || [ ! -x "$BUNDLE/Contents/MacOS/$APP" ]; then
  echo "app bundle is missing Contents/MacOS/$APP" >&2
  exit 2
fi
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
cp -a "$BUNDLE" "$STAGE/OpenOctopus Client.app"
ln -s /Applications "$STAGE/Applications"
ARCH=$(uname -m)
[ "$ARCH" != x86_64 ] || ARCH=x64
mkdir -p "$OUT"
hdiutil create -volname "OpenOctopus Client" -srcfolder "$STAGE" -ov -format UDZO \
  "$OUT/OpenOctopusClient-$VERSION-$ARCH.dmg"
