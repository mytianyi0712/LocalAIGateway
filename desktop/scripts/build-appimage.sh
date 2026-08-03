#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TAURI_DIR="${SCRIPT_DIR}/../src-tauri"
cd "$TAURI_DIR"

# Tauri caches linuxdeploy in target/.tauri when useLocalToolsDir is enabled.
# Keep the tools inside the project so CI and local builds use the same patched
# plugin instead of a machine-wide cache.
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
TOOLS_DIR="${TARGET_DIR}/.tauri"
mkdir -p "$TOOLS_DIR"

case "$(uname -m)" in
  x86_64)
    TOOLS_ARCH="x86_64"
    ;;
  *)
    echo "Unsupported AppImage build architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

fetch_tool() {
  local destination="$1"
  local url="$2"
  if [[ ! -s "$destination" ]]; then
    local temporary="${destination}.tmp"
    curl --fail --location --retry 3 --silent --show-error "$url" --output "$temporary"
    mv -- "$temporary" "$destination"
  fi
  chmod +x "$destination"
}

fetch_tool "$TOOLS_DIR/AppRun-${TOOLS_ARCH}" \
  "https://github.com/tauri-apps/binary-releases/releases/download/apprun-old/AppRun-${TOOLS_ARCH}"
fetch_tool "$TOOLS_DIR/linuxdeploy-${TOOLS_ARCH}.AppImage" \
  "https://github.com/tauri-apps/binary-releases/releases/download/linuxdeploy/linuxdeploy-${TOOLS_ARCH}.AppImage"
fetch_tool "$TOOLS_DIR/linuxdeploy-plugin-appimage.AppImage" \
  "https://github.com/linuxdeploy/linuxdeploy-plugin-appimage/releases/download/continuous/linuxdeploy-plugin-appimage-${TOOLS_ARCH}.AppImage"
fetch_tool "$TOOLS_DIR/linuxdeploy-plugin-gtk.sh" \
  "https://raw.githubusercontent.com/tauri-apps/linuxdeploy-plugin-gtk/master/linuxdeploy-plugin-gtk.sh"
fetch_tool "$TOOLS_DIR/linuxdeploy-plugin-gstreamer.sh" \
  "https://raw.githubusercontent.com/tauri-apps/linuxdeploy-plugin-gstreamer/master/linuxdeploy-plugin-gstreamer.sh"

# The upstream GTK plugin recursively scans /usr/lib. On rolling Linux
# distributions this picks up unrelated libraries (for example OpenShot's
# libgobject.so) and makes the bundle fail on a missing optional dependency.
# GTK's pkg-config libdirs contain the required runtime libraries at the
# directory root, so limit this scan to one level while retaining all normal
# Ubuntu/Debian layouts.
python - "$TOOLS_DIR/linuxdeploy-plugin-gtk.sh" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text()
old = r'find "$directory" \( -type l -o -type f \) -name "$library" -print0'
new = r'find "$directory" -maxdepth 1 \( -type l -o -type f \) -name "$library" -print0'
if old in text:
    path.write_text(text.replace(old, new))
elif new not in text:
    raise SystemExit("linuxdeploy GTK plugin layout changed; refusing an unsafe patch")
PY

# linuxdeploy's bundled strip is older than the ELF tooling on Arch and other
# rolling distributions, so it cannot parse .relr.dyn sections. NO_STRIP is
# supported by linuxdeploy and leaves symbols intact for a larger but valid
# AppImage.
export NO_STRIP="${NO_STRIP:-true}"

# Tauri CLI treats CI as a strict boolean; some harnesses export CI=1.
export CI=true

cargo tauri build --bundles "${BUNDLES:-deb,appimage}"
