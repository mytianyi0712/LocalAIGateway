#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage: packaging/build.sh <target>

Targets:
  appimage       Build Linux AppImage and Debian package on the host
  arch           Build an Arch Linux .pkg.tar.zst package
  ubuntu         Build Ubuntu 22.04 and 24.04 .deb packages in Docker
  windows-wine   Cross-build a Windows NSIS installer through Wine
  all            Run appimage, arch, ubuntu, and windows-wine builds
USAGE
}

run_target() {
  local target="$1"
  shift
  case "$target" in
    appimage) "${ROOT_DIR}/desktop/scripts/build-appimage.sh" "$@" ;;
    arch) "${ROOT_DIR}/packaging/build-arch.sh" "$@" ;;
    ubuntu) "${ROOT_DIR}/packaging/build-ubuntu.sh" "$@" ;;
    windows-wine) "${ROOT_DIR}/packaging/build-windows-wine.sh" "$@" ;;
    *)
      echo "Unknown packaging target: ${target}" >&2
      usage >&2
      exit 2
      ;;
  esac
}

TARGET="${1:-}"
if [[ -z "$TARGET" || "$TARGET" == "-h" || "$TARGET" == "--help" ]]; then
  usage
  [[ -z "$TARGET" ]] && exit 2 || exit 0
fi

if [[ "$TARGET" == "all" ]]; then
  run_target appimage
  run_target arch
  run_target ubuntu
  run_target windows-wine
else
  shift
  run_target "$TARGET" "$@"
fi
