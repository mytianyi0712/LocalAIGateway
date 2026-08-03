#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASES_DIR="${ROOT_DIR}/releases"

usage() {
  cat <<'USAGE'
Usage: packaging/build.sh <target>

Targets:
  appimage       Build Linux AppImage and Debian package on the host
  arch           Build an Arch Linux .pkg.tar.zst package
  windows-wine   Cross-build a Windows NSIS installer through Wine
  all            Run appimage, arch, and windows-wine builds

Every target copies its artifacts into the repository-root releases/ folder.
USAGE
}

run_target() {
  local target="$1"
  shift
  case "$target" in
    appimage) "${ROOT_DIR}/desktop/scripts/build-appimage.sh" "$@" ;;
    arch) "${ROOT_DIR}/packaging/build-arch.sh" "$@" ;;
    windows-wine) "${ROOT_DIR}/packaging/build-windows-wine.sh" "$@" ;;
    *)
      echo "Unknown packaging target: ${target}" >&2
      usage >&2
      exit 2
      ;;
  esac
}

collect_artifacts() {
  local target="$1"
  local files=()
  case "$target" in
    appimage)
      files=(
        "${ROOT_DIR}/desktop/src-tauri/target/release/bundle/deb/"*.deb
        "${ROOT_DIR}/desktop/src-tauri/target/release/bundle/appimage/"*.AppImage
      )
      ;;
    arch)
      files=("${ROOT_DIR}/desktop/src-tauri/target/packages/arch/"*.pkg.tar.*)
      ;;
    windows-wine)
      files=("${ROOT_DIR}/target/packages/windows-wine/"*.exe)
      ;;
  esac
  mkdir -p "${RELEASES_DIR}"
  local copied=0
  local file
  for file in "${files[@]}"; do
    if [[ -f "${file}" ]]; then
      cp -- "${file}" "${RELEASES_DIR}/"
      echo "  -> releases/$(basename "${file}")"
      copied=$((copied + 1))
    fi
  done
  if ((copied == 0)); then
    echo "warning: no artifacts produced by ${target}" >&2
  fi
}

TARGET="${1:-}"
if [[ -z "$TARGET" || "$TARGET" == "-h" || "$TARGET" == "--help" ]]; then
  usage
  [[ -z "$TARGET" ]] && exit 2 || exit 0
fi

if [[ "$TARGET" == "all" ]]; then
  run_target appimage
  collect_artifacts appimage
  run_target arch
  collect_artifacts arch
  run_target windows-wine
  collect_artifacts windows-wine
else
  shift
  run_target "$TARGET" "$@"
  collect_artifacts "$TARGET"
fi
