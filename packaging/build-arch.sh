#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TAURI_DIR="${ROOT_DIR}/desktop/src-tauri"
TARGET_DIR="$(cargo metadata --manifest-path "${TAURI_DIR}/Cargo.toml" --no-deps --format-version 1 | python -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
BUILD_DIR="${TARGET_DIR}/packages/arch"

command -v makepkg >/dev/null || {
  echo "makepkg is required; install the Arch base-devel toolchain first." >&2
  exit 1
}
command -v cargo >/dev/null || {
  echo "cargo is required; install the Rust toolchain first." >&2
  exit 1
}

VERSION="$(python - "${TAURI_DIR}/Cargo.toml" <<'PY'
from pathlib import Path
import re
import sys
text = Path(sys.argv[1]).read_text()
match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
if not match:
    raise SystemExit("could not determine Cargo package version")
print(match.group(1))
PY
)"
# Arch pkgver forbids hyphens (allowed: alphanumerics, ., _, +); map the
# semver prerelease separator to an underscore, e.g. 0.2.0-fix1 -> 0.2.0_fix1.
VERSION="${VERSION//-/_}"

chmod -R u+rwX "${BUILD_DIR}" 2>/dev/null || true
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
sed "s/^pkgver=.*/pkgver=${VERSION}/" \
  "${ROOT_DIR}/packaging/arch/PKGBUILD" | tr -d '\r' > "${BUILD_DIR}/PKGBUILD"
ln -s "${ROOT_DIR}" "${BUILD_DIR}/repo"

read -r -a MAKEPKG_FLAGS <<< "${MAKEPKG_FLAGS:---clean --force --syncdeps}"
(
  cd "${BUILD_DIR}"
  makepkg "${MAKEPKG_FLAGS[@]}"
)

printf 'Arch package(s):\n'
find "${BUILD_DIR}" -maxdepth 1 -type f -name '*.pkg.tar.*' -print
