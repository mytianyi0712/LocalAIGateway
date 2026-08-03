#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TAURI_DIR="${ROOT_DIR}/desktop/src-tauri"
TARGET="x86_64-pc-windows-gnu"
TARGET_DIR="$(cargo metadata --manifest-path "${TAURI_DIR}/Cargo.toml" --no-deps --format-version 1 | python -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
TOOLS_DIR="${TARGET_DIR}/windows-tools"
WINEPREFIX="${WINEPREFIX:-${TOOLS_DIR}/wineprefix}"
NSIS_VERSION="${NSIS_VERSION:-3.10}"
NSIS_EXE="${NSIS_EXE:-}"

command -v wine >/dev/null || { echo "Wine is required" >&2; exit 1; }
command -v x86_64-w64-mingw32-gcc >/dev/null || {
  echo "MinGW-w64 is required; install mingw-w64-gcc first." >&2
  exit 1
}
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v unzip >/dev/null || { echo "unzip is required" >&2; exit 1; }
RUSTUP_BIN="$(command -v rustup || true)"
if [[ -z "${RUSTUP_BIN}" && -x "${CARGO_HOME:-${HOME}/.cargo}/bin/rustup" ]]; then
  RUSTUP_BIN="${CARGO_HOME:-${HOME}/.cargo}/bin/rustup"
fi
[[ -n "${RUSTUP_BIN}" ]] || {
  echo "rustup is required to install ${TARGET}; install rustup first." >&2
  exit 1
}
export PATH="$(dirname "${RUSTUP_BIN}"):${PATH}"
command -v cargo >/dev/null || { echo "cargo is required" >&2; exit 1; }
"${RUSTUP_BIN}" target add "${TARGET}"
cargo tauri --version >/dev/null || {
  echo "Tauri CLI is required; run: cargo install --locked tauri-cli --version '^2'" >&2
  exit 1
}
mkdir -p "${TOOLS_DIR}/bin"

if [[ -z "${NSIS_EXE}" ]]; then
  NSIS_ROOT="${TOOLS_DIR}/nsis-${NSIS_VERSION}"
  NSIS_ARCHIVE="${TOOLS_DIR}/nsis-${NSIS_VERSION}.zip"
  NSIS_EXE="$(find "${NSIS_ROOT}" -type f -iname makensis.exe -print -quit 2>/dev/null || true)"
  if [[ -z "${NSIS_EXE}" ]]; then
    if [[ ! -s "${NSIS_ARCHIVE}" ]]; then
      curl --fail --location --retry 3 --silent --show-error \
        "https://prdownloads.sourceforge.net/nsis/nsis-${NSIS_VERSION}.zip" \
        --output "${NSIS_ARCHIVE}.tmp"
      mv "${NSIS_ARCHIVE}.tmp" "${NSIS_ARCHIVE}"
    fi
    rm -rf "${NSIS_ROOT}"
    mkdir -p "${NSIS_ROOT}"
    unzip -q "${NSIS_ARCHIVE}" -d "${NSIS_ROOT}"
    NSIS_EXE="$(find "${NSIS_ROOT}" -type f -iname makensis.exe -print -quit)"
  fi
fi

[[ -f "${NSIS_EXE}" ]] || {
  echo "Could not locate makensis.exe; set NSIS_EXE=/path/to/makensis.exe" >&2
  exit 1
}

export WINEPREFIX
wineboot -u >/dev/null 2>&1 || true

cat > "${TOOLS_DIR}/bin/makensis" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
args=()
for arg in "$@"; do
  if [[ "$arg" == /* && -f "$arg" && "$arg" == *.nsi ]]; then
    python - "$arg" <<'PYTHON'
import os
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8-sig")
windows_root = os.environ["NSIS_WINDOWS_ROOT"]
text = text.replace(os.environ["NSIS_UNIX_ROOT"], windows_root)
pattern = re.compile(re.escape(windows_root) + r'[^"\r\n]*')
text = pattern.sub(lambda match: match.group(0).replace("/", "\\"), text)
path.write_text(text, encoding="utf-8-sig")
PYTHON
  fi
  if [[ "$arg" == /* && -e "$arg" ]]; then
    arg="$(winepath -w "$arg")"
  fi
  args+=("$arg")
done
exec wine "${NSIS_EXE:?NSIS_EXE is not set}" "${args[@]}"
WRAPPER
chmod +x "${TOOLS_DIR}/bin/makensis"

export NSIS_EXE
export NSIS_UNIX_ROOT="${ROOT_DIR}"
export NSIS_WINDOWS_ROOT="$(winepath -w "${ROOT_DIR}")"
export PATH="${TOOLS_DIR}/bin:${PATH}"
export CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER="x86_64-w64-mingw32-gcc"
export CC_x86_64_pc_windows_gnu="x86_64-w64-mingw32-gcc"
export CXX_x86_64_pc_windows_gnu="x86_64-w64-mingw32-g++"
export AR_x86_64_pc_windows_gnu="x86_64-w64-mingw32-ar"
export CI=true

cd "${TAURI_DIR}"
cargo clean --release --target "${TARGET}" --package local-ai-gateway
cargo tauri build --target "${TARGET}" --bundles nsis

OUTPUT_DIR="${ROOT_DIR}/target/packages/windows-wine"
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"
find "${TARGET_DIR}/${TARGET}/release/bundle/nsis" -maxdepth 1 -type f -iname '*.exe' \
  -exec cp {} "${OUTPUT_DIR}/" \;

printf 'Wine Windows package(s):\n'
find "${OUTPUT_DIR}" -maxdepth 1 -type f -iname '*.exe' -print
