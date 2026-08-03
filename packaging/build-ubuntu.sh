#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_ROOT="${ROOT_DIR}/target/packages/ubuntu"

usage() {
  cat <<'USAGE'
Usage: packaging/build-ubuntu.sh [22.04] [24.04]

Build Ubuntu-series .deb packages in disposable Docker containers.
Set UBUNTU_NATIVE=1 to build only on the current host without Docker.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if (($# == 0)); then
  SERIES=(22.04 24.04)
else
  SERIES=("$@")
fi

for series in "${SERIES[@]}"; do
  case "$series" in
    22.04|24.04) ;;
    *)
      echo "Unsupported Ubuntu series: ${series}; supported: 22.04 24.04" >&2
      exit 2
      ;;
  esac
done

build_native() {
  local series="$1"
  command -v cargo >/dev/null || { echo "cargo is required" >&2; exit 1; }
  cargo tauri --version >/dev/null || { echo "Tauri CLI is required" >&2; exit 1; }
  mkdir -p "${OUTPUT_ROOT}/${series}"
  (
    cd "${ROOT_DIR}/desktop/src-tauri"
    CI=true BUNDLES=deb cargo tauri build --bundles deb
  )
  cp "${ROOT_DIR}/desktop/src-tauri/target/release/bundle/deb/"*.deb \
    "${OUTPUT_ROOT}/${series}/local-ai-gateway-${series}-amd64.deb"
}

build_container() {
  local series="$1"
  command -v docker >/dev/null || {
    echo "Docker is required for Ubuntu ${series}; install Docker or set UBUNTU_NATIVE=1." >&2
    exit 1
  }
  docker info >/dev/null 2>&1 || {
    echo "Docker daemon is unavailable; start Docker or set UBUNTU_NATIVE=1." >&2
    exit 1
  }

  mkdir -p "${OUTPUT_ROOT}/${series}"
  docker run --rm \
    --interactive \
    --env HOST_UID="$(id -u)" \
    --env HOST_GID="$(id -g)" \
    --volume "${ROOT_DIR}:/workspace" \
    --workdir /workspace \
    "ubuntu:${series}" \
    bash -s -- "${series}" <<'INNER'
set -euo pipefail
series="$1"
export DEBIAN_FRONTEND=noninteractive
export RUSTUP_HOME=/tmp/rustup
export CARGO_HOME=/tmp/cargo
export CARGO_TARGET_DIR=/tmp/local-ai-gateway-target

apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates curl build-essential pkg-config python3 file patchelf \
  libssl-dev libgtk-3-dev libwebkit2gtk-4.1-dev \
  libayatana-appindicator3-dev librsvg2-dev

curl --proto '=https' --tlsv1.2 --silent --show-error --fail \
  https://sh.rustup.rs | sh -s -- -y --profile minimal
source /tmp/cargo/env
cargo install --locked tauri-cli --version '^2'

cd /workspace/desktop/src-tauri
CI=true cargo tauri build --bundles deb
mkdir -p "/workspace/target/packages/ubuntu/${series}"
cp /tmp/local-ai-gateway-target/release/bundle/deb/*.deb \
  "/workspace/target/packages/ubuntu/${series}/local-ai-gateway-${series}-amd64.deb"
chown -R "${HOST_UID}:${HOST_GID}" "/workspace/target/packages/ubuntu/${series}"
INNER
}

for series in "${SERIES[@]}"; do
  if [[ "${UBUNTU_NATIVE:-0}" == "1" ]]; then
    build_native "$series"
  else
    build_container "$series"
  fi
done

printf 'Ubuntu package(s):\n'
find "${OUTPUT_ROOT}" -type f -name '*.deb' -print
