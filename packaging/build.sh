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
  # Tauri leaves stale bundles from previous versions in target/; copy only
  # artifacts whose name carries the current VERSION (both the semver form and
  # the Arch underscore form, e.g. 0.2.1-fix1 vs 0.2.1_fix1).
  local version
  version="$(cat "${ROOT_DIR}/VERSION")"
  local patterns=("${version}" "${version//-/_}")
  mkdir -p "${RELEASES_DIR}"
  local copied=0
  local file
  for file in "${files[@]}"; do
    if [[ ! -f "${file}" ]]; then
      continue
    fi
    local name="$(basename "${file}")"
    local matched=0
    local pattern
    for pattern in "${patterns[@]}"; do
      if [[ "${name}" == *[-_]"${pattern}"[-_]* ]]; then
        matched=1
        break
      fi
    done
    if (( matched )); then
      cp -- "${file}" "${RELEASES_DIR}/"
      echo "  -> releases/$(basename "${file}")"
      copied=$((copied + 1))
    fi
  done
  if (( copied == 0 )); then
    echo "warning: no artifacts produced by ${target}" >&2
  fi
}

TARGET="${1:-}"
if [[ -z "$TARGET" || "$TARGET" == "-h" || "$TARGET" == "--help" ]]; then
  usage
  [[ -z "$TARGET" ]] && exit 2 || exit 0
fi

# The releases/ folder is a pure artifact directory. Every build removes
# stale artifacts of previous versions (identified by the current VERSION in
# both the semver form and the Arch underscore form), while keeping artifacts
# of the current version so step-by-step builds (appimage, then arch, then
# windows) accumulate into one complete release set.
clean_stale_releases() {
  local version="$(cat "${ROOT_DIR}/VERSION")"
  local patterns=("${version}" "${version//-/_}")
  mkdir -p "${RELEASES_DIR}"
  local file
  for file in "${RELEASES_DIR}"/*; do
    [[ -e "${file}" ]] || continue
    local name="$(basename "${file}")"
    local matched=0
    local pattern
    for pattern in "${patterns[@]}"; do
      if [[ "${name}" == *[-_]"${pattern}"[-_]* ]]; then
        matched=1
        break
      fi
    done
    if (( ! matched )); then
      rm -f -- "${file}"
      echo "  cleaned releases/$(basename "${file}")"
    fi
  done
}
clean_stale_releases

# ---- Linux AppImage 构建环境修复 ----
# 1. NO_STRIP: linuxdeploy 内嵌的 strip 太旧,无法识别新版 binutils 生成的
#    .relr.dyn section(系统库如 libxml2/libzstd 均带),strip 阶段会批量报错;
#    禁用 strip 后产物功能完整,仅体积略增。(desktop/scripts/build-appimage.sh
#    同样导出;此处兜底覆盖所有子脚本。)
# 2. 代理检测:AppImage 打包首次需要从 GitHub 下载 type2-runtime(缓存在本机
#    后才离线可用);直连不通时自动使用本机常见代理端口(7897/7890/10809/1080),
#    不覆盖用户已显式设置的代理。
export NO_STRIP=true

if [[ -z "${HTTPS_PROXY:-}" && -z "${https_proxy:-}" ]]; then
  for port in 7897 7890 10809 1080; do
    if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
      exec 3>&- 3<&-
      export HTTPS_PROXY="http://127.0.0.1:${port}"
      export HTTP_PROXY="http://127.0.0.1:${port}"
      echo "  detected local proxy on port ${port}: using ${HTTPS_PROXY}"
      break
    fi
  done
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
