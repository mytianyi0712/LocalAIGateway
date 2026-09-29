#!/usr/bin/env bash
# 打包入口：按目标分发到各构建脚本，并把当前版本的产物收集进 releases/。
#
# 用法：packaging/build.sh <windows|appimage|arch|windows-wine|all>
# 前提：需要 cargo 与目标平台对应的工具链（Tauri CLI / makepkg / Wine）；
#       releases/ 中与当前 VERSION 不匹配的旧产物会被清理。
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
RELEASES_DIR="${ROOT_DIR}/releases"

read_project_version() {
  tr -d '\r' < "${ROOT_DIR}/VERSION" | sed -n '1s/^[[:space:]]*//;s/[[:space:]]*$//;p;q'
}

cargo_target_dir() {
  cargo metadata --manifest-path "${ROOT_DIR}/desktop/src-tauri/Cargo.toml" \
    --no-deps --format-version 1 \
    | python -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])'
}
usage() {
  cat <<'USAGE'
Usage: packaging/build.sh <target>

Targets:
  windows        Build a native Windows NSIS installer (Windows host)
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
    windows)
      case "$(uname -s)" in
        MINGW*|MSYS*|CYGWIN*) ;;
        *)
          if [[ "${OS:-}" != Windows_NT ]]; then
            echo "windows target requires a Windows host; use windows-wine on Linux" >&2
            exit 1
          fi
          ;;
      esac
      (
        cd "${ROOT_DIR}/desktop/src-tauri"
        # Tauri 会残留上一次的 NSIS 产物；先清掉，保证 releases/ 只看到当前 VERSION 的成品。
        rm -rf target/release/bundle/nsis
        cargo tauri build --bundles nsis
      )
      ;;
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
    windows)
      files=(
        "${ROOT_DIR}/desktop/src-tauri/target/release/bundle/nsis/"*.exe
      )
      ;;
    appimage)
      local linux_target
      linux_target="$(cargo_target_dir)"
      files=(
        "${linux_target}/release/bundle/deb/"*.deb
        "${linux_target}/release/bundle/appimage/"*.AppImage
      )
      ;;
    arch)
      files=("$(cargo_target_dir)/packages/arch/"*.pkg.tar.*)
      ;;
    windows-wine)
      files=("${ROOT_DIR}/target/packages/windows-wine/"*.exe)
      ;;
  esac
  # target/ 里会残留旧版本的 bundle；只复制文件名带当前 VERSION 的产物
  #（semver 形式与 Arch 下划线形式都算，例如 0.2.1-fix1 与 0.2.1_fix1）。
  local version
  version="$(read_project_version)"
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

# releases/ 是纯产物目录：每次构建都清掉不属于当前 VERSION 的旧产物
#（按 semver 与 Arch 下划线两种形式识别），但保留当前版本的产物，
# 让分步构建（先 appimage、再 arch、后 windows）能累积成完整的一套发布件。
clean_stale_releases() {
  local version
  version="$(read_project_version)"
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
export NO_STRIP=1

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
