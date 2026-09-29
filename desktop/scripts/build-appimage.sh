#!/usr/bin/env bash
# 构建 Linux AppImage 与 deb：准备 linuxdeploy 工具链（含 GTK/gstreamer 插件）、
# 收窄 GTK 插件的库扫描深度，最后调用 cargo tauri build。
#
# 用法：desktop/scripts/build-appimage.sh（BUNDLES 可覆盖默认的 deb,appimage）
# 前提：需要 cargo、Tauri CLI 与 curl，首次运行会联网下载 linuxdeploy；
#       WSL 下会设置 APPIMAGE_EXTRACT_AND_RUN=1，并在无法 chmod +x 时改用缓存副本。
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TAURI_DIR="${SCRIPT_DIR}/../src-tauri"
cd "$TAURI_DIR"

# 启用 useLocalToolsDir 时，Tauri 把 linuxdeploy 缓存在 $CARGO_TARGET_DIR/.tauri。
# WSL 上仓库位于 drvfs（/mnt/d）无法 chmod +x，因此 Makefile 会把 CARGO_TARGET_DIR
# 指向 Linux 原生目录作为缓存。
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

# WSL 通常没有 FUSE，linuxdeploy 的 AppImage 必须解包后直接运行。
if [[ -n "${WSL_DISTRO_NAME:-}" || -e /proc/sys/fs/binfmt_misc/WSLInterop ]]; then
  export APPIMAGE_EXTRACT_AND_RUN="${APPIMAGE_EXTRACT_AND_RUN:-1}"
fi

ensure_executable() {
  local path="$1"
  if chmod +x "$path" 2>/dev/null && [[ -x "$path" ]]; then
    return 0
  fi
  local cache="${HOME}/.cache/local-ai-gateway/tauri-tools"
  mkdir -p "$cache"
  local copy="${cache}/$(basename "$path")"
  cp -f "$path" "$copy"
  chmod +x "$copy"
  ln -sfn "$copy" "$path"
}

fetch_tool() {
  local destination="$1"
  local url="$2"
  if [[ ! -s "$destination" ]]; then
    local temporary="${destination}.tmp"
    curl --fail --location --retry 3 --silent --show-error "$url" --output "$temporary"
    mv -- "$temporary" "$destination"
  fi
  ensure_executable "$destination"
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

# 上游 GTK 插件会递归扫描 /usr/lib：在滚动发行版上会误收无关库（例如 OpenShot 的
# libgobject.so），使打包产物因缺少可选依赖而失败。GTK 的 pkg-config libdir 把所需
# 运行库放在目录根下，因此把扫描限制为一层深度，同时兼容常见的 Ubuntu/Debian 布局。
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

# linuxdeploy 自带的 strip 比 Arch 等滚动发行版的 ELF 工具链更旧，无法解析
# .relr.dyn 段。NO_STRIP 由 linuxdeploy 支持，保留符号后 AppImage 体积更大但依然有效。
export NO_STRIP="${NO_STRIP:-1}"

# Tauri CLI 把 CI 当作严格布尔值，而某些 CI 环境会导出 CI=1。
export CI=true

# Tauri 不会删除 target/ 里旧版本的 bundle 目录，先手动清掉，
# 保证 release 目录里只出现当前版本的产物。
# 注意：此处 cwd 是 src-tauri/，Tauri 把 bundle 写到 target/release/bundle/。
rm -rf target/release/bundle/deb target/release/bundle/appimage
cargo tauri build --bundles "${BUNDLES:-deb,appimage}"
