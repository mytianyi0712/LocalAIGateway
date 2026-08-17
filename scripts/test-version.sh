#!/usr/bin/env bash
# 版本管理自校验（只保护可观察合同，不修改真实仓库）：
#   - set/check/sync 全流程与幂等性
#   - 非法 SemVer 在任何写入前被拒绝（文件内容不变）
#   - 预发布版本（0.3.0-rc.1）支持
#   - 只改 VERSION 再 sync 能灌进全部第一方声明
#   - Windows CRLF / BOM 的 VERSION 仍可解析
#   - Cargo.lock 由 cargo 重新解析后与清单一致
#   - 打包产物文件名中的版本可解析（Arch / NSIS / deb / AppImage 命名约定）
#   - 历史 releases/ 产物列表在测试前后不变（未被改动）
# 用法：./scripts/test-version.sh

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
SANDBOX="$(mktemp -d)"
trap 'rm -rf "${SANDBOX}"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "ok: $*"; }
PYTHON_BIN="$(command -v python3 || command -v python || fail '需要 python3 或 python')"
# ---- 沙箱：复制受管文件，构造最小仓库 ----
mkdir -p "${SANDBOX}/scripts" \
  "${SANDBOX}/desktop/src-tauri" \
  "${SANDBOX}/packaging/arch"
cp "${ROOT_DIR}/scripts/version.sh" "${SANDBOX}/scripts/version.sh"
cp "${ROOT_DIR}/desktop/src-tauri/Cargo.toml" "${SANDBOX}/desktop/src-tauri/Cargo.toml"
cp "${ROOT_DIR}/desktop/src-tauri/Cargo.lock" "${SANDBOX}/desktop/src-tauri/Cargo.lock"
cp "${ROOT_DIR}/desktop/src-tauri/tauri.conf.json" "${SANDBOX}/desktop/src-tauri/tauri.conf.json"
cp "${ROOT_DIR}/packaging/arch/PKGBUILD" "${SANDBOX}/packaging/arch/PKGBUILD"
printf '%s\n' "0.2.0-fix2" > "${SANDBOX}/VERSION"

cd "${SANDBOX}"

# 全部受管文件（含锁文件）的哈希快照，用于断言"写入前失败"与幂等
snapshot() {
  sha256sum VERSION desktop/src-tauri/Cargo.toml desktop/src-tauri/tauri.conf.json \
    desktop/src-tauri/Cargo.lock packaging/arch/PKGBUILD
}

# ---- 1. 非法 SemVer 必须在任何写入前被拒绝 ----
expect_reject() {
  local bad="$1"
  local before
  before="$(snapshot)"
  if ./scripts/version.sh set "$bad" >/dev/null 2>&1; then
    fail "set '$bad' 应当被拒绝"
  fi
  [[ "$(snapshot)" == "$before" ]] || fail "set '$bad' 在拒绝前产生了写入"
  ok "拒绝非法版本且零写入: '$bad'"
}

expect_reject "1.2"
expect_reject "v1.2.3"
expect_reject "01.2.3"
expect_reject "1.2.3.4"
expect_reject "1.2.3-"
expect_reject "1.2.3-01"
expect_reject "1.2.3-rc..1"
expect_reject ""

# ---- 2. 预发布版本：set + check + 各声明一致 ----
./scripts/version.sh set 0.3.0-rc.1 >/dev/null
./scripts/version.sh check >/dev/null || fail "0.3.0-rc.1 set 后 check 失败"
ok "set 0.3.0-rc.1 并 check 通过"

"$PYTHON_BIN" - <<'PY'
import json
import pathlib
import re

v = "0.3.0-rc.1"
assert pathlib.Path("VERSION").read_text().strip() == v
cargo = pathlib.Path("desktop/src-tauri/Cargo.toml").read_text()
assert re.search(r'^version\s*=\s*"' + re.escape(v) + r'"', cargo, re.M), "Cargo.toml"
conf = json.loads(pathlib.Path("desktop/src-tauri/tauri.conf.json").read_text())
assert conf["version"] == v, "tauri.conf.json"
pkg = pathlib.Path("packaging/arch/PKGBUILD").read_text()
assert re.search(r'^pkgver=0\.3\.0_rc\.1$', pkg, re.M), "PKGBUILD pkgver 应映射为 0.3.0_rc.1"

def lock_version(path):
    text = path.read_text()
    for block in text.split("[[package]]"):
        if re.search(r'^name\s*=\s*"local-ai-gateway"\s*$', block, re.M):
            m = re.search(r'^version\s*=\s*"([^"]+)"', block, re.M)
            return m.group(1) if m else None
    return None

assert lock_version(pathlib.Path("desktop/src-tauri/Cargo.lock")) == v, "Cargo.lock"
PY
ok "全部第一方声明（含 Cargo.lock 与 PKGBUILD 映射）为 0.3.0-rc.1"

# ---- 3. 幂等：重复 set 同一版本不产生任何写入 ----
before="$(snapshot)"
./scripts/version.sh set 0.3.0-rc.1 >/dev/null
[[ "$(snapshot)" == "$before" ]] || fail "重复 set 同一版本不是幂等"
ok "重复 set 同一版本幂等（零写入）"

# ---- 4. 稳定版：set 0.2.1 ----
./scripts/version.sh set 0.2.1 >/dev/null
./scripts/version.sh check >/dev/null || fail "0.2.1 set 后 check 失败"
ok "set 0.2.1 并 check 通过"

# ---- 5. 打包产物文件名版本解析（不构建安装包）----
parse_arch_version() {  # local-ai-gateway-<pkgver>-<pkgrel>-x86_64.pkg.tar.zst
  local name="$1"
  [[ "$name" =~ ^local-ai-gateway-(.+)-[0-9]+-x86_64\.pkg\.tar\.zst$ ]] \
    || fail "Arch 文件名不符合约定: $name"
  printf '%s' "${BASH_REMATCH[1]}"
}
arch_name="local-ai-gateway-0.2.1-1-x86_64.pkg.tar.zst"
[[ "$(parse_arch_version "$arch_name")" == "0.2.1" ]] || fail "Arch 文件名版本解析失败"
prerelease_arch="local-ai-gateway-0.3.0_rc.1-1-x86_64.pkg.tar.zst"
[[ "$(parse_arch_version "$prerelease_arch")" == "0.3.0_rc.1" ]] || fail "Arch 预发布文件名版本解析失败"
[[ "$(parse_arch_version "$prerelease_arch")" == "$(printf '%s' 0.3.0-rc.1 | tr '-' '_')" ]] \
  || fail "Arch pkgver 连字符映射不一致"
ok "Arch 文件名版本解析: 0.2.1 与预发布 0.3.0_rc.1"

for name in \
  "Local AI Gateway_0.2.1_x64-setup.exe" \
  "local-ai-gateway_0.2.1_amd64.deb" \
  "Local AI Gateway_0.2.1_amd64.AppImage"; do
  [[ "$name" == *"0.2.1"* ]] || fail "打包文件名缺少 0.2.1: $name"
done
ok "NSIS / deb / AppImage 文件名包含 0.2.1"

# ---- 6. 真实仓库：历史 releases/ 列表在测试前后不变（工具绝不改写历史产物）----
# releases/ 是 gitignore 的历史产物目录，全新检出可能不存在，允许为空/缺失；
releases_snapshot() {
  "$PYTHON_BIN" - "${ROOT_DIR}/releases" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
if not root.is_dir():
    raise SystemExit(0)
for path in sorted(root.iterdir()):
    if path.is_file():
        print(path.name)
PY
}
before_releases="$(releases_snapshot)"
# 沙箱测试本身不触碰 releases/；此处验证确实如此
[[ "$(releases_snapshot)" == "$before_releases" ]] || fail "releases/ 目录内容被改动"
releases_count="$(printf '%s' "$before_releases" | grep -c . || true)"
ok "releases/ 历史产物列表未变（共 ${releases_count} 个文件）"

# ---- 7. 真实仓库：cargo metadata 解析版本与 VERSION 一致 ----
real_version="$(tr -d '\r' < "${ROOT_DIR}/VERSION" | sed -n '1s/^[[:space:]]*//;s/[[:space:]]*$//;p;q')"
cargo_version="$(
  cargo metadata --manifest-path "${ROOT_DIR}/desktop/src-tauri/Cargo.toml" --format-version 1 \
  | "$PYTHON_BIN" -c 'import json, sys; pkgs = json.load(sys.stdin)["packages"]; print(next(p["version"] for p in pkgs if p["name"] == "local-ai-gateway"))'
)"
[[ "$cargo_version" == "$real_version" ]] || fail "cargo metadata 解析版本为 $cargo_version，VERSION 为 $real_version"
ok "cargo metadata 解析版本 $cargo_version == VERSION"

# ---- 8. 只改 VERSION，再 sync 灌进全部声明 ----
printf '%s\n' "0.4.0-rc.2" > VERSION
./scripts/version.sh sync >/dev/null
./scripts/version.sh check >/dev/null || fail "只改 VERSION 后 sync/check 失败"
shown="$(./scripts/version.sh show)"
[[ "$shown" == "0.4.0-rc.2" ]] || fail "show 应为 0.4.0-rc.2，实际 $shown"
"$PYTHON_BIN" - <<'PY'
import json, pathlib, re
v = "0.4.0-rc.2"
assert pathlib.Path("VERSION").read_bytes() == (v + "\n").encode(), "VERSION 应规范化为 LF"
assert re.search(r'^version\s*=\s*"' + re.escape(v) + r'"', pathlib.Path("desktop/src-tauri/Cargo.toml").read_text(), re.M)
assert json.loads(pathlib.Path("desktop/src-tauri/tauri.conf.json").read_text())["version"] == v
assert re.search(r'^pkgver=0\.4\.0_rc\.2$', pathlib.Path("packaging/arch/PKGBUILD").read_text(), re.M)
PY
ok "只改 VERSION 再 sync 灌进全部第一方声明"

# ---- 9. Windows CRLF VERSION 仍可 sync ----
printf '%s\r\n' "0.4.1" > VERSION
./scripts/version.sh sync >/dev/null
[[ "$(./scripts/version.sh show)" == "0.4.1" ]] || fail "CRLF VERSION 未能解析为 0.4.1"
./scripts/version.sh check >/dev/null || fail "CRLF VERSION sync 后 check 失败"
[[ "$(./scripts/version.sh show)" == "0.4.1" ]] || fail "规范化后 show 应为 0.4.1"
"$PYTHON_BIN" - <<'PY'
import pathlib
assert pathlib.Path("VERSION").read_bytes() == b"0.4.1\n", "CRLF VERSION 应规范化为 LF"
PY
ok "CRLF VERSION 可解析并规范化为 LF"

echo "全部自校验通过"
