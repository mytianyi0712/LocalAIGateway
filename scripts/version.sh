#!/usr/bin/env bash
# Local AI Gateway 版本管理（单一权威源：仓库根目录 VERSION）
#
# 日常：编辑 VERSION，然后 `make version-sync` 或直接 `make package-all`。
#
# 用法：
#   ./scripts/version.sh show              # 显示当前版本
#   ./scripts/version.sh sync              # 以 VERSION 为准同步所有第一方声明
#   ./scripts/version.sh set 0.3.0-rc.1    # 写入 VERSION 并同步
#   ./scripts/version.sh check             # 校验全部声明一致（供 CI/本地检查，退出码 0/1）
#
# 同步范围（只更新第一方项目版本；依赖版本与 releases/ 历史产物绝不被改动）：
#   VERSION                         权威源（只改这一个文件）
#   desktop/src-tauri/Cargo.toml    [package] version
#   desktop/src-tauri/tauri.conf.json 顶层 version（见下方取舍说明）
#   desktop/src-tauri/Cargo.lock    由 cargo metadata 重新解析，不手工替换
#   packaging/arch/PKGBUILD         pkgver 字面量（Arch 映射：连字符转下划线）；构建时
#                                   packaging/build-arch.sh 还会从 Cargo.toml 覆盖该值，
#                                   模板本身也由 sync/check 保持与 VERSION 一致
#
# 取舍说明：Tauri CLI 在 tauri.conf.json 缺省 version 时会回退到 Cargo.toml，
# 但 tauri-build 的 Windows 可执行文件版本资源（FileVersion/ProductVersion）
# 只读取 tauri.conf.json（tauri-build 2.6.3 src/lib.rs），删除该字段会让
# Windows 构建产物丢失版本元数据。因此保留并同步 tauri.conf.json 的 version，
# 而不是把 Cargo.toml 当作 Tauri 侧唯一来源。
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

VERSION_FILE="${ROOT_DIR}/VERSION"
CARGO_MANIFEST="${ROOT_DIR}/desktop/src-tauri/Cargo.toml"
TAURI_CONF="${ROOT_DIR}/desktop/src-tauri/tauri.conf.json"
CARGO_LOCK="${ROOT_DIR}/desktop/src-tauri/Cargo.lock"
PKGBUILD="${ROOT_DIR}/packaging/arch/PKGBUILD"

# 官方 SemVer 语法（semver.org）。拒绝 1.2、v1.2.3、01.2.3、1.2.3.4、1.2.3- 等非规范输入。
SEMVER_RE='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'

die() { echo "version.sh: $*" >&2; exit 1; }

# 版本字面量同步/校验用 Python 实现；windows-latest 的 Git Bash 可能只有 python，
# 这里做一次 python3/python 回退，避免 CI 上 check 步骤因解释器名缺失而失败。
PYTHON_BIN="$(command -v python3 || command -v python || die '需要 python3 或 python')"

usage() {
  local code="$1"
  cat >&2 <<'EOF'
用法：
  ./scripts/version.sh show              # 显示当前版本
  ./scripts/version.sh sync              # 以 VERSION 为准同步所有第一方声明
  ./scripts/version.sh set 0.3.0-rc.1    # 写入 VERSION 并同步
  ./scripts/version.sh check             # 校验全部声明一致（供 CI/本地检查，退出码 0/1）
EOF
  exit "$code"
}

is_semver() { [[ "$1" =~ $SEMVER_RE ]]; }

# 接受 Windows 编辑器的 CRLF / UTF-8 BOM；只取第一行。
read_version() {
  "$PYTHON_BIN" - "$VERSION_FILE" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8-sig")
line = text.splitlines()[0].strip() if text.strip() else ""
print(line)
PY
}
# 把 VERSION 写成单一 LF 行。内容未变则不写，返回 1。
write_version_file() {
  local new="$1"
  "$PYTHON_BIN" - "$VERSION_FILE" "$new" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
desired = (sys.argv[2] + "\n").encode("utf-8")
current = path.read_bytes() if path.exists() else None
if current == desired:
    raise SystemExit(1)
path.write_bytes(desired)
raise SystemExit(0)
PY
}

# 逐个文件把各自当前版本替换为 new。任一文件缺失、版本非法或字面量出现多次时，
# 在写入任何文件之前整体失败（保证"无效输入在任何写入前失败"且不留半成品）；
# 内容未变的文件不写入，重复执行同一版本是真正的空操作。
update_manifests() {
  local new="$1"
  "$PYTHON_BIN" - "$new" "$CARGO_MANIFEST" "$TAURI_CONF" "$PKGBUILD" <<'PY'
import json
import pathlib
import re
import sys

new, *paths = sys.argv[1:]
paths = [pathlib.Path(p) for p in paths]
SEMVER = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?"
    r"(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$"
)

def load(path):
    try:
        return path.read_text()
    except OSError as exc:
        raise SystemExit(f"无法读取 {path}: {exc}")

def version_line(path, text):
    m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    if not m:
        raise SystemExit(f"{path}: 未找到 ^version = \"...\" 声明")
    return m.group(1)

def check_old(path, old):
    if not SEMVER.match(old):
        raise SystemExit(f"{path}: 当前版本不是合法 SemVer: {old!r}")

results = []  # (path, original, new_content, old)

# 1. Cargo.toml：^version = "..." 行（semver 原样）
text = load(paths[0])
old = version_line(paths[0], text)
check_old(paths[0], old)
results.append((
    paths[0], text,
    re.sub(r'^(version\s*=\s*")[^"]+(")$', lambda m: m.group(1) + new + m.group(2),
           text, count=1, flags=re.MULTILINE),
    old,
))

# 2. tauri.conf.json：顶层 version 键（保留原格式化，只替换该行）
text = load(paths[1])
conf = json.loads(text)
old = conf.get("version")
if not isinstance(old, str):
    raise SystemExit(f"{paths[1]}: 顶层 version 缺失")
check_old(paths[1], old)
needle = f'"version": "{old}"'
if text.count(needle) != 1:
    raise SystemExit(f"{paths[1]}: 期望恰好一个 {needle!r}，实际 {text.count(needle)} 个")
results.append((paths[1], text, text.replace(needle, f'"version": "{new}"'), old))

# 3. packaging/arch/PKGBUILD：pkgver 字面量（Arch 规则：无连字符；映射 - -> _）
text = load(paths[2])
m = re.search(r'^pkgver=([^\n]+)', text, re.MULTILINE)
if not m:
    raise SystemExit(f"{paths[2]}: 未找到 pkgver= 声明")
old = m.group(1).strip()
if not re.fullmatch(r'[A-Za-z0-9._+]+', old):
    raise SystemExit(f"{paths[2]}: pkgver={old!r} 含非法字符（Arch pkgver 不允许连字符）")
results.append((paths[2], text,
                re.sub(r'^(pkgver=)[^\n]*',
                       lambda mm: mm.group(1) + new.replace("-", "_"),
                       text, count=1, flags=re.MULTILINE),
                old))

for path, original, content, old in results:
    if content != original:
        path.write_text(content)
        print(f"  {path}: {old} -> {new}")
    else:
        print(f"  {path}: 已一致 ({new})")
PY
}

# 用 cargo 重新解析锁文件，保证本地包记录与清单一致（非手工全文替换）。
update_lockfiles() {
  echo "  Cargo.lock: cargo metadata 重新解析"
  cargo metadata --manifest-path "${CARGO_MANIFEST}" --format-version 1 >/dev/null
}

cmd_show() {
  [[ -f "${VERSION_FILE}" ]] || die "缺少 ${VERSION_FILE}，请先执行 set"
  read_version
}

cmd_check() {
  [[ -f "${VERSION_FILE}" ]] || die "缺少 ${VERSION_FILE}，请先执行 set"
  local version
  version="$(read_version)"
  is_semver "${version}" || die "VERSION 内容不是合法 SemVer: ${version}"
  "$PYTHON_BIN" - "$version" "$CARGO_MANIFEST" "$CARGO_LOCK" "$TAURI_CONF" "$PKGBUILD" <<'PY'
import json
import pathlib
import re
import sys

expected, *paths = sys.argv[1:]
cargo_manifest, cargo_lock, tauri_conf, pkgbuild = map(pathlib.Path, paths)
failures = []

def version_line(path):
    text = path.read_text()
    m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    return m.group(1) if m else None

def lock_version(path):
    text = path.read_text()
    for block in text.split("[[package]]"):
        if re.search(r'^name\s*=\s*"local-ai-gateway"\s*$', block, re.MULTILINE):
            m = re.search(r'^version\s*=\s*"([^"]+)"', block, re.MULTILINE)
            return m.group(1) if m else None
    return None

checks = [
    ("desktop/src-tauri/Cargo.toml", version_line(cargo_manifest), expected),
    ("desktop/src-tauri/Cargo.lock", lock_version(cargo_lock), expected),
    ("desktop/src-tauri/tauri.conf.json", json.loads(tauri_conf.read_text()).get("version"), expected),
]
for name, actual, want in checks:
    ok = actual == want
    print(f"  [{'OK' if ok else 'FAIL'}] {name}: {actual if actual is not None else '(未找到)'}")
    if not ok:
        failures.append(name)

pkgbuild_text = pkgbuild.read_text()
m = re.search(r'^pkgver=([^\n]+)', pkgbuild_text, re.MULTILINE)
pkgver_ok = m is not None and m.group(1).strip() == expected.replace("-", "_")
print(f"  [{'OK' if pkgver_ok else 'FAIL'}] packaging/arch/PKGBUILD: pkgver 与 VERSION 一致（Arch 映射）")
if not pkgver_ok:
    failures.append("packaging/arch/PKGBUILD")

if failures:
    print(f"check 失败：{', '.join(failures)} 与 VERSION ({expected}) 不一致")
    sys.exit(1)
print(f"check 通过：全部第一方版本声明与 VERSION ({expected}) 一致")
PY
}

sync_from_version() {
  local version
  version="$(read_version)"
  is_semver "${version}" || die "VERSION 内容不是合法 SemVer: ${version}"
  command -v cargo >/dev/null || die "需要 cargo（用于重新解析 Cargo.lock）"
  if write_version_file "$version"; then
    echo "  ${VERSION_FILE}: 已规范化为 ${version}"
  fi
  update_manifests "$version"
  update_lockfiles
  cmd_check
}

cmd_sync() {
  [[ -f "${VERSION_FILE}" ]] || die "缺少 ${VERSION_FILE}，请先编辑该文件或执行 set"
  local version
  version="$(read_version)"
  echo "version.sh: 以 VERSION (${version}) 为权威源同步第一方声明"
  sync_from_version
}

cmd_set() {
  local new="${1:-}"
  [[ -n "$new" ]] || die "set 需要一个版本参数，如 ./scripts/version.sh set 0.3.0-rc.1"
  is_semver "$new" || die "非法 SemVer（示例：0.2.1、0.3.0-rc.1）: ${new}"

  local old=""
  if [[ -f "${VERSION_FILE}" ]]; then
    old="$(read_version)"
    is_semver "$old" || die "VERSION 内容不是合法 SemVer: ${old}"
  fi

  if [[ "$old" == "$new" ]]; then
    echo "version.sh: 已处于 ${new}，同步并校验一致性"
  else
    echo "version.sh: 更新 VERSION 至 ${new}"
    write_version_file "$new" || true
    echo "  ${VERSION_FILE}: ${old:-（无）} -> ${new}"
  fi
  sync_from_version
}

case "${1:-}" in
  show) cmd_show ;;
  sync) cmd_sync ;;
  set) cmd_set "${2:-}" ;;
  check) cmd_check ;;
  -h|--help|help) usage 0 ;;
  *) usage 2 ;;
esac
