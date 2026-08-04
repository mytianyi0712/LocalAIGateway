#!/usr/bin/env bash
# Local AI Gateway 版本管理（单一权威源：仓库根目录 VERSION）
#
# 用法：
#   ./scripts/version.sh show              # 显示当前版本
#   ./scripts/version.sh set 0.3.0-rc.1    # 设置新版本并同步所有第一方版本声明
#   ./scripts/version.sh check             # 校验全部声明一致（供 CI/本地检查，退出码 0/1）
#
# 同步范围（只更新第一方项目版本；依赖版本与 releases/ 历史产物绝不被改动）：
#   VERSION                         权威源
#   desktop/src-tauri/Cargo.toml    [package] version
#   desktop/src-tauri/tauri.conf.json 顶层 version（见下方取舍说明）
#   desktop/src-tauri/Cargo.lock    由 cargo metadata 重新解析，不手工替换
#   backend/pyproject.toml          [project] version
#   backend/uv.lock                 由 uv lock 重新解析，不手工替换
#   backend/app/main.py             FastAPI(version=...) 字面量
#   packaging/arch/PKGBUILD         pkgver 字面量（Arch 映射：连字符转下划线）；构建时
#                                   packaging/build-arch.sh 还会从 Cargo.toml 覆盖该值，
#                                   模板本身也由 set/check 保持与 VERSION 一致
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
PYPROJECT="${ROOT_DIR}/backend/pyproject.toml"
UV_LOCK="${ROOT_DIR}/backend/uv.lock"
APP_MAIN="${ROOT_DIR}/backend/app/main.py"
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
  ./scripts/version.sh set 0.3.0-rc.1    # 设置新版本并同步所有第一方版本声明
  ./scripts/version.sh check             # 校验全部声明一致（供 CI/本地检查，退出码 0/1）
EOF
  exit "$code"
}

is_semver() { [[ "$1" =~ $SEMVER_RE ]]; }

read_version() { cat "${VERSION_FILE}"; }

# 逐个文件把各自当前版本替换为 new。任一文件缺失、版本非法或字面量出现多次时，
# 在写入任何文件之前整体失败（保证"无效输入在任何写入前失败"且不留半成品）；
# 内容未变的文件不写入，重复执行同一版本是真正的空操作。
update_manifests() {
  local new="$1"
  "$PYTHON_BIN" - "$new" "$CARGO_MANIFEST" "$TAURI_CONF" "$PYPROJECT" "$APP_MAIN" "$PKGBUILD" <<'PY'
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

# PEP 440 合法 pre 标签（uv 会把它们规范化写入 uv.lock，例如 0.3.0-rc.1 -> 0.3.0rc1）。
PEP440_PRE = {"a", "b", "c", "rc", "alpha", "beta", "pre", "preview", "dev"}


def pep440_write(semver):
    """把 semver 版本映射为 PEP 440 可解析形式（用于 pyproject.toml）。

    合法 pre 标签原样保留（uv 自行规范化）；其余 prerelease（如 fix1）无法用
    PEP 440 pre 表示，映射为 local segment（0.2.1-fix1 -> 0.2.1+fix1）。
    """
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)*)-(.*)", semver)
    if not m:
        return semver
    release, pre = m.groups()
    parts = pre.split(".")
    if parts and parts[0].lower() in PEP440_PRE and all(part.isdigit() for part in parts[1:]):
        return semver
    return f"{release}+{pre}"

def check_old(path, old):
    if not SEMVER.match(old):
        raise SystemExit(f"{path}: 当前版本不是合法 SemVer: {old!r}")

results = []  # (path, original, new_content, old)

# 1. Cargo.toml 与 3. pyproject.toml：^version = "..." 行
#    pyproject.toml 使用 PEP 440 映射形式（pep440_write），其余文件保持 semver 原样。
for index, path in ((0, paths[0]), (2, paths[2])):
    text = load(path)
    old = version_line(path, text)
    check_old(path, old)
    replacement = pep440_write(new) if index == 2 else new
    results.append((
        path, text,
        re.sub(r'^(version\s*=\s*")[^"]+(")$', lambda m: m.group(1) + replacement + m.group(2),
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

# 4. backend/app/main.py：FastAPI(version="...") 字面量必须恰好出现一次
text = load(paths[3])
m = re.search(r'FastAPI\([^)]*version="([^"]+)"', text)
if not m:
    raise SystemExit(f"{paths[3]}: 未找到 FastAPI(version=\"...\") 字面量")
old = m.group(1)
check_old(paths[3], old)
needle = f'version="{old}"'
if text.count(needle) != 1:
    raise SystemExit(f"{paths[3]}: 期望恰好一个 {needle!r}，实际 {text.count(needle)} 个")
results.append((paths[3], text, text.replace(needle, f'version="{new}"'), old))

# 5. packaging/arch/PKGBUILD：pkgver 字面量（Arch 规则：无连字符；映射 - -> _）
text = load(paths[4])
m = re.search(r'^pkgver=([^\n]+)', text, re.MULTILINE)
if not m:
    raise SystemExit(f"{paths[4]}: 未找到 pkgver= 声明")
old = m.group(1).strip()
if not re.fullmatch(r'[A-Za-z0-9._+]+', old):
    raise SystemExit(f"{paths[4]}: pkgver={old!r} 含非法字符（Arch pkgver 不允许连字符）")
results.append((paths[4], text,
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

# 用 cargo/uv 各自重新解析锁文件，保证本地包记录与清单一致（非手工全文替换）。
update_lockfiles() {
  echo "  Cargo.lock: cargo metadata 重新解析"
  cargo metadata --manifest-path "${CARGO_MANIFEST}" --format-version 1 >/dev/null
  echo "  uv.lock: uv lock 重新解析"
  (cd "${ROOT_DIR}/backend" && uv lock)
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
  "$PYTHON_BIN" - "$version" "$CARGO_MANIFEST" "$CARGO_LOCK" "$TAURI_CONF" "$PYPROJECT" "$UV_LOCK" "$APP_MAIN" "$PKGBUILD" <<'PY'
import json
import pathlib
import re
import sys

expected, *paths = sys.argv[1:]
cargo_manifest, cargo_lock, tauri_conf, pyproject, uv_lock, app_main, pkgbuild = map(pathlib.Path, paths)
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

def main_version(path):
    text = path.read_text()
    m = re.search(r'FastAPI\([^)]*version="([^"]+)"', text)
    return m.group(1) if m else None

def pep440_compact(v):
    # uv.lock 存 PEP 440 规范形（如 0.3.0-rc.1 -> 0.3.0rc1），把两侧都归一化后再比较。
    v = v.strip().lower()
    if v.startswith("v"):
        v = v[1:]
    m = re.match(r"^([0-9]+(?:\.[0-9]+)*)", v)
    release = ".".join(str(int(p)) for p in m.group(1).split("."))
    rest = v[m.end():]
    if not rest:
        return release
    m = re.match(r"^[-_.]?(a|b|c|rc|alpha|beta|pre|preview)[-_.]?([0-9]*)(.*)$", rest)
    if m:
        label = {"a": "a", "alpha": "a", "b": "b", "beta": "b",
                 "c": "rc", "pre": "rc", "preview": "rc", "rc": "rc"}[m.group(1)]
        return f"{release}{label}{m.group(2) or '0'}"
    m = re.match(r"^[-_.]?(dev)[-_.]?([0-9]*)(.*)$", rest)
    if m:
        return f"{release}dev{m.group(2) or '0'}"
    m = re.match(r"^[-_.]?(post|rev|r)[-_.]?([0-9]*)(.*)$", rest)
    if m:
        return f"{release}post{m.group(2) or '0'}"
    m = re.match(r"^[-_.]?([0-9]+)(.*)$", rest)
    if m:
        return f"{release}post{m.group(1)}"
    # 其余内容（+local 或非法 pre 标签如 -fix1）按 PEP 440 local segment 归一：
    # 0.2.1-fix1 与 0.2.1+fix1 视为同一版本。
    local = rest.lstrip("+-_).").replace("-", "").replace("_", "").lower()
    return f"{release}+{local}"

checks = [
    ("desktop/src-tauri/Cargo.toml", version_line(cargo_manifest), expected),
    ("desktop/src-tauri/Cargo.lock", lock_version(cargo_lock), expected),
    ("desktop/src-tauri/tauri.conf.json", json.loads(tauri_conf.read_text()).get("version"), expected),
    ("backend/pyproject.toml", version_line(pyproject), expected),
    ("backend/uv.lock", lock_version(uv_lock), pep440_compact(expected)),
    ("backend/app/main.py", main_version(app_main), expected),
]
# pyproject.toml / uv.lock 允许 PEP 440 映射（0.2.1-fix1 -> 0.2.1+fix1 / 0.2.1fix1）。
for name, actual, want in checks:
    ok = actual == want or (
        name in ("backend/pyproject.toml", "backend/uv.lock")
        and pep440_compact(actual) == pep440_compact(want)
    )
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

cmd_set() {
  local new="${1:-}"
  [[ -n "$new" ]] || die "set 需要一个版本参数，如 ./scripts/version.sh set 0.3.0-rc.1"
  is_semver "$new" || die "非法 SemVer（示例：0.2.1、0.3.0-rc.1）: ${new}"
  command -v cargo >/dev/null || die "需要 cargo（用于重新解析 Cargo.lock）"
  command -v uv >/dev/null || die "需要 uv（用于重新解析 uv.lock）"

  local old=""
  if [[ -f "${VERSION_FILE}" ]]; then
    old="$(read_version)"
    is_semver "$old" || die "VERSION 内容不是合法 SemVer: ${old}"
  fi

  if [[ "$old" == "$new" ]]; then
    echo "version.sh: 已处于 ${new}，同步并校验一致性"
  else
    echo "version.sh: 更新第一方版本声明至 ${new}"
  fi
  # 始终收敛各声明（内容未变的文件不会写入），幂等且能修复手工漂移
  update_manifests "$new"
  if [[ "$old" != "$new" ]]; then
    if [[ -n "$old" ]]; then
      local tmp="${VERSION_FILE}.tmp"
      printf '%s\n' "$new" > "$tmp"
      mv -- "$tmp" "${VERSION_FILE}"
    else
      printf '%s\n' "$new" > "${VERSION_FILE}"
    fi
    echo "  ${VERSION_FILE}: ${old:-（无）} -> ${new}"
  fi
  update_lockfiles
  cmd_check
}

case "${1:-}" in
  show) cmd_show ;;
  set) cmd_set "${2:-}" ;;
  check) cmd_check ;;
  -h|--help|help) usage 0 ;;
  *) usage 2 ;;
esac
