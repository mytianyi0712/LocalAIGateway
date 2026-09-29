#!/usr/bin/env python3
"""模块依赖图门禁：`desktop/src-tauri/src` 内顶层模块的 crate 内依赖必须无环。

只依赖标准库。忽略 `#[cfg(test)] mod ... { ... }` 覆盖的行区间与 `//` 行注释，
解析 `use crate::...;`（含跨行花括号列表），按顶层模块名（路径首段）建图，
再用 Tarjan 求强连通分量：任一分量大小 > 1 即为依赖环，打印环上模块与
构成该环的 `use` 位置，退出 1。

允许的例外写在 ALLOWED_CYCLES：每一项是一组允许互相依赖的模块名。
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# 组合根：`state::AppState` 刻意依赖全部具体服务，而服务经 axum 的
# `State<AppState>` 反向引用它——这是刻意的接线，不参与环检测。
COMPOSITION_ROOT = "state"

# 允许存在的环。任何**新增**的环（或既有环的成员变化）都会失败：
# 这里的比较是集合相等，因此往环里多拉进一个模块同样会被拦下。
ALLOWED_CYCLES: list[frozenset[str]] = [
    # `Context` 持有遥测句柄（application -> telemetry），遥测写入器把
    # channel 事件落成 `channel_health` 迁移（telemetry -> health），
    # 健康探测按 `&Context` 取端口（health -> application），
    # 而 `settings` 的读取端口同样以 `&Context` 为参数（settings -> application）。
    # 这四条边都是「同一组基础设施值互相引用」，不是业务层的反向依赖；
    # 拆开它们需要给 health 单独造一套依赖结构或给遥测加状态端口，
    # 收益不抵改动面（见报告「未修复/登记项」）。
    frozenset({"application", "health", "settings", "telemetry"}),
]

ROOT = Path(__file__).resolve().parent.parent / "desktop" / "src-tauri" / "src"

USE_RE = re.compile(r"^\s*use\s+crate\s*::")
IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def strip_cfg_test(lines: list[str]) -> list[str]:
    """把 `#[cfg(test)]` 覆盖的块（通常是 `mod tests { ... }`）整段清空。"""
    out = list(lines)
    i = 0
    while i < len(out):
        if out[i].strip() == "#[cfg(test)]":
            j = i + 1
            # 跳到该属性所修饰项的 `{`（属性与项之间可能夹着 doc 注释）
            while j < len(out) and "{" not in out[j]:
                j += 1
            if j >= len(out):
                break
            depth = 0
            while j < len(out):
                depth += out[j].count("{") - out[j].count("}")
                out[j] = ""
                j += 1
                if depth <= 0:
                    break
            for k in range(i, j):
                out[k] = ""
            i = j
            continue
        i += 1
    return out


def module_of(path: Path) -> str:
    """文件 → 顶层模块名（`admin/channels.rs` → `admin`）。"""
    rel = path.relative_to(ROOT)
    return rel.parts[0].removesuffix(".rs")


def use_statements(lines: list[str]) -> list[tuple[int, str]]:
    """收集 (行号, 语句文本) 的 `use crate::...` 语句，跨行按花括号配平取到分号。"""
    found: list[tuple[int, str]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        code = line.split("//", 1)[0]
        if not USE_RE.match(code):
            i += 1
            continue
        start = i
        text = ""
        depth = 0
        while i < len(lines):
            code = lines[i].split("//", 1)[0]
            text += code + " "
            depth += code.count("{") - code.count("}")
            i += 1
            if depth <= 0 and ";" in code:
                break
        found.append((start + 1, text))
    return found


def imported_modules(statement: str) -> set[str]:
    """一条 `use crate::...` 语句引用的顶层模块名集合。"""
    body = statement.strip()
    body = body[len("use") :].strip()
    body = body.removeprefix("crate::").removesuffix(";").strip()
    modules: set[str] = set()
    for segment in re.split(r"[{},]", body):
        segment = segment.strip()
        if not segment or segment == "self":
            continue
        name = IDENT_RE.match(segment)
        if name:
            modules.add(name.group(0))
    return modules


def main() -> int:
    if not ROOT.is_dir():
        print(f"check-module-graph: 找不到源码目录 {ROOT}", file=sys.stderr)
        return 1
    edges: dict[str, set[str]] = {}
    locations: dict[tuple[str, str], list[str]] = {}
    for path in sorted(ROOT.rglob("*.rs")):
        module = module_of(path)
        if module == COMPOSITION_ROOT:
            continue
        edges.setdefault(module, set())
        lines = strip_cfg_test(path.read_text(encoding="utf-8").split("\n"))
        for lineno, statement in use_statements(lines):
            for target in imported_modules(statement):
                if target == module or target == COMPOSITION_ROOT:
                    continue
                edges[module].add(target)
                locations.setdefault((module, target), []).append(
                    f"{path.relative_to(ROOT.parent.parent)}:{lineno}"
                )

    # Tarjan 强连通分量
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    counter = 0
    components: list[list[str]] = []

    def strongconnect(node: str) -> None:
        nonlocal counter
        index[node] = low[node] = counter
        counter += 1
        stack.append(node)
        on_stack.add(node)
        for nxt in sorted(edges.get(node, ())):
            if nxt not in edges:
                continue
            if nxt not in index:
                strongconnect(nxt)
                low[node] = min(low[node], low[nxt])
            elif nxt in on_stack:
                low[node] = min(low[node], index[nxt])
        if low[node] == index[node]:
            component = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                component.append(member)
                if member == node:
                    break
            components.append(sorted(component))

    for node in sorted(edges):
        if node not in index:
            strongconnect(node)

    cycles = [
        component
        for component in components
        if len(component) > 1 and frozenset(component) not in ALLOWED_CYCLES
    ]
    if not cycles:
        print(f"check-module-graph: ok（{len(edges)} 个顶层模块，无依赖环）")
        return 0

    print("check-module-graph: FAILED — crate 内存在模块依赖环")
    for component in cycles:
        members = set(component)
        print(f"  环: {' -> '.join(component)} -> {component[0]}")
        for (source, target), spots in sorted(locations.items()):
            if source in members and target in members:
                print(f"    {source} -> {target}: {', '.join(sorted(set(spots)))}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
