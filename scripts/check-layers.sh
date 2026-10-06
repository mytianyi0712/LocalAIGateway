#!/usr/bin/env bash
# 依赖方向门禁：应用层（以及未来的 domain/ports 层）不得直接 import HTTP 框架、
# SQL 或 HTTP 客户端基础设施——业务逻辑必须经 context 与（最终）repository/
# upstream/event 端口访问外部世界；这里一旦报错就说明分层正在回退，
# 而分层回退会顺着每个 handler 扩散。
#
# 可直接在 CI 运行：干净退出 0，列出全部违规后整体退出 1。
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
cd "${SCRIPT_DIR}/../desktop/src-tauri"

fail=0
check_layer() {
    local target="$1"
    local label="$2"
    [ -e "$target" ] || return 0
    local files
    if [ -d "$target" ]; then
        files=$(find "$target" -name '*.rs')
    else
        files="$target"
    fi
    for f in $files; do
        if grep -nE '^\s*use (axum|sqlx|reqwest)(::|;|\s)' "$f" >/dev/null; then
            echo "LAYER VIOLATION ($label): $f imports framework infrastructure (axum/sqlx/reqwest)"
            fail=1
        fi
    done
}

check_layer src/application.rs "application"
check_layer src/application "application"
check_layer src/domain "domain"
check_layer src/domain.rs "domain"
check_layer src/ports "ports"
check_layer src/ports.rs "ports"

# 管理端各域文件：handler 必须是「提取参数 → 调 AdminService → 组装响应」的薄壳，
# SQL 只允许出现在 `impl AdminService` 块内。handler 体内出现 sqlx:: 或
# state.db.pool() 都属服务边界回退。新子域迁移完成后请把文件加进下面这份清单
# （当前清单已覆盖 src/admin 下全部 13 个子模块）。
check_admin_handlers() {
    local f="$1"
    [ -e "$f" ] || return 0
    awk -v file="$f" '
        /async fn/ { pending = 1; sig = ""; handler = 0; depth = 0; seen_open = 0 }
        pending {
            if ($0 ~ /AdminAuth/) handler = 1
            n_open = gsub(/{/, "{")
            n_close = gsub(/}/, "}")
            depth += n_open - n_close
            if (n_open > 0) { seen_open = 1; pending = 0 }
            next
        }
        handler && seen_open {
            n_open = gsub(/{/, "{")
            n_close = gsub(/}/, "}")
            depth += n_open - n_close
            if ($0 ~ /sqlx::/) {
                print "LAYER VIOLATION (admin handlers): " file ": handler body contains direct SQL"
                fail_flag = 1
            }
            if ($0 ~ /state\.db\.pool\(\)/) {
                print "LAYER VIOLATION (admin handlers): " file ": handler body touches the DB pool directly"
                fail_flag = 1
            }
            if (depth <= 0) { handler = 0; seen_open = 0 }
        }
        END { exit fail_flag }
    ' "$f" || fail=1
}

check_admin_handlers src/admin/balances.rs
check_admin_handlers src/admin/channels.rs
check_admin_handlers src/admin/commandcode.rs
check_admin_handlers src/admin/discovery.rs
check_admin_handlers src/admin/logs.rs
check_admin_handlers src/admin/models.rs
check_admin_handlers src/admin/mod.rs
check_admin_handlers src/admin/presets.rs
check_admin_handlers src/admin/profiles.rs
check_admin_handlers src/admin/providers.rs
check_admin_handlers src/admin/routes.rs
check_admin_handlers src/admin/settings.rs
check_admin_handlers src/admin/stats.rs

# 协议清单必须从 ProtocolId 派生；PROTOCOL_ORDER / PROTOCOL_ORDER_SQL 是需手工
# 同步的镜像，禁止新增第三份。
if grep -rn "PROTOCOL_ENDPOINTS" src/ --include='*.rs' >/dev/null; then
    echo "LAYER VIOLATION (protocol): PROTOCOL_ENDPOINTS was removed — derive endpoints from ProtocolId"
    fail=1
fi

# 模块依赖图：生产代码（忽略 #[cfg(test)] 区域）的 crate 内依赖必须无环
# （组合根 `state` 与其刻意接线除外，见脚本内的说明）。
PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [ -n "$PYTHON_BIN" ]; then
    "$PYTHON_BIN" "$SCRIPT_DIR/check-module-graph.py" || fail=1
else
    echo "check-layers: 跳过模块依赖图检查（未找到 python3/python）"
fi

if [ "$fail" -ne 0 ]; then
    echo "check-layers: FAILED"
    exit 1
fi
echo "check-layers: ok"
