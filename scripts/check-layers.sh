#!/usr/bin/env bash
# Dependency-direction gate (P2-4 / architecture report): the application
# layer (and any future domain/ports layers) must not import HTTP framework,
# SQL or HTTP-client infrastructure directly. Business logic goes through
# the context and (eventually) repository/upstream/event ports; a violation
# here means a layering regression that would ripple into every handler.
#
# CI-safe: exit 0 when clean, 1 on the first violation.
set -euo pipefail
cd "$(dirname "$0")/../desktop/src-tauri"

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
check_layer src/ports "ports"
check_layer src/ports.rs "ports"

# Admin domain files that completed the service migration (P2-1/D): their
# handler functions must stay thin extractor/DTO shells — all SQL lives in
# `impl AdminService` blocks or storage helpers. A handler body containing
# sqlx:: is a service-boundary regression. Files are added to this list as
# their subdomain migrates.
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
            if (depth <= 0) { handler = 0; seen_open = 0 }
        }
        END { exit fail_flag }
    ' "$f" || fail=1
}

check_admin_handlers src/admin/providers.rs
check_admin_handlers src/admin/presets.rs
check_admin_handlers src/admin/commandcode.rs
check_admin_handlers src/admin/channels.rs
check_admin_handlers src/admin/balances.rs

# The protocol registry must stay enum-driven: PROTOCOL_ORDER is the only
# allowed protocol-list constant (everything else derives from ProtocolId).
if grep -rn "PROTOCOL_ENDPOINTS" src/ --include='*.rs' >/dev/null; then
    echo "LAYER VIOLATION (protocol): PROTOCOL_ENDPOINTS was removed — derive endpoints from ProtocolId"
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    echo "check-layers: FAILED"
    exit 1
fi
echo "check-layers: ok"
