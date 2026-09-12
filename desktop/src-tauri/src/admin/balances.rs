//! admin API 域模块：渠道余额查询与配置（balance 域，服务层为
//! `crate::balance::BalanceService`）。
//!
//! Handlers are thin extractor/DTO shells: no SQL, no HTTP client, no
//! business rules. The service owns validation, network access and snapshot
//! persistence; `scripts/check-layers.sh` keeps this file on the migrated
//! handler list.

use super::*;
use crate::balance::BalanceConfigInput;
use axum::extract::{Path, State};

/// `GET /channels/{id}/balance` — config plus the latest snapshot.
pub(super) async fn get_balance(
    _: AdminAuth,
    State(state): State<Context>,
    Path(channel_id): Path<String>,
) -> ApiResult {
    Ok(ok(state.balance.balance_json(&channel_id).await?))
}

/// `POST /channels/{id}/balance` — query now and persist the snapshot.
/// Upstream failures still return 200 with `status=error`.
pub(super) async fn query_balance(
    _: AdminAuth,
    State(state): State<Context>,
    Path(channel_id): Path<String>,
) -> ApiResult {
    Ok(ok(state.balance.query_json(&state, &channel_id).await?))
}

/// `PUT /channels/{id}/balance-config` — save adapter/switch/template/token.
pub(super) async fn put_balance_config(
    _: AdminAuth,
    State(state): State<Context>,
    Path(channel_id): Path<String>,
    Json(input): Json<BalanceConfigInput>,
) -> ApiResult {
    let config = state.balance.save_config(&channel_id, input).await?;
    Ok(ok(config.to_json()))
}

/// `DELETE /channels/{id}/balance-config` — back to default-off.
pub(super) async fn delete_balance_config(
    _: AdminAuth,
    State(state): State<Context>,
    Path(channel_id): Path<String>,
) -> ApiResult {
    state.balance.delete_config(&channel_id).await?;
    Ok(no_content())
}

/// `POST /balances/refresh` — refresh every enabled channel in parallel.
pub(super) async fn refresh_balances(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    Ok(ok(state.balance.refresh_json(&state).await?))
}
