//! admin API 域模块：渠道余额查询与配置（balance 域）。
//!
//! 服务层是 `crate::balance::BalanceService`：handler 只是 extractor/DTO
//! 薄壳，不含 SQL、不发 HTTP、不做业务判断；校验、网络访问与快照持久化
//! 都归该服务。上游查询失败仍返回 200 + `status=error`（余额是可选信息，
//! 不能拖垮渠道页）。
//! 分层门禁只检查 handler 体，本文件不含 SQL，天然通过。

use super::{ApiResult, no_content, ok};
use crate::auth::AdminAuth;
use crate::balance::BalanceConfigInput;
use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, State},
};

/// `GET /channels/{id}/balance` —— 返回余额配置与最近一次快照。
pub(super) async fn get_balance(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> ApiResult {
    Ok(ok(state.balance.balance_json(&channel_id).await?))
}

/// `POST /channels/{id}/balance` —— 立即查询并持久化快照。
/// 上游失败仍返回 200，响应体为 `status=error`。
pub(super) async fn query_balance(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> ApiResult {
    Ok(ok(state.balance.query_json(&state, &channel_id).await?))
}

/// `PUT /channels/{id}/balance-config` —— 保存适配器、开关、请求模板与专用令牌。
pub(super) async fn put_balance_config(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(input): Json<BalanceConfigInput>,
) -> ApiResult {
    let config = state.balance.save_config(&channel_id, input).await?;
    Ok(ok(config.to_json()))
}

/// `DELETE /channels/{id}/balance-config` —— 删除配置，回到默认关闭。
pub(super) async fn delete_balance_config(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> ApiResult {
    state.balance.delete_config(&channel_id).await?;
    Ok(no_content())
}

/// `POST /balances/refresh` —— 并行刷新全部已启用渠道的余额。
pub(super) async fn refresh_balances(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    Ok(ok(state.balance.refresh_json(&state).await?))
}
