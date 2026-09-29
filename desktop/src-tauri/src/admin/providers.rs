//! admin API 域模块：供应商 CRUD（provider 域）。
//!
//! handler 只做参数提取与响应透传；供应商的具名服务方法在 `admin/mod.rs` 的
//! `impl AdminService` 里。本文件还保留 `load_provider` 查询助手，以及被该服务
//! 复用的 `provider_json`、`validate_provider_kind` 等无状态函数。

use super::ApiResult;
use crate::api_error::ApiError;
use crate::auth::AdminAuth;
use crate::db::Database;
use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, State},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::FromRow;

#[derive(FromRow)]
pub(super) struct ProviderRow {
    id: String,
    name: String,
    base_url: String,
    kind: Option<String>,
    created_at: String,
    updated_at: String,
    channel_count: i64,
}
pub(super) fn provider_json(row: ProviderRow) -> Value {
    json!({"id":row.id,"name":row.name,"base_url":row.base_url,"kind":row.kind,"channel_count":row.channel_count,"created_at":row.created_at,"updated_at":row.updated_at})
}

/// 已知的非默认供应商类型。`command_code` 表示该供应商走 Command Code CLI
/// 身份路径（请求头、协议绑定与探测都受影响）；其余取值一律为 `None`。
pub(super) const KNOWN_PROVIDER_KINDS: [&str; 1] = ["command_code"];

/// 规范化并校验可选的 `kind` 字段：空字符串视为清除。
pub(super) fn validate_provider_kind(kind: Option<&str>) -> Result<Option<String>, ApiError> {
    let Some(kind) = kind else { return Ok(None) };
    let kind = kind.trim();
    if kind.is_empty() {
        return Ok(None);
    }
    if !KNOWN_PROVIDER_KINDS.contains(&kind) {
        return Err(ApiError::validation("不支持的供应商类型"));
    }
    Ok(Some(kind.to_owned()))
}

#[derive(Deserialize)]
pub struct ProviderInput {
    pub name: String,
    pub base_url: String,
    #[serde(default)]
    pub kind: Option<String>,
}
#[derive(Deserialize)]
pub struct ProviderPatch {
    pub name: Option<String>,
    pub base_url: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
}

pub(super) async fn list_providers(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    state.admin.list_providers().await
}
pub(super) async fn load_provider(db: &Database, row_id: &str) -> Result<ProviderRow, ApiError> {
    sqlx::query_as::<_,ProviderRow>("SELECT p.id,p.name,p.base_url,p.kind,p.created_at,p.updated_at,COUNT(c.id) channel_count FROM providers p LEFT JOIN channels c ON c.provider_id=p.id WHERE p.id=? GROUP BY p.id")
        .bind(row_id).fetch_optional(db.pool()).await?.ok_or_else(||ApiError::not_found("Provider not found"))
}
pub(super) async fn create_provider(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<ProviderInput>,
) -> ApiResult {
    state.admin.create_provider(input).await
}
pub(super) async fn get_provider(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.get_provider(&row_id).await
}
pub(super) async fn patch_provider(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ProviderPatch>,
) -> ApiResult {
    state.admin.patch_provider(&row_id, input).await
}
pub(super) async fn delete_provider(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_provider(&row_id).await
}
