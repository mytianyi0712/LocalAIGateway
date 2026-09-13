//! admin API 域模块：供应商 CRUD（provider 域，服务层已形式化）
//! 每个域文件包含该资源的 handler（薄壳）与输入/输出类型；
//! 所有 SQL 与响应组装都在 super::AdminService —— 这里只做 extractor/DTO。

use super::*;
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

/// Known non-default provider kinds. `command_code` opts a provider into
/// Command Code CLI identity headers (plan decision 2); everything else is
/// `None` and behaviorally identical to the pre-0005 gateway.
pub(super) const KNOWN_PROVIDER_KINDS: [&str; 1] = ["command_code"];

/// Normalize + validate the optional `kind` field: an empty string clears it.
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

pub(super) async fn list_providers(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    state.admin.list_providers().await
}
pub(super) async fn load_provider(db: &Database, row_id: &str) -> Result<ProviderRow, ApiError> {
    sqlx::query_as::<_,ProviderRow>("SELECT p.id,p.name,p.base_url,p.kind,p.created_at,p.updated_at,COUNT(c.id) channel_count FROM providers p LEFT JOIN channels c ON c.provider_id=p.id WHERE p.id=? GROUP BY p.id")
        .bind(row_id).fetch_optional(db.pool()).await?.ok_or_else(||ApiError::not_found("Provider not found"))
}
pub(super) async fn create_provider(
    _: AdminAuth,
    State(state): State<Context>,
    Json(input): Json<ProviderInput>,
) -> ApiResult {
    state.admin.create_provider(input).await
}
pub(super) async fn get_provider(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.get_provider(&row_id).await
}
pub(super) async fn patch_provider(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
    Json(input): Json<ProviderPatch>,
) -> ApiResult {
    state.admin.patch_provider(&row_id, input).await
}
pub(super) async fn delete_provider(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_provider(&row_id).await
}
