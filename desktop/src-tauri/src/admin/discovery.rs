//! admin API 域模块：模型发现与手工探测（discovery/probe 域）
//! 每个域文件包含该资源的 handler（薄壳）与输入/输出类型；
//! 直写 SQL 的域逻辑正逐步收敛到 super::AdminService。

use super::*;
use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;

pub(super) async fn discover_models(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.ensure_channel_exists(&row_id).await?;
    let run_id = state.discovery.queue(&state, row_id).await?;
    Ok(json_response(
        StatusCode::ACCEPTED,
        json!({"run_id":run_id,"status":"queued"}),
    ))
}
pub(super) async fn manual_probe(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.ensure_channel_exists(&row_id).await?;
    crate::health::queue(state, row_id).await?;
    Ok(json_response(
        StatusCode::ACCEPTED,
        json!({"status":"queued"}),
    ))
}
pub(super) async fn get_discovery_run(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let row = sqlx::query("SELECT * FROM discovery_runs WHERE id=?")
        .bind(row_id)
        .fetch_optional(state.db.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("Discovery run not found"))?;
    // The management UI polls this endpoint and switches on `status`
    // (running/succeeded/failed), mirroring the Python gateway.
    let finished_at: Option<String> = row.try_get("finished_at")?;
    let success: Option<bool> = row.try_get("success")?;
    let status = match finished_at {
        None => "running",
        Some(_) if success == Some(true) => "succeeded",
        Some(_) => "failed",
    };
    Ok(ok(json!({
        "id": row.get::<String, _>("id"),
        "channel_id": row.get::<String, _>("channel_id"),
        "status": status,
        "model_count": row.get::<Option<i64>, _>("model_count"),
        "status_code": row.get::<Option<i64>, _>("status_code"),
        "error_kind": row.get::<Option<String>, _>("error_kind"),
        "started_at": row.get::<String, _>("started_at"),
        "finished_at": finished_at,
    })))
}
pub(super) async fn list_discovery_runs(
    _: AdminAuth,
    State(state): State<Context>,
    Path(channel_id): Path<String>,
) -> ApiResult {
    let rows = sqlx::query(
        "SELECT * FROM discovery_runs WHERE channel_id=? ORDER BY started_at DESC LIMIT 100",
    )
    .bind(channel_id)
    .fetch_all(state.db.pool())
    .await?;
    let items=rows.iter().map(|row|json!({"id":row.get::<String,_>("id"),"channel_id":row.get::<String,_>("channel_id"),"trigger":row.get::<String,_>("trigger"),"started_at":row.get::<String,_>("started_at"),"finished_at":row.get::<Option<String>,_>("finished_at"),"success":row.get::<Option<bool>,_>("success"),"model_count":row.get::<Option<i64>,_>("model_count"),"status_code":row.get::<Option<i64>,_>("status_code"),"error_kind":row.get::<Option<String>,_>("error_kind")})).collect::<Vec<_>>();
    Ok(ok(json!({"items":items,"total":items.len()})))
}
