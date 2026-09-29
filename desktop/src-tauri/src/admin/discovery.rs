//! admin API 域模块：模型发现与手工探测（discovery/probe 域）。
//!
//! 两个 POST 只做「渠道存在性校验 + 入队」，真正的探测由
//! `crate::discovery` / `crate::health` 的后台任务执行：两个 POST 均返回 202
//! 入队回执；`discover` 另返回 `run_id`，`probe` 不返回。
//! 两个 GET 读 `discovery_runs`：详情把 `finished_at`/`success` 归一成
//! `running`/`succeeded`/`failed` 三态供前端轮询；SQL 与响应组装都在同
//! 文件的 `impl AdminService` 内。

use super::{AdminService, ApiResult, json_response, ok};
use crate::api_error::ApiError;
use crate::auth::AdminAuth;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;
use sqlx::Row;

pub(super) async fn discover_models(
    _: AdminAuth,
    State(state): State<AppState>,
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
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.ensure_channel_exists(&row_id).await?;
    crate::health::queue(state.ctx.clone(), row_id).await?;
    Ok(json_response(
        StatusCode::ACCEPTED,
        json!({"status":"queued"}),
    ))
}
pub(super) async fn get_discovery_run(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.get_discovery_run(&row_id).await
}
pub(super) async fn list_discovery_runs(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
) -> ApiResult {
    state.admin.list_discovery_runs(&channel_id).await
}

impl AdminService {
    pub(super) async fn get_discovery_run(&self, row_id: &str) -> ApiResult {
        let row = sqlx::query("SELECT * FROM discovery_runs WHERE id=?")
            .bind(row_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| ApiError::not_found("Discovery run not found"))?;
        // 管理端界面轮询此端点并按 `status` 分支：`finished_at` 为空表示仍在
        // 运行，已结束则按 `success` 取 `succeeded`/`failed`。
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

    pub(super) async fn list_discovery_runs(&self, channel_id: &str) -> ApiResult {
        let rows = sqlx::query(
            "SELECT * FROM discovery_runs WHERE channel_id=? ORDER BY started_at DESC LIMIT 100",
        )
        .bind(channel_id)
        .fetch_all(self.db.pool())
        .await?;
        let items=rows.iter().map(|row|json!({"id":row.get::<String,_>("id"),"channel_id":row.get::<String,_>("channel_id"),"trigger":row.get::<String,_>("trigger"),"started_at":row.get::<String,_>("started_at"),"finished_at":row.get::<Option<String>,_>("finished_at"),"success":row.get::<Option<bool>,_>("success"),"model_count":row.get::<Option<i64>,_>("model_count"),"status_code":row.get::<Option<i64>,_>("status_code"),"error_kind":row.get::<Option<String>,_>("error_kind")})).collect::<Vec<_>>();
        Ok(ok(json!({"items":items,"total":items.len()})))
    }
}
