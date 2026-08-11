//! admin API 域模块：请求日志与健康探测日志（logs 域）
//! 每个域文件包含该资源的 handler（薄壳）与输入/输出类型；
//! 直写 SQL 的域逻辑正逐步收敛到 super::AdminService。

use super::*;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize, Default)]
pub(super) struct RequestQuery {
    protocol: Option<String>,
    model_id: Option<String>,
    page: Option<i64>,
    page_size: Option<i64>,
}
pub(super) async fn list_requests(
    _: AdminAuth,
    State(state): State<Context>,
    Query(query): Query<RequestQuery>,
) -> ApiResult {
    let page = query.page.unwrap_or(1).max(1);
    let size = query.page_size.unwrap_or(50).clamp(1, 200);
    let rows = sqlx::query("SELECT id,protocol,model_id,endpoint,stream,started_at,finished_at,total_duration_ms,final_status_code,outcome,attempt_count,final_channel_id,request_bytes,response_bytes FROM request_logs WHERE (? IS NULL OR protocol=?) AND (? IS NULL OR model_id=?) ORDER BY started_at DESC LIMIT ? OFFSET ?").bind(&query.protocol).bind(&query.protocol).bind(&query.model_id).bind(&query.model_id).bind(size).bind((page-1)*size).fetch_all(state.db.pool()).await?;
    let ids: Vec<String> = rows.iter().map(|row| row.get::<String, _>("id")).collect();
    // Aggregate per-request attempt metadata (channels, upstream identity) the
    // same way the Python backend does, so the log list can show the responding
    // channel and upstream model without opening the detail view.
    // (channel_name, upstream_protocol, upstream_model_id)
    type AttemptSummary = Vec<(String, Option<String>, Option<String>)>;
    let mut attempts_by_request: HashMap<String, AttemptSummary> = HashMap::new();
    if !ids.is_empty() {
        let mut builder = QueryBuilder::new(
            "SELECT request_id, channel_name, upstream_protocol, upstream_model_id FROM request_attempts WHERE request_id IN (",
        );
        let mut separated = builder.separated(", ");
        for id in &ids {
            separated.push_bind(id);
        }
        builder.push(") ORDER BY request_id, attempt_no");
        let attempts = builder.build().fetch_all(state.db.pool()).await?;
        for attempt in attempts {
            let request_id: String = attempt.get("request_id");
            attempts_by_request.entry(request_id).or_default().push((
                attempt.get::<String, _>("channel_name"),
                attempt.get::<Option<String>, _>("upstream_protocol"),
                attempt.get::<Option<String>, _>("upstream_model_id"),
            ));
        }
    }
    let items = rows.iter().map(|row| {
        let id: String = row.get("id");
        let attempts = attempts_by_request.get(&id).cloned().unwrap_or_default();
        let response_channels: Vec<String> =
            attempts.iter().map(|(name, _, _)| name.clone()).collect();
        let upstream = attempts
            .iter()
            .find(|(_, protocol, model)| protocol.is_some() || model.is_some());
        let upstream_protocol = upstream.and_then(|(_, protocol, _)| protocol.clone());
        let upstream_model_id = upstream.and_then(|(_, _, model)| model.clone());
        json!({"id":id,"protocol":row.get::<String,_>("protocol"),"model_id":row.get::<Option<String>,_>("model_id"),"endpoint":row.get::<String,_>("endpoint"),"stream":row.get::<Option<bool>,_>("stream"),"started_at":row.get::<String,_>("started_at"),"finished_at":row.get::<Option<String>,_>("finished_at"),"total_duration_ms":row.get::<Option<i64>,_>("total_duration_ms"),"final_status_code":row.get::<Option<i64>,_>("final_status_code"),"outcome":row.get::<String,_>("outcome"),"attempt_count":row.get::<i64,_>("attempt_count"),"final_channel_id":row.get::<Option<String>,_>("final_channel_id"),"request_bytes":row.get::<Option<i64>,_>("request_bytes"),"response_bytes":row.get::<Option<i64>,_>("response_bytes"),"response_channels":response_channels,"upstream_protocol":upstream_protocol,"upstream_model_id":upstream_model_id})
    }).collect::<Vec<_>>();
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM request_logs")
        .fetch_one(state.db.pool())
        .await?;
    Ok(ok(
        json!({"items":items,"total":total,"page":page,"page_size":size}),
    ))
}
pub(super) async fn get_request(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let row = sqlx::query("SELECT * FROM request_logs WHERE id=?")
        .bind(&row_id)
        .fetch_optional(state.db.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("Request not found"))?;
    let attempts =
        sqlx::query("SELECT * FROM request_attempts WHERE request_id=? ORDER BY attempt_no")
            .bind(&row_id)
            .fetch_all(state.db.pool())
            .await?;
    let attempt_values=attempts.iter().map(|a|json!({"id":a.get::<String,_>("id"),"channel_id":a.get::<Option<String>,_>("channel_id"),"channel_name":a.get::<String,_>("channel_name"),"attempt_no":a.get::<i64,_>("attempt_no"),"priority_snapshot":a.get::<i64,_>("priority_snapshot"),"started_at":a.get::<String,_>("started_at"),"finished_at":a.get::<Option<String>,_>("finished_at"),"status_code":a.get::<Option<i64>,_>("status_code"),"outcome":a.get::<String,_>("outcome"),"error_kind":a.get::<Option<String>,_>("error_kind"),"failover_eligible":a.get::<bool,_>("failover_eligible"),"response_started":a.get::<bool,_>("response_started"),"first_byte_ms":a.get::<Option<i64>,_>("first_byte_ms"),"first_token_ms":a.get::<Option<i64>,_>("first_token_ms"),"duration_ms":a.get::<Option<i64>,_>("duration_ms"),"input_tokens":a.get::<Option<i64>,_>("input_tokens"),"cache_read_tokens":a.get::<Option<i64>,_>("cache_read_tokens"),"cache_write_tokens":a.get::<Option<i64>,_>("cache_write_tokens"),"cache_miss_input_tokens":a.get::<Option<i64>,_>("cache_miss_input_tokens"),"output_tokens":a.get::<Option<i64>,_>("output_tokens"),"tps":a.get::<Option<f64>,_>("tps"),"raw_usage_json":a.get::<Option<String>,_>("raw_usage_json"),"response_bytes":a.get::<Option<i64>,_>("response_bytes"),"upstream_protocol":a.get::<Option<String>,_>("upstream_protocol"),"upstream_model_id":a.get::<Option<String>,_>("upstream_model_id")})).collect::<Vec<_>>();
    let mut value = Map::new();
    // The SQLite driver cannot decode columns directly into serde_json::Value;
    // extract each field with its concrete type instead.
    value.insert("id".into(), json!(row.get::<String, _>("id")));
    value.insert("protocol".into(), json!(row.get::<String, _>("protocol")));
    value.insert(
        "model_id".into(),
        json!(row.get::<Option<String>, _>("model_id")),
    );
    value.insert("endpoint".into(), json!(row.get::<String, _>("endpoint")));
    value.insert("stream".into(), json!(row.get::<Option<bool>, _>("stream")));
    value.insert(
        "started_at".into(),
        json!(row.get::<String, _>("started_at")),
    );
    value.insert(
        "finished_at".into(),
        json!(row.get::<Option<String>, _>("finished_at")),
    );
    value.insert(
        "total_duration_ms".into(),
        json!(row.get::<Option<i64>, _>("total_duration_ms")),
    );
    value.insert(
        "final_status_code".into(),
        json!(row.get::<Option<i64>, _>("final_status_code")),
    );
    value.insert("outcome".into(), json!(row.get::<String, _>("outcome")));
    value.insert(
        "attempt_count".into(),
        json!(row.get::<i64, _>("attempt_count")),
    );
    value.insert(
        "final_channel_id".into(),
        json!(row.get::<Option<String>, _>("final_channel_id")),
    );
    value.insert(
        "request_bytes".into(),
        json!(row.get::<Option<i64>, _>("request_bytes")),
    );
    value.insert(
        "response_bytes".into(),
        json!(row.get::<Option<i64>, _>("response_bytes")),
    );
    value.insert("attempts".into(), json!(attempt_values));
    Ok(ok(Value::Object(value)))
}
pub(super) async fn clear_logs(
    _: AdminAuth,
    State(state): State<Context>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    if query.get("confirm").is_none_or(|value| value != "true") {
        return Err(ApiError::validation("confirm=true is required"));
    }
    let mut tx = state.db.pool().begin().await?;
    sqlx::query("DELETE FROM request_attempts")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM request_logs")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM health_probe_logs")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(no_content())
}
pub(super) async fn list_health_probes(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    let rows = sqlx::query("SELECT * FROM health_probe_logs ORDER BY started_at DESC LIMIT 200")
        .fetch_all(state.db.pool())
        .await?;
    let items=rows.iter().map(|row|json!({"id":row.get::<String,_>("id"),"channel_id":row.get::<String,_>("channel_id"),"model_id":row.get::<String,_>("model_id"),"started_at":row.get::<String,_>("started_at"),"duration_ms":row.get::<Option<i64>,_>("duration_ms"),"success":row.get::<bool,_>("success"),"status_code":row.get::<Option<i64>,_>("status_code"),"error_kind":row.get::<Option<String>,_>("error_kind"),"next_probe_at":row.get::<Option<String>,_>("next_probe_at")})).collect::<Vec<_>>();
    Ok(ok(json!({"items":items,"total":items.len()})))
}
