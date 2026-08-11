//! admin API 域模块：Claude/Codex 映射与预设（mappings/presets 域）
//! 每个域文件包含该资源的 handler（薄壳）与输入/输出类型；
//! 直写 SQL 的域逻辑正逐步收敛到 super::AdminService。

use super::*;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::routes::route_bundle;

#[derive(Deserialize)]
pub(super) struct MappingInput {
    #[serde(alias = "claude_model_id", alias = "codex_model_id")]
    model_id: Option<String>,
    display_name: Option<String>,
    upstream_protocol: String,
    upstream_model_id: String,
    enabled: Option<bool>,
}
#[derive(Deserialize)]
pub(super) struct MappingPatch {
    #[serde(alias = "claude_model_id", alias = "codex_model_id")]
    model_id: Option<String>,
    display_name: Option<String>,
    upstream_protocol: Option<String>,
    upstream_model_id: Option<String>,
    enabled: Option<bool>,
}
pub(super) fn mapping_defaults(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}
pub(super) async fn validate_upstream(
    state: &Context,
    protocol: &str,
    model_id: &str,
) -> Result<(), ApiError> {
    if !valid_protocol(protocol) {
        return Err(ApiError::validation("Unsupported protocol"));
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM model_routes WHERE protocol=? AND requested_model_id=?",
    )
    .bind(protocol)
    .bind(model_id)
    .fetch_one(state.db.pool())
    .await?;
    if count == 0 {
        return Err(ApiError::validation("Upstream model route not found"));
    }
    Ok(())
}
pub(super) async fn mapping_json(
    state: &Context,
    kind: &str,
    row_id: &str,
) -> Result<Value, ApiError> {
    let (table, idcol) = if kind == "claude" {
        ("claude_model_mappings", "claude_model_id")
    } else {
        ("codex_model_mappings", "codex_model_id")
    };
    let sql = format!(
        "SELECT id,{idcol} model_id,display_name,upstream_protocol,upstream_model_id,enabled,created_at,updated_at FROM {table} WHERE id=?"
    );
    let row = sqlx::query(&sql)
        .bind(row_id)
        .fetch_optional(state.db.pool())
        .await?
        .ok_or_else(|| ApiError::not_found("Mapping not found"))?;
    let upstream_protocol: String = row.get("upstream_protocol");
    let upstream_model_id: String = row.get("upstream_model_id");
    let candidates = route_bundle(state, &upstream_model_id)
        .await
        .ok()
        .and_then(|v| v.get("candidates").cloned())
        .unwrap_or_else(|| json!([]));
    let model_id: String = row.get("model_id");
    // The management UI reads the protocol-specific key (claude_model_id /
    // codex_model_id), mirroring the Python gateway; `model_id` is kept as a
    // compatibility alias.
    let value = json!({
        "id": row.get::<String, _>("id"),
        "model_id": model_id,
        idcol: model_id,
        "display_name": row.get::<Option<String>, _>("display_name"),
        "upstream_protocol": upstream_protocol,
        "upstream_model_id": upstream_model_id,
        "enabled": row.get::<bool, _>("enabled"),
        "candidates": candidates,
        "created_at": row.get::<String, _>("created_at"),
        "updated_at": row.get::<String, _>("updated_at"),
    });
    Ok(value)
}
pub(super) async fn list_mappings(state: &Context, kind: &str) -> Result<Vec<Value>, ApiError> {
    let (table, idcol) = if kind == "claude" {
        ("claude_model_mappings", "claude_model_id")
    } else {
        ("codex_model_mappings", "codex_model_id")
    };
    let sql = format!("SELECT id FROM {table} ORDER BY {idcol}");
    let ids: Vec<String> = sqlx::query_scalar(&sql).fetch_all(state.db.pool()).await?;
    let mut items = Vec::new();
    for row_id in ids {
        items.push(mapping_json(state, kind, &row_id).await?);
    }
    Ok(items)
}
pub(super) async fn list_claude_mappings(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    let items = list_mappings(&state, "claude").await?;
    Ok(ok(json!({"items":items,"total":items.len()})))
}
pub(super) async fn create_claude_mapping(
    _: AdminAuth,
    State(state): State<Context>,
    Json(input): Json<MappingInput>,
) -> ApiResult {
    validate_upstream(&state, &input.upstream_protocol, &input.upstream_model_id).await?;
    let model_id = input
        .model_id
        .ok_or_else(|| ApiError::validation("claude_model_id is required"))?;
    validate_text(&model_id, "claude_model_id", 255)?;
    let row_id = id();
    let time = now();
    sqlx::query("INSERT INTO claude_model_mappings(id,claude_model_id,display_name,upstream_protocol,upstream_model_id,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)").bind(&row_id).bind(model_id).bind(input.display_name).bind(input.upstream_protocol).bind(input.upstream_model_id).bind(mapping_defaults(input.enabled)).bind(&time).bind(&time).execute(state.db.pool()).await.map_err(|e|integrity(e,"Mapping already exists"))?;
    Ok(json_response(
        StatusCode::CREATED,
        mapping_json(&state, "claude", &row_id).await?,
    ))
}
pub(super) async fn patch_claude_mapping(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
    Json(input): Json<MappingPatch>,
) -> ApiResult {
    let current = mapping_json(&state, "claude", &row_id).await?;
    let model_id = input
        .model_id
        .or_else(|| {
            current
                .get("model_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| ApiError::validation("model id missing"))?;
    let protocol = input
        .upstream_protocol
        .or_else(|| {
            current
                .get("upstream_protocol")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default();
    let upstream = input
        .upstream_model_id
        .or_else(|| {
            current
                .get("upstream_model_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default();
    validate_upstream(&state, &protocol, &upstream).await?;
    let time = now();
    sqlx::query("UPDATE claude_model_mappings SET claude_model_id=?,display_name=?,upstream_protocol=?,upstream_model_id=?,enabled=?,updated_at=? WHERE id=?").bind(model_id).bind(input.display_name.or_else(||current.get("display_name").and_then(Value::as_str).map(str::to_owned))).bind(protocol).bind(upstream).bind(input.enabled.unwrap_or_else(||current.get("enabled").and_then(Value::as_bool).unwrap_or(true))).bind(time).bind(&row_id).execute(state.db.pool()).await.map_err(|e|integrity(e,"Mapping already exists"))?;
    Ok(ok(mapping_json(&state, "claude", &row_id).await?))
}
pub(super) async fn delete_claude_mapping(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let result = sqlx::query("DELETE FROM claude_model_mappings WHERE id=?")
        .bind(row_id)
        .execute(state.db.pool())
        .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("Mapping not found"));
    }
    Ok(no_content())
}
pub(super) async fn list_codex_mappings(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    let items = list_mappings(&state, "codex").await?;
    Ok(ok(json!({"items":items,"total":items.len()})))
}
pub(super) async fn create_codex_mapping(
    _: AdminAuth,
    State(state): State<Context>,
    Json(input): Json<MappingInput>,
) -> ApiResult {
    validate_upstream(&state, &input.upstream_protocol, &input.upstream_model_id).await?;
    let model_id = input
        .model_id
        .ok_or_else(|| ApiError::validation("codex_model_id is required"))?;
    validate_text(&model_id, "codex_model_id", 255)?;
    let row_id = id();
    let time = now();
    sqlx::query("INSERT INTO codex_model_mappings(id,codex_model_id,display_name,upstream_protocol,upstream_model_id,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)").bind(&row_id).bind(model_id).bind(input.display_name).bind(input.upstream_protocol).bind(input.upstream_model_id).bind(mapping_defaults(input.enabled)).bind(&time).bind(&time).execute(state.db.pool()).await.map_err(|e|integrity(e,"Mapping already exists"))?;
    Ok(json_response(
        StatusCode::CREATED,
        mapping_json(&state, "codex", &row_id).await?,
    ))
}
pub(super) async fn patch_codex_mapping(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
    Json(input): Json<MappingPatch>,
) -> ApiResult {
    let current = mapping_json(&state, "codex", &row_id).await?;
    let model_id = input
        .model_id
        .or_else(|| {
            current
                .get("model_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| ApiError::validation("model id missing"))?;
    let protocol = input
        .upstream_protocol
        .or_else(|| {
            current
                .get("upstream_protocol")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default();
    let upstream = input
        .upstream_model_id
        .or_else(|| {
            current
                .get("upstream_model_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default();
    validate_upstream(&state, &protocol, &upstream).await?;
    let time = now();
    sqlx::query("UPDATE codex_model_mappings SET codex_model_id=?,display_name=?,upstream_protocol=?,upstream_model_id=?,enabled=?,updated_at=? WHERE id=?").bind(model_id).bind(input.display_name.or_else(||current.get("display_name").and_then(Value::as_str).map(str::to_owned))).bind(protocol).bind(upstream).bind(input.enabled.unwrap_or_else(||current.get("enabled").and_then(Value::as_bool).unwrap_or(true))).bind(time).bind(&row_id).execute(state.db.pool()).await.map_err(|e|integrity(e,"Mapping already exists"))?;
    Ok(ok(mapping_json(&state, "codex", &row_id).await?))
}
pub(super) async fn delete_codex_mapping(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let result = sqlx::query("DELETE FROM codex_model_mappings WHERE id=?")
        .bind(row_id)
        .execute(state.db.pool())
        .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("Mapping not found"));
    }
    Ok(no_content())
}
pub(super) fn default_claude_presets() -> Value {
    json!({"items":[{"id":"claude-opus-5","display_name":"Claude Opus 5（默认）"},{"id":"claude-fable-5","display_name":"Claude Fable 5"},{"id":"claude-sonnet-5","display_name":"Claude Sonnet 5"},{"id":"claude-mythos-5","display_name":"Claude Mythos 5"},{"id":"claude-haiku-5","display_name":"Claude Haiku 5"},{"id":"claude-opus-4-6","display_name":"Claude Opus 4.6"},{"id":"claude-sonnet-4-5","display_name":"Claude Sonnet 4.5"}],"source":"defaults","refreshed_at":null})
}
pub(super) fn default_codex_presets() -> Value {
    json!({"items":[{"id":"gpt-5-codex","display_name":"GPT-5 Codex（默认）"},{"id":"gpt-5","display_name":"GPT-5"},{"id":"gpt-5-mini","display_name":"GPT-5 Mini"},{"id":"gpt-5-nano","display_name":"GPT-5 Nano"},{"id":"o3","display_name":"o3"},{"id":"o4-mini","display_name":"o4-mini"},{"id":"gpt-4.1","display_name":"GPT-4.1"},{"id":"gpt-4o","display_name":"GPT-4o"}],"source":"defaults","refreshed_at":null})
}
/// Protocols whose channels feed the Claude preset list (P1-5).
const CLAUDE_PRESET_PROTOCOLS: &[&str] = &["claude"];
/// Protocols whose channels feed the Codex preset list (P1-5): the Codex
/// CLI speaks the Responses API, and openai_compatible channels serve the
/// same model set.
const CODEX_PRESET_PROTOCOLS: &[&str] = &["openai_responses", "openai_compatible"];

/// Live preset aggregation (P1-5): built-in defaults merged with every
/// available model that current channels expose over the target protocols,
/// deduplicated by model id. `channel_models + channel_model_protocols`
/// are the single source of truth — there is no second, easily-stale
/// settings cache anymore.
pub(super) async fn presets(
    state: &Context,
    protocols: &[&str],
    defaults: Value,
) -> Result<Value, ApiError> {
    let placeholders = protocols.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT DISTINCT cm.model_id, cm.display_name FROM channel_models cm \
         JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id \
         WHERE cm.available = 1 AND cmp.protocol IN ({placeholders}) \
         ORDER BY cm.model_id"
    );
    let mut query = sqlx::query(&sql);
    for protocol in protocols {
        query = query.bind(protocol);
    }
    let rows = query.fetch_all(state.db.pool()).await?;
    let mut items = defaults
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut seen = HashSet::new();
    for item in &items {
        if let Some(id) = item.get("id").and_then(Value::as_str) {
            seen.insert(id.to_owned());
        }
    }
    for row in rows {
        let model_id: String = row.get("model_id");
        if seen.insert(model_id.clone()) {
            let display_name: Option<String> = row.get("display_name");
            items.push(json!({
                "id": model_id.clone(),
                "display_name": display_name.unwrap_or(model_id),
            }));
        }
    }
    let refreshed_at: Option<String> = sqlx::query_scalar(
        "SELECT MAX(finished_at) FROM discovery_runs WHERE finished_at IS NOT NULL",
    )
    .fetch_one(state.db.pool())
    .await?;
    Ok(json!({"items": items, "source": "channels", "refreshed_at": refreshed_at}))
}
pub(super) async fn claude_presets(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    Ok(ok(presets(
        &state,
        CLAUDE_PRESET_PROTOCOLS,
        default_claude_presets(),
    )
    .await?))
}
pub(super) async fn codex_presets(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    Ok(ok(presets(
        &state,
        CODEX_PRESET_PROTOCOLS,
        default_codex_presets(),
    )
    .await?))
}
pub(super) async fn refresh_claude_presets(
    _: AdminAuth,
    State(state): State<Context>,
) -> ApiResult {
    refresh_presets_for(&state, "Claude", CLAUDE_PRESET_PROTOCOLS).await
}
pub(super) async fn refresh_codex_presets(
    _: AdminAuth,
    State(state): State<Context>,
) -> ApiResult {
    refresh_presets_for(&state, "Codex", CODEX_PRESET_PROTOCOLS).await
}
/// Queues a REAL discovery run for every enabled channel exposing at least
/// one of the target protocols and returns the run IDs (P1-5). With no
/// eligible channel the request fails clearly instead of pretending work
/// was queued.
async fn refresh_presets_for(state: &Context, kind: &str, protocols: &[&str]) -> ApiResult {
    let placeholders = protocols.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT DISTINCT c.id FROM channels c \
         JOIN channel_protocols cp ON cp.channel_id = c.id \
         WHERE c.manual_enabled = 1 AND cp.protocol IN ({placeholders}) ORDER BY c.id"
    );
    let mut query = sqlx::query_scalar::<_, String>(&sql);
    for protocol in protocols {
        query = query.bind(protocol);
    }
    let channels: Vec<String> = query.fetch_all(state.db.pool()).await?;
    if channels.is_empty() {
        return Err(ApiError::conflict(format!(
            "没有启用且支持该协议集合的渠道，无法刷新 {kind} 预设"
        )));
    }
    let mut run_ids = Vec::with_capacity(channels.len());
    let mut failed_channels = Vec::new();
    for channel_id in channels {
        let queued = state.discovery.queue(state, channel_id.clone()).await;
        match queued {
            Ok(run_id) => run_ids.push(run_id),
            Err(error) => {
                tracing::warn!(%error, "preset refresh: discovery queue failed");
                failed_channels.push(channel_id);
            }
        }    }
    if run_ids.is_empty() {
        return Err(ApiError::internal(format!(
            "{kind} 预设刷新：全部渠道的 discovery 排队失败"
        )));
    }
    Ok(json_response(
        StatusCode::ACCEPTED,
        json!({
            "status": "queued",
            "run_ids": run_ids,
            "queued_channels": run_ids.len(),
            "failed_channels": failed_channels,
        }),
    ))
}
