use std::collections::{HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post, put},
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sqlx::{FromRow, QueryBuilder, Row};
use uuid::Uuid;

use crate::{
    api_error::{ApiError, json_response},
    auth::AdminAuth,
    crypto::SecretStore,
    protocol::{PROTOCOL_ENDPOINTS, PROTOCOL_ORDER, normalize_base_url, valid_protocol},
    server::AppState,
    settings,
};

type ApiResult = Result<Response, ApiError>;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/admin/v1/providers",
            get(list_providers).post(create_provider),
        )
        .route(
            "/api/admin/v1/providers/{id}",
            get(get_provider)
                .patch(patch_provider)
                .delete(delete_provider),
        )
        .route(
            "/api/admin/v1/channels",
            get(list_channels).post(create_channel),
        )
        .route(
            "/api/admin/v1/channels/{id}",
            get(get_channel).patch(patch_channel).delete(delete_channel),
        )
        .route("/api/admin/v1/channels/{id}/api-key", put(replace_api_key))
        .route(
            "/api/admin/v1/channels/{id}/reset-health",
            post(reset_health),
        )
        .route("/api/admin/v1/channels/{id}/probe", post(manual_probe))
        .route(
            "/api/admin/v1/channels/{id}/discover-models",
            post(discover_models),
        )
        .route(
            "/api/admin/v1/channels/{id}/discovery-runs",
            get(list_discovery_runs),
        )
        .route("/api/admin/v1/discovery-runs/{id}", get(get_discovery_run))
        .route("/api/admin/v1/channel-models", get(list_channel_models))
        .route(
            "/api/admin/v1/channels/{id}/models",
            post(create_manual_model),
        )
        .route(
            "/api/admin/v1/channel-models/{id}",
            patch(patch_channel_model).delete(delete_channel_model),
        )
        .route("/api/admin/v1/routes", get(list_routes).post(create_route))
        .route(
            "/api/admin/v1/routes/{id}",
            patch(patch_route).delete(delete_route),
        )
        .route(
            "/api/admin/v1/routes/{id}/candidates",
            put(replace_candidates),
        )
        .route(
            "/api/admin/v1/capability-profiles",
            get(list_profiles).post(create_profile),
        )
        .route(
            "/api/admin/v1/capability-profiles/{id}",
            get(get_profile).put(update_profile).delete(delete_profile),
        )
        .route(
            "/api/admin/v1/model-capabilities/detect/{*model_id}",
            post(detect_capabilities),
        )
        .route(
            "/api/admin/v1/model-capabilities/{*model_id}",
            get(get_capabilities).put(put_capabilities),
        )
        .route("/api/admin/v1/claude-presets", get(claude_presets))
        .route(
            "/api/admin/v1/claude-presets/refresh",
            post(refresh_presets),
        )
        .route(
            "/api/admin/v1/claude-mappings",
            get(list_claude_mappings).post(create_claude_mapping),
        )
        .route(
            "/api/admin/v1/claude-mappings/{id}",
            patch(patch_claude_mapping).delete(delete_claude_mapping),
        )
        .route("/api/admin/v1/codex-presets", get(codex_presets))
        .route("/api/admin/v1/codex-presets/refresh", post(refresh_presets))
        .route(
            "/api/admin/v1/codex-mappings",
            get(list_codex_mappings).post(create_codex_mapping),
        )
        .route(
            "/api/admin/v1/codex-mappings/{id}",
            patch(patch_codex_mapping).delete(delete_codex_mapping),
        )
        .route("/api/admin/v1/requests", get(list_requests))
        .route("/api/admin/v1/requests/{id}", get(get_request))
        .route("/api/admin/v1/health-probes", get(list_health_probes))
        .route("/api/admin/v1/logs", delete(clear_logs))
        .route("/api/admin/v1/stats/summary", get(stats_summary))
        .route("/api/admin/v1/stats/cache", get(stats_cache))
        .route("/api/admin/v1/stats/models", get(stats_models))
        .route("/api/admin/v1/stats/channels", get(stats_channels))
        .route("/api/admin/v1/stats/timeseries", get(stats_timeseries))
        .route(
            "/api/admin/v1/settings",
            get(get_settings).patch(patch_settings),
        )
        .route(
            "/api/admin/v1/settings/access-keys/generate",
            post(generate_access_keys),
        )
        .route("/api/admin/v1/system/status", get(system_status))
        .route("/api/admin/v1/system/protocols", get(system_protocols))
}

fn now() -> String {
    Utc::now().to_rfc3339()
}
fn id() -> String {
    Uuid::new_v4().to_string()
}
fn ok(value: Value) -> Response {
    Json(value).into_response()
}
fn no_content() -> Response {
    StatusCode::NO_CONTENT.into_response()
}
fn validate_text(value: &str, field: &str, max: usize) -> Result<(), ApiError> {
    if value.trim().is_empty() || value.chars().count() > max {
        return Err(ApiError::validation(format!("{field} 长度无效")));
    }
    Ok(())
}
fn integrity(error: sqlx::Error, message: &str) -> ApiError {
    if matches!(error, sqlx::Error::Database(_)) {
        ApiError::conflict(message)
    } else {
        ApiError::internal(error)
    }
}

#[derive(FromRow)]
struct ProviderRow {
    id: String,
    name: String,
    base_url: String,
    created_at: String,
    updated_at: String,
    channel_count: i64,
}
fn provider_json(row: ProviderRow) -> Value {
    json!({"id":row.id,"name":row.name,"base_url":row.base_url,"channel_count":row.channel_count,"created_at":row.created_at,"updated_at":row.updated_at})
}

#[derive(Deserialize)]
struct ProviderInput {
    name: String,
    base_url: String,
}
#[derive(Deserialize)]
struct ProviderPatch {
    name: Option<String>,
    base_url: Option<String>,
}

async fn list_providers(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let rows = sqlx::query_as::<_, ProviderRow>("SELECT p.id,p.name,p.base_url,p.created_at,p.updated_at,COUNT(c.id) channel_count FROM providers p LEFT JOIN channels c ON c.provider_id=p.id GROUP BY p.id ORDER BY p.name")
        .fetch_all(state.db.pool()).await?;
    let items: Vec<_> = rows.into_iter().map(provider_json).collect();
    Ok(ok(
        json!({"items":items,"total":items.len(),"page":1,"page_size":items.len()}),
    ))
}
async fn load_provider(state: &AppState, row_id: &str) -> Result<ProviderRow, ApiError> {
    sqlx::query_as::<_,ProviderRow>("SELECT p.id,p.name,p.base_url,p.created_at,p.updated_at,COUNT(c.id) channel_count FROM providers p LEFT JOIN channels c ON c.provider_id=p.id WHERE p.id=? GROUP BY p.id")
        .bind(row_id).fetch_optional(state.db.pool()).await?.ok_or_else(||ApiError::not_found("Provider not found"))
}
async fn create_provider(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<ProviderInput>,
) -> ApiResult {
    validate_text(&input.name, "name", 120)?;
    let base_url = normalize_base_url(&input.base_url)
        .map_err(|error| ApiError::validation(error.to_string()))?;
    let row_id = id();
    let time = now();
    sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES(?,?,?,?,?)")
        .bind(&row_id)
        .bind(input.name.trim())
        .bind(base_url)
        .bind(&time)
        .bind(&time)
        .execute(state.db.pool())
        .await
        .map_err(|e| integrity(e, "Provider already exists"))?;
    let row = load_provider(&state, &row_id).await?;
    Ok(json_response(StatusCode::CREATED, provider_json(row)))
}
async fn get_provider(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    Ok(ok(provider_json(load_provider(&state, &row_id).await?)))
}
async fn patch_provider(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ProviderPatch>,
) -> ApiResult {
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM providers WHERE id=?")
        .bind(&row_id)
        .fetch_one(state.db.pool())
        .await?;
    if exists == 0 {
        return Err(ApiError::not_found("Provider not found"));
    }
    if let Some(name) = input.name {
        validate_text(&name, "name", 120)?;
        sqlx::query("UPDATE providers SET name=?,updated_at=? WHERE id=?")
            .bind(name.trim())
            .bind(now())
            .bind(&row_id)
            .execute(state.db.pool())
            .await
            .map_err(|e| integrity(e, "Provider already exists"))?;
    }
    if let Some(url) = input.base_url {
        let url =
            normalize_base_url(&url).map_err(|error| ApiError::validation(error.to_string()))?;
        sqlx::query("UPDATE providers SET base_url=?,updated_at=? WHERE id=?")
            .bind(url)
            .bind(now())
            .bind(&row_id)
            .execute(state.db.pool())
            .await?;
    }
    get_provider(AdminAuth, State(state), Path(row_id)).await
}
async fn delete_provider(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let mut tx = state.db.pool().begin().await?;
    let changed=sqlx::query("DELETE FROM route_candidates WHERE channel_model_id IN (SELECT cm.id FROM channel_models cm JOIN channels c ON c.id=cm.channel_id WHERE c.provider_id=?)").bind(&row_id).execute(&mut *tx).await?;
    let _ = changed;
    sqlx::query("DELETE FROM channels WHERE provider_id=?")
        .bind(&row_id)
        .execute(&mut *tx)
        .await?;
    let result = sqlx::query("DELETE FROM providers WHERE id=?")
        .bind(&row_id)
        .execute(&mut *tx)
        .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("Provider not found"));
    }
    tx.commit().await?;
    Ok(no_content())
}

#[derive(FromRow)]
struct ChannelRow {
    id: String,
    provider_id: String,
    provider_name: String,
    name: String,
    protocol: String,
    api_key_encrypted: Vec<u8>,
    api_key_hint: String,
    manual_enabled: bool,
    health_check_model_id: Option<String>,
    created_at: String,
    updated_at: String,
    state: Option<String>,
    consecutive_failures: Option<i64>,
    disabled_until: Option<String>,
    last_success_at: Option<String>,
    last_failure_at: Option<String>,
    last_error_kind: Option<String>,
    last_status_code: Option<i64>,
    model_count: i64,
}
async fn load_channel(state: &AppState, row_id: &str) -> Result<ChannelRow, ApiError> {
    sqlx::query_as::<_,ChannelRow>("SELECT c.id,c.provider_id,p.name provider_name,c.name,c.protocol,c.api_key_encrypted,c.api_key_hint,c.manual_enabled,c.health_check_model_id,c.created_at,c.updated_at,h.state,h.consecutive_failures,h.disabled_until,h.last_success_at,h.last_failure_at,h.last_error_kind,h.last_status_code,COUNT(DISTINCT cm.id) model_count FROM channels c JOIN providers p ON p.id=c.provider_id LEFT JOIN channel_health h ON h.channel_id=c.id LEFT JOIN channel_models cm ON cm.channel_id=c.id WHERE c.id=? GROUP BY c.id")
        .bind(row_id).fetch_optional(state.db.pool()).await?.ok_or_else(||ApiError::not_found("Channel not found"))
}
async fn channel_json(state: &AppState, row: ChannelRow) -> Result<Value, ApiError> {
    let protocols:Vec<String>=sqlx::query_scalar("SELECT protocol FROM channel_protocols WHERE channel_id=? ORDER BY CASE protocol WHEN 'openai_compatible' THEN 0 WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 ELSE 3 END").bind(&row.id).fetch_all(state.db.pool()).await?;
    Ok(
        json!({"id":row.id,"provider_id":row.provider_id,"provider_name":row.provider_name,"name":row.name,"protocol":row.protocol,"protocols":protocols,"manual_enabled":row.manual_enabled,"health_check_model_id":row.health_check_model_id,"has_api_key":!row.api_key_encrypted.is_empty(),"api_key_hint":row.api_key_hint,"health":{"state":row.state.unwrap_or_else(||"active".into()),"consecutive_failures":row.consecutive_failures.unwrap_or(0),"disabled_until":row.disabled_until,"last_success_at":row.last_success_at,"last_failure_at":row.last_failure_at,"last_error_kind":row.last_error_kind,"last_status_code":row.last_status_code},"model_count":row.model_count,"created_at":row.created_at,"updated_at":row.updated_at}),
    )
}
#[derive(Deserialize)]
struct ChannelInput {
    provider_id: String,
    name: String,
    protocol: Option<String>,
    #[serde(default)]
    protocols: Vec<String>,
    api_key: String,
    #[serde(default = "yes")]
    manual_enabled: bool,
    health_check_model_id: Option<String>,
}
fn yes() -> bool {
    true
}
#[derive(Deserialize)]
struct ChannelPatch {
    name: Option<String>,
    protocol: Option<String>,
    protocols: Option<Vec<String>>,
    manual_enabled: Option<bool>,
    #[serde(default)]
    health_check_model_id: Option<String>,
}
#[derive(Deserialize)]
struct ApiKeyInput {
    api_key: String,
}
#[derive(Deserialize, Default)]
struct ChannelFilter {
    provider_id: Option<String>,
    protocol: Option<String>,
    state: Option<String>,
}
fn selected_protocols(
    protocol: Option<String>,
    protocols: Vec<String>,
) -> Result<Vec<String>, ApiError> {
    let values = if protocols.is_empty() {
        protocol.into_iter().collect()
    } else {
        protocols
    };
    let mut out = Vec::new();
    for value in values {
        if !valid_protocol(&value) {
            return Err(ApiError::validation("Unsupported protocol"));
        }
        if !out.contains(&value) {
            out.push(value);
        }
    }
    if out.is_empty() {
        return Err(ApiError::validation("At least one protocol is required"));
    }
    out.sort_by_key(|v| PROTOCOL_ORDER.iter().position(|p| p == v).unwrap_or(99));
    Ok(out)
}
async fn list_channels(
    _: AdminAuth,
    State(state): State<AppState>,
    Query(filter): Query<ChannelFilter>,
) -> ApiResult {
    let ids:Vec<String>=sqlx::query_scalar("SELECT DISTINCT c.id FROM channels c LEFT JOIN channel_protocols cp ON cp.channel_id=c.id LEFT JOIN channel_health h ON h.channel_id=c.id WHERE (? IS NULL OR c.provider_id=?) AND (? IS NULL OR cp.protocol=?) AND (? IS NULL OR h.state=?) ORDER BY c.name")
        .bind(&filter.provider_id).bind(&filter.provider_id).bind(&filter.protocol).bind(&filter.protocol).bind(&filter.state).bind(&filter.state).fetch_all(state.db.pool()).await?;
    let mut items = Vec::new();
    for row_id in ids {
        items.push(channel_json(&state, load_channel(&state, &row_id).await?).await?);
    }
    Ok(ok(
        json!({"items":items,"total":items.len(),"page":1,"page_size":items.len()}),
    ))
}
async fn create_channel(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<ChannelInput>,
) -> ApiResult {
    validate_text(&input.name, "name", 120)?;
    validate_text(&input.api_key, "api_key", 65535)?;
    let protocols = selected_protocols(input.protocol, input.protocols)?;
    let provider: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM providers WHERE id=?")
        .bind(&input.provider_id)
        .fetch_one(state.db.pool())
        .await?;
    if provider == 0 {
        return Err(ApiError::not_found("Provider not found"));
    }
    let row_id = id();
    let time = now();
    let encrypted = state.secrets.encrypt(&input.api_key);
    let hint = SecretStore::hint(&input.api_key);
    let mut tx = state.db.pool().begin().await?;
    sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,health_check_model_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(&row_id).bind(input.provider_id).bind(input.name.trim()).bind(&protocols[0]).bind(encrypted).bind(hint).bind(input.manual_enabled).bind(input.health_check_model_id).bind(&time).bind(&time).execute(&mut *tx).await.map_err(|e|integrity(e,"Channel already exists"))?;
    for protocol in protocols {
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES(?,?)")
            .bind(&row_id)
            .bind(protocol)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES(?,'active',0,?)").bind(&row_id).bind(time).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(json_response(
        StatusCode::CREATED,
        channel_json(&state, load_channel(&state, &row_id).await?).await?,
    ))
}
async fn get_channel(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    Ok(ok(channel_json(
        &state,
        load_channel(&state, &row_id).await?,
    )
    .await?))
}
async fn patch_channel(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ChannelPatch>,
) -> ApiResult {
    load_channel(&state, &row_id).await?;
    let mut tx = state.db.pool().begin().await?;
    if let Some(name) = input.name {
        validate_text(&name, "name", 120)?;
        sqlx::query("UPDATE channels SET name=?,updated_at=? WHERE id=?")
            .bind(name.trim())
            .bind(now())
            .bind(&row_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| integrity(e, "Channel already exists"))?;
    }
    if let Some(enabled) = input.manual_enabled {
        sqlx::query("UPDATE channels SET manual_enabled=?,updated_at=? WHERE id=?")
            .bind(enabled)
            .bind(now())
            .bind(&row_id)
            .execute(&mut *tx)
            .await?;
    }
    if input.health_check_model_id.is_some() {
        sqlx::query("UPDATE channels SET health_check_model_id=?,updated_at=? WHERE id=?")
            .bind(input.health_check_model_id)
            .bind(now())
            .bind(&row_id)
            .execute(&mut *tx)
            .await?;
    }
    if input.protocol.is_some() || input.protocols.is_some() {
        let protocols = selected_protocols(input.protocol, input.protocols.unwrap_or_default())?;
        let bound:i64=sqlx::query_scalar("SELECT COUNT(*) FROM route_candidates rc JOIN channel_models cm ON cm.id=rc.channel_model_id JOIN model_routes mr ON mr.id=rc.route_id WHERE cm.channel_id=? AND mr.protocol NOT IN (SELECT value FROM json_each(?))").bind(&row_id).bind(serde_json::to_string(&protocols)?).fetch_one(&mut *tx).await?;
        if bound > 0 {
            return Err(ApiError::conflict("Protocol is used by route candidates"));
        }
        sqlx::query("DELETE FROM channel_protocols WHERE channel_id=?")
            .bind(&row_id)
            .execute(&mut *tx)
            .await?;
        for protocol in &protocols {
            sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES(?,?)")
                .bind(&row_id)
                .bind(protocol)
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query("UPDATE channels SET protocol=?,updated_at=? WHERE id=?")
            .bind(&protocols[0])
            .bind(now())
            .bind(&row_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id IN (SELECT id FROM channel_models WHERE channel_id=?) AND protocol NOT IN (SELECT value FROM json_each(?))").bind(&row_id).bind(serde_json::to_string(&protocols)?).execute(&mut *tx).await?;
        for protocol in protocols {
            sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) SELECT id,? FROM channel_models WHERE channel_id=?").bind(protocol).bind(&row_id).execute(&mut *tx).await?;
        }
    }
    tx.commit().await?;
    Ok(ok(channel_json(
        &state,
        load_channel(&state, &row_id).await?,
    )
    .await?))
}
async fn replace_api_key(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ApiKeyInput>,
) -> ApiResult {
    validate_text(&input.api_key, "api_key", 65535)?;
    let result = sqlx::query(
        "UPDATE channels SET api_key_encrypted=?,api_key_hint=?,updated_at=? WHERE id=?",
    )
    .bind(state.secrets.encrypt(&input.api_key))
    .bind(SecretStore::hint(&input.api_key))
    .bind(now())
    .bind(&row_id)
    .execute(state.db.pool())
    .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("Channel not found"));
    }
    Ok(ok(
        json!({"id":row_id,"has_api_key":true,"api_key_hint":SecretStore::hint(&input.api_key)}),
    ))
}
async fn reset_health(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let time = now();
    let result=sqlx::query("UPDATE channel_health SET state='active',consecutive_failures=0,disabled_until=NULL,last_error_kind=NULL,last_status_code=NULL,updated_at=? WHERE channel_id=?").bind(time).bind(&row_id).execute(state.db.pool()).await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("Channel not found"));
    }
    Ok(ok(channel_json(
        &state,
        load_channel(&state, &row_id).await?,
    )
    .await?["health"]
        .clone()))
}
async fn delete_channel(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let mut tx = state.db.pool().begin().await?;
    sqlx::query("DELETE FROM route_candidates WHERE channel_model_id IN (SELECT id FROM channel_models WHERE channel_id=?)").bind(&row_id).execute(&mut *tx).await?;
    let result = sqlx::query("DELETE FROM channels WHERE id=?")
        .bind(&row_id)
        .execute(&mut *tx)
        .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("Channel not found"));
    }
    tx.commit().await?;
    Ok(no_content())
}

#[derive(FromRow)]
struct ModelRow {
    id: String,
    channel_id: String,
    channel_name: String,
    model_id: String,
    display_name: Option<String>,
    source: String,
    available: bool,
    last_seen_at: Option<String>,
}
async fn model_json(state: &AppState, row: ModelRow) -> Result<Value, ApiError> {
    let protocols:Vec<String>=sqlx::query_scalar("SELECT protocol FROM channel_model_protocols WHERE channel_model_id=? ORDER BY CASE protocol WHEN 'openai_compatible' THEN 0 WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 ELSE 3 END").bind(&row.id).fetch_all(state.db.pool()).await?;
    Ok(
        json!({"id":row.id,"channel_id":row.channel_id,"channel_name":row.channel_name,"protocol":protocols.first(),"protocols":protocols,"model_id":row.model_id,"display_name":row.display_name,"source":row.source,"available":row.available,"last_seen_at":row.last_seen_at}),
    )
}
#[derive(Deserialize, Default)]
struct ModelFilter {
    channel_id: Option<String>,
    protocol: Option<String>,
}
async fn list_channel_models(
    _: AdminAuth,
    State(state): State<AppState>,
    Query(filter): Query<ModelFilter>,
) -> ApiResult {
    let rows=sqlx::query_as::<_,ModelRow>("SELECT DISTINCT cm.id,cm.channel_id,c.name channel_name,cm.model_id,cm.display_name,cm.source,cm.available,cm.last_seen_at FROM channel_models cm JOIN channels c ON c.id=cm.channel_id LEFT JOIN channel_model_protocols cmp ON cmp.channel_model_id=cm.id WHERE (? IS NULL OR cm.channel_id=?) AND (? IS NULL OR cmp.protocol=?) ORDER BY cm.model_id,c.name").bind(&filter.channel_id).bind(&filter.channel_id).bind(&filter.protocol).bind(&filter.protocol).fetch_all(state.db.pool()).await?;
    let mut items = Vec::new();
    for row in rows {
        items.push(model_json(&state, row).await?);
    }
    Ok(ok(
        json!({"items":items,"total":items.len(),"page":1,"page_size":items.len()}),
    ))
}
#[derive(Deserialize)]
struct ManualModelInput {
    model_id: String,
    display_name: Option<String>,
    protocols: Option<Vec<String>>,
}
async fn create_manual_model(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(input): Json<ManualModelInput>,
) -> ApiResult {
    validate_text(&input.model_id, "model_id", 255)?;
    let _channel = load_channel(&state, &channel_id).await?;
    let available: Vec<String> =
        sqlx::query_scalar("SELECT protocol FROM channel_protocols WHERE channel_id=?")
            .bind(&channel_id)
            .fetch_all(state.db.pool())
            .await?;
    let protocols = input.protocols.unwrap_or(available);
    if protocols.is_empty() || protocols.iter().any(|p| !valid_protocol(p)) {
        return Err(ApiError::validation("Unsupported or empty protocols"));
    }
    let row_id = id();
    let time = now();
    let mut tx = state.db.pool().begin().await?;
    sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,metadata_json,first_seen_at,last_seen_at,created_at,updated_at) VALUES(?,?,?,?,'manual',1,NULL,?,?,?,?)")
        .bind(&row_id).bind(&channel_id).bind(input.model_id).bind(input.display_name).bind(&time).bind(&time).bind(&time).bind(&time)
        .execute(&mut *tx).await.map_err(|e|integrity(e,"Model already exists"))?;
    for protocol in protocols {
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)")
            .bind(&row_id)
            .bind(protocol)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    let row=sqlx::query_as::<_,ModelRow>("SELECT cm.id,cm.channel_id,c.name channel_name,cm.model_id,cm.display_name,cm.source,cm.available,cm.last_seen_at FROM channel_models cm JOIN channels c ON c.id=cm.channel_id WHERE cm.id=?").bind(&row_id).fetch_one(state.db.pool()).await?;
    Ok(json_response(
        StatusCode::CREATED,
        model_json(&state, row).await?,
    ))
}
#[derive(Deserialize)]
struct ModelPatch {
    display_name: Option<String>,
    available: Option<bool>,
    protocols: Option<Vec<String>>,
}
async fn patch_channel_model(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ModelPatch>,
) -> ApiResult {
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channel_models WHERE id=?")
        .bind(&row_id)
        .fetch_one(state.db.pool())
        .await?;
    if exists == 0 {
        return Err(ApiError::not_found("Model not found"));
    }
    let mut tx = state.db.pool().begin().await?;
    if input.display_name.is_some() {
        sqlx::query("UPDATE channel_models SET display_name=?,updated_at=? WHERE id=?")
            .bind(input.display_name)
            .bind(now())
            .bind(&row_id)
            .execute(&mut *tx)
            .await?;
    }
    if let Some(value) = input.available {
        sqlx::query("UPDATE channel_models SET available=?,updated_at=? WHERE id=?")
            .bind(value)
            .bind(now())
            .bind(&row_id)
            .execute(&mut *tx)
            .await?;
    }
    if let Some(protocols) = input.protocols {
        if protocols.is_empty() || protocols.iter().any(|p| !valid_protocol(p)) {
            return Err(ApiError::validation("Unsupported or empty protocols"));
        }
        let used:Vec<String>=sqlx::query_scalar("SELECT DISTINCT mr.protocol FROM route_candidates rc JOIN model_routes mr ON mr.id=rc.route_id WHERE rc.channel_model_id=?").bind(&row_id).fetch_all(&mut *tx).await?;
        if used.iter().any(|p| !protocols.contains(p)) {
            return Err(ApiError::conflict("Protocol is used by route candidates"));
        }
        sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id=?")
            .bind(&row_id)
            .execute(&mut *tx)
            .await?;
        for protocol in protocols {
            sqlx::query(
                "INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)",
            )
            .bind(&row_id)
            .bind(protocol)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;
    let row=sqlx::query_as::<_,ModelRow>("SELECT cm.id,cm.channel_id,c.name channel_name,cm.model_id,cm.display_name,cm.source,cm.available,cm.last_seen_at FROM channel_models cm JOIN channels c ON c.id=cm.channel_id WHERE cm.id=?").bind(&row_id).fetch_one(state.db.pool()).await?;
    Ok(ok(model_json(&state, row).await?))
}
async fn delete_channel_model(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let used: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM route_candidates WHERE channel_model_id=?")
            .bind(&row_id)
            .fetch_one(state.db.pool())
            .await?;
    if used > 0 {
        return Err(ApiError::conflict("Model is used by routes"));
    }
    let result = sqlx::query("DELETE FROM channel_models WHERE id=? AND source='manual'")
        .bind(&row_id)
        .execute(state.db.pool())
        .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::not_found("Manual model not found"));
    }
    Ok(no_content())
}

// Remaining route, mapping, telemetry, discovery, and settings handlers follow below.
#[derive(Deserialize)]
struct RouteInput {
    protocol: Option<String>,
    requested_model_id: String,
    #[serde(default = "yes")]
    enabled: bool,
}
#[derive(Deserialize)]
struct RoutePatch {
    enabled: bool,
}
#[derive(Deserialize)]
struct CandidateInput {
    channel_model_id: String,
    priority: i64,
    #[serde(default = "yes")]
    enabled: bool,
}
#[derive(Deserialize)]
struct CandidateList {
    candidates: Vec<CandidateInput>,
}

async fn route_ids_for_model(
    state: &AppState,
    model_id: &str,
) -> Result<Vec<(String, String, bool, String, String)>, ApiError> {
    Ok(sqlx::query_as::<_,(String,String,bool,String,String)>("SELECT id,protocol,enabled,requested_model_id,created_at FROM model_routes WHERE requested_model_id=? ORDER BY CASE protocol WHEN 'openai_compatible' THEN 0 WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 ELSE 3 END").bind(model_id).fetch_all(state.db.pool()).await?)
}
async fn route_bundle(state: &AppState, model_id: &str) -> Result<Value, ApiError> {
    let rows = route_ids_for_model(state, model_id).await?;
    if rows.is_empty() {
        return Err(ApiError::not_found("Route not found"));
    }
    let mut by_model: HashMap<String, Value> = HashMap::new();
    for (route_id, protocol, _, _, _) in &rows {
        let candidates=sqlx::query("SELECT rc.id,rc.channel_model_id,rc.priority,rc.enabled,cm.channel_id,c.name channel_name,p.name provider_name,h.state,c.manual_enabled FROM route_candidates rc JOIN channel_models cm ON cm.id=rc.channel_model_id JOIN channels c ON c.id=cm.channel_id JOIN providers p ON p.id=c.provider_id LEFT JOIN channel_health h ON h.channel_id=c.id WHERE rc.route_id=? ORDER BY rc.priority,c.name").bind(route_id).fetch_all(state.db.pool()).await?;
        for row in candidates {
            let cmid: String = row.try_get("channel_model_id")?;
            let candidate=by_model.entry(cmid.clone()).or_insert_with(||json!({"id":row.get::<String,_>("id"),"channel_model_id":cmid,"channel_id":row.get::<String,_>("channel_id"),"channel_name":row.get::<String,_>("channel_name"),"provider_name":row.get::<String,_>("provider_name"),"priority":row.get::<i64,_>("priority"),"enabled":row.get::<bool,_>("enabled"),"health_state":row.get::<Option<String>,_>("state").unwrap_or_else(||"active".into()),"manual_enabled":row.get::<bool,_>("manual_enabled"),"protocols":Vec::<String>::new()}));
            let object = candidate.as_object_mut().expect("candidate object");
            let current = object.get("priority").and_then(Value::as_i64).unwrap_or(0);
            object.insert(
                "priority".into(),
                json!(current.min(row.get::<i64, _>("priority"))),
            );
            let current_enabled = object
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            object.insert(
                "enabled".into(),
                json!(current_enabled && row.get::<bool, _>("enabled")),
            );
            object
                .get_mut("protocols")
                .and_then(Value::as_array_mut)
                .unwrap()
                .push(json!(protocol));
        }
    }
    let mut candidates: Vec<Value> = by_model.into_values().collect();
    candidates.sort_by_key(|value| {
        value
            .get("priority")
            .and_then(Value::as_i64)
            .unwrap_or(i64::MAX)
    });
    let route_ids = rows
        .iter()
        .map(|(id, protocol, _, _, _)| (protocol.clone(), json!(id)))
        .collect::<Map<String, Value>>();
    let enabled = rows.iter().all(|(_, _, value, _, _)| *value);
    let created = rows
        .iter()
        .map(|(_, _, _, _, time)| time.clone())
        .min()
        .unwrap_or_default();
    let caps = get_caps_value(state, model_id).await?;
    Ok(
        json!({"id":rows[0].0,"route_ids":route_ids,"protocols":rows.iter().map(|(_,p,_,_,_)|p).collect::<Vec<_>>(),"requested_model_id":model_id,"enabled":enabled,"candidates":candidates,"capabilities":caps,"created_at":created,"updated_at":now()}),
    )
}
async fn list_routes(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT requested_model_id FROM model_routes ORDER BY requested_model_id",
    )
    .fetch_all(state.db.pool())
    .await?;
    let mut items = Vec::new();
    for id in ids {
        items.push(route_bundle(&state, &id).await?);
    }
    let total = items.len();
    Ok(ok(
        json!({"items":items,"total":total,"page":1,"page_size":total}),
    ))
}
async fn create_route(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<RouteInput>,
) -> ApiResult {
    validate_text(&input.requested_model_id, "requested_model_id", 255)?;
    let protocols = if let Some(protocol) = input.protocol {
        if !valid_protocol(&protocol) {
            return Err(ApiError::validation("Unsupported protocol"));
        }
        vec![protocol]
    } else {
        let values:Vec<String>=sqlx::query_scalar("SELECT DISTINCT cmp.protocol FROM channel_models cm JOIN channel_model_protocols cmp ON cmp.channel_model_id=cm.id WHERE cm.model_id=? AND cm.available=1").bind(&input.requested_model_id).fetch_all(state.db.pool()).await?;
        PROTOCOL_ORDER
            .iter()
            .filter(|p| values.iter().any(|v| v == *p))
            .map(|p| p.to_string())
            .collect()
    };
    if protocols.is_empty() {
        return Err(ApiError::validation(
            "No available channel supports this model",
        ));
    }
    let existing:i64=sqlx::query_scalar("SELECT COUNT(*) FROM model_routes WHERE requested_model_id=? AND protocol IN (SELECT value FROM json_each(?))").bind(&input.requested_model_id).bind(serde_json::to_string(&protocols)?).fetch_one(state.db.pool()).await?;
    if existing > 0 {
        return Err(ApiError::conflict("Route already exists"));
    }
    let mut tx = state.db.pool().begin().await?;
    for protocol in protocols {
        let time = now();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?)").bind(id()).bind(protocol).bind(&input.requested_model_id).bind(input.enabled).bind(&time).bind(&time).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(json_response(
        StatusCode::CREATED,
        route_bundle(&state, &input.requested_model_id).await?,
    ))
}
async fn patch_route(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<RoutePatch>,
) -> ApiResult {
    let model: Option<String> =
        sqlx::query_scalar("SELECT requested_model_id FROM model_routes WHERE id=?")
            .bind(&row_id)
            .fetch_optional(state.db.pool())
            .await?;
    let model = model.ok_or_else(|| ApiError::not_found("Route not found"))?;
    sqlx::query("UPDATE model_routes SET enabled=?,updated_at=? WHERE requested_model_id=?")
        .bind(input.enabled)
        .bind(now())
        .bind(&model)
        .execute(state.db.pool())
        .await?;
    Ok(ok(json!({"id":row_id,"enabled":input.enabled})))
}
async fn replace_candidates(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<CandidateList>,
) -> ApiResult {
    let model: Option<String> =
        sqlx::query_scalar("SELECT requested_model_id FROM model_routes WHERE id=?")
            .bind(&row_id)
            .fetch_optional(state.db.pool())
            .await?;
    let model = model.ok_or_else(|| ApiError::not_found("Route not found"))?;
    let mut priorities = HashSet::new();
    let mut models = HashSet::new();
    for item in &input.candidates {
        if item.priority < 0
            || !priorities.insert(item.priority)
            || !models.insert(item.channel_model_id.clone())
        {
            return Err(ApiError::conflict(
                "Candidate priorities and models must be unique",
            ));
        }
    }
    let protocols: HashSet<String> = route_ids_for_model(&state, &model)
        .await?
        .into_iter()
        .map(|(_, p, _, _, _)| p)
        .collect();
    for item in &input.candidates {
        let model_info: Option<(String, bool)> =
            sqlx::query_as("SELECT model_id,available FROM channel_models WHERE id=?")
                .bind(&item.channel_model_id)
                .fetch_optional(state.db.pool())
                .await?;
        let Some((upstream, available)) = model_info else {
            return Err(ApiError::validation(
                "One or more channel models do not exist",
            ));
        };
        if upstream != model || !available {
            return Err(ApiError::validation(
                "Candidate protocol and model must match the route",
            ));
        }
    }
    let sibling = route_ids_for_model(&state, &model).await?;
    let sibling_ids: Vec<String> = sibling.iter().map(|(id, _, _, _, _)| id.clone()).collect();
    let mut tx = state.db.pool().begin().await?;
    for sibling_id in &sibling_ids {
        sqlx::query("DELETE FROM route_candidates WHERE route_id=?")
            .bind(sibling_id)
            .execute(&mut *tx)
            .await?;
    }
    for item in input.candidates {
        let supported: HashSet<String> = sqlx::query_scalar(
            "SELECT protocol FROM channel_model_protocols WHERE channel_model_id=?",
        )
        .bind(&item.channel_model_id)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .collect();
        for (sibling_id, protocol, _, _, _) in &sibling {
            if protocols.contains(protocol) && supported.contains(protocol) {
                let time = now();
                sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?)").bind(id()).bind(sibling_id).bind(&item.channel_model_id).bind(item.priority).bind(item.enabled).bind(&time).bind(&time).execute(&mut *tx).await?;
            }
        }
    }
    tx.commit().await?;
    Ok(ok(route_bundle(&state, &model).await?))
}
async fn delete_route(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let model: Option<String> =
        sqlx::query_scalar("SELECT requested_model_id FROM model_routes WHERE id=?")
            .bind(&row_id)
            .fetch_optional(state.db.pool())
            .await?;
    let model = model.ok_or_else(|| ApiError::not_found("Route not found"))?;
    sqlx::query("DELETE FROM model_routes WHERE requested_model_id=?")
        .bind(model)
        .execute(state.db.pool())
        .await?;
    Ok(no_content())
}

#[derive(Deserialize)]
struct ProfileInput {
    name: String,
    description: Option<String>,
    context_window: Option<i64>,
    max_tokens: Option<i64>,
    supports_image_input: Option<bool>,
    reasoning: Option<bool>,
    thinking_level_map: Option<Value>,
}
async fn profile_json(state: &AppState, row_id: &str) -> Result<Value, ApiError> {
    let row=sqlx::query("SELECT id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at FROM capability_profiles WHERE id=?").bind(row_id).fetch_optional(state.db.pool()).await?.ok_or_else(||ApiError::not_found("Profile not found"))?;
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT requested_model_id FROM model_caps WHERE profile_id=? ORDER BY requested_model_id",
    )
    .bind(row_id)
    .fetch_all(state.db.pool())
    .await?;
    let map: Option<String> = row.try_get("thinking_level_map")?;
    let thinking: Option<Value> = map.and_then(|value| serde_json::from_str::<Value>(&value).ok());
    Ok(
        json!({"id":row.get::<String,_>("id"),"name":row.get::<String,_>("name"),"description":row.get::<Option<String>,_>("description"),"capabilities":{"context_window":row.get::<Option<i64>,_>("context_window"),"max_tokens":row.get::<Option<i64>,_>("max_tokens"),"supports_image_input":row.get::<Option<bool>,_>("supports_image_input"),"reasoning":row.get::<Option<bool>,_>("reasoning"),"thinking_level_map":thinking},"used_by":ids,"usage_count":ids.len(),"created_at":row.get::<String,_>("created_at"),"updated_at":row.get::<String,_>("updated_at")}),
    )
}
async fn list_profiles(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM capability_profiles ORDER BY name")
        .fetch_all(state.db.pool())
        .await?;
    let mut items = Vec::new();
    for row_id in ids {
        items.push(profile_json(&state, &row_id).await?);
    }
    Ok(ok(json!({"items":items,"total":items.len()})))
}
async fn create_profile(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<ProfileInput>,
) -> ApiResult {
    validate_text(&input.name, "name", 255)?;
    let row_id = id();
    let time = now();
    sqlx::query("INSERT INTO capability_profiles(id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(&row_id).bind(input.name).bind(input.description).bind(input.context_window).bind(input.max_tokens).bind(input.supports_image_input).bind(input.reasoning).bind(input.thinking_level_map.map(|v|v.to_string())).bind(&time).bind(&time).execute(state.db.pool()).await.map_err(|e|integrity(e,"Profile already exists"))?;
    Ok(json_response(
        StatusCode::CREATED,
        profile_json(&state, &row_id).await?,
    ))
}
async fn get_profile(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    Ok(ok(profile_json(&state, &row_id).await?))
}
async fn update_profile(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ProfileInput>,
) -> ApiResult {
    profile_json(&state, &row_id).await?;
    let time = now();
    sqlx::query("UPDATE capability_profiles SET name=?,description=?,context_window=?,max_tokens=?,supports_image_input=?,reasoning=?,thinking_level_map=?,updated_at=? WHERE id=?").bind(input.name).bind(input.description).bind(input.context_window).bind(input.max_tokens).bind(input.supports_image_input).bind(input.reasoning).bind(input.thinking_level_map.map(|v|v.to_string())).bind(time).bind(&row_id).execute(state.db.pool()).await.map_err(|e|integrity(e,"Profile already exists"))?;
    let row=sqlx::query("SELECT context_window,max_tokens,supports_image_input,reasoning,thinking_level_map FROM capability_profiles WHERE id=?").bind(&row_id).fetch_one(state.db.pool()).await?;
    sqlx::query("UPDATE model_caps SET context_window=?,max_tokens=?,supports_image_input=?,reasoning=?,thinking_level_map=?,updated_at=? WHERE profile_id=?").bind(row.get::<Option<i64>,_>("context_window")).bind(row.get::<Option<i64>,_>("max_tokens")).bind(row.get::<Option<bool>,_>("supports_image_input")).bind(row.get::<Option<bool>,_>("reasoning")).bind(row.get::<Option<String>,_>("thinking_level_map")).bind(now()).bind(&row_id).execute(state.db.pool()).await?;
    Ok(ok(profile_json(&state, &row_id).await?))
}
async fn delete_profile(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    profile_json(&state, &row_id).await?;
    sqlx::query("UPDATE model_caps SET profile_id=NULL,updated_at=? WHERE profile_id=?")
        .bind(now())
        .bind(&row_id)
        .execute(state.db.pool())
        .await?;
    sqlx::query("DELETE FROM capability_profiles WHERE id=?")
        .bind(row_id)
        .execute(state.db.pool())
        .await?;
    Ok(no_content())
}

async fn get_caps_value(state: &AppState, model_id: &str) -> Result<Value, ApiError> {
    // C5: capabilities are detected on the fly for `auto` rows / missing rows,
    // mirroring the Python `get_model_caps`.
    crate::capabilities::get_model_caps(state, model_id)
        .await
        .map_err(ApiError::internal)
}
async fn get_capabilities(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(model_id): Path<String>,
) -> ApiResult {
    Ok(ok(get_caps_value(&state, &model_id).await?))
}
#[derive(Deserialize)]
struct CapsInput {
    source: Option<String>,
    profile_id: Option<String>,
    context_window: Option<i64>,
    max_tokens: Option<i64>,
    supports_image_input: Option<bool>,
    reasoning: Option<bool>,
    thinking_level_map: Option<Value>,
    cost_input: Option<f64>,
    cost_output: Option<f64>,
    cost_cache_read: Option<f64>,
    cost_cache_write: Option<f64>,
}
async fn put_capabilities(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(model_id): Path<String>,
    Json(input): Json<CapsInput>,
) -> ApiResult {
    let route:i64=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM model_routes WHERE requested_model_id=?)+(SELECT COUNT(*) FROM claude_model_mappings WHERE claude_model_id=?)+(SELECT COUNT(*) FROM codex_model_mappings WHERE codex_model_id=? )").bind(&model_id).bind(&model_id).bind(&model_id).fetch_one(state.db.pool()).await?;
    if route == 0 {
        return Err(ApiError::not_found("Route not found"));
    }
    let source = input.source.clone().unwrap_or_else(|| "manual".into());
    if !matches!(source.as_str(), "auto" | "manual") {
        return Err(ApiError::validation("Unsupported capability source"));
    }
    for (key, value) in [
        ("context_window", input.context_window),
        ("max_tokens", input.max_tokens),
    ] {
        if value.is_some_and(|value| value < 1) {
            return Err(ApiError::validation(format!("{key} must be >= 1")));
        }
    }
    for (key, value) in [
        ("cost_input", input.cost_input),
        ("cost_output", input.cost_output),
        ("cost_cache_read", input.cost_cache_read),
        ("cost_cache_write", input.cost_cache_write),
    ] {
        if value.is_some_and(|value| value < 0.0) {
            return Err(ApiError::validation(format!("{key} must be >= 0")));
        }
    }
    if let Some(map) = &input.thinking_level_map
        && let Some(object) = map.as_object()
    {
        for key in object.keys() {
            if !matches!(
                key.as_str(),
                "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
            ) {
                return Err(ApiError::validation("Unsupported thinking level"));
            }
        }
    }
    // C5: `auto` source runs the real capability detection pipeline instead
    // of writing an all-null row.
    let (values, profile_id): (Value, Option<String>) = if source == "auto" {
        (
            crate::capabilities::detect_model_capabilities(&state, &model_id)
                .await
                .map_err(ApiError::internal)?,
            None,
        )
    } else {
        let mut values = json!({
            "context_window": input.context_window,
            "max_tokens": input.max_tokens,
            "supports_image_input": input.supports_image_input,
            "reasoning": input.reasoning,
            "thinking_level_map": input.thinking_level_map,
            "cost_input": input.cost_input,
            "cost_output": input.cost_output,
            "cost_cache_read": input.cost_cache_read,
            "cost_cache_write": input.cost_cache_write,
        });
        if let Some(id) = &input.profile_id {
            let profile=sqlx::query("SELECT context_window,max_tokens,supports_image_input,reasoning,thinking_level_map FROM capability_profiles WHERE id=?").bind(id).fetch_optional(state.db.pool()).await?.ok_or_else(||ApiError::not_found("Profile not found"))?;
            if values["context_window"].is_null() {
                values["context_window"] = profile
                    .get::<Option<i64>, _>("context_window")
                    .map(Value::from)
                    .unwrap_or(Value::Null);
            }
            if values["max_tokens"].is_null() {
                values["max_tokens"] = profile
                    .get::<Option<i64>, _>("max_tokens")
                    .map(Value::from)
                    .unwrap_or(Value::Null);
            }
            if values["supports_image_input"].is_null() {
                values["supports_image_input"] = profile
                    .get::<Option<bool>, _>("supports_image_input")
                    .map(Value::from)
                    .unwrap_or(Value::Null);
            }
            if values["reasoning"].is_null() {
                values["reasoning"] = profile
                    .get::<Option<bool>, _>("reasoning")
                    .map(Value::from)
                    .unwrap_or(Value::Null);
            }
            if values["thinking_level_map"].is_null() {
                values["thinking_level_map"] = profile
                    .get::<Option<String>, _>("thinking_level_map")
                    .and_then(|text| serde_json::from_str(&text).ok())
                    .unwrap_or(Value::Null);
            }
        }
        (values, input.profile_id.clone())
    };
    let time = now();
    sqlx::query("INSERT INTO model_caps(requested_model_id,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,cost_input,cost_output,cost_cache_read,cost_cache_write,source,profile_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(requested_model_id) DO UPDATE SET context_window=excluded.context_window,max_tokens=excluded.max_tokens,supports_image_input=excluded.supports_image_input,reasoning=excluded.reasoning,thinking_level_map=excluded.thinking_level_map,cost_input=excluded.cost_input,cost_output=excluded.cost_output,cost_cache_read=excluded.cost_cache_read,cost_cache_write=excluded.cost_cache_write,source=excluded.source,profile_id=excluded.profile_id,updated_at=excluded.updated_at")
        .bind(&model_id)
        .bind(values.get("context_window").and_then(Value::as_i64))
        .bind(values.get("max_tokens").and_then(Value::as_i64))
        .bind(values.get("supports_image_input").and_then(Value::as_bool))
        .bind(values.get("reasoning").and_then(Value::as_bool))
        .bind(values.get("thinking_level_map").map(|value| serde_json::to_string(value).unwrap_or_default()))
        .bind(values.get("cost_input").and_then(Value::as_f64))
        .bind(values.get("cost_output").and_then(Value::as_f64))
        .bind(values.get("cost_cache_read").and_then(Value::as_f64))
        .bind(values.get("cost_cache_write").and_then(Value::as_f64))
        .bind(&source)
        .bind(&profile_id)
        .bind(&time)
        .bind(&time)
        .execute(state.db.pool())
        .await?;
    Ok(ok(get_caps_value(&state, &model_id).await?))
}
async fn detect_capabilities(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(model_id): Path<String>,
) -> ApiResult {
    put_capabilities(
        AdminAuth,
        State(state),
        Path(model_id),
        Json(CapsInput {
            source: Some("auto".into()),
            profile_id: None,
            context_window: None,
            max_tokens: None,
            supports_image_input: None,
            reasoning: None,
            thinking_level_map: None,
            cost_input: None,
            cost_output: None,
            cost_cache_read: None,
            cost_cache_write: None,
        }),
    )
    .await
}
#[derive(Deserialize)]
struct MappingInput {
    #[serde(alias = "claude_model_id", alias = "codex_model_id")]
    model_id: Option<String>,
    display_name: Option<String>,
    upstream_protocol: String,
    upstream_model_id: String,
    enabled: Option<bool>,
}
#[derive(Deserialize)]
struct MappingPatch {
    #[serde(alias = "claude_model_id", alias = "codex_model_id")]
    model_id: Option<String>,
    display_name: Option<String>,
    upstream_protocol: Option<String>,
    upstream_model_id: Option<String>,
    enabled: Option<bool>,
}
fn mapping_defaults(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}
async fn validate_upstream(
    state: &AppState,
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
async fn mapping_json(state: &AppState, kind: &str, row_id: &str) -> Result<Value, ApiError> {
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
async fn list_mappings(state: &AppState, kind: &str) -> Result<Vec<Value>, ApiError> {
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
async fn list_claude_mappings(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let items = list_mappings(&state, "claude").await?;
    Ok(ok(json!({"items":items,"total":items.len()})))
}
async fn create_claude_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn patch_claude_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn delete_claude_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn list_codex_mappings(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let items = list_mappings(&state, "codex").await?;
    Ok(ok(json!({"items":items,"total":items.len()})))
}
async fn create_codex_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn patch_codex_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn delete_codex_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
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
fn default_claude_presets() -> Value {
    json!({"items":[{"id":"claude-opus-5","display_name":"Claude Opus 5（默认）"},{"id":"claude-fable-5","display_name":"Claude Fable 5"},{"id":"claude-sonnet-5","display_name":"Claude Sonnet 5"},{"id":"claude-mythos-5","display_name":"Claude Mythos 5"},{"id":"claude-haiku-5","display_name":"Claude Haiku 5"},{"id":"claude-opus-4-6","display_name":"Claude Opus 4.6"},{"id":"claude-sonnet-4-5","display_name":"Claude Sonnet 4.5"}],"source":"defaults","refreshed_at":null})
}
fn default_codex_presets() -> Value {
    json!({"items":[{"id":"gpt-5-codex","display_name":"GPT-5 Codex（默认）"},{"id":"gpt-5","display_name":"GPT-5"},{"id":"gpt-5-mini","display_name":"GPT-5 Mini"},{"id":"gpt-5-nano","display_name":"GPT-5 Nano"},{"id":"o3","display_name":"o3"},{"id":"o4-mini","display_name":"o4-mini"},{"id":"gpt-4.1","display_name":"GPT-4.1"},{"id":"gpt-4o","display_name":"GPT-4o"}],"source":"defaults","refreshed_at":null})
}
async fn presets(state: &AppState, key: &str, defaults: Value) -> Result<Value, ApiError> {
    let raw: Option<String> =
        sqlx::query_scalar("SELECT CAST(value_json AS TEXT) FROM settings WHERE key=?")
            .bind(key)
            .fetch_optional(state.db.pool())
            .await?;
    if let Some(raw) = raw
        && let Ok(stored) = serde_json::from_str::<Value>(&raw)
    {
        let mut merged = defaults
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if let Some(items) = stored.get("items").and_then(Value::as_array) {
            merged.extend(items.clone());
        }
        let mut seen = HashSet::new();
        merged.retain(|item| {
            item.get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| seen.insert(id.to_owned()))
        });
        return Ok(
            json!({"items":merged,"source":"channels","refreshed_at":stored.get("refreshed_at")}),
        );
    }
    Ok(defaults)
}
async fn claude_presets(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    Ok(ok(presets(
        &state,
        "claude_presets",
        default_claude_presets(),
    )
    .await?))
}
async fn codex_presets(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    Ok(ok(presets(
        &state,
        "codex_presets",
        default_codex_presets(),
    )
    .await?))
}
async fn refresh_presets(_: AdminAuth) -> ApiResult {
    Ok(json_response(
        StatusCode::ACCEPTED,
        json!({"status":"queued"}),
    ))
}

async fn get_settings(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let values = settings::runtime_settings(&state).await?;
    let policy = settings::access_policy(&state).await?;
    Ok(ok(settings::settings_with_hints(&values, &policy)))
}
async fn patch_settings(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(mut value): Json<Value>,
) -> ApiResult {
    let submitted_admin = value
        .get("admin_access_key")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    let submitted_gateway = value
        .get("gateway_access_key")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    if let Some(object) = value.as_object_mut() {
        object.remove("admin_access_key");
        object.remove("gateway_access_key");
    }
    settings::validate_updates(&value)?;
    let current = settings::access_policy(&state).await?;
    if value.get("trust_local_network").and_then(Value::as_bool) == Some(false)
        && submitted_admin
            .as_deref()
            .unwrap_or(&current.admin_key)
            .is_empty()
    {
        return Err(ApiError::validation("关闭局域网信任前必须设置管理密钥"));
    }
    for (key, raw) in [
        ("admin_access_key", submitted_admin),
        ("gateway_access_key", submitted_gateway),
    ] {
        if let Some(value) = raw {
            settings::save_setting(
                &state,
                key,
                &json!(String::from_utf8(state.secrets.encrypt(&value)).unwrap_or_default()),
            )
            .await?;
        }
    }
    for (key, item) in value.as_object().cloned().unwrap_or_default() {
        settings::save_setting(&state, &key, &item).await?;
    }
    let values = settings::runtime_settings(&state).await?;
    let policy = settings::access_policy(&state).await?;
    Ok(ok(settings::settings_with_hints(&values, &policy)))
}
async fn generate_access_keys(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let mut bytes = [0u8; 32];
    rand::fill(&mut bytes);
    let admin = URL_SAFE_NO_PAD.encode(bytes);
    rand::fill(&mut bytes);
    let gateway = URL_SAFE_NO_PAD.encode(bytes);
    for (key, value) in [
        ("admin_access_key", &admin),
        ("gateway_access_key", &gateway),
    ] {
        settings::save_setting(
            &state,
            key,
            &json!(String::from_utf8(state.secrets.encrypt(value)).unwrap_or_default()),
        )
        .await?;
    }
    Ok(ok(
        json!({"admin_access_key":admin,"gateway_access_key":gateway,"warning":"这些密钥只在本次响应中返回，请立即保存。"}),
    ))
}
async fn system_status(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let policy = settings::access_policy(&state).await?;
    Ok(ok(
        json!({"status":"ok","database":"ok","host":state.config.host,"port":state.config.port,"telemetry_queue_size":state.telemetry.queue_size(),"telemetry_dropped":state.telemetry.dropped(),"protocols":PROTOCOL_ORDER,"trust_local_network":policy.trust_local_network}),
    ))
}
async fn system_protocols(_: AdminAuth) -> ApiResult {
    let items=PROTOCOL_ORDER.iter().map(|protocol|{let endpoints=PROTOCOL_ENDPOINTS.iter().find(|(id,_)|id==protocol).map(|(_,values)|values.to_vec()).unwrap_or_default();json!({"id":protocol,"endpoints":endpoints,"model_discovery_endpoint":if *protocol=="gemini"{"/v1beta/models"}else{"/v1/models"}})}).collect::<Vec<_>>();
    Ok(ok(json!({"items":items})))
}

#[derive(Deserialize, Default)]
struct RequestQuery {
    protocol: Option<String>,
    model_id: Option<String>,
    page: Option<i64>,
    page_size: Option<i64>,
}
async fn list_requests(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn get_request(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn clear_logs(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn list_health_probes(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let rows = sqlx::query("SELECT * FROM health_probe_logs ORDER BY started_at DESC LIMIT 200")
        .fetch_all(state.db.pool())
        .await?;
    let items=rows.iter().map(|row|json!({"id":row.get::<String,_>("id"),"channel_id":row.get::<String,_>("channel_id"),"model_id":row.get::<String,_>("model_id"),"started_at":row.get::<String,_>("started_at"),"duration_ms":row.get::<Option<i64>,_>("duration_ms"),"success":row.get::<bool,_>("success"),"status_code":row.get::<Option<i64>,_>("status_code"),"error_kind":row.get::<Option<String>,_>("error_kind"),"next_probe_at":row.get::<Option<String>,_>("next_probe_at")})).collect::<Vec<_>>();
    Ok(ok(json!({"items":items,"total":items.len()})))
}
const CACHE_PROVIDER_PROTOCOLS: [(&str, &str); 4] = [
    ("openai_compatible", "OpenAI"),
    ("openai_responses", "OpenAI"),
    ("claude", "Claude"),
    ("gemini", "Gemini"),
];
const CACHE_PROVIDER_ORDER: [&str; 3] = ["OpenAI", "Claude", "Gemini"];

fn cache_provider(protocol: &str) -> String {
    CACHE_PROVIDER_PROTOCOLS
        .iter()
        .find(|(id, _)| *id == protocol)
        .map(|(_, provider)| (*provider).to_string())
        .unwrap_or_else(|| protocol.to_string())
}

#[derive(Deserialize, Default)]
struct SummaryQuery {
    from: Option<String>,
    to: Option<String>,
}

struct TokenWindow {
    from: chrono::DateTime<Utc>,
    to: chrono::DateTime<Utc>,
}

/// Parses an offset-aware RFC3339 timestamp and normalizes it to UTC.
/// Naive timestamps without a timezone offset are rejected.
fn parse_utc_rfc3339(value: &str) -> Result<chrono::DateTime<Utc>, ()> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ())
}

/// Formats a UTC instant in the canonical representation used by
/// `token_usage.occurred_at`: fixed milliseconds, Z suffix. Range comparisons
/// are only exact when bounds share this representation.
fn format_utc_millis(value: chrono::DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Resolves the optional token time window. Both bounds must be provided
/// together (an unbounded half-open range would be ambiguous), and the range
/// must be non-empty. All bounds are normalized to UTC.
fn resolve_token_window(query: &SummaryQuery) -> Result<Option<TokenWindow>, ApiError> {
    match (&query.from, &query.to) {
        (None, None) => Ok(None),
        (Some(from), Some(to)) => {
            let from = parse_utc_rfc3339(from).map_err(|_| {
                ApiError::validation("from must be an RFC3339 timestamp with timezone")
            })?;
            let to = parse_utc_rfc3339(to).map_err(|_| {
                ApiError::validation("to must be an RFC3339 timestamp with timezone")
            })?;
            if from >= to {
                return Err(ApiError::validation("from must be earlier than to"));
            }
            Ok(Some(TokenWindow { from, to }))
        }
        _ => Err(ApiError::validation(
            "from and to must be provided together",
        )),
    }
}

/// Appends the `[from, to)` filter on `token_usage.occurred_at` to a query
/// builder, using the same canonical representation as stored values so the
/// index stays usable.
fn push_token_window<'a>(
    builder: &mut QueryBuilder<'a, sqlx::Sqlite>,
    window: &Option<TokenWindow>,
) {
    if let Some(window) = window {
        builder.push(" WHERE occurred_at >= ");
        builder.push_bind(format_utc_millis(window.from));
        builder.push(" AND occurred_at < ");
        builder.push_bind(format_utc_millis(window.to));
    }
}

async fn stats_summary(
    _: AdminAuth,
    State(state): State<AppState>,
    Query(query): Query<SummaryQuery>,
) -> ApiResult {
    let window = resolve_token_window(&query)?;
    // requests / success_rate / average_duration stay log-scoped: they are
    // intentionally NOT filtered by the token window.
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM request_logs")
        .fetch_one(state.db.pool())
        .await?;
    let success: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM request_logs WHERE outcome='success'")
            .fetch_one(state.db.pool())
            .await?;
    let avg_duration: Option<f64> =
        sqlx::query_scalar("SELECT AVG(total_duration_ms) FROM request_logs")
            .fetch_one(state.db.pool())
            .await?;
    // Token-derived fields come from the log-independent token_usage table, so
    // they survive log cleanup/expiry. The optional window filters them only.
    let mut token_query = QueryBuilder::new(
        "SELECT \
         COALESCE(SUM(cache_read_tokens),0) cache_read, \
         COALESCE(SUM(cache_write_tokens),0) cache_write, \
         COALESCE(SUM(cache_miss_input_tokens),0) cache_miss, \
         COALESCE(SUM(output_tokens),0) output_tokens, \
         AVG(first_token_ms) avg_first_token, \
         COALESCE(SUM(CASE WHEN output_tokens IS NOT NULL AND duration_ms > 0 THEN output_tokens ELSE 0 END),0) tps_tokens, \
         COALESCE(SUM(CASE WHEN output_tokens IS NOT NULL AND duration_ms > 0 THEN duration_ms ELSE 0 END),0) tps_duration \
         FROM token_usage",
    );
    push_token_window(&mut token_query, &window);
    let token_row = token_query.build().fetch_one(state.db.pool()).await?;
    let cache_read: i64 = token_row.try_get("cache_read")?;
    let cache_write: i64 = token_row.try_get("cache_write")?;
    let cache_miss: i64 = token_row.try_get("cache_miss")?;
    let output_tokens: i64 = token_row.try_get("output_tokens")?;
    let avg_first_token: Option<f64> = token_row.try_get("avg_first_token")?;
    let token_sum: i64 = token_row.try_get("tps_tokens")?;
    let duration_sum: i64 = token_row.try_get("tps_duration")?;
    let channels: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels")
        .fetch_one(state.db.pool())
        .await?;
    let active_channels: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM channel_health WHERE state='active'")
            .fetch_one(state.db.pool())
            .await?;
    let mut cache_query = QueryBuilder::new(
        "SELECT protocol, COUNT(*) request_count, \
         COALESCE(SUM(cache_read_tokens),0) cache_read, \
         COALESCE(SUM(cache_write_tokens),0) cache_write, \
         COALESCE(SUM(cache_miss_input_tokens),0) cache_miss \
         FROM token_usage",
    );
    push_token_window(&mut cache_query, &window);
    cache_query.push(" GROUP BY protocol");
    let cache_rows = cache_query.build().fetch_all(state.db.pool()).await?;
    let mut by_provider: HashMap<String, (i64, i64, i64, i64)> = HashMap::new();
    for row in cache_rows {
        let protocol: String = row.get(0);
        let request_count: i64 = row.get(1);
        let read: i64 = row.get::<Option<i64>, _>(2).unwrap_or(0);
        let write: i64 = row.get::<Option<i64>, _>(3).unwrap_or(0);
        let miss: i64 = row.get::<Option<i64>, _>(4).unwrap_or(0);
        let provider = cache_provider(&protocol);
        let entry = by_provider.entry(provider).or_insert((0, 0, 0, 0));
        entry.0 += request_count;
        entry.1 += read;
        entry.2 += write;
        entry.3 += miss;
    }
    let mut extra: Vec<String> = by_provider
        .keys()
        .filter(|provider| !CACHE_PROVIDER_ORDER.contains(&provider.as_str()))
        .cloned()
        .collect();
    extra.sort_unstable();
    let cache_provider_items: Vec<Value> = CACHE_PROVIDER_ORDER
        .iter()
        .map(|provider| provider.to_string())
        .chain(extra)
        .filter_map(|provider| {
            let (request_count, read, write, miss) = by_provider.get(&provider).copied()?;
            let total_input = read + write + miss;
            Some(json!({
                "provider": provider,
                "request_count": request_count,
                "cache_read_tokens": read,
                "cache_write_tokens": write,
                "cache_miss_input_tokens": miss,
                "total_input_tokens": total_input,
                "cache_hit_rate": if total_input > 0 {
                    Some((read as f64 / total_input as f64 * 10000.0).round() / 10000.0)
                } else {
                    None
                },
            }))
        })
        .collect();
    Ok(ok(json!({
        "requests": total,
        "success_rate": if total > 0 {
            Some((success as f64 / total as f64 * 10000.0).round() / 10000.0)
        } else {
            None
        },
        "average_duration_ms": avg_duration.map(|value| (value * 100.0).round() / 100.0),
        "average_first_token_ms": avg_first_token.map(|value| (value * 100.0).round() / 100.0),
        "average_tps": if duration_sum > 0 {
            Some((token_sum as f64 * 1000.0 / duration_sum as f64 * 1000.0).round() / 1000.0)
        } else {
            None
        },
        "cache_read_tokens": cache_read,
        "cache_write_tokens": cache_write,
        "cache_miss_input_tokens": cache_miss,
        "output_tokens": output_tokens,
        "cache_by_provider": cache_provider_items,
        "channels": channels,
        "active_channels": active_channels,
        "token_range": window.as_ref().map(|window| json!({
            "from": format_utc_millis(window.from),
            "to": format_utc_millis(window.to),
        })),
    })))
}
async fn stats_cache(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    // Token counts come from the log-independent token_usage table: clearing
    // or expiring request logs never erases them. unknown_attempts counts
    // responded attempts whose usage carried no token field at all.
    let row=sqlx::query("SELECT COALESCE(SUM(cache_read_tokens),0) cache_read,COALESCE(SUM(cache_write_tokens),0) cache_write,COALESCE(SUM(cache_miss_input_tokens),0) cache_miss,COALESCE(SUM(output_tokens),0) output_tokens,COALESCE(SUM(CASE WHEN input_tokens IS NULL AND cache_read_tokens IS NULL AND cache_write_tokens IS NULL AND cache_miss_input_tokens IS NULL AND output_tokens IS NULL THEN 1 ELSE 0 END),0) unknown FROM token_usage").fetch_one(state.db.pool()).await?;
    Ok(ok(
        json!({"cache_read_tokens":row.get::<i64,_>("cache_read"),"cache_write_tokens":row.get::<i64,_>("cache_write"),"cache_miss_input_tokens":row.get::<i64,_>("cache_miss"),"output_tokens":row.get::<i64,_>("output_tokens"),"unknown_attempts":row.get::<i64,_>("unknown")}),
    ))
}
async fn stats_models(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let rows=sqlx::query("SELECT COALESCE(model_id,'') model_id,COUNT(*) requests FROM request_logs GROUP BY model_id ORDER BY requests DESC").fetch_all(state.db.pool()).await?;
    Ok(ok(
        json!({"items":rows.iter().map(|row|json!({"model_id":row.get::<String,_>("model_id"),"requests":row.get::<i64,_>("requests")})).collect::<Vec<_>>()}),
    ))
}
async fn stats_channels(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let rows=sqlx::query("SELECT COALESCE(final_channel_id,'') channel_id,COUNT(*) requests FROM request_logs GROUP BY final_channel_id ORDER BY requests DESC").fetch_all(state.db.pool()).await?;
    Ok(ok(
        json!({"items":rows.iter().map(|row|json!({"channel_id":row.get::<String,_>("channel_id"),"requests":row.get::<i64,_>("requests")})).collect::<Vec<_>>()}),
    ))
}
#[derive(Deserialize, Default)]
struct TimeseriesQuery {
    interval: Option<String>,
}
async fn stats_timeseries(
    _: AdminAuth,
    State(state): State<AppState>,
    Query(query): Query<TimeseriesQuery>,
) -> ApiResult {
    let format = if query.interval.as_deref() == Some("day") {
        "%Y-%m-%dT00:00:00Z"
    } else {
        "%Y-%m-%dT%H:00:00Z"
    };
    let rows=sqlx::query("SELECT strftime(?,started_at) bucket,COUNT(*) requests FROM request_logs GROUP BY bucket ORDER BY bucket").bind(format).fetch_all(state.db.pool()).await?;
    Ok(ok(
        json!({"items":rows.iter().map(|row|json!({"bucket":row.get::<Option<String>,_>("bucket"),"requests":row.get::<i64,_>("requests")})).collect::<Vec<_>>()}),
    ))
}
async fn discover_models(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels WHERE id=?")
        .bind(&row_id)
        .fetch_one(state.db.pool())
        .await?;
    if exists == 0 {
        return Err(ApiError::not_found("Channel not found"));
    }
    let run_id = crate::discovery::queue(state, row_id).await?;
    Ok(json_response(
        StatusCode::ACCEPTED,
        json!({"run_id":run_id,"status":"queued"}),
    ))
}
async fn manual_probe(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels WHERE id=?")
        .bind(&row_id)
        .fetch_one(state.db.pool())
        .await?;
    if exists == 0 {
        return Err(ApiError::not_found("Channel not found"));
    }
    crate::health::queue(state, row_id).await?;
    Ok(json_response(
        StatusCode::ACCEPTED,
        json!({"status":"queued"}),
    ))
}
async fn get_discovery_run(
    _: AdminAuth,
    State(state): State<AppState>,
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
async fn list_discovery_runs(
    _: AdminAuth,
    State(state): State<AppState>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn window(from: &str, to: &str) -> SummaryQuery {
        SummaryQuery {
            from: Some(from.to_owned()),
            to: Some(to.to_owned()),
        }
    }

    #[test]
    fn utc_plus_8_local_midnight_maps_to_previous_day_16z() {
        let from = parse_utc_rfc3339("2026-08-04T00:00:00+08:00").unwrap();
        assert_eq!(format_utc_millis(from), "2026-08-03T16:00:00.000Z");
    }

    #[test]
    fn arbitrary_offsets_and_fractional_seconds_normalize_to_utc() {
        let from = parse_utc_rfc3339("2026-08-04T08:30:00Z").unwrap();
        assert_eq!(format_utc_millis(from), "2026-08-04T08:30:00.000Z");
        let from = parse_utc_rfc3339("2026-08-04T08:30:00.125+05:30").unwrap();
        assert_eq!(format_utc_millis(from), "2026-08-04T03:00:00.125Z");
        let from = parse_utc_rfc3339("2026-08-04T23:59:59.999-07:00").unwrap();
        assert_eq!(format_utc_millis(from), "2026-08-05T06:59:59.999Z");
    }

    #[test]
    fn naive_timestamp_without_timezone_is_rejected() {
        assert!(parse_utc_rfc3339("2026-08-04T08:30:00").is_err());
        assert!(parse_utc_rfc3339("2026-08-04").is_err());
    }

    #[test]
    fn canonical_occurred_at_format_is_lexicographically_ordered() {
        let values = [
            "2026-08-03T15:59:59.999Z",
            "2026-08-03T16:00:00.000Z",
            "2026-08-03T16:00:00.001Z",
            "2026-08-03T16:00:01.000Z",
        ];
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        assert_eq!(
            sorted, values,
            "fixed-millis Z format must sort chronologically"
        );
        // The half-open window start must compare equal to a stored value at
        // the same instant.
        let window = resolve_token_window(&window(
            "2026-08-04T00:00:00+08:00",
            "2026-08-05T00:00:00+08:00",
        ))
        .unwrap()
        .unwrap();
        assert_eq!(format_utc_millis(window.from), "2026-08-03T16:00:00.000Z");
        assert_eq!(format_utc_millis(window.to), "2026-08-04T16:00:00.000Z");
    }

    #[test]
    fn omitted_range_means_all_history() {
        assert!(
            resolve_token_window(&SummaryQuery::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn partial_range_is_rejected() {
        let both = resolve_token_window(&window(
            "2026-08-04T00:00:00+08:00",
            "2026-08-05T00:00:00+08:00",
        ));
        assert!(both.is_ok());
        assert!(
            resolve_token_window(&SummaryQuery {
                from: Some("2026-08-04T00:00:00Z".into()),
                to: None
            })
            .is_err()
        );
        assert!(
            resolve_token_window(&SummaryQuery {
                from: None,
                to: Some("2026-08-05T00:00:00Z".into())
            })
            .is_err()
        );
    }

    #[test]
    fn empty_or_reversed_range_is_rejected() {
        let equal = resolve_token_window(&window("2026-08-04T00:00:00Z", "2026-08-04T00:00:00Z"));
        assert!(equal.is_err());
        let reversed = resolve_token_window(&window(
            "2026-08-05T00:00:00+08:00",
            "2026-08-04T00:00:00+08:00",
        ));
        assert!(reversed.is_err());
    }

    #[test]
    fn boundary_values_keep_half_open_semantics() {
        // A stored occurred_at exactly at `from` must compare >= the bound,
        // and one exactly at `to` must compare < the bound.
        let window = resolve_token_window(&window(
            "2026-08-03T16:00:00.000Z",
            "2026-08-04T16:00:00.000Z",
        ))
        .unwrap()
        .unwrap();
        let from = format_utc_millis(window.from);
        let to = format_utc_millis(window.to);
        assert!("2026-08-03T16:00:00.000Z" >= from.as_str());
        assert!("2026-08-04T15:59:59.999Z" < to.as_str());
        assert!(
            "2026-08-04T16:00:00.000Z" >= to.as_str(),
            "end is exclusive"
        );
        assert!(
            "2026-08-03T15:59:59.999Z" < from.as_str(),
            "start is inclusive"
        );
    }
}
