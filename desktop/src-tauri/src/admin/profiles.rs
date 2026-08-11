//! admin API 域模块：能力画像与能力检测（capability-profiles 域）
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

#[derive(Deserialize)]
pub(super) struct ProfileInput {
    pub name: String,
    pub description: Option<String>,
    pub context_window: Option<i64>,
    pub max_tokens: Option<i64>,
    pub supports_image_input: Option<bool>,
    pub reasoning: Option<bool>,
    pub thinking_level_map: Option<Value>,
}
pub(super) async fn profile_json(state: &Context, row_id: &str) -> Result<Value, ApiError> {
    let row=sqlx::query("SELECT id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at FROM capability_profiles WHERE id=?").bind(row_id).fetch_optional(state.db.pool()).await?.ok_or_else(||ApiError::not_found("Profile not found"))?;
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT requested_model_id FROM model_caps WHERE profile_id=? ORDER BY requested_model_id",
    )
    .bind(row_id)
    .fetch_all(state.db.pool())
    .await?;
    let map: Option<String> = row.try_get("thinking_level_map")?;
    // P2-4: persisted JSON that fails to decode is corruption, not "no
    // configuration" — name the table/key and surface a config_corrupted
    // error instead of silently returning null.
    let thinking: Option<Value> = match map {
        Some(value) => Some(
            serde_json::from_str::<Value>(&value).map_err(|error| {
                tracing::error!(
                    table = "capability_profiles",
                    field = "thinking_level_map",
                    row_id,
                    %error,
                    "persisted JSON corrupt"
                );
                ApiError::config_corrupted_with(
                    "capability_profiles.thinking_level_map 数据损坏，请重新保存该画像",
                )
            })?,
        ),
        None => None,
    };
    Ok(
        json!({"id":row.get::<String,_>("id"),"name":row.get::<String,_>("name"),"description":row.get::<Option<String>,_>("description"),"capabilities":{"context_window":row.get::<Option<i64>,_>("context_window"),"max_tokens":row.get::<Option<i64>,_>("max_tokens"),"supports_image_input":row.get::<Option<bool>,_>("supports_image_input"),"reasoning":row.get::<Option<bool>,_>("reasoning"),"thinking_level_map":thinking},"used_by":ids,"usage_count":ids.len(),"created_at":row.get::<String,_>("created_at"),"updated_at":row.get::<String,_>("updated_at")}),
    )
}
pub(super) async fn list_profiles(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM capability_profiles ORDER BY name")
        .fetch_all(state.db.pool())
        .await?;
    let mut items = Vec::new();
    for row_id in ids {
        items.push(profile_json(&state, &row_id).await?);
    }
    Ok(ok(json!({"items":items,"total":items.len()})))
}

/// P2-4: capability values are validated on write — a zero/negative window
/// and a non-object thinking map are rejected with 422 before any SQL.
pub(super) fn validate_capability_input(
    name: &str,
    context_window: Option<i64>,
    max_tokens: Option<i64>,
    thinking_level_map: Option<&Value>,
) -> Result<(), ApiError> {
    validate_text(name, "name", 255)?;
    if context_window.is_some_and(|value| value < 1) {
        return Err(ApiError::validation("context_window must be >= 1"));
    }
    if max_tokens.is_some_and(|value| value < 1) {
        return Err(ApiError::validation("max_tokens must be >= 1"));
    }
    if thinking_level_map.is_some_and(|value| !value.is_object()) {
        return Err(ApiError::validation("thinking_level_map must be an object"));
    }
    Ok(())
}

pub(super) async fn create_profile(
    _: AdminAuth,
    State(state): State<Context>,
    Json(input): Json<ProfileInput>,
) -> ApiResult {
    validate_capability_input(
        &input.name,
        input.context_window,
        input.max_tokens,
        input.thinking_level_map.as_ref(),
    )?;
    let row_id = id();
    let time = now();
    sqlx::query("INSERT INTO capability_profiles(id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(&row_id).bind(input.name).bind(input.description).bind(input.context_window).bind(input.max_tokens).bind(input.supports_image_input).bind(input.reasoning).bind(input.thinking_level_map.map(|v|v.to_string())).bind(&time).bind(&time).execute(state.db.pool()).await.map_err(|e|integrity(e,"Profile already exists"))?;
    Ok(json_response(
        StatusCode::CREATED,
        profile_json(&state, &row_id).await?,
    ))
}
pub(super) async fn get_profile(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    Ok(ok(profile_json(&state, &row_id).await?))
}
pub(super) async fn update_profile(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
    Json(input): Json<ProfileInput>,
) -> ApiResult {
    profile_json(&state, &row_id).await?;
    validate_capability_input(
        &input.name,
        input.context_window,
        input.max_tokens,
        input.thinking_level_map.as_ref(),
    )?;
    // P1-4: the profile row and every model_caps row that references it
    // commit in ONE transaction — a failure in the propagation can never
    // leave the profile changed and the caps stale. No network calls run
    // inside the transaction.
    let time = now();
    let mut tx = state.db.pool().begin().await?;
    sqlx::query("UPDATE capability_profiles SET name=?,description=?,context_window=?,max_tokens=?,supports_image_input=?,reasoning=?,thinking_level_map=?,updated_at=? WHERE id=?").bind(input.name).bind(input.description).bind(input.context_window).bind(input.max_tokens).bind(input.supports_image_input).bind(input.reasoning).bind(input.thinking_level_map.map(|v|v.to_string())).bind(&time).bind(&row_id).execute(&mut *tx).await.map_err(|e|integrity(e,"Profile already exists"))?;
    let row=sqlx::query("SELECT context_window,max_tokens,supports_image_input,reasoning,thinking_level_map FROM capability_profiles WHERE id=?").bind(&row_id).fetch_one(&mut *tx).await?;
    sqlx::query("UPDATE model_caps SET context_window=?,max_tokens=?,supports_image_input=?,reasoning=?,thinking_level_map=?,updated_at=? WHERE profile_id=?").bind(row.get::<Option<i64>,_>("context_window")).bind(row.get::<Option<i64>,_>("max_tokens")).bind(row.get::<Option<bool>,_>("supports_image_input")).bind(row.get::<Option<bool>,_>("reasoning")).bind(row.get::<Option<String>,_>("thinking_level_map")).bind(&time).bind(&row_id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(ok(profile_json(&state, &row_id).await?))
}
pub(super) async fn delete_profile(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    profile_json(&state, &row_id).await?;
    let mut tx = state.db.pool().begin().await?;
    sqlx::query("UPDATE model_caps SET profile_id=NULL,updated_at=? WHERE profile_id=?")
        .bind(now())
        .bind(&row_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM capability_profiles WHERE id=?")
        .bind(row_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(no_content())
}

pub(super) async fn get_caps_value(state: &Context, model_id: &str) -> Result<Value, ApiError> {
    // C5: capabilities are detected on the fly for `auto` rows / missing rows,
    // mirroring the Python `get_model_caps`.
    crate::capabilities::get_model_caps(state, model_id)
        .await
        .map_err(ApiError::internal)
}
pub(super) async fn get_capabilities(
    _: AdminAuth,
    State(state): State<Context>,
    Path(model_id): Path<String>,
) -> ApiResult {
    Ok(ok(get_caps_value(&state, &model_id).await?))
}
#[derive(Deserialize)]
pub(super) struct CapsInput {
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
pub(super) async fn put_capabilities(
    _: AdminAuth,
    State(state): State<Context>,
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
                // P2-4: a corrupt stored map is surfaced, never silently
                // treated as "no configuration".
                values["thinking_level_map"] = match profile
                    .get::<Option<String>, _>("thinking_level_map")
                {
                    Some(text) => serde_json::from_str::<Value>(&text).map_err(|error| {
                        tracing::error!(
                            table = "capability_profiles",
                            field = "thinking_level_map",
                            profile_id = %id,
                            %error,
                            "persisted JSON corrupt"
                        );
                        ApiError::config_corrupted_with(
                            "capability_profiles.thinking_level_map 数据损坏，请重新保存该画像",
                        )
                    })?,
                    None => Value::Null,
                };
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
pub(super) async fn detect_capabilities(
    _: AdminAuth,
    State(state): State<Context>,
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
