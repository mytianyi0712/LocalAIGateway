//! admin API 域模块：能力画像与能力检测（capability-profiles 域）。
//!
//! 画像（`capability_profiles`）落到 `model_caps` 时在**同一事务**内传播，
//! 失败即整体回滚；持久化的 `thinking_level_map` 解不出 JSON 属数据损坏，
//! 报 `config_corrupted` 而不是当成「无配置」。SQL 都在同文件的
//! `impl AdminService`；`get_caps_value` 需要能力检测服务，接受 `&Context`。

use super::{AdminService, ApiResult, id, integrity, json_response, no_content, now, ok, validate_text};
use crate::api_error::ApiError;
use crate::application::Context;
use crate::auth::AdminAuth;
use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;

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

/// 能力值在写入前校验：`context_window` / `max_tokens` 非正数、`thinking_level_map`
/// 不是对象，都在进入 SQL 之前返回 422。
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

pub(super) async fn list_profiles(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    state.admin.list_profiles().await
}
pub(super) async fn create_profile(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<ProfileInput>,
) -> ApiResult {
    state.admin.create_profile(input).await
}
pub(super) async fn get_profile(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.get_profile(&row_id).await
}
pub(super) async fn update_profile(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ProfileInput>,
) -> ApiResult {
    state.admin.update_profile(&row_id, input).await
}
pub(super) async fn delete_profile(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_profile(&row_id).await
}
pub(super) async fn get_capabilities(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(model_id): Path<String>,
) -> ApiResult {
    Ok(ok(get_caps_value(&state, &model_id).await?))
}
pub(super) async fn put_capabilities(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(model_id): Path<String>,
    Json(input): Json<CapsInput>,
) -> ApiResult {
    state.admin.put_capabilities(&state, &model_id, input).await
}
pub(super) async fn detect_capabilities(
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

pub(super) async fn get_caps_value(state: &Context, model_id: &str) -> Result<Value, ApiError> {
    // 表现为 `auto` 来源或整行缺失时，实时跑一次能力检测（不落库）。
    crate::capabilities::get_model_caps(state, model_id)
        .await
        .map_err(ApiError::internal)
}

impl AdminService {
    /// 能力画像详情：画像字段 + 引用它的模型清单。
    pub(super) async fn profile_json(&self, row_id: &str) -> Result<Value, ApiError> {
        let row=sqlx::query("SELECT id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at FROM capability_profiles WHERE id=?").bind(row_id).fetch_optional(self.db.pool()).await?.ok_or_else(||ApiError::not_found("Profile not found"))?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT requested_model_id FROM model_caps WHERE profile_id=? ORDER BY requested_model_id",
        )
        .bind(row_id)
        .fetch_all(self.db.pool())
        .await?;
        let map: Option<String> = row.try_get("thinking_level_map")?;
        // 持久化的 JSON 解不出来属数据损坏，而不是「没有配置」：带上表名/字段名
        // 返回 `config_corrupted`，不静默当成 null。
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

    pub(super) async fn list_profiles(&self) -> ApiResult {
        let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM capability_profiles ORDER BY name")
            .fetch_all(self.db.pool())
            .await?;
        let mut items = Vec::new();
        for row_id in ids {
            items.push(self.profile_json(&row_id).await?);
        }
        Ok(ok(json!({"items":items,"total":items.len()})))
    }

    pub(super) async fn create_profile(&self, input: ProfileInput) -> ApiResult {
        validate_capability_input(
            &input.name,
            input.context_window,
            input.max_tokens,
            input.thinking_level_map.as_ref(),
        )?;
        let row_id = id();
        let time = now();
        sqlx::query("INSERT INTO capability_profiles(id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(&row_id).bind(input.name).bind(input.description).bind(input.context_window).bind(input.max_tokens).bind(input.supports_image_input).bind(input.reasoning).bind(input.thinking_level_map.map(|v|v.to_string())).bind(&time).bind(&time).execute(self.db.pool()).await.map_err(|e|integrity(e,"Profile already exists"))?;
        Ok(json_response(
            StatusCode::CREATED,
            self.profile_json(&row_id).await?,
        ))
    }

    pub(super) async fn get_profile(&self, row_id: &str) -> ApiResult {
        Ok(ok(self.profile_json(row_id).await?))
    }

    pub(super) async fn update_profile(&self, row_id: &str, input: ProfileInput) -> ApiResult {
        self.profile_json(row_id).await?;
        validate_capability_input(
            &input.name,
            input.context_window,
            input.max_tokens,
            input.thinking_level_map.as_ref(),
        )?;
        // 画像行与所有引用它的 model_caps 行在同一事务内提交：传播失败不会留下
        // 「画像已改、能力仍旧」的中间态。事务内不发起任何网络调用。
        let time = now();
        let mut tx = self.db.pool().begin().await?;
        sqlx::query("UPDATE capability_profiles SET name=?,description=?,context_window=?,max_tokens=?,supports_image_input=?,reasoning=?,thinking_level_map=?,updated_at=? WHERE id=?").bind(input.name).bind(input.description).bind(input.context_window).bind(input.max_tokens).bind(input.supports_image_input).bind(input.reasoning).bind(input.thinking_level_map.map(|v|v.to_string())).bind(&time).bind(row_id).execute(&mut *tx).await.map_err(|e|integrity(e,"Profile already exists"))?;
        let row=sqlx::query("SELECT context_window,max_tokens,supports_image_input,reasoning,thinking_level_map FROM capability_profiles WHERE id=?").bind(row_id).fetch_one(&mut *tx).await?;
        sqlx::query("UPDATE model_caps SET context_window=?,max_tokens=?,supports_image_input=?,reasoning=?,thinking_level_map=?,updated_at=? WHERE profile_id=?").bind(row.get::<Option<i64>,_>("context_window")).bind(row.get::<Option<i64>,_>("max_tokens")).bind(row.get::<Option<bool>,_>("supports_image_input")).bind(row.get::<Option<bool>,_>("reasoning")).bind(row.get::<Option<String>,_>("thinking_level_map")).bind(&time).bind(row_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(ok(self.profile_json(row_id).await?))
    }

    pub(super) async fn delete_profile(&self, row_id: &str) -> ApiResult {
        self.profile_json(row_id).await?;
        let mut tx = self.db.pool().begin().await?;
        sqlx::query("UPDATE model_caps SET profile_id=NULL,updated_at=? WHERE profile_id=?")
            .bind(now())
            .bind(row_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM capability_profiles WHERE id=?")
            .bind(row_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(no_content())
    }

    /// 写入某请求模型的能力配置：`source=auto` 走真实检测，`manual` 用请求值
    /// 并在缺省时用画像补齐；随后 upsert 单行 `model_caps`。
    /// `state` 仅用于能力检测（按仓库既有约定传入，本服务不持有）。
    pub(super) async fn put_capabilities(
        &self,
        state: &Context,
        model_id: &str,
        input: CapsInput,
    ) -> ApiResult {
        let (routes, claude, codex) = self.entry_model_reference_counts(model_id).await?;
        if routes + claude + codex == 0 {
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
        // `auto` 来源走真实的能力检测流程，而不是写一行全 null 的配置。
        let (values, profile_id): (Value, Option<String>) = if source == "auto" {
            (
                crate::capabilities::detect_model_capabilities(state, model_id)
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
                let profile=sqlx::query("SELECT context_window,max_tokens,supports_image_input,reasoning,thinking_level_map FROM capability_profiles WHERE id=?").bind(id).fetch_optional(self.db.pool()).await?.ok_or_else(||ApiError::not_found("Profile not found"))?;
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
                    // 存储的映射损坏时明确报错，绝不静默当成「没有配置」。
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
            .bind(model_id)
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
            .execute(self.db.pool())
            .await?;
        Ok(ok(get_caps_value(state, model_id).await?))
    }
}
