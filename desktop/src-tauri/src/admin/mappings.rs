//! admin API 域模块：Claude/Codex 映射与预设（mappings/presets 域）。
//!
//! 两张映射表（`claude_model_mappings` / `codex_model_mappings`）结构同构，
//! 仅表名与主键列名不同：SQL 用 `format!` 拼表名，claude 与 codex 的 handler
//! 只是同一服务方法的不同 `kind`。映射必须指向存在的上游路由（创建/更新前
//! 校验）。预设 = 内置默认项 ∪ 当前渠道按协议暴露的可用模型（按模型名去重），
//! 刷新预设会为每个已启用（`manual_enabled = 1`）且暴露目标协议的渠道排队
//! 一次真实 discovery。

use super::{AdminService, ApiResult, id, integrity, json_response, no_content, now, ok, validate_text};
use crate::api_error::ApiError;
use crate::application::Context;
use crate::auth::AdminAuth;
use crate::protocol::valid_protocol;
use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use std::collections::HashSet;

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
pub(super) fn default_claude_presets() -> Value {
    json!({"items":[{"id":"claude-opus-5","display_name":"Claude Opus 5（默认）"},{"id":"claude-fable-5","display_name":"Claude Fable 5"},{"id":"claude-sonnet-5","display_name":"Claude Sonnet 5"},{"id":"claude-mythos-5","display_name":"Claude Mythos 5"},{"id":"claude-haiku-5","display_name":"Claude Haiku 5"},{"id":"claude-opus-4-6","display_name":"Claude Opus 4.6"},{"id":"claude-sonnet-4-5","display_name":"Claude Sonnet 4.5"}],"source":"defaults","refreshed_at":null})
}
pub(super) fn default_codex_presets() -> Value {
    json!({"items":[{"id":"gpt-5-codex","display_name":"GPT-5 Codex（默认）"},{"id":"gpt-5","display_name":"GPT-5"},{"id":"gpt-5-mini","display_name":"GPT-5 Mini"},{"id":"gpt-5-nano","display_name":"GPT-5 Nano"},{"id":"o3","display_name":"o3"},{"id":"o4-mini","display_name":"o4-mini"},{"id":"gpt-4.1","display_name":"GPT-4.1"},{"id":"gpt-4o","display_name":"GPT-4o"}],"source":"defaults","refreshed_at":null})
}
/// 这些协议的渠道为 Claude 预设列表提供模型。
const CLAUDE_PRESET_PROTOCOLS: &[&str] = &["claude"];
/// 这些协议的渠道为 Codex 预设列表提供模型：Codex CLI 讲 Responses API，
/// openai_compatible 渠道提供同一批模型。
const CODEX_PRESET_PROTOCOLS: &[&str] = &["openai_responses", "openai_compatible"];

pub(super) async fn list_claude_mappings(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let items = state.admin.list_mappings(&state, "claude").await?;
    Ok(ok(json!({"items":items,"total":items.len()})))
}
pub(super) async fn create_claude_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<MappingInput>,
) -> ApiResult {
    state.admin.create_mapping(&state, "claude", input).await
}
pub(super) async fn patch_claude_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<MappingPatch>,
) -> ApiResult {
    state.admin.patch_mapping(&state, "claude", &row_id, input).await
}
pub(super) async fn delete_claude_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_mapping("claude", &row_id).await
}
pub(super) async fn list_codex_mappings(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let items = state.admin.list_mappings(&state, "codex").await?;
    Ok(ok(json!({"items":items,"total":items.len()})))
}
pub(super) async fn create_codex_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<MappingInput>,
) -> ApiResult {
    state.admin.create_mapping(&state, "codex", input).await
}
pub(super) async fn patch_codex_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<MappingPatch>,
) -> ApiResult {
    state.admin.patch_mapping(&state, "codex", &row_id, input).await
}
pub(super) async fn delete_codex_mapping(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_mapping("codex", &row_id).await
}
pub(super) async fn claude_presets(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    Ok(ok(state
        .admin
        .presets(CLAUDE_PRESET_PROTOCOLS, default_claude_presets())
        .await?))
}
pub(super) async fn codex_presets(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    Ok(ok(state
        .admin
        .presets(CODEX_PRESET_PROTOCOLS, default_codex_presets())
        .await?))
}
pub(super) async fn refresh_claude_presets(
    _: AdminAuth,
    State(state): State<AppState>,
) -> ApiResult {
    state
        .admin
        .refresh_presets_for(&state, "Claude", CLAUDE_PRESET_PROTOCOLS)
        .await
}
pub(super) async fn refresh_codex_presets(
    _: AdminAuth,
    State(state): State<AppState>,
) -> ApiResult {
    state
        .admin
        .refresh_presets_for(&state, "Codex", CODEX_PRESET_PROTOCOLS)
        .await
}

/// 映射表与主键列名：claude/codex 两张同构表按 `kind` 选择。
fn mapping_table(kind: &str) -> (&'static str, &'static str) {
    if kind == "claude" {
        ("claude_model_mappings", "claude_model_id")
    } else {
        ("codex_model_mappings", "codex_model_id")
    }
}

impl AdminService {
    /// 校验上游（协议合法且存在对应路由）后才能挂映射。
    pub(super) async fn validate_upstream(
        &self,
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
        .fetch_one(self.db.pool())
        .await?;
        if count == 0 {
            return Err(ApiError::validation("Upstream model route not found"));
        }
        Ok(())
    }

    /// 映射详情：协议专属的模型键（`claude_model_id` / `codex_model_id`）读出，
    /// 同时保留兼容别名 `model_id`；`candidates` 复用上游路由详情。
    pub(super) async fn mapping_json(
        &self,
        state: &Context,
        kind: &str,
        row_id: &str,
    ) -> Result<Value, ApiError> {
        let (table, idcol) = mapping_table(kind);
        let sql = format!(
            "SELECT id,{idcol} model_id,display_name,upstream_protocol,upstream_model_id,enabled,created_at,updated_at FROM {table} WHERE id=?"
        );
        let row = sqlx::query(&sql)
            .bind(row_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or_else(|| ApiError::not_found("Mapping not found"))?;
        let upstream_protocol: String = row.get("upstream_protocol");
        let upstream_model_id: String = row.get("upstream_model_id");
        let candidates = self
            .route_bundle(state, &upstream_model_id)
            .await
            .ok()
            .and_then(|v| v.get("candidates").cloned())
            .unwrap_or_else(|| json!([]));
        let model_id: String = row.get("model_id");
        // 管理端界面读取协议专属键（`claude_model_id` / `codex_model_id`），
        // 因此两者都要写入；`model_id` 作为兼容别名保留。
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

    pub(super) async fn list_mappings(
        &self,
        state: &Context,
        kind: &str,
    ) -> Result<Vec<Value>, ApiError> {
        let (table, idcol) = mapping_table(kind);
        let sql = format!("SELECT id FROM {table} ORDER BY {idcol}");
        let ids: Vec<String> = sqlx::query_scalar(&sql).fetch_all(self.db.pool()).await?;
        let mut items = Vec::new();
        for row_id in ids {
            items.push(self.mapping_json(state, kind, &row_id).await?);
        }
        Ok(items)
    }

    pub(super) async fn create_mapping(
        &self,
        state: &Context,
        kind: &str,
        input: MappingInput,
    ) -> ApiResult {
        self.validate_upstream(&input.upstream_protocol, &input.upstream_model_id)
            .await?;
        let (table, idcol) = mapping_table(kind);
        let model_id = input
            .model_id
            .ok_or_else(|| ApiError::validation(format!("{idcol} is required")))?;
        validate_text(&model_id, idcol, 255)?;
        let row_id = id();
        let time = now();
        let sql = format!("INSERT INTO {table}(id,{idcol},display_name,upstream_protocol,upstream_model_id,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)");
        sqlx::query(&sql).bind(&row_id).bind(model_id).bind(input.display_name).bind(input.upstream_protocol).bind(input.upstream_model_id).bind(mapping_defaults(input.enabled)).bind(&time).bind(&time).execute(self.db.pool()).await.map_err(|e|integrity(e,"Mapping already exists"))?;
        Ok(json_response(
            StatusCode::CREATED,
            self.mapping_json(state, kind, &row_id).await?,
        ))
    }

    pub(super) async fn patch_mapping(
        &self,
        state: &Context,
        kind: &str,
        row_id: &str,
        input: MappingPatch,
    ) -> ApiResult {
        let current = self.mapping_json(state, kind, row_id).await?;
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
        self.validate_upstream(&protocol, &upstream).await?;
        let (table, idcol) = mapping_table(kind);
        let time = now();
        let sql = format!("UPDATE {table} SET {idcol}=?,display_name=?,upstream_protocol=?,upstream_model_id=?,enabled=?,updated_at=? WHERE id=?");
        sqlx::query(&sql).bind(model_id).bind(input.display_name.or_else(||current.get("display_name").and_then(Value::as_str).map(str::to_owned))).bind(protocol).bind(upstream).bind(input.enabled.unwrap_or_else(||current.get("enabled").and_then(Value::as_bool).unwrap_or(true))).bind(time).bind(row_id).execute(self.db.pool()).await.map_err(|e|integrity(e,"Mapping already exists"))?;
        Ok(ok(self.mapping_json(state, kind, row_id).await?))
    }

    pub(super) async fn delete_mapping(&self, kind: &str, row_id: &str) -> ApiResult {
        let (table, _) = mapping_table(kind);
        let sql = format!("DELETE FROM {table} WHERE id=?");
        let result = sqlx::query(&sql)
            .bind(row_id)
            .execute(self.db.pool())
            .await?;
        if result.rows_affected() == 0 {
            return Err(ApiError::not_found("Mapping not found"));
        }
        Ok(no_content())
    }

    /// 预设聚合：内置默认项 ∪ 目标协议下当前可用渠道模型，按模型 id 去重。
    /// `channel_models + channel_model_protocols` 是唯一事实来源，没有第二份
    /// 容易过期的设置缓存。
    pub(super) async fn presets(
        &self,
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
        let rows = query.fetch_all(self.db.pool()).await?;
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
        .fetch_one(self.db.pool())
        .await?;
        Ok(json!({"items": items, "source": "channels", "refreshed_at": refreshed_at}))
    }

    /// 为每个暴露了目标协议之一的已启用渠道各排一次真实 discovery，返回 run id
    /// 列表；没有合格渠道时直接报错，而不是假装已入队。
    pub(super) async fn refresh_presets_for(
        &self,
        state: &AppState,
        kind: &str,
        protocols: &[&str],
    ) -> ApiResult {
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
        let channels: Vec<String> = query.fetch_all(self.db.pool()).await?;
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

    /// 入口模型被引用的次数：`(路由, claude 映射, codex 映射)`。
    /// 能力配置只能写给「至少被引用一次」的模型。
    pub(super) async fn entry_model_reference_counts(
        &self,
        model_id: &str,
    ) -> Result<(i64, i64, i64), ApiError> {
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM model_routes WHERE requested_model_id=?) \
             ,(SELECT COUNT(*) FROM claude_model_mappings WHERE claude_model_id=?) \
             ,(SELECT COUNT(*) FROM codex_model_mappings WHERE codex_model_id=?)",
        )
        .bind(model_id)
        .bind(model_id)
        .bind(model_id)
        .fetch_one(self.db.pool())
        .await?;
        Ok(counts)
    }
}
