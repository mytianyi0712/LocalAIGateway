//! admin API 域模块：渠道 CRUD 与健康重置（channel 域）。
//!
//! handler 只做参数提取与响应透传；全部 SQL、校验与响应组装都在同文件的
//! `impl AdminService` 内。渠道 DTO（`ChannelRow`/`ChannelInput`/`ChannelPatch`
//! 等）与 `yes`、`selected_protocols` 校验助手也留在本文件。

use super::{AdminService, ApiResult, id, integrity, json_response, no_content, now, ok, validate_text};
use crate::api_error::ApiError;
use crate::auth::AdminAuth;
use crate::commandcode_login::CommandCodeLogin;
use crate::crypto::SecretStore;
use crate::protocol::{PROTOCOL_ORDER, valid_protocol};
use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::FromRow;

#[derive(FromRow)]
pub(super) struct ChannelRow {
    id: String,
    provider_id: String,
    provider_name: String,
    provider_kind: Option<String>,
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
#[derive(Deserialize)]
pub struct ChannelInput {
    pub provider_id: String,
    pub name: String,
    pub protocol: Option<String>,
    #[serde(default)]
    pub protocols: Vec<String>,
    /// 手动粘贴的 API Key；使用 `login_id` 时可留空。
    #[serde(default)]
    pub api_key: String,
    /// Command Code 网页登录的一次性交接句柄（`POST /command-code/login`
    /// 成功后返回）。服务端用它取走已校验的密钥，密钥绝不经过浏览器。
    #[serde(default)]
    pub login_id: Option<String>,
    #[serde(default = "yes")]
    pub manual_enabled: bool,
    pub health_check_model_id: Option<String>,
}
pub(super) fn yes() -> bool {
    true
}
#[derive(Deserialize)]
pub struct ChannelPatch {
    name: Option<String>,
    protocol: Option<String>,
    protocols: Option<Vec<String>>,
    manual_enabled: Option<bool>,
    /// 可选的新 API Key 与补丁的其它字段在同一事务内提交：保存渠道表单不会再
    /// 出现「字段已存、密钥被拒」的半成品状态。缺省或 `null` 表示保持不变。
    #[serde(default, deserialize_with = "patch_optional_string")]
    api_key: Option<Option<String>>,
    /// 同上：网页登录成功后的密钥交接；与显式 api_key 同时给出时以本字段为准。
    #[serde(default)]
    login_id: Option<String>,
    /// 三种状态：`None`（缺省）不改动取值，`Some(None)`（JSON `null`）清回自动，
    /// `Some(Some(model))` 在确认模型属于该渠道后写入。普通 `Option<String>` 会
    /// 把 `null` 与缺省合并成 `None`，因此该字段使用专用解码器。
    #[serde(default, deserialize_with = "patch_optional_string")]
    health_check_model_id: Option<Option<String>>,
}

/// 区分「缺省」与 `null` 的解码器：serde 派生的 `Option<T>` 会把两者都变成
/// `None`，那样就无法把已存值清回自动。
fn patch_optional_string<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // 先反序列化成普通 Value：直接解 `Option<Value>` 会在下面的 match 之前
    // 就把 null 折成 None。
    let raw = serde_json::Value::deserialize(deserializer)?;
    if raw.is_null() {
        Ok(Some(None))
    } else {
        serde_json::from_value(raw)
            .map(Some)
            .map_err(serde::de::Error::custom)
    }
}
#[derive(Deserialize)]
pub struct ApiKeyInput {
    api_key: String,
}
#[derive(Deserialize, Default)]
pub struct ChannelFilter {
    provider_id: Option<String>,
    protocol: Option<String>,
    state: Option<String>,
}
pub(super) fn selected_protocols(
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
pub(super) async fn list_channels(
    _: AdminAuth,
    State(state): State<AppState>,
    Query(filter): Query<ChannelFilter>,
) -> ApiResult {
    state.admin.list_channels(filter).await
}
pub(super) async fn create_channel(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<ChannelInput>,
) -> ApiResult {
    // login_id 的 peek/consume 由服务层统一处理（失败不消耗交接）。
    state
        .admin
        .create_channel(input, &state.command_code_login)
        .await
}
pub(super) async fn get_channel(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.get_channel(&row_id).await
}
pub(super) async fn patch_channel(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ChannelPatch>,
) -> ApiResult {
    // login_id 的 peek/consume 由服务层统一处理（失败不消耗交接）。
    state
        .admin
        .patch_channel(&row_id, input, &state.command_code_login)
        .await
}
pub(super) async fn replace_api_key(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ApiKeyInput>,
) -> ApiResult {
    state.admin.replace_api_key(&row_id, input).await
}
pub(super) async fn reset_health(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.reset_health(&row_id).await
}
pub(super) async fn delete_channel(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_channel(&row_id).await
}

impl AdminService {
    /// 渠道行：渠道字段 + provider 名称/类型 + 健康状态 + 已绑定模型数。
    pub(super) async fn load_channel(&self, row_id: &str) -> Result<ChannelRow, ApiError> {
        sqlx::query_as::<_,ChannelRow>("SELECT c.id,c.provider_id,p.name provider_name,p.kind provider_kind,c.name,c.protocol,c.api_key_encrypted,c.api_key_hint,c.manual_enabled,c.health_check_model_id,c.created_at,c.updated_at,h.state,h.consecutive_failures,h.disabled_until,h.last_success_at,h.last_failure_at,h.last_error_kind,h.last_status_code,COUNT(DISTINCT cm.id) model_count FROM channels c JOIN providers p ON p.id=c.provider_id LEFT JOIN channel_health h ON h.channel_id=c.id LEFT JOIN channel_models cm ON cm.channel_id=c.id WHERE c.id=? GROUP BY c.id")
        .bind(row_id).fetch_optional(self.db.pool()).await?.ok_or_else(||ApiError::not_found("Channel not found"))
    }

    /// 渠道详情 JSON：协议清单、远端压缩探测标记、余额快照与健康状态。
    pub(super) async fn channel_json(&self, row: ChannelRow) -> Result<Value, ApiError> {
        // 端口按插入顺序返回；展示顺序仍按 `PROTOCOL_ORDER`（与旧 SQL 的
        // `CASE` 排序一致）。
        let mut protocols = self.channels.protocols(&row.id).await?;
        protocols.sort_by_key(|protocol| {
            PROTOCOL_ORDER
                .iter()
                .position(|known| known == protocol)
                .unwrap_or(PROTOCOL_ORDER.len())
        });
        let mut remote_compaction = json!({});
        let compaction_rows: Vec<(String, i64, i64, Option<String>)> = sqlx::query_as(
            "SELECT protocol, remote_compaction_v1_support, remote_compaction_v2_support, remote_compaction_probed_at \
             FROM channel_protocols WHERE channel_id=? AND protocol='openai_responses'",
        )
        .bind(&row.id)
        .fetch_all(self.db.pool())
        .await?;
        for (protocol, v1, v2, probed_at) in compaction_rows {
            let support = |value: i64| match value {
                1 => "supported",
                2 => "unsupported",
                _ => "unknown",
            };
            remote_compaction[protocol] = json!({
                "v1": support(v1),
                "v2": support(v2),
                "probed_at": probed_at,
            });
        }
        let balance = crate::balance::channel_balance_json(&self.db, &row.id).await?;
        Ok(
            json!({"id":row.id,"provider_id":row.provider_id,"provider_name":row.provider_name,"provider_kind":row.provider_kind,"name":row.name,"protocol":row.protocol,"protocols":protocols,"manual_enabled":row.manual_enabled,"health_check_model_id":row.health_check_model_id,"has_api_key":!row.api_key_encrypted.is_empty(),"api_key_hint":row.api_key_hint,"health":{"state":row.state.unwrap_or_else(||"active".into()),"consecutive_failures":row.consecutive_failures.unwrap_or(0),"disabled_until":row.disabled_until,"last_success_at":row.last_success_at,"last_failure_at":row.last_failure_at,"last_error_kind":row.last_error_kind,"last_status_code":row.last_status_code},"model_count":row.model_count,"remote_compaction":remote_compaction,"balance":balance,"created_at":row.created_at,"updated_at":row.updated_at}),
        )
    }

    pub async fn list_channels(&self, filter: ChannelFilter) -> ApiResult {
        let ids: Vec<String> = sqlx::query_scalar("SELECT DISTINCT c.id FROM channels c LEFT JOIN channel_protocols cp ON cp.channel_id=c.id LEFT JOIN channel_health h ON h.channel_id=c.id LEFT JOIN providers p ON p.id=c.provider_id WHERE (? IS NULL OR c.provider_id=?) AND (? IS NULL OR cp.protocol=?) AND (? IS NULL OR h.state=?) ORDER BY p.name,c.name")
            .bind(&filter.provider_id).bind(&filter.provider_id).bind(&filter.protocol).bind(&filter.protocol).bind(&filter.state).bind(&filter.state).fetch_all(self.db.pool()).await?;
        let mut items = Vec::new();
        for row_id in ids {
            items.push(self.channel_json(self.load_channel(&row_id).await?).await?);
        }
        Ok(ok(
            json!({"items":items,"total":items.len(),"page":1,"page_size":items.len()}),
        ))
    }

    /// 创建渠道；`login` 是 Command Code 一次性登录密钥的交接方
    /// （`AdminService` 不持有登录服务，由 handler 注入引用）。
    pub async fn create_channel(
        &self,
        mut input: ChannelInput,
        login: &CommandCodeLogin,
    ) -> ApiResult {
        let login_id = input.login_id.take();
        if let Some(login_id) = &login_id {
            // 先 peek：创建失败（重名、校验错误……）不消耗交接，用户可直接重试。
            input.api_key = self.peek_login_key(login, login_id)?;
        }
        let result = self.insert_channel(input).await;
        if result.is_ok()
            && let Some(login_id) = &login_id
        {
            login.consume_key(login_id);
        }
        result
    }

    /// 一次性交接：登录流程已通过 `/alpha/whoami` 校验的密钥在此进入加密库。
    fn peek_login_key(&self, login: &CommandCodeLogin, login_id: &str) -> Result<String, ApiError> {
        login.peek_key(login_id).ok_or_else(|| {
            ApiError::validation("Command Code 登录已失效或已被使用，请重新发起网页登录")
        })
    }

    async fn insert_channel(&self, input: ChannelInput) -> ApiResult {
        validate_text(&input.name, "name", 120)?;
        validate_text(&input.api_key, "api_key", 65535)?;
        let protocols = selected_protocols(input.protocol, input.protocols)?;
        let provider: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM providers WHERE id=?")
            .bind(&input.provider_id)
            .fetch_one(self.db.pool())
            .await?;
        if provider == 0 {
            return Err(ApiError::not_found("Provider not found"));
        }
        let row_id = id();
        let time = now();
        let encrypted = self.secrets.encrypt(&input.api_key);
        let hint = SecretStore::hint(&input.api_key);
        let mut tx = self.db.pool().begin().await?;
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,health_check_model_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)").bind(&row_id).bind(input.provider_id).bind(input.name.trim()).bind(&protocols[0]).bind(encrypted).bind(hint).bind(input.manual_enabled).bind(input.health_check_model_id.as_deref()).bind(&time).bind(&time).execute(&mut *tx).await.map_err(|e|integrity(e,"Channel already exists"))?;
        for protocol in protocols {
            sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES(?,?)")
                .bind(&row_id)
                .bind(protocol)
                .execute(&mut *tx)
                .await?;
        }
        // 初始健康行走 `channel_health` 的唯一写入口（同一事务内）。
        crate::health::apply_health_event(
            &mut *tx,
            crate::health::HealthEvent::Initialize {
                channel_id: &row_id,
                at: &time,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(json_response(
            StatusCode::CREATED,
            self.channel_json(self.load_channel(&row_id).await?).await?,
        ))
    }

    pub async fn get_channel(&self, row_id: &str) -> ApiResult {
        Ok(ok(self.channel_json(self.load_channel(row_id).await?).await?))
    }

    /// 更新渠道；`login` 语义同 [`AdminService::create_channel`]。
    pub async fn patch_channel(
        &self,
        row_id: &str,
        mut input: ChannelPatch,
        login: &CommandCodeLogin,
    ) -> ApiResult {
        let login_id = input.login_id.take();
        if let Some(login_id) = &login_id {
            input.api_key = Some(Some(self.peek_login_key(login, login_id)?));
        }
        let result = self.update_channel(row_id, input).await;
        if result.is_ok()
            && let Some(login_id) = &login_id
        {
            login.consume_key(login_id);
        }
        result
    }

    async fn update_channel(&self, row_id: &str, input: ChannelPatch) -> ApiResult {
        self.load_channel(row_id).await?;
        let mut tx = self.db.pool().begin().await?;
        // 可选的新 API Key 与其它字段同一事务提交：密钥被拒不会留下
        // 半更新状态的渠道。
        if let Some(Some(key)) = input.api_key {
            let key = key.trim();
            validate_text(key, "api_key", 65535)?;
            sqlx::query("UPDATE channels SET api_key_encrypted=?,api_key_hint=?,updated_at=? WHERE id=?")
                .bind(self.secrets.encrypt(key))
                .bind(SecretStore::hint(key))
                .bind(now())
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(name) = input.name {
            validate_text(&name, "name", 120)?;
            sqlx::query("UPDATE channels SET name=?,updated_at=? WHERE id=?")
                .bind(name.trim())
                .bind(now())
                .bind(row_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| integrity(e, "Channel already exists"))?;
        }
        if let Some(enabled) = input.manual_enabled {
            sqlx::query("UPDATE channels SET manual_enabled=?,updated_at=? WHERE id=?")
                .bind(enabled)
                .bind(now())
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(model) = input.health_check_model_id {
            // Some(None) 清回自动；Some(Some(model)) 则必须引用属于该渠道的
            // 模型。
            if let Some(model) = &model {
                let owned: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM channel_models WHERE channel_id=? AND model_id=?",
                )
                .bind(row_id)
                .bind(model)
                .fetch_one(&mut *tx)
                .await?;
                if owned == 0 {
                    return Err(ApiError::validation(
                        "Health check model does not belong to the channel",
                    ));
                }
            }
            sqlx::query("UPDATE channels SET health_check_model_id=?,updated_at=? WHERE id=?")
                .bind(model.as_deref())
                .bind(now())
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
        }
        if input.protocol.is_some() || input.protocols.is_some() {
            let protocols =
                selected_protocols(input.protocol, input.protocols.unwrap_or_default())?;
            // Command Code 渠道还会绑定转换层支持的每个入口协议（见
            // `protocol::model_binding_protocols`）：守卫与目录改写都必须使用
            // 这个扩展集合，否则编辑渠道会把路由池已在用的绑定删掉（或拒绝
            // 添加）。
            let provider_kind = self.channels.provider_kind(row_id).await?;
            let bindings =
                crate::protocol::model_binding_protocols(provider_kind.as_deref(), &protocols);
            let bound = self
                .route_candidates_use_protocols(&mut tx, row_id, &bindings)
                .await?;
            if bound > 0 {
                return Err(ApiError::conflict("Protocol is used by route candidates"));
            }
            sqlx::query("DELETE FROM channel_protocols WHERE channel_id=?")
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
            for protocol in &protocols {
                sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES(?,?)")
                    .bind(row_id)
                    .bind(protocol)
                    .execute(&mut *tx)
                    .await?;
            }
            sqlx::query("UPDATE channels SET protocol=?,updated_at=? WHERE id=?")
                .bind(&protocols[0])
                .bind(now())
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id IN (SELECT id FROM channel_models WHERE channel_id=?) AND protocol NOT IN (SELECT value FROM json_each(?))").bind(row_id).bind(serde_json::to_string(&bindings)?).execute(&mut *tx).await?;
            for protocol in bindings {
                sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) SELECT id,? FROM channel_models WHERE channel_id=?").bind(protocol).bind(row_id).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(ok(self.channel_json(self.load_channel(row_id).await?).await?))
    }

    pub async fn delete_channel(&self, row_id: &str) -> ApiResult {
        let mut tx = self.db.pool().begin().await?;
        self.delete_candidates_for_channel(&mut tx, row_id).await?;
        let result = sqlx::query("DELETE FROM channels WHERE id=?")
            .bind(row_id)
            .execute(&mut *tx)
            .await?;
        if result.rows_affected() == 0 {
            return Err(ApiError::not_found("Channel not found"));
        }
        tx.commit().await?;
        Ok(no_content())
    }

    pub async fn replace_api_key(&self, row_id: &str, input: ApiKeyInput) -> ApiResult {
        validate_text(&input.api_key, "api_key", 65535)?;
        let result = sqlx::query(
            "UPDATE channels SET api_key_encrypted=?,api_key_hint=?,updated_at=? WHERE id=?",
        )
        .bind(self.secrets.encrypt(&input.api_key))
        .bind(SecretStore::hint(&input.api_key))
        .bind(now())
        .bind(row_id)
        .execute(self.db.pool())
        .await?;
        if result.rows_affected() == 0 {
            return Err(ApiError::not_found("Channel not found"));
        }
        Ok(ok(self.channel_json(self.load_channel(row_id).await?).await?))
    }

    pub async fn reset_health(&self, row_id: &str) -> ApiResult {
        let time = now();
        let result = crate::health::apply_health_event(
            self.db.pool(),
            crate::health::HealthEvent::ManualReset {
                channel_id: row_id,
                at: &time,
            },
        )
        .await?;
        if result.rows_affected() == 0 {
            return Err(ApiError::not_found("Channel not found"));
        }
        Ok(ok(self.channel_json(self.load_channel(row_id).await?).await?["health"]
            .clone()))
    }

    pub async fn ensure_channel_exists(&self, row_id: &str) -> Result<(), ApiError> {
        if !self.channels.exists(row_id).await? {
            return Err(ApiError::not_found("Channel not found"));
        }
        Ok(())
    }

    /// 当前处于 `active` 的渠道数（统计页的渠道健康摘要）。
    pub(super) async fn count_active_channels(&self) -> Result<i64, ApiError> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM channel_health WHERE state='active'")
                .fetch_one(self.db.pool())
                .await?,
        )
    }
}
