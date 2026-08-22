//! admin API 域模块：渠道 CRUD 与健康重置（channel 域，服务层已形式化）
//! 每个域文件包含该资源的 handler（薄壳）与输入/输出类型；
//! 直写 SQL 的域逻辑正逐步收敛到 super::AdminService。

use super::*;
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
pub(super) async fn load_channel(db: &Database, row_id: &str) -> Result<ChannelRow, ApiError> {
    sqlx::query_as::<_,ChannelRow>("SELECT c.id,c.provider_id,p.name provider_name,c.name,c.protocol,c.api_key_encrypted,c.api_key_hint,c.manual_enabled,c.health_check_model_id,c.created_at,c.updated_at,h.state,h.consecutive_failures,h.disabled_until,h.last_success_at,h.last_failure_at,h.last_error_kind,h.last_status_code,COUNT(DISTINCT cm.id) model_count FROM channels c JOIN providers p ON p.id=c.provider_id LEFT JOIN channel_health h ON h.channel_id=c.id LEFT JOIN channel_models cm ON cm.channel_id=c.id WHERE c.id=? GROUP BY c.id")
        .bind(row_id).fetch_optional(db.pool()).await?.ok_or_else(||ApiError::not_found("Channel not found"))
}
pub(super) async fn channel_json(db: &Database, row: ChannelRow) -> Result<Value, ApiError> {
    let protocols:Vec<String>=sqlx::query_scalar("SELECT protocol FROM channel_protocols WHERE channel_id=? ORDER BY CASE protocol WHEN 'openai_compatible' THEN 0 WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 ELSE 3 END").bind(&row.id).fetch_all(db.pool()).await?;
    let mut remote_compaction = json!({});
    let compaction_rows: Vec<(String, i64, i64, Option<String>)> = sqlx::query_as(
        "SELECT protocol, remote_compaction_v1_support, remote_compaction_v2_support, remote_compaction_probed_at \
         FROM channel_protocols WHERE channel_id=? AND protocol='openai_responses'",
    )
    .bind(&row.id)
    .fetch_all(db.pool())
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
    Ok(
        json!({"id":row.id,"provider_id":row.provider_id,"provider_name":row.provider_name,"name":row.name,"protocol":row.protocol,"protocols":protocols,"manual_enabled":row.manual_enabled,"health_check_model_id":row.health_check_model_id,"has_api_key":!row.api_key_encrypted.is_empty(),"api_key_hint":row.api_key_hint,"health":{"state":row.state.unwrap_or_else(||"active".into()),"consecutive_failures":row.consecutive_failures.unwrap_or(0),"disabled_until":row.disabled_until,"last_success_at":row.last_success_at,"last_failure_at":row.last_failure_at,"last_error_kind":row.last_error_kind,"last_status_code":row.last_status_code},"model_count":row.model_count,"remote_compaction":remote_compaction,"created_at":row.created_at,"updated_at":row.updated_at}),
    )
}
#[derive(Deserialize)]
pub struct ChannelInput {
    pub provider_id: String,
    pub name: String,
    pub protocol: Option<String>,
    #[serde(default)]
    pub protocols: Vec<String>,
    pub api_key: String,
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
    /// P2-3: an optional new API key commits in the SAME transaction as the
    /// rest of the patch — saving the channel form can no longer succeed
    /// half-way (fields saved, key rejected). Omitted or `null` = keep.
    #[serde(default, deserialize_with = "patch_optional_string")]
    api_key: Option<Option<String>>,
    /// P1-6: three states — `None` (omitted) leaves the value untouched,
    /// `Some(None)` (JSON `null`) clears it back to automatic, and
    /// `Some(Some(model))` sets it after validating membership. Plain
    /// `Option<String>` would collapse `null` and omission, so the field
    /// uses a dedicated decoder.
    #[serde(default, deserialize_with = "patch_optional_string")]
    health_check_model_id: Option<Option<String>>,
}

/// Decoder that keeps omitted and `null` apart: serde's derived `Option<T>`
/// maps both to `None`, which would make it impossible to clear a stored
/// value back to automatic (P1-6).
fn patch_optional_string<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Deserialize as a plain Value first: deserializing `Option<Value>`
    // would collapse null to None before the match below.
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
    State(state): State<Context>,
    Query(filter): Query<ChannelFilter>,
) -> ApiResult {
    state.admin.list_channels(filter).await
}
pub(super) async fn create_channel(
    _: AdminAuth,
    State(state): State<Context>,
    Json(input): Json<ChannelInput>,
) -> ApiResult {
    state.admin.create_channel(input).await
}
pub(super) async fn get_channel(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.get_channel(&row_id).await
}
pub(super) async fn patch_channel(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
    Json(input): Json<ChannelPatch>,
) -> ApiResult {
    state.admin.patch_channel(&row_id, input).await
}
pub(super) async fn replace_api_key(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
    Json(input): Json<ApiKeyInput>,
) -> ApiResult {
    state.admin.replace_api_key(&row_id, input).await
}
pub(super) async fn reset_health(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.reset_health(&row_id).await
}
pub(super) async fn delete_channel(
    _: AdminAuth,
    State(state): State<Context>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_channel(&row_id).await
}

impl AdminService {
    pub async fn list_channels(&self, filter: ChannelFilter) -> ApiResult {
        let ids: Vec<String> = sqlx::query_scalar("SELECT DISTINCT c.id FROM channels c LEFT JOIN channel_protocols cp ON cp.channel_id=c.id LEFT JOIN channel_health h ON h.channel_id=c.id LEFT JOIN providers p ON p.id=c.provider_id WHERE (? IS NULL OR c.provider_id=?) AND (? IS NULL OR cp.protocol=?) AND (? IS NULL OR h.state=?) ORDER BY p.name,c.name")
            .bind(&filter.provider_id).bind(&filter.provider_id).bind(&filter.protocol).bind(&filter.protocol).bind(&filter.state).bind(&filter.state).fetch_all(self.db.pool()).await?;
        let mut items = Vec::new();
        for row_id in ids {
            items.push(channel_json(&self.db, load_channel(&self.db, &row_id).await?).await?);
        }
        Ok(ok(
            json!({"items":items,"total":items.len(),"page":1,"page_size":items.len()}),
        ))
    }

    pub async fn create_channel(&self, input: ChannelInput) -> ApiResult {
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
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES(?,'active',0,?)").bind(&row_id).bind(time).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(json_response(
            StatusCode::CREATED,
            channel_json(&self.db, load_channel(&self.db, &row_id).await?).await?,
        ))
    }

    pub async fn get_channel(&self, row_id: &str) -> ApiResult {
        Ok(ok(channel_json(
            &self.db,
            load_channel(&self.db, row_id).await?,
        )
        .await?))
    }

    pub async fn patch_channel(&self, row_id: &str, input: ChannelPatch) -> ApiResult {
        load_channel(&self.db, row_id).await?;
        let mut tx = self.db.pool().begin().await?;
        // P2-3: an optional new API key commits in the same transaction as
        // the other fields — a rejected key can never leave the channel
        // half-updated.
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
            // P1-6: Some(None) clears back to automatic; Some(Some(model))
            // must reference a model that belongs to this channel.
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
            let bound: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM route_candidates rc JOIN channel_models cm ON cm.id=rc.channel_model_id JOIN model_routes mr ON mr.id=rc.route_id WHERE cm.channel_id=? AND mr.protocol NOT IN (SELECT value FROM json_each(?))").bind(row_id).bind(serde_json::to_string(&protocols)?).fetch_one(&mut *tx).await?;
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
            sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id IN (SELECT id FROM channel_models WHERE channel_id=?) AND protocol NOT IN (SELECT value FROM json_each(?))").bind(row_id).bind(serde_json::to_string(&protocols)?).execute(&mut *tx).await?;
            for protocol in protocols {
                sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) SELECT id,? FROM channel_models WHERE channel_id=?").bind(protocol).bind(row_id).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(ok(channel_json(
            &self.db,
            load_channel(&self.db, row_id).await?,
        )
        .await?))
    }

    pub async fn delete_channel(&self, row_id: &str) -> ApiResult {
        let mut tx = self.db.pool().begin().await?;
        sqlx::query("DELETE FROM route_candidates WHERE channel_model_id IN (SELECT id FROM channel_models WHERE channel_id=?)").bind(row_id).execute(&mut *tx).await?;
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
        Ok(ok(channel_json(
            &self.db,
            load_channel(&self.db, row_id).await?,
        )
        .await?))
    }

    pub async fn reset_health(&self, row_id: &str) -> ApiResult {
        let time = now();
        let result = sqlx::query("UPDATE channel_health SET state='active',consecutive_failures=0,disabled_until=NULL,last_error_kind=NULL,last_status_code=NULL,updated_at=? WHERE channel_id=?").bind(time).bind(row_id).execute(self.db.pool()).await?;
        if result.rows_affected() == 0 {
            return Err(ApiError::not_found("Channel not found"));
        }
        Ok(ok(channel_json(
            &self.db,
            load_channel(&self.db, row_id).await?,
        )
        .await?["health"]
            .clone()))
    }

    pub async fn ensure_channel_exists(&self, row_id: &str) -> Result<(), ApiError> {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels WHERE id=?")
            .bind(row_id)
            .fetch_one(self.db.pool())
            .await?;
        if exists == 0 {
            return Err(ApiError::not_found("Channel not found"));
        }
        Ok(())
    }
}
