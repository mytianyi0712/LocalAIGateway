//! admin API 域模块：渠道模型清单（channel_models 域）。
//!
//! handler 只做参数提取与响应透传；全部 SQL、校验与响应组装都在同文件的
//! `impl AdminService` 内。渠道级联（渠道删除时模型一并消失）由数据库外键
//! 负责，这里不重复实现。

use super::{
    AdminService, ApiResult, id, integrity, json_response, no_content, now, ok, validate_text,
};
use crate::api_error::ApiError;
use crate::auth::AdminAuth;
use crate::protocol::{PROTOCOL_ORDER_SQL, valid_protocol};
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
pub(super) struct ModelRow {
    id: String,
    channel_id: String,
    channel_name: String,
    model_id: String,
    display_name: Option<String>,
    source: String,
    available: bool,
    last_seen_at: Option<String>,
    channel_enabled: bool,
    health_state: Option<String>,
}
#[derive(Deserialize, Default)]
pub(super) struct ModelFilter {
    channel_id: Option<String>,
    protocol: Option<String>,
}
#[derive(Deserialize)]
pub(super) struct ManualModelInput {
    model_id: String,
    display_name: Option<String>,
    protocols: Option<Vec<String>>,
}
#[derive(Deserialize)]
pub(super) struct ModelPatch {
    display_name: Option<String>,
    available: Option<bool>,
    protocols: Option<Vec<String>>,
}
pub(super) async fn list_channel_models(
    _: AdminAuth,
    State(state): State<AppState>,
    Query(filter): Query<ModelFilter>,
) -> ApiResult {
    state.admin.list_channel_models(filter).await
}
pub(super) async fn create_manual_model(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(input): Json<ManualModelInput>,
) -> ApiResult {
    state.admin.create_manual_model(&channel_id, input).await
}
pub(super) async fn patch_channel_model(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<ModelPatch>,
) -> ApiResult {
    state.admin.patch_channel_model(&row_id, input).await
}
pub(super) async fn delete_channel_model(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_channel_model(&row_id).await
}

impl AdminService {
    /// 渠道模型详情 JSON：绑定协议（按协议序，首项即主协议）与所属渠道、
    /// 健康状态；`health_state` 缺失时按 `active` 呈现。
    pub(super) async fn model_json(&self, row: ModelRow) -> Result<Value, ApiError> {
        let protocols:Vec<String>=sqlx::query_scalar(&format!("SELECT protocol FROM channel_model_protocols WHERE channel_model_id=? {}", PROTOCOL_ORDER_SQL)).bind(&row.id).fetch_all(self.db.pool()).await?;
        Ok(
            json!({"id":row.id,"channel_id":row.channel_id,"channel_name":row.channel_name,"protocol":protocols.first(),"protocols":protocols,"model_id":row.model_id,"display_name":row.display_name,"source":row.source,"available":row.available,"last_seen_at":row.last_seen_at,"channel_enabled":row.channel_enabled,"health_state":row.health_state.unwrap_or_else(||"active".into())}),
        )
    }

    /// 读取单个渠道模型行（新建/更新后的回读共用同一查询）。
    async fn load_model(&self, row_id: &str) -> Result<ModelRow, ApiError> {
        Ok(sqlx::query_as::<_,ModelRow>("SELECT cm.id,cm.channel_id,c.name channel_name,cm.model_id,cm.display_name,cm.source,cm.available,cm.last_seen_at,c.manual_enabled channel_enabled,h.state health_state FROM channel_models cm JOIN channels c ON c.id=cm.channel_id LEFT JOIN channel_health h ON h.channel_id=c.id WHERE cm.id=?").bind(row_id).fetch_one(self.db.pool()).await?)
    }

    pub(super) async fn list_channel_models(&self, filter: ModelFilter) -> ApiResult {
        let rows=sqlx::query_as::<_,ModelRow>("SELECT DISTINCT cm.id,cm.channel_id,c.name channel_name,cm.model_id,cm.display_name,cm.source,cm.available,cm.last_seen_at,c.manual_enabled channel_enabled,h.state health_state FROM channel_models cm JOIN channels c ON c.id=cm.channel_id LEFT JOIN channel_health h ON h.channel_id=c.id LEFT JOIN channel_model_protocols cmp ON cmp.channel_model_id=cm.id WHERE (? IS NULL OR cm.channel_id=?) AND (? IS NULL OR cmp.protocol=?) ORDER BY cm.model_id,c.name").bind(&filter.channel_id).bind(&filter.channel_id).bind(&filter.protocol).bind(&filter.protocol).fetch_all(self.db.pool()).await?;
        let mut items = Vec::new();
        for row in rows {
            items.push(self.model_json(row).await?);
        }
        Ok(ok(
            json!({"items":items,"total":items.len(),"page":1,"page_size":items.len()}),
        ))
    }

    /// 手工新增模型：先确认渠道存在，再把模型绑定到「渠道已绑定协议」
    /// （Command Code 渠道额外绑定全部可转换的入口协议）的展开集合上。
    pub(super) async fn create_manual_model(
        &self,
        channel_id: &str,
        input: ManualModelInput,
    ) -> ApiResult {
        validate_text(&input.model_id, "model_id", 255)?;
        let _channel = self.load_channel(channel_id).await?;
        let available: Vec<String> =
            sqlx::query_scalar("SELECT protocol FROM channel_protocols WHERE channel_id=?")
                .bind(channel_id)
                .fetch_all(self.db.pool())
                .await?;
        // Command Code 渠道的目录行会绑定全部可转换入口协议（见
        // `protocol::model_binding_protocols`），因此手工新增的模型一开始就能
        // 经 Claude / OpenAI 路由命中。
        let provider_kind = self.channels.provider_kind(channel_id).await?;
        let protocols = match input.protocols {
            Some(protocols) => {
                if protocols.is_empty() || protocols.iter().any(|p| !valid_protocol(p)) {
                    return Err(ApiError::validation("Unsupported or empty protocols"));
                }
                crate::protocol::model_binding_protocols(provider_kind.as_deref(), &protocols)
            }
            None => crate::protocol::model_binding_protocols(provider_kind.as_deref(), &available),
        };
        if protocols.is_empty() {
            return Err(ApiError::validation("Unsupported or empty protocols"));
        }
        let row_id = id();
        let time = now();
        let mut tx = self.db.pool().begin().await?;
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,metadata_json,first_seen_at,last_seen_at,created_at,updated_at) VALUES(?,?,?,?,'manual',1,NULL,?,?,?,?)")
            .bind(&row_id).bind(channel_id).bind(input.model_id).bind(input.display_name).bind(&time).bind(&time).bind(&time).bind(&time)
            .execute(&mut *tx).await.map_err(|e|integrity(e,"Model already exists"))?;
        for protocol in protocols {
            sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)")
                .bind(&row_id)
                .bind(protocol)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(json_response(
            StatusCode::CREATED,
            self.model_json(self.load_model(&row_id).await?).await?,
        ))
    }

    pub(super) async fn patch_channel_model(
        &self,
        row_id: &str,
        input: ModelPatch,
    ) -> ApiResult {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channel_models WHERE id=?")
            .bind(row_id)
            .fetch_one(self.db.pool())
            .await?;
        if exists == 0 {
            return Err(ApiError::not_found("Model not found"));
        }
        let mut tx = self.db.pool().begin().await?;
        if input.display_name.is_some() {
            sqlx::query("UPDATE channel_models SET display_name=?,updated_at=? WHERE id=?")
                .bind(input.display_name)
                .bind(now())
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(value) = input.available {
            sqlx::query("UPDATE channel_models SET available=?,updated_at=? WHERE id=?")
                .bind(value)
                .bind(now())
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
        }
        if let Some(protocols) = input.protocols {
            if protocols.is_empty() || protocols.iter().any(|p| !valid_protocol(p)) {
                return Err(ApiError::validation("Unsupported or empty protocols"));
            }
            // Command Code 行还会额外携带全部可转换入口协议：这里的守卫按
            // 扩展后的集合比较，因此只会放宽「允许删除」的范围。
            // 模型行的渠道 id 先查出，再走渠道端口（`providers.kind` 的单键查询
            // 只有端口这一处实现）。
            let channel_id: String =
                sqlx::query_scalar("SELECT channel_id FROM channel_models WHERE id=?")
                    .bind(row_id)
                    .fetch_one(&mut *tx)
                    .await?;
            let provider_kind = self.channels.provider_kind(&channel_id).await?;
            let bindings =
                crate::protocol::model_binding_protocols(provider_kind.as_deref(), &protocols);
            let used = self.route_protocols_using_model(&mut tx, row_id).await?;
            if used.iter().any(|p| !bindings.contains(p)) {
                return Err(ApiError::conflict("Protocol is used by route candidates"));
            }
            sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id=?")
                .bind(row_id)
                .execute(&mut *tx)
                .await?;
            for protocol in bindings {
                sqlx::query(
                    "INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)",
                )
                .bind(row_id)
                .bind(protocol)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(ok(self.model_json(self.load_model(row_id).await?).await?))
    }

    pub(super) async fn delete_channel_model(&self, row_id: &str) -> ApiResult {
        let used = self
            .count_candidates_for_model(self.db.pool(), row_id)
            .await?;
        if used > 0 {
            return Err(ApiError::conflict("Model is used by routes"));
        }
        let result = sqlx::query("DELETE FROM channel_models WHERE id=? AND source='manual'")
            .bind(row_id)
            .execute(self.db.pool())
            .await?;
        if result.rows_affected() == 0 {
            return Err(ApiError::not_found("Manual model not found"));
        }
        Ok(no_content())
    }
}
