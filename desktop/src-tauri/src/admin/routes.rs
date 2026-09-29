//! admin API 域模块：路由与候选（routes/candidates 域）。
//!
//! 路由是「请求模型名 → 各协议候选」的绑定：同一 `requested_model_id` 可为
//! 每个协议各存一行 `model_routes`，候选（`route_candidates`）按协议共享。
//! handler 只做参数提取与响应透传，SQL 全部在同文件的 `impl AdminService`。
//! `capabilities` 需要 `crate::capabilities` 的检测服务（依赖完整 Context），
//! 按仓库既有约定由 `AdminService` 方法临时接收 `&Context` 求值（不持有）。

use super::{AdminService, ApiResult, id, json_response, no_content, now, ok, validate_text};
use crate::api_error::ApiError;
use crate::application::Context;
use crate::auth::AdminAuth;
use crate::protocol::{PROTOCOL_ORDER, PROTOCOL_ORDER_SQL, valid_protocol};
use crate::state::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sqlx::Row;
use std::collections::{HashMap, HashSet};

use super::{channels::yes, profiles::get_caps_value};

#[derive(Deserialize)]
pub(super) struct RouteInput {
    pub protocol: Option<String>,
    pub protocols: Option<Vec<String>>,
    pub requested_model_id: String,
    #[serde(default = "yes")]
    pub enabled: bool,
}
#[derive(Deserialize)]
pub(super) struct RoutePatch {
    enabled: bool,
}
#[derive(Deserialize)]
pub(super) struct CandidateInput {
    pub channel_model_id: String,
    pub priority: i64,
    #[serde(default = "yes")]
    pub enabled: bool,
}
#[derive(Deserialize)]
pub(super) struct CandidateList {
    pub candidates: Vec<CandidateInput>,
}

/// `GET /routes` —— 全部请求模型名的路由详情清单。
pub(super) async fn list_routes(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    state.admin.list_routes(&state).await
}
pub(super) async fn create_route(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(input): Json<RouteInput>,
) -> ApiResult {
    state.admin.create_route(&state, input).await
}
pub(super) async fn patch_route(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<RoutePatch>,
) -> ApiResult {
    state.admin.patch_route(&row_id, input).await
}
pub(super) async fn replace_candidates(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
    Json(input): Json<CandidateList>,
) -> ApiResult {
    state.admin.replace_candidates(&state, &row_id, input).await
}
pub(super) async fn delete_route(
    _: AdminAuth,
    State(state): State<AppState>,
    Path(row_id): Path<String>,
) -> ApiResult {
    state.admin.delete_route(&row_id).await
}

impl AdminService {
    /// 某请求模型名下的全部路由行：`(id, protocol, enabled, requested_model_id, created_at)`。
    pub(super) async fn route_ids_for_model(
        &self,
        model_id: &str,
    ) -> Result<Vec<(String, String, bool, String, String)>, ApiError> {
        Ok(sqlx::query_as::<_,(String,String,bool,String,String)>(&format!("SELECT id,protocol,enabled,requested_model_id,created_at FROM model_routes WHERE requested_model_id=? {}", PROTOCOL_ORDER_SQL)).bind(model_id).fetch_all(self.db.pool()).await?)
    }

    /// 已存在的请求模型名（路由列表的遍历入口）。
    pub(super) async fn route_model_ids(&self) -> Result<Vec<String>, ApiError> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT requested_model_id FROM model_routes ORDER BY requested_model_id",
        )
        .fetch_all(self.db.pool())
        .await?)
    }

    /// 路由详情：候选按 `channel_model_id` 合并（priority 取最小、enabled 取与）。
    /// `capabilities` 需要能力检测服务，故按仓库既有约定接收 `&Context`
    /// （本服务只把它当调用参数，不持有）。
    pub(super) async fn route_bundle(
        &self,
        state: &Context,
        model_id: &str,
    ) -> Result<Value, ApiError> {
        let rows = self.route_ids_for_model(model_id).await?;
        if rows.is_empty() {
            return Err(ApiError::not_found("Route not found"));
        }
        let mut by_model: HashMap<String, Value> = HashMap::new();
        for (route_id, protocol, _, _, _) in &rows {
            let candidates=sqlx::query("SELECT rc.id,rc.channel_model_id,rc.priority,rc.enabled,cm.channel_id,cm.model_id,cm.display_name,c.name channel_name,p.name provider_name,h.state,c.manual_enabled FROM route_candidates rc JOIN channel_models cm ON cm.id=rc.channel_model_id JOIN channels c ON c.id=cm.channel_id JOIN providers p ON p.id=c.provider_id LEFT JOIN channel_health h ON h.channel_id=c.id WHERE rc.route_id=? ORDER BY rc.priority,c.name").bind(route_id).fetch_all(self.db.pool()).await?;
            for row in candidates {
                let cmid: String = row.try_get("channel_model_id")?;
                let candidate=by_model.entry(cmid.clone()).or_insert_with(||json!({"id":row.get::<String,_>("id"),"channel_model_id":cmid,"channel_id":row.get::<String,_>("channel_id"),"channel_name":row.get::<String,_>("channel_name"),"provider_name":row.get::<String,_>("provider_name"),"model_id":row.get::<String,_>("model_id"),"display_name":row.get::<Option<String>,_>("display_name"),"priority":row.get::<i64,_>("priority"),"enabled":row.get::<bool,_>("enabled"),"health_state":row.get::<Option<String>,_>("state").unwrap_or_else(||"active".into()),"manual_enabled":row.get::<bool,_>("manual_enabled"),"protocols":Vec::<String>::new()}));
                // 候选对象由上面的 `json!` 构造，永远是 object；拿不到时直接跳过该行，
                // 不用 `expect` 在请求路径上 panic（release 为 panic=abort）。
                let Some(object) = candidate.as_object_mut() else {
                    continue;
                };
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
                if let Some(protocols) = object.get_mut("protocols").and_then(Value::as_array_mut) {
                    protocols.push(json!(protocol));
                }
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
        let capabilities = get_caps_value(state, model_id).await?;
        Ok(json!({
            // 上面已断言 rows 非空，首行即该模型名下的第一条路由。
            "id": rows.first().map(|row| row.0.clone()).unwrap_or_default(),
            "route_ids": route_ids,
            "protocols": rows.iter().map(|(_, p, _, _, _)| p).collect::<Vec<_>>(),
            "requested_model_id": model_id,
            "enabled": enabled,
            "candidates": candidates,
            "capabilities": capabilities,
            "created_at": created,
            "updated_at": now(),
        }))
    }

    /// `GET /routes` —— 逐个请求模型名组装详情。
    pub(super) async fn list_routes(&self, state: &Context) -> ApiResult {
        let ids = self.route_model_ids().await?;
        let mut items = Vec::new();
        for id in ids {
            items.push(self.route_bundle(state, &id).await?);
        }
        let total = items.len();
        Ok(ok(
            json!({"items":items,"total":total,"page":1,"page_size":total}),
        ))
    }

    /// 建路由：协议可显式指定，否则从「可用渠道模型」的协议集合推导。
    /// `state` 仅用于创建后组装详情（能力检测需要完整 Context）。
    pub(super) async fn create_route(&self, state: &Context, input: RouteInput) -> ApiResult {
        validate_text(&input.requested_model_id, "requested_model_id", 255)?;
        // 自定义模型会在请求里显式给出协议；否则该模型名可能没有任何可用渠道
        // 模型，推导为空而永远不可达。
        let explicit: Option<Vec<String>> = input
            .protocols
            .filter(|items| !items.is_empty())
            .or_else(|| input.protocol.map(|protocol| vec![protocol]));
        let protocols = if let Some(items) = explicit {
            if items.iter().any(|protocol| !valid_protocol(protocol)) {
                return Err(ApiError::validation("Unsupported protocol"));
            }
            let mut unique: Vec<String> = Vec::new();
            for protocol in items {
                if !unique.contains(&protocol) {
                    unique.push(protocol);
                }
            }
            unique
        } else {
            let values:Vec<String>=sqlx::query_scalar("SELECT DISTINCT cmp.protocol FROM channel_models cm JOIN channel_model_protocols cmp ON cmp.channel_model_id=cm.id WHERE cm.model_id=? AND cm.available=1").bind(&input.requested_model_id).fetch_all(self.db.pool()).await?;
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
        let existing:i64=sqlx::query_scalar("SELECT COUNT(*) FROM model_routes WHERE requested_model_id=? AND protocol IN (SELECT value FROM json_each(?))").bind(&input.requested_model_id).bind(serde_json::to_string(&protocols)?).fetch_one(self.db.pool()).await?;
        if existing > 0 {
            return Err(ApiError::conflict("Route already exists"));
        }
        let mut tx = self.db.pool().begin().await?;
        for protocol in protocols {
            let time = now();
            sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?)").bind(id()).bind(protocol).bind(&input.requested_model_id).bind(input.enabled).bind(&time).bind(&time).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(json_response(
            StatusCode::CREATED,
            self.route_bundle(state, &input.requested_model_id).await?,
        ))
    }

    pub(super) async fn patch_route(&self, row_id: &str, input: RoutePatch) -> ApiResult {
        let model: Option<String> =
            sqlx::query_scalar("SELECT requested_model_id FROM model_routes WHERE id=?")
                .bind(row_id)
                .fetch_optional(self.db.pool())
                .await?;
        let model = model.ok_or_else(|| ApiError::not_found("Route not found"))?;
        sqlx::query("UPDATE model_routes SET enabled=?,updated_at=? WHERE requested_model_id=?")
            .bind(input.enabled)
            .bind(now())
            .bind(&model)
            .execute(self.db.pool())
            .await?;
        Ok(ok(json!({"id":row_id,"enabled":input.enabled})))
    }

    /// 整体替换某条路由的候选：先校验（优先级/模型唯一、可用、协议交集非空），
    /// 再在同一事务里按协议兄弟路由重建候选行。`state` 语义同 [`Self::create_route`]。
    pub(super) async fn replace_candidates(
        &self,
        state: &Context,
        row_id: &str,
        input: CandidateList,
    ) -> ApiResult {
        let model: Option<String> =
            sqlx::query_scalar("SELECT requested_model_id FROM model_routes WHERE id=?")
                .bind(row_id)
                .fetch_optional(self.db.pool())
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
        let protocols: HashSet<String> = self
            .route_ids_for_model(&model)
            .await?
            .into_iter()
            .map(|(_, p, _, _, _)| p)
            .collect();
        for item in &input.candidates {
            let available: Option<bool> =
                sqlx::query_scalar("SELECT available FROM channel_models WHERE id=?")
                    .bind(&item.channel_model_id)
                    .fetch_optional(self.db.pool())
                    .await?;
            let Some(available) = available else {
                return Err(ApiError::validation(
                    "One or more channel models do not exist",
                ));
            };
            // 自定义模型的候选，其上游 `model_id` 允许与 `requested_model_id`
            // 不同（网关会按候选改写外发模型名），因此这里只强制可用性与
            // 协议支持。
            if !available {
                return Err(ApiError::validation(
                    "One or more channel models are not available",
                ));
            }
            // 渠道模型不支持该路由任何协议的候选会被直接拒绝，且发生在删除
            // 任何既有候选行之前。
            let supported: HashSet<String> = sqlx::query_scalar(
                "SELECT protocol FROM channel_model_protocols WHERE channel_model_id=?",
            )
            .bind(&item.channel_model_id)
            .fetch_all(self.db.pool())
            .await?
            .into_iter()
            .collect();
            if supported.is_disjoint(&protocols) {
                return Err(ApiError::validation(
                    "Candidate protocol and model must match the route",
                ));
            }
        }
        let sibling = self.route_ids_for_model(&model).await?;
        let sibling_ids: Vec<String> = sibling.iter().map(|(id, _, _, _, _)| id.clone()).collect();
        let mut tx = self.db.pool().begin().await?;
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
        Ok(ok(self.route_bundle(state, &model).await?))
    }

    pub(super) async fn delete_route(&self, row_id: &str) -> ApiResult {
        let model: Option<String> =
            sqlx::query_scalar("SELECT requested_model_id FROM model_routes WHERE id=?")
                .bind(row_id)
                .fetch_optional(self.db.pool())
                .await?;
        let model = model.ok_or_else(|| ApiError::not_found("Route not found"))?;
        sqlx::query("DELETE FROM model_routes WHERE requested_model_id=?")
            .bind(model)
            .execute(self.db.pool())
            .await?;
        Ok(no_content())
    }

    // ------------------------------------------------------ 候选行 SQL（跨子域调用点）

    /// 渠道的协议绑定是否仍被路由候选引用：返回使用「`bindings` 之外协议」的
    /// 候选行数。渠道与模型的编辑守卫都经它判断能否收窄绑定。
    pub(super) async fn route_candidates_use_protocols(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        channel_id: &str,
        bindings: &[String],
    ) -> Result<i64, ApiError> {
        Ok(sqlx::query_scalar("SELECT COUNT(*) FROM route_candidates rc JOIN channel_models cm ON cm.id=rc.channel_model_id JOIN model_routes mr ON mr.id=rc.route_id WHERE cm.channel_id=? AND mr.protocol NOT IN (SELECT value FROM json_each(?))").bind(channel_id).bind(serde_json::to_string(bindings)?).fetch_one(&mut **tx).await?)
    }

    /// 删除渠道时先清掉它的候选行（级联顺序：`route_candidates` → `channels`）。
    pub(super) async fn delete_candidates_for_channel(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        channel_id: &str,
    ) -> Result<(), ApiError> {
        sqlx::query("DELETE FROM route_candidates WHERE channel_model_id IN (SELECT id FROM channel_models WHERE channel_id=?)").bind(channel_id).execute(&mut **tx).await?;
        Ok(())
    }

    /// 删除 provider 时先清掉其全部渠道的候选行
    /// （级联顺序：`route_candidates` → `channels` → `providers`）。
    pub(super) async fn delete_candidates_for_provider(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        provider_id: &str,
    ) -> Result<(), ApiError> {
        sqlx::query("DELETE FROM route_candidates WHERE channel_model_id IN (SELECT cm.id FROM channel_models cm JOIN channels c ON c.id=cm.channel_id WHERE c.provider_id=?)").bind(provider_id).execute(&mut **tx).await?;
        Ok(())
    }

    /// 该渠道模型当前被哪些路由协议引用（模型绑定收窄守卫）。
    pub(super) async fn route_protocols_using_model(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        channel_model_id: &str,
    ) -> Result<Vec<String>, ApiError> {
        Ok(sqlx::query_scalar("SELECT DISTINCT mr.protocol FROM route_candidates rc JOIN model_routes mr ON mr.id=rc.route_id WHERE rc.channel_model_id=?").bind(channel_model_id).fetch_all(&mut **tx).await?)
    }

    /// 该渠道模型的候选行数（删除模型前的占用守卫）。
    pub(super) async fn count_candidates_for_model<'e, E>(
        &self,
        executor: E,
        channel_model_id: &str,
    ) -> Result<i64, ApiError>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        Ok(sqlx::query_scalar("SELECT COUNT(*) FROM route_candidates WHERE channel_model_id=?")
            .bind(channel_model_id)
            .fetch_one(executor)
            .await?)
    }
}
