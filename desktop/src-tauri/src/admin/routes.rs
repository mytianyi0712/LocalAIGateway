//! admin API 域模块：路由与候选（routes/candidates 域）
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

use super::{channels::yes, profiles::get_caps_value};

#[derive(Deserialize)]
pub(super) struct RouteInput {
    protocol: Option<String>,
    requested_model_id: String,
    #[serde(default = "yes")]
    enabled: bool,
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

pub(super) async fn route_ids_for_model(
    state: &Context,
    model_id: &str,
) -> Result<Vec<(String, String, bool, String, String)>, ApiError> {
    Ok(sqlx::query_as::<_,(String,String,bool,String,String)>("SELECT id,protocol,enabled,requested_model_id,created_at FROM model_routes WHERE requested_model_id=? ORDER BY CASE protocol WHEN 'openai_compatible' THEN 0 WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 ELSE 3 END").bind(model_id).fetch_all(state.db.pool()).await?)
}
pub(super) async fn route_bundle(state: &Context, model_id: &str) -> Result<Value, ApiError> {
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
pub(super) async fn list_routes(_: AdminAuth, State(state): State<Context>) -> ApiResult {
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
pub(super) async fn create_route(
    _: AdminAuth,
    State(state): State<Context>,
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
pub(super) async fn patch_route(
    _: AdminAuth,
    State(state): State<Context>,
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
pub(super) async fn replace_candidates(
    _: AdminAuth,
    State(state): State<Context>,
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
        // P2-6: a candidate whose channel model supports none of the route's
        // protocols is rejected outright, before any row is deleted.
        let supported: HashSet<String> = sqlx::query_scalar(
            "SELECT protocol FROM channel_model_protocols WHERE channel_model_id=?",
        )
        .bind(&item.channel_model_id)
        .fetch_all(state.db.pool())
        .await?
        .into_iter()
        .collect();
        if supported.is_disjoint(&protocols) {
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
pub(super) async fn delete_route(
    _: AdminAuth,
    State(state): State<Context>,
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
