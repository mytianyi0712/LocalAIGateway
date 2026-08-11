//! admin API 域模块：渠道模型清单（channel_models 域）
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

use super::channels::load_channel;

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
}
pub(super) async fn model_json(state: &Context, row: ModelRow) -> Result<Value, ApiError> {
    let protocols:Vec<String>=sqlx::query_scalar("SELECT protocol FROM channel_model_protocols WHERE channel_model_id=? ORDER BY CASE protocol WHEN 'openai_compatible' THEN 0 WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 ELSE 3 END").bind(&row.id).fetch_all(state.db.pool()).await?;
    Ok(
        json!({"id":row.id,"channel_id":row.channel_id,"channel_name":row.channel_name,"protocol":protocols.first(),"protocols":protocols,"model_id":row.model_id,"display_name":row.display_name,"source":row.source,"available":row.available,"last_seen_at":row.last_seen_at}),
    )
}
#[derive(Deserialize, Default)]
pub(super) struct ModelFilter {
    channel_id: Option<String>,
    protocol: Option<String>,
}
pub(super) async fn list_channel_models(
    _: AdminAuth,
    State(state): State<Context>,
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
pub(super) struct ManualModelInput {
    model_id: String,
    display_name: Option<String>,
    protocols: Option<Vec<String>>,
}
pub(super) async fn create_manual_model(
    _: AdminAuth,
    State(state): State<Context>,
    Path(channel_id): Path<String>,
    Json(input): Json<ManualModelInput>,
) -> ApiResult {
    validate_text(&input.model_id, "model_id", 255)?;
    let _channel = load_channel(&state.db, &channel_id).await?;
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
pub(super) struct ModelPatch {
    display_name: Option<String>,
    available: Option<bool>,
    protocols: Option<Vec<String>>,
}
pub(super) async fn patch_channel_model(
    _: AdminAuth,
    State(state): State<Context>,
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
pub(super) async fn delete_channel_model(
    _: AdminAuth,
    State(state): State<Context>,
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
