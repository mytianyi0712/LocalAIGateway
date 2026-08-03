use anyhow::Result;
use serde::Serialize;
use sqlx::FromRow;

use crate::server::AppState;

#[derive(Clone, Debug, FromRow)]
pub struct Candidate {
    pub candidate_id: String,
    pub channel_id: String,
    pub channel_name: String,
    pub priority: i64,
    pub base_url: String,
    pub api_key_encrypted: Vec<u8>,
    pub model_id: String,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct RoutableModel {
    pub id: String,
    pub display_name: String,
    pub created_at: String,
}

pub async fn resolve_candidates(
    state: &AppState,
    protocol: &str,
    model_id: &str,
    limit: i64,
) -> Result<Vec<Candidate>> {
    Ok(sqlx::query_as::<_, Candidate>(
        "SELECT rc.id AS candidate_id, c.id AS channel_id, c.name AS channel_name, \
                rc.priority, p.base_url, c.api_key_encrypted, cm.model_id \
         FROM route_candidates rc \
         JOIN model_routes mr ON mr.id = rc.route_id \
         JOIN channel_models cm ON cm.id = rc.channel_model_id \
         JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
         JOIN channels c ON c.id = cm.channel_id \
         JOIN providers p ON p.id = c.provider_id \
         JOIN channel_health ch ON ch.channel_id = c.id \
         WHERE mr.protocol = ? AND mr.requested_model_id = ? AND mr.enabled = 1 \
           AND rc.enabled = 1 AND cm.available = 1 AND c.manual_enabled = 1 AND ch.state = 'active' \
         ORDER BY rc.priority ASC LIMIT ?"
    ).bind(protocol).bind(model_id).bind(limit).fetch_all(state.db.pool()).await?)
}

pub async fn list_routable_models(
    state: &AppState,
    protocol: Option<&str>,
) -> Result<Vec<RoutableModel>> {
    let rows = sqlx::query_as::<_, RoutableModel>(
        "SELECT mr.requested_model_id AS id, mr.requested_model_id AS display_name, MIN(mr.created_at) AS created_at \
         FROM model_routes mr \
         WHERE mr.enabled = 1 AND (? IS NULL OR mr.protocol = ?) \
           AND EXISTS (SELECT 1 FROM route_candidates rc \
             JOIN channel_models cm ON cm.id = rc.channel_model_id \
             JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
             JOIN channels c ON c.id = cm.channel_id \
             JOIN channel_health ch ON ch.channel_id = c.id \
             WHERE rc.route_id = mr.id AND rc.enabled = 1 AND cm.available = 1 \
               AND c.manual_enabled = 1 AND ch.state = 'active') \
         GROUP BY mr.requested_model_id ORDER BY mr.requested_model_id"
    ).bind(protocol).bind(protocol).fetch_all(state.db.pool()).await?;
    Ok(rows)
}

pub async fn list_mapping_models(state: &AppState, kind: &str) -> Result<Vec<RoutableModel>> {
    let (table, id_column) = match kind {
        "claude" => ("claude_model_mappings", "claude_model_id"),
        "codex" => ("codex_model_mappings", "codex_model_id"),
        _ => return Ok(Vec::new()),
    };
    let sql = format!(
        "SELECT m.{id_column} AS id, COALESCE(m.display_name, m.{id_column}) AS display_name, m.created_at \
         FROM {table} m WHERE m.enabled = 1 AND EXISTS (SELECT 1 FROM model_routes mr \
           JOIN route_candidates rc ON rc.route_id = mr.id \
           JOIN channel_models cm ON cm.id = rc.channel_model_id \
           JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
           JOIN channels c ON c.id = cm.channel_id \
           JOIN channel_health ch ON ch.channel_id = c.id \
           WHERE mr.protocol = m.upstream_protocol AND mr.requested_model_id = m.upstream_model_id \
             AND mr.enabled = 1 AND rc.enabled = 1 AND cm.available = 1 AND c.manual_enabled = 1 AND ch.state = 'active') \
         ORDER BY m.{id_column}"
    );
    Ok(sqlx::query_as::<_, RoutableModel>(&sql)
        .fetch_all(state.db.pool())
        .await?)
}
