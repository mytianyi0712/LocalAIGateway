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

/// Primary proxy entrypoint per protocol, as advertised in the model catalog.
/// OpenAI Compatible only advertises `/v1/chat/completions` — embeddings and
/// completions are deliberately not guessed (see requirements.md 3.7).
pub fn protocol_main_endpoint(protocol: &str, model_id: &str) -> Option<String> {
    match protocol {
        "openai_compatible" => Some("/v1/chat/completions".to_owned()),
        "openai_responses" => Some("/v1/responses".to_owned()),
        "claude" => Some("/v1/messages".to_owned()),
        "gemini" => Some(format!("/v1beta/models/{model_id}:generateContent")),
        _ => None,
    }
}

/// All primary entrypoints the model can currently be routed through: the
/// distinct enabled route protocols with at least one live candidate, ordered
/// by `PROTOCOL_ORDER`. Mirrors the candidate-existence filter of
/// `list_routable_models` so endpoints only appear when the route is actually
/// callable.
pub async fn list_routable_model_endpoints(
    state: &AppState,
    model_id: &str,
) -> Result<Vec<String>> {
    let protocols: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT mr.protocol FROM model_routes mr \
         WHERE mr.requested_model_id = ? AND mr.enabled = 1 \
           AND EXISTS (SELECT 1 FROM route_candidates rc \
             JOIN channel_models cm ON cm.id = rc.channel_model_id \
             JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
             JOIN channels c ON c.id = cm.channel_id \
             JOIN channel_health ch ON ch.channel_id = c.id \
             WHERE rc.route_id = mr.id AND rc.enabled = 1 AND cm.available = 1 \
               AND c.manual_enabled = 1 AND ch.state = 'active') \
         ORDER BY CASE mr.protocol WHEN 'openai_compatible' THEN 0 \
           WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 ELSE 3 END",
    )
    .bind(model_id)
    .fetch_all(state.db.pool())
    .await?;
    Ok(endpoints_for_protocols(
        &protocols.iter().map(String::as_str).collect::<Vec<_>>(),
        model_id,
    ))
}

/// Pure mapping used by `list_routable_model_endpoints` (and unit-tested):
/// unknown protocols are dropped, known ones keep their input order.
pub fn endpoints_for_protocols(protocols: &[&str], model_id: &str) -> Vec<String> {
    protocols
        .iter()
        .filter_map(|protocol| protocol_main_endpoint(protocol, model_id))
        .collect()
}

pub async fn list_mapping_models(state: &AppState, kind: &str) -> Result<Vec<RoutableModel>> {
    let (table, id_column) = match kind {
        "claude" => ("claude_model_mappings", "claude_model_id"),
        // codex 映射按入口协议 openai_responses 标识（proxy::mapped_models 传 entry）
        "openai_responses" => ("codex_model_mappings", "codex_model_id"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_endpoints_follow_protocol_order_and_drop_unknown() {
        let endpoints = endpoints_for_protocols(
            &["openai_compatible", "openai_responses", "claude", "gemini", "bogus"],
            "mimo-v2.5",
        );
        assert_eq!(
            endpoints,
            [
                "/v1/chat/completions",
                "/v1/responses",
                "/v1/messages",
                "/v1beta/models/mimo-v2.5:generateContent",
            ]
        );
    }

    #[test]
    fn gemini_endpoint_embeds_model_id() {
        assert_eq!(
            protocol_main_endpoint("gemini", "gpt-5.6-sol").as_deref(),
            Some("/v1beta/models/gpt-5.6-sol:generateContent")
        );
    }

    #[test]
    fn openai_compatible_advertises_chat_completions_only() {
        assert_eq!(
            protocol_main_endpoint("openai_compatible", "any-model").as_deref(),
            Some("/v1/chat/completions")
        );
        // Embeddings/completions are not guessed.
        assert_eq!(endpoints_for_protocols(&["bogus"], "any-model"), Vec::<String>::new());
    }
}
