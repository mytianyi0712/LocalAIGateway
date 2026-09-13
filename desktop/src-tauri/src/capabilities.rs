//! Model capability detection — port of backend/app/services/capabilities.py.
//!
//! Capabilities are inferred from the metadata captured during model
//! discovery (per-protocol dicts inside `channel_models.metadata_json`) and
//! aggregated across every live candidate of a route. `source = "auto"` rows
//! are re-detected on read; `source = "manual"` rows store explicit values.

use anyhow::Result;
use serde_json::{Value, json};
use sqlx::Row;

use crate::application::Context;

const THINKING_LEVELS: [&str; 7] = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];

const CAPABILITY_FIELDS: [&str; 9] = [
    "context_window",
    "max_tokens",
    "supports_image_input",
    "reasoning",
    "thinking_level_map",
    "cost_input",
    "cost_output",
    "cost_cache_read",
    "cost_cache_write",
];

fn nested_get<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn first_int(value: &Value, paths: &[&[&str]]) -> Option<i64> {
    for path in paths {
        let raw = nested_get(value, path)?;
        match raw {
            Value::Bool(_) => continue,
            Value::Number(number) => {
                if let Some(int) = number.as_i64() {
                    if int > 0 {
                        return Some(int);
                    }
                } else if let Some(float) = number.as_f64()
                    && float > 0.0
                {
                    return Some(float as i64);
                }
            }
            Value::String(text) => {
                if let Ok(parsed) = text.trim().parse::<i64>()
                    && parsed > 0
                {
                    return Some(parsed);
                }
            }
            _ => {}
        }
    }
    None
}

fn first_bool(value: &Value, paths: &[&[&str]]) -> Option<bool> {
    for path in paths {
        let raw = nested_get(value, path)?;
        match raw {
            Value::Bool(flag) => return Some(*flag),
            Value::String(text) => {
                let normalized = text.trim().to_ascii_lowercase();
                if matches!(normalized.as_str(), "true" | "yes" | "1" | "supported") {
                    return Some(true);
                }
                if matches!(normalized.as_str(), "false" | "no" | "0" | "unsupported") {
                    return Some(false);
                }
            }
            _ => {}
        }
    }
    None
}

fn contains_image(value: &Value) -> Option<bool> {
    match value {
        Value::Null => None,
        Value::String(text) => {
            let normalized = text.trim().to_ascii_lowercase();
            if matches!(normalized.as_str(), "image" | "vision" | "multimodal") {
                Some(true)
            } else {
                None
            }
        }
        Value::Array(items) => {
            let normalized = items
                .iter()
                .filter_map(|item| item.as_str())
                .map(|item| item.trim().to_ascii_lowercase())
                .collect::<Vec<_>>();
            let hit = normalized.iter().any(|item| {
                matches!(
                    item.as_str(),
                    "image" | "vision" | "multimodal" | "image_url"
                )
            });
            if hit { Some(true) } else { None }
        }
        Value::Object(_) => {
            for path in [
                &["input"][..],
                &["inputs"][..],
                &["input_types"][..],
                &["inputTypes"][..],
                &["modalities"][..],
                &["input_modalities"][..],
                &["inputModalities"][..],
                &["architecture", "input_modalities"][..],
                &["architecture", "modality"][..],
                &["capabilities", "input"][..],
                &["capabilities", "modalities"][..],
            ] {
                if let Some(detected) = contains_image(nested_get(value, path)?) {
                    return Some(detected);
                }
            }
            None
        }
        _ => None,
    }
}

fn thinking_level_map_from_efforts(value: &Value) -> Option<Value> {
    let items = value.as_array()?;
    let mut result = serde_json::Map::new();
    for item in items {
        let raw = if let Some(object) = item.as_object() {
            object
                .get("value")
                .or_else(|| object.get("id"))
                .or_else(|| object.get("name"))
                .or_else(|| object.get("level"))
        } else {
            Some(item)
        };
        let Some(raw) = raw else { continue };
        let Some(raw) = raw.as_str() else { continue };
        let effort = raw.trim().to_ascii_lowercase();
        if THINKING_LEVELS.contains(&effort.as_str()) {
            result.insert(effort.clone(), Value::String(effort));
        } else if effort == "none" {
            result.insert("off".into(), Value::String("none".into()));
        }
    }
    if result.is_empty() {
        None
    } else {
        Some(Value::Object(result))
    }
}

fn cost_value(value: &Value, paths: &[&[&str]]) -> Option<f64> {
    for path in paths {
        let raw = nested_get(value, path)?;
        match raw {
            Value::Bool(_) => continue,
            Value::Number(number) => {
                if let Some(float) = number.as_f64()
                    && float >= 0.0
                {
                    return Some(float);
                }
            }
            Value::String(text) => {
                if let Ok(parsed) = text.trim().parse::<f64>()
                    && parsed >= 0.0
                {
                    return Some(parsed);
                }
            }
            _ => {}
        }
    }
    None
}

/// Extract capability fields from a discovery metadata dict (or a per-protocol
/// metadata sub-dict). Only keys with a detected value are present.
pub fn extract_capabilities(metadata: &Value) -> Value {
    if !metadata.is_object() {
        return json!({});
    }
    let context_window = first_int(
        metadata,
        &[
            &["context_window"][..],
            &["contextWindow"][..],
            &["context_length"][..],
            &["contextLength"][..],
            &["max_context_tokens"][..],
            &["maxContextTokens"][..],
            &["inputTokenLimit"][..],
            &["input_token_limit"][..],
            &["limits", "context_window"][..],
            &["limits", "contextWindow"][..],
        ],
    );
    let max_tokens = first_int(
        metadata,
        &[
            &["max_tokens"][..],
            &["maxTokens"][..],
            &["max_output_tokens"][..],
            &["maxOutputTokens"][..],
            &["outputTokenLimit"][..],
            &["output_token_limit"][..],
            &["max_completion_tokens"][..],
            &["limits", "max_tokens"][..],
            &["limits", "maxTokens"][..],
        ],
    );
    let mut image = first_bool(
        metadata,
        &[
            &["supports_image_input"][..],
            &["supportsImageInput"][..],
            &["supports_vision"][..],
            &["supportsVision"][..],
            &["vision"][..],
            &["capabilities", "vision"][..],
        ],
    );
    if image.is_none() {
        image = contains_image(metadata);
    }
    let mut reasoning = first_bool(
        metadata,
        &[
            &["reasoning"][..],
            &["supports_reasoning"][..],
            &["supportsReasoning"][..],
            &["supports_reasoning_effort"][..],
            &["supportsReasoningEffort"][..],
            &["thinking"][..],
            &["supports_thinking"][..],
            &["supportsThinking"][..],
            &["capabilities", "reasoning"][..],
            &["capabilities", "thinking"][..],
        ],
    );
    let thinking_level_map = metadata
        .get("thinkingLevelMap")
        .or_else(|| metadata.get("thinking_level_map"))
        .and_then(|value| value.as_object())
        .map(|map| {
            let mut filtered = serde_json::Map::new();
            for level in THINKING_LEVELS {
                if let Some(value) = map.get(level) {
                    filtered.insert(level.to_owned(), value.clone());
                }
            }
            if reasoning.is_none() && filtered.values().any(|value| !value.is_null()) {
                reasoning = Some(true);
            }
            Value::Object(filtered)
        })
        .or_else(|| {
            let from_efforts = thinking_level_map_from_efforts(
                metadata
                    .get("reasoningEfforts")
                    .or_else(|| metadata.get("reasoning_efforts"))
                    .unwrap_or(&Value::Null),
            );
            if reasoning.is_none() && from_efforts.is_some() {
                reasoning = Some(true);
            }
            from_efforts
        });
    let cost = metadata
        .get("cost")
        .or_else(|| metadata.get("pricing"))
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or_else(|| json!({}));
    let mut result = serde_json::Map::new();
    for (key, value) in [
        ("context_window", context_window.map(Value::from)),
        ("max_tokens", max_tokens.map(Value::from)),
        ("supports_image_input", image.map(Value::from)),
        ("reasoning", reasoning.map(Value::from)),
        ("thinking_level_map", thinking_level_map),
        (
            "cost_input",
            cost_value(&cost, &[&["input"][..], &["prompt"][..]]).map(Value::from),
        ),
        (
            "cost_output",
            cost_value(&cost, &[&["output"][..], &["completion"][..]]).map(Value::from),
        ),
        (
            "cost_cache_read",
            cost_value(
                &cost,
                &[
                    &["cacheRead"][..],
                    &["cache_read"][..],
                    &["cached_input"][..],
                ],
            )
            .map(Value::from),
        ),
        (
            "cost_cache_write",
            cost_value(&cost, &[&["cacheWrite"][..], &["cache_write"][..]]).map(Value::from),
        ),
    ] {
        if let Some(value) = value {
            result.insert(key.to_owned(), value);
        }
    }
    Value::Object(result)
}

fn merge_values(values: &[Value]) -> Option<Value> {
    let known: Vec<&Value> = values.iter().filter(|value| !value.is_null()).collect();
    if known.is_empty() {
        return None;
    }
    if known.iter().all(|value| value.is_boolean()) {
        return Some(Value::Bool(
            known.iter().all(|value| value.as_bool().unwrap_or(false)),
        ));
    }
    if known
        .iter()
        .all(|value| value.is_number() && !value.is_boolean())
    {
        // Preserve integers (Python `min(known)` keeps the numeric type).
        if known.iter().all(|value| value.as_i64().is_some()) {
            let minimum = known
                .iter()
                .filter_map(|value| value.as_i64())
                .min()
                .unwrap_or(0);
            return Some(Value::from(minimum));
        }
        let minimum = known
            .iter()
            .filter_map(|value| value.as_f64())
            .fold(f64::INFINITY, f64::min);
        return Some(Value::from(minimum));
    }
    Some(known[0].clone())
}

fn merge_thinking_level_maps(values: &[Value]) -> Option<Value> {
    let maps: Vec<&Value> = values.iter().filter(|value| value.is_object()).collect();
    if maps.is_empty() {
        return None;
    }
    let mut merged = serde_json::Map::new();
    for level in THINKING_LEVELS {
        let all_present = maps.iter().all(|map| map.get(level).is_some());
        let any_present = maps.iter().any(|map| map.get(level).is_some());
        if all_present {
            let first = maps[0].get(level).cloned().unwrap_or(Value::Null);
            if !first.is_null() {
                merged.insert(level.to_owned(), first);
                continue;
            }
        }
        if any_present {
            merged.insert(level.to_owned(), Value::Null);
        }
    }
    if merged.is_empty() {
        None
    } else {
        Some(Value::Object(merged))
    }
}

/// Aggregate a list of extracted capability dicts: numerics take the minimum,
/// booleans are AND-ed, thinking maps are merged level-by-level.
pub fn aggregate_capabilities(items: &[Value]) -> Value {
    let mut result = serde_json::Map::new();
    for key in CAPABILITY_FIELDS {
        if key == "thinking_level_map" {
            continue;
        }
        let values: Vec<Value> = items
            .iter()
            .map(|item| item.get(key).cloned().unwrap_or(Value::Null))
            .collect();
        if let Some(merged) = merge_values(&values) {
            result.insert(key.to_owned(), merged);
        }
    }
    let maps: Vec<Value> = items
        .iter()
        .map(|item| {
            item.get("thinking_level_map")
                .cloned()
                .unwrap_or(Value::Null)
        })
        .collect();
    if let Some(merged) = merge_thinking_level_maps(&maps) {
        result.insert("thinking_level_map".to_owned(), merged);
    }
    Value::Object(result)
}

/// Recompute the capabilities of a route's model from its live candidates'
/// discovery metadata — mirror of services/capabilities.py
/// `detect_model_capabilities`. Never touches the network.
pub async fn detect_model_capabilities(state: &Context, requested_model_id: &str) -> Result<Value> {
    let rows = sqlx::query(
        "SELECT cm.metadata_json, cmp.protocol FROM channel_models cm \
         JOIN route_candidates rc ON rc.channel_model_id = cm.id \
         JOIN model_routes mr ON mr.id = rc.route_id \
         JOIN channels c ON c.id = cm.channel_id \
         JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
         WHERE mr.requested_model_id = ? AND mr.enabled = 1 AND rc.enabled = 1 \
           AND cm.available = 1 AND c.manual_enabled = 1",
    )
    .bind(requested_model_id)
    .fetch_all(state.db.pool())
    .await?;
    let mut detected: Vec<Value> = Vec::new();
    for row in rows {
        let metadata: Option<String> = row.try_get("metadata_json")?;
        let Some(metadata) = metadata else { continue };
        let Ok(metadata) = serde_json::from_str::<Value>(&metadata) else {
            continue;
        };
        if !metadata.is_object() {
            continue;
        }
        // INNER JOIN guarantees a protocol binding (P2-2); candidates without
        // one are excluded instead of leaking whole-metadata fallbacks.
        let protocol: String = row.try_get("protocol")?;
        let mut extracted: Vec<Value> = Vec::new();
        if let Some(per_protocol) = metadata.get(&protocol)
            && per_protocol.is_object()
        {
            extracted.push(extract_capabilities(per_protocol));
        }
        if extracted.is_empty() {
            extracted.push(extract_capabilities(&metadata));
        }
        if extracted.len() == 1 {
            detected.push(extracted.into_iter().next().unwrap_or_else(|| json!({})));
        } else {
            detected.push(aggregate_capabilities(&extracted));
        }
    }
    Ok(aggregate_capabilities(&detected))
}

pub fn has_capability_data(capabilities: &Value) -> bool {
    for key in [
        "context_window",
        "max_tokens",
        "supports_image_input",
        "reasoning",
        "thinking_level_map",
    ] {
        if capabilities.get(key).is_some_and(|value| !value.is_null()) {
            return true;
        }
    }
    let cost = capabilities.get("cost").unwrap_or(&Value::Null);
    for key in ["input", "output", "cacheRead", "cacheWrite"] {
        if cost.get(key).is_some_and(|value| !value.is_null()) {
            return true;
        }
    }
    false
}

/// The full capability object served by the admin API and embedded in the
/// model catalog — mirror of services/capabilities.py `get_model_caps` +
/// `caps_json`. `source = "auto"` (or a missing row) re-detects on read.
pub async fn get_model_caps(state: &Context, model_id: &str) -> Result<Value> {
    let row = sqlx::query(
        "SELECT source, profile_id, context_window, max_tokens, supports_image_input, \
                reasoning, thinking_level_map, cost_input, cost_output, cost_cache_read, \
                cost_cache_write, updated_at \
         FROM model_caps WHERE requested_model_id = ?",
    )
    .bind(model_id)
    .fetch_optional(state.db.pool())
    .await?;
    let Some(row) = row else {
        let auto = detect_model_capabilities(state, model_id).await?;
        return Ok(caps_json("auto", None, None, &auto, None));
    };
    let source: String = row.try_get("source")?;
    let profile_id: Option<String> = row.try_get("profile_id")?;
    let stored = caps_from_row(&row);
    let (values, profile_name) = if source == "auto" {
        let auto = detect_model_capabilities(state, model_id).await?;
        let name = profile_name(state, profile_id.as_deref()).await?;
        (auto, name)
    } else {
        let name = profile_name(state, profile_id.as_deref()).await?;
        (stored, name)
    };
    let updated_at: Option<String> = row.try_get("updated_at")?;
    Ok(caps_json(
        &source,
        profile_id.as_deref(),
        profile_name.as_deref(),
        &values,
        updated_at.as_deref(),
    ))
}

async fn profile_name(state: &Context, profile_id: Option<&str>) -> Result<Option<String>> {
    let Some(profile_id) = profile_id else {
        return Ok(None);
    };
    let name: Option<String> =
        sqlx::query_scalar("SELECT name FROM capability_profiles WHERE id = ?")
            .bind(profile_id)
            .fetch_optional(state.db.pool())
            .await?;
    Ok(name)
}

fn caps_from_row(row: &sqlx::sqlite::SqliteRow) -> Value {
    let mut result = serde_json::Map::new();
    for (key, value) in [
        (
            "context_window",
            row.try_get::<Option<i64>, _>("context_window")
                .ok()
                .flatten()
                .map(Value::from),
        ),
        (
            "max_tokens",
            row.try_get::<Option<i64>, _>("max_tokens")
                .ok()
                .flatten()
                .map(Value::from),
        ),
        (
            "supports_image_input",
            row.try_get::<Option<bool>, _>("supports_image_input")
                .ok()
                .flatten()
                .map(Value::from),
        ),
        (
            "reasoning",
            row.try_get::<Option<bool>, _>("reasoning")
                .ok()
                .flatten()
                .map(Value::from),
        ),
        (
            "cost_input",
            row.try_get::<Option<f64>, _>("cost_input")
                .ok()
                .flatten()
                .map(Value::from),
        ),
        (
            "cost_output",
            row.try_get::<Option<f64>, _>("cost_output")
                .ok()
                .flatten()
                .map(Value::from),
        ),
        (
            "cost_cache_read",
            row.try_get::<Option<f64>, _>("cost_cache_read")
                .ok()
                .flatten()
                .map(Value::from),
        ),
        (
            "cost_cache_write",
            row.try_get::<Option<f64>, _>("cost_cache_write")
                .ok()
                .flatten()
                .map(Value::from),
        ),
    ] {
        if let Some(value) = value {
            result.insert(key.to_owned(), value);
        }
    }
    if let Ok(Some(raw)) = row.try_get::<Option<String>, _>("thinking_level_map")
        && let Ok(map) = serde_json::from_str::<Value>(&raw)
    {
        result.insert("thinking_level_map".to_owned(), map);
    }
    Value::Object(result)
}

fn caps_json(
    source: &str,
    profile_id: Option<&str>,
    profile_name: Option<&str>,
    values: &Value,
    updated_at: Option<&str>,
) -> Value {
    json!({
        "source": source,
        "profile_id": profile_id,
        "profile_name": profile_name,
        "context_window": values.get("context_window").cloned().unwrap_or(Value::Null),
        "max_tokens": values.get("max_tokens").cloned().unwrap_or(Value::Null),
        "supports_image_input": values.get("supports_image_input").cloned().unwrap_or(Value::Null),
        "reasoning": values.get("reasoning").cloned().unwrap_or(Value::Null),
        "thinking_level_map": values.get("thinking_level_map").cloned().unwrap_or(Value::Null),
        "cost": {
            "input": values.get("cost_input").cloned().unwrap_or(Value::Null),
            "output": values.get("cost_output").cloned().unwrap_or(Value::Null),
            "cacheRead": values.get("cost_cache_read").cloned().unwrap_or(Value::Null),
            "cacheWrite": values.get("cost_cache_write").cloned().unwrap_or(Value::Null),
        },
        "updated_at": updated_at,
    })
}

/// Capability summary embedded in `x_local_gateway.pi_model_config`.
pub fn pi_model_config(capabilities: &Value) -> Value {
    let mut input = vec!["text"];
    if capabilities
        .get("supports_image_input")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        input.push("image");
    }
    let mut config = json!({
        "input": input,
        "reasoning": capabilities.get("reasoning").cloned().unwrap_or(Value::Null),
        "cost": {
            "input": capabilities.get("cost_input").cloned().unwrap_or(Value::from(0)),
            "output": capabilities.get("cost_output").cloned().unwrap_or(Value::from(0)),
            "cacheRead": capabilities.get("cost_cache_read").cloned().unwrap_or(Value::from(0)),
            "cacheWrite": capabilities.get("cost_cache_write").cloned().unwrap_or(Value::from(0)),
        },
    });
    if let Some(window) = capabilities.get("context_window") {
        config["contextWindow"] = window.clone();
    }
    if let Some(tokens) = capabilities.get("max_tokens") {
        config["maxTokens"] = tokens.clone();
    }
    if let Some(map) = capabilities.get("thinking_level_map") {
        config["thinkingLevelMap"] = map.clone();
    }
    config
}

/// The `x_local_gateway` metadata for a catalog item — mirror of
/// `gateway_metadata` in api/proxy.py (capabilities/pi_model_config part).
pub fn gateway_metadata(item: &Value) -> Value {
    let mut metadata = json!({});
    let capabilities = item
        .get("capabilities")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if has_capability_data(&capabilities) {
        metadata["capabilities"] = capabilities.clone();
        metadata["pi_model_config"] = pi_model_config(&capabilities);
    }
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{application::Context, config::AppConfig, db::Database};
    use std::sync::Arc;

    async fn test_state() -> (Context, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("lagw-caps-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::open(&dir.join("test.db")).await.unwrap();
        let secrets = crate::crypto::SecretStore::load(&dir.join("master.key"))
            .await
            .unwrap();
        let (telemetry, _rx) = crate::telemetry::Telemetry::new(1000);
        let http: Arc<dyn crate::ports::UpstreamClient> =
            Arc::new(crate::infrastructure::HttpClientPool::default());
        let routes: Arc<dyn crate::ports::RouteRepository> =
            crate::infrastructure::SqliteRouteRepository::new(db.clone());
        let channels: Arc<dyn crate::ports::ChannelRepository> =
            crate::infrastructure::SqliteChannelRepository::new(db.clone());
        let clock: Arc<dyn crate::ports::Clock> = Arc::new(crate::infrastructure::SystemClock);
        let background = crate::infrastructure::RuntimeSupervisor::new(
            tokio_util::sync::CancellationToken::new(),
        );
        let limits = Arc::new(crate::runtime::RuntimeLimits::default());
        let discovery = crate::discovery::DiscoveryService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&background),
            Arc::clone(&limits),
        );
        let notifier: std::sync::Arc<dyn crate::ports::Notifier> =
            crate::notification::DesktopNotifier::new(std::time::Duration::from_millis(50));
        let proxy = crate::proxy::ProxyService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            routes.clone(),
            telemetry.clone(),
            Arc::clone(&clock),
            Arc::clone(&limits),
            crate::notification::DesktopNotifier::new(std::time::Duration::from_millis(50)),
        );
        let balance = crate::balance::BalanceService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            channels.clone(),
            Arc::clone(&clock),
            Arc::clone(&limits),
        );
        let command_code_login = crate::commandcode_login::CommandCodeLogin::new(std::sync::Arc::clone(&http));
        let state = crate::application::Context {
            config: Arc::new(AppConfig::default()),
            db: db.clone(),
            secrets: secrets.clone(),
            http,
            routes,
            channels,
            clock,
            notifier,
            discovery,
            proxy,
            telemetry,
            background,
            limits,
            balance,
            admin: crate::admin::AdminService::new(db.clone(), secrets.clone()),
            command_code_login,
            recovery: crate::auth::RecoverySession::new(),
        };
        (state, dir)
    }

    /// P2-5: capability detection must only aggregate metadata of the route's
    /// own protocol. The claude metadata carries a *smaller* context window so
    /// the pre-fix min-aggregation across protocols would pick it up.
    #[tokio::test]
    async fn detection_uses_route_protocol_metadata_only() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',x'00','',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,metadata_json,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','test-model','Test','discovered',1,?,?,?,?)")
            .bind(r#"{"openai_compatible":{"context_window":128000},"claude":{"context_window":32000}}"#)
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_compatible'),('cm-1','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-1','openai_compatible','test-model',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-1','route-1','cm-1',1,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let caps = detect_model_capabilities(&state, "test-model")
            .await
            .unwrap();
        assert_eq!(
            caps.get("context_window").and_then(Value::as_i64),
            Some(128000),
            "only the route protocol's metadata must be aggregated"
        );
    }

    /// P2-2: a candidate without a `channel_model_protocols` binding is
    /// excluded from capability detection entirely — pre-fix the LEFT JOIN
    /// produced a NULL protocol that fell back to the whole metadata dict.
    #[tokio::test]
    async fn candidate_without_protocol_binding_is_excluded() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',x'00','',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // No channel_model_protocols row at all.
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,metadata_json,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','test-model','Test','discovered',1,?,?,?,?)")
            .bind(r#"{"openai_compatible":{"context_window":128000}}"#)
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-1','openai_compatible','test-model',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-1','route-1','cm-1',1,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let caps = detect_model_capabilities(&state, "test-model")
            .await
            .unwrap();
        assert_eq!(
            caps,
            json!({}),
            "a candidate without a protocol binding must not contribute metadata"
        );
    }
}
