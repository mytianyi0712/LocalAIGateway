//! 模型能力（capability）检测。
//!
//! 能力从模型发现阶段捕获的元数据推断（`channel_models.metadata_json` 内
//! 按协议分组的字典），并跨某条路由的全部存活候选项聚合；`source = "auto"`
//! 的行在读取时重新检测，`source = "manual"` 的行保存显式取值。

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

/// 从发现元数据字典（或按协议分组的子字典）中提取能力字段。
/// 只有检测到取值的键才会出现在返回值中。
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
        // 保留整数：全为整数时取最小值并保持整数类型（不转成浮点）。
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

/// 聚合一组已提取的能力字典：数值取最小值，布尔取逻辑与，
/// thinking 映射按层级逐级合并。
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

/// 依据路由各存活候选项的发现元数据重新计算该路由模型的能力。
/// 纯本地计算，绝不访问网络。
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
        // INNER JOIN 保证候选项有协议绑定；无绑定的候选项被直接排除，
        // 不会退化成用整份元数据兜底。
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

/// 管理端 API 返回、并嵌入模型目录的完整能力对象。
/// `source = "auto"`（或缺少记录）时在读取时重新检测。
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

/// 嵌入 `x_local_gateway.pi_model_config` 的能力摘要。
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

/// 目录条目的 `x_local_gateway` 元数据（仅 capabilities/pi_model_config 部分）。
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
    use crate::state::AppState;
    use crate::test_support::TempDir;

    async fn test_state() -> (AppState, TempDir) {
        // 统一夹具：临时目录 + 整套 Context（见 `crate::test_support`）。
        let crate::test_support::TestEnv {
            dir,
            context: state,
        } = crate::test_support::context("caps").await;
        (state, dir)
    }

    /// 能力检测只能聚合路由自身协议的元数据：claude 元数据携带*更小*的
    /// 上下文窗口，若跨协议取最小值就会错误地把它采纳。
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

    /// 没有 `channel_model_protocols` 绑定的候选项被完全排除在能力检测之外：
    /// 早先的 LEFT JOIN 会产生 NULL 协议，从而退化成用整份元数据兜底。
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
        // 完全没有 channel_model_protocols 记录。
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
