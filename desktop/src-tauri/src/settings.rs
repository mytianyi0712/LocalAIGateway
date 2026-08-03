use std::collections::HashMap;

use anyhow::{Result, bail};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;

use crate::server::AppState;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeSettings {
    pub trust_local_network: bool,
    pub failure_threshold: i64,
    pub circuit_open_seconds: i64,
    pub max_failover_attempts: i64,
    pub connect_timeout_seconds: i64,
    pub first_byte_timeout_seconds: i64,
    pub first_token_timeout_seconds: i64,
    pub stream_idle_timeout_seconds: i64,
    pub non_stream_total_timeout_seconds: i64,
    pub model_discovery_interval_hours: i64,
    pub log_retention_days: i64,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            trust_local_network: true,
            failure_threshold: 3,
            circuit_open_seconds: 900,
            max_failover_attempts: 3,
            connect_timeout_seconds: 10,
            first_byte_timeout_seconds: 60,
            first_token_timeout_seconds: 60,
            stream_idle_timeout_seconds: 300,
            non_stream_total_timeout_seconds: 600,
            model_discovery_interval_hours: 24,
            log_retention_days: 30,
        }
    }
}

impl RuntimeSettings {
    pub fn as_value(&self) -> Value {
        serde_json::to_value(self).expect("settings serialize")
    }
}

pub async fn runtime_settings(state: &AppState) -> Result<RuntimeSettings> {
    let mut value = RuntimeSettings::default().as_value();
    let rows = sqlx::query("SELECT key, CAST(value_json AS TEXT) value_json FROM settings WHERE key NOT IN ('admin_access_key', 'gateway_access_key')")
        .fetch_all(state.db.pool()).await?;
    for row in rows {
        let key: String = row.try_get("key")?;
        let raw: String = row.try_get("value_json")?;
        if let Ok(item) = serde_json::from_str(&raw) {
            value[&key] = item;
        }
    }
    Ok(serde_json::from_value(value)?)
}

pub struct AccessPolicy {
    pub trust_local_network: bool,
    pub admin_key: String,
    pub gateway_key: String,
}

pub async fn access_policy(state: &AppState) -> Result<AccessPolicy> {
    let settings = runtime_settings(state).await?;
    let rows = sqlx::query("SELECT key, CAST(value_json AS TEXT) value_json FROM settings WHERE key IN ('admin_access_key', 'gateway_access_key')")
        .fetch_all(state.db.pool()).await?;
    let mut keys = HashMap::new();
    for row in rows {
        let key: String = row.try_get("key")?;
        let raw: String = row.try_get("value_json")?;
        if let Ok(token) = serde_json::from_str::<String>(&raw) {
            if let Ok(value) = state.secrets.decrypt(token.as_bytes()) {
                keys.insert(key, value);
            }
        }
    }
    Ok(AccessPolicy {
        trust_local_network: settings.trust_local_network,
        admin_key: keys.remove("admin_access_key").unwrap_or_default(),
        gateway_key: keys.remove("gateway_access_key").unwrap_or_default(),
    })
}

pub async fn authorize_admin(state: &AppState, headers: &HeaderMap) -> Result<bool> {
    let policy = access_policy(state).await?;
    if policy.trust_local_network {
        return Ok(true);
    }
    Ok(bearer(headers)
        .is_some_and(|value| constant_time_eq(value.as_bytes(), policy.admin_key.as_bytes())))
}

pub async fn authorize_gateway(
    state: &AppState,
    headers: &HeaderMap,
    query: Option<&str>,
    protocol: &str,
) -> Result<bool> {
    let policy = access_policy(state).await?;
    if policy.trust_local_network {
        return Ok(true);
    }
    let supplied = headers
        .get("x-local-gateway-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or_else(|| match protocol {
            "openai_compatible" | "openai_responses" | "catalog" => {
                bearer(headers).map(str::to_owned)
            }
            "claude" => headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            "gemini" => headers
                .get("x-goog-api-key")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
                .or_else(|| {
                    query.and_then(|q| {
                        url::form_urlencoded::parse(q.as_bytes())
                            .find(|(k, _)| k == "key")
                            .map(|(_, v)| v.into_owned())
                    })
                }),
            _ => None,
        });
    Ok(supplied
        .is_some_and(|value| constant_time_eq(value.as_bytes(), policy.gateway_key.as_bytes())))
}

pub fn validate_updates(value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("设置必须是 JSON 对象"))?;
    let ranges: HashMap<&str, (i64, i64)> = HashMap::from([
        ("failure_threshold", (1, 20)),
        ("circuit_open_seconds", (0, 86400)),
        ("max_failover_attempts", (1, 20)),
        ("connect_timeout_seconds", (1, 120)),
        ("first_byte_timeout_seconds", (1, 600)),
        ("first_token_timeout_seconds", (1, 600)),
        ("stream_idle_timeout_seconds", (10, 3600)),
        ("non_stream_total_timeout_seconds", (10, 3600)),
        ("model_discovery_interval_hours", (1, 168)),
        ("log_retention_days", (1, 365)),
    ]);
    for (key, value) in object {
        if matches!(key.as_str(), "admin_access_key" | "gateway_access_key") {
            continue;
        }
        if key == "trust_local_network" {
            if !value.is_boolean() {
                bail!("trust_local_network 必须是布尔值");
            }
            continue;
        }
        let Some((min, max)) = ranges.get(key.as_str()) else {
            bail!("未知设置：{key}");
        };
        let number = value
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("{key} 必须是整数"))?;
        if number < *min || number > *max {
            bail!("{key} 必须在 {min}–{max} 之间");
        }
    }
    Ok(())
}

pub async fn save_setting(state: &AppState, key: &str, value: &Value) -> Result<()> {
    sqlx::query("INSERT INTO settings(key, value_json, updated_at) VALUES(?, ?, ?) \
                 ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at")
        .bind(key).bind(serde_json::to_string(value)?).bind(chrono::Utc::now().to_rfc3339())
        .execute(state.db.pool()).await?;
    Ok(())
}

pub fn settings_with_hints(settings: &RuntimeSettings, policy: &AccessPolicy) -> Value {
    let mut value = settings.as_value();
    value["admin_key_hint"] = json!(if policy.admin_key.is_empty() {
        String::new()
    } else {
        crate::crypto::SecretStore::hint(&policy.admin_key)
    });
    value["gateway_key_hint"] = json!(if policy.gateway_key.is_empty() {
        String::new()
    } else {
        crate::crypto::SecretStore::hint(&policy.gateway_key)
    });
    value["access_keys_configured"] =
        json!(!policy.admin_key.is_empty() && !policy.gateway_key.is_empty());
    value
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}
