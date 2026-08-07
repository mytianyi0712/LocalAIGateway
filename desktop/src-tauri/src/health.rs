//! Channel health probing and automatic circuit recovery — port of
//! backend/app/services/health.py.
//!
//! `probe` checks a channel with a minimal per-protocol request and treats a
//! response as healthy only when it is 2xx **and** carries a completion or
//! first-token signal (a 2xx JSON error body must not reset the circuit).
//! Probe failures open the circuit immediately with the configured
//! `circuit_open_seconds` cooldown.
//!
//! `spawn_supervisor` runs a background loop that re-probes every open,
//! enabled channel once its `disabled_until` has passed — the Rust equivalent
//! of the Python `HealthSupervisor` half-open recovery (C3).

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::Row;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, timeout};

use crate::{protocol, server::AppState, settings};

/// The per-protocol probe body/endpoint — mirror of `adapter.health_probe`.
fn probe_request(protocol: &str, model: &str) -> Option<(String, serde_json::Value)> {
    match protocol {
        "openai_compatible" => Some((
            "/v1/chat/completions".to_owned(),
            json!({"model": model, "messages": [{"role": "user", "content": "Reply only OK"}], "max_tokens": 2}),
        )),
        "openai_responses" => Some((
            "/v1/responses".to_owned(),
            json!({"model": model, "input": "Reply only OK", "max_output_tokens": 2}),
        )),
        "claude" => Some((
            "/v1/messages".to_owned(),
            json!({"model": model, "max_tokens": 2, "messages": [{"role": "user", "content": "Reply only OK"}]}),
        )),
        "gemini" => Some((
            format!("/v1beta/models/{model}:generateContent"),
            json!({"contents": [{"parts": [{"text": "Reply only OK"}]}], "generationConfig": {"maxOutputTokens": 2}}),
        )),
        _ => None,
    }
}

/// A probe succeeds only when the upstream answered 2xx **and** the body
/// carries a completion or first content signal (Python: `is_success and
/// (saw_completion or first_token_at is not None)`).
fn probe_body_ok(protocol: &str, body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    if !value.is_object() {
        return false;
    }
    match protocol {
        "openai_compatible" => {
            value.pointer("/choices/0/finish_reason").is_some()
                || value.pointer("/choices/0/message/content").is_some_and(
                    |content| match content {
                        Value::String(text) => !text.is_empty(),
                        Value::Array(parts) => !parts.is_empty(),
                        _ => false,
                    },
                )
        }
        "openai_responses" => {
            value.get("status").and_then(Value::as_str) == Some("completed")
                || value.get("output").is_some()
                || value
                    .get("output_text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| !text.is_empty())
        }
        "claude" => {
            value.get("stop_reason").is_some()
                || value
                    .get("content")
                    .and_then(Value::as_array)
                    .is_some_and(|blocks| !blocks.is_empty())
        }
        "gemini" => {
            value.pointer("/candidates/0/finishReason").is_some()
                || value
                    .pointer("/candidates/0/content/parts")
                    .and_then(Value::as_array)
                    .is_some_and(|parts| !parts.is_empty())
        }
        _ => true,
    }
}

pub async fn queue(state: AppState, channel_id: String) -> Result<()> {
    tokio::spawn(async move {
        let _ = probe(&state, &channel_id).await;
    });
    Ok(())
}

/// Probe one channel and update its health state. A failed probe opens the
/// circuit immediately (Python uses threshold 1 for probes) with the runtime
/// `circuit_open_seconds` cooldown; a successful probe resets it.
async fn probe(state: &AppState, channel_id: &str) -> Result<bool> {
    let row = sqlx::query("SELECT c.protocol,c.health_check_model_id,c.api_key_encrypted,p.base_url FROM channels c JOIN providers p ON p.id=c.provider_id WHERE c.id=?")
        .bind(channel_id).fetch_optional(state.db.pool()).await?.context("Channel not found")?;
    let protocol_name: String = row.try_get("protocol")?;
    let configured_model: Option<String> = row.try_get("health_check_model_id")?;
    let model = match configured_model.filter(|value| !value.is_empty()) {
        Some(value) => value,
        None => sqlx::query_scalar("SELECT model_id FROM channel_models WHERE channel_id=? AND available=1 ORDER BY model_id LIMIT 1")
            .bind(channel_id).fetch_optional(state.db.pool()).await?.unwrap_or_default(),
    };
    if model.is_empty() {
        return Ok(false);
    }
    let Some((path, body)) = probe_request(&protocol_name, &model) else {
        return Ok(false);
    };
    let key = state
        .secrets
        .decrypt(&row.try_get::<Vec<u8>, _>("api_key_encrypted")?)?;
    let base: String = row.try_get("base_url")?;
    let url = protocol::upstream_url(&base, &path, None, &protocol_name)?;
    let headers = protocol::outbound_headers(&axum::http::HeaderMap::new(), &protocol_name, &key)?;
    let runtime = settings::runtime_settings(state).await?;
    let open_seconds = runtime.circuit_open_seconds.max(1);
    let started = Instant::now();
    let result = timeout(
        Duration::from_secs(20),
        state.http.post(url).headers(headers).json(&body).send(),
    )
    .await;
    let (success, status, error_kind) = match result {
        Ok(Ok(response)) => {
            let status_code = response.status();
            let status = status_code.as_u16() as i64;
            let body = match timeout(Duration::from_secs(20), response.bytes()).await {
                Ok(Ok(body)) => body.to_vec(),
                _ => Vec::new(),
            };
            (
                status_code.is_success() && probe_body_ok(&protocol_name, &body),
                Some(status),
                None,
            )
        }
        Ok(Err(error)) => (false, None, Some(error.to_string())),
        Err(_) => (false, None, Some("timeout".into())),
    };
    let time = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO health_probe_logs(id,channel_id,model_id,started_at,duration_ms,success,status_code,error_kind,next_probe_at) VALUES(?,?,?,?,?,?,?,?,?)")
        .bind(uuid::Uuid::new_v4().to_string()).bind(channel_id).bind(&model).bind(&time).bind(started.elapsed().as_millis() as i64).bind(success).bind(status).bind(error_kind.clone()).bind(if success { None } else { Some((chrono::Utc::now() + chrono::Duration::seconds(open_seconds)).to_rfc3339()) }).execute(state.db.pool()).await?;
    if success {
        sqlx::query("UPDATE channel_health SET state='active',consecutive_failures=0,disabled_until=NULL,last_success_at=?,last_error_kind=NULL,last_status_code=NULL,updated_at=? WHERE channel_id=?")
            .bind(&time).bind(&time).bind(channel_id).execute(state.db.pool()).await?;
    } else {
        sqlx::query("UPDATE channel_health SET consecutive_failures=consecutive_failures+1,last_failure_at=?,last_error_kind=?,last_status_code=?,state='open',disabled_until=?,updated_at=? WHERE channel_id=?")
            .bind(&time).bind(error_kind).bind(status).bind((chrono::Utc::now() + chrono::Duration::seconds(open_seconds)).to_rfc3339()).bind(&time).bind(channel_id).execute(state.db.pool()).await?;
    }
    Ok(success)
}

/// Background supervisor: polls every few seconds for open channels whose
/// cooldown has expired and probes them (deduplicated while in flight).
/// Mirrors the Python `HealthSupervisor` auto-recovery loop (C3).
pub fn spawn_supervisor(state: AppState) {
    tokio::spawn(async move {
        let probing: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let due: Vec<String> = match sqlx::query_scalar(
                "SELECT ch.channel_id FROM channel_health ch \
                 JOIN channels c ON c.id = ch.channel_id \
                 WHERE ch.state = 'open' AND c.manual_enabled = 1 \
                   AND ch.disabled_until IS NOT NULL AND ch.disabled_until <= ?",
            )
            .bind(chrono::Utc::now().to_rfc3339())
            .fetch_all(state.db.pool())
            .await
            {
                Ok(due) => due,
                Err(error) => {
                    tracing::warn!(%error, "health supervisor query failed");
                    continue;
                }
            };
            for channel_id in due {
                let mut guard = probing.lock().await;
                if guard.contains(&channel_id) {
                    continue;
                }
                guard.insert(channel_id.clone());
                drop(guard);
                let state = state.clone();
                let probing = probing.clone();
                tokio::spawn(async move {
                    let _ = probe(&state, &channel_id).await;
                    probing.lock().await.remove(&channel_id);
                });
            }
        }
    });
}
