//! Model discovery — port of backend/app/services/discovery.py.
//!
//! Fixes over the original port: per-protocol pagination (Claude `after_id`,
//! Gemini `pageToken`), per-protocol metadata merge inside
//! `channel_models.metadata_json` (instead of overwriting the whole dict),
//! stale protocol-binding cleanup with `available` recomputation, and
//! discovery runs that record `status_code` / aggregated `error_kind`.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::Row;
use tokio::time::{Duration, timeout};

use crate::{protocol, server::AppState};

pub async fn queue(state: AppState, channel_id: String) -> Result<String> {
    queue_with_trigger(state, channel_id, "manual").await
}

/// Used by the maintenance supervisor for the scheduled re-discovery cycle.
pub async fn queue_scheduled(state: AppState, channel_id: String) -> Result<String> {
    queue_with_trigger(state, channel_id, "scheduled").await
}

async fn queue_with_trigger(state: AppState, channel_id: String, trigger: &str) -> Result<String> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let time = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO discovery_runs(id,channel_id,trigger,started_at) VALUES(?,?,?,?)")
        .bind(&run_id)
        .bind(&channel_id)
        .bind(trigger)
        .bind(&time)
        .execute(state.db.pool())
        .await?;
    let task_run_id = run_id.clone();
    let task_channel_id = channel_id.clone();
    tokio::spawn(async move {
        let result =
            timeout(Duration::from_secs(120), discover(&state, &task_channel_id)).await;
        let finished = chrono::Utc::now().to_rfc3339();
        match result {
            Ok(Ok((count, status_code, error_kind))) => {
                let _ = sqlx::query(
                    "UPDATE discovery_runs SET finished_at=?,success=?,model_count=?,status_code=?,error_kind=? WHERE id=?",
                )
                .bind(finished)
                .bind(error_kind.is_none())
                .bind(count)
                .bind(status_code)
                .bind(error_kind)
                .bind(&task_run_id)
                .execute(state.db.pool())
                .await;
            }
            Ok(Err(error)) => {
                let _ = sqlx::query(
                    "UPDATE discovery_runs SET finished_at=?,success=0,status_code=NULL,error_kind=? WHERE id=?",
                )
                .bind(finished)
                .bind(error.to_string())
                .bind(&task_run_id)
                .execute(state.db.pool())
                .await;
            }
            Err(_) => {
                let _ = sqlx::query(
                    "UPDATE discovery_runs SET finished_at=?,success=0,status_code=NULL,error_kind=? WHERE id=?",
                )
                .bind(finished)
                .bind("模型探测超时")
                .bind(&task_run_id)
                .execute(state.db.pool())
                .await;
            }
        }
    });
    Ok(run_id)
}

/// Fetch one model catalog with per-protocol pagination, up to 50 pages.
/// Returns (model_id -> item, last status code).
async fn fetch_models(
    state: &AppState,
    base_url: &str,
    protocol_name: &str,
    api_key: &str,
) -> Result<(HashMap<String, Value>, i64)> {
    let mut models: HashMap<String, Value> = HashMap::new();
    let mut url = protocol::upstream_url(
        base_url,
        protocol::discovery_path(protocol_name),
        None,
        protocol_name,
    )?;
    let mut status_code: i64 = 0;
    let mut visited: HashSet<String> = HashSet::new();
    for _ in 0..50 {
        if visited.contains(url.as_str()) {
            break;
        }
        visited.insert(url.to_string());
        let headers =
            protocol::outbound_headers(&axum::http::HeaderMap::new(), protocol_name, api_key)?;
        let response = state.http.get(url.clone()).headers(headers).send().await?;
        status_code = response.status().as_u16() as i64;
        if !response.status().is_success() {
            bail!("{protocol_name} discovery returned {status_code}");
        }
        let body = response.bytes().await?;
        let (items, next) = parse_models(protocol_name, &body, &url)?;
        for item in items {
            if let Some(model_id) = item.get("id").and_then(Value::as_str) {
                models.insert(model_id.to_owned(), item);
            }
        }
        let Some(next_url) = next else { break };
        url = next_url;
    }
    Ok((models, status_code))
}

/// Parse a catalog response; returns (items, optional next-page URL).
fn parse_models(protocol_name: &str, body: &[u8], current_url: &url::Url) -> Result<(Vec<Value>, Option<url::Url>)> {
    let value: Value = serde_json::from_slice(body).context("模型目录 JSON 无效")?;
    let items = if protocol_name == "gemini" {
        value
            .get("models")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    } else {
        value
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    let mut parsed: Vec<Value> = Vec::new();
    for mut item in items {
        if protocol_name == "gemini" {
            let id = item
                .get("name")
                .and_then(Value::as_str)
                .and_then(|name| name.rsplit('/').next())
                .map(str::to_owned);
            match id {
                Some(id) => {
                    if let Some(object) = item.as_object_mut() {
                        object.insert("id".into(), Value::String(id));
                    }
                }
                None => continue,
            }
        }
        if item.get("id").and_then(Value::as_str).is_some() {
            parsed.push(item);
        }
    }
    // Pagination mirrors the Python adapters: Gemini continues on
    // `nextPageToken`; Claude only when the response explicitly says
    // `has_more` and carries `last_id`. Never page unconditionally — many
    // gateways reject synthetic `after_id` requests.
    let next = if protocol_name == "gemini" {
        value
            .get("nextPageToken")
            .and_then(Value::as_str)
            .map(|token| {
                let mut next = current_url.clone();
                next.query_pairs_mut().append_pair("pageToken", token);
                next
            })
    } else if protocol_name == "claude"
        && value.get("has_more").and_then(Value::as_bool) == Some(true)
    {
        value.get("last_id").and_then(Value::as_str).map(|last_id| {
            let mut next = current_url.clone();
            next.query_pairs_mut().append_pair("after_id", last_id);
            next
        })
    } else {
        None
    };
    Ok((parsed, next))
}

/// Run a discovery for one channel. Returns (model_count, last_status_code,
/// aggregated error_kind) so the caller can finalize the discovery run row.
async fn discover(
    state: &AppState,
    channel_id: &str,
) -> Result<(i64, Option<i64>, Option<String>)> {
    let channel = sqlx::query("SELECT c.protocol,c.api_key_encrypted,p.base_url FROM channels c JOIN providers p ON p.id=c.provider_id WHERE c.id=?")
        .bind(channel_id).fetch_optional(state.db.pool()).await?.context("Channel not found")?;
    let primary: String = channel.try_get("protocol")?;
    let configured: Vec<String> = sqlx::query_scalar(
        "SELECT protocol FROM channel_protocols WHERE channel_id=? ORDER BY protocol",
    )
    .bind(channel_id)
    .fetch_all(state.db.pool())
    .await?;
    let protocols = if configured.is_empty() {
        vec![primary]
    } else {
        configured
    };
    let key_bytes: Vec<u8> = channel.try_get("api_key_encrypted")?;
    let api_key = state.secrets.decrypt(&key_bytes)?;
    let base_url: String = channel.try_get("base_url")?;

    // Group protocols sharing one discovery URL + auth (the openai family
    // shares /v1/models with the same Bearer header, mirroring Python).
    let mut groups: Vec<(String, String, Vec<String>)> = Vec::new();
    for protocol_name in &protocols {
        let url = protocol::upstream_url(
            &base_url,
            protocol::discovery_path(protocol_name),
            None,
            protocol_name,
        )?;
        let headers = protocol::outbound_headers(
            &axum::http::HeaderMap::new(),
            protocol_name,
            &api_key,
        )?;
        let mut header_key = String::new();
        for (name, value) in headers.iter() {
            header_key.push_str(&format!("{}:{};", name, value.to_str().unwrap_or("")));
        }
        let url_key = url.to_string();
        match groups
            .iter_mut()
            .find(|(group_url, group_headers, _)| *group_url == url_key && *group_headers == header_key)
        {
            Some((_, _, group_protocols)) => group_protocols.push(protocol_name.clone()),
            None => groups.push((url_key, header_key, vec![protocol_name.clone()])),
        }
    }

    let mut succeeded: HashSet<String> = HashSet::new();
    let mut failed: Vec<String> = Vec::new();
    let mut seen_by_protocol: HashMap<String, HashMap<String, Value>> = HashMap::new();
    let mut last_status_code: Option<i64> = None;
    for (_, _, group_protocols) in &groups {
        let primary_protocol = &group_protocols[0];
        match timeout(
            Duration::from_secs(120),
            fetch_models(state, &base_url, primary_protocol, &api_key),
        )
        .await
        {
            Ok(Ok((models, status_code))) => {
                last_status_code = Some(status_code);
                for protocol_name in group_protocols {
                    succeeded.insert(protocol_name.clone());
                    seen_by_protocol.insert(protocol_name.clone(), models.clone());
                }
            }
            Ok(Err(error)) => {
                tracing::warn!(channel_id, %error, "model discovery failed");
                for protocol_name in group_protocols {
                    failed.push(protocol_name.clone());
                }
            }
            Err(_) => {
                tracing::warn!(channel_id, "model discovery timed out");
                for protocol_name in group_protocols {
                    failed.push(protocol_name.clone());
                }
            }
        }
    }

    let existing_rows = sqlx::query(
        "SELECT id, model_id, display_name, source, metadata_json FROM channel_models WHERE channel_id=?",
    )
    .bind(channel_id)
    .fetch_all(state.db.pool())
    .await?;
    let mut existing: HashMap<String, (String, Option<String>, bool, Option<String>)> =
        HashMap::new();
    for row in existing_rows {
        existing.insert(
            row.try_get("model_id")?,
            (
                row.try_get("id")?,
                row.try_get("display_name")?,
                row.try_get::<String, _>("source")? == "manual",
                row.try_get("metadata_json")?,
            ),
        );
    }
    let binding_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT cm.model_id, cmp.protocol FROM channel_model_protocols cmp \
         JOIN channel_models cm ON cm.id = cmp.channel_model_id WHERE cm.channel_id=?",
    )
    .bind(channel_id)
    .fetch_all(state.db.pool())
    .await?;
    let mut protocol_sets: HashMap<String, HashSet<String>> = HashMap::new();
    for (model_id, protocol_name) in binding_rows {
        protocol_sets
            .entry(model_id)
            .or_default()
            .insert(protocol_name);
    }

    let now = chrono::Utc::now().to_rfc3339();
    for protocol_name in &succeeded {
        let Some(models) = seen_by_protocol.get(protocol_name) else {
            continue;
        };
        for (model_id, item) in models {
            let mut metadata: serde_json::Map<String, Value> = existing
                .get(model_id)
                .and_then(|(_, _, _, metadata)| metadata.clone())
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                .and_then(|value| value.as_object().cloned())
                .unwrap_or_default();
            let mut protocol_meta = item.clone();
            if let Some(object) = protocol_meta.as_object_mut() {
                object.remove("id");
            }
            metadata.insert(protocol_name.clone(), protocol_meta);
            let display_name = item
                .get("display_name")
                .or_else(|| item.get("displayName"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| model_id.clone());
            let metadata_json = serde_json::to_string(&Value::Object(metadata))?;
            match existing.get_mut(model_id) {
                Some((row_id, stored_display, _, _)) => {
                    let updated_display = if stored_display.as_deref().is_some_and(|value| !value.is_empty()) {
                        stored_display.clone()
                    } else {
                        Some(display_name)
                    };
                    sqlx::query(
                        "UPDATE channel_models SET display_name=?,available=1,metadata_json=?,last_seen_at=?,updated_at=? WHERE id=?",
                    )
                    .bind(updated_display)
                    .bind(&metadata_json)
                    .bind(&now)
                    .bind(&now)
                    .bind(row_id.as_str())
                    .execute(state.db.pool())
                    .await?;
                }
                None => {
                    let row_id = uuid::Uuid::new_v4().to_string();
                    sqlx::query(
                        "INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,metadata_json,first_seen_at,last_seen_at,created_at,updated_at) VALUES(?,?,?,?, 'discovered',1,?,?,?,?,?)",
                    )
                    .bind(&row_id)
                    .bind(channel_id)
                    .bind(model_id)
                    .bind(&display_name)
                    .bind(&metadata_json)
                    .bind(&now)
                    .bind(&now)
                    .bind(&now)
                    .bind(&now)
                    .execute(state.db.pool())
                    .await?;
                    existing.insert(
                        model_id.clone(),
                        (row_id, Some(display_name), false, Some(metadata_json.clone())),
                    );
                }
            }
            let row_id = existing.get(model_id).map(|(id, _, _, _)| id.clone()).unwrap_or_default();
            sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)")
                .bind(&row_id)
                .bind(protocol_name)
                .execute(state.db.pool())
                .await?;
            protocol_sets
                .entry(model_id.clone())
                .or_default()
                .insert(protocol_name.clone());
        }
    }

    // Prune bindings for protocols that no longer list the model, and
    // recompute `available` — a model with no remaining protocols is hidden.
    for (model_id, (row_id, _, manual, _)) in &existing {
        if *manual {
            continue;
        }
        let mut stale: Vec<String> = Vec::new();
        if let Some(bound) = protocol_sets.get(model_id) {
            for protocol_name in &succeeded {
                let still_seen = seen_by_protocol
                    .get(protocol_name)
                    .is_some_and(|models| models.contains_key(model_id));
                if bound.contains(protocol_name) && !still_seen {
                    stale.push(protocol_name.clone());
                }
            }
        }
        for protocol_name in &stale {
            sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id=? AND protocol=?")
                .bind(row_id)
                .bind(protocol_name)
                .execute(state.db.pool())
                .await?;
        }
        let remaining = protocol_sets
            .get(model_id)
            .map(|bound| bound.len() - stale.iter().filter(|item| bound.contains(*item)).count())
            .unwrap_or(0);
        if !stale.is_empty() || remaining == 0 {
            sqlx::query("UPDATE channel_models SET available=?,updated_at=? WHERE id=?")
                .bind(remaining > 0)
                .bind(&now)
                .bind(row_id)
                .execute(state.db.pool())
                .await?;
        }
    }

    let model_count = seen_by_protocol
        .values()
        .flat_map(|models| models.keys().cloned().collect::<Vec<_>>())
        .collect::<HashSet<_>>()
        .len() as i64;
    let error_kind = if failed.is_empty() {
        None
    } else {
        Some(format!("protocol_discovery_failed:{}", failed.join(",")))
    };
    Ok((model_count, last_status_code, error_kind))
}
