use std::collections::HashSet;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::Row;
use tokio::time::{Duration, timeout};

use crate::{protocol, server::AppState};

pub async fn queue(state: AppState, channel_id: String) -> Result<String> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let time = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO discovery_runs(id,channel_id,trigger,started_at) VALUES(?,?, 'manual', ?)",
    )
    .bind(&run_id)
    .bind(&channel_id)
    .bind(&time)
    .execute(state.db.pool())
    .await?;
    let task_run_id = run_id.clone();
    tokio::spawn(async move {
        let result = timeout(Duration::from_secs(120), discover(&state, &channel_id)).await;
        let finished = chrono::Utc::now().to_rfc3339();
        match result {
            Ok(Ok(count)) => {
                let _ = sqlx::query(
                    "UPDATE discovery_runs SET finished_at=?,success=1,model_count=? WHERE id=?",
                )
                .bind(finished)
                .bind(count)
                .bind(&task_run_id)
                .execute(state.db.pool())
                .await;
            }
            Ok(Err(error)) => {
                let _ = sqlx::query(
                    "UPDATE discovery_runs SET finished_at=?,success=0,error_kind=? WHERE id=?",
                )
                .bind(finished)
                .bind(error.to_string())
                .bind(&task_run_id)
                .execute(state.db.pool())
                .await;
            }
            Err(_) => {
                let _ = sqlx::query(
                    "UPDATE discovery_runs SET finished_at=?,success=0,error_kind=? WHERE id=?",
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

async fn discover(state: &AppState, channel_id: &str) -> Result<i64> {
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
    let mut seen = HashSet::new();
    let mut total = 0i64;
    for protocol_name in protocols {
        if protocol_name == "openai_responses" && seen.contains("openai_compatible") {
            continue;
        }
        let url = protocol::upstream_url(
            &base_url,
            protocol::discovery_path(&protocol_name),
            None,
            &protocol_name,
        )?;
        let headers =
            protocol::outbound_headers(&axum::http::HeaderMap::new(), &protocol_name, &api_key)?;
        let response = state.http.get(url).headers(headers).send().await?;
        let status = response.status();
        let body = response.bytes().await?;
        if !status.is_success() {
            anyhow::bail!("{} discovery returned {}", protocol_name, status);
        }
        let models = parse_models(&protocol_name, &body)?;
        for model in models {
            let Some(model_id) = model.get("id").and_then(Value::as_str) else {
                continue;
            };
            let display = model
                .get("display_name")
                .or_else(|| model.get("displayName"))
                .and_then(Value::as_str)
                .unwrap_or(model_id);
            let time = chrono::Utc::now().to_rfc3339();
            sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,metadata_json,first_seen_at,last_seen_at,created_at,updated_at) VALUES(?,?,?,?, 'discovered',1,?,?,?,?,?) ON CONFLICT(channel_id,model_id) DO UPDATE SET display_name=excluded.display_name,available=1,metadata_json=excluded.metadata_json,last_seen_at=excluded.last_seen_at,updated_at=excluded.updated_at")
                .bind(uuid::Uuid::new_v4().to_string()).bind(channel_id).bind(model_id).bind(display).bind(model.to_string()).bind(&time).bind(&time).bind(&time).bind(&time).execute(state.db.pool()).await?;
            let model_row: String = sqlx::query_scalar(
                "SELECT id FROM channel_models WHERE channel_id=? AND model_id=?",
            )
            .bind(channel_id)
            .bind(model_id)
            .fetch_one(state.db.pool())
            .await?;
            sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)").bind(model_row).bind(&protocol_name).execute(state.db.pool()).await?;
            if protocol_name == "openai_compatible" {
                sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) VALUES((SELECT id FROM channel_models WHERE channel_id=? AND model_id=?),'openai_responses')").bind(channel_id).bind(model_id).execute(state.db.pool()).await?;
            }
            total += 1;
        }
        seen.insert(protocol_name);
    }
    Ok(total)
}

fn parse_models(protocol_name: &str, body: &[u8]) -> Result<Vec<Value>> {
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
    Ok(items
        .into_iter()
        .filter_map(|mut item| {
            if protocol_name == "gemini" {
                let name = item.get("name").and_then(Value::as_str)?.to_owned();
                let id = name.rsplit('/').next()?.to_owned();
                item.as_object_mut()?.insert("id".into(), Value::String(id));
            }
            if item.get("id").and_then(Value::as_str).is_some() {
                Some(item)
            } else {
                None
            }
        })
        .collect())
}
