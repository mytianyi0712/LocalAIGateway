use anyhow::{Context, Result};
use serde_json::json;
use sqlx::Row;
use tokio::time::{Duration, timeout};

use crate::{protocol, server::AppState};

pub async fn queue(state: AppState, channel_id: String) -> Result<()> {
    tokio::spawn(async move {
        let _ = probe(&state, &channel_id).await;
    });
    Ok(())
}

async fn probe(state: &AppState, channel_id: &str) -> Result<()> {
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
        return Ok(());
    }
    let key = state
        .secrets
        .decrypt(&row.try_get::<Vec<u8>, _>("api_key_encrypted")?)?;
    let base: String = row.try_get("base_url")?;
    let (path, body): (String, serde_json::Value) = match protocol_name.as_str() {
        "openai_compatible" => (
            "/v1/chat/completions".to_owned(),
            json!({"model":model,"messages":[{"role":"user","content":"Reply only OK"}],"max_tokens":2}),
        ),
        "openai_responses" => (
            "/v1/responses".to_owned(),
            json!({"model":model,"input":"Reply only OK","max_output_tokens":2}),
        ),
        "claude" => (
            "/v1/messages".to_owned(),
            json!({"model":model,"max_tokens":2,"messages":[{"role":"user","content":"Reply only OK"}]}),
        ),
        "gemini" => (
            format!("/v1beta/models/{model}:generateContent"),
            json!({"contents":[{"parts":[{"text":"Reply only OK"}]}],"generationConfig":{"maxOutputTokens":2}}),
        ),
        _ => return Ok(()),
    };
    let url = protocol::upstream_url(&base, &path, None, &protocol_name)?;
    let headers = protocol::outbound_headers(&axum::http::HeaderMap::new(), &protocol_name, &key)?;
    let started = std::time::Instant::now();
    let result = timeout(
        Duration::from_secs(20),
        state.http.post(url).headers(headers).json(&body).send(),
    )
    .await;
    let (success, status, error_kind) = match result {
        Ok(Ok(response)) => {
            let status = response.status();
            let _ = response.bytes().await;
            (status.is_success(), Some(status.as_u16() as i64), None)
        }
        Ok(Err(error)) => (false, None, Some(error.to_string())),
        Err(_) => (false, None, Some("timeout".into())),
    };
    let time = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO health_probe_logs(id,channel_id,model_id,started_at,duration_ms,success,status_code,error_kind,next_probe_at) VALUES(?,?,?,?,?,?,?,?,?)")
        .bind(uuid::Uuid::new_v4().to_string()).bind(channel_id).bind(&model).bind(&time).bind(started.elapsed().as_millis() as i64).bind(success).bind(status).bind(error_kind.clone()).bind(if success{None}else{Some((chrono::Utc::now()+chrono::Duration::seconds(900)).to_rfc3339())}).execute(state.db.pool()).await?;
    if success {
        sqlx::query("UPDATE channel_health SET state='active',consecutive_failures=0,disabled_until=NULL,last_success_at=?,updated_at=? WHERE channel_id=?")
            .bind(&time).bind(&time).bind(channel_id).execute(state.db.pool()).await?;
    } else {
        sqlx::query("UPDATE channel_health SET consecutive_failures=consecutive_failures+1,last_failure_at=?,last_error_kind=?,last_status_code=?,state=CASE WHEN consecutive_failures+1>=3 THEN 'open' ELSE state END,disabled_until=CASE WHEN consecutive_failures+1>=3 THEN ? ELSE disabled_until END,updated_at=? WHERE channel_id=?")
            .bind(&time).bind(error_kind).bind(status).bind((chrono::Utc::now()+chrono::Duration::seconds(900)).to_rfc3339()).bind(&time).bind(channel_id).execute(state.db.pool()).await?;
    }
    Ok(())
}
