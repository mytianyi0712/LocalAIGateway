use std::{convert::Infallible, pin::Pin, sync::Arc, sync::atomic::{AtomicBool, Ordering}, task::{Context, Poll}, time::Duration};

use anyhow::Result;
use async_stream::stream;
use axum::{
    body::{Body, to_bytes},
    extract::{Path, Request, State},
    http::{HeaderValue, Response, StatusCode},
    response::IntoResponse,
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};
use sqlx::Row;

use crate::{
    capabilities, convert, protocol,
    routing::{self, Candidate},
    server::AppState,
    settings,
    telemetry::{Event, Usage},
};

const MAX_REQUEST_BODY: usize = 8 * 1024 * 1024;

/// Wraps an upstream byte stream so a client-side disconnect (which drops the
/// response body without polling it to completion) still records a cancelled
/// attempt instead of leaving the request permanently pending.
struct CancelAware<S> {
    inner: S,
    completed: Arc<AtomicBool>,
    responded: Arc<AtomicBool>,
    on_cancel: Option<Box<dyn FnOnce() + Send>>,
}

impl<S: futures_util::Stream + Unpin> futures_util::Stream for CancelAware<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                self.responded.store(true, Ordering::SeqCst);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                self.completed.store(true, Ordering::SeqCst);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Drop for CancelAware<S> {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::SeqCst) {
            if let Some(callback) = self.on_cancel.take() {
                callback();
            }
        }
    }
}

#[derive(Clone)]
struct MappingTarget {
    entry: String,
    upstream_protocol: String,
    upstream_model: String,
}

fn json_response(status: StatusCode, value: Value) -> Response<Body> {
    let mut response = (status, axum::Json(value)).into_response();
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    response
}

fn gateway_error(
    entry: &str,
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
) -> Response<Body> {
    if entry == "claude" {
        json_response(
            status,
            json!({"type":"error","error":{"type":code,"message":message},"request_id":request_id}),
        )
    } else if entry == "gemini" {
        json_response(
            status,
            json!({"error":{"code":status.as_u16(),"message":message,"status":code},"request_id":request_id}),
        )
    } else {
        json_response(
            status,
            json!({"error":{"message":message,"type":code,"code":code,"request_id":request_id}}),
        )
    }
}

fn status_kind(status: StatusCode) -> (&'static str, bool) {
    if status == StatusCode::TOO_MANY_REQUESTS {
        ("rate_limit", true)
    } else if status == StatusCode::REQUEST_TIMEOUT || status == StatusCode::GATEWAY_TIMEOUT {
        ("timeout", true)
    } else if status.is_server_error() {
        ("upstream_5xx", true)
    } else if status.is_client_error() {
        ("upstream_4xx", false)
    } else {
        ("upstream_error", false)
    }
}

fn response_with_headers(
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    for (name, value) in headers {
        if let Some(name) = name {
            response.headers_mut().append(name, value);
        }
    }
    response
}

fn mapped_path(
    mapping: &MappingTarget,
    request_path: &str,
    stream_requested: bool,
    model: &str,
) -> String {
    if mapping.entry == "claude" && request_path.starts_with("/claudecode/") {
        return match mapping.upstream_protocol.as_str() {
            "openai_compatible" => "/v1/chat/completions".into(),
            "openai_responses" => "/v1/responses".into(),
            "claude" => "/v1/messages".into(),
            "gemini" => format!(
                "/v1beta/models/{model}:{}",
                if stream_requested {
                    "streamGenerateContent"
                } else {
                    "generateContent"
                }
            ),
            _ => request_path.into(),
        };
    }
    if mapping.entry == "openai_responses" && request_path.starts_with("/codex/") {
        return match mapping.upstream_protocol.as_str() {
            "openai_compatible" => "/v1/chat/completions".into(),
            "openai_responses" => "/v1/responses".into(),
            "claude" => "/v1/messages".into(),
            "gemini" => format!(
                "/v1beta/models/{model}:{}",
                if stream_requested {
                    "streamGenerateContent"
                } else {
                    "generateContent"
                }
            ),
            _ => request_path.into(),
        };
    }
    request_path.into()
}

async fn resolve_mapping(
    state: &AppState,
    entry: &str,
    model: &str,
) -> Result<Option<MappingTarget>> {
    let (table, id_column) = match entry {
        "claude" => ("claude_model_mappings", "claude_model_id"),
        "openai_responses" => ("codex_model_mappings", "codex_model_id"),
        _ => return Ok(None),
    };
    let sql = format!(
        "SELECT upstream_protocol,upstream_model_id FROM {table} WHERE {id_column}=? AND enabled=1"
    );
    let row = sqlx::query(&sql)
        .bind(model)
        .fetch_optional(state.db.pool())
        .await?;
    Ok(row.map(|row| MappingTarget {
        entry: entry.to_owned(),
        upstream_protocol: row.get("upstream_protocol"),
        upstream_model: row.get("upstream_model_id"),
    }))
}

fn attempt_event(
    request_id: &str,
    candidate: &Candidate,
    attempt_no: i64,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: chrono::DateTime<chrono::Utc>,
    status: Option<i64>,
    outcome: &str,
    error_kind: Option<String>,
    failover: bool,
    response_started: bool,
    first_byte_ms: Option<i64>,
    first_token_ms: Option<i64>,
    usage: Usage,
    response_bytes: i64,
    upstream_protocol: Option<String>,
    upstream_model_id: Option<String>,
) -> Event {
    Event::Attempt {
        id: uuid::Uuid::new_v4().to_string(),
        request_id: request_id.to_owned(),
        channel_id: candidate.channel_id.clone(),
        channel_name: candidate.channel_name.clone(),
        attempt_no,
        priority: candidate.priority,
        started_at: started_at.to_rfc3339(),
        finished_at: finished_at.to_rfc3339(),
        status,
        outcome: outcome.to_owned(),
        error_kind,
        failover,
        response_started,
        first_byte_ms,
        first_token_ms,
        duration_ms: finished_at.signed_duration_since(started_at).num_milliseconds(),
        usage,
        response_bytes,
        upstream_protocol,
        upstream_model_id,
    }
}

/// Extracts the per-protocol usage object from a streamed SSE value or a
/// non-streamed response root, mirroring the Python adapters.
fn stream_usage_value(protocol: &str, value: &Value) -> Option<Value> {
    match protocol {
        "claude" => value
            .get("message")
            .and_then(|message| message.get("usage"))
            .cloned()
            .or_else(|| value.get("usage").cloned()),
        "gemini" => value.get("usageMetadata").cloned(),
        // OpenAI Responses API carries streamed usage inside the
        // response.completed event's response object, not at the top level.
        _ => value
            .get("usage")
            .cloned()
            .or_else(|| value.get("response").and_then(|item| item.get("usage")).cloned()),
    }
}

/// Normalizes a raw usage object into the gateway's Usage model, mirroring the
/// Python adapters (OpenAI chat/completions, OpenAI responses, Claude, Gemini).
fn normalize_usage(protocol: &str, usage: &Value) -> Usage {
    let raw = Some(usage.clone());
    match protocol {
        "claude" => {
            let read = usage.get("cache_read_input_tokens").and_then(Value::as_i64);
            let write = usage.get("cache_creation_input_tokens").and_then(Value::as_i64);
            let miss = usage.get("input_tokens").and_then(Value::as_i64);
            let total = if miss.is_some() || read.is_some() || write.is_some() {
                Some(miss.unwrap_or(0) + read.unwrap_or(0) + write.unwrap_or(0))
            } else {
                None
            };
            Usage {
                input_tokens: total,
                cache_read_tokens: read,
                cache_write_tokens: write,
                cache_miss_input_tokens: miss,
                output_tokens: usage.get("output_tokens").and_then(Value::as_i64),
                raw,
            }
        }
        "gemini" => {
            let total = usage.get("promptTokenCount").and_then(Value::as_i64);
            let cache = usage.get("cachedContentTokenCount").and_then(Value::as_i64);
            let miss = total.map(|value| (value - cache.unwrap_or(0)).max(0));
            Usage {
                input_tokens: total,
                cache_read_tokens: cache,
                cache_write_tokens: None,
                cache_miss_input_tokens: miss,
                output_tokens: usage.get("candidatesTokenCount").and_then(Value::as_i64),
                raw,
            }
        }
        _ => {
            let details = usage
                .get("prompt_tokens_details")
                .or_else(|| usage.get("input_tokens_details"));
            let total_input = usage
                .get("prompt_tokens")
                .and_then(Value::as_i64)
                .or_else(|| usage.get("input_tokens").and_then(Value::as_i64));
            let cache_read = details
                .and_then(|item| item.get("cached_tokens"))
                .and_then(Value::as_i64);
            let cache_write = details
                .and_then(|item| {
                    item.get("cache_write_tokens")
                        .or_else(|| item.get("cached_write_tokens"))
                })
                .and_then(Value::as_i64);
            let miss = match (total_input, cache_read) {
                (Some(total), Some(read)) => Some((total - read - cache_write.unwrap_or(0)).max(0)),
                _ => None,
            };
            let output = usage
                .get("completion_tokens")
                .and_then(Value::as_i64)
                .or_else(|| usage.get("output_tokens").and_then(Value::as_i64));
            Usage {
                input_tokens: total_input,
                cache_read_tokens: cache_read,
                cache_write_tokens: cache_write,
                cache_miss_input_tokens: miss,
                output_tokens: output,
                raw,
            }
        }
    }
}

/// Parses a non-streamed upstream response body into Usage.
fn usage_from_body(protocol: &str, body: &[u8]) -> Usage {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Usage::default();
    };
    match stream_usage_value(protocol, &value) {
        Some(usage) => normalize_usage(protocol, &usage),
        None => Usage::default(),
    }
}

async fn read_body(
    request: Request,
) -> Result<(axum::http::HeaderMap, String, Option<String>, Vec<u8>)> {
    let headers = request.headers().clone();
    let path = request.uri().path().to_owned();
    let query = request.uri().query().map(str::to_owned);
    let body = to_bytes(request.into_body(), MAX_REQUEST_BODY).await?;
    Ok((headers, path, query, body.to_vec()))
}

async fn proxy(
    state: AppState,
    request: Request,
    entry_protocol: &str,
    fixed_path: Option<String>,
) -> Response<Body> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let started = chrono::Utc::now();
    let started_at = started.to_rfc3339();
    let (headers, request_path, query_string, body) = match read_body(request).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry_protocol,
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                &error.to_string(),
                &request_id,
            );
        }
    };
    let path = fixed_path.unwrap_or(request_path);
    let query = query_string.as_deref();
    if !settings::authorize_gateway(&state, &headers, query, entry_protocol)
        .await
        .unwrap_or(false)
    {
        return gateway_error(
            entry_protocol,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            &request_id,
        );
    }
    let (entry_model, stream_requested) =
        protocol::inspect_request(entry_protocol, &path, query, &body);
    let Some(entry_model) = entry_model else {
        return gateway_error(
            entry_protocol,
            StatusCode::BAD_REQUEST,
            "model_required",
            "Unable to determine model from request.",
            &request_id,
        );
    };
    // Model mappings only apply to the dedicated mapping entry points
    // (/codex/v1/responses, /claudecode/v1/messages); the regular protocol
    // endpoints (/v1/responses, /v1/messages) are always routed directly,
    // mirroring the Python gateway.
    let is_mapped_entry = (entry_protocol == "openai_responses" && path.starts_with("/codex/"))
        || (entry_protocol == "claude" && path.starts_with("/claudecode/"));
    let mapping = if is_mapped_entry {
        match resolve_mapping(&state, entry_protocol, &entry_model).await {
            Ok(value) => value,
            Err(error) => {
                return gateway_error(
                    entry_protocol,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database_error",
                    &error.to_string(),
                    &request_id,
                );
            }
        }
    } else {
        None
    };
    // C1: a mapped entry with an unknown or disabled mapping must fail with a
    // clear 404 (Python: `unknown_mapped_model`) instead of silently routing
    // the request as a regular protocol request.
    if is_mapped_entry && mapping.is_none() {
        let finished = chrono::Utc::now();
        state.telemetry.emit(Event::RequestStart {
            id: request_id.clone(),
            protocol: entry_protocol.to_owned(),
            model_id: Some(entry_model.clone()),
            endpoint: path.clone(),
            stream: stream_requested,
            started_at: started_at.clone(),
            request_bytes: body.len() as i64,
        });
        state.telemetry.emit(Event::RequestFinish {
            id: request_id.clone(),
            finished_at: finished.to_rfc3339(),
            duration_ms: finished.signed_duration_since(started).num_milliseconds(),
            status: Some(404),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return gateway_error(
            entry_protocol,
            StatusCode::NOT_FOUND,
            "unknown_mapped_model",
            &format!("Model '{entry_model}' is not a configured model mapping."),
            &request_id,
        );
    }
    let upstream_protocol = mapping
        .as_ref()
        .map(|value| value.upstream_protocol.as_str())
        .unwrap_or(entry_protocol);
    let upstream_model = mapping
        .as_ref()
        .map(|value| value.upstream_model.as_str())
        .unwrap_or(entry_model.as_str());
    let converted_body = if let Some(value) = mapping.as_ref() {
        match convert::convert_request(
            &value.entry,
            &value.upstream_protocol,
            &value.upstream_model,
            &body,
        ) {
            Ok(body) => body,
            Err(error) => {
                return gateway_error(
                    entry_protocol,
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    &error.to_string(),
                    &request_id,
                );
            }
        }
    } else {
        body.clone()
    };
    let (model_id, stream) =
        protocol::inspect_request(upstream_protocol, &path, query, &converted_body);
    let stream_requested = stream_requested || stream;
    let route_model = model_id.as_deref().unwrap_or(upstream_model);
    state.telemetry.emit(Event::RequestStart {
        id: request_id.clone(),
        protocol: entry_protocol.to_owned(),
        model_id: Some(entry_model.clone()),
        endpoint: path.clone(),
        stream: stream_requested,
        started_at: started_at.clone(),
        request_bytes: body.len() as i64,
    });
    let runtime = match settings::runtime_settings(&state).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_error",
                &error.to_string(),
                &request_id,
            );
        }
    };
    let candidates = match routing::resolve_candidates(
        &state,
        upstream_protocol,
        route_model,
        runtime.max_failover_attempts,
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "routing_error",
                &error.to_string(),
                &request_id,
            );
        }
    };
    if candidates.is_empty() {
        state.telemetry.emit(Event::RequestFinish {
            id: request_id.clone(),
            finished_at: chrono::Utc::now().to_rfc3339(),
            duration_ms: chrono::Utc::now()
                .signed_duration_since(started)
                .num_milliseconds(),
            status: Some(503),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return gateway_error(
            entry_protocol,
            StatusCode::SERVICE_UNAVAILABLE,
            "no_active_channel",
            "No active channel is available for this model.",
            &request_id,
        );
    }
    let mut last_error: Option<(StatusCode, axum::http::HeaderMap, Vec<u8>, String)> = None;
    let mut attempts = 0i64;
    for (index, candidate) in candidates.iter().enumerate() {
        attempts = index as i64 + 1;
        let attempt_started = chrono::Utc::now();
        let api_key = match state.secrets.decrypt(&candidate.api_key_encrypted) {
            Ok(value) => value,
            Err(error) => {
                last_error = None;
                state.telemetry.emit(attempt_event(
                    &request_id,
                    candidate,
                    attempts,
                    attempt_started,
                    chrono::Utc::now(),
                    None,
                    "transport_error",
                    Some("key_decrypt_error".into()),
                    attempts < candidates.len() as i64,
                    false,
                    None,
                    None,
                    Usage::default(),
                    0,
                    Some(upstream_protocol.into()),
                    Some(upstream_model.into()),
                ));
                let _ = error;
                continue;
            }
        };
        let target_path = mapping
            .as_ref()
            .map(|value| mapped_path(value, &path, stream_requested, &value.upstream_model))
            .unwrap_or_else(|| path.clone());
        let target_url = match protocol::upstream_url(
            &candidate.base_url,
            &target_path,
            query,
            upstream_protocol,
        ) {
            Ok(value) => value,
            Err(error) => {
                last_error = None;
                let _ = error;
                continue;
            }
        };
        let outbound = match protocol::outbound_headers(&headers, upstream_protocol, &api_key) {
            Ok(value) => value,
            Err(error) => {
                let _ = error;
                continue;
            }
        };
        let send = state
            .http
            .request(reqwest::Method::from_bytes(b"POST").unwrap(), target_url)
            .headers(outbound)
            .body(converted_body.clone())
            .send();
        let response = match tokio::time::timeout(
            Duration::from_secs(runtime.first_byte_timeout_seconds.max(1) as u64),
            send,
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let finished = chrono::Utc::now();
                state.telemetry.emit(Event::ChannelFailure {
                    channel_id: candidate.channel_id.clone(),
                    error_kind: "transport_error".into(),
                    status: None,
                    threshold: runtime.failure_threshold,
                    open_seconds: runtime.circuit_open_seconds,
                    countable: true,
                });
                state.telemetry.emit(attempt_event(
                    &request_id,
                    candidate,
                    attempts,
                    attempt_started,
                    finished,
                    None,
                    "transport_error",
                    Some(error.to_string()),
                    attempts < candidates.len() as i64,
                    false,
                    None,
                    None,
                    Usage::default(),
                    0,
                    Some(upstream_protocol.into()),
                    Some(upstream_model.into()),
                ));
                continue;
            }
            Err(_) => {
                let finished = chrono::Utc::now();
                state.telemetry.emit(Event::ChannelFailure {
                    channel_id: candidate.channel_id.clone(),
                    error_kind: "timeout".into(),
                    status: None,
                    threshold: runtime.failure_threshold,
                    open_seconds: runtime.circuit_open_seconds,
                    countable: true,
                });
                state.telemetry.emit(attempt_event(
                    &request_id,
                    candidate,
                    attempts,
                    attempt_started,
                    finished,
                    None,
                    "transport_error",
                    Some("timeout".into()),
                    attempts < candidates.len() as i64,
                    false,
                    None,
                    None,
                    Usage::default(),
                    0,
                    Some(upstream_protocol.into()),
                    Some(upstream_model.into()),
                ));
                continue;
            }
        };
        let status = response.status();
        let response_headers = protocol::response_headers(response.headers());
        if status.is_success() {
            if stream_requested && mapping.is_none() {
                let upstream_stream = response.bytes_stream();
                let response_headers_for_client = response_headers.clone();
                let telemetry = state.telemetry.clone();
                let request_id_stream = request_id.clone();
                let candidate_stream = candidate.clone();
                let upstream_protocol_stream = upstream_protocol.to_owned();
                let upstream_model_stream = upstream_model.to_owned();
                let stream_protocol = upstream_protocol_stream.clone();
                let failure_threshold = runtime.failure_threshold;
                let circuit_open_seconds = runtime.circuit_open_seconds;
                let cancel_completed = Arc::new(AtomicBool::new(false));
                let cancel_responded = Arc::new(AtomicBool::new(false));
                let cancel_telemetry = state.telemetry.clone();
                let cancel_request_id = request_id.clone();
                let cancel_candidate = candidate.clone();
                let cancel_attempt_no = attempts;
                let cancel_attempt_started = attempt_started;
                let cancel_status = status.as_u16() as i64;
                let cancel_upstream_protocol = upstream_protocol_stream.clone();
                let cancel_upstream_model = upstream_model_stream.clone();
                let cancel_responded_flag = cancel_responded.clone();
                let on_cancel: Box<dyn FnOnce() + Send> = Box::new(move || {
                    let finished = chrono::Utc::now();
                    cancel_telemetry.emit(attempt_event(
                        &cancel_request_id,
                        &cancel_candidate,
                        cancel_attempt_no,
                        cancel_attempt_started,
                        finished,
                        Some(cancel_status),
                        "cancelled",
                        None,
                        false,
                        cancel_responded_flag.load(Ordering::SeqCst),
                        None,
                        None,
                        Usage::default(),
                        0,
                        Some(cancel_upstream_protocol),
                        Some(cancel_upstream_model),
                    ));
                    cancel_telemetry.emit(Event::RequestFinish {
                        id: cancel_request_id,
                        finished_at: finished.to_rfc3339(),
                        duration_ms: finished
                            .signed_duration_since(cancel_attempt_started)
                            .num_milliseconds(),
                        status: Some(cancel_status),
                        outcome: "cancelled".into(),
                        attempts: cancel_attempt_no,
                        channel_id: Some(cancel_candidate.channel_id),
                        response_bytes: 0,
                    });
                });
                let cancel_aware = CancelAware {
                    inner: upstream_stream,
                    completed: cancel_completed,
                    responded: cancel_responded,
                    on_cancel: Some(on_cancel),
                };
                let stream_body = stream! {
                    let mut upstream_stream = cancel_aware;
                    let mut response_bytes = 0i64;
                    let mut ok = true;
                    let mut pending: Vec<u8> = Vec::new();
                    let mut first_byte_ms: Option<i64> = None;
                    let mut first_token_ms: Option<i64> = None;
                    let mut usage: Usage = Usage::default();
                    while let Some(chunk) = upstream_stream.next().await {
                        match chunk {
                            Ok(chunk) => {
                                response_bytes += chunk.len() as i64;
                                let now = chrono::Utc::now();
                                if first_byte_ms.is_none() {
                                    first_byte_ms = Some(
                                        now.signed_duration_since(attempt_started).num_milliseconds(),
                                    );
                                }
                                pending.extend_from_slice(&chunk);
                                let mut consumed = 0usize;
                                while let Some(offset) =
                                    pending[consumed..].iter().position(|byte| *byte == b'\n')
                                {
                                    let end = consumed + offset;
                                    let line = &pending[consumed..end];
                                    consumed = end + 1;
                                    let line = String::from_utf8_lossy(line);
                                    let line = line.trim_end_matches('\r');
                                    let Some(data) = line.strip_prefix("data:") else {
                                        continue;
                                    };
                                    let data = data.trim();
                                    if data.is_empty() || data == "[DONE]" {
                                        continue;
                                    }
                                    let Ok(value) = serde_json::from_str::<Value>(data) else {
                                        continue;
                                    };
                                    if first_token_ms.is_none()
                                        && convert::chunk_has_content(&stream_protocol, &value)
                                    {
                                        first_token_ms = Some(
                                            now.signed_duration_since(attempt_started)
                                                .num_milliseconds(),
                                        );
                                    }
                                    if let Some(raw_usage) =
                                        stream_usage_value(&stream_protocol, &value)
                                    {
                                        usage = normalize_usage(&stream_protocol, &raw_usage);
                                    }
                                }
                                pending.drain(..consumed);
                                yield Ok::<Bytes, Infallible>(chunk);
                            }
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    let finished = chrono::Utc::now();
                    let finished_str = finished.to_rfc3339();
                    let outcome = if ok { "success" } else { "stream_interrupted" };
                    if ok { telemetry.emit(Event::ChannelSuccess { channel_id:candidate_stream.channel_id.clone() }); }
                    else { telemetry.emit(Event::ChannelFailure { channel_id:candidate_stream.channel_id.clone(), error_kind:"stream_interrupted".into(), status:Some(status.as_u16() as i64), threshold:failure_threshold, open_seconds:circuit_open_seconds, countable:true }); }
                    telemetry.emit(attempt_event(&request_id_stream,&candidate_stream,attempts,attempt_started,finished,Some(status.as_u16() as i64),outcome, if ok {None}else{Some("stream_interrupted".into())},false,true,first_byte_ms,first_token_ms,usage,response_bytes,Some(upstream_protocol_stream),Some(upstream_model_stream)));
                    telemetry.emit(Event::RequestFinish { id:request_id_stream, finished_at:finished_str, duration_ms:chrono::Utc::now().signed_duration_since(started).num_milliseconds(), status:Some(status.as_u16() as i64), outcome:outcome.into(), attempts, channel_id:Some(candidate_stream.channel_id), response_bytes });
                };
                let mut result = Response::new(Body::from_stream(stream_body));
                *result.status_mut() = status;
                *result.headers_mut() = response_headers_for_client;
                return result;
            }
            // C2: mapped entries stream through an incremental converter. A
            // short prelude is buffered first so a 200 response whose body is
            // actually an upstream error can still fail over, and
            // `first_token_timeout_seconds` is honored like the Python
            // gateway. The remaining body is converted event-by-event so the
            // client sees a live stream.
            if stream_requested {
                let failure_threshold = runtime.failure_threshold;
                let circuit_open_seconds = runtime.circuit_open_seconds;
                let first_token_timeout =
                    Duration::from_secs(runtime.first_token_timeout_seconds.max(1) as u64);
                let stream_idle_timeout =
                    Duration::from_secs(runtime.stream_idle_timeout_seconds.max(1) as u64);
                let mut upstream_stream = response.bytes_stream();
                let mut prelude: Vec<Bytes> = Vec::new();
                let mut prelude_bytes = 0usize;
                let mut scan = convert::StreamScan::new(upstream_protocol);
                let prelude_outcome = tokio::time::timeout(
                    first_token_timeout,
                    async {
                        loop {
                            match upstream_stream.next().await {
                                Some(Ok(chunk)) => {
                                    prelude_bytes += chunk.len();
                                    prelude.push(chunk.clone());
                                    if let Some(message) =
                                        scan.feed(upstream_protocol, &chunk)
                                    {
                                        break Err(message);
                                    }
                                    if scan.first_token_now || prelude_bytes >= 1024 * 1024 {
                                        break Ok(());
                                    }
                                }
                                Some(Err(_)) => break Ok(()),
                                None => break Ok(()),
                            }
                        }
                    },
                )
                .await;
                match prelude_outcome {
                    Ok(Err(message)) => {
                        // 2xx body carries an upstream error: record and fail
                        // over to the next candidate (Python prelude path).
                        let finished = chrono::Utc::now();
                        state.telemetry.emit(Event::ChannelFailure {
                            channel_id: candidate.channel_id.clone(),
                            error_kind: "upstream_error".into(),
                            status: Some(502),
                            threshold: failure_threshold,
                            open_seconds: circuit_open_seconds,
                            countable: true,
                        });
                        state.telemetry.emit(attempt_event(
                            &request_id,
                            candidate,
                            attempts,
                            attempt_started,
                            finished,
                            Some(502),
                            "http_error",
                            Some("upstream_error".into()),
                            attempts < candidates.len() as i64,
                            false,
                            None,
                            None,
                            Usage::default(),
                            prelude_bytes as i64,
                            Some(upstream_protocol.into()),
                            Some(upstream_model.into()),
                        ));
                        let _ = message;
                        continue;
                    }
                    Err(_) => {
                        // No first token within first_token_timeout: fail over.
                        let finished = chrono::Utc::now();
                        state.telemetry.emit(Event::ChannelFailure {
                            channel_id: candidate.channel_id.clone(),
                            error_kind: "timeout".into(),
                            status: None,
                            threshold: failure_threshold,
                            open_seconds: circuit_open_seconds,
                            countable: true,
                        });
                        state.telemetry.emit(attempt_event(
                            &request_id,
                            candidate,
                            attempts,
                            attempt_started,
                            finished,
                            None,
                            "transport_error",
                            Some("timeout".into()),
                            attempts < candidates.len() as i64,
                            false,
                            None,
                            None,
                            Usage::default(),
                            prelude_bytes as i64,
                            Some(upstream_protocol.into()),
                            Some(upstream_model.into()),
                        ));
                        continue;
                    }
                    Ok(Ok(())) => {}
                }
                let converter =
                    match convert::MappedStreamConverter::new(
                        entry_protocol,
                        upstream_protocol,
                        &entry_model,
                    ) {
                        Ok(converter) => converter,
                        Err(error) => {
                            return gateway_error(
                                entry_protocol,
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "conversion_error",
                                &error.to_string(),
                                &request_id,
                            );
                        }
                    };
                let stream_protocol = upstream_protocol.to_owned();
                let response_headers_for_client = response_headers.clone();
                let telemetry = state.telemetry.clone();
                let request_id_stream = request_id.clone();
                let candidate_stream = candidate.clone();
                let upstream_protocol_stream = upstream_protocol.to_owned();
                let upstream_model_stream = upstream_model.to_owned();
                let cancel_completed = Arc::new(AtomicBool::new(false));
                let cancel_responded = Arc::new(AtomicBool::new(false));
                let cancel_telemetry = state.telemetry.clone();
                let cancel_request_id = request_id.clone();
                let cancel_candidate = candidate.clone();
                let cancel_attempt_no = attempts;
                let cancel_attempt_started = attempt_started;
                let cancel_status = status.as_u16() as i64;
                let cancel_upstream_protocol = upstream_protocol_stream.clone();
                let cancel_upstream_model = upstream_model_stream.clone();
                let cancel_responded_flag = cancel_responded.clone();
                let on_cancel: Box<dyn FnOnce() + Send> = Box::new(move || {
                    let finished = chrono::Utc::now();
                    cancel_telemetry.emit(attempt_event(
                        &cancel_request_id,
                        &cancel_candidate,
                        cancel_attempt_no,
                        cancel_attempt_started,
                        finished,
                        Some(cancel_status),
                        "cancelled",
                        None,
                        false,
                        cancel_responded_flag.load(Ordering::SeqCst),
                        None,
                        None,
                        Usage::default(),
                        0,
                        Some(cancel_upstream_protocol),
                        Some(cancel_upstream_model),
                    ));
                    cancel_telemetry.emit(Event::RequestFinish {
                        id: cancel_request_id,
                        finished_at: finished.to_rfc3339(),
                        duration_ms: finished
                            .signed_duration_since(cancel_attempt_started)
                            .num_milliseconds(),
                        status: Some(cancel_status),
                        outcome: "cancelled".into(),
                        attempts: cancel_attempt_no,
                        channel_id: Some(cancel_candidate.channel_id),
                        response_bytes: 0,
                    });
                });
                let cancel_aware = CancelAware {
                    inner: upstream_stream,
                    completed: cancel_completed,
                    responded: cancel_responded,
                    on_cancel: Some(on_cancel),
                };
                let stream_body = stream! {
                    let mut upstream_stream = cancel_aware;
                    let mut converter = converter;
                    let mut scan = scan;
                    let mut response_bytes = 0i64;
                    let mut ok = true;
                    let mut idle_timeout = false;
                    let mut error_message: Option<String> = None;
                    let mut first_byte_ms: Option<i64> = None;
                    let mut first_token_ms: Option<i64> = None;
                    // Replay the buffered prelude through the converter.
                    for chunk in prelude {
                        response_bytes += chunk.len() as i64;
                        if first_byte_ms.is_none() {
                            first_byte_ms = Some(
                                chrono::Utc::now()
                                    .signed_duration_since(attempt_started)
                                    .num_milliseconds(),
                            );
                        }
                        let converted = converter.feed(&chunk);
                        if !converted.is_empty() {
                            yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                        }
                    }
                    if first_token_ms.is_none() && scan.first_token() {
                        first_token_ms = Some(
                            chrono::Utc::now()
                                .signed_duration_since(attempt_started)
                                .num_milliseconds(),
                        );
                    }
                    loop {
                        let next = tokio::time::timeout(stream_idle_timeout, upstream_stream.next()).await;
                        match next {
                            Ok(Some(Ok(chunk))) => {
                                response_bytes += chunk.len() as i64;
                                let now = chrono::Utc::now();
                                if first_byte_ms.is_none() {
                                    first_byte_ms = Some(
                                        now.signed_duration_since(attempt_started)
                                            .num_milliseconds(),
                                    );
                                }
                                if let Some(message) = scan.feed(&stream_protocol, &chunk) {
                                    error_message = Some(message);
                                    let converted = converter.error_event(
                                        error_message
                                            .as_deref()
                                            .unwrap_or("Upstream stream error"),
                                    );
                                    if !converted.is_empty() {
                                        yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                                    }
                                    break;
                                }
                                if first_token_ms.is_none() && scan.first_token_now {
                                    first_token_ms = Some(
                                        now.signed_duration_since(attempt_started)
                                            .num_milliseconds(),
                                    );
                                }
                                let converted = converter.feed(&chunk);
                                if !converted.is_empty() {
                                    yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                                }
                            }
                            Ok(Some(Err(_))) => {
                                ok = false;
                                break;
                            }
                            Ok(None) => break,
                            Err(_) => {
                                ok = false;
                                idle_timeout = true;
                                break;
                            }
                        }
                    }
                    // Close any remaining items on a clean finish.
                    if error_message.is_none() && ok {
                        let tail = converter.flush();
                        if !tail.is_empty() {
                            yield Ok::<Bytes, Infallible>(Bytes::from(tail));
                        }
                    }
                    let finished = chrono::Utc::now();
                    let finished_str = finished.to_rfc3339();
                    let (outcome, error_kind, final_status): (&str, Option<String>, i64) =
                        if error_message.is_some() {
                            (
                                "upstream_error",
                                Some("upstream_error".into()),
                                status.as_u16() as i64,
                            )
                        } else if !ok {
                            if idle_timeout {
                                (
                                    "stream_interrupted",
                                    Some("transport_timeout".into()),
                                    504,
                                )
                            } else {
                                (
                                    "stream_interrupted",
                                    Some("stream_interrupted".into()),
                                    status.as_u16() as i64,
                                )
                            }
                        } else {
                            ("success", None, status.as_u16() as i64)
                        };
                    let (input, output, cache_read, cache_write) = converter.usage();
                    let cache_miss = match stream_protocol.as_str() {
                        "claude" => input,
                        "gemini" => input.map(|total| (total - cache_read.unwrap_or(0)).max(0)),
                        _ => match (input, cache_read) {
                            (Some(total), Some(read)) => {
                                Some((total - read - cache_write.unwrap_or(0)).max(0))
                            }
                            _ => None,
                        },
                    };
                    let usage = Usage {
                        input_tokens: input,
                        cache_read_tokens: cache_read,
                        cache_write_tokens: cache_write,
                        cache_miss_input_tokens: cache_miss,
                        output_tokens: output,
                        raw: None,
                    };
                    if outcome == "success" {
                        telemetry.emit(Event::ChannelSuccess {
                            channel_id: candidate_stream.channel_id.clone(),
                        });
                    } else {
                        telemetry.emit(Event::ChannelFailure {
                            channel_id: candidate_stream.channel_id.clone(),
                            error_kind: error_kind
                                .clone()
                                .unwrap_or_else(|| "upstream_error".into()),
                            status: Some(final_status),
                            threshold: failure_threshold,
                            open_seconds: circuit_open_seconds,
                            countable: true,
                        });
                    }
                    telemetry.emit(attempt_event(
                        &request_id_stream,
                        &candidate_stream,
                        attempts,
                        attempt_started,
                        finished,
                        Some(final_status),
                        outcome,
                        error_kind,
                        false,
                        true,
                        first_byte_ms,
                        first_token_ms,
                        usage,
                        response_bytes,
                        Some(upstream_protocol_stream),
                        Some(upstream_model_stream),
                    ));
                    telemetry.emit(Event::RequestFinish {
                        id: request_id_stream,
                        finished_at: finished_str,
                        duration_ms: chrono::Utc::now()
                            .signed_duration_since(started)
                            .num_milliseconds(),
                        status: Some(final_status),
                        outcome: outcome.into(),
                        attempts,
                        channel_id: Some(candidate_stream.channel_id),
                        response_bytes,
                    });
                };
                let mut result = Response::new(Body::from_stream(stream_body));
                *result.status_mut() = status;
                let mut headers = response_headers_for_client;
                headers.remove("content-length");
                headers.insert(
                    "content-type",
                    HeaderValue::from_static("text/event-stream"),
                );
                *result.headers_mut() = headers;
                return result;
            }
            let read = tokio::time::timeout(
                Duration::from_secs(runtime.non_stream_total_timeout_seconds.max(1) as u64),
                response.bytes(),
            )
            .await;
            let raw = match read {
                Ok(Ok(value)) => value.to_vec(),
                _ => {
                    state.telemetry.emit(Event::ChannelFailure {
                        channel_id: candidate.channel_id.clone(),
                        error_kind: "timeout".into(),
                        status: None,
                        threshold: runtime.failure_threshold,
                        open_seconds: runtime.circuit_open_seconds,
                        countable: true,
                    });
                    continue;
                }
            };
            let usage = usage_from_body(upstream_protocol, &raw);
            let mut conversion_failed = false;
            let result_body = if let Some(value) = mapping.as_ref() {
                match convert::convert_response(
                    &value.entry,
                    &value.upstream_protocol,
                    &entry_model,
                    &raw,
                ) {
                    Ok(converted) => converted,
                    Err(error) => {
                        // Conversion failure produces a gateway error body in
                        // the entry format (Python `_mapped_error_body`) plus a
                        // channel_failure telemetry event; the HTTP status
                        // stays 200 like the Python gateway.
                        let payload = if entry_protocol == "claude" {
                            json!({"type": "error", "error": {"type": "gateway_error", "message": error.to_string()}})
                        } else {
                            json!({"error": {"message": error.to_string(), "type": "gateway_error", "code": "conversion_error"}})
                        };
                        conversion_failed = true;
                        state.telemetry.emit(Event::ChannelFailure {
                            channel_id: candidate.channel_id.clone(),
                            error_kind: "conversion_error".into(),
                            status: None,
                            threshold: runtime.failure_threshold,
                            open_seconds: runtime.circuit_open_seconds,
                            countable: false,
                        });
                        serde_json::to_vec(&payload).unwrap_or_else(|_| raw.clone())
                    }
                }
            } else {
                raw
            };
            let mut result = response_with_headers(status, response_headers, result_body.clone());
            if mapping.is_some() {
                result.headers_mut().remove("content-length");
                result.headers_mut().insert(
                    "content-type",
                    HeaderValue::from_static("application/json"),
                );
            }
            if !conversion_failed {
                state.telemetry.emit(Event::ChannelSuccess {
                    channel_id: candidate.channel_id.clone(),
                });
            }
            let finished_at = chrono::Utc::now();
            state.telemetry.emit(attempt_event(
                &request_id,
                candidate,
                attempts,
                attempt_started,
                finished_at,
                Some(status.as_u16() as i64),
                "success",
                None,
                false,
                true,
                None,
                None,
                usage,
                result_body.len() as i64,
                Some(upstream_protocol.into()),
                Some(upstream_model.into()),
            ));
            state.telemetry.emit(Event::RequestFinish {
                id: request_id,
                finished_at: chrono::Utc::now().to_rfc3339(),
                duration_ms: chrono::Utc::now()
                    .signed_duration_since(started)
                    .num_milliseconds(),
                status: Some(status.as_u16() as i64),
                outcome: "success".into(),
                attempts,
                channel_id: Some(candidate.channel_id.clone()),
                response_bytes: result_body.len() as i64,
            });
            return result;
        }
        let raw = match tokio::time::timeout(
            Duration::from_secs(runtime.first_byte_timeout_seconds.max(1) as u64),
            response.bytes(),
        )
        .await
        {
            Ok(Ok(value)) => value.to_vec(),
            _ => Vec::new(),
        };
        let (kind, countable) = status_kind(status);
        state.telemetry.emit(Event::ChannelFailure {
            channel_id: candidate.channel_id.clone(),
            error_kind: kind.into(),
            status: Some(status.as_u16() as i64),
            threshold: runtime.failure_threshold,
            open_seconds: runtime.circuit_open_seconds,
            countable,
        });
        let finished = chrono::Utc::now();
        state.telemetry.emit(attempt_event(
            &request_id,
            candidate,
            attempts,
            attempt_started,
            finished,
            Some(status.as_u16() as i64),
            "http_error",
            Some(kind.into()),
            attempts < candidates.len() as i64,
            false,
            None,
            None,
            Usage::default(),
            raw.len() as i64,
            Some(upstream_protocol.into()),
            Some(upstream_model.into()),
        ));
        last_error = Some((status, response_headers, raw, candidate.channel_id.clone()));
    }
    if let Some((status, headers, raw, channel_id)) = last_error {
        let result_body = if let Some(value) = mapping.as_ref() {
            convert::convert_error(&value.entry, &value.upstream_protocol, &raw)
        } else {
            raw
        };
        state.telemetry.emit(Event::RequestFinish {
            id: request_id,
            finished_at: chrono::Utc::now().to_rfc3339(),
            duration_ms: chrono::Utc::now()
                .signed_duration_since(started)
                .num_milliseconds(),
            status: Some(status.as_u16() as i64),
            outcome: "upstream_error".into(),
            attempts,
            channel_id: Some(channel_id),
            response_bytes: result_body.len() as i64,
        });
        return response_with_headers(status, headers, result_body);
    }
    state.telemetry.emit(Event::RequestFinish {
        id: request_id.clone(),
        finished_at: chrono::Utc::now().to_rfc3339(),
        duration_ms: chrono::Utc::now()
            .signed_duration_since(started)
            .num_milliseconds(),
        status: Some(502),
        outcome: "gateway_error".into(),
        attempts,
        channel_id: None,
        response_bytes: 0,
    });
    gateway_error(
        entry_protocol,
        StatusCode::BAD_GATEWAY,
        "upstream_unreachable",
        "All eligible upstream channels failed before a response was available.",
        &request_id,
    )
}

async fn normal(
    State(state): State<AppState>,
    request: Request,
    protocol: &'static str,
) -> Response<Body> {
    proxy(state, request, protocol, None).await
}

pub async fn openai(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_compatible").await
}
pub async fn responses(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}
pub async fn claude(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "claude").await
}
pub async fn gemini(
    State(state): State<AppState>,
    Path(_action): Path<String>,
    request: Request,
) -> Response<Body> {
    normal(State(state), request, "gemini").await
}
pub async fn claudecode(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "claude").await
}
pub async fn codex(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}

pub async fn models(
    State(state): State<AppState>,
    request: Request,
    protocol: &'static str,
) -> Response<Body> {
    if !settings::authorize_gateway(&state, request.headers(), request.uri().query(), "catalog")
        .await
        .unwrap_or(false)
    {
        return gateway_error(
            protocol,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "catalog",
        );
    }
    let items = match routing::list_routable_models(&state, Some(protocol)).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                &error.to_string(),
                "catalog",
            );
        }
    };
    if protocol == "gemini" {
        json_response(
            StatusCode::OK,
            json!({"models":items.iter().map(|item|json!({"name":format!("models/{}",item.id),"displayName":item.display_name,"supportedGenerationMethods":["generateContent"]})).collect::<Vec<_>>() }),
        )
    } else {
        // Attach capability metadata (context window, max tokens, reasoning,
        // image input, costs) under x_local_gateway.capabilities so catalog
        // consumers such as the omp extension can use real values instead of
        // their built-in defaults. Capabilities are detected on the fly for
        // `auto` rows (C5); models without any capability data omit the field.
        let mut data = Vec::new();
        for item in &items {
            let mut value = json!({
                "id": item.id,
                "object": "model",
                "owned_by": "local-gateway",
                "created": item.created_at,
            });
            match capabilities::get_model_caps(&state, &item.id).await {
                Ok(caps) => {
                    if capabilities::has_capability_data(&caps) {
                        value["x_local_gateway"] = json!({
                            "capabilities": caps,
                            "pi_model_config": capabilities::pi_model_config(&caps),
                        });
                    }
                }
                Err(error) => {
                    return gateway_error(
                        protocol,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "database_error",
                        &error.to_string(),
                        "catalog",
                    );
                }
            }
            data.push(value);
        }
        json_response(StatusCode::OK, json!({ "object": "list", "data": data }))
    }
}

pub async fn mapped_models(
    State(state): State<AppState>,
    request: Request,
    entry: &'static str,
) -> Response<Body> {
    if !settings::authorize_gateway(&state, request.headers(), request.uri().query(), entry)
        .await
        .unwrap_or(false)
    {
        return gateway_error(
            entry,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "catalog",
        );
    }
    let items = match routing::list_mapping_models(&state, entry).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                &error.to_string(),
                "catalog",
            );
        }
    };
    json_response(
        StatusCode::OK,
        json!({"object":"list","data":items.iter().map(|item|json!({"id":item.id,"object":"model","owned_by":"local-gateway","created":item.created_at})).collect::<Vec<_>>() }),
    )
}

pub async fn claudecode_info(State(state): State<AppState>, request: Request) -> Response<Body> {
    if !settings::authorize_gateway(&state, request.headers(), request.uri().query(), "claude")
        .await
        .unwrap_or(false)
    {
        gateway_error(
            "claude",
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "info",
        )
    } else {
        json_response(
            StatusCode::OK,
            json!({"protocol":"claude","endpoint":"/claudecode/v1/messages","models":"/claudecode/v1/models"}),
        )
    }
}
pub async fn codex_info(State(state): State<AppState>, request: Request) -> Response<Body> {
    if !settings::authorize_gateway(
        &state,
        request.headers(),
        request.uri().query(),
        "openai_responses",
    )
    .await
    .unwrap_or(false)
    {
        gateway_error(
            "openai_responses",
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "info",
        )
    } else {
        json_response(
            StatusCode::OK,
            json!({"protocol":"openai_responses","endpoint":"/codex/v1/responses","models":"/codex/v1/models"}),
        )
    }
}

pub async fn openai_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "openai_compatible").await
}
pub async fn responses_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "openai_responses").await
}
pub async fn claude_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "claude").await
}
pub async fn gemini_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "gemini").await
}
pub async fn claudecode_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    mapped_models(State(state), request, "claude").await
}
pub async fn codex_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    mapped_models(State(state), request, "openai_responses").await
}
